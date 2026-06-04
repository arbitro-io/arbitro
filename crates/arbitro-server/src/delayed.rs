//! Delayed publish journal — append-only log + in-memory min-heap.
//!
//! Delayed messages do NOT go into the main store initially. They are
//! written to a separate `delayed.log` file (append-only) and tracked
//! by a min-heap in memory `(deliver_at_ms, offset)`. A background
//! maturation task sleeps until `heap.peek()`, pops mature entries, and
//! moves them to the main store via `gate.release()`.
//!
//! On restart the journal is scanned, the heap rebuilt, and already-matured
//! entries are caught up immediately.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use tokio::sync::watch;

use arbitro_engine_v2::types::StreamId;
use arbitro_store::EntryRef;

use crate::shard::router::ShardRouter;

// ── On-disk record format ──────────────────────────────────────────────
//
// Each record in `delayed.log`:
//
//   deliver_at_ms : u64 LE  (8 bytes)
//   stream_id     : u32 LE  (4 bytes)
//   subject_len   : u16 LE  (2 bytes)
//   payload_len   : u32 LE  (4 bytes)
//   flags         : u8      (1 byte)
//   matured       : u8      (1 byte)  — 0 = pending, 1 = already moved to main store
//   subject       : [u8; subject_len]
//   payload       : [u8; payload_len]
//
// Total header = 20 bytes + variable tail.

const RECORD_HEADER_SIZE: usize = 20;

/// Shared delayed journal handle (Arc-wrapped for cross-task access).
pub type SharedDelayedJournal = std::sync::Arc<Mutex<DelayedJournal>>;

/// An entry waiting for maturation.
#[derive(Debug, Clone)]
pub struct DelayedEntry {
    pub deliver_at_ms: u64,
    pub stream_id: u32,
    pub subject: Vec<u8>,
    pub payload: Vec<u8>,
    pub flags: u8,
    /// Byte offset in the journal file (for marking matured on disk).
    pub file_offset: u64,
}

/// Append-only delayed journal with a min-heap index.
pub struct DelayedJournal {
    /// Path to the `delayed.log` file.
    path: PathBuf,
    /// File handle for appending new delayed entries.
    file: Option<std::fs::File>,
    /// Min-heap: `(deliver_at_ms, offset_in_log)`.
    heap: BinaryHeap<Reverse<(u64, u64)>>,
    /// In-memory store of pending delayed entries, keyed by file offset.
    entries: std::collections::HashMap<u64, DelayedEntry>,
    /// Current write offset in the file.
    write_offset: u64,
}

impl DelayedJournal {
    /// Create a new delayed journal at the given directory.
    /// The file is created lazily on the first `append`.
    pub fn new(data_dir: &Path) -> Self {
        let path = data_dir.join("delayed.log");
        Self {
            path,
            file: None,
            heap: BinaryHeap::new(),
            entries: std::collections::HashMap::new(),
            write_offset: 0,
        }
    }

    /// Open the file for appending (creates if absent).
    fn ensure_file(&mut self) -> std::io::Result<()> {
        if self.file.is_none() {
            if let Some(parent) = self.path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let f = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&self.path)?;
            // Set write_offset to end of file.
            let meta = f.metadata()?;
            self.write_offset = meta.len();
            self.file = Some(f);
        }
        Ok(())
    }

    /// Append a delayed entry to the journal and insert into the heap.
    pub fn append(
        &mut self,
        deliver_at_ms: u64,
        stream_id: u32,
        subject: &[u8],
        payload: &[u8],
        flags: u8,
    ) -> std::io::Result<()> {
        self.ensure_file()?;

        let offset = self.write_offset;
        let subject_len = subject.len() as u16;
        let payload_len = payload.len() as u32;

        let f = self.file.as_mut().unwrap();
        f.write_all(&deliver_at_ms.to_le_bytes())?;
        f.write_all(&stream_id.to_le_bytes())?;
        f.write_all(&subject_len.to_le_bytes())?;
        f.write_all(&payload_len.to_le_bytes())?;
        f.write_all(&[flags])?;
        f.write_all(&[0u8])?; // matured = 0 (pending)
        f.write_all(subject)?;
        f.write_all(payload)?;
        f.flush()?;

        let total = RECORD_HEADER_SIZE as u64 + subject.len() as u64 + payload.len() as u64;
        self.write_offset += total;

        let entry = DelayedEntry {
            deliver_at_ms,
            stream_id,
            subject: subject.to_vec(),
            payload: payload.to_vec(),
            flags,
            file_offset: offset,
        };
        self.heap.push(Reverse((deliver_at_ms, offset)));
        self.entries.insert(offset, entry);

        Ok(())
    }

    /// Peek the earliest maturation timestamp. Returns `None` if heap is empty.
    pub fn peek_deadline_ms(&self) -> Option<u64> {
        self.heap.peek().map(|Reverse((ts, _))| *ts)
    }

    /// Pop all entries whose `deliver_at_ms <= now_ms`.
    /// Returns owned entries ready to be moved to the main store.
    pub fn pop_matured(&mut self, now_ms: u64) -> Vec<DelayedEntry> {
        let mut matured = Vec::new();
        while let Some(&Reverse((ts, offset))) = self.heap.peek() {
            if ts > now_ms {
                break;
            }
            self.heap.pop();
            if let Some(entry) = self.entries.remove(&offset) {
                // Mark as matured on disk (best-effort — if the write fails
                // the recovery scan will catch up on restart).
                self.mark_matured_on_disk(offset);
                matured.push(entry);
            }
        }
        matured
    }

    /// Mark a record as matured on disk by writing `1` to the `matured` byte.
    fn mark_matured_on_disk(&self, offset: u64) {
        // The `matured` byte is at offset + 19 (after the 19 header bytes
        // before it: 8 + 4 + 2 + 4 + 1 = 19).
        let matured_pos = offset + 19;
        if let Ok(mut f) = std::fs::OpenOptions::new().write(true).open(&self.path) {
            use std::io::Seek;
            let _ = f.seek(std::io::SeekFrom::Start(matured_pos));
            let _ = f.write_all(&[1u8]);
        }
    }

    /// Returns `true` if the heap is empty (no pending delayed entries).
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Number of pending delayed entries.
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Recover state from an existing `delayed.log` on disk.
    /// Scans the entire file, rebuilds the heap for un-matured entries,
    /// and returns entries that matured while the broker was down
    /// (deliver_at_ms <= now_ms) for immediate catch-up.
    pub fn recover(&mut self, now_ms: u64) -> std::io::Result<Vec<DelayedEntry>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let mut file = std::fs::File::open(&self.path)?;
        let file_len = file.metadata()?.len();
        let mut offset: u64 = 0;
        let mut catch_up = Vec::new();

        while offset + RECORD_HEADER_SIZE as u64 <= file_len {
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(offset))?;

            let mut header = [0u8; RECORD_HEADER_SIZE];
            if file.read_exact(&mut header).is_err() {
                break; // truncated record — stop
            }

            let deliver_at_ms = u64::from_le_bytes(header[0..8].try_into().unwrap());
            let stream_id = u32::from_le_bytes(header[8..12].try_into().unwrap());
            let subject_len = u16::from_le_bytes(header[12..14].try_into().unwrap()) as usize;
            let payload_len = u32::from_le_bytes(header[14..18].try_into().unwrap()) as usize;
            let flags = header[18];
            let matured = header[19];

            let tail_len = subject_len + payload_len;
            if offset + RECORD_HEADER_SIZE as u64 + tail_len as u64 > file_len {
                break; // truncated record
            }

            let mut tail = vec![0u8; tail_len];
            if file.read_exact(&mut tail).is_err() {
                break;
            }

            let subject = tail[..subject_len].to_vec();
            let payload = tail[subject_len..].to_vec();

            let entry = DelayedEntry {
                deliver_at_ms,
                stream_id,
                subject,
                payload,
                flags,
                file_offset: offset,
            };

            let record_total = RECORD_HEADER_SIZE as u64 + tail_len as u64;

            if matured == 0 {
                // Not yet matured — check if it should have matured by now.
                if deliver_at_ms <= now_ms {
                    // Already matured — catch up immediately.
                    self.mark_matured_on_disk(offset);
                    catch_up.push(entry);
                } else {
                    // Still pending — add to heap.
                    self.heap.push(Reverse((deliver_at_ms, offset)));
                    self.entries.insert(offset, entry);
                }
            }
            // else: already matured on a previous run, skip.

            offset += record_total;
        }

        // Re-open the file in append mode for future writes.
        self.write_offset = offset;
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.file = Some(f);

        Ok(catch_up)
    }
}

// ── Maturation background task ─────────────────────────────────────────

/// Spawn the delayed maturation task. Sleeps until the earliest heap entry
/// is due, pops it, appends to the main store, and calls `gate.release()`.
pub async fn delayed_maturation_loop(
    journal: SharedDelayedJournal,
    server: ShardRouter,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        // Determine how long to sleep.
        let deadline_ms = {
            let j = journal.lock();
            j.peek_deadline_ms()
        };

        match deadline_ms {
            Some(deadline) => {
                let now_ms = current_time_ms();
                if deadline > now_ms {
                    let sleep_dur = std::time::Duration::from_millis(deadline - now_ms);
                    tokio::select! {
                        biased;
                        _ = shutdown.changed() => {
                            tracing::info!("delayed maturation loop shutting down");
                            return;
                        }
                        _ = tokio::time::sleep(sleep_dur) => {}
                    }
                }
                // else: deadline already passed, process immediately
            }
            None => {
                // No pending entries — wait for a notification or shutdown.
                // We poll every 100ms to pick up newly appended entries.
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => {
                        tracing::info!("delayed maturation loop shutting down");
                        return;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        continue;
                    }
                }
            }
        }

        // Pop all matured entries.
        let now_ms = current_time_ms();
        let matured = {
            let mut j = journal.lock();
            j.pop_matured(now_ms)
        };

        for entry in matured {
            // The stream_id stored in the delayed entry is the sequential
            // engine ID (from seq_stream.raw()), not the wire hash. We can
            // construct a StreamId directly.
            let seq_stream = StreamId(entry.stream_id);

            let store_entry = EntryRef {
                stream_id: entry.stream_id,
                subject: &entry.subject,
                payload: &entry.payload,
                flags: entry.flags,
                deliver_at_ms: 0,
            };

            let shared_store = server.store_for(seq_stream);
            match shared_store.lock().append(store_entry, now_ms) {
                Ok(_seq) => {
                    tracing::debug!(
                        stream_id = entry.stream_id,
                        deliver_at_ms = entry.deliver_at_ms,
                        "delayed entry matured and appended to main store"
                    );
                    server.gate_for(seq_stream).release();
                }
                Err(e) => {
                    tracing::error!(
                        stream_id = entry.stream_id,
                        error = ?e,
                        "failed to append matured delayed entry to main store"
                    );
                }
            }
        }
    }
}

/// Get the current time in milliseconds since epoch.
#[inline]
fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
