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
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{watch, Notify};

use arbitro_engine_v2::types::StreamId;
use arbitro_store::EntryRef;

use crate::config::FsyncPolicy;
use crate::shard::router::ShardRouter;

/// SEC-8: maximum allowed delay from now, in milliseconds (7 days).
/// Guards against a client scheduling an entry so far in the future
/// that it never realistically matures, while also bounding
/// `deliver_at_ms` arithmetic.
pub const MAX_DELAY_MS: u64 = 604_800_000;

/// SEC-8: maximum number of pending (un-matured) delayed entries held
/// in the journal at once. Guards against unbounded growth of the
/// in-memory heap/index and the on-disk log.
pub const MAX_PENDING_DELAYED: usize = 100_000;

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

/// SEC-8: `append()` failure modes, distinct from raw I/O errors so
/// callers can map them to specific wire `ErrorCode`s.
#[derive(Debug)]
pub enum DelayedAppendError {
    /// `deliver_at_ms - now_ms` exceeded `MAX_DELAY_MS`.
    DelayTooLarge,
    /// The journal already holds `MAX_PENDING_DELAYED` un-matured entries.
    TooManyPending,
    /// Underlying file I/O failure.
    Io(std::io::Error),
}

impl std::fmt::Display for DelayedAppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DelayTooLarge => write!(f, "delay exceeds MAX_DELAY_MS ({MAX_DELAY_MS}ms)"),
            Self::TooManyPending => {
                write!(
                    f,
                    "too many pending delayed entries (max {MAX_PENDING_DELAYED})"
                )
            }
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for DelayedAppendError {}

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
    /// ROB-5: persistent write handle used by `mark_matured_on_disk`,
    /// opened once instead of re-opening the file on every matured
    /// entry. Seeks + writes the `matured` byte for a batch, then a
    /// single `sync_data()` call durably commits the whole batch.
    mark_file: Option<std::fs::File>,
    /// Min-heap: `(deliver_at_ms, offset_in_log)`.
    heap: BinaryHeap<Reverse<(u64, u64)>>,
    /// In-memory store of pending delayed entries, keyed by file offset.
    entries: std::collections::HashMap<u64, DelayedEntry>,
    /// Current write offset in the file.
    write_offset: u64,
    /// ROB-4: fsync policy for the journal, mirrors the metadata
    /// command log's policy. Read from `ARBITRO_FSYNC_POLICY` since
    /// this type is constructed with only a data-dir path.
    fsync_policy: FsyncPolicy,
    /// ROB-6: signalled on every successful `append()` so the
    /// maturation loop can wake up immediately and recompute its
    /// sleep deadline instead of polling.
    notify: Arc<Notify>,
}

impl DelayedJournal {
    /// Create a new delayed journal at the given directory.
    /// The file is created lazily on the first `append`.
    pub fn new(data_dir: &Path) -> Self {
        let path = data_dir.join("delayed.log");
        // Mirrors config.rs: durability is opt-in. Default = no per-write fsync.
        let fsync_policy = match std::env::var("ARBITRO_FSYNC_POLICY").as_deref() {
            Ok("every") => FsyncPolicy::Every,
            _ => FsyncPolicy::None,
        };
        Self {
            path,
            file: None,
            mark_file: None,
            heap: BinaryHeap::new(),
            entries: std::collections::HashMap::new(),
            write_offset: 0,
            fsync_policy,
            notify: Arc::new(Notify::new()),
        }
    }

    /// ROB-6: clone the notify handle so the maturation loop can await
    /// it alongside the sleep deadline.
    pub fn notify_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
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
        self.ensure_mark_file()
    }

    /// ROB-5: open the persistent write handle used to flip `matured`
    /// bytes, once, instead of re-opening the file on every entry.
    fn ensure_mark_file(&mut self) -> std::io::Result<()> {
        if self.mark_file.is_none() {
            let f = std::fs::OpenOptions::new().write(true).open(&self.path)?;
            self.mark_file = Some(f);
        }
        Ok(())
    }

    /// Append a delayed entry to the journal and insert into the heap.
    ///
    /// SEC-8: rejects entries scheduled further than `MAX_DELAY_MS` from
    /// `now_ms`, and rejects new entries once `MAX_PENDING_DELAYED` are
    /// already pending.
    pub fn append(
        &mut self,
        now_ms: u64,
        deliver_at_ms: u64,
        stream_id: u32,
        subject: &[u8],
        payload: &[u8],
        flags: u8,
    ) -> Result<(), DelayedAppendError> {
        if deliver_at_ms.saturating_sub(now_ms) > MAX_DELAY_MS {
            return Err(DelayedAppendError::DelayTooLarge);
        }
        if self.entries.len() >= MAX_PENDING_DELAYED {
            return Err(DelayedAppendError::TooManyPending);
        }

        self.ensure_file().map_err(DelayedAppendError::Io)?;

        let offset = self.write_offset;
        let subject_len = subject.len() as u16;
        let payload_len = payload.len() as u32;

        let f = self.file.as_mut().unwrap();
        f.write_all(&deliver_at_ms.to_le_bytes())
            .map_err(DelayedAppendError::Io)?;
        f.write_all(&stream_id.to_le_bytes())
            .map_err(DelayedAppendError::Io)?;
        f.write_all(&subject_len.to_le_bytes())
            .map_err(DelayedAppendError::Io)?;
        f.write_all(&payload_len.to_le_bytes())
            .map_err(DelayedAppendError::Io)?;
        f.write_all(&[flags]).map_err(DelayedAppendError::Io)?;
        f.write_all(&[0u8]).map_err(DelayedAppendError::Io)?; // matured = 0 (pending)
        f.write_all(subject).map_err(DelayedAppendError::Io)?;
        f.write_all(payload).map_err(DelayedAppendError::Io)?;
        f.flush().map_err(DelayedAppendError::Io)?;
        // ROB-4: real durability — `flush()` only drains userspace
        // buffers, it does not force the write to stable storage. A
        // delayed entry the client believes is durable must survive a
        // crash, so fsync it before acknowledging (unless the operator
        // opted out via ARBITRO_FSYNC_POLICY=none).
        if self.fsync_policy == FsyncPolicy::Every {
            f.sync_data().map_err(DelayedAppendError::Io)?;
        }

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

        // ROB-6: wake the maturation loop so it can recompute its sleep
        // deadline against the newly inserted entry instead of waiting
        // out whatever deadline it was already sleeping on.
        self.notify.notify_one();

        Ok(())
    }

    /// Peek the earliest maturation timestamp. Returns `None` if heap is empty.
    pub fn peek_deadline_ms(&self) -> Option<u64> {
        self.heap.peek().map(|Reverse((ts, _))| *ts)
    }

    /// Pop all entries whose `deliver_at_ms <= now_ms`.
    /// Returns owned entries ready to be moved to the main store.
    ///
    /// ROB-5: matured offsets are written through the persistent
    /// `mark_file` handle and a single `sync_data()` commits the whole
    /// batch, instead of opening the file and fsyncing once per entry.
    pub fn pop_matured(&mut self, now_ms: u64) -> Vec<DelayedEntry> {
        let mut matured = Vec::new();
        let mut offsets = Vec::new();
        while let Some(&Reverse((ts, offset))) = self.heap.peek() {
            if ts > now_ms {
                break;
            }
            self.heap.pop();
            if let Some(entry) = self.entries.remove(&offset) {
                offsets.push(offset);
                matured.push(entry);
            }
        }
        if !offsets.is_empty() {
            self.mark_matured_on_disk(&offsets);
        }
        matured
    }

    /// Mark a batch of records as matured on disk by writing `1` to each
    /// record's `matured` byte, then issuing one `sync_data()` for the
    /// whole batch. Best-effort — if the write/sync fails the recovery
    /// scan will catch up on restart.
    fn mark_matured_on_disk(&mut self, offsets: &[u64]) {
        if self.ensure_mark_file().is_err() {
            return;
        }
        let Some(f) = self.mark_file.as_mut() else {
            return;
        };
        use std::io::Seek;
        for &offset in offsets {
            // The `matured` byte is at offset + 19 (after the 19 header
            // bytes before it: 8 + 4 + 2 + 4 + 1 = 19).
            let matured_pos = offset + 19;
            if f.seek(std::io::SeekFrom::Start(matured_pos)).is_err() {
                continue;
            }
            let _ = f.write_all(&[1u8]);
        }
        let _ = f.flush();
        if self.fsync_policy == FsyncPolicy::Every {
            let _ = f.sync_data();
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
        let mut catch_up_offsets = Vec::new();

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
                    catch_up_offsets.push(offset);
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

        if !catch_up_offsets.is_empty() {
            self.mark_matured_on_disk(&catch_up_offsets);
        }

        Ok(catch_up)
    }
}

// ── Maturation background task ─────────────────────────────────────────

/// Spawn the delayed maturation task. Sleeps until the earliest heap entry
/// is due, pops it, appends to the main store, and calls `gate.release()`.
///
/// ROB-6: waits on `journal`'s `Notify` alongside the sleep deadline so a
/// freshly appended entry with an earlier deadline than the one we're
/// currently sleeping on wakes the loop immediately, instead of waiting
/// out the stale deadline (or, when idle, polling every 100ms).
pub async fn delayed_maturation_loop(
    journal: SharedDelayedJournal,
    server: ShardRouter,
    mut shutdown: watch::Receiver<bool>,
) {
    let notify = journal.lock().notify_handle();

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
                        _ = notify.notified() => {
                            // A new entry landed — recompute the deadline,
                            // it may now be earlier than what we slept on.
                            continue;
                        }
                    }
                }
                // else: deadline already passed, process immediately
            }
            None => {
                // No pending entries — wait for a notification or shutdown.
                // No polling: Notify wakes us the instant append() inserts
                // the first entry into an empty heap.
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => {
                        tracing::info!("delayed maturation loop shutting down");
                        return;
                    }
                    _ = notify.notified() => {
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
