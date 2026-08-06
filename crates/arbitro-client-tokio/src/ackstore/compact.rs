//! Log compaction — a WAL never deletes records in place, so the file grows
//! with every Record/Confirm/Expire/ConfirmUpTo tombstone until compaction
//! reclaims the space.
//!
//! The writer lock is held for the whole operation, so no appends run
//! concurrently. The OLD file (flushed to a stable point) is re-scanned to
//! derive the live sets — reading the file rather than the in-memory slot maps
//! avoids a lock-order inversion (append path is slot lock → writer lock;
//! taking a slot lock while holding the writer lock would deadlock). The
//! compacted file is written to a temp path, fsync'd, and atomically renamed
//! over the original. After compaction the on-disk representation is
//! `magic + {Register, Snapshot}` per non-empty slot; in-memory state is
//! untouched.

use std::fs::OpenOptions;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::atomic::Ordering;

use super::format::*;
use super::store::StoreError;
use super::wal::{scan_bytes, Wal};

impl Wal {
    /// Rewrite the log so it contains only the live state.
    pub fn compact(&self) -> Result<(), StoreError> {
        let mut w = self.core.writer.lock().unwrap();
        if w.closed {
            return Err(StoreError::Closed);
        }
        // Flush so the file is complete up to a stable point, then read it all.
        w.file.flush()?;
        let f = w.file.get_mut();
        f.seek(SeekFrom::Start(0))?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;

        let ttl_cutoff = self
            .core
            .cfg
            .ttl
            .filter(|t| !t.is_zero())
            .map(|t| (self.core.cfg.now)() - t.as_millis() as i64)
            .unwrap_or(0);
        let (slots, names, _good) = scan_bytes(&data, ttl_cutoff)?;

        // Write compacted content to a temp file in the same dir.
        let tmp_path = self.core.dir.join("ackstore.log.compact");
        let new_size = {
            let tmp = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut bw = BufWriter::new(tmp);
            bw.write_all(&FILE_MAGIC)?;
            let mut size = FILE_MAGIC.len() as i64;

            let now = (self.core.cfg.now)() as u64;
            // Iterate over every REGISTERED slot (`names`), not only the ones
            // with live seqs. A registered-but-empty slot (all seqs confirmed)
            // is still live in the in-memory sym table and can record again;
            // dropping its Register here would orphan those future records on
            // the next replay (slot_id → names lookup fails) and let the id be
            // reused by a different (stream, consumer). We always re-emit the
            // Register, and a Snapshot only when there is live state to persist.
            let mut ids: Vec<u32> = names.keys().copied().collect();
            ids.sort_unstable();
            for id in ids {
                let (stream, consumer) = &names[&id];
                let mut reg = vec![0u8; register_frame_size(stream.len(), consumer.len())];
                let n = encode_register(&mut reg, id, now, stream.as_bytes(), consumer.as_bytes());
                bw.write_all(&reg[..n])?;
                size += n as i64;

                let live = match slots.get(&id) {
                    Some(l) if !l.is_empty() => l,
                    _ => continue,
                };
                let mut seqs: Vec<u64> = live.keys().copied().collect();
                seqs.sort_unstable();
                let mut snap = vec![0u8; snapshot_frame_size(seqs.len())];
                let m = encode_snapshot(&mut snap, id, now, seqs[0], seqs[seqs.len() - 1], &seqs);
                bw.write_all(&snap[..m])?;
                size += m as i64;
            }
            bw.flush()?;
            bw.get_ref().sync_all()?;
            size
        };

        // Atomic swap: rename temp → log, reopen for appends.
        let log_path = self.core.dir.join("ackstore.log");
        std::fs::rename(&tmp_path, &log_path)?;
        let file = OpenOptions::new().read(true).write(true).open(&log_path)?;
        let mut bw = BufWriter::new(file);
        bw.get_mut().seek(SeekFrom::End(0))?;
        w.file = bw;
        w.size = new_size;
        self.core
            .counters
            .recs_since_snap
            .store(0, Ordering::Relaxed);
        Ok(())
    }
}
