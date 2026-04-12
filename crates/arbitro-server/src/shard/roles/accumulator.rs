//! Accumulator role — buffer small publishes → one batched store append.
//!
//! Per `.agent/rules/roles.md` ACCUMULATOR:
//!   * Buffer with caller metadata (conn_id, env_seq, entry_count).
//!   * Reset deadline on enqueue.
//!   * On flush: atomically run publisher path (`append_batch` → reply all
//!     callers with computed seqs → one `gate.release()`).
//!   * On failure: reply `StreamFull` to all buffered callers.
//!
//! Invariant: all buffered callers succeed atomically or all fail.

use std::time::{Duration, Instant};

use arbitro_proto::error::ErrorCode;
use arbitro_store::EntryRef;

use crate::common::reply::{send_error, send_rep_ok};
use crate::shard::command::PublishCmd;
use crate::shard::worker::{AccumCaller, ShardWorker, StreamAccum};

impl ShardWorker {
    pub(in crate::shard) fn handle_publish_accumulate(&mut self, cmd: PublishCmd) {
        if !self.stores.contains_key(&cmd.stream_id) {
            send_error(&self.registry, cmd.conn_id, cmd.env_seq, ErrorCode::StreamNotFound);
            return;
        }

        let entry_count = cmd.entries.len() as u32;
        let entry_bytes: usize = cmd.entries.iter()
            .map(|e| e.subject.len() + e.payload.len())
            .sum();

        let accum = self.accum_streams.entry(cmd.stream_id).or_insert_with(|| StreamAccum {
            store_entries: Vec::new(),
            callers: Vec::new(),
            bytes: 0,
        });

        accum.store_entries.extend(cmd.entries);
        accum.callers.push(AccumCaller {
            conn_id: cmd.conn_id,
            env_seq: cmd.env_seq,
            entry_count,
        });
        accum.bytes += entry_bytes;

        self.accum_total += entry_count as usize;
        self.accum_bytes += entry_bytes;

        // Reset timer on every new entry — flush only after interval_ms of silence
        let interval = Duration::from_millis(self.flusher_config.interval_ms);
        self.accum_deadline = Some(Instant::now() + interval);
    }

    pub(in crate::shard) fn flush_accumulator(&mut self) {
        if self.accum_total == 0 { return; }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let stream_ids: Vec<_> = self.accum_streams.keys().copied().collect();

        for stream_id in stream_ids {
            let accum = self.accum_streams.get_mut(&stream_id).unwrap();
            if accum.store_entries.is_empty() { continue; }

            let store_entries = std::mem::take(&mut accum.store_entries);
            let callers = std::mem::take(&mut accum.callers);
            accum.bytes = 0;

            let store = match self.stores.get_mut(&stream_id) {
                Some(s) => s,
                None => {
                    for caller in &callers {
                        send_error(&self.registry, caller.conn_id, caller.env_seq, ErrorCode::StreamNotFound);
                    }
                    continue;
                }
            };

            let refs: Vec<EntryRef<'_>> = store_entries.iter()
                .map(|e| EntryRef { subject: &e.subject, payload: &e.payload })
                .collect();

            match store.append_batch(&refs, now_ms) {
                Ok(first_seq) => {
                    let mut seq_offset = 0u64;
                    for caller in &callers {
                        send_rep_ok(&self.registry, caller.conn_id, caller.env_seq, first_seq + seq_offset);
                        seq_offset += caller.entry_count as u64;
                    }
                    self.gate.release();
                }
                Err(_) => {
                    for caller in &callers {
                        send_error(&self.registry, caller.conn_id, caller.env_seq, ErrorCode::StreamFull);
                    }
                }
            }
        }

        // Stop timer — no data pending
        self.accum_deadline = None;
        self.accum_total = 0;
        self.accum_bytes = 0;
    }
}
