//! Publisher role — accept publish → persist → ack.
//!
//! Per `.agent/rules/roles.md` PUBLISHER:
//!   1. `store.append_batch`
//!   2. Reply `RepOk(first_seq)` or `RepError`
//!   3. `gate.release()` after ack
//!
//! MUST NOT: touch engine, read store after write, build Deliver frames,
//! know about consumers, couple latency to drainer.
//!
//! Invariant: zero subscribers cost = `store.append + reply + gate.release`.

use arbitro_proto::error::ErrorCode;
use arbitro_store::EntryRef;

use crate::common::reply::{send_error, send_rep_ok};
use crate::lifecycle_trace;
use crate::shard::command::PublishCmd;
use crate::shard::worker::ShardWorker;

impl ShardWorker {
    pub(in crate::shard) fn handle_publish(&mut self, cmd: PublishCmd) {
        lifecycle_trace::record("10_publisher_enter", cmd.conn_id, cmd.entries.len() as u64, "shard");
        // 1. Stream exists?
        let store = match self.stores.get_mut(&cmd.stream_id) {
            Some(s) => s,
            None => {
                send_error(&self.registry, cmd.conn_id, cmd.env_seq, ErrorCode::StreamNotFound);
                return;
            }
        };

        // 2. Store — persist (source of truth)
        let store_entries: Vec<EntryRef<'_>> = cmd.entries.iter().map(|e| {
            EntryRef {
                subject: &e.subject,
                payload: &e.payload,
            }
        }).collect();

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        lifecycle_trace::record("11_pub_store_append_start", cmd.conn_id, 0, "shard");

        let first_seq = match store.append_batch(&store_entries, now_ms) {
            Ok(seq) => seq,
            Err(_) => {
                send_error(&self.registry, cmd.conn_id, cmd.env_seq, ErrorCode::StreamFull);
                return;
            }
        };
        lifecycle_trace::record("12_pub_store_append_done", cmd.conn_id, first_seq, "shard");

        // 3. Reply + signal — engine processing happens in drain_deliver
        send_rep_ok(&self.registry, cmd.conn_id, cmd.env_seq, first_seq);
        lifecycle_trace::record("13_pub_rep_ok_sent", cmd.conn_id, first_seq, "shard");
        self.gate.release();
        lifecycle_trace::record("14_pub_gate_released", cmd.conn_id, first_seq, "shard");
    }
}
