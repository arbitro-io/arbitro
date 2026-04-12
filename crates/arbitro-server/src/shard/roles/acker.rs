//! Acker role — feed consumer ack/nack back into engine state.
//!
//! Per `.agent/rules/roles.md` ACKER:
//!   1. Copy entries into `scratch_ack` / `scratch_nack` (engine borrow rules).
//!   2. Call `engine.ack` / `engine.nack`.
//!   3. Reply with counts.
//!   4. `gate.release()` iff `accepted > 0` (ack) or `requeued > 0` (nack).
//!
//! Invariant: engine state reflects the request exactly. Gate released iff
//! the drainer would find new work.
//!
//! `handle_claim` is a **test-only** direct probe — it bypasses the drainer
//! and returns entries to the caller. Production delivery flows through
//! `drainer::handle_drain_deliver` shipping `RepBatch` frames. Lives here
//! because it shares the ack batch/scratch discipline.

use arbitro_engine_v2::batch::{AckBatch, ClaimBatch, NackBatch};

use crate::lifecycle_trace;
use crate::shard::command::{AckCmd, AckReply, ClaimCmd, NackCmd, NackReply};
use crate::shard::worker::ShardWorker;

impl ShardWorker {
    pub(in crate::shard) fn handle_ack(&mut self, cmd: AckCmd) {
        lifecycle_trace::record("a10_acker_enter", 0, cmd.entries.len() as u64, "shard");
        self.scratch_ack.clear();
        self.scratch_ack.extend_from_slice(&cmd.entries);

        lifecycle_trace::record("a11_engine_ack_start", 0, 0, "shard");
        let result = self.engine.ack(&AckBatch {
            consumer_id: cmd.consumer_id,
            entries: &self.scratch_ack,
            now: cmd.now,
        });
        lifecycle_trace::record("a12_engine_ack_done", 0, result.accepted as u64, "shard");

        if result.accepted > 0 {
            self.gate.release();
            lifecycle_trace::record("a13_acker_gate_released", 0, 0, "shard");
        }

        let _ = cmd.reply.send(AckReply {
            accepted: result.accepted,
            rejected: result.rejected,
        });
        lifecycle_trace::record("a14_acker_reply_sent", 0, 0, "shard");
    }

    pub(in crate::shard) fn handle_nack(&mut self, cmd: NackCmd) {
        self.scratch_nack.clear();
        self.scratch_nack.extend_from_slice(&cmd.entries);

        let result = self.engine.nack(&NackBatch {
            consumer_id: cmd.consumer_id,
            entries: &self.scratch_nack,
            now: cmd.now,
        });

        let requeued = result.accepted;
        let _ = cmd.reply.send(NackReply {
            requeued,
            not_found: result.rejected,
        });

        // Wake drain task to redeliver requeued messages
        if requeued > 0 {
            self.gate.release();
        }
    }

    /// Test-only direct claim — bypass the drainer and return entries to the
    /// caller. Production code never reaches this; the wire path delivers via
    /// `RepBatch` from `drainer::handle_drain_deliver`. Integration tests use
    /// it to inspect engine state without spinning up a full TCP client.
    pub(in crate::shard) fn handle_claim(&mut self, cmd: ClaimCmd) {
        // First feed any pending journal entries so the engine has work to claim,
        // mirroring what the drainer would do before its claim loop.
        self.publish_pending_to_engine(cmd.now);

        let batch = ClaimBatch {
            queue_id: cmd.queue_id,
            connection_id: cmd.connection_id,
            consumer_id: cmd.consumer_id,
            max_items: cmd.max_items,
            now: cmd.now,
        };
        // Cold-path: tests don't cache subscription/binding IDs the way
        // `drainer::handle_drain_deliver` does, so resolve them via the
        // engine's edge indexes on each call.
        let (subscription_id, binding_id) =
            arbitro_engine_v2::runtime::claim::resolve_ids_for_batch(self.engine.ctx(), &batch);
        let claimed = self.engine.claim(&batch, subscription_id, binding_id);
        let entries = claimed.entries().to_vec();
        let _ = cmd.reply.send(entries);
    }
}
