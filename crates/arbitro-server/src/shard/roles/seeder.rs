//! Seeder role — bulk-load store entries into engine ready state.
//!
//! Per `.agent/rules/roles.md` SEEDER:
//!   1. Temp-remove `stores[id]` to avoid borrow conflict.
//!   2. `store.for_each` → `engine.enqueue_ready(...)` per entry.
//!   3. Reinsert store.
//!   4. `seeded_streams.insert(id)`.
//!   5. `last_engine_seq[id] = info.last_seq`.
//!   6. `ctx.next_seq = max(ctx.next_seq, last_seq + 1)`.
//!
//! MUST NOT: call `engine.publish` (would reassign seqs + double-count
//! `next_seq`); send Deliver frames; run concurrently with
//! `publish_pending_to_engine` on same stream; run twice on same stream
//! (check `seeded_streams` first).

use arbitro_engine_v2::types::StreamId;

use crate::shard::command::SeedStoresCmd;
use crate::shard::worker::ShardWorker;

impl ShardWorker {
    /// Seed engine from a specific stream's store. Temporarily removes the
    /// store from the map to avoid borrow conflicts with the engine.
    pub(in crate::shard) fn seed_from_store(
        &mut self,
        stream_id: StreamId,
        info: &arbitro_store::StoreInfo,
    ) -> u64 {
        let first = info.first_seq;
        let end = info.last_seq + 1;
        let mut seeded = 0u64;

        // Temporarily take store out to avoid borrow conflict with self.engine
        let store = match self.stores.remove(&stream_id) {
            Some(s) => s,
            None => return 0,
        };

        store.for_each(first, end, &mut |entry| {
            let subject_hash = arbitro_engine_v2::catalog::fnv1a_32(entry.subject);
            self.engine.enqueue_ready(stream_id, entry.subject, subject_hash, entry.seq);
            seeded += 1;
        }).ok();

        // Put store back + mark engine seq as caught up.
        // Also advance ctx.next_seq so subsequent engine.publish() calls assign
        // seqs that match the store (store seqs come after last_seq).
        self.stores.insert(stream_id, store);
        self.seeded_streams.insert(stream_id);
        self.last_engine_seq.insert(stream_id, info.last_seq);
        let next = info.last_seq + 1;
        if self.engine.ctx().next_seq < next {
            self.engine.ctx_mut().next_seq = next;
        }

        if seeded > 0 {
            tracing::info!(
                stream_id = stream_id.raw(),
                messages = seeded,
                "seeded engine from store"
            );
        }

        seeded
    }

    /// Handle `SeedStores` command — seed engine from all non-empty stores.
    /// Called after ALL recovery commands (streams + consumers) are replayed.
    pub(in crate::shard) fn handle_seed_stores(&mut self, cmd: SeedStoresCmd) {
        let mut total_seeded = 0u64;

        let stream_ids: Vec<StreamId> = self.stores.keys().copied().collect();
        for stream_id in stream_ids {
            if self.seeded_streams.contains(&stream_id) {
                continue;
            }
            let info = self.stores[&stream_id].info();
            if info.messages == 0 {
                continue;
            }
            total_seeded += self.seed_from_store(stream_id, &info);
        }

        let _ = cmd.reply.send(total_seeded);
    }
}
