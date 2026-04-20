//! NameRegistry — server-side translation between client wire identifiers
//! and small sequential engine IDs.
//!
//! ## Why this exists
//!
//! The engine catalog (`arbitro-engine/src/catalog/mod.rs`) stores per-stream
//! match tables in a `Vec<Option<MatchTable>>` indexed directly by
//! `stream_id.0 as usize`, and the engine's edge storage
//! (`arbitro-engine/src/edge/storage.rs`) stores per-consumer deques in a
//! `Vec<VecDeque<...>>` indexed by `consumer_id.0 as usize`. Both grow on
//! insert via `resize_with(idx + N, ...)`.
//!
//! That means **`StreamId` and `ConsumerId` values are interpreted as
//! physical Vec indices**. They MUST be small. Hashing a name with
//! `wire_hash_32` produces values up to ~4_000_000_000, which would resize the
//! catalog Vec to that length and instantly OOM (~300 GB on a 64-bit host).
//! The engine is frozen and cannot be changed, so the constraint lives here
//! on the server side.
//!
//! `SubscriptionId` is HashMap-keyed inside the engine, so it is safe to
//! hash and is NOT handled by this registry.
//!
//! `QueueId` is also HashMap-keyed in the engine catalog, **but** it must
//! be **content-addressed** by `(stream, group)` so that two consumers
//! created with the same group share a single ready ring (queue groups,
//! `DeliverMode::Queue` round-robin). The original code derived this id
//! with `wire_hash_32(group)`, which collides with the StreamId/ConsumerId
//! Vec-index constraint when the engine ever indexes by `QueueId.0`. The
//! registry therefore allocates **deterministic small ints** keyed by the
//! `(seq_stream, group_bytes)` tuple — same group → same id, but small.
//!
//! ## Wire convention
//!
//! Stream IDs are computed **client-side** as `wire_hash_32(name)` and shipped
//! as the `u32` wire stream_id on every frame (the server does not own this
//! value). The registry therefore maintains a `wire_id → seq_id` translation
//! table populated at `CreateStream` time so dispatch can convert wire IDs
//! to small-int engine IDs at the boundary.
//!
//! Consumer IDs are different — the client never computes them; the server
//! returns one in the `CreateConsumer` reply and the client just echoes it
//! on subsequent frames. The registry therefore tracks consumers by **name**
//! (for idempotent re-creates and recovery) and hands out sequential
//! `ConsumerId`s directly.
//!
//! ## Semantics
//!
//! * Sequential IDs start at 0 and only grow. Removal frees the mapping but
//!   does NOT recycle the integer — recycling would clash with in-flight
//!   references and complicate recovery.
//! * Recovery replays Create commands in journal order, so post-restart IDs
//!   match pre-restart IDs as long as the registry is fresh per process.
//! * All operations are cold/control path (CRUD over the wire). A single
//!   `Mutex` is enough — the hot publish/ack path never touches this.

use std::collections::HashMap;
use std::sync::Mutex;

use arbitro_engine_v2::types::{ConsumerId, QueueId, StreamId};

/// Shared wire-id → engine-id registry.
#[derive(Debug)]
pub struct NameRegistry {
    inner: Mutex<Inner>,
}

impl Default for NameRegistry {
    fn default() -> Self {
        Self { inner: Mutex::new(Inner::new()) }
    }
}

#[derive(Debug)]
struct Inner {
    /// Forward translation: wire stream_id (`wire_hash_32(name)`, client-computed)
    /// → small sequential engine `StreamId`.
    /// Sparse key (wire_hash_32 hash) → ahash (rule: sparse IDs).
    streams_by_wire: HashMap<u32, StreamId, foldhash::fast::FixedState>,
    /// Reverse translation: engine `StreamId.0` → wire stream_id. Indexed
    /// directly by seq id; gaps from removed streams stay as `0`. Used by
    /// `ListStreams` to give the client the same wire IDs it computes
    /// locally with `wire_hash_32`.
    streams_seq_to_wire: Vec<u32>,
    next_stream: u32,

    /// Consumers are keyed by name because the wire never carries a
    /// pre-computed consumer id — the server allocates one and the client
    /// echoes it back. Re-creates with the same name return the same id.
    /// Sparse key (arbitrary bytes) → ahash (rule: sparse IDs).
    consumers_by_name: HashMap<Vec<u8>, ConsumerId, foldhash::fast::FixedState>,
    /// Per-consumer queue mapping. Populated at create time so that
    /// `dispatch_subscribe` can recover the same queue id without parsing
    /// the (group-less) Subscribe wire body — guarantees that the binding
    /// reads from the same ready ring `ensure_subscription` writes to via
    /// the match table.
    /// Dense key (ConsumerId) but admin path — HashMap+ahash still
    /// dominates the SipHash default (rule: dense IDs may still use ahash
    /// when direct indexing is impractical across callers).
    consumer_queue: HashMap<ConsumerId, QueueId, foldhash::fast::FixedState>,
    /// Consumer ids start at 1 so `0` can keep its conventional "unset /
    /// invalid" meaning on the wire (and so client tests can sanity-check
    /// that a real id was returned). The engine indexes its per-consumer
    /// Vec by raw `consumer_id.0`, so wasting slot 0 costs one VecDeque.
    next_consumer: u32,

    /// Content-addressed queue ids: `(seq_stream, group_bytes) → QueueId`.
    /// Two consumers with the same group on the same stream MUST resolve
    /// to the same queue id (queue-group semantics). Allocation is shared
    /// across all queue creates so a single counter advances per request.
    /// Composite key with a sparse `Vec<u8>` → ahash (rule: sparse IDs).
    queues_by_key: HashMap<(StreamId, Vec<u8>), QueueId, foldhash::fast::FixedState>,
    next_queue: u32,
}

impl Inner {
    fn new() -> Self {
        Self {
            streams_by_wire: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            streams_seq_to_wire: Vec::new(),
            next_stream: 0,
            consumers_by_name: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            consumer_queue: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            next_consumer: 1,
            queues_by_key: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            // Queue ids start at 1 for the same reason as consumers — leave
            // 0 as "unset" so accidental zeroed-out queue ids are easy to
            // spot in logs.
            next_queue: 1,
        }
    }
}

impl NameRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    // ── Streams ────────────────────────────────────────────────────────────

    /// Translate or allocate: given the client-computed `wire_id`, return
    /// the corresponding engine `StreamId`. Allocates a fresh sequential id
    /// the first time `wire_id` is seen. The `created` flag is `true` only
    /// on first allocation so callers can distinguish a fresh create from
    /// an idempotent retry.
    pub fn get_or_create_stream(&self, wire_id: u32) -> (StreamId, bool) {
        let mut g = self.inner.lock().expect("name registry poisoned");
        if let Some(&id) = g.streams_by_wire.get(&wire_id) {
            return (id, false);
        }
        let id = StreamId(g.next_stream);
        g.next_stream += 1;
        g.streams_by_wire.insert(wire_id, id);
        // Reverse map — grow to cover the new seq slot.
        if (id.0 as usize) >= g.streams_seq_to_wire.len() {
            g.streams_seq_to_wire.resize((id.0 as usize) + 1, 0);
        }
        g.streams_seq_to_wire[id.0 as usize] = wire_id;
        (id, true)
    }

    /// Lookup only — returns `None` if `wire_id` was never registered.
    /// Used by hot dispatch handlers (publish/ack/subscribe) where allocate
    /// would mask "stream does not exist" as "everything is fine".
    pub fn stream_seq(&self, wire_id: u32) -> Option<StreamId> {
        self.inner
            .lock()
            .expect("name registry poisoned")
            .streams_by_wire
            .get(&wire_id)
            .copied()
    }

    /// Reverse translation: engine seq id → wire id. Used by `ListStreams`
    /// when assembling the reply for the client.
    pub fn stream_wire(&self, seq: StreamId) -> Option<u32> {
        let g = self.inner.lock().expect("name registry poisoned");
        g.streams_seq_to_wire.get(seq.0 as usize).copied().filter(|&w| w != 0)
    }

    /// Drop a stream mapping. The integer is intentionally NOT recycled.
    pub fn remove_stream(&self, wire_id: u32) -> Option<StreamId> {
        let mut g = self.inner.lock().expect("name registry poisoned");
        let removed = g.streams_by_wire.remove(&wire_id)?;
        if let Some(slot) = g.streams_seq_to_wire.get_mut(removed.0 as usize) {
            *slot = 0;
        }
        Some(removed)
    }

    // ── Consumers ──────────────────────────────────────────────────────────

    /// Resolve `name` → `ConsumerId`, allocating a fresh sequential id the
    /// first time the name is seen. The `created` flag is `true` only on
    /// first allocation.
    pub fn get_or_create_consumer(&self, name: &[u8]) -> (ConsumerId, bool) {
        let mut g = self.inner.lock().expect("name registry poisoned");
        if let Some(&id) = g.consumers_by_name.get(name) {
            return (id, false);
        }
        let id = ConsumerId(g.next_consumer);
        g.next_consumer += 1;
        g.consumers_by_name.insert(name.to_vec(), id);
        (id, true)
    }

    /// Lookup only — returns `None` if the name has never been registered.
    pub fn consumer_id(&self, name: &[u8]) -> Option<ConsumerId> {
        self.inner
            .lock()
            .expect("name registry poisoned")
            .consumers_by_name
            .get(name)
            .copied()
    }

    /// Drop a consumer mapping. The integer is intentionally NOT recycled.
    pub fn remove_consumer(&self, name: &[u8]) -> Option<ConsumerId> {
        let mut g = self.inner.lock().expect("name registry poisoned");
        let removed = g.consumers_by_name.remove(name)?;
        g.consumer_queue.remove(&removed);
        Some(removed)
    }

    // ── Queues ─────────────────────────────────────────────────────────────

    /// Resolve `(seq_stream, group)` → `QueueId`, allocating a fresh
    /// sequential id the first time the tuple is seen. **Crucially** the
    /// same `(stream, group)` pair always returns the same id, which is
    /// what gives queue groups their round-robin semantics: two consumers
    /// created with the same group share a single ready ring.
    pub fn get_or_create_queue(&self, stream: StreamId, group: &[u8]) -> QueueId {
        let mut g = self.inner.lock().expect("name registry poisoned");
        let key = (stream, group.to_vec());
        if let Some(&id) = g.queues_by_key.get(&key) {
            return id;
        }
        let id = QueueId(g.next_queue);
        g.next_queue += 1;
        g.queues_by_key.insert(key, id);
        id
    }

    /// Record a consumer's resolved queue id so subsequent `Subscribe`
    /// frames (which carry no group) can recover it without re-deriving.
    pub fn set_consumer_queue(&self, consumer: ConsumerId, queue: QueueId) {
        self.inner
            .lock()
            .expect("name registry poisoned")
            .consumer_queue
            .insert(consumer, queue);
    }

    /// Look up the queue id previously associated with `consumer`.
    pub fn consumer_queue(&self, consumer: ConsumerId) -> Option<QueueId> {
        self.inner
            .lock()
            .expect("name registry poisoned")
            .consumer_queue
            .get(&consumer)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streams_translate_wire_to_sequential() {
        let r = NameRegistry::new();
        // Two arbitrary 32-bit hash values such as wire_hash_32 would produce.
        let w_a = 0xDEAD_BEEF;
        let w_b = 0x1234_5678;
        assert_eq!(r.get_or_create_stream(w_a), (StreamId(0), true));
        assert_eq!(r.get_or_create_stream(w_b), (StreamId(1), true));
        assert_eq!(r.get_or_create_stream(w_a), (StreamId(0), false));
        assert_eq!(r.stream_seq(w_a), Some(StreamId(0)));
        assert_eq!(r.stream_seq(w_b), Some(StreamId(1)));
        assert_eq!(r.stream_seq(0xCAFE), None);
    }

    #[test]
    fn streams_reverse_lookup_for_list_replies() {
        let r = NameRegistry::new();
        let w = 0x9999_AAAA;
        let (seq, _) = r.get_or_create_stream(w);
        assert_eq!(r.stream_wire(seq), Some(w));
    }

    #[test]
    fn stream_remove_clears_both_directions_without_recycling() {
        let r = NameRegistry::new();
        r.get_or_create_stream(0x1);
        r.get_or_create_stream(0x2);
        r.remove_stream(0x1);
        // Reverse slot is cleared.
        assert_eq!(r.stream_wire(StreamId(0)), None);
        // New allocations skip past the freed slot.
        assert_eq!(r.get_or_create_stream(0x3), (StreamId(2), true));
    }

    #[test]
    fn queue_groups_share_one_id_per_stream_group_pair() {
        let r = NameRegistry::new();
        let s0 = StreamId(0);
        let s1 = StreamId(1);
        // Same stream + same group → same queue (round-robin sharing).
        let q_a = r.get_or_create_queue(s0, b"workers");
        let q_b = r.get_or_create_queue(s0, b"workers");
        assert_eq!(q_a, q_b);
        // Same group on a different stream → different queue.
        let q_other_stream = r.get_or_create_queue(s1, b"workers");
        assert_ne!(q_a, q_other_stream);
        // Different group on the same stream → different queue.
        let q_other_group = r.get_or_create_queue(s0, b"audit");
        assert_ne!(q_a, q_other_group);
    }

    #[test]
    fn consumer_queue_round_trips() {
        let r = NameRegistry::new();
        let (c, _) = r.get_or_create_consumer(b"alice");
        let q = r.get_or_create_queue(StreamId(0), b"workers");
        r.set_consumer_queue(c, q);
        assert_eq!(r.consumer_queue(c), Some(q));
        // Removing the consumer also drops the queue association.
        r.remove_consumer(b"alice");
        assert_eq!(r.consumer_queue(c), None);
    }

    #[test]
    fn consumers_are_sequential_by_name() {
        let r = NameRegistry::new();
        assert_eq!(r.get_or_create_consumer(b"alice"), (ConsumerId(1), true));
        assert_eq!(r.get_or_create_consumer(b"bob"), (ConsumerId(2), true));
        assert_eq!(r.get_or_create_consumer(b"alice"), (ConsumerId(1), false));
        assert_eq!(r.consumer_id(b"alice"), Some(ConsumerId(1)));
    }
}
