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
//! ## Hot-path lock-free reads (F1 / S1)
//!
//! The publish hot path needs `stream_seq(wire_id)` AND
//! `stream_idempotency_window_ms(seq)` on *every* publish. The ack hot
//! path needs `consumer_stream(consumer_id)` on every ack. Under
//! 64-conn × 100 k msg/s, hitting a global `Mutex` for every read
//! serialises every publish/ack across every other publish/ack on
//! the broker.
//!
//! Refactor: split the registry into two parts.
//!
//! - **Read-mostly hot-path lookups** live in `Snapshot`, swapped
//!   atomically via `ArcSwap`. Drains/dispatchers do `load()` →
//!   pointer-bump (~1–2 ns), no contention. The snapshot is rebuilt
//!   only on cold admin events (create/delete stream or consumer).
//! - **Sparse / less-hot maps** (`consumers_by_name`, `consumer_queue`,
//!   `consumer_deliver`, `queues_by_key`) stay behind a single
//!   `Mutex<Inner>`. Admin paths take it; the hot path doesn't.
//!
//! Public method signatures are unchanged.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use arbitro_engine_v2::types::{ConsumerId, QueueId, StreamId};

/// Shared wire-id → engine-id registry.
#[derive(Debug)]
pub struct NameRegistry {
    /// Cold/admin path — sparse maps, allocation counters.
    inner: Mutex<Inner>,
    /// Hot path — read-mostly dense snapshots. Replaced on admin events.
    hot: arc_swap::ArcSwap<HotSnapshot>,
}

impl Default for NameRegistry {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner::new()),
            hot: arc_swap::ArcSwap::from_pointee(HotSnapshot::default()),
        }
    }
}

/// Hot-path lookups. Lives behind `ArcSwap` — readers do a single
/// pointer-bump load.
#[derive(Debug, Default)]
struct HotSnapshot {
    /// Forward translation: wire stream_id → small sequential `StreamId`.
    /// Sparse key → foldhash.
    streams_by_wire: HashMap<u32, StreamId, foldhash::fast::FixedState>,
    /// Reverse translation: engine seq → wire stream_id. Indexed by
    /// `StreamId.0`; gaps stay as `0`.
    streams_seq_to_wire: Vec<u32>,
    /// Per-stream idempotency window in milliseconds — fast-bail check
    /// for the publish hot path. Indexed by `StreamId.0`. A value of `0`
    /// means the stream has dedup disabled.
    streams_idempotency_window_ms: Vec<u32>,
    /// Per-consumer stream binding — needed on every ack/nack frame to
    /// recover the stream id (v2 ack body has no stream_id field).
    /// Indexed by `ConsumerId.0`; gaps stay as `u32::MAX` (sentinel).
    consumer_stream: Vec<u32>,
}

impl HotSnapshot {
    /// Build a fresh snapshot from the authoritative `Inner` state.
    fn rebuild_from(inner: &Inner) -> Self {
        // Clone the small dense vectors directly.
        let streams_by_wire = inner.streams_by_wire.clone();
        let streams_seq_to_wire = inner.streams_seq_to_wire.clone();
        let streams_idempotency_window_ms = inner.streams_idempotency_window_ms.clone();
        let consumer_stream = inner.consumer_stream.clone();
        Self {
            streams_by_wire,
            streams_seq_to_wire,
            streams_idempotency_window_ms,
            consumer_stream,
        }
    }
}

#[derive(Debug)]
struct Inner {
    /// Forward translation: wire stream_id (`wire_hash_32(name)`, client-computed)
    /// → small sequential engine `StreamId`. Authoritative copy mirrored
    /// into `HotSnapshot` on every admin mutation.
    streams_by_wire: HashMap<u32, StreamId, foldhash::fast::FixedState>,
    /// Reverse translation: engine `StreamId.0` → wire stream_id. Same shape.
    streams_seq_to_wire: Vec<u32>,
    /// Per-stream idempotency window in milliseconds. Indexed by
    /// `StreamId.0`. Authoritative copy.
    streams_idempotency_window_ms: Vec<u32>,
    next_stream: u32,

    /// Consumers are keyed by name because the wire never carries a
    /// pre-computed consumer id — the server allocates one and the client
    /// echoes it back. Re-creates with the same name return the same id.
    /// Sparse key (arbitrary bytes) → foldhash.
    consumers_by_name: HashMap<Vec<u8>, ConsumerId, foldhash::fast::FixedState>,
    /// Per-consumer queue mapping. Populated at create time so that
    /// `dispatch_subscribe` can recover the same queue id without parsing
    /// the (group-less) Subscribe wire body — guarantees that the binding
    /// reads from the same ready ring `ensure_subscription` writes to via
    /// the match table.
    consumer_queue: HashMap<ConsumerId, QueueId, foldhash::fast::FixedState>,
    /// Per-consumer stream binding, indexed by `ConsumerId.0`. Gaps are
    /// `u32::MAX` (sentinel). Authoritative copy; mirrored to
    /// `HotSnapshot.consumer_stream`.
    consumer_stream: Vec<u32>,
    /// Per-consumer deliver policy (0=All, 1=New, 2=ByStartSeq) + start_seq.
    /// Set at CreateConsumer time, consumed at Subscribe time for cursor positioning.
    consumer_deliver: HashMap<ConsumerId, (u8, u64), foldhash::fast::FixedState>,
    /// Consumer ids start at 1 so `0` can keep its conventional "unset /
    /// invalid" meaning on the wire (and so client tests can sanity-check
    /// that a real id was returned). The engine indexes its per-consumer
    /// Vec by raw `consumer_id.0`, so wasting slot 0 costs one VecDeque.
    next_consumer: u32,

    /// Content-addressed queue ids: `(seq_stream, group_bytes) → QueueId`.
    /// Two consumers with the same group on the same stream MUST resolve
    /// to the same queue id (queue-group semantics).
    queues_by_key: HashMap<(StreamId, Vec<u8>), QueueId, foldhash::fast::FixedState>,
    next_queue: u32,
}

const CONSUMER_STREAM_UNSET: u32 = u32::MAX;

impl Inner {
    fn new() -> Self {
        // M16: a fresh broker typically lands somewhere between 8 and 64
        // streams + consumers. Pre-allocating a modest capacity avoids
        // 4–6 grow-and-copy reallocations during startup recovery
        // without burning real memory.
        const PREALLOC: usize = 32;
        Self {
            streams_by_wire: HashMap::with_capacity_and_hasher(
                PREALLOC,
                foldhash::fast::FixedState::default(),
            ),
            streams_seq_to_wire: Vec::with_capacity(PREALLOC),
            streams_idempotency_window_ms: Vec::with_capacity(PREALLOC),
            next_stream: 0,
            consumers_by_name: HashMap::with_capacity_and_hasher(
                PREALLOC,
                foldhash::fast::FixedState::default(),
            ),
            consumer_queue: HashMap::with_capacity_and_hasher(
                PREALLOC,
                foldhash::fast::FixedState::default(),
            ),
            consumer_stream: Vec::with_capacity(PREALLOC),
            consumer_deliver: HashMap::with_capacity_and_hasher(
                PREALLOC,
                foldhash::fast::FixedState::default(),
            ),
            next_consumer: 1,
            queues_by_key: HashMap::with_capacity_and_hasher(
                PREALLOC,
                foldhash::fast::FixedState::default(),
            ),
            next_queue: 1,
        }
    }
}

impl NameRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Maximum window the broker honours. Matches
    /// `arbitro_server::shard::idempotency::MAX_WINDOW_MS`. Duplicated
    /// here so this crate can clamp without depending on the server.
    pub const MAX_IDEMPOTENCY_WINDOW_MS: u32 = 5 * 60 * 1000;

    /// Maximum number of streams **or** consumers the broker accepts.
    /// Matches `arbitro_server::shard::shared::SLOT_COUNT` — the server's
    /// `SharedCounters` arrays are pre-allocated `Box<[AtomicU32; N]>`
    /// and indexed directly by `consumer_id.0 as usize`. Indexing past
    /// the array panics the shard worker thread (B1 — remote-triggerable
    /// DoS). Allocations beyond this point are rejected at the registry
    /// boundary BEFORE any slot is consumed.
    pub const MAX_SLOT_COUNT: u32 = 4096;

    /// `true` iff a fresh `get_or_create_stream` would succeed today.
    /// `false` once `next_stream` has hit `MAX_SLOT_COUNT`.
    pub fn stream_slots_available(&self) -> bool {
        let g = self.inner.lock().expect("name registry poisoned");
        g.next_stream < Self::MAX_SLOT_COUNT
    }

    /// `true` iff a fresh `get_or_create_consumer` would succeed today.
    pub fn consumer_slots_available(&self) -> bool {
        let g = self.inner.lock().expect("name registry poisoned");
        g.next_consumer < Self::MAX_SLOT_COUNT
    }

    /// Take the inner mutex, run a mutator, then atomically swap a
    /// fresh `HotSnapshot` into `self.hot`. Centralises the
    /// "admin mutation → snapshot rebuild" invariant.
    #[inline]
    fn with_inner_swap<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Inner) -> R,
    {
        let mut g = self.inner.lock().expect("name registry poisoned");
        let out = f(&mut g);
        let snap = HotSnapshot::rebuild_from(&g);
        self.hot.store(Arc::new(snap));
        out
    }

    // ── Streams ────────────────────────────────────────────────────────────

    /// Translate or allocate: given the client-computed `wire_id`, return
    /// the corresponding engine `StreamId`. Allocates a fresh sequential id
    /// the first time `wire_id` is seen. The `created` flag is `true` only
    /// on first allocation so callers can distinguish a fresh create from
    /// an idempotent retry.
    pub fn get_or_create_stream(&self, wire_id: u32) -> (StreamId, bool) {
        self.with_inner_swap(|g| {
            if let Some(&id) = g.streams_by_wire.get(&wire_id) {
                return (id, false);
            }
            // B1: refuse to mint a fresh StreamId past MAX_SLOT_COUNT —
            // the engine indexes per-stream arrays directly by `.0 as
            // usize` and panics on OOB. Returning the sentinel
            // `StreamId(u32::MAX)` with `created=false` lets the
            // dispatcher map it to `ErrorCode::StreamFull` (closest
            // existing wire code for "no room") without inventing a new
            // variant or letting the panic escape.
            if g.next_stream >= Self::MAX_SLOT_COUNT {
                return (StreamId(u32::MAX), false);
            }
            let id = StreamId(g.next_stream);
            g.next_stream += 1;
            g.streams_by_wire.insert(wire_id, id);
            if (id.0 as usize) >= g.streams_seq_to_wire.len() {
                g.streams_seq_to_wire.resize((id.0 as usize) + 1, 0);
            }
            g.streams_seq_to_wire[id.0 as usize] = wire_id;
            if (id.0 as usize) >= g.streams_idempotency_window_ms.len() {
                g.streams_idempotency_window_ms.resize((id.0 as usize) + 1, 0);
            }
            g.streams_idempotency_window_ms[id.0 as usize] = 0;
            (id, true)
        })
    }

    /// Lookup only — returns `None` if `wire_id` was never registered.
    /// **F1 hot path**: lock-free `ArcSwap` load, then HashMap lookup.
    #[inline]
    pub fn stream_seq(&self, wire_id: u32) -> Option<StreamId> {
        self.hot.load().streams_by_wire.get(&wire_id).copied()
    }

    /// Reverse translation: engine seq id → wire id.
    #[inline]
    pub fn stream_wire(&self, seq: StreamId) -> Option<u32> {
        self.hot
            .load()
            .streams_seq_to_wire
            .get(seq.0 as usize)
            .copied()
            .filter(|&w| w != 0)
    }

    /// Drop a stream mapping. The integer is intentionally NOT recycled.
    pub fn remove_stream(&self, wire_id: u32) -> Option<StreamId> {
        self.with_inner_swap(|g| {
            let removed = g.streams_by_wire.remove(&wire_id)?;
            if let Some(slot) = g.streams_seq_to_wire.get_mut(removed.0 as usize) {
                *slot = 0;
            }
            if let Some(slot) = g.streams_idempotency_window_ms.get_mut(removed.0 as usize) {
                *slot = 0;
            }
            Some(removed)
        })
    }

    /// Set the idempotency window for an already-allocated stream. A
    /// non-zero value enables per-stream dedup on the publish hot
    /// path (the value is checked by `stream_idempotency_window_ms`).
    /// A zero value disables it. Setting on an unknown `seq` is a
    /// silent no-op (defensive — recovery may replay out of order).
    ///
    /// **F39**: clamp once here at set time.
    pub fn set_stream_idempotency(&self, seq: StreamId, window_ms: u32) {
        let clamped = window_ms.min(Self::MAX_IDEMPOTENCY_WINDOW_MS);
        self.with_inner_swap(|g| {
            let idx = seq.0 as usize;
            if idx >= g.streams_idempotency_window_ms.len() {
                g.streams_idempotency_window_ms.resize(idx + 1, 0);
            }
            g.streams_idempotency_window_ms[idx] = clamped;
        });
    }

    /// Get the idempotency window for a stream. Returns `0` when the
    /// stream doesn't exist or when idempotency is disabled.
    ///
    /// **F1 hot path**: lock-free `ArcSwap` load + indexed `Vec<u32>`
    /// access — branch-predictor learns the value almost always is 0.
    #[inline]
    pub fn stream_idempotency_window_ms(&self, seq: StreamId) -> u32 {
        self.hot
            .load()
            .streams_idempotency_window_ms
            .get(seq.0 as usize)
            .copied()
            .unwrap_or(0)
    }

    // ── Consumers ──────────────────────────────────────────────────────────

    /// Resolve `name` → `ConsumerId`, allocating a fresh sequential id the
    /// first time the name is seen. The `created` flag is `true` only on
    /// first allocation.
    pub fn get_or_create_consumer(&self, name: &[u8]) -> (ConsumerId, bool) {
        self.with_inner_swap(|g| {
            if let Some(&id) = g.consumers_by_name.get(name) {
                return (id, false);
            }
            // B1: same admission check as `get_or_create_stream`.
            // Sentinel `ConsumerId(u32::MAX)` with `created=false`
            // signals "no slots" to the dispatcher.
            if g.next_consumer >= Self::MAX_SLOT_COUNT {
                return (ConsumerId(u32::MAX), false);
            }
            let id = ConsumerId(g.next_consumer);
            g.next_consumer += 1;
            g.consumers_by_name.insert(name.to_vec(), id);
            // Reserve a slot in `consumer_stream` so future
            // `set_consumer_stream` writes can land directly. Until the
            // caller binds a stream, the slot stays UNSET.
            let idx = id.0 as usize;
            if idx >= g.consumer_stream.len() {
                g.consumer_stream.resize(idx + 1, CONSUMER_STREAM_UNSET);
            }
            (id, true)
        })
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

    /// Drop a consumer mapping by NAME. Removes the wire-name→id mapping
    /// and every reverse index keyed by that id, so a subsequent
    /// `consumer_id(name)`, `consumer_queue(id)`, `consumer_stream(id)` or
    /// `consumer_deliver_policy(id)` all return `None`.
    pub fn remove_consumer(&self, name: &[u8]) -> Option<ConsumerId> {
        self.with_inner_swap(|g| {
            let removed = g.consumers_by_name.remove(name)?;
            g.consumer_queue.remove(&removed);
            if let Some(slot) = g.consumer_stream.get_mut(removed.0 as usize) {
                *slot = CONSUMER_STREAM_UNSET;
            }
            g.consumer_deliver.remove(&removed);
            Some(removed)
        })
    }

    /// Return the `ConsumerId`s currently registered against
    /// `stream_id`. Used by the `DeleteStream` wire handler to figure
    /// out which consumers it must also drop from NameRegistry so the
    /// engine's cascade removal stays in lock-step with the wire-name
    /// → id mapping. O(N) scan of `consumer_stream`; DeleteStream is
    /// a cold admin path.
    pub fn consumers_for_stream(&self, stream_id: StreamId) -> Vec<ConsumerId> {
        let g = self.inner.lock().expect("name registry poisoned");
        let target = stream_id.0;
        g.consumer_stream
            .iter()
            .enumerate()
            .filter_map(|(i, &sid)| {
                if sid != CONSUMER_STREAM_UNSET && sid == target {
                    Some(ConsumerId(i as u32))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Drop a consumer mapping by ID.
    pub fn remove_consumer_by_id(&self, id: ConsumerId) -> Option<Vec<u8>> {
        self.with_inner_swap(|g| {
            let name = g.consumers_by_name
                .iter()
                .find_map(|(n, &v)| if v == id { Some(n.clone()) } else { None })?;
            g.consumers_by_name.remove(&name);
            g.consumer_queue.remove(&id);
            if let Some(slot) = g.consumer_stream.get_mut(id.0 as usize) {
                *slot = CONSUMER_STREAM_UNSET;
            }
            g.consumer_deliver.remove(&id);
            Some(name)
        })
    }

    // ── Queues ─────────────────────────────────────────────────────────────

    /// Resolve `(seq_stream, group)` → `QueueId`, allocating a fresh
    /// sequential id the first time the tuple is seen.
    pub fn get_or_create_queue(&self, stream: StreamId, group: &[u8]) -> QueueId {
        // Queues only live in `Inner`; no snapshot rebuild needed for them.
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

    /// Record a consumer's owning stream so v2 `SubFrame` (which has no
    /// `stream_id` in its body) can recover the routing target from just
    /// the `ConsumerId`. Should be set together with `set_consumer_queue`.
    pub fn set_consumer_stream(&self, consumer: ConsumerId, stream: StreamId) {
        self.with_inner_swap(|g| {
            let idx = consumer.0 as usize;
            if idx >= g.consumer_stream.len() {
                g.consumer_stream.resize(idx + 1, CONSUMER_STREAM_UNSET);
            }
            g.consumer_stream[idx] = stream.0;
        });
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

    /// Look up the owning stream of `consumer`.
    ///
    /// **F1 hot path**: lock-free `ArcSwap` load + indexed `Vec<u32>`
    /// access — every ack/nack/disconnect frame hits this path.
    #[inline]
    pub fn consumer_stream(&self, consumer: ConsumerId) -> Option<StreamId> {
        let snap = self.hot.load();
        let slot = *snap.consumer_stream.get(consumer.0 as usize)?;
        if slot == CONSUMER_STREAM_UNSET {
            None
        } else {
            Some(StreamId(slot))
        }
    }

    /// Store deliver policy for a consumer (set at CreateConsumer time).
    pub fn set_consumer_deliver_policy(
        &self,
        consumer: ConsumerId,
        deliver_policy: u8,
        start_seq: u64,
    ) {
        self.inner
            .lock()
            .expect("name registry poisoned")
            .consumer_deliver
            .insert(consumer, (deliver_policy, start_seq));
    }

    /// Look up the deliver policy for a consumer. Returns `(policy, start_seq)`.
    pub fn consumer_deliver_policy(&self, consumer: ConsumerId) -> Option<(u8, u64)> {
        self.inner
            .lock()
            .expect("name registry poisoned")
            .consumer_deliver
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

    #[test]
    fn consumer_stream_lookup_is_lock_free_round_trip() {
        let r = NameRegistry::new();
        let (c, _) = r.get_or_create_consumer(b"alice");
        let s = StreamId(7);
        r.set_consumer_stream(c, s);
        assert_eq!(r.consumer_stream(c), Some(s));
        assert_eq!(r.consumer_stream(ConsumerId(999)), None);
    }

    #[test]
    fn consumers_for_stream_via_dense_index() {
        let r = NameRegistry::new();
        let (c1, _) = r.get_or_create_consumer(b"alice");
        let (c2, _) = r.get_or_create_consumer(b"bob");
        let (c3, _) = r.get_or_create_consumer(b"carol");
        let s = StreamId(3);
        r.set_consumer_stream(c1, s);
        r.set_consumer_stream(c2, StreamId(4));
        r.set_consumer_stream(c3, s);
        let mut found = r.consumers_for_stream(s);
        found.sort_by_key(|c| c.0);
        assert_eq!(found, vec![c1, c3]);
    }
}
