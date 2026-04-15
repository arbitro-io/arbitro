//! Built-in edge indexes as a concrete struct.
//!
//! Level 3 — depends on `types`, `edge/mod`.
//!
//! `BuiltinEdges` holds every secondary index the engine needs as a **direct
//! field**. Hot-path callers write `ctx.edges.pending_by_connection.insert(...)`
//! — one memory load to reach the correct `HashEdge`, no `TypeId` hash, no
//! `Box<dyn Any>` downcast, no virtual dispatch. This replaces the previous
//! `EdgeRegistry` which paid ~15 ns × 7 = ~100 ns of pure dispatch cost
//! per claim and per ack.
//!
//! Each field below documents its access pattern (hot vs cold). Fields used
//! on the hot path are grouped first so the compiler lays them out in cache
//! order of first access during `on_claim_batch` / `release_pending`.

use crate::graph::node::{pending_edge_idx, PendingNode};
use crate::graph::slab::TypedSlab;
use crate::types::*;
use super::{ConsumerSeqEdge, HashEdge, PendingEdge};

/// Concrete container for all engine edges. One field per index.
///
/// Constructed once via [`BuiltinEdges::new`]; fields are accessed directly
/// throughout the codebase. There is no dynamic dispatch and no registry
/// lookup — field identity distinguishes every edge at compile time.
pub struct BuiltinEdges {
    // ── Pending edges — hot path (claim insert / ack remove) ────────────
    //
    // `pending_by_consumer_seq` is the ONE lookup the ack path must do
    // before calling `release_pending`; the other six are only inserted on
    // claim and surgically removed on release (no read in the hot loop).

    /// (ConsumerId, seq) → PendingId. Ack/nack use this to find the exact
    /// pending entry. Hot path on both insert (claim) and lookup+remove (ack).
    ///
    /// Backed by a per-consumer `VecDeque` (not a HashMap) — the ack fast
    /// path is `deques[consumer_id].front() == seq → pop_front`, which is
    /// ~3-5 ns vs ~15-20 ns for a tuple-key HashMap hit.
    pub pending_by_consumer_seq: ConsumerSeqEdge,

    /// Connection → Pending. Used by `drain_connection` (O(k)) and inserted
    /// on every claim / removed on every ack. Intrusive list — O(1) remove.
    pub pending_by_connection: PendingEdge<ConnectionId>,

    /// Consumer → Pending. Used by `drain_consumer`, inflight introspection.
    pub pending_by_consumer: PendingEdge<ConsumerId>,

    /// Queue → Pending. Used by `purge_queue`.
    pub pending_by_queue: PendingEdge<QueueId>,

    /// Subscription → Pending. Used by `drain_subscription`.
    pub pending_by_subscription: PendingEdge<SubscriptionId>,

    // ── Binding edges — cold path (bind/unbind only) ─────────────────────

    /// Connection → Bindings. Used by `drain_connection`.
    pub bindings_by_connection: HashEdge<ConnectionId, SlabKey>,

    /// Subscription → Bindings. Used by `drain_subscription`.
    pub bindings_by_subscription: HashEdge<SubscriptionId, SlabKey>,

    /// Consumer → Bindings. Used by `drain_consumer` and `resolve_binding`.
    pub bindings_by_consumer: HashEdge<ConsumerId, SlabKey>,

    // ── Connection edges — cold path ─────────────────────────────────────

    /// Node → Connections. Used by `drain_node`.
    pub connections_by_node: HashEdge<NodeId, SlabKey>,

    // ── Subscription edges — cold path ───────────────────────────────────

    /// Consumer → Subscriptions. Used by `drain_consumer` and
    /// `resolve_subscription` (test-only cold helper).
    pub subscriptions_by_consumer: HashEdge<ConsumerId, SlabKey>,

    /// Stream → Subscriptions. Used by match-table rebuild.
    pub subscriptions_by_stream: HashEdge<StreamId, SlabKey>,

    // ── Consumer edges — cold path ───────────────────────────────────────

    /// Queue → Consumers. Used by `ready-queue` routing bookkeeping.
    pub consumers_by_queue: HashEdge<QueueId, SlabKey>,

    /// Stream → Consumers. Used by `remove_stream_full` (cascading delete).
    pub consumers_by_stream: HashEdge<StreamId, SlabKey>,
}

impl BuiltinEdges {
    /// Create a fully initialized set of empty edges.
    pub fn new() -> Self {
        Self {
            pending_by_consumer_seq: ConsumerSeqEdge::new(),
            pending_by_connection: PendingEdge::new(pending_edge_idx::CONNECTION),
            pending_by_consumer: PendingEdge::new(pending_edge_idx::CONSUMER),
            pending_by_queue: PendingEdge::new(pending_edge_idx::QUEUE),
            pending_by_subscription: PendingEdge::new(pending_edge_idx::SUBSCRIPTION),
            bindings_by_connection: HashEdge::new(),
            bindings_by_subscription: HashEdge::new(),
            bindings_by_consumer: HashEdge::new(),
            connections_by_node: HashEdge::new(),
            subscriptions_by_consumer: HashEdge::new(),
            subscriptions_by_stream: HashEdge::new(),
            consumers_by_queue: HashEdge::new(),
            consumers_by_stream: HashEdge::new(),
        }
    }
}

impl Default for BuiltinEdges {
    fn default() -> Self { Self::new() }
}

impl BuiltinEdges {
    /// Hot-path: insert a freshly-built `PendingNode` into the slab AND wire
    /// it into all four intrusive pending edges + the consumer-seq edge in
    /// one consolidated call.
    ///
    /// Why this exists: the per-edge `insert_head` path pays
    /// `slab.get_mut(node_key)` **four times** in a row on the same fresh
    /// slot — once to write `edge_prev/next` for each of CONNECTION,
    /// CONSUMER, QUEUE, SUBSCRIPTION. The slot is hot in L1 after the
    /// first call but every lookup still pays the bounds check, generation
    /// check, and `SlabEntry` enum match (~3-5 ns each). Folding the four
    /// writes into one borrow drops three of those redundant lookups.
    ///
    /// Caller invariants:
    /// - `pending` must already have its parent IDs set; `connection_id`,
    ///   `consumer_id`, `queue_id`, `subscription_id`, and `seq` are read
    ///   back from the node.
    /// - `pending.edge_prev` / `edge_next` are overwritten — caller does
    ///   not need to pre-fill them.
    #[inline]
    pub fn insert_pending_all(
        &mut self,
        slab: &mut TypedSlab<PendingNode>,
        mut pending: PendingNode,
    ) -> SlabKey {
        let connection_id   = pending.connection_id;
        let consumer_id     = pending.consumer_id;
        let queue_id        = pending.queue_id;
        let subscription_id = pending.subscription_id;
        let seq             = pending.seq;

        // Reset link slots — we will fill `edge_next` from the heads we
        // are about to swap out below. `edge_prev` stays DANGLING because
        // we are inserting at the head of every list.
        let i_conn = pending_edge_idx::CONNECTION;
        let i_cons = pending_edge_idx::CONSUMER;
        let i_q    = pending_edge_idx::QUEUE;
        let i_sub  = pending_edge_idx::SUBSCRIPTION;

        pending.edge_prev[i_conn] = SlabKey::DANGLING;
        pending.edge_prev[i_cons] = SlabKey::DANGLING;
        pending.edge_prev[i_q]    = SlabKey::DANGLING;
        pending.edge_prev[i_sub]  = SlabKey::DANGLING;

        let key = slab.insert(pending);

        // Swap each edge's head pointer to the freshly-inserted key,
        // collecting the previous heads in one HashMap op per edge.
        let old_conn = self.pending_by_connection.swap_head(connection_id, key);
        let old_cons = self.pending_by_consumer.swap_head(consumer_id, key);
        let old_q    = self.pending_by_queue.swap_head(queue_id, key);
        let old_sub  = self.pending_by_subscription.swap_head(subscription_id, key);

        // Single borrow of the fresh node — write all four `edge_next`
        // slots in one shot. The previous insert_head path borrowed it
        // four times. `expect` is fine here: we just inserted the key.
        {
            let node = slab
                .get_mut(key)
                .expect("insert_pending_all: just-inserted node missing");
            node.edge_next[i_conn] = old_conn;
            node.edge_next[i_cons] = old_cons;
            node.edge_next[i_q]    = old_q;
            node.edge_next[i_sub]  = old_sub;
        }

        // Patch each old head's `edge_prev` slot to point at the new head.
        // Cold case for empty parents: skip when DANGLING. Each branch is
        // an independent borrow so the previous one is released first.
        if old_conn.index != u32::MAX {
            if let Ok(n) = slab.get_mut(old_conn) {
                n.edge_prev[i_conn] = key;
            }
        }
        if old_cons.index != u32::MAX {
            if let Ok(n) = slab.get_mut(old_cons) {
                n.edge_prev[i_cons] = key;
            }
        }
        if old_q.index != u32::MAX {
            if let Ok(n) = slab.get_mut(old_q) {
                n.edge_prev[i_q] = key;
            }
        }
        if old_sub.index != u32::MAX {
            if let Ok(n) = slab.get_mut(old_sub) {
                n.edge_prev[i_sub] = key;
            }
        }

        // Per-consumer seq lookup (separate edge type — VecDeque-backed).
        self.pending_by_consumer_seq.insert(consumer_id, seq, key);

        key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: `pending_by_connection_usage` was removed because `PendingEdge`
    // requires a live `TypedSlab<PendingNode>` for insert/unlink/take. The
    // intrusive edge's unit coverage now lives in `edge/pending_edge.rs`.

    #[test]
    fn pending_by_consumer_seq_unique() {
        let mut edges = BuiltinEdges::new();
        let consumer = ConsumerId(10);

        edges.pending_by_consumer_seq.insert(consumer, 100, SlabKey::new(5, 0));
        edges.pending_by_consumer_seq.insert(consumer, 200, SlabKey::new(6, 0));

        assert_eq!(edges.pending_by_consumer_seq.get(consumer, 100), Some(SlabKey::new(5, 0)));
        assert_eq!(edges.pending_by_consumer_seq.get(consumer, 200), Some(SlabKey::new(6, 0)));
        assert_eq!(edges.pending_by_consumer_seq.get(consumer, 300), None);

        edges.pending_by_consumer_seq.remove(consumer, 100);
        assert_eq!(edges.pending_by_consumer_seq.get(consumer, 100), None);
    }

    #[test]
    fn independent_edges_do_not_collide() {
        // Sanity: edges with the same K/V types are separate fields and
        // never share storage (the raison d'être of ditching the
        // `TypeId`-keyed `EdgeRegistry`). `pending_by_consumer` is now
        // intrusive and needs a slab — this test just checks that the
        // *other* HashEdge indexes start clean and independent.
        let edges = BuiltinEdges::new();
        let c = ConsumerId(1);

        assert!(!edges.pending_by_consumer.contains_key(&c));
        assert_eq!(edges.bindings_by_consumer.get(&c).len(), 0);
        assert_eq!(edges.subscriptions_by_consumer.get(&c).len(), 0);
    }

    #[test]
    fn bindings_by_connection_drain() {
        let mut edges = BuiltinEdges::new();
        let conn = ConnectionId(42);

        edges.bindings_by_connection.insert(&conn, SlabKey::new(0, 0));
        edges.bindings_by_connection.insert(&conn, SlabKey::new(1, 0));
        edges.bindings_by_connection.insert(&conn, SlabKey::new(2, 0));

        let all = edges.bindings_by_connection.take(&conn);
        assert_eq!(all.len(), 3);
        assert!(edges.bindings_by_connection.get(&conn).is_empty());
    }
}
