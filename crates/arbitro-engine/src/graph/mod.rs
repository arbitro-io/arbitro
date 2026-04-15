//! GraphStore — owns all typed slabs. Source of truth for entity state.
//!
//! Level 3 — depends on `types`, `error`, `graph/slab`, `graph/node`.

pub mod node;
pub mod slab;

use crate::error::EngineResult;
use crate::types::SlabKey;
use node::*;
use slab::TypedSlab;

/// The graph store: owns one `TypedSlab` per entity type.
///
/// All entity state lives here. Edges (in `BuiltinEdges`) are secondary
/// indexes — the slabs are the source of truth.
pub struct GraphStore {
    pub connections: TypedSlab<ConnectionNode>,
    pub consumers: TypedSlab<ConsumerNode>,
    pub subscriptions: TypedSlab<SubscriptionNode>,
    pub bindings: TypedSlab<BindingNode>,
    pub pending: TypedSlab<PendingNode>,
    pub queues: TypedSlab<QueueNode>,
    pub streams: TypedSlab<StreamNode>,
}

impl GraphStore {
    pub fn new() -> Self {
        Self {
            connections: TypedSlab::new(),
            consumers: TypedSlab::new(),
            subscriptions: TypedSlab::new(),
            bindings: TypedSlab::new(),
            pending: TypedSlab::with_capacity(1024),
            queues: TypedSlab::new(),
            streams: TypedSlab::new(),
        }
    }

    // ── Connection ───────────────────────────────────────────────────────

    #[inline]
    pub fn insert_connection(&mut self, node: ConnectionNode) -> SlabKey {
        self.connections.insert(node)
    }

    #[inline]
    pub fn get_connection(&self, key: SlabKey) -> EngineResult<&ConnectionNode> {
        self.connections.get(key)
    }

    #[inline]
    pub fn remove_connection(&mut self, key: SlabKey) -> EngineResult<ConnectionNode> {
        self.connections.remove(key)
    }

    // ── Consumer ─────────────────────────────────────────────────────────

    #[inline]
    pub fn insert_consumer(&mut self, node: ConsumerNode) -> SlabKey {
        self.consumers.insert(node)
    }

    #[inline]
    pub fn get_consumer(&self, key: SlabKey) -> EngineResult<&ConsumerNode> {
        self.consumers.get(key)
    }

    #[inline]
    pub fn get_consumer_mut(&mut self, key: SlabKey) -> EngineResult<&mut ConsumerNode> {
        self.consumers.get_mut(key)
    }

    #[inline]
    pub fn remove_consumer(&mut self, key: SlabKey) -> EngineResult<ConsumerNode> {
        self.consumers.remove(key)
    }

    // ── Subscription ─────────────────────────────────────────────────────

    #[inline]
    pub fn insert_subscription(&mut self, node: SubscriptionNode) -> SlabKey {
        self.subscriptions.insert(node)
    }

    #[inline]
    pub fn get_subscription(&self, key: SlabKey) -> EngineResult<&SubscriptionNode> {
        self.subscriptions.get(key)
    }

    #[inline]
    pub fn remove_subscription(&mut self, key: SlabKey) -> EngineResult<SubscriptionNode> {
        self.subscriptions.remove(key)
    }

    // ── Binding ──────────────────────────────────────────────────────────

    #[inline]
    pub fn insert_binding(&mut self, node: BindingNode) -> SlabKey {
        self.bindings.insert(node)
    }

    #[inline]
    pub fn get_binding(&self, key: SlabKey) -> EngineResult<&BindingNode> {
        self.bindings.get(key)
    }

    #[inline]
    pub fn remove_binding(&mut self, key: SlabKey) -> EngineResult<BindingNode> {
        self.bindings.remove(key)
    }

    // ── Pending ──────────────────────────────────────────────────────────

    #[inline]
    pub fn insert_pending(&mut self, node: PendingNode) -> SlabKey {
        self.pending.insert(node)
    }

    #[inline]
    pub fn get_pending(&self, key: SlabKey) -> EngineResult<&PendingNode> {
        self.pending.get(key)
    }

    #[inline]
    pub fn remove_pending(&mut self, key: SlabKey) -> EngineResult<PendingNode> {
        self.pending.remove(key)
    }

    // ── Queue ────────────────────────────────────────────────────────────

    #[inline]
    pub fn insert_queue(&mut self, node: QueueNode) -> SlabKey {
        self.queues.insert(node)
    }

    #[inline]
    pub fn get_queue(&self, key: SlabKey) -> EngineResult<&QueueNode> {
        self.queues.get(key)
    }

    #[inline]
    pub fn remove_queue(&mut self, key: SlabKey) -> EngineResult<QueueNode> {
        self.queues.remove(key)
    }

    // ── Stream ───────────────────────────────────────────────────────────

    #[inline]
    pub fn insert_stream(&mut self, node: StreamNode) -> SlabKey {
        self.streams.insert(node)
    }

    #[inline]
    pub fn get_stream(&self, key: SlabKey) -> EngineResult<&StreamNode> {
        self.streams.get(key)
    }

    #[inline]
    pub fn remove_stream(&mut self, key: SlabKey) -> EngineResult<StreamNode> {
        self.streams.remove(key)
    }
}

impl Default for GraphStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;

    #[test]
    fn graph_store_basic_lifecycle() {
        let mut g = GraphStore::new();

        // Insert connection
        let ck = g.insert_connection(ConnectionNode {
            connection_id: ConnectionId(1),
            node_id: NodeId(1),
            opened_at: Timestamp::new(0),
        });

        let conn = g.get_connection(ck).unwrap();
        assert_eq!(conn.connection_id, ConnectionId(1));

        // Remove
        let removed = g.remove_connection(ck).unwrap();
        assert_eq!(removed.connection_id, ConnectionId(1));

        // Stale key
        assert!(g.get_connection(ck).is_err());
    }

    #[test]
    fn pending_full_lifecycle() {
        let mut g = GraphStore::new();

        let pk = g.insert_pending(PendingNode {
            pending_id: PendingId(0),
            seq: 1,
            queue_id: QueueId(10),
            consumer_id: ConsumerId(20),
            subscription_id: SubscriptionId(30),
            binding_id: BindingId(40),
            connection_id: ConnectionId(500),
            subject_hash: 0xBEEF,
            credits: [CreditEntry {
                scope: CreditScope::Node,
                _pad: [0; 3],
                counter_idx: 0,
            }; 3],
            credit_count: 1,
            deadline_id: 0,
            delivered_at: Timestamp::new(1000),
            ack_wait_ns: 0,
            edge_prev: [SlabKey::DANGLING; crate::graph::node::pending_edge_idx::COUNT],
            edge_next: [SlabKey::DANGLING; crate::graph::node::pending_edge_idx::COUNT],
        });

        let p = g.get_pending(pk).unwrap();
        assert_eq!(p.seq, 1);
        assert_eq!(p.queue_id, QueueId(10));
        assert_eq!(p.connection_id, ConnectionId(500));

        let removed = g.remove_pending(pk).unwrap();
        assert_eq!(removed.subject_hash, 0xBEEF);
    }
}
