//! Entity node structs — all parent IDs stored inline.
//!
//! Level 2 — depends only on `types`, `error`.
//!
//! Every node stores its parent IDs inline so the release protocol
//! never walks the ownership graph. One slab lookup gives all data.

use crate::types::*;

/// Edge index constants for `PendingNode::edge_prev` / `edge_next`.
/// Must match the field order in `BuiltinEdges` for the pending edges.
///
/// The intrusive doubly-linked lists threaded through `PendingNode` turn
/// the 4 pending edge removes in `release_pending` from O(1) HashMap ops
/// (pathologically cache-bound at high inflight) into pure pointer patches.
///
/// Historical note: `SUBJECT` and `BINDING` edges were removed — nothing
/// productive read them (only their own tests did). That dropped 2×
/// `insert_head` from claim and 2× `unlink` from ack, and trimmed 32 B
/// from `PendingNode` (4 fewer `SlabKey` slots in edge_prev/edge_next).
pub mod pending_edge_idx {
    pub const CONNECTION: usize = 0;
    pub const CONSUMER: usize = 1;
    pub const QUEUE: usize = 2;
    pub const SUBSCRIPTION: usize = 3;
    pub const COUNT: usize = 4;
}

// ── ConnectionNode ───────────────────────────────────────────────────────────

/// A connected client. Parent: NodeId.
#[derive(Debug, Clone)]
pub struct ConnectionNode {
    pub connection_id: ConnectionId,
    pub node_id: NodeId,
    pub opened_at: Timestamp,
}

// ── ConsumerNode ─────────────────────────────────────────────────────────────

/// A logical consumer bound to a queue. Created via catalog.
#[derive(Debug, Clone)]
pub struct ConsumerNode {
    pub consumer_id: ConsumerId,
    pub queue_id: QueueId,
    pub stream_id: StreamId,
    pub durable: bool,
    pub ack_policy: AckPolicy,
    pub max_inflight: u32,
    pub paused: bool,
}

// ── SubscriptionNode ─────────────────────────────────────────────────────────

/// A subscription binds a stream (with optional subject filters) to a consumer.
#[derive(Debug, Clone)]
pub struct SubscriptionNode {
    pub subscription_id: SubscriptionId,
    pub stream_id: StreamId,
    pub consumer_id: ConsumerId,
    /// Subject filter patterns (e.g. `b"message.meta.>"`, `b"message.qr.>"`).
    /// Empty = accept all subjects on this stream.
    pub filters: Vec<Vec<u8>>,
}

// ── BindingNode ──────────────────────────────────────────────────────────────

/// A binding connects a subscription to a specific connection (client session).
/// Created at runtime when a client binds.
#[derive(Debug, Clone)]
pub struct BindingNode {
    pub binding_id: BindingId,
    pub connection_id: ConnectionId,
    pub subscription_id: SubscriptionId,
    pub consumer_id: ConsumerId,
    pub created_at: Timestamp,
}

// ── QueueNode ────────────────────────────────────────────────────────────────

/// A queue that holds ready work for consumers.
#[derive(Debug, Clone)]
pub struct QueueNode {
    pub queue_id: QueueId,
    pub stream_id: StreamId,
    pub paused: bool,
}

// ── PendingNode ──────────────────────────────────────────────────────────────

/// An in-flight message awaiting acknowledgment.
///
/// ALL parent IDs and credit reservations are stored inline. The release
/// protocol reads this struct alone — zero pointer chasing.
///
/// Layout: ~144 bytes. All fields needed for `release_pending` are here,
/// plus 32 bytes of intrusive doubly-linked-list pointers (4 × 8 × 2) that
/// thread every pending node into the 4 per-parent pending edges. This is
/// larger than the original 128 B budget, but it's what lets
/// `release_pending` unlink from each of those 4 edges in O(1) with zero
/// HashMap work — see `release_breakdown` bench for the pathological
/// cache-bound scaling of the prior `HashMap<K, Vec<SlabKey>>` edges.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct PendingNode {
    // Identity
    pub pending_id: PendingId,
    pub seq: u64,

    // Parent IDs (inline — no pointer chasing)
    pub queue_id: QueueId,
    pub consumer_id: ConsumerId,
    pub subscription_id: SubscriptionId,
    pub binding_id: BindingId,
    pub connection_id: ConnectionId,
    pub subject_hash: u32,

    // Credits to release (inline array, no heap)
    pub credits: [CreditEntry; MAX_CREDITS_PER_PENDING],
    pub credit_count: u8,

    // Deadline
    pub deadline_id: u32,

    // Timestamps
    pub delivered_at: Timestamp,
    pub ack_wait_ns: u64,

    /// Intrusive linked-list pointers for each pending edge. Index matches
    /// `pending_edge_idx::*`. `SlabKey::DANGLING` means "no neighbor".
    pub edge_prev: [SlabKey; pending_edge_idx::COUNT],
    pub edge_next: [SlabKey; pending_edge_idx::COUNT],
}

// ── StreamNode ───────────────────────────────────────────────────────────────

/// A named stream that holds messages.
#[derive(Debug, Clone)]
pub struct StreamNode {
    pub stream_id: StreamId,
    pub name: Vec<u8>,
}

// ── Size assertions ──────────────────────────────────────────────────────────

// Size budget raised from 128 to 192 bytes. The original budget assumed
// HashMap-backed edges; Fix #2 inlines 48 bytes of intrusive linked-list
// pointers (6 edges × 2 SlabKeys × 8 B) to make `release_pending` O(1)
// regardless of backlog size. See the `release_breakdown` benchmark.
const _: () = assert!(std::mem::size_of::<PendingNode>() <= 192);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_node_size() {
        let size = std::mem::size_of::<PendingNode>();
        // Must fit in 3 cache lines (192 bytes) after adding intrusive
        // edge list pointers — see module-level comment.
        assert!(size <= 192, "PendingNode is {size} bytes, max 192");
    }

    #[test]
    fn pending_node_has_all_inline_ids() {
        let p = PendingNode {
            pending_id: PendingId(1),
            seq: 100,
            queue_id: QueueId(10),
            consumer_id: ConsumerId(20),
            subscription_id: SubscriptionId(30),
            binding_id: BindingId(40),
            connection_id: ConnectionId(500),
            subject_hash: 0xDEAD,
            credits: [
                CreditEntry { scope: CreditScope::Node, _pad: [0; 3], counter_idx: 0 },
                CreditEntry { scope: CreditScope::Connection, _pad: [0; 3], counter_idx: 1 },
                CreditEntry { scope: CreditScope::Subject, _pad: [0; 3], counter_idx: 2 },
            ],
            credit_count: 3,
            deadline_id: 99,
            delivered_at: Timestamp::new(1000),
            ack_wait_ns: 5_000_000_000,
            edge_prev: [SlabKey::DANGLING; pending_edge_idx::COUNT],
            edge_next: [SlabKey::DANGLING; pending_edge_idx::COUNT],
        };

        // All parent IDs accessible without extra lookups
        assert_eq!(p.queue_id, QueueId(10));
        assert_eq!(p.consumer_id, ConsumerId(20));
        assert_eq!(p.subscription_id, SubscriptionId(30));
        assert_eq!(p.binding_id, BindingId(40));
        assert_eq!(p.connection_id, ConnectionId(500));
        assert_eq!(p.subject_hash, 0xDEAD);
        assert_eq!(p.credit_count, 3);
        assert_eq!(p.credits[0].scope, CreditScope::Node);
        assert_eq!(p.credits[1].scope, CreditScope::Connection);
        assert_eq!(p.credits[2].scope, CreditScope::Subject);
    }

    #[test]
    fn credit_entry_alignment() {
        assert_eq!(std::mem::size_of::<CreditEntry>(), 8);
    }

    #[test]
    fn connection_node_basic() {
        let c = ConnectionNode {
            connection_id: ConnectionId(1),
            node_id: NodeId(2),
            opened_at: Timestamp::new(0),
        };
        assert_eq!(c.node_id, NodeId(2));
    }
}
