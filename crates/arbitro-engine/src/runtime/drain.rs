//! Drain operations — connection, subscription, consumer, queue, node.
//!
//! Level 7 — depends on everything below + context.
//!
//! All drains follow the same pattern:
//! 1. `edges.take::<XByY>(&id)` — O(1) to find all children
//! 2. Iterate children — O(k) over only THIS entity's owned items
//! 3. `release_pending()` each — O(1) per pending
//!
//! Current engine: O(S×C) for disconnect. ArbitroDB: O(k).

use std::sync::atomic::Ordering;

use crate::context::EngineContext;
use crate::types::*;
use crate::batch::{DrainConnectionReq, DrainReport};
use crate::runtime::ack::release_pending;
use crate::runtime::bind::unbind;

/// Drain a connection: release all pending, remove all bindings.
///
/// O(k) where k = pending + bindings for THIS connection only.
/// This replaces the old O(S×C) scan across all streams × consumers.
pub fn drain_connection(ctx: &mut EngineContext, req: &DrainConnectionReq) -> DrainReport {
    let mut report = DrainReport::default();

    let pending_keys = ctx.edges.pending_by_connection.take(&mut ctx.graph.pending, req.connection_id);
    let removed = pending_keys.len() as u64;
    for key in pending_keys {
        release_with_mode(ctx, key, req.mode, &mut report);
    }

    let binding_keys = ctx.edges.bindings_by_connection.take(&req.connection_id);
    for key in binding_keys {
        unbind(ctx, key);
        report.bindings_removed += 1;
    }

    ctx.metrics.drain_connections.fetch_add(1, Ordering::Relaxed);
    if removed != 0 {
        ctx.metrics.drain_pending_removed.fetch_add(removed, Ordering::Relaxed);
    }

    report
}

/// Release pending entries with mode-aware requeue/drop logic.
/// Shared by all drain operations.
#[inline]
fn release_with_mode(ctx: &mut EngineContext, pending_key: SlabKey, mode: DrainMode, report: &mut DrainReport) {
    if let Some(pending) = release_pending(ctx, pending_key) {
        match mode {
            DrainMode::ReleaseAndRequeue | DrainMode::ReleaseAndRetryNow => {
                ctx.ready.push_nacked(pending.queue_id, pending.subject_hash, pending.seq);
                report.pending_requeued += 1;
            }
            DrainMode::ReleaseAndDrop => {
                report.pending_released += 1;
            }
            DrainMode::ReleaseAndRetryScheduled { .. } => {
                ctx.ready.push_nacked(pending.queue_id, pending.subject_hash, pending.seq);
                report.pending_requeued += 1;
            }
        }
    }
}

/// Drain a subscription: release all pending, remove bindings.
pub fn drain_subscription(ctx: &mut EngineContext, subscription_id: SubscriptionId, mode: DrainMode) -> DrainReport {
    let mut report = DrainReport::default();

    let pending_keys = ctx.edges.pending_by_subscription.take(&mut ctx.graph.pending, subscription_id);
    for key in pending_keys {
        release_with_mode(ctx, key, mode, &mut report);
    }

    let binding_keys = ctx.edges.bindings_by_subscription.take(&subscription_id);
    for key in binding_keys {
        unbind(ctx, key);
        report.bindings_removed += 1;
    }

    report
}

/// Drain a consumer: release all pending, remove bindings.
pub fn drain_consumer(ctx: &mut EngineContext, consumer_id: ConsumerId, mode: DrainMode) -> DrainReport {
    let mut report = DrainReport::default();

    let pending_keys = ctx.edges.pending_by_consumer.take(&mut ctx.graph.pending, consumer_id);
    let removed = pending_keys.len() as u64;
    for key in pending_keys {
        release_with_mode(ctx, key, mode, &mut report);
    }

    let binding_keys = ctx.edges.bindings_by_consumer.take(&consumer_id);
    for key in binding_keys {
        unbind(ctx, key);
        report.bindings_removed += 1;
    }

    ctx.metrics.drain_consumers.fetch_add(1, Ordering::Relaxed);
    if removed != 0 {
        ctx.metrics.drain_pending_removed.fetch_add(removed, Ordering::Relaxed);
    }

    report
}

/// Drain a queue: release all pending, clear ready state.
pub fn drain_queue(ctx: &mut EngineContext, queue_id: QueueId, mode: DrainMode) -> DrainReport {
    let mut report = DrainReport::default();

    let pending_keys = ctx.edges.pending_by_queue.take(&mut ctx.graph.pending, queue_id);
    for key in pending_keys {
        release_with_mode(ctx, key, mode, &mut report);
    }

    ctx.ready.clear_queue(queue_id);
    report
}

/// Drain a node: drain all connections on this node.
pub fn drain_node(ctx: &mut EngineContext, node_id: NodeId, mode: DrainMode, now: Timestamp) -> DrainReport {
    let mut report = DrainReport::default();

    let conn_keys = ctx.edges.connections_by_node.take(&node_id);

    for conn_key in conn_keys {
        if let Ok(conn_node) = ctx.graph.get_connection(conn_key) {
            let conn_id = conn_node.connection_id;
            let req = DrainConnectionReq {
                connection_id: conn_id,
                mode,
                now,
            };
            let sub = drain_connection(ctx, &req);
            report.pending_released += sub.pending_released;
            report.pending_requeued += sub.pending_requeued;
            report.bindings_removed += sub.bindings_removed;
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::node::{pending_edge_idx, PendingNode, ConnectionNode, BindingNode};
    use crate::inflight::InFlightScope;

    fn setup_ctx() -> EngineContext {
        EngineContext::new()
    }

    fn _insert_connection(ctx: &mut EngineContext, conn_id: ConnectionId, node_id: NodeId) -> SlabKey {
        let key = ctx.graph.insert_connection(ConnectionNode {
            connection_id: conn_id,
            node_id,
            opened_at: Timestamp::new(0),
        });
        ctx.edges.connections_by_node.insert(&node_id, key);
        key
    }

    fn insert_pending_for_conn(
        ctx: &mut EngineContext,
        seq: u64,
        conn_id: ConnectionId,
        consumer_id: ConsumerId,
        queue_id: QueueId,
    ) -> SlabKey {
        let pending = PendingNode {
            pending_id: PendingId(seq as u32),
            seq,
            queue_id,
            consumer_id,
            subscription_id: SubscriptionId(1),
            binding_id: BindingId(1),
            connection_id: conn_id,
            subject_hash: 0xBEEF,
            credits: [CreditEntry { scope: CreditScope::Node, _pad: [0; 3], counter_idx: 0 }; 3],
            credit_count: 0,
            deadline_id: 0,
            delivered_at: Timestamp::new(0),
            ack_wait_ns: 0,
            edge_prev: [SlabKey::DANGLING; pending_edge_idx::COUNT],
            edge_next: [SlabKey::DANGLING; pending_edge_idx::COUNT],
        };

        let key = ctx.graph.insert_pending(pending);

        ctx.edges.pending_by_connection.insert_head(&mut ctx.graph.pending, conn_id, key);
        ctx.edges.pending_by_consumer.insert_head(&mut ctx.graph.pending, consumer_id, key);
        ctx.edges.pending_by_queue.insert_head(&mut ctx.graph.pending, queue_id, key);
        ctx.edges.pending_by_subscription.insert_head(&mut ctx.graph.pending, SubscriptionId(1), key);
        ctx.edges.pending_by_consumer_seq.insert(consumer_id, seq, key);
        ctx.inflight.inc_pending(0xBEEF, consumer_id.raw(), queue_id.raw());

        key
    }

    fn insert_binding_for_conn(
        ctx: &mut EngineContext,
        conn_id: ConnectionId,
        binding_id: BindingId,
    ) -> SlabKey {
        let node = BindingNode {
            binding_id,
            connection_id: conn_id,
            subscription_id: SubscriptionId(1),
            consumer_id: ConsumerId(10),
            created_at: Timestamp::new(0),
        };
        let key = ctx.graph.insert_binding(node);
        ctx.edges.bindings_by_connection.insert(&conn_id, key);
        ctx.edges.bindings_by_subscription.insert(&SubscriptionId(1), key);
        ctx.edges.bindings_by_consumer.insert(&ConsumerId(10), key);
        key
    }

    #[test]
    fn drain_connection_releases_and_requeues() {
        let mut ctx = setup_ctx();
        let conn = ConnectionId(100);
        let consumer = ConsumerId(10);
        let queue = QueueId(20);

        insert_pending_for_conn(&mut ctx, 1, conn, consumer, queue);
        insert_pending_for_conn(&mut ctx, 2, conn, consumer, queue);
        insert_binding_for_conn(&mut ctx, conn, BindingId(1));

        let req = DrainConnectionReq {
            connection_id: conn,
            mode: DrainMode::ReleaseAndRequeue,
            now: Timestamp::new(0),
        };

        let report = drain_connection(&mut ctx, &req);
        assert_eq!(report.pending_requeued, 2);
        assert_eq!(report.bindings_removed, 1);

        // All inflight cleared
        assert_eq!(ctx.inflight.get(InFlightScope::Consumer, 10), 0);
        assert_eq!(ctx.inflight.get(InFlightScope::Queue, 20), 0);

        // Edges cleaned
        assert!(!ctx.edges.pending_by_connection.contains_key(&conn));
        assert!(ctx.edges.bindings_by_connection.get(&conn).is_empty());

        // Messages requeued in ready
        assert!(ctx.ready.has_ready(queue));
    }

    #[test]
    fn drain_connection_drop_mode() {
        let mut ctx = setup_ctx();
        let conn = ConnectionId(100);
        let consumer = ConsumerId(10);
        let queue = QueueId(20);

        insert_pending_for_conn(&mut ctx, 1, conn, consumer, queue);

        let req = DrainConnectionReq {
            connection_id: conn,
            mode: DrainMode::ReleaseAndDrop,
            now: Timestamp::new(0),
        };

        let report = drain_connection(&mut ctx, &req);
        assert_eq!(report.pending_released, 1);
        assert_eq!(report.pending_requeued, 0);

        // Not requeued
        assert!(!ctx.ready.has_ready(queue));
    }

    #[test]
    fn drain_queue_clears_all() {
        let mut ctx = setup_ctx();
        let conn = ConnectionId(100);
        let consumer = ConsumerId(10);
        let queue = QueueId(20);

        insert_pending_for_conn(&mut ctx, 1, conn, consumer, queue);
        ctx.ready.push(queue, 0xDEAD, 99);

        let report = drain_queue(&mut ctx, queue, DrainMode::ReleaseAndDrop);
        assert_eq!(report.pending_released, 1);
        assert!(!ctx.ready.has_ready(queue));
    }

    #[test]
    fn drain_consumer_releases_pending() {
        let mut ctx = setup_ctx();
        let conn = ConnectionId(100);
        let consumer = ConsumerId(10);
        let queue = QueueId(20);

        insert_pending_for_conn(&mut ctx, 1, conn, consumer, queue);
        insert_pending_for_conn(&mut ctx, 2, conn, consumer, queue);

        let report = drain_consumer(&mut ctx, consumer, DrainMode::ReleaseAndRequeue);
        assert_eq!(report.pending_requeued, 2);
        assert_eq!(ctx.inflight.get(InFlightScope::Consumer, 10), 0);
    }
}
