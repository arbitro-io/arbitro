//! Ack/Nack batch processing + release_pending protocol.
//!
//! Level 7 — depends on everything below + context.
//!
//! `release_pending` is the core primitive used by ack, nack, and drain.
//! It removes a PendingNode from the graph, decrements all counters,
//! releases credits, cancels the deadline, and cleans up all edge indexes.
//! Target: ~100-120ns per ack.

use std::sync::atomic::Ordering;

use crate::context::EngineContext;
use crate::types::*;
use crate::batch::{AckBatch, AckResult, NackBatch, NackResult};
use crate::reply::ScratchReply;
use crate::graph::node::{pending_edge_idx, PendingNode};

/// Core release protocol. Removes a pending entry and all associated state.
///
/// O(1): slab remove + 3 counter decs + 0-3 credit releases
///       + deadline cancel + 5 edge index removes.
///
/// All information is read from the PendingNode's inline fields —
/// zero pointer chasing, zero extra lookups.
pub fn release_pending(ctx: &mut EngineContext, pending_key: SlabKey) -> Option<PendingNode> {
    // Step 1: Remove from graph slab — gives us all inline IDs
    let pending = match ctx.graph.remove_pending(pending_key) {
        Ok(p) => p,
        Err(_) => return None,
    };

    // Step 2: Dec 3 inflight counters (~15ns)
    ctx.inflight.dec_pending(
        pending.subject_hash,
        pending.consumer_id.raw(),
        pending.queue_id.raw(),
    );

    // Step 3: Release credits — inline array, 0-3 entries (~15ns).
    // `ctx.credit` is a direct field now: zero registry indirection.
    if pending.credit_count > 0 {
        let credit = &mut ctx.credit;
        for i in 0..pending.credit_count as usize {
            let entry = &pending.credits[i];
            credit.release(entry.scope, entry.counter_idx);
        }
    }

    // Step 4: Cancel deadline (~10ns) — direct field access.
    if pending.deadline_id != 0 {
        ctx.scheduler.cancel(pending.deadline_id);
    }

    // Step 5: Unlink from all 4 pending edges using prev/next read from
    // the removed node — O(1) pointer patching via `PendingEdge::unlink`.
    // `consumer_seq` stays HashMap-based (one entry per key).
    use pending_edge_idx as EI;
    let slab = &mut ctx.graph.pending;
    ctx.edges.pending_by_connection.unlink(
        slab,
        pending.connection_id,
        pending_key,
        pending.edge_prev[EI::CONNECTION],
        pending.edge_next[EI::CONNECTION],
    );
    ctx.edges.pending_by_consumer.unlink(
        slab,
        pending.consumer_id,
        pending_key,
        pending.edge_prev[EI::CONSUMER],
        pending.edge_next[EI::CONSUMER],
    );
    ctx.edges.pending_by_queue.unlink(
        slab,
        pending.queue_id,
        pending_key,
        pending.edge_prev[EI::QUEUE],
        pending.edge_next[EI::QUEUE],
    );
    ctx.edges.pending_by_subscription.unlink(
        slab,
        pending.subscription_id,
        pending_key,
        pending.edge_prev[EI::SUBSCRIPTION],
        pending.edge_next[EI::SUBSCRIPTION],
    );
    ctx.edges.pending_by_consumer_seq.remove(pending.consumer_id, pending.seq);

    Some(pending)
}

/// Process an ack batch. Each entry is looked up by (consumer_id, seq).
///
/// Batch-as-standard: single ack = batch of 1. One code path.
/// Uses pre-allocated scratch buffer — zero heap alloc steady-state.
pub fn on_ack_batch<'c>(ctx: &'c mut EngineContext, batch: &AckBatch) -> &'c ScratchReply<AckResult> {
    ctx.reply_ack.reset();

    let mut m_ok: u64 = 0;
    let mut m_miss: u64 = 0;

    for entry in batch.entries {
        let key = ctx.edges.pending_by_consumer_seq
            .get(batch.consumer_id, entry.seq);

        match key {
            Some(pending_key) => {
                release_pending(ctx, pending_key);
                ctx.reply_ack.accept(AckResult::Acked);
                m_ok += 1;
            }
            None => {
                ctx.reply_ack.accept(AckResult::NotFound);
                m_miss += 1;
            }
        }
    }

    if m_ok   != 0 { ctx.metrics.ack_accepted.fetch_add(m_ok, Ordering::Relaxed); }
    if m_miss != 0 { ctx.metrics.ack_not_found.fetch_add(m_miss, Ordering::Relaxed); }

    &ctx.reply_ack
}

/// Process a nack batch. Release pending + requeue for redelivery.
///
/// Nack = release_pending + push_nacked (priority front of ready ring).
/// Uses pre-allocated scratch buffer — zero heap alloc steady-state.
pub fn on_nack_batch<'c>(ctx: &'c mut EngineContext, batch: &NackBatch) -> &'c ScratchReply<NackResult> {
    ctx.reply_nack.reset();

    let mut m_requeued: u64 = 0;

    for entry in batch.entries {
        let key = ctx.edges.pending_by_consumer_seq
            .get(batch.consumer_id, entry.seq);

        match key {
            Some(pending_key) => {
                if let Some(pending) = release_pending(ctx, pending_key) {
                    ctx.ready.push_nacked(
                        pending.queue_id,
                        pending.subject_hash,
                        pending.seq,
                    );
                }
                ctx.reply_nack.accept(NackResult::Requeued);
                m_requeued += 1;
            }
            None => {
                ctx.reply_nack.accept(NackResult::NotFound);
            }
        }
    }

    if m_requeued != 0 {
        ctx.metrics.nack_accepted.fetch_add(m_requeued, Ordering::Relaxed);
    }

    &ctx.reply_nack
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::node::PendingNode;
    use crate::batch::{AckEntry, NackEntry};
    use crate::inflight::InFlightScope;

    fn setup_ctx() -> EngineContext {
        EngineContext::new()
    }

    fn insert_pending(ctx: &mut EngineContext, seq: u64, consumer_id: ConsumerId, queue_id: QueueId) -> SlabKey {
        let pending = PendingNode {
            pending_id: PendingId(seq as u32),
            seq,
            queue_id,
            consumer_id,
            subscription_id: SubscriptionId(1),
            binding_id: BindingId(1),
            connection_id: ConnectionId(100),
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

        // Register in edge indexes — intrusive inserts for the 4 pending edges.
        ctx.edges.pending_by_connection.insert_head(&mut ctx.graph.pending, ConnectionId(100), key);
        ctx.edges.pending_by_consumer.insert_head(&mut ctx.graph.pending, consumer_id, key);
        ctx.edges.pending_by_queue.insert_head(&mut ctx.graph.pending, queue_id, key);
        ctx.edges.pending_by_subscription.insert_head(&mut ctx.graph.pending, SubscriptionId(1), key);
        ctx.edges.pending_by_consumer_seq.insert(consumer_id, seq, key);

        // Inc inflight
        ctx.inflight.inc_pending(0xBEEF, consumer_id.raw(), queue_id.raw());

        key
    }

    #[test]
    fn release_pending_cleans_all_state() {
        let mut ctx = setup_ctx();
        let consumer = ConsumerId(10);
        let queue = QueueId(20);
        let key = insert_pending(&mut ctx, 1, consumer, queue);

        assert_eq!(ctx.inflight.get(InFlightScope::Consumer, 10), 1);

        let released = release_pending(&mut ctx, key);
        assert!(released.is_some());
        assert_eq!(released.unwrap().seq, 1);

        // All state cleaned
        assert_eq!(ctx.inflight.get(InFlightScope::Consumer, 10), 0);
        assert_eq!(ctx.inflight.get(InFlightScope::Queue, 20), 0);
        assert_eq!(ctx.inflight.get(InFlightScope::Subject, 0xBEEF), 0);
        assert!(!ctx.edges.pending_by_consumer.contains_key(&consumer));
        assert!(!ctx.edges.pending_by_queue.contains_key(&queue));
        assert!(ctx.edges.pending_by_consumer_seq.get(consumer, 1).is_none());
    }

    #[test]
    fn release_stale_key_returns_none() {
        let mut ctx = setup_ctx();
        let stale = SlabKey::new(999, 0);
        assert!(release_pending(&mut ctx, stale).is_none());
    }

    #[test]
    fn ack_batch_releases_pending() {
        let mut ctx = setup_ctx();
        let consumer = ConsumerId(10);
        let queue = QueueId(20);
        insert_pending(&mut ctx, 100, consumer, queue);
        insert_pending(&mut ctx, 200, consumer, queue);

        let entries = [AckEntry { seq: 100 }, AckEntry { seq: 200 }, AckEntry { seq: 999 }];
        let batch = AckBatch {
            consumer_id: consumer,
            entries: &entries,
            now: Timestamp::new(0),
        };

        {
            let reply = on_ack_batch(&mut ctx, &batch);
            assert_eq!(reply.accepted, 3);
            assert_eq!(reply.entries()[0], AckResult::Acked);
            assert_eq!(reply.entries()[1], AckResult::Acked);
            assert_eq!(reply.entries()[2], AckResult::NotFound);
        }

        // All inflight released
        assert_eq!(ctx.inflight.get(InFlightScope::Consumer, 10), 0);
    }

    #[test]
    fn nack_batch_requeues() {
        let mut ctx = setup_ctx();
        let consumer = ConsumerId(10);
        let queue = QueueId(20);
        insert_pending(&mut ctx, 100, consumer, queue);

        let entries = [NackEntry { seq: 100, retry_at: None }];
        let batch = NackBatch {
            consumer_id: consumer,
            entries: &entries,
            now: Timestamp::new(0),
        };

        {
            let reply = on_nack_batch(&mut ctx, &batch);
            assert_eq!(reply.entries()[0], NackResult::Requeued);
        }

        // Message is back in ready queue
        assert!(ctx.ready.has_ready(queue));
    }
}
