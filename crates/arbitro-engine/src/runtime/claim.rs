//! Claim (deliver) — pop ready messages and build PendingNodes.
//!
//! Level 7 — depends on everything below + context.
//!
//! The claim path:
//! 1. Pre-check: consumer not paused, inflight < max_inflight
//! 2. Pop from ready queue (round-robin across subjects)
//! 3. Check credit availability
//! 4. Build PendingNode with all inline IDs
//! 5. Insert into graph + all edge indexes
//! 6. Increment inflight counters
//!
//! ## Entry point
//!
//! `on_claim_batch` is the **single** entry point. It requires the caller
//! to supply `subscription_id` and `binding_id` — the drainer caches both
//! in `ActiveBinding` at subscribe time, so the hot path never does edge
//! lookups. Cold-path callers (tests, benches, REPL probes) resolve both
//! via `resolve_ids_for_batch` and pass them in.
//!
//! In debug builds, `debug_assert_eq!` checks that the supplied hints
//! match the current edge state — catches stale caches (e.g. caller
//! forgot to invalidate `ActiveBinding` after `remove_subscription` /
//! `unbind`). Zero cost in release.
//!
//! ## Observability
//!
//! No tracing, no logging, no locks. Only `ctx.metrics.*.fetch_add(_,
//! Relaxed)` — per `performance.md` §15 and `code-anti-patterns.md`.

use std::sync::atomic::Ordering;

use crate::context::EngineContext;
use crate::types::*;
use crate::batch::{ClaimBatch, ClaimedEntry};
use crate::reply::ScratchReply;
use crate::graph::node::{pending_edge_idx, PendingNode};
use crate::inflight::InFlightScope;

/// Process a claim batch: pop ready messages and create PendingNodes.
///
/// Callers MUST supply `subscription_id` and `binding_id` — the server
/// drainer caches them in `ActiveBinding`; tests/benches can get them
/// from `resolve_ids_for_batch`. **If the caller mutates subscription or
/// binding topology (unbind, remove_subscription, etc.), it MUST
/// invalidate its cached hints before the next `on_claim_batch` call.**
/// Debug builds will panic via `debug_assert_eq!` on stale hints.
///
/// Enforces:
/// - Consumer pause: returns empty if consumer is paused
/// - max_inflight: stops claiming when inflight reaches limit
/// - Credit availability: skips subject if no credits (re-pushes to ready)
pub fn on_claim_batch<'c>(
    ctx: &'c mut EngineContext,
    batch: &ClaimBatch,
    subscription_id: SubscriptionId,
    binding_id: BindingId,
) -> &'c ScratchReply<ClaimedEntry> {
    ctx.reply_claim.reset();
    ctx.metrics.claim_batches.fetch_add(1, Ordering::Relaxed);

    // Pre-check: is consumer paused? Also fetch stream_id for subject limits.
    let (max_inflight, paused, stream_id) = match ctx.catalog.consumer_key(batch.consumer_id) {
        Ok(key) => match ctx.graph.get_consumer(key) {
            Ok(node) => (node.max_inflight, node.paused, node.stream_id),
            Err(_) => return &ctx.reply_claim,
        },
        Err(_) => (u32::MAX, false, StreamId(0)),
    };

    if paused {
        ctx.metrics.claim_skipped_consumer_paused.fetch_add(1, Ordering::Relaxed);
        return &ctx.reply_claim;
    }

    // Debug-only: detect stale hints before they corrupt edge indexes.
    #[cfg(debug_assertions)]
    {
        let expected_sub = resolve_subscription(ctx, batch.consumer_id);
        let expected_bind = resolve_binding(ctx, batch.connection_id, batch.consumer_id);
        debug_assert_eq!(
            subscription_id, expected_sub,
            "stale subscription hint — caller did not invalidate cached \
             subscription_id after a subscription topology change",
        );
        debug_assert_eq!(
            binding_id, expected_bind,
            "stale binding hint — caller did not invalidate cached \
             binding_id after unbind / rebind",
        );
    }

    // Track skipped subjects to detect when all are blocked (anti-infinite-loop).
    let mut skipped_subjects = 0u32;
    let total_subjects = ctx.ready.total_ready(batch.queue_id) as u32;

    // Happy-path delivery counter: one local u64, one fetch_add on exit.
    // Avoids N atomic ops inside the loop (see plan: saves ~7 ns/entry).
    let mut delivered: u64 = 0;
    // Batched skip counters — same batching trick for skip paths that can
    // fire multiple times in a single loop (the `continue` paths).
    let mut m_skip_subject_limit: u64 = 0;
    let mut m_skip_credit_subject: u64 = 0;

    // Hoisted fast-path flags: paid once per batch instead of per msg.
    // When false, the corresponding lookup is skipped entirely inside the
    // hot loop (each lookup is a full Vec+Option+HashMap chain, ~10-15 ns).
    let check_subject_limits = ctx.catalog.stream_has_subject_limits(stream_id);
    let has_conn_credits = ctx.credit.has_connection_limits();
    let has_subject_credits = ctx.credit.has_subject_limits();

    for _ in 0..batch.max_items {
        // Check max_inflight: stop if at limit
        let current_inflight = ctx.inflight.get(
            InFlightScope::Consumer,
            batch.consumer_id.raw(),
        );
        if current_inflight >= max_inflight {
            ctx.metrics.claim_skipped_max_inflight.fetch_add(1, Ordering::Relaxed);
            break;
        }

        // Pop from ready queue (round-robin across subjects)
        let popped = ctx.ready.pop(batch.queue_id);
        let (subject_hash, seq) = match popped {
            Some(entry) => entry,
            None => {
                ctx.metrics.claim_empty_pop.fetch_add(1, Ordering::Relaxed);
                break;
            }
        };

        // Check subject inflight limit (anti-HOL: skip, don't block).
        // Fast-path: entire block is skipped when no limits are configured
        // on this stream (common case) — no HashMap lookup at all.
        if check_subject_limits {
            if let Some(limit) = ctx.catalog.max_subject_inflight(stream_id, subject_hash) {
                let subject_inflight = ctx.inflight.get(InFlightScope::Subject, subject_hash);
                if subject_inflight >= limit {
                    // Push seq back — pop() already moved subject to back of ring,
                    // so next iteration naturally advances to the next subject.
                    ctx.ready.push(batch.queue_id, subject_hash, seq);
                    m_skip_subject_limit += 1;
                    skipped_subjects += 1;
                    if skipped_subjects >= total_subjects {
                        break; // all subjects are blocked
                    }
                    continue;
                }
            }
        }
        skipped_subjects = 0; // reset on successful delivery

        // Check credit availability (if CreditPlugin registered)
        let mut credit_entries: [CreditEntry; MAX_CREDITS_PER_PENDING] = [CreditEntry {
            scope: CreditScope::Node,
            _pad: [0; 3],
            counter_idx: 0,
        }; MAX_CREDITS_PER_PENDING];
        let mut credit_count: u8 = 0;

        // Credit acquisition — `ctx.credit` is a direct field (no registry
        // indirection). Fast-path flags hoisted above the loop skip the
        // HashMap lookups entirely when no limits exist for that scope.
        {
            let credit = &mut ctx.credit;

            // Connection-level credit — skipped entirely when no conn limits.
            if has_conn_credits {
                if !credit.try_acquire(CreditScope::Connection, batch.connection_id.raw() as u32) {
                    // No credit — push back to ready and stop
                    ctx.ready.push(batch.queue_id, subject_hash, seq);
                    ctx.metrics.claim_skipped_credit_conn.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                credit_entries[0] = CreditEntry {
                    scope: CreditScope::Connection,
                    _pad: [0; 3],
                    counter_idx: batch.connection_id.raw() as u32,
                };
                credit_count = 1;
            }

            // Subject-level credit — skipped entirely when no subject limits.
            if has_subject_credits && credit.available(CreditScope::Subject, subject_hash) != u32::MAX {
                if !credit.try_acquire(CreditScope::Subject, subject_hash) {
                    // Subject credit exhausted — release connection credit and push back
                    if has_conn_credits {
                        credit.release(CreditScope::Connection, batch.connection_id.raw() as u32);
                    }
                    ctx.ready.push(batch.queue_id, subject_hash, seq);
                    ctx.ready.skip_current(batch.queue_id);
                    m_skip_credit_subject += 1;
                    skipped_subjects += 1;
                    if skipped_subjects >= total_subjects {
                        break;
                    }
                    continue;
                }
                credit_entries[credit_count as usize] = CreditEntry {
                    scope: CreditScope::Subject,
                    _pad: [0; 3],
                    counter_idx: subject_hash,
                };
                credit_count += 1;
            }
        }

        // Build PendingNode — all parent IDs inline
        let pending = PendingNode {
            pending_id: PendingId(0),
            seq,
            queue_id: batch.queue_id,
            consumer_id: batch.consumer_id,
            subscription_id,
            binding_id,
            connection_id: batch.connection_id,
            subject_hash,
            credits: credit_entries,
            credit_count,
            deadline_id: 0,
            delivered_at: batch.now,
            ack_wait_ns: 0,
            edge_prev: [SlabKey::DANGLING; pending_edge_idx::COUNT],
            edge_next: [SlabKey::DANGLING; pending_edge_idx::COUNT],
        };

        // Consolidated insert: one slab.insert + 4 head swaps + ONE
        // slab.get_mut(key) shared across all 4 intrusive edges, instead
        // of 4 separate insert_head calls each paying the slab borrow.
        let key = ctx.edges.insert_pending_all(&mut ctx.graph.pending, pending);

        // Increment inflight counters
        ctx.inflight.inc_pending(
            subject_hash,
            batch.consumer_id.raw(),
            batch.queue_id.raw(),
        );

        ctx.reply_claim.accept(ClaimedEntry {
            pending_id: PendingId(key.index),
            seq,
            subject_hash,
        });
        delivered += 1;
    }

    // Single batched fetch_add for the happy-path counter.
    if delivered > 0 {
        ctx.metrics.claim_entries_delivered.fetch_add(delivered, Ordering::Relaxed);
    }
    if m_skip_subject_limit > 0 {
        ctx.metrics.claim_skipped_subject_limit.fetch_add(m_skip_subject_limit, Ordering::Relaxed);
    }
    if m_skip_credit_subject > 0 {
        ctx.metrics.claim_skipped_credit_subject.fetch_add(m_skip_credit_subject, Ordering::Relaxed);
    }

    &ctx.reply_claim
}

/// Cold-path helper: resolve `(subscription_id, binding_id)` for a batch
/// via edge-index lookups. Used by tests, benches, and the test-only
/// `handle_claim` probe in the server. Production drainer caches both in
/// `ActiveBinding` and never calls this.
#[doc(hidden)]
pub fn resolve_ids_for_batch(
    ctx: &EngineContext,
    batch: &ClaimBatch,
) -> (SubscriptionId, BindingId) {
    (
        resolve_subscription(ctx, batch.consumer_id),
        resolve_binding(ctx, batch.connection_id, batch.consumer_id),
    )
}

/// Resolve subscription ID for a consumer.
#[inline]
pub(crate) fn resolve_subscription(ctx: &EngineContext, consumer_id: ConsumerId) -> SubscriptionId {
    let subs = ctx.edges.subscriptions_by_consumer.get(&consumer_id);
    if let Some(&first_key) = subs.first() {
        if let Ok(node) = ctx.graph.get_subscription(first_key) {
            return node.subscription_id;
        }
    }
    SubscriptionId(0)
}

/// Resolve binding ID for a connection + consumer.
#[inline]
pub(crate) fn resolve_binding(ctx: &EngineContext, connection_id: ConnectionId, consumer_id: ConsumerId) -> BindingId {
    let bindings = ctx.edges.bindings_by_connection.get(&connection_id);
    for &key in bindings {
        if let Ok(node) = ctx.graph.get_binding(key) {
            if node.consumer_id == consumer_id {
                return node.binding_id;
            }
        }
    }
    BindingId(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_ctx() -> EngineContext {
        EngineContext::new()
    }

    /// Shorthand for test call sites — resolve IDs cold-path and invoke.
    fn claim(ctx: &mut EngineContext, batch: &ClaimBatch) -> usize {
        let (sub, bind) = resolve_ids_for_batch(ctx, batch);
        let reply = on_claim_batch(ctx, batch, sub, bind);
        reply.accepted as usize
    }

    #[test]
    fn claim_from_empty_queue() {
        let mut ctx = setup_ctx();

        let batch = ClaimBatch {
            queue_id: QueueId(1),
            connection_id: ConnectionId(100),
            consumer_id: ConsumerId(10),
            max_items: 10,
            now: Timestamp::new(0),
        };

        let accepted = claim(&mut ctx, &batch);
        assert_eq!(accepted, 0);
    }

    #[test]
    fn claim_creates_pending_with_edges() {
        let mut ctx = setup_ctx();
        let queue = QueueId(1);
        let consumer = ConsumerId(10);
        let conn = ConnectionId(100);

        ctx.ready.push(queue, 0xBEEF, 1);
        ctx.ready.push(queue, 0xDEAD, 2);

        let batch = ClaimBatch {
            queue_id: queue,
            connection_id: conn,
            consumer_id: consumer,
            max_items: 5,
            now: Timestamp::new(1000),
        };

        {
            let (sub, bind) = resolve_ids_for_batch(&ctx, &batch);
            let reply = on_claim_batch(&mut ctx, &batch, sub, bind);
            assert_eq!(reply.accepted, 2);
            assert_eq!(reply.entries()[0].seq, 1);
            assert_eq!(reply.entries()[0].subject_hash, 0xBEEF);
            assert_eq!(reply.entries()[1].seq, 2);
        }

        assert_eq!(ctx.edges.pending_by_connection.len_for(&ctx.graph.pending, &conn), 2);
        assert_eq!(ctx.edges.pending_by_consumer.len_for(&ctx.graph.pending, &consumer), 2);
        assert_eq!(ctx.edges.pending_by_queue.len_for(&ctx.graph.pending, &queue), 2);

        assert_eq!(ctx.inflight.get(InFlightScope::Consumer, 10), 2);
        assert_eq!(ctx.inflight.get(InFlightScope::Queue, 1), 2);
        assert_eq!(ctx.metrics.claim_entries_delivered.load(Ordering::Relaxed), 2);
        assert_eq!(ctx.metrics.claim_batches.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn claim_respects_max_items() {
        let mut ctx = setup_ctx();
        let queue = QueueId(1);

        for seq in 1..=10 {
            ctx.ready.push(queue, 0xBEEF, seq);
        }

        let batch = ClaimBatch {
            queue_id: queue,
            connection_id: ConnectionId(100),
            consumer_id: ConsumerId(10),
            max_items: 3,
            now: Timestamp::new(0),
        };

        let accepted = claim(&mut ctx, &batch);
        assert_eq!(accepted, 3);
        assert!(ctx.ready.has_ready(queue));
    }

    #[test]
    fn claim_then_ack_full_cycle() {
        let mut ctx = setup_ctx();
        let queue = QueueId(1);
        let consumer = ConsumerId(10);
        let conn = ConnectionId(100);

        ctx.ready.push(queue, 0xBEEF, 42);

        let claim_batch = ClaimBatch {
            queue_id: queue,
            connection_id: conn,
            consumer_id: consumer,
            max_items: 1,
            now: Timestamp::new(0),
        };
        {
            let (sub, bind) = resolve_ids_for_batch(&ctx, &claim_batch);
            let claimed = on_claim_batch(&mut ctx, &claim_batch, sub, bind);
            assert_eq!(claimed.accepted, 1);
            assert_eq!(claimed.entries()[0].seq, 42);
        }

        let ack_entries = [crate::batch::AckEntry { seq: 42 }];
        let ack_batch = crate::batch::AckBatch {
            consumer_id: consumer,
            entries: &ack_entries,
            now: Timestamp::new(1000),
        };
        {
            let ack_reply = super::super::ack::on_ack_batch(&mut ctx, &ack_batch);
            assert_eq!(ack_reply.entries()[0], crate::batch::AckResult::Acked);
        }

        assert_eq!(ctx.inflight.get(InFlightScope::Consumer, 10), 0);
        assert!(!ctx.edges.pending_by_consumer.contains_key(&consumer));
    }
}
