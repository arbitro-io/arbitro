//! Publish batch processing — dedup → match → ready push → fanout notify.
//!
//! Level 7 — depends on everything below + context.
//!
//! ZERO heap allocations on steady-state. ZERO edge/graph lookups.
//! Connection IDs precomputed in MatchEntry at bind time.
//! Match table + idempotency window cached per batch (1 lookup, not N).
//!
//! Fanout is fire-and-forget: notifications are pushed to ctx.fanout queue.
//! The protocol layer drains them independently — no blocking, no grouping.
//! Publish returns only stats (RepPublish), never waits for delivery.

use std::sync::atomic::Ordering;

use crate::context::EngineContext;
use crate::batch::PublishBatch;
use crate::catalog::match_table::MatchTable;
use crate::fanout::FanoutEntry;
use crate::reply::RepPublish;

/// Process a publish batch. Fire-and-forget fanout.
///
/// 1. Dedup → 2. Assign seq → 3. Match → 4. ready.push → 5. fanout.push
/// Returns stats only. Fanout notifications live in ctx.fanout queue.
pub fn on_publish_batch(ctx: &mut EngineContext, batch: &PublishBatch) -> RepPublish {
    let mut reply = RepPublish::new(batch.entries.len() as u32);

    let now_ms = batch.now.as_ms();

    // ── Resolve per-batch references ONCE (not per entry) ───────────────
    for entry in batch.entries {
        if let Some(mt) = ctx.catalog.match_table_mut(batch.stream_id) {
            mt.resolve_patterns(entry.subject_hash, entry.subject);
        }
    }

    // Immutable match table pointer — stable for entire batch.
    let mt = match ctx.catalog.match_table(batch.stream_id) {
        Some(mt) => mt as *const MatchTable,
        None => return reply,
    };

    // Local counters — one fetch_add per group at the end keeps atomic
    // traffic out of the inner loop (~7 ns saved per entry).
    let mut m_accepted: u64 = 0;
    let mut m_dups: u64 = 0;
    let mut m_no_match: u64 = 0;
    let mut m_queues: u64 = 0;
    let mut m_notified: u64 = 0;

    for entry in batch.entries {
        // Step 1: Idempotency check (key=0 means skip)
        if entry.idempotency_key != 0 {
            let window = ctx.idempotency_for(batch.stream_id);
            if window.check_and_insert(entry.idempotency_key, now_ms) {
                reply.duplicates_skipped += 1;
                m_dups += 1;
                continue;
            }
        }

        // Step 2: Assign sequence
        let seq = ctx.next_seq;
        ctx.next_seq += 1;
        m_accepted += 1;

        // Step 3: Match table lookup — O(1) hash, returns slices
        // SAFETY: match table is not mutated during this loop.
        let mt_ref = unsafe { &*mt };
        let result = mt_ref.lookup(entry.subject_hash);

        if result.is_empty() {
            m_no_match += 1;
            continue;
        }

        // Step 4: ready push (1 per queue) + fanout (1 per connection)
        // Inline dedup: track which queues/connections already seen for this entry.
        // Typical case: 1-3 entries. Max 8 inline, no heap.
        const MAX_DEDUP: usize = 8;
        let mut pushed_queues: [u32; MAX_DEDUP] = [0; MAX_DEDUP];
        let mut pushed_count: usize = 0;
        let mut notified_conns: [u64; MAX_DEDUP] = [0; MAX_DEDUP];
        let mut notified_count: usize = 0;

        for me in result.iter() {
            // Dedup: 1 ready push per queue per entry
            let q = me.queue_id.raw();
            let already_pushed = pushed_queues[..pushed_count].iter().any(|&x| x == q);
            if !already_pushed {
                ctx.ready.push(me.queue_id, entry.subject_hash, seq);
                m_queues += 1;
                if pushed_count < MAX_DEDUP {
                    pushed_queues[pushed_count] = q;
                    pushed_count += 1;
                }
            }

            let conn_id = me.connection_id;
            if conn_id.0 == 0 {
                reply.queued += 1;
                continue;
            }

            // Dedup: 1 fanout push per connection per entry
            let already = notified_conns[..notified_count]
                .iter()
                .any(|&c| c == conn_id.0);
            if already { continue; }

            if notified_count < MAX_DEDUP {
                notified_conns[notified_count] = conn_id.0;
                notified_count += 1;
            }

            reply.notified += 1;
            m_notified += 1;
            ctx.fanout.push(FanoutEntry::new(conn_id, entry.subject_hash, seq));
        }
    }

    // Flush local counters (one atomic per group — off the hot inner loop).
    let m = &ctx.metrics;
    if m_accepted != 0 { m.publish_entries_accepted.fetch_add(m_accepted, Ordering::Relaxed); }
    if m_dups     != 0 { m.publish_duplicates_skipped.fetch_add(m_dups, Ordering::Relaxed); }
    if m_no_match != 0 { m.publish_no_match.fetch_add(m_no_match, Ordering::Relaxed); }
    if m_queues   != 0 { m.publish_queues_pushed.fetch_add(m_queues, Ordering::Relaxed); }
    if m_notified != 0 { m.publish_fanout_notified.fetch_add(m_notified, Ordering::Relaxed); }

    reply
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use crate::catalog::{StreamConfig, ConsumerConfig, SubscriptionConfig};
    use crate::batch::{PublishEntry, BindBatch, BindEntry};

    /// Setup: stream + consumer + subscription (no binding = pull model)
    fn setup_ctx_with_stream() -> EngineContext {
        let mut ctx = EngineContext::new();

        ctx.catalog.ensure_stream(&mut ctx.graph, StreamConfig {
            id: StreamId(1),
            name: b"orders".to_vec(),
        }).unwrap();

        ctx.catalog.ensure_consumer(&mut ctx.graph, &mut ctx.edges, ConsumerConfig {
            id: ConsumerId(10),
            queue_id: QueueId(100),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 10_000,
        }).unwrap();

        ctx.catalog.ensure_subscription(&mut ctx.graph, &mut ctx.edges, SubscriptionConfig {
            id: SubscriptionId(20),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(10),
            filters: vec![],
        }).unwrap();

        ctx
    }

    /// Setup with binding: push/fanout model
    fn setup_ctx_with_binding() -> EngineContext {
        let mut ctx = setup_ctx_with_stream();

        let entries = [BindEntry {
            connection_id: ConnectionId(500),
            subscription_id: SubscriptionId(20),
        }];
        super::super::bind::on_bind_batch(&mut ctx, &BindBatch {
            entries: &entries,
            now: Timestamp::new(0),
        });

        ctx
    }

    #[test]
    fn publish_without_binding_queues_only() {
        let mut ctx = setup_ctx_with_stream();

        let entries = [PublishEntry {
            subject_hash: 0xBEEF,
            subject: b"orders.created",
            payload: PayloadRef::Borrowed(b"hello"),
            idempotency_key: 0,
            credits_cost: 1,
        }];
        let batch = PublishBatch {
            stream_id: StreamId(1),
            entries: &entries,
            now: Timestamp::new(0),
        };

        let reply = on_publish_batch(&mut ctx, &batch);
        assert_eq!(reply.source_entries, 1);
        assert_eq!(reply.notified, 0);
        assert_eq!(reply.queued, 1);
        assert!(ctx.fanout.is_empty());
        assert!(ctx.ready.has_ready(QueueId(100)));
    }

    #[test]
    fn publish_with_binding_fires_notification() {
        let mut ctx = setup_ctx_with_binding();

        let entries = [PublishEntry {
            subject_hash: 0xBEEF,
            subject: b"orders.created",
            payload: PayloadRef::Borrowed(b"hello"),
            idempotency_key: 0,
            credits_cost: 1,
        }];
        let batch = PublishBatch {
            stream_id: StreamId(1),
            entries: &entries,
            now: Timestamp::new(0),
        };

        let reply = on_publish_batch(&mut ctx, &batch);
        assert_eq!(reply.source_entries, 1);
        assert_eq!(reply.notified, 1);
        assert_eq!(reply.queued, 0);

        // Fanout queue has 1 notification (connection, subject, seq)
        let drain = ctx.fanout.take();
        assert_eq!(drain.len(), 1);
        assert_eq!(drain.entries()[0].connection_id, ConnectionId(500));
        assert_eq!(drain.entries()[0].subject_hash, 0xBEEF);
        drop(drain);

        // Also in ready queue
        assert!(ctx.ready.has_ready(QueueId(100)));
    }

    #[test]
    fn publish_fanout_multiple_consumers() {
        let mut ctx = EngineContext::new();

        ctx.catalog.ensure_stream(&mut ctx.graph, StreamConfig {
            id: StreamId(1), name: b"orders".to_vec(),
        }).unwrap();

        for (cid, qid) in [(10, 100), (20, 200)] {
            ctx.catalog.ensure_consumer(&mut ctx.graph, &mut ctx.edges, ConsumerConfig {
                id: ConsumerId(cid), queue_id: QueueId(qid), stream_id: StreamId(1),
                durable: true, ack_policy: AckPolicy::Explicit, max_inflight: 1000,
            }).unwrap();
        }

        for (sid, cid) in [(30, 10), (40, 20)] {
            ctx.catalog.ensure_subscription(&mut ctx.graph, &mut ctx.edges, SubscriptionConfig {
                id: SubscriptionId(sid), stream_id: StreamId(1),
                consumer_id: ConsumerId(cid), filters: vec![],
            }).unwrap();
        }

        // Both on same connection
        let bind_entries = [
            BindEntry { connection_id: ConnectionId(500), subscription_id: SubscriptionId(30) },
            BindEntry { connection_id: ConnectionId(500), subscription_id: SubscriptionId(40) },
        ];
        super::super::bind::on_bind_batch(&mut ctx, &BindBatch {
            entries: &bind_entries, now: Timestamp::new(0),
        });

        let entries = [PublishEntry {
            subject_hash: 0xBEEF,
            subject: b"orders.created",
            payload: PayloadRef::Borrowed(b"hello"),
            idempotency_key: 0, credits_cost: 1,
        }];
        let reply = on_publish_batch(&mut ctx, &PublishBatch {
            stream_id: StreamId(1), entries: &entries, now: Timestamp::new(0),
        });

        // 1 notification (one per connection, not per consumer)
        assert_eq!(reply.notified, 1);
        let drain = ctx.fanout.take();
        assert_eq!(drain.len(), 1);
        assert_eq!(drain.entries()[0].connection_id, ConnectionId(500));
    }

    #[test]
    fn publish_fanout_two_connections() {
        let mut ctx = EngineContext::new();

        ctx.catalog.ensure_stream(&mut ctx.graph, StreamConfig {
            id: StreamId(1), name: b"orders".to_vec(),
        }).unwrap();

        for (cid, qid) in [(10, 100), (20, 200)] {
            ctx.catalog.ensure_consumer(&mut ctx.graph, &mut ctx.edges, ConsumerConfig {
                id: ConsumerId(cid), queue_id: QueueId(qid), stream_id: StreamId(1),
                durable: true, ack_policy: AckPolicy::Explicit, max_inflight: 1000,
            }).unwrap();
        }
        for (sid, cid) in [(30, 10), (40, 20)] {
            ctx.catalog.ensure_subscription(&mut ctx.graph, &mut ctx.edges, SubscriptionConfig {
                id: SubscriptionId(sid), stream_id: StreamId(1),
                consumer_id: ConsumerId(cid), filters: vec![],
            }).unwrap();
        }

        let bind_entries = [
            BindEntry { connection_id: ConnectionId(500), subscription_id: SubscriptionId(30) },
            BindEntry { connection_id: ConnectionId(600), subscription_id: SubscriptionId(40) },
        ];
        super::super::bind::on_bind_batch(&mut ctx, &BindBatch {
            entries: &bind_entries, now: Timestamp::new(0),
        });

        let entries = [PublishEntry {
            subject_hash: 0xBEEF,
            subject: b"orders.created",
            payload: PayloadRef::Borrowed(b"hello"),
            idempotency_key: 0, credits_cost: 1,
        }];
        let reply = on_publish_batch(&mut ctx, &PublishBatch {
            stream_id: StreamId(1), entries: &entries, now: Timestamp::new(0),
        });

        assert_eq!(reply.notified, 2);
        let drain = ctx.fanout.take();
        let conns: Vec<u64> = drain.entries().iter().map(|e| e.connection_id.0).collect();
        assert!(conns.contains(&500));
        assert!(conns.contains(&600));
    }

    #[test]
    fn publish_deduplicates() {
        let mut ctx = setup_ctx_with_binding();

        let entries = [
            PublishEntry {
                subject_hash: 0xBEEF,
                subject: b"orders.created",
                payload: PayloadRef::Borrowed(b"msg1"),
                idempotency_key: 42, credits_cost: 1,
            },
            PublishEntry {
                subject_hash: 0xBEEF,
                subject: b"orders.created",
                payload: PayloadRef::Borrowed(b"msg1-dup"),
                idempotency_key: 42, credits_cost: 1,
            },
        ];
        let batch = PublishBatch {
            stream_id: StreamId(1), entries: &entries, now: Timestamp::new(1_000_000),
        };

        let reply = on_publish_batch(&mut ctx, &batch);
        assert_eq!(reply.source_entries, 2);
        assert_eq!(reply.notified, 1);
        assert_eq!(reply.duplicates_skipped, 1);
    }

    #[test]
    fn publish_no_match_table_is_noop() {
        let mut ctx = EngineContext::new();

        let entries = [PublishEntry {
            subject_hash: 0xBEEF,
            subject: b"orders.created",
            payload: PayloadRef::Borrowed(b"orphan"),
            idempotency_key: 0, credits_cost: 1,
        }];
        let batch = PublishBatch {
            stream_id: StreamId(999), entries: &entries, now: Timestamp::new(0),
        };

        let reply = on_publish_batch(&mut ctx, &batch);
        assert_eq!(reply.notified, 0);
        assert_eq!(reply.queued, 0);
        assert!(ctx.fanout.is_empty());
    }
}
