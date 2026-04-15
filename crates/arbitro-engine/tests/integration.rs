//! Comprehensive integration tests — public API only via ArbitroEngine.
//!
//! Each test exercises a full scenario through the engine facade.

use arbitro_engine::*;
use arbitro_engine::batch::*;
use arbitro_engine::catalog::{StreamConfig, ConsumerConfig, SubscriptionConfig, fnv1a_32};
use arbitro_engine::inflight::InFlightScope;
use arbitro_engine::types::*;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Standard test setup: 1 stream, 1 consumer (catch-all), 1 connection, 1 binding.
fn engine_with_one_consumer(max_inflight: u32) -> ArbitroEngine {
    let mut e = ArbitroEngine::new();

    e.ensure_stream(StreamConfig {
        id: StreamId(1),
        name: b"orders".to_vec(),
    }).unwrap();

    e.ensure_consumer(ConsumerConfig {
        id: ConsumerId(1),
        queue_id: QueueId(1),
        stream_id: StreamId(1),
        durable: true,
        ack_policy: AckPolicy::Explicit,
        max_inflight,
    }).unwrap();

    e.ensure_subscription(SubscriptionConfig {
        id: SubscriptionId(1),
        stream_id: StreamId(1),
        consumer_id: ConsumerId(1),
        filters: vec![],
    }).unwrap();

    e.open_connection(&OpenConnectionReq {
        connection_id: ConnectionId(100),
        node_id: NodeId(1),
        now: Timestamp::new(0),
    });

    e.bind(&BindBatch {
        entries: &[BindEntry {
            connection_id: ConnectionId(100),
            subscription_id: SubscriptionId(1),
        }],
        now: Timestamp::new(0),
    });

    e
}

fn publish_n(e: &mut ArbitroEngine, n: usize) {
    let subjects: Vec<Vec<u8>> = (0..n).map(|i| format!("order.{i}").into_bytes()).collect();
    let entries: Vec<PublishEntry> = subjects.iter().map(|s| PublishEntry {
        subject_hash: fnv1a_32(s),
        subject: s,
        payload: PayloadRef::Borrowed(b"payload"),
        idempotency_key: 0,
        credits_cost: 1,
    }).collect();

    e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_000_000),
    });
}

/// Claim result snapshot — copies data out of scratch buffer for test use.
struct ClaimResult {
    accepted: u32,
    entries: Vec<ClaimedEntry>,
}

fn claim_n(e: &mut ArbitroEngine, n: u16) -> ClaimResult {
    let batch = ClaimBatch {
        queue_id: QueueId(1),
        connection_id: ConnectionId(100),
        consumer_id: ConsumerId(1),
        max_items: n,
        now: Timestamp::new(2_000_000),
    };
    claim_batch(e, &batch)
}

/// Cold-path claim helper for integration tests — resolves IDs via edge
/// lookup (production callers cache both in `ActiveBinding`).
fn claim_batch(e: &mut ArbitroEngine, batch: &ClaimBatch) -> ClaimResult {
    let (sub, bind) = arbitro_engine::runtime::claim::resolve_ids_for_batch(e.ctx(), batch);
    let reply = e.claim(batch, sub, bind);
    ClaimResult {
        accepted: reply.accepted,
        entries: reply.entries().to_vec(),
    }
}

/// Ack result snapshot — copies data out of scratch buffer for test use.
struct AckReply {
    accepted: u32,
    entries: Vec<AckResult>,
}

fn ack_all(e: &mut ArbitroEngine, claimed: &[ClaimedEntry]) -> AckReply {
    let ack_entries: Vec<AckEntry> = claimed.iter().map(|c| AckEntry { seq: c.seq }).collect();
    let reply = e.ack(&AckBatch {
        consumer_id: ConsumerId(1),
        entries: &ack_entries,
        now: Timestamp::new(3_000_000),
    });
    AckReply {
        accepted: reply.accepted,
        entries: reply.entries().to_vec(),
    }
}

// ── 1. Max ack_pending enforcement ──────────────────────────────────────────

#[test]
fn max_inflight_stops_claim_at_limit() {
    let mut e = engine_with_one_consumer(5);

    publish_n(&mut e, 20);

    // Claim — should stop at 5 (max_inflight)
    let claimed = claim_n(&mut e, 20);
    assert_eq!(claimed.accepted, 5);

    // Inflight is exactly at limit
    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 5);

    // Another claim gets 0
    let claimed2 = claim_n(&mut e, 10);
    assert_eq!(claimed2.accepted, 0);
}

#[test]
fn max_inflight_resumes_after_ack() {
    let mut e = engine_with_one_consumer(3);

    publish_n(&mut e, 10);

    // Fill to limit
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 3);

    // Can't claim more
    let stuck = claim_n(&mut e, 10);
    assert_eq!(stuck.accepted, 0);

    // Ack 1 message
    let ack_entries = [AckEntry { seq: claimed.entries[0].seq }];
    e.ack(&AckBatch {
        consumer_id: ConsumerId(1),
        entries: &ack_entries,
        now: Timestamp::new(3_000_000),
    });

    // Now can claim 1 more
    let freed = claim_n(&mut e, 10);
    assert_eq!(freed.accepted, 1);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 3);
}

// ── 2. Credit exhaustion / backpressure ─────────────────────────────────────

#[test]
fn credit_exhaustion_blocks_claims() {
    let mut e = engine_with_one_consumer(100);

    // Set connection credit limit to 3
    e.ctx_mut().credit
        .set_limit(CreditScope::Connection, 100, 3);

    publish_n(&mut e, 10);

    // Claim — should stop at 3 (credit limit)
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 3);

    // Credits exhausted — claim blocked
    let stuck = claim_n(&mut e, 10);
    assert_eq!(stuck.accepted, 0);
}

#[test]
fn credit_resumes_after_ack_releases() {
    let mut e = engine_with_one_consumer(100);

    e.ctx_mut().credit
        .set_limit(CreditScope::Connection, 100, 2);

    publish_n(&mut e, 10);

    // Claim 2 (exhaust credits)
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 2);

    // Ack 1 — releases 1 credit
    let ack_entries = [AckEntry { seq: claimed.entries[0].seq }];
    e.ack(&AckBatch {
        consumer_id: ConsumerId(1),
        entries: &ack_entries,
        now: Timestamp::new(3_000_000),
    });

    // Now can claim 1 more
    let freed = claim_n(&mut e, 10);
    assert_eq!(freed.accepted, 1);

    // Credit exhausted again
    let stuck = claim_n(&mut e, 10);
    assert_eq!(stuck.accepted, 0);
}

// ── 3. Subject-level inflight tracking ──────────────────────────────────────

#[test]
fn subject_inflight_tracked_per_subject() {
    let mut e = engine_with_one_consumer(100);
    // Enable subject-inflight tracking — gated optimization means the
    // subject HashMap is only maintained once a limit is configured.
    e.set_max_subject_inflight(StreamId(1), b">", u32::MAX).unwrap();

    // Publish 3 messages on subject "orders.A" and 2 on "orders.B"
    let subj_a = b"orders.A";
    let subj_b = b"orders.B";
    let hash_a = fnv1a_32(subj_a);
    let hash_b = fnv1a_32(subj_b);

    let entries = [
        PublishEntry { subject_hash: hash_a, subject: subj_a, payload: PayloadRef::Borrowed(b"a1"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_a, subject: subj_a, payload: PayloadRef::Borrowed(b"a2"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_a, subject: subj_a, payload: PayloadRef::Borrowed(b"a3"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_b, subject: subj_b, payload: PayloadRef::Borrowed(b"b1"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_b, subject: subj_b, payload: PayloadRef::Borrowed(b"b2"), idempotency_key: 0, credits_cost: 1 },
    ];
    e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_000_000),
    });

    // Claim all 5
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 5);

    // Check per-subject inflight
    assert_eq!(e.ctx().inflight.get(InFlightScope::Subject, hash_a), 3);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Subject, hash_b), 2);

    // Ack the 3 subject-A messages
    let a_entries: Vec<AckEntry> = claimed.entries.iter()
        .filter(|c| c.subject_hash == hash_a)
        .map(|c| AckEntry { seq: c.seq })
        .collect();
    e.ack(&AckBatch {
        consumer_id: ConsumerId(1),
        entries: &a_entries,
        now: Timestamp::new(3_000_000),
    });

    // Subject A cleared, B untouched
    assert_eq!(e.ctx().inflight.get(InFlightScope::Subject, hash_a), 0);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Subject, hash_b), 2);
}

// ── 4. Consumer pause blocks claims ─────────────────────────────────────────

#[test]
fn pause_consumer_blocks_all_claims() {
    let mut e = engine_with_one_consumer(100);

    publish_n(&mut e, 5);

    // Pause
    assert!(e.pause_consumer(ConsumerId(1)));

    // Claims return empty
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 0);

    // Resume
    assert!(e.resume_consumer(ConsumerId(1)));

    // Now claims work
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 5);
}

#[test]
fn pause_does_not_affect_ack() {
    let mut e = engine_with_one_consumer(100);

    publish_n(&mut e, 3);
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 3);

    // Pause consumer
    e.pause_consumer(ConsumerId(1));

    // Ack still works while paused
    let ack_result = ack_all(&mut e, &claimed.entries);
    assert_eq!(ack_result.accepted, 3);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 0);
}

// ── 5. Nack → reclaim → ack full cycle ─────────────────────────────────────

#[test]
fn nack_redelivery_then_ack() {
    let mut e = engine_with_one_consumer(100);

    publish_n(&mut e, 3);

    // Claim 3
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 3);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 3);

    // Nack all 3
    let nack_entries: Vec<NackEntry> = claimed.entries.iter()
        .map(|c| NackEntry { seq: c.seq, retry_at: None })
        .collect();
    {
        let nack_result = e.nack(&NackBatch {
            consumer_id: ConsumerId(1),
            entries: &nack_entries,
            now: Timestamp::new(3_000_000),
        });
        assert_eq!(nack_result.accepted, 3);
        for r in nack_result.entries() {
            assert_eq!(*r, NackResult::Requeued);
        }
    }

    // Inflight back to 0 after nack
    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 0);

    // Reclaim — same 3 messages should be available
    let reclaimed = claim_n(&mut e, 10);
    assert_eq!(reclaimed.accepted, 3);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 3);

    // Ack this time
    let ack_result = ack_all(&mut e, &reclaimed.entries);
    assert_eq!(ack_result.accepted, 3);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 0);
}

#[test]
fn nack_not_found_for_unknown_seq() {
    let mut e = engine_with_one_consumer(100);

    let nack_entries = [NackEntry { seq: 9999, retry_at: None }];
    let result = e.nack(&NackBatch {
        consumer_id: ConsumerId(1),
        entries: &nack_entries,
        now: Timestamp::new(0),
    });
    assert_eq!(result.entries()[0], NackResult::NotFound);
}

// ── 6. Multi-consumer fanout ────────────────────────────────────────────────

#[test]
fn multi_consumer_fanout_same_message_to_all() {
    let mut e = ArbitroEngine::new();

    e.ensure_stream(StreamConfig {
        id: StreamId(1),
        name: b"events".to_vec(),
    }).unwrap();

    // 3 consumers, each with own queue, all catch-all
    for i in 1..=3u32 {
        e.ensure_consumer(ConsumerConfig {
            id: ConsumerId(i),
            queue_id: QueueId(i),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 100,
        }).unwrap();

        e.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(i),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(i),
            filters: vec![],
        }).unwrap();
    }

    // One connection bound to all 3
    e.open_connection(&OpenConnectionReq {
        connection_id: ConnectionId(100),
        node_id: NodeId(1),
        now: Timestamp::new(0),
    });
    let bind_entries: Vec<BindEntry> = (1..=3).map(|i| BindEntry {
        connection_id: ConnectionId(100),
        subscription_id: SubscriptionId(i),
    }).collect();
    e.bind(&BindBatch {
        entries: &bind_entries,
        now: Timestamp::new(0),
    });

    // Publish 1 message
    let subj = b"events.click";
    let entries = [PublishEntry {

        subject_hash: fnv1a_32(subj),
        subject: subj,
        payload: PayloadRef::Borrowed(b"click-data"),
        idempotency_key: 0,
        credits_cost: 1,
    }];
    let fanout = e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_000_000),
    });

    // Fire-and-forget: 1 notification (one per connection, not per consumer)
    assert_eq!(fanout.source_entries, 1);
    assert_eq!(fanout.notified, 1);

    // Protocol layer drains fanout queue
    let drain = e.drain_fanout();
    assert_eq!(drain.len(), 1);
    assert_eq!(drain.entries()[0].connection_id, ConnectionId(100));
    drop(drain);

    // Each consumer can independently claim
    for i in 1..=3u32 {
        let claimed = claim_batch(&mut e, &ClaimBatch {
        queue_id: QueueId(i),
        connection_id: ConnectionId(100),
        consumer_id: ConsumerId(i),
        max_items: 10,
        now: Timestamp::new(2_000_000),
    });
        assert_eq!(claimed.accepted, 1, "consumer {i} should claim 1");
    }
}

// ── 7. Drain connection with multiple connections ───────────────────────────

#[test]
fn drain_connection_only_affects_its_own_pending() {
    let mut e = engine_with_one_consumer(100);

    // Open a second connection
    e.open_connection(&OpenConnectionReq {
        connection_id: ConnectionId(200),
        node_id: NodeId(1),
        now: Timestamp::new(0),
    });
    e.bind(&BindBatch {
        entries: &[BindEntry {
            connection_id: ConnectionId(200),
            subscription_id: SubscriptionId(1),
        }],
        now: Timestamp::new(0),
    });

    publish_n(&mut e, 6);

    // Claim 3 on connection 100
    let claimed_100 = claim_batch(&mut e, &ClaimBatch {
        queue_id: QueueId(1),
        connection_id: ConnectionId(100),
        consumer_id: ConsumerId(1),
        max_items: 3,
        now: Timestamp::new(2_000_000),
    });
    assert_eq!(claimed_100.accepted, 3);

    // Claim 3 on connection 200
    let claimed_200 = claim_batch(&mut e, &ClaimBatch {
        queue_id: QueueId(1),
        connection_id: ConnectionId(200),
        consumer_id: ConsumerId(1),
        max_items: 3,
        now: Timestamp::new(2_000_000),
    });
    assert_eq!(claimed_200.accepted, 3);

    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 6);

    // Drain connection 100 — only its 3 pending released + requeued
    let report = e.drain_connection(&DrainConnectionReq {
        connection_id: ConnectionId(100),
        mode: DrainMode::ReleaseAndRequeue,
        now: Timestamp::new(4_000_000),
    });
    assert_eq!(report.pending_requeued, 3);

    // Connection 200's pending still inflight
    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 3);

    // The requeued messages are back in ready — we can claim them again
    let reclaimed = claim_batch(&mut e, &ClaimBatch {
        queue_id: QueueId(1),
        connection_id: ConnectionId(200),
        consumer_id: ConsumerId(1),
        max_items: 10,
        now: Timestamp::new(5_000_000),
    });
    assert_eq!(reclaimed.accepted, 3);
}

// ── 8. Pattern subscription routing ─────────────────────────────────────────

#[test]
fn pattern_subscription_routes_matching_subjects() {
    let mut e = ArbitroEngine::new();

    e.ensure_stream(StreamConfig {
        id: StreamId(1),
        name: b"messages".to_vec(),
    }).unwrap();

    // Consumer A: pattern "message.meta.>"
    e.ensure_consumer(ConsumerConfig {
        id: ConsumerId(1),
        queue_id: QueueId(1),
        stream_id: StreamId(1),
        durable: true,
        ack_policy: AckPolicy::Explicit,
        max_inflight: 100,
    }).unwrap();
    e.ensure_subscription(SubscriptionConfig {
        id: SubscriptionId(1),
        stream_id: StreamId(1),
        consumer_id: ConsumerId(1),
        filters: vec![b"message.meta.>".to_vec()],
    }).unwrap();

    // Consumer B: pattern "message.qr.>"
    e.ensure_consumer(ConsumerConfig {
        id: ConsumerId(2),
        queue_id: QueueId(2),
        stream_id: StreamId(1),
        durable: true,
        ack_policy: AckPolicy::Explicit,
        max_inflight: 100,
    }).unwrap();
    e.ensure_subscription(SubscriptionConfig {
        id: SubscriptionId(2),
        stream_id: StreamId(1),
        consumer_id: ConsumerId(2),
        filters: vec![b"message.qr.>".to_vec()],
    }).unwrap();

    // Publish "message.meta.123" — should route to consumer A only
    let subj_meta = b"message.meta.123";
    let subj_qr = b"message.qr.456";
    let entries = [
        PublishEntry {

            subject_hash: fnv1a_32(subj_meta),
            subject: subj_meta,
            payload: PayloadRef::Borrowed(b"meta-data"),
            idempotency_key: 0,
            credits_cost: 1,
        },
        PublishEntry {

            subject_hash: fnv1a_32(subj_qr),
            subject: subj_qr,
            payload: PayloadRef::Borrowed(b"qr-data"),
            idempotency_key: 0,
            credits_cost: 1,
        },
    ];

    // No bindings → messages go to ready queues (pull model)
    let fanout = e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_000_000),
    });

    // No notifications (no bindings), but queued for pull
    assert_eq!(fanout.notified, 0);
    assert_eq!(fanout.queued, 2);

    // Verify messages are in the correct ready queues by claiming
    let claimed_q1 = claim_batch(&mut e, &ClaimBatch {
        queue_id: QueueId(1),
        connection_id: ConnectionId(100),
        consumer_id: ConsumerId(1),
        max_items: 10,
        now: Timestamp::new(2_000_000),
    });
    assert_eq!(claimed_q1.accepted, 1); // meta → consumer 1

    let claimed_q2 = claim_batch(&mut e, &ClaimBatch {
        queue_id: QueueId(2),
        connection_id: ConnectionId(100),
        consumer_id: ConsumerId(2),
        max_items: 10,
        now: Timestamp::new(2_000_000),
    });
    assert_eq!(claimed_q2.accepted, 1); // qr → consumer 2
}

// ── 9. Idempotency enforcement ──────────────────────────────────────────────

#[test]
fn idempotency_rejects_duplicate_across_batches() {
    let mut e = engine_with_one_consumer(100);

    let subj = b"order.1";
    let hash = fnv1a_32(subj);

    // Publish with idempotency key
    let entries = [PublishEntry {

        subject_hash: hash,
        subject: subj,
        payload: PayloadRef::Borrowed(b"first"),
        idempotency_key: 42,
        credits_cost: 1,
    }];
    let r1 = e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_000_000),
    });
    assert_eq!(r1.notified, 1);
    assert_eq!(r1.duplicates_skipped, 0);
    e.drain_fanout(); // consume notification

    // Same key in second batch — rejected
    let r2 = e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_500_000),
    });
    assert_eq!(r2.notified, 0);
    assert_eq!(r2.duplicates_skipped, 1);
}

#[test]
fn idempotency_key_zero_never_deduplicates() {
    let mut e = engine_with_one_consumer(100);

    let subj = b"order.x";
    let entries = [PublishEntry {

        subject_hash: fnv1a_32(subj),
        subject: subj,
        payload: PayloadRef::Borrowed(b"data"),
        idempotency_key: 0, // no dedup
        credits_cost: 1,
    }];

    // Publish same entry 3 times — all accepted
    for _ in 0..3 {
        let r = e.publish(&PublishBatch {
            stream_id: StreamId(1),
            entries: &entries,
            now: Timestamp::new(1_000_000),
        });
        assert_eq!(r.notified, 1);
        assert_eq!(r.duplicates_skipped, 0);
        e.drain_fanout();
    }

    // All 3 in ready queue
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 3);
}

// ── 10. Deadline expiry via scheduler ───────────────────────────────────────

#[test]
fn scheduler_tick_returns_expired_deadlines() {
    let mut e = ArbitroEngine::new();

    // Schedule a deadline via the scheduler directly
    {
        let sched = &mut e.ctx_mut().scheduler;
        // schedule(deadline_ms, pending_key_index, pending_key_gen)
        sched.schedule(100, 42, 1);
    }

    // Tick to 50 — nothing expired
    let mut expired = Vec::new();
    e.tick(50, &mut expired);
    assert!(expired.is_empty());

    // Tick to 100 — deadline fires
    e.tick(100, &mut expired);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].pending_key_index, 42);
    assert_eq!(expired[0].pending_key_gen, 1);
}

// ── 11. Drain consumer releases all pending ─────────────────────────────────

#[test]
fn drain_consumer_releases_all_inflight() {
    let mut e = engine_with_one_consumer(100);

    publish_n(&mut e, 5);
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 5);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 5);

    let report = e.drain_consumer(ConsumerId(1), DrainMode::ReleaseAndRequeue);
    assert_eq!(report.pending_requeued, 5);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 0);
}

// ── 12. Drain queue clears everything ───────────────────────────────────────

#[test]
fn drain_queue_clears_pending_and_ready() {
    let mut e = engine_with_one_consumer(100);

    publish_n(&mut e, 10);

    // Claim 5 (5 pending, 5 still in ready)
    let claimed = claim_n(&mut e, 5);
    assert_eq!(claimed.accepted, 5);

    let report = e.drain_queue(QueueId(1), DrainMode::ReleaseAndDrop);
    assert_eq!(report.pending_released, 5); // 5 pending released

    // Ready queue also drained
    let claimed_after = claim_n(&mut e, 10);
    assert_eq!(claimed_after.accepted, 0);
}

// ── 13. Full publish→claim→ack cycle verifies zero residual state ───────────

#[test]
fn full_cycle_leaves_zero_residual_state() {
    let mut e = engine_with_one_consumer(1000);

    publish_n(&mut e, 100);

    let claimed = claim_n(&mut e, 100);
    assert_eq!(claimed.accepted, 100);

    let result = ack_all(&mut e, &claimed.entries);
    assert_eq!(result.accepted, 100);

    // All counters at zero
    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 0);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Queue, 1), 0);

    // No more messages in ready
    let empty = claim_n(&mut e, 10);
    assert_eq!(empty.accepted, 0);
}

// ── 14. Double ack returns NotFound ─────────────────────────────────────────

#[test]
fn double_ack_returns_not_found() {
    let mut e = engine_with_one_consumer(100);

    publish_n(&mut e, 1);
    let claimed = claim_n(&mut e, 1);
    assert_eq!(claimed.accepted, 1);

    // First ack
    let r1 = ack_all(&mut e, &claimed.entries);
    assert_eq!(r1.entries[0], AckResult::Acked);

    // Second ack — already released
    let r2 = ack_all(&mut e, &claimed.entries);
    assert_eq!(r2.entries[0], AckResult::NotFound);
}

// ── 15. Drain connection drop mode ──────────────────────────────────────────

#[test]
fn drain_connection_drop_mode_does_not_requeue() {
    let mut e = engine_with_one_consumer(100);

    publish_n(&mut e, 5);
    let claimed = claim_n(&mut e, 5);
    assert_eq!(claimed.accepted, 5);

    let report = e.drain_connection(&DrainConnectionReq {
        connection_id: ConnectionId(100),
        mode: DrainMode::ReleaseAndDrop,
        now: Timestamp::new(4_000_000),
    });
    assert_eq!(report.pending_released, 5);
    assert_eq!(report.pending_requeued, 0);

    // Messages are gone — no requeue
    let empty = claim_n(&mut e, 10);
    assert_eq!(empty.accepted, 0);
}

// ── 16. Interleaved ack and nack in same session ────────────────────────────

#[test]
fn mixed_ack_and_nack() {
    let mut e = engine_with_one_consumer(100);

    publish_n(&mut e, 4);
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 4);

    // Ack first 2
    let ack_entries = [
        AckEntry { seq: claimed.entries[0].seq },
        AckEntry { seq: claimed.entries[1].seq },
    ];
    e.ack(&AckBatch {
        consumer_id: ConsumerId(1),
        entries: &ack_entries,
        now: Timestamp::new(3_000_000),
    });

    // Nack last 2
    let nack_entries = [
        NackEntry { seq: claimed.entries[2].seq, retry_at: None },
        NackEntry { seq: claimed.entries[3].seq, retry_at: None },
    ];
    e.nack(&NackBatch {
        consumer_id: ConsumerId(1),
        entries: &nack_entries,
        now: Timestamp::new(3_000_000),
    });

    // Inflight is 0
    assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, 1), 0);

    // 2 nacked messages available for reclaim
    let reclaimed = claim_n(&mut e, 10);
    assert_eq!(reclaimed.accepted, 2);
}

// ── 17. Max ack_pending interacts with credit limit ─────────────────────────

#[test]
fn max_inflight_and_credit_both_enforced() {
    let mut e = engine_with_one_consumer(5); // max_inflight=5

    // Credit limit=3 (stricter than ack_pending)
    e.ctx_mut().credit
        .set_limit(CreditScope::Connection, 100, 3);

    publish_n(&mut e, 10);

    // Should stop at 3 (credit limit is tighter)
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 3);
}

#[test]
fn ack_pending_tighter_than_credit() {
    let mut e = engine_with_one_consumer(2); // max_inflight=2

    // Credit limit=10 (looser than ack_pending)
    e.ctx_mut().credit
        .set_limit(CreditScope::Connection, 100, 10);

    publish_n(&mut e, 10);

    // Should stop at 2 (ack_pending is tighter)
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 2);
}

// ── 18. Massive disconnect: 1 connection, 100 subscriptions, credits per-subject ──

/// Simulates a realistic crash/disconnect scenario:
/// - 1 stream, 20 consumers (each with own queue + catch-all subscription)
/// - 1 connection bound to all 20 subscriptions
/// - Credit limit per connection
/// - Publish many messages across distinct subjects → claim on each consumer
/// - DROP the connection → verify every counter, edge, and credit is clean
#[test]
fn massive_disconnect_100_subscriptions_full_cleanup() {
    let num_consumers: u32 = 20;
    let msgs_per_consumer: u16 = 5;
    let conn = ConnectionId(100);
    let node = NodeId(1);

    let mut e = ArbitroEngine::new();

    // ── Setup catalog ───────────────────────────────────────────────────
    e.ensure_stream(StreamConfig {
        id: StreamId(1),
        name: b"events".to_vec(),
    }).unwrap();

    // Enable subject-inflight tracking (gated since optimization) — the test
    // inspects `InFlightScope::Subject` counts, which are only maintained
    // when a limit is configured somewhere on the stream. A no-op u32::MAX
    // limit on a catch-all pattern flips the tracking flag on.
    e.set_max_subject_inflight(StreamId(1), b">", u32::MAX).unwrap();

    for i in 1..=num_consumers {
        e.ensure_consumer(ConsumerConfig {
            id: ConsumerId(i),
            queue_id: QueueId(i),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 1000,
        }).unwrap();

        e.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(i),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(i),
            filters: vec![], // catch-all: every consumer sees every message
        }).unwrap();
    }

    // ── Connection + bindings ────────────────────────────────────────────
    e.open_connection(&OpenConnectionReq {
        connection_id: conn,
        node_id: node,
        now: Timestamp::new(0),
    });

    let bind_entries: Vec<BindEntry> = (1..=num_consumers)
        .map(|i| BindEntry {
            connection_id: conn,
            subscription_id: SubscriptionId(i),
        })
        .collect();
    let bind_result = e.bind(&BindBatch {
        entries: &bind_entries,
        now: Timestamp::new(0),
    });
    assert_eq!(bind_result.accepted, num_consumers);

    // ── Set credit limit on the connection ───────────────────────────────
    let credit_limit = (num_consumers * msgs_per_consumer as u32) + 100; // generous
    e.ctx_mut().credit
        .set_limit(CreditScope::Connection, conn.raw() as u32, credit_limit);

    // ── Publish messages with distinct subjects ─────────────────────────
    let total_msgs = msgs_per_consumer as usize;
    let subjects: Vec<Vec<u8>> = (0..total_msgs)
        .map(|i| format!("event.type.{i}").into_bytes())
        .collect();
    let pub_entries: Vec<PublishEntry> = subjects.iter()
        .map(|s| PublishEntry {
            subject_hash: fnv1a_32(s),
            subject: s,
            payload: PayloadRef::Borrowed(b"event-payload-data"),
            idempotency_key: 0,
            credits_cost: 1,
        })
        .collect();

    let fanout = e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &pub_entries,
        now: Timestamp::new(1_000_000),
    });

    // Fire-and-forget: 1 notification per message (all consumers on same conn)
    assert_eq!(fanout.source_entries, total_msgs as u32);
    assert_eq!(fanout.notified, total_msgs as u32);

    // Protocol layer drains fanout
    let drain = e.drain_fanout();
    assert_eq!(drain.len(), total_msgs);
    for entry in drain.entries() {
        assert_eq!(entry.connection_id, conn);
    }
    drop(drain);

    // ── Claim on every consumer ─────────────────────────────────────────
    let mut all_subject_hashes = std::collections::HashSet::new();
    let mut total_claimed: u32 = 0;

    for i in 1..=num_consumers {
        let claimed = claim_batch(&mut e, &ClaimBatch {
        queue_id: QueueId(i),
        connection_id: conn,
        consumer_id: ConsumerId(i),
        max_items: msgs_per_consumer,
        now: Timestamp::new(2_000_000),
    });
        assert_eq!(
            claimed.accepted, msgs_per_consumer as u32,
            "consumer {i} should claim {msgs_per_consumer}"
        );
        total_claimed += claimed.accepted;

        for entry in &claimed.entries {
            all_subject_hashes.insert(entry.subject_hash);
        }
    }

    let expected_total = num_consumers * msgs_per_consumer as u32;
    assert_eq!(total_claimed, expected_total);

    // ── Verify pre-drain state ──────────────────────────────────────────

    // Inflight per consumer
    for i in 1..=num_consumers {
        assert_eq!(
            e.ctx().inflight.get(InFlightScope::Consumer, i),
            msgs_per_consumer as u32,
            "consumer {i} inflight should be {msgs_per_consumer}"
        );
    }

    // Inflight per queue
    for i in 1..=num_consumers {
        assert_eq!(
            e.ctx().inflight.get(InFlightScope::Queue, i),
            msgs_per_consumer as u32,
        );
    }

    // Per-subject inflight: each subject has N consumers claiming it
    for &hash in &all_subject_hashes {
        assert_eq!(
            e.ctx().inflight.get(InFlightScope::Subject, hash),
            num_consumers,
            "subject {hash:#X} should have {num_consumers} inflight"
        );
    }

    // Credits used on connection
    let credits_used_before = credit_limit
        - e.ctx().credit.available(CreditScope::Connection, conn.raw() as u32);
    assert_eq!(credits_used_before, expected_total);

    // Edge counts
    assert_eq!(
        e.ctx().edges.pending_by_connection.len_for(&e.ctx().graph.pending, &conn),
        expected_total as usize
    );
    assert_eq!(
        e.ctx().edges.bindings_by_connection.get(&conn).len(),
        num_consumers as usize
    );

    // ── DISCONNECT: drain_connection with Requeue ───────────────────────
    let report = e.drain_connection(&DrainConnectionReq {
        connection_id: conn,
        mode: DrainMode::ReleaseAndRequeue,
        now: Timestamp::new(5_000_000),
    });

    assert_eq!(report.pending_requeued, expected_total);
    assert_eq!(report.bindings_removed, num_consumers);

    // ── Verify EVERYTHING is clean ──────────────────────────────────────

    // All inflight counters at zero
    for i in 1..=num_consumers {
        assert_eq!(
            e.ctx().inflight.get(InFlightScope::Consumer, i), 0,
            "consumer {i} inflight should be 0 after drain"
        );
        assert_eq!(
            e.ctx().inflight.get(InFlightScope::Queue, i), 0,
            "queue {i} inflight should be 0 after drain"
        );
    }

    for &hash in &all_subject_hashes {
        assert_eq!(
            e.ctx().inflight.get(InFlightScope::Subject, hash), 0,
            "subject {hash:#X} inflight should be 0 after drain"
        );
    }

    // All connection credits released
    let available_after = e.ctx().credit
        .available(CreditScope::Connection, conn.raw() as u32);
    assert_eq!(available_after, credit_limit, "all credits should be returned");

    // All edge indexes for this connection empty
    assert!(!e.ctx().edges.pending_by_connection.contains_key(&conn));
    assert!(e.ctx().edges.bindings_by_connection.get(&conn).is_empty());

    // No pending by consumer (all were on this connection)
    for i in 1..=num_consumers {
        assert!(
            !e.ctx().edges.pending_by_consumer.contains_key(&ConsumerId(i)),
            "consumer {i} should have no pending after drain"
        );
    }

    // No pending by queue
    for i in 1..=num_consumers {
        assert!(
            !e.ctx().edges.pending_by_queue.contains_key(&QueueId(i)),
            "queue {i} should have no pending after drain"
        );
    }

// ── Messages requeued: each consumer's queue should have msgs ready ──
    for i in 1..=num_consumers {
        let reclaimed = claim_batch(&mut e, &ClaimBatch {
            queue_id: QueueId(i),
            connection_id: conn, // same conn (even though drained — it can re-connect)
            consumer_id: ConsumerId(i),
            max_items: msgs_per_consumer,
            now: Timestamp::new(6_000_000),
        });
        assert_eq!(
            reclaimed.accepted, msgs_per_consumer as u32,
            "consumer {i} should reclaim {msgs_per_consumer} after requeue"
        );
    }
}

/// Same setup but with Drop mode — messages are gone forever.
#[test]
fn massive_disconnect_drop_mode_no_requeue() {
    let num_consumers: u32 = 10;
    let msgs_per_consumer: u16 = 8;
    let conn = ConnectionId(200);

    let mut e = ArbitroEngine::new();

    e.ensure_stream(StreamConfig {
        id: StreamId(1),
        name: b"logs".to_vec(),
    }).unwrap();

    for i in 1..=num_consumers {
        e.ensure_consumer(ConsumerConfig {
            id: ConsumerId(i),
            queue_id: QueueId(i),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 1000,
        }).unwrap();
        e.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(i),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(i),
            filters: vec![],
        }).unwrap();
    }

    e.open_connection(&OpenConnectionReq {
        connection_id: conn,
        node_id: NodeId(1),
        now: Timestamp::new(0),
    });

    let bind_entries: Vec<BindEntry> = (1..=num_consumers)
        .map(|i| BindEntry {
            connection_id: conn,
            subscription_id: SubscriptionId(i),
        })
        .collect();
    e.bind(&BindBatch {
        entries: &bind_entries,
        now: Timestamp::new(0),
    });

    // Credit limit per connection
    e.ctx_mut().credit
        .set_limit(CreditScope::Connection, conn.raw() as u32, 500);

    // Publish
    let subjects: Vec<Vec<u8>> = (0..msgs_per_consumer as usize)
        .map(|i| format!("log.level.{i}").into_bytes())
        .collect();
    let pub_entries: Vec<PublishEntry> = subjects.iter()
        .map(|s| PublishEntry {
            subject_hash: fnv1a_32(s),
            subject: s,
            payload: PayloadRef::Borrowed(b"log-line"),
            idempotency_key: 0,
            credits_cost: 1,
        })
        .collect();
    e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &pub_entries,
        now: Timestamp::new(1_000_000),
    });

    // Claim on all consumers
    for i in 1..=num_consumers {
        let claimed = claim_batch(&mut e, &ClaimBatch {
        queue_id: QueueId(i),
        connection_id: conn,
        consumer_id: ConsumerId(i),
        max_items: msgs_per_consumer,
        now: Timestamp::new(2_000_000),
    });
        assert_eq!(claimed.accepted, msgs_per_consumer as u32);
    }

    let expected = num_consumers * msgs_per_consumer as u32;

    // DROP mode drain
    let report = e.drain_connection(&DrainConnectionReq {
        connection_id: conn,
        mode: DrainMode::ReleaseAndDrop,
        now: Timestamp::new(5_000_000),
    });
    assert_eq!(report.pending_released, expected);
    assert_eq!(report.pending_requeued, 0);
    assert_eq!(report.bindings_removed, num_consumers);

    // Verify all clean
    for i in 1..=num_consumers {
        assert_eq!(e.ctx().inflight.get(InFlightScope::Consumer, i), 0);
        assert_eq!(e.ctx().inflight.get(InFlightScope::Queue, i), 0);
        assert!(!e.ctx().edges.pending_by_consumer.contains_key(&ConsumerId(i)));
    }

    // Credits fully returned
    assert_eq!(
        e.ctx().credit
            .available(CreditScope::Connection, conn.raw() as u32),
        500
    );

    // Nothing to reclaim — messages were dropped
    for i in 1..=num_consumers {
        let empty = claim_batch(&mut e, &ClaimBatch {
        queue_id: QueueId(i),
        connection_id: conn,
        consumer_id: ConsumerId(i),
        max_items: 10,
        now: Timestamp::new(6_000_000),
    });
        assert_eq!(empty.accepted, 0, "queue {i} should be empty after drop-mode drain");
    }
}

// ── 19. Subject inflight limit enforcement ──────────────────────────────────

/// Per the spec: max_subject_inflight("message.qr.>", 1) means each concrete
/// subject matching that pattern has its own counter capped at 1.
/// message.qr.user_212 inflight=1 → blocked
/// message.qr.user_999 inflight=0 → can deliver
#[test]
fn max_subject_inflight_caps_inflight_per_subject() {
    let mut e = engine_with_one_consumer(100);

    // Set: each "order.*" subject can have at most 2 in-flight
    e.set_max_subject_inflight(StreamId(1), b"order.>", 2).unwrap();

    // Publish 5 on subject "order.A" and 3 on "order.B"
    let subj_a = b"order.A";
    let subj_b = b"order.B";
    let hash_a = fnv1a_32(subj_a);
    let hash_b = fnv1a_32(subj_b);

    let entries = [
        PublishEntry { subject_hash: hash_a, subject: subj_a, payload: PayloadRef::Borrowed(b"a1"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_a, subject: subj_a, payload: PayloadRef::Borrowed(b"a2"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_a, subject: subj_a, payload: PayloadRef::Borrowed(b"a3"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_a, subject: subj_a, payload: PayloadRef::Borrowed(b"a4"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_a, subject: subj_a, payload: PayloadRef::Borrowed(b"a5"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_b, subject: subj_b, payload: PayloadRef::Borrowed(b"b1"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_b, subject: subj_b, payload: PayloadRef::Borrowed(b"b2"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_b, subject: subj_b, payload: PayloadRef::Borrowed(b"b3"), idempotency_key: 0, credits_cost: 1 },
    ];
    e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_000_000),
    });

    // Claim — should get 2 from A + 2 from B = 4 (not all 8)
    let claimed = claim_n(&mut e, 20);
    assert_eq!(claimed.accepted, 4);

    // Subject A: exactly 2 inflight
    assert_eq!(e.ctx().inflight.get(InFlightScope::Subject, hash_a), 2);
    // Subject B: exactly 2 inflight
    assert_eq!(e.ctx().inflight.get(InFlightScope::Subject, hash_b), 2);

    // Remaining 4 messages still in ready
    assert_eq!(e.ctx().ready.total_ready(QueueId(1)), 4);
}

#[test]
fn max_subject_inflight_resumes_after_ack() {
    let mut e = engine_with_one_consumer(100);

    e.set_max_subject_inflight(StreamId(1), b"order.>", 1).unwrap();

    let subj = b"order.X";
    let hash = fnv1a_32(subj);

    let entries = [
        PublishEntry { subject_hash: hash, subject: subj, payload: PayloadRef::Borrowed(b"x1"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash, subject: subj, payload: PayloadRef::Borrowed(b"x2"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash, subject: subj, payload: PayloadRef::Borrowed(b"x3"), idempotency_key: 0, credits_cost: 1 },
    ];
    e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_000_000),
    });

    // Claim: limit=1, so only 1
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 1);

    // Ack it
    ack_all(&mut e, &claimed.entries);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Subject, hash), 0);

    // Now can claim 1 more
    let claimed2 = claim_n(&mut e, 10);
    assert_eq!(claimed2.accepted, 1);

    // Ack, claim last one
    ack_all(&mut e, &claimed2.entries);
    let claimed3 = claim_n(&mut e, 10);
    assert_eq!(claimed3.accepted, 1);

    // All 3 processed serially
    ack_all(&mut e, &claimed3.entries);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Subject, hash), 0);
    assert_eq!(e.ctx().ready.total_ready(QueueId(1)), 0);
}

/// Anti-HOL blocking: subject A is at limit but subject B is not.
/// Claim should skip A and deliver B.
#[test]
fn max_subject_inflight_anti_hol_skips_blocked_subject() {
    let mut e = engine_with_one_consumer(100);

    e.set_max_subject_inflight(StreamId(1), b"order.>", 1).unwrap();

    let subj_a = b"order.A";
    let subj_b = b"order.B";
    let hash_a = fnv1a_32(subj_a);
    let hash_b = fnv1a_32(subj_b);

    // Publish: A, A, B, B
    let entries = [
        PublishEntry { subject_hash: hash_a, subject: subj_a, payload: PayloadRef::Borrowed(b"a1"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_a, subject: subj_a, payload: PayloadRef::Borrowed(b"a2"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_b, subject: subj_b, payload: PayloadRef::Borrowed(b"b1"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash_b, subject: subj_b, payload: PayloadRef::Borrowed(b"b2"), idempotency_key: 0, credits_cost: 1 },
    ];
    e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_000_000),
    });

    // Claim with limit=1 per subject: should get 1×A + 1×B = 2
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 2);

    // One of each subject
    let has_a = claimed.entries.iter().any(|c| c.subject_hash == hash_a);
    let has_b = claimed.entries.iter().any(|c| c.subject_hash == hash_b);
    assert!(has_a, "should have claimed from subject A");
    assert!(has_b, "should have claimed from subject B");

    // 2 remaining in ready
    assert_eq!(e.ctx().ready.total_ready(QueueId(1)), 2);
}

/// Literal subject limit (no wildcards) — resolved immediately.
#[test]
fn max_subject_inflight_literal_pattern() {
    let mut e = engine_with_one_consumer(100);

    // Literal limit on exact subject
    let subj = b"order.priority";
    let hash = fnv1a_32(subj);
    e.set_max_subject_inflight(StreamId(1), subj, 1).unwrap();

    let entries = [
        PublishEntry { subject_hash: hash, subject: subj, payload: PayloadRef::Borrowed(b"p1"), idempotency_key: 0, credits_cost: 1 },
        PublishEntry { subject_hash: hash, subject: subj, payload: PayloadRef::Borrowed(b"p2"), idempotency_key: 0, credits_cost: 1 },
    ];
    e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_000_000),
    });

    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 1);
}

/// Subject limit + disconnect = credits and inflight released, messages requeued.
#[test]
fn max_subject_inflight_with_disconnect_full_cleanup() {
    let mut e = engine_with_one_consumer(100);

    e.set_max_subject_inflight(StreamId(1), b"order.>", 2).unwrap();

    // Also set subject credit limit
    let subj = b"order.X";
    let hash = fnv1a_32(subj);
    e.ctx_mut().credit
        .set_limit(CreditScope::Subject, hash, 5);

    // Publish 4 messages on order.X
    let entries: Vec<PublishEntry> = (0..4).map(|_| PublishEntry {

        subject_hash: hash,
        subject: subj,
        payload: PayloadRef::Borrowed(b"data"),
        idempotency_key: 0,
        credits_cost: 1,
    }).collect();
    e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_000_000),
    });

    // Claim — limited to 2 by subject limit
    let claimed = claim_n(&mut e, 10);
    assert_eq!(claimed.accepted, 2);
    assert_eq!(e.ctx().inflight.get(InFlightScope::Subject, hash), 2);

    // Subject credits: 2 used of 5
    assert_eq!(
        e.ctx().credit.available(CreditScope::Subject, hash),
        3
    );

    // Disconnect — drain
    let report = e.drain_connection(&DrainConnectionReq {
        connection_id: ConnectionId(100),
        mode: DrainMode::ReleaseAndRequeue,
        now: Timestamp::new(5_000_000),
    });
    assert_eq!(report.pending_requeued, 2);

    // Subject inflight back to 0
    assert_eq!(e.ctx().inflight.get(InFlightScope::Subject, hash), 0);

    // Subject credits fully released
    assert_eq!(
        e.ctx().credit.available(CreditScope::Subject, hash),
        5
    );

    // All 4 messages back in ready (2 requeued + 2 never claimed)
    assert_eq!(e.ctx().ready.total_ready(QueueId(1)), 4);
}
