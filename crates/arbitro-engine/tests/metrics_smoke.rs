//! Smoke test for `EngineMetrics` — verify counters increment on a full
//! publish → claim → ack cycle via the public API.
//!
//! This is deliberately lightweight — exhaustive counter-by-counter tests
//! would duplicate the unit tests in each runtime module. The goal here is
//! to catch regressions where a counter is wired to the wrong call site,
//! missed entirely, or reset accidentally.

use arbitro_engine::*;
use arbitro_engine::batch::*;
use arbitro_engine::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig, fnv1a_32};
use arbitro_engine::runtime::claim::resolve_ids_for_batch;
use arbitro_engine::types::*;

fn setup() -> ArbitroEngine {
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
        max_inflight: 1000,
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

#[test]
fn publish_claim_ack_increments_counters() {
    let mut e = setup();

    // Baseline is zero.
    let baseline = e.metrics_snapshot();
    assert_eq!(baseline.publish_entries_accepted, 0);
    assert_eq!(baseline.claim_entries_delivered, 0);
    assert_eq!(baseline.ack_accepted, 0);

    // Publish 5 messages.
    let subjects: Vec<Vec<u8>> = (0..5)
        .map(|i| format!("orders.{i}").into_bytes())
        .collect();
    let entries: Vec<PublishEntry> = (0..5).map(|i| PublishEntry {
        subject_hash: fnv1a_32(&subjects[i]),
        subject: &subjects[i],
        payload: PayloadRef::Borrowed(b"payload"),
        idempotency_key: 0,
        credits_cost: 1,
    }).collect();
    e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_000_000),
    });

    let after_pub = e.metrics_snapshot();
    assert_eq!(after_pub.publish_entries_accepted, 5);
    assert_eq!(after_pub.publish_queues_pushed, 5);
    assert_eq!(after_pub.publish_fanout_notified, 5);
    assert_eq!(after_pub.publish_no_match, 0);
    assert_eq!(after_pub.publish_duplicates_skipped, 0);

    // Claim all 5.
    let claim = ClaimBatch {
        queue_id: QueueId(1),
        connection_id: ConnectionId(100),
        consumer_id: ConsumerId(1),
        max_items: 10,
        now: Timestamp::new(2_000_000),
    };
    let ack_entries: Vec<AckEntry>;
    {
        let (sub, bind) = resolve_ids_for_batch(e.ctx(), &claim);
        let claimed = e.claim(&claim, sub, bind);
        assert_eq!(claimed.accepted, 5);
        ack_entries = claimed.entries().iter().map(|c| AckEntry { seq: c.seq }).collect();
    }

    let after_claim = e.metrics_snapshot();
    assert_eq!(after_claim.claim_batches, 1);
    assert_eq!(after_claim.claim_entries_delivered, 5);

    // Ack all 5.
    e.ack(&AckBatch {
        consumer_id: ConsumerId(1),
        entries: &ack_entries,
        now: Timestamp::new(3_000_000),
    });

    let after_ack = e.metrics_snapshot();
    assert_eq!(after_ack.ack_accepted, 5);
    assert_eq!(after_ack.ack_not_found, 0);
}

#[test]
fn duplicates_and_no_match_counted() {
    let mut e = setup();

    // Publish with idempotency key — 2nd entry is a duplicate.
    let entries = [
        PublishEntry {
            subject_hash: fnv1a_32(b"orders.x"),
            subject: b"orders.x",
            payload: PayloadRef::Borrowed(b"a"),
            idempotency_key: 999,
            credits_cost: 1,
        },
        PublishEntry {
            subject_hash: fnv1a_32(b"orders.x"),
            subject: b"orders.x",
            payload: PayloadRef::Borrowed(b"b"),
            idempotency_key: 999,
            credits_cost: 1,
        },
    ];
    e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(0),
    });

    let snap = e.metrics_snapshot();
    assert_eq!(snap.publish_entries_accepted, 1);
    assert_eq!(snap.publish_duplicates_skipped, 1);
}

#[test]
fn inflight_introspection_tracks_consumer_capacity() {
    let mut e = setup();
    let cid = ConsumerId(1);

    // Cache max_inflight as the worker would (cold path).
    let max = e.consumer_max_inflight(cid).expect("consumer exists");
    assert_eq!(max, 1000);
    assert!(!e.consumer_paused(cid));

    // Empty engine: full capacity, zero inflight.
    assert_eq!(e.consumer_inflight(cid), 0);
    assert!(e.consumer_has_capacity(cid, max));
    assert_eq!(e.consumer_capacity_remaining(cid, max), 1000);

    // Publish + claim 5 — inflight should jump to 5.
    let subjects: Vec<Vec<u8>> = (0..5)
        .map(|i| format!("orders.{i}").into_bytes())
        .collect();
    let entries: Vec<PublishEntry> = (0..5).map(|i| PublishEntry {
        subject_hash: fnv1a_32(&subjects[i]),
        subject: &subjects[i],
        payload: PayloadRef::Borrowed(b"x"),
        idempotency_key: 0,
        credits_cost: 1,
    }).collect();
    e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(0),
    });

    let claim = ClaimBatch {
        queue_id: QueueId(1),
        connection_id: ConnectionId(100),
        consumer_id: cid,
        max_items: 10,
        now: Timestamp::new(1),
    };
    let acks: Vec<AckEntry> = {
        let (sub, bind) = resolve_ids_for_batch(e.ctx(), &claim);
        let claimed = e.claim(&claim, sub, bind);
        claimed.entries().iter().map(|c| AckEntry { seq: c.seq }).collect()
    };

    assert_eq!(e.consumer_inflight(cid), 5);
    assert!(e.consumer_has_capacity(cid, max));
    assert_eq!(e.consumer_capacity_remaining(cid, max), 995);

    // Capacity check fails when caller pretends max is 5.
    assert!(!e.consumer_has_capacity(cid, 5));
    assert_eq!(e.consumer_capacity_remaining(cid, 5), 0);

    // Ack drops inflight back to zero.
    e.ack(&AckBatch {
        consumer_id: cid,
        entries: &acks,
        now: Timestamp::new(2),
    });
    assert_eq!(e.consumer_inflight(cid), 0);
    assert_eq!(e.consumer_capacity_remaining(cid, max), 1000);
}

#[test]
fn subject_inflight_introspection_respects_tracking_gate() {
    let mut e = setup();

    // Tracking off by default — subject_inflight always returns 0.
    assert!(!e.subject_tracking_enabled());
    let h = fnv1a_32(b"orders.gated");
    assert_eq!(e.subject_inflight(h), 0);
    // No limit configured for this subject.
    assert_eq!(e.subject_max_inflight(StreamId(1), h), None);
    // has_capacity is trivially true while tracking is off.
    assert!(e.subject_has_capacity(h, 1));

    // Configure a per-subject limit — flips the sticky tracking flag.
    e.set_max_subject_inflight(StreamId(1), b"orders.gated", 2).unwrap();
    assert!(e.subject_tracking_enabled());
    assert_eq!(e.subject_max_inflight(StreamId(1), h), Some(2));

    // Publish + claim 2 to fill the subject limit.
    for i in 0..2 {
        e.publish(&PublishBatch {
            stream_id: StreamId(1),
            entries: &[PublishEntry {
                subject_hash: h,
                subject: b"orders.gated",
                payload: PayloadRef::Borrowed(b"x"),
                idempotency_key: i + 1, // distinct keys to avoid dedup
                credits_cost: 1,
            }],
            now: Timestamp::new(i),
        });
    }
    let claim = ClaimBatch {
        queue_id: QueueId(1),
        connection_id: ConnectionId(100),
        consumer_id: ConsumerId(1),
        max_items: 5,
        now: Timestamp::new(10),
    };
    {
        let (sub, bind) = resolve_ids_for_batch(e.ctx(), &claim);
        let _ = e.claim(&claim, sub, bind);
    }

    // 2 inflight on the subject — at the limit.
    assert_eq!(e.subject_inflight(h), 2);
    assert!(!e.subject_has_capacity(h, 2));
    assert!(e.subject_has_capacity(h, 3));
}

#[test]
fn nack_increments_counter() {
    let mut e = setup();

    // Publish 1 and claim it.
    let subject = b"orders.one";
    e.publish(&PublishBatch {
        stream_id: StreamId(1),
        entries: &[PublishEntry {
            subject_hash: fnv1a_32(subject),
            subject,
            payload: PayloadRef::Borrowed(b"x"),
            idempotency_key: 0,
            credits_cost: 1,
        }],
        now: Timestamp::new(0),
    });

    let claim = ClaimBatch {
        queue_id: QueueId(1),
        connection_id: ConnectionId(100),
        consumer_id: ConsumerId(1),
        max_items: 1,
        now: Timestamp::new(1),
    };
    let seq = {
        let (sub, bind) = resolve_ids_for_batch(e.ctx(), &claim);
        let claimed = e.claim(&claim, sub, bind);
        claimed.entries()[0].seq
    };

    e.nack(&NackBatch {
        consumer_id: ConsumerId(1),
        entries: &[NackEntry { seq, retry_at: None }],
        now: Timestamp::new(2),
    });

    assert_eq!(e.metrics_snapshot().nack_accepted, 1);
}
