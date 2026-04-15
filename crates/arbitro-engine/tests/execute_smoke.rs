//! Smoke test for the Command kernel entry point.
//!
//! Verifies that `engine.execute(&Command::*)` wires each variant to its
//! metrics counter(s). This is the W3.1 contract — `execute` is live in
//! parallel to the legacy API and advances observability counters so the
//! forthcoming drainer (Fase 2) can route through it without losing
//! metrics coverage.

use std::sync::atomic::Ordering;

use arbitro_engine::command::{Command, DropReason, MsgRef, StreamSeq};
use arbitro_engine::types::{
    ConnectionId, ConsumerId, QueueId, StreamId, SubscriptionId,
};
use arbitro_engine::ArbitroEngine;
use bytes::Bytes;

#[test]
fn execute_fanout_advances_delivery_counters() {
    let mut engine = ArbitroEngine::new();
    let before_fanout = engine.metrics().publish_fanout_notified.load(Ordering::Relaxed);
    let before_delivered = engine
        .metrics()
        .claim_entries_delivered
        .load(Ordering::Relaxed);

    let consumers = [ConsumerId(1), ConsumerId(2), ConsumerId(3)];
    let entries = [
        MsgRef {
            seq: 10,
            subject_hash: 0xABCD,
            subject: b"orders.created.v1",
            payload: Bytes::from_static(b"payload-a"),
        },
        MsgRef {
            seq: 11,
            subject_hash: 0xABCD,
            subject: b"orders.created.v1",
            payload: Bytes::from_static(b"payload-b"),
        },
    ];

    engine.execute(&Command::Fanout {
        stream_id: StreamId(1),
        connection_id: ConnectionId(42),
        consumers: &consumers,
        entries: &entries,
    });

    let after_fanout = engine.metrics().publish_fanout_notified.load(Ordering::Relaxed);
    let after_delivered = engine
        .metrics()
        .claim_entries_delivered
        .load(Ordering::Relaxed);

    // 3 consumers × 2 entries = 6 fanout notifications
    assert_eq!(after_fanout - before_fanout, 6);
    assert_eq!(after_delivered - before_delivered, 2);
}

#[test]
fn execute_queue_advances_queue_and_delivery() {
    let mut engine = ArbitroEngine::new();
    let before_delivered = engine
        .metrics()
        .claim_entries_delivered
        .load(Ordering::Relaxed);
    let before_queue = engine.metrics().publish_queues_pushed.load(Ordering::Relaxed);

    engine.execute(&Command::Queue {
        stream_id: StreamId(1),
        queue_id: QueueId(7),
        consumer_id: ConsumerId(5),
        subscription_id: SubscriptionId(9),
        connection_id: ConnectionId(42),
        entry: MsgRef {
            seq: 100,
            subject_hash: 0xBEEF,
            subject: b"jobs.work",
            payload: Bytes::from_static(b"job-1"),
        },
    });

    assert_eq!(
        engine
            .metrics()
            .claim_entries_delivered
            .load(Ordering::Relaxed)
            - before_delivered,
        1
    );
    assert_eq!(
        engine.metrics().publish_queues_pushed.load(Ordering::Relaxed) - before_queue,
        1
    );
}

#[test]
fn execute_batch_ack_nack_repok_tombstone() {
    let mut engine = ArbitroEngine::new();
    let m = engine.metrics();
    let ack0 = m.ack_accepted.load(Ordering::Relaxed);
    let nack0 = m.nack_accepted.load(Ordering::Relaxed);
    let rep0 = m.publish_entries_accepted.load(Ordering::Relaxed);
    let drop0 = m.publish_no_match.load(Ordering::Relaxed);

    let ack_entries = [
        StreamSeq { stream_id: StreamId(1), seq: 1 },
        StreamSeq { stream_id: StreamId(1), seq: 2 },
    ];
    let nack_entries = [StreamSeq { stream_id: StreamId(1), seq: 3 }];
    let rep_entries = [
        StreamSeq { stream_id: StreamId(1), seq: 10 },
        StreamSeq { stream_id: StreamId(1), seq: 11 },
        StreamSeq { stream_id: StreamId(1), seq: 12 },
    ];

    let cmds = [
        Command::Ack { entries: &ack_entries },
        Command::Nack { entries: &nack_entries },
        Command::RepOk {
            connection_id: ConnectionId(42),
            env_seq: 77,
            entries: &rep_entries,
        },
        Command::Tombstone {
            stream_id: StreamId(1),
            seq: 99,
            reason: DropReason::Expired,
        },
    ];

    engine.execute_batch(&cmds);

    let m = engine.metrics();
    assert_eq!(m.ack_accepted.load(Ordering::Relaxed) - ack0, 2);
    assert_eq!(m.nack_accepted.load(Ordering::Relaxed) - nack0, 1);
    assert_eq!(m.publish_entries_accepted.load(Ordering::Relaxed) - rep0, 3);
    assert_eq!(m.publish_no_match.load(Ordering::Relaxed) - drop0, 1);
}
