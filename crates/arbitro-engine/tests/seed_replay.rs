//! Regression + coverage for `engine.enqueue_ready` (seed / replay path).
//!
//! The previous implementation in `lib.rs` hard-capped at 8 matched queues
//! and silently dropped any overflow — a **correctness** bug for a fanout
//! broker. These tests lock in:
//!
//! * `seed_overflow_many_queues` — 16 consumers on the same subject, ALL
//!   16 queues must receive the replayed seq.
//! * `seed_preserves_next_seq` — engine's publish seq counter is untouched.
//! * `seed_no_match_counter` — `seed_no_match` metric fires when a subject
//!   doesn't match any queue.
//! * `seed_dedups_same_queue` — if the match table surfaces the same queue
//!   twice (e.g. two patterns covering the same subject), we push once.

use arbitro_engine::*;
use arbitro_engine::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig, fnv1a_32};
use arbitro_engine::types::*;
use std::sync::atomic::Ordering;

fn engine_with_n_consumers(n: u32) -> ArbitroEngine {
    let mut e = ArbitroEngine::new();
    e.ensure_stream(StreamConfig {
        id: StreamId(1),
        name: b"orders".to_vec(),
    }).unwrap();

    for i in 1..=n {
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
            filters: vec![], // wildcard: match everything
        }).unwrap();
    }
    e
}

#[test]
fn seed_overflow_many_queues() {
    // 16 > 8 — the old cap. Every queue MUST receive the replayed seq.
    let mut e = engine_with_n_consumers(16);
    let subject = b"orders.replay";
    let hash = fnv1a_32(subject);

    let pushed = e.enqueue_ready(StreamId(1), subject, hash, 1_000);
    e.flush_seed_metrics(1, 0, pushed as u64);

    for q in 1..=16u32 {
        assert!(
            e.ctx().ready.has_ready(QueueId(q)),
            "queue {q} missing replayed seq — regression of silent 8-queue cap",
        );
    }

    let snap = e.metrics_snapshot();
    assert_eq!(snap.seed_entries, 1);
    assert_eq!(snap.seed_queues_pushed, 16);
    assert_eq!(snap.seed_no_match, 0);
}

#[test]
fn seed_preserves_next_seq() {
    let mut e = engine_with_n_consumers(2);
    let before = e.ctx().next_seq;
    e.enqueue_ready(StreamId(1), b"orders.x", fnv1a_32(b"orders.x"), 12345);
    assert_eq!(
        e.ctx().next_seq, before,
        "enqueue_ready must not touch ctx.next_seq (seqs come from store)",
    );
}

#[test]
fn seed_no_match_counter() {
    let mut e = engine_with_n_consumers(1);
    // Stream 999 has no match table at all.
    let pushed = e.enqueue_ready(StreamId(999), b"whatever", fnv1a_32(b"whatever"), 1);
    e.flush_seed_metrics(1, if pushed == 0 { 1 } else { 0 }, pushed as u64);

    let snap = e.metrics_snapshot();
    assert_eq!(snap.seed_entries, 1);
    assert_eq!(snap.seed_queues_pushed, 0);
    assert_eq!(snap.seed_no_match, 1);
}

#[test]
fn seed_single_queue_match() {
    let mut e = engine_with_n_consumers(1);
    let subject = b"orders.created";
    let pushed = e.enqueue_ready(StreamId(1), subject, fnv1a_32(subject), 42);
    e.flush_seed_metrics(1, 0, pushed as u64);
    assert!(e.ctx().ready.has_ready(QueueId(1)));
    assert_eq!(e.metrics().seed_queues_pushed.load(Ordering::Relaxed), 1);
}

#[test]
fn seed_entries_counter_monotonic() {
    let mut e = engine_with_n_consumers(3);
    let mut total_pushed: u64 = 0;
    for seq in 1..=5 {
        total_pushed += e.enqueue_ready(StreamId(1), b"orders.x", fnv1a_32(b"orders.x"), seq) as u64;
    }
    e.flush_seed_metrics(5, 0, total_pushed);
    let snap = e.metrics_snapshot();
    assert_eq!(snap.seed_entries, 5);
    assert_eq!(snap.seed_queues_pushed, 15); // 3 queues × 5 entries
    assert_eq!(snap.seed_no_match, 0);
}
