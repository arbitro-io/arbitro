//! Benchmark: inflight limit enforcement under stress.
//!
//! Verifies that max_inflight and subject_inflight limits are NEVER
//! violated across thousands of publish→claim→ack cycles.
//! Panics immediately if any invariant breaks.

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, black_box, Throughput};
use arbitro_engine::*;
use arbitro_engine::batch::*;
use arbitro_engine::catalog::{StreamConfig, ConsumerConfig, SubscriptionConfig, fnv1a_32};
use arbitro_engine::inflight::InFlightScope;
use arbitro_engine::types::*;

// ── Helpers ────────────────────────────────────────────────────────────────

fn engine_with_limit(max_inflight: u32) -> ArbitroEngine {
    let mut e = ArbitroEngine::new();

    e.ensure_stream(StreamConfig {
        id: StreamId(1),
        name: b"stress".to_vec(),
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

struct StressMessages {
    subjects: Vec<Vec<u8>>,
    hashes: Vec<u32>,
}

impl StressMessages {
    fn new(count: usize) -> Self {
        let subjects: Vec<Vec<u8>> = (0..count)
            .map(|i| format!("stress.subj.{}", i % 20).into_bytes())
            .collect();
        let hashes: Vec<u32> = subjects.iter().map(|s| fnv1a_32(s)).collect();
        Self { subjects, hashes }
    }

    fn publish_entries(&self) -> Vec<PublishEntry<'_>> {
        self.subjects.iter().enumerate().map(|(i, s)| PublishEntry {

            subject_hash: self.hashes[i],
            subject: s,
            payload: PayloadRef::Borrowed(b"stress-payload"),
            idempotency_key: 0,
            credits_cost: 1,
        }).collect()
    }
}

// ── 1. max_inflight stress ──────────────────────────────────────────────

fn bench_max_inflight_stress(c: &mut Criterion) {
    let mut group = c.benchmark_group("inflight_max_inflight");

    for (msgs, limit) in [(100, 10u32), (100, 25), (1000, 50)] {
        let data = StressMessages::new(msgs);

        group.throughput(Throughput::Elements(msgs as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{msgs}m_limit{limit}"), msgs),
            &(),
            |b, _| {
                let mut engine = engine_with_limit(limit);
                let mut ts = 1_000_000u64;

                b.iter(|| {
                    ts += 1_000_000;

                    // Publish
                    let entries = data.publish_entries();
                    engine.publish(&PublishBatch {
                        stream_id: StreamId(1),
                        entries: black_box(&entries),
                        now: Timestamp::new(ts),
                    });

                    // Claim→ack in rounds until all drained
                    let mut total_acked = 0u32;
                    let mut rounds = 0u32;
                    while total_acked < msgs as u32 {
                        ts += 1000;
                        let (accepted, ack_entries): (u32, Vec<AckEntry>);
                        {
                            let __batch = ClaimBatch {
                                queue_id: QueueId(1),
                                connection_id: ConnectionId(100),
                                consumer_id: ConsumerId(1),
                                max_items: msgs as u16,
                                now: Timestamp::new(ts),
                            };
                            let (__sub, __bind) = arbitro_engine::runtime::claim::resolve_ids_for_batch(engine.ctx(), &__batch);
                            let claimed = engine.claim(&__batch, __sub, __bind);
                            accepted = claimed.accepted;
                            ack_entries = claimed.entries().iter()
                                .map(|e| AckEntry { seq: e.seq })
                                .collect();
                        }

                        // INVARIANT: inflight must NEVER exceed limit
                        let inflight = engine.ctx().inflight.get(
                            InFlightScope::Consumer, 1,
                        );
                        assert!(
                            inflight <= limit,
                            "max_inflight VIOLATED: inflight={inflight} > limit={limit} at round {rounds}"
                        );

                        if accepted == 0 { break; }

                        // Also verify claimed count respects limit
                        assert!(
                            accepted <= limit,
                            "claimed {accepted} > limit {limit}",
                        );

                        // Ack all
                        ts += 1000;
                        engine.ack(&AckBatch {
                            consumer_id: ConsumerId(1),
                            entries: &ack_entries,
                            now: Timestamp::new(ts),
                        });

                        total_acked += accepted;
                        rounds += 1;
                    }

                    // INVARIANT: all messages must be processed
                    assert_eq!(total_acked, msgs as u32, "not all messages acked");

                    // INVARIANT: clean state after full drain
                    let final_inflight = engine.ctx().inflight.get(
                        InFlightScope::Consumer, 1,
                    );
                    assert_eq!(final_inflight, 0, "inflight not zero after full ack");

                    black_box(total_acked);
                });
            },
        );
    }

    group.finish();
}

// ── 2. subject_inflight stress ─────────────────────────────────────────────

fn bench_subject_inflight_stress(c: &mut Criterion) {
    let mut group = c.benchmark_group("inflight_max_subject_inflight");

    for (msgs, max_subject_inflight) in [(100, 1u32), (100, 3), (1000, 5)] {
        let data = StressMessages::new(msgs);
        let unique_hashes: Vec<u32> = {
            let mut h: Vec<u32> = data.hashes.clone();
            h.sort();
            h.dedup();
            h
        };

        group.throughput(Throughput::Elements(msgs as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{msgs}m_sublimit{max_subject_inflight}"), msgs),
            &(),
            |b, _| {
                let mut engine = engine_with_limit(100_000); // high ack limit
                engine.set_max_subject_inflight(StreamId(1), b"stress.>", max_subject_inflight).unwrap();
                let mut ts = 1_000_000u64;

                b.iter(|| {
                    ts += 1_000_000;

                    // Publish
                    let entries = data.publish_entries();
                    engine.publish(&PublishBatch {
                        stream_id: StreamId(1),
                        entries: black_box(&entries),
                        now: Timestamp::new(ts),
                    });

                    // Claim→ack in rounds
                    let mut total_acked = 0u32;
                    let mut rounds = 0u32;
                    while total_acked < msgs as u32 {
                        ts += 1000;
                        let (accepted, ack_entries): (u32, Vec<AckEntry>);
                        {
                            let __batch = ClaimBatch {
                                queue_id: QueueId(1),
                                connection_id: ConnectionId(100),
                                consumer_id: ConsumerId(1),
                                max_items: msgs as u16,
                                now: Timestamp::new(ts),
                            };
                            let (__sub, __bind) = arbitro_engine::runtime::claim::resolve_ids_for_batch(engine.ctx(), &__batch);
                            let claimed = engine.claim(&__batch, __sub, __bind);
                            accepted = claimed.accepted;
                            ack_entries = claimed.entries().iter()
                                .map(|e| AckEntry { seq: e.seq })
                                .collect();
                        }

                        // INVARIANT: no subject exceeds its limit
                        for &h in &unique_hashes {
                            let inflight = engine.ctx().inflight.get(
                                InFlightScope::Subject, h,
                            );
                            assert!(
                                inflight <= max_subject_inflight,
                                "max_subject_inflight VIOLATED: subject_hash={h:#X} inflight={inflight} > limit={max_subject_inflight} at round {rounds}"
                            );
                        }

                        if accepted == 0 { break; }

                        // Ack all
                        ts += 1000;
                        engine.ack(&AckBatch {
                            consumer_id: ConsumerId(1),
                            entries: &ack_entries,
                            now: Timestamp::new(ts),
                        });

                        total_acked += accepted;
                        rounds += 1;
                    }

                    // INVARIANT: all messages processed
                    assert_eq!(total_acked, msgs as u32, "not all messages acked");

                    // INVARIANT: all subjects clean
                    for &h in &unique_hashes {
                        assert_eq!(
                            engine.ctx().inflight.get(InFlightScope::Subject, h), 0,
                            "subject {h:#X} inflight not zero after full ack"
                        );
                    }

                    black_box(total_acked);
                });
            },
        );
    }

    group.finish();
}

// ── 3. Both limits active simultaneously ───────────────────────────────────

fn bench_combined_limits_stress(c: &mut Criterion) {
    let mut group = c.benchmark_group("inflight_combined");

    let msgs = 200;
    let max_ack = 20u32;
    let max_subject_inflight = 3u32;

    let data = StressMessages::new(msgs);
    let unique_hashes: Vec<u32> = {
        let mut h: Vec<u32> = data.hashes.clone();
        h.sort();
        h.dedup();
        h
    };

    group.throughput(Throughput::Elements(msgs as u64));
    group.bench_function(
        format!("{msgs}m_ack{max_ack}_subj{max_subject_inflight}"),
        |b| {
            let mut engine = engine_with_limit(max_ack);
            engine.set_max_subject_inflight(StreamId(1), b"stress.>", max_subject_inflight).unwrap();
            let mut ts = 1_000_000u64;

            b.iter(|| {
                ts += 1_000_000;

                let entries = data.publish_entries();
                engine.publish(&PublishBatch {
                    stream_id: StreamId(1),
                    entries: black_box(&entries),
                    now: Timestamp::new(ts),
                });

                let mut total_acked = 0u32;
                let mut rounds = 0u32;
                while total_acked < msgs as u32 {
                    ts += 1000;
                    let (accepted, ack_entries): (u32, Vec<AckEntry>);
                    {
                        let __batch = ClaimBatch {
                            queue_id: QueueId(1),
                            connection_id: ConnectionId(100),
                            consumer_id: ConsumerId(1),
                            max_items: msgs as u16,
                            now: Timestamp::new(ts),
                        };
                            let (__sub, __bind) = arbitro_engine::runtime::claim::resolve_ids_for_batch(engine.ctx(), &__batch);
                            let claimed = engine.claim(&__batch, __sub, __bind);
                        accepted = claimed.accepted;
                        ack_entries = claimed.entries().iter()
                            .map(|e| AckEntry { seq: e.seq })
                            .collect();
                    }

                    // INVARIANT: consumer inflight <= max_inflight
                    let consumer_inflight = engine.ctx().inflight.get(
                        InFlightScope::Consumer, 1,
                    );
                    assert!(
                        consumer_inflight <= max_ack,
                        "max_inflight VIOLATED: {consumer_inflight} > {max_ack} round {rounds}"
                    );

                    // INVARIANT: each subject <= max_subject_inflight
                    for &h in &unique_hashes {
                        let si = engine.ctx().inflight.get(InFlightScope::Subject, h);
                        assert!(
                            si <= max_subject_inflight,
                            "max_subject_inflight VIOLATED: {h:#X} {si} > {max_subject_inflight} round {rounds}"
                        );
                    }

                    if accepted == 0 { break; }

                    ts += 1000;
                    engine.ack(&AckBatch {
                        consumer_id: ConsumerId(1),
                        entries: &ack_entries,
                        now: Timestamp::new(ts),
                    });

                    total_acked += accepted;
                    rounds += 1;
                }

                assert_eq!(total_acked, msgs as u32, "not all messages acked");

                let final_inflight = engine.ctx().inflight.get(
                    InFlightScope::Consumer, 1,
                );
                assert_eq!(final_inflight, 0);

                black_box(total_acked);
            });
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    bench_max_inflight_stress,
    bench_subject_inflight_stress,
    bench_combined_limits_stress,
);
criterion_main!(benches);
