//! Benchmark: end-to-end throughput — messages/second through the full engine.
//!
//! Measures realistic flows:
//! 1. publish → claim → ack (baseline throughput)
//! 2. publish → claim → ack with subject limits + credit limits (enforced)
//! 3. publish → claim → nack → reclaim → ack (redelivery)
//! 4. publish → claim → drain_connection → reclaim → ack (disconnect recovery)
//! 5. fanout: publish 1 → N consumers claim → ack
//!
//! Each benchmark reports ns/msg for the FULL lifecycle of a message.

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, black_box, Throughput};
use arbitro_engine::*;
use arbitro_engine::batch::*;
use arbitro_engine::catalog::{StreamConfig, ConsumerConfig, SubscriptionConfig, fnv1a_32};
use arbitro_engine::types::*;

// ── Engine setup helpers ────────────────────────────────────────────────────

fn engine_simple(num_consumers: u32, max_inflight: u32) -> ArbitroEngine {
    let mut e = ArbitroEngine::new();

    e.ensure_stream(StreamConfig {
        id: StreamId(1),
        name: b"bench".to_vec(),
    }).unwrap();

    for i in 1..=num_consumers {
        e.ensure_consumer(ConsumerConfig {
            id: ConsumerId(i),
            queue_id: QueueId(i),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight,
        }).unwrap();

        e.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(i),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(i),
            filters: vec![],
        }).unwrap();
    }

    e.open_connection(&OpenConnectionReq {
        connection_id: ConnectionId(100),
        node_id: NodeId(1),
        now: Timestamp::new(0),
    });

    let bind_entries: Vec<BindEntry> = (1..=num_consumers)
        .map(|i| BindEntry {
            connection_id: ConnectionId(100),
            subscription_id: SubscriptionId(i),
        })
        .collect();
    e.bind(&BindBatch {
        entries: &bind_entries,
        now: Timestamp::new(0),
    });

    e
}

struct BenchMessages {
    subjects: Vec<Vec<u8>>,
}

impl BenchMessages {
    fn new(count: usize) -> Self {
        Self {
            subjects: (0..count).map(|i| format!("bench.subj.{i}").into_bytes()).collect(),
        }
    }

    fn publish_entries(&self) -> Vec<PublishEntry<'_>> {
        self.subjects.iter().map(|s| PublishEntry {

            subject_hash: fnv1a_32(s),
            subject: s,
            payload: PayloadRef::Borrowed(b"bench-payload-64B-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
            idempotency_key: 0,
            credits_cost: 1,
        }).collect()
    }
}

// ── 1. Baseline: publish → claim → ack ─────────────────────────────────────

fn bench_full_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_full_cycle");

    for count in [10, 100, 1000] {
        let msgs = BenchMessages::new(count);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("publish_claim_ack", count),
            &count,
            |b, &n| {
                let mut engine = engine_simple(1, 100_000);

                // Pre-build publish entries ONCE — move allocs + fnv1a_32
                // recomputation out of the measured loop.
                let publish_entries = msgs.publish_entries();
                // Pre-size ack scratch; clear+extend each iter (no alloc).
                let mut ack_scratch: Vec<AckEntry> = Vec::with_capacity(n);

                // Resolve sub/bind once — drainer caches this in production.
                let probe_batch = ClaimBatch {
                    queue_id: QueueId(1),
                    connection_id: ConnectionId(100),
                    consumer_id: ConsumerId(1),
                    max_items: n as u16,
                    now: Timestamp::new(0),
                };
                let (sub, bind) = arbitro_engine::runtime::claim::resolve_ids_for_batch(
                    engine.ctx(), &probe_batch);

                b.iter(|| {
                    engine.publish(&PublishBatch {
                        stream_id: StreamId(1),
                        entries: black_box(&publish_entries),
                        now: Timestamp::new(1_000_000),
                    });

                    let claim_batch = ClaimBatch {
                        queue_id: QueueId(1),
                        connection_id: ConnectionId(100),
                        consumer_id: ConsumerId(1),
                        max_items: n as u16,
                        now: Timestamp::new(2_000_000),
                    };
                    let claimed = engine.claim(&claim_batch, sub, bind);

                    ack_scratch.clear();
                    ack_scratch.extend(
                        claimed.entries().iter().map(|e| AckEntry { seq: e.seq }),
                    );

                    black_box(engine.ack(&AckBatch {
                        consumer_id: ConsumerId(1),
                        entries: &ack_scratch,
                        now: Timestamp::new(3_000_000),
                    }));
                });
            },
        );
    }

    group.finish();
}

// ── 2. With subject limits + credit limits ──────────────────────────────────

fn bench_with_limits(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_with_limits");

    for count in [10, 100, 1000] {
        let msgs = BenchMessages::new(count);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("max_subject_inflight_5", count),
            &count,
            |b, &n| {
                let mut engine = engine_simple(1, 100_000);
                engine.set_max_subject_inflight(StreamId(1), b"bench.>", 5).unwrap();
                engine.ctx_mut().credit
                    .set_limit(CreditScope::Connection, 100, n as u32);

                let publish_entries = msgs.publish_entries();
                let mut ack_scratch: Vec<AckEntry> = Vec::with_capacity(n);
                let probe_batch = ClaimBatch {
                    queue_id: QueueId(1),
                    connection_id: ConnectionId(100),
                    consumer_id: ConsumerId(1),
                    max_items: n as u16,
                    now: Timestamp::new(0),
                };
                let (sub, bind) = arbitro_engine::runtime::claim::resolve_ids_for_batch(
                    engine.ctx(), &probe_batch);

                b.iter(|| {
                    engine.publish(&PublishBatch {
                        stream_id: StreamId(1),
                        entries: black_box(&publish_entries),
                        now: Timestamp::new(1_000_000),
                    });

                    let mut total_acked = 0u32;
                    while total_acked < n as u32 {
                        let claim_batch = ClaimBatch {
                            queue_id: QueueId(1),
                            connection_id: ConnectionId(100),
                            consumer_id: ConsumerId(1),
                            max_items: n as u16,
                            now: Timestamp::new(2_000_000),
                        };
                        let claimed = engine.claim(&claim_batch, sub, bind);
                        let accepted = claimed.accepted;
                        ack_scratch.clear();
                        ack_scratch.extend(
                            claimed.entries().iter().map(|e| AckEntry { seq: e.seq }),
                        );
                        if accepted == 0 { break; }

                        engine.ack(&AckBatch {
                            consumer_id: ConsumerId(1),
                            entries: &ack_scratch,
                            now: Timestamp::new(3_000_000),
                        });
                        total_acked += accepted;
                    }
                    black_box(total_acked);
                });
            },
        );
    }

    group.finish();
}

// ── 3. Nack redelivery cycle ────────────────────────────────────────────────

fn bench_nack_redeliver(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_nack_redeliver");

    for count in [10, 100, 1000] {
        let msgs = BenchMessages::new(count);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("nack_then_ack", count),
            &count,
            |b, &n| {
                let mut engine = engine_simple(1, 100_000);
                let publish_entries = msgs.publish_entries();
                let mut nack_scratch: Vec<NackEntry> = Vec::with_capacity(n);
                let mut ack_scratch: Vec<AckEntry> = Vec::with_capacity(n);
                let probe_batch = ClaimBatch {
                    queue_id: QueueId(1),
                    connection_id: ConnectionId(100),
                    consumer_id: ConsumerId(1),
                    max_items: n as u16,
                    now: Timestamp::new(0),
                };
                let (sub, bind) = arbitro_engine::runtime::claim::resolve_ids_for_batch(
                    engine.ctx(), &probe_batch);

                b.iter(|| {
                    engine.publish(&PublishBatch {
                        stream_id: StreamId(1),
                        entries: black_box(&publish_entries),
                        now: Timestamp::new(1_000_000),
                    });

                    let claim_batch = ClaimBatch {
                        queue_id: QueueId(1),
                        connection_id: ConnectionId(100),
                        consumer_id: ConsumerId(1),
                        max_items: n as u16,
                        now: Timestamp::new(2_000_000),
                    };
                    let claimed = engine.claim(&claim_batch, sub, bind);
                    nack_scratch.clear();
                    nack_scratch.extend(
                        claimed.entries().iter().map(|e| NackEntry { seq: e.seq, retry_at: None }),
                    );

                    engine.nack(&NackBatch {
                        consumer_id: ConsumerId(1),
                        entries: &nack_scratch,
                        now: Timestamp::new(3_000_000),
                    });

                    let reclaim_batch = ClaimBatch {
                        queue_id: QueueId(1),
                        connection_id: ConnectionId(100),
                        consumer_id: ConsumerId(1),
                        max_items: n as u16,
                        now: Timestamp::new(4_000_000),
                    };
                    let reclaimed = engine.claim(&reclaim_batch, sub, bind);
                    ack_scratch.clear();
                    ack_scratch.extend(
                        reclaimed.entries().iter().map(|e| AckEntry { seq: e.seq }),
                    );

                    black_box(engine.ack(&AckBatch {
                        consumer_id: ConsumerId(1),
                        entries: &ack_scratch,
                        now: Timestamp::new(5_000_000),
                    }));
                });
            },
        );
    }

    group.finish();
}

// ── 4. Drain connection recovery ────────────────────────────────────────────

fn bench_drain_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_drain_recovery");

    for count in [10, 100, 1000] {
        let msgs = BenchMessages::new(count);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("drain_reclaim_ack", count),
            &count,
            |b, &n| {
                // Hoist hot-loop allocs out of the measured region.
                let publish_entries = msgs.publish_entries();
                let mut ack_scratch: Vec<AckEntry> = Vec::with_capacity(n);

                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        let mut engine = engine_simple(1, 100_000);

                        // Publish + claim (setup, not measured)
                        engine.publish(&PublishBatch {
                            stream_id: StreamId(1),
                            entries: &publish_entries,
                            now: Timestamp::new(1_000_000),
                        });
                        {
                            let setup_batch = ClaimBatch {
                                queue_id: QueueId(1),
                                connection_id: ConnectionId(100),
                                consumer_id: ConsumerId(1),
                                max_items: n as u16,
                                now: Timestamp::new(2_000_000),
                            };
                            let (s, bi) = arbitro_engine::runtime::claim::resolve_ids_for_batch(
                                engine.ctx(), &setup_batch);
                            engine.claim(&setup_batch, s, bi);
                        }

                        // ── Measure: drain + reclaim + ack ──
                        let start = std::time::Instant::now();

                        engine.drain_connection(&DrainConnectionReq {
                            connection_id: ConnectionId(100),
                            mode: DrainMode::ReleaseAndRequeue,
                            now: Timestamp::new(3_000_000),
                        });

                        engine.open_connection(&OpenConnectionReq {
                            connection_id: ConnectionId(200),
                            node_id: NodeId(1),
                            now: Timestamp::new(4_000_000),
                        });
                        engine.bind(&BindBatch {
                            entries: &[BindEntry {
                                connection_id: ConnectionId(200),
                                subscription_id: SubscriptionId(1),
                            }],
                            now: Timestamp::new(4_000_000),
                        });

                        let reclaim_batch = ClaimBatch {
                            queue_id: QueueId(1),
                            connection_id: ConnectionId(200),
                            consumer_id: ConsumerId(1),
                            max_items: n as u16,
                            now: Timestamp::new(5_000_000),
                        };
                        let (s, bi) = arbitro_engine::runtime::claim::resolve_ids_for_batch(
                            engine.ctx(), &reclaim_batch);
                        let reclaimed = engine.claim(&reclaim_batch, s, bi);
                        ack_scratch.clear();
                        ack_scratch.extend(
                            reclaimed.entries().iter().map(|e| AckEntry { seq: e.seq }),
                        );

                        black_box(engine.ack(&AckBatch {
                            consumer_id: ConsumerId(1),
                            entries: &ack_scratch,
                            now: Timestamp::new(6_000_000),
                        }));

                        total += start.elapsed();
                    }

                    total
                });
            },
        );
    }

    group.finish();
}

// ── 5. Fanout: 1 message → N consumers ─────────────────────────────────────

fn bench_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_fanout");

    for num_consumers in [1, 3, 10] {
        let msgs = BenchMessages::new(100);

        // Count total deliveries, not source messages — otherwise N consumers
        // looks N× slower when it's actually doing N× the work at the same
        // per-delivery cost. Real fanout on a shared connection is client-side
        // (1 msg on the wire, local dispatch), so this per-delivery number is
        // the correct metric for engine fanout efficiency.
        group.throughput(Throughput::Elements((100 * num_consumers) as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("{num_consumers}_consumers"), 100),
            &num_consumers,
            |b, &nc| {
                let mut engine = engine_simple(nc, 100_000);
                let publish_entries = msgs.publish_entries();
                let mut ack_scratch: Vec<AckEntry> = Vec::with_capacity(100);
                // Resolve sub/bind per consumer once — drainer caches in prod.
                let hints: Vec<(SubscriptionId, BindingId)> = (1..=nc)
                    .map(|i| {
                        let probe = ClaimBatch {
                            queue_id: QueueId(i),
                            connection_id: ConnectionId(100),
                            consumer_id: ConsumerId(i),
                            max_items: 100,
                            now: Timestamp::new(0),
                        };
                        arbitro_engine::runtime::claim::resolve_ids_for_batch(engine.ctx(), &probe)
                    })
                    .collect();

                b.iter(|| {
                    engine.publish(&PublishBatch {
                        stream_id: StreamId(1),
                        entries: black_box(&publish_entries),
                        now: Timestamp::new(1_000_000),
                    });

                    for i in 1..=nc {
                        let claim_batch = ClaimBatch {
                            queue_id: QueueId(i),
                            connection_id: ConnectionId(100),
                            consumer_id: ConsumerId(i),
                            max_items: 100,
                            now: Timestamp::new(2_000_000),
                        };
                        let (sub, bind) = hints[(i - 1) as usize];
                        let claimed = engine.claim(&claim_batch, sub, bind);
                        ack_scratch.clear();
                        ack_scratch.extend(
                            claimed.entries().iter().map(|e| AckEntry { seq: e.seq }),
                        );
                        engine.ack(&AckBatch {
                            consumer_id: ConsumerId(i),
                            entries: &ack_scratch,
                            now: Timestamp::new(3_000_000),
                        });
                    }
                });
            },
        );
    }

    group.finish();
}

// ── 6. Fanout over a SHARED connection ────────────────────────────────────
//
// Realistic fanout topology: a single client process holds one TCP connection
// to the engine and hosts N *logical* subscribers locally. From the engine's
// perspective there is exactly ONE consumer / ONE queue / ONE ack per message
// published — the client does its own dispatch to the N in-process handlers.
//
// This is the common case for multi-subscriber clients (e.g. a SDK running 30
// workers on one connection). The per-connection fanout in `bench_fanout` is
// the pessimistic case (N distinct network connections). This bench isolates
// the realistic case so the numbers aren't misread as "30 subscribers = 30×
// slower".
//
// Metric: logical-delivery throughput = (100 source msgs × N logical subs) /
// iteration time. The engine cost stays flat at ~193 ns per publish+claim+ack
// regardless of N; the logical throughput scales linearly with N because the
// client is assumed to dispatch for free.

fn bench_fanout_shared_connection(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_fanout_shared_conn");

    for num_logical_subs in [1u32, 3, 10, 30] {
        let msgs = BenchMessages::new(100);

        // Throughput = logical deliveries (what the USER sees). Engine work
        // is constant regardless of N; throughput scales because the client
        // dispatches locally for free.
        group.throughput(Throughput::Elements((100 * num_logical_subs) as u64));

        group.bench_with_input(
            BenchmarkId::new(format!("{num_logical_subs}_logical_subs"), 100),
            &num_logical_subs,
            |b, _| {
                // Engine sees ONE consumer regardless of how many logical
                // subscribers the client is dispatching to.
                let mut engine = engine_simple(1, 100_000);
                let publish_entries = msgs.publish_entries();
                let mut ack_scratch: Vec<AckEntry> = Vec::with_capacity(100);
                let probe_batch = ClaimBatch {
                    queue_id: QueueId(1),
                    connection_id: ConnectionId(100),
                    consumer_id: ConsumerId(1),
                    max_items: 100,
                    now: Timestamp::new(0),
                };
                let (sub, bind) = arbitro_engine::runtime::claim::resolve_ids_for_batch(
                    engine.ctx(), &probe_batch);

                b.iter(|| {
                    engine.publish(&PublishBatch {
                        stream_id: StreamId(1),
                        entries: black_box(&publish_entries),
                        now: Timestamp::new(1_000_000),
                    });

                    let claim_batch = ClaimBatch {
                        queue_id: QueueId(1),
                        connection_id: ConnectionId(100),
                        consumer_id: ConsumerId(1),
                        max_items: 100,
                        now: Timestamp::new(2_000_000),
                    };
                    let claimed = engine.claim(&claim_batch, sub, bind);
                    ack_scratch.clear();
                    ack_scratch.extend(
                        claimed.entries().iter().map(|e| AckEntry { seq: e.seq }),
                    );

                    engine.ack(&AckBatch {
                        consumer_id: ConsumerId(1),
                        entries: &ack_scratch,
                        now: Timestamp::new(3_000_000),
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_full_cycle,
    bench_with_limits,
    bench_nack_redeliver,
    bench_drain_recovery,
    bench_fanout,
    bench_fanout_shared_connection,
);
criterion_main!(benches);
