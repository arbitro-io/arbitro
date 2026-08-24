//! What the engine's operations actually cost.
//!
//! Every number here is a direct call into the engine — no broker, no
//! socket, no store. That is the honest label: these are catalog and
//! bookkeeping costs, NOT end-to-end throughput, and they must never be
//! quoted as messages per second.
//!
//! ## Why this exists
//!
//! Two decisions were being argued without a number.
//!
//! **Teardown.** Deleting a stream cascades: every consumer, every
//! subscription, every binding, every in-flight pending. Nothing measured
//! how that scales, so "deleting a stream is O(1) for the caller" was true
//! and useless — the caller's thread is the one paying the cascade.
//!
//! **The mirrors.** `SharedCounters` keeps its own copy of four facts the
//! engine already owns: per-consumer inflight, per-queue inflight,
//! per-stream demand, and paused. The mirror answers in one relaxed atomic
//! load. The question is what the engine charges for the same answer, and
//! whether the difference is worth two sources of truth that can drift.
//! `truth_vs_mirror` measures exactly that, against an atomic load taken
//! under the same harness so the comparison is not against a remembered
//! number.

use std::hint::black_box;
use std::sync::atomic::{AtomicU32, Ordering};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use arbitro_engine::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine::command::{AckEntry, Command, DeliveredEntry};
use arbitro_engine::types::*;
use arbitro_engine::ArbitroEngine;

/// One stream, `consumers` consumers, `subs_per_consumer` subscriptions
/// each, every subscription bound to its own connection.
///
/// Shaped like a real fan-out rather than a flat list: a consumer with ten
/// subscriptions is the case that makes teardown cascade, and a benchmark
/// built from one subscription per consumer would never exercise it.
struct World {
    engine: ArbitroEngine,
    stream: StreamId,
    consumers: Vec<ConsumerId>,
    bindings: Vec<BindingId>,
}

fn build(consumers: u32, subs_per_consumer: u32, pendings_per_binding: u32) -> World {
    let mut engine = ArbitroEngine::new();
    let stream = StreamId(1);

    engine
        .create_stream(StreamConfig {
            id: stream,
            name: b"bench".to_vec(),
        })
        .expect("create_stream");

    let mut consumer_ids = Vec::with_capacity(consumers as usize);
    let mut bindings = Vec::with_capacity((consumers * subs_per_consumer) as usize);
    let mut next_sub = 1u32;
    let mut next_conn = 1u64;

    for c in 0..consumers {
        let consumer = ConsumerId(c + 1);
        engine
            .create_consumer(ConsumerConfig {
                id: consumer,
                queue_id: QueueId(0),
                stream_id: stream,
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100_000,
                ack_wait_ms: 0,
                max_nack: 0,
                filter: Box::from(&b""[..]),
            })
            .expect("create_consumer");
        consumer_ids.push(consumer);

        for s in 0..subs_per_consumer {
            let sub = SubscriptionId(next_sub);
            next_sub += 1;
            engine
                .create_subscription(SubscriptionConfig {
                    id: sub,
                    external_id: s,
                    stream_id: stream,
                    consumer_id: consumer,
                    // Distinct exact subjects, not a catch-all: a catch-all
                    // collapses the match table to one entry and would make
                    // teardown look cheaper than any real deployment.
                    filters: vec![format!("bench.c{c}.s{s}").into_bytes()],
                })
                .expect("create_subscription");

            let conn = ConnectionId(next_conn);
            next_conn += 1;
            engine.open_connection(conn, NodeId(0));
            let (binding, _) = engine.subscribe(conn, sub);
            bindings.push(binding.expect("subscribe"));
        }
    }

    // In-flight pendings, so teardown pays for releasing them.
    if pendings_per_binding > 0 {
        let mut entries = Vec::with_capacity(pendings_per_binding as usize);
        for (i, &binding) in bindings.iter().enumerate() {
            entries.clear();
            let base = (i as u64) * 1_000_000;
            for k in 0..pendings_per_binding as u64 {
                entries.push(DeliveredEntry {
                    seq: base + k + 1,
                    subject_hash: (k as u32) | 1,
                    _pad: 0,
                });
            }
            let _ = engine.execute(&Command::Delivered {
                stream_id: stream,
                binding_id: binding,
                entries: &entries,
            });
        }
    }

    World {
        engine,
        stream,
        consumers: consumer_ids,
        bindings,
    }
}

// ── Teardown ────────────────────────────────────────────────────────────

/// The scenario that started this: how long does deleting one stream take
/// when it owns 20 consumers, 10 subscriptions each, and messages are in
/// flight.
///
/// Every iteration rebuilds the world, and the build is NOT timed — only
/// `delete_stream` is.
fn teardown(c: &mut Criterion) {
    let mut g = c.benchmark_group("teardown");
    g.sample_size(30);

    for &(consumers, subs, pendings) in &[
        (20u32, 10u32, 0u32),
        (20, 10, 10),
        (20, 10, 100),
        (100, 10, 10),
    ] {
        let bindings = consumers * subs;
        let label = format!("{consumers}c_x{subs}s_x{pendings}p");
        // One element per binding torn down, so the report reads as cost
        // per binding and the shapes stay comparable.
        g.throughput(Throughput::Elements(bindings as u64));

        g.bench_function(BenchmarkId::new("delete_stream", &label), |b| {
            b.iter_batched(
                || build(consumers, subs, pendings),
                |mut w| {
                    let _ = black_box(w.engine.delete_stream(w.stream));
                },
                criterion::BatchSize::LargeInput,
            )
        });

        g.bench_function(BenchmarkId::new("delete_every_consumer", &label), |b| {
            b.iter_batched(
                || build(consumers, subs, pendings),
                |mut w| {
                    for &id in &w.consumers {
                        let _ = black_box(w.engine.delete_consumer(id));
                    }
                },
                criterion::BatchSize::LargeInput,
            )
        });

        g.bench_function(BenchmarkId::new("unsubscribe_every_binding", &label), |b| {
            b.iter_batched(
                || build(consumers, subs, pendings),
                |mut w| {
                    for &id in &w.bindings {
                        let _ = black_box(w.engine.unsubscribe(id));
                    }
                },
                criterion::BatchSize::LargeInput,
            )
        });

        // A dead connection retires ONE binding. Measured per connection so
        // it is not mistaken for the whole-stream cascade above.
        g.bench_function(BenchmarkId::new("connection_dead_all", &label), |b| {
            b.iter_batched(
                || build(consumers, subs, pendings),
                |mut w| {
                    for i in 0..bindings as u64 {
                        let _ = black_box(w.engine.mark_connection_dead(ConnectionId(i + 1)));
                    }
                },
                criterion::BatchSize::LargeInput,
            )
        });
    }
    g.finish();
}

/// What building that world costs, so the teardown numbers have a scale to
/// sit against. A teardown that is slower than its own setup is a finding;
/// without this it is a number with nothing to compare to.
fn setup_cost(c: &mut Criterion) {
    let mut g = c.benchmark_group("build");
    g.sample_size(30);
    for &(consumers, subs) in &[(20u32, 10u32), (100, 10)] {
        g.throughput(Throughput::Elements((consumers * subs) as u64));
        g.bench_function(BenchmarkId::new("catalog", format!("{consumers}c_x{subs}s")), |b| {
            b.iter(|| black_box(build(consumers, subs, 0)))
        });
    }
    g.finish();
}

// ── Hot path ────────────────────────────────────────────────────────────

/// Delivery, ack and nack bookkeeping, per entry.
///
/// Batch sizes matter more than the totals: the per-entry cost at 1 and at
/// 256 is what says whether batching buys anything, and a single average
/// hides exactly that.
fn hot_path(c: &mut Criterion) {
    let mut g = c.benchmark_group("hot");

    for &batch in &[1usize, 16, 256] {
        g.throughput(Throughput::Elements(batch as u64));

        // Steady state, NOT a growing map. An earlier version pushed new
        // seqs forever inside `iter`, so the binding's pending map grew
        // across millions of iterations and the benchmark measured
        // rehashing. It showed as `delivered/256` (12.7 us) being SLOWER
        // than deliver-and-ack of the same 256 (4.0 us), which is
        // impossible and is the tell.
        //
        // The vec is built once: allocating it inside the timed loop would
        // charge the allocator to the engine.
        g.bench_function(BenchmarkId::new("delivered", batch), |b| {
            let mut w = build(1, 1, 0);
            let binding = w.bindings[0];
            let stream = w.stream;
            let entries: Vec<DeliveredEntry> = (0..batch)
                .map(|i| DeliveredEntry {
                    seq: i as u64 + 1,
                    subject_hash: (i as u32) | 1,
                    _pad: 0,
                })
                .collect();
            let settle: Vec<AckEntry> = (0..batch)
                .map(|i| AckEntry {
                    stream_id: stream,
                    seq: i as u64 + 1,
                    sub_id: 0,
                })
                .collect();
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let t = std::time::Instant::now();
                    let _ = black_box(w.engine.execute(&Command::Delivered {
                        stream_id: stream,
                        binding_id: binding,
                        entries: &entries,
                    }));
                    total += t.elapsed();
                    // Untimed: returns the map to empty so the next
                    // iteration inserts into the same shape as this one.
                    let _ = w.engine.execute(&Command::Ack {
                        conn_id: ConnectionId(1),
                        consumer_id: ConsumerId(1),
                        entries: &settle,
                    });
                }
                total
            })
        });

        // Ack and nack need their pendings to exist, so deliver and settle
        // are measured as ONE operation against a world built once.
        //
        // Not `iter_batched` with a fresh world per iteration: that charges
        // the world's `Drop` to the ack. It showed as a flat ~2 us on every
        // batch size — at batch 1 that was 26x the whole delivery, which is
        // the shape of a measurement artefact, not of an ack.
        //
        // Subtract `delivered/N` from these to get the settle cost alone.
        for (name, nack) in [("deliver_then_ack", false), ("deliver_then_nack", true)] {
            g.bench_function(BenchmarkId::new(name, batch), |b| {
                let mut w = build(1, 1, 0);
                let binding = w.bindings[0];
                let stream = w.stream;
                let delivered: Vec<DeliveredEntry> = (0..batch)
                    .map(|i| DeliveredEntry {
                        seq: i as u64 + 1,
                        subject_hash: (i as u32) | 1,
                        _pad: 0,
                    })
                    .collect();
                let settle: Vec<AckEntry> = (0..batch)
                    .map(|i| AckEntry {
                        stream_id: stream,
                        seq: i as u64 + 1,
                        sub_id: 0,
                    })
                    .collect();
                b.iter(|| {
                    let _ = black_box(w.engine.execute(&Command::Delivered {
                        stream_id: stream,
                        binding_id: binding,
                        entries: &delivered,
                    }));
                    let cmd = if nack {
                        Command::Nack {
                            conn_id: ConnectionId(1),
                            consumer_id: ConsumerId(1),
                            entries: &settle,
                        }
                    } else {
                        Command::Ack {
                            conn_id: ConnectionId(1),
                            consumer_id: ConsumerId(1),
                            entries: &settle,
                        }
                    };
                    let _ = black_box(w.engine.execute(&cmd));
                })
            });
        }
    }
    g.finish();
}

// ── The mirrors ─────────────────────────────────────────────────────────

/// The four questions `SharedCounters` answers with its own copy, asked of
/// the engine instead — plus the atomic load the mirror costs, measured
/// here so the comparison is against a number from this machine and not a
/// remembered one.
///
/// Catalog size is varied because a mirror's cost is flat and a walk's is
/// not. If the engine stays flat too, the mirror buys nothing.
fn truth_vs_mirror(c: &mut Criterion) {
    let mut g = c.benchmark_group("truth_vs_mirror");

    let mirror = AtomicU32::new(7);
    g.bench_function("mirror/atomic_load", |b| {
        b.iter(|| black_box(mirror.load(Ordering::Relaxed)))
    });

    // What the drain actually does: ask ONCE PER BINDING while it walks.
    // There is no aggregate "is any consumer free" call, so the per-call
    // number only matters multiplied by the walk.
    for &(consumers, subs) in &[(20u32, 10u32), (100, 10)] {
        let w = build(consumers, subs, 10);
        let bindings = (consumers * subs) as u64;
        let label = format!("{consumers}c_x{subs}s");
        let mirrors: Vec<AtomicU32> = (0..bindings).map(|_| AtomicU32::new(7)).collect();

        g.throughput(Throughput::Elements(bindings));
        g.bench_function(BenchmarkId::new("walk/mirror", &label), |b| {
            b.iter(|| {
                let mut free = 0u32;
                for m in &mirrors {
                    free += (black_box(m.load(Ordering::Relaxed)) < 1000) as u32;
                }
                black_box(free)
            })
        });
        g.bench_function(BenchmarkId::new("walk/engine", &label), |b| {
            b.iter(|| {
                let mut free = 0u32;
                for &c in &w.consumers {
                    for _ in 0..subs {
                        free += black_box(w.engine.consumer_has_capacity(c, 1000)) as u32;
                    }
                }
                black_box(free)
            })
        });
        g.throughput(Throughput::Elements(1));
    }

    for &(consumers, subs) in &[(1u32, 1u32), (20, 10), (100, 10)] {
        let w = build(consumers, subs, 10);
        let label = format!("{consumers}c_x{subs}s");

        g.bench_function(BenchmarkId::new("engine/has_any_demand", &label), |b| {
            b.iter(|| black_box(w.engine.has_any_demand()))
        });
        g.bench_function(BenchmarkId::new("engine/has_demand", &label), |b| {
            b.iter(|| black_box(w.engine.has_demand(w.stream)))
        });
        g.bench_function(BenchmarkId::new("engine/consumer_inflight", &label), |b| {
            b.iter(|| black_box(w.engine.consumer_inflight(w.consumers[0])))
        });
        g.bench_function(
            BenchmarkId::new("engine/consumer_has_capacity", &label),
            |b| b.iter(|| black_box(w.engine.consumer_has_capacity(w.consumers[0], 1000))),
        );
    }
    g.finish();
}

// ── Lookups the teardown walks ──────────────────────────────────────────

/// What it costs to ask a stream for what it owns.
///
/// Both used to be full scans -- `consumers_for_stream` over every
/// consumer, `subscriptions_for_consumer` over every subscription -- which
/// is what made the cascade quadratic. Measured at two sizes so a scan
/// cannot hide: an index is flat per element, a scan is not.
///
/// `bindings_for_stream` returns a borrowed slice and allocates nothing;
/// the other two clone their index because the caller mutates while it
/// iterates. That clone is the honest remaining cost and it is measured
/// here rather than assumed away.
fn ownership_lookups(c: &mut Criterion) {
    let mut g = c.benchmark_group("lookup");
    for &(consumers, subs) in &[(20u32, 10u32), (100, 10)] {
        let w = build(consumers, subs, 0);
        let label = format!("{consumers}c_x{subs}s");
        let consumer0 = w.consumers[0];

        g.bench_function(BenchmarkId::new("bindings_for_stream", &label), |b| {
            b.iter(|| black_box(w.engine.ctx().catalog.bindings_for_stream(w.stream).len()))
        });
        g.bench_function(BenchmarkId::new("consumers_for_stream", &label), |b| {
            b.iter(|| black_box(w.engine.ctx().catalog.consumers_for_stream(w.stream)))
        });
        g.bench_function(BenchmarkId::new("subscriptions_for_consumer", &label), |b| {
            b.iter(|| {
                black_box(
                    w.engine
                        .ctx()
                        .catalog
                        .subscriptions_for_consumer(consumer0),
                )
            })
        });
        g.bench_function(BenchmarkId::new("bindings_for_consumer", &label), |b| {
            b.iter(|| {
                black_box(
                    w.engine
                        .ctx()
                        .catalog
                        .bindings_for_consumer(consumer0)
                        .len(),
                )
            })
        });
    }
    g.finish();
}

// ── Cold admin ──────────────────────────────────────────────────────────

/// The list and snapshot paths. Cold, but they allocate per call and are
/// reached from health checks and dashboards, so "cold" is a claim worth
/// a number rather than an assumption.
fn admin(c: &mut Criterion) {
    let mut g = c.benchmark_group("admin");
    g.sample_size(30);
    for &(consumers, subs) in &[(20u32, 10u32), (100, 10)] {
        let w = build(consumers, subs, 10);
        let label = format!("{consumers}c_x{subs}s");
        g.bench_function(BenchmarkId::new("list_consumers", &label), |b| {
            b.iter(|| black_box(w.engine.list_consumers()))
        });
        g.bench_function(BenchmarkId::new("consumer_states_snapshot", &label), |b| {
            b.iter(|| black_box(w.engine.consumer_states_snapshot()))
        });
        g.bench_function(BenchmarkId::new("pause_resume", &label), |b| {
            let mut e = build(consumers, subs, 0);
            let id = e.consumers[0];
            b.iter(|| {
                black_box(e.engine.pause_consumer(id));
                black_box(e.engine.resume_consumer(id));
            })
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    teardown,
    setup_cost,
    hot_path,
    truth_vs_mirror,
    ownership_lookups,
    admin
);
criterion_main!(benches);
