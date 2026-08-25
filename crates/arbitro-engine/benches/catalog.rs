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
/// Both forms return a borrowed slice and allocate nothing. The cloning
/// forms are gone: their only caller was the cascade, which now TAKES the
/// list it is about to destroy.
fn ownership_lookups(c: &mut Criterion) {
    let mut g = c.benchmark_group("lookup");
    for &(consumers, subs) in &[(20u32, 10u32), (100, 10)] {
        let w = build(consumers, subs, 0);
        let label = format!("{consumers}c_x{subs}s");
        let consumer0 = w.consumers[0];

        g.bench_function(BenchmarkId::new("bindings_for_stream", &label), |b| {
            b.iter(|| black_box(w.engine.ctx().catalog.bindings_for_stream(w.stream).len()))
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

// ── The delivery lookup ─────────────────────────────────────────────────

/// What the drain asks per message: given a stream and a subject, who
/// gets it -- and then walking the answer, which is what it does next.
///
/// Shaped like a real deployment: 8 consumers, 30 subscriptions each, 240
/// subscriptions on one stream. Only ONE subscription per consumer matches
/// any given subject, so a lookup returns 8 entries out of 240.
///
/// The `inherited` variant is the case where subscriptions declare no
/// filter of their own and inherit the consumer's. `transport::rules`
/// resolves that above the engine, and an EMPTY filter reaches the match
/// table as a catch-all -- which is appended to EVERY lookup, matched or
/// not. That is the shape that stops being free.
fn delivery_lookup(c: &mut Criterion) {
    use arbitro_engine::common::wire_hash_32;

    const CONSUMERS: u32 = 8;
    const SUBS_EACH: u32 = 30;
    const SHORT: &[u8] = b"orders.eu.west.created";
    const UUID: &[u8] =
        b"orders.eu-west-1.tenant-4f3a9c22-8b1e-4d7a-9c3f-2e6b8a1d5f04.created";

    /// `inherited` = how many of each consumer's subscriptions carry no
    /// filter and therefore land in `catch_all`.
    fn world(subject: &[u8], inherited: u32) -> (ArbitroEngine, StreamId) {
        let mut engine = ArbitroEngine::new();
        let stream = StreamId(1);
        engine
            .create_stream(StreamConfig {
                id: stream,
                name: b"bench".to_vec(),
            })
            .unwrap();
        let mut next = 1u32;
        for c in 0..CONSUMERS {
            engine
                .create_consumer(ConsumerConfig {
                    id: ConsumerId(c + 1),
                    queue_id: QueueId(0),
                    stream_id: stream,
                    durable: true,
                    ack_policy: AckPolicy::Explicit,
                    max_inflight: 100_000,
                    ack_wait_ms: 0,
                    max_nack: 0,
                    filter: Box::from(&b""[..]),
                })
                .unwrap();
            for k in 0..SUBS_EACH {
                // One subscription per consumer matches the subject; some
                // inherit (empty filter -> catch-all); the rest are exact
                // filters on other subjects.
                let filters = if k == 0 {
                    vec![subject.to_vec()]
                } else if k <= inherited {
                    Vec::new()
                } else {
                    vec![format!("other.c{c}.sub{k}").into_bytes()]
                };
                engine
                    .create_subscription(SubscriptionConfig {
                        id: SubscriptionId(next),
                        external_id: k,
                        stream_id: stream,
                        consumer_id: ConsumerId(c + 1),
                        filters,
                    })
                    .unwrap();
                engine.open_connection(ConnectionId(next as u64), NodeId(0));
                let _ = engine.subscribe(ConnectionId(next as u64), SubscriptionId(next));
                next += 1;
            }
        }
        (engine, stream)
    }

    let mut g = c.benchmark_group("lookup_subject");
    for (sname, subject) in [("short_22B", SHORT), ("uuid_68B", UUID)] {
        for &inherited in &[0u32, 1, 29] {
            let (engine, stream) = world(subject, inherited);
            let mt = engine.ctx().catalog.match_table(stream).unwrap();
            let label = format!("{sname}/{CONSUMERS}c_x{SUBS_EACH}s_inh{inherited}");

            // Report how many entries come back, so the timings are read
            // against a known answer rather than a guessed one.
            let r = mt.lookup_verified(wire_hash_32(subject), subject);
            println!(
                "  [{label}] exact={} catch_all={}",
                r.exact.len(),
                r.catch_all.len()
            );

            g.bench_function(BenchmarkId::new("hash_and_lookup", &label), |b| {
                b.iter(|| {
                    let h = wire_hash_32(black_box(subject));
                    let r = mt.lookup_verified(h, black_box(subject));
                    black_box(r.exact.len() + r.catch_all.len())
                })
            });

            // Lookup AND walk every recipient -- what dispatch does next.
            g.bench_function(BenchmarkId::new("lookup_and_walk", &label), |b| {
                b.iter(|| {
                    let h = wire_hash_32(black_box(subject));
                    let r = mt.lookup_verified(h, black_box(subject));
                    let mut acc = 0u64;
                    for e in r.exact.iter().chain(r.catch_all.iter()) {
                        acc += e.consumer_id.raw() as u64 + e.binding_idx as u64;
                    }
                    black_box(acc)
                })
            });
        }
    }
    g.finish();
}

// ── The worst case: wildcards at scale ──────────────────────────────────

/// 20 consumers x 15 subscriptions = 300 on one stream, a third of them
/// carrying wildcards.
///
/// The earlier lookup numbers used ONLY literal filters, which is the best
/// case: a literal lands in `exact` and costs one probe. A wildcard cannot
/// be precomputed -- the subject is not known until the message arrives --
/// so it goes to the pattern trie and is walked per subject, then cached
/// by the caller.
///
/// Three costs are separated here, because they are paid at different
/// rates:
///  * `exact` -- the probe, paid on every message
///  * `patterns_cold` -- the trie walk, paid ONCE per distinct subject
///  * `patterns_warm` -- what a cached resolve costs (a Vec copy)
///
/// The `>` in the mix matters: it matches one or more trailing tokens, so
/// it fires for every subject under its prefix and its entries end up in
/// every result.
fn wildcards_at_scale(c: &mut Criterion) {
    use arbitro_engine::catalog::match_table::MatchEntry;
    use arbitro_engine::common::wire_hash_32;

    const CONSUMERS: u32 = 20;
    const SUBS_EACH: u32 = 15;
    const WILDCARDS_EACH: u32 = 5;
    const SUBJECT: &[u8] = b"orders.eu-west-1.tenant-4f3a9c22-8b1e-4d7a-9c3f-2e6b8a1d5f04.created";

    let mut engine = ArbitroEngine::new();
    let stream = StreamId(1);
    engine
        .create_stream(StreamConfig {
            id: stream,
            name: b"bench".to_vec(),
        })
        .unwrap();

    let mut next = 1u32;
    for c in 0..CONSUMERS {
        engine
            .create_consumer(ConsumerConfig {
                id: ConsumerId(c + 1),
                queue_id: QueueId(0),
                stream_id: stream,
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100_000,
                ack_wait_ms: 0,
                max_nack: 0,
                filter: Box::from(&b""[..]),
            })
            .unwrap();
        for k in 0..SUBS_EACH {
            let filters = if k == 0 {
                // One literal that matches the subject exactly.
                vec![SUBJECT.to_vec()]
            } else if k <= WILDCARDS_EACH {
                // Wildcards. Some match the subject, some do not -- a real
                // stream has both, and a trie walk visits candidates either
                // way.
                match k % 5 {
                    1 => vec![b"orders.*.*.created".to_vec()],
                    2 => vec![b"orders.eu-west-1.>".to_vec()],
                    3 => vec![b"orders.>".to_vec()],
                    4 => vec![format!("billing.*.tenant-{c}.>").into_bytes()],
                    _ => vec![b"events.*.>".to_vec()],
                }
            } else {
                vec![format!("other.c{c}.sub{k}").into_bytes()]
            };
            engine
                .create_subscription(SubscriptionConfig {
                    id: SubscriptionId(next),
                    external_id: k,
                    stream_id: stream,
                    consumer_id: ConsumerId(c + 1),
                    filters,
                })
                .unwrap();
            engine.open_connection(ConnectionId(next as u64), NodeId(0));
            let _ = engine.subscribe(ConnectionId(next as u64), SubscriptionId(next));
            next += 1;
        }
    }

    let mt = engine.ctx().catalog.match_table(stream).unwrap();
    let h = wire_hash_32(SUBJECT);
    let r = mt.lookup_verified(h, SUBJECT);
    let mut scratch: Vec<MatchEntry> = Vec::new();
    mt.resolve_patterns_readonly(h, SUBJECT, &mut scratch);
    println!(
        "  [{}c x {}s, {} wildcard each] exact={} catch_all={} from_patterns={}",
        CONSUMERS,
        SUBS_EACH,
        WILDCARDS_EACH,
        r.exact.len(),
        r.catch_all.len(),
        scratch.len()
    );

    // Does the dedup find anything at all?
    {
        let mut raw: Vec<MatchEntry> = Vec::new();
        mt.walk_patterns(SUBJECT, |e| raw.push(*e));
        let mut deduped: Vec<MatchEntry> = Vec::new();
        mt.resolve_patterns_readonly(h, SUBJECT, &mut deduped);
        let mut subs: Vec<u32> = raw.iter().map(|e| e.subscription_id.raw()).collect();
        subs.sort_unstable();
        let distinct_subs = {
            let mut d = subs.clone();
            d.dedup();
            d.len()
        };
        println!(
            "  [dedup check] walked={} after_dedup={} distinct_subscriptions={} -> duplicates removed={}",
            raw.len(),
            deduped.len(),
            distinct_subs,
            raw.len() - deduped.len()
        );
    }

    let mut g = c.benchmark_group("wildcards");

    g.bench_function("exact_lookup", |b| {
        b.iter(|| {
            let h = wire_hash_32(black_box(SUBJECT));
            let r = mt.lookup_verified(h, black_box(SUBJECT));
            black_box(r.exact.len() + r.catch_all.len())
        })
    });

    // Cold: the trie walk, paid once per distinct subject.
    g.bench_function("patterns_cold", |b| {
        let mut out: Vec<MatchEntry> = Vec::with_capacity(64);
        b.iter(|| {
            out.clear();
            mt.resolve_patterns_readonly(black_box(h), black_box(SUBJECT), &mut out);
            black_box(out.len())
        })
    });

    // The SAME resolve, but with `out` pre-filled past DEDUP_THRESHOLD so
    // the HashSet branch is taken instead of the linear one.
    //
    // The threshold tests the INPUT length, and the drain clears its
    // buffer before every call -- so the linear branch always wins the
    // decision no matter how many entries come out. With 60 results that
    // is ~1830 `contains` comparisons.
    g.bench_function("patterns_cold_hashset_branch", |b| {
        let mut out: Vec<MatchEntry> = Vec::with_capacity(128);
        // Four entries that cannot match anything real, only there to trip
        // the threshold.
        let filler: Vec<MatchEntry> = (0..4)
            .map(|i| MatchEntry {
                consumer_id: ConsumerId(9000 + i),
                queue_id: QueueId(0),
                subscription_id: SubscriptionId(9000 + i),
                connection_id: ConnectionId(9000 + i as u64),
                binding_idx: 0,
            })
            .collect();
        b.iter(|| {
            out.clear();
            out.extend_from_slice(&filler);
            mt.resolve_patterns_readonly(black_box(h), black_box(SUBJECT), &mut out);
            black_box(out.len())
        })
    });

    // ── The dedup, four ways, over the SAME walk ────────────────────
    //
    // `walk_patterns` hands every match to a closure with no dedup at all,
    // so what changes between these rows is only how duplicates are
    // settled. The walk itself was measured at 46 ns; everything above
    // that is the dedup.

    // No dedup at all -- the floor. NOT correct on its own: a subscription
    // reachable through two patterns lands twice.
    g.bench_function("dedup/none", |b| {
        let mut out: Vec<MatchEntry> = Vec::with_capacity(128);
        b.iter(|| {
            out.clear();
            mt.walk_patterns(black_box(SUBJECT), |e| out.push(*e));
            black_box(out.len())
        })
    });

    // What the engine does today, reached through the raw walk so the
    // comparison is like for like.
    g.bench_function("dedup/linear_matchentry", |b| {
        let mut out: Vec<MatchEntry> = Vec::with_capacity(128);
        b.iter(|| {
            out.clear();
            mt.walk_patterns(black_box(SUBJECT), |e| {
                if !out.contains(e) {
                    out.push(*e);
                }
            });
            black_box(out.len())
        })
    });

    // Same shape, but comparing the 4-byte subscription id instead of the
    // whole 24-byte entry. A subscription cannot legitimately appear twice.
    g.bench_function("dedup/linear_sub_id", |b| {
        let mut out: Vec<MatchEntry> = Vec::with_capacity(128);
        let mut seen: Vec<u32> = Vec::with_capacity(128);
        b.iter(|| {
            out.clear();
            seen.clear();
            mt.walk_patterns(black_box(SUBJECT), |e| {
                let id = e.subscription_id.raw();
                if !seen.contains(&id) {
                    seen.push(id);
                    out.push(*e);
                }
            });
            black_box(out.len())
        })
    });

    // A visited bitmap over pattern-entry indices. Reused across calls and
    // cleared by touching only the words that were set, so the cost does
    // not grow with the table -- only with the number of hits.
    g.bench_function("dedup/bitmap", |b| {
        let words = mt.pattern_count().div_ceil(64).max(1);
        let mut seen: Vec<u64> = vec![0; words];
        let mut touched: Vec<usize> = Vec::with_capacity(64);
        let mut out: Vec<MatchEntry> = Vec::with_capacity(128);
        b.iter(|| {
            out.clear();
            for &w in touched.iter() {
                seen[w] = 0;
            }
            touched.clear();
            mt.walk_patterns_indexed(black_box(SUBJECT), |idx, e| {
                let (w, bit) = (idx as usize / 64, 1u64 << (idx as usize % 64));
                if seen[w] & bit == 0 {
                    if seen[w] == 0 {
                        touched.push(w);
                    }
                    seen[w] |= bit;
                    out.push(*e);
                }
            });
            black_box(out.len())
        })
    });

    // Warm: what the drain actually pays after the first message -- copying
    // the cached answer into its scratch.
    let cached = scratch.clone();
    g.bench_function("patterns_warm_copy", |b| {
        let mut out: Vec<MatchEntry> = Vec::with_capacity(64);
        b.iter(|| {
            out.clear();
            out.extend_from_slice(black_box(&cached));
            black_box(out.len())
        })
    });

    // Everything a message pays on its FIRST appearance.
    g.bench_function("first_message_total", |b| {
        let mut out: Vec<MatchEntry> = Vec::with_capacity(64);
        b.iter(|| {
            let h = wire_hash_32(black_box(SUBJECT));
            let r = mt.lookup_verified(h, black_box(SUBJECT));
            out.clear();
            mt.resolve_patterns_readonly(h, black_box(SUBJECT), &mut out);
            let mut acc = 0u64;
            for e in r.exact.iter().chain(r.catch_all.iter()).chain(out.iter()) {
                acc += e.consumer_id.raw() as u64 + e.binding_idx as u64;
            }
            black_box(acc)
        })
    });
    g.finish();
}

// ── Byte trie vs hash trie ──────────────────────────────────────────────

/// The two tries walking the SAME patterns and the SAME subject, with a
/// closure that only counts.
///
/// No deduplication, no cache, no `Vec` to fill -- just the walk, so the
/// only difference is how a level decides which child to descend into:
/// comparing segment BYTES through a `Box<[u8]>`, or comparing a `u64`
/// sitting next to the child index.
///
/// Three hash walks are measured because two questions are open:
///  * fused vs two-pass -- does folding the hash into the scan pay?
///  * FNV vs foldhash -- one byte per multiply, or eight?
fn trie_shootout(c: &mut Criterion) {
    use arbitro_engine::common::hash_trie::HashTrie;
    use arbitro_engine::common::SubjectTrie;

    const SHORT: &[u8] = b"orders.eu.west.created";
    const UUID: &[u8] = b"orders.eu-west-1.tenant-4f3a9c22-8b1e-4d7a-9c3f-2e6b8a1d5f04.created";
    const MISS: &[u8] = b"billing.eu.west.created";

    /// `width` distinct literal siblings per level, plus the wildcards a
    /// real stream carries. The subject's own path is inserted LAST, which
    /// is the worst position for a linear scan of children.
    fn patterns(width: u32, subject: &[u8]) -> Vec<Vec<u8>> {
        let mut v = Vec::new();
        for k in 0..width {
            v.push(format!("other{k}.a.b.c").into_bytes());
        }
        v.push(b"orders.>".to_vec());
        v.push(b"orders.*.*.created".to_vec());
        v.push(b"orders.eu-west-1.>".to_vec());
        v.push(subject.to_vec());
        v
    }

    let mut g = c.benchmark_group("trie");
    for (sname, subject) in [("short_22B", SHORT), ("uuid_68B", UUID), ("miss_at_lvl1", MISS)] {
        for &width in &[4u32, 32] {
            let pats = patterns(width, if sname == "miss_at_lvl1" { SHORT } else { subject });

            let mut bytes_trie = SubjectTrie::new();
            let mut hash_trie = HashTrie::new(0x51ed_5eed_51ed_5eed);
            for (i, p) in pats.iter().enumerate() {
                bytes_trie.insert(p, i as u32);
                hash_trie.insert(p, i as u32);
            }

            // Every walk must agree, or the comparison is meaningless.
            let count = |f: &dyn Fn(&mut dyn FnMut(u32))| {
                let mut n = 0u32;
                f(&mut |_| n += 1);
                n
            };
            let a = count(&|cb| bytes_trie.find_matches(subject, cb));
            let b_ = count(&|cb| hash_trie.find_matches(subject, cb));
            assert_eq!(a, b_, "walks disagree on {sname}/w{width}");
            println!("  [{sname}/w{width}] hits={a} nodes bytes={} hash={}",
                bytes_trie.node_count(), hash_trie.node_count());

            let label = format!("{sname}/w{width}");
            g.bench_function(BenchmarkId::new("bytes", &label), |b| {
                b.iter(|| {
                    let mut n = 0u32;
                    bytes_trie.find_matches(black_box(subject), |_| n += 1);
                    black_box(n)
                })
            });
            g.bench_function(BenchmarkId::new("hash_level_sync", &label), |b| {
                b.iter(|| {
                    let mut n = 0u32;
                    hash_trie.find_matches(black_box(subject), |_| n += 1);
                    black_box(n)
                })
            });
        }
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
    delivery_lookup,
    wildcards_at_scale,
    trie_shootout,
    admin
);
criterion_main!(benches);
