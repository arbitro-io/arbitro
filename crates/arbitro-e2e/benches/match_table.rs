//! Benchmark: match table lookup — ns/match for 1/10/100 consumers.
//!
//! Measures precomputed match table lookup at publish time.
//! Also measures exact vs catch-all vs pattern matching.

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, black_box};
use arbitro_engine_v2::catalog::match_table::{MatchTable, MatchEntry};
use arbitro_engine_v2::catalog::fnv1a_32;
use arbitro_engine_v2::types::*;

fn build_table_exact(num_consumers: u32) -> MatchTable {
    let mut mt = MatchTable::default();
    let subject_hash = fnv1a_32(b"orders.created");

    for i in 0..num_consumers {
        mt.add_exact(subject_hash, MatchEntry {
            consumer_id: ConsumerId(i + 1),
            queue_id: QueueId(i + 1),
            subscription_id: SubscriptionId(i + 1),
            connection_id: ConnectionId(100),
        });
    }

    mt
}

fn build_table_catch_all(num_consumers: u32) -> MatchTable {
    let mut mt = MatchTable::default();

    for i in 0..num_consumers {
        mt.add_catch_all(MatchEntry {
            consumer_id: ConsumerId(i + 1),
            queue_id: QueueId(i + 1),
            subscription_id: SubscriptionId(i + 1),
            connection_id: ConnectionId(100),
        });
    }

    mt
}

fn build_table_pattern(num_consumers: u32) -> MatchTable {
    let mut mt = MatchTable::default();

    for i in 0..num_consumers {
        mt.add_pattern(format!("orders.item_{i}.*").into_bytes(), MatchEntry {
            consumer_id: ConsumerId(i + 1),
            queue_id: QueueId(i + 1),
            subscription_id: SubscriptionId(i + 1),
            connection_id: ConnectionId(100),
        });
    }

    // Resolve a subject so it's cached
    let subject = b"orders.item_0.created";
    mt.resolve_patterns(fnv1a_32(subject), subject);

    mt
}

fn bench_match_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("match_table_lookup");

    for num_consumers in [1, 3, 10, 100] {
        // Exact match
        let table = build_table_exact(num_consumers);
        let hash = fnv1a_32(b"orders.created");

        group.bench_with_input(
            BenchmarkId::new("exact", num_consumers),
            &num_consumers,
            |b, _| {
                b.iter(|| {
                    let result = table.lookup(black_box(hash));
                    black_box(result.count());
                });
            },
        );

        // Catch-all
        let table = build_table_catch_all(num_consumers);

        group.bench_with_input(
            BenchmarkId::new("catch_all", num_consumers),
            &num_consumers,
            |b, _| {
                b.iter(|| {
                    let result = table.lookup(black_box(0xBEEF));
                    black_box(result.count());
                });
            },
        );
    }

    group.finish();
}

fn bench_pattern_resolve(c: &mut Criterion) {
    let mut group = c.benchmark_group("match_table_pattern");

    for num_patterns in [1, 10, 100] {
        group.bench_with_input(
            BenchmarkId::new("resolve_uncached", num_patterns),
            &num_patterns,
            |b, &n| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        let mut mt = MatchTable::default();
                        for i in 0..n {
                            mt.add_pattern(
                                format!("orders.item_{i}.*").into_bytes(),
                                MatchEntry {
                                    consumer_id: ConsumerId(i as u32 + 1),
                                    queue_id: QueueId(i as u32 + 1),
                                    subscription_id: SubscriptionId(i as u32 + 1),
                                    connection_id: ConnectionId(100),
                                },
                            );
                        }

                        let subject = b"orders.item_0.created";
                        let hash = fnv1a_32(subject);

                        let start = std::time::Instant::now();
                        mt.resolve_patterns(black_box(hash), black_box(subject));
                        total += start.elapsed();
                    }

                    total
                });
            },
        );
    }

    group.finish();
}

fn bench_fnv1a(c: &mut Criterion) {
    let mut group = c.benchmark_group("fnv1a_hash");

    for subject in [
        "a.b",
        "orders.created",
        "message.meta.user.12345",
        "very.long.subject.with.many.tokens.for.routing.purposes",
    ] {
        group.bench_with_input(
            BenchmarkId::new("hash", subject.len()),
            subject.as_bytes(),
            |b, s| {
                b.iter(|| {
                    black_box(fnv1a_32(black_box(s)));
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_match_lookup, bench_pattern_resolve, bench_fnv1a);
criterion_main!(benches);
