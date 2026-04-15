//! Benchmark: drain_connection — ns/total for 1/10/100/1000 pending.
//!
//! Measures the drain protocol: take edges + release_pending per entry.
//! This is the O(k) replacement for the old O(S×C) scan.

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, black_box};
use arbitro_engine::context::EngineContext;
use arbitro_engine::graph::node::{pending_edge_idx, PendingNode};
use arbitro_engine::types::*;
use arbitro_engine::batch::DrainConnectionReq;
use arbitro_engine::runtime::drain::drain_connection;

fn setup_ctx() -> EngineContext {
    EngineContext::new()
}

/// Insert N pending entries for a given connection.
fn insert_pending_batch(ctx: &mut EngineContext, conn_id: ConnectionId, count: usize) {
    let consumer = ConsumerId(10);
    let queue = QueueId(20);

    for i in 0..count {
        let pending = PendingNode {
            pending_id: PendingId(i as u32),
            seq: i as u64 + 1,
            queue_id: queue,
            consumer_id: consumer,
            subscription_id: SubscriptionId(1),
            binding_id: BindingId(1),
            connection_id: conn_id,
            subject_hash: (i as u32) ^ 0xBEEF,
            credits: [CreditEntry {
                scope: CreditScope::Node,
                _pad: [0; 3],
                counter_idx: 0,
            }; 3],
            credit_count: 0,
            deadline_id: 0,
            delivered_at: Timestamp::new(0),
            ack_wait_ns: 0,
            edge_prev: [SlabKey::DANGLING; pending_edge_idx::COUNT],
            edge_next: [SlabKey::DANGLING; pending_edge_idx::COUNT],
        };

        let key = ctx.graph.insert_pending(pending);

        ctx.edges.pending_by_connection.insert_head(&mut ctx.graph.pending, conn_id, key);
        ctx.edges.pending_by_consumer.insert_head(&mut ctx.graph.pending, consumer, key);
        ctx.edges.pending_by_queue.insert_head(&mut ctx.graph.pending, queue, key);
        ctx.edges.pending_by_subscription.insert_head(&mut ctx.graph.pending, SubscriptionId(1), key);
        ctx.edges.pending_by_consumer_seq
            .insert(consumer, i as u64 + 1, key);

        ctx.inflight.inc_pending(
            (i as u32) ^ 0xBEEF,
            consumer.raw(),
            queue.raw(),
        );
    }
}

fn bench_drain_connection(c: &mut Criterion) {
    let mut group = c.benchmark_group("drain_connection");

    for pending_count in [1, 10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(pending_count),
            &pending_count,
            |b, &count| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        let mut ctx = setup_ctx();
                        let conn = ConnectionId(42);
                        insert_pending_batch(&mut ctx, conn, count);

                        let req = DrainConnectionReq {
                            connection_id: conn,
                            mode: DrainMode::ReleaseAndRequeue,
                            now: Timestamp::new(0),
                        };

                        let start = std::time::Instant::now();
                        black_box(drain_connection(&mut ctx, &req));
                        total += start.elapsed();
                    }

                    total
                });
            },
        );
    }

    group.finish();
}

fn bench_drain_per_pending(c: &mut Criterion) {
    let mut group = c.benchmark_group("drain_per_pending_ns");

    // Measure ns/pending at different scales
    for pending_count in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(pending_count),
            &pending_count,
            |b, &count| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        let mut ctx = setup_ctx();
                        let conn = ConnectionId(42);
                        insert_pending_batch(&mut ctx, conn, count);

                        let req = DrainConnectionReq {
                            connection_id: conn,
                            mode: DrainMode::ReleaseAndDrop,
                            now: Timestamp::new(0),
                        };

                        let start = std::time::Instant::now();
                        black_box(drain_connection(&mut ctx, &req));
                        total += start.elapsed();
                    }

                    total
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_drain_connection, bench_drain_per_pending);
criterion_main!(benches);
