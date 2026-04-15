//! Benchmark: ack release — ns/ack for the release_pending protocol.
//!
//! Measures the full ack hot path:
//! slab remove + 3 inflight decs + credit release + deadline cancel + 7 edge removes.

use arbitro_engine::batch::{AckBatch, AckEntry, ClaimBatch};
use arbitro_engine::batch::{PublishBatch, PublishEntry};
use arbitro_engine::catalog::fnv1a_32;
use arbitro_engine::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine::context::EngineContext;
use arbitro_engine::runtime::ack::on_ack_batch;
use arbitro_engine::runtime::claim::{on_claim_batch, resolve_ids_for_batch};
use arbitro_engine::runtime::publish::on_publish_batch;
use arbitro_engine::types::*;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

fn setup_ctx() -> EngineContext {
    let mut ctx = EngineContext::new();

    ctx.catalog
        .ensure_stream(
            &mut ctx.graph,
            StreamConfig {
                id: StreamId(1),
                name: b"bench".to_vec(),
            },
        )
        .unwrap();

    ctx.catalog
        .ensure_consumer(
            &mut ctx.graph,
            &mut ctx.edges,
            ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100_000,
            },
        )
        .unwrap();

    ctx.catalog
        .ensure_subscription(
            &mut ctx.graph,
            &mut ctx.edges,
            SubscriptionConfig {
                id: SubscriptionId(1),
                stream_id: StreamId(1),
                consumer_id: ConsumerId(1),
                filters: vec![],
            },
        )
        .unwrap();

    ctx
}

/// Publish + claim N messages, returning their seqs for acking.
fn publish_and_claim(ctx: &mut EngineContext, count: usize) -> Vec<u64> {
    let subjects: Vec<Vec<u8>> = (0..count)
        .map(|i| format!("bench.{i}").into_bytes())
        .collect();
    let entries: Vec<_> = subjects
        .iter()
        .map(|s| PublishEntry {
            subject_hash: fnv1a_32(s),
            subject: s,
            payload: PayloadRef::Borrowed(b"ack-bench-payload"),
            idempotency_key: 0,
            credits_cost: 1,
        })
        .collect();

    let batch = PublishBatch {
        stream_id: StreamId(1),
        entries: &entries,
        now: Timestamp::new(1_000_000),
    };
    on_publish_batch(ctx, &batch);

    let claim = ClaimBatch {
        queue_id: QueueId(1),
        connection_id: ConnectionId(100),
        consumer_id: ConsumerId(1),
        max_items: count as u16,
        now: Timestamp::new(2_000_000),
    };
    let (sub, bind) = resolve_ids_for_batch(ctx, &claim);
    let claimed = on_claim_batch(ctx, &claim, sub, bind);
    claimed.entries().iter().map(|e| e.seq).collect()
}

fn bench_ack_release(c: &mut Criterion) {
    let mut group = c.benchmark_group("ack_release");

    for batch_size in [1, 10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;

                    for _ in 0..iters {
                        let mut ctx = setup_ctx();
                        let seqs = publish_and_claim(&mut ctx, size);

                        let ack_entries: Vec<_> =
                            seqs.iter().map(|&s| AckEntry { seq: s }).collect();
                        let batch = AckBatch {
                            consumer_id: ConsumerId(1),
                            entries: &ack_entries,
                            now: Timestamp::new(3_000_000),
                        };

                        let start = std::time::Instant::now();
                        black_box(on_ack_batch(&mut ctx, &batch));
                        total += start.elapsed();
                    }

                    total
                });
            },
        );
    }

    group.finish();
}

fn bench_release_pending_direct(c: &mut Criterion) {
    let mut group = c.benchmark_group("release_pending_direct");

    group.bench_function("single", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;

            for _ in 0..iters {
                let mut ctx = setup_ctx();
                let seqs = publish_and_claim(&mut ctx, 1);

                let key = ctx
                    .edges
                    .pending_by_consumer_seq
                    .get(ConsumerId(1), seqs[0])
                    .unwrap();

                let start = std::time::Instant::now();
                black_box(arbitro_engine::runtime::ack::release_pending(&mut ctx, key));
                total += start.elapsed();
            }

            total
        });
    });

    group.finish();
}

criterion_group!(benches, bench_ack_release, bench_release_pending_direct);
criterion_main!(benches);
