//! Benchmark: publish batch — ns/entry for 1/10/100/1000 entries.
//!
//! Measures the full publish hot path:
//! dedup check → match table lookup → enqueue ready → fanout push.

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, black_box};
use arbitro_engine::context::EngineContext;
use arbitro_engine::catalog::{StreamConfig, ConsumerConfig, SubscriptionConfig, fnv1a_32};
use arbitro_engine::types::*;
use arbitro_engine::batch::{PublishBatch, PublishEntry, BindBatch, BindEntry};
use arbitro_engine::runtime::publish::on_publish_batch;
use arbitro_engine::runtime::bind::on_bind_batch;

fn setup_ctx(num_consumers: u32) -> EngineContext {
    let mut ctx = EngineContext::new();

    ctx.catalog.ensure_stream(&mut ctx.graph, StreamConfig {
        id: StreamId(1),
        name: b"bench-stream".to_vec(),
    }).unwrap();

    for i in 0..num_consumers {
        ctx.catalog.ensure_consumer(&mut ctx.graph, &mut ctx.edges, ConsumerConfig {
            id: ConsumerId(i + 1),
            queue_id: QueueId(i + 1),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 100_000,
        }).unwrap();

        ctx.catalog.ensure_subscription(&mut ctx.graph, &mut ctx.edges, SubscriptionConfig {
            id: SubscriptionId(i + 1),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(i + 1),
            filters: vec![],
        }).unwrap();
    }

    ctx
}

struct BenchEntries {
    subjects: Vec<Vec<u8>>,
}

impl BenchEntries {
    fn new(count: usize) -> Self {
        let subjects: Vec<Vec<u8>> = (0..count)
            .map(|i| format!("orders.item.{i}").into_bytes())
            .collect();
        Self { subjects }
    }

    fn entries(&self) -> Vec<PublishEntry<'_>> {
        self.subjects.iter()
            .map(|s| PublishEntry {

                subject_hash: fnv1a_32(s),
                subject: s,
                payload: PayloadRef::Borrowed(b"bench-payload-64-bytes-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
                idempotency_key: 0,
                credits_cost: 1,
            })
            .collect()
    }
}

fn bench_publish_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_batch");

    for batch_size in [1, 10, 100, 1000] {
        let bench_data = BenchEntries::new(batch_size);

        group.bench_with_input(
            BenchmarkId::new("1_consumer", batch_size),
            &batch_size,
            |b, _| {
                let mut ctx = setup_ctx(1);
                b.iter(|| {
                    while ctx.ready.pop(QueueId(1)).is_some() {}
                    ctx.fanout.reset();
                    let entries = bench_data.entries();
                    let batch = PublishBatch {
                        stream_id: StreamId(1),
                        entries: black_box(&entries),
                        now: Timestamp::new(1_000_000),
                    };
                    black_box(on_publish_batch(&mut ctx, &batch));
                });
            },
        );

        let bench_data = BenchEntries::new(batch_size);
        group.bench_with_input(
            BenchmarkId::new("3_consumers", batch_size),
            &batch_size,
            |b, _| {
                let mut ctx = setup_ctx(3);
                b.iter(|| {
                    for q in 1..=3u32 {
                        while ctx.ready.pop(QueueId(q)).is_some() {}
                    }
                    ctx.fanout.reset();
                    let entries = bench_data.entries();
                    let batch = PublishBatch {
                        stream_id: StreamId(1),
                        entries: black_box(&entries),
                        now: Timestamp::new(1_000_000),
                    };
                    black_box(on_publish_batch(&mut ctx, &batch));
                });
            },
        );
    }

    group.finish();
}

fn bench_publish_with_dedup(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_dedup");

    let dedup_data = BenchEntries::new(100);

    group.bench_function("100_unique_keys", |b| {
        let mut ctx = setup_ctx(1);
        b.iter(|| {
            while ctx.ready.pop(QueueId(1)).is_some() {}
            ctx.fanout.reset();
            ctx.idempotency.clear();
            let mut entries = dedup_data.entries();
            for (i, e) in entries.iter_mut().enumerate() {
                e.idempotency_key = (i as u64) + 1;
            }
            let batch = PublishBatch {
                stream_id: StreamId(1),
                entries: black_box(&entries),
                now: Timestamp::new(1_000_000),
            };
            black_box(on_publish_batch(&mut ctx, &batch));
        });
    });

    group.finish();
}

// ── Fanout: publish with bindings → fire-and-forget notifications ────────

fn setup_ctx_with_bindings(num_consumers: u32, num_connections: u32) -> EngineContext {
    let mut ctx = setup_ctx(num_consumers);

    let bind_entries: Vec<BindEntry> = (1..=num_consumers)
        .map(|i| BindEntry {
            connection_id: ConnectionId((((i - 1) % num_connections) + 1) as u64),
            subscription_id: SubscriptionId(i),
        })
        .collect();

    on_bind_batch(&mut ctx, &BindBatch {
        entries: &bind_entries,
        now: Timestamp::new(0),
    });

    ctx
}

fn bench_publish_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_fanout");

    // Scenario A: N consumers all on 1 connection
    for nc in [1u32, 3, 10, 32] {
        let data = BenchEntries::new(100);
        group.bench_with_input(
            BenchmarkId::new(format!("{nc}_consumers_1_conn"), 100),
            &nc,
            |b, &nc| {
                let mut ctx = setup_ctx_with_bindings(nc, 1);
                b.iter(|| {
                    for q in 1..=nc {
                        while ctx.ready.pop(QueueId(q)).is_some() {}
                    }
                    ctx.fanout.reset();
                    let entries = data.entries();
                    let batch = PublishBatch {
                        stream_id: StreamId(1),
                        entries: black_box(&entries),
                        now: Timestamp::new(1_000_000),
                    };
                    black_box(on_publish_batch(&mut ctx, &batch));
                });
            },
        );
    }

    // Scenario B: N consumers on N connections
    for nc in [1u32, 3, 10] {
        let data = BenchEntries::new(100);
        group.bench_with_input(
            BenchmarkId::new(format!("{nc}_consumers_{nc}_conns"), 100),
            &nc,
            |b, &nc| {
                let mut ctx = setup_ctx_with_bindings(nc, nc);
                b.iter(|| {
                    for q in 1..=nc {
                        while ctx.ready.pop(QueueId(q)).is_some() {}
                    }
                    ctx.fanout.reset();
                    let entries = data.entries();
                    let batch = PublishBatch {
                        stream_id: StreamId(1),
                        entries: black_box(&entries),
                        now: Timestamp::new(1_000_000),
                    };
                    black_box(on_publish_batch(&mut ctx, &batch));
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_publish_batch, bench_publish_with_dedup, bench_publish_fanout);
criterion_main!(benches);
