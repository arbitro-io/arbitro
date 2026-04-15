//! TEMPORARY bench — isolates the cost of edge removes in `release_pending`.
//!
//! Hypothesis: the 7 × `HashEdge::remove` calls dominate `release_pending`,
//! and — more importantly — their cost **scales with inflight population**
//! because HashMaps degrade as they grow (cache footprint, bucket depth,
//! `Vec<V>` scan length per parent).
//!
//! Method: pre-populate N pending entries (1, 100, 1k, 10k, 100k), then
//! measure the cost of releasing ONE entry in the middle, for two variants:
//!
//!   A. `release_full`    — full `release_pending` (graph + inflight + 7 edges)
//!   B. `release_no_edges`— same as A but skips the 7 edge removes
//!
//! The delta (A − B) is the cost attributable to edges at that population.
//! If the theory is right, (A − B) grows with N while B stays ~flat.
//!
//! This file is temporary — delete once the intrusive-edge refactor lands.

use arbitro_engine::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine::context::EngineContext;
use arbitro_engine::graph::node::{pending_edge_idx, PendingNode};
use arbitro_engine::types::*;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

/// Populate `ctx` with exactly `n` pending entries spread across a realistic
/// graph shape: 16 consumers, 16 queues, 16 subscriptions, 16 connections,
/// 256 distinct subject hashes. Returns all the SlabKeys so the bench can
/// pick one in the middle for measurement.
fn setup_ctx_with_n_pending(n: usize) -> (EngineContext, Vec<SlabKey>) {
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

    // 16 consumers, each with its own queue + subscription. Enough distinct
    // parents to make the edge HashMaps non-trivial without overwhelming setup.
    for i in 1..=16u32 {
        ctx.catalog
            .ensure_consumer(
                &mut ctx.graph,
                &mut ctx.edges,
                ConsumerConfig {
                    id: ConsumerId(i),
                    queue_id: QueueId(i),
                    stream_id: StreamId(1),
                    durable: true,
                    ack_policy: AckPolicy::Explicit,
                    max_inflight: 10_000_000,
                },
            )
            .unwrap();

        ctx.catalog
            .ensure_subscription(
                &mut ctx.graph,
                &mut ctx.edges,
                SubscriptionConfig {
                    id: SubscriptionId(i),
                    stream_id: StreamId(1),
                    consumer_id: ConsumerId(i),
                    filters: vec![],
                },
            )
            .unwrap();
    }

    // Insert n pending nodes directly into the graph + edges, bypassing the
    // publish/claim pipeline so setup stays linear in n. This is exactly what
    // the ack.rs unit tests' `insert_pending` helper does.
    let mut keys = Vec::with_capacity(n);
    for i in 0..n {
        let consumer = ConsumerId((i as u32 % 16) + 1);
        let queue = QueueId((i as u32 % 16) + 1);
        let subscription = SubscriptionId((i as u32 % 16) + 1);
        let connection = ConnectionId((i as u64 % 16) + 100);
        let binding = BindingId((i as u32 % 16) + 1);
        let subject_hash = (i as u32) % 256;
        let seq = i as u64 + 1;

        let pending = PendingNode {
            pending_id: PendingId(seq as u32),
            seq,
            queue_id: queue,
            consumer_id: consumer,
            subscription_id: subscription,
            binding_id: binding,
            connection_id: connection,
            subject_hash,
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

        ctx.edges.pending_by_connection.insert_head(&mut ctx.graph.pending, connection, key);
        ctx.edges.pending_by_consumer.insert_head(&mut ctx.graph.pending, consumer, key);
        ctx.edges.pending_by_queue.insert_head(&mut ctx.graph.pending, queue, key);
        ctx.edges.pending_by_subscription.insert_head(&mut ctx.graph.pending, subscription, key);
        let _ = (subject_hash, binding);
        ctx.edges
            .pending_by_consumer_seq
            .insert(consumer, seq, key);

        ctx.inflight
            .inc_pending(subject_hash, consumer.raw(), queue.raw());

        keys.push(key);
    }

    (ctx, keys)
}

/// Full release: graph remove + inflight dec + 4 intrusive edge unlinks +
/// consumer_seq remove. Mirrors the real `release_pending` so the variant
/// below (no_edges) isolates exactly the edge cost.
#[inline(never)]
fn release_full(ctx: &mut EngineContext, key: SlabKey) -> Option<PendingNode> {
    let pending = ctx.graph.remove_pending(key).ok()?;
    ctx.inflight.dec_pending(
        pending.subject_hash,
        pending.consumer_id.raw(),
        pending.queue_id.raw(),
    );
    use pending_edge_idx as EI;
    let slab = &mut ctx.graph.pending;
    ctx.edges.pending_by_connection.unlink(
        slab,
        pending.connection_id,
        key,
        pending.edge_prev[EI::CONNECTION],
        pending.edge_next[EI::CONNECTION],
    );
    ctx.edges.pending_by_consumer.unlink(
        slab,
        pending.consumer_id,
        key,
        pending.edge_prev[EI::CONSUMER],
        pending.edge_next[EI::CONSUMER],
    );
    ctx.edges.pending_by_queue.unlink(
        slab,
        pending.queue_id,
        key,
        pending.edge_prev[EI::QUEUE],
        pending.edge_next[EI::QUEUE],
    );
    ctx.edges.pending_by_subscription.unlink(
        slab,
        pending.subscription_id,
        key,
        pending.edge_prev[EI::SUBSCRIPTION],
        pending.edge_next[EI::SUBSCRIPTION],
    );
    ctx.edges
        .pending_by_consumer_seq
        .remove(pending.consumer_id, pending.seq);
    Some(pending)
}

/// Hypothetical release **without** edges — simulates the intrusive-edge
/// endpoint where removing an entry from the graph implicitly updates all
/// edge lists in O(1) (or rather, where the edge work is zero because the
/// PendingNode carries its own list pointers).
#[inline(never)]
fn release_no_edges(ctx: &mut EngineContext, key: SlabKey) -> Option<PendingNode> {
    let pending = ctx.graph.remove_pending(key).ok()?;
    ctx.inflight.dec_pending(
        pending.subject_hash,
        pending.consumer_id.raw(),
        pending.queue_id.raw(),
    );
    // NO edge removes — this is the whole point of the comparison.
    Some(pending)
}

fn bench_release_breakdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("release_breakdown");
    group.sample_size(30);

    // Measure at several inflight populations. 100k = ~20 MB edge HashMaps,
    // well beyond L2. If HashMap cost is cache-bound, it shows up here.
    for &n in &[1usize, 100, 1_000, 10_000, 100_000] {
        // Variant A: full release_pending (graph + inflight + 7 edges)
        group.bench_with_input(
            BenchmarkId::new("full_with_edges", n),
            &n,
            |b, &n| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (mut ctx, keys) = setup_ctx_with_n_pending(n);
                        // Pick the middle key so we're not always hitting
                        // the first bucket of every HashMap.
                        let key = keys[n / 2];

                        let start = std::time::Instant::now();
                        black_box(release_full(&mut ctx, key));
                        total += start.elapsed();
                    }
                    total
                });
            },
        );

        // Variant B: graph + inflight only, no edges.
        group.bench_with_input(
            BenchmarkId::new("no_edges", n),
            &n,
            |b, &n| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let (mut ctx, keys) = setup_ctx_with_n_pending(n);
                        let key = keys[n / 2];

                        let start = std::time::Instant::now();
                        black_box(release_no_edges(&mut ctx, key));
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_release_breakdown);
criterion_main!(benches);
