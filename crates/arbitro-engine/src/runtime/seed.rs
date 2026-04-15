//! Store → ready-queue enqueue — push a store-backed entry into the ready
//! queue using its pre-assigned store seq.
//!
//! Level 7 — depends on catalog + ready + context.
//!
//! Used by the **drainer hot loop** (`publish_pending_to_engine`) on every
//! drain cycle, and by initial seed/replay when a new consumer subscribes.
//! Publish assigns seqs from `ctx.next_seq` — this path must NOT do that
//! or it would desync the store's historical ordering.
//!
//! ## Correctness invariants (NON-NEGOTIABLE for a message broker)
//!
//! 1. **No message loss**: every queue matching the subject receives the
//!    seq. The previous implementation in `lib.rs` hard-capped dedup at 8
//!    queues and silently dropped any overflow — catastrophic for a fanout
//!    broker. This implementation uses `ctx.seed_scratch` (reusable Vec)
//!    with no cap.
//! 2. **Per-queue dedup**: same subject cannot be pushed twice to the same
//!    queue within one call (match table may surface the same queue via
//!    multiple patterns — e.g. `orders.*` and `orders.>`).
//! 3. **FIFO per subject preserved**: `ReadySubjectRing::push` maintains
//!    per-subject ordering by design; we do nothing that could reorder.
//! 4. **`next_seq` untouched**: seq arrives pre-assigned from the store.
//!
//! ## Budget
//!
//! This is a **hot path** — called up to `max_feed_per_cycle` times per
//! drain wakeup. Metrics are NOT updated per call; the caller accumulates
//! counts and calls `flush_seed_metrics` once per batch. Allocation growth
//! in `seed_scratch` amortizes to zero after the first few calls.

use std::sync::atomic::Ordering;

use crate::context::EngineContext;
use crate::types::StreamId;

/// Push a single store entry into every matching ready queue.
/// Returns the number of queues pushed (0 = no match).
///
/// **Metrics are NOT updated** — the caller must accumulate the returned
/// counts and call [`flush_seed_metrics`] once per batch to avoid
/// per-message atomic overhead.
pub fn enqueue_ready(
    ctx: &mut EngineContext,
    stream_id: StreamId,
    subject: &[u8],
    subject_hash: u32,
    seq: u64,
) -> usize {
    // Split-borrow dance: `match_table_mut` needs `&mut ctx.catalog`, and
    // afterwards we need `&mut ctx.ready`. Collect the matched queue IDs
    // into `ctx.seed_scratch` while the catalog borrow is live; then drop
    // the catalog borrow and drain the scratch into `ctx.ready`.
    //
    // We `mem::take` the scratch Vec so we can hold both `&mut ctx.catalog`
    // and `&mut scratch` at once — `ctx.seed_scratch` is left empty during
    // the borrow and put back before we touch it again.
    let mut scratch = std::mem::take(&mut ctx.seed_scratch);
    scratch.clear();

    if let Some(mt) = ctx.catalog.match_table_mut(stream_id) {
        // resolve_patterns must run before lookup (caches wildcard matches).
        mt.resolve_patterns(subject_hash, subject);
        let result = mt.lookup(subject_hash);
        for me in result.iter() {
            let q = me.queue_id;
            // Linear dedup — scratch is typically 1-3 entries; even at 32
            // the quadratic cost is dwarfed by match-table lookup itself.
            if !scratch.contains(&q) {
                scratch.push(q);
            }
        }
    }

    let count = scratch.len();

    // Drain scratch into ready — catalog borrow is released.
    for i in 0..count {
        let q = scratch[i];
        ctx.ready.push(q, subject_hash, seq);
    }

    // Return the scratch buffer (now with grown capacity) to the context.
    ctx.seed_scratch = scratch;

    count
}

/// Flush accumulated seed metrics in a single batch. One atomic store per
/// counter instead of one per message.
#[inline]
pub fn flush_seed_metrics(
    ctx: &EngineContext,
    entries: u64,
    no_match: u64,
    queues_pushed: u64,
) {
    ctx.metrics.seed_entries.fetch_add(entries, Ordering::Relaxed);
    if no_match > 0 {
        ctx.metrics.seed_no_match.fetch_add(no_match, Ordering::Relaxed);
    }
    if queues_pushed > 0 {
        ctx.metrics.seed_queues_pushed.fetch_add(queues_pushed, Ordering::Relaxed);
    }
}

/// Batch-enqueue multiple store entries into the ready queues.
///
/// `items` is a slice of `(subject: &[u8], subject_hash: u32, seq: u64)`.
/// Resolves patterns once per unique subject_hash, resolves each queue ring
/// once, then pushes all seqs in bulk. Returns `(entries, no_match, queues_pushed)`
/// for the caller to flush via [`flush_seed_metrics`].
pub fn enqueue_ready_batch(
    ctx: &mut EngineContext,
    stream_id: StreamId,
    items: &[(&[u8], u32, u64)],
) -> (u64, u64, u64) {
    if items.is_empty() {
        return (0, 0, 0);
    }

    // ── Phase 1: resolve patterns + collect per-queue batch ─────────
    //
    // `per_queue` maps QueueId → Vec<(subject_hash, seq)>.
    // We reuse `ctx.seed_batch_queues` to avoid alloc per call.
    let mut per_queue = std::mem::take(&mut ctx.seed_batch_queues);
    for bucket in per_queue.values_mut() {
        bucket.clear();
    }

    let mut no_match: u64 = 0;
    let mut queues_pushed: u64 = 0;

    // Scratch for dedup of matched queues per entry.
    let mut scratch = std::mem::take(&mut ctx.seed_scratch);

    for &(subject, subject_hash, seq) in items {
        scratch.clear();

        if let Some(mt) = ctx.catalog.match_table_mut(stream_id) {
            mt.resolve_patterns(subject_hash, subject);
            let result = mt.lookup(subject_hash);
            for me in result.iter() {
                let q = me.queue_id;
                if !scratch.contains(&q) {
                    scratch.push(q);
                }
            }
        }

        if scratch.is_empty() {
            no_match += 1;
        } else {
            for &q in scratch.iter() {
                per_queue.entry(q).or_default().push((subject_hash, seq));
                queues_pushed += 1;
            }
        }
    }

    ctx.seed_scratch = scratch;

    // ── Phase 2: bulk-push per queue (1 ring lookup per queue) ──────
    for (&queue_id, batch) in per_queue.iter() {
        ctx.ready.push_batch(queue_id, batch);
    }

    ctx.seed_batch_queues = per_queue;

    (items.len() as u64, no_match, queues_pushed)
}

/// Fast-path batch enqueue for subjects whose patterns are already resolved.
///
/// Takes `&[(subject_hash, seq)]` — no subject bytes needed.
/// Skips `resolve_patterns` entirely and goes straight to `lookup`.
/// Unresolved hashes will get no_match (caller is responsible for
/// ensuring patterns were resolved beforehand).
pub fn enqueue_ready_seed_batch(
    ctx: &mut EngineContext,
    stream_id: StreamId,
    items: &[(u32, u64)],
) -> (u64, u64, u64) {
    if items.is_empty() {
        return (0, 0, 0);
    }

    let mut per_queue = std::mem::take(&mut ctx.seed_batch_queues);
    for bucket in per_queue.values_mut() {
        bucket.clear();
    }

    let mut no_match: u64 = 0;
    let mut queues_pushed: u64 = 0;
    let mut scratch = std::mem::take(&mut ctx.seed_scratch);

    for &(subject_hash, seq) in items {
        scratch.clear();

        if let Some(mt) = ctx.catalog.match_table_mut(stream_id) {
            let result = mt.lookup(subject_hash);
            for me in result.iter() {
                let q = me.queue_id;
                if !scratch.contains(&q) {
                    scratch.push(q);
                }
            }
        }

        if scratch.is_empty() {
            no_match += 1;
        } else {
            for &q in scratch.iter() {
                per_queue.entry(q).or_default().push((subject_hash, seq));
                queues_pushed += 1;
            }
        }
    }

    ctx.seed_scratch = scratch;

    for (&queue_id, batch) in per_queue.iter() {
        ctx.ready.push_batch(queue_id, batch);
    }

    ctx.seed_batch_queues = per_queue;

    (items.len() as u64, no_match, queues_pushed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
    use crate::types::*;

    fn setup(num_consumers: u32) -> EngineContext {
        let mut ctx = EngineContext::new();
        ctx.catalog.ensure_stream(&mut ctx.graph, StreamConfig {
            id: StreamId(1),
            name: b"orders".to_vec(),
        }).unwrap();

        for i in 1..=num_consumers {
            ctx.catalog.ensure_consumer(&mut ctx.graph, &mut ctx.edges, ConsumerConfig {
                id: ConsumerId(i),
                queue_id: QueueId(i),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 1000,
            }).unwrap();
            ctx.catalog.ensure_subscription(&mut ctx.graph, &mut ctx.edges, SubscriptionConfig {
                id: SubscriptionId(i),
                stream_id: StreamId(1),
                consumer_id: ConsumerId(i),
                filters: vec![], // wildcard: match everything
            }).unwrap();
        }
        ctx
    }

    #[test]
    fn enqueue_no_match_increments_counter() {
        let mut ctx = EngineContext::new();
        let pushed = enqueue_ready(&mut ctx, StreamId(999), b"orders.created", 0xBEEF, 1);
        flush_seed_metrics(&ctx, 1, if pushed == 0 { 1 } else { 0 }, pushed as u64);
        assert_eq!(ctx.metrics.seed_entries.load(Ordering::Relaxed), 1);
        assert_eq!(ctx.metrics.seed_no_match.load(Ordering::Relaxed), 1);
        assert_eq!(ctx.metrics.seed_queues_pushed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn enqueue_preserves_next_seq() {
        let mut ctx = setup(1);
        let before = ctx.next_seq;
        enqueue_ready(&mut ctx, StreamId(1), b"orders.created", 0xBEEF, 42);
        assert_eq!(ctx.next_seq, before, "enqueue_ready must not touch next_seq");
    }

    #[test]
    fn enqueue_many_queues_no_drop() {
        // Regression for the prior 8-queue cap bug: ALL 16 queues must
        // receive the seq, not just the first 8.
        let mut ctx = setup(16);
        let pushed = enqueue_ready(&mut ctx, StreamId(1), b"orders.created", 0xBEEF, 100);
        flush_seed_metrics(&ctx, 1, 0, pushed as u64);

        for q in 1..=16u32 {
            assert!(
                ctx.ready.has_ready(QueueId(q)),
                "queue {q} did not receive replayed seq — regression of 8-queue cap",
            );
        }
        assert_eq!(
            ctx.metrics.seed_queues_pushed.load(Ordering::Relaxed),
            16,
            "all 16 queues must be counted",
        );
    }

    #[test]
    fn enqueue_scratch_is_reused() {
        let mut ctx = setup(4);
        let mut total_pushed: u64 = 0;
        for seq in 1..=5 {
            total_pushed += enqueue_ready(&mut ctx, StreamId(1), b"orders.created", 0xBEEF, seq) as u64;
        }
        flush_seed_metrics(&ctx, 5, 0, total_pushed);
        // After 5 calls, scratch capacity ≥ 4 (grown once, then reused).
        assert!(ctx.seed_scratch.capacity() >= 4);
        // Scratch is left populated with the last batch's content — that's
        // fine, next call clears before use.
        assert_eq!(ctx.metrics.seed_entries.load(Ordering::Relaxed), 5);
        assert_eq!(ctx.metrics.seed_queues_pushed.load(Ordering::Relaxed), 20);
    }

    // ── enqueue_ready_batch tests ─────────────────────────────────────

    #[test]
    fn batch_single_entry_matches_single_call() {
        use crate::catalog::fnv1a_32;
        let mut ctx = setup(1);
        let subject = b"orders.created";
        let hash = fnv1a_32(subject);

        let (entries, no_match, pushed) =
            enqueue_ready_batch(&mut ctx, StreamId(1), &[(subject, hash, 42)]);
        flush_seed_metrics(&ctx, entries, no_match, pushed);

        assert_eq!(entries, 1);
        assert_eq!(no_match, 0);
        assert_eq!(pushed, 1);
        assert!(ctx.ready.has_ready(QueueId(1)));
        assert_eq!(ctx.metrics.seed_entries.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn batch_many_entries_all_queues_receive() {
        use crate::catalog::fnv1a_32;
        let mut ctx = setup(4);
        let subject = b"orders.created";
        let hash = fnv1a_32(subject);

        let items: Vec<(&[u8], u32, u64)> = (1..=10)
            .map(|seq| (subject.as_slice(), hash, seq))
            .collect();

        let (entries, no_match, pushed) =
            enqueue_ready_batch(&mut ctx, StreamId(1), &items);
        flush_seed_metrics(&ctx, entries, no_match, pushed);

        assert_eq!(entries, 10);
        assert_eq!(no_match, 0);
        assert_eq!(pushed, 40); // 4 queues × 10 entries
        for q in 1..=4u32 {
            assert!(ctx.ready.has_ready(QueueId(q)));
            assert_eq!(ctx.ready.total_ready(QueueId(q)), 10);
        }
    }

    #[test]
    fn batch_no_match_counts_correctly() {
        use crate::catalog::fnv1a_32;
        let mut ctx = setup(1);
        // Stream 999 has no match table
        let items: Vec<(&[u8], u32, u64)> = (1..=5)
            .map(|seq| (b"nope".as_slice(), fnv1a_32(b"nope"), seq))
            .collect();

        let (entries, no_match, pushed) =
            enqueue_ready_batch(&mut ctx, StreamId(999), &items);
        flush_seed_metrics(&ctx, entries, no_match, pushed);

        assert_eq!(entries, 5);
        assert_eq!(no_match, 5);
        assert_eq!(pushed, 0);
    }

    #[test]
    fn batch_empty_is_noop() {
        let mut ctx = setup(1);
        let (entries, no_match, pushed) =
            enqueue_ready_batch(&mut ctx, StreamId(1), &[]);
        assert_eq!((entries, no_match, pushed), (0, 0, 0));
    }

    #[test]
    fn batch_reuses_scratch_buffers() {
        use crate::catalog::fnv1a_32;
        let mut ctx = setup(2);
        let subject = b"orders.created";
        let hash = fnv1a_32(subject);

        // Call twice to verify scratch reuse
        for round in 0..2 {
            let items: Vec<(&[u8], u32, u64)> = (1..=5)
                .map(|seq| (subject.as_slice(), hash, round * 100 + seq))
                .collect();
            enqueue_ready_batch(&mut ctx, StreamId(1), &items);
        }

        assert!(ctx.seed_batch_queues.capacity() >= 2);
        assert!(ctx.seed_scratch.capacity() >= 2);
    }

    // ── Performance breakdown ───────────────────────────────────────────

    #[test]
    fn perf_enqueue_ready_breakdown() {
        use std::time::Instant;
        use crate::catalog::fnv1a_32;

        const N: usize = 1_000_000;
        let subject = b"orders.premium.created";
        let subject_hash = fnv1a_32(subject);
        let stream_id = StreamId(1);

        let fmt = |label: &str, elapsed: std::time::Duration| {
            let ns = elapsed.as_nanos() as f64 / N as f64;
            let rate = N as f64 / elapsed.as_secs_f64();
            eprintln!("  {:<30} {:.1} ns/msg  ({:.1}M msg/s)", label, ns, rate / 1e6);
        };

        // ── 1. Full enqueue_ready (baseline) ────────────────────────────
        {
            let mut ctx = setup(1);
            // Warm up resolve cache
            enqueue_ready(&mut ctx, stream_id, subject, subject_hash, 0);

            let t0 = Instant::now();
            for seq in 1..=N as u64 {
                enqueue_ready(&mut ctx, stream_id, subject, subject_hash, seq);
            }
            fmt("enqueue_ready (full)", t0.elapsed());
        }

        // ── 2. resolve_patterns only (cached path) ─────────────────────
        {
            let mut ctx = setup(1);
            let mt = ctx.catalog.match_table_mut(stream_id).unwrap();
            mt.resolve_patterns(subject_hash, subject); // warm cache

            let t0 = Instant::now();
            for _ in 0..N {
                let mt = ctx.catalog.match_table_mut(stream_id).unwrap();
                mt.resolve_patterns(subject_hash, subject);
            }
            fmt("resolve_patterns (cached)", t0.elapsed());
        }

        // ── 3. lookup only ──────────────────────────────────────────────
        {
            let mut ctx = setup(1);
            let mt = ctx.catalog.match_table_mut(stream_id).unwrap();
            mt.resolve_patterns(subject_hash, subject);

            let t0 = Instant::now();
            for _ in 0..N {
                let mt = ctx.catalog.match_table_mut(stream_id).unwrap();
                let result = mt.lookup(subject_hash);
                std::hint::black_box(result.iter().count());
            }
            fmt("lookup + iter", t0.elapsed());
        }

        // ── 4. ready.push only ──────────────────────────────────────────
        {
            let mut ctx = setup(1);
            let queue_id = QueueId(1);

            let t0 = Instant::now();
            for seq in 1..=N as u64 {
                ctx.ready.push(queue_id, subject_hash, seq);
            }
            fmt("ready.push", t0.elapsed());
        }

        // ── 5. metrics only (3× atomic fetch_add) ──────────────────────
        {
            let ctx = setup(1);

            let t0 = Instant::now();
            for _ in 0..N {
                ctx.metrics.seed_entries.fetch_add(1, Ordering::Relaxed);
                ctx.metrics.seed_queues_pushed.fetch_add(1, Ordering::Relaxed);
            }
            fmt("metrics (2× atomic)", t0.elapsed());
        }

        // ── 6. scratch take + clear + return ────────────────────────────
        {
            let mut ctx = setup(1);
            ctx.seed_scratch.reserve(16);

            let t0 = Instant::now();
            for _ in 0..N {
                let mut scratch = std::mem::take(&mut ctx.seed_scratch);
                scratch.clear();
                scratch.push(QueueId(1));
                ctx.seed_scratch = scratch;
            }
            fmt("scratch take/clear/return", t0.elapsed());
        }

        // ── 7. match_table_mut access ───────────────────────────────────
        {
            let mut ctx = setup(1);

            let t0 = Instant::now();
            for _ in 0..N {
                std::hint::black_box(ctx.catalog.match_table_mut(stream_id));
            }
            fmt("match_table_mut", t0.elapsed());
        }

        // ── 8. enqueue_ready_batch (256 items per call) ─────────────────
        {
            let mut ctx = setup(1);
            // Warm up resolve cache
            enqueue_ready(&mut ctx, stream_id, subject, subject_hash, 0);

            const BATCH: usize = 256;
            let batches = N / BATCH;
            let t0 = Instant::now();
            for b in 0..batches {
                // Rebuild batch with correct seqs
                let base = (b * BATCH) as u64 + 1;
                let items: Vec<(&[u8], u32, u64)> = (0..BATCH as u64)
                    .map(|i| (subject.as_slice(), subject_hash, base + i))
                    .collect();
                enqueue_ready_batch(&mut ctx, stream_id, &items);
            }
            let total = batches * BATCH;
            let elapsed = t0.elapsed();
            let ns = elapsed.as_nanos() as f64 / total as f64;
            let rate = total as f64 / elapsed.as_secs_f64();
            eprintln!("  {:<30} {:.1} ns/msg  ({:.1}M msg/s)", "enqueue_ready_batch (256)", ns, rate / 1e6);
        }
    }
}
