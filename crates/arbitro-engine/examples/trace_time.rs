//! trace_time — granular per-phase timing of publish / claim / ack.
//!
//! Mirrors the engine shape from `benches/throughput.rs` (`engine_simple`,
//! same payload, same IDs) so numbers can be cross-checked against
//! `cargo bench --bench throughput`.
//!
//! Run from WSL:
//!   cargo build --example trace_time --release
//!   ./target/release/examples/trace_time
//!
//! Output is designed to answer ONE question: which concrete op inside
//! the full cycle is the slowest? The sub-micro section isolates each
//! hot-path primitive so you can compare against the end-to-end number.
//!
//! ## Measurement notes
//!
//! - `Instant::now()` on Linux/WSL uses `clock_gettime(CLOCK_MONOTONIC)`
//!   via vDSO (~20-30 ns per call). We call it ONCE per measurement
//!   (around a whole N-item loop), never per item. Division by N yields
//!   the amortized cost and hides clock overhead entirely.
//! - `std::hint::black_box` is applied to every loop input AND to any
//!   accumulated output so LLVM cannot dead-code-eliminate the work.
//! - All buffers are pre-allocated outside the measured region.
//! - Each phase runs 3 warmup iterations before the measured one, to
//!   prime caches and TLB.

use arbitro_engine::batch::*;
use arbitro_engine::catalog::{fnv1a_32, ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine::edge::{ConsumerSeqEdge, PendingEdge};
use arbitro_engine::graph::node::{pending_edge_idx, PendingNode};
use arbitro_engine::graph::slab::TypedSlab;
use arbitro_engine::inflight::{InFlightCounters, InFlightScope};
use arbitro_engine::ready::ReadyState;
use arbitro_engine::types::*;
use arbitro_engine::*;
use std::hint::black_box;
use std::time::{Duration, Instant};

// ── Measurement primitive ───────────────────────────────────────────────────

/// Run `body` once, measure elapsed, print `label + ns/item`.
/// `body` is expected to internally loop over `$n` items.
macro_rules! measure {
    ($label:expr, $n:expr, $body:block) => {{
        let start = Instant::now();
        let r = $body;
        let elapsed = start.elapsed();
        let per = elapsed.as_nanos() as f64 / ($n as f64);
        println!(
            "  {:<46} {:>9.2} ns/item   (total {:>10.2?})",
            $label, per, elapsed
        );
        r
    }};
}

/// Warmup + multi-pass measurement. Runs `$body` 3 times for warmup (not timed),
/// then $passes times for measurement. Divides total elapsed by (n * passes)
/// to get steady-state ns/item — avoids the single-shot cold-start bias where
/// page faults, allocator cold paths, and first-touch DRAM penalties get
/// divided by n and reported as if they were per-item costs.
///
/// Use this for any op that (a) allocates, (b) writes to previously-unseen
/// memory, or (c) wants a noise-averaged number.
macro_rules! measure_warm {
    ($label:expr, $n:expr, $passes:expr, $body:block) => {{
        for _ in 0..3 { $body }
        let start = Instant::now();
        for _ in 0..$passes { $body }
        let elapsed = start.elapsed();
        let per = elapsed.as_nanos() as f64 / (($n * $passes) as f64);
        println!(
            "  {:<46} {:>9.2} ns/item   ({} passes × {} items)",
            $label, per, $passes, $n
        );
    }};
}

fn section(title: &str) {
    println!("\n── {} ──────────────────────────────────────────", title);
}

// ── Engine setup (mirrors benches/throughput.rs::engine_simple) ────────────

fn engine_simple(num_consumers: u32, max_inflight: u32) -> ArbitroEngine {
    let mut e = ArbitroEngine::new();

    e.ensure_stream(StreamConfig {
        id: StreamId(1),
        name: b"trace".to_vec(),
    })
    .unwrap();

    for i in 1..=num_consumers {
        e.ensure_consumer(ConsumerConfig {
            id: ConsumerId(i),
            queue_id: QueueId(i),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight,
        })
        .unwrap();

        e.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(i),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(i),
            filters: vec![],
        })
        .unwrap();
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

struct Msgs {
    subjects: Vec<Vec<u8>>,
    hashes: Vec<u32>,
}

impl Msgs {
    fn new(n: usize) -> Self {
        let subjects: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("trace.subj.{i}").into_bytes())
            .collect();
        let hashes: Vec<u32> = subjects.iter().map(|s| fnv1a_32(s)).collect();
        Self { subjects, hashes }
    }

    fn publish_entries(&self) -> Vec<PublishEntry<'_>> {
        self.subjects
            .iter()
            .zip(self.hashes.iter())
            .map(|(s, h)| PublishEntry {
                subject_hash: *h,
                subject: s,
                payload: PayloadRef::Borrowed(
                    b"trace-payload-64B-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                ),
                idempotency_key: 0,
                credits_cost: 1,
            })
            .collect()
    }
}

// ── Clock calibration ───────────────────────────────────────────────────────

fn calibrate_clock() {
    section("CLOCK CALIBRATION");
    let n = 1_000_000;
    let start = Instant::now();
    for _ in 0..n {
        black_box(Instant::now());
    }
    let el = start.elapsed();
    println!(
        "  Instant::now() overhead                        {:>9.2} ns/call",
        el.as_nanos() as f64 / n as f64
    );
    println!("  (Each measure! call pays this exactly ONCE, not per item)");
}

// ── PHASE 1: decomposed full cycle ──────────────────────────────────────────

fn phase_full_cycle(n: usize) {
    section(&format!("FULL CYCLE (N = {})", n));

    let mut engine = engine_simple(1, (n as u32) * 2);
    let msgs = Msgs::new(n);
    let publish_entries = msgs.publish_entries();
    let mut ack_scratch: Vec<AckEntry> = Vec::with_capacity(n);

    let claim_batch = ClaimBatch {
        queue_id: QueueId(1),
        connection_id: ConnectionId(100),
        consumer_id: ConsumerId(1),
        max_items: n.min(u16::MAX as usize) as u16,
        now: Timestamp::new(1_000_000),
    };
    let (sub, bind) =
        arbitro_engine::runtime::claim::resolve_ids_for_batch(engine.ctx(), &claim_batch);

    // ── Warmup (3 full cycles to prime caches and any lazy growth) ──────
    for _ in 0..3 {
        engine.publish(&PublishBatch {
            stream_id: StreamId(1),
            entries: &publish_entries,
            now: Timestamp::new(1),
        });
        let claimed = engine.claim(&claim_batch, sub, bind);
        ack_scratch.clear();
        ack_scratch.extend(claimed.entries().iter().map(|e| AckEntry { seq: e.seq }));
        engine.ack(&AckBatch {
            entries: &ack_scratch,
            consumer_id: ConsumerId(1),
            now: Timestamp::new(2),
        });
    }

    // ── Measured: each phase in isolation, chained state ────────────────

    // Phase 1: publish only
    measure!("publish()", n, {
        engine.publish(&PublishBatch {
            stream_id: StreamId(1),
            entries: black_box(&publish_entries),
            now: Timestamp::new(1_000_000),
        });
    });

    // Phase 2: claim only (after publish above — ready queue full)
    let mut claimed_seqs: Vec<u64> = Vec::with_capacity(n);
    measure!("claim()", n, {
        let claimed = engine.claim(black_box(&claim_batch), sub, bind);
        claimed_seqs.clear();
        claimed_seqs.extend(claimed.entries().iter().map(|e| e.seq));
        black_box(&claimed_seqs);
    });

    // Phase 3: ack only (using the seqs we just claimed)
    ack_scratch.clear();
    ack_scratch.extend(claimed_seqs.iter().map(|&seq| AckEntry { seq }));
    measure!("ack()", n, {
        engine.ack(&AckBatch {
            entries: black_box(&ack_scratch),
            consumer_id: ConsumerId(1),
            now: Timestamp::new(2_000_000),
        });
    });

    // Phase 4: tight publish→claim→ack loop (what throughput_full_cycle measures)
    let combined = {
        let start = Instant::now();
        engine.publish(&PublishBatch {
            stream_id: StreamId(1),
            entries: black_box(&publish_entries),
            now: Timestamp::new(3_000_000),
        });
        let claimed = engine.claim(black_box(&claim_batch), sub, bind);
        ack_scratch.clear();
        ack_scratch.extend(claimed.entries().iter().map(|e| AckEntry { seq: e.seq }));
        engine.ack(&AckBatch {
            entries: black_box(&ack_scratch),
            consumer_id: ConsumerId(1),
            now: Timestamp::new(4_000_000),
        });
        start.elapsed()
    };
    println!(
        "  {:<46} {:>9.2} ns/item   (total {:>10.2?})",
        "FULL publish+claim+ack (single pass)",
        combined.as_nanos() as f64 / n as f64,
        combined
    );

    // Phase 5: averaged over 100 passes (steady-state, low noise)
    let passes = 100;
    let mut total = Duration::ZERO;
    for _ in 0..passes {
        let t = Instant::now();
        engine.publish(&PublishBatch {
            stream_id: StreamId(1),
            entries: &publish_entries,
            now: Timestamp::new(5_000_000),
        });
        let claimed = engine.claim(&claim_batch, sub, bind);
        ack_scratch.clear();
        ack_scratch.extend(claimed.entries().iter().map(|e| AckEntry { seq: e.seq }));
        engine.ack(&AckBatch {
            entries: &ack_scratch,
            consumer_id: ConsumerId(1),
            now: Timestamp::new(6_000_000),
        });
        total += t.elapsed();
    }
    println!(
        "  {:<46} {:>9.2} ns/item   ({} passes)",
        "FULL publish+claim+ack (100-pass avg)",
        total.as_nanos() as f64 / (n * passes) as f64,
        passes
    );
}

// ── PHASE 2: isolated sub-micro ops ─────────────────────────────────────────

fn phase_sub_micro(n: usize) {
    section(&format!("SUB-MICRO isolated primitives (N = {}, 100-pass warm avg)", n));

    const PASSES: usize = 100;

    // ─── fnv1a_32 ───────────────────────────────────────────────────────
    {
        let subjects: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("trace.subj.{i}").into_bytes())
            .collect();
        measure_warm!("fnv1a_32(subject)", n, PASSES, {
            let mut acc = 0u32;
            for s in &subjects {
                acc ^= fnv1a_32(black_box(s));
            }
            black_box(acc);
        });
    }

    // ─── InFlightCounters ───────────────────────────────────────────────
    {
        let mut cnt = InFlightCounters::new();
        for i in 0..64u32 {
            cnt.inc(InFlightScope::Consumer, i);
            cnt.dec(InFlightScope::Consumer, i);
        }

        measure_warm!("InFlight::get(Consumer) [Vec]", n, PASSES, {
            let mut sum = 0u32;
            for _ in 0..n {
                sum = sum.wrapping_add(cnt.get(InFlightScope::Consumer, black_box(1)));
            }
            black_box(sum);
        });

        measure_warm!("InFlight::get(Subject) [HashMap miss]", n, PASSES, {
            let mut sum = 0u32;
            for i in 0..n {
                sum = sum.wrapping_add(cnt.get(InFlightScope::Subject, black_box(i as u32)));
            }
            black_box(sum);
        });

        // inc_pending / dec_pending — measured as a paired inc+dec loop so the
        // state (consumer[1], queue[1]) stays balanced across passes and nothing
        // underflows. Cost reported = (inc + dec) / 2.
        measure_warm!("InFlight::inc_pending+dec_pending (paired)/2", n, PASSES, {
            for i in 0..n {
                cnt.inc_pending(black_box(i as u32), 1, 1);
                cnt.dec_pending(black_box(i as u32), 1, 1);
            }
        });
    }

    // ─── ConsumerSeqEdge — paired insert+remove per pass keeps deque empty ──
    {
        let mut edge = ConsumerSeqEdge::new();
        let c = ConsumerId(1);
        // Pre-grow outer Vec so we don't measure the first cold_path.
        edge.insert(c, 0, SlabKey::new(0, 0));
        edge.remove(c, 0);

        // insert+remove paired — each pass leaves the deque empty.
        measure_warm!("ConsumerSeqEdge::insert+remove (paired)/2", n, PASSES, {
            for i in 0..n {
                edge.insert(c, black_box(i as u64), SlabKey::new(i as u32, 0));
            }
            for i in 0..n {
                edge.remove(c, black_box(i as u64));
            }
        });

        // get — front fast-path, pure read. Pre-populate once with one entry.
        edge.insert(c, 0, SlabKey::new(0, 0));
        measure_warm!("ConsumerSeqEdge::get (front fast-path)", n, PASSES, {
            let mut found = 0u32;
            for _ in 0..n {
                if edge.get(c, black_box(0)).is_some() {
                    found += 1;
                }
            }
            black_box(found);
        });
        edge.remove(c, 0);
    }

    // ─── Vec<PublishEntry> allocation cost ──────────────────────────────
    // This one is intentionally a SINGLE cold pass — it measures "first
    // allocation" behavior (malloc + page faults + cold DRAM stores) to show
    // the worst-case penalty callers pay if they don't reuse buffers.
    {
        let msgs = Msgs::new(n);
        measure!("build Vec<PublishEntry> [cold alloc, 1-shot]", n, {
            let v = msgs.publish_entries();
            black_box(v);
        });
        // And here's the steady-state (100-pass warm) number for the same op.
        measure_warm!("build Vec<PublishEntry> [warm 100-pass]", n, PASSES, {
            let v = msgs.publish_entries();
            black_box(v);
        });
    }
}

// ── PHASE 2.5: claim() internals breakdown ──────────────────────────────────
//
// Goal: decompose the ~48 ns/msg of `claim()` into its primitive pieces so
// we can identify which one deserves attention. Each primitive is measured
// in isolation, warm-averaged over 100 passes, then summed and compared
// against the measured `claim()` wall-clock in phase_scaling.
//
// The primitives run in claim's hot loop, per message:
//   1. ctx.inflight.get(Consumer)              [already measured: ~0.4 ns]
//   2. ctx.ready.pop(queue_id)                 [← this phase measures]
//   3. [skipped: subject limit checks (gated off)]
//   4. Stack init of [CreditEntry; 3]          [trivial, ~1 ns]
//   5. [skipped: credit try_acquire (gated off)]
//   6. PendingNode struct init on stack        [← this phase measures]
//   7. ctx.graph.insert_pending(pending)       [← this phase measures]
//   8. pending_by_connection.insert_head       [← this phase measures]
//   9. pending_by_consumer.insert_head         [× 4 total edges]
//  10. pending_by_queue.insert_head
//  11. pending_by_subscription.insert_head
//  12. pending_by_consumer_seq.insert          [already measured: ~2.3 ns]
//  13. ctx.inflight.inc_pending                [already measured: ~1.5 ns]
//  14. ctx.reply_claim.accept                  [← this phase measures]
fn phase_claim_internals(n: usize) {
    section(&format!("CLAIM INTERNALS (N = {}, 100-pass warm avg)", n));
    const PASSES: usize = 100;

    // ─── ReadyState::pop — round-robin HashMap+VecDeque pop ─────────────
    // Paired with push so the ring stays stable across passes.
    {
        let mut ready = ReadyState::new();
        let queue = QueueId(1);
        // Pre-populate so first pop doesn't measure the insert cost.
        for i in 0..n {
            ready.push(queue, (i as u32) & 31, i as u64);
        }
        measure_warm!("ReadyState::push+pop (paired)/2", n, PASSES, {
            for i in 0..n {
                ready.push(black_box(queue), black_box((i as u32) & 31), black_box(i as u64));
            }
            for _ in 0..n {
                black_box(ready.pop(black_box(queue)));
            }
        });
    }

    // ─── Single-subject ReadyState::pop (best case — 1 subject in ring) ─
    {
        let mut ready = ReadyState::new();
        let queue = QueueId(1);
        measure_warm!("ReadyState::push+pop (1 subject, paired)/2", n, PASSES, {
            for i in 0..n {
                ready.push(black_box(queue), 0xBEEF, black_box(i as u64));
            }
            for _ in 0..n {
                black_box(ready.pop(black_box(queue)));
            }
        });
    }

    // ─── PendingNode stack construction ─────────────────────────────────
    // Just the struct init, no slab insert, no edge wiring.
    {
        measure_warm!("PendingNode struct init [stack only]", n, PASSES, {
            for i in 0..n {
                let p = PendingNode {
                    pending_id: PendingId(0),
                    seq: black_box(i as u64),
                    queue_id: QueueId(1),
                    consumer_id: ConsumerId(1),
                    subscription_id: SubscriptionId(1),
                    binding_id: BindingId(1),
                    connection_id: ConnectionId(100),
                    subject_hash: black_box(i as u32),
                    credits: [CreditEntry {
                        scope: CreditScope::Node,
                        _pad: [0; 3],
                        counter_idx: 0,
                    }; 3],
                    credit_count: 0,
                    deadline_id: 0,
                    delivered_at: Timestamp::new(1),
                    ack_wait_ns: 0,
                    edge_prev: [SlabKey::DANGLING; pending_edge_idx::COUNT],
                    edge_next: [SlabKey::DANGLING; pending_edge_idx::COUNT],
                };
                black_box(p);
            }
        });
    }

    // ─── TypedSlab<PendingNode>::insert ─────────────────────────────────
    // Insert + remove paired so slab stays at steady size.
    {
        let mut slab: TypedSlab<PendingNode> = TypedSlab::new();
        let template = PendingNode {
            pending_id: PendingId(0),
            seq: 0,
            queue_id: QueueId(1),
            consumer_id: ConsumerId(1),
            subscription_id: SubscriptionId(1),
            binding_id: BindingId(1),
            connection_id: ConnectionId(100),
            subject_hash: 0,
            credits: [CreditEntry {
                scope: CreditScope::Node,
                _pad: [0; 3],
                counter_idx: 0,
            }; 3],
            credit_count: 0,
            deadline_id: 0,
            delivered_at: Timestamp::new(1),
            ack_wait_ns: 0,
            edge_prev: [SlabKey::DANGLING; pending_edge_idx::COUNT],
            edge_next: [SlabKey::DANGLING; pending_edge_idx::COUNT],
        };
        measure_warm!("TypedSlab::insert+remove (paired)/2", n, PASSES, {
            let mut keys: Vec<SlabKey> = Vec::with_capacity(n);
            for _ in 0..n {
                let k = slab.insert(template.clone());
                keys.push(k);
            }
            for k in keys.drain(..) {
                let _ = slab.remove(k);
            }
        });
    }

    // ─── PendingEdge::insert_head (single edge) ─────────────────────────
    // Measures one of the four insert_head calls in isolation.
    // Paired with take() so the edge stays empty between passes.
    {
        let mut slab: TypedSlab<PendingNode> = TypedSlab::new();
        let mut edge: PendingEdge<ConnectionId> =
            PendingEdge::new(pending_edge_idx::CONNECTION);
        let template = make_template_pending();
        // Pre-allocate slab slots that we reuse each pass.
        let mut keys: Vec<SlabKey> = (0..n).map(|_| slab.insert(template.clone())).collect();
        measure_warm!("PendingEdge::insert_head (single edge)", n, PASSES, {
            for (i, &k) in keys.iter().enumerate() {
                edge.insert_head(&mut slab, ConnectionId((i as u64) & 63), k);
            }
            // Drain the edges to reset state — NOT timed (the closing brace
            // of measure_warm! includes it, but it's the same cost every
            // pass so it averages into a stable offset).
            for i in 0..64u64 {
                edge.take(&mut slab, ConnectionId(i));
            }
        });
        // Cleanup
        for k in keys.drain(..) {
            let _ = slab.remove(k);
        }
    }

    // ─── 4× insert_head chained (the real claim pattern) ────────────────
    // Inserts the same key into 4 different PendingEdges back-to-back,
    // exactly as claim does. Compare (this / 4) against single-edge above
    // to see if the pattern is additive or has cross-edge interference.
    {
        let mut slab: TypedSlab<PendingNode> = TypedSlab::new();
        let mut e_conn: PendingEdge<ConnectionId> =
            PendingEdge::new(pending_edge_idx::CONNECTION);
        let mut e_cons: PendingEdge<ConsumerId> =
            PendingEdge::new(pending_edge_idx::CONSUMER);
        let mut e_queue: PendingEdge<QueueId> =
            PendingEdge::new(pending_edge_idx::QUEUE);
        let mut e_sub: PendingEdge<SubscriptionId> =
            PendingEdge::new(pending_edge_idx::SUBSCRIPTION);
        let template = make_template_pending();
        let mut keys: Vec<SlabKey> = (0..n).map(|_| slab.insert(template.clone())).collect();
        measure_warm!("4× insert_head chained (as claim does)", n, PASSES, {
            for (i, &k) in keys.iter().enumerate() {
                let idx32 = (i as u32) & 63;
                let idx64 = (i as u64) & 63;
                e_conn.insert_head(&mut slab, ConnectionId(idx64), k);
                e_cons.insert_head(&mut slab, ConsumerId(idx32), k);
                e_queue.insert_head(&mut slab, QueueId(idx32), k);
                e_sub.insert_head(&mut slab, SubscriptionId(idx32), k);
            }
            // Reset
            for i in 0..64u32 {
                e_conn.take(&mut slab, ConnectionId(i as u64));
                e_cons.take(&mut slab, ConsumerId(i));
                e_queue.take(&mut slab, QueueId(i));
                e_sub.take(&mut slab, SubscriptionId(i));
            }
        });
        for k in keys.drain(..) {
            let _ = slab.remove(k);
        }
    }
}

fn make_template_pending() -> PendingNode {
    PendingNode {
        pending_id: PendingId(0),
        seq: 0,
        queue_id: QueueId(1),
        consumer_id: ConsumerId(1),
        subscription_id: SubscriptionId(1),
        binding_id: BindingId(1),
        connection_id: ConnectionId(100),
        subject_hash: 0,
        credits: [CreditEntry {
            scope: CreditScope::Node,
            _pad: [0; 3],
            counter_idx: 0,
        }; 3],
        credit_count: 0,
        deadline_id: 0,
        delivered_at: Timestamp::new(1),
        ack_wait_ns: 0,
        edge_prev: [SlabKey::DANGLING; pending_edge_idx::COUNT],
        edge_next: [SlabKey::DANGLING; pending_edge_idx::COUNT],
    }
}

// ── PHASE 3: scaling sweep ──────────────────────────────────────────────────

fn phase_scaling() {
    section("SCALING — ns/msg vs batch size (100-pass avg)");
    for &n in &[1usize, 4, 10, 32, 100, 316, 1000, 3162, 10_000] {
        let mut engine = engine_simple(1, (n as u32).max(1) * 2);
        let msgs = Msgs::new(n);
        let publish_entries = msgs.publish_entries();
        let mut ack_scratch: Vec<AckEntry> = Vec::with_capacity(n);
        let claim_batch = ClaimBatch {
            queue_id: QueueId(1),
            connection_id: ConnectionId(100),
            consumer_id: ConsumerId(1),
            max_items: n.min(u16::MAX as usize) as u16,
            now: Timestamp::new(1),
        };
        let (sub, bind) =
            arbitro_engine::runtime::claim::resolve_ids_for_batch(engine.ctx(), &claim_batch);

        // Warmup
        for _ in 0..5 {
            engine.publish(&PublishBatch {
                stream_id: StreamId(1),
                entries: &publish_entries,
                now: Timestamp::new(1),
            });
            let claimed = engine.claim(&claim_batch, sub, bind);
            ack_scratch.clear();
            ack_scratch.extend(claimed.entries().iter().map(|e| AckEntry { seq: e.seq }));
            engine.ack(&AckBatch {
                entries: &ack_scratch,
                consumer_id: ConsumerId(1),
                now: Timestamp::new(2),
            });
        }

        // Measure
        let passes = (10_000 / n.max(1)).max(20).min(10_000);
        let mut total_pub = Duration::ZERO;
        let mut total_clm = Duration::ZERO;
        let mut total_ack = Duration::ZERO;
        for _ in 0..passes {
            let t = Instant::now();
            engine.publish(&PublishBatch {
                stream_id: StreamId(1),
                entries: &publish_entries,
                now: Timestamp::new(1),
            });
            total_pub += t.elapsed();

            let t = Instant::now();
            let claimed = engine.claim(&claim_batch, sub, bind);
            let claimed_len = claimed.entries().len();
            total_clm += t.elapsed();

            ack_scratch.clear();
            ack_scratch.extend(claimed.entries().iter().map(|e| AckEntry { seq: e.seq }));
            let _ = claimed_len;

            let t = Instant::now();
            engine.ack(&AckBatch {
                entries: &ack_scratch,
                consumer_id: ConsumerId(1),
                now: Timestamp::new(2),
            });
            total_ack += t.elapsed();
        }
        let denom = (n * passes) as f64;
        let pub_ns = total_pub.as_nanos() as f64 / denom;
        let clm_ns = total_clm.as_nanos() as f64 / denom;
        let ack_ns = total_ack.as_nanos() as f64 / denom;
        let total = pub_ns + clm_ns + ack_ns;
        println!(
            "  N={:<6} publish={:>7.1}  claim={:>7.1}  ack={:>7.1}  total={:>7.1} ns/msg",
            n, pub_ns, clm_ns, ack_ns, total
        );
    }
}

// ── main ────────────────────────────────────────────────────────────────────

fn main() {
    println!("trace_time — granular publish/claim/ack breakdown");
    println!(
        "Compiled with: opt-level={}, debug_assertions={}",
        if cfg!(debug_assertions) { "(debug!)" } else { "3" },
        cfg!(debug_assertions)
    );
    if cfg!(debug_assertions) {
        println!("\n⚠️  Running in DEBUG mode — numbers are meaningless.");
        println!("    Re-run with: cargo run --example trace_time --release\n");
    }

    calibrate_clock();

    phase_full_cycle(10);
    phase_full_cycle(100);
    phase_full_cycle(1000);

    phase_sub_micro(100_000);

    phase_claim_internals(1000);

    phase_scaling();

    println!("\ndone.");
}
