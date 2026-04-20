//! Accumulator grouping strategies — realistic drain cycle.
//!
//! Simulates **one drain cycle**: 256 messages pulled from the store,
//! each belonging to one of 8 active streams (of 64 total). Each stream
//! owns ~16 bindings (consumer × connection × subject pattern). For each
//! message, resolve the full target set `(conn, consumer, subject_hash,
//! payload)` and push it into the Accumulator.
//!
//! Three strategies, same workload, same output:
//!
//! **A — Flat HashMaps (current-ish)**
//!   `HashMap<StreamId, Vec<BindingId>, ahash>` +
//!   `HashMap<BindingId, Binding, ahash>`.
//!   Per message: 1 stream probe + N binding probes.
//!
//! **B — Nested Vec (stream owns bindings inline)**
//!   `Vec<StreamEntry>` dense-indexed by stream_id; each entry holds
//!   `Box<[Binding]>`. Per message: 1 direct index load + linear scan
//!   over the inline bindings array.
//!
//! **C — Nested Vec + pre-group by stream**
//!   Before resolving, bucket the 256 messages by stream_id into a
//!   scratch `Vec<Vec<MsgRef>>`. Then for each stream: one index load,
//!   iterate its msgs against the (now hot) bindings array.
//!
//! Measures: ns per drain cycle, ns per message resolved, ns per emitted
//! target (what finally goes into the accumulator).
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench accumulator_grouping -p arbitro-server --no-run"
//!   wsl bash -lc "
//!     mkdir -p /tmp/arbitro &&
//!     cp -a target/release/deps/accumulator_grouping-* /tmp/arbitro/ &&
//!     cd /tmp/arbitro &&
//!     timeout 120 ./accumulator_grouping-<hash> --bench 2>&1 | tee /tmp/bench.log
//!   "

#![allow(unused)]

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

use ahash::RandomState as AHashState;

// ── Xorshift RNG ────────────────────────────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed.max(1)) }
    #[inline]
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    #[inline] fn range(&mut self, n: u32) -> u32 { (self.next() as u32) % n }
}

// ── Workload params ─────────────────────────────────────────────────────────

const TOTAL_STREAMS: usize   = 64;       // streams in the server
const ACTIVE_STREAMS: usize  = 8;        // streams hit in this drain cycle
const BINDINGS_PER_STREAM: usize = 16;   // consumers × conns per stream
const MSGS_PER_CYCLE: usize  = 256;      // drain batch
const CYCLES: usize          = 100_000;  // iterations

// Realistic subject-hash match rate: ~50% of bindings match a given msg
// (bindings have subject filters; mixed wildcard + exact).
const MATCH_MASK: u32 = 0x1;             // match if (sub_hash ^ bind_hash) & 0x1 == 0

// ── Types ───────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct Binding {
    connection_id: u64,
    consumer_id:   u32,
    subject_hash:  u32,  // filter pattern hash
    fire_and_forget: bool,
}

#[derive(Clone, Copy)]
struct Msg {
    stream_id:    u32,
    seq:          u64,
    subject_hash: u32,
}

// ── A. Flat HashMaps ────────────────────────────────────────────────────────

struct FlatIndex {
    // stream_id → list of binding_ids for that stream
    stream_bindings: HashMap<u32, Vec<u32>, AHashState>,
    // binding_id → binding
    bindings: HashMap<u32, Binding, AHashState>,
}

impl FlatIndex {
    fn build(streams: &[Vec<Binding>]) -> Self {
        let mut stream_bindings: HashMap<u32, Vec<u32>, AHashState> =
            HashMap::with_hasher(AHashState::new());
        let mut bindings: HashMap<u32, Binding, AHashState> =
            HashMap::with_hasher(AHashState::new());
        let mut next_bid: u32 = 1;
        for (sid, binds) in streams.iter().enumerate() {
            let mut ids = Vec::with_capacity(binds.len());
            for b in binds {
                bindings.insert(next_bid, *b);
                ids.push(next_bid);
                next_bid += 1;
            }
            stream_bindings.insert(sid as u32, ids);
        }
        Self { stream_bindings, bindings }
    }

    /// Resolve `msgs` → emit targets into `out`.
    #[inline(never)]
    fn resolve(&self, msgs: &[Msg], out: &mut Vec<(u64, u32, u32, u64)>) {
        out.clear();
        for m in msgs {
            let bids = match self.stream_bindings.get(&m.stream_id) {
                Some(v) => v,
                None => continue,
            };
            for bid in bids {
                let b = unsafe { self.bindings.get(bid).unwrap_unchecked() };
                // subject match — simple xor mask, simulates trie/pattern match
                if (m.subject_hash ^ b.subject_hash) & MATCH_MASK == 0 {
                    out.push((b.connection_id, b.consumer_id, m.subject_hash, m.seq));
                }
            }
        }
    }
}

// ── B. Nested Vec — stream owns bindings inline ─────────────────────────────

struct StreamEntry {
    bindings: Box<[Binding]>,
}

struct NestedIndex {
    streams: Box<[StreamEntry]>,  // dense by stream_id
}

impl NestedIndex {
    fn build(streams: &[Vec<Binding>]) -> Self {
        let v: Vec<StreamEntry> = streams.iter()
            .map(|b| StreamEntry { bindings: b.clone().into_boxed_slice() })
            .collect();
        Self { streams: v.into_boxed_slice() }
    }

    /// Per-message resolution, no pre-grouping. One direct index load
    /// per message, then inline array scan.
    #[inline(never)]
    fn resolve_flat(&self, msgs: &[Msg], out: &mut Vec<(u64, u32, u32, u64)>) {
        out.clear();
        for m in msgs {
            let entry = unsafe { self.streams.get_unchecked(m.stream_id as usize) };
            for b in entry.bindings.iter() {
                if (m.subject_hash ^ b.subject_hash) & MATCH_MASK == 0 {
                    out.push((b.connection_id, b.consumer_id, m.subject_hash, m.seq));
                }
            }
        }
    }
}

// ── C. Nested + pre-grouped by stream ───────────────────────────────────────

struct PreGroupScratch {
    /// `(stream_id, msg_idx)` pairs, sorted by stream_id so same-stream
    /// msgs are contiguous. Radix-like bucket: `bucket_offsets[sid]` →
    /// start index into `sorted_msgs`.
    sorted_msgs:  Vec<Msg>,
    bucket_heads: Vec<u32>,  // bucket_heads[sid] = count for that stream
}

impl PreGroupScratch {
    fn new() -> Self {
        Self {
            sorted_msgs: Vec::with_capacity(MSGS_PER_CYCLE),
            bucket_heads: vec![0; TOTAL_STREAMS],
        }
    }
}

impl NestedIndex {
    /// Pre-group msgs by stream, then resolve stream-by-stream. Bindings
    /// stay hot in L1 for the full inner loop over each stream's msgs.
    #[inline(never)]
    fn resolve_grouped(
        &self,
        msgs: &[Msg],
        scratch: &mut PreGroupScratch,
        out: &mut Vec<(u64, u32, u32, u64)>,
    ) {
        out.clear();

        // Bucket pass: count per stream, then scatter.
        for c in scratch.bucket_heads.iter_mut() { *c = 0; }
        for m in msgs { scratch.bucket_heads[m.stream_id as usize] += 1; }

        // Prefix sum → offsets
        let mut offsets: [u32; TOTAL_STREAMS] = [0; TOTAL_STREAMS];
        let mut acc: u32 = 0;
        for (i, &c) in scratch.bucket_heads.iter().enumerate() {
            offsets[i] = acc;
            acc += c;
        }

        // Scatter msgs in stream-sorted order
        scratch.sorted_msgs.clear();
        scratch.sorted_msgs.resize(msgs.len(), Msg { stream_id: 0, seq: 0, subject_hash: 0 });
        let mut cursor = offsets;
        for m in msgs {
            let slot = cursor[m.stream_id as usize] as usize;
            scratch.sorted_msgs[slot] = *m;
            cursor[m.stream_id as usize] += 1;
        }

        // Walk stream-by-stream: 1 load per stream, inline array hot.
        for sid in 0..TOTAL_STREAMS {
            let count = scratch.bucket_heads[sid] as usize;
            if count == 0 { continue; }
            let start = offsets[sid] as usize;
            let entry = unsafe { self.streams.get_unchecked(sid) };
            let binds = &entry.bindings;
            for m in &scratch.sorted_msgs[start..start + count] {
                for b in binds.iter() {
                    if (m.subject_hash ^ b.subject_hash) & MATCH_MASK == 0 {
                        out.push((b.connection_id, b.consumer_id, m.subject_hash, m.seq));
                    }
                }
            }
        }
    }
}

// ── Dataset generation ──────────────────────────────────────────────────────

fn build_streams(rng: &mut Rng) -> Vec<Vec<Binding>> {
    let mut streams = Vec::with_capacity(TOTAL_STREAMS);
    let mut next_conn: u64 = 1;
    let mut next_cons: u32 = 1;
    for _ in 0..TOTAL_STREAMS {
        let mut b = Vec::with_capacity(BINDINGS_PER_STREAM);
        for _ in 0..BINDINGS_PER_STREAM {
            b.push(Binding {
                connection_id: next_conn,
                consumer_id:   next_cons,
                subject_hash:  rng.next() as u32,
                fire_and_forget: (rng.next() & 0b11) == 0,
            });
            next_conn += 1;
            next_cons += 1;
        }
        streams.push(b);
    }
    streams
}

fn build_cycle_msgs(rng: &mut Rng) -> Vec<Msg> {
    // Pick 8 active streams out of 64, then distribute 256 msgs over them.
    // Skewed distribution (some streams get more msgs), closer to reality.
    let mut active: Vec<u32> = (0..ACTIVE_STREAMS as u32)
        .map(|_| rng.range(TOTAL_STREAMS as u32))
        .collect();
    active.sort_unstable();
    active.dedup();
    // Top up if dedup dropped any
    while active.len() < ACTIVE_STREAMS {
        let s = rng.range(TOTAL_STREAMS as u32);
        if !active.contains(&s) { active.push(s); }
    }

    let mut msgs = Vec::with_capacity(MSGS_PER_CYCLE);
    for i in 0..MSGS_PER_CYCLE {
        let sid = active[i % active.len()];  // round-robin-ish
        msgs.push(Msg {
            stream_id:    sid,
            seq:          i as u64,
            subject_hash: rng.next() as u32,
        });
    }
    msgs
}

// ── Benchmark loop ──────────────────────────────────────────────────────────

fn run_flat(idx: &FlatIndex, cycles: &[Vec<Msg>]) -> (f64, usize) {
    let mut out = Vec::with_capacity(MSGS_PER_CYCLE * BINDINGS_PER_STREAM);
    // Warmup
    for c in cycles.iter().take(100) { idx.resolve(c, &mut out); black_box(&out); }

    let mut total_targets = 0usize;
    let start = Instant::now();
    for c in cycles {
        idx.resolve(c, &mut out);
        total_targets = total_targets.wrapping_add(out.len());
        black_box(&out);
    }
    let elapsed = start.elapsed();
    (elapsed.as_nanos() as f64 / cycles.len() as f64, total_targets)
}

fn run_nested_flat(idx: &NestedIndex, cycles: &[Vec<Msg>]) -> (f64, usize) {
    let mut out = Vec::with_capacity(MSGS_PER_CYCLE * BINDINGS_PER_STREAM);
    for c in cycles.iter().take(100) { idx.resolve_flat(c, &mut out); black_box(&out); }

    let mut total_targets = 0usize;
    let start = Instant::now();
    for c in cycles {
        idx.resolve_flat(c, &mut out);
        total_targets = total_targets.wrapping_add(out.len());
        black_box(&out);
    }
    let elapsed = start.elapsed();
    (elapsed.as_nanos() as f64 / cycles.len() as f64, total_targets)
}

fn run_nested_grouped(idx: &NestedIndex, cycles: &[Vec<Msg>]) -> (f64, usize) {
    let mut out = Vec::with_capacity(MSGS_PER_CYCLE * BINDINGS_PER_STREAM);
    let mut scratch = PreGroupScratch::new();
    for c in cycles.iter().take(100) {
        idx.resolve_grouped(c, &mut scratch, &mut out);
        black_box(&out);
    }

    let mut total_targets = 0usize;
    let start = Instant::now();
    for c in cycles {
        idx.resolve_grouped(c, &mut scratch, &mut out);
        total_targets = total_targets.wrapping_add(out.len());
        black_box(&out);
    }
    let elapsed = start.elapsed();
    (elapsed.as_nanos() as f64 / cycles.len() as f64, total_targets)
}

fn main() {
    println!("\nAccumulator grouping — realistic drain cycle");
    println!("============================================");
    println!(
        "total_streams={TOTAL_STREAMS}  active_per_cycle={ACTIVE_STREAMS}  \
         bindings_per_stream={BINDINGS_PER_STREAM}  msgs_per_cycle={MSGS_PER_CYCLE}"
    );
    println!("cycles={CYCLES}  match_rate≈50%\n");

    let mut rng = Rng::new(0xC0FFEE);
    let streams = build_streams(&mut rng);

    // Pre-generate all cycles so dataset cost is outside the timed region.
    let cycles: Vec<Vec<Msg>> = (0..CYCLES)
        .map(|_| build_cycle_msgs(&mut rng))
        .collect();

    let flat    = FlatIndex::build(&streams);
    let nested  = NestedIndex::build(&streams);

    let (ns_a, t_a) = run_flat(&flat, &cycles);
    let (ns_b, t_b) = run_nested_flat(&nested, &cycles);
    let (ns_c, t_c) = run_nested_grouped(&nested, &cycles);

    assert_eq!(t_a, t_b, "A and B must emit the same target set");
    assert_eq!(t_a, t_c, "A and C must emit the same target set");

    let targets_per_cycle = t_a as f64 / CYCLES as f64;

    println!(
        "{:<34} | {:>12} | {:>13} | {:>12} | {:>8}",
        "Strategy", "ns / cycle", "ns / msg", "ns / target", "vs A"
    );
    println!("{}", "-".repeat(98));

    let rows = [
        ("A — Flat HashMaps (ahash)",                 ns_a),
        ("B — Nested Vec (per-msg index)",            ns_b),
        ("C — Nested Vec + pre-group by stream",      ns_c),
    ];
    for (label, ns) in rows {
        let per_msg    = ns / MSGS_PER_CYCLE as f64;
        let per_target = ns / targets_per_cycle;
        let ratio      = ns_a / ns;
        println!(
            "{:<34} | {:>9.0} ns | {:>10.2} ns | {:>9.2} ns | {:>6.2}×",
            label, ns, per_msg, per_target, ratio
        );
    }

    println!();
    println!("Targets emitted per cycle (avg): {:.1}", targets_per_cycle);
    println!(
        "Throughput (msgs/s): A={:.1}M  B={:.1}M  C={:.1}M",
        (MSGS_PER_CYCLE as f64 * 1e9 / ns_a) / 1e6,
        (MSGS_PER_CYCLE as f64 * 1e9 / ns_b) / 1e6,
        (MSGS_PER_CYCLE as f64 * 1e9 / ns_c) / 1e6,
    );
    println!();
}
