//! bench_entry_build — temporary bench to decide the best way to assemble
//! `Vec<PublishEntry>` for publish batches. Tests four strategies against the
//! same input data at the same batch sizes.
//!
//! Strategies:
//!   A) `.map().collect()`                            — current baseline
//!   B) `Vec::with_capacity() + push`                 — should match A in asm
//!   C) pre-fill once, overwrite ALL fields per call  — expected WORST (2× writes)
//!   D) pre-fill once, overwrite ONLY (hash, subject) — expected BEST if payload
//!      is constant across the batch (real zero-copy edit pattern)
//!   E) pre-fill once, overwrite via unsafe raw ptr writes — absolute floor
//!
//! Run from WSL, in place:
//!   wsl bash -lc 'cd /mnt/d/.../arbitro-engine && \
//!     cargo build --example bench_entry_build --release && \
//!     timeout 120 ./target/release/examples/bench_entry_build'

use std::hint::black_box;
use std::time::Instant;

use arbitro_engine::batch::PublishEntry;
use arbitro_engine::types::PayloadRef;

const WARMUP_PASSES: usize = 3;
const MEASURE_PASSES: usize = 100;
const SIZES: &[usize] = &[1, 4, 10, 32, 100, 316, 1000, 3162, 10_000];

/// Source data — like what a real caller would hold.
struct Input {
    subjects: Vec<Vec<u8>>,
    hashes: Vec<u32>,
    payload: &'static [u8],
}

impl Input {
    fn new(n: usize) -> Self {
        let subjects: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("trace.subj.{i}").into_bytes())
            .collect();
        // cheap rolling hash — doesn't matter, just needs to be N values
        let hashes: Vec<u32> = (0..n as u32).map(|i| i.wrapping_mul(0x9E37_79B1)).collect();
        Self {
            subjects,
            hashes,
            payload: b"trace-payload-64B-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        }
    }
}

// ── Strategy A: .map().collect() ────────────────────────────────────────────
#[inline(never)]
fn build_a<'a>(inp: &'a Input) -> Vec<PublishEntry<'a>> {
    inp.subjects
        .iter()
        .zip(inp.hashes.iter())
        .map(|(s, h)| PublishEntry {
            subject_hash: *h,
            subject: s,
            payload: PayloadRef::Borrowed(inp.payload),
            idempotency_key: 0,
            credits_cost: 1,
        })
        .collect()
}

// ── Strategy B: Vec::with_capacity + push ───────────────────────────────────
#[inline(never)]
fn build_b<'a>(inp: &'a Input) -> Vec<PublishEntry<'a>> {
    let n = inp.subjects.len();
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        v.push(PublishEntry {
            subject_hash: inp.hashes[i],
            subject: &inp.subjects[i],
            payload: PayloadRef::Borrowed(inp.payload),
            idempotency_key: 0,
            credits_cost: 1,
        });
    }
    v
}

// ── Strategy C: pre-fill, overwrite ALL fields in place ─────────────────────
#[inline(never)]
fn build_c<'a>(inp: &'a Input, scratch: &mut Vec<PublishEntry<'a>>) {
    let n = inp.subjects.len();
    // Grow scratch to n entries if needed.
    while scratch.len() < n {
        scratch.push(PublishEntry {
            subject_hash: 0,
            subject: &[],
            payload: PayloadRef::Borrowed(inp.payload),
            idempotency_key: 0,
            credits_cost: 1,
        });
    }
    for i in 0..n {
        scratch[i] = PublishEntry {
            subject_hash: inp.hashes[i],
            subject: &inp.subjects[i],
            payload: PayloadRef::Borrowed(inp.payload),
            idempotency_key: 0,
            credits_cost: 1,
        };
    }
}

// ── Strategy D: pre-fill, overwrite ONLY (hash, subject) ────────────────────
// Constant fields written once during template prep; per-call touches only
// what actually differs per message.
#[inline(never)]
fn build_d<'a>(inp: &'a Input, scratch: &mut Vec<PublishEntry<'a>>) {
    let n = inp.subjects.len();
    while scratch.len() < n {
        scratch.push(PublishEntry {
            subject_hash: 0,
            subject: &[],
            payload: PayloadRef::Borrowed(inp.payload),
            idempotency_key: 0,
            credits_cost: 1,
        });
    }
    for i in 0..n {
        // Only fields that vary per message.
        scratch[i].subject_hash = inp.hashes[i];
        scratch[i].subject = &inp.subjects[i];
    }
}

// ── Strategy E: pre-fill + unsafe ptr writes (absolute floor) ───────────────
#[inline(never)]
fn build_e<'a>(inp: &'a Input, scratch: &mut Vec<PublishEntry<'a>>) {
    let n = inp.subjects.len();
    while scratch.len() < n {
        scratch.push(PublishEntry {
            subject_hash: 0,
            subject: &[],
            payload: PayloadRef::Borrowed(inp.payload),
            idempotency_key: 0,
            credits_cost: 1,
        });
    }
    let base = scratch.as_mut_ptr();
    unsafe {
        for i in 0..n {
            let slot = &mut *base.add(i);
            slot.subject_hash = *inp.hashes.get_unchecked(i);
            slot.subject = inp.subjects.get_unchecked(i);
        }
    }
}

// Timing macros — inlined so HRTB lifetime issues don't bite closures.
macro_rules! time_fresh {
    ($inp:expr, $build_fn:ident) => {{
        let inp = $inp;
        for _ in 0..WARMUP_PASSES {
            black_box($build_fn(inp));
        }
        let t0 = Instant::now();
        for _ in 0..MEASURE_PASSES {
            let v = $build_fn(inp);
            black_box(&v);
        }
        let elapsed = t0.elapsed().as_nanos() as f64;
        elapsed / (MEASURE_PASSES as f64) / (inp.subjects.len() as f64)
    }};
}

macro_rules! time_scratch {
    ($inp:expr, $build_fn:ident) => {{
        let inp = $inp;
        let mut scratch: Vec<PublishEntry> = Vec::with_capacity(inp.subjects.len());
        for _ in 0..WARMUP_PASSES {
            $build_fn(inp, &mut scratch);
            black_box(&scratch);
        }
        let t0 = Instant::now();
        for _ in 0..MEASURE_PASSES {
            $build_fn(inp, &mut scratch);
            black_box(&scratch);
        }
        let elapsed = t0.elapsed().as_nanos() as f64;
        elapsed / (MEASURE_PASSES as f64) / (inp.subjects.len() as f64)
    }};
}

fn main() {
    println!("bench_entry_build — Vec<PublishEntry> assembly strategies");
    println!("opt-level=3, debug_assertions=false\n");

    println!(
        "size_of::<PublishEntry>() = {} bytes",
        std::mem::size_of::<PublishEntry>()
    );
    println!(
        "align_of::<PublishEntry>() = {} bytes\n",
        std::mem::align_of::<PublishEntry>()
    );

    println!(
        "{:>6}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        "N", "A map", "B push", "C full", "D min", "E unsafe"
    );
    println!("{}", "─".repeat(60));

    for &n in SIZES {
        let inp = Input::new(n);
        let a = time_fresh!(&inp, build_a);
        let b = time_fresh!(&inp, build_b);
        let c = time_scratch!(&inp, build_c);
        let d = time_scratch!(&inp, build_d);
        let e = time_scratch!(&inp, build_e);
        println!(
            "{:>6}  {:>7.2}  {:>7.2}  {:>7.2}  {:>7.2}  {:>7.2}",
            n, a, b, c, d, e
        );
    }

    println!("\nLegend (ns/item, 100-pass avg):");
    println!("  A map      — .iter().zip().map(|..| PublishEntry {{..}}).collect()");
    println!("  B push     — Vec::with_capacity + for push  (expected ≈ A)");
    println!("  C full     — scratch reuse, overwrite ALL fields per call");
    println!("  D min      — scratch reuse, overwrite ONLY (subject_hash, subject)");
    println!("  E unsafe   — scratch reuse + unsafe get_unchecked/ptr writes");
    println!();
    println!("Interpretation:");
    println!("  A ≈ B              → map/collect compiles to the same as push loop");
    println!("  C < A              → scratch reuse saves alloc + realloc + cold-cache");
    println!("  D < C              → writing ~20B/item beats writing ~64B/item");
    println!("  E ≤ D              → manual bounds-check elimination gains are marginal");
    println!("  (D vs A)           → the actual win available from this optimization");
}
