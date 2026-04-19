//! Micro-bench for the `local_delta` helpers used inside the drain cycle.
//!
//! The drain keeps per-cycle pending counts for consumer_id and subject_hash
//! so it doesn't over-commit against atomic counters that haven't been
//! updated yet. It uses a plain `Vec<(u32, u32)>` scanned linearly because
//! the typical N per cycle is small (≤ 10 unique keys).
//!
//! This bench measures that choice vs two alternatives at realistic sizes:
//!
//!   1. Vec<(u32, u32)> linear scan       — current implementation
//!   2. HashMap<u32, u32, ahash>          — O(1) with hash cost
//!   3. Box<[u32]> direct index by key    — O(1) no hash, only feasible
//!                                          when keys are dense and bounded
//!
//! Workload per sample: interleaved `get` + `inc` calls, 1:1 ratio,
//! simulating how the drain touches each unique key inside dispatch.
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench local_delta -p arbitro-server --no-run"
//!   wsl bash -lc "cp .../target/release/deps/local_delta-* /tmp/arbitro-bench/ \
//!     && cd /tmp/arbitro-bench && ./local_delta-* --bench"

#![allow(unused)]

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

const OPS_PER_RUN: usize = 10_000_000;

// ── Current implementation — verbatim from drain.rs ─────────────────────────

#[inline]
fn vec_get(list: &[(u32, u32)], key: u32) -> u32 {
    for &(k, v) in list.iter() {
        if k == key {
            return v;
        }
    }
    0
}

#[inline]
fn vec_inc(list: &mut Vec<(u32, u32)>, key: u32) {
    for e in list.iter_mut() {
        if e.0 == key {
            e.1 += 1;
            return;
        }
    }
    list.push((key, 1));
}

// ── RNG ─────────────────────────────────────────────────────────────────────

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }
    #[inline]
    fn next_u32(&mut self) -> u32 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x as u32
    }
}

// ── Key generation ──────────────────────────────────────────────────────────

/// Generate the access pattern: a stream of u32 keys drawn from a set of `n`
/// distinct values. Mimics how a cycle touches a small number of unique
/// consumers/subjects many times over.
fn gen_access_pattern(n: usize, ops: usize) -> Vec<u32> {
    let mut rng = Rng::new(0xC0FFEE);
    let distinct: Vec<u32> =
        (0..n).map(|i| (i as u32) * 7 + 1).collect();
    (0..ops)
        .map(|_| distinct[(rng.next_u32() as usize) % distinct.len()])
        .collect()
}

// ── Benches ─────────────────────────────────────────────────────────────────

fn bench_vec(n: usize, pattern: &[u32]) -> f64 {
    let mut list: Vec<(u32, u32)> = Vec::with_capacity(n);
    // warmup
    for &k in pattern.iter().take(10_000) {
        black_box(vec_get(&list, k));
        vec_inc(&mut list, k);
    }
    list.clear();

    let start = Instant::now();
    // Interleave get+inc 1:1
    for &k in pattern {
        black_box(vec_get(&list, k));
        vec_inc(&mut list, k);
    }
    let elapsed = start.elapsed();
    elapsed.as_nanos() as f64 / (pattern.len() as f64 * 2.0) // 2 ops per iter
}

fn bench_hashmap(n: usize, pattern: &[u32]) -> f64 {
    let mut map: HashMap<u32, u32, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(n, ahash::RandomState::new());
    for &k in pattern.iter().take(10_000) {
        black_box(map.get(&k).copied().unwrap_or(0));
        *map.entry(k).or_insert(0) += 1;
    }
    map.clear();

    let start = Instant::now();
    for &k in pattern {
        black_box(map.get(&k).copied().unwrap_or(0));
        *map.entry(k).or_insert(0) += 1;
    }
    let elapsed = start.elapsed();
    elapsed.as_nanos() as f64 / (pattern.len() as f64 * 2.0)
}

fn bench_direct(n: usize, pattern: &[u32]) -> (f64, usize) {
    // Key range derived from the generator: (i * 7 + 1), i in 0..n.
    let max_key = if n == 0 { 1 } else { (n as u32 - 1) * 7 + 1 };
    let mut arr: Box<[u32]> = vec![0u32; max_key as usize + 1].into_boxed_slice();
    let mem_bytes = std::mem::size_of_val(&*arr);
    for &k in pattern.iter().take(10_000) {
        black_box(arr[k as usize]);
        arr[k as usize] += 1;
    }
    for slot in arr.iter_mut() {
        *slot = 0;
    }

    let start = Instant::now();
    for &k in pattern {
        black_box(arr[k as usize]);
        arr[k as usize] += 1;
    }
    let elapsed = start.elapsed();
    (elapsed.as_nanos() as f64 / (pattern.len() as f64 * 2.0), mem_bytes)
}

fn main() {
    println!();
    println!("========================================================");
    println!("           local_delta helpers — micro-bench");
    println!("========================================================");
    println!("  ops per sample: {OPS_PER_RUN} get+inc pairs (2 ops each).");
    println!("  N = unique keys (consumer_id or subject_hash) per cycle.");
    println!();
    println!(
        "  {:<6} | {:>10} | {:>10} | {:>18} | {:>10}",
        "N", "Vec scan", "HashMap+ahash", "Box[] direct", "direct mem"
    );
    println!("{}", "-".repeat(68));

    for &n in &[1usize, 2, 4, 8, 16, 32, 64, 128] {
        let pattern = gen_access_pattern(n, OPS_PER_RUN);

        let t_vec = bench_vec(n, &pattern);
        let t_hm = bench_hashmap(n, &pattern);
        let (t_dir, mem) = bench_direct(n, &pattern);

        println!(
            "  {n:<6} | {t_vec:>7.2} ns | {t_hm:>10.2} ns | {t_dir:>15.2} ns | {mem:>8} B"
        );
    }
    println!();
    println!("  (time is per single op — get or inc — averaged over pairs.)");
    println!();
}
