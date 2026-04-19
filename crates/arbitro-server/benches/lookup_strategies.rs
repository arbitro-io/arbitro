//! Lookup strategies for drain hot-path indexes.
//!
//! Compares three candidates for `find_writer` (u64 → Writer) and
//! `find_binding_idx` ((u32, u64) → usize):
//!
//! 1. **HashMap + ahash** — O(1) amortised, bounded memory.
//! 2. **Box<[Option<T>]> direct index** — O(1) worst-case, memory =
//!    max_id × sizeof(Option<T>). Only practical for bounded IDs.
//! 3. **Sorted Vec + binary_search** — O(log n), minimal memory.
//!
//! Workload: 10_000_000 random lookups against a populated index of
//! size `N`, where keys are drawn from the populated set (hit) and a
//! spread range (miss ~5%).
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench lookup_strategies -p arbitro-server --no-run"
//!   wsl bash -lc "cp /mnt/.../target/release/deps/lookup_strategies-* \
//!     /tmp/arbitro-bench/ && cd /tmp/arbitro-bench && \
//!     ./lookup_strategies-* --bench"

#![allow(unused)]

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

// ── Shared RNG (xorshift, no external dep) ──────────────────────────────────

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }
    #[inline]
    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
    #[inline]
    fn next_u32(&mut self) -> u32 {
        self.next_u64() as u32
    }
}

// ── Workload ────────────────────────────────────────────────────────────────

const OPS: usize = 10_000_000;
const SIZES: &[usize] = &[10, 100, 1_000, 10_000];

// ── Value types ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct WriterEntry {
    conn_id: u64,
    payload: u64, // pretend this is an Arc ptr; keep it small so we fit in cache
}

#[derive(Clone, Copy)]
struct BindingEntry {
    consumer_id: u32,
    connection_id: u64,
    binding_idx: u32,
}

// ── find_writer: conn_id → WriterEntry ──────────────────────────────────────

fn build_writer_dataset(n: usize, conn_id_max: u64) -> (Vec<u64>, Vec<WriterEntry>) {
    let mut rng = Rng::new(0xC0FFEE);
    let mut keys = Vec::with_capacity(n);
    let mut used = std::collections::HashSet::new();
    while keys.len() < n {
        let k = (rng.next_u64() % conn_id_max).max(1);
        if used.insert(k) {
            keys.push(k);
        }
    }
    keys.sort();
    let entries: Vec<WriterEntry> = keys
        .iter()
        .map(|&k| WriterEntry { conn_id: k, payload: k.wrapping_mul(0x9E37) })
        .collect();
    (keys, entries)
}

fn gen_lookup_keys(keys: &[u64], misses_pct: u32, seed: u64) -> Vec<u64> {
    let mut rng = Rng::new(seed);
    let mut out = Vec::with_capacity(OPS);
    let max_key = *keys.last().unwrap_or(&1) * 2;
    for _ in 0..OPS {
        let miss = rng.next_u32() % 100 < misses_pct;
        if miss {
            out.push(rng.next_u64() % max_key + max_key);
        } else {
            out.push(keys[(rng.next_u32() as usize) % keys.len()]);
        }
    }
    out
}

fn bench_writer_hashmap(entries: &[WriterEntry], lookups: &[u64]) -> f64 {
    let mut map: HashMap<u64, WriterEntry, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(entries.len(), ahash::RandomState::new());
    for e in entries {
        map.insert(e.conn_id, *e);
    }
    // Warmup
    for &k in lookups.iter().take(10_000) {
        black_box(map.get(&k));
    }
    let start = Instant::now();
    let mut hits = 0u64;
    for &k in lookups {
        if let Some(e) = map.get(&k) {
            hits = hits.wrapping_add(e.payload);
        }
    }
    black_box(hits);
    let ns = start.elapsed().as_nanos() as f64 / lookups.len() as f64;
    ns
}

fn bench_writer_direct(entries: &[WriterEntry], lookups: &[u64]) -> (f64, usize) {
    let max_key = entries.iter().map(|e| e.conn_id).max().unwrap_or(0) as usize;
    let mut arr: Box<[Option<WriterEntry>]> = vec![None; max_key + 1].into_boxed_slice();
    for e in entries {
        arr[e.conn_id as usize] = Some(*e);
    }
    let mem_mb = std::mem::size_of_val(&*arr) / (1024 * 1024);
    // Warmup
    for &k in lookups.iter().take(10_000) {
        if (k as usize) < arr.len() {
            black_box(&arr[k as usize]);
        }
    }
    let start = Instant::now();
    let mut hits = 0u64;
    for &k in lookups {
        let idx = k as usize;
        if idx < arr.len() {
            if let Some(e) = &arr[idx] {
                hits = hits.wrapping_add(e.payload);
            }
        }
    }
    black_box(hits);
    let ns = start.elapsed().as_nanos() as f64 / lookups.len() as f64;
    (ns, mem_mb)
}

fn bench_writer_binsearch(entries: &[WriterEntry], lookups: &[u64]) -> f64 {
    // entries assumed already sorted by conn_id
    // Warmup
    for &k in lookups.iter().take(10_000) {
        black_box(entries.binary_search_by(|e| e.conn_id.cmp(&k)));
    }
    let start = Instant::now();
    let mut hits = 0u64;
    for &k in lookups {
        if let Ok(i) = entries.binary_search_by(|e| e.conn_id.cmp(&k)) {
            hits = hits.wrapping_add(entries[i].payload);
        }
    }
    black_box(hits);
    let ns = start.elapsed().as_nanos() as f64 / lookups.len() as f64;
    ns
}

// ── find_binding_idx: (consumer_id, connection_id) → binding_idx ────────────

fn build_binding_dataset(n: usize, cid_max: u32, conn_max: u64) -> Vec<BindingEntry> {
    let mut rng = Rng::new(0xBEEF);
    let mut out = Vec::with_capacity(n);
    let mut used = std::collections::HashSet::new();
    while out.len() < n {
        let cid = (rng.next_u32() % cid_max).max(1);
        let conn = (rng.next_u64() % conn_max).max(1);
        if used.insert((cid, conn)) {
            out.push(BindingEntry {
                consumer_id: cid,
                connection_id: conn,
                binding_idx: out.len() as u32,
            });
        }
    }
    out.sort_by(|a, b| a.consumer_id.cmp(&b.consumer_id).then(a.connection_id.cmp(&b.connection_id)));
    out
}

fn gen_binding_lookups(entries: &[BindingEntry], misses_pct: u32, seed: u64) -> Vec<(u32, u64)> {
    let mut rng = Rng::new(seed);
    let mut out = Vec::with_capacity(OPS);
    for _ in 0..OPS {
        let miss = rng.next_u32() % 100 < misses_pct;
        if miss {
            out.push((rng.next_u32(), rng.next_u64()));
        } else {
            let e = &entries[(rng.next_u32() as usize) % entries.len()];
            out.push((e.consumer_id, e.connection_id));
        }
    }
    out
}

fn bench_binding_hashmap(entries: &[BindingEntry], lookups: &[(u32, u64)]) -> f64 {
    let mut map: HashMap<(u32, u64), u32, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(entries.len(), ahash::RandomState::new());
    for e in entries {
        map.insert((e.consumer_id, e.connection_id), e.binding_idx);
    }
    for &k in lookups.iter().take(10_000) {
        black_box(map.get(&k));
    }
    let start = Instant::now();
    let mut hits = 0u64;
    for &k in lookups {
        if let Some(&i) = map.get(&k) {
            hits = hits.wrapping_add(i as u64);
        }
    }
    black_box(hits);
    start.elapsed().as_nanos() as f64 / lookups.len() as f64
}

fn bench_binding_binsearch(entries: &[BindingEntry], lookups: &[(u32, u64)]) -> f64 {
    for &k in lookups.iter().take(10_000) {
        black_box(entries.binary_search_by(|e| {
            e.consumer_id.cmp(&k.0).then(e.connection_id.cmp(&k.1))
        }));
    }
    let start = Instant::now();
    let mut hits = 0u64;
    for &k in lookups {
        if let Ok(i) = entries.binary_search_by(|e| {
            e.consumer_id.cmp(&k.0).then(e.connection_id.cmp(&k.1))
        }) {
            hits = hits.wrapping_add(entries[i].binding_idx as u64);
        }
    }
    black_box(hits);
    start.elapsed().as_nanos() as f64 / lookups.len() as f64
}

// Direct 2D is impractical for (u32, u64), so we skip it. But we do include
// a 1D dense scheme where keys are packed with `(consumer_id as u64) << 40 |
// connection_id_mod` into a direct Box — only works if both IDs are bounded.
// For realism, we keep just HashMap vs binary_search.

// ── Runner ──────────────────────────────────────────────────────────────────

fn main() {
    println!("\nLookup strategies — hot-path index micro-benchmark");
    println!("==================================================");
    println!("Workload: {OPS} lookups, 5% miss rate.\n");

    // ── find_writer (u64 → WriterEntry) ─────────────────────────────────
    println!("── find_writer (conn_id: u64 → WriterEntry) ──");
    println!(
        "{:<16} | {:>12} | {:>18} | {:>14}",
        "N (conns)", "HashMap+ahash", "Box<[Option<T>]> direct", "binary_search"
    );
    println!("{}", "-".repeat(72));

    for &n in SIZES {
        // Two density regimes: dense (conn_id_max ≈ n) and sparse (max = 10*n)
        for (density_label, max_mul) in [("dense", 1u64), ("sparse", 10u64)] {
            let conn_id_max = (n as u64) * max_mul + 1;
            let (keys, entries) = build_writer_dataset(n, conn_id_max);
            let lookups = gen_lookup_keys(&keys, 5, 0xDEAD);

            let t_hm = bench_writer_hashmap(&entries, &lookups);
            let (t_dir, mem_mb) = bench_writer_direct(&entries, &lookups);
            let t_bs = bench_writer_binsearch(&entries, &lookups);

            println!(
                "{:<16} | {:>9.1} ns | {:>9.1} ns ({:>3} MB) | {:>11.1} ns",
                format!("{} {}", n, density_label),
                t_hm,
                t_dir,
                mem_mb,
                t_bs
            );
        }
    }

    // ── find_binding_idx ((u32, u64) → u32) ─────────────────────────────
    println!();
    println!("── find_binding_idx ((consumer_id, connection_id) → binding_idx) ──");
    println!(
        "{:<16} | {:>12} | {:>14}",
        "N (bindings)", "HashMap+ahash", "binary_search"
    );
    println!("{}", "-".repeat(52));

    for &n in SIZES {
        let entries = build_binding_dataset(n, 100, 10_000);
        let lookups = gen_binding_lookups(&entries, 5, 0xBEEF);
        let t_hm = bench_binding_hashmap(&entries, &lookups);
        let t_bs = bench_binding_binsearch(&entries, &lookups);
        println!(
            "{:<16} | {:>9.1} ns | {:>11.1} ns",
            n, t_hm, t_bs
        );
    }

    println!();
}
