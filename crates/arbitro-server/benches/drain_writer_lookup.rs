//! Drain writer-lookup faithful micro-benchmark.
//!
//! Mirrors the exact access pattern of `shard::drain::drain_cycle` Phase 2:
//! for each frame produced by the accumulator, look up the writer for its
//! connection and dereference every field that `write_all_blocking` touches.
//!
//! The existing `lookup_strategies` bench exaggerated the gap because it
//! used a 16-byte `WriterEntry { conn_id, payload: u64 }` and only touched
//! `.payload`. The real `WriterIndexEntry` is 32 bytes and the drain reads
//! `writer.writer`, `writer.write_lock`, `writer.runtime` — two Arc refs
//! plus a tokio Handle — on every frame.
//!
//! Workload shape:
//!   - N active connections, values fully populated.
//!   - `conn_id` is u64 monotonic (never recycled) as in production.
//!   - Lookups issued in frame-sorted order (as the accumulator produces).
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench drain_writer_lookup -p arbitro-server --no-run"
//!   wsl bash -lc "cp .../target/release/deps/drain_writer_lookup-* /tmp/w \
//!     && chmod +x /tmp/w && timeout 60 /tmp/w --bench"

#![allow(unused)]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::runtime::{Handle, Runtime};

// ── RNG ────────────────────────────────────────────────────────────────────

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
}

// ── Real-size entry ─────────────────────────────────────────────────────────
// Mirrors WriterIndexEntry in src/shard/shared.rs — 3 heap refs, 32 bytes.

#[derive(Clone)]
struct WriterIndexEntry {
    // Stand-in for Arc<OwnedWriteHalf>. Arc<T> = 8 bytes, NonNull ptr.
    writer: Arc<[u8; 64]>,
    // Stand-in for Arc<Mutex<()>>.
    write_lock: Arc<Mutex<()>>,
    // Real tokio::runtime::Handle is 16 bytes (internal Arc + spawner flag).
    runtime: Handle,
}

const _SIZE: () = assert!(std::mem::size_of::<WriterIndexEntry>() == 32);

// ── Workload ────────────────────────────────────────────────────────────────

const OPS: usize = 10_000_000;
const SIZES: &[usize] = &[100, 1_000, 10_000];

fn build_entries(n: usize, runtime: &Handle) -> (Vec<u64>, Vec<WriterIndexEntry>) {
    // Monotonic conn_ids (production pattern — ConnIdGen never recycles).
    // Simulate some churn: alive conns occupy IDs in a sparse range.
    let conn_id_max = (n as u64) * 2 + 1;
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
    let entries: Vec<WriterIndexEntry> = keys
        .iter()
        .map(|_| WriterIndexEntry {
            writer: Arc::new([0u8; 64]),
            write_lock: Arc::new(Mutex::new(())),
            runtime: runtime.clone(),
        })
        .collect();
    (keys, entries)
}

/// Frame-ordered lookup stream: the accumulator emits one frame per
/// connection per drain cycle, so lookups are spread evenly over all alive
/// conns. Simulate that by cycling through `keys` in shuffled order, with a
/// small fraction of misses (closed conn).
fn gen_frame_lookups(keys: &[u64], misses_pct: u32) -> Vec<u64> {
    let mut rng = Rng::new(0xDEAD);
    let max_key = *keys.last().unwrap_or(&1) * 2;
    let mut out = Vec::with_capacity(OPS);
    for _ in 0..OPS {
        let miss = (rng.next_u64() as u32) % 100 < misses_pct;
        if miss {
            out.push((rng.next_u64() % max_key) + max_key);
        } else {
            out.push(keys[(rng.next_u64() as usize) % keys.len()]);
        }
    }
    out
}

// ── Container: HashMap + foldhash (what the code uses today) ────────────────

fn bench_hashmap_foldhash(entries: &[WriterIndexEntry], keys: &[u64], lookups: &[u64]) -> f64 {
    let mut map: HashMap<u64, WriterIndexEntry, foldhash::fast::FixedState> =
        HashMap::with_capacity_and_hasher(entries.len(), foldhash::fast::FixedState::default());
    for (k, e) in keys.iter().zip(entries.iter()) {
        map.insert(*k, e.clone());
    }
    // Warmup
    for &k in lookups.iter().take(10_000) {
        if let Some(e) = map.get(&k) {
            black_box(&e.writer);
            black_box(&e.write_lock);
            black_box(&e.runtime);
        }
    }
    // Real drain access: fetch entry + touch every field write_all_blocking uses.
    let start = Instant::now();
    let mut sink = 0u64;
    for &k in lookups {
        if let Some(e) = map.get(&k) {
            sink = sink.wrapping_add(Arc::as_ptr(&e.writer) as u64);
            sink = sink.wrapping_add(Arc::as_ptr(&e.write_lock) as u64);
            black_box(&e.runtime);
        }
    }
    black_box(sink);
    start.elapsed().as_nanos() as f64 / lookups.len() as f64
}

// ── Container: Vec<Option<T>> direct index ──────────────────────────────────

fn bench_vec_direct(entries: &[WriterIndexEntry], keys: &[u64], lookups: &[u64]) -> (f64, usize) {
    let max_key = *keys.iter().max().unwrap_or(&0) as usize;
    let mut arr: Vec<Option<WriterIndexEntry>> = Vec::with_capacity(max_key + 1);
    arr.resize(max_key + 1, None);
    for (k, e) in keys.iter().zip(entries.iter()) {
        arr[*k as usize] = Some(e.clone());
    }
    let mem_bytes = arr.capacity() * std::mem::size_of::<Option<WriterIndexEntry>>();
    // Warmup
    for &k in lookups.iter().take(10_000) {
        let idx = k as usize;
        if idx < arr.len() {
            if let Some(e) = &arr[idx] {
                black_box(&e.writer);
                black_box(&e.write_lock);
                black_box(&e.runtime);
            }
        }
    }
    let start = Instant::now();
    let mut sink = 0u64;
    for &k in lookups {
        let idx = k as usize;
        if idx < arr.len() {
            if let Some(e) = &arr[idx] {
                sink = sink.wrapping_add(Arc::as_ptr(&e.writer) as u64);
                sink = sink.wrapping_add(Arc::as_ptr(&e.write_lock) as u64);
                black_box(&e.runtime);
            }
        }
    }
    black_box(sink);
    let ns = start.elapsed().as_nanos() as f64 / lookups.len() as f64;
    (ns, mem_bytes)
}

// ── Runner ──────────────────────────────────────────────────────────────────

fn main() {
    // tokio runtime is required for Handle::current()
    let rt = Runtime::new().expect("tokio runtime");
    let handle = rt.handle().clone();

    println!();
    println!("Drain writer-lookup faithful micro-benchmark");
    println!("============================================");
    println!("Entry: WriterIndexEntry (32 B, 2 Arc + Handle). Access pattern: drain Phase 2.");
    println!("Workload: {} lookups, 5% miss (closed conn).", OPS);
    println!();
    println!(
        "{:<12} | {:>18} | {:>22} | {:>10} | {:>10}",
        "N conns", "HashMap+foldhash", "Vec<Option<T>> direct", "Δ ns/op", "Vec mem"
    );
    println!("{}", "-".repeat(90));

    for &n in SIZES {
        let (keys, entries) = build_entries(n, &handle);
        let lookups = gen_frame_lookups(&keys, 5);
        let t_hm = bench_hashmap_foldhash(&entries, &keys, &lookups);
        let (t_vec, mem) = bench_vec_direct(&entries, &keys, &lookups);
        let delta = t_hm - t_vec;
        let mem_str = if mem >= 1024 * 1024 {
            format!("{} MB", mem / (1024 * 1024))
        } else {
            format!("{} KB", mem / 1024)
        };
        println!(
            "{:<12} | {:>14.2} ns | {:>18.2} ns | {:>7.2} ns | {:>10}",
            n, t_hm, t_vec, delta, mem_str
        );
    }

    println!();
    println!("Per-system impact:");
    println!("  Drain emits 1 lookup PER FRAME (not per message). Frames batch ~256 msgs.");
    println!("  At 4 M msg/s throughput → ~15.6 k lookups/s → savings in CPU are nanoscopic.");
    println!();
}
