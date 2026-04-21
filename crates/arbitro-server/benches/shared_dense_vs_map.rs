//! Shared dense container vs HashMap — multi-thread read contention.
//!
//! Models the real scenario: many drain threads (one per shard or more)
//! reading from the same Connection/Stream/Consumer table concurrently.
//! Writes (bind/unbind) are rare vs reads (delivery per-message).
//!
//! Contenders:
//!
//!   A0 Box<[Entry]> owned          — single-thread direct access (real shard)
//!   A  Arc<Box<[Entry]>>          — immutable dense array, zero sync on read
//!   B  Arc<RwLock<Vec<Entry>>>    — read-shared, write-exclusive dense array
//!   C  Arc<Mutex<HashMap>>        — std hasher (SipHash), single lock
//!   D  Arc<RwLock<HashMap,foldhash>> — foldhash, reader-writer lock
//!   E  papaya::HashMap<u32, _, foldhash> — lock-free concurrent
//!   F  DashMap<u32, _, foldhash>  — sharded locks
//!
//! Workloads:
//!
//!   W1  100% reads, 1/4/8/16 threads
//!   W2  99% reads / 1% writes
//!   W3  90% reads / 10% writes
//!
//! Entry size = 32 B to mimic a cache-friendly Topology node reference
//! (writer ptr + consumer id + flags + padding).
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench shared_dense_vs_map -p arbitro-server --no-run"
//!   wsl bash -lc "
//!     mkdir -p /tmp/arbitro &&
//!     cp -a target/release/deps/shared_dense_vs_map-* /tmp/arbitro/ &&
//!     cd /tmp/arbitro &&
//!     timeout 120 ./shared_dense_vs_map-<hash> --bench 2>&1 | tee /tmp/bench.log
//!   "

#![allow(unused)]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex, RwLock};
use std::thread;
use std::time::Instant;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use foldhash::fast::FixedState;
use papaya::HashMap as PapayaMap;

// ── Params ──────────────────────────────────────────────────────────────────

const N_KEYS:       u32   = 10_000;
const OPS_PER_THR:  usize = 2_000_000;
const THREAD_SETS:  &[usize] = &[1, 4, 8, 16];

// ── Payload ─────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
#[repr(C)]
struct Entry {
    writer_ptr:  u64,   // 8
    consumer_id: u32,   // 4
    stream_id:   u32,   // 4
    max_inflight: u32,  // 4
    flags:       u32,   // 4
    _pad:        [u8; 8],
}
impl Default for Entry {
    fn default() -> Self {
        Self { writer_ptr: 0, consumer_id: 0, stream_id: 0,
               max_inflight: 0, flags: 0, _pad: [0; 8] }
    }
}
const _: () = assert!(std::mem::size_of::<Entry>() == 32);

fn make_entry(i: u32) -> Entry {
    Entry {
        writer_ptr: 0xDEAD_BEEF_0000 + i as u64,
        consumer_id: i,
        stream_id: i % 100,
        max_inflight: 256,
        flags: i & 0xFF,
        _pad: [0; 8],
    }
}

// ── RNG (per-thread) ────────────────────────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed.max(1)) }
    #[inline] fn next(&mut self) -> u64 {
        let mut x = self.0; x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        self.0 = x; x
    }
    #[inline] fn key(&mut self) -> u32 { (self.next() as u32) % N_KEYS }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

#[inline(always)]
fn write_threshold(write_pct: u8) -> u64 {
    (write_pct as u64) * (u64::MAX / 100)
}

// ── A0. Box<[Entry]> owned, single-thread (real shard pattern) ──────────────
//
// This is what arbitro actually does: the shard thread owns the topology
// slice directly. No Arc, no sharing, no atomics — just `&Box<[Entry]>`
// index access. Only makes sense at n_threads=1 (each shard owns its own).

fn bench_box_owned(_write_pct: u8) -> f64 {
    let arr: Box<[Entry]> = (0..N_KEYS).map(make_entry).collect::<Vec<_>>().into_boxed_slice();
    let mut rng = Rng::new(0xB0B);
    let start = Instant::now();
    let mut sink = 0u64;
    for _ in 0..OPS_PER_THR {
        let k = rng.key();
        let e = unsafe { arr.get_unchecked(k as usize) };
        sink = sink.wrapping_add(e.writer_ptr ^ e.consumer_id as u64);
    }
    black_box(sink);
    start.elapsed().as_nanos() as f64 / OPS_PER_THR as f64
}

// ── A. Arc<Box<[Entry]>> (immutable) ────────────────────────────────────────

fn bench_arc_box(n_threads: usize, _write_pct: u8) -> f64 {
    let arr: Arc<Box<[Entry]>> = Arc::new(
        (0..N_KEYS).map(make_entry).collect::<Vec<_>>().into_boxed_slice()
    );
    let barrier = Arc::new(Barrier::new(n_threads));
    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let arr = arr.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new(0xBEEF ^ tid as u64);
                barrier.wait();
                let start = Instant::now();
                let mut sink = 0u64;
                for _ in 0..OPS_PER_THR {
                    let k = rng.key();
                    let e = unsafe { arr.get_unchecked(k as usize) };
                    sink = sink.wrapping_add(e.writer_ptr ^ e.consumer_id as u64);
                }
                black_box(sink);
                start.elapsed().as_nanos() as f64 / OPS_PER_THR as f64
            }));
        }
        let mut sum = 0.0;
        for j in h { sum += j.join().unwrap(); }
        sum / n_threads as f64
    })
}

// ── A2. ArcSwap<Box<[Entry]>> — snapshot CoW ────────────────────────────────

fn bench_arc_swap(n_threads: usize, write_pct: u8) -> f64 {
    // Copy-on-write model: readers load the current snapshot (lock-free,
    // refcount bump). Writers build a new Box<[Entry]>, swap it in
    // atomically. Under `write_pct` we serialize writes with a Mutex
    // because ArcSwap is single-writer-coherent (readers always see a
    // consistent snapshot, but two concurrent writers would race on
    // the clone-modify-swap sequence). Real arbitro uses a single
    // writer (the shard thread), so the Mutex here just models that.
    let initial: Box<[Entry]> = (0..N_KEYS).map(make_entry).collect::<Vec<_>>().into_boxed_slice();
    let swap: Arc<ArcSwap<Box<[Entry]>>> = Arc::new(ArcSwap::from_pointee(initial));
    let write_lock = Arc::new(Mutex::new(()));
    let thresh = write_threshold(write_pct);
    let barrier = Arc::new(Barrier::new(n_threads));

    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let swap = swap.clone();
            let write_lock = write_lock.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new(0x5A5A ^ tid as u64);
                barrier.wait();
                let start = Instant::now();
                let mut sink = 0u64;
                for _ in 0..OPS_PER_THR {
                    let r = rng.next();
                    let k = (r as u32) % N_KEYS;
                    if r < thresh {
                        // CoW publish: clone current, mutate, swap
                        let _g = write_lock.lock().unwrap();
                        let cur = swap.load_full();
                        let mut next: Box<[Entry]> = cur.iter().copied().collect::<Vec<_>>().into_boxed_slice();
                        next[k as usize].flags = next[k as usize].flags.wrapping_add(1);
                        swap.store(Arc::new(next));
                    } else {
                        // Cheap path: guard is an epoch-style handle
                        let snap = swap.load();
                        let e = unsafe { snap.get_unchecked(k as usize) };
                        sink = sink.wrapping_add(e.writer_ptr ^ e.consumer_id as u64);
                    }
                }
                black_box(sink);
                start.elapsed().as_nanos() as f64 / OPS_PER_THR as f64
            }));
        }
        let mut sum = 0.0;
        for j in h { sum += j.join().unwrap(); }
        sum / n_threads as f64
    })
}

// ── B. Arc<RwLock<Vec<Entry>>> ──────────────────────────────────────────────

fn bench_arc_rwlock_vec(n_threads: usize, write_pct: u8) -> f64 {
    let v: Vec<Entry> = (0..N_KEYS).map(make_entry).collect();
    let lock = Arc::new(RwLock::new(v));
    let thresh = write_threshold(write_pct);
    let barrier = Arc::new(Barrier::new(n_threads));
    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let lock = lock.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new(0xCAFE ^ tid as u64);
                barrier.wait();
                let start = Instant::now();
                let mut sink = 0u64;
                for _ in 0..OPS_PER_THR {
                    let r = rng.next();
                    let k = (r as u32) % N_KEYS;
                    if r < thresh {
                        let mut w = lock.write().unwrap();
                        w[k as usize].flags = w[k as usize].flags.wrapping_add(1);
                    } else {
                        let g = lock.read().unwrap();
                        let e = unsafe { g.get_unchecked(k as usize) };
                        sink = sink.wrapping_add(e.writer_ptr ^ e.consumer_id as u64);
                    }
                }
                black_box(sink);
                start.elapsed().as_nanos() as f64 / OPS_PER_THR as f64
            }));
        }
        let mut sum = 0.0;
        for j in h { sum += j.join().unwrap(); }
        sum / n_threads as f64
    })
}

// ── C. Arc<Mutex<HashMap>> (std SipHash) ────────────────────────────────────

fn bench_arc_mutex_hm(n_threads: usize, write_pct: u8) -> f64 {
    let mut m = HashMap::with_capacity(N_KEYS as usize);
    for i in 0..N_KEYS { m.insert(i, make_entry(i)); }
    let lock = Arc::new(Mutex::new(m));
    let thresh = write_threshold(write_pct);
    let barrier = Arc::new(Barrier::new(n_threads));
    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let lock = lock.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new(0xF00D ^ tid as u64);
                barrier.wait();
                let start = Instant::now();
                let mut sink = 0u64;
                for _ in 0..OPS_PER_THR {
                    let r = rng.next();
                    let k = (r as u32) % N_KEYS;
                    if r < thresh {
                        let mut g = lock.lock().unwrap();
                        if let Some(e) = g.get_mut(&k) { e.flags = e.flags.wrapping_add(1); }
                    } else {
                        let g = lock.lock().unwrap();
                        if let Some(e) = g.get(&k) {
                            sink = sink.wrapping_add(e.writer_ptr ^ e.consumer_id as u64);
                        }
                    }
                }
                black_box(sink);
                start.elapsed().as_nanos() as f64 / OPS_PER_THR as f64
            }));
        }
        let mut sum = 0.0;
        for j in h { sum += j.join().unwrap(); }
        sum / n_threads as f64
    })
}

// ── D. Arc<RwLock<HashMap, foldhash>> ───────────────────────────────────────

fn bench_arc_rwlock_hm(n_threads: usize, write_pct: u8) -> f64 {
    let mut m: HashMap<u32, Entry, FixedState> =
        HashMap::with_capacity_and_hasher(N_KEYS as usize, FixedState::default());
    for i in 0..N_KEYS { m.insert(i, make_entry(i)); }
    let lock = Arc::new(RwLock::new(m));
    let thresh = write_threshold(write_pct);
    let barrier = Arc::new(Barrier::new(n_threads));
    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let lock = lock.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new(0xACE ^ tid as u64);
                barrier.wait();
                let start = Instant::now();
                let mut sink = 0u64;
                for _ in 0..OPS_PER_THR {
                    let r = rng.next();
                    let k = (r as u32) % N_KEYS;
                    if r < thresh {
                        let mut g = lock.write().unwrap();
                        if let Some(e) = g.get_mut(&k) { e.flags = e.flags.wrapping_add(1); }
                    } else {
                        let g = lock.read().unwrap();
                        if let Some(e) = g.get(&k) {
                            sink = sink.wrapping_add(e.writer_ptr ^ e.consumer_id as u64);
                        }
                    }
                }
                black_box(sink);
                start.elapsed().as_nanos() as f64 / OPS_PER_THR as f64
            }));
        }
        let mut sum = 0.0;
        for j in h { sum += j.join().unwrap(); }
        sum / n_threads as f64
    })
}

// ── E. papaya::HashMap (lock-free) ──────────────────────────────────────────

fn bench_papaya(n_threads: usize, write_pct: u8) -> f64 {
    let m: Arc<PapayaMap<u32, Entry, FixedState>> = Arc::new(
        PapayaMap::builder()
            .hasher(FixedState::default())
            .build()
    );
    {
        let g = m.pin();
        for i in 0..N_KEYS { g.insert(i, make_entry(i)); }
    }
    let thresh = write_threshold(write_pct);
    let barrier = Arc::new(Barrier::new(n_threads));
    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let m = m.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new(0xBAAD ^ tid as u64);
                barrier.wait();
                let start = Instant::now();
                let mut sink = 0u64;
                for _ in 0..OPS_PER_THR {
                    let r = rng.next();
                    let k = (r as u32) % N_KEYS;
                    let g = m.pin();
                    if r < thresh {
                        if let Some(e) = g.get(&k) {
                            let mut new_e = *e;
                            new_e.flags = new_e.flags.wrapping_add(1);
                            g.insert(k, new_e);
                        }
                    } else if let Some(e) = g.get(&k) {
                        sink = sink.wrapping_add(e.writer_ptr ^ e.consumer_id as u64);
                    }
                }
                black_box(sink);
                start.elapsed().as_nanos() as f64 / OPS_PER_THR as f64
            }));
        }
        let mut sum = 0.0;
        for j in h { sum += j.join().unwrap(); }
        sum / n_threads as f64
    })
}

// ── F. DashMap (sharded locks) ──────────────────────────────────────────────

fn bench_dashmap(n_threads: usize, write_pct: u8) -> f64 {
    let m: Arc<DashMap<u32, Entry, FixedState>> = Arc::new(
        DashMap::with_capacity_and_hasher(N_KEYS as usize, FixedState::default())
    );
    for i in 0..N_KEYS { m.insert(i, make_entry(i)); }
    let thresh = write_threshold(write_pct);
    let barrier = Arc::new(Barrier::new(n_threads));
    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let m = m.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new(0xD00D ^ tid as u64);
                barrier.wait();
                let start = Instant::now();
                let mut sink = 0u64;
                for _ in 0..OPS_PER_THR {
                    let r = rng.next();
                    let k = (r as u32) % N_KEYS;
                    if r < thresh {
                        if let Some(mut e) = m.get_mut(&k) { e.flags = e.flags.wrapping_add(1); }
                    } else if let Some(e) = m.get(&k) {
                        sink = sink.wrapping_add(e.writer_ptr ^ e.consumer_id as u64);
                    }
                }
                black_box(sink);
                start.elapsed().as_nanos() as f64 / OPS_PER_THR as f64
            }));
        }
        let mut sum = 0.0;
        for j in h { sum += j.join().unwrap(); }
        sum / n_threads as f64
    })
}

// ── Runner ──────────────────────────────────────────────────────────────────

fn run_workload(label: &str, write_pct: u8, include_arc_box: bool) {
    println!("\n── {}  (writes={}%)  ──", label, write_pct);
    println!(
        "{:<4} | {:>9} | {:>9} | {:>9} | {:>11} | {:>12} | {:>12} | {:>12} | {:>9}",
        "thr", "Box owned", "Arc<Box>", "ArcSwap", "RwL<Vec>", "Mtx<HM,std>", "RwL<HM,fh>",
        "papaya+fh", "DashMap"
    );
    println!("{}", "-".repeat(118));

    for &n in THREAD_SETS {
        let a0 = if include_arc_box && write_pct == 0 && n == 1 { bench_box_owned(write_pct) } else { f64::NAN };
        let a  = if include_arc_box && write_pct == 0 { bench_arc_box(n, write_pct) } else { f64::NAN };
        let a2 = bench_arc_swap(n, write_pct);
        let b  = bench_arc_rwlock_vec(n, write_pct);
        let c  = bench_arc_mutex_hm(n, write_pct);
        let d  = bench_arc_rwlock_hm(n, write_pct);
        let e  = bench_papaya(n, write_pct);
        let f  = bench_dashmap(n, write_pct);
        let fmt = |x: f64| if x.is_nan() { "   —    ".to_string() } else { format!("{:>6.1} ns", x) };
        println!(
            "{:<4} | {:>9} | {:>9} | {:>9} | {:>11} | {:>12} | {:>12} | {:>12} | {:>9}",
            n, fmt(a0), fmt(a), fmt(a2), fmt(b), fmt(c), fmt(d), fmt(e), fmt(f)
        );
    }
}

fn main() {
    println!("\nShared dense container vs HashMap — multi-thread contention");
    println!("===========================================================");
    println!(
        "N_KEYS={}  OPS_PER_THREAD={}  entry_size={}B",
        N_KEYS, OPS_PER_THR, std::mem::size_of::<Entry>()
    );
    println!("threads tested: {:?}", THREAD_SETS);

    run_workload("W1 — 100% reads (pure lookup)",   0,  true);
    run_workload("W2 — 99% reads / 1% writes",      1,  false);
    run_workload("W3 — 90% reads / 10% writes",     10, false);

    println!("\nLower is better (ns per op, averaged across threads).");
    println!("Box owned = single-thread, no Arc (real shard pattern).");
    println!("Arc<Box> only applies to pure-read workloads (immutable).\n");
}
