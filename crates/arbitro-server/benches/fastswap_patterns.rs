//! FastSwap patterns — lock-free state sharing under concurrent load.
//!
//! Purpose: identify which patterns keep a thread from blocking for too long.
//! Each bench reports both:
//!   - avg ns/op    (total throughput)
//!   - max-chunk    (worst 1000-op window — spikes reveal blocking)
//!
//! Contenders:
//!   P1  AtomicU64 packed      — 8B/slot, pure atomic ops
//!   P2  Seqlock<Entry>        — 32B Copy payload per slot, multi-writer CAS
//!   P3  LeftRight<Vec<Entry>> — double-buffer snapshot, serialized writer
//!   P4  Arc<Box<[Entry]>>     — immutable baseline (W1 only)
//!   P5  ArcSwap<Box<[Entry]>> — CoW publish
//!   P6  papaya::HashMap       — lock-free concurrent hashmap
//!
//! Workloads:  W1 100%R,  W2 99%R/1%W,  W3 90%R/10%W
//! Threads:    1, 4, 8, 16
//!
//! Run (smoke first):
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench fastswap_patterns -p arbitro-server --no-run"
//!   wsl bash -lc "
//!     mkdir -p /tmp/arbitro &&
//!     cp -a target/release/deps/fastswap_patterns-* /tmp/arbitro/ &&
//!     cd /tmp/arbitro &&
//!     timeout 120 ./fastswap_patterns-<hash> --smoke 2>&1 | tee /tmp/bench.log
//!   "
//! Then without --smoke for the full run.

#![allow(unused)]

use std::cell::UnsafeCell;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Instant;

use arc_swap::ArcSwap;
use foldhash::fast::FixedState;
use papaya::HashMap as PapayaMap;

// ── Params ──────────────────────────────────────────────────────────────────

const N_KEYS:  u32       = 10_000;
const CHUNK:   usize     = 1_000;
const THREADS: &[usize]  = &[1, 4, 8, 16];

#[inline] fn ops_per_thread(smoke: bool) -> usize {
    if smoke { 5_000 } else { 100_000 }
}

// ── Payload ─────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
#[repr(C)]
struct Entry {
    writer_ptr:  u64,
    consumer_id: u32,
    stream_id:   u32,
    max_inflight:u32,
    flags:       u32,
    _pad:        [u8; 8],
}
impl Default for Entry {
    fn default() -> Self {
        Self { writer_ptr:0, consumer_id:0, stream_id:0,
               max_inflight:0, flags:0, _pad:[0;8] }
    }
}
const _: () = assert!(std::mem::size_of::<Entry>() == 32);

fn make_entry(i: u32) -> Entry {
    Entry { writer_ptr: 0xDEAD + i as u64, consumer_id: i, stream_id: i % 100,
            max_inflight: 256, flags: i & 0xFF, _pad: [0; 8] }
}

// ── RNG ─────────────────────────────────────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed.max(1)) }
    #[inline] fn next(&mut self) -> u64 {
        let mut x=self.0; x^=x<<13; x^=x>>7; x^=x<<17; self.0=x; x
    }
    #[inline] fn key(&mut self) -> u32 { (self.next() as u32) % N_KEYS }
}
#[inline] fn wt(pct: u8) -> u64 { (pct as u64) * (u64::MAX / 100) }

// ── Timing stats ────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct Stats { total_ns: f64, max_chunk_ns_per_op: f64 }
impl Stats {
    fn new() -> Self { Self { total_ns: 0.0, max_chunk_ns_per_op: 0.0 } }
    #[inline] fn add_chunk(&mut self, chunk_ns: f64, n: usize) {
        self.total_ns += chunk_ns;
        let per = chunk_ns / n as f64;
        if per > self.max_chunk_ns_per_op { self.max_chunk_ns_per_op = per; }
    }
}

fn reduce(all: Vec<(Stats, usize)>) -> (f64, f64) {
    let mut sum_avg = 0.0f64;
    let mut max_chunk = 0.0f64;
    let n = all.len() as f64;
    for (s, n_ops) in all {
        sum_avg += s.total_ns / n_ops as f64;
        if s.max_chunk_ns_per_op > max_chunk { max_chunk = s.max_chunk_ns_per_op; }
    }
    (sum_avg / n, max_chunk)
}

// ── Seqlock (multi-writer via CAS on seq counter) ───────────────────────────

struct SeqLock<T: Copy> {
    seq:  AtomicU64,
    cell: UnsafeCell<T>,
}
unsafe impl<T: Copy + Send> Send for SeqLock<T> {}
unsafe impl<T: Copy + Send> Sync for SeqLock<T> {}

impl<T: Copy> SeqLock<T> {
    fn new(v: T) -> Self { Self { seq: AtomicU64::new(0), cell: UnsafeCell::new(v) } }

    #[inline]
    fn read(&self) -> T {
        loop {
            let s1 = self.seq.load(Ordering::Acquire);
            if s1 & 1 != 0 { std::hint::spin_loop(); continue; }
            let v = unsafe { *self.cell.get() };
            let s2 = self.seq.load(Ordering::Acquire);
            if s1 == s2 { return v; }
            std::hint::spin_loop();
        }
    }

    // Multi-writer safe: CAS claims the odd-seq window before mutating.
    #[inline]
    fn update(&self, mutate: impl Fn(T) -> T) {
        loop {
            let s = self.seq.load(Ordering::Acquire);
            if s & 1 != 0 { std::hint::spin_loop(); continue; }
            if self.seq.compare_exchange(s, s + 1,
                Ordering::AcqRel, Ordering::Relaxed).is_err() {
                std::hint::spin_loop();
                continue;
            }
            // unique writer window
            let old = unsafe { *self.cell.get() };
            unsafe { *self.cell.get() = mutate(old); }
            self.seq.store(s + 2, Ordering::Release);
            return;
        }
    }
}

// ── LeftRight<Vec<Entry>> — double-buffer snapshot, single-writer critical ──
//
// Writes are serialized by writer_lock. Each logical write hits BOTH slots
// (write inactive → flip → write now-inactive) so both buffers stay in sync
// for future reads. This is the price for O(1) lock-free reads.

struct LeftRight<T> {
    slots:       [UnsafeCell<T>; 2],
    active:      AtomicUsize,
    readers:     [AtomicUsize; 2],
    writer_lock: Mutex<()>,
}
unsafe impl<T: Send> Send for LeftRight<T> {}
unsafe impl<T: Send> Sync for LeftRight<T> {}

impl<T: Clone> LeftRight<T> {
    fn new(v: T) -> Self {
        Self {
            slots: [UnsafeCell::new(v.clone()), UnsafeCell::new(v)],
            active: AtomicUsize::new(0),
            readers: [AtomicUsize::new(0), AtomicUsize::new(0)],
            writer_lock: Mutex::new(()),
        }
    }

    // Reader: double-check pattern to guard against flip mid-register.
    #[inline]
    fn read<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        loop {
            let i = self.active.load(Ordering::Acquire);
            self.readers[i].fetch_add(1, Ordering::AcqRel);
            let j = self.active.load(Ordering::Acquire);
            if i == j {
                let r = f(unsafe { &*self.slots[i].get() });
                self.readers[i].fetch_sub(1, Ordering::Release);
                return r;
            }
            self.readers[i].fetch_sub(1, Ordering::Release);
        }
    }

    // Writer: apply mutation to both slots, flip in between.
    fn write(&self, mutate: impl Fn(&mut T)) {
        let _g = self.writer_lock.lock().unwrap();
        let cur = self.active.load(Ordering::Acquire);
        let nxt = 1 - cur;
        while self.readers[nxt].load(Ordering::Acquire) > 0 {
            std::hint::spin_loop();
        }
        unsafe { mutate(&mut *self.slots[nxt].get()); }
        self.active.store(nxt, Ordering::Release);
        while self.readers[cur].load(Ordering::Acquire) > 0 {
            std::hint::spin_loop();
        }
        unsafe { mutate(&mut *self.slots[cur].get()); }
    }
}

// ── Bench functions ─────────────────────────────────────────────────────────

// P1. AtomicU64 packed
fn bench_atomic_u64(n_threads: usize, write_pct: u8, ops: usize) -> (f64, f64) {
    let data: Arc<Box<[AtomicU64]>> = Arc::new(
        (0..N_KEYS).map(|i| AtomicU64::new(i as u64)).collect::<Vec<_>>().into_boxed_slice()
    );
    let thresh = wt(write_pct);
    let barrier = Arc::new(Barrier::new(n_threads));
    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let data = data.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new((tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
                let mut st = Stats::new(); let mut sink = 0u64;
                barrier.wait();
                let mut remaining = ops;
                while remaining > 0 {
                    let n = CHUNK.min(remaining);
                    let t0 = Instant::now();
                    for _ in 0..n {
                        let r = rng.next();
                        let k = (r as u32) % N_KEYS;
                        if r < thresh {
                            sink = sink.wrapping_add(
                                unsafe { data.get_unchecked(k as usize) }.fetch_add(1, Ordering::AcqRel)
                            );
                        } else {
                            sink = sink.wrapping_add(
                                unsafe { data.get_unchecked(k as usize) }.load(Ordering::Acquire)
                            );
                        }
                    }
                    st.add_chunk(t0.elapsed().as_nanos() as f64, n);
                    remaining -= n;
                }
                black_box(sink);
                (st, ops)
            }));
        }
        reduce(h.into_iter().map(|j| j.join().unwrap()).collect())
    })
}

// P2. Seqlock<Entry>
fn bench_seqlock(n_threads: usize, write_pct: u8, ops: usize) -> (f64, f64) {
    let data: Arc<Box<[SeqLock<Entry>]>> = Arc::new(
        (0..N_KEYS).map(|i| SeqLock::new(make_entry(i))).collect::<Vec<_>>().into_boxed_slice()
    );
    let thresh = wt(write_pct);
    let barrier = Arc::new(Barrier::new(n_threads));
    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let data = data.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new((tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x2);
                let mut st = Stats::new(); let mut sink = 0u64;
                barrier.wait();
                let mut remaining = ops;
                while remaining > 0 {
                    let n = CHUNK.min(remaining);
                    let t0 = Instant::now();
                    for _ in 0..n {
                        let r = rng.next();
                        let k = (r as u32) % N_KEYS;
                        if r < thresh {
                            data[k as usize].update(|mut e| { e.flags = e.flags.wrapping_add(1); e });
                        } else {
                            let e = data[k as usize].read();
                            sink = sink.wrapping_add(e.writer_ptr ^ e.consumer_id as u64);
                        }
                    }
                    st.add_chunk(t0.elapsed().as_nanos() as f64, n);
                    remaining -= n;
                }
                black_box(sink);
                (st, ops)
            }));
        }
        reduce(h.into_iter().map(|j| j.join().unwrap()).collect())
    })
}

// P3. LeftRight<Vec<Entry>>
fn bench_leftright(n_threads: usize, write_pct: u8, ops: usize) -> (f64, f64) {
    let init: Vec<Entry> = (0..N_KEYS).map(make_entry).collect();
    let data: Arc<LeftRight<Vec<Entry>>> = Arc::new(LeftRight::new(init));
    let thresh = wt(write_pct);
    let barrier = Arc::new(Barrier::new(n_threads));
    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let data = data.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new((tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x3);
                let mut st = Stats::new(); let mut sink = 0u64;
                barrier.wait();
                let mut remaining = ops;
                while remaining > 0 {
                    let n = CHUNK.min(remaining);
                    let t0 = Instant::now();
                    for _ in 0..n {
                        let r = rng.next();
                        let k = (r as u32) % N_KEYS;
                        if r < thresh {
                            data.write(|v| { v[k as usize].flags = v[k as usize].flags.wrapping_add(1); });
                        } else {
                            sink = sink.wrapping_add(data.read(|v| {
                                let e = v[k as usize];
                                e.writer_ptr ^ e.consumer_id as u64
                            }));
                        }
                    }
                    st.add_chunk(t0.elapsed().as_nanos() as f64, n);
                    remaining -= n;
                }
                black_box(sink);
                (st, ops)
            }));
        }
        reduce(h.into_iter().map(|j| j.join().unwrap()).collect())
    })
}

// P4. Arc<Box<[Entry]>> — immutable baseline (W1 only)
fn bench_arc_box(n_threads: usize, _write_pct: u8, ops: usize) -> (f64, f64) {
    let data: Arc<Box<[Entry]>> = Arc::new(
        (0..N_KEYS).map(make_entry).collect::<Vec<_>>().into_boxed_slice()
    );
    let barrier = Arc::new(Barrier::new(n_threads));
    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let data = data.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new((tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x4);
                let mut st = Stats::new(); let mut sink = 0u64;
                barrier.wait();
                let mut remaining = ops;
                while remaining > 0 {
                    let n = CHUNK.min(remaining);
                    let t0 = Instant::now();
                    for _ in 0..n {
                        let k = rng.key();
                        let e = unsafe { data.get_unchecked(k as usize) };
                        sink = sink.wrapping_add(e.writer_ptr ^ e.consumer_id as u64);
                    }
                    st.add_chunk(t0.elapsed().as_nanos() as f64, n);
                    remaining -= n;
                }
                black_box(sink);
                (st, ops)
            }));
        }
        reduce(h.into_iter().map(|j| j.join().unwrap()).collect())
    })
}

// P5. ArcSwap<Box<[Entry]>>
fn bench_arc_swap(n_threads: usize, write_pct: u8, ops: usize) -> (f64, f64) {
    let init: Box<[Entry]> = (0..N_KEYS).map(make_entry).collect::<Vec<_>>().into_boxed_slice();
    let swap: Arc<ArcSwap<Box<[Entry]>>> = Arc::new(ArcSwap::from_pointee(init));
    let write_lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
    let thresh = wt(write_pct);
    let barrier = Arc::new(Barrier::new(n_threads));
    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let swap = swap.clone();
            let write_lock = write_lock.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new((tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x5);
                let mut st = Stats::new(); let mut sink = 0u64;
                barrier.wait();
                let mut remaining = ops;
                while remaining > 0 {
                    let n = CHUNK.min(remaining);
                    let t0 = Instant::now();
                    for _ in 0..n {
                        let r = rng.next();
                        let k = (r as u32) % N_KEYS;
                        if r < thresh {
                            let _g = write_lock.lock().unwrap();
                            let cur = swap.load_full();
                            let mut next: Box<[Entry]> = cur.iter().copied().collect::<Vec<_>>().into_boxed_slice();
                            next[k as usize].flags = next[k as usize].flags.wrapping_add(1);
                            swap.store(Arc::new(next));
                        } else {
                            let snap = swap.load();
                            let e = unsafe { snap.get_unchecked(k as usize) };
                            sink = sink.wrapping_add(e.writer_ptr ^ e.consumer_id as u64);
                        }
                    }
                    st.add_chunk(t0.elapsed().as_nanos() as f64, n);
                    remaining -= n;
                }
                black_box(sink);
                (st, ops)
            }));
        }
        reduce(h.into_iter().map(|j| j.join().unwrap()).collect())
    })
}

// P6. papaya::HashMap
fn bench_papaya(n_threads: usize, write_pct: u8, ops: usize) -> (f64, f64) {
    let m: Arc<PapayaMap<u32, Entry, FixedState>> = Arc::new(
        PapayaMap::builder().hasher(FixedState::default()).build()
    );
    { let g = m.pin(); for i in 0..N_KEYS { g.insert(i, make_entry(i)); } }
    let thresh = wt(write_pct);
    let barrier = Arc::new(Barrier::new(n_threads));
    thread::scope(|s| {
        let mut h = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let m = m.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new((tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x6);
                let mut st = Stats::new(); let mut sink = 0u64;
                barrier.wait();
                let mut remaining = ops;
                while remaining > 0 {
                    let n = CHUNK.min(remaining);
                    let t0 = Instant::now();
                    for _ in 0..n {
                        let r = rng.next();
                        let k = (r as u32) % N_KEYS;
                        let g = m.pin();
                        if r < thresh {
                            if let Some(e) = g.get(&k) {
                                let mut ne = *e;
                                ne.flags = ne.flags.wrapping_add(1);
                                g.insert(k, ne);
                            }
                        } else if let Some(e) = g.get(&k) {
                            sink = sink.wrapping_add(e.writer_ptr ^ e.consumer_id as u64);
                        }
                    }
                    st.add_chunk(t0.elapsed().as_nanos() as f64, n);
                    remaining -= n;
                }
                black_box(sink);
                (st, ops)
            }));
        }
        reduce(h.into_iter().map(|j| j.join().unwrap()).collect())
    })
}

// ── Runner ──────────────────────────────────────────────────────────────────

fn fmt_pair(x: (f64, f64)) -> String {
    format!("{:>7.1} / {:>8.0}", x.0, x.1)
}

fn run_workload(label: &str, write_pct: u8, include_immut: bool, ops: usize) {
    println!("\n── {}  (writes={}%)   avg ns / max-chunk ns ──", label, write_pct);
    println!(
        "{:<4} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16}",
        "thr", "AtomicU64", "Seqlock", "LeftRight", "Arc<Box>", "ArcSwap", "papaya"
    );
    println!("{}", "-".repeat(118));

    for &n in THREADS {
        let p1 = bench_atomic_u64(n, write_pct, ops);
        let p2 = bench_seqlock(n, write_pct, ops);
        let p3 = bench_leftright(n, write_pct, ops);
        let p4 = if include_immut && write_pct == 0 {
            bench_arc_box(n, write_pct, ops)
        } else { (f64::NAN, f64::NAN) };
        let p5 = bench_arc_swap(n, write_pct, ops);
        let p6 = bench_papaya(n, write_pct, ops);
        let fmt = |x: (f64, f64)| -> String {
            if x.0.is_nan() { "      —    /     —   ".to_string() } else { fmt_pair(x) }
        };
        println!(
            "{:<4} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16}",
            n, fmt(p1), fmt(p2), fmt(p3), fmt(p4), fmt(p5), fmt(p6)
        );
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let smoke = args.iter().any(|a| a == "--smoke");
    let ops = ops_per_thread(smoke);

    println!("\nFastSwap patterns — lock-free state sharing");
    println!("============================================");
    println!(
        "N_KEYS={}  OPS_PER_THREAD={}  chunk={}  entry_size={}B",
        N_KEYS, ops, CHUNK, std::mem::size_of::<Entry>()
    );
    println!("threads tested: {:?}", THREADS);
    println!("mode: {}", if smoke { "SMOKE" } else { "FULL" });

    if smoke {
        // Smoke: only W1 at 1 and 4 threads, just to verify nothing hangs
        println!("\n── SMOKE  (writes=0%)   avg / max-chunk ns ──");
        println!("{:<4} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16}",
            "thr", "AtomicU64", "Seqlock", "LeftRight", "Arc<Box>", "ArcSwap", "papaya");
        println!("{}", "-".repeat(118));
        for &n in &[1usize, 4] {
            let p1 = bench_atomic_u64(n, 0, ops);
            let p2 = bench_seqlock(n, 0, ops);
            let p3 = bench_leftright(n, 0, ops);
            let p4 = bench_arc_box(n, 0, ops);
            let p5 = bench_arc_swap(n, 0, ops);
            let p6 = bench_papaya(n, 0, ops);
            println!("{:<4} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16}",
                n, fmt_pair(p1), fmt_pair(p2), fmt_pair(p3),
                   fmt_pair(p4), fmt_pair(p5), fmt_pair(p6));
        }
        println!("\nSmoke OK. Run without --smoke for full matrix.");
        return;
    }

    run_workload("W1 — 100% reads",           0,  true,  ops);
    run_workload("W2 — 99% reads / 1% writes", 1,  false, ops);
    run_workload("W3 — 90% reads / 10% writes",10, false, ops);

    println!("\nLower is better. `avg` = throughput. `max-chunk` = worst 1000-op window.");
    println!("A high max-chunk with low avg means one or more threads got blocked briefly.\n");
}
