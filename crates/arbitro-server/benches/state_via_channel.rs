//! State updates via channel — actor pattern benchmark.
//!
//! Model: 1 consumer thread owns `Box<[Entry]>` exclusively. N producer
//! threads send `UpdateEvent { key, flags }` via a channel. The consumer
//! applies events serially (no sync on the state itself).
//!
//! This measures the HOT PATH of the producer: how long does `send()`
//! take? Does it block? Does the producer ever get stuck?
//!
//! Contenders:
//!   C1  crossbeam::unbounded              — MPSC, grows unbounded
//!   C2  crossbeam::bounded(1024)          — MPSC, backpressure when full
//!   C3  crossbeam::bounded(64)            — tighter bound
//!   C4  std::sync::mpsc                   — std's MPSC
//!   C5  crossbeam_queue::SegQueue         — lock-free queue, no wakeup
//!   C6  crossbeam_queue::ArrayQueue(1024) — bounded lock-free, no wakeup
//!
//! Metrics per producer thread:
//!   - avg ns / send()         (throughput of send side)
//!   - max-chunk ns/op         (worst 1000-op window — reveals blocking)
//!
//! Consumer thread metric:
//!   - total events drained / wall time  (consumer throughput)
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench state_via_channel -p arbitro-server --no-run"
//!   wsl bash -lc "
//!     mkdir -p /tmp/arbitro &&
//!     cp -a target/release/deps/state_via_channel-* /tmp/arbitro/ &&
//!     cd /tmp/arbitro &&
//!     timeout 120 ./state_via_channel-<hash> --smoke 2>&1 | tee /tmp/bench.log
//!   "

#![allow(unused)]

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel as cb;
use crossbeam_queue::{ArrayQueue, SegQueue};

// ── Params ──────────────────────────────────────────────────────────────────

const N_KEYS:   u32      = 10_000;
const CHUNK:    usize    = 1_000;
const THREADS:  &[usize] = &[1, 4, 8, 16];

#[inline] fn events_per_producer(smoke: bool) -> usize {
    if smoke { 5_000 } else { 100_000 }
}

// ── Event + state ───────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
#[repr(C)]
struct UpdateEvent { key: u32, flags: u32 }
const _: () = assert!(std::mem::size_of::<UpdateEvent>() == 8);

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

// ── RNG ─────────────────────────────────────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed.max(1)) }
    #[inline] fn next(&mut self) -> u64 {
        let mut x=self.0; x^=x<<13; x^=x>>7; x^=x<<17; self.0=x; x
    }
    #[inline] fn key(&mut self) -> u32 { (self.next() as u32) % N_KEYS }
}

// ── Stats ───────────────────────────────────────────────────────────────────

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

// ── Report row ──────────────────────────────────────────────────────────────

struct Row {
    producer_avg_ns: f64,
    producer_max_chunk_ns: f64,
    consumer_events_per_sec: f64,
    wall_ms: f64,
}

fn fmt_row(r: &Row) -> String {
    format!("{:>6.1} / {:>6.0}  | {:>7.0}k ev/s | {:>5.0}ms",
        r.producer_avg_ns, r.producer_max_chunk_ns,
        r.consumer_events_per_sec / 1000.0,
        r.wall_ms)
}

// ── Consumer helper ─────────────────────────────────────────────────────────
// Applies events to a local Box<[Entry]>. No sync — this thread owns it.
// Returns (events_applied, wall_time_ns).

// ── C1. crossbeam::unbounded ────────────────────────────────────────────────

fn bench_cb_unbounded(n_producers: usize, events_per: usize) -> Row {
    let (tx, rx) = cb::unbounded::<UpdateEvent>();
    let done = Arc::new(AtomicBool::new(false));
    let total_expected = n_producers * events_per;

    let done_c = done.clone();
    let consumer = thread::spawn(move || {
        let mut state: Box<[Entry]> =
            (0..N_KEYS).map(|_| Entry::default()).collect::<Vec<_>>().into_boxed_slice();
        let mut n = 0usize;
        let start = Instant::now();
        loop {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(ev) => {
                    state[ev.key as usize].flags = ev.flags;
                    n += 1;
                }
                Err(_) => {
                    if done_c.load(Ordering::Acquire) && rx.is_empty() { break; }
                }
            }
        }
        let elapsed = start.elapsed().as_nanos() as f64;
        black_box(&state);
        (n, elapsed)
    });

    let barrier = Arc::new(Barrier::new(n_producers));
    let wall_start = Instant::now();
    let prods = thread::scope(|s| {
        let mut h = Vec::with_capacity(n_producers);
        for tid in 0..n_producers {
            let tx = tx.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new((tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x1);
                let mut st = Stats::new();
                barrier.wait();
                let mut remaining = events_per;
                while remaining > 0 {
                    let n = CHUNK.min(remaining);
                    let t0 = Instant::now();
                    for _ in 0..n {
                        let k = rng.key();
                        let _ = tx.send(UpdateEvent { key: k, flags: k });
                    }
                    st.add_chunk(t0.elapsed().as_nanos() as f64, n);
                    remaining -= n;
                }
                (st, events_per)
            }));
        }
        h.into_iter().map(|j| j.join().unwrap()).collect::<Vec<_>>()
    });
    drop(tx);
    done.store(true, Ordering::Release);
    let (n_consumed, _cons_elapsed_ns) = consumer.join().unwrap();
    let wall_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let (avg, maxc) = reduce(prods);
    Row {
        producer_avg_ns: avg,
        producer_max_chunk_ns: maxc,
        consumer_events_per_sec: total_expected as f64 / (wall_ms / 1000.0),
        wall_ms,
    }
}

// ── C2/C3. crossbeam::bounded(cap) ──────────────────────────────────────────

fn bench_cb_bounded(n_producers: usize, events_per: usize, cap: usize) -> Row {
    let (tx, rx) = cb::bounded::<UpdateEvent>(cap);
    let done = Arc::new(AtomicBool::new(false));
    let total_expected = n_producers * events_per;

    let done_c = done.clone();
    let consumer = thread::spawn(move || {
        let mut state: Box<[Entry]> =
            (0..N_KEYS).map(|_| Entry::default()).collect::<Vec<_>>().into_boxed_slice();
        let mut n = 0usize;
        let start = Instant::now();
        loop {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(ev) => {
                    state[ev.key as usize].flags = ev.flags;
                    n += 1;
                }
                Err(_) => {
                    if done_c.load(Ordering::Acquire) && rx.is_empty() { break; }
                }
            }
        }
        let elapsed = start.elapsed().as_nanos() as f64;
        black_box(&state);
        (n, elapsed)
    });

    let barrier = Arc::new(Barrier::new(n_producers));
    let wall_start = Instant::now();
    let prods = thread::scope(|s| {
        let mut h = Vec::with_capacity(n_producers);
        for tid in 0..n_producers {
            let tx = tx.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new((tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x2);
                let mut st = Stats::new();
                barrier.wait();
                let mut remaining = events_per;
                while remaining > 0 {
                    let n = CHUNK.min(remaining);
                    let t0 = Instant::now();
                    for _ in 0..n {
                        let k = rng.key();
                        let _ = tx.send(UpdateEvent { key: k, flags: k });
                    }
                    st.add_chunk(t0.elapsed().as_nanos() as f64, n);
                    remaining -= n;
                }
                (st, events_per)
            }));
        }
        h.into_iter().map(|j| j.join().unwrap()).collect::<Vec<_>>()
    });
    drop(tx);
    done.store(true, Ordering::Release);
    let (_, _) = consumer.join().unwrap();
    let wall_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let (avg, maxc) = reduce(prods);
    Row {
        producer_avg_ns: avg,
        producer_max_chunk_ns: maxc,
        consumer_events_per_sec: total_expected as f64 / (wall_ms / 1000.0),
        wall_ms,
    }
}

// ── C4. std::sync::mpsc ─────────────────────────────────────────────────────

fn bench_std_mpsc(n_producers: usize, events_per: usize) -> Row {
    use std::sync::mpsc;
    let (tx, rx) = mpsc::channel::<UpdateEvent>();
    let done = Arc::new(AtomicBool::new(false));
    let total_expected = n_producers * events_per;

    let done_c = done.clone();
    let consumer = thread::spawn(move || {
        let mut state: Box<[Entry]> =
            (0..N_KEYS).map(|_| Entry::default()).collect::<Vec<_>>().into_boxed_slice();
        let mut n = 0usize;
        let start = Instant::now();
        loop {
            match rx.recv_timeout(Duration::from_millis(50)) {
                Ok(ev) => { state[ev.key as usize].flags = ev.flags; n += 1; }
                Err(_) => {
                    if done_c.load(Ordering::Acquire) { break; }
                }
            }
        }
        let elapsed = start.elapsed().as_nanos() as f64;
        black_box(&state);
        (n, elapsed)
    });

    let barrier = Arc::new(Barrier::new(n_producers));
    let wall_start = Instant::now();
    let prods = thread::scope(|s| {
        let mut h = Vec::with_capacity(n_producers);
        for tid in 0..n_producers {
            let tx = tx.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new((tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x4);
                let mut st = Stats::new();
                barrier.wait();
                let mut remaining = events_per;
                while remaining > 0 {
                    let n = CHUNK.min(remaining);
                    let t0 = Instant::now();
                    for _ in 0..n {
                        let k = rng.key();
                        let _ = tx.send(UpdateEvent { key: k, flags: k });
                    }
                    st.add_chunk(t0.elapsed().as_nanos() as f64, n);
                    remaining -= n;
                }
                (st, events_per)
            }));
        }
        h.into_iter().map(|j| j.join().unwrap()).collect::<Vec<_>>()
    });
    drop(tx);
    done.store(true, Ordering::Release);
    let (_, _) = consumer.join().unwrap();
    let wall_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let (avg, maxc) = reduce(prods);
    Row {
        producer_avg_ns: avg,
        producer_max_chunk_ns: maxc,
        consumer_events_per_sec: total_expected as f64 / (wall_ms / 1000.0),
        wall_ms,
    }
}

// ── C5. crossbeam_queue::SegQueue (lock-free, no wakeup) ────────────────────
//
// No blocking — producer just pushes. Consumer polls. If consumer falls
// behind, queue grows unbounded.

fn bench_seg_queue(n_producers: usize, events_per: usize) -> Row {
    let q: Arc<SegQueue<UpdateEvent>> = Arc::new(SegQueue::new());
    let done = Arc::new(AtomicBool::new(false));
    let total_expected = n_producers * events_per;

    let q_c = q.clone();
    let done_c = done.clone();
    let consumer = thread::spawn(move || {
        let mut state: Box<[Entry]> =
            (0..N_KEYS).map(|_| Entry::default()).collect::<Vec<_>>().into_boxed_slice();
        let mut n = 0usize;
        let start = Instant::now();
        loop {
            match q_c.pop() {
                Some(ev) => { state[ev.key as usize].flags = ev.flags; n += 1; }
                None => {
                    if done_c.load(Ordering::Acquire) && q_c.is_empty() { break; }
                    std::hint::spin_loop();
                }
            }
        }
        let elapsed = start.elapsed().as_nanos() as f64;
        black_box(&state);
        (n, elapsed)
    });

    let barrier = Arc::new(Barrier::new(n_producers));
    let wall_start = Instant::now();
    let prods = thread::scope(|s| {
        let mut h = Vec::with_capacity(n_producers);
        for tid in 0..n_producers {
            let q = q.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new((tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x5);
                let mut st = Stats::new();
                barrier.wait();
                let mut remaining = events_per;
                while remaining > 0 {
                    let n = CHUNK.min(remaining);
                    let t0 = Instant::now();
                    for _ in 0..n {
                        let k = rng.key();
                        q.push(UpdateEvent { key: k, flags: k });
                    }
                    st.add_chunk(t0.elapsed().as_nanos() as f64, n);
                    remaining -= n;
                }
                (st, events_per)
            }));
        }
        h.into_iter().map(|j| j.join().unwrap()).collect::<Vec<_>>()
    });
    done.store(true, Ordering::Release);
    let (_, _) = consumer.join().unwrap();
    let wall_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let (avg, maxc) = reduce(prods);
    Row {
        producer_avg_ns: avg,
        producer_max_chunk_ns: maxc,
        consumer_events_per_sec: total_expected as f64 / (wall_ms / 1000.0),
        wall_ms,
    }
}

// ── C6. ArrayQueue (bounded lock-free) ──────────────────────────────────────

fn bench_array_queue(n_producers: usize, events_per: usize, cap: usize) -> Row {
    let q: Arc<ArrayQueue<UpdateEvent>> = Arc::new(ArrayQueue::new(cap));
    let done = Arc::new(AtomicBool::new(false));
    let total_expected = n_producers * events_per;

    let q_c = q.clone();
    let done_c = done.clone();
    let consumer = thread::spawn(move || {
        let mut state: Box<[Entry]> =
            (0..N_KEYS).map(|_| Entry::default()).collect::<Vec<_>>().into_boxed_slice();
        let mut n = 0usize;
        let start = Instant::now();
        loop {
            match q_c.pop() {
                Some(ev) => { state[ev.key as usize].flags = ev.flags; n += 1; }
                None => {
                    if done_c.load(Ordering::Acquire) && q_c.is_empty() { break; }
                    std::hint::spin_loop();
                }
            }
        }
        let elapsed = start.elapsed().as_nanos() as f64;
        black_box(&state);
        (n, elapsed)
    });

    let barrier = Arc::new(Barrier::new(n_producers));
    let wall_start = Instant::now();
    let prods = thread::scope(|s| {
        let mut h = Vec::with_capacity(n_producers);
        for tid in 0..n_producers {
            let q = q.clone();
            let barrier = barrier.clone();
            h.push(s.spawn(move || {
                let mut rng = Rng::new((tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x6);
                let mut st = Stats::new();
                barrier.wait();
                let mut remaining = events_per;
                while remaining > 0 {
                    let n = CHUNK.min(remaining);
                    let t0 = Instant::now();
                    for _ in 0..n {
                        let k = rng.key();
                        // push_or_spin: if full, retry. This IS the blocking behavior.
                        let mut ev = UpdateEvent { key: k, flags: k };
                        while let Err(back) = q.push(ev) { ev = back; std::hint::spin_loop(); }
                    }
                    st.add_chunk(t0.elapsed().as_nanos() as f64, n);
                    remaining -= n;
                }
                (st, events_per)
            }));
        }
        h.into_iter().map(|j| j.join().unwrap()).collect::<Vec<_>>()
    });
    done.store(true, Ordering::Release);
    let (_, _) = consumer.join().unwrap();
    let wall_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let (avg, maxc) = reduce(prods);
    Row {
        producer_avg_ns: avg,
        producer_max_chunk_ns: maxc,
        consumer_events_per_sec: total_expected as f64 / (wall_ms / 1000.0),
        wall_ms,
    }
}

// ── Runner ──────────────────────────────────────────────────────────────────

fn run(label: &str, r: Row) {
    println!("  {:<28} {}", label, fmt_row(&r));
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let smoke = args.iter().any(|a| a == "--smoke");
    let events_per = events_per_producer(smoke);

    println!("\nState updates via channel — producer → consumer pattern");
    println!("========================================================");
    println!("N_KEYS={}  EVENTS_PER_PRODUCER={}  chunk={}", N_KEYS, events_per, CHUNK);
    println!("mode: {}", if smoke { "SMOKE" } else { "FULL" });
    println!("producers tested: {:?}", if smoke { &[1usize, 4][..] } else { THREADS });

    let threads_to_test: Vec<usize> = if smoke { vec![1, 4] } else { THREADS.to_vec() };

    for &n in &threads_to_test {
        println!("\n── {} producer(s) ──", n);
        println!("  {:<28} {:<16}  {:<14}  {:<6}",
            "channel",
            "send avg / maxc ns",
            "consumer thrpt",
            "wall");
        println!("  {}", "-".repeat(75));
        run("cb::unbounded",          bench_cb_unbounded(n, events_per));
        run("cb::bounded(1024)",      bench_cb_bounded(n, events_per, 1024));
        run("cb::bounded(64)",        bench_cb_bounded(n, events_per, 64));
        run("std::sync::mpsc",        bench_std_mpsc(n, events_per));
        run("cb_queue::SegQueue",     bench_seg_queue(n, events_per));
        run("cb_queue::ArrayQueue(1024)", bench_array_queue(n, events_per, 1024));
    }

    println!("\n`send avg` = per-op producer latency; `maxc` = worst 1000-op chunk.");
    println!("Bounded queues with small cap show maxc spikes when consumer lags.");
    println!("SegQueue/ArrayQueue have no wakeup — consumer spins (uses CPU when idle).\n");
}
