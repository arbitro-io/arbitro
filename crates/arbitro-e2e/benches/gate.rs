//! Benchmark: Gate final design.
//!
//! Scenario: worker loop { acquire → work → lock } until jobs done.
//! Compares: spin, old gate, new gate, crossbeam, crossbeam + gate.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

extern crate crossbeam_channel;
extern crate libc;

const ITERATIONS: u32 = 50_000;

// ── Old Gate (UnsafeCell, unpark always) ────────────────────────

use std::cell::UnsafeCell;

#[repr(align(64))]
struct GateOld {
    locked: UnsafeCell<bool>,
    worker: UnsafeCell<Option<std::thread::Thread>>,
}
unsafe impl Sync for GateOld {}

impl GateOld {
    fn new() -> Self {
        Self {
            locked: UnsafeCell::new(true),
            worker: UnsafeCell::new(None),
        }
    }
    fn set_worker(&self, t: std::thread::Thread) {
        unsafe { *self.worker.get() = Some(t); }
    }
    #[inline] fn release(&self) {
        unsafe {
            std::ptr::write_volatile(self.locked.get(), false);
            if let Some(t) = &*self.worker.get() { t.unpark(); }
        }
    }
    #[inline] fn lock(&self) {
        unsafe { std::ptr::write_volatile(self.locked.get(), true); }
    }
    #[inline] fn acquire(&self) {
        unsafe { if !std::ptr::read_volatile(self.locked.get()) { return; } }
        for _ in 0..512 {
            unsafe { if !std::ptr::read_volatile(self.locked.get()) { return; } }
            std::hint::spin_loop();
        }
        loop {
            std::thread::park();
            unsafe { if !std::ptr::read_volatile(self.locked.get()) { return; } }
        }
    }
}

// ── New Gate (AtomicBool Relaxed + parked flag) ─────────────────

#[repr(align(64))]
struct Gate {
    locked: AtomicBool,
    parked: AtomicBool,
    worker: UnsafeCell<Option<std::thread::Thread>>,
}
unsafe impl Sync for Gate {}

impl Gate {
    fn new() -> Self {
        Self {
            locked: AtomicBool::new(true),
            parked: AtomicBool::new(false),
            worker: UnsafeCell::new(None),
        }
    }

    fn set_worker(&self, t: std::thread::Thread) {
        unsafe { *self.worker.get() = Some(t); }
    }

    #[inline]
    fn release(&self) {
        self.locked.store(false, Ordering::Relaxed);
        if self.parked.load(Ordering::Relaxed) {
            unsafe {
                if let Some(t) = &*self.worker.get() { t.unpark(); }
            }
        }
    }

    #[inline]
    fn lock(&self) {
        self.locked.store(true, Ordering::Relaxed);
    }

    #[inline]
    fn acquire(&self) {
        // Fast path
        if !self.locked.load(Ordering::Relaxed) { return; }
        // Spin phase
        for _ in 0..512 {
            if !self.locked.load(Ordering::Relaxed) { return; }
            std::hint::spin_loop();
        }
        // Park phase
        self.parked.store(true, Ordering::Relaxed);
        loop {
            if !self.locked.load(Ordering::Relaxed) {
                self.parked.store(false, Ordering::Relaxed);
                return;
            }
            std::thread::park();
            if !self.locked.load(Ordering::Relaxed) {
                self.parked.store(false, Ordering::Relaxed);
                return;
            }
        }
    }
}

// ── Spin baseline ───────────────────────────────────────────────

fn bench_spin() -> Duration {
    let ready = Arc::new(AtomicBool::new(false));
    let count = Arc::new(AtomicU32::new(0));
    let r = ready.clone(); let c = count.clone();
    let w = std::thread::Builder::new().name("spin".into()).spawn(move || {
        loop {
            if c.load(Ordering::Relaxed) >= ITERATIONS { break; }
            while !r.load(Ordering::Acquire) { std::hint::spin_loop(); }
            r.store(false, Ordering::Release);
            c.fetch_add(1, Ordering::Relaxed);
        }
    }).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    let start = Instant::now();
    while count.load(Ordering::Relaxed) < ITERATIONS { ready.store(true, Ordering::Release); std::hint::spin_loop(); }
    let elapsed = start.elapsed();
    w.join().unwrap();
    elapsed
}

// ── Bench old ───────────────────────────────────────────────────

fn bench_old() -> Duration {
    let gate = Arc::new(GateOld::new());
    let count = Arc::new(AtomicU32::new(0));
    let g = gate.clone(); let c = count.clone();
    let w = std::thread::Builder::new().name("old".into()).spawn(move || {
        g.set_worker(std::thread::current());
        loop {
            if c.load(Ordering::Relaxed) >= ITERATIONS { break; }
            g.acquire(); c.fetch_add(1, Ordering::Relaxed); g.lock();
        }
    }).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    let start = Instant::now();
    while count.load(Ordering::Relaxed) < ITERATIONS { gate.release(); std::hint::spin_loop(); }
    let elapsed = start.elapsed();
    w.join().unwrap();
    elapsed
}

// ── Bench new ───────────────────────────────────────────────────

fn bench_new() -> Duration {
    let gate = Arc::new(Gate::new());
    let count = Arc::new(AtomicU32::new(0));
    let g = gate.clone(); let c = count.clone();
    let w = std::thread::Builder::new().name("new".into()).spawn(move || {
        g.set_worker(std::thread::current());
        loop {
            if c.load(Ordering::Relaxed) >= ITERATIONS { break; }
            g.acquire(); c.fetch_add(1, Ordering::Relaxed); g.lock();
        }
    }).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    let start = Instant::now();
    while count.load(Ordering::Relaxed) < ITERATIONS { gate.release(); std::hint::spin_loop(); }
    let elapsed = start.elapsed();
    w.join().unwrap();
    elapsed
}

// ── Bench crossbeam ─────────────────────────────────────────────

fn bench_crossbeam() -> Duration {
    let (tx, rx) = crossbeam_channel::bounded::<()>(65536);
    let count = Arc::new(AtomicU32::new(0));
    let c = count.clone();
    let w = std::thread::Builder::new().name("crossbeam".into()).spawn(move || {
        loop {
            if c.load(Ordering::Relaxed) >= ITERATIONS { break; }
            if rx.recv().is_err() { break; }
            c.fetch_add(1, Ordering::Relaxed);
        }
    }).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    let start = Instant::now();
    while count.load(Ordering::Relaxed) < ITERATIONS { let _ = tx.send(()); std::hint::spin_loop(); }
    let elapsed = start.elapsed();
    drop(tx);
    w.join().unwrap();
    elapsed
}

// ── Bench crossbeam + Gate ──────────────────────────────────────

fn bench_crossbeam_gate() -> Duration {
    let (tx, rx) = crossbeam_channel::bounded::<()>(65536);
    let gate = Arc::new(Gate::new());
    let count = Arc::new(AtomicU32::new(0));
    let g = gate.clone(); let c = count.clone();
    let w = std::thread::Builder::new().name("cb+gate".into()).spawn(move || {
        g.set_worker(std::thread::current());
        loop {
            if c.load(Ordering::Relaxed) >= ITERATIONS { break; }
            g.acquire();
            while rx.try_recv().is_ok() {
                c.fetch_add(1, Ordering::Relaxed);
            }
            g.lock();
        }
    }).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    let start = Instant::now();
    while count.load(Ordering::Relaxed) < ITERATIONS {
        let _ = tx.send(());
        gate.release();
        std::hint::spin_loop();
    }
    let elapsed = start.elapsed();
    drop(tx);
    w.join().unwrap();
    elapsed
}

// ── Main ────────────────────────────────────────────────────────

fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages * 4)
        .unwrap_or(0)
}

fn cpu_time_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts); }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn print_result(label: &str, elapsed: Duration, cpu_ns: u64, rss_delta: i64) {
    let ops = ITERATIONS as f64 / elapsed.as_secs_f64();
    let latency_ns = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    let wall_ns = elapsed.as_nanos() as u64;
    let cpu_pct = if wall_ns > 0 { (cpu_ns as f64 / wall_ns as f64) * 100.0 } else { 0.0 };
    println!(
        "  {label:40} | {elapsed:>9.2?} | {ops:>10.0} ops/s | {latency_ns:>5.0} ns | cpu {cpu_pct:>5.1}% | rss {rss_delta:>+5} KB",
    );
}

fn run_bench<F: FnOnce() -> Duration>(label: &str, f: F) {
    let rss_before = rss_kb() as i64;
    let cpu_before = cpu_time_ns();
    let elapsed = f();
    let cpu_after = cpu_time_ns();
    let rss_after = rss_kb() as i64;
    print_result(label, elapsed, cpu_after - cpu_before, rss_after - rss_before);
}

fn main() {
    println!("\nGate Final: {ITERATIONS} jobs (acquire → work → lock)");
    println!("{}", "=".repeat(100));
    println!(
        "  {:40} | {:>9} | {:>10} | {:>5} | {:>9} | {:>9}",
        "Variant", "Time", "Ops/s", "Lat", "CPU", "RSS"
    );
    println!("  {}", "-".repeat(95));

    run_bench("spin atomic (baseline)", bench_spin);
    run_bench("OLD: volatile + unpark always", bench_old);
    run_bench("NEW: AtomicBool Relaxed + parked", bench_new);
    run_bench("crossbeam only", bench_crossbeam);
    run_bench("crossbeam + Gate (target arch)", bench_crossbeam_gate);

    println!("\n{}", "=".repeat(100));
}
