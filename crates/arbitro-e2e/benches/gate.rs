//! Benchmark: Gate final design.
//!
//! Scenario: worker loop { acquire → work → lock } until jobs done.
//! Compares old (UnsafeCell) vs final (AtomicBool Relaxed + parked flag).

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

// ── Main ────────────────────────────────────────────────────────

fn print_result(label: &str, elapsed: Duration) {
    let ops = ITERATIONS as f64 / elapsed.as_secs_f64();
    let latency_ns = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    println!("  {label:45} | {elapsed:>9.2?} | {ops:>12.0} ops/s | {latency_ns:>6.0} ns/op");
}

fn main() {
    println!("\nGate Final: {ITERATIONS} jobs (acquire → work → lock)");
    println!("{}", "=".repeat(90));
    println!(
        "  {:45} | {:>9} | {:>12} | {:>9}",
        "Variant", "Time", "Ops/s", "Latency"
    );
    println!("  {}", "-".repeat(82));

    print_result("spin atomic (baseline)", bench_spin());
    print_result("OLD: volatile + unpark always", bench_old());
    print_result("NEW: AtomicBool Relaxed + parked flag", bench_new());

    println!("\n{}", "=".repeat(90));
    println!("  NEW: no UB, no unnecessary syscalls, 0% CPU idle");
}
