//! Measures CPU consumption under LOAD and IDLE for each signaling method.
//!
//! Load: 50K jobs (acquire → work → lock)
//! Idle: 2s with no releases

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ITERATIONS: u32 = 50_000;
const IDLE_SECS: u64 = 2;

// ── Gate (final) ────────────────────────────────────────────────

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
    #[inline] fn release(&self) {
        self.locked.store(false, Ordering::Relaxed);
        if self.parked.load(Ordering::Relaxed) {
            unsafe { if let Some(t) = &*self.worker.get() { t.unpark(); } }
        }
    }
    #[inline] fn lock(&self) { self.locked.store(true, Ordering::Relaxed); }
    #[inline] fn acquire(&self) {
        if !self.locked.load(Ordering::Relaxed) { return; }
        for _ in 0..512 {
            if !self.locked.load(Ordering::Relaxed) { return; }
            std::hint::spin_loop();
        }
        self.parked.store(true, Ordering::Relaxed);
        loop {
            if !self.locked.load(Ordering::Relaxed) { self.parked.store(false, Ordering::Relaxed); return; }
            std::thread::park();
            if !self.locked.load(Ordering::Relaxed) { self.parked.store(false, Ordering::Relaxed); return; }
        }
    }
}

// ── CPU reader ──────────────────────────────────────────────────

fn read_cpu_ns(pid: u32) -> Option<u64> {
    let data = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields: Vec<&str> = data.split_whitespace().collect();
    let u: u64 = fields.get(13)?.parse().ok()?;
    let s: u64 = fields.get(14)?.parse().ok()?;
    Some((u + s) * 1_000_000_000 / 100)
}

// ── Bench: load ─────────────────────────────────────────────────

fn load_gate() -> (Duration, f64) {
    let pid = std::process::id();
    let gate = Arc::new(Gate::new());
    let count = Arc::new(AtomicU32::new(0));
    let g = gate.clone(); let c = count.clone();
    let w = std::thread::Builder::new().name("gate".into()).spawn(move || {
        g.set_worker(std::thread::current());
        loop {
            if c.load(Ordering::Relaxed) >= ITERATIONS { break; }
            g.acquire(); c.fetch_add(1, Ordering::Relaxed); g.lock();
        }
    }).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    let cpu0 = read_cpu_ns(pid);
    let start = Instant::now();
    while count.load(Ordering::Relaxed) < ITERATIONS { gate.release(); std::hint::spin_loop(); }
    let elapsed = start.elapsed();
    let cpu1 = read_cpu_ns(pid);
    w.join().unwrap();
    let pct = match (cpu0, cpu1) {
        (Some(a), Some(b)) => (b - a) as f64 / elapsed.as_nanos() as f64 * 100.0,
        _ => -1.0,
    };
    (elapsed, pct)
}

fn load_spin() -> (Duration, f64) {
    let pid = std::process::id();
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
    let cpu0 = read_cpu_ns(pid);
    let start = Instant::now();
    while count.load(Ordering::Relaxed) < ITERATIONS { ready.store(true, Ordering::Release); std::hint::spin_loop(); }
    let elapsed = start.elapsed();
    let cpu1 = read_cpu_ns(pid);
    w.join().unwrap();
    let pct = match (cpu0, cpu1) {
        (Some(a), Some(b)) => (b - a) as f64 / elapsed.as_nanos() as f64 * 100.0,
        _ => -1.0,
    };
    (elapsed, pct)
}

fn load_crossbeam() -> (Duration, f64) {
    let pid = std::process::id();
    let count = Arc::new(AtomicU32::new(0));
    let (tx, rx) = crossbeam_channel::bounded::<()>(1);
    let c = count.clone();
    let w = std::thread::Builder::new().name("xbeam".into()).spawn(move || {
        while rx.recv().is_ok() { c.fetch_add(1, Ordering::Relaxed); }
    }).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    let cpu0 = read_cpu_ns(pid);
    let start = Instant::now();
    while count.load(Ordering::Relaxed) < ITERATIONS { let _ = tx.send(()); }
    let elapsed = start.elapsed();
    let cpu1 = read_cpu_ns(pid);
    drop(tx);
    w.join().unwrap();
    let pct = match (cpu0, cpu1) {
        (Some(a), Some(b)) => (b - a) as f64 / elapsed.as_nanos() as f64 * 100.0,
        _ => -1.0,
    };
    (elapsed, pct)
}

fn load_tokio() -> (Duration, f64) {
    let pid = std::process::id();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let count = Arc::new(AtomicU32::new(0));
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
        let c = count.clone();
        let w = std::thread::Builder::new().name("tokio".into()).spawn(move || {
            while rx.blocking_recv().is_some() { c.fetch_add(1, Ordering::Relaxed); }
        }).unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let cpu0 = read_cpu_ns(pid);
        let start = Instant::now();
        while count.load(Ordering::Relaxed) < ITERATIONS { let _ = tx.send(()).await; }
        let elapsed = start.elapsed();
        let cpu1 = read_cpu_ns(pid);
        drop(tx);
        w.join().unwrap();
        let pct = match (cpu0, cpu1) {
            (Some(a), Some(b)) => (b - a) as f64 / elapsed.as_nanos() as f64 * 100.0,
            _ => -1.0,
        };
        (elapsed, pct)
    })
}

// ── Bench: idle ─────────────────────────────────────────────────

fn idle_measure(label: &str, f: fn(Arc<AtomicBool>)) -> f64 {
    let pid = std::process::id();
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();
    let w = std::thread::Builder::new().name(label.into()).spawn(move || f(s)).unwrap();
    std::thread::sleep(Duration::from_millis(50)); // let worker settle
    let cpu0 = read_cpu_ns(pid);
    let start = Instant::now();
    std::thread::sleep(Duration::from_secs(IDLE_SECS));
    stop.store(true, Ordering::Release);
    // Wake the thread multiple times to ensure it sees stop
    for _ in 0..5 {
        w.thread().unpark();
        std::thread::sleep(Duration::from_millis(1));
    }
    w.join().unwrap();
    let wall = start.elapsed().as_nanos() as f64;
    match (cpu0, read_cpu_ns(pid)) {
        (Some(a), Some(b)) => (b - a) as f64 / wall * 100.0,
        _ => -1.0,
    }
}

fn idle_spin(stop: Arc<AtomicBool>) {
    let flag = AtomicBool::new(false);
    while !stop.load(Ordering::Relaxed) {
        if flag.load(Ordering::Relaxed) { break; }
        std::hint::spin_loop();
    }
}

fn idle_gate(stop: Arc<AtomicBool>) {
    // Simulates gate idle: spin 512x → park, checking stop on wake
    while !stop.load(Ordering::Relaxed) {
        // Spin phase
        let mut done = false;
        for _ in 0..512 {
            if stop.load(Ordering::Relaxed) { return; }
            std::hint::spin_loop();
        }
        // Park phase
        if !done {
            std::thread::park();
        }
    }
}

fn idle_crossbeam(stop: Arc<AtomicBool>) {
    let (_tx, rx) = crossbeam_channel::bounded::<()>(1);
    while !stop.load(Ordering::Relaxed) {
        let _ = rx.recv_timeout(Duration::from_millis(100));
    }
}

fn idle_tokio(stop: Arc<AtomicBool>) {
    let (_tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
    while !stop.load(Ordering::Relaxed) {
        let _ = rx.blocking_recv();
    }
}

// ── Main ────────────────────────────────────────────────────────

fn main() {
    println!("\n=== LOAD: {ITERATIONS} jobs (acquire → work → lock) ===");
    println!("  {:25} | {:>9} | {:>8} | {:>6}", "Method", "Time", "ns/op", "CPU%");
    println!("  {}", "-".repeat(60));

    let (e, cpu) = load_spin();
    let ns = e.as_nanos() as f64 / ITERATIONS as f64;
    println!("  {:25} | {:>9.2?} | {:>8.0} | {:>5.1}%", "spin atomic", e, ns, cpu);

    let (e, cpu) = load_gate();
    let ns = e.as_nanos() as f64 / ITERATIONS as f64;
    println!("  {:25} | {:>9.2?} | {:>8.0} | {:>5.1}%", "Gate (final)", e, ns, cpu);

    let (e, cpu) = load_crossbeam();
    let ns = e.as_nanos() as f64 / ITERATIONS as f64;
    println!("  {:25} | {:>9.2?} | {:>8.0} | {:>5.1}%", "crossbeam", e, ns, cpu);

    let (e, cpu) = load_tokio();
    let ns = e.as_nanos() as f64 / ITERATIONS as f64;
    println!("  {:25} | {:>9.2?} | {:>8.0} | {:>5.1}%", "tokio::mpsc", e, ns, cpu);

    println!("\n=== IDLE: {IDLE_SECS}s no work ===");
    println!("  {:25} | {:>6}", "Method", "CPU%");
    println!("  {}", "-".repeat(35));

    let pct = idle_measure("spin", idle_spin);
    println!("  {:25} | {:>5.1}%", "spin atomic", pct);

    let pct = idle_measure("gate", idle_gate);
    println!("  {:25} | {:>5.1}%", "Gate (final)", pct);

    let pct = idle_measure("crossbeam", idle_crossbeam);
    println!("  {:25} | {:>5.1}%", "crossbeam", pct);

    let pct = idle_measure("tokio", idle_tokio);
    println!("  {:25} | {:>5.1}%", "tokio::mpsc", pct);

    println!();
}
