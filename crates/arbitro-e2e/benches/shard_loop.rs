//! Benchmark: shard loop — channel alternatives + Gate.
//!
//! Compares transport→shard command delivery:
//!   A) tokio::mpsc (current)
//!   B) crossbeam
//!   C) Gate only (no channel — direct shared buffer)
//!
//! Each variant: send command + gate.release → worker processes + drains.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ITERATIONS: u32 = 50_000;

// ── Gate ────────────────────────────────────────────────────────

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
    #[inline] fn is_open(&self) -> bool { !self.locked.load(Ordering::Relaxed) }
}

// ═══════════════════════════════════════════════════════════════
// A) CURRENT: tokio::mpsc blocking_recv (+ DrainDeliver round-trip)
// ═══════════════════════════════════════════════════════════════

fn bench_tokio_blocking() -> Duration {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all().build().unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<u8>(4096);
    let cmd_count = Arc::new(AtomicU32::new(0));
    let drain_count = Arc::new(AtomicU32::new(0));
    let cc = cmd_count.clone(); let dc = drain_count.clone();

    let w = std::thread::Builder::new().name("tokio-block".into()).spawn(move || {
        while let Some(msg) = rx.blocking_recv() {
            match msg { 0 => { cc.fetch_add(1, Ordering::Relaxed); } 1 => { dc.fetch_add(1, Ordering::Relaxed); } _ => break }
        }
    }).unwrap();

    std::thread::sleep(Duration::from_millis(5));
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..ITERATIONS { let _ = tx.send(0).await; let _ = tx.send(1).await; }
        let _ = tx.send(255).await;
    });
    w.join().unwrap();
    start.elapsed()
}

// ═══════════════════════════════════════════════════════════════
// B) tokio::mpsc try_recv + Gate
// ═══════════════════════════════════════════════════════════════

fn bench_tokio_tryrecv_gate() -> Duration {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all().build().unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<u8>(4096);
    let gate = Arc::new(Gate::new());
    let cmd_count = Arc::new(AtomicU32::new(0));
    let drain_count = Arc::new(AtomicU32::new(0));
    let g = gate.clone(); let cc = cmd_count.clone(); let dc = drain_count.clone();

    let w = std::thread::Builder::new().name("tokio-try".into()).spawn(move || {
        g.set_worker(std::thread::current());
        loop {
            let mut shutdown = false;
            while let Ok(msg) = rx.try_recv() {
                if msg == 255 { shutdown = true; break; }
                cc.fetch_add(1, Ordering::Relaxed);
            }
            if shutdown { break; }
            if g.is_open() { dc.fetch_add(1, Ordering::Relaxed); g.lock(); }
            if rx.is_empty() && !g.is_open() { std::thread::park(); }
        }
    }).unwrap();

    let st = w.thread().clone();
    std::thread::sleep(Duration::from_millis(5));
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..ITERATIONS { let _ = tx.send(0).await; st.unpark(); gate.release(); }
        let _ = tx.send(255).await; st.unpark();
    });
    w.join().unwrap();
    start.elapsed()
}

// ═══════════════════════════════════════════════════════════════
// C) crossbeam try_recv + Gate
// ═══════════════════════════════════════════════════════════════

fn bench_crossbeam_tryrecv_gate() -> Duration {
    let (tx, rx) = crossbeam_channel::bounded::<u8>(4096);
    let gate = Arc::new(Gate::new());
    let cmd_count = Arc::new(AtomicU32::new(0));
    let drain_count = Arc::new(AtomicU32::new(0));
    let g = gate.clone(); let cc = cmd_count.clone(); let dc = drain_count.clone();

    let w = std::thread::Builder::new().name("xbeam-try".into()).spawn(move || {
        g.set_worker(std::thread::current());
        loop {
            let mut shutdown = false;
            while let Ok(msg) = rx.try_recv() {
                if msg == 255 { shutdown = true; break; }
                cc.fetch_add(1, Ordering::Relaxed);
            }
            if shutdown { break; }
            if g.is_open() { dc.fetch_add(1, Ordering::Relaxed); g.lock(); }
            if rx.is_empty() && !g.is_open() { std::thread::park(); }
        }
    }).unwrap();

    let st = w.thread().clone();
    std::thread::sleep(Duration::from_millis(5));
    let start = Instant::now();
    for _ in 0..ITERATIONS { let _ = tx.send(0); st.unpark(); gate.release(); }
    let _ = tx.send(255); st.unpark();
    w.join().unwrap();
    start.elapsed()
}

// ═══════════════════════════════════════════════════════════════
// D) crossbeam blocking_recv (no gate, DrainDeliver via channel)
// ═══════════════════════════════════════════════════════════════

fn bench_crossbeam_blocking() -> Duration {
    let (tx, rx) = crossbeam_channel::bounded::<u8>(4096);
    let cmd_count = Arc::new(AtomicU32::new(0));
    let drain_count = Arc::new(AtomicU32::new(0));
    let cc = cmd_count.clone(); let dc = drain_count.clone();

    let w = std::thread::Builder::new().name("xbeam-block".into()).spawn(move || {
        while let Ok(msg) = rx.recv() {
            match msg { 0 => { cc.fetch_add(1, Ordering::Relaxed); } 1 => { dc.fetch_add(1, Ordering::Relaxed); } _ => break }
        }
    }).unwrap();

    std::thread::sleep(Duration::from_millis(5));
    let start = Instant::now();
    for _ in 0..ITERATIONS { let _ = tx.send(0); let _ = tx.send(1); }
    let _ = tx.send(255);
    w.join().unwrap();
    start.elapsed()
}

// ═══════════════════════════════════════════════════════════════
// E) Gate ONLY — no channel at all
//    Command written to shared slot, gate.release() wakes worker.
// ═══════════════════════════════════════════════════════════════

#[repr(align(64))]
struct SharedSlot {
    cmd: AtomicU32,     // 0 = empty, 1 = command, 255 = shutdown
    consumed: AtomicBool,
}

fn bench_gate_only() -> Duration {
    let gate = Arc::new(Gate::new());
    let slot = Arc::new(SharedSlot {
        cmd: AtomicU32::new(0),
        consumed: AtomicBool::new(true),
    });
    let cmd_count = Arc::new(AtomicU32::new(0));
    let drain_count = Arc::new(AtomicU32::new(0));
    let g = gate.clone(); let s = slot.clone();
    let cc = cmd_count.clone(); let dc = drain_count.clone();

    let w = std::thread::Builder::new().name("gate-only".into()).spawn(move || {
        g.set_worker(std::thread::current());
        loop {
            // Check slot
            let cmd = s.cmd.load(Ordering::Relaxed);
            if cmd == 255 { break; }
            if cmd == 1 {
                cc.fetch_add(1, Ordering::Relaxed);
                s.cmd.store(0, Ordering::Relaxed);
                s.consumed.store(true, Ordering::Relaxed);
            }
            // Check gate
            if g.is_open() { dc.fetch_add(1, Ordering::Relaxed); g.lock(); }
            // Park
            if s.cmd.load(Ordering::Relaxed) == 0 && !g.is_open() {
                std::thread::park();
            }
        }
    }).unwrap();

    let st = w.thread().clone();
    std::thread::sleep(Duration::from_millis(5));
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        // Wait for slot to be consumed
        while !slot.consumed.load(Ordering::Relaxed) { std::hint::spin_loop(); }
        slot.consumed.store(false, Ordering::Relaxed);
        slot.cmd.store(1, Ordering::Relaxed);
        st.unpark();
        gate.release();
    }
    slot.cmd.store(255, Ordering::Relaxed);
    st.unpark();
    w.join().unwrap();
    start.elapsed()
}

// ═══════════════════════════════════════════════════════════════
// F) Gate + lock-free ring (multiple commands, no channel)
//    Uses a simple atomic ring buffer for commands.
// ═══════════════════════════════════════════════════════════════

const RING_SIZE: usize = 4096;
const RING_MASK: usize = RING_SIZE - 1;

struct AtomicRing {
    buf: [AtomicU32; RING_SIZE],
    head: AtomicU32, // producer writes here
    tail: AtomicU32, // consumer reads here
}

impl AtomicRing {
    fn new() -> Self {
        Self {
            buf: std::array::from_fn(|_| AtomicU32::new(0)),
            head: AtomicU32::new(0),
            tail: AtomicU32::new(0),
        }
    }
    #[inline] fn push(&self, val: u32) {
        let h = self.head.load(Ordering::Relaxed);
        self.buf[(h as usize) & RING_MASK].store(val, Ordering::Relaxed);
        self.head.store(h.wrapping_add(1), Ordering::Release);
    }
    #[inline] fn try_pop(&self) -> Option<u32> {
        let t = self.tail.load(Ordering::Relaxed);
        let h = self.head.load(Ordering::Acquire);
        if t == h { return None; }
        let val = self.buf[(t as usize) & RING_MASK].load(Ordering::Relaxed);
        self.tail.store(t.wrapping_add(1), Ordering::Relaxed);
        Some(val)
    }
    #[inline] fn is_empty(&self) -> bool {
        self.tail.load(Ordering::Relaxed) == self.head.load(Ordering::Acquire)
    }
}

fn bench_gate_ring() -> Duration {
    let gate = Arc::new(Gate::new());
    let ring = Arc::new(AtomicRing::new());
    let cmd_count = Arc::new(AtomicU32::new(0));
    let drain_count = Arc::new(AtomicU32::new(0));
    let g = gate.clone(); let r = ring.clone();
    let cc = cmd_count.clone(); let dc = drain_count.clone();

    let w = std::thread::Builder::new().name("gate-ring".into()).spawn(move || {
        g.set_worker(std::thread::current());
        loop {
            let mut shutdown = false;
            while let Some(cmd) = r.try_pop() {
                if cmd == 255 { shutdown = true; break; }
                cc.fetch_add(1, Ordering::Relaxed);
            }
            if shutdown { break; }
            if g.is_open() { dc.fetch_add(1, Ordering::Relaxed); g.lock(); }
            if r.is_empty() && !g.is_open() { std::thread::park(); }
        }
    }).unwrap();

    let st = w.thread().clone();
    std::thread::sleep(Duration::from_millis(5));
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        ring.push(1);
        st.unpark();
        gate.release();
    }
    ring.push(255); st.unpark();
    w.join().unwrap();
    start.elapsed()
}

// ── Main ────────────────────────────────────────────────────────

fn print_result(label: &str, elapsed: Duration) {
    let ops = ITERATIONS as f64 / elapsed.as_secs_f64();
    let latency_ns = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    println!("  {label:55} | {elapsed:>9.2?} | {ops:>10.0} ops/s | {latency_ns:>6.0} ns/op");
}

fn main() {
    println!("\nShard Loop: {ITERATIONS} (cmd + drain per iteration)");
    println!("{}", "=".repeat(100));
    println!(
        "  {:55} | {:>9} | {:>10} | {:>9}",
        "Pattern", "Time", "Ops/s", "Latency"
    );
    println!("  {}", "-".repeat(90));

    print_result("A) tokio::mpsc blocking_recv (current)", bench_tokio_blocking());
    print_result("B) tokio::mpsc try_recv + Gate", bench_tokio_tryrecv_gate());
    print_result("C) crossbeam try_recv + Gate", bench_crossbeam_tryrecv_gate());
    print_result("D) crossbeam blocking_recv (no gate)", bench_crossbeam_blocking());
    print_result("E) Gate only — shared slot (no channel)", bench_gate_only());
    print_result("F) Gate + atomic ring buffer (no channel)", bench_gate_ring());

    println!("\n{}", "=".repeat(100));
}
