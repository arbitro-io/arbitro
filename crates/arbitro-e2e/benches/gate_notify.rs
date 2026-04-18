//! Wake-up latency bench — models the shard drain's one-way signal:
//!
//!   waiter thread:  loop { gate.acquire(); gate.lock(); /* work */ }
//!   signaler:       gate.release()  (from publish / ack path)
//!
//! We benchmark three primitives for the "release → acquire returns" wake:
//!
//!   1. `Gate`                    — atomic+spin+park (drain's current primitive)
//!   2. `tokio::sync::Notify`     — async permit; requires a runtime
//!   3. `atomic_wait` (futex)     — raw wait/wake on a u32
//!   4. `crossbeam::channel`      — bounded(1) as wake semaphore
//!   5. `parking_lot::Condvar`    — tuned Mutex+Condvar (parking_lot parker)
//!   6. `crossbeam_utils::Parker` — per-thread lightweight Parker/Unparker
//!
//! The back-sync (waiter → signaler ack) is always `atomic_wait`, identical
//! across all three backends, so differences are attributable to the
//! forward primitive only.
//!
//! Per backend we report:
//!   - ns per release→wake cycle (one-way wake latency ≈ half of that)
//!   - CPU time consumed during the measured phase (user+sys, %)
//!   - RSS delta (kB) during the measured phase
//!
//! Rule: compile from /mnt, run from /tmp/arbitro, timeout 120, tee.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::{Acquire, Relaxed, Release}};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_common::gate::Gate;
use tokio::sync::Notify;

const WAKES: u32 = 200_000;

// ── /proc stats (Linux) ───────────────────────────────────────────────────

#[cfg(target_os = "linux")]
mod stats {
    use std::fs;

    #[derive(Clone, Copy)]
    pub struct Snap {
        pub cpu_ns: u128,
        pub rss_kb: u64,
    }

    pub fn snap() -> Snap {
        let stat = fs::read_to_string("/proc/self/stat").unwrap_or_default();
        let (_, rest) = stat.split_once(") ").unwrap_or(("", ""));
        let f: Vec<&str> = rest.split_ascii_whitespace().collect();
        let utime: u64 = f.get(11).and_then(|s| s.parse().ok()).unwrap_or(0);
        let stime: u64 = f.get(12).and_then(|s| s.parse().ok()).unwrap_or(0);
        let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as u128;
        let cpu_ns = if hz > 0 { (utime + stime) as u128 * 1_000_000_000u128 / hz } else { 0 };

        let statm = fs::read_to_string("/proc/self/statm").unwrap_or_default();
        let sf: Vec<&str> = statm.split_ascii_whitespace().collect();
        let pages: u64 = sf.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        let page_kb = (unsafe { libc::sysconf(libc::_SC_PAGESIZE) } / 1024) as u64;
        Snap { cpu_ns, rss_kb: pages * page_kb }
    }

    pub fn delta(a: Snap, b: Snap) -> (u128, i64) {
        (b.cpu_ns.saturating_sub(a.cpu_ns), b.rss_kb as i64 - a.rss_kb as i64)
    }
}

#[cfg(not(target_os = "linux"))]
mod stats {
    #[derive(Clone, Copy)] pub struct Snap { pub cpu_ns: u128, pub rss_kb: u64 }
    pub fn snap() -> Snap { Snap { cpu_ns: 0, rss_kb: 0 } }
    pub fn delta(_: Snap, _: Snap) -> (u128, i64) { (0, 0) }
}

// ── Helpers: ack counter via atomic_wait ─────────────────────────────────

#[inline]
fn ack_bump(counter: &AtomicU32) {
    counter.fetch_add(1, Release);
    atomic_wait::wake_one(counter);
}

#[inline]
fn ack_wait_until(counter: &AtomicU32, target: u32) {
    loop {
        let cur = counter.load(Acquire);
        if cur >= target { return; }
        atomic_wait::wait(counter, cur);
    }
}

// ── Gate backend ─────────────────────────────────────────────────────────

fn run_gate(n: u32) -> Duration {
    let gate = Arc::new(Gate::new());
    let ack = Arc::new(AtomicU32::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let waiter = {
        let gate = Arc::clone(&gate);
        let ack = Arc::clone(&ack);
        let done = Arc::clone(&done);
        std::thread::Builder::new().name("gate-waiter".into()).spawn(move || {
            gate.set_worker(std::thread::current());
            loop {
                gate.acquire();
                gate.lock();
                if done.load(Relaxed) { return; }
                ack_bump(&ack);
            }
        }).unwrap()
    };
    std::thread::sleep(Duration::from_millis(20));

    let start = Instant::now();
    for i in 1..=n {
        gate.release();
        ack_wait_until(&ack, i);
    }
    let elapsed = start.elapsed();

    done.store(true, Relaxed);
    gate.release();
    waiter.join().unwrap();
    elapsed
}

// ── atomic_wait backend ──────────────────────────────────────────────────

fn run_atomic_wait(n: u32) -> Duration {
    let sig = Arc::new(AtomicU32::new(0));
    let ack = Arc::new(AtomicU32::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let waiter = {
        let sig = Arc::clone(&sig);
        let ack = Arc::clone(&ack);
        let done = Arc::clone(&done);
        std::thread::Builder::new().name("aw-waiter".into()).spawn(move || {
            loop {
                // Park while sig == 0.
                while sig.load(Acquire) == 0 {
                    atomic_wait::wait(&*sig, 0);
                }
                sig.store(0, Release);
                if done.load(Relaxed) { return; }
                ack_bump(&ack);
            }
        }).unwrap()
    };
    std::thread::sleep(Duration::from_millis(20));

    let start = Instant::now();
    for i in 1..=n {
        sig.store(1, Release);
        atomic_wait::wake_one(&*sig);
        ack_wait_until(&ack, i);
    }
    let elapsed = start.elapsed();

    done.store(true, Relaxed);
    sig.store(1, Release);
    atomic_wait::wake_one(&*sig);
    waiter.join().unwrap();
    elapsed
}

// ── parking_lot::Condvar backend ─────────────────────────────────────────

fn run_parking_lot(n: u32) -> Duration {
    use parking_lot::{Condvar, Mutex};
    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let ack = Arc::new(AtomicU32::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let waiter = {
        let pair = Arc::clone(&pair);
        let ack = Arc::clone(&ack);
        let done = Arc::clone(&done);
        std::thread::Builder::new().name("pl-waiter".into()).spawn(move || {
            loop {
                let mut guard = pair.0.lock();
                while !*guard {
                    pair.1.wait(&mut guard);
                }
                *guard = false;
                drop(guard);
                if done.load(Relaxed) { return; }
                ack_bump(&ack);
            }
        }).unwrap()
    };
    std::thread::sleep(Duration::from_millis(20));

    let start = Instant::now();
    for i in 1..=n {
        *pair.0.lock() = true;
        pair.1.notify_one();
        ack_wait_until(&ack, i);
    }
    let elapsed = start.elapsed();

    done.store(true, Relaxed);
    *pair.0.lock() = true;
    pair.1.notify_one();
    waiter.join().unwrap();
    elapsed
}

// ── crossbeam_utils::Parker backend ──────────────────────────────────────

fn run_cb_parker(n: u32) -> Duration {
    use crossbeam_utils::sync::Parker;

    let parker = Parker::new();
    let unparker = parker.unparker().clone();
    // Atomic gate so the signaler only unparks when a signal is pending,
    // mirroring how Gate uses its `locked` flag.
    let pending = Arc::new(AtomicU32::new(0));
    let ack = Arc::new(AtomicU32::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let waiter = {
        let pending = Arc::clone(&pending);
        let ack = Arc::clone(&ack);
        let done = Arc::clone(&done);
        std::thread::Builder::new().name("cbp-waiter".into()).spawn(move || {
            loop {
                while pending.load(Acquire) == 0 {
                    parker.park();
                }
                pending.store(0, Release);
                if done.load(Relaxed) { return; }
                ack_bump(&ack);
            }
        }).unwrap()
    };
    std::thread::sleep(Duration::from_millis(20));

    let start = Instant::now();
    for i in 1..=n {
        pending.store(1, Release);
        unparker.unpark();
        ack_wait_until(&ack, i);
    }
    let elapsed = start.elapsed();

    done.store(true, Relaxed);
    pending.store(1, Release);
    unparker.unpark();
    waiter.join().unwrap();
    elapsed
}

// ── crossbeam channel backend ────────────────────────────────────────────

fn run_crossbeam(n: u32) -> Duration {
    let (tx, rx) = crossbeam_channel::bounded::<()>(1);
    let ack = Arc::new(AtomicU32::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let waiter = {
        let ack = Arc::clone(&ack);
        let done = Arc::clone(&done);
        std::thread::Builder::new().name("cb-waiter".into()).spawn(move || {
            while rx.recv().is_ok() {
                if done.load(Relaxed) { return; }
                ack_bump(&ack);
            }
        }).unwrap()
    };
    std::thread::sleep(Duration::from_millis(20));

    let start = Instant::now();
    for i in 1..=n {
        tx.send(()).unwrap();
        ack_wait_until(&ack, i);
    }
    let elapsed = start.elapsed();

    done.store(true, Relaxed);
    let _ = tx.send(());
    waiter.join().unwrap();
    elapsed
}

// ── tokio Notify backend ─────────────────────────────────────────────────

async fn run_notify(n: u32) -> Duration {
    let notify = Arc::new(Notify::new());
    let ack = Arc::new(AtomicU32::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let waiter = {
        let notify = Arc::clone(&notify);
        let ack = Arc::clone(&ack);
        let done = Arc::clone(&done);
        tokio::spawn(async move {
            loop {
                notify.notified().await;
                if done.load(Relaxed) { return; }
                ack_bump(&ack);
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;

    let start = Instant::now();
    for i in 1..=n {
        notify.notify_one();
        ack_wait_until(&ack, i);
    }
    let elapsed = start.elapsed();

    done.store(true, Relaxed);
    notify.notify_one();
    waiter.abort();
    elapsed
}

// ── Driver ───────────────────────────────────────────────────────────────

fn report(label: &str, n: u32, el: Duration, cpu_ns: u128, rss_dk: i64) {
    let ns = el.as_nanos() as f64 / n as f64;
    let hz = n as f64 / el.as_secs_f64();
    let cpu_pct = if el.as_nanos() > 0 {
        cpu_ns as f64 / el.as_nanos() as f64 * 100.0
    } else { 0.0 };
    println!(
        "  {:<22} {:>8.0} ns/wake   {:>11.0} wakes/s   cpu {:>5.1}%   Δrss {:+5} kB   total {:.2?}",
        label, ns, hz, cpu_pct, rss_dk, el
    );
}

// ── Idle-hold phase (no wakes, waiter parked) ────────────────────────────
//
// Spawns the waiter, lets it park on its primitive, then sleeps `hold`
// without signaling. Measures CPU consumed by the whole process during
// that window. All park-based primitives should report ~0%.

fn idle_gate(hold: Duration) -> (u128, i64) {
    let gate = Arc::new(Gate::new());
    let done = Arc::new(AtomicBool::new(false));
    let waiter = {
        let gate = Arc::clone(&gate);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            gate.set_worker(std::thread::current());
            while !done.load(Relaxed) {
                gate.acquire();
                gate.lock();
            }
        })
    };
    std::thread::sleep(Duration::from_millis(50)); // let it park
    let s0 = stats::snap();
    std::thread::sleep(hold);
    let s1 = stats::snap();
    done.store(true, Relaxed);
    gate.release();
    waiter.join().unwrap();
    stats::delta(s0, s1)
}

fn idle_atomic_wait(hold: Duration) -> (u128, i64) {
    let sig = Arc::new(AtomicU32::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let waiter = {
        let sig = Arc::clone(&sig);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            while !done.load(Relaxed) {
                while sig.load(Acquire) == 0 {
                    atomic_wait::wait(&*sig, 0);
                    if done.load(Relaxed) { return; }
                }
                sig.store(0, Release);
            }
        })
    };
    std::thread::sleep(Duration::from_millis(50));
    let s0 = stats::snap();
    std::thread::sleep(hold);
    let s1 = stats::snap();
    done.store(true, Relaxed);
    sig.store(1, Release);
    atomic_wait::wake_one(&*sig);
    waiter.join().unwrap();
    stats::delta(s0, s1)
}

fn idle_parking_lot(hold: Duration) -> (u128, i64) {
    use parking_lot::{Condvar, Mutex};
    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let done = Arc::new(AtomicBool::new(false));
    let waiter = {
        let pair = Arc::clone(&pair);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            loop {
                let mut g = pair.0.lock();
                while !*g { pair.1.wait(&mut g); }
                *g = false;
                if done.load(Relaxed) { return; }
            }
        })
    };
    std::thread::sleep(Duration::from_millis(50));
    let s0 = stats::snap();
    std::thread::sleep(hold);
    let s1 = stats::snap();
    done.store(true, Relaxed);
    *pair.0.lock() = true;
    pair.1.notify_one();
    waiter.join().unwrap();
    stats::delta(s0, s1)
}

fn idle_cb_parker(hold: Duration) -> (u128, i64) {
    use crossbeam_utils::sync::Parker;
    let parker = Parker::new();
    let unparker = parker.unparker().clone();
    let pending = Arc::new(AtomicU32::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let waiter = {
        let pending = Arc::clone(&pending);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            loop {
                while pending.load(Acquire) == 0 {
                    parker.park();
                }
                pending.store(0, Release);
                if done.load(Relaxed) { return; }
            }
        })
    };
    std::thread::sleep(Duration::from_millis(50));
    let s0 = stats::snap();
    std::thread::sleep(hold);
    let s1 = stats::snap();
    done.store(true, Relaxed);
    pending.store(1, Release);
    unparker.unpark();
    waiter.join().unwrap();
    stats::delta(s0, s1)
}

fn idle_crossbeam(hold: Duration) -> (u128, i64) {
    let (tx, rx) = crossbeam_channel::bounded::<()>(1);
    let done = Arc::new(AtomicBool::new(false));
    let waiter = {
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            while let Ok(()) = rx.recv() {
                if done.load(Relaxed) { return; }
            }
        })
    };
    std::thread::sleep(Duration::from_millis(50));
    let s0 = stats::snap();
    std::thread::sleep(hold);
    let s1 = stats::snap();
    done.store(true, Relaxed);
    let _ = tx.send(());
    waiter.join().unwrap();
    stats::delta(s0, s1)
}

async fn idle_notify(hold: Duration) -> (u128, i64) {
    let notify = Arc::new(Notify::new());
    let done = Arc::new(AtomicBool::new(false));
    let waiter = {
        let notify = Arc::clone(&notify);
        let done = Arc::clone(&done);
        tokio::spawn(async move {
            while !done.load(Relaxed) {
                notify.notified().await;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let s0 = stats::snap();
    tokio::time::sleep(hold).await;
    let s1 = stats::snap();
    done.store(true, Relaxed);
    notify.notify_one();
    waiter.abort();
    stats::delta(s0, s1)
}

fn report_idle(label: &str, hold: Duration, cpu_ns: u128, rss_dk: i64) {
    let pct = cpu_ns as f64 / hold.as_nanos() as f64 * 100.0;
    println!(
        "  {:<22} cpu {:>5.2}%   Δrss {:+5} kB   (over {:?} idle)",
        label, pct, rss_dk, hold
    );
}

fn main() {
    println!("\n=== Wake-up latency: Gate vs tokio Notify vs atomic_wait ===");
    println!("Forward wake = primitive under test. Back-ack = atomic_wait (identical across backends).");
    println!("N = {} cycles per backend\n", WAKES);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .unwrap();

    std::thread::sleep(Duration::from_millis(200));
    let s0 = stats::snap();
    let el = run_gate(WAKES);
    let s1 = stats::snap();
    let (cpu, rss) = stats::delta(s0, s1);
    report("Gate (park/unpark)", WAKES, el, cpu, rss);

    std::thread::sleep(Duration::from_millis(200));
    let s0 = stats::snap();
    let el = run_atomic_wait(WAKES);
    let s1 = stats::snap();
    let (cpu, rss) = stats::delta(s0, s1);
    report("atomic_wait (futex)", WAKES, el, cpu, rss);

    std::thread::sleep(Duration::from_millis(200));
    let s0 = stats::snap();
    let el = run_cb_parker(WAKES);
    let s1 = stats::snap();
    let (cpu, rss) = stats::delta(s0, s1);
    report("crossbeam_utils Parker", WAKES, el, cpu, rss);

    std::thread::sleep(Duration::from_millis(200));
    let s0 = stats::snap();
    let el = run_parking_lot(WAKES);
    let s1 = stats::snap();
    let (cpu, rss) = stats::delta(s0, s1);
    report("parking_lot Cv+Mutex", WAKES, el, cpu, rss);

    std::thread::sleep(Duration::from_millis(200));
    let s0 = stats::snap();
    let el = run_crossbeam(WAKES);
    let s1 = stats::snap();
    let (cpu, rss) = stats::delta(s0, s1);
    report("crossbeam::channel(1)", WAKES, el, cpu, rss);

    std::thread::sleep(Duration::from_millis(200));
    let s0 = stats::snap();
    let el = rt.block_on(run_notify(WAKES));
    let s1 = stats::snap();
    let (cpu, rss) = stats::delta(s0, s1);
    report("tokio Notify (async)", WAKES, el, cpu, rss);

    // ── Idle-hold phase ─────────────────────────────────────────────────
    let hold = Duration::from_secs(1);
    println!("\n--- Idle-hold: waiter parked, signaler silent for {:?} ---", hold);

    std::thread::sleep(Duration::from_millis(200));
    let (cpu, rss) = idle_gate(hold);
    report_idle("Gate (park/unpark)", hold, cpu, rss);

    std::thread::sleep(Duration::from_millis(200));
    let (cpu, rss) = idle_atomic_wait(hold);
    report_idle("atomic_wait (futex)", hold, cpu, rss);

    std::thread::sleep(Duration::from_millis(200));
    let (cpu, rss) = idle_cb_parker(hold);
    report_idle("crossbeam_utils Parker", hold, cpu, rss);

    std::thread::sleep(Duration::from_millis(200));
    let (cpu, rss) = idle_parking_lot(hold);
    report_idle("parking_lot Cv+Mutex", hold, cpu, rss);

    std::thread::sleep(Duration::from_millis(200));
    let (cpu, rss) = idle_crossbeam(hold);
    report_idle("crossbeam::channel(1)", hold, cpu, rss);

    std::thread::sleep(Duration::from_millis(200));
    let (cpu, rss) = rt.block_on(idle_notify(hold));
    report_idle("tokio Notify (async)", hold, cpu, rss);

    println!();
}
