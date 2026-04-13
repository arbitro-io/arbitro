//! Micro-bench: cost of checking channel capacity before sending.
//!
//! Measures the overhead of different backpressure strategies so we can
//! decide what to use in the drainer hot path.
//!
//! Run: cargo bench --bench capacity_check -p arbitro-e2e

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;

const ITERATIONS: u64 = 10_000_000;
const CHANNEL_CAP: usize = 8192;
const ROUNDS: usize = 5;

fn fmt_ns(d: Duration) -> String {
    let ns = d.as_nanos() as f64 / ITERATIONS as f64;
    if ns >= 1000.0 {
        format!("{:.1}µs", ns / 1000.0)
    } else {
        format!("{:.1}ns", ns)
    }
}

fn fmt_ns_custom(d: Duration, iters: u64) -> String {
    let ns = d.as_nanos() as f64 / iters as f64;
    if ns >= 1000.0 {
        format!("{:.1}µs", ns / 1000.0)
    } else {
        format!("{:.1}ns", ns)
    }
}

/// Run a bench closure ROUNDS times with 1 warmup round.
fn bench_rounds(name: &str, mut f: impl FnMut()) {
    f(); // warmup
    let mut times = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let t0 = Instant::now();
        f();
        times.push(t0.elapsed());
    }
    times.sort();
    println!(
        "  {:<32} min={:<9} med={:<9} max={}",
        name, fmt_ns(times[0]), fmt_ns(times[ROUNDS / 2]), fmt_ns(times[ROUNDS - 1]),
    );
}

fn bench_rounds_custom(name: &str, iters: u64, mut f: impl FnMut()) {
    f(); // warmup
    let mut times = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let t0 = Instant::now();
        f();
        times.push(t0.elapsed());
    }
    times.sort();
    println!(
        "  {:<32} min={:<9} med={:<9} max={}",
        name,
        fmt_ns_custom(times[0], iters),
        fmt_ns_custom(times[ROUNDS / 2], iters),
        fmt_ns_custom(times[ROUNDS - 1], iters),
    );
}

// ─── Simulated ActiveBinding with cached tx ────────────────────────────────

struct SimBinding {
    consumer_id: u32,
    connection_id: u64,
    max_inflight: u32,
    fire_and_forget: bool,
    paused: bool,
    /// Cached at subscribe time — one clone, then free reads forever.
    tx: mpsc::Sender<Bytes>,
}

fn main() {
    println!("Capacity check micro-bench — {} iter × {} rounds + warmup", ITERATIONS, ROUNDS);
    println!("{}", "=".repeat(80));

    let (tx, _rx) = mpsc::channel::<Bytes>(CHANNEL_CAP);

    // ── Section 1: Isolated primitives ─────────────────────────────────────
    println!("\n  --- Isolated primitives ---");

    bench_rounds("tx.capacity()", || {
        let mut sum = 0u64;
        for _ in 0..ITERATIONS { sum += tx.capacity() as u64; }
        std::hint::black_box(sum);
    });

    bench_rounds("tx.is_closed()", || {
        let mut sum = 0u64;
        for _ in 0..ITERATIONS { if !tx.is_closed() { sum += 1; } }
        std::hint::black_box(sum);
    });

    {
        let s = Arc::new(Mutex::new({
            let mut m: HashMap<u64, mpsc::Sender<Bytes>> = HashMap::new();
            for i in 1..=10 { m.insert(i, tx.clone()); }
            m
        }));
        let s2 = s.clone();
        bench_rounds("mutex+get+capacity", move || {
            let mut sum = 0u64;
            for _ in 0..ITERATIONS {
                let g = s2.lock().unwrap();
                if let Some(tx) = g.get(&1) { sum += tx.capacity() as u64; }
            }
            std::hint::black_box(sum);
        });
    }

    // ── Section 2: Simulated drainer — current vs cached tx ────────────────
    println!("\n  --- Drainer simulation: 10 bindings, check capacity per binding ---");

    let num_bindings = 10;
    let inflight: Vec<u32> = vec![50; 1024]; // simulate some inflight

    // Build bindings with cached tx
    let bindings: Vec<SimBinding> = (0..num_bindings)
        .map(|i| SimBinding {
            consumer_id: i as u32,
            connection_id: (i + 1) as u64,
            max_inflight: 1000,
            fire_and_forget: false,
            paused: false,
            tx: tx.clone(), // one clone per binding, at subscribe time
        })
        .collect();

    // Build registry (current approach)
    let registry = Arc::new(Mutex::new({
        let mut m: HashMap<u64, mpsc::Sender<Bytes>> = HashMap::new();
        for b in &bindings {
            m.insert(b.connection_id, tx.clone());
        }
        m
    }));

    let cycles = ITERATIONS / num_bindings as u64;

    // 2a. CURRENT: per-binding, go through registry mutex for capacity check
    {
        let reg = registry.clone();
        bench_rounds_custom("current: mutex per binding", cycles * num_bindings as u64, || {
            let mut can_send = 0u64;
            for _ in 0..cycles {
                for b in &bindings {
                    if b.paused { continue; }
                    if !b.fire_and_forget && inflight[b.consumer_id as usize] >= b.max_inflight {
                        continue;
                    }
                    // Check channel capacity through registry (mutex)
                    let guard = reg.lock().unwrap();
                    if let Some(tx) = guard.get(&b.connection_id) {
                        if tx.capacity() > 0 { can_send += 1; }
                    }
                }
            }
            std::hint::black_box(can_send);
        });
    }

    // 2b. CACHED TX: per-binding, use cached tx directly
    {
        bench_rounds_custom("cached: binding.tx.capacity()", cycles * num_bindings as u64, || {
            let mut can_send = 0u64;
            for _ in 0..cycles {
                for b in &bindings {
                    if b.paused { continue; }
                    if !b.fire_and_forget && inflight[b.consumer_id as usize] >= b.max_inflight {
                        continue;
                    }
                    // Check channel capacity directly from cached tx
                    if b.tx.capacity() > 0 { can_send += 1; }
                }
            }
            std::hint::black_box(can_send);
        });
    }

    // 2c. NO CHECK: per-binding, only engine checks (what we do today minus send)
    {
        bench_rounds_custom("baseline: engine checks only", cycles * num_bindings as u64, || {
            let mut can_send = 0u64;
            for _ in 0..cycles {
                for b in &bindings {
                    if b.paused { continue; }
                    if !b.fire_and_forget && inflight[b.consumer_id as usize] >= b.max_inflight {
                        continue;
                    }
                    can_send += 1;
                }
            }
            std::hint::black_box(can_send);
        });
    }

    // ── Section 3: Full send path comparison ───────────────────────────────
    println!("\n  --- Full send: check + send per frame (1 binding) ---");

    let send_iters = 1_000_000u64;

    // 3a. Current: mutex + clone + blocking_send
    {
        let (tx_a, mut rx_a) = mpsc::channel::<Bytes>(CHANNEL_CAP);
        let reg_a = Arc::new(Mutex::new({
            let mut m: HashMap<u64, mpsc::Sender<Bytes>> = HashMap::new();
            m.insert(1, tx_a);
            m
        }));
        let drain = std::thread::spawn(move || {
            let mut c = 0u64;
            while let Some(_) = rx_a.blocking_recv() { c += 1; }
            c
        });
        let frame = Bytes::from_static(b"x");
        // warmup
        for _ in 0..1000 {
            let g = reg_a.lock().unwrap();
            if let Some(tx) = g.get(&1) { let c = tx.clone(); drop(g); let _ = c.blocking_send(frame.clone()); }
        }
        let mut times = Vec::with_capacity(ROUNDS);
        for _ in 0..ROUNDS {
            let t0 = Instant::now();
            for _ in 0..send_iters {
                let g = reg_a.lock().unwrap();
                if let Some(tx) = g.get(&1) {
                    let c = tx.clone();
                    drop(g);
                    let _ = c.blocking_send(frame.clone());
                }
            }
            times.push(t0.elapsed());
        }
        times.sort();
        println!(
            "  {:<32} min={:<9} med={:<9} max={}",
            "current: mutex+clone+block_send",
            fmt_ns_custom(times[0], send_iters),
            fmt_ns_custom(times[ROUNDS/2], send_iters),
            fmt_ns_custom(times[ROUNDS-1], send_iters),
        );
        drop(reg_a);
        let _ = drain.join().unwrap();
    }

    // 3b. Cached: check capacity + try_send (no mutex at all)
    {
        let (tx_b, mut rx_b) = mpsc::channel::<Bytes>(CHANNEL_CAP);
        let drain = std::thread::spawn(move || {
            let mut c = 0u64;
            while let Some(_) = rx_b.blocking_recv() { c += 1; }
            c
        });
        let frame = Bytes::from_static(b"x");
        // warmup
        for _ in 0..1000 { let _ = tx_b.try_send(frame.clone()); }
        std::thread::sleep(Duration::from_millis(10));
        let mut times = Vec::with_capacity(ROUNDS);
        for _ in 0..ROUNDS {
            let t0 = Instant::now();
            for _ in 0..send_iters {
                if tx_b.capacity() > 0 {
                    let _ = tx_b.try_send(frame.clone());
                }
            }
            times.push(t0.elapsed());
            std::thread::sleep(Duration::from_millis(5));
        }
        times.sort();
        println!(
            "  {:<32} min={:<9} med={:<9} max={}",
            "cached: cap_check+try_send",
            fmt_ns_custom(times[0], send_iters),
            fmt_ns_custom(times[ROUNDS/2], send_iters),
            fmt_ns_custom(times[ROUNDS-1], send_iters),
        );
        drop(tx_b);
        let _ = drain.join().unwrap();
    }

    // 3c. Cached: try_send only (no pre-check, handle Full in result)
    {
        let (tx_c, mut rx_c) = mpsc::channel::<Bytes>(CHANNEL_CAP);
        let drain = std::thread::spawn(move || {
            let mut c = 0u64;
            while let Some(_) = rx_c.blocking_recv() { c += 1; }
            c
        });
        let frame = Bytes::from_static(b"x");
        for _ in 0..1000 { let _ = tx_c.try_send(frame.clone()); }
        std::thread::sleep(Duration::from_millis(10));
        let mut times = Vec::with_capacity(ROUNDS);
        for _ in 0..ROUNDS {
            let t0 = Instant::now();
            for _ in 0..send_iters {
                let _ = tx_c.try_send(frame.clone());
            }
            times.push(t0.elapsed());
            std::thread::sleep(Duration::from_millis(5));
        }
        times.sort();
        println!(
            "  {:<32} min={:<9} med={:<9} max={}",
            "cached: try_send only",
            fmt_ns_custom(times[0], send_iters),
            fmt_ns_custom(times[ROUNDS/2], send_iters),
            fmt_ns_custom(times[ROUNDS-1], send_iters),
        );
        drop(tx_c);
        let _ = drain.join().unwrap();
    }

    println!("\n{}", "=".repeat(80));
}
