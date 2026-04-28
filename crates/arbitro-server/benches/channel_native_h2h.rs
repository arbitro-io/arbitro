//! `channel_native_h2h` — channel-only comparison, both primitives in
//! their NATIVE mode. No TCP. No mixing of async/sync.
//!
//! Companion to `writer_stack_h2h` (which adds TCP). This one isolates
//! the channel cost so we can tell apart "channel performance" from
//! "TCP throughput ceiling".
//!
//! Two patterns:
//!   A. **pure async**: tokio runtime (workers W), `tokio::sync::mpsc`.
//!      Producer tokio tasks call `Sender::send().await`; consumer tokio
//!      task calls `Receiver::recv().await + try_recv` to drain in
//!      bursts. Native waker parking on backpressure.
//!   B. **pure sync**:  N std::threads as producers, `kit::Mpsc` with
//!      `producer.send()` parking on `std::thread::park`. Single
//!      std::thread consumer drains via `consumer.recv_batch`. No
//!      tokio anywhere.
//!
//! Reports min / p50 / p99 / max. The "msg" travels through the channel
//! and the consumer simply increments a counter — no real work, no I/O.
//! The number we care about is **how fast can the channel move
//! N × FRAMES_PER_PROD messages from N producers into 1 drain**.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;
use tokio::runtime::Builder;
use tokio::sync::mpsc;

use arbitro_kit::route::Mpsc;

const N_PRODUCERS: usize = 16;              // ≤ physical cores to avoid over-subscription noise
const FRAMES_PER_PROD: usize = 1000;        // bench_safety ≤ 1000
const PAYLOAD_SIZE: usize = 256;            // matches writer_stack_h2h
const TOKIO_CHAN_CAP: usize = 8192;
const KIT_RING_CAP: usize = 256;
const RUNS: usize = 20;
const WARMUP: usize = 2;
const TOKIO_WORKER_SWEEP: &[usize] = &[1, 4, 8, 16];

#[inline]
fn make_frame() -> Bytes {
    Bytes::from(vec![0xABu8; PAYLOAD_SIZE])
}

/// Pattern A: pure async — tokio runtime hosts both producers and consumer.
fn run_pure_async(workers: usize) -> u128 {
    let total = N_PRODUCERS * FRAMES_PER_PROD;
    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let (tx, mut rx) = mpsc::channel::<Bytes>(TOKIO_CHAN_CAP);
        let received = Arc::new(AtomicUsize::new(0));
        let received_c = received.clone();

        let drain_h = tokio::spawn(async move {
            let mut got = 0usize;
            while got < total {
                match rx.recv().await {
                    Some(_b) => {
                        received_c.fetch_add(1, Ordering::Relaxed);
                        got += 1;
                        while let Ok(_b) = rx.try_recv() {
                            received_c.fetch_add(1, Ordering::Relaxed);
                            got += 1;
                        }
                    }
                    None => break,
                }
            }
        });

        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(N_PRODUCERS);
        for _ in 0..N_PRODUCERS {
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                let frame = make_frame();
                for _ in 0..FRAMES_PER_PROD {
                    let _ = tx.send(frame.clone()).await;
                }
            }));
        }
        for h in handles { let _ = h.await; }
        drop(tx);
        let _ = drain_h.await;
        t0.elapsed().as_nanos()
    })
}

/// Pattern B: pure sync — N std::threads as producers, kit::Mpsc, std::thread drain.
fn run_pure_sync() -> u128 {
    let total = N_PRODUCERS * FRAMES_PER_PROD;
    let (producers, consumer, shutdown) =
        Mpsc::<Bytes, KIT_RING_CAP>::new(N_PRODUCERS);
    let received = Arc::new(AtomicUsize::new(0));
    let received_c = received.clone();

    let drain_h = std::thread::Builder::new()
        .name("kit-drain".into())
        .spawn(move || {
            consumer.bind();
            let mut got = 0usize;
            while got < total {
                match consumer.recv_batch(|_b: Bytes| {
                    received_c.fetch_add(1, Ordering::Relaxed);
                }) {
                    Ok(n) => got += n,
                    Err(_) => break,
                }
            }
        })
        .unwrap();

    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(N_PRODUCERS);
    for producer in producers.into_iter() {
        handles.push(std::thread::spawn(move || {
            producer.bind();
            let frame = make_frame();
            for _ in 0..FRAMES_PER_PROD {
                producer.send(frame.clone());
            }
        }));
    }
    for h in handles { let _ = h.join(); }
    let _ = drain_h.join();
    let elapsed = t0.elapsed().as_nanos();
    shutdown.signal();
    elapsed
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() { return 0; }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn run_pattern<F: Fn() -> u128>(name: &str, runs_fn: F) -> u128 {
    let total = N_PRODUCERS * FRAMES_PER_PROD;
    for _ in 0..WARMUP { let _ = runs_fn(); }
    let mut times: Vec<u128> = (0..RUNS).map(|_| runs_fn()).collect();
    times.sort();
    let min = times[0];
    let p50 = percentile(&times, 0.50);
    let p99 = percentile(&times, 0.99);
    let max = *times.last().unwrap();

    let bps = |ns: u128| (total as f64 * PAYLOAD_SIZE as f64) / (ns as f64 / 1e9) / (1024.0 * 1024.0);
    let mps = |ns: u128| total as f64 / (ns as f64 / 1e9);

    println!("  {:<22}  min={:>6.2}ms  p50={:>6.2}ms  p99={:>6.2}ms  max={:>6.2}ms",
             name, min as f64 / 1e6, p50 as f64 / 1e6, p99 as f64 / 1e6, max as f64 / 1e6);
    println!("  {:<22}  msg/s p50={:>10.0}  MB/s p50={:>8.1}  (msg/s min={:>10.0})",
             "", mps(p50), bps(p50), mps(min));

    p50
}

fn main() {
    let total = N_PRODUCERS * FRAMES_PER_PROD;
    println!("=== channel_native_h2h: tokio::mpsc vs kit::Mpsc, native mode ===");
    println!("N_PRODUCERS={}  FRAMES_PER_PROD={}  PAYLOAD={} B  RUNS={}",
             N_PRODUCERS, FRAMES_PER_PROD, PAYLOAD_SIZE, RUNS);
    println!("Total bytes/run = {} B = {:.1} MB",
             total * PAYLOAD_SIZE,
             (total * PAYLOAD_SIZE) as f64 / (1024.0 * 1024.0));
    println!("Reporting min / p50 / p99 / max so noise is visible.");
    println!();

    println!("════ B. pure sync ({} std::threads + kit::Mpsc) ════", N_PRODUCERS);
    let sync_p50 = run_pattern("sync(std)", run_pure_sync);
    println!();

    let mut async_p50: Vec<(usize, u128)> = Vec::new();
    for &workers in TOKIO_WORKER_SWEEP {
        println!("════ A. pure async (tokio workers={} + tokio::mpsc) ════", workers);
        let p50 = run_pattern(&format!("async(w={})", workers),
                              || run_pure_async(workers));
        async_p50.push((workers, p50));
        println!();
    }

    let bps = |ns: u128| (total as f64 * PAYLOAD_SIZE as f64) / (ns as f64 / 1e9) / (1024.0 * 1024.0);
    let mps = |ns: u128| total as f64 / (ns as f64 / 1e9);

    println!("════ FINAL SUMMARY (p50, channel-only, no TCP) ════");
    println!("  sync(std)       : msg/s={:>10.0}  MB/s={:>8.1}",
             mps(sync_p50), bps(sync_p50));
    for (workers, p50) in &async_p50 {
        let ratio = sync_p50 as f64 / *p50 as f64;
        let label = if ratio < 0.95 { "sync wins" }
                    else if ratio > 1.05 { "async wins" }
                    else { "tie" };
        println!("  async(w={:<2})       : msg/s={:>10.0}  MB/s={:>8.1}  (sync/async={:.2}× — {})",
                 workers, mps(*p50), bps(*p50), ratio, label);
    }
}
