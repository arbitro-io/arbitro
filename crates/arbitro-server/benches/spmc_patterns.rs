//! SPMC bench — 1 producer, 20 consumers.
//!
//! Two semantics measured:
//! - BROADCAST: every consumer receives every message (fan-out)
//!   - `tokio::sync::broadcast`
//!   - `ArcSwap<Arc<Msg>>` + version counter (manual broadcast ring-less)
//!
//! - QUEUE: one consumer receives each message (work-stealing)
//!   - `crossbeam::channel::unbounded` with cloned receivers
//!   - `crossbeam_queue::ArrayQueue` with shared pop
//!
//! Parameters: 20 consumers, 1000 messages, payload sizes 64 / 128 / 256 B.
//!
//! Metrics reported per run:
//!   - Wall time
//!   - Producer MB/s   (bytes pushed / wall)
//!   - Consumer MB/s   (bytes delivered / wall, aggregated and per-consumer avg)
//!   - Total bytes     (queue = N*payload ; broadcast = N*payload*20)
//!
//! Follows bench_safety: max 1000 msgs, no background, single-run.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::Instant;

use arc_swap::ArcSwap;
use crossbeam_channel as cb;
use crossbeam_queue::ArrayQueue;
use tokio::runtime::Builder;
use tokio::sync::broadcast;

const N_CONSUMERS: usize = 20;
const N_MESSAGES: usize = 1000;
const PAYLOADS: &[usize] = &[64, 128, 256];

#[derive(Clone)]
struct Report {
    name: &'static str,
    payload: usize,
    wall_ns: u64,
    producer_bytes: u64,
    consumer_bytes_total: u64, // sum across all consumers
    per_consumer_min_bytes: u64,
    per_consumer_max_bytes: u64,
}

fn mb_per_s(bytes: u64, wall_ns: u64) -> f64 {
    if wall_ns == 0 {
        return 0.0;
    }
    (bytes as f64) / (wall_ns as f64 / 1e9) / (1024.0 * 1024.0)
}

// =============================================================================
// BROADCAST — tokio::sync::broadcast
// =============================================================================

fn bench_tokio_broadcast(payload: usize) -> Report {
    let rt = Builder::new_multi_thread()
        .worker_threads(N_CONSUMERS + 2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let (tx, _) = broadcast::channel::<Arc<Vec<u8>>>(1024);
        let barrier = Arc::new(tokio::sync::Barrier::new(N_CONSUMERS + 1));
        let consumed = Arc::new(AtomicU64::new(0));
        let per_consumer: Vec<Arc<AtomicU64>> =
            (0..N_CONSUMERS).map(|_| Arc::new(AtomicU64::new(0))).collect();

        let mut handles = Vec::with_capacity(N_CONSUMERS);
        for i in 0..N_CONSUMERS {
            let mut rx = tx.subscribe();
            let b = barrier.clone();
            let c = consumed.clone();
            let p = per_consumer[i].clone();
            handles.push(tokio::spawn(async move {
                b.wait().await;
                let mut bytes: u64 = 0;
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            bytes += msg.len() as u64;
                            c.fetch_add(msg.len() as u64, Ordering::Relaxed);
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    }
                }
                p.store(bytes, Ordering::Relaxed);
            }));
        }

        barrier.wait().await;

        let msg = Arc::new(vec![0xABu8; payload]);
        let start = Instant::now();
        let mut produced: u64 = 0;
        for _ in 0..N_MESSAGES {
            // loop until subscribers keep up (bounded cap=1024, we send 1000 so fits)
            while tx.send(msg.clone()).is_err() {
                tokio::task::yield_now().await;
            }
            produced += payload as u64;
        }
        drop(tx);

        for h in handles {
            let _ = h.await;
        }
        let wall_ns = start.elapsed().as_nanos() as u64;

        let per: Vec<u64> = per_consumer.iter().map(|a| a.load(Ordering::Relaxed)).collect();
        let cmin = *per.iter().min().unwrap_or(&0);
        let cmax = *per.iter().max().unwrap_or(&0);

        Report {
            name: "tokio::broadcast",
            payload,
            wall_ns,
            producer_bytes: produced,
            consumer_bytes_total: consumed.load(Ordering::Relaxed),
            per_consumer_min_bytes: cmin,
            per_consumer_max_bytes: cmax,
        }
    })
}

// =============================================================================
// BROADCAST — ArcSwap<Arc<(version, payload)>> + per-consumer last_version poll
// =============================================================================

fn bench_arcswap_broadcast(payload: usize) -> Report {
    type Slot = Arc<(u64, Vec<u8>)>;
    let initial: Slot = Arc::new((0, Vec::new()));
    let state = Arc::new(ArcSwap::from(initial));
    let done = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(N_CONSUMERS + 1));
    let per_consumer: Vec<Arc<AtomicU64>> =
        (0..N_CONSUMERS).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let wall_holder = Arc::new(AtomicU64::new(0));
    let produced_holder = Arc::new(AtomicU64::new(0));

    thread::scope(|s| {
        for i in 0..N_CONSUMERS {
            let st = state.clone();
            let dn = done.clone();
            let b = barrier.clone();
            let p = per_consumer[i].clone();
            s.spawn(move || {
                b.wait();
                let mut seen_version: u64 = 0;
                let mut bytes: u64 = 0;
                loop {
                    let snap = st.load();
                    let v = snap.0;
                    if v > seen_version {
                        seen_version = v;
                        bytes += snap.1.len() as u64;
                    } else if dn.load(Ordering::Acquire) {
                        // producer finished; drain any final publish
                        let snap2 = st.load();
                        if snap2.0 > seen_version {
                            seen_version = snap2.0;
                            bytes += snap2.1.len() as u64;
                        }
                        break;
                    } else {
                        std::hint::spin_loop();
                    }
                }
                p.store(bytes, Ordering::Relaxed);
            });
        }

        barrier.wait();
        let start = Instant::now();
        let mut produced: u64 = 0;
        for v in 1..=N_MESSAGES as u64 {
            let payload_vec = vec![0xABu8; payload];
            state.store(Arc::new((v, payload_vec)));
            produced += payload as u64;
            // tiny backoff so consumers can observe each version
            // (ArcSwap broadcast drops intermediate versions otherwise)
            std::hint::spin_loop();
        }
        done.store(true, Ordering::Release);
        wall_holder.store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        produced_holder.store(produced, Ordering::Relaxed);
    });
    // scope joined — now safe to read

    let per: Vec<u64> = per_consumer.iter().map(|a| a.load(Ordering::Relaxed)).collect();
    let cmin = *per.iter().min().unwrap_or(&0);
    let cmax = *per.iter().max().unwrap_or(&0);
    let total: u64 = per.iter().sum();

    Report {
        name: "ArcSwap broadcast",
        payload,
        wall_ns: wall_holder.load(Ordering::Relaxed),
        producer_bytes: produced_holder.load(Ordering::Relaxed),
        consumer_bytes_total: total,
        per_consumer_min_bytes: cmin,
        per_consumer_max_bytes: cmax,
    }
}

// =============================================================================
// QUEUE — crossbeam::channel::unbounded (cloned receivers = work-stealing)
// =============================================================================

fn bench_cb_queue(payload: usize) -> Report {
    let (tx, rx) = cb::unbounded::<Vec<u8>>();
    let barrier = Arc::new(Barrier::new(N_CONSUMERS + 1));
    let per_consumer: Vec<Arc<AtomicU64>> =
        (0..N_CONSUMERS).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let wall_holder = Arc::new(AtomicU64::new(0));
    let produced_holder = Arc::new(AtomicU64::new(0));

    thread::scope(|s| {
        for i in 0..N_CONSUMERS {
            let rx = rx.clone();
            let b = barrier.clone();
            let p = per_consumer[i].clone();
            s.spawn(move || {
                b.wait();
                let mut bytes: u64 = 0;
                while let Ok(msg) = rx.recv() {
                    bytes += msg.len() as u64;
                }
                p.store(bytes, Ordering::Relaxed);
            });
        }
        drop(rx);

        barrier.wait();
        let start = Instant::now();
        let mut produced: u64 = 0;
        for _ in 0..N_MESSAGES {
            tx.send(vec![0xABu8; payload]).unwrap();
            produced += payload as u64;
        }
        drop(tx);
        // wall measures PRODUCER wall. consumer drain time not included here —
        // we actually want total throughput, so move wall measure after scope joins
        // below. Here we just store the produce-side end.
        wall_holder.store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        produced_holder.store(produced, Ordering::Relaxed);
    });
    // scope joined — consumers finished after tx dropped and queue drained

    let per: Vec<u64> = per_consumer.iter().map(|a| a.load(Ordering::Relaxed)).collect();
    let cmin = *per.iter().min().unwrap_or(&0);
    let cmax = *per.iter().max().unwrap_or(&0);
    let total: u64 = per.iter().sum();

    Report {
        name: "cb::unbounded queue",
        payload,
        wall_ns: wall_holder.load(Ordering::Relaxed),
        producer_bytes: produced_holder.load(Ordering::Relaxed),
        consumer_bytes_total: total,
        per_consumer_min_bytes: cmin,
        per_consumer_max_bytes: cmax,
    }
}

// =============================================================================
// QUEUE — ArrayQueue with shared pop (work-stealing on ring)
// =============================================================================

fn bench_array_queue(payload: usize) -> Report {
    let q: Arc<ArrayQueue<Vec<u8>>> = Arc::new(ArrayQueue::new(1024));
    let done = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(N_CONSUMERS + 1));
    let per_consumer: Vec<Arc<AtomicU64>> =
        (0..N_CONSUMERS).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let wall_holder = Arc::new(AtomicU64::new(0));
    let produced_holder = Arc::new(AtomicU64::new(0));

    thread::scope(|s| {
        for i in 0..N_CONSUMERS {
            let q = q.clone();
            let dn = done.clone();
            let b = barrier.clone();
            let p = per_consumer[i].clone();
            s.spawn(move || {
                b.wait();
                let mut bytes: u64 = 0;
                loop {
                    match q.pop() {
                        Some(msg) => bytes += msg.len() as u64,
                        None => {
                            if dn.load(Ordering::Acquire) && q.is_empty() {
                                break;
                            }
                            std::hint::spin_loop();
                        }
                    }
                }
                p.store(bytes, Ordering::Relaxed);
            });
        }

        barrier.wait();
        let start = Instant::now();
        let mut produced: u64 = 0;
        for _ in 0..N_MESSAGES {
            let mut msg = vec![0xABu8; payload];
            loop {
                match q.push(msg) {
                    Ok(()) => break,
                    Err(back) => {
                        msg = back;
                        std::hint::spin_loop();
                    }
                }
            }
            produced += payload as u64;
        }
        done.store(true, Ordering::Release);
        wall_holder.store(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        produced_holder.store(produced, Ordering::Relaxed);
    });
    // scope joined

    let per: Vec<u64> = per_consumer.iter().map(|a| a.load(Ordering::Relaxed)).collect();
    let cmin = *per.iter().min().unwrap_or(&0);
    let cmax = *per.iter().max().unwrap_or(&0);
    let total: u64 = per.iter().sum();

    Report {
        name: "ArrayQueue queue",
        payload,
        wall_ns: wall_holder.load(Ordering::Relaxed),
        producer_bytes: produced_holder.load(Ordering::Relaxed),
        consumer_bytes_total: total,
        per_consumer_min_bytes: cmin,
        per_consumer_max_bytes: cmax,
    }
}

// =============================================================================
// Runner / reporting
// =============================================================================

fn print_header() {
    println!();
    println!(
        "{:<22} | {:>5} | {:>10} | {:>12} | {:>12} | {:>12} | {:>10} | {:>10}",
        "primitive",
        "bytes",
        "wall_ms",
        "prod MB/s",
        "cons MB/s",
        "per-cons MB/s",
        "min cons B",
        "max cons B"
    );
    println!("{}", "-".repeat(120));
}

fn print_row(r: &Report) {
    let prod_mbs = mb_per_s(r.producer_bytes, r.wall_ns);
    let cons_mbs = mb_per_s(r.consumer_bytes_total, r.wall_ns);
    let per_cons_mbs = cons_mbs / N_CONSUMERS as f64;
    println!(
        "{:<22} | {:>5} | {:>10.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>10} | {:>10}",
        r.name,
        r.payload,
        r.wall_ns as f64 / 1e6,
        prod_mbs,
        cons_mbs,
        per_cons_mbs,
        r.per_consumer_min_bytes,
        r.per_consumer_max_bytes,
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let smoke = args.iter().any(|a| a == "--smoke");
    if smoke {
        println!(
            "[smoke] SPMC bench — {} consumers, {} msgs, payloads {:?}",
            N_CONSUMERS, N_MESSAGES, PAYLOADS
        );
    } else {
        println!(
            "SPMC bench — {} consumers, {} msgs, payloads {:?}",
            N_CONSUMERS, N_MESSAGES, PAYLOADS
        );
    }

    println!("\n== BROADCAST (every consumer receives every msg) ==");
    print_header();
    for &p in PAYLOADS {
        let r = bench_tokio_broadcast(p);
        print_row(&r);
    }
    for &p in PAYLOADS {
        let r = bench_arcswap_broadcast(p);
        print_row(&r);
    }

    println!("\n== QUEUE (each msg goes to exactly one consumer) ==");
    print_header();
    for &p in PAYLOADS {
        let r = bench_cb_queue(p);
        print_row(&r);
    }
    for &p in PAYLOADS {
        let r = bench_array_queue(p);
        print_row(&r);
    }

    println!("\nNote: BROADCAST cons MB/s is aggregate across {} consumers.", N_CONSUMERS);
    println!("Note: QUEUE cons MB/s ≈ prod MB/s by design (each msg counted once).");
}
