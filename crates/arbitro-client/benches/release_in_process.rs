//! release_in_process — release-primitive throughput WITHOUT TCP.
//!
//! Same architecture as `release_primitive` but the loopback TCP echo
//! is replaced by an in-process responder thread. This isolates the
//! cost of the release primitive itself from the syscall floor of TCP.
//!
//! Pipeline:
//!
//! ```
//! producer ── seq ──► kit::Mpsc ──► responder thread ──► release(seq) ──► producer
//! ```
//!
//! Variants:
//! 1. `tokio::sync::oneshot`  (async, multi-thread runtime)
//! 2. `kit::OneShot`          (OS threads, park-based)
//! 3. `tokio::sync::mpsc(1)`  (async, single-slot mpsc as a release primitive)
//!
//! All three share: identical Mpsc<u32, 4096> for the producer→responder
//! path, identical `FxHashMap<u32, Releaser>` for seq→release lookup,
//! identical responder thread.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arbitro_kit::route::{Mpsc, MpscConsumer, MpscProducer, MpscShutdown, OneShot as KitOneShot};
use rustc_hash::FxHashMap;

const RING_CAP: usize = 4096;

type Releaser = Box<dyn FnOnce() + Send>;
type SeqMap = Arc<Mutex<FxHashMap<u32, Releaser>>>;

// ── Harness: producer-mpsc + responder thread (no TCP) ──────────────────

struct Harness {
    prods: Vec<MpscProducer<u32, RING_CAP>>,
    map: SeqMap,
    seq_gen: Arc<AtomicU32>,
    teardown: Box<dyn FnOnce() + Send>,
}

fn spawn_responder(
    consumer: MpscConsumer<u32, RING_CAP>,
    map: SeqMap,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        consumer.bind();
        loop {
            match consumer.recv() {
                Ok(seq) => {
                    let r = map.lock().unwrap().remove(&seq);
                    if let Some(release) = r {
                        release();
                    }
                }
                Err(_) => return,
            }
        }
    })
}

fn setup(producers: usize) -> Harness {
    let (prods, cons, shutdown) = Mpsc::<u32, RING_CAP>::new(producers);
    let map: SeqMap = Arc::new(Mutex::new(FxHashMap::default()));
    let seq_gen = Arc::new(AtomicU32::new(1));

    let responder = spawn_responder(cons, map.clone());

    let teardown_shutdown: MpscShutdown<u32, RING_CAP> = shutdown;
    let teardown = Box::new(move || {
        teardown_shutdown.signal();
        let _ = responder.join();
    }) as Box<dyn FnOnce() + Send>;

    Harness { prods, map, seq_gen, teardown }
}

// ── Variant 1: tokio::sync::oneshot ─────────────────────────────────────

fn bench_tokio_oneshot(producers: usize, per_producer: u64) -> Duration {
    let Harness { mut prods, map, seq_gen, teardown } = setup(producers);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(producers.max(2))
        .enable_all()
        .build()
        .unwrap();

    let start = Instant::now();
    rt.block_on(async {
        let mut js = tokio::task::JoinSet::new();
        for _ in 0..producers {
            let prod = Arc::new(Mutex::new(prods.pop().unwrap()));
            let map = map.clone();
            let seq_gen = seq_gen.clone();
            js.spawn(async move {
                for _ in 0..per_producer {
                    let seq = seq_gen.fetch_add(1, Ordering::Relaxed);
                    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
                    map.lock().unwrap()
                        .insert(seq, Box::new(move || { let _ = tx.send(()); }));

                    loop {
                        let r = { prod.lock().unwrap().try_send(seq) };
                        match r {
                            Ok(()) => break,
                            Err(_) => tokio::task::yield_now().await,
                        }
                    }
                    let _ = rx.await;
                }
            });
        }
        while js.join_next().await.is_some() {}
    });
    let elapsed = start.elapsed();
    teardown();
    elapsed
}

// ── Variant 2: kit::OneShot (OS threads) ────────────────────────────────

fn bench_kit_oneshot(producers: usize, per_producer: u64) -> Duration {
    let Harness { mut prods, map, seq_gen, teardown } = setup(producers);

    let start = Instant::now();
    let handles: Vec<_> = (0..producers).map(|_| {
        let prod = prods.pop().unwrap();
        let map = map.clone();
        let seq_gen = seq_gen.clone();
        thread::spawn(move || {
            for _ in 0..per_producer {
                let seq = seq_gen.fetch_add(1, Ordering::Relaxed);
                let (tx, rx) = KitOneShot::<()>::new();
                map.lock().unwrap()
                    .insert(seq, Box::new(move || tx.send(())));

                while let Err(_) = prod.try_send(seq) {
                    thread::yield_now();
                }
                rx.bind();
                let _ = rx.recv();
            }
        })
    }).collect();
    for h in handles { h.join().unwrap(); }
    let elapsed = start.elapsed();
    teardown();
    elapsed
}

// ── Variant 3: tokio::sync::mpsc(1) as release primitive ────────────────

fn bench_tokio_mpsc(producers: usize, per_producer: u64) -> Duration {
    let Harness { mut prods, map, seq_gen, teardown } = setup(producers);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(producers.max(2))
        .enable_all()
        .build()
        .unwrap();

    let start = Instant::now();
    rt.block_on(async {
        let mut js = tokio::task::JoinSet::new();
        for _ in 0..producers {
            let prod = Arc::new(Mutex::new(prods.pop().unwrap()));
            let map = map.clone();
            let seq_gen = seq_gen.clone();
            js.spawn(async move {
                for _ in 0..per_producer {
                    let seq = seq_gen.fetch_add(1, Ordering::Relaxed);
                    // 1-slot mpsc: tx is cloneable but we use it once;
                    // the release callback consumes a clone via try_send.
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
                    map.lock().unwrap().insert(
                        seq,
                        Box::new(move || { let _ = tx.try_send(()); }),
                    );

                    loop {
                        let r = { prod.lock().unwrap().try_send(seq) };
                        match r {
                            Ok(()) => break,
                            Err(_) => tokio::task::yield_now().await,
                        }
                    }
                    let _ = rx.recv().await;
                }
            });
        }
        while js.join_next().await.is_some() {}
    });
    let elapsed = start.elapsed();
    teardown();
    elapsed
}

// ── Driver ──────────────────────────────────────────────────────────────

fn fmt_row(name: &str, producers: usize, per_p: u64, dur: Duration) {
    let total = (producers as u64) * per_p;
    let ns_per = dur.as_nanos() as f64 / total as f64;
    let mps = total as f64 / dur.as_secs_f64();
    println!(
        "  {name:24} | P={producers:>3} | {total:>9} acks | {:>9.2}ms | {ns_per:>8.0} ns/op | {mps:>11.0} ack/s",
        dur.as_secs_f64() * 1000.0,
    );
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
fn env_flag(key: &str) -> bool {
    matches!(std::env::var(key).as_deref(), Ok("1") | Ok("true") | Ok("yes"))
}

fn run_variant(name: &str, producers: usize, per_p: u64) -> Duration {
    match name {
        "tokio"   => bench_tokio_oneshot(producers, per_p),
        "oneshot" => bench_kit_oneshot(producers, per_p),
        "mpsc"    => bench_tokio_mpsc(producers, per_p),
        other     => panic!("unknown variant: {other}"),
    }
}

fn label_of(name: &str) -> &'static str {
    match name {
        "tokio"   => "1.tokio::oneshot",
        "oneshot" => "2.kit::OneShot (park)",
        "mpsc"    => "3.tokio::mpsc(1)",
        _         => "?",
    }
}

fn main() {
    let per_producer = env_u64("BENCH_MSGS", 5_000);
    let runs = env_u64("BENCH_RUNS", 1).max(1);
    let configs: Vec<usize> = env_str("BENCH_PRODUCERS", "1,4,16,64")
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let variant = env_str("BENCH_VARIANT", "all");

    let variants_to_run: Vec<&str> = if variant == "all" {
        vec!["tokio", "oneshot", "mpsc"]
    } else {
        vec![variant.as_str()]
    };
    let _keep = variant.clone();

    println!("release_in_process — release-primitive throughput WITHOUT TCP");
    println!("per-producer msgs = {per_producer}, runs = {runs}, producers = {configs:?}, variants = {variants_to_run:?}\n");
    println!("  {:24} | {:>4} | {:>8}      | {:>9}  | {:>11}  | {}",
        "variant", "P", "acks", "elapsed", "ns/op", "ack/s");
    println!("  {}", "-".repeat(100));

    if !env_flag("BENCH_NO_WARMUP") {
        println!("  (warmup: kit::OneShot P=1 msgs=200)");
        let _ = bench_kit_oneshot(1, 200);
    }

    for &p in &configs {
        println!();
        for v in &variants_to_run {
            let mut best: Option<Duration> = None;
            for _ in 0..runs {
                let d = run_variant(v, p, per_producer);
                best = Some(match best { Some(b) => b.min(d), None => d });
            }
            fmt_row(label_of(v), p, per_producer, best.unwrap());
        }
    }
}
