//! tokio_scale — validates "same code, 1 core or 16 cores".
//!
//! Topology:
//!
//!     publish (task) ─┐                 ┌──► Shard 0 (actor)
//!                     ├── mpsc per ──┬──┼──► Shard 1 (actor)
//!     drain   (task) ─┘   shard      └──┼──► ...
//!                                       └──► Shard 7 (actor)
//!
//! publish and drain both hold `Vec<Sender<Msg>>`: each can target ANY shard.
//! Shards are actor-style async tasks (single owner, no locks).
//!
//! Run variants: worker_threads = 1, 4, 16. Same code, different runtime shape.
//! We report throughput and fairness (per-shard msg count).
//!
//! Follows bench_safety: 2000 total msgs (1000 publish + 1000 drain), single-run.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::runtime::Builder;
use tokio::sync::mpsc;

const N_SHARDS: usize = 8;
const N_MSGS_PUBLISH: usize = 1000;
const N_MSGS_DRAIN: usize = 1000;
const PAYLOAD: usize = 128;
const CAP: usize = 1024;

#[derive(Clone)]
struct Msg {
    #[allow(dead_code)]
    origin: u8, // 0 = publish, 1 = drain
    payload: Arc<Vec<u8>>,
    sent_ns: u64,
}

struct Report {
    worker_threads: usize,
    wall_ns: u64,
    total_msgs: u64,
    per_shard: Vec<u64>,
    latency_sum_ns: u64,
}

fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

async fn shard_actor(
    mut rx: mpsc::Receiver<Msg>,
    counter: Arc<AtomicU64>,
    lat_sum: Arc<AtomicU64>,
) {
    let mut local: u64 = 0;
    let mut local_lat: u64 = 0;
    while let Some(msg) = rx.recv().await {
        local += 1;
        let now = now_ns();
        local_lat += now.saturating_sub(msg.sent_ns);
        // touch payload so compiler can't drop it
        std::hint::black_box(msg.payload.len());
    }
    counter.store(local, Ordering::Relaxed);
    lat_sum.fetch_add(local_lat, Ordering::Relaxed);
}

async fn dispatcher_task(
    origin: u8,
    senders: Vec<mpsc::Sender<Msg>>,
    n: usize,
    payload: Arc<Vec<u8>>,
) {
    for i in 0..n {
        let shard = i % senders.len();
        let msg = Msg {
            origin,
            payload: payload.clone(),
            sent_ns: now_ns(),
        };
        // backpressure if channel full — await
        senders[shard].send(msg).await.ok();
    }
    // drop senders clones here → each dispatcher releases its refs
}

fn run(worker_threads: usize) -> Report {
    let rt = Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let mut senders: Vec<mpsc::Sender<Msg>> = Vec::with_capacity(N_SHARDS);
        let mut counters: Vec<Arc<AtomicU64>> = Vec::with_capacity(N_SHARDS);
        let lat_sum = Arc::new(AtomicU64::new(0));
        let mut shard_handles = Vec::with_capacity(N_SHARDS);

        for _ in 0..N_SHARDS {
            let (tx, rx) = mpsc::channel::<Msg>(CAP);
            let c = Arc::new(AtomicU64::new(0));
            counters.push(c.clone());
            senders.push(tx);
            let ls = lat_sum.clone();
            shard_handles.push(tokio::spawn(shard_actor(rx, c, ls)));
        }

        let payload = Arc::new(vec![0xABu8; PAYLOAD]);

        // Clone senders for each dispatcher
        let publish_senders = senders.clone();
        let drain_senders = senders.clone();
        // Drop the originals so shard receivers close when dispatchers finish
        drop(senders);

        let start = Instant::now();

        let publish_handle = tokio::spawn(dispatcher_task(
            0,
            publish_senders,
            N_MSGS_PUBLISH,
            payload.clone(),
        ));
        let drain_handle = tokio::spawn(dispatcher_task(
            1,
            drain_senders,
            N_MSGS_DRAIN,
            payload.clone(),
        ));

        // Wait for both dispatchers
        let _ = publish_handle.await;
        let _ = drain_handle.await;

        // Wait for shards to finish draining (receivers close when all senders dropped)
        for h in shard_handles {
            let _ = h.await;
        }

        let wall_ns = start.elapsed().as_nanos() as u64;
        let per_shard: Vec<u64> =
            counters.iter().map(|c| c.load(Ordering::Relaxed)).collect();
        let total_msgs: u64 = per_shard.iter().sum();

        Report {
            worker_threads,
            wall_ns,
            total_msgs,
            per_shard,
            latency_sum_ns: lat_sum.load(Ordering::Relaxed),
        }
    })
}

fn print_report(r: &Report) {
    let wall_ms = r.wall_ns as f64 / 1e6;
    let tput = r.total_msgs as f64 / (r.wall_ns as f64 / 1e9);
    let bytes = r.total_msgs * PAYLOAD as u64;
    let mbs = (bytes as f64) / (r.wall_ns as f64 / 1e9) / (1024.0 * 1024.0);
    let avg_lat_ns = if r.total_msgs > 0 {
        r.latency_sum_ns / r.total_msgs
    } else {
        0
    };
    let min = *r.per_shard.iter().min().unwrap_or(&0);
    let max = *r.per_shard.iter().max().unwrap_or(&0);
    let avg = r.total_msgs / r.per_shard.len() as u64;

    println!(
        "worker_threads = {:>2}  |  wall = {:>7.2} ms  |  total = {:>5} msgs  |  {:>10.0} msgs/s  |  {:>7.2} MB/s  |  avg_lat = {:>7} ns  |  per-shard min/avg/max = {}/{}/{}",
        r.worker_threads, wall_ms, r.total_msgs, tput, mbs, avg_lat_ns, min, avg, max
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let smoke = args.iter().any(|a| a == "--smoke");

    println!(
        "{} tokio_scale — {} shards, {} publish + {} drain msgs, payload {} B",
        if smoke { "[smoke]" } else { "[full]" },
        N_SHARDS,
        N_MSGS_PUBLISH,
        N_MSGS_DRAIN,
        PAYLOAD
    );
    println!();

    let configs = if smoke {
        vec![1usize, 4, 16]
    } else {
        vec![1usize, 2, 4, 8, 16]
    };

    for wt in configs {
        let r = run(wt);
        print_report(&r);
    }

    println!();
    println!("Note: SAME code path for every row. Only `worker_threads` changed.");
    println!("Note: per-shard min/max shows fairness — round-robin should give uniform.");
}
