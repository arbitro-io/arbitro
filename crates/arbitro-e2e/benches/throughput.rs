//! Benchmark: publish ingestion + replay (drain) throughput.
//!
//! Measures publish and replay throughput scaling across cores.
//! Each connection publishes to its own stream → different shards → real parallelism.
//!
//! No Criterion — direct measurement with timeout protection.

extern crate libc;

use std::time::{Duration, Instant};

use tokio::runtime::Runtime;

use arbitro_client::Client;
use arbitro_proto::config::{ConsumerConfig, DeliverPolicy, JournalKind, StreamConfig};
use arbitro_server::{ArbitroServer, Config};

// ── Settings ────────────────────────────────────────────────────

/// Switch between Memory and Tolerant (disk mmap) without touching anything else.
const JOURNAL_KIND: JournalKind = JournalKind::Memory;
/// Only used when JOURNAL_KIND == Tolerant.
const TOLERANT_DATA_DIR: &str = "/tmp/arbitro_bench_tolerant";

const ITERATIONS: u32        = 50;
const MSGS_PER_CLIENT: u32   = 1_000;
const BATCH_SIZE: usize      = 1_000;
/// Messages pre-published per stream for the replay scenario.
const REPLAY_MSGS: u32       = 10_000;
const CONCURRENCY: &[usize]  = &[1, 2, 4, 8, 16, 32];
const MAX_STREAMS: usize     = 32;
const LEVEL_TIMEOUT: Duration = Duration::from_secs(15);
const REPLAY_TIMEOUT: Duration = Duration::from_secs(30);

// ── Metrics ─────────────────────────────────────────────────────

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

// ── Infrastructure ──────────────────────────────────────────────

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

async fn start_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");

    let mut config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(500)
        .write_buffer_cap(8192);

    if matches!(JOURNAL_KIND, JournalKind::Tolerant) {
        config = config.data_dir(TOLERANT_DATA_DIR);
    }

    let server = ArbitroServer::new(config);
    tokio::spawn(async move { let _ = server.run().await; });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect_with_timeout(addr, Duration::from_secs(5))
        .await
        .expect("client must connect")
}

// ── Single publish (1 msg per RTT) ─────────────────────────────

async fn run_single(clients: &[Client], stream_names: &[Vec<u8>], msgs: u32) -> Duration {
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let stream = stream_names[i % stream_names.len()].clone();
        js.spawn(async move {
            let payload = vec![0u8; 64];
            for _ in 0..msgs {
                c.publish(&stream, b"bench.msg", &payload).await.expect("publish");
            }
        });
    }
    while js.join_next().await.is_some() {}
    start.elapsed()
}

// ── Batch publish (BATCH_SIZE msgs per RTT) ────────────────────

async fn run_batch(clients: &[Client], stream_names: &[Vec<u8>], batch_size: usize) -> Duration {
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let stream = stream_names[i % stream_names.len()].clone();
        js.spawn(async move {
            let payload = vec![0u8; 64];
            let entries: Vec<(&[u8], &[u8])> = (0..batch_size)
                .map(|_| (b"bench.msg".as_slice(), payload.as_slice()))
                .collect();
            c.publish_batch(&stream, &entries).await.expect("publish_batch");
        });
    }
    while js.join_next().await.is_some() {}
    start.elapsed()
}

// ── Replay drain (subscribe from seq=1, drain all pre-published msgs) ──

/// Pre-populate each stream with REPLAY_MSGS messages.
async fn prefill_streams(client: &Client, stream_names: &[Vec<u8>], n: usize, msgs: u32) {
    let payload = vec![0u8; 64];
    let mut js = tokio::task::JoinSet::new();
    for i in 0..n {
        let c = client.clone();
        let stream = stream_names[i % stream_names.len()].clone();
        let payload = payload.clone();
        let batches = (msgs as usize + BATCH_SIZE - 1) / BATCH_SIZE;
        js.spawn(async move {
            for _ in 0..batches {
                let size = BATCH_SIZE.min(msgs as usize);
                let entries: Vec<(&[u8], &[u8])> = (0..size)
                    .map(|_| (b"bench.msg".as_slice(), payload.as_slice()))
                    .collect();
                c.publish_batch(&stream, &entries).await.expect("prefill publish_batch");
            }
        });
    }
    while js.join_next().await.is_some() {}
}

/// One replay iteration: for each of the n streams, create a consumer from seq=1
/// and drain all REPLAY_MSGS messages. Returns elapsed time.
async fn run_replay(
    setup_client: &Client,
    stream_names: &[Vec<u8>],
    n: usize,
    msgs_per_stream: u32,
    iter: u32,
) -> Duration {
    // Create n consumers (one per stream), all with DeliverPolicy::All (replay from start).
    let mut consumers = Vec::with_capacity(n);
    for i in 0..n {
        let stream = &stream_names[i % stream_names.len()];
        let name = format!("replay_{}_{i}", iter);
        let cfg = ConsumerConfig::new(name.as_bytes(), stream)
            .deliver_policy(DeliverPolicy::All)
            .build();
        let consumer = setup_client.create_consumer(&cfg).await.expect("create consumer");
        consumers.push(consumer);
    }

    // Subscribe all consumers and drain concurrently.
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for consumer in consumers {
        let expected = msgs_per_stream;
        js.spawn(async move {
            let mut handle = consumer.subscribe(None).await.expect("subscribe");
            let mut count = 0u32;
            while count < expected {
                if handle.next().await.is_none() { break; }
                count += 1;
            }
            // Clean up consumer after drain
            let _ = consumer.delete().await;
        });
    }
    while js.join_next().await.is_some() {}
    start.elapsed()
}

// ── Bench result ────────────────────────────────────────────────

struct BenchResult {
    label: String,
    avg: Duration,
    throughput: f64,
    per_conn: f64,
    rss: u64,
    rss_delta: u64,
    cpu_pct: f64,
}

fn print_header() {
    println!("  {:30} | {:>9} | {:>12} | {:>10} | {:>8} | {:>8} | {:>7}",
        "Config", "Avg time", "Throughput", "Per-conn", "RSS", "Δ RSS", "CPU");
    println!("  {}", "-".repeat(100));
}

fn print_result(r: &BenchResult) {
    println!("  {:30} | {:>9.2?} | {:>10.0} msg/s | {:>8.0} msg/s | {:>5} MB | {:>+5} MB | {:>5.1}%",
        r.label, r.avg, r.throughput, r.per_conn,
        r.rss / 1024, r.rss_delta as i64 / 1024, r.cpu_pct);
}

// ── Cleanup ─────────────────────────────────────────────────────

fn cleanup_tolerant() {
    if !matches!(JOURNAL_KIND, JournalKind::Tolerant) { return; }
    let data_path = std::path::Path::new(TOLERANT_DATA_DIR);
    if data_path.exists() {
        let total_bytes = walkdir(data_path);
        println!("Tolerant store data written: {:.2} MB — cleaning up...", total_bytes as f64 / 1_048_576.0);
        let _ = std::fs::remove_dir_all(data_path);
        println!("Cleaned up: {}", if data_path.exists() { "FAILED" } else { "OK" });
    } else {
        println!("WARNING: data_dir was never created — Tolerant store did not write to disk!");
    }
}

fn walkdir(p: &std::path::Path) -> u64 {
    std::fs::read_dir(p).ok().map(|rd| {
        rd.filter_map(|e| e.ok()).map(|e| {
            let m = e.metadata().ok();
            if e.path().is_dir() { walkdir(&e.path()) }
            else { m.map(|m| m.len()).unwrap_or(0) }
        }).sum()
    }).unwrap_or(0)
}

// ── Main ────────────────────────────────────────────────────────

fn main() {
    let journal_label = match JOURNAL_KIND {
        JournalKind::Memory   => "memory",
        JournalKind::Disk     => "disk",
        JournalKind::Tolerant => "tolerant",
    };

    let rt = Runtime::new().unwrap();
    let addr = rt.block_on(start_server());

    // Create streams (enough for max concurrency).
    let stream_names: Vec<Vec<u8>> = (0..MAX_STREAMS)
        .map(|i| format!("ingest_{i}").into_bytes())
        .collect();

    let setup_client = rt.block_on(connect(&addr));
    rt.block_on(async {
        for name in &stream_names {
            setup_client
                .create_stream(
                    &StreamConfig::new(name, b">")
                        .journal_kind(JOURNAL_KIND)
                        .build(),
                )
                .await
                .expect("create stream");
        }
    });

    println!("\nPublish + Replay Throughput — 64B payload, {ITERATIONS} iter, journal={journal_label}");
    println!("{}", "=".repeat(110));

    // ── Single publish ──────────────────────────────────────────
    println!("\n[ publish_single — {MSGS_PER_CLIENT} msgs/client/iter ]");
    print_header();

    for &n in CONCURRENCY {
        let clients: Vec<Client> = rt.block_on(async {
            let mut v = Vec::with_capacity(n);
            for _ in 0..n { v.push(connect(&addr).await); }
            v
        });

        let total_msgs_per_iter = MSGS_PER_CLIENT as u64 * n as u64;
        let label = format!("{n}conn_{n}stream/{total_msgs_per_iter}");

        rt.block_on(async {
            let _ = tokio::time::timeout(LEVEL_TIMEOUT, run_single(&clients, &stream_names, 100)).await;

            let rss_before = rss_kb();
            let cpu_before = cpu_time_ns();
            let mut total_time = Duration::ZERO;

            for _ in 0..ITERATIONS {
                match tokio::time::timeout(LEVEL_TIMEOUT, run_single(&clients, &stream_names, MSGS_PER_CLIENT)).await {
                    Ok(d) => total_time += d,
                    Err(_) => { println!("  {label:30} | TIMEOUT ({LEVEL_TIMEOUT:?})"); return; }
                }
            }

            let cpu_after = cpu_time_ns();
            let rss_after = rss_kb();
            let wall_ns = total_time.as_nanos() as u64;
            let cpu_ns  = cpu_after.saturating_sub(cpu_before);
            let cpu_pct = if wall_ns > 0 { cpu_ns as f64 / wall_ns as f64 * 100.0 } else { 0.0 };
            let total_msgs_all = total_msgs_per_iter * ITERATIONS as u64;

            print_result(&BenchResult {
                label,
                avg:        total_time / ITERATIONS,
                throughput: total_msgs_all as f64 / total_time.as_secs_f64(),
                per_conn:   total_msgs_all as f64 / total_time.as_secs_f64() / n as f64,
                rss:        rss_after,
                rss_delta:  rss_after.saturating_sub(rss_before),
                cpu_pct,
            });
        });
    }

    // ── Batch publish ───────────────────────────────────────────
    println!("\n[ publish_batch — {BATCH_SIZE} msgs/batch/client/iter ]");
    print_header();

    for &n in CONCURRENCY {
        let clients: Vec<Client> = rt.block_on(async {
            let mut v = Vec::with_capacity(n);
            for _ in 0..n { v.push(connect(&addr).await); }
            v
        });

        let total_msgs_per_iter = BATCH_SIZE as u64 * n as u64;
        let label = format!("{n}conn_{n}stream/{total_msgs_per_iter}");

        rt.block_on(async {
            let _ = tokio::time::timeout(LEVEL_TIMEOUT, run_batch(&clients, &stream_names, BATCH_SIZE)).await;

            let rss_before = rss_kb();
            let cpu_before = cpu_time_ns();
            let mut total_time = Duration::ZERO;

            for _ in 0..ITERATIONS {
                match tokio::time::timeout(LEVEL_TIMEOUT, run_batch(&clients, &stream_names, BATCH_SIZE)).await {
                    Ok(d) => total_time += d,
                    Err(_) => { println!("  {label:30} | TIMEOUT ({LEVEL_TIMEOUT:?})"); return; }
                }
            }

            let cpu_after = cpu_time_ns();
            let rss_after = rss_kb();
            let wall_ns = total_time.as_nanos() as u64;
            let cpu_ns  = cpu_after.saturating_sub(cpu_before);
            let cpu_pct = if wall_ns > 0 { cpu_ns as f64 / wall_ns as f64 * 100.0 } else { 0.0 };
            let total_msgs_all = total_msgs_per_iter * ITERATIONS as u64;

            print_result(&BenchResult {
                label,
                avg:        total_time / ITERATIONS,
                throughput: total_msgs_all as f64 / total_time.as_secs_f64(),
                per_conn:   total_msgs_all as f64 / total_time.as_secs_f64() / n as f64,
                rss:        rss_after,
                rss_delta:  rss_after.saturating_sub(rss_before),
                cpu_pct,
            });
        });
    }

    // ── Replay (drain pre-published backlog) ────────────────────
    println!("\n[ replay_drain — {REPLAY_MSGS} msgs pre-loaded/stream, DeliverPolicy::All ]");
    print_header();

    for &n in CONCURRENCY {
        let total_msgs_per_iter = REPLAY_MSGS as u64 * n as u64;
        let label = format!("{n}conn_{n}stream/{total_msgs_per_iter}");

        rt.block_on(async {
            // Pre-fill streams with REPLAY_MSGS messages each (done once per n, outside timing).
            prefill_streams(&setup_client, &stream_names, n, REPLAY_MSGS).await;

            let rss_before = rss_kb();
            let cpu_before = cpu_time_ns();
            let mut total_time = Duration::ZERO;

            for iter in 0..ITERATIONS {
                match tokio::time::timeout(
                    REPLAY_TIMEOUT,
                    run_replay(&setup_client, &stream_names, n, REPLAY_MSGS, iter),
                ).await {
                    Ok(d) => total_time += d,
                    Err(_) => { println!("  {label:30} | TIMEOUT ({REPLAY_TIMEOUT:?})"); return; }
                }
            }

            let cpu_after = cpu_time_ns();
            let rss_after = rss_kb();
            let wall_ns = total_time.as_nanos() as u64;
            let cpu_ns  = cpu_after.saturating_sub(cpu_before);
            let cpu_pct = if wall_ns > 0 { cpu_ns as f64 / wall_ns as f64 * 100.0 } else { 0.0 };
            let total_msgs_all = total_msgs_per_iter * ITERATIONS as u64;

            print_result(&BenchResult {
                label,
                avg:        total_time / ITERATIONS,
                throughput: total_msgs_all as f64 / total_time.as_secs_f64(),
                per_conn:   total_msgs_all as f64 / total_time.as_secs_f64() / n as f64,
                rss:        rss_after,
                rss_delta:  rss_after.saturating_sub(rss_before),
                cpu_pct,
            });
        });
    }

    println!("\n{}", "=".repeat(110));
    cleanup_tolerant();
}
