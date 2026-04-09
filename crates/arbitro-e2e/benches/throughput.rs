//! Benchmark: in-memory publish (ingestion) throughput.
//!
//! Measures publish throughput scaling across cores.
//! Each connection publishes to its own stream → different shards → real parallelism.
//!
//! No Criterion — direct measurement with timeout protection.

extern crate libc;

use std::time::{Duration, Instant};

use tokio::runtime::Runtime;

use arbitro_client::Client;
use arbitro_proto::config::StreamConfig;
use arbitro_server::{ArbitroServer, Config};

// ── Settings ────────────────────────────────────────────────────

const ITERATIONS: u32 = 50;
const MSGS_PER_CLIENT: u32 = 1_000;
const BATCH_SIZE: usize = 1_000;
const CONCURRENCY: &[usize] = &[1, 2, 4, 8, 16, 32];
const MAX_STREAMS: usize = 32;
const LEVEL_TIMEOUT: Duration = Duration::from_secs(15);

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

    let config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(200)
        .write_buffer_cap(8192);

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
    let mut handles = Vec::with_capacity(clients.len());
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let stream = stream_names[i % stream_names.len()].clone();
        handles.push(tokio::spawn(async move {
            let payload = vec![0u8; 64];
            for _ in 0..msgs {
                c.publish(&stream, b"bench.msg", &payload).await.expect("publish");
            }
        }));
    }
    for h in handles { h.await.unwrap(); }
    start.elapsed()
}

// ── Batch publish (1K msgs per RTT) ────────────────────────────

async fn run_batch(clients: &[Client], stream_names: &[Vec<u8>], batch_size: usize) -> Duration {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(clients.len());
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let stream = stream_names[i % stream_names.len()].clone();
        handles.push(tokio::spawn(async move {
            let payload = vec![0u8; 64];
            let entries: Vec<(&[u8], &[u8])> = (0..batch_size)
                .map(|_| (b"bench.msg".as_slice(), payload.as_slice()))
                .collect();
            c.publish_batch(&stream, &entries).await.expect("publish_batch");
        }));
    }
    for h in handles { h.await.unwrap(); }
    start.elapsed()
}

// ── Runner ──────────────────────────────────────────────────────

struct BenchResult {
    label: String,
    avg: Duration,
    throughput: f64,
    per_conn: f64,
    rss: u64,
    rss_delta: u64,
    cpu_pct: f64,
}

fn run_level<F, Fut>(
    rt: &Runtime,
    n: usize,
    label: String,
    stream_names: &[Vec<u8>],
    addr: &str,
    make_run: F,
) -> Option<BenchResult>
where
    F: Fn(Vec<Client>, Vec<Vec<u8>>) -> Fut,
    Fut: std::future::Future<Output = Option<(Duration, u64)>>,
{
    let clients: Vec<Client> = rt.block_on(async {
        let mut v = Vec::with_capacity(n);
        for _ in 0..n { v.push(connect(addr).await); }
        v
    });

    let streams: Vec<Vec<u8>> = stream_names.to_vec();

    rt.block_on(async {
        let result = make_run(clients, streams).await;
        result.map(|(total_time, total_msgs_all)| {
            let avg = total_time / ITERATIONS;
            let throughput = total_msgs_all as f64 / total_time.as_secs_f64();
            let per_conn = throughput / n as f64;
            BenchResult { label, avg, throughput, per_conn, rss: 0, rss_delta: 0, cpu_pct: 0.0 }
        })
    })
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

// ── Main ────────────────────────────────────────────────────────

fn main() {
    let rt = Runtime::new().unwrap();
    let addr = rt.block_on(start_server());

    // Create streams (enough for max concurrency)
    let stream_names: Vec<Vec<u8>> = (0..MAX_STREAMS)
        .map(|i| format!("ingest_{i}").into_bytes())
        .collect();

    let setup_client = rt.block_on(connect(&addr));
    rt.block_on(async {
        for name in &stream_names {
            setup_client.create_stream(&StreamConfig::new(name, b">").build())
                .await.expect("create stream");
        }
    });

    println!("\nPublish Throughput: 64B payload, {ITERATIONS} iterations, store=yes");
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

        let result = rt.block_on(async {
            // warmup
            let _ = tokio::time::timeout(
                LEVEL_TIMEOUT,
                run_single(&clients, &stream_names, 100),
            ).await;

            let rss_before = rss_kb();
            let cpu_before = cpu_time_ns();

            let mut total_time = Duration::ZERO;
            for _ in 0..ITERATIONS {
                match tokio::time::timeout(LEVEL_TIMEOUT, run_single(&clients, &stream_names, MSGS_PER_CLIENT)).await {
                    Ok(d) => total_time += d,
                    Err(_) => {
                        println!("  {label:30} | TIMEOUT ({LEVEL_TIMEOUT:?})");
                        return;
                    }
                }
            }

            let cpu_after = cpu_time_ns();
            let rss_after = rss_kb();
            let wall_ns = total_time.as_nanos() as u64;
            let cpu_ns = cpu_after.saturating_sub(cpu_before);
            let cpu_pct = if wall_ns > 0 { (cpu_ns as f64 / wall_ns as f64) * 100.0 } else { 0.0 };

            let total_msgs_all = total_msgs_per_iter * ITERATIONS as u64;
            let avg = total_time / ITERATIONS;
            let throughput = total_msgs_all as f64 / total_time.as_secs_f64();
            let per_conn = throughput / n as f64;

            print_result(&BenchResult {
                label, avg, throughput, per_conn,
                rss: rss_after,
                rss_delta: rss_after.saturating_sub(rss_before),
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

        let result = rt.block_on(async {
            // warmup
            let _ = tokio::time::timeout(
                LEVEL_TIMEOUT,
                run_batch(&clients, &stream_names, BATCH_SIZE),
            ).await;

            let rss_before = rss_kb();
            let cpu_before = cpu_time_ns();

            let mut total_time = Duration::ZERO;
            for _ in 0..ITERATIONS {
                match tokio::time::timeout(LEVEL_TIMEOUT, run_batch(&clients, &stream_names, BATCH_SIZE)).await {
                    Ok(d) => total_time += d,
                    Err(_) => {
                        println!("  {label:30} | TIMEOUT ({LEVEL_TIMEOUT:?})");
                        return;
                    }
                }
            }

            let cpu_after = cpu_time_ns();
            let rss_after = rss_kb();
            let wall_ns = total_time.as_nanos() as u64;
            let cpu_ns = cpu_after.saturating_sub(cpu_before);
            let cpu_pct = if wall_ns > 0 { (cpu_ns as f64 / wall_ns as f64) * 100.0 } else { 0.0 };

            let total_msgs_all = total_msgs_per_iter * ITERATIONS as u64;
            let avg = total_time / ITERATIONS;
            let throughput = total_msgs_all as f64 / total_time.as_secs_f64();
            let per_conn = throughput / n as f64;

            print_result(&BenchResult {
                label, avg, throughput, per_conn,
                rss: rss_after,
                rss_delta: rss_after.saturating_sub(rss_before),
                cpu_pct,
            });
        });
    }

    println!("\n{}", "=".repeat(110));
}
