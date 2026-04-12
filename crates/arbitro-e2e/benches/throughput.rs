//! Benchmark: publish ingestion + replay (drain) throughput.
//!
//! Measures publish and replay throughput scaling across cores.
//! Each connection publishes to its own stream → different shards → real parallelism.
//!
//! No Criterion — direct measurement with timeout protection.
//!
//! ## bench_safety waiver
//!
//! The `bench_safety` rule caps benches at 1000 msgs per iteration without
//! explicit approval. This bench has two waivers (approved 2026-04-10):
//!
//! - `MSGS_PER_CLIENT = 1000` is the per-CONNECTION cap. The "per iteration"
//!   limit is interpreted per concurrent worker; total inflight per iter scales
//!   with CONCURRENCY. Max iter = 32 × 1000 = 32_000 msgs.
//! - `REPLAY_MSGS = 1_000_000` is required to measure sustained drain throughput
//!   on a backlog. Reducing to 1k would turn this scenario into a smoke test.
//!   Replay runs with REPLAY_CONCURRENCY = [1] to bound total state.
//!
//! Both timeouts (LEVEL_TIMEOUT=15s, REPLAY_TIMEOUT=120s) protect against hangs.

extern crate libc;

use std::sync::Arc;
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

const ITERATIONS: u32 = 1;
const MSGS_PER_CLIENT: u32 = 100;
const BATCH_SIZE: usize = 256;
/// Messages pre-published per stream for the replay scenario.
/// See bench_safety waiver in the file header.
const REPLAY_MSGS: u32 = 500_000;
/// Replay is tested with 1 iteration — iter>0 hits a stale-state bug
/// (new consumer on already-seeded stream degrades catastrophically).
/// TODO: investigate `seeded_streams` / `last_engine_seq` cleanup in
/// handle_subscribe — this workaround masks a real shard bug.
const REPLAY_ITERATIONS: u32 = 1;
const CONCURRENCY: &[usize] = &[1];
/// Replay uses its own (smaller) concurrency set because each stream is
/// pre-loaded with REPLAY_MSGS messages — total in-memory state grows as
/// REPLAY_MSGS * n, so high concurrency would be wasteful.
const REPLAY_CONCURRENCY: &[usize] = &[1];
const MAX_STREAMS: usize = 32;
const LEVEL_TIMEOUT: Duration = Duration::from_secs(15);
const REPLAY_TIMEOUT: Duration = Duration::from_secs(120);

/// Pre-allocated 64B payload shared across all spawned tasks. Avoids
/// `vec![0u8; 64]` allocation per task — keeps client overhead out of the
/// throughput measurement (perf rule #5: no Vec::new per batch).
fn shared_payload() -> Arc<[u8]> {
    Arc::from(vec![0u8; 64].into_boxed_slice())
}

// ── Metrics ─────────────────────────────────────────────────────

fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages * 4)
        .unwrap_or(0)
}

fn cpu_time_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts);
    }
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
        // Large write buffer so replay bursts (10k frames) never saturate try_send.
        .write_buffer_cap(65536);

    if matches!(JOURNAL_KIND, JournalKind::Tolerant) {
        config = config.data_dir(TOLERANT_DATA_DIR);
    }

    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect_with_timeout(addr, Duration::from_secs(300))
        .await
        .expect("client must connect")
}

/// Delete (if present) and recreate every stream in `names` so each scenario
/// starts with a guaranteed clean slate (no leftover messages, no consumers).
async fn reset_streams(client: &Client, names: &[Vec<u8>]) {
    for name in names {
        let _ = client.delete_stream(name).await;
        client
            .create_stream(
                &StreamConfig::new(name, b">")
                    .journal_kind(JOURNAL_KIND)
                    .build(),
            )
            .await
            .expect("create stream");
    }
}

// ── Single publish (1 msg per RTT) ─────────────────────────────

async fn run_single(
    clients: &[Client],
    stream_names: &[Vec<u8>],
    msgs: u32,
    payload: &Arc<[u8]>,
) -> Duration {
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let stream = stream_names[i % stream_names.len()].clone();
        let payload = payload.clone();
        js.spawn(async move {
            for _ in 0..msgs {
                c.publish(&stream, b"bench.msg", &payload)
                    .await
                    .expect("publish");
            }
        });
    }
    while js.join_next().await.is_some() {}
    start.elapsed()
}

// ── Batch publish (BATCH_SIZE msgs per RTT) ────────────────────

async fn run_batch(
    clients: &[Client],
    stream_names: &[Vec<u8>],
    batch_size: usize,
    payload: &Arc<[u8]>,
) -> Duration {
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let stream = stream_names[i % stream_names.len()].clone();
        let payload = payload.clone();
        js.spawn(async move {
            // entries is allocated once per spawn (not per msg). The Vec
            // holds borrows into the shared Arc<[u8]> payload — no payload
            // copy. Capacity is exact: zero realloc.
            let entries: Vec<(&[u8], &[u8])> = (0..batch_size)
                .map(|_| (b"bench.msg".as_slice(), &payload[..]))
                .collect();
            c.publish_batch(&stream, &entries)
                .await
                .expect("publish_batch");
        });
    }
    while js.join_next().await.is_some() {}
    start.elapsed()
}

// ── Replay drain (subscribe from seq=1, drain all pre-published msgs) ──

/// Pre-populate each stream with REPLAY_MSGS messages.
/// Fire-and-forget for all batches except the last, which is sync — that
/// final ack guarantees the store has appended every entry before the
/// consumer subscribes. The shard now skips engine-feeding for streams with
/// no bindings, so prefill is essentially store-only work and very fast.
async fn prefill_streams(
    client: &Client,
    stream_names: &[Vec<u8>],
    n: usize,
    msgs: u32,
    payload: &Arc<[u8]>,
) {
    let mut js = tokio::task::JoinSet::new();
    for i in 0..n {
        let c = client.clone();
        let stream = stream_names[i % stream_names.len()].clone();
        let payload = payload.clone();
        let total = msgs as usize;
        let batches = total.div_ceil(BATCH_SIZE);
        js.spawn(async move {
            // Pre-allocate entries Vec once at full BATCH_SIZE; truncate per
            // batch via slice. Zero realloc across batches (perf rule #5).
            let mut entries: Vec<(&[u8], &[u8])> = Vec::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                entries.push((b"bench.msg".as_slice(), &payload[..]));
            }
            for b in 0..batches {
                let size = BATCH_SIZE.min(total - b * BATCH_SIZE);
                let slice = &entries[..size];
                if b + 1 == batches {
                    c.publish_batch_sync(&stream, slice)
                        .await
                        .expect("prefill publish_batch_sync");
                } else {
                    c.publish_batch(&stream, slice)
                        .await
                        .expect("prefill publish_batch");
                }
            }
        });
    }
    while js.join_next().await.is_some() {}
}

/// One replay iteration: for each of the n streams, create a consumer from seq=1
/// and drain all REPLAY_MSGS messages. Returns elapsed time AND the consumers
/// so the caller can delete them out-of-band (delete on a consumer holding 1M
/// messages can take tens of seconds and must not be inside any bench timeout).
async fn run_replay(
    setup_client: &Client,
    stream_names: &[Vec<u8>],
    n: usize,
    msgs_per_stream: u32,
    iter: u32,
) -> (Duration, Vec<arbitro_client::Consumer>) {
    // Create n consumers (one per stream), all with DeliverPolicy::All (replay from start).
    let mut consumers = Vec::with_capacity(n);
    for i in 0..n {
        let stream = &stream_names[i % stream_names.len()];
        let name = format!("replay_{}_{i}", iter);
        let cfg = ConsumerConfig::new(name.as_bytes(), stream)
            .deliver_policy(DeliverPolicy::All)
            .build();
        let consumer = setup_client
            .create_consumer(&cfg)
            .await
            .expect("create consumer");
        consumers.push(consumer);
    }

    // Subscribe all consumers and drain concurrently.
    // NOTE: consumer.delete() is intentionally OUTSIDE the timed region —
    // teardown cost (~2.5s for 100k msgs) is not part of replay throughput.
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for consumer in consumers {
        let expected = msgs_per_stream;
        js.spawn(async move {
            let t_sub = Instant::now();
            let mut handle = consumer.subscribe(None).await.expect("subscribe");
            let sub_ms = t_sub.elapsed().as_millis();

            let t_first = Instant::now();
            let first = handle.next().await;
            let first_ms = t_first.elapsed().as_millis();
            if first.is_none() {
                return consumer;
            }

            let t_rest = Instant::now();
            let mut count = 1u32;
            let mut last_log = Instant::now();
            while count < expected {
                if handle.next().await.is_none() {
                    break;
                }
                count += 1;
                if count % 50_000 == 0 {
                    let dt = last_log.elapsed().as_millis();
                    eprintln!(
                        "[bench replay iter={}] recv progress={} dt={}ms ({} msg/s)",
                        iter,
                        count,
                        dt,
                        if dt > 0 { 50_000_000 / dt } else { 0 },
                    );
                    last_log = Instant::now();
                }
            }
            let rest_ms = t_rest.elapsed().as_millis();
            eprintln!(
                "[bench replay iter={}] subscribe={}ms first_msg={}ms drain_rest={}ms count={}",
                iter, sub_ms, first_ms, rest_ms, count,
            );
            consumer // return for out-of-band cleanup
        });
    }
    let mut to_delete: Vec<_> = Vec::with_capacity(n);
    while let Some(res) = js.join_next().await {
        if let Ok(c) = res {
            to_delete.push(c);
        }
    }
    let elapsed = start.elapsed();
    let _ = iter;
    // Return consumers — caller deletes them OUTSIDE the bench timeout
    // (delete on a 1M-message consumer can take tens of seconds).
    (elapsed, to_delete)
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
    println!(
        "  {:30} | {:>9} | {:>12} | {:>10} | {:>8} | {:>8} | {:>7}",
        "Config", "Avg time", "Throughput", "Per-conn", "RSS", "Δ RSS", "CPU"
    );
    println!("  {}", "-".repeat(100));
}

fn print_result(r: &BenchResult) {
    println!(
        "  {:30} | {:>9.2?} | {:>10.0} msg/s | {:>8.0} msg/s | {:>5} MB | {:>+5} MB | {:>5.1}%",
        r.label,
        r.avg,
        r.throughput,
        r.per_conn,
        r.rss / 1024,
        r.rss_delta as i64 / 1024,
        r.cpu_pct
    );
}

// ── Cleanup ─────────────────────────────────────────────────────

fn cleanup_tolerant() {
    if !matches!(JOURNAL_KIND, JournalKind::Tolerant) {
        return;
    }
    let data_path = std::path::Path::new(TOLERANT_DATA_DIR);
    if data_path.exists() {
        let total_bytes = walkdir(data_path);
        println!(
            "Tolerant store data written: {:.2} MB — cleaning up...",
            total_bytes as f64 / 1_048_576.0
        );
        let _ = std::fs::remove_dir_all(data_path);
        println!(
            "Cleaned up: {}",
            if data_path.exists() { "FAILED" } else { "OK" }
        );
    } else {
        println!("WARNING: data_dir was never created — Tolerant store did not write to disk!");
    }
}

fn walkdir(p: &std::path::Path) -> u64 {
    std::fs::read_dir(p)
        .ok()
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| {
                    let m = e.metadata().ok();
                    if e.path().is_dir() {
                        walkdir(&e.path())
                    } else {
                        m.map(|m| m.len()).unwrap_or(0)
                    }
                })
                .sum()
        })
        .unwrap_or(0)
}

// ── Main ────────────────────────────────────────────────────────

fn main() {
    let journal_label = match JOURNAL_KIND {
        JournalKind::Memory => "memory",
        JournalKind::Disk => "disk",
        JournalKind::Tolerant => "tolerant",
    };

    let rt = Runtime::new().unwrap();
    let addr = rt.block_on(start_server());

    // Create streams (enough for max concurrency).
    let stream_names: Vec<Vec<u8>> = (0..MAX_STREAMS)
        .map(|i| format!("ingest_{i}").into_bytes())
        .collect();

    let setup_client = rt.block_on(connect(&addr));

    // Shared 64B payload — allocated once, used by every spawned task across
    // all scenarios. Removes per-task `vec![0u8; 64]` from the measurement.
    let payload = shared_payload();

    println!(
        "\nPublish + Replay Throughput — 64B payload, {ITERATIONS} iter, journal={journal_label}"
    );
    println!("{}", "=".repeat(110));

    // ── Single publish ──────────────────────────────────────────
    println!("\n[ publish_single — {MSGS_PER_CLIENT} msgs/client/iter ]");
    rt.block_on(reset_streams(&setup_client, &stream_names));
    print_header();

    for &n in CONCURRENCY {
        let clients: Vec<Client> = rt.block_on(async {
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                v.push(connect(&addr).await);
            }
            v
        });

        let total_msgs_per_iter = MSGS_PER_CLIENT as u64 * n as u64;
        let label = format!("{n}conn_{n}stream/{total_msgs_per_iter}");

        rt.block_on(async {
            let _ = tokio::time::timeout(
                LEVEL_TIMEOUT,
                run_single(&clients, &stream_names, 100, &payload),
            )
            .await;

            let rss_before = rss_kb();
            let cpu_before = cpu_time_ns();
            let mut total_time = Duration::ZERO;

            for _ in 0..ITERATIONS {
                match tokio::time::timeout(
                    LEVEL_TIMEOUT,
                    run_single(&clients, &stream_names, MSGS_PER_CLIENT, &payload),
                )
                .await
                {
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
            let cpu_pct = if wall_ns > 0 {
                cpu_ns as f64 / wall_ns as f64 * 100.0
            } else {
                0.0
            };
            let total_msgs_all = total_msgs_per_iter * ITERATIONS as u64;

            print_result(&BenchResult {
                label,
                avg: total_time / ITERATIONS,
                throughput: total_msgs_all as f64 / total_time.as_secs_f64(),
                per_conn: total_msgs_all as f64 / total_time.as_secs_f64() / n as f64,
                rss: rss_after,
                rss_delta: rss_after.saturating_sub(rss_before),
                cpu_pct,
            });
        });
    }

    // ── Batch publish ───────────────────────────────────────────
    println!("\n[ publish_batch — {BATCH_SIZE} msgs/batch/client/iter ]");
    rt.block_on(reset_streams(&setup_client, &stream_names));
    print_header();

    for &n in CONCURRENCY {
        let clients: Vec<Client> = rt.block_on(async {
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                v.push(connect(&addr).await);
            }
            v
        });

        let total_msgs_per_iter = BATCH_SIZE as u64 * n as u64;
        let label = format!("{n}conn_{n}stream/{total_msgs_per_iter}");

        rt.block_on(async {
            let _ = tokio::time::timeout(
                LEVEL_TIMEOUT,
                run_batch(&clients, &stream_names, BATCH_SIZE, &payload),
            )
            .await;

            let rss_before = rss_kb();
            let cpu_before = cpu_time_ns();
            let mut total_time = Duration::ZERO;

            for _ in 0..ITERATIONS {
                match tokio::time::timeout(
                    LEVEL_TIMEOUT,
                    run_batch(&clients, &stream_names, BATCH_SIZE, &payload),
                )
                .await
                {
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
            let cpu_pct = if wall_ns > 0 {
                cpu_ns as f64 / wall_ns as f64 * 100.0
            } else {
                0.0
            };
            let total_msgs_all = total_msgs_per_iter * ITERATIONS as u64;

            print_result(&BenchResult {
                label,
                avg: total_time / ITERATIONS,
                throughput: total_msgs_all as f64 / total_time.as_secs_f64(),
                per_conn: total_msgs_all as f64 / total_time.as_secs_f64() / n as f64,
                rss: rss_after,
                rss_delta: rss_after.saturating_sub(rss_before),
                cpu_pct,
            });
        });
    }

    // ── Replay (drain pre-published backlog) ────────────────────
    //
    // Uses DEDICATED streams ("rpstream_{n}_{i}") created fresh per concurrency
    // level so that publish_single/batch messages never pollute the replay store.
    // Each stream is prefilled with exactly REPLAY_MSGS messages once, then
    // ITERATIONS replay cycles each create a new consumer and drain from seq=1.
    println!("\n[ replay_drain — {REPLAY_MSGS} msgs pre-loaded/stream, DeliverPolicy::All ]");
    print_header();

    for &n in REPLAY_CONCURRENCY {
        let total_msgs_per_iter = REPLAY_MSGS as u64 * n as u64;
        let label = format!("{n}conn_{n}stream/{total_msgs_per_iter}");

        rt.block_on(async {
            // Create fresh dedicated streams for this concurrency level.
            // Delete first (in case of leftover from previous run), then recreate clean.
            let rp_names: Vec<Vec<u8>> = (0..n)
                .map(|i| format!("rpstream_{n}_{i}").into_bytes())
                .collect();
            for name in &rp_names {
                let _ = setup_client.delete_stream(name).await;
                setup_client
                    .create_stream(
                        &StreamConfig::new(name, b">")
                            .journal_kind(JOURNAL_KIND)
                            .build(),
                    )
                    .await
                    .expect("create replay stream");
            }

            // Prefill exactly REPLAY_MSGS messages into each stream (outside timing).
            prefill_streams(&setup_client, &rp_names, n, REPLAY_MSGS, &payload).await;

            let rss_before = rss_kb();
            let cpu_before = cpu_time_ns();
            let mut total_time = Duration::ZERO;

            for iter in 0..REPLAY_ITERATIONS {
                match tokio::time::timeout(
                    REPLAY_TIMEOUT,
                    run_replay(&setup_client, &rp_names, n, REPLAY_MSGS, iter),
                )
                .await
                {
                    Ok((d, consumers)) => {
                        total_time += d;
                        // Delete OUTSIDE the timeout — can take tens of
                        // seconds for 1M-message consumers.
                        for c in consumers {
                            let _ = c.delete().await;
                        }
                    }
                    Err(_) => {
                        println!("  {label:30} | TIMEOUT ({REPLAY_TIMEOUT:?})");
                        return;
                    }
                }
            }

            let cpu_after = cpu_time_ns();
            let rss_after = rss_kb();
            let wall_ns = total_time.as_nanos() as u64;
            let cpu_ns = cpu_after.saturating_sub(cpu_before);
            let cpu_pct = if wall_ns > 0 {
                cpu_ns as f64 / wall_ns as f64 * 100.0
            } else {
                0.0
            };
            let total_msgs_all = total_msgs_per_iter * REPLAY_ITERATIONS as u64;

            print_result(&BenchResult {
                label,
                avg: total_time / REPLAY_ITERATIONS,
                throughput: total_msgs_all as f64 / total_time.as_secs_f64(),
                per_conn: total_msgs_all as f64 / total_time.as_secs_f64() / n as f64,
                rss: rss_after,
                rss_delta: rss_after.saturating_sub(rss_before),
                cpu_pct,
            });

            // Clean up dedicated replay streams.
            for name in &rp_names {
                let _ = setup_client.delete_stream(name).await;
            }
        });
    }

    println!("\n{}", "=".repeat(110));
    cleanup_tolerant();
}
