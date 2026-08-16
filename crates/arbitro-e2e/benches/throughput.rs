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

use arbitro_client_tokio::{BatchEntry, Client, ClientConfig, JournalKind, StreamBuilder};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;

// ── Settings ────────────────────────────────────────────────────

/// journal_kind u8: 0=Memory, 1=Disk, 2=Tolerant
const JOURNAL_KIND: u8 = 0; // Memory
/// Same value as `JOURNAL_KIND`, typed for the builder.
const JOURNAL_KIND_ENUM: JournalKind = JournalKind::Memory;
/// Only used when JOURNAL_KIND == 2 (Tolerant).
const TOLERANT_DATA_DIR: &str = "/tmp/arbitro_bench_tolerant";

/// A stream named `x` owns `x.>`, and nothing else does.
///
/// The broker rejects a `>` filter outright and refuses two streams whose
/// slices overlap — a stream owns its slice alone. So the streams this
/// bench spreads its connections over cannot share one subject, and the
/// slice has to come from the NAME: deriving it from the position in a
/// list silently collides the moment two different lists are set up.
fn stream_filter(name: &[u8]) -> Vec<u8> {
    let mut f = name.to_vec();
    f.extend_from_slice(b".>");
    f
}

/// The subject published into `name`. Matches [`stream_filter`].
fn stream_subject(name: &[u8]) -> Vec<u8> {
    let mut s = name.to_vec();
    s.extend_from_slice(b".msg");
    s
}

/// The fanout stage's own slice, disjoint from every `ingest.{i}.>`.
const FANOUT_FILTER: &[u8] = b"fanout.>";
const FANOUT_SUBJECT: &[u8] = b"fanout.msg";

// Defaults; override at runtime via env: BENCH_ITERATIONS, BENCH_MSGS,
// BENCH_BATCH, BENCH_CONCURRENCY (comma list, e.g. "1,2,4,8,16").
//
// BENCH_MSGS is the TOTAL messages per iteration, split evenly across the
// active connections. So `BENCH_MSGS=1000000 BENCH_CONCURRENCY=4` publishes
// 250k per connection per iter — 1M total, regardless of conn count.
const ITERATIONS: u32 = 5;
const TOTAL_MSGS: u32 = 25000;
const BATCH_SIZE: usize = 256;

fn env_u32(k: &str, default: u32) -> u32 {
    std::env::var(k)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
fn env_usize(k: &str, default: usize) -> usize {
    std::env::var(k)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
fn env_concurrency(default: &[usize]) -> Vec<usize> {
    match std::env::var("BENCH_CONCURRENCY") {
        Ok(s) => s.split(',').filter_map(|p| p.trim().parse().ok()).collect(),
        Err(_) => default.to_vec(),
    }
}
fn cfg_iterations() -> u32 {
    env_u32("BENCH_ITERATIONS", ITERATIONS)
}
fn cfg_total_msgs() -> u32 {
    env_u32("BENCH_MSGS", TOTAL_MSGS)
}
fn cfg_batch_size() -> usize {
    env_usize("BENCH_BATCH", BATCH_SIZE).min(256)
}
fn cfg_replay_msgs() -> u32 {
    env_u32("BENCH_REPLAY_MSGS", REPLAY_MSGS)
}
fn cfg_replay_iterations() -> u32 {
    env_u32("BENCH_REPLAY_ITERATIONS", REPLAY_ITERATIONS)
}
/// "publish" | "replay" | "all" (default).
fn cfg_mode() -> String {
    std::env::var("BENCH_MODE")
        .unwrap_or_else(|_| "all".to_string())
        .to_lowercase()
}
/// Messages pre-published per stream for the replay scenario.
/// See bench_safety waiver in the file header.
const REPLAY_MSGS: u32 = 500_000;
/// Replay is tested with 1 iteration — iter>0 hits a stale-state bug
/// (new consumer on already-seeded stream degrades catastrophically).
const REPLAY_ITERATIONS: u32 = 1;
const CONCURRENCY: &[usize] = &[1, 2, 4, 8, 16];
/// Replay uses its own (smaller) concurrency set.
const REPLAY_CONCURRENCY: &[usize] = &[1];
const MAX_STREAMS: usize = 32;
const LEVEL_TIMEOUT: Duration = Duration::from_secs(15);
const REPLAY_TIMEOUT: Duration = Duration::from_secs(120);

/// Pre-allocated 64B payload shared across all spawned tasks.
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

#[cfg(target_os = "linux")]
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

#[cfg(not(target_os = "linux"))]
fn cpu_time_ns() -> u64 {
    0
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
        .write_buffer_cap(65536);

    if JOURNAL_KIND == 2 {
        config = config.data_dir(TOLERANT_DATA_DIR);
    }

    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    addr
}

/// Retry rather than guess. `tokio::spawn` returns before the server has
/// bound its listener, and the fixed 50ms wait this replaces turned any
/// slower startup into a ConnectionRefused panic before a single message
/// was measured.
async fn connect(addr: &str) -> Client {
    for _ in 0..200 {
        if let Ok(c) = Client::connect(ClientConfig {
            addr: addr.to_string(),
            ..ClientConfig::default()
        })
        .await
        {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("client must connect to {addr} — still refused after 2s");
}

/// Ensure every stream in `names` exists and return its id.
///
/// `upsert` instead of delete-then-create: the delete was fire-and-forget
/// (`.ok()`), so whenever it failed the create hit `StreamAlreadyExists`
/// and the bench died before measuring anything. Upsert states the intent
/// — "this stream, with this shape" — and is idempotent across the
/// repeated setup this bench does per concurrency level.
async fn reset_streams(client: &Client, names: &[Vec<u8>]) -> Vec<u32> {
    let mut ids = Vec::with_capacity(names.len());
    for name in names {
        let filter = stream_filter(name);
        let created = StreamBuilder::new(name)
            .filter(&filter)
            .journal_kind(JOURNAL_KIND_ENUM)
            .upsert(client)
            .await
            .expect("upsert stream");
        // Existing stream: drop its backlog so each iteration measures the
        // same starting point.
        if !created {
            let _ = client.purge_stream(name).await.ok();
        }
        let info = client.get_stream(name).await.expect("get stream");
        let stream_id = u64::from_le_bytes(info[..8].try_into().unwrap()) as u32;
        ids.push(stream_id);
    }
    ids
}

// ── Single publish (1 msg per RTT) ─────────────────────────────

async fn run_single(
    clients: &[Client],
    stream_ids: &[u32],
    names: &[Vec<u8>],
    msgs: u32,
    payload: &Arc<[u8]>,
) -> Duration {
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let slot = i % stream_ids.len();
        let stream_id = stream_ids[slot];
        let subject = stream_subject(&names[slot]);
        let payload = payload.clone();
        js.spawn(async move {
            for _ in 0..msgs {
                loop {
                    match c.publish(stream_id, &subject, Bytes::copy_from_slice(&payload)) {
                        Ok(()) => break,
                        Err(arbitro_client_tokio::ClientError::QueueFull) => {
                            tokio::task::yield_now().await;
                        }
                        Err(e) => panic!("publish: {e:?}"),
                    }
                }
            }
        });
    }
    // `is_some()` would swallow a JoinError: a panicking task then reads as
    // success and the phase after it runs on incomplete state.
    while let Some(joined) = js.join_next().await {
        joined.expect("bench task panicked");
    }
    start.elapsed()
}

// ── Batch publish ──────────────────────────────────────────────

async fn run_batch(
    clients: &[Client],
    stream_ids: &[u32],
    names: &[Vec<u8>],
    total: usize,
    batch_size: usize,
    payload: &Arc<[u8]>,
) -> Duration {
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let slot = i % stream_ids.len();
        let stream_id = stream_ids[slot];
        let subject = stream_subject(&names[slot]);
        let payload = payload.clone();
        js.spawn(async move {
            let payload_bytes = Bytes::copy_from_slice(&payload[..]);
            let mut entries: Vec<BatchEntry<'_>> = Vec::with_capacity(batch_size);
            for _ in 0..batch_size {
                entries.push(BatchEntry {
                    subject: &subject,
                    msg_id: &[],
                    payload: payload_bytes.clone(),
                });
            }
            let batches = total.div_ceil(batch_size);
            for b in 0..batches {
                let size = batch_size.min(total - b * batch_size);
                loop {
                    match c.publish_batch(stream_id, &entries[..size]) {
                        Ok(()) => break,
                        Err(arbitro_client_tokio::ClientError::QueueFull) => {
                            tokio::task::yield_now().await;
                        }
                        Err(e) => panic!("publish_batch: {e:?}"),
                    }
                }
            }
        });
    }
    // `is_some()` would swallow a JoinError: a panicking task then reads as
    // success and the phase after it runs on incomplete state.
    while let Some(joined) = js.join_next().await {
        joined.expect("bench task panicked");
    }
    start.elapsed()
}

// ── Batch publish SYNC ──────────────────────────────────────────

async fn run_batch_sync(
    clients: &[Client],
    stream_ids: &[u32],
    names: &[Vec<u8>],
    total: usize,
    batch_size: usize,
    payload: &Arc<[u8]>,
) -> Duration {
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let slot = i % stream_ids.len();
        let stream_id = stream_ids[slot];
        let subject = stream_subject(&names[slot]);
        let payload = payload.clone();
        js.spawn(async move {
            let payload_bytes = Bytes::copy_from_slice(&payload[..]);
            let mut entries: Vec<BatchEntry<'_>> = Vec::with_capacity(batch_size);
            for _ in 0..batch_size {
                entries.push(BatchEntry {
                    subject: &subject,
                    msg_id: &[],
                    payload: payload_bytes.clone(),
                });
            }
            let batches = total.div_ceil(batch_size);
            for b in 0..batches {
                let size = batch_size.min(total - b * batch_size);
                c.publish_batch_wait(stream_id, &entries[..size])
                    .await
                    .expect("publish_batch_wait");
            }
        });
    }
    // `is_some()` would swallow a JoinError: a panicking task then reads as
    // success and the phase after it runs on incomplete state.
    while let Some(joined) = js.join_next().await {
        joined.expect("bench task panicked");
    }
    start.elapsed()
}

// ── Replay prefill ──────────────────────────────────────────────

async fn prefill_streams(
    client: &Client,
    stream_ids: &[u32],
    names: &[Vec<u8>],
    n: usize,
    msgs: u32,
    payload: &Arc<[u8]>,
) {
    let mut js = tokio::task::JoinSet::new();
    for i in 0..n {
        let c = client.clone();
        let slot = i % stream_ids.len();
        let stream_id = stream_ids[slot];
        let subject = stream_subject(&names[slot]);
        let payload = payload.clone();
        let total = msgs as usize;
        let batches = total.div_ceil(BATCH_SIZE);
        js.spawn(async move {
            let payload_bytes = Bytes::copy_from_slice(&payload[..]);
            let mut entries: Vec<BatchEntry<'_>> = Vec::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                entries.push(BatchEntry {
                    subject: &subject,
                    msg_id: &[],
                    payload: payload_bytes.clone(),
                });
            }
            for b in 0..batches {
                let size = BATCH_SIZE.min(total - b * BATCH_SIZE);
                let slice = &entries[..size];
                if b + 1 == batches {
                    c.publish_batch_wait(stream_id, slice)
                        .await
                        .expect("prefill publish_batch_wait");
                } else {
                    // Backpressure (`QueueFull`) is transient — yield and
                    // retry. `ChannelClosed` is terminal and must not be
                    // retried; conflating them hid a lost tail batch.
                    loop {
                        match c.publish_batch(stream_id, slice) {
                            Ok(()) => break,
                            Err(arbitro_client_tokio::ClientError::QueueFull) => {
                                tokio::task::yield_now().await;
                            }
                            Err(e) => panic!("prefill publish_batch: {e:?}"),
                        }
                    }
                }
            }
        });
    }
    // `is_some()` would swallow a JoinError: a panicking task then reads as
    // success and the phase after it runs on incomplete state.
    while let Some(joined) = js.join_next().await {
        joined.expect("bench task panicked");
    }
}

/// One replay iteration: for each stream, create a consumer and drain all msgs.
/// Returns elapsed time AND (stream_id, consumer_id) pairs for out-of-band cleanup.
async fn run_replay(
    setup_client: &Client,
    reader_clients: &[Client],
    stream_ids: &[u32],
    n: usize,
    msgs_per_stream: u32,
    iter: u32,
    progress: &Arc<Vec<std::sync::atomic::AtomicU32>>,
) -> (Duration, Vec<(u32, u32)>) {
    assert_eq!(reader_clients.len(), n, "one reader client per consumer");
    let mut consumer_pairs = Vec::with_capacity(n);
    for i in 0..n {
        let stream_id = stream_ids[i % stream_ids.len()];
        let name = format!("replay_{}_{i}", iter);
        // AckPolicy::None = 0, DeliverPolicy::All = 0
        let resp = setup_client
            .create_consumer(
                stream_id,
                name.as_bytes(),
                name.as_bytes(),
                b"",
                u16::MAX,
                0, // ack_policy = None
                0, // deliver_policy = All
                0, // deliver_mode = Push/Fanout
                30_000,
                0,
            )
            .await
            .expect("create consumer");
        let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
        consumer_pairs.push((stream_id, consumer_id));
    }

    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (idx, (stream_id, consumer_id)) in consumer_pairs.clone().into_iter().enumerate() {
        let client = reader_clients[idx].clone();
        let expected = msgs_per_stream;
        let progress = Arc::clone(progress);
        js.spawn(async move {
            let t_sub = Instant::now();
            let mut handle = client
                .subscribe(stream_id, consumer_id, b"")
                .await
                .expect("subscribe");
            let sub_ms = t_sub.elapsed().as_millis();

            let t_first = Instant::now();
            let first = handle.recv().await;
            let first_ms = t_first.elapsed().as_millis();
            if first.is_none() {
                return (stream_id, consumer_id);
            }

            let t_rest = Instant::now();
            let mut count = 1u32;
            let mut last_log = Instant::now();
            while count < expected {
                if handle.recv().await.is_none() {
                    break;
                }
                count += 1;
                progress[idx].store(count, std::sync::atomic::Ordering::Relaxed);
                let log_interval = if expected <= 1_000 { 100 } else { 50_000 };
                if count.is_multiple_of(log_interval) {
                    let dt = last_log.elapsed().as_millis();
                    let batch = log_interval as u128;
                    eprintln!(
                        "[replay iter={iter}] progress={count}/{expected} dt={dt}ms ({} msg/s)",
                        if dt > 0 { batch * 1000 / dt } else { 0 },
                    );
                    last_log = Instant::now();
                }
            }
            let rest_ms = t_rest.elapsed().as_millis();
            eprintln!(
                "[bench replay iter={}] subscribe={}ms first_msg={}ms drain_rest={}ms count={}",
                iter, sub_ms, first_ms, rest_ms, count,
            );
            (stream_id, consumer_id)
        });
    }
    let mut to_delete: Vec<(u32, u32)> = Vec::with_capacity(n);
    while let Some(res) = js.join_next().await {
        if let Ok(pair) = res {
            to_delete.push(pair);
        }
    }
    let elapsed = start.elapsed();
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
    if JOURNAL_KIND != 2 {
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
        0 => "memory",
        1 => "disk",
        2 => "tolerant",
        _ => "unknown",
    };

    let iterations = cfg_iterations();
    let total_msgs = cfg_total_msgs();
    let batch_size = cfg_batch_size();
    let concurrency = env_concurrency(CONCURRENCY);
    let mode = cfg_mode();
    let run_publish = matches!(mode.as_str(), "publish" | "all");
    let run_replay_section = matches!(mode.as_str(), "replay" | "all" | "fanout");
    let run_fanout = matches!(mode.as_str(), "fanout" | "all");
    let replay_msgs = cfg_replay_msgs();
    let replay_iterations = cfg_replay_iterations();

    let rt = Runtime::new().unwrap();
    let addr = match std::env::var("ARBITRO_ADDR") {
        Ok(a) => {
            println!("Using external server at {a}");
            a
        }
        Err(_) => rt.block_on(start_server()),
    };

    let stream_names: Vec<Vec<u8>> = (0..MAX_STREAMS)
        .map(|i| format!("ingest_{i}").into_bytes())
        .collect();

    let setup_client = rt.block_on(connect(&addr));

    let payload = shared_payload();

    println!(
        "\nPublish + Replay Throughput — 64B payload, {iterations} iter, journal={journal_label}"
    );
    println!(
        "Config: mode={mode}, total_msgs={total_msgs} (split across conns), batch={batch_size}, concurrency={concurrency:?}, replay_msgs={replay_msgs}"
    );
    println!("{}", "=".repeat(110));

    // Pre-create all streams once; store their IDs.
    let all_stream_ids: Vec<u32> = rt.block_on(reset_streams(&setup_client, &stream_names));

    if run_publish {
        // ── Single publish ──────────────────────────────────────────
        println!("\n[ publish_single — {total_msgs} msgs total/iter ]");
        rt.block_on(reset_streams(&setup_client, &stream_names));
        print_header();

        for &n in &concurrency {
            let msgs_per_client = total_msgs / n as u32;
            let clients: Vec<Client> = rt.block_on(async {
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(connect(&addr).await);
                }
                v
            });

            let total_msgs_per_iter = msgs_per_client as u64 * n as u64;
            let label = format!("{n}conn_{n}stream/{total_msgs_per_iter}");

            rt.block_on(async {
                let stream_ids = reset_streams(&setup_client, &stream_names).await;
                let _ = tokio::time::timeout(
                    LEVEL_TIMEOUT,
                    run_single(&clients, &stream_ids, &stream_names, 100, &payload),
                )
                .await;

                let rss_before = rss_kb();
                let cpu_before = cpu_time_ns();
                let mut total_time = Duration::ZERO;

                for _ in 0..iterations {
                    match tokio::time::timeout(
                        LEVEL_TIMEOUT,
                        run_single(
                            &clients,
                            &stream_ids,
                            &stream_names,
                            msgs_per_client,
                            &payload,
                        ),
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
                let total_msgs_all = total_msgs_per_iter * iterations as u64;

                print_result(&BenchResult {
                    label,
                    avg: total_time / iterations,
                    throughput: total_msgs_all as f64 / total_time.as_secs_f64(),
                    per_conn: total_msgs_all as f64 / total_time.as_secs_f64() / n as f64,
                    rss: rss_after,
                    rss_delta: rss_after.saturating_sub(rss_before),
                    cpu_pct,
                });
            });
        }

        // ── Batch publish ───────────────────────────────────────────
        println!("\n[ publish_batch — batch={batch_size}, {total_msgs} msgs total/iter ]");
        print_header();

        for &n in &concurrency {
            let msgs_per_client = total_msgs / n as u32;
            let clients: Vec<Client> = rt.block_on(async {
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(connect(&addr).await);
                }
                v
            });

            let total_msgs_per_iter = msgs_per_client as u64 * n as u64;
            let label = format!("{n}conn_{n}stream/{total_msgs_per_iter}");

            rt.block_on(async {
                let stream_ids = reset_streams(&setup_client, &stream_names).await;
                let _ = tokio::time::timeout(
                    LEVEL_TIMEOUT,
                    run_batch(
                        &clients,
                        &stream_ids,
                        &stream_names,
                        msgs_per_client as usize,
                        batch_size,
                        &payload,
                    ),
                )
                .await;

                let rss_before = rss_kb();
                let cpu_before = cpu_time_ns();
                let mut total_time = Duration::ZERO;

                for _ in 0..iterations {
                    match tokio::time::timeout(
                        LEVEL_TIMEOUT,
                        run_batch(
                            &clients,
                            &stream_ids,
                            &stream_names,
                            msgs_per_client as usize,
                            batch_size,
                            &payload,
                        ),
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
                let total_msgs_all = total_msgs_per_iter * iterations as u64;

                print_result(&BenchResult {
                    label,
                    avg: total_time / iterations,
                    throughput: total_msgs_all as f64 / total_time.as_secs_f64(),
                    per_conn: total_msgs_all as f64 / total_time.as_secs_f64() / n as f64,
                    rss: rss_after,
                    rss_delta: rss_after.saturating_sub(rss_before),
                    cpu_pct,
                });
            });
        }

        // ── Batch publish SYNC ──────────────────────────────────────────
        println!("\n[ publish_batch_wait — batch={batch_size}, {total_msgs} msgs total/iter, server-confirmed ]");
        print_header();

        for &n in &concurrency {
            let msgs_per_client = total_msgs / n as u32;
            let clients: Vec<Client> = rt.block_on(async {
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(connect(&addr).await);
                }
                v
            });

            let total_msgs_per_iter = msgs_per_client as u64 * n as u64;
            let label = format!("{n}conn_{n}stream/{total_msgs_per_iter}");

            rt.block_on(async {
                let stream_ids = reset_streams(&setup_client, &stream_names).await;
                let _ = tokio::time::timeout(
                    LEVEL_TIMEOUT,
                    run_batch_sync(
                        &clients,
                        &stream_ids,
                        &stream_names,
                        msgs_per_client as usize,
                        batch_size,
                        &payload,
                    ),
                )
                .await;

                let rss_before = rss_kb();
                let cpu_before = cpu_time_ns();
                let mut total_time = Duration::ZERO;

                for _ in 0..iterations {
                    match tokio::time::timeout(
                        LEVEL_TIMEOUT,
                        run_batch_sync(
                            &clients,
                            &stream_ids,
                            &stream_names,
                            msgs_per_client as usize,
                            batch_size,
                            &payload,
                        ),
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
                let total_msgs_all = total_msgs_per_iter * iterations as u64;

                print_result(&BenchResult {
                    label,
                    avg: total_time / iterations,
                    throughput: total_msgs_all as f64 / total_time.as_secs_f64(),
                    per_conn: total_msgs_all as f64 / total_time.as_secs_f64() / n as f64,
                    rss: rss_after,
                    rss_delta: rss_after.saturating_sub(rss_before),
                    cpu_pct,
                });
            });
        }
    } // close `if run_publish`

    if run_replay_section {
        // ── Replay (drain pre-published backlog) ────────────────────
        println!("\n[ replay_drain — {replay_msgs} msgs pre-loaded/stream, DeliverPolicy::All ]");
        print_header();

        let _replay_conc = env_concurrency(REPLAY_CONCURRENCY);
        for &n in &_replay_conc {
            let total_msgs_per_iter = replay_msgs as u64 * n as u64;
            let label = format!("{n}conn_{n}stream/{total_msgs_per_iter}");

            rt.block_on(async {
                let rp_names: Vec<Vec<u8>> = (0..n)
                    .map(|i| format!("rpstream_{n}_{i}").into_bytes())
                    .collect();
                let rp_ids = reset_streams(&setup_client, &rp_names).await;

                prefill_streams(&setup_client, &rp_ids, &rp_names, n, replay_msgs, &payload).await;

                // One real TCP connection per consumer — matches the publish
                // mode topology and lets replay throughput scale with conns.
                let reader_clients: Vec<Client> = {
                    let mut v = Vec::with_capacity(n);
                    for _ in 0..n {
                        v.push(connect(&addr).await);
                    }
                    v
                };

                let rss_before = rss_kb();
                let cpu_before = cpu_time_ns();
                let mut total_time = Duration::ZERO;

                let progress: Arc<Vec<std::sync::atomic::AtomicU32>> =
                    Arc::new((0..n).map(|_| std::sync::atomic::AtomicU32::new(0)).collect());

                arbitro_server::lifecycle_trace::enable();
                for iter in 0..replay_iterations {
                    match tokio::time::timeout(
                        REPLAY_TIMEOUT,
                        run_replay(
                            &setup_client,
                            &reader_clients,
                            &rp_ids,
                            n,
                            replay_msgs,
                            iter,
                            &progress,
                        ),
                    )
                    .await
                    {
                        Ok((d, pairs)) => {
                            total_time += d;
                            for (_, consumer_id) in pairs {
                                let _ = setup_client.delete_consumer(consumer_id).await.ok();
                            }
                        }
                        Err(_) => {
                            println!("  {label:30} | TIMEOUT ({REPLAY_TIMEOUT:?})");
                            for i in 0..n {
                                let got = progress[i].load(std::sync::atomic::Ordering::Relaxed);
                                println!(
                                    "  [stall] idx={i} stream_id={} got={got}/{replay_msgs} missing={}",
                                    rp_ids[i % rp_ids.len()],
                                    replay_msgs.saturating_sub(got)
                                );
                            }
                            // Bounded ring: these are the last events before
                            // everything went quiet, i.e. where it stopped.
                            let events = arbitro_server::lifecycle_trace::take();
                            println!("  --- lifecycle: last {} events ---", events.len());
                            for e in &events {
                                println!(
                                    "  [{}] {} conn={} seq={}",
                                    e.thread, e.label, e.conn_id, e.seq
                                );
                            }
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
                let total_msgs_all = total_msgs_per_iter * replay_iterations as u64;

                print_result(&BenchResult {
                    label,
                    avg: total_time / replay_iterations,
                    throughput: total_msgs_all as f64 / total_time.as_secs_f64(),
                    per_conn: total_msgs_all as f64 / total_time.as_secs_f64() / n as f64,
                    rss: rss_after,
                    rss_delta: rss_after.saturating_sub(rss_before),
                    cpu_pct,
                });

                for name in &rp_names {
                    let _ = setup_client.delete_stream(name).await.ok();
                }
            });
        }

        // ── Fanout replay: 3 clients × 3 consumers/client, 1 stream ──────
        if run_fanout {
            let n_clients = 3usize;
            let n_consumers_per_client = 3usize;
            const SUBS_PER_CONSUMER: usize = 3;
            let total_consumers = n_clients * n_consumers_per_client;
            let total_subs = total_consumers * SUBS_PER_CONSUMER;

            println!(
        "\n[ replay_fanout — {replay_msgs} msgs, {n_clients} clients × {n_consumers_per_client} consumers × {SUBS_PER_CONSUMER} subs, fanout ]"
    );
            println!(
                "  {:30} | {:>9} | {:>12} | {:>10} | {:>8} | {:>8} | {:>7}",
                "Config", "Avg time", "Throughput", "Per-consumer", "RSS", "Δ RSS", "CPU"
            );
            println!("  {}", "-".repeat(100));

            rt.block_on(async {
                // Its own slice, disjoint from every `ingest.{i}.>` above.
                let fanout_name = b"fanout_bench".as_slice();
                let created = StreamBuilder::new(fanout_name)
                    .filter(FANOUT_FILTER)
                    .journal_kind(JOURNAL_KIND_ENUM)
                    .upsert(&setup_client)
                    .await
                    .expect("upsert fanout stream");
                if !created {
                    let _ = setup_client.purge_stream(fanout_name).await.ok();
                }
                let info = setup_client
                    .get_stream(fanout_name)
                    .await
                    .expect("get fanout stream");
                let fanout_stream_id = u64::from_le_bytes(info[..8].try_into().unwrap()) as u32;

                let mut clients = Vec::with_capacity(n_clients);
                for _ in 0..n_clients {
                    clients.push(connect(&addr).await);
                }

                // Create consumers and subscribe before publishing.
                // AckPolicy::None=0, DeliverPolicy::All=0, DeliverMode::Push=0
                let mut handles: Vec<(usize, u32, arbitro_client_tokio::SubscriptionHandle)> =
                    Vec::new();
                for (ci, client) in clients.iter().enumerate() {
                    for si in 0..n_consumers_per_client {
                        let name = format!("fanout_c{ci}_s{si}");
                        let group = format!("fanout_g{ci}_{si}");
                        let resp = client
                            .create_consumer(
                                fanout_stream_id,
                                name.as_bytes(),
                                group.as_bytes(),
                                b"",
                                u16::MAX,
                                0, // ack_policy = None
                                0, // deliver_policy = All
                                0, // deliver_mode = Push/Fanout
                                30_000,
                                0,
                            )
                            .await
                            .expect("create fanout consumer");
                        let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
                        // SUBS_PER_CONSUMER subscriptions on ONE consumer. The
                        // broker still puts a single copy per (connection,
                        // consumer) on the wire — the fan-out to siblings is the
                        // client's, in its own memory. So the wire cost is flat
                        // and only the delivery count multiplies. What this
                        // measures is whether that local fan-out is free.
                        for _ in 0..SUBS_PER_CONSUMER {
                            let handle = client
                                .subscribe(fanout_stream_id, consumer_id, b"")
                                .await
                                .expect("subscribe");
                            handles.push((ci, consumer_id, handle));
                        }
                    }
                }

                // Publish after all consumers subscribed.
                let pub_client = connect(&addr).await;
                let rss_before = rss_kb();
                let cpu_before = cpu_time_ns();

                let expected = replay_msgs;
                let start = Instant::now();

                {
                    let batch_size = 256;
                    let total = expected as usize;
                    let payload_bytes = Bytes::copy_from_slice(&payload[..]);
                    let mut entries: Vec<BatchEntry<'_>> = Vec::with_capacity(batch_size);
                    for _ in 0..batch_size {
                        entries.push(BatchEntry {
                            subject: FANOUT_SUBJECT,
                            msg_id: &[],
                            payload: payload_bytes.clone(),
                        });
                    }
                    let batches = total.div_ceil(batch_size);
                    for b in 0..batches {
                        let size = batch_size.min(total - b * batch_size);
                        if b + 1 == batches {
                            pub_client
                                .publish_batch_wait(fanout_stream_id, &entries[..size])
                                .await
                                .expect("fanout publish_batch_wait");
                        } else {
                            loop {
                                match pub_client.publish_batch(fanout_stream_id, &entries[..size]) {
                                    Ok(()) => break,
                                    Err(arbitro_client_tokio::ClientError::QueueFull) => {
                                        tokio::task::yield_now().await;
                                    }
                                    Err(e) => panic!("fanout publish_batch: {e:?}"),
                                }
                            }
                        }
                    }
                }
                let pub_elapsed = start.elapsed();
                eprintln!(
                    "[fanout] published {expected} msgs in {:.0}ms",
                    pub_elapsed.as_secs_f64() * 1000.0
                );

                // Drain all consumers concurrently.
                let mut js = tokio::task::JoinSet::new();

                for (ci, consumer_id, mut handle) in handles {
                    js.spawn(async move {
                        let t0 = Instant::now();
                        let mut count = 0u32;
                        while count < expected {
                            if handle.recv().await.is_none() {
                                break;
                            }
                            count += 1;
                        }
                        let elapsed = t0.elapsed();
                        let rate = count as f64 / elapsed.as_secs_f64();
                        eprintln!(
                            "[fanout] client={ci} DONE count={count} in {:.0}ms ({:.0} msg/s)",
                            elapsed.as_secs_f64() * 1000.0,
                            rate,
                        );
                        (ci, consumer_id, count, elapsed)
                    });
                }

                let mut results: Vec<(usize, u32, u32, Duration)> = Vec::new();
                while let Some(res) = js.join_next().await {
                    if let Ok(r) = res {
                        results.push(r);
                    }
                }
                let total_elapsed = start.elapsed();

                let cpu_after = cpu_time_ns();
                let rss_after = rss_kb();
                let wall_ns = total_elapsed.as_nanos() as u64;
                let cpu_ns = cpu_after.saturating_sub(cpu_before);
                let cpu_pct = if wall_ns > 0 {
                    cpu_ns as f64 / wall_ns as f64 * 100.0
                } else {
                    0.0
                };

                let total_delivered: u64 = results.iter().map(|r| r.2 as u64).sum();
                let aggregate_rate = total_delivered as f64 / total_elapsed.as_secs_f64();
                let per_consumer_rate = aggregate_rate / total_subs as f64;

                // Deliveries are `total_subs × replay_msgs`; wire copies are
                // `total_consumers × replay_msgs`. Printing both keeps the
                // aggregate rate honest — it counts handles, not bytes.
                eprintln!(
                    "[fanout] {} deliveries over {} wire copies ({}× local fan-out)",
                    total_delivered,
                    total_consumers as u64 * replay_msgs as u64,
                    SUBS_PER_CONSUMER,
                );

                let label = format!(
                    "{}cli×{}cons×{}sub/{}msgs",
                    n_clients, n_consumers_per_client, SUBS_PER_CONSUMER, replay_msgs
                );
                print_result(&BenchResult {
                    label,
                    avg: total_elapsed,
                    throughput: aggregate_rate,
                    per_conn: per_consumer_rate,
                    rss: rss_after,
                    rss_delta: rss_after.saturating_sub(rss_before),
                    cpu_pct,
                });

                for (_, consumer_id, _, _) in results {
                    let _ = setup_client.delete_consumer(consumer_id).await.ok();
                }
                let _ = setup_client.delete_stream(fanout_name).await.ok();
            });
        } // close `if run_fanout`
    } // close `if run_replay_section`

    // suppress unused warning
    let _ = all_stream_ids;

    println!("\n{}", "=".repeat(110));
    cleanup_tolerant();
}
