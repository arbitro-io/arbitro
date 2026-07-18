//! Benchmark: storage-category × scenario × concurrency matrix.
//!
//! Reworks `throughput.rs` around the insight that the broker's durability is
//! a SERVER-side property (store type + fsync policy), not something the
//! publish call decides. To make the fsync cost observable we sweep three
//! storage categories and, for each, the same scenarios at the same
//! concurrency levels — so the identical grid can be replicated across the
//! other clients (TS/Go/C) for apples-to-apples observability.
//!
//! ## Storage categories (a fresh in-process server per category)
//!
//! - `memory`         — MemoryStore (no `data_dir`). Pure RAM, fsync never runs.
//! - `tolerant`       — TolerantStore (mmap) + `FsyncPolicy::None`. The OK is
//!                      returned BEFORE fsync (flush happens via the store's own
//!                      timer/rotation mechanism).
//! - `tolerant-fsync` — TolerantStore (mmap) + `FsyncPolicy::EveryWrite`. fsync
//!                      per write BEFORE the OK. Must be the slowest.
//!
//! ## Scenarios (per category), grouped fire-and-forget → wait → replay
//!
//! - `single_ff`   — single publish, fire-and-forget (does NOT wait for the OK;
//!                   only waits for the socket write). fsync is irrelevant here.
//! - `batch_ff`    — batch-256 publish, fire-and-forget.
//! - `single_wait` — single publish, waits for the broker OK per message. THE
//!                   fsync demonstrator: memory ≈ tolerant (OK before fsync) ≪
//!                   tolerant-fsync (OK after fsync, ~30× slower).
//! - `batch_wait`  — batch-256 publish, waits for the OK per batch. Amortizes
//!                   the fsync over the batch → stays fast where single_wait
//!                   collapses (the batching-vs-fsync tradeoff).
//! - `replay`      — drain: publish ALL messages first, THEN subscribe and drain
//!                   (isolates broker→client delivery throughput; a READ, so
//!                   ~fsync-independent).
//!
//! ## External broker mode
//!
//! Set `ARBITRO_ADDR=host:port` to CONNECT to an already-running broker instead
//! of spinning up in-process servers. In that mode the storage category is
//! whatever that broker was launched with, so the matrix collapses to a single
//! `external` category. This lets the same binary drive a Dockerized broker
//! without a second bench.
//!
//! ## Cleanup
//!
//! Every stream created is tracked and DELETED at the end of its category, so
//! repeated runs never leave orphan streams on a shared/external broker.
//! In-process temp data dirs are auto-removed (tempfile) on drop.
//!
//! ## Tunables (env)
//!
//! - `ARBITRO_ADDR`        — external broker host:port (else in-process).
//! - `BENCH_STORAGE`       — comma list: `memory,tolerant,tolerant-fsync`
//!                           (in-process only; default all three).
//! - `BENCH_SCENARIOS`     — comma list: `single_ff,single_wait,batch_ff,replay`
//!                           (default all four).
//! - `BENCH_CONCURRENCY`   — comma list of connection counts (default `1,8`).
//! - `BENCH_MSGS`          — total publish msgs/iter, split across conns (10_000).
//! - `BENCH_REPLAY_MSGS`   — msgs pre-loaded per stream for replay (10_000).
//! - `BENCH_ITERATIONS`    — measured iterations per cell (5).
//! - `BENCH_BATCH`         — batch size for batch_ff (256, capped at 256).
//!
//! ## bench_safety waiver
//!
//! Defaults are bounded for a quick run: `BENCH_MSGS=10_000` total/iter and
//! `BENCH_REPLAY_MSGS=10_000` per stream. Per-iteration inflight scales with
//! concurrency (max 32×1000 under the shared waiver). Timeouts (LEVEL=15s,
//! REPLAY=120s) protect against hangs.

extern crate libc;

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::runtime::Runtime;

use arbitro_client_tokio::{BatchEntry, Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config, FsyncPolicy};
use bytes::Bytes;
use tempfile::TempDir;

// ── Defaults ────────────────────────────────────────────────────

const TOTAL_MSGS: u32 = 10_000;
/// `single_wait` is a per-message latency probe, not a throughput test — on
/// `tolerant-fsync` each op pays a full fsync (6-20ms+ on a slow/virtual disk
/// like WSL2), so even a few thousand round-trips can blow the timeout. Keep
/// the sample small — a few hundred is enough for a stable median. Override
/// via BENCH_WAIT_MSGS.
const WAIT_MSGS: u32 = 500;
const REPLAY_MSGS: u32 = 10_000;
const ITERATIONS: u32 = 5;
const BATCH_SIZE: usize = 256;
const CONCURRENCY: &[usize] = &[1, 8];
const PAYLOAD_SIZE: usize = 64;

// fsync-every makes every store append durable, so per-op paths are ~1000×
// slower than memory; give the level enough headroom to finish rather than
// TIMEOUT.
const LEVEL_TIMEOUT: Duration = Duration::from_secs(30);
const REPLAY_TIMEOUT: Duration = Duration::from_secs(120);

// ── Storage category ────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
enum Storage {
    Memory,
    Tolerant,
    TolerantFsync,
}

impl Storage {
    fn label(self) -> &'static str {
        match self {
            Storage::Memory => "memory",
            Storage::Tolerant => "tolerant",
            Storage::TolerantFsync => "tolerant-fsync",
        }
    }
    fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "memory" => Some(Storage::Memory),
            "tolerant" => Some(Storage::Tolerant),
            "tolerant-fsync" | "tolerant_fsync" => Some(Storage::TolerantFsync),
            _ => None,
        }
    }
}

// ── Scenario ────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    SingleFf,
    BatchFf,
    SingleWait,
    BatchWait,
    Replay,
}

impl Scenario {
    fn label(self) -> &'static str {
        match self {
            Scenario::SingleFf => "single_ff",
            Scenario::BatchFf => "batch_ff",
            Scenario::SingleWait => "single_wait",
            Scenario::BatchWait => "batch_wait",
            Scenario::Replay => "replay",
        }
    }
    fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "single_ff" => Some(Scenario::SingleFf),
            "batch_ff" => Some(Scenario::BatchFf),
            "single_wait" => Some(Scenario::SingleWait),
            "batch_wait" => Some(Scenario::BatchWait),
            "replay" | "replay_drain" => Some(Scenario::Replay),
            _ => None,
        }
    }
}

// ── Env helpers ─────────────────────────────────────────────────

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
fn env_list<T>(k: &str, default: Vec<T>, parse: impl Fn(&str) -> Option<T>) -> Vec<T> {
    match std::env::var(k) {
        Ok(s) => {
            let v: Vec<T> = s.split(',').filter_map(parse).collect();
            if v.is_empty() {
                default
            } else {
                v
            }
        }
        Err(_) => default,
    }
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

/// Start a fresh in-process server for `storage`. Returns the listen address
/// AND the server task handle — the caller MUST `.abort()` it before starting
/// the next category, otherwise the previous servers keep running and their
/// retained stores pile up RAM + (for disk categories) thrash the fsync path,
/// which silently degrades the later categories' numbers.
async fn start_server(
    storage: Storage,
    data_dir: Option<String>,
) -> (String, tokio::task::JoinHandle<()>) {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");

    let mut config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(500)
        .write_buffer_cap(65536);

    match storage {
        Storage::Memory => {
            // No data_dir → the shard router picks MemoryStore.
        }
        Storage::Tolerant => {
            config = config
                .data_dir(data_dir.expect("tolerant needs data_dir"))
                .fsync_policy(FsyncPolicy::None);
        }
        Storage::TolerantFsync => {
            config = config
                .data_dir(data_dir.expect("tolerant-fsync needs data_dir"))
                .fsync_policy(FsyncPolicy::Every);
        }
    }

    let server = ArbitroServer::new(config);
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle)
}

async fn connect(addr: &str) -> Client {
    Client::connect(ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    })
    .await
    .expect("client must connect")
}

/// Create a stream (delete-first for idempotency) and record its name so the
/// category can delete every stream it created on the way out. `journal_kind`
/// is sent as Memory(0) but is advisory — the physical store is chosen by the
/// server's data_dir, so the real durability comes from the category's config.
async fn make_stream(client: &Client, name: &[u8], created: &mut Vec<Vec<u8>>) -> u32 {
    let _ = client.delete_stream(name).await.ok();
    let resp = client
        .create_stream(name, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create stream");
    created.push(name.to_vec());
    u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32
}

async fn cleanup_streams(client: &Client, created: &[Vec<u8>]) {
    for name in created {
        let _ = client.delete_stream(name).await.ok();
    }
}

fn shared_payload() -> Arc<[u8]> {
    Arc::from(vec![0u8; PAYLOAD_SIZE].into_boxed_slice())
}

// ── Publish runners ─────────────────────────────────────────────

/// single_ff — fire-and-forget, one msg per publish. Never waits for the OK.
async fn run_single_ff(
    clients: &[Client],
    stream_ids: &[u32],
    msgs: u32,
    payload: &Arc<[u8]>,
) -> Duration {
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let stream_id = stream_ids[i % stream_ids.len()];
        let payload = payload.clone();
        js.spawn(async move {
            for _ in 0..msgs {
                loop {
                    match c.publish(stream_id, b"bench.msg", Bytes::copy_from_slice(&payload)) {
                        Ok(()) => break,
                        Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                            tokio::task::yield_now().await;
                        }
                        Err(e) => panic!("publish: {e:?}"),
                    }
                }
            }
        });
    }
    while js.join_next().await.is_some() {}
    start.elapsed()
}

/// single_wait — one msg per publish, waits for the broker OK each time
/// (batch-of-1 sync). This is where the fsync policy becomes visible.
async fn run_single_wait(
    clients: &[Client],
    stream_ids: &[u32],
    msgs: u32,
    payload: &Arc<[u8]>,
) -> Duration {
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let stream_id = stream_ids[i % stream_ids.len()];
        let payload = payload.clone();
        js.spawn(async move {
            let payload_bytes = Bytes::copy_from_slice(&payload[..]);
            let entry = [BatchEntry {
                subject: b"bench.msg".as_slice(),
                msg_id: &[],
                payload: payload_bytes,
            }];
            for _ in 0..msgs {
                c.publish_batch_wait(stream_id, &entry)
                    .await
                    .expect("publish_batch_wait(1)");
            }
        });
    }
    while js.join_next().await.is_some() {}
    start.elapsed()
}

/// batch_ff — batch publish, fire-and-forget.
async fn run_batch_ff(
    clients: &[Client],
    stream_ids: &[u32],
    total: usize,
    batch_size: usize,
    payload: &Arc<[u8]>,
) -> Duration {
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let stream_id = stream_ids[i % stream_ids.len()];
        let payload = payload.clone();
        js.spawn(async move {
            let payload_bytes = Bytes::copy_from_slice(&payload[..]);
            let mut entries: Vec<BatchEntry<'_>> = Vec::with_capacity(batch_size);
            for _ in 0..batch_size {
                entries.push(BatchEntry {
                    subject: b"bench.msg".as_slice(),
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
                        Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                            tokio::task::yield_now().await;
                        }
                        Err(e) => panic!("publish_batch: {e:?}"),
                    }
                }
            }
        });
    }
    while js.join_next().await.is_some() {}
    start.elapsed()
}

/// batch_wait — batch publish, waits for the broker OK per batch. On
/// `tolerant-fsync` this amortizes the fsync over the whole batch (one fsync
/// per batch, not per message), so it stays fast even while `single_wait`
/// collapses — the batching-vs-fsync tradeoff, measured.
async fn run_batch_wait(
    clients: &[Client],
    stream_ids: &[u32],
    total: usize,
    batch_size: usize,
    payload: &Arc<[u8]>,
) -> Duration {
    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        let stream_id = stream_ids[i % stream_ids.len()];
        let payload = payload.clone();
        js.spawn(async move {
            let payload_bytes = Bytes::copy_from_slice(&payload[..]);
            let mut entries: Vec<BatchEntry<'_>> = Vec::with_capacity(batch_size);
            for _ in 0..batch_size {
                entries.push(BatchEntry {
                    subject: b"bench.msg".as_slice(),
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
    while js.join_next().await.is_some() {}
    start.elapsed()
}

// ── Replay (prefill then drain) ─────────────────────────────────

/// Prefill `msgs` into a stream. Every batch is published SYNC (waits for the
/// broker OK) — paced rather than fire-and-forget. Firing batches F&F floods an
/// `fsync-every` server faster than it can drain and the connection drops
/// (`Disconnected`), which is exactly what this pacing avoids. Untimed, so the
/// per-batch sync cost doesn't matter. Returns Err if the broker rejects.
async fn prefill(
    client: &Client,
    stream_id: u32,
    msgs: u32,
    payload: &Arc<[u8]>,
) -> Result<(), arbitro_client_tokio::ClientError> {
    let payload_bytes = Bytes::copy_from_slice(&payload[..]);
    let mut entries: Vec<BatchEntry<'_>> = Vec::with_capacity(BATCH_SIZE);
    for _ in 0..BATCH_SIZE {
        entries.push(BatchEntry {
            subject: b"bench.msg".as_slice(),
            msg_id: &[],
            payload: payload_bytes.clone(),
        });
    }
    let total = msgs as usize;
    let batches = total.div_ceil(BATCH_SIZE);
    for b in 0..batches {
        let size = BATCH_SIZE.min(total - b * BATCH_SIZE);
        client.publish_batch_wait(stream_id, &entries[..size]).await?;
    }
    Ok(())
}

/// One replay drain: each of `n` streams is prefilled, then a dedicated reader
/// connection creates a consumer and drains all messages. Returns elapsed.
async fn run_replay(
    setup: &Client,
    reader_clients: &[Client],
    stream_ids: &[u32],
    n: usize,
    msgs: u32,
    tag: u64,
) -> Option<Duration> {
    // Prefill every stream (concurrently). If any prefill fails (e.g. the
    // broker drops under load) the drain would hang forever waiting for
    // messages that never arrived — bail with None so the cell shows SKIP.
    let mut pf = tokio::task::JoinSet::new();
    for i in 0..n {
        let c = setup.clone();
        let sid = stream_ids[i];
        let payload: Arc<[u8]> = Arc::from(vec![0u8; PAYLOAD_SIZE].into_boxed_slice());
        pf.spawn(async move { prefill(&c, sid, msgs, &payload).await });
    }
    let mut prefill_ok = true;
    while let Some(res) = pf.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                eprintln!("  [replay] prefill error: {e:?}");
                prefill_ok = false;
            }
            Err(e) => {
                eprintln!("  [replay] prefill task panicked: {e:?}");
                prefill_ok = false;
            }
        }
    }
    if !prefill_ok {
        return None;
    }

    // Create consumers.
    let mut consumers: Vec<(u32, u32)> = Vec::with_capacity(n);
    for i in 0..n {
        let sid = stream_ids[i];
        let name = format!("replay_{tag}_{i}");
        let resp = setup
            .create_consumer(sid, name.as_bytes(), b"", b"", u16::MAX, 0, 0, 0, 30_000, 0)
            .await
            .expect("create consumer");
        let cid = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
        consumers.push((sid, cid));
    }

    let start = Instant::now();
    let mut js = tokio::task::JoinSet::new();
    for (idx, (sid, cid)) in consumers.clone().into_iter().enumerate() {
        let client = reader_clients[idx].clone();
        let expected = msgs;
        js.spawn(async move {
            let mut handle = client.subscribe(sid, cid, b"").await.expect("subscribe");
            let mut count = 0u32;
            while count < expected {
                if handle.recv().await.is_none() {
                    break;
                }
                count += 1;
            }
            count
        });
    }
    let mut drained = 0u64;
    while let Some(res) = js.join_next().await {
        if let Ok(c) = res {
            drained += c as u64;
        }
    }
    let elapsed = start.elapsed();

    // Cleanup consumers (streams are cleaned by the caller).
    for (_, cid) in &consumers {
        let _ = setup.delete_consumer(*cid).await.ok();
    }

    let want = msgs as u64 * n as u64;
    if drained != want {
        eprintln!("  [replay] WARNING drained {drained}/{want}");
    }
    Some(elapsed)
}

// ── Reporting ───────────────────────────────────────────────────

struct Row {
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
        "  {:26} | {:>9} | {:>12} | {:>10} | {:>6} | {:>7} | {:>6}",
        "Config", "Avg time", "Throughput", "Per-conn", "RSS", "Δ RSS", "CPU"
    );
    println!("  {}", "-".repeat(92));
}

fn print_row(r: &Row) {
    println!(
        "  {:26} | {:>9.2?} | {:>10.0} msg/s | {:>8.0} msg/s | {:>3} MB | {:>+4} MB | {:>5.1}%",
        r.label,
        r.avg,
        r.throughput,
        r.per_conn,
        r.rss / 1024,
        r.rss_delta as i64 / 1024,
        r.cpu_pct
    );
}

// ── One category (either in-process server or external broker) ──

#[allow(clippy::too_many_arguments)]
fn run_category(
    rt: &Runtime,
    cat_label: &str,
    addr: &str,
    scenarios: &[Scenario],
    concurrency: &[usize],
    iterations: u32,
    total_msgs: u32,
    wait_msgs: u32,
    replay_msgs: u32,
    batch_size: usize,
    run_tag: u32,
) {
    let payload = shared_payload();
    let setup = rt.block_on(connect(addr));
    let mut created: Vec<Vec<u8>> = Vec::new();

    println!("\n{}", "=".repeat(94));
    println!("### storage = {cat_label}");
    println!("{}", "=".repeat(94));

    for &scenario in scenarios {
        // single_wait is a latency probe → fewer messages (cap).
        let scenario_total = if scenario == Scenario::SingleWait {
            total_msgs.min(wait_msgs)
        } else {
            total_msgs
        };

        if scenario == Scenario::Replay {
            println!(
                "\n[ replay — {replay_msgs} msgs pre-loaded/stream, publish-all-then-drain ]"
            );
        } else {
            println!("\n[ {} — {scenario_total} msgs total/iter ]", scenario.label());
        }
        print_header();

        for &n in concurrency {
            if scenario == Scenario::Replay {
                run_replay_cell(
                    rt,
                    addr,
                    &setup,
                    &mut created,
                    cat_label,
                    n,
                    replay_msgs,
                    run_tag,
                );
                continue;
            }

            let msgs_per_client = scenario_total / n as u32;
            let total_per_iter = msgs_per_client as u64 * n as u64;
            let label = format!("{n}conn/{total_per_iter}");

            // Connections + fresh streams for this cell.
            let clients: Vec<Client> = rt.block_on(async {
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(connect(addr).await);
                }
                v
            });
            let stream_ids: Vec<u32> = rt.block_on(async {
                let mut ids = Vec::with_capacity(n);
                for i in 0..n {
                    let name =
                        format!("m_{run_tag}_{cat_label}_{}_{n}_{i}", scenario.label());
                    ids.push(make_stream(&setup, name.as_bytes(), &mut created).await);
                }
                ids
            });

            rt.block_on(async {
                // Warmup (untimed).
                let warm = (msgs_per_client / 10).max(1).min(200);
                let _ = tokio::time::timeout(
                    LEVEL_TIMEOUT,
                    run_scenario(scenario, &clients, &stream_ids, warm, batch_size, &payload),
                )
                .await;

                let rss_before = rss_kb();
                let cpu_before = cpu_time_ns();
                let mut total_time = Duration::ZERO;

                for _ in 0..iterations {
                    match tokio::time::timeout(
                        LEVEL_TIMEOUT,
                        run_scenario(
                            scenario,
                            &clients,
                            &stream_ids,
                            msgs_per_client,
                            batch_size,
                            &payload,
                        ),
                    )
                    .await
                    {
                        Ok(d) => total_time += d,
                        Err(_) => {
                            println!("  {label:26} | TIMEOUT ({LEVEL_TIMEOUT:?})");
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
                let total_all = total_per_iter * iterations as u64;

                print_row(&Row {
                    label,
                    avg: total_time / iterations,
                    throughput: total_all as f64 / total_time.as_secs_f64(),
                    per_conn: total_all as f64 / total_time.as_secs_f64() / n as f64,
                    rss: rss_after,
                    rss_delta: rss_after.saturating_sub(rss_before),
                    cpu_pct,
                });
            });
        }
    }

    // Delete every stream this category created.
    let deleted = created.len();
    rt.block_on(cleanup_streams(&setup, &created));
    println!("\n  cleaned up {deleted} streams for storage={cat_label}");
}

/// Dispatch a publish scenario to its runner (unified future type).
async fn run_scenario(
    scenario: Scenario,
    clients: &[Client],
    stream_ids: &[u32],
    msgs_per_client: u32,
    batch_size: usize,
    payload: &Arc<[u8]>,
) -> Duration {
    match scenario {
        Scenario::SingleFf => run_single_ff(clients, stream_ids, msgs_per_client, payload).await,
        Scenario::SingleWait => run_single_wait(clients, stream_ids, msgs_per_client, payload).await,
        Scenario::BatchFf => {
            run_batch_ff(
                clients,
                stream_ids,
                msgs_per_client as usize,
                batch_size,
                payload,
            )
            .await
        }
        Scenario::BatchWait => {
            run_batch_wait(
                clients,
                stream_ids,
                msgs_per_client as usize,
                batch_size,
                payload,
            )
            .await
        }
        Scenario::Replay => unreachable!("replay handled separately"),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_replay_cell(
    rt: &Runtime,
    addr: &str,
    setup: &Client,
    created: &mut Vec<Vec<u8>>,
    cat_label: &str,
    n: usize,
    replay_msgs: u32,
    run_tag: u32,
) {
    let total_per_iter = replay_msgs as u64 * n as u64;
    let label = format!("{n}conn/{total_per_iter}");

    rt.block_on(async {
        // Fresh streams + one reader connection per consumer.
        let mut stream_ids = Vec::with_capacity(n);
        for i in 0..n {
            let name = format!("r_{run_tag}_{cat_label}_{n}_{i}");
            stream_ids.push(make_stream(setup, name.as_bytes(), created).await);
        }
        let readers: Vec<Client> = {
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                v.push(connect(addr).await);
            }
            v
        };

        let rss_before = rss_kb();
        let cpu_before = cpu_time_ns();

        let tag = run_tag as u64 * 100 + n as u64;
        let elapsed = match tokio::time::timeout(
            REPLAY_TIMEOUT,
            run_replay(setup, &readers, &stream_ids, n, replay_msgs, tag),
        )
        .await
        {
            Ok(Some(d)) => d,
            Ok(None) => {
                println!("  {label:26} | SKIP (prefill failed)");
                return;
            }
            Err(_) => {
                println!("  {label:26} | TIMEOUT ({REPLAY_TIMEOUT:?})");
                return;
            }
        };

        let cpu_after = cpu_time_ns();
        let rss_after = rss_kb();
        let wall_ns = elapsed.as_nanos() as u64;
        let cpu_ns = cpu_after.saturating_sub(cpu_before);
        let cpu_pct = if wall_ns > 0 {
            cpu_ns as f64 / wall_ns as f64 * 100.0
        } else {
            0.0
        };

        print_row(&Row {
            label,
            avg: elapsed,
            throughput: total_per_iter as f64 / elapsed.as_secs_f64(),
            per_conn: total_per_iter as f64 / elapsed.as_secs_f64() / n as f64,
            rss: rss_after,
            rss_delta: rss_after.saturating_sub(rss_before),
            cpu_pct,
        });
    });
}

// ── Main ────────────────────────────────────────────────────────

fn main() {
    let total_msgs = env_u32("BENCH_MSGS", TOTAL_MSGS);
    let wait_msgs = env_u32("BENCH_WAIT_MSGS", WAIT_MSGS);
    let replay_msgs = env_u32("BENCH_REPLAY_MSGS", REPLAY_MSGS);
    let iterations = env_u32("BENCH_ITERATIONS", ITERATIONS);
    let batch_size = env_usize("BENCH_BATCH", BATCH_SIZE).min(256);
    let concurrency = env_list("BENCH_CONCURRENCY", CONCURRENCY.to_vec(), |s| {
        s.trim().parse::<usize>().ok().filter(|&n| n > 0)
    });
    let scenarios = env_list(
        "BENCH_SCENARIOS",
        vec![
            // fire-and-forget group…
            Scenario::SingleFf,
            Scenario::BatchFf,
            // …then the same paths but waiting for the broker OK…
            Scenario::SingleWait,
            Scenario::BatchWait,
            // …then replay (drain).
            Scenario::Replay,
        ],
        Scenario::parse,
    );
    let run_tag = std::process::id();

    let rt = Runtime::new().unwrap();

    println!("\nStorage × Scenario × Concurrency matrix — {PAYLOAD_SIZE}B payload");
    println!(
        "Config: msgs={total_msgs} (split across conns), batch={batch_size}, concurrency={concurrency:?}, iters={iterations}, replay_msgs={replay_msgs}"
    );
    let scen_labels: Vec<&str> = scenarios.iter().map(|s| s.label()).collect();
    println!("Scenarios: {scen_labels:?}");

    if let Ok(addr) = std::env::var("ARBITRO_ADDR") {
        // External broker: one category, storage is whatever it was launched
        // with. We connect instead of spawning a server.
        println!("Mode: EXTERNAL broker at {addr} (storage category = whatever it runs)");
        run_category(
            &rt,
            "external",
            &addr,
            &scenarios,
            &concurrency,
            iterations,
            total_msgs,
            wait_msgs,
            replay_msgs,
            batch_size,
            run_tag,
        );
    } else {
        let storages = env_list(
            "BENCH_STORAGE",
            vec![Storage::Memory, Storage::Tolerant, Storage::TolerantFsync],
            Storage::parse,
        );
        println!("Mode: IN-PROCESS servers, one per storage category");

        for storage in storages {
            // Disk-backed categories need a temp data dir (auto-removed on drop).
            let tmp: Option<TempDir> = match storage {
                Storage::Memory => None,
                _ => Some(
                    tempfile::Builder::new()
                        .prefix(&format!("arbitro_bench_{}_", storage.label()))
                        .tempdir()
                        .expect("tempdir"),
                ),
            };
            let data_dir = tmp
                .as_ref()
                .map(|d| d.path().to_string_lossy().into_owned());

            let (addr, server) = rt.block_on(start_server(storage, data_dir));
            run_category(
                &rt,
                storage.label(),
                &addr,
                &scenarios,
                &concurrency,
                iterations,
                total_msgs,
                wait_msgs,
                replay_msgs,
                batch_size,
                run_tag,
            );
            // Tear the server down before the next category so its retained
            // stores don't accumulate and skew the later categories.
            server.abort();
            rt.block_on(async { let _ = server.await; });
            // `tmp` drops here → temp data dir removed.
            drop(tmp);
        }
    }

    println!("\n{}", "=".repeat(94));
    println!("done.");
}
