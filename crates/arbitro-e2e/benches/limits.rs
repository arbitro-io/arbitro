//! Subject-limit isolation bench — VIP delivery latency under basic load.
//!
//! Verifies that a high-priority subject (`orders.premium.*`) keeps
//! delivering with bounded latency even while `orders.basic.*` holds a
//! large backlog at its `max_subject_inflight`.
//!
//! ## Semantics being verified
//!
//! `max_subject_inflight(pattern, N)` sets a **per-subject** limit whose
//! VALUE comes from the pattern. Each unique subject that matches the
//! pattern keeps its own atomic counter capped at N — they do NOT share
//! one counter per pattern.
//!
//! ## Setup (once per stage)
//!   - Consumer:
//!       max_inflight = 10_000
//!       AckPolicy::Explicit
//!   - 100 UNIQUE basic subjects published and drained without ack so the
//!     consumer has 100 independent basic counters each at 1/1 inflight.
//!
//! Loop (BENCH_LIMITS_ITERS iters, default 1000):
//!   - Publish a fresh "orders.premium.vip_{i}" message.
//!   - Measure time until delivery.
//!   - ack to free the premium subject inflight slot for next iter.
//!
//! Reports avg / p50 / p99 latency. Constant latency across iterations
//! confirms the basic load does not bleed into premium.
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench limits --no-run 2>&1"
//!   wsl bash -lc "cp .../target/release/deps/limits-* /tmp/arbitro-bench/ && \
//!     cd /tmp/arbitro-bench && timeout 60 ./limits-* --bench"

use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_client_tokio::{BatchEntry, Client, ClientConfig, SubscriptionHandle};
use bytes::Bytes;
use arbitro_server::{ArbitroServer, Config};

const DEFAULT_ITERS: u64 = 1_000;
const BASIC_BACKLOG: u32 = 100;
const PAYLOAD_SIZE: usize = 64;
const STREAM: &[u8] = b"limits_e2e";
/// Default users for Stage 4 (dynamic subjects throughput).
const DEFAULT_DYNAMIC_USERS: u64 = 10_000;

fn env_u64(var: &str, fallback: u64) -> u64 {
    std::env::var(var).ok().and_then(|s| s.parse().ok()).unwrap_or(fallback)
}

fn portpicker() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn spawn_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(32)
        .shard_count(1)
        .write_buffer_cap(1024 * 1024);
    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(120)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect(ClientConfig { addr: addr.to_string(), ..ClientConfig::default() })
        .await
        .expect("client connects")
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Run `iters` VIP publish + deliver rounds, measuring each.
/// Takes client wrapped in Arc so this async fn is Send (Client is Send but not Sync,
/// so &Client is not Send; Arc<Client> with only sync usage of publish is also not Send,
/// so we clone the arc and take by value but only call sync methods before await).
///
/// Note: We can't use `&Client` in an async fn that is `Send` because Client is not Sync.
/// Solution: pass client by value. Caller should clone before passing.
async fn measure_vip_latency(
    client: Client,
    stream_id: u32,
    mut sub: SubscriptionHandle,
    payload: Vec<u8>,
    iters: u64,
) -> (Vec<Duration>, Client, SubscriptionHandle) {
    let mut latencies = Vec::with_capacity(iters as usize);
    for i in 0..iters {
        let subj = format!("orders.premium.vip_{i}");
        let start = Instant::now();
        // publish is sync — loop on ring full
        loop {
            match client.publish(stream_id, subj.as_bytes(), Bytes::copy_from_slice(&payload)) {
                Ok(()) => break,
                Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => panic!("vip publish: {e:?}"),
            }
        }
        let vip_msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("VIP delivery timeout")
            .expect("subscription closed");
        latencies.push(start.elapsed());
        vip_msg.ack();
    }
    (latencies, client, sub)
}

async fn baseline_latency(iters: u64) -> Vec<Duration> {
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    let resp = client
        .create_stream(STREAM, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // No subject-inflight limits — pure baseline.
    // AckPolicy::Explicit=1, DeliverPolicy::All=0
    let resp = client
        .create_consumer(
            stream_id,
            b"baseline",
            b"",
            b">",
            10_000,
            1, // ack_policy = Explicit
            0, // deliver_policy = All
            0, // deliver_mode = Push/Fanout
            30_000,
            0,
        )
        .await
        .unwrap();
    let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
    let sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
    let payload = vec![0u8; PAYLOAD_SIZE];

    let (latencies, _client, _sub) = measure_vip_latency(client, stream_id, sub, payload, iters).await;
    latencies
}

/// Multi-client isolation: N parallel clients, each on its own STREAM
/// with its own consumer. Runs concurrently via join_all (no tokio::spawn
/// since Client futures are not Send).
async fn multi_client_isolated_latency(
    iters: u64,
    n_clients: u64,
) -> Vec<Vec<Duration>> {
    let addr = spawn_server().await;

    let mut futs = Vec::new();
    for i in 0..n_clients {
        let addr = addr.clone();
        futs.push(async move {
            let client = connect(&addr).await;
            let stream = format!("limits_stream_c{i}");
            let resp = client
                .create_stream(stream.as_bytes(), b">", 0, 0, 0, 1, 0, 0, 0, 0)
                .await
                .unwrap();
            let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

            let consumer_name = format!("isolation_tester_c{i}");
            let resp = client
                .create_consumer(
                    stream_id,
                    consumer_name.as_bytes(),
                    b"",
                    b">",
                    10_000,
                    1, // ack_policy = Explicit
                    0, // deliver_policy = All
                    0, // deliver_mode = Push/Fanout
                    30_000,
                    0,
                )
                .await
                .unwrap();
            let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
            let mut sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
            let payload = vec![0u8; PAYLOAD_SIZE];

            let basic_subjects: Vec<String> =
                (0..BASIC_BACKLOG).map(|j| format!("orders.basic.user_{j}")).collect();
            let basic_entries: Vec<BatchEntry<'_>> = basic_subjects
                .iter()
                .map(|s| BatchEntry::new(s.as_bytes(), Bytes::copy_from_slice(payload.as_slice())))
                .collect();
            client
                .publish_batch_sync(stream_id, &basic_entries)
                .await
                .unwrap();

            let mut got = 0u32;
            while got < BASIC_BACKLOG {
                let _msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
                    .await
                    .expect("basic backlog timeout")
                    .expect("subscription closed");
                got += 1;
            }

            let (latencies, _c, _s) =
                measure_vip_latency(client, stream_id, sub, payload, iters).await;
            latencies
        });
    }

    futures::future::join_all(futs).await
}

async fn isolated_latency(iters: u64) -> Vec<Duration> {
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    let resp = client
        .create_stream(STREAM, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    let resp = client
        .create_consumer(
            stream_id,
            b"isolation_tester",
            b"",
            b">",
            10_000,
            1, // ack_policy = Explicit
            0, // deliver_policy = All
            0, // deliver_mode = Push/Fanout
            30_000,
            0,
        )
        .await
        .unwrap();
    let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
    let mut sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
    let payload = vec![0u8; PAYLOAD_SIZE];

    // ── Saturate basic: publish 100 unique basic subjects, drain without ack.
    let basic_subjects: Vec<String> =
        (0..BASIC_BACKLOG).map(|i| format!("orders.basic.user_{i}")).collect();
    let basic_entries: Vec<BatchEntry<'_>> = basic_subjects
        .iter()
        .map(|s| BatchEntry::new(s.as_bytes(), Bytes::copy_from_slice(payload.as_slice())))
        .collect();
    client.publish_batch_sync(stream_id, &basic_entries).await.unwrap();

    // Receive all BASIC_BACKLOG but do NOT ack — keep pressure on.
    let mut got = 0u32;
    while got < BASIC_BACKLOG {
        let _msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("basic backlog timeout")
            .expect("subscription closed");
        got += 1;
    }

    // ── Measure premium-VIP delivery while 100 basic pendings hold.
    let (latencies, _c, _s) = measure_vip_latency(client, stream_id, sub, payload, iters).await;
    latencies
}

fn report(label: &str, latencies: &[Duration]) {
    let mut sorted = latencies.to_vec();
    sorted.sort();
    let sum: Duration = sorted.iter().sum();
    let avg = sum / sorted.len() as u32;
    let p50 = percentile(&sorted, 0.50);
    let p90 = percentile(&sorted, 0.90);
    let p99 = percentile(&sorted, 0.99);
    let min = sorted.first().copied().unwrap_or(Duration::ZERO);
    let max = sorted.last().copied().unwrap_or(Duration::ZERO);
    println!(
        "  {label:<36} | n={:<5} | avg={:>8.2?} | p50={:>8.2?} | p90={:>8.2?} | p99={:>8.2?} | min={:>8.2?} | max={:>8.2?}",
        sorted.len(),
        avg,
        p50,
        p90,
        p99,
        min,
        max
    );
}

/// Stage 4 — high-cardinality dynamic subjects throughput.
async fn dynamic_subjects_throughput(n_users: u64) -> (Duration, u64) {
    let addr = spawn_server().await;
    let client = connect(&addr).await;

    let stream_name: &[u8] = b"dynamic_subjects";
    let resp = client
        .create_stream(stream_name, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // Consumer with AckPolicy::Explicit=1
    let resp = client
        .create_consumer(
            stream_id,
            b"dyn_consumer",
            b"",
            b">",
            60_000,
            1, // ack_policy = Explicit
            0, // deliver_policy = All
            0, // deliver_mode = Push/Fanout
            30_000,
            0,
        )
        .await
        .unwrap();
    let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
    let mut sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
    let payload = vec![0u8; PAYLOAD_SIZE];

    let subjects: Vec<String> =
        (0..n_users).map(|i| format!("notif.user.{i}")).collect();
    let entries: Vec<BatchEntry<'_>> = subjects
        .iter()
        .map(|s| BatchEntry::new(s.as_bytes(), Bytes::copy_from_slice(payload.as_slice())))
        .collect();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let start = Instant::now();

    client.publish_batch_sync(stream_id, &entries).await.unwrap();

    let mut got = 0u64;
    while got < n_users {
        match tokio::time::timeout(Duration::from_secs(30), sub.recv()).await {
            Ok(Some(msg)) => {
                msg.ack();
                got += 1;
            }
            _ => break,
        }
    }

    let elapsed = start.elapsed();
    (elapsed, got)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let iters = env_u64("BENCH_LIMITS_ITERS", DEFAULT_ITERS);
    let n_clients = env_u64("BENCH_LIMITS_CLIENTS", 4);
    let dynamic_users = env_u64("BENCH_LIMITS_DYNAMIC_USERS", DEFAULT_DYNAMIC_USERS);

    println!();
    println!("========================================================");
    println!("                  Subject limits bench");
    println!("========================================================");
    println!("  iters={iters}   payload={PAYLOAD_SIZE}B");
    println!("  basic backlog held unacked during isolated run: {BASIC_BACKLOG}");
    println!();

    // Stage 1 — baseline
    println!("--------------------------------------------------------");
    println!("  Stage 1 — baseline (no subject limits, no backlog)");
    println!("--------------------------------------------------------");
    let base = baseline_latency(iters).await;
    report("baseline VIP publish -> deliver", &base);

    // Stage 2 — isolated under load
    println!();
    println!("--------------------------------------------------------");
    println!("  Stage 2 — isolated (100 basic held unacked)");
    println!("--------------------------------------------------------");
    let iso = isolated_latency(iters).await;
    report("VIP under basic load", &iso);

    // Summary
    let avg_base: Duration =
        base.iter().sum::<Duration>() / base.len() as u32;
    let avg_iso: Duration =
        iso.iter().sum::<Duration>() / iso.len() as u32;
    let ratio = avg_iso.as_secs_f64() / avg_base.as_secs_f64();

    // Stage 3 — multi-client isolated
    println!();
    println!("--------------------------------------------------------");
    println!(
        "  Stage 3 — multi-client isolated ({n_clients} parallel clients, each with 100 basic held)"
    );
    println!("--------------------------------------------------------");
    let per_client = multi_client_isolated_latency(iters, n_clients).await;
    let iters_per_client = iters;
    for (i, lats) in per_client.iter().enumerate() {
        let label = format!("client {i} VIP under load");
        report(&label, lats);
    }

    // Aggregate latency across all clients.
    let mut all: Vec<Duration> = Vec::with_capacity((iters_per_client * n_clients) as usize);
    for lats in &per_client {
        all.extend_from_slice(lats);
    }
    let avg_multi: Duration = all.iter().sum::<Duration>() / all.len() as u32;
    let ratio_multi = avg_multi.as_secs_f64() / avg_base.as_secs_f64();

    println!();
    println!("--------------------------------------------------------");
    println!("  Summary");
    println!("--------------------------------------------------------");
    println!("  baseline (1 client, no load)    avg : {:>9.2?}", avg_base);
    println!("  isolated (1 client, basic load) avg : {:>9.2?}", avg_iso);
    println!("  multi    ({n_clients} clients, basic load each) avg : {:>9.2?}", avg_multi);
    println!("  ratios (vs baseline):  isolated={ratio:.2}x   multi={ratio_multi:.2}x");
    println!("  (closer to 1.0 = better isolation)");
    println!();

    // Stage 4 — dynamic subjects throughput
    println!("--------------------------------------------------------");
    println!(
        "  Stage 4 — dynamic subjects throughput ({dynamic_users} unique users)"
    );
    println!("  Pattern: `notif.user.<id>` with AckPolicy::Explicit");
    println!("  Exercises: HashMap insert+remove on every msg lifecycle");
    println!("--------------------------------------------------------");
    let (elapsed, delivered) = dynamic_subjects_throughput(dynamic_users).await;
    let msgs_per_sec = delivered as f64 / elapsed.as_secs_f64();
    let ns_per_msg = elapsed.as_nanos() as f64 / delivered as f64;
    println!(
        "  {dynamic_users} users | delivered={delivered} | elapsed={:.2?} | {msgs_per_sec:>10.0} msg/s | {ns_per_msg:>7.0} ns/msg",
        elapsed
    );
    println!();

    // Stage 4b — 1k for comparison
    let small_n = 1_000u64;
    let (elapsed_s, delivered_s) = dynamic_subjects_throughput(small_n).await;
    let msgs_per_sec_s = delivered_s as f64 / elapsed_s.as_secs_f64();
    let ns_per_msg_s = elapsed_s.as_nanos() as f64 / delivered_s as f64;
    println!(
        "  {small_n} users  | delivered={delivered_s} | elapsed={:.2?} | {msgs_per_sec_s:>10.0} msg/s | {ns_per_msg_s:>7.0} ns/msg",
        elapsed_s
    );
    println!();

    // suppress unused warnings
    let _ = (ratio, ratio_multi, Arc::new(()));
}
