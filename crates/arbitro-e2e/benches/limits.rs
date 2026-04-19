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
//! Example: `max_subject_inflight(b"orders.basic.>", 1)` means every
//! unique subject under `orders.basic.>` has its own counter with cap 1.
//! 100 different subjects can each have 1 pending simultaneously (= 100
//! total pending). The bench exercises exactly this: 100 unique subjects
//! are held unacked in parallel, then we publish premium VIP msgs whose
//! (separate) subject counters are unaffected.
//!
//! ## Setup (once per stage)
//!   - Consumer:
//!       max_inflight                         = 10_000
//!       max_subject_inflight(`premium.>`, 10)   // per-subject cap
//!       max_subject_inflight(`basic.>`,    1)   // per-subject cap
//!   - 100 UNIQUE basic subjects published and drained without ack so the
//!     consumer has 100 independent basic counters each at 1/1 inflight.
//!
//! Loop (BENCH_LIMITS_ITERS iters, default 1000):
//!   - Publish a fresh "orders.premium.vip_{i}" message.
//!   - Measure time until delivery.
//!   - ack_sync to free the premium-subject inflight slot for next iter.
//!
//! Reports avg / p50 / p99 latency. Constant latency across iterations
//! confirms the basic load does not bleed into premium.
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench limits --no-run 2>&1"
//!   wsl bash -lc "cp .../target/release/deps/limits-* /tmp/arbitro-bench/ && \
//!     cd /tmp/arbitro-bench && timeout 60 ./limits-* --bench"

use std::time::{Duration, Instant};

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, StreamConfig};
use arbitro_server::{ArbitroServer, Config};

const DEFAULT_ITERS: u64 = 1_000;
const BASIC_BACKLOG: u32 = 100;
const PAYLOAD_SIZE: usize = 64;
const STREAM: &[u8] = b"limits_e2e";

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
    Client::connect_with_timeout(addr, Duration::from_secs(5))
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

/// Run `iters` VIP publish + deliver rounds, measuring each. Returns the
/// per-iteration latencies.
async fn measure_vip_under_load(
    client: &Client,
    sub: &mut arbitro_client::SubscriptionHandle,
    payload: &[u8],
    iters: u64,
) -> Vec<Duration> {
    let mut latencies = Vec::with_capacity(iters as usize);
    for i in 0..iters {
        let subj = format!("orders.premium.vip_{i}");
        let start = Instant::now();
        client.publish(STREAM, subj.as_bytes(), payload).await.unwrap();
        let vip_msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .expect("VIP delivery timeout")
            .expect("subscription closed");
        latencies.push(start.elapsed());
        // ack_sync keeps the premium subject inflight from drifting upward
        // across iterations — one RTT per iter, not timed.
        vip_msg
            .ack_sync()
            .await
            .expect("VIP ack_sync should succeed");
    }
    latencies
}

async fn baseline_latency(iters: u64) -> Vec<Duration> {
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    client
        .create_stream(&StreamConfig::new(STREAM, b">").build())
        .await
        .unwrap();

    // No subject-inflight limits on this consumer — pure baseline.
    let cfg = ConsumerConfig::new(b"baseline", STREAM)
        .filter(b">")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(10_000)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&cfg).await.unwrap();
    let mut sub = consumer.subscribe(None).await.unwrap();
    let payload = vec![0u8; PAYLOAD_SIZE];

    measure_vip_under_load(&client, &mut sub, &payload, iters).await
}

/// Multi-client isolation: N parallel clients, each on its own STREAM
/// with its own consumer. The patterns (`orders.premium.>`,
/// `orders.basic.>`) are identical to the single-client stages — only the
/// stream is namespaced per client so the clients don't share a stream
/// and their workloads stay isolated at the server level too.
async fn multi_client_isolated_latency(
    iters: u64,
    n_clients: u64,
) -> Vec<Vec<Duration>> {
    let addr = spawn_server().await;

    let mut handles = Vec::new();
    for i in 0..n_clients {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let client = connect(&addr).await;
            let stream = format!("limits_stream_c{i}");
            client
                .create_stream(&StreamConfig::new(stream.as_bytes(), b">").build())
                .await
                .unwrap();

            // Unique consumer name per client — otherwise name_registry
            // would resolve all of them to the same consumer_id.
            let consumer_name = format!("isolation_tester_c{i}");
            let cfg = ConsumerConfig::new(consumer_name.as_bytes(), stream.as_bytes())
                .filter(b">")
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(10_000)
                .max_subject_inflight(b"orders.premium.>", 10)
                .max_subject_inflight(b"orders.basic.>", 1)
                .build()
                .unwrap();
            let consumer = client.create_consumer(&cfg).await.unwrap();
            let mut sub = consumer.subscribe(None).await.unwrap();
            let payload = vec![0u8; PAYLOAD_SIZE];

            // Same basic backlog shape as stage 2.
            let basic_subjects: Vec<String> =
                (0..BASIC_BACKLOG).map(|j| format!("orders.basic.user_{j}")).collect();
            let basic_entries: Vec<(&[u8], &[u8])> = basic_subjects
                .iter()
                .map(|s| (s.as_bytes(), payload.as_slice()))
                .collect();
            client
                .publish_batch(stream.as_bytes(), &basic_entries)
                .await
                .unwrap();

            let mut got = 0u32;
            while got < BASIC_BACKLOG {
                let _msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
                    .await
                    .expect("basic backlog timeout")
                    .expect("subscription closed");
                got += 1;
            }

            measure_vip_under_load_per_stream(
                &client,
                stream.as_bytes(),
                &mut sub,
                &payload,
                iters,
            )
            .await
        }));
    }

    let mut per_client: Vec<Vec<Duration>> = Vec::with_capacity(n_clients as usize);
    for h in handles {
        per_client.push(h.await.unwrap());
    }
    per_client
}

async fn measure_vip_under_load_per_stream(
    client: &Client,
    stream: &[u8],
    sub: &mut arbitro_client::SubscriptionHandle,
    payload: &[u8],
    iters: u64,
) -> Vec<Duration> {
    let mut latencies = Vec::with_capacity(iters as usize);
    for i in 0..iters {
        let subj = format!("orders.premium.vip_{i}");
        let start = Instant::now();
        client.publish(stream, subj.as_bytes(), payload).await.unwrap();
        let vip_msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .expect("VIP delivery timeout")
            .expect("subscription closed");
        assert!(
            vip_msg.subject.starts_with(b"orders.premium."),
            "got non-VIP msg: {:?}",
            std::str::from_utf8(&vip_msg.subject).unwrap_or("?")
        );
        latencies.push(start.elapsed());
        vip_msg
            .ack_sync()
            .await
            .expect("VIP ack_sync should succeed");
    }
    latencies
}

async fn isolated_latency(iters: u64) -> Vec<Duration> {
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    client
        .create_stream(&StreamConfig::new(STREAM, b">").build())
        .await
        .unwrap();

    let cfg = ConsumerConfig::new(b"isolation_tester", STREAM)
        .filter(b">")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(10_000)
        .max_subject_inflight(b"orders.premium.>", 10)
        .max_subject_inflight(b"orders.basic.>", 1)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&cfg).await.unwrap();
    let mut sub = consumer.subscribe(None).await.unwrap();
    let payload = vec![0u8; PAYLOAD_SIZE];

    // ── Saturate basic: publish 100 unique basic subjects, drain without ack.
    let basic_subjects: Vec<String> =
        (0..BASIC_BACKLOG).map(|i| format!("orders.basic.user_{i}")).collect();
    let basic_entries: Vec<(&[u8], &[u8])> = basic_subjects
        .iter()
        .map(|s| (s.as_bytes(), payload.as_slice()))
        .collect();
    client.publish_batch(STREAM, &basic_entries).await.unwrap();

    // Receive all BASIC_BACKLOG but do NOT ack — keep pressure on.
    // Dropping the Message does not send an ack (Message has no Drop impl
    // that acks), so the server-side inflight stays at 1 per basic subject.
    let mut got = 0u32;
    while got < BASIC_BACKLOG {
        let _msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .expect("basic backlog timeout")
            .expect("subscription closed");
        got += 1;
    }

    // ── Measure premium-VIP delivery while 100 basic pendings hold.
    measure_vip_under_load(&client, &mut sub, &payload, iters).await
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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let iters = env_u64("BENCH_LIMITS_ITERS", DEFAULT_ITERS);
    let n_clients = env_u64("BENCH_LIMITS_CLIENTS", 4);

    println!();
    println!("========================================================");
    println!("                  Subject limits bench");
    println!("========================================================");
    println!("  iters={iters}   payload={PAYLOAD_SIZE}B");
    println!("  premium max_subject_inflight=10   basic max_subject_inflight=1");
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
}
