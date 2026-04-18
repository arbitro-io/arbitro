//! Benchmark: Multi-client High-Density Fanout & Filtering Stress Test.
//!
//! Runs two scenarios back-to-back:
//!   1. **pub/sub** — subscribe first, publish afterwards (live fanout).
//!   2. **replay**  — publish first, subscribe afterwards with
//!                    `DeliverPolicy::All` (historical replay).
//!
//! Density in each scenario:
//! - 3 Clients (Connections)
//! - 20 Subscriptions per Client (Total: 60)
//! - Mixed Filters: Direct, Wildcard, Global, and Non-Matching.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_client::Client;
use arbitro_proto::config::{
    AckPolicy, ConsumerConfig, DeliverMode, DeliverPolicy, StreamConfig,
};
use arbitro_server::{ArbitroServer, Config};

const NUM_CLIENTS: usize = 3;
const SUBS_PER_CLIENT: usize = 20;
const MSG_PER_TYPE: u64 = 100_000;

struct SubStats {
    count: AtomicU64,
    id: String,
    expected: u64,
    filter: String,
}

fn portpicker() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn create_test_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(100)
        .write_buffer_cap(1024 * 1024);
    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    addr
}

#[tokio::main]
async fn main() {
    let mut total_failures = 0;

    println!("\n╔══════════════════════════════════════════════╗");
    println!("║ Scenario 1: pub/sub (subscribe → publish)   ║");
    println!("╚══════════════════════════════════════════════╝");
    total_failures += run_pub_sub().await;

    println!("\n╔══════════════════════════════════════════════╗");
    println!("║ Scenario 2: replay (publish → subscribe)    ║");
    println!("╚══════════════════════════════════════════════╝");
    total_failures += run_replay().await;

    if total_failures > 0 {
        std::process::exit(1);
    }
}

/// Scenario 1 — subscribe first, then publish. Live fanout path.
async fn run_pub_sub() -> usize {
    let addr = create_test_server().await;
    let stream_name = b"fanout_live";

    let setup_client = Client::connect(&addr).await.unwrap();
    setup_client
        .create_stream(&StreamConfig::new(stream_name, b">").build())
        .await
        .unwrap();

    let (stats, _tasks) = subscribe_all(&addr, stream_name, DeliverPolicy::New).await;

    // Give subs a moment to propagate before bursting.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let start = Instant::now();
    println!("Bursting 300,000 messages across 3 subjects...");
    let payload = vec![0u8; 64];
    publish_all(&setup_client, stream_name, &payload).await;

    println!("Waiting for delivery...");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let elapsed = start.elapsed();

    report(&stats, elapsed, "pub/sub")
}

/// Scenario 2 — publish first, then subscribe with `DeliverPolicy::All`.
/// Replay path — late subscribers receive historical entries from seq=1.
async fn run_replay() -> usize {
    let addr = create_test_server().await;
    let stream_name = b"fanout_replay";

    let setup_client = Client::connect(&addr).await.unwrap();
    setup_client
        .create_stream(&StreamConfig::new(stream_name, b">").build())
        .await
        .unwrap();

    // ── Publish first ──────────────────────────────────────────────────
    println!("Publishing 300,000 messages BEFORE any subscription...");
    let payload = vec![0u8; 64];
    let pub_start = Instant::now();
    publish_all(&setup_client, stream_name, &payload).await;
    println!("Published in {:?}", pub_start.elapsed());

    // ── Subscribe later with DeliverPolicy::All (triggers replay) ──────
    let subscribe_start = Instant::now();
    let (stats, _tasks) = subscribe_all(&addr, stream_name, DeliverPolicy::All).await;
    println!("All subs registered in {:?}", subscribe_start.elapsed());

    println!("Waiting for replay delivery...");
    tokio::time::sleep(Duration::from_secs(5)).await;
    let elapsed = subscribe_start.elapsed();

    report(&stats, elapsed, "replay")
}

/// Create 3 clients × 20 subs with the given `DeliverPolicy`. Returns the
/// stats collectors + handles to keep subs alive.
async fn subscribe_all(
    addr: &str,
    stream_name: &[u8],
    policy: DeliverPolicy,
) -> (Vec<Arc<SubStats>>, Vec<arbitro_client::subscription::CallbackHandle>) {
    let mut stats = Vec::new();
    let mut tasks = Vec::new();

    println!(
        "Subscribing: {} clients × {} subs = {} total (policy={policy:?})",
        NUM_CLIENTS,
        SUBS_PER_CLIENT,
        NUM_CLIENTS * SUBS_PER_CLIENT
    );

    for c_idx in 0..NUM_CLIENTS {
        let client = Client::connect(addr).await.unwrap();
        let ccfg = ConsumerConfig::new(format!("c{}", c_idx).as_bytes(), stream_name)
            .deliver_mode(DeliverMode::Fanout)
            .deliver_policy(policy)
            .ack_policy(AckPolicy::None)
            .build()
            .unwrap();
        let consumer = client.create_consumer(&ccfg).await.unwrap();

        for s_idx in 0..SUBS_PER_CLIENT {
            let (filter, expected) = match s_idx % 4 {
                0 => ("iot.1.status", MSG_PER_TYPE),
                1 => ("iot.*.status", MSG_PER_TYPE * 2),
                2 => (">", MSG_PER_TYPE * 3),
                _ => ("ignore.me", 0),
            };

            let sub_stat = Arc::new(SubStats {
                count: AtomicU64::new(0),
                id: format!("C{}-S{}", c_idx, s_idx),
                expected,
                filter: filter.to_string(),
            });
            stats.push(sub_stat.clone());

            let sc = sub_stat.clone();
            let handle = consumer
                .subscribe_callback(Some(filter.as_bytes()), move |_msg| {
                    sc.count.fetch_add(1, Relaxed);
                })
                .await
                .unwrap();
            tasks.push(handle);
        }
    }

    (stats, tasks)
}

/// Publish all three subject groups. Shared between both scenarios so the
/// message mix is identical.
async fn publish_all(client: &Client, stream: &[u8], payload: &[u8]) {
    let batch_size = 1000;
    publish_burst(client, stream, b"iot.1.status", payload, batch_size).await;
    publish_burst(client, stream, b"iot.2.status", payload, batch_size).await;
    publish_burst(client, stream, b"other.logs", payload, batch_size).await;
}

async fn publish_burst(
    client: &Client,
    stream: &[u8],
    subject: &[u8],
    payload: &[u8],
    batch: usize,
) {
    for _ in 0..(MSG_PER_TYPE / batch as u64) {
        let mut entries = Vec::with_capacity(batch);
        for _ in 0..batch {
            entries.push((subject, payload));
        }
        client.publish_batch(stream, &entries).await.unwrap();
    }
}

/// Print a results table and return the number of failures.
fn report(stats: &[Arc<SubStats>], elapsed: Duration, label: &str) -> usize {
    println!("\n[{label}] Results:");
    println!("+----------+--------------------+----------------+----------------+------------+");
    println!("| Sub ID   | Filter             | Received       | Expected       | Status     |");
    println!("+----------+--------------------+----------------+----------------+------------+");

    let mut total_received = 0u64;
    let mut failures = 0;

    for (i, s) in stats.iter().enumerate() {
        let count = s.count.load(Relaxed);
        total_received += count;
        let is_ok = count == s.expected;
        if !is_ok {
            failures += 1;
        }

        if i < 8 || i > stats.len() - 5 {
            let status = if is_ok { "OK" } else { "FAIL" };
            println!(
                "| {:<8} | {:<18} | {:<14} | {:<14} | {:<10} |",
                s.id, s.filter, count, s.expected, status
            );
        } else if i == 8 {
            println!(
                "| ...      | ...                | ...            | ...            | ...        |"
            );
        }
    }

    println!("+----------+--------------------+----------------+----------------+------------+");
    println!("[{label}] Total Logical Deliveries: {}", total_received);
    println!("[{label}] Total Failures: {}/{}", failures, stats.len());
    println!(
        "[{label}] Overall Throughput: {:.2} msg/s",
        total_received as f64 / elapsed.as_secs_f64()
    );
    println!("[{label}] Total Elapsed: {:?}", elapsed);

    failures
}
