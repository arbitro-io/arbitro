//! Benchmark: Multi-client High-Density Fanout & Filtering Stress Test.
//! 
//! Density:
//! - 3 Clients (Connections)
//! - 20 Subscriptions per Client (Total: 60)
//! - Mixed Filters: Direct, Wildcard, Global, and Non-Matching.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverMode, StreamConfig};
use arbitro_server::{ArbitroServer, Config, TokioTransport};

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
    let config = Config {
        listen_addr: addr.clone(),
        max_connections: 100,
        write_buffer_cap: 1024 * 1024,
        idle_timeout: Duration::from_secs(60),
        keepalive_interval: Duration::from_secs(30),
        shutdown_timeout: Duration::from_secs(2),
    };
    let write_cap = config.write_buffer_cap;
    let server = ArbitroServer::new(config, Arc::new(TokioTransport::new(write_cap)));
    tokio::spawn(async move { let _ = server.run().await; });
    tokio::time::sleep(Duration::from_millis(100)).await;
    addr
}

#[tokio::main]
async fn main() {
    let addr = create_test_server().await;
    let stream_name = b"high_density_fanout";

    let setup_client = Client::connect(&addr).await.unwrap();
    setup_client.create_stream(&StreamConfig::new(stream_name).build()).await.unwrap();

    let mut stats = Vec::new();
    let mut tasks = Vec::new();

    println!("Scaling: {} clients * {} subs = {} total subscriptions...", NUM_CLIENTS, SUBS_PER_CLIENT, NUM_CLIENTS * SUBS_PER_CLIENT);

    for c_idx in 0..NUM_CLIENTS {
        let client = Client::connect(&addr).await.unwrap();
        let ccfg = ConsumerConfig::new(format!("c{}", c_idx).as_bytes(), stream_name)
            .deliver_mode(DeliverMode::Fanout)
            .ack_policy(AckPolicy::None)
            .build();
        let consumer = client.create_consumer(&ccfg).await.unwrap();

        for s_idx in 0..SUBS_PER_CLIENT {
            // Interleave 4 types of filters per client
            let (filter, expected) = match s_idx % 4 {
                0 => ("iot.1.status", MSG_PER_TYPE),     // Group A: Direct (1M)
                1 => ("iot.*.status", MSG_PER_TYPE * 2), // Group B: Wildcard (2M)
                2 => (">", MSG_PER_TYPE * 3),            // Group C: Global (3M)
                _ => ("ignore.me", 0),                   // Group D: No Match (0)
            };

            let sub_stat = Arc::new(SubStats {
                count: AtomicU64::new(0),
                id: format!("C{}-S{}", c_idx, s_idx),
                expected,
                filter: filter.to_string(),
            });
            stats.push(sub_stat.clone());

            let sc = sub_stat.clone();
            let handle = consumer.subscribe_callback(Some(filter.as_bytes()), move |_msg| {
                sc.count.fetch_add(1, Relaxed);
            }).await.unwrap();
            tasks.push(handle);
        }
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    let start = Instant::now();
    let payload = vec![0u8; 64];
    let batch_size = 1000;

    println!("Bursting 3,000,000 messages across 3 types...");
    publish_burst(&setup_client, stream_name, b"iot.1.status", &payload, batch_size).await; // 1M
    publish_burst(&setup_client, stream_name, b"iot.2.status", &payload, batch_size).await; // 1M
    publish_burst(&setup_client, stream_name, b"other.logs", &payload, batch_size).await;   // 1M

    println!("Waiting for delivery (High Density Delivery)...");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let elapsed = start.elapsed();

    // REPORT
    println!("\n+----------+--------------------+----------------+----------------+------------+");
    println!("| Sub ID   | Filter             | Received       | Expected       | Status     |");
    println!("+----------+--------------------+----------------+----------------+------------+");
    let mut total_received = 0;
    let mut failures = 0;

    for (i, s) in stats.iter().enumerate() {
        let count = s.count.load(Relaxed);
        total_received += count;
        let is_ok = count == s.expected;
        if !is_ok { failures += 1; }

        // Only show first 8 and last 4 if list is long, but always check all
        if i < 8 || i > stats.len() - 5 {
            let status = if is_ok { "OK" } else { "FAIL" };
            println!("| {:<8} | {:<18} | {:<14} | {:<14} | {:<10} |", s.id, s.filter, count, s.expected, status);
        } else if i == 8 {
            println!("| ...      | ...                | ...            | ...            | ...        |");
        }
    }

    println!("+----------+--------------------+----------------+----------------+------------+");
    println!("Total Logical Deliveries: {}", total_received);
    println!("Total Failures: {}/{}", failures, stats.len());
    println!("Overall Throughput: {:.2} msg/s", total_received as f64 / elapsed.as_secs_f64());
    println!("Total Elapsed: {:?}", elapsed);

    if failures > 0 {
        std::process::exit(1);
    }
}

async fn publish_burst(client: &Client, stream: &[u8], subject: &[u8], payload: &[u8], batch: usize) {
    for _ in 0..(MSG_PER_TYPE / batch as u64) {
        let mut entries = Vec::with_capacity(batch);
        for _ in 0..batch { entries.push((subject, payload)); }
        client.publish_batch(stream, &entries).await.unwrap();
    }
}
