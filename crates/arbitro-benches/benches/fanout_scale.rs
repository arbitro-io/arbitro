//! Benchmark: Specialized Fanout Throughput & Efficiency.
//!
//! Features:
//! - 3 independent connections (clients).
//! - 3 subscriptions per client (Total: 9).
//! - Callback-style delivery loops.
//! - Per-subscriber throughput measurement.
//! - Verifies SubjectTrie filtering (some subs have specific filters).

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverMode, StreamConfig};
use arbitro_server::{ArbitroServer, Config, TokioTransport};

// ── Configuration ────────────────────────────────────────────────

const NUM_CLIENTS: usize = 3;
const SUBS_PER_CLIENT: usize = 3;
const MSG_COUNT: u64 = 1_000_000;

struct SubStats {
    count: AtomicU64,
    id: String,
}

// ── Infrastructure ───────────────────────────────────────────────

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
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    addr
}

#[tokio::main]
async fn main() {
    let addr = create_test_server().await;
    let stream_name = b"fanout_bench";

    // Initial Setup Client (Management Path)
    let setup_client = Client::connect(&addr).await.unwrap();
    setup_client
        .create_stream(&StreamConfig::new(stream_name).build())
        .await
        .unwrap();

    let mut stats = Vec::new();
    let mut tasks = Vec::new();

    println!(
        "Setting up {} clients with {} subscriptions each...",
        NUM_CLIENTS, SUBS_PER_CLIENT
    );

    for c_idx in 0..NUM_CLIENTS {
        let client = Client::connect(&addr).await.unwrap();

        let ccfg = ConsumerConfig::new(format!("c{}", c_idx).as_bytes(), stream_name)
            .deliver_mode(DeliverMode::Fanout)
            .ack_policy(AckPolicy::None)
            .build();

        let consumer = client.create_consumer(&ccfg).await.unwrap();

        for s_idx in 0..SUBS_PER_CLIENT {
            let filter = match c_idx {
                0 => "iot.*.status".to_string(), // Wildcard
                1 => "iot.1.status".to_string(), // Specific
                _ => ">".to_string(),            // All
            };

            let sub_stat = Arc::new(SubStats {
                count: AtomicU64::new(0),
                id: format!("C{}-S{}", c_idx, s_idx),
            });
            stats.push(sub_stat.clone());

            // Use the new callback API
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

    // PUBLISH (Shared Subject matching ALL filters)
    let publish_subject = b"iot.1.status";
    println!(
        "Publishing {} messages to '{}'...",
        MSG_COUNT,
        String::from_utf8_lossy(publish_subject)
    );
    tokio::time::sleep(Duration::from_millis(200)).await;

    let start = Instant::now();
    let payload = vec![0u8; 64];
    let batch_size = 1000;

    for _ in 0..(MSG_COUNT / batch_size as u64) {
        let mut entries = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            entries.push((publish_subject.as_slice(), payload.as_slice()));
        }
        setup_client
            .publish_batch(stream_name, &entries)
            .await
            .unwrap();
    }

    // Wait for all messengers to finish
    println!("Waiting for delivery to all 9 subscriptions...");
    let timeout = Duration::from_secs(30);
    let wait_start = Instant::now();

    loop {
        let mut all_done = true;
        for s in &stats {
            if s.count.load(Relaxed) < MSG_COUNT {
                all_done = false;
                break;
            }
        }
        if all_done {
            break;
        }
        if wait_start.elapsed() > timeout {
            println!("TIMEOUT waiting for all subscriptions to finish!");
            for s in &stats {
                if s.count.load(Relaxed) < MSG_COUNT {
                    println!(
                        "  - {} only received {}/{}",
                        s.id,
                        s.count.load(Relaxed),
                        MSG_COUNT
                    );
                }
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let elapsed = start.elapsed();

    // REPORT
    println!("\n+----------+----------------+----------------+");
    println!("| Sub ID   | Total Received | Throughput     |");
    println!("+----------+----------------+----------------+");
    for s in &stats {
        let count = s.count.load(Relaxed);
        let thrpt = count as f64 / elapsed.as_secs_f64();
        println!("| {:<8} | {:<14} | {:<13.2} msg/s |", s.id, count, thrpt);
    }
    println!("+----------+----------------+----------------+");
    println!("Total Elapsed: {:?}", elapsed);
    println!(
        "Global Effective Throughput (All Subs Combined): {:.2} msg/s",
        (MSG_COUNT * (NUM_CLIENTS * SUBS_PER_CLIENT) as u64) as f64 / elapsed.as_secs_f64()
    );

    // Cleanup
    drop(tasks);
}
