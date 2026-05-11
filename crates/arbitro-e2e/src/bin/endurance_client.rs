//! Standalone Endurance Client — separated process for real stress testing.
//! Supports Dual Roles: High-speed ingestion and aggressive ACK scavenging.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::env;

use arbitro_client_tokio::{BatchEntry, Client, ClientConfig};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = env::var("ARBITRO_ADDR").unwrap_or_else(|_| "127.0.0.1:9898".to_string());
    let role = env::var("ARBITRO_ROLE").unwrap_or_else(|_| "producer".to_string());
    let group_name = env::var("ARBITRO_GROUP").unwrap_or_else(|_| "default_group".to_string());
    let mode_str = env::var("ARBITRO_MODE").unwrap_or_else(|_| "fanout".to_string());

    // deliver_mode: 0=Push/Fanout, 1=Queue
    let deliver_mode: u8 = if mode_str == "queue" { 1 } else { 0 };
    let duration_secs: u64 = env::var("ARBITRO_DURATION").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
    let concurrency: usize = env::var("ARBITRO_CONCURRENCY").ok().and_then(|v| v.parse().ok()).unwrap_or(4);
    let kind_str = env::var("ARBITRO_KIND").unwrap_or_else(|_| "memory".to_string());

    // journal_kind: 0=Memory, 2=Tolerant
    let journal_kind: u8 = if kind_str == "tolerant" { 2 } else { 0 };
    let duration = Duration::from_secs(duration_secs);
    let stream_str = env::var("ARBITRO_STREAM").unwrap_or_else(|_| "endurance_test".to_string());
    let stream_name = stream_str.as_bytes().to_vec();

    println!("--- ENDURANCE CLIENT started ---");
    println!(
        "Role: {}, Group: {}, Mode: {}, Addr: {}, Duration: {}s, Kind: {}",
        role, group_name, mode_str, addr, duration_secs,
        if journal_kind == 2 { "tolerant" } else { "memory" }
    );

    let client_master = Client::connect(ClientConfig { addr: addr.clone(), ..ClientConfig::default() }).await?;
    let filter = format!("{}.>", std::str::from_utf8(&stream_name).unwrap());
    let stream_id = match client_master
        .create_stream(&stream_name, filter.as_bytes(), 0, 0, 0, 1, journal_kind, 0, 0)
        .await
    {
        Ok(bytes) if bytes.len() >= 8 => {
            u64::from_le_bytes(bytes[..8].try_into().unwrap()) as u32
        }
        _ => {
            eprintln!("Warning: create_stream failed or returned unexpected response");
            0u32
        }
    };

    let total_published = Arc::new(AtomicU64::new(0));
    let total_acked = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();

    // Producer futures (run concurrently via join_all — producers only use
    // sync publish, so are Send and can also be spawned; using join_all for
    // consistency with consumers).
    let mut producer_futs = Vec::new();
    if role == "producer" || role == "dual" {
        for _ in 0..concurrency {
            let addr_clone = addr.clone();
            let counter = total_published.clone();
            let start = start_time;
            producer_futs.push(async move {
                let client = Client::connect(ClientConfig { addr: addr_clone, ..ClientConfig::default() })
                    .await
                    .expect("prod connect");
                let payload: Bytes = Bytes::from(vec![0u8; 128]);
                let batch_size = 500usize;
                let entries: Vec<BatchEntry<'_>> = (0..batch_size)
                    .map(|_| BatchEntry::new(b"endurance.stress", payload.clone()))
                    .collect();

                while start.elapsed() < duration {
                    loop {
                        match client.publish_batch(stream_id, &entries) {
                            Ok(()) => {
                                counter.fetch_add(batch_size as u64, Ordering::Relaxed);
                                break;
                            }
                            Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                                tokio::task::yield_now().await;
                            }
                            Err(_) => {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                break;
                            }
                        }
                    }
                }
            });
        }
    }

    // Consumer futures (use join_all because Client::create_consumer is async
    // and takes &self, making the future non-Send for tokio::spawn).
    let mut consumer_futs = Vec::new();
    if role == "consumer" || role == "dual" {
        for i in 0..concurrency {
            let addr_clone = addr.clone();
            let counter = total_acked.clone();
            let group = group_name.clone();
            let start = start_time;
            consumer_futs.push(async move {
                let client = Client::connect(ClientConfig { addr: addr_clone, ..ClientConfig::default() })
                    .await
                    .expect("cons connect");

                let consumer_name = format!("{}-{i}", group);
                let resp = client
                    .create_consumer(
                        stream_id,
                        consumer_name.as_bytes(),
                        group.as_bytes(),
                        b"",
                        5000,
                        1,           // ack_policy = Explicit
                        0,           // deliver_policy = All
                        deliver_mode,
                        30_000,
                        0,
                    )
                    .await
                    .expect("create consumer");
                let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
                let mut sub = client
                    .subscribe(stream_id, consumer_id, b"")
                    .await
                    .expect("subscribe");

                while start.elapsed() < duration {
                    match tokio::time::timeout(Duration::from_millis(500), sub.recv()).await {
                        Ok(Some(msg)) => {
                            msg.ack();
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            if start.elapsed() >= duration {
                                break;
                            }
                        }
                    }
                }
            });
        }
    }

    // Run producers and consumers concurrently; monitoring runs alongside.
    let monitor = {
        let total_published = total_published.clone();
        let total_acked = total_acked.clone();
        async move {
            let mut last_report = Instant::now();
            let mut last_pub = 0u64;
            while start_time.elapsed() < duration {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let p = total_published.load(Ordering::Relaxed);
                let a = total_acked.load(Ordering::Relaxed);
                let elapsed = last_report.elapsed().as_secs_f64();
                let rate = (p - last_pub) as f64 / elapsed;
                println!("  > Telemetry | Published: {} (avg {:.0} msg/s), Acked: {}", p, rate, a);
                last_pub = p;
                last_report = Instant::now();
            }
        }
    };

    tokio::join!(
        futures::future::join_all(producer_futs),
        futures::future::join_all(consumer_futs),
        monitor,
    );

    println!(
        "Final Report | Total Published: {}, Total Acked: {}",
        total_published.load(Ordering::Relaxed),
        total_acked.load(Ordering::Relaxed)
    );
    Ok(())
}
