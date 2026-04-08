//! Standalone Endurance Client — separated process for real stress testing.
//! Supports Dual Roles: High-speed ingestion and aggressive ACK scavenging.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::env;
use arbitro_client::Client;
use arbitro_proto::config::{StreamConfig, JournalKind, ConsumerConfig, AckPolicy, DeliverPolicy, DeliverMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = env::var("ARBITRO_ADDR").unwrap_or_else(|_| "127.0.0.1:9898".to_string());
    let role = env::var("ARBITRO_ROLE").unwrap_or_else(|_| "producer".to_string());
    let group_name = env::var("ARBITRO_GROUP").unwrap_or_else(|_| "default_group".to_string());
    let mode_str = env::var("ARBITRO_MODE").unwrap_or_else(|_| "fanout".to_string());
    
    let deliver_mode = if mode_str == "queue" { DeliverMode::Queue } else { DeliverMode::Fanout };
    let duration_secs = env::var("ARBITRO_DURATION").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
    let concurrency = env::var("ARBITRO_CONCURRENCY").ok().and_then(|v| v.parse().ok()).unwrap_or(4);
    let kind_str = env::var("ARBITRO_KIND").unwrap_or_else(|_| "memory".to_string());
    
    let kind = if kind_str == "tolerant" { JournalKind::Tolerant } else { JournalKind::Memory };
    let duration = Duration::from_secs(duration_secs);
    let stream_str = env::var("ARBITRO_STREAM").unwrap_or_else(|_| "endurance_test".to_string());
    let stream_name = stream_str.as_bytes();
    
    println!("--- ENDURANCE CLIENT started ---");
    println!("Role: {}, Group: {}, Mode: {:?}, Addr: {}, Duration: {}s, Kind: {:?}", role, group_name, deliver_mode, addr, duration_secs, kind);

    let client_master = Client::connect(&addr).await?;
    let filter = format!("{}.>", std::str::from_utf8(stream_name).unwrap());
    let _ = client_master.create_stream(&StreamConfig::new(stream_name, filter.as_bytes()).journal_kind(kind).build()).await;

    let total_published = Arc::new(AtomicU64::new(0));
    let total_acked = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();

    let mut handles = Vec::new();

    // --- PRODUCER LOGIC ---
    if role == "producer" || role == "dual" {
        for _ in 0..concurrency {
            let addr_clone = addr.clone();
            let counter = total_published.clone();
            let stream = stream_name.to_vec();
            let start = start_time;
            
            handles.push(tokio::spawn(async move {
                let client = Client::connect(&addr_clone).await.expect("prod connect");
                let payload = vec![0u8; 128];
                let batch_size = 500;
                let entries: Vec<(&[u8], &[u8])> = (0..batch_size)
                    .map(|_| (b"endurance.stress".as_slice(), payload.as_slice()))
                    .collect();
                
                while start.elapsed() < duration {
                    if client.publish_batch(&stream, &entries).await.is_ok() {
                        counter.fetch_add(batch_size as u64, Ordering::Relaxed);
                    } else {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }));
        }
    }

    // --- CONSUMER LOGIC (SCAVENGER) ---
    if role == "consumer" || role == "dual" {
        for _ in 0..concurrency {
            let addr_clone = addr.clone();
            let counter = total_acked.clone();
            let stream = stream_name.to_vec();
            let start = start_time;
            let group = group_name.clone();
            let d_mode = deliver_mode;
            
            handles.push(tokio::spawn(async move {
                let client = Client::connect(&addr_clone).await.expect("cons connect");
                let ccfg = ConsumerConfig::new(group.as_bytes(), &stream)
                    .ack_policy(AckPolicy::Explicit)
                    .deliver_policy(DeliverPolicy::All)
                    .deliver_mode(d_mode)
                    .max_inflight(5000)
                    .build();
                
                let consumer = client.create_consumer(&ccfg).await.expect("create consumer");
                let mut sub = consumer.subscribe(None).await.expect("subscribe");
                
                while start.elapsed() < duration {
                    if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(500), sub.next()).await {
                        // Aggressive ACK
                        msg.ack();
                        counter.fetch_add(1, Ordering::Relaxed);
                    } else if start.elapsed() >= duration {
                        break;
                    }
                }
            }));
        }
    }

    // Monitoring
    let mut last_report = Instant::now();
    let mut last_pub = 0;
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

    for h in handles { let _ = h.await; }
    println!("Final Report | Total Published: {}, Total Acked: {}", total_published.load(Ordering::Relaxed), total_acked.load(Ordering::Relaxed));
    Ok(())
}
