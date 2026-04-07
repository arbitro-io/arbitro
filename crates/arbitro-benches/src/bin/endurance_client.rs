//! Standalone Endurance Client — separated process for real stress testing.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use arbitro_client::Client;
use arbitro_proto::config::{StreamConfig, JournalKind};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("ARBITRO_ADDR").unwrap_or_else(|_| "127.0.0.1:9898".to_string());
    let duration_secs = std::env::var("ARBITRO_DURATION").ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let concurrency = std::env::var("ARBITRO_CONCURRENCY").ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    
    let duration = Duration::from_secs(duration_secs);
    let stream_name = b"endurance_disk";
    
    println!("Connecting to {} (duration: {}s, concurrency: {})", addr, duration_secs, concurrency);
    
    let client_master = Client::connect(&addr).await?;
    
    // Create stream (ignore error if exists)
    let scfg = StreamConfig::new(stream_name)
        .journal_kind(JournalKind::Tolerant)
        .build();
    let _ = client_master.create_stream(&scfg).await;

    let total_msgs = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();
    let batch_size = 100;
    let batch_delay_ms = 10;

    let mut handles = Vec::new();

    for i in 0..concurrency {
        let addr_clone = addr.clone();
        let total_msgs_clone = total_msgs.clone();
        let stream_ref = stream_name.to_vec();
        
        let handle = tokio::spawn(async move {
            let client = Client::connect(&addr_clone).await.expect("connect");
            let payload = vec![0u8; 64];
            let entries: Vec<(&[u8], &[u8])> = (0..batch_size)
                .map(|_| (b"endurance.disk".as_slice(), payload.as_slice()))
                .collect();
            
            while start_time.elapsed() < duration {
                if client.publish_batch(&stream_ref, &entries).await.is_ok() {
                    total_msgs_clone.fetch_add(batch_size as u64, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(batch_delay_ms)).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });
        handles.push(handle);
    }

    // Monitoring
    while start_time.elapsed() < duration {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let count = total_msgs.load(Ordering::Relaxed);
        println!("[{}] Local Msgs: {}", i32::from(concurrency), count);
    }

    for h in handles {
        let _ = h.await;
    }

    println!("Client Finished. Total Msgs: {}", total_msgs.load(Ordering::Relaxed));
    Ok(())
}
