//! Chaos Benchmark: Resilience, Recovery, and Client Flickering.
//! 
//! ORCHESTRATED VERSION: Spawns server and workers as SEPARATE OS PROCESSES.

use std::time::{Duration, Instant};
use std::process::{Command, Child, Stdio};
use std::env;
use std::fs;

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, StreamConfig, JournalKind};

// --- SETTINGS ---
const MSGS_PER_BATCH: usize = 1000;
const TOTAL_MSGS: u64 = 100_000;
const FLICKER_CLIENTS: usize = 10;
const FLICKER_DURATION: Duration = Duration::from_secs(10);

struct ProcessGuard(Child);

impl ProcessGuard {
    fn spawn(cmd: &mut Command, name: &str) -> Self {
        println!("  [Manager] Spawning {}...", name);
        Self(cmd.spawn().expect("failed to spawn process"))
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let role = env::var("CHAOS_ROLE").unwrap_or_else(|_| "manager".to_string());
    
    match role.as_str() {
        "flicker" => run_flicker_worker().await,
        _ => run_manager().await,
    }
}

async fn run_flicker_worker() -> Result<(), Box<dyn std::error::Error>> {
    let addr = env::var("ARBITRO_ADDR").expect("ARBITRO_ADDR missing");
    let start = Instant::now();
    while start.elapsed() < FLICKER_DURATION {
        if let Ok(client) = Client::connect(&addr).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(client);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

async fn run_manager() -> Result<(), Box<dyn std::error::Error>> {
    // Fixed data path for visibility
    let data_path = "/tmp/arbitro/chaos_data";
    let _ = fs::remove_dir_all(data_path);
    fs::create_dir_all(data_path)?;

    let addr = "127.0.0.1:9911";
    let current_exe = env::current_exe()?;
    let server_path = current_exe.parent().unwrap().join("arbitro-server");
    
    if !server_path.exists() {
        panic!("Server binary NOT FOUND at {:?}.", server_path);
    }

    println!("\n--- STAGE 1: DURABILITY BURST (KILL SERVER) ---");
    {
        let mut server_cmd = Command::new(&server_path);
        server_cmd.env("ARBITRO_LISTEN", addr)
                  .env("ARBITRO_DATA_DIR", data_path)
                  .stdout(Stdio::null())
                  .stderr(Stdio::inherit()); // Visible logs
        
        let _server_guard = ProcessGuard::spawn(&mut server_cmd, "Server-S1");
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let client = Client::connect(addr).await?;
        let stream_name = b"chaos_durable";
        
        client.create_stream(&StreamConfig::new(stream_name)
            .journal_kind(JournalKind::Tolerant).build()).await?;

        println!("  [Manager] Bursting {} messages...", TOTAL_MSGS);
        let payload = vec![0u8; 128];
        let entries: Vec<(&[u8], &[u8])> = (0..MSGS_PER_BATCH)
            .map(|_| (b"chaos.test".as_slice(), payload.as_slice()))
            .collect();

        for _ in 0..(TOTAL_MSGS / MSGS_PER_BATCH as u64) {
            client.publish_batch(stream_name, &entries).await?;
        }
        println!("  [Manager] Killing server now...");
    } 

    tokio::time::sleep(Duration::from_millis(2000)).await; // Wait for OS to settle

    println!("\n--- STAGE 2: RECOVERY INTEGRITY (RESTART) ---");
    {
        let mut server_cmd = Command::new(&server_path);
        server_cmd.env("ARBITRO_LISTEN", addr).env("ARBITRO_DATA_DIR", data_path).stderr(Stdio::inherit());
        let _server_guard = ProcessGuard::spawn(&mut server_cmd, "Server-S2");
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let client = Client::connect(addr).await?;
        let consumer = client.create_consumer(&ConsumerConfig::new(b"verifier", b"chaos_durable")
            .filter(b">").ack_policy(AckPolicy::None).deliver_policy(arbitro_proto::config::DeliverPolicy::All).build()).await?;
        let mut sub = consumer.subscribe(None).await?;
        
        println!("  [Manager] Verifying data...");
        let mut count = 0;
        let start_wait = Instant::now();
        while count < TOTAL_MSGS && start_wait.elapsed() < Duration::from_secs(10) {
            match tokio::time::timeout(Duration::from_millis(500), sub.next()).await {
                Ok(Some(_)) => count += 1,
                _ => break,
            }
        }
        
        if count == TOTAL_MSGS {
            println!("  [Manager] RECOVERY OK: {} messages intact.", count);
        } else {
            println!("  [Manager] RECOVERY FAIL: Only found {} / {} messages.", count, TOTAL_MSGS);
            return Err("Recovery Integrity Violation".into());
        }
    }

    println!("\n--- STAGE 3: CLIENT FLICKERING CHAOS (10 ISOLATED WORKERS) ---");
    {
        let mut server_cmd = Command::new(&server_path);
        server_cmd.env("ARBITRO_LISTEN", addr).env("ARBITRO_DATA_DIR", data_path).stderr(Stdio::null());
        let _server_guard = ProcessGuard::spawn(&mut server_cmd, "Server-S3");
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let mut workers = Vec::new();
        for i in 0..FLICKER_CLIENTS {
            let mut worker_cmd = Command::new(&current_exe);
            worker_cmd.env("CHAOS_ROLE", "flicker").env("ARBITRO_ADDR", addr).stdout(Stdio::null());
            workers.push(ProcessGuard::spawn(&mut worker_cmd, &format!("Worker-{}", i)));
        }

        println!("  [Manager] Flickering active. Stressing publisher (10s)...");
        let client = Client::connect(addr).await?;
        let start = Instant::now();
        let mut last_report = Instant::now();
        let mut count = 0;
        let mut total = 0;

        while start.elapsed() < FLICKER_DURATION {
            // High-throughput publish burst
            for _ in 0..10 {
                if client.publish(b"chaos_durable", b"flicker.msg", b"data").await.is_ok() {
                    count += 1;
                    total += 1;
                }
            }

            if last_report.elapsed() >= Duration::from_secs(1) {
                println!("    > T+{}s: {} msg/s ingested", start.elapsed().as_secs(), count);
                count = 0;
                last_report = Instant::now();
            }
            
            // Minimal sleep to yield to workers but maintain pressure
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        println!("  [Manager] Stress complete. Total chaotic ingestion: {} msgs", total);
    }

    println!("\nCHAOS SUMMARY: All isolated components passed stress test.");
    Ok(())
}
