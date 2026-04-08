//! Endurance Benchmark: 1-minute sustained burst on DISK.
//! TARGET: Strictly 10,000 messages per second in TolerantStore.
//! INTEGRATED PER-PROCESS TELEMETRY (CPU, RAM, MSGS/SEC).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::fs;
use tempfile::tempdir;

const TEST_DURATION: Duration = Duration::from_secs(60);
const CONCURRENCY: usize = 1; // Single client is enough for 10k/s precision
const BATCH_SIZE: usize = 100;
const REPORT_INTERVAL: Duration = Duration::from_secs(5);
const BATCH_DELAY_MS: u64 = 10; // 100 msg / 10ms = 10,000 msg/s

use arbitro_client::Client;
use arbitro_proto::config::{StreamConfig, JournalKind};
use arbitro_server::{ArbitroServer, Config, TokioTransport};

fn portpicker() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn create_server(addr: &str, data_dir: String) -> ArbitroServer {
    let config = Config::default()
        .listen_addr(addr)
        .max_connections(100)
        .data_dir(data_dir)
        .write_buffer_cap(1024 * 1024);
    
    let transport = Arc::new(TokioTransport::new(config.write_buffer_cap));
    ArbitroServer::new(config, transport, None)
}

struct Stats {
    utime: u64,
    stime: u64,
    rss_kb: u64,
}

fn get_process_stats() -> Stats {
    let stat = fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let parts: Vec<&str> = stat.split_whitespace().collect();
    
    let utime = parts.get(13).and_then(|&s| s.parse().ok()).unwrap_or(0);
    let stime = parts.get(14).and_then(|&s| s.parse().ok()).unwrap_or(0);

    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let rss_kb = status.lines()
        .find(|line| line.starts_with("VmRSS:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    Stats { utime, stime, rss_kb }
}

#[tokio::main]
async fn main() {
    // Stage 0: Prep disk
    let dir = tempdir().expect("temp dir");
    let data_path = dir.path().to_str().expect("valid path").to_string();
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    
    println!("\n--- ARBITRO ENDURANCE: 1-MINUTE PERSISTENT TEST ---");
    println!("Target: 10,000 Msgs/sec (TolerantStore)");
    println!("Data Dir: {}", data_path);
    println!("Telemetry: Inner-Process Radar (CPU/RAM/Throughput)");
    
    let server = create_server(&addr, data_path).await;
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move { let _ = server.run_with_shutdown(stop_rx).await; });
    
    tokio::time::sleep(Duration::from_millis(300)).await;
    let client_master = Client::connect(&addr).await.expect("master connect");
    let stream_name = b"endurance_disk";
    
    // Create TOLERANT stream
    let scfg = StreamConfig::new(stream_name, b">")
        .journal_kind(JournalKind::Tolerant)
        .build();
    client_master.create_stream(&scfg).await.expect("create stream");

    let total_msgs = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();
    
    // --- Single publisher (Rate-limited to 10k/s) ---
    let addr_clone = addr.clone();
    let total_msgs_clone = total_msgs.clone();
    let stream_ref = stream_name.to_vec();
    
    tokio::spawn(async move {
        let client = Client::connect(&addr_clone).await.expect("burst connect");
        let payload = vec![0u8; 64];
        let entries: Vec<(&[u8], &[u8])> = (0..BATCH_SIZE)
            .map(|_| (b"endurance.disk".as_slice(), payload.as_slice()))
            .collect();
        
        while start_time.elapsed() < TEST_DURATION {
            if client.publish_batch(&stream_ref, &entries).await.is_ok() {
                total_msgs_clone.fetch_add(BATCH_SIZE as u64, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(BATCH_DELAY_MS)).await;
            } else {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });

    // --- Telemetry Loop ---
    let mut last_msgs = 0;
    let mut last_stats = get_process_stats();
    let clk_tck = 100.0;

    while start_time.elapsed() < TEST_DURATION {
        tokio::time::sleep(REPORT_INTERVAL).await;
        
        let now_msgs = total_msgs.load(Ordering::Relaxed);
        let now_stats = get_process_stats();
        let elapsed = REPORT_INTERVAL.as_secs_f64();

        let delta_ticks = (now_stats.utime + now_stats.stime).saturating_sub(last_stats.utime + last_stats.stime);
        let cpu_pct = (delta_ticks as f64 / clk_tck) / elapsed * 100.0;
        
        let diff_msgs = now_msgs - last_msgs;
        let rate = diff_msgs as f64 / elapsed;
        
        println!("[{:.0}s] Throughput: {:.2} msg/s | CPU: {:.1}% | RAM: {:.1} MB", 
            start_time.elapsed().as_secs_f64(), 
            rate,
            cpu_pct,
            now_stats.rss_kb as f64 / 1024.0
        );
        
        last_msgs = now_msgs;
        last_stats = now_stats;
    }

    let final_msgs = total_msgs.load(Ordering::Relaxed);
    let final_elapsed = start_time.elapsed().as_secs_f64();
    
    println!("\n--- ENDURANCE SUMMARY (DISK) ---");
    println!("Total Duration: {:.2}s", final_elapsed);
    println!("Total Messages: {}", final_msgs);
    println!("Avg Throughput: {:.2} msg/s", final_msgs as f64 / final_elapsed);
    
    let _ = stop_tx.send(true);
    tokio::time::sleep(Duration::from_millis(500)).await;
}
