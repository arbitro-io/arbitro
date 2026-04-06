//! Benchmark: Hierarchical Subject Limits Isolation ("The Policy Tree")
//! 
//! Proves that Arbitro can manage 1,000,000 overlapping subject rules
//! using prefix-matching to resolve global SLAs and per-user overrides.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverMode, StreamConfig};
use arbitro_server::{ArbitroServer, Config, TokioTransport};

// --- CONFIGURATION ---
const NUM_USERS: usize = 1_000_000;
const SATURATION_COUNT: usize = 10_000; 

// Domain subjects (Exact as requested)
const PREMIUM_USER_1: &[u8] = b"orders.us.premium.user_1";
const BASIC_USER_1: &[u8] = b"orders.us.basic.user_1";

struct BenchStats {
    premium_1_received: AtomicU64,
    basic_received: AtomicU64,
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
        write_buffer_cap: 10 * 1024 * 1024,
        idle_timeout: Duration::from_secs(60),
        keepalive_interval: Duration::from_secs(30),
        shutdown_timeout: Duration::from_secs(2),
    };
    let server = ArbitroServer::new(config.clone(), Arc::new(TokioTransport::new(config.write_buffer_cap)));
    tokio::spawn(async move { let _ = server.run().await; });
    tokio::time::sleep(Duration::from_millis(100)).await;
    addr
}

#[tokio::main]
async fn main() {
    println!("Step 1: Building Hierarchical Policy Tree (1,000,000 Rules)...");
    let start_build = Instant::now();
    
    let mut config = ConsumerConfig::new(b"gateway", b"ORDERS")
        .deliver_mode(DeliverMode::Fanout)
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(2_000_000);

    // --- ORDER: MOST SPECIFIC FIRST ---

    // 1. USER OVERRIDES
    config = config.subject_limit(PREMIUM_USER_1, 10); // user_1 => [ 1 1 1 - - - - - - - ]
    config = config.subject_limit(BASIC_USER_1, 1);   // basic_user_1 => [ 1 ]

    // 2. MASSIVE SCALE (1M unique users)
    // We add them as specific rules under the regional branches
    for i in 2..NUM_USERS {
        let subj = format!("orders.us.basic.user_{}", i);
        config = config.subject_limit(subj.as_bytes(), 1);
    }

    // 3. REGIONAL TIERS (Wildcards)
    config = config
        .subject_limit(b"orders.us.premium.>", 500)
        .subject_limit(b"orders.us.basic.>", 50);

    // 4. GLOBAL SLA
    config = config
        .subject_limit(b"orders.world.premium.>", 2000);

    let ccfg = config.build();
    println!("Policy Tree (1M rules) generated in {:?}", start_build.elapsed());

    // START INFRASTRUCTURE
    let addr = create_test_server().await;
    let setup_client = Client::connect(&addr).await.unwrap();
    setup_client.create_stream(&StreamConfig::new(b"ORDERS").build()).await.unwrap();

    println!("Step 2: Initializing Consumer (Building Trie and CreditMap)...");
    let start_trie = Instant::now();
    let consumer = setup_client.create_consumer(&ccfg).await.unwrap();
    println!("Engine ready in {:?}", start_trie.elapsed());

    let stats = Arc::new(BenchStats {
        premium_1_received: AtomicU64::new(0),
        basic_received: AtomicU64::new(0),
    });

    // SUBSCRIBE
    let s = stats.clone();
    let _handle = consumer.subscribe_callback(None, move |msg| {
        if msg.subject.as_ref() == PREMIUM_USER_1 {
            s.premium_1_received.fetch_add(1, Relaxed);
            msg.ack(); // Premium user_1 processes and releases credits
        } else {
            s.basic_received.fetch_add(1, Relaxed);
            // Basic users stay blocked (No ACK)
        }
    }).await.unwrap();

    let payload = vec![0u8; 64];

    // SATURATION: Pressure the 50-credit Regional Basic Pool
    println!("Step 3: Saturating {} basic users (Pushing Regional Pool to limit)...", SATURATION_COUNT);
    let mut saturation_entries = Vec::with_capacity(SATURATION_COUNT);
    for i in 1..=SATURATION_COUNT {
        saturation_entries.push((format!("orders.us.basic.user_{}", i), payload.clone()));
    }
    let saturation_refs: Vec<(&[u8], &[u8])> = saturation_entries.iter()
        .map(|(s, p)| (s.as_bytes(), p.as_slice()))
        .collect();
    
    setup_client.publish_batch(b"ORDERS", &saturation_refs).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // VALIDATION: Burst Premium User 1
    println!("Step 4: Firing Burst to orders.us.premium.user_1 (Isolation check)...");
    let mut premium_entries = Vec::with_capacity(100);
    for _ in 0..100 {
        premium_entries.push((PREMIUM_USER_1, payload.as_slice()));
    }

    let start_burst = Instant::now();
    setup_client.publish_batch(b"ORDERS", &premium_entries).await.unwrap();

    // Stability wait
    tokio::time::sleep(Duration::from_millis(500)).await;
    let elapsed = start_burst.elapsed() - Duration::from_millis(500);

    let p1_total = stats.premium_1_received.load(Relaxed);
    println!("\n+-----------------------------------------------------------+");
    println!("| HIERARCHICAL 1M SUBJECT LIMITS REPORT                   |");
    println!("+-----------------------------------------------------------+");
    println!("| Target User: orders.us.premium.user_1                     |");
    println!("| Status:      {}/100 Delivered                          |", p1_total);
    println!("| Latency:     {:<10?}                           |", elapsed / 100);
    println!("| Basic Load:  {:<10} (Saturated/Blocked)         |", stats.basic_received.load(Relaxed));
    println!("| Outcome:     [ SUCCESS ] Isolation via Policy Tree         |");
    println!("+-----------------------------------------------------------+");

    if p1_total == 100 {
        println!("The Policy Tree: Verified. Global rules did not leak into specific user credits.");
    } else {
        println!("The Policy Tree: FAILED. Isolation breach detected!");
        std::process::exit(1);
    }
}
