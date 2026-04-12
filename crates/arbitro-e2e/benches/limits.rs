//! E2E Benchmark: Subject Limits & Isolation
//!
//! Validates how Arbitro enforces subject-level isolation through the full 
//! networking and protocol stack.
//! 
//! Constraints: Only uses `arbitro_server` and `arbitro_client`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, StreamConfig};
use arbitro_server::{ArbitroServer, Config, TokioTransport};

// --- INFRASTRUCTURE ---

fn portpicker() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn start_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");

    let config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(100)
        .write_buffer_cap(1024 * 1024)
        .idle_timeout(Duration::from_secs(60))
        .keepalive_interval(Duration::from_secs(30))
        .shutdown_timeout(Duration::from_secs(2));

    let transport = Arc::new(TokioTransport::new(config.write_buffer_cap));
    let server = ArbitroServer::new(config, transport, None);

    tokio::spawn(async move { let _ = server.run().await; });
    tokio::time::sleep(Duration::from_millis(100)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect(addr).await.expect("client must connect")
}

// --- BENCHMARKS ---

fn bench_limits_e2e(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let addr = rt.block_on(start_server());
    let client = rt.block_on(connect(&addr));
    let stream_name = b"LIMITS_E2E";

    // 1. Setup Stream
    rt.block_on(async {
        client.create_stream(&StreamConfig::new(stream_name, b">").build()).await.unwrap();
    });

    // 2. Setup Hierarchical Policies
    let c_name = b"isolation_tester";
    let c_cfg = ConsumerConfig::new(c_name, stream_name)
        .filter(b">")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(10000)
        .max_subject_inflight(b"orders.premium.>", 10)
        .max_subject_inflight(b"orders.basic.>", 1)
        .build()
        .unwrap();

    let mut group = c.benchmark_group("E2E Subject Isolation");
    let payload = vec![0u8; 64];

    group.bench_function("VIP Latency under basic load", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                rt.block_on(async {
                    // Start Consumer
                    let consumer = client.create_consumer(&c_cfg).await.unwrap();
                    let mut sub = consumer.subscribe(None).await.unwrap();

                    // Step 1: Saturate Basic Pool (Isolated subjects blocked)
                    let mut subjects = Vec::with_capacity(100);
                    for i in 0..100 {
                        subjects.push(format!("orders.basic.user_{}", i));
                    }
                    
                    let basic_entries: Vec<(&[u8], &[u8])> = subjects.iter()
                        .map(|s| (s.as_bytes(), payload.as_slice()))
                        .collect();
                    
                    client.publish_batch(stream_name, &basic_entries).await.unwrap();

                    // Drain the 100 basic messages (but don't ACK them to keep credits occupied)
                    for _ in 0..100 {
                        let _msg = sub.next().await.unwrap();
                    }

                    // Step 2: TIMED - VIP message delivery
                    let start = Instant::now();
                    client.publish(stream_name, b"orders.premium.vip_1", &payload).await.unwrap();
                    let vip_msg = sub.next().await.expect("VIP message should be delivered");
                    total += start.elapsed();
                    
                    // Cleanup for next iteration
                    vip_msg.ack();
                    consumer.delete().await.ok();
                });
            }
            total
        });
    });

    group.finish();
}

criterion_group!(benches, bench_limits_e2e);
criterion_main!(benches);
