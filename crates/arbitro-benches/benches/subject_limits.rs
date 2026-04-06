//! Benchmark: Native Subject Limits and Deferred Overhead
//!
//! Evaluates the engine's overhead when a consumer hits a `SubjectLimit`.
//! Under per-subject limits, messages beyond the limit are pushed to a `deferred` queue.
//! As acks come in, the engine scans and removes items using `VecDeque::remove(i)`.
//! This benchmark scales the backlog to expose potential O(N^2) penalties in
//! chronic redelivery/throttling scenarios, explicitly using the broker's native configs.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::runtime::Runtime;

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, StreamConfig};
use arbitro_server::{ArbitroServer, Config, TokioTransport};

// ── Infrastructure ───────────────────────────────────────────────

fn portpicker() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn start_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");

    let config = Config {
        listen_addr: addr.clone(),
        max_connections: 100,
        write_buffer_cap: 8192,
        idle_timeout: Duration::from_secs(60),
        keepalive_interval: Duration::from_secs(30),
        shutdown_timeout: Duration::from_secs(2),
    };

    let transport = Arc::new(TokioTransport::new(config.write_buffer_cap));
    let server = ArbitroServer::new(config, transport);

    tokio::spawn(async move {
        let _ = server.run().await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect_with_timeout(addr, Duration::from_secs(30))
        .await
        .expect("client must connect")
}

// ── Benchmarks ───────────────────────────────────────────────────

fn bench_subject_limits(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let addr = rt.block_on(start_server());
    let client = rt.block_on(connect(&addr));
    let payload = vec![0u8; 64];

    // We scale the number of messages injected into a throttle limit.
    // This fills the deferred queue, heavily penalizing VecDeque::remove during acks.
    for &msg_count in &[100, 1000, 5000] {
        let sname = b"bench_limits".to_vec();
        let scfg = StreamConfig::new(&sname).build();

        // 3 distinct rules to validate independent credit slots per subject.
        let ccfg = ConsumerConfig::new(b"c1", &sname)
            .filter(b">")
            .ack_policy(AckPolicy::Explicit)
            .max_inflight(10000)         // Huge global credit
            .subject_limit(b"orders.>",  5)   // Rule A: tight
            .subject_limit(b"events.>",  10)  // Rule B: medium
            .subject_limit(b"alerts.>",  20)  // Rule C: loose
            .build();

        let mut group = c.benchmark_group("subject_limits_deferred");
        group.throughput(Throughput::Elements(msg_count as u64));
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(5));

        group.bench_function(BenchmarkId::new("throttle_overhead", msg_count), |b| {
            // Mix of all three subject families to hit each credit slot.
            let mut entries = Vec::with_capacity(msg_count as usize);
            for i in 0..msg_count {
                let subj: &[u8] = match i % 3 {
                    0 => b"orders.new",
                    1 => b"events.click",
                    _ => b"alerts.sev1",
                };
                entries.push((subj, payload.as_slice()));
            }

            b.iter_custom(|iters| {
                let mut total_time = Duration::ZERO;
                for _ in 0..iters {
                    rt.block_on(async {
                        // Setup (Untimed)
                        client.delete_stream(&sname).await.ok();
                        client.create_stream(&scfg).await.expect("create");
                        let consumer = client
                            .create_consumer(&ccfg)
                            .await
                            .expect("create consumer");
                        let mut sub = consumer.subscribe(None).await.expect("subscribe");

                        // --- STRICT LIMIT VALIDATION per rule ---
                        // Rule A: orders.> limit = 5
                        let a_entries: Vec<_> = (0..30).map(|_| (b"orders.new".as_slice(), payload.as_slice())).collect();
                        client.publish_batch(&sname, &a_entries).await.expect("pub orders");
                        for _ in 0..5 {
                            let _m = tokio::time::timeout(Duration::from_secs(1), sub.next()).await
                                .expect("orders msg timeout").expect("stream closed");
                        }
                        let over_a = tokio::time::timeout(Duration::from_millis(50), sub.next()).await;
                        assert!(over_a.is_err(), "LIMIT BREACH orders.> (limit=5): received 6th message without ack!");

                        // Rule B: events.> limit = 10
                        let b_entries: Vec<_> = (0..30).map(|_| (b"events.click".as_slice(), payload.as_slice())).collect();
                        client.publish_batch(&sname, &b_entries).await.expect("pub events");
                        for _ in 0..10 {
                            let _m = tokio::time::timeout(Duration::from_secs(1), sub.next()).await
                                .expect("events msg timeout").expect("stream closed");
                        }
                        let over_b = tokio::time::timeout(Duration::from_millis(50), sub.next()).await;
                        assert!(over_b.is_err(), "LIMIT BREACH events.> (limit=10): received 11th message without ack!");

                        // Rule C: alerts.> limit = 20
                        let c_entries: Vec<_> = (0..50).map(|_| (b"alerts.sev1".as_slice(), payload.as_slice())).collect();
                        client.publish_batch(&sname, &c_entries).await.expect("pub alerts");
                        for _ in 0..20 {
                            let _m = tokio::time::timeout(Duration::from_secs(1), sub.next()).await
                                .expect("alerts msg timeout").expect("stream closed");
                        }
                        let over_c = tokio::time::timeout(Duration::from_millis(50), sub.next()).await;
                        assert!(over_c.is_err(), "LIMIT BREACH alerts.> (limit=20): received 21st message without ack!");

                        // Purge the queue by dropping sub/consumer
                        drop(sub);
                        client.delete_consumer(&sname, consumer.id()).await.ok();
                        
                        // Recreate cleanly for the timed execution
                        let consumer = client.create_consumer(&ccfg).await.expect("create consumer");
                        let mut sub = consumer.subscribe(None).await.expect("subscribe");

                        // --- Timed Execution ---
                        let start = Instant::now();

                        client
                            .publish_batch(&sname, &entries)
                            .await
                            .expect("publish payload");

                        let mut received = 0;
                        while received < msg_count {
                            if let Ok(Some(msg)) =
                                tokio::time::timeout(Duration::from_secs(5), sub.next()).await
                            {
                                msg.ack();
                                received += 1;
                            } else {
                                break;
                            }
                        }

                        total_time += start.elapsed();
                    });
                }
                total_time
            });

            // Teardown
            rt.block_on(async {
                client.delete_stream(&sname).await.ok();
            });
        });

        group.finish();
    }
}

criterion_group!(benches, bench_subject_limits);
criterion_main!(benches);
