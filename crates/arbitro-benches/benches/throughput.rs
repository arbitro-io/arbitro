//! Benchmark: end-to-end throughput.
//!
//! Three measurements per mode:
//!   1. Publish throughput — time to publish N messages (batch)
//!   2. Delivery throughput — publish backlog, then measure subscribe + receive only
//!   3. Full cycle — publish + subscribe + receive (+ ack) measured together
//!
//! Modes: fire_forget (AckPolicy::None) vs explicit_ack (AckPolicy::Explicit).
//! Single server instance. Streams recreated between iterations.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
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

    let config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(100)
        .write_buffer_cap(8192);

    let transport = Arc::new(TokioTransport::new(config.write_buffer_cap));
    let server = ArbitroServer::new(config, transport, None);

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

const CHUNK: usize = 50_000;

async fn publish_n(client: &Client, stream: &[u8], entries: &[(&[u8], &[u8])], n: u32) {
    let mut remaining = n as usize;
    while remaining > 0 {
        let batch_size = remaining.min(entries.len());
        client.publish_batch(stream, &entries[..batch_size]).await.expect("publish");
        remaining -= batch_size;
    }
}

async fn receive_n(sub: &mut arbitro_client::SubscriptionHandle, n: u32) {
    let mut received = 0u32;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while received < n {
        match tokio::time::timeout_at(deadline, sub.next()).await {
            Ok(Some(_)) => received += 1,
            _ => panic!("timeout after {received}/{n} msgs"),
        }
    }
}

async fn receive_and_ack_n(sub: &mut arbitro_client::SubscriptionHandle, n: u32) {
    let mut received = 0u32;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while received < n {
        match tokio::time::timeout_at(deadline, sub.next()).await {
            Ok(Some(msg)) => { msg.ack(); received += 1; }
            _ => panic!("timeout after {received}/{n} msgs"),
        }
    }
}

// ── Benchmarks ───────────────────────────────────────────────────

fn bench_e2e(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let addr = rt.block_on(start_server());
    let client = rt.block_on(connect(&addr));

    let payload = vec![0u8; 64];

    for &n in &[1_000u32, 1_000_000] {
        let chunk_size = (n as usize).min(CHUNK);
        let entries: Vec<(&[u8], &[u8])> = (0..chunk_size)
            .map(|_| (b"bench.msg".as_slice(), payload.as_slice()))
            .collect();

        let mtime = if n >= 1_000_000 { Duration::from_secs(30) } else { Duration::from_secs(5) };
        let samples = if n >= 1_000_000 { 10 } else { 100 };

        // ── 1. Publish throughput ──────────────────────────────────
        {
            let sname = format!("pub_{n}").into_bytes();
            let scfg = StreamConfig::new(&sname).build();

            let mut group = c.benchmark_group("publish");
            group.throughput(Throughput::Elements(n as u64));
            group.measurement_time(mtime);
            group.sample_size(samples);

            group.bench_function(format!("{n}msg_64B"), |b| {
                // Setup (OUTSIDE timing): fresh stream
                rt.block_on(async {
                    client.delete_stream(&sname).await.ok();
                    client.create_stream(&scfg).await.expect("create");
                });

                b.iter(|| {
                    rt.block_on(async {
                        publish_n(&client, &sname, &entries, n).await;
                    });
                });
            });
            group.finish();
            rt.block_on(client.delete_stream(&sname)).ok();
        }

        // ── 2. Delivery fire-forget (backlog → subscribe → measure receive) ──
        {
            let sname = format!("ff_d_{n}").into_bytes();
            let scfg = StreamConfig::new(&sname).build();
            let ccfg = ConsumerConfig::new(b"ff_c", &sname)
                .filter(b">")
                .ack_policy(AckPolicy::None)
                .build();

            let mut group = c.benchmark_group("deliver_fire_forget");
            group.throughput(Throughput::Elements(n as u64));
            group.measurement_time(mtime);
            group.sample_size(samples);

            group.bench_function(format!("{n}msg_64B"), |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        rt.block_on(async {
                            // Setup (NOT timed): fresh stream + publish backlog + consumer
                            client.delete_stream(&sname).await.ok();
                            client.create_stream(&scfg).await.expect("create");
                            publish_n(&client, &sname, &entries, n).await;
                            let consumer = client.create_consumer(&ccfg).await.expect("consumer");
                            let mut sub = consumer.subscribe(None).await.expect("subscribe");

                            // Timed: receive only
                            let start = Instant::now();
                            receive_n(&mut sub, n).await;
                            total += start.elapsed();
                        });
                    }
                    total
                });
            });
            group.finish();
            rt.block_on(client.delete_stream(&sname)).ok();
        }

        // ── 3. Delivery explicit ack (backlog → subscribe → measure receive + ack) ──
        {
            let sname = format!("ack_d_{n}").into_bytes();
            let scfg = StreamConfig::new(&sname).build();
            let max_inflight: u16 = if n >= 1_000_000 { 60_000 } else { 1000 };
            let ccfg = ConsumerConfig::new(b"ack_c", &sname)
                .filter(b">")
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(max_inflight)
                .ack_wait_ms(60_000)
                .build();

            let mut group = c.benchmark_group("deliver_explicit_ack");
            group.throughput(Throughput::Elements(n as u64));
            group.measurement_time(mtime);
            group.sample_size(samples);

            group.bench_function(format!("{n}msg_64B"), |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        rt.block_on(async {
                            // Setup (NOT timed)
                            client.delete_stream(&sname).await.ok();
                            client.create_stream(&scfg).await.expect("create");
                            publish_n(&client, &sname, &entries, n).await;
                            let consumer = client.create_consumer(&ccfg).await.expect("consumer");
                            let mut sub = consumer.subscribe(None).await.expect("subscribe");

                            // Timed: receive + ack
                            let start = Instant::now();
                            receive_and_ack_n(&mut sub, n).await;
                            total += start.elapsed();
                        });
                    }
                    total
                });
            });
            group.finish();
            rt.block_on(client.delete_stream(&sname)).ok();
        }

        // ── 4. Full cycle fire-forget (publish + receive measured together) ──
        {
            let sname = format!("ff_f_{n}").into_bytes();
            let scfg = StreamConfig::new(&sname).build();
            let ccfg = ConsumerConfig::new(b"ff_fc", &sname)
                .filter(b">")
                .ack_policy(AckPolicy::None)
                .build();

            let mut group = c.benchmark_group("full_cycle_fire_forget");
            group.throughput(Throughput::Elements(n as u64));
            group.measurement_time(mtime);
            group.sample_size(samples);

            group.bench_function(format!("{n}msg_64B"), |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        rt.block_on(async {
                            // Setup (NOT timed)
                            client.delete_stream(&sname).await.ok();
                            client.create_stream(&scfg).await.expect("create");
                            let consumer = client.create_consumer(&ccfg).await.expect("consumer");
                            let mut sub = consumer.subscribe(None).await.expect("subscribe");

                            // Timed: publish + receive
                            let start = Instant::now();
                            publish_n(&client, &sname, &entries, n).await;
                            receive_n(&mut sub, n).await;
                            total += start.elapsed();
                        });
                    }
                    total
                });
            });
            group.finish();
            rt.block_on(client.delete_stream(&sname)).ok();
        }

        // ── 5. Full cycle explicit ack (publish + receive + ack measured together) ──
        {
            let sname = format!("ack_f_{n}").into_bytes();
            let scfg = StreamConfig::new(&sname).build();
            let max_inflight: u16 = if n >= 1_000_000 { 60_000 } else { 1000 };
            let ccfg = ConsumerConfig::new(b"ack_fc", &sname)
                .filter(b">")
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(max_inflight)
                .ack_wait_ms(60_000)
                .build();

            let mut group = c.benchmark_group("full_cycle_explicit_ack");
            group.throughput(Throughput::Elements(n as u64));
            group.measurement_time(mtime);
            group.sample_size(samples);

            group.bench_function(format!("{n}msg_64B"), |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        rt.block_on(async {
                            // Setup (NOT timed)
                            client.delete_stream(&sname).await.ok();
                            client.create_stream(&scfg).await.expect("create");
                            let consumer = client.create_consumer(&ccfg).await.expect("consumer");
                            let mut sub = consumer.subscribe(None).await.expect("subscribe");

                            // Timed: publish + receive + ack
                            let start = Instant::now();
                            publish_n(&client, &sname, &entries, n).await;
                            receive_and_ack_n(&mut sub, n).await;
                            total += start.elapsed();
                        });
                    }
                    total
                });
            });
            group.finish();
            rt.block_on(client.delete_stream(&sname)).ok();
        }
    }
}

criterion_group!(benches, bench_e2e);
criterion_main!(benches);
