//! Benchmark: in-memory publish (ingestion) throughput.
//!
//! Measures publish throughput scaling:
//!   - publish_single: 1 msg per RTT, across N connections
//!   - publish_batch:  1K msgs per RTT, across N connections
//!
//! Each connection publishes to its own stream → different shards → real parallelism.
//! Concurrency levels: 1, 4, 8.

use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion, Throughput, BenchmarkId};
use tokio::runtime::Runtime;

use arbitro_client::Client;
use arbitro_proto::config::StreamConfig;
use arbitro_server::{ArbitroServer, Config};

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

    let server = ArbitroServer::new(config);
    tokio::spawn(async move { let _ = server.run().await; });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect_with_timeout(addr, Duration::from_secs(5))
        .await
        .expect("client must connect")
}

const MSGS_PER_CLIENT: u32 = 1_000;
const BATCH_SIZE: usize = 1_000;
const CONCURRENCY: &[usize] = &[1, 4, 8];

// ── Benchmarks ───────────────────────────────────────────────────

fn bench_publish(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let addr = rt.block_on(start_server());

    // Create 8 streams — each client publishes to its own stream
    let stream_names: Vec<Vec<u8>> = (0..8)
        .map(|i| format!("ingest_{i}").into_bytes())
        .collect();

    let setup_client = rt.block_on(connect(&addr));
    rt.block_on(async {
        for name in &stream_names {
            setup_client.create_stream(&StreamConfig::new(name, b">").build())
                .await.expect("create stream");
        }
    });

    // ── 1. Single publish (1 msg per RTT) ───────────────────────
    {
        let mut group = c.benchmark_group("publish_single");
        group.measurement_time(Duration::from_secs(5));
        group.sample_size(20);

        for &n_clients in CONCURRENCY {
            let total_msgs = MSGS_PER_CLIENT as u64 * n_clients as u64;
            group.throughput(Throughput::Elements(total_msgs));

            let clients: Vec<Client> = rt.block_on(async {
                let mut v = Vec::with_capacity(n_clients);
                for _ in 0..n_clients { v.push(connect(&addr).await); }
                v
            });

            group.bench_with_input(
                BenchmarkId::new(format!("{n_clients}conn_{n_clients}stream"), total_msgs),
                &n_clients,
                |b, _| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let elapsed = rt.block_on(async {
                                let start = Instant::now();
                                let mut handles = Vec::with_capacity(n_clients);
                                for (i, client) in clients.iter().enumerate() {
                                    let c = client.clone();
                                    let stream = stream_names[i % stream_names.len()].clone();
                                    handles.push(tokio::spawn(async move {
                                        let payload = vec![0u8; 64];
                                        for _ in 0..MSGS_PER_CLIENT {
                                            c.publish(&stream, b"bench.msg", &payload)
                                                .await.expect("publish");
                                        }
                                    }));
                                }
                                for h in handles { h.await.unwrap(); }
                                start.elapsed()
                            });
                            total += elapsed;
                        }
                        total
                    });
                },
            );
        }
        group.finish();
    }

    // ── 2. Batch publish (1K msgs per RTT) ──────────────────────
    {
        let mut group = c.benchmark_group("publish_batch");
        group.measurement_time(Duration::from_secs(5));
        group.sample_size(20);

        for &n_clients in CONCURRENCY {
            let total_msgs = BATCH_SIZE as u64 * n_clients as u64;
            group.throughput(Throughput::Elements(total_msgs));

            let clients: Vec<Client> = rt.block_on(async {
                let mut v = Vec::with_capacity(n_clients);
                for _ in 0..n_clients { v.push(connect(&addr).await); }
                v
            });

            group.bench_with_input(
                BenchmarkId::new(format!("{n_clients}conn_{n_clients}stream"), total_msgs),
                &n_clients,
                |b, _| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let elapsed = rt.block_on(async {
                                let start = Instant::now();
                                let mut handles = Vec::with_capacity(n_clients);
                                for (i, client) in clients.iter().enumerate() {
                                    let c = client.clone();
                                    let stream = stream_names[i % stream_names.len()].clone();
                                    handles.push(tokio::spawn(async move {
                                        let payload = vec![0u8; 64];
                                        let entries: Vec<(&[u8], &[u8])> = (0..BATCH_SIZE)
                                            .map(|_| (b"bench.msg".as_slice(), payload.as_slice()))
                                            .collect();
                                        c.publish_batch(&stream, &entries)
                                            .await.expect("publish_batch");
                                    }));
                                }
                                for h in handles { h.await.unwrap(); }
                                start.elapsed()
                            });
                            total += elapsed;
                        }
                        total
                    });
                },
            );
        }
        group.finish();
    }
}

criterion_group!(benches, bench_publish);
criterion_main!(benches);
