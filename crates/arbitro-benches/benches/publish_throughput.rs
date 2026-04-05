//! Benchmark: end-to-end publish throughput (client → server → reply).
//!
//! Every group starts with a smoke test (small even number) that panics
//! if the publish didn't actually succeed. This guarantees we measure
//! real work, not a silent no-op.
//!
//! Max 1000 messages per iteration (bench safety rule).

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tokio::runtime::Runtime;

use arbitro_client::Client;
use arbitro_engine::EngineBuilder;
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
    let engine = EngineBuilder::new()
        .transport(transport.clone())
        .build();
    let server = ArbitroServer::new(config, engine, transport);

    tokio::spawn(async move {
        let _ = server.run().await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn connected_client(addr: &str) -> Client {
    let client = Client::connect_with_timeout(addr, Duration::from_secs(3))
        .await
        .expect("client must connect");
    client
        .create_stream(b"bench", 0, 0, 0)  // 0 = unlimited
        .await
        .expect("stream must be created");
    client
}

// ── Smoke tests ──────────────────────────────────────────────────

async fn smoke_single(client: &Client, n: u32) {
    for i in 0..n {
        let seq = client
            .publish(b"bench", b"bench.smoke", &i.to_le_bytes())
            .await
            .expect("smoke publish failed");
        assert!(seq >= 1, "smoke: seq must be >= 1, got {seq}");
    }
}

async fn smoke_batch(client: &Client, n: usize) {
    let payload = vec![0u8; 64];
    let entries: Vec<(&[u8], &[u8])> = (0..n)
        .map(|_| (b"bench.smoke" as &[u8], payload.as_slice()))
        .collect();
    let seq = client
        .publish_batch(b"bench", &entries)
        .await
        .expect("smoke batch failed");
    assert!(seq >= 1, "smoke batch: seq must be >= 1, got {seq}");
}

// ── Benchmarks ───────────────────────────────────────────────────

fn bench_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Start server + connect client once for all benches.
    let (_addr, client) = rt.block_on(async {
        let addr = start_server().await;
        let client = connected_client(&addr).await;
        (addr, client)
    });

    // Smoke tests — small even numbers to prove it works.
    rt.block_on(async {
        smoke_single(&client, 2).await;
        smoke_batch(&client, 4).await;
        smoke_single(&client, 10).await;
    });

    let payload_64 = vec![0u8; 64];
    let payload_1k = vec![0u8; 1024];

    let mut group = c.benchmark_group("e2e_publish");
    group.measurement_time(Duration::from_secs(5));

    // --- Single publish, 64B ---
    group.throughput(Throughput::Elements(1));
    group.bench_function("single_64B", |b| {
        b.iter(|| {
            rt.block_on(async {
                client
                    .publish(b"bench", b"bench.msg", &payload_64)
                    .await
                    .unwrap();
            });
        });
    });

    // --- Single publish, 1KB ---
    group.throughput(Throughput::Bytes(1024));
    group.bench_function("single_1KB", |b| {
        b.iter(|| {
            rt.block_on(async {
                client
                    .publish(b"bench", b"bench.msg", &payload_1k)
                    .await
                    .unwrap();
            });
        });
    });

    // --- Batch 10, 64B ---
    group.throughput(Throughput::Elements(10));
    group.bench_function("batch10_64B", |b| {
        let entries: Vec<(&[u8], &[u8])> = (0..10)
            .map(|_| (b"bench.msg" as &[u8], payload_64.as_slice()))
            .collect();
        b.iter(|| {
            rt.block_on(async {
                client
                    .publish_batch(b"bench", &entries)
                    .await
                    .unwrap();
            });
        });
    });

    // --- Batch 100, 64B ---
    group.throughput(Throughput::Elements(100));
    group.bench_function("batch100_64B", |b| {
        let entries: Vec<(&[u8], &[u8])> = (0..100)
            .map(|_| (b"bench.msg" as &[u8], payload_64.as_slice()))
            .collect();
        b.iter(|| {
            rt.block_on(async {
                client
                    .publish_batch(b"bench", &entries)
                    .await
                    .unwrap();
            });
        });
    });

    // --- Batch 1000, 64B (max per safety rule) ---
    group.throughput(Throughput::Elements(1000));
    group.bench_function("batch1000_64B", |b| {
        let entries: Vec<(&[u8], &[u8])> = (0..1000)
            .map(|_| (b"bench.msg" as &[u8], payload_64.as_slice()))
            .collect();
        b.iter(|| {
            rt.block_on(async {
                client
                    .publish_batch(b"bench", &entries)
                    .await
                    .unwrap();
            });
        });
    });

    // --- Batch 100, 1KB ---
    group.throughput(Throughput::Bytes(100 * 1024));
    group.bench_function("batch100_1KB", |b| {
        let entries: Vec<(&[u8], &[u8])> = (0..100)
            .map(|_| (b"bench.msg" as &[u8], payload_1k.as_slice()))
            .collect();
        b.iter(|| {
            rt.block_on(async {
                client
                    .publish_batch(b"bench", &entries)
                    .await
                    .unwrap();
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
