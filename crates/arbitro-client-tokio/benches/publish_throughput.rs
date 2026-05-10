//! Pure-tokio publish throughput benches against an in-process broker.
//!
//! Four groups, each parameterised by connection count `1, 2, 4, 8, 16`:
//!
//! * `publish_single`       — fire-and-forget single PUB per iter.
//! * `publish_batch`        — fire-and-forget batch of 100 messages per iter.
//! * `publish_single_sync`  — request/reply single PUB per iter.
//! * `publish_batch_sync`   — request/reply batch of 100 messages per iter.
//!
//! Each iteration fans the workload across `N` pre-spawned client tasks; the
//! reported throughput is the aggregate (criterion divides by elapsed time).
//!
//! The bench only compiles unless explicitly invoked — see `.agent/rules/testing.md`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use tokio::runtime::{Builder, Runtime};

use arbitro_client_tokio::client::BatchEntry;
use arbitro_client_tokio::{Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};

// ── helpers ──────────────────────────────────────────────────────────

const PAYLOAD_LEN:        usize = 64;
const BATCH_MESSAGES:     usize = 100;
const CONCURRENCY_LEVELS: &[usize] = &[1, 2, 4, 8, 16];

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

async fn start_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let cfg = Config::default()
        .listen_addr(addr.clone())
        .max_connections(256);
    let server = ArbitroServer::new(cfg);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    let cfg = ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    };
    Client::connect(cfg).await.expect("client connect")
}

async fn ensure_stream(client: &Client) -> u32 {
    // CreateStream — replicas=1, everything else default. Idempotent enough
    // for our purposes: if it already exists, reuse via `get_stream`.
    let resp = client
        .create_stream(b"bench", b">", 0, 0, 0, 1, 0, 0, 0)
        .await;
    let body = match resp {
        Ok(b) => b,
        Err(_) => client
            .get_stream(b"bench")
            .await
            .expect("get_stream fallback"),
    };
    assert!(body.len() >= 8, "RepOk must carry ref_seq");
    u64::from_le_bytes(body[..8].try_into().unwrap()) as u32
}

/// Build a multi-thread tokio runtime sized to the host.
fn build_runtime() -> Runtime {
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// One-shot fixture: server + N pre-connected clients + the wire stream id.
struct Fixture {
    _addr:     String,
    stream_id: u32,
    clients:   Vec<Client>,
}

async fn fixture(conns: usize) -> Fixture {
    let addr = start_server().await;
    let admin = connect(&addr).await;
    let stream_id = ensure_stream(&admin).await;
    drop(admin);

    let mut clients = Vec::with_capacity(conns);
    for _ in 0..conns {
        clients.push(connect(&addr).await);
    }
    Fixture { _addr: addr, stream_id, clients }
}

/// Spread `total` iterations across `clients.len()` tasks, each looping its
/// share, calling `op` per message. Returns once every task is done.
async fn fanout<F, Fut>(clients: &[Client], total: usize, op: F)
where
    F: Fn(Client, usize) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let n = clients.len();
    let per = total / n;
    let rem = total % n;
    let mut joins = Vec::with_capacity(n);
    for (i, c) in clients.iter().enumerate() {
        let share = per + if i < rem { 1 } else { 0 };
        let client = c.clone();
        let op = op.clone();
        joins.push(tokio::spawn(async move {
            for k in 0..share {
                op(client.clone(), k).await;
            }
        }));
    }
    for j in joins {
        let _ = j.await;
    }
}

// ── bench groups ─────────────────────────────────────────────────────

fn bench_publish_single(c: &mut Criterion) {
    let rt = build_runtime();
    let mut group = c.benchmark_group("publish_single");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(1));

    for &conns in CONCURRENCY_LEVELS {
        let fix = Arc::new(rt.block_on(fixture(conns)));
        let stream_id = fix.stream_id;
        group.bench_with_input(
            BenchmarkId::from_parameter(conns),
            &conns,
            |b, &conns| {
                let fix = Arc::clone(&fix);
                b.iter(|| {
                    let fix = Arc::clone(&fix);
                    rt.block_on(async move {
                        fanout(&fix.clients, conns, move |client, _| {
                            let payload = Bytes::from_static(&[0u8; PAYLOAD_LEN]);
                            async move {
                                let _ = client.publish(stream_id, b"bench", payload);
                            }
                        })
                        .await;
                    });
                });
            },
        );
    }
    group.finish();
}

fn bench_publish_batch(c: &mut Criterion) {
    let rt = build_runtime();
    let mut group = c.benchmark_group("publish_batch");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(BATCH_MESSAGES as u64));

    for &conns in CONCURRENCY_LEVELS {
        let fix = Arc::new(rt.block_on(fixture(conns)));
        let stream_id = fix.stream_id;
        group.bench_with_input(
            BenchmarkId::from_parameter(conns),
            &conns,
            |b, &conns| {
                let fix = Arc::clone(&fix);
                b.iter(|| {
                    let fix = Arc::clone(&fix);
                    rt.block_on(async move {
                        fanout(&fix.clients, conns, move |client, _| async move {
                            let payload = Bytes::from_static(&[0u8; PAYLOAD_LEN]);
                            let entries: Vec<BatchEntry<'_>> = (0..BATCH_MESSAGES)
                                .map(|_| BatchEntry::new(b"bench", payload.clone()))
                                .collect();
                            let _ = client.publish_batch(stream_id, &entries);
                        })
                        .await;
                    });
                });
            },
        );
    }
    group.finish();
}

fn bench_publish_single_sync(c: &mut Criterion) {
    let rt = build_runtime();
    let mut group = c.benchmark_group("publish_single_sync");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(1));

    for &conns in CONCURRENCY_LEVELS {
        let fix = Arc::new(rt.block_on(fixture(conns)));
        let stream_id = fix.stream_id;
        group.bench_with_input(
            BenchmarkId::from_parameter(conns),
            &conns,
            |b, &conns| {
                let fix = Arc::clone(&fix);
                b.iter(|| {
                    let fix = Arc::clone(&fix);
                    rt.block_on(async move {
                        fanout(&fix.clients, conns, move |client, _| async move {
                            let payload = Bytes::from_static(&[0u8; PAYLOAD_LEN]);
                            let _ = client
                                .publish_sync(stream_id, b"bench", payload)
                                .await;
                        })
                        .await;
                    });
                });
            },
        );
    }
    group.finish();
}

fn bench_publish_batch_sync(c: &mut Criterion) {
    let rt = build_runtime();
    let mut group = c.benchmark_group("publish_batch_sync");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(BATCH_MESSAGES as u64));

    for &conns in CONCURRENCY_LEVELS {
        let fix = Arc::new(rt.block_on(fixture(conns)));
        let stream_id = fix.stream_id;
        group.bench_with_input(
            BenchmarkId::from_parameter(conns),
            &conns,
            |b, &conns| {
                let fix = Arc::clone(&fix);
                b.iter(|| {
                    let fix = Arc::clone(&fix);
                    rt.block_on(async move {
                        fanout(&fix.clients, conns, move |client, _| async move {
                            let payload = Bytes::from_static(&[0u8; PAYLOAD_LEN]);
                            let entries: Vec<BatchEntry<'_>> = (0..BATCH_MESSAGES)
                                .map(|_| BatchEntry::new(b"bench", payload.clone()))
                                .collect();
                            let _ = client
                                .publish_batch_sync(stream_id, &entries)
                                .await;
                        })
                        .await;
                    });
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_publish_single,
    bench_publish_batch,
    bench_publish_single_sync,
    bench_publish_batch_sync,
);
criterion_main!(benches);
