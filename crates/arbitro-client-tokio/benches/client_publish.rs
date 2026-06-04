//! Client-side publish microbench.
//!
//! Measures ONLY the client path: encode → mpsc → write_socket.
//!
//! The "server" is a raw TCP drain that reads and discards everything —
//! no parsing, no RepOk, no store. This isolates the client hot path
//! from any server-side cost.
//!
//! Groups:
//!   publish_single  — fire-and-forget single PUB, 64B payload
//!   publish_batch   — fire-and-forget batch of 100 × 64B
//!
//! Each group runs at conn counts [1, 2, 4, 8, 16].
//! Each iter sends N messages (one per conn) and waits for all to be
//! accepted by the MPSC — does NOT wait for TCP ack or any server reply.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};

use arbitro_client_tokio::{Client, ClientConfig};

const PAYLOAD_LEN: usize = 64;
const BATCH_SIZE: usize = 100;
const CONCURRENCY_LEVELS: &[usize] = &[1, 2, 4, 8, 16];
const STREAM_ID: u32 = 1;

// ── drain server ─────────────────────────────────────────────────────────────

async fn start_drain_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64 * 1024];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            });
        }
    });
    addr
}

// ── client connect ────────────────────────────────────────────────────────────

async fn connect(addr: &str) -> Client {
    let cfg = ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    };
    Client::connect(cfg).await.expect("connect")
}

// ── runtime ───────────────────────────────────────────────────────────────────

fn build_rt() -> Runtime {
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap()
}

// ── fixture ───────────────────────────────────────────────────────────────────

struct Fixture {
    clients: Vec<Client>,
}

async fn fixture(conns: usize) -> Fixture {
    let addr = start_drain_server().await;
    let mut clients = Vec::with_capacity(conns);
    for _ in 0..conns {
        clients.push(connect(&addr).await);
    }
    Fixture { clients }
}

// ── bench: publish_single ─────────────────────────────────────────────────────

fn bench_publish_single(c: &mut Criterion) {
    let rt = build_rt();
    let mut group = c.benchmark_group("client_publish_single");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    for &conns in CONCURRENCY_LEVELS {
        let fix = Arc::new(rt.block_on(fixture(conns)));
        group.throughput(Throughput::Elements(conns as u64));
        group.bench_with_input(BenchmarkId::from_parameter(conns), &conns, |b, &conns| {
            let fix = Arc::clone(&fix);
            b.to_async(&rt).iter(|| {
                let fix = Arc::clone(&fix);
                async move {
                    let mut joins = Vec::with_capacity(conns);
                    for c in &fix.clients {
                        let client = c.clone();
                        joins.push(tokio::spawn(async move {
                            let payload = Bytes::from_static(&[0u8; PAYLOAD_LEN]);
                            let _ = client.publish(STREAM_ID, b"bench", payload);
                        }));
                    }
                    for j in joins {
                        let _ = j.await;
                    }
                }
            });
        });
    }
    group.finish();
}

// ── bench: publish_batch ──────────────────────────────────────────────────────

fn bench_publish_batch(c: &mut Criterion) {
    use arbitro_client_tokio::client::BatchEntry;

    let rt = build_rt();
    let mut group = c.benchmark_group("client_publish_batch");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    for &conns in CONCURRENCY_LEVELS {
        let fix = Arc::new(rt.block_on(fixture(conns)));
        group.throughput(Throughput::Elements((conns * BATCH_SIZE) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(conns), &conns, |b, &conns| {
            let fix = Arc::clone(&fix);
            b.to_async(&rt).iter(|| {
                let fix = Arc::clone(&fix);
                async move {
                    let mut joins = Vec::with_capacity(conns);
                    for c in &fix.clients {
                        let client = c.clone();
                        joins.push(tokio::spawn(async move {
                            let payload = Bytes::from_static(&[0u8; PAYLOAD_LEN]);
                            let entries: Vec<BatchEntry<'_>> = (0..BATCH_SIZE)
                                .map(|_| BatchEntry::new(b"bench", payload.clone()))
                                .collect();
                            let _ = client.publish_batch(STREAM_ID, &entries);
                        }));
                    }
                    for j in joins {
                        let _ = j.await;
                    }
                }
            });
        });
    }
    group.finish();
}

// ── bench: step 1 — encode only ──────────────────────────────────────────────

fn bench_step1(c: &mut Criterion) {
    use arbitro_proto::v2::ingress::pub_frame::PubFrame;

    let mut g = c.benchmark_group("step_1_encode");
    g.sample_size(200);
    g.measurement_time(Duration::from_secs(3));
    g.throughput(Throughput::Elements(1));

    let subject = b"bench";
    let payload = [0u8; PAYLOAD_LEN];
    let size = PubFrame::wire_size(subject.len(), 0, PAYLOAD_LEN);
    let mut buf = vec![0u8; size];
    let seq = std::sync::atomic::AtomicU64::new(1);

    g.bench_function("encode", |b| {
        b.iter(|| {
            PubFrame::encode_into(
                &mut buf,
                seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                STREAM_ID,
                0,
                0,
                subject,
                &[],
                &payload,
            );
        });
    });
    g.finish();
}

// ── bench: step 2 — encode + tokio mpsc send ─────────────────────────────────

fn bench_step2(c: &mut Criterion) {
    use arbitro_proto::v2::ingress::pub_frame::PubFrame;
    use tokio::sync::mpsc;

    let rt = build_rt();
    let mut g = c.benchmark_group("step_2_encode_mpsc1");
    g.sample_size(200);
    g.measurement_time(Duration::from_secs(3));
    g.throughput(Throughput::Elements(1));

    let subject = b"bench";
    let payload = [0u8; PAYLOAD_LEN];
    let seq = std::sync::atomic::AtomicU64::new(1);
    let size = PubFrame::wire_size(subject.len(), 0, PAYLOAD_LEN);
    let mut buf = vec![0u8; size];

    let (tx, mut rx) = mpsc::channel::<usize>(4096);
    rt.block_on(async {
        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(64);
            while rx.recv_many(&mut batch, 64).await > 0 {
                batch.clear();
            }
        });
    });

    g.bench_function("encode+mpsc1", |b| {
        b.to_async(&rt).iter(|| {
            PubFrame::encode_into(
                &mut buf,
                seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                STREAM_ID,
                0,
                0,
                subject,
                &[],
                &payload,
            );
            let ptr = buf.as_ptr() as usize;
            let tx = tx.clone();
            async move {
                let _ = tx.send(ptr).await;
            }
        });
    });
    g.finish();
}

// ── bench: step 3 — encode + mpsc1 + forwarder + mpsc2 ───────────────────────

fn bench_step3(c: &mut Criterion) {
    use arbitro_proto::v2::ingress::pub_frame::PubFrame;
    use tokio::sync::mpsc;

    let rt = build_rt();
    let mut g = c.benchmark_group("step_3_encode_mpsc2");
    g.sample_size(200);
    g.measurement_time(Duration::from_secs(3));
    g.throughput(Throughput::Elements(1));

    let subject = b"bench";
    let payload = [0u8; PAYLOAD_LEN];
    let seq = std::sync::atomic::AtomicU64::new(1);
    let size = PubFrame::wire_size(subject.len(), 0, PAYLOAD_LEN);
    let mut buf = vec![0u8; size];

    let (tx, mut rx) = mpsc::channel::<usize>(4096);
    let (fwd_tx, mut fwd_rx) = mpsc::channel::<usize>(1024);

    rt.block_on(async {
        tokio::spawn(async move {
            while let Some(p) = rx.recv().await {
                let _ = fwd_tx.send(p).await;
            }
        });
        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(64);
            while fwd_rx.recv_many(&mut batch, 64).await > 0 {
                batch.clear();
            }
        });
    });

    g.bench_function("encode+mpsc1+fwd+mpsc2", |b| {
        b.to_async(&rt).iter(|| {
            PubFrame::encode_into(
                &mut buf,
                seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                STREAM_ID,
                0,
                0,
                subject,
                &[],
                &payload,
            );
            let ptr = buf.as_ptr() as usize;
            let tx = tx.clone();
            async move {
                let _ = tx.send(ptr).await;
            }
        });
    });
    g.finish();
}

// ── bench: step 4 — encode + kit::MpscAsync try_send (pure sync) ─────────────

fn bench_step4_kit(c: &mut Criterion) {
    use arbitro_kit::route::MpscAsync;
    use arbitro_proto::v2::ingress::pub_frame::PubFrame;

    let rt = build_rt();
    let mut g = c.benchmark_group("step_4_kit_mpsc");
    g.sample_size(200);
    g.measurement_time(Duration::from_secs(3));
    g.throughput(Throughput::Elements(1));

    let subject = b"bench";
    let payload = [0u8; PAYLOAD_LEN];
    let seq = std::sync::atomic::AtomicU64::new(1);
    let size = PubFrame::wire_size(subject.len(), 0, PAYLOAD_LEN);
    let mut buf = vec![0u8; size];

    let (mut producers, mut consumer, _shutdown) = MpscAsync::<usize, 4096>::new(1);
    let producer = producers.remove(0);

    rt.block_on(async {
        tokio::spawn(async move { while consumer.recv_async().await.is_ok() {} });
    });

    // purely sync — no block_on, no async wrapper
    g.bench_function("encode+kit_mpsc", |b| {
        b.iter(|| {
            PubFrame::encode_into(
                &mut buf,
                seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                STREAM_ID,
                0,
                0,
                subject,
                &[],
                &payload,
            );
            let _ = producer.try_send(buf.as_ptr() as usize);
        });
    });
    g.finish();
}

// ── bench: step 5 — encode + tokio mpsc + write_all ──────────────────────────

fn bench_step5(c: &mut Criterion) {
    use arbitro_proto::v2::ingress::pub_frame::PubFrame;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;

    let rt = build_rt();
    let mut g = c.benchmark_group("step_5_full_path");
    g.sample_size(50);
    g.measurement_time(Duration::from_secs(5));
    g.throughput(Throughput::Elements(1));

    let subject = b"bench";
    let payload = [0u8; PAYLOAD_LEN];
    let seq = std::sync::atomic::AtomicU64::new(1);
    let size = PubFrame::wire_size(subject.len(), 0, PAYLOAD_LEN);
    let mut buf = vec![0u8; size];

    let (tx, mut rx) = mpsc::channel::<usize>(4096);

    let addr = rt.block_on(start_drain_server());
    rt.block_on(async {
        let stream = TcpStream::connect(&addr).await.unwrap();
        let (_, mut w) = stream.into_split();
        tokio::spawn(async move {
            let mut batch: Vec<usize> = Vec::with_capacity(64);
            loop {
                let n = rx.recv_many(&mut batch, 64).await;
                if n == 0 {
                    break;
                }
                for ptr in batch.drain(..) {
                    let slice = unsafe { std::slice::from_raw_parts(ptr as *const u8, size) };
                    if w.write_all(slice).await.is_err() {
                        return;
                    }
                }
            }
        });
    });

    g.bench_function("encode+mpsc+write", |b| {
        b.to_async(&rt).iter(|| {
            PubFrame::encode_into(
                &mut buf,
                seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                STREAM_ID,
                0,
                0,
                subject,
                &[],
                &payload,
            );
            let ptr = buf.as_ptr() as usize;
            let tx = tx.clone();
            async move {
                let _ = tx.send(ptr).await;
            }
        });
    });
    g.finish();
}

// ── bench: step 6 — encode + kit::MpscAsync + write_all (pure sync send) ─────

fn bench_step6(c: &mut Criterion) {
    use arbitro_kit::route::MpscAsync;
    use arbitro_proto::v2::ingress::pub_frame::PubFrame;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let rt = build_rt();
    let mut g = c.benchmark_group("step_6_kit_mpsc_write");
    g.sample_size(50);
    g.measurement_time(Duration::from_secs(5));
    g.throughput(Throughput::Elements(1));

    let subject = b"bench";
    let payload = [0u8; PAYLOAD_LEN];
    let seq = std::sync::atomic::AtomicU64::new(1);
    let size = PubFrame::wire_size(subject.len(), 0, PAYLOAD_LEN);
    let mut buf = vec![0u8; size];

    let (mut producers, mut consumer, _shutdown) = MpscAsync::<usize, 4096>::new(1);
    let producer = producers.remove(0);

    let addr = rt.block_on(start_drain_server());
    rt.block_on(async {
        let stream = TcpStream::connect(&addr).await.unwrap();
        let (_, mut w) = stream.into_split();
        tokio::spawn(async move {
            loop {
                let Ok(ptr) = consumer.recv_async().await else {
                    break;
                };
                let slice = unsafe { std::slice::from_raw_parts(ptr as *const u8, size) };
                if w.write_all(slice).await.is_err() {
                    break;
                }
                while let Some(ptr) = consumer.try_recv() {
                    let slice = unsafe { std::slice::from_raw_parts(ptr as *const u8, size) };
                    if w.write_all(slice).await.is_err() {
                        return;
                    }
                }
            }
        });
    });

    // purely sync — try_send never yields
    g.bench_function("encode+kit_mpsc+write", |b| {
        b.iter(|| {
            PubFrame::encode_into(
                &mut buf,
                seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                STREAM_ID,
                0,
                0,
                subject,
                &[],
                &payload,
            );
            let _ = producer.try_send(buf.as_ptr() as usize);
        });
    });
    g.finish();
}

// ── bench: step 7 — encode + tokio mpsc + write + tokio oneshot ack ──────────

fn bench_step7(c: &mut Criterion) {
    use arbitro_proto::v2::ingress::pub_frame::PubFrame;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::{mpsc, oneshot};

    let rt = build_rt();
    let mut g = c.benchmark_group("step_7_tokio_ack");
    g.sample_size(50);
    g.measurement_time(Duration::from_secs(5));
    g.throughput(Throughput::Elements(1));

    let subject = b"bench";
    let payload = [0u8; PAYLOAD_LEN];
    let seq = std::sync::atomic::AtomicU64::new(1);
    let size = PubFrame::wire_size(subject.len(), 0, PAYLOAD_LEN);
    let mut buf = vec![0u8; size];

    let (tx, mut rx) = mpsc::channel::<(usize, oneshot::Sender<()>)>(4096);

    let addr = rt.block_on(start_drain_server());
    rt.block_on(async {
        let stream = TcpStream::connect(&addr).await.unwrap();
        let (_, mut w) = stream.into_split();
        tokio::spawn(async move {
            while let Some((ptr, ack)) = rx.recv().await {
                let slice = unsafe { std::slice::from_raw_parts(ptr as *const u8, size) };
                if w.write_all(slice).await.is_err() {
                    return;
                }
                let _ = ack.send(());
            }
        });
    });

    g.bench_function("encode+tokio_mpsc+write+ack", |b| {
        b.to_async(&rt).iter(|| {
            PubFrame::encode_into(
                &mut buf,
                seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                STREAM_ID,
                0,
                0,
                subject,
                &[],
                &payload,
            );
            let ptr = buf.as_ptr() as usize;
            let tx = tx.clone();
            let (ack_tx, ack_rx) = oneshot::channel::<()>();
            async move {
                let _ = tx.send((ptr, ack_tx)).await;
                let _ = ack_rx.await;
            }
        });
    });
    g.finish();
}

// ── bench: step 8 — encode + kit mpsc + write + kit OneShotAsync ack ─────────

fn bench_step8(c: &mut Criterion) {
    use arbitro_kit::route::{MpscAsync, OneShotAsync, OneShotAsyncSender};
    use arbitro_proto::v2::ingress::pub_frame::PubFrame;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let rt = build_rt();
    let mut g = c.benchmark_group("step_8_kit_ack");
    g.sample_size(50);
    g.measurement_time(Duration::from_secs(5));
    g.throughput(Throughput::Elements(1));

    let subject = b"bench";
    let payload = [0u8; PAYLOAD_LEN];
    let seq = std::sync::atomic::AtomicU64::new(1);
    let size = PubFrame::wire_size(subject.len(), 0, PAYLOAD_LEN);
    let mut buf = vec![0u8; size];

    let (mut producers, mut consumer, _shutdown) =
        MpscAsync::<(usize, OneShotAsyncSender<()>), 4096>::new(1);
    let producer = producers.remove(0);

    let addr = rt.block_on(start_drain_server());
    rt.block_on(async {
        let stream = TcpStream::connect(&addr).await.unwrap();
        let (_, mut w) = stream.into_split();
        tokio::spawn(async move {
            loop {
                let Ok((ptr, ack)) = consumer.recv_async().await else {
                    break;
                };
                let slice = unsafe { std::slice::from_raw_parts(ptr as *const u8, size) };
                if w.write_all(slice).await.is_err() {
                    return;
                }
                ack.send(());
            }
        });
    });

    g.bench_function("encode+kit_mpsc+write+ack", |b| {
        b.to_async(&rt).iter(|| {
            PubFrame::encode_into(
                &mut buf,
                seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                STREAM_ID,
                0,
                0,
                subject,
                &[],
                &payload,
            );
            let ptr = buf.as_ptr() as usize;
            let (ack_tx, ack_rx) = OneShotAsync::<()>::new();
            // try_send is sync — call before the async block, no move needed
            let _ = producer.try_send((ptr, ack_tx));
            async move {
                let _ = ack_rx.recv_async().await;
            }
        });
    });
    g.finish();
}

// ── bench: step 9 — alloc-per-msg (old) vs Inline (new) ─────────────────────
//
// Directly measures the improvement from `WriteFrame::Inline`:
//
//   old_alloc : vec![0u8; 92] + encode + Mono(Bytes::from(buf)) + try_send
//   new_inline: [0u8; INLINE_CAP] on stack + encode + Inline + try_send
//
// Both send through the real kit MpscAsync ring to a drain writer task so
// the try_send path (slot write + wake-check) is identical.
fn bench_step9_inline_vs_alloc(c: &mut Criterion) {
    use std::sync::atomic::{AtomicU64, Ordering};

    use arbitro_kit::route::MpscAsync;
    use arbitro_proto::v2::ingress::pub_frame::PubFrame;
    use bytes::Bytes;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    use arbitro_client_tokio::transport_internal::{WriteFrame, INLINE_CAP, WRITE_QUEUE_CAP};

    let rt = build_rt();
    let mut g = c.benchmark_group("step_9_inline_vs_alloc");
    g.sample_size(200);
    g.measurement_time(Duration::from_secs(5));
    g.throughput(Throughput::Elements(1));

    let subject = b"bench";
    let payload = [0u8; PAYLOAD_LEN];
    let seq = AtomicU64::new(1);
    let size = PubFrame::wire_size(subject.len(), 0, PAYLOAD_LEN);

    let (mut producers, mut consumer, _shutdown) = MpscAsync::<WriteFrame, WRITE_QUEUE_CAP>::new(1);
    let producer = producers.remove(0);

    let addr = rt.block_on(start_drain_server());
    rt.block_on(async {
        let stream = TcpStream::connect(&addr).await.unwrap();
        let (_, mut w) = stream.into_split();
        tokio::spawn(async move {
            loop {
                let Ok(f) = consumer.recv_async().await else {
                    break;
                };
                if w.write_all(f.as_slice()).await.is_err() {
                    break;
                }
                while let Some(f) = consumer.try_recv() {
                    if w.write_all(f.as_slice()).await.is_err() {
                        return;
                    }
                }
            }
        });
    });

    // old path: one heap alloc per publish
    g.bench_function("old_alloc:  vec+encode+Mono+try_send", |b| {
        b.iter(|| {
            let mut buf = vec![0u8; size];
            PubFrame::encode_into(
                &mut buf,
                seq.fetch_add(1, Ordering::Relaxed),
                STREAM_ID,
                0,
                0,
                subject,
                &[],
                &payload,
            );
            let _ = producer.try_send(WriteFrame::Mono(Bytes::from(buf)));
        });
    });

    // new path: zero heap alloc — stack array copied into ring slot
    g.bench_function("new_inline: stack+encode+Inline+try_send", |b| {
        b.iter(|| {
            let mut data = [0u8; INLINE_CAP];
            PubFrame::encode_into(
                &mut data[..size],
                seq.fetch_add(1, Ordering::Relaxed),
                STREAM_ID,
                0,
                0,
                subject,
                &[],
                &payload,
            );
            let _ = producer.try_send(WriteFrame::Inline(data, size as u16));
        });
    });

    g.finish();
}

criterion_group!(
    benches,
    bench_publish_single,
    bench_publish_batch,
    bench_step1,
    bench_step2,
    bench_step3,
    bench_step4_kit,
    bench_step5,
    bench_step6,
    bench_step7,
    bench_step8,
    bench_step9_inline_vs_alloc,
);
criterion_main!(benches);
