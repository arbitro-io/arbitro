//! release_primitive — full-tokio publish→reply correlation bench.
//!
//! Every component runs inside a single multi-thread tokio runtime —
//! broker, writer, reader, and producers are all tokio tasks. No OS
//! threads, no `std::thread::spawn`, no cross-runtime `notify_one` from
//! a parked OS thread into a tokio task. This eliminates the asymmetric
//! wake noise of the previous version and makes the comparison between
//! reply primitives apples-to-apples.
//!
//! ```text
//! producer task ──► alloc seq + reply-slot pair
//!                ──► pending.lock().insert(seq, sender)        // FxHashMap
//!                ──► encode header (Bytes) + payload (Bytes)
//!                ──► mpsc.try_send + tokio::yield_now backoff   // matches inner::enqueue
//!                ──► rx.recv_async().await                      // ← what we measure
//!
//! writer task   ◄── kit::Mpsc<WriteFrame>::recv_async()         // tokio task park
//!                ──► OwnedWriteHalf write_vectored              // async iovec
//!
//! broker task   ──► reads request, replies with [seq u32 LE]
//!
//! reader task   ◄── async read_exact (4 B reply)
//!                ──► pending.lock().remove(&seq).map(|tx| tx.send(seq as u64))
//! ```
//!
//! Variants compared (the only thing that varies is the type stored in
//! `pending: FxHashMap<u32, _>`):
//!
//! - `tokio::sync::oneshot`     — baseline.
//! - `kit::OneShotAsync<u64>`   — production primitive.
//! - `kit::PipeAsync<u64>`      — sibling kit primitive (Arc-shared).

use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use rustc_hash::FxHashMap;
use tokio::io::{AsyncReadExt, AsyncWrite};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

use arbitro_kit::route::{
    MpscAsync, MpscAsyncConsumer, MpscAsyncProducer, OneShotAsync, OneShotAsyncSender,
};
use arbitro_kit::slot::PipeAsync;

// ─── Constants ────────────────────────────────────────────────────────────

const RING_CAP: usize = 4096;
/// Header layout: `[pad 12][seq u32 LE]` (16 B — same shape as prod envelope).
const HEADER_SIZE: usize = 16;
/// Representative payload size for a small publish.
const PAYLOAD_SIZE: usize = 128;
/// Reply: `[seq u32 LE]`.
const REPLY_SIZE: usize = 4;

/// Frame on the wire: header (Bytes) + payload (Bytes). Shipped as 2 iovecs
/// via `poll_write_vectored` — never coalesced in userspace, exactly like
/// `WriteFrame::PubSingle` in `arbitro-client::inner`.
type WriteFrame = (Bytes, Bytes);

// ─── Encode / decode ──────────────────────────────────────────────────────

#[inline]
fn encode_header(seq: u32) -> Bytes {
    let mut buf = vec![0u8; HEADER_SIZE];
    buf[12..16].copy_from_slice(&seq.to_le_bytes());
    Bytes::from(buf)
}

#[inline]
fn decode_seq_from_header(buf: &[u8]) -> u32 {
    u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]])
}

#[inline]
fn decode_seq_from_reply(buf: &[u8]) -> u32 {
    u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])
}

// ─── Broker (tokio task; one connection-handler task per accept) ─────────

async fn run_broker(listener: TcpListener) {
    loop {
        let (mut sock, _) = match listener.accept().await {
            Ok(p) => p,
            Err(_) => return,
        };
        let _ = sock.set_nodelay(true);
        tokio::spawn(async move {
            let mut req = vec![0u8; HEADER_SIZE + PAYLOAD_SIZE];
            loop {
                if sock.read_exact(&mut req).await.is_err() {
                    return;
                }
                let seq = decode_seq_from_header(&req);
                let reply = seq.to_le_bytes();
                use tokio::io::AsyncWriteExt;
                if sock.write_all(&reply).await.is_err() {
                    return;
                }
            }
        });
    }
}

// ─── Writer (tokio task; mpsc.recv_async → poll_write_vectored) ──────────
//
// `tokio::io::AsyncWriteExt` does not provide `write_all_vectored`; we
// drive `poll_write_vectored` via `poll_fn` so the iovec hot path stays
// zero-copy (no userspace coalescing of header+payload).

async fn write_all_vectored2(
    writer: &mut OwnedWriteHalf,
    header: &[u8],
    payload: &[u8],
) -> std::io::Result<()> {
    use std::future::poll_fn;
    use std::io::IoSlice;
    use tokio::io::AsyncWriteExt;

    let mut header_written = 0usize;
    while header_written < header.len() {
        let h = &header[header_written..];
        let bufs = [IoSlice::new(h), IoSlice::new(payload)];
        let n = poll_fn(|cx| Pin::new(&mut *writer).poll_write_vectored(cx, &bufs)).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "poll_write_vectored returned 0",
            ));
        }
        if n <= h.len() {
            header_written += n;
        } else {
            let payload_done = n - h.len();
            return writer.write_all(&payload[payload_done..]).await;
        }
    }
    writer.write_all(payload).await
}

async fn run_writer(
    mut consumer: MpscAsyncConsumer<WriteFrame, RING_CAP>,
    mut sock: OwnedWriteHalf,
) {
    while let Ok((header, payload)) = consumer.recv_async().await {
        if write_all_vectored2(&mut sock, &header, &payload).await.is_err() {
            return;
        }
    }
}

// ─── Reader (tokio task; async read_exact → release closure) ─────────────

async fn run_reader<F>(mut sock: OwnedReadHalf, mut release: F)
where
    F: FnMut(u32) + Send + 'static,
{
    let mut buf = [0u8; REPLY_SIZE];
    while sock.read_exact(&mut buf).await.is_ok() {
        let seq = decode_seq_from_reply(&buf);
        release(seq);
    }
}

// ─── Common harness ──────────────────────────────────────────────────────

/// Produce a reusable `Bytes` payload — Arc-shared across all in-flight
/// publishes (matches the production hot path: one allocation, every
/// `BatchEntry::payload` is a refcount bump).
fn shared_payload() -> Bytes {
    Bytes::from(vec![0u8; PAYLOAD_SIZE])
}

/// Production-shaped enqueue: `try_send` with `tokio::task::yield_now`
/// backoff. Same pattern as `inner::enqueue`'s 64-cycle absorption window.
async fn enqueue(
    prod: &Mutex<MpscAsyncProducer<WriteFrame, RING_CAP>>,
    mut frame: WriteFrame,
) {
    loop {
        let r = {
            let p = prod.lock().unwrap();
            p.try_send(frame)
        };
        match r {
            Ok(()) => return,
            Err(returned) => {
                frame = returned;
                tokio::task::yield_now().await;
            }
        }
    }
}

// ─── Variant runners ─────────────────────────────────────────────────────
//
// Each variant builds its own runtime (so the worker count matches the
// producer count fairly), spawns broker + writer + reader as tasks, runs
// the producers, and tears down. The runtime is dropped at the end of
// each variant so tasks don't bleed across runs.

fn build_runtime(producers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(producers.max(2))
        .enable_all()
        .build()
        .unwrap()
}

// ── Variant 1: tokio::sync::oneshot ──

fn bench_tokio_oneshot(producers: usize, per_producer: u64) -> Duration {
    let rt = build_runtime(producers);
    rt.block_on(async move {
        let (mut prods, cons, shutdown) = MpscAsync::<WriteFrame, RING_CAP>::new(producers);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let broker = tokio::spawn(run_broker(listener));
        let stream = TcpStream::connect(addr).await.unwrap();
        let _ = stream.set_nodelay(true);
        let (read_sock, write_sock) = stream.into_split();

        let pending: Arc<Mutex<FxHashMap<u32, tokio::sync::oneshot::Sender<u64>>>> =
            Arc::new(Mutex::new(FxHashMap::default()));
        let pending_r = pending.clone();
        let writer_h = tokio::spawn(run_writer(cons, write_sock));
        let reader_h = tokio::spawn(run_reader(read_sock, move |seq| {
            if let Some(tx) = pending_r.lock().unwrap().remove(&seq) {
                let _ = tx.send(seq as u64);
            }
        }));

        let payload = shared_payload();
        let seq_gen = Arc::new(AtomicU32::new(1));
        let start = Instant::now();
        let mut js = tokio::task::JoinSet::new();
        for _ in 0..producers {
            let prod = Arc::new(Mutex::new(prods.pop().unwrap()));
            let pending = pending.clone();
            let seq_gen = seq_gen.clone();
            let payload = payload.clone();
            js.spawn(async move {
                for _ in 0..per_producer {
                    let seq = seq_gen.fetch_add(1, Ordering::Relaxed);
                    let (tx, rx) = tokio::sync::oneshot::channel::<u64>();
                    pending.lock().unwrap().insert(seq, tx);
                    let header = encode_header(seq);
                    enqueue(&prod, (header, payload.clone())).await;
                    let _ = rx.await;
                }
            });
        }
        while js.join_next().await.is_some() {}
        let elapsed = start.elapsed();

        shutdown.signal();
        let _ = writer_h.await;
        broker.abort();
        reader_h.abort();
        let _ = broker.await;
        let _ = reader_h.await;
        elapsed
    })
}

// ── Variant 2: kit::OneShotAsync<u64> ──

fn bench_kit_oneshot_async(producers: usize, per_producer: u64) -> Duration {
    let rt = build_runtime(producers);
    rt.block_on(async move {
        let (mut prods, cons, shutdown) = MpscAsync::<WriteFrame, RING_CAP>::new(producers);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let broker = tokio::spawn(run_broker(listener));
        let stream = TcpStream::connect(addr).await.unwrap();
        let _ = stream.set_nodelay(true);
        let (read_sock, write_sock) = stream.into_split();

        let pending: Arc<Mutex<FxHashMap<u32, OneShotAsyncSender<u64>>>> =
            Arc::new(Mutex::new(FxHashMap::default()));
        let pending_r = pending.clone();
        let writer_h = tokio::spawn(run_writer(cons, write_sock));
        let reader_h = tokio::spawn(run_reader(read_sock, move |seq| {
            if let Some(tx) = pending_r.lock().unwrap().remove(&seq) {
                tx.send(seq as u64);
            }
        }));

        let payload = shared_payload();
        let seq_gen = Arc::new(AtomicU32::new(1));
        let start = Instant::now();
        let mut js = tokio::task::JoinSet::new();
        for _ in 0..producers {
            let prod = Arc::new(Mutex::new(prods.pop().unwrap()));
            let pending = pending.clone();
            let seq_gen = seq_gen.clone();
            let payload = payload.clone();
            js.spawn(async move {
                for _ in 0..per_producer {
                    let seq = seq_gen.fetch_add(1, Ordering::Relaxed);
                    let (tx, rx) = OneShotAsync::<u64>::new();
                    pending.lock().unwrap().insert(seq, tx);
                    let header = encode_header(seq);
                    enqueue(&prod, (header, payload.clone())).await;
                    let _ = rx.recv_async().await;
                }
            });
        }
        while js.join_next().await.is_some() {}
        let elapsed = start.elapsed();

        shutdown.signal();
        let _ = writer_h.await;
        broker.abort();
        reader_h.abort();
        let _ = broker.await;
        let _ = reader_h.await;
        elapsed
    })
}

// ── Variant 3: kit::PipeAsync<u64> (Arc-shared) ──

fn bench_kit_pipe_async(producers: usize, per_producer: u64) -> Duration {
    let rt = build_runtime(producers);
    rt.block_on(async move {
        let (mut prods, cons, shutdown) = MpscAsync::<WriteFrame, RING_CAP>::new(producers);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let broker = tokio::spawn(run_broker(listener));
        let stream = TcpStream::connect(addr).await.unwrap();
        let _ = stream.set_nodelay(true);
        let (read_sock, write_sock) = stream.into_split();

        let pending: Arc<Mutex<FxHashMap<u32, Arc<PipeAsync<u64>>>>> =
            Arc::new(Mutex::new(FxHashMap::default()));
        let pending_r = pending.clone();
        let writer_h = tokio::spawn(run_writer(cons, write_sock));
        let reader_h = tokio::spawn(run_reader(read_sock, move |seq| {
            if let Some(pipe) = pending_r.lock().unwrap().remove(&seq) {
                pipe.send(seq as u64);
            }
        }));

        let payload = shared_payload();
        let seq_gen = Arc::new(AtomicU32::new(1));
        let start = Instant::now();
        let mut js = tokio::task::JoinSet::new();
        for _ in 0..producers {
            let prod = Arc::new(Mutex::new(prods.pop().unwrap()));
            let pending = pending.clone();
            let seq_gen = seq_gen.clone();
            let payload = payload.clone();
            js.spawn(async move {
                for _ in 0..per_producer {
                    let seq = seq_gen.fetch_add(1, Ordering::Relaxed);
                    let pipe: Arc<PipeAsync<u64>> = Arc::new(PipeAsync::new());
                    pending.lock().unwrap().insert(seq, pipe.clone());
                    let header = encode_header(seq);
                    enqueue(&prod, (header, payload.clone())).await;
                    let _ = pipe.recv_async().await;
                }
            });
        }
        while js.join_next().await.is_some() {}
        let elapsed = start.elapsed();

        shutdown.signal();
        let _ = writer_h.await;
        broker.abort();
        reader_h.abort();
        let _ = broker.await;
        let _ = reader_h.await;
        elapsed
    })
}

// ─── Driver ──────────────────────────────────────────────────────────────

fn fmt_row(name: &str, producers: usize, per_p: u64, dur: Duration) {
    let total = (producers as u64) * per_p;
    let ns_per = dur.as_nanos() as f64 / total as f64;
    let mps = total as f64 / dur.as_secs_f64();
    println!(
        "  {name:25} | P={producers:>2} | {total:>8} acks | {:>9.2}ms | {ns_per:>8.0} ns/op | {mps:>11.0} ack/s",
        dur.as_secs_f64() * 1000.0,
    );
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
fn env_flag(key: &str) -> bool {
    matches!(std::env::var(key).as_deref(), Ok("1") | Ok("true") | Ok("yes"))
}

fn run_variant(name: &str, producers: usize, per_p: u64) -> Duration {
    match name {
        "tokio"   => bench_tokio_oneshot(producers, per_p),
        "oneshot" => bench_kit_oneshot_async(producers, per_p),
        "pipe"    => bench_kit_pipe_async(producers, per_p),
        other     => panic!("unknown variant: {other}"),
    }
}

fn label_of(name: &str) -> &'static str {
    match name {
        "tokio"   => "1.tokio::oneshot",
        "oneshot" => "2.kit::OneShotAsync",
        "pipe"    => "3.kit::PipeAsync",
        _         => "?",
    }
}

fn main() {
    let per_producer = env_u64("BENCH_MSGS", 5_000);
    let runs = env_u64("BENCH_RUNS", 1).max(1);
    let configs: Vec<usize> = env_str("BENCH_PRODUCERS", "1,4,16")
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let variant = env_str("BENCH_VARIANT", "all");
    let variants_to_run: Vec<&str> = if variant == "all" {
        vec!["tokio", "oneshot", "pipe"]
    } else {
        vec![variant.as_str()]
    };
    let _keep = variant.clone();

    println!("release_primitive — full-tokio publish→reply correlation");
    println!(
        "header={HEADER_SIZE}B  payload={PAYLOAD_SIZE}B  reply={REPLY_SIZE}B  ring={RING_CAP}\n\
         per-producer msgs = {per_producer}, runs = {runs}, producers = {configs:?}, variants = {variants_to_run:?}\n",
    );
    println!(
        "  {:25} | {:>3} | {:>8}      | {:>9}  | {:>11}  | {}",
        "variant", "P", "acks", "elapsed", "ns/op", "ack/s",
    );
    println!("  {}", "-".repeat(100));

    if !env_flag("BENCH_NO_WARMUP") {
        println!("  (warmup: oneshot P=1 msgs=200)");
        let _ = bench_kit_oneshot_async(1, 200);
    }

    for &p in &configs {
        println!();
        for v in &variants_to_run {
            let mut best: Option<Duration> = None;
            for _ in 0..runs {
                let d = run_variant(v, p, per_producer);
                best = Some(match best {
                    Some(b) => b.min(d),
                    None => d,
                });
            }
            fmt_row(label_of(v), p, per_producer, best.unwrap());
        }
    }
}
