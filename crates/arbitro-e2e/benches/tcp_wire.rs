//! Benchmark: step-by-step pipeline profiling.
//!
//! Measures each layer's cost by adding one component at a time:
//!
//!   Level 0 — TCP only:       encode → send → recv → decode → reply
//!   Level 1 — + Store:        … → store.append_batch → …
//!   Level 2 — + Engine:       … → engine.publish + drain_fanout → …
//!   Level 3 — + Channel hop:  client → mpsc → worker thread → mpsc → reply
//!
//! 1K msgs/batch × 64B payload. 5000 iterations per level.
//! Runs 1 core (current_thread) then all cores (multi_thread), 1 connection.
//!
//! EntryRef / EnginePublishEntry vecs are pre-computed once from a leaked
//! copy of the wire body — the loop only does TCP + store + engine work.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::IntoBytes;

use arbitro_proto::action::Action;
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_proto::wire::publish::{BatchIter, PublishEntry, PUBLISH_ENTRY_SIZE};
use arbitro_store::{EntryRef, MemoryStore, Store};

// ── Settings ─────────────────────────────────────────────────────

const MSGS_PER_BATCH: u16 = 256;
const SUBJECT: &[u8] = b"bench.msg";
const PAYLOAD_LEN: usize = 64;
const ITERATIONS: u32 = 5_000;

// ── Wire helpers ─────────────────────────────────────────────────

fn encode_publish_batch(stream_id: u32, count: u16, subject: &[u8], payload: &[u8]) -> Vec<u8> {
    let entry_wire = PUBLISH_ENTRY_SIZE + subject.len() + payload.len();
    let body_len = 2 + entry_wire * count as usize;
    let total = ENVELOPE_SIZE + body_len;
    let mut buf = Vec::with_capacity(total);

    let env = Envelope {
        action: U16::new(Action::Publish.as_u16()),
        flags: 0, _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(body_len as u32),
        env_seq: U32::new(0),
    };
    buf.extend_from_slice(env.as_bytes());
    buf.extend_from_slice(&count.to_le_bytes());

    for _ in 0..count {
        let entry = PublishEntry {
            data_len: U32::new(payload.len() as u32),
            subj_len: U16::new(subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0, _pad: [0; 3],
        };
        buf.extend_from_slice(entry.as_bytes());
        buf.extend_from_slice(subject);
        buf.extend_from_slice(payload);
    }
    buf
}

fn rep_ok_bytes() -> [u8; ENVELOPE_SIZE] {
    let env = Envelope {
        action: U16::new(Action::RepOk.as_u16()),
        flags: 0, _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(0),
        env_seq: U32::new(0),
    };
    let mut b = [0u8; ENVELOPE_SIZE];
    b.copy_from_slice(env.as_bytes());
    b
}

// ── Pre-computed batch entries (leaked, 'static) ─────────────────

fn leak_body(frame: &[u8]) -> &'static [u8] {
    let body = &frame[ENVELOPE_SIZE..];
    Box::leak(body.to_vec().into_boxed_slice())
}

fn precompute_store_entries(body: &'static [u8]) -> Vec<EntryRef<'static>> {
    BatchIter::new(body)
        .map(|e| EntryRef { subject: e.subject(), payload: e.payload() })
        .collect()
}


// ── TCP read frame helper ────────────────────────────────────────

async fn read_frame(stream: &mut TcpStream, header: &mut [u8; ENVELOPE_SIZE], body: &mut BytesMut) -> bool {
    if stream.read_exact(header.as_mut()).await.is_err() { return false; }
    let msg_len = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;
    body.clear();
    body.resize(msg_len, 0);
    if msg_len > 0 {
        if stream.read_exact(&mut body[..]).await.is_err() { return false; }
    }
    true
}

// ── Client (shared for all levels) ───────────────────────────────

async fn run_client(addr: &str, frame: &[u8], iterations: u32) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let _ = stream.set_nodelay(true);
    let mut reply = [0u8; ENVELOPE_SIZE];
    for _ in 0..iterations {
        stream.write_all(frame).await.expect("write");
        stream.read_exact(&mut reply).await.expect("read");
    }
}

// ── Level 0: TCP only ────────────────────────────────────────────

async fn server_l0(listener: TcpListener) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let _ = stream.set_nodelay(true);
    let mut header = [0u8; ENVELOPE_SIZE];
    let mut body = BytesMut::with_capacity(128 * 1024);
    let reply = rep_ok_bytes();

    loop {
        if !read_frame(&mut stream, &mut header, &mut body).await { break; }
        let iter = BatchIter::new(&body);
        for entry in iter { let _ = entry.subject(); let _ = entry.payload(); }
        stream.write_all(&reply).await.ok();
    }
}

// ── Level 1: TCP + Store ─────────────────────────────────────────

async fn server_l1(listener: TcpListener, store_entries: &'static [EntryRef<'static>]) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let _ = stream.set_nodelay(true);
    let mut header = [0u8; ENVELOPE_SIZE];
    let mut buf = BytesMut::with_capacity(128 * 1024);
    let reply = rep_ok_bytes();

    let mut store = MemoryStore::new();

    loop {
        if !read_frame(&mut stream, &mut header, &mut buf).await { break; }
        store.purge();
        let _ = store.append_batch(store_entries, 0);
        stream.write_all(&reply).await.ok();
    }
}

// ── Level 2: TCP + Channel hop only (no store) ──────────────────
//
// Measures pure channel cost: tokio → mpsc → shard thread → mpsc back → reply

async fn server_l2_channel(listener: TcpListener) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let _ = stream.set_nodelay(true);

    let (shard_tx, mut shard_rx) = mpsc::channel::<()>(65536);
    let (done_tx, mut done_rx) = mpsc::channel::<()>(65536);

    std::thread::Builder::new()
        .name("shard-bench".into())
        .spawn(move || {
            while shard_rx.blocking_recv().is_some() {
                let _ = done_tx.blocking_send(());
            }
        })
        .unwrap();

    let mut header = [0u8; ENVELOPE_SIZE];
    let mut body = BytesMut::with_capacity(128 * 1024);
    let reply = rep_ok_bytes();

    loop {
        if !read_frame(&mut stream, &mut header, &mut body).await { break; }
        if shard_tx.send(()).await.is_err() { break; }
        if done_rx.recv().await.is_none() { break; }
        stream.write_all(&reply).await.ok();
    }
}

// ── Level 3: TCP + Store + Channel hop (real publish path) ───────
//
// tokio read loop → mpsc → shard OS thread (store only) → mpsc back → reply
// This is the actual publish pipeline: store + signal, no engine.

async fn server_l3(
    listener: TcpListener,
    store_entries: &'static [EntryRef<'static>],
) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let _ = stream.set_nodelay(true);

    let (shard_tx, mut shard_rx) = mpsc::channel::<()>(65536);
    let (done_tx, mut done_rx) = mpsc::channel::<()>(65536);

    std::thread::Builder::new()
        .name("shard-bench".into())
        .spawn(move || {
            let mut store = MemoryStore::new();

            while shard_rx.blocking_recv().is_some() {
                store.purge();
                let _ = store.append_batch(store_entries, 0);
                let _ = done_tx.blocking_send(());
            }
        })
        .unwrap();

    let mut header = [0u8; ENVELOPE_SIZE];
    let mut body = BytesMut::with_capacity(128 * 1024);
    let reply = rep_ok_bytes();

    loop {
        if !read_frame(&mut stream, &mut header, &mut body).await { break; }
        if shard_tx.send(()).await.is_err() { break; }
        if done_rx.recv().await.is_none() { break; }
        stream.write_all(&reply).await.ok();
    }
}

// ── Level 2x: crossbeam channel hop only (no store) ─────────────

async fn server_l2_crossbeam(listener: TcpListener) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let _ = stream.set_nodelay(true);

    let (shard_tx, shard_rx) = crossbeam_channel::bounded::<()>(65536);
    let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(65536);

    std::thread::Builder::new()
        .name("shard-bench".into())
        .spawn(move || {
            while shard_rx.recv().is_ok() {
                let _ = done_tx.send(());
            }
        })
        .unwrap();

    let mut header = [0u8; ENVELOPE_SIZE];
    let mut body = BytesMut::with_capacity(128 * 1024);
    let reply = rep_ok_bytes();

    loop {
        if !read_frame(&mut stream, &mut header, &mut body).await { break; }
        if shard_tx.send(()).is_err() { break; }
        if done_rx.recv().is_err() { break; }
        stream.write_all(&reply).await.ok();
    }
}

// ── Level 3x: crossbeam + Store (real publish with crossbeam) ───

async fn server_l3_crossbeam(
    listener: TcpListener,
    store_entries: &'static [EntryRef<'static>],
) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let _ = stream.set_nodelay(true);

    let (shard_tx, shard_rx) = crossbeam_channel::bounded::<()>(65536);
    let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(65536);

    std::thread::Builder::new()
        .name("shard-bench".into())
        .spawn(move || {
            let mut store = MemoryStore::new();

            while shard_rx.recv().is_ok() {
                store.purge();
                let _ = store.append_batch(store_entries, 0);
                let _ = done_tx.send(());
            }
        })
        .unwrap();

    let mut header = [0u8; ENVELOPE_SIZE];
    let mut body = BytesMut::with_capacity(128 * 1024);
    let reply = rep_ok_bytes();

    loop {
        if !read_frame(&mut stream, &mut header, &mut body).await { break; }
        if shard_tx.send(()).is_err() { break; }
        if done_rx.recv().is_err() { break; }
        stream.write_all(&reply).await.ok();
    }
}

// ── Level 2f: flume channel hop only (no store) ─────────────────

async fn server_l2_flume(listener: TcpListener) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let _ = stream.set_nodelay(true);

    let (shard_tx, shard_rx) = flume::bounded::<()>(65536);
    let (done_tx, done_rx) = flume::bounded::<()>(65536);

    std::thread::Builder::new()
        .name("shard-bench".into())
        .spawn(move || {
            while shard_rx.recv().is_ok() {
                let _ = done_tx.send(());
            }
        })
        .unwrap();

    let mut header = [0u8; ENVELOPE_SIZE];
    let mut body = BytesMut::with_capacity(128 * 1024);
    let reply = rep_ok_bytes();

    loop {
        if !read_frame(&mut stream, &mut header, &mut body).await { break; }
        if shard_tx.send(()).is_err() { break; }
        if done_rx.recv().is_err() { break; }
        stream.write_all(&reply).await.ok();
    }
}

// ── Level 3f: flume + Store (real publish with flume) ────────────

async fn server_l3_flume(
    listener: TcpListener,
    store_entries: &'static [EntryRef<'static>],
) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let _ = stream.set_nodelay(true);

    let (shard_tx, shard_rx) = flume::bounded::<()>(65536);
    let (done_tx, done_rx) = flume::bounded::<()>(65536);

    std::thread::Builder::new()
        .name("shard-bench".into())
        .spawn(move || {
            let mut store = MemoryStore::new();

            while shard_rx.recv().is_ok() {
                store.purge();
                let _ = store.append_batch(store_entries, 0);
                let _ = done_tx.send(());
            }
        })
        .unwrap();

    let mut header = [0u8; ENVELOPE_SIZE];
    let mut body = BytesMut::with_capacity(128 * 1024);
    let reply = rep_ok_bytes();

    loop {
        if !read_frame(&mut stream, &mut header, &mut body).await { break; }
        if shard_tx.send(()).is_err() { break; }
        if done_rx.recv().is_err() { break; }
        stream.write_all(&reply).await.ok();
    }
}

// ── Level 2s: spin channel hop (no store) ───────────────────────
//
// Pure atomic spin — no kernel syscall, no parking.

async fn server_l2_spin(listener: TcpListener) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let _ = stream.set_nodelay(true);

    let ready = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let ready2 = ready.clone();
    let done2 = done.clone();

    std::thread::Builder::new()
        .name("shard-bench".into())
        .spawn(move || {
            loop {
                while !ready2.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                ready2.store(false, Ordering::Release);
                done2.store(true, Ordering::Release);
            }
        })
        .unwrap();

    let mut header = [0u8; ENVELOPE_SIZE];
    let mut body = BytesMut::with_capacity(128 * 1024);
    let reply = rep_ok_bytes();

    loop {
        if !read_frame(&mut stream, &mut header, &mut body).await { break; }
        ready.store(true, Ordering::Release);
        while !done.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        done.store(false, Ordering::Release);
        stream.write_all(&reply).await.ok();
    }
}

// ── Level 3s: spin + Store ──────────────────────────────────────

async fn server_l3_spin(
    listener: TcpListener,
    store_entries: &'static [EntryRef<'static>],
) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let _ = stream.set_nodelay(true);

    let ready = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let ready2 = ready.clone();
    let done2 = done.clone();

    std::thread::Builder::new()
        .name("shard-bench".into())
        .spawn(move || {
            let mut store = MemoryStore::new();
            loop {
                while !ready2.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                ready2.store(false, Ordering::Release);
                store.purge();
                let _ = store.append_batch(store_entries, 0);
                done2.store(true, Ordering::Release);
            }
        })
        .unwrap();

    let mut header = [0u8; ENVELOPE_SIZE];
    let mut body = BytesMut::with_capacity(128 * 1024);
    let reply = rep_ok_bytes();

    loop {
        if !read_frame(&mut stream, &mut header, &mut body).await { break; }
        ready.store(true, Ordering::Release);
        while !done.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        done.store(false, Ordering::Release);
        stream.write_all(&reply).await.ok();
    }
}

// ── Gate ────────────────────────────────────────────────────────

#[repr(align(64))]
struct Gate {
    locked: AtomicBool,
    parked: AtomicBool,
    worker: std::cell::UnsafeCell<Option<std::thread::Thread>>,
}
unsafe impl Sync for Gate {}

impl Gate {
    fn new() -> Self {
        Self {
            locked: AtomicBool::new(true),
            parked: AtomicBool::new(false),
            worker: std::cell::UnsafeCell::new(None),
        }
    }
    fn set_worker(&self, t: std::thread::Thread) {
        unsafe { *self.worker.get() = Some(t); }
    }
    #[inline] fn release(&self) {
        self.locked.store(false, Ordering::Relaxed);
        if self.parked.load(Ordering::Relaxed) {
            unsafe { if let Some(t) = &*self.worker.get() { t.unpark(); } }
        }
    }
    #[inline] fn lock(&self) {
        self.locked.store(true, Ordering::Relaxed);
    }
    #[inline] fn acquire(&self) {
        if !self.locked.load(Ordering::Relaxed) { return; }
        for _ in 0..512 {
            if !self.locked.load(Ordering::Relaxed) { return; }
            std::hint::spin_loop();
        }
        self.parked.store(true, Ordering::Relaxed);
        loop {
            if !self.locked.load(Ordering::Relaxed) { self.parked.store(false, Ordering::Relaxed); return; }
            std::thread::park();
            if !self.locked.load(Ordering::Relaxed) { self.parked.store(false, Ordering::Relaxed); return; }
        }
    }
}

// ── Level 2g: Gate channel hop (no store) ───────────────────────

async fn server_l2_gate(listener: TcpListener) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let _ = stream.set_nodelay(true);

    let gate = Arc::new(Gate::new());
    let done = Arc::new(AtomicBool::new(false));
    let gate2 = gate.clone();
    let done2 = done.clone();
    let main_thread = std::thread::current();

    std::thread::Builder::new()
        .name("shard-bench".into())
        .spawn(move || {
            gate2.set_worker(std::thread::current());
            loop {
                gate2.acquire();
                done2.store(true, Ordering::Relaxed);
                main_thread.unpark();
                gate2.lock();
            }
        })
        .unwrap();

    let mut header = [0u8; ENVELOPE_SIZE];
    let mut body = BytesMut::with_capacity(128 * 1024);
    let reply = rep_ok_bytes();

    loop {
        if !read_frame(&mut stream, &mut header, &mut body).await { break; }
        gate.release();
        while !done.load(Ordering::Relaxed) { std::hint::spin_loop(); }
        done.store(false, Ordering::Relaxed);
        stream.write_all(&reply).await.ok();
    }
}

// ── Level 3g: Gate + Store ──────────────────────────────────────

async fn server_l3_gate(
    listener: TcpListener,
    store_entries: &'static [EntryRef<'static>],
) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let _ = stream.set_nodelay(true);

    let gate = Arc::new(Gate::new());
    let done = Arc::new(AtomicBool::new(false));
    let gate2 = gate.clone();
    let done2 = done.clone();
    let main_thread = std::thread::current();

    std::thread::Builder::new()
        .name("shard-bench".into())
        .spawn(move || {
            gate2.set_worker(std::thread::current());
            let mut store = MemoryStore::new();
            loop {
                gate2.acquire();
                store.purge();
                let _ = store.append_batch(store_entries, 0);
                done2.store(true, Ordering::Relaxed);
                main_thread.unpark();
                gate2.lock();
            }
        })
        .unwrap();

    let mut header = [0u8; ENVELOPE_SIZE];
    let mut body = BytesMut::with_capacity(128 * 1024);
    let reply = rep_ok_bytes();

    loop {
        if !read_frame(&mut stream, &mut header, &mut body).await { break; }
        gate.release();
        while !done.load(Ordering::Relaxed) { std::hint::spin_loop(); }
        done.store(false, Ordering::Relaxed);
        stream.write_all(&reply).await.ok();
    }
}

// ── Runner ───────────────────────────────────────────────────────

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn run_level<F, Fut>(rt: &tokio::runtime::Runtime, label: &str, frame: &[u8], msgs_per_batch: u16, server_fn: F)
where
    F: FnOnce(TcpListener) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let total_msgs = msgs_per_batch as u64 * ITERATIONS as u64;

    rt.block_on(async {
        let listener = TcpListener::bind(&addr).await.unwrap();
        tokio::spawn(server_fn(listener));
        tokio::time::sleep(Duration::from_millis(20)).await;

        let start = Instant::now();
        run_client(&addr, frame, ITERATIONS).await;
        let elapsed = start.elapsed();

        let throughput = total_msgs as f64 / elapsed.as_secs_f64();
        let data_mb = (frame.len() as f64 * ITERATIONS as f64) / 1_000_000.0;
        let rate = data_mb / elapsed.as_secs_f64();

        println!(
            "  {label:45} | {elapsed:>9.2?} | {throughput:>12.0} msg/s | {rate:>8.1} MB/s",
        );
    });
}

struct BatchPrecomputed {
    frame: Vec<u8>,
    store_entries: &'static [EntryRef<'static>],
    batch_size: u16,
}

fn precompute_batch(batch_size: u16) -> BatchPrecomputed {
    let payload = vec![0u8; PAYLOAD_LEN];
    let frame = encode_publish_batch(1, batch_size, SUBJECT, &payload);
    let body: &'static [u8] = leak_body(&frame);
    let store_entries: &'static [EntryRef<'static>] =
        Box::leak(precompute_store_entries(body).into_boxed_slice());
    BatchPrecomputed { frame, store_entries, batch_size }
}

fn run_suite(rt: &tokio::runtime::Runtime, suite_label: &str, b: &BatchPrecomputed) {
    println!("\n[ {suite_label} — batch={} ]", b.batch_size);
    println!("  {:45} | {:>9} | {:>12} | {:>8}", "Level", "Time", "Throughput", "Data");
    println!("  {}", "-".repeat(90));

    let se = b.store_entries;

    run_level(rt, "L0  TCP only (recv → decode → reply)", &b.frame, b.batch_size, server_l0);
    run_level(rt, "L1  + MemoryStore.append_batch", &b.frame, b.batch_size, move |l| server_l1(l, se));
    run_level(rt, "L2  tokio::mpsc channel hop (no store)", &b.frame, b.batch_size, server_l2_channel);
    run_level(rt, "L2x crossbeam channel hop (no store)", &b.frame, b.batch_size, server_l2_crossbeam);
    run_level(rt, "L2f flume channel hop (no store)", &b.frame, b.batch_size, server_l2_flume);
    run_level(rt, "L2s spin atomic (no store)", &b.frame, b.batch_size, server_l2_spin);
    run_level(rt, "L2g Gate (no store)", &b.frame, b.batch_size, server_l2_gate);
    run_level(rt, "L3  tokio::mpsc + Store (real publish)", &b.frame, b.batch_size, move |l| server_l3(l, se));
    run_level(rt, "L3x crossbeam + Store (real publish)", &b.frame, b.batch_size, move |l| server_l3_crossbeam(l, se));
    run_level(rt, "L3f flume + Store (real publish)", &b.frame, b.batch_size, move |l| server_l3_flume(l, se));
    run_level(rt, "L3s spin + Store (real publish)", &b.frame, b.batch_size, move |l| server_l3_spin(l, se));
    run_level(rt, "L3g Gate + Store (real publish)", &b.frame, b.batch_size, move |l| server_l3_gate(l, se));
}

fn main() {
    let batches = [1u16, 256, 1000];
    let precomputed: Vec<_> = batches.iter().map(|&b| precompute_batch(b)).collect();

    println!("\nPipeline Profiling: {PAYLOAD_LEN}B payload, {ITERATIONS} iterations, 1 connection");
    println!("{}", "=".repeat(100));

    for b in &precomputed {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap();
        run_suite(&rt, "current_thread — 1 core", b);
    }

    for b in &precomputed {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all().build().unwrap();
        run_suite(&rt, "multi_thread — all cores", b);
    }

    println!("\n{}", "=".repeat(100));
}
