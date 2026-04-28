//! tcp_real_publish — REALISTIC sync bench (e2e publish_batch_sync mirror).
//!
//! Every variant uses per-conn per-batch sync semantics:
//!   - Each frame carries `cid` encoded in the high 16 bits of env_seq.
//!   - After server stores a frame, it increments `ack_counters[cid]` by
//!     the number of entries stored.
//!   - Publisher per conn awaits its own `ack_counters[cid]` to catch up
//!     before sending the next batch.
//!
//! This matches `arbitro_e2e::throughput::run_batch_sync` semantics: each
//! batch round-trip = (TCP send) + (server parse + store.append_batch) +
//! (client sees stored bump). No per-conn pipelining.
//!
//! Connections are reused across iterations.

#![allow(unused)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, IntoBytes};

use arbitro_proto::action::Action;
use arbitro_proto::wire::envelope::{Envelope, FrameView, ENVELOPE_SIZE};
use arbitro_proto::wire::publish::{BatchIter, PublishEntry, PUBLISH_ENTRY_SIZE};

use arbitro_store::{EntryRef, MemoryStore, Store};

use arbitro_kit::gate::{Lifeline, WaiterId};
use arbitro_kit::route::{Mpmc, MpmcConsumer, MpmcProducer, Mpsc, MpscConsumer, MpscProducer};
use arbitro_kit::stream::Stream;

const SUBJECT: &[u8] = b"bench.publish.x";

#[derive(Clone)]
struct Opts {
    n_clients: usize,
    frames_per_iter: usize,
    k: usize,
    payload_len: usize,
    n_streams: usize,
    n_shards: usize,
    warmup: usize,
    rounds: usize,
}

fn env_usize(var: &str, default: usize) -> usize {
    std::env::var(var).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn make_stores(n: usize) -> Vec<Arc<Mutex<MemoryStore>>> {
    (0..n).map(|_| Arc::new(Mutex::new(MemoryStore::new()))).collect()
}

#[inline]
fn cid_from_env_seq(env_seq: u32) -> usize { (env_seq >> 16) as usize }

#[inline]
fn make_env_seq(cid: usize, frame_idx: usize) -> u32 {
    ((cid as u32) << 16) | ((frame_idx as u32) & 0xFFFF)
}

async fn write_vectored_all(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    frames: &[Bytes],
) -> std::io::Result<()> {
    use std::io::IoSlice;
    let mut slices: Vec<IoSlice<'_>> = frames.iter().map(|f| IoSlice::new(f)).collect();
    let total: usize = frames.iter().map(|f| f.len()).sum();
    let mut written = 0usize;
    while written < total {
        let n = writer.write_vectored(&slices).await?;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "wv 0"));
        }
        written += n;
        let mut skip = n;
        while !slices.is_empty() && skip >= slices[0].len() {
            skip -= slices[0].len();
            slices.remove(0);
        }
        if skip > 0 && !slices.is_empty() {
            let remaining_idx = frames.len() - slices.len();
            writer.write_all(&frames[remaining_idx][skip..]).await?;
            for frame in &frames[remaining_idx + 1..] {
                writer.write_all(frame).await?;
            }
            return Ok(());
        }
    }
    Ok(())
}

async fn read_frames_async<F: FnMut(Bytes) + Send>(
    sock: &mut TcpStream,
    mut on_frame: F,
) {
    let mut buf = BytesMut::with_capacity(64 * 1024);
    loop {
        if buf.capacity() - buf.len() < 16 * 1024 {
            buf.reserve(64 * 1024);
        }
        match sock.read_buf(&mut buf).await {
            Ok(0)  => break,
            Ok(_n) => {
                while buf.len() >= ENVELOPE_SIZE {
                    let env = Envelope::ref_from_bytes(&buf[..ENVELOPE_SIZE]).unwrap();
                    let total = ENVELOPE_SIZE + env.msg_len.get() as usize;
                    if buf.len() < total { break; }
                    let frame = buf.split_to(total).freeze();
                    on_frame(frame);
                }
            }
            Err(_) => break,
        }
    }
}

#[inline]
fn parse_into_store(
    frame: &Bytes,
    stores: &[Arc<Mutex<MemoryStore>>],
    n_shards: usize,
) -> Option<usize> {
    let view = FrameView::new(frame);
    if view.action() != Some(Action::Publish) { return None; }
    let stream_id = view.stream_id();
    let body = view.body();
    let count = BatchIter::new(body).count() as usize;
    let mut refs: Vec<EntryRef<'_>> = Vec::with_capacity(count);
    for v in BatchIter::new(body) {
        refs.push(EntryRef {
            stream_id,
            subject: v.subject(),
            payload: v.payload(),
            flags: v.flags(),
        });
    }
    let shard = (stream_id as usize) % n_shards;
    let mut g = stores[shard].lock().unwrap();
    let _ = g.append_batch(&refs, 0);
    Some(count)
}

// ── ClientPool with per-conn ack atomics ─────────────────────────────────

struct ClientPool {
    txs: Vec<tokio::sync::mpsc::Sender<Bytes>>,
    writers: Vec<tokio::task::JoinHandle<()>>,
    acks: Arc<Vec<Arc<AtomicU64>>>,
}

impl ClientPool {
    async fn connect(addr: SocketAddr, n_clients: usize, acks: Arc<Vec<Arc<AtomicU64>>>) -> Self {
        let mut txs = Vec::with_capacity(n_clients);
        let mut writers = Vec::with_capacity(n_clients);
        for _ in 0..n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            let (_rh, mut wh) = sock.into_split();
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(8192);
            txs.push(tx);
            writers.push(tokio::spawn(async move {
                let mut batch: Vec<Bytes> = Vec::with_capacity(64);
                loop {
                    match rx.recv().await {
                        Some(frame) => batch.push(frame),
                        None => break,
                    }
                    while let Ok(frame) = rx.try_recv() {
                        batch.push(frame);
                    }
                    let failed = if batch.len() == 1 {
                        wh.write_all(&batch[0]).await.is_err()
                    } else {
                        write_vectored_all(&mut wh, &batch).await.is_err()
                    };
                    batch.clear();
                    if failed { break; }
                }
                let _ = wh.shutdown().await;
            }));
        }
        ClientPool { txs, writers, acks }
    }

    async fn run_iter(&self, opts: &Opts) -> Duration {
        let payload: Arc<Vec<u8>> = Arc::new(vec![0u8; opts.payload_len]);
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };
        let entry_size = PUBLISH_ENTRY_SIZE + SUBJECT.len() + payload.len();
        let body_len   = 4 + opts.k * entry_size;
        let frame_len  = ENVELOPE_SIZE + body_len;

        // Snapshot baseline so we wait for THIS iter's entries only.
        let baselines: Vec<u64> = self.acks.iter()
            .map(|a| a.load(Ordering::Acquire))
            .collect();

        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(opts.n_clients);
        for (cid, tx) in self.txs.iter().enumerate() {
            let tx = tx.clone();
            let payload = payload.clone();
            let ack = self.acks[cid].clone();
            let baseline = baselines[cid];
            let n_streams = opts.n_streams;
            let frames_per_iter = opts.frames_per_iter;
            let k = opts.k;
            handles.push(tokio::spawn(async move {
                let mut sent_entries: u64 = 0;
                for f in 0..frames_per_iter {
                    let stream_id = (cid * 7 + f) as u32 % (n_streams as u32);
                    let env_seq   = make_env_seq(cid, f);

                    let mut body: Vec<u8> = Vec::with_capacity(body_len);
                    body.extend_from_slice(&(k as u32).to_le_bytes());
                    for _ in 0..k {
                        body.extend_from_slice(entry_hdr.as_bytes());
                        body.extend_from_slice(SUBJECT);
                        body.extend_from_slice(&payload);
                    }
                    let envelope = Envelope::new(Action::Publish, stream_id, body_len as u32, env_seq);
                    let mut frame: Vec<u8> = Vec::with_capacity(frame_len);
                    frame.extend_from_slice(envelope.as_bytes());
                    frame.extend_from_slice(&body);
                    if tx.send(Bytes::from(frame)).await.is_err() { break; }

                    sent_entries += k as u64;
                    let want = baseline + sent_entries;
                    while ack.load(Ordering::Acquire) < want {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }
        for h in handles { h.await.unwrap(); }
        t0.elapsed()
    }

    async fn teardown(self) {
        drop(self.txs);
        for h in self.writers { let _ = h.await; }
    }
}

// ── ClientPool variant that ALSO reads RepOk acks via TCP ────────────────
// Used by VKitTcp. Keeps the writer half + spawns a reader task per conn
// that increments per-conn ack counters when the server's RepOk arrives.

struct ClientPoolTcp {
    txs: Vec<tokio::sync::mpsc::Sender<Bytes>>,
    writers: Vec<tokio::task::JoinHandle<()>>,
    readers: Vec<tokio::task::JoinHandle<()>>,
    acks: Arc<Vec<Arc<AtomicU64>>>,
}

impl ClientPoolTcp {
    async fn connect(addr: SocketAddr, n_clients: usize, acks: Arc<Vec<Arc<AtomicU64>>>) -> Self {
        let mut txs = Vec::with_capacity(n_clients);
        let mut writers = Vec::with_capacity(n_clients);
        let mut readers = Vec::with_capacity(n_clients);
        for cid in 0..n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            let (mut rh, mut wh) = sock.into_split();
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(8192);
            txs.push(tx);

            // Writer task — same as ClientPool.
            writers.push(tokio::spawn(async move {
                let mut batch: Vec<Bytes> = Vec::with_capacity(64);
                loop {
                    match rx.recv().await {
                        Some(frame) => batch.push(frame),
                        None => break,
                    }
                    while let Ok(frame) = rx.try_recv() {
                        batch.push(frame);
                    }
                    let failed = if batch.len() == 1 {
                        wh.write_all(&batch[0]).await.is_err()
                    } else {
                        write_vectored_all(&mut wh, &batch).await.is_err()
                    };
                    batch.clear();
                    if failed { break; }
                }
                let _ = wh.shutdown().await;
            }));

            // Ack reader task — reads RepOk envelopes from the server and
            // bumps acks[cid] by the count carried in env_seq. Each RepOk
            // is a 16-byte envelope (no body): action=RepOk, env_seq=count.
            let ack_for_cid = acks[cid].clone();
            readers.push(tokio::spawn(async move {
                let mut buf = BytesMut::with_capacity(64 * 1024);
                loop {
                    if buf.capacity() - buf.len() < 4 * 1024 {
                        buf.reserve(16 * 1024);
                    }
                    match rh.read_buf(&mut buf).await {
                        Ok(0)  => break,
                        Ok(_n) => {
                            while buf.len() >= ENVELOPE_SIZE {
                                let env = Envelope::ref_from_bytes(&buf[..ENVELOPE_SIZE]).unwrap();
                                let total = ENVELOPE_SIZE + env.msg_len.get() as usize;
                                if buf.len() < total { break; }
                                let count = env.env_seq.get();
                                let _ = buf.split_to(total);
                                ack_for_cid.fetch_add(count as u64, Ordering::Release);
                            }
                        }
                        Err(_) => break,
                    }
                }
            }));
        }
        ClientPoolTcp { txs, writers, readers, acks }
    }

    async fn run_iter(&self, opts: &Opts) -> Duration {
        let payload: Arc<Vec<u8>> = Arc::new(vec![0u8; opts.payload_len]);
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };
        let entry_size = PUBLISH_ENTRY_SIZE + SUBJECT.len() + payload.len();
        let body_len   = 4 + opts.k * entry_size;
        let frame_len  = ENVELOPE_SIZE + body_len;

        let baselines: Vec<u64> = self.acks.iter()
            .map(|a| a.load(Ordering::Acquire))
            .collect();

        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(opts.n_clients);
        for (cid, tx) in self.txs.iter().enumerate() {
            let tx = tx.clone();
            let payload = payload.clone();
            let ack = self.acks[cid].clone();
            let baseline = baselines[cid];
            let n_streams = opts.n_streams;
            let frames_per_iter = opts.frames_per_iter;
            let k = opts.k;
            handles.push(tokio::spawn(async move {
                let mut sent_entries: u64 = 0;
                for f in 0..frames_per_iter {
                    let stream_id = (cid * 7 + f) as u32 % (n_streams as u32);
                    let env_seq   = make_env_seq(cid, f);

                    let mut body: Vec<u8> = Vec::with_capacity(body_len);
                    body.extend_from_slice(&(k as u32).to_le_bytes());
                    for _ in 0..k {
                        body.extend_from_slice(entry_hdr.as_bytes());
                        body.extend_from_slice(SUBJECT);
                        body.extend_from_slice(&payload);
                    }
                    let envelope = Envelope::new(Action::Publish, stream_id, body_len as u32, env_seq);
                    let mut frame: Vec<u8> = Vec::with_capacity(frame_len);
                    frame.extend_from_slice(envelope.as_bytes());
                    frame.extend_from_slice(&body);
                    if tx.send(Bytes::from(frame)).await.is_err() { break; }

                    sent_entries += k as u64;
                    let want = baseline + sent_entries;
                    while ack.load(Ordering::Acquire) < want {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }
        for h in handles { h.await.unwrap(); }
        t0.elapsed()
    }

    async fn teardown(self) {
        drop(self.txs);
        for h in self.writers { let _ = h.await; }
        for h in self.readers { h.abort(); let _ = h.await; }
    }
}

#[derive(Copy, Clone)]
struct RoundResult {
    elapsed_ns: u128,
    total_entries: u64,
}

fn make_acks(n: usize) -> Arc<Vec<Arc<AtomicU64>>> {
    Arc::new((0..n).map(|_| Arc::new(AtomicU64::new(0))).collect())
}

trait Variant {
    fn name(&self) -> &'static str;
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult>;
}

// ── A INLINE — tokio→Mutex<MemoryStore> + ack inline ──────────────────────

struct VInline;
impl Variant for VInline {
    fn name(&self) -> &'static str { "A INLINE  tokio→Mutex<MemoryStore>::append_batch" }
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult> {
        let stores = make_stores(opts.n_shards);
        let acks = make_acks(opts.n_clients);
        let n_shards = opts.n_shards;
        let n_clients = opts.n_clients;
        let opts_c = opts.clone();

        rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let stores_acc = stores.clone();
            let acks_acc = acks.clone();
            tokio::spawn(async move {
                let mut accepted = 0;
                while accepted < n_clients {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    sock.set_nodelay(true).ok();
                    let stores = stores_acc.clone();
                    let acks = acks_acc.clone();
                    tokio::spawn(async move {
                        read_frames_async(&mut sock, move |frame| {
                            let env_seq = FrameView::new(&frame).envelope().env_seq.get();
                            let cid = cid_from_env_seq(env_seq);
                            if let Some(count) = parse_into_store(&frame, &stores, n_shards) {
                                acks[cid].fetch_add(count as u64, Ordering::Release);
                            }
                        }).await;
                    });
                    accepted += 1;
                }
            });

            let pool = ClientPool::connect(addr, n_clients, acks.clone()).await;
            let mut results = Vec::with_capacity(opts_c.rounds);
            for r in 0..(opts_c.warmup + opts_c.rounds) {
                let elapsed = pool.run_iter(&opts_c).await;
                if r >= opts_c.warmup {
                    results.push(RoundResult {
                        elapsed_ns: elapsed.as_nanos(),
                        total_entries: (opts_c.n_clients * opts_c.frames_per_iter * opts_c.k) as u64,
                    });
                }
            }
            pool.teardown().await;
            results
        })
    }
}

// ── B SPLIT ────────────────────────────────────────────────────────────────

struct VSplit;
impl Variant for VSplit {
    fn name(&self) -> &'static str { "B SPLIT   tokio→std::sync::mpsc→sync store" }
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult> {
        use std::sync::mpsc;
        let stores = make_stores(opts.n_shards);
        let acks = make_acks(opts.n_clients);
        let n_shards = opts.n_shards;
        let n_clients = opts.n_clients;
        let opts_c = opts.clone();

        let mut txs = Vec::with_capacity(n_shards);
        let mut store_handles = Vec::with_capacity(n_shards);
        for shard_idx in 0..n_shards {
            let (tx, rx) = mpsc::channel::<Bytes>();
            txs.push(tx);
            let stores = stores.clone();
            let acks_t = acks.clone();
            store_handles.push(thread::Builder::new().name(format!("v2-store-{shard_idx}"))
                .spawn(move || {
                    while let Ok(frame) = rx.recv() {
                        let cid = cid_from_env_seq(FrameView::new(&frame).envelope().env_seq.get());
                        if let Some(count) = parse_into_store(&frame, &stores, n_shards) {
                            acks_t[cid].fetch_add(count as u64, Ordering::Release);
                        }
                    }
                }).unwrap());
        }
        let txs = Arc::new(txs);
        let txs_outer = txs.clone();

        let results = rt.block_on(async move {
            let txs_c = txs_outer;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let txs_acc = txs_c.clone();
            tokio::spawn(async move {
                let mut accepted = 0;
                while accepted < n_clients {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    sock.set_nodelay(true).ok();
                    let txs = txs_acc.clone();
                    tokio::spawn(async move {
                        read_frames_async(&mut sock, move |frame| {
                            let sid = FrameView::new(&frame).stream_id();
                            let shard = (sid as usize) % txs.len();
                            let _ = txs[shard].send(frame);
                        }).await;
                    });
                    accepted += 1;
                }
            });

            let pool = ClientPool::connect(addr, n_clients, acks.clone()).await;
            let mut results = Vec::with_capacity(opts_c.rounds);
            for r in 0..(opts_c.warmup + opts_c.rounds) {
                let elapsed = pool.run_iter(&opts_c).await;
                if r >= opts_c.warmup {
                    results.push(RoundResult {
                        elapsed_ns: elapsed.as_nanos(),
                        total_entries: (opts_c.n_clients * opts_c.frames_per_iter * opts_c.k) as u64,
                    });
                }
            }
            pool.teardown().await;
            results
        });

        drop(txs);
        for h in store_handles { h.join().unwrap(); }
        results
    }
}

// ── C KIT ──────────────────────────────────────────────────────────────────

struct VKit;
impl Variant for VKit {
    fn name(&self) -> &'static str { "C KIT     tokio→kit::Stream→drain→shard store" }
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult> {
        let stores = make_stores(opts.n_shards);
        let acks = make_acks(opts.n_clients);
        let n_shards = opts.n_shards;
        let n_clients = opts.n_clients;
        let opts_c = opts.clone();

        let conn_streams: Arc<Vec<Arc<Stream<Bytes>>>> = Arc::new(
            (0..n_clients).map(|_| Arc::new(Stream::<Bytes>::new())).collect()
        );
        let shard_streams: Vec<Arc<Stream<Bytes>>> =
            (0..n_shards).map(|_| Arc::new(Stream::new())).collect();
        let life = Arc::new(Lifeline::new());

        let mut shard_handles = Vec::with_capacity(n_shards);
        for sid in 0..n_shards {
            let s = shard_streams[sid].clone();
            let stores = stores.clone();
            let acks_t = acks.clone();
            let life = life.clone();
            shard_handles.push(thread::Builder::new().name(format!("v3-store-{sid}"))
                .spawn(move || {
                    s.set_consumer(thread::current());
                    let id = life.register(thread::current());
                    let mut buf: Vec<Bytes> = Vec::with_capacity(64);
                    loop {
                        match s.recv_or_cancel(&life, id) {
                            Ok(frame) => {
                                buf.push(frame);
                                let _ = s.recv_bulk(&mut buf, 63);
                                let mut all_refs: Vec<EntryRef<'_>> =
                                    Vec::with_capacity(buf.len() * 8);
                                let mut per_conn_counts: Vec<(usize, u64)> =
                                    Vec::with_capacity(buf.len());
                                for f in buf.iter() {
                                    let view = FrameView::new(f);
                                    if view.action() != Some(Action::Publish) { continue; }
                                    let stream_id = view.stream_id();
                                    let cid = cid_from_env_seq(view.envelope().env_seq.get());
                                    let mut local_count = 0u64;
                                    for ev in BatchIter::new(view.body()) {
                                        all_refs.push(EntryRef {
                                            stream_id,
                                            subject: ev.subject(),
                                            payload: ev.payload(),
                                            flags: ev.flags(),
                                        });
                                        local_count += 1;
                                    }
                                    per_conn_counts.push((cid, local_count));
                                }
                                if !all_refs.is_empty() {
                                    let _ = stores[sid].lock().unwrap()
                                        .append_batch(&all_refs, 0);
                                }
                                for (cid, c) in per_conn_counts {
                                    acks_t[cid].fetch_add(c, Ordering::Release);
                                }
                                buf.clear();
                            }
                            Err(_) => break,
                        }
                    }
                }).unwrap());
        }

        let drain_streams = conn_streams.clone();
        let drain_shards: Vec<Arc<Stream<Bytes>>> = shard_streams.iter().cloned().collect();
        let drain_done = Arc::new(AtomicU64::new(0));
        let drain_done_c = drain_done.clone();
        let drain = thread::Builder::new().name("v3-drain".into())
            .spawn(move || {
                let mut local: Vec<Bytes> = Vec::with_capacity(64);
                let mut buckets: Vec<Vec<Bytes>> =
                    (0..n_shards).map(|_| Vec::with_capacity(64)).collect();
                let mut empty: u32 = 0;
                while drain_done_c.load(Ordering::Acquire) == 0 {
                    let mut found = false;
                    for cs in drain_streams.iter() {
                        local.clear();
                        let n = cs.recv_bulk(&mut local, 64);
                        if n > 0 {
                            found = true;
                            for f in local.drain(..) {
                                let s = FrameView::new(&f).stream_id();
                                buckets[(s as usize) % n_shards].push(f);
                            }
                            for (idx, b) in buckets.iter_mut().enumerate() {
                                if !b.is_empty() {
                                    let _ = drain_shards[idx].send_iter(b.drain(..));
                                }
                            }
                        }
                    }
                    if !found {
                        empty = empty.saturating_add(1);
                        if empty < 64 { std::hint::spin_loop(); } else { thread::yield_now(); }
                    } else { empty = 0; }
                }
            }).unwrap();

        let conn_streams_for_acc = conn_streams.clone();
        let results = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let acc_streams = conn_streams_for_acc.clone();
            let n_clients_e = n_clients;
            tokio::spawn(async move {
                let mut accepted = 0;
                while accepted < n_clients_e {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    sock.set_nodelay(true).ok();
                    let conn_stream = acc_streams[accepted].clone();
                    tokio::spawn(async move {
                        read_frames_async(&mut sock, |frame| {
                            conn_stream.send(frame);
                        }).await;
                    });
                    accepted += 1;
                }
            });

            let pool = ClientPool::connect(addr, n_clients, acks.clone()).await;
            let mut results = Vec::with_capacity(opts_c.rounds);
            for r in 0..(opts_c.warmup + opts_c.rounds) {
                let elapsed = pool.run_iter(&opts_c).await;
                if r >= opts_c.warmup {
                    results.push(RoundResult {
                        elapsed_ns: elapsed.as_nanos(),
                        total_entries: (opts_c.n_clients * opts_c.frames_per_iter * opts_c.k) as u64,
                    });
                }
            }
            pool.teardown().await;
            results
        });

        drain_done.store(1, Ordering::Release);
        drain.join().unwrap();
        life.cancel_all();
        for h in shard_handles { h.join().unwrap(); }
        results
    }
}

// ── D CB ──────────────────────────────────────────────────────────────────

struct VCrossbeam;
impl Variant for VCrossbeam {
    fn name(&self) -> &'static str { "D CB      tokio→crossbeam→sync store" }
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult> {
        use crossbeam_channel::unbounded;
        let stores = make_stores(opts.n_shards);
        let acks = make_acks(opts.n_clients);
        let n_shards = opts.n_shards;
        let n_clients = opts.n_clients;
        let opts_c = opts.clone();

        let mut txs = Vec::with_capacity(n_shards);
        let mut store_handles = Vec::with_capacity(n_shards);
        for shard_idx in 0..n_shards {
            let (tx, rx) = unbounded::<Bytes>();
            txs.push(tx);
            let stores = stores.clone();
            let acks_t = acks.clone();
            store_handles.push(thread::Builder::new().name(format!("v4-store-{shard_idx}"))
                .spawn(move || {
                    while let Ok(frame) = rx.recv() {
                        let cid = cid_from_env_seq(FrameView::new(&frame).envelope().env_seq.get());
                        if let Some(count) = parse_into_store(&frame, &stores, n_shards) {
                            acks_t[cid].fetch_add(count as u64, Ordering::Release);
                        }
                    }
                }).unwrap());
        }
        let txs = Arc::new(txs);
        let txs_outer = txs.clone();

        let results = rt.block_on(async move {
            let txs_c = txs_outer;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let txs_acc = txs_c.clone();
            tokio::spawn(async move {
                let mut accepted = 0;
                while accepted < n_clients {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    sock.set_nodelay(true).ok();
                    let txs = txs_acc.clone();
                    tokio::spawn(async move {
                        read_frames_async(&mut sock, move |frame| {
                            let s = FrameView::new(&frame).stream_id();
                            let shard = (s as usize) % txs.len();
                            let _ = txs[shard].send(frame);
                        }).await;
                    });
                    accepted += 1;
                }
            });

            let pool = ClientPool::connect(addr, n_clients, acks.clone()).await;
            let mut results = Vec::with_capacity(opts_c.rounds);
            for r in 0..(opts_c.warmup + opts_c.rounds) {
                let elapsed = pool.run_iter(&opts_c).await;
                if r >= opts_c.warmup {
                    results.push(RoundResult {
                        elapsed_ns: elapsed.as_nanos(),
                        total_entries: (opts_c.n_clients * opts_c.frames_per_iter * opts_c.k) as u64,
                    });
                }
            }
            pool.teardown().await;
            results
        });

        drop(txs);
        for h in store_handles { h.join().unwrap(); }
        results
    }
}

// ── E TOKIO ──────────────────────────────────────────────────────────────

struct VTokio;
impl Variant for VTokio {
    fn name(&self) -> &'static str { "E TOKIO   tokio→tokio::mpsc→sync store" }
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult> {
        use tokio::sync::mpsc as tmpsc;
        let stores = make_stores(opts.n_shards);
        let acks = make_acks(opts.n_clients);
        let n_shards = opts.n_shards;
        let n_clients = opts.n_clients;
        let opts_c = opts.clone();

        let mut txs = Vec::with_capacity(n_shards);
        let mut store_handles = Vec::with_capacity(n_shards);
        for shard_idx in 0..n_shards {
            let (tx, mut rx) = tmpsc::unbounded_channel::<Bytes>();
            txs.push(tx);
            let stores = stores.clone();
            let acks_t = acks.clone();
            store_handles.push(thread::Builder::new().name(format!("v5-store-{shard_idx}"))
                .spawn(move || {
                    while let Some(frame) = rx.blocking_recv() {
                        let cid = cid_from_env_seq(FrameView::new(&frame).envelope().env_seq.get());
                        if let Some(count) = parse_into_store(&frame, &stores, n_shards) {
                            acks_t[cid].fetch_add(count as u64, Ordering::Release);
                        }
                    }
                }).unwrap());
        }
        let txs = Arc::new(txs);
        let txs_outer = txs.clone();

        let results = rt.block_on(async move {
            let txs_c = txs_outer;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let txs_acc = txs_c.clone();
            tokio::spawn(async move {
                let mut accepted = 0;
                while accepted < n_clients {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    sock.set_nodelay(true).ok();
                    let txs = txs_acc.clone();
                    tokio::spawn(async move {
                        read_frames_async(&mut sock, move |frame| {
                            let s = FrameView::new(&frame).stream_id();
                            let shard = (s as usize) % txs.len();
                            let _ = txs[shard].send(frame);
                        }).await;
                    });
                    accepted += 1;
                }
            });

            let pool = ClientPool::connect(addr, n_clients, acks.clone()).await;
            let mut results = Vec::with_capacity(opts_c.rounds);
            for r in 0..(opts_c.warmup + opts_c.rounds) {
                let elapsed = pool.run_iter(&opts_c).await;
                if r >= opts_c.warmup {
                    results.push(RoundResult {
                        elapsed_ns: elapsed.as_nanos(),
                        total_entries: (opts_c.n_clients * opts_c.frames_per_iter * opts_c.k) as u64,
                    });
                }
            }
            pool.teardown().await;
            results
        });

        drop(txs);
        for h in store_handles { h.join().unwrap(); }
        results
    }
}

// ── F KIT-TCP — same as C KIT but ack via TCP RepOk frame ───────────────
//
// Mirrors the real arbitro flow: server side, after store.append_batch,
// sends a 16-byte RepOk envelope per (cid, count). Client reads RepOk
// frames and increments per-conn ack counters. Round-trip = TCP write +
// store + TCP write back + TCP read.

struct VKitTcp;
impl Variant for VKitTcp {
    fn name(&self) -> &'static str { "F KIT-TCP tokio→kit::Stream→drain→shard store + TCP RepOk" }
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult> {
        let stores = make_stores(opts.n_shards);
        let acks = make_acks(opts.n_clients);
        let n_shards = opts.n_shards;
        let n_clients = opts.n_clients;
        let opts_c = opts.clone();

        let conn_streams: Arc<Vec<Arc<Stream<Bytes>>>> = Arc::new(
            (0..n_clients).map(|_| Arc::new(Stream::<Bytes>::new())).collect()
        );
        let shard_streams: Vec<Arc<Stream<Bytes>>> =
            (0..n_shards).map(|_| Arc::new(Stream::new())).collect();
        let life = Arc::new(Lifeline::new());

        // Per-conn ack-write channels (sync→async bridge): shard thread
        // sends `count` via these; a per-conn tokio task drains and writes
        // RepOk frames to TCP.
        let mut ack_txs: Vec<tokio::sync::mpsc::UnboundedSender<u32>> =
            Vec::with_capacity(n_clients);
        let mut ack_rxs: Vec<tokio::sync::mpsc::UnboundedReceiver<u32>> =
            Vec::with_capacity(n_clients);
        for _ in 0..n_clients {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
            ack_txs.push(tx);
            ack_rxs.push(rx);
        }
        let ack_txs = Arc::new(ack_txs);

        // Shard store threads.
        let mut shard_handles = Vec::with_capacity(n_shards);
        for sid in 0..n_shards {
            let s = shard_streams[sid].clone();
            let stores = stores.clone();
            let ack_txs_t = ack_txs.clone();
            let life = life.clone();
            shard_handles.push(thread::Builder::new().name(format!("v6-store-{sid}"))
                .spawn(move || {
                    s.set_consumer(thread::current());
                    let id = life.register(thread::current());
                    let mut buf: Vec<Bytes> = Vec::with_capacity(64);
                    loop {
                        match s.recv_or_cancel(&life, id) {
                            Ok(frame) => {
                                buf.push(frame);
                                let _ = s.recv_bulk(&mut buf, 63);
                                let mut all_refs: Vec<EntryRef<'_>> =
                                    Vec::with_capacity(buf.len() * 8);
                                let mut per_conn_counts: Vec<(usize, u64)> =
                                    Vec::with_capacity(buf.len());
                                for f in buf.iter() {
                                    let view = FrameView::new(f);
                                    if view.action() != Some(Action::Publish) { continue; }
                                    let stream_id = view.stream_id();
                                    let cid = cid_from_env_seq(view.envelope().env_seq.get());
                                    let mut local_count = 0u64;
                                    for ev in BatchIter::new(view.body()) {
                                        all_refs.push(EntryRef {
                                            stream_id,
                                            subject: ev.subject(),
                                            payload: ev.payload(),
                                            flags: ev.flags(),
                                        });
                                        local_count += 1;
                                    }
                                    per_conn_counts.push((cid, local_count));
                                }
                                if !all_refs.is_empty() {
                                    let _ = stores[sid].lock().unwrap()
                                        .append_batch(&all_refs, 0);
                                }
                                // Send RepOk via per-conn ack mpsc — tokio
                                // task on the conn writes the actual envelope.
                                for (cid, c) in per_conn_counts {
                                    let _ = ack_txs_t[cid].send(c as u32);
                                }
                                buf.clear();
                            }
                            Err(_) => break,
                        }
                    }
                }).unwrap());
        }

        let drain_streams = conn_streams.clone();
        let drain_shards: Vec<Arc<Stream<Bytes>>> = shard_streams.iter().cloned().collect();
        let drain_done = Arc::new(AtomicU64::new(0));
        let drain_done_c = drain_done.clone();
        let drain = thread::Builder::new().name("v6-drain".into())
            .spawn(move || {
                let mut local: Vec<Bytes> = Vec::with_capacity(64);
                let mut buckets: Vec<Vec<Bytes>> =
                    (0..n_shards).map(|_| Vec::with_capacity(64)).collect();
                let mut empty: u32 = 0;
                while drain_done_c.load(Ordering::Acquire) == 0 {
                    let mut found = false;
                    for cs in drain_streams.iter() {
                        local.clear();
                        let n = cs.recv_bulk(&mut local, 64);
                        if n > 0 {
                            found = true;
                            for f in local.drain(..) {
                                let s = FrameView::new(&f).stream_id();
                                buckets[(s as usize) % n_shards].push(f);
                            }
                            for (idx, b) in buckets.iter_mut().enumerate() {
                                if !b.is_empty() {
                                    let _ = drain_shards[idx].send_iter(b.drain(..));
                                }
                            }
                        }
                    }
                    if !found {
                        empty = empty.saturating_add(1);
                        if empty < 64 { std::hint::spin_loop(); } else { thread::yield_now(); }
                    } else { empty = 0; }
                }
            }).unwrap();

        let conn_streams_for_acc = conn_streams.clone();
        let results = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let acc_streams = conn_streams_for_acc.clone();
            let mut ack_rxs_iter = ack_rxs.into_iter();
            let n_clients_e = n_clients;

            // Acceptor loop runs inline before client connect, so ack_rxs
            // are consumed in connection order = cid order.
            let acceptor = tokio::spawn(async move {
                let mut accepted = 0;
                while accepted < n_clients_e {
                    let (sock, _) = listener.accept().await.unwrap();
                    sock.set_nodelay(true).ok();
                    let (mut rh, mut wh) = sock.into_split();
                    let conn_stream = acc_streams[accepted].clone();
                    // Spawn ack-writer task for this conn.
                    let mut ack_rx = ack_rxs_iter.next().unwrap();
                    tokio::spawn(async move {
                        while let Some(count) = ack_rx.recv().await {
                            let env = Envelope::new(Action::RepOk, 0, 0, count);
                            if wh.write_all(env.as_bytes()).await.is_err() { break; }
                        }
                        let _ = wh.shutdown().await;
                    });
                    // Spawn frame reader.
                    tokio::spawn(async move {
                        // Use a TcpStream-style read loop on the read half.
                        let mut buf = BytesMut::with_capacity(64 * 1024);
                        loop {
                            if buf.capacity() - buf.len() < 16 * 1024 {
                                buf.reserve(64 * 1024);
                            }
                            match rh.read_buf(&mut buf).await {
                                Ok(0)  => break,
                                Ok(_n) => {
                                    while buf.len() >= ENVELOPE_SIZE {
                                        let env = Envelope::ref_from_bytes(&buf[..ENVELOPE_SIZE]).unwrap();
                                        let total = ENVELOPE_SIZE + env.msg_len.get() as usize;
                                        if buf.len() < total { break; }
                                        let frame = buf.split_to(total).freeze();
                                        conn_stream.send(frame);
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    });
                    accepted += 1;
                }
            });

            let pool = ClientPoolTcp::connect(addr, n_clients, acks.clone()).await;
            let mut results = Vec::with_capacity(opts_c.rounds);
            for r in 0..(opts_c.warmup + opts_c.rounds) {
                let elapsed = pool.run_iter(&opts_c).await;
                if r >= opts_c.warmup {
                    results.push(RoundResult {
                        elapsed_ns: elapsed.as_nanos(),
                        total_entries: (opts_c.n_clients * opts_c.frames_per_iter * opts_c.k) as u64,
                    });
                }
            }
            pool.teardown().await;
            // acceptor will hang since connections still alive, but pool
            // teardown closes write side; readers will break on EOF eventually.
            acceptor.abort();
            results
        });

        drain_done.store(1, Ordering::Release);
        drain.join().unwrap();
        life.cancel_all();
        for h in shard_handles { h.join().unwrap(); }
        results
    }
}

// ── G MPMC — kit::route::Mpmc<Bytes>, M=N_CLIENTS, N=N_SHARDS ────────────
//
// NOTE on semantics: Mpmc.try_send uses ADAPTIVE LOAD BALANCING (cursor
// scans shards), NOT stream_id affinity. Differs from B/D/E/H which shard
// by stream_id. Included as raw-throughput comparison with kit's built-in
// M:N topology.

struct VKitMpmc;
impl Variant for VKitMpmc {
    fn name(&self) -> &'static str { "G MPMC    tokio→kit::Mpmc<Bytes> M:N (load-balanced)" }
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult> {
        let stores = make_stores(opts.n_shards);
        let acks = make_acks(opts.n_clients);
        let n_shards = opts.n_shards;
        let n_clients = opts.n_clients;
        let opts_c = opts.clone();

        let (producers, consumers, shutdown) =
            Mpmc::<Bytes, 64>::new(n_clients, n_shards);
        let producers = Arc::new(Mutex::new(
            producers.into_iter().map(Some).collect::<Vec<_>>()
        ));

        // Shard store threads (consumers).
        let mut shard_handles = Vec::with_capacity(n_shards);
        for (sid, consumer) in consumers.into_iter().enumerate() {
            let stores = stores.clone();
            let acks_t = acks.clone();
            shard_handles.push(thread::Builder::new().name(format!("g-store-{sid}"))
                .spawn(move || {
                    consumer.bind();
                    loop {
                        let r = consumer.recv_batch(|frame: Bytes| {
                            let cid = cid_from_env_seq(
                                FrameView::new(&frame).envelope().env_seq.get(),
                            );
                            if let Some(count) = parse_into_store(&frame, &stores, n_shards) {
                                acks_t[cid].fetch_add(count as u64, Ordering::Release);
                            }
                        });
                        if r.is_err() { break; }
                    }
                }).unwrap());
        }

        let producers_for_acc = producers.clone();
        let results = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let producers_acc = producers_for_acc.clone();
            tokio::spawn(async move {
                let mut accepted = 0;
                while accepted < n_clients {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    sock.set_nodelay(true).ok();
                    let producer = producers_acc.lock().unwrap()[accepted].take().unwrap();
                    tokio::spawn(async move {
                        read_frames_async(&mut sock, move |frame| {
                            let mut v = frame;
                            loop {
                                match producer.try_send(v) {
                                    Ok(()) => break,
                                    Err(rejected) => {
                                        v = rejected;
                                        std::hint::spin_loop();
                                    }
                                }
                            }
                        }).await;
                    });
                    accepted += 1;
                }
            });

            let pool = ClientPool::connect(addr, n_clients, acks.clone()).await;
            let mut results = Vec::with_capacity(opts_c.rounds);
            for r in 0..(opts_c.warmup + opts_c.rounds) {
                let elapsed = pool.run_iter(&opts_c).await;
                if r >= opts_c.warmup {
                    results.push(RoundResult {
                        elapsed_ns: elapsed.as_nanos(),
                        total_entries: (opts_c.n_clients * opts_c.frames_per_iter * opts_c.k) as u64,
                    });
                }
            }
            pool.teardown().await;
            results
        });

        shutdown.signal();
        for h in shard_handles { h.join().unwrap(); }
        results
    }
}

// ── H MPSC — N_SHARDS × kit::route::Mpsc<Bytes>, stream_id sharding ──────
//
// Apples-to-apples with B/D/E: same `stream_id % N` sharding logic, just
// the channel implementation differs. Each cid task owns N producer
// handles (one per shard).

struct VKitMpsc;
impl Variant for VKitMpsc {
    fn name(&self) -> &'static str { "H MPSC    tokio→N×kit::Mpsc<Bytes> (stream_id sharded)" }
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult> {
        let stores = make_stores(opts.n_shards);
        let acks = make_acks(opts.n_clients);
        let n_shards = opts.n_shards;
        let n_clients = opts.n_clients;
        let opts_c = opts.clone();

        // N independent Mpsc instances, each with M=n_clients producers.
        let mut all_consumers = Vec::with_capacity(n_shards);
        let mut all_shutdowns = Vec::with_capacity(n_shards);
        // handles_per_cid[cid][shard] = the producer cid uses to push to shard.
        let mut handles_per_cid: Vec<Vec<Option<MpscProducer<Bytes, 64>>>> =
            (0..n_clients).map(|_| (0..n_shards).map(|_| None).collect()).collect();
        for sid in 0..n_shards {
            let (prods, c, s) = Mpsc::<Bytes, 64>::new(n_clients);
            all_consumers.push(c);
            all_shutdowns.push(s);
            for (cid, p) in prods.into_iter().enumerate() {
                handles_per_cid[cid][sid] = Some(p);
            }
        }
        let handles_per_cid = Arc::new(Mutex::new(handles_per_cid));

        let mut shard_handles = Vec::with_capacity(n_shards);
        for (sid, consumer) in all_consumers.into_iter().enumerate() {
            let stores = stores.clone();
            let acks_t = acks.clone();
            shard_handles.push(thread::Builder::new().name(format!("h-store-{sid}"))
                .spawn(move || {
                    consumer.bind();
                    loop {
                        let r = consumer.recv_batch(|frame: Bytes| {
                            let cid = cid_from_env_seq(
                                FrameView::new(&frame).envelope().env_seq.get(),
                            );
                            if let Some(count) = parse_into_store(&frame, &stores, n_shards) {
                                acks_t[cid].fetch_add(count as u64, Ordering::Release);
                            }
                        });
                        if r.is_err() { break; }
                    }
                }).unwrap());
        }

        let handles_for_acc = handles_per_cid.clone();
        let results = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let handles_acc = handles_for_acc.clone();
            tokio::spawn(async move {
                let mut accepted = 0;
                while accepted < n_clients {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    sock.set_nodelay(true).ok();
                    let my_handles: Vec<MpscProducer<Bytes, 64>> = {
                        let mut g = handles_acc.lock().unwrap();
                        g[accepted].iter_mut().map(|h| h.take().unwrap()).collect()
                    };
                    tokio::spawn(async move {
                        read_frames_async(&mut sock, move |frame| {
                            let sid = (FrameView::new(&frame).stream_id() as usize) % n_shards;
                            let mut v = frame;
                            loop {
                                match my_handles[sid].try_send(v) {
                                    Ok(()) => break,
                                    Err(rejected) => {
                                        v = rejected;
                                        std::hint::spin_loop();
                                    }
                                }
                            }
                        }).await;
                    });
                    accepted += 1;
                }
            });

            let pool = ClientPool::connect(addr, n_clients, acks.clone()).await;
            let mut results = Vec::with_capacity(opts_c.rounds);
            for r in 0..(opts_c.warmup + opts_c.rounds) {
                let elapsed = pool.run_iter(&opts_c).await;
                if r >= opts_c.warmup {
                    results.push(RoundResult {
                        elapsed_ns: elapsed.as_nanos(),
                        total_entries: (opts_c.n_clients * opts_c.frames_per_iter * opts_c.k) as u64,
                    });
                }
            }
            pool.teardown().await;
            results
        });

        for s in all_shutdowns { s.signal(); }
        for h in shard_handles { h.join().unwrap(); }
        results
    }
}

// ── Channel-only variants (NULL CONSUME) ────────────────────────────────
//
// These mirror B/D/E/H but replace `parse_into_store` with minimal work:
// extract cid from envelope, increment ack counter by `k_entries` (known
// from opts). Goal: isolate channel cost in TCP context, since the full
// pipeline (parse + Mutex + append_batch ≈ 11 µs/frame) was masking any
// channel-level differences (~2-65 ns/send → 0.02-0.5% of frame time).
//
// Expected: kit::Mpsc beats crossbeam/tokio/std visibly here.

#[inline(always)]
fn null_consume(frame: &Bytes, k_entries: u64, acks_t: &Arc<Vec<Arc<AtomicU64>>>) {
    let cid = cid_from_env_seq(FrameView::new(frame).envelope().env_seq.get());
    std::hint::black_box(frame);
    acks_t[cid].fetch_add(k_entries, Ordering::Release);
}

// ── I NULL-STD ──
struct VNullStd;
impl Variant for VNullStd {
    fn name(&self) -> &'static str { "I NULL-STD   tokio→std::sync::mpsc→null consume" }
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult> {
        use std::sync::mpsc;
        let acks = make_acks(opts.n_clients);
        let n_shards = opts.n_shards;
        let n_clients = opts.n_clients;
        let k_entries = opts.k as u64;
        let opts_c = opts.clone();

        let mut txs = Vec::with_capacity(n_shards);
        let mut store_handles = Vec::with_capacity(n_shards);
        for shard_idx in 0..n_shards {
            let (tx, rx) = mpsc::channel::<Bytes>();
            txs.push(tx);
            let acks_t = acks.clone();
            store_handles.push(thread::Builder::new().name(format!("nullstd-{shard_idx}"))
                .spawn(move || {
                    while let Ok(frame) = rx.recv() {
                        null_consume(&frame, k_entries, &acks_t);
                    }
                }).unwrap());
        }
        let txs = Arc::new(txs);
        let txs_outer = txs.clone();

        let results = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let txs_acc = txs_outer.clone();
            tokio::spawn(async move {
                let mut accepted = 0;
                while accepted < n_clients {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    sock.set_nodelay(true).ok();
                    let txs = txs_acc.clone();
                    tokio::spawn(async move {
                        read_frames_async(&mut sock, move |frame| {
                            let s = FrameView::new(&frame).stream_id();
                            let _ = txs[(s as usize) % txs.len()].send(frame);
                        }).await;
                    });
                    accepted += 1;
                }
            });
            let pool = ClientPool::connect(addr, n_clients, acks.clone()).await;
            let mut results = Vec::with_capacity(opts_c.rounds);
            for r in 0..(opts_c.warmup + opts_c.rounds) {
                let elapsed = pool.run_iter(&opts_c).await;
                if r >= opts_c.warmup {
                    results.push(RoundResult {
                        elapsed_ns: elapsed.as_nanos(),
                        total_entries: (opts_c.n_clients * opts_c.frames_per_iter * opts_c.k) as u64,
                    });
                }
            }
            pool.teardown().await;
            results
        });
        drop(txs);
        for h in store_handles { h.join().unwrap(); }
        results
    }
}

// ── J NULL-CB ──
struct VNullCb;
impl Variant for VNullCb {
    fn name(&self) -> &'static str { "J NULL-CB    tokio→crossbeam→null consume" }
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult> {
        use crossbeam_channel::unbounded;
        let acks = make_acks(opts.n_clients);
        let n_shards = opts.n_shards;
        let n_clients = opts.n_clients;
        let k_entries = opts.k as u64;
        let opts_c = opts.clone();

        let mut txs = Vec::with_capacity(n_shards);
        let mut store_handles = Vec::with_capacity(n_shards);
        for shard_idx in 0..n_shards {
            let (tx, rx) = unbounded::<Bytes>();
            txs.push(tx);
            let acks_t = acks.clone();
            store_handles.push(thread::Builder::new().name(format!("nullcb-{shard_idx}"))
                .spawn(move || {
                    while let Ok(frame) = rx.recv() {
                        null_consume(&frame, k_entries, &acks_t);
                    }
                }).unwrap());
        }
        let txs = Arc::new(txs);
        let txs_outer = txs.clone();

        let results = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let txs_acc = txs_outer.clone();
            tokio::spawn(async move {
                let mut accepted = 0;
                while accepted < n_clients {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    sock.set_nodelay(true).ok();
                    let txs = txs_acc.clone();
                    tokio::spawn(async move {
                        read_frames_async(&mut sock, move |frame| {
                            let s = FrameView::new(&frame).stream_id();
                            let _ = txs[(s as usize) % txs.len()].send(frame);
                        }).await;
                    });
                    accepted += 1;
                }
            });
            let pool = ClientPool::connect(addr, n_clients, acks.clone()).await;
            let mut results = Vec::with_capacity(opts_c.rounds);
            for r in 0..(opts_c.warmup + opts_c.rounds) {
                let elapsed = pool.run_iter(&opts_c).await;
                if r >= opts_c.warmup {
                    results.push(RoundResult {
                        elapsed_ns: elapsed.as_nanos(),
                        total_entries: (opts_c.n_clients * opts_c.frames_per_iter * opts_c.k) as u64,
                    });
                }
            }
            pool.teardown().await;
            results
        });
        drop(txs);
        for h in store_handles { h.join().unwrap(); }
        results
    }
}

// ── K NULL-TOKIO ──
struct VNullTokio;
impl Variant for VNullTokio {
    fn name(&self) -> &'static str { "K NULL-TOKIO tokio→tokio::mpsc→null consume" }
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult> {
        use tokio::sync::mpsc as tmpsc;
        let acks = make_acks(opts.n_clients);
        let n_shards = opts.n_shards;
        let n_clients = opts.n_clients;
        let k_entries = opts.k as u64;
        let opts_c = opts.clone();

        let mut txs = Vec::with_capacity(n_shards);
        let mut store_handles = Vec::with_capacity(n_shards);
        for shard_idx in 0..n_shards {
            let (tx, mut rx) = tmpsc::unbounded_channel::<Bytes>();
            txs.push(tx);
            let acks_t = acks.clone();
            store_handles.push(thread::Builder::new().name(format!("nulltok-{shard_idx}"))
                .spawn(move || {
                    while let Some(frame) = rx.blocking_recv() {
                        null_consume(&frame, k_entries, &acks_t);
                    }
                }).unwrap());
        }
        let txs = Arc::new(txs);
        let txs_outer = txs.clone();

        let results = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let txs_acc = txs_outer.clone();
            tokio::spawn(async move {
                let mut accepted = 0;
                while accepted < n_clients {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    sock.set_nodelay(true).ok();
                    let txs = txs_acc.clone();
                    tokio::spawn(async move {
                        read_frames_async(&mut sock, move |frame| {
                            let s = FrameView::new(&frame).stream_id();
                            let _ = txs[(s as usize) % txs.len()].send(frame);
                        }).await;
                    });
                    accepted += 1;
                }
            });
            let pool = ClientPool::connect(addr, n_clients, acks.clone()).await;
            let mut results = Vec::with_capacity(opts_c.rounds);
            for r in 0..(opts_c.warmup + opts_c.rounds) {
                let elapsed = pool.run_iter(&opts_c).await;
                if r >= opts_c.warmup {
                    results.push(RoundResult {
                        elapsed_ns: elapsed.as_nanos(),
                        total_entries: (opts_c.n_clients * opts_c.frames_per_iter * opts_c.k) as u64,
                    });
                }
            }
            pool.teardown().await;
            results
        });
        drop(txs);
        for h in store_handles { h.join().unwrap(); }
        results
    }
}

// ── L NULL-MPSC (kit::Mpsc) ──
struct VNullKitMpsc;
impl Variant for VNullKitMpsc {
    fn name(&self) -> &'static str { "L NULL-MPSC  tokio→N×kit::Mpsc<Bytes>→null consume" }
    fn run_all(&self, rt: &Runtime, opts: &Opts) -> Vec<RoundResult> {
        let acks = make_acks(opts.n_clients);
        let n_shards = opts.n_shards;
        let n_clients = opts.n_clients;
        let k_entries = opts.k as u64;
        let opts_c = opts.clone();

        let mut all_consumers = Vec::with_capacity(n_shards);
        let mut all_shutdowns = Vec::with_capacity(n_shards);
        let mut handles_per_cid: Vec<Vec<Option<MpscProducer<Bytes, 64>>>> =
            (0..n_clients).map(|_| (0..n_shards).map(|_| None).collect()).collect();
        for sid in 0..n_shards {
            let (prods, c, s) = Mpsc::<Bytes, 64>::new(n_clients);
            all_consumers.push(c);
            all_shutdowns.push(s);
            for (cid, p) in prods.into_iter().enumerate() {
                handles_per_cid[cid][sid] = Some(p);
            }
        }
        let handles_per_cid = Arc::new(Mutex::new(handles_per_cid));

        let mut shard_handles = Vec::with_capacity(n_shards);
        for (sid, consumer) in all_consumers.into_iter().enumerate() {
            let acks_t = acks.clone();
            shard_handles.push(thread::Builder::new().name(format!("nullkit-{sid}"))
                .spawn(move || {
                    consumer.bind();
                    loop {
                        let r = consumer.recv_batch(|frame: Bytes| {
                            null_consume(&frame, k_entries, &acks_t);
                        });
                        if r.is_err() { break; }
                    }
                }).unwrap());
        }

        let handles_for_acc = handles_per_cid.clone();
        let results = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let handles_acc = handles_for_acc.clone();
            tokio::spawn(async move {
                let mut accepted = 0;
                while accepted < n_clients {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    sock.set_nodelay(true).ok();
                    let my_handles: Vec<MpscProducer<Bytes, 64>> = {
                        let mut g = handles_acc.lock().unwrap();
                        g[accepted].iter_mut().map(|h| h.take().unwrap()).collect()
                    };
                    tokio::spawn(async move {
                        read_frames_async(&mut sock, move |frame| {
                            let sid = (FrameView::new(&frame).stream_id() as usize) % n_shards;
                            let mut v = frame;
                            loop {
                                match my_handles[sid].try_send(v) {
                                    Ok(()) => break,
                                    Err(rejected) => { v = rejected; std::hint::spin_loop(); }
                                }
                            }
                        }).await;
                    });
                    accepted += 1;
                }
            });
            let pool = ClientPool::connect(addr, n_clients, acks.clone()).await;
            let mut results = Vec::with_capacity(opts_c.rounds);
            for r in 0..(opts_c.warmup + opts_c.rounds) {
                let elapsed = pool.run_iter(&opts_c).await;
                if r >= opts_c.warmup {
                    results.push(RoundResult {
                        elapsed_ns: elapsed.as_nanos(),
                        total_entries: (opts_c.n_clients * opts_c.frames_per_iter * opts_c.k) as u64,
                    });
                }
            }
            pool.teardown().await;
            results
        });

        for s in all_shutdowns { s.signal(); }
        for h in shard_handles { h.join().unwrap(); }
        results
    }
}

// ── Driver ───────────────────────────────────────────────────────────────

fn run_variant(v: &dyn Variant, rt: &Runtime, opts: &Opts) {
    let results = v.run_all(rt, opts);
    let mut samples_ns: Vec<u128> = results.iter().map(|r| r.elapsed_ns).collect();
    samples_ns.sort();
    let total = results[0].total_entries as f64;
    let min_ns = samples_ns[0] as f64;
    let p50_ns = samples_ns[samples_ns.len() / 2] as f64;
    println!("{:<58} {:>10.1} {:>10.1} {:>14.0}",
        v.name(),
        min_ns / total,
        p50_ns / total,
        1e9 / (min_ns / total));
}

fn main() {
    let opts = Opts {
        n_clients:        env_usize("BENCH_PUB_CLIENTS", 16),
        frames_per_iter:  env_usize("BENCH_PUB_FRAMES", 6),
        k:                env_usize("BENCH_PUB_BATCH_K", 256),
        payload_len:      env_usize("BENCH_PUB_PAYLOAD", 256),
        n_streams:        env_usize("BENCH_PUB_STREAMS", 16),
        n_shards:         env_usize("BENCH_PUB_SHARDS", 4),
        warmup:           env_usize("BENCH_PUB_WARMUP", 1),
        rounds:           env_usize("BENCH_PUB_ROUNDS", 5),
    };
    let workers = env_usize("BENCH_PUB_TOKIO_WORKERS", 8);

    let entry_size = PUBLISH_ENTRY_SIZE + SUBJECT.len() + opts.payload_len;
    let frame_size = ENVELOPE_SIZE + 4 + opts.k * entry_size;

    println!("=== tcp_real_publish (PER-CONN PER-BATCH SYNC, e2e publish_batch_sync mirror) ===");
    println!("clients={}  frames/iter={}  K={} entries/frame  payload={}B",
             opts.n_clients, opts.frames_per_iter, opts.k, opts.payload_len);
    println!("streams={}  shards={}  warmup={}  rounds={}  tokio_workers={}",
             opts.n_streams, opts.n_shards, opts.warmup, opts.rounds, workers);
    println!("frame size = {} B   entries/iter = {}", frame_size,
             opts.n_clients * opts.frames_per_iter * opts.k);
    println!();
    println!("{:<58} {:>10} {:>10} {:>14}",
             "variant", "ns/entry min", "ns/entry p50", "entries/sec");
    println!("{}", "─".repeat(100));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap();

    run_variant(&VInline,    &rt, &opts);
    run_variant(&VSplit,     &rt, &opts);
    run_variant(&VKit,       &rt, &opts);
    run_variant(&VCrossbeam, &rt, &opts);
    run_variant(&VTokio,     &rt, &opts);
    run_variant(&VKitTcp,    &rt, &opts);
    run_variant(&VKitMpmc,   &rt, &opts);
    run_variant(&VKitMpsc,   &rt, &opts);

    println!();
    println!("─── NULL-CONSUME group: TCP + channel + atomic ack only (no parse, no store) ───");
    println!("    These isolate channel cost. Frame time drops from ~11 µs to ~1-2 µs,");
    println!("    so the channel becomes a measurable fraction of the total.");
    println!();
    run_variant(&VNullStd,    &rt, &opts);
    run_variant(&VNullCb,     &rt, &opts);
    run_variant(&VNullTokio,  &rt, &opts);
    run_variant(&VNullKitMpsc, &rt, &opts);

    println!();
    println!("Done.");
}
