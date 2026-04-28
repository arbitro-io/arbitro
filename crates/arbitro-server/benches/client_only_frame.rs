//! client_only_frame — V1 vs V2 + server real.
//!
//! Frames pre-construidos antes del timer. Variantes:
//!   V1 — Sender<Bytes>, hace `Bytes::from(Vec)` antes de send.
//!   V2 — Sender<Vec<u8>>, manda Vec directo (sin wrap a inmutable).
//!
//! Server (compartido por ambas):
//!   - tokio acceptor + read task per conn.
//!   - Cada frame: parse FrameView, BatchIter, build EntryRef[],
//!     `MemoryStore::append_batch`, send RepOk envelope back.
//!
//! Semántica per-conn per-batch sync (ack via TCP RepOk).

#![allow(unused)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

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

const SUBJECT: &[u8] = b"bench.publish.x";

fn env_usize(var: &str, default: usize) -> usize {
    std::env::var(var).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[derive(Clone, Copy)]
struct Cfg {
    n_clients: usize,
    frames: usize,
    k: usize,
    payload_len: usize,
    rounds: usize,
}

#[inline]
fn cid_from_env_seq(env_seq: u32) -> usize { (env_seq >> 16) as usize }
#[inline]
fn make_env_seq(cid: usize, frame_idx: usize) -> u32 {
    ((cid as u32) << 16) | ((frame_idx as u32) & 0xFFFF)
}

fn build_payload_and_hdr(payload_len: usize) -> (Vec<u8>, PublishEntry) {
    let payload = vec![0u8; payload_len];
    let entry_hdr = PublishEntry {
        data_len:  U32::new(payload_len as u32),
        subj_len:  U16::new(SUBJECT.len() as u16),
        reply_len: U16::new(0),
        flags:     0,
        _pad:      [0u8; 3],
    };
    (payload, entry_hdr)
}

fn prebuild_frames(cfg: Cfg) -> Vec<Vec<Vec<u8>>> {
    let (payload, entry_hdr) = build_payload_and_hdr(cfg.payload_len);
    let entry_size = PUBLISH_ENTRY_SIZE + SUBJECT.len() + payload.len();
    let body_len = 4 + cfg.k * entry_size;
    let frame_len = ENVELOPE_SIZE + body_len;

    (0..cfg.n_clients).map(|cid| {
        (0..cfg.frames).map(|f| {
            let mut body: Vec<u8> = Vec::with_capacity(body_len);
            body.extend_from_slice(&(cfg.k as u32).to_le_bytes());
            for _ in 0..cfg.k {
                body.extend_from_slice(entry_hdr.as_bytes());
                body.extend_from_slice(SUBJECT);
                body.extend_from_slice(&payload);
            }
            let env_seq = make_env_seq(cid, f);
            let envelope = Envelope::new(Action::Publish, cid as u32, body_len as u32, env_seq);
            let mut frame: Vec<u8> = Vec::with_capacity(frame_len);
            frame.extend_from_slice(envelope.as_bytes());
            frame.extend_from_slice(&body);
            frame
        }).collect()
    }).collect()
}

// ── Server: tokio acceptor + reader task per conn + per-conn ack writer ──

async fn spawn_server(
    n_clients: usize,
    store: Arc<Mutex<MemoryStore>>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut accepted = 0;
        while accepted < n_clients {
            let (sock, _) = listener.accept().await.unwrap();
            sock.set_nodelay(true).ok();
            let (mut rh, mut wh) = sock.into_split();
            let store = store.clone();

            // Per-conn ack channel + writer task.
            let (ack_tx, mut ack_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
            tokio::spawn(async move {
                while let Some(count) = ack_rx.recv().await {
                    let env = Envelope::new(Action::RepOk, 0, 0, count);
                    if wh.write_all(env.as_bytes()).await.is_err() { break; }
                }
                let _ = wh.shutdown().await;
            });

            // Reader task: parse + store + ack.
            tokio::spawn(async move {
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
                                let view = FrameView::new(&frame);
                                let stream_id = view.stream_id();
                                let count = BatchIter::new(view.body()).count() as u64;
                                let mut refs: Vec<EntryRef<'_>> =
                                    Vec::with_capacity(count as usize);
                                for v in BatchIter::new(view.body()) {
                                    refs.push(EntryRef {
                                        stream_id,
                                        subject: v.subject(),
                                        payload: v.payload(),
                                        flags:   v.flags(),
                                    });
                                }
                                let _ = store.lock().unwrap().append_batch(&refs, 0);
                                let _ = ack_tx.send(count as u32);
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
            accepted += 1;
        }
    });
    addr
}

async fn connect_client(
    addr: SocketAddr,
    cid: usize,
    ack: Arc<AtomicU64>,
) -> (tokio::net::tcp::OwnedWriteHalf, tokio::task::JoinHandle<()>) {
    let sock = TcpStream::connect(addr).await.unwrap();
    sock.set_nodelay(true).ok();
    let (mut rh, wh) = sock.into_split();
    let reader = tokio::spawn(async move {
        let mut buf = BytesMut::with_capacity(8 * 1024);
        loop {
            if buf.capacity() - buf.len() < 1024 {
                buf.reserve(4 * 1024);
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
                        ack.fetch_add(count as u64, Ordering::Release);
                    }
                }
                Err(_) => break,
            }
        }
    });
    (wh, reader)
}

// V1 — Channel<Bytes>, Bytes::from(Vec) before send.
fn run_v1_bytes(rt: &Runtime, cfg: Cfg, prebuilt: Vec<Vec<Vec<u8>>>) -> u128 {
    rt.block_on(async move {
        let store = Arc::new(Mutex::new(MemoryStore::new()));
        let addr = spawn_server(cfg.n_clients, store.clone()).await;
        let acks: Vec<Arc<AtomicU64>> =
            (0..cfg.n_clients).map(|_| Arc::new(AtomicU64::new(0))).collect();

        // Connect all clients (writers + ack readers).
        let mut writers = Vec::with_capacity(cfg.n_clients);
        let mut ack_readers = Vec::with_capacity(cfg.n_clients);
        let mut publish_txs = Vec::with_capacity(cfg.n_clients);
        for cid in 0..cfg.n_clients {
            let (mut wh, reader) = connect_client(addr, cid, acks[cid].clone()).await;
            ack_readers.push(reader);

            let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(8192);
            publish_txs.push(tx);
            writers.push(tokio::spawn(async move {
                while let Some(b) = rx.recv().await {
                    if wh.write_all(&b).await.is_err() { break; }
                }
                let _ = wh.shutdown().await;
            }));
        }

        // Wait a tick for connections to be fully established server-side.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for (cid, frames) in prebuilt.into_iter().enumerate() {
            let tx = publish_txs[cid].clone();
            let ack = acks[cid].clone();
            let k = cfg.k;
            pubs.push(tokio::spawn(async move {
                let mut sent_entries = 0u64;
                for frame in frames {
                    if tx.send(Bytes::from(frame)).await.is_err() { break; }
                    sent_entries += k as u64;
                    while ack.load(Ordering::Acquire) < sent_entries {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }
        for h in pubs { h.await.unwrap(); }
        let elapsed = t0.elapsed().as_nanos();

        // Cleanup.
        drop(publish_txs);
        for h in writers { let _ = h.await; }
        for h in ack_readers { h.abort(); let _ = h.await; }
        elapsed
    })
}

// V2 — Channel<Vec<u8>>, send Vec directly.
fn run_v2_vec(rt: &Runtime, cfg: Cfg, prebuilt: Vec<Vec<Vec<u8>>>) -> u128 {
    rt.block_on(async move {
        let store = Arc::new(Mutex::new(MemoryStore::new()));
        let addr = spawn_server(cfg.n_clients, store.clone()).await;
        let acks: Vec<Arc<AtomicU64>> =
            (0..cfg.n_clients).map(|_| Arc::new(AtomicU64::new(0))).collect();

        let mut writers = Vec::with_capacity(cfg.n_clients);
        let mut ack_readers = Vec::with_capacity(cfg.n_clients);
        let mut publish_txs = Vec::with_capacity(cfg.n_clients);
        for cid in 0..cfg.n_clients {
            let (mut wh, reader) = connect_client(addr, cid, acks[cid].clone()).await;
            ack_readers.push(reader);

            let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(8192);
            publish_txs.push(tx);
            writers.push(tokio::spawn(async move {
                while let Some(v) = rx.recv().await {
                    if wh.write_all(&v).await.is_err() { break; }
                }
                let _ = wh.shutdown().await;
            }));
        }

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for (cid, frames) in prebuilt.into_iter().enumerate() {
            let tx = publish_txs[cid].clone();
            let ack = acks[cid].clone();
            let k = cfg.k;
            pubs.push(tokio::spawn(async move {
                let mut sent_entries = 0u64;
                for frame in frames {
                    if tx.send(frame).await.is_err() { break; }   // Vec<u8> directo
                    sent_entries += k as u64;
                    while ack.load(Ordering::Acquire) < sent_entries {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }
        for h in pubs { h.await.unwrap(); }
        let elapsed = t0.elapsed().as_nanos();

        drop(publish_txs);
        for h in writers { let _ = h.await; }
        for h in ack_readers { h.abort(); let _ = h.await; }
        elapsed
    })
}

fn main() {
    let cfg = Cfg {
        n_clients:   env_usize("BENCH_CLIENT_CONNS", 16),
        frames:      env_usize("BENCH_CLIENT_FRAMES", 100),
        k:           env_usize("BENCH_CLIENT_K", 256),
        payload_len: env_usize("BENCH_CLIENT_PAYLOAD", 256),
        rounds:      env_usize("BENCH_CLIENT_ROUNDS", 5),
    };
    let workers = env_usize("BENCH_CLIENT_WORKERS", 8);

    let entry_size = PUBLISH_ENTRY_SIZE + SUBJECT.len() + cfg.payload_len;
    let frame_size = ENVELOPE_SIZE + 4 + cfg.k * entry_size;

    println!("=== client_only_frame (CLIENT pre-built + REAL server with TCP ack) ===");
    println!("clients={}  frames/client={}  K={}  payload={}B  rounds={}  workers={}",
             cfg.n_clients, cfg.frames, cfg.k, cfg.payload_len, cfg.rounds, workers);
    println!("frame size = {} B   total entries/round = {}",
             frame_size, cfg.n_clients * cfg.frames * cfg.k);
    println!();
    println!("{:<48} {:>12} {:>12} {:>14}",
             "variant", "min ms", "p50 ms", "entries/sec");
    println!("{}", "─".repeat(90));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap();

    // Warmup.
    let _ = run_v1_bytes(&rt, cfg, prebuild_frames(cfg));
    let _ = run_v2_vec(&rt, cfg, prebuild_frames(cfg));

    let mut s1: Vec<u128> = (0..cfg.rounds)
        .map(|_| run_v1_bytes(&rt, cfg, prebuild_frames(cfg))).collect();
    let mut s2: Vec<u128> = (0..cfg.rounds)
        .map(|_| run_v2_vec(&rt, cfg, prebuild_frames(cfg))).collect();
    s1.sort();
    s2.sort();

    let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as f64;
    let report = |name: &str, s: &[u128]| {
        let min_ns = s[0] as f64;
        let p50_ns = s[s.len() / 2] as f64;
        println!("{:<48} {:>12.2} {:>12.2} {:>14.0}",
                 name,
                 min_ns / 1e6,
                 p50_ns / 1e6,
                 total_entries * 1e9 / min_ns);
    };
    report("V1 Bytes::from(frame) → Sender<Bytes>", &s1);
    report("V2 Vec<u8> directo    → Sender<Vec<u8>>", &s2);

    println!();
    let speedup = s1[0] as f64 / s2[0] as f64;
    if speedup > 1.0 {
        println!("V2 (sin Bytes wrap) es {:.2}× más rápido", speedup);
    } else {
        println!("V1 (con Bytes) es {:.2}× más rápido", 1.0 / speedup);
    }
    println!("Done.");
}
