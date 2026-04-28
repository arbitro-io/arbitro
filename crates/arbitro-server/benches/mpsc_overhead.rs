//! mpsc_overhead — measures the cost of mpsc on each side of TCP.
//!
//! Three scenarios, mismo workload:
//!
//!   S1  encode → mpsc → TCP → mpsc → decode    (lo que hace arbitro hoy)
//!   S2  encode → TCP → mpsc → decode           (cliente directo, server con mpsc)
//!   S3  encode → TCP → decode                  (raw TCP, sin mpsc)
//!
//! Cada cliente construye frames POR ITER (matching real arbitro-client) y los
//! envía. Server cuenta entries recibidas. El timer corre hasta que el server
//! procesó todo. NO hay ack — pure forward throughput.

#![allow(unused)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use std::thread;

use arbitro_kit::route::{Hub, Mpmc};
use arbitro_kit::stream::{Ring, Stream};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Handle, Runtime};
use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, IntoBytes};

use arbitro_proto::action::Action;
use arbitro_proto::wire::envelope::{Envelope, FrameView, ENVELOPE_SIZE};
use arbitro_proto::wire::publish::{BatchIter, PublishEntry, PUBLISH_ENTRY_SIZE};

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
fn build_frame(cfg: Cfg, cid: usize, entry_hdr: &PublishEntry, payload: &[u8]) -> Vec<u8> {
    let entry_size = PUBLISH_ENTRY_SIZE + SUBJECT.len() + payload.len();
    let body_len = 4 + cfg.k * entry_size;
    let frame_len = ENVELOPE_SIZE + body_len;

    let mut body: Vec<u8> = Vec::with_capacity(body_len);
    body.extend_from_slice(&(cfg.k as u32).to_le_bytes());
    for _ in 0..cfg.k {
        body.extend_from_slice(entry_hdr.as_bytes());
        body.extend_from_slice(SUBJECT);
        body.extend_from_slice(payload);
    }
    let envelope = Envelope::new(Action::Publish, cid as u32, body_len as u32, 0);
    let mut frame: Vec<u8> = Vec::with_capacity(frame_len);
    frame.extend_from_slice(envelope.as_bytes());
    frame.extend_from_slice(&body);
    frame
}

// ── Server side, with mpsc (S1, S2): reader → mpsc → decoder ─────────────

async fn spawn_server_with_mpsc(
    n_clients: usize, total_entries: u64,
    received: Arc<AtomicU64>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut accepted = 0;
        while accepted < n_clients {
            let (mut sock, _) = listener.accept().await.unwrap();
            sock.set_nodelay(true).ok();
            let received = received.clone();

            // Per-conn: mpsc<Bytes> → decoder task.
            let (frame_tx, mut frame_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
            tokio::spawn(async move {
                while let Some(frame) = frame_rx.recv().await {
                    let view = FrameView::new(&frame);
                    let count = BatchIter::new(view.body()).count() as u64;
                    received.fetch_add(count, Ordering::Relaxed);
                }
            });

            // Reader task: TCP → frame_tx.
            tokio::spawn(async move {
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
                                let _ = frame_tx.send(frame);
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

// ── Server side, NO mpsc (S3): reader processes inline ───────────────────

async fn spawn_server_inline(
    n_clients: usize,
    received: Arc<AtomicU64>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut accepted = 0;
        while accepted < n_clients {
            let (mut sock, _) = listener.accept().await.unwrap();
            sock.set_nodelay(true).ok();
            let received = received.clone();
            tokio::spawn(async move {
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
                                // INLINE decode (no mpsc).
                                let view = FrameView::new(&frame);
                                let count = BatchIter::new(view.body()).count() as u64;
                                received.fetch_add(count, Ordering::Relaxed);
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

// ── S1: encode → mpsc → TCP → mpsc → decode ────────────────────────────

fn run_s1(rt: &Runtime, cfg: Cfg) -> u128 {
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));
        let addr = spawn_server_with_mpsc(cfg.n_clients, total_entries, received.clone()).await;

        // Establecer conexiones (writer task per conn drena mpsc → TCP).
        let mut publish_txs = Vec::with_capacity(cfg.n_clients);
        let mut writers = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let mut sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            let (_rh, mut wh) = sock.into_split();
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(8192);
            publish_txs.push(tx);
            writers.push(tokio::spawn(async move {
                while let Some(b) = rx.recv().await {
                    if wh.write_all(&b).await.is_err() { break; }
                }
                let _ = wh.shutdown().await;
            }));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for cid in 0..cfg.n_clients {
            let tx = publish_txs[cid].clone();
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    if tx.send(Bytes::from(frame)).await.is_err() { break; }
                }
            }));
        }
        for h in pubs { h.await.unwrap(); }

        // Esperar que server procese todo.
        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        let elapsed = t0.elapsed().as_nanos();

        drop(publish_txs);
        for h in writers { let _ = h.await; }
        elapsed
    })
}

// ── S2: encode → TCP → mpsc → decode ──────────────────────────────────

fn run_s2(rt: &Runtime, cfg: Cfg) -> u128 {
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));
        let addr = spawn_server_with_mpsc(cfg.n_clients, total_entries, received.clone()).await;

        // Cliente: tokio task con socket directo (sin writer task, sin mpsc).
        let mut socks = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            socks.push(sock);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for (cid, mut sock) in socks.into_iter().enumerate() {
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    if sock.write_all(&frame).await.is_err() { break; }
                }
                let _ = sock.shutdown().await;
            }));
        }
        for h in pubs { h.await.unwrap(); }

        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        t0.elapsed().as_nanos()
    })
}

// ── S3: encode → TCP → decode (sin mpsc anywhere) ─────────────────────

fn run_s3(rt: &Runtime, cfg: Cfg) -> u128 {
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));
        let addr = spawn_server_inline(cfg.n_clients, received.clone()).await;

        let mut socks = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            socks.push(sock);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for (cid, mut sock) in socks.into_iter().enumerate() {
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    if sock.write_all(&frame).await.is_err() { break; }
                }
                let _ = sock.shutdown().await;
            }));
        }
        for h in pubs { h.await.unwrap(); }

        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        t0.elapsed().as_nanos()
    })
}

// ── Server side using kit::Stream<Bytes> + sync decoder thread ───────────

async fn spawn_server_kit(
    n_clients: usize,
    received: Arc<AtomicU64>,
    handle: Handle,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let rec = received.clone();
    tokio::spawn(async move {
        let mut accepted = 0;
        while accepted < n_clients {
            let (mut sock, _) = listener.accept().await.unwrap();
            sock.set_nodelay(true).ok();
            let received = rec.clone();

            // Per-conn kit::Stream<Bytes> + dedicated SYNC decoder thread.
            let kit_stream: Arc<Stream<Bytes>> = Arc::new(Stream::new());
            let kit_for_decoder = kit_stream.clone();
            thread::spawn(move || {
                kit_for_decoder.set_consumer(thread::current());
                loop {
                    let frame = kit_for_decoder.recv();
                    let view = FrameView::new(&frame);
                    let count = BatchIter::new(view.body()).count() as u64;
                    received.fetch_add(count, Ordering::Relaxed);
                }
            });

            // Reader task: TCP → kit::Stream.
            tokio::spawn(async move {
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
                                kit_stream.send(frame);
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

// ── S4: encode → kit::Stream → TCP → kit::Stream → decode ───────────────
//
// Cliente: tokio task encodea + push a kit::Stream<Bytes>.
//          SYNC writer thread drena kit::Stream y escribe a TCP via Handle.block_on.
// Server: TCP reader task → kit::Stream → SYNC decoder thread.

fn run_s4(rt: &Runtime, cfg: Cfg) -> u128 {
    let handle = rt.handle().clone();
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));
        let addr = spawn_server_kit(cfg.n_clients, received.clone(), handle.clone()).await;

        // Cliente: kit::Stream + sync writer thread por conn.
        let mut publish_streams: Vec<Arc<Stream<Bytes>>> = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            let (_rh, wh) = sock.into_split();
            let kit_stream: Arc<Stream<Bytes>> = Arc::new(Stream::new());
            publish_streams.push(kit_stream.clone());

            let handle_for_writer = handle.clone();
            let kit_for_writer = kit_stream.clone();
            thread::spawn(move || {
                kit_for_writer.set_consumer(thread::current());
                let mut wh = wh;
                loop {
                    let frame = kit_for_writer.recv();
                    if handle_for_writer.block_on(wh.write_all(&frame)).is_err() { break; }
                }
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for cid in 0..cfg.n_clients {
            let stream = publish_streams[cid].clone();
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    stream.send(Bytes::from(frame));
                }
            }));
        }
        for h in pubs { h.await.unwrap(); }

        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        t0.elapsed().as_nanos()
    })
}

// ── Server side using kit::Ring<Bytes, CAP> + sync decoder thread ───────

const RING_CAP: usize = 8192;

async fn spawn_server_ring(
    n_clients: usize,
    received: Arc<AtomicU64>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let rec = received.clone();
    tokio::spawn(async move {
        let mut accepted = 0;
        while accepted < n_clients {
            let (mut sock, _) = listener.accept().await.unwrap();
            sock.set_nodelay(true).ok();
            let received = rec.clone();

            let kit_ring: Arc<Ring<Bytes, RING_CAP>> = Arc::new(Ring::new());
            let ring_for_decoder = kit_ring.clone();
            thread::spawn(move || {
                ring_for_decoder.set_consumer(thread::current());
                loop {
                    let frame = ring_for_decoder.recv();
                    let view = FrameView::new(&frame);
                    let count = BatchIter::new(view.body()).count() as u64;
                    received.fetch_add(count, Ordering::Relaxed);
                }
            });

            tokio::spawn(async move {
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
                                // Ring is bounded — try_send with yield_now backoff.
                                let mut f = frame;
                                loop {
                                    match kit_ring.try_send(f) {
                                        Ok(_) => break,
                                        Err(returned) => {
                                            f = returned;
                                            tokio::task::yield_now().await;
                                        }
                                    }
                                }
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

// ── S5: encode → kit::Ring → TCP → kit::Ring → decode ───────────────────

fn run_s5(rt: &Runtime, cfg: Cfg) -> u128 {
    let handle = rt.handle().clone();
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));
        let addr = spawn_server_ring(cfg.n_clients, received.clone()).await;

        let mut publish_rings: Vec<Arc<Ring<Bytes, RING_CAP>>> = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            let (_rh, wh) = sock.into_split();
            let kit_ring: Arc<Ring<Bytes, RING_CAP>> = Arc::new(Ring::new());
            publish_rings.push(kit_ring.clone());

            let handle_for_writer = handle.clone();
            let ring_for_writer = kit_ring.clone();
            thread::spawn(move || {
                ring_for_writer.set_consumer(thread::current());
                let mut wh = wh;
                loop {
                    let frame = ring_for_writer.recv();
                    if handle_for_writer.block_on(wh.write_all(&frame)).is_err() { break; }
                }
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for cid in 0..cfg.n_clients {
            let ring = publish_rings[cid].clone();
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    let mut f = Bytes::from(frame);
                    loop {
                        match ring.try_send(f) {
                            Ok(_) => break,
                            Err(returned) => {
                                f = returned;
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                }
            }));
        }
        for h in pubs { h.await.unwrap(); }

        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        t0.elapsed().as_nanos()
    })
}

// ── S6: encode → TCP → kit::Stream → decode (kit solo server) ───────────

fn run_s6(rt: &Runtime, cfg: Cfg) -> u128 {
    let handle = rt.handle().clone();
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));
        let addr = spawn_server_kit(cfg.n_clients, received.clone(), handle).await;

        let mut socks = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            socks.push(sock);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for (cid, mut sock) in socks.into_iter().enumerate() {
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    if sock.write_all(&frame).await.is_err() { break; }
                }
                let _ = sock.shutdown().await;
            }));
        }
        for h in pubs { h.await.unwrap(); }

        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        t0.elapsed().as_nanos()
    })
}

// ── S7: encode → TCP → kit::Ring → decode (kit::Ring solo server) ──────

fn run_s7(rt: &Runtime, cfg: Cfg) -> u128 {
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));
        let addr = spawn_server_ring(cfg.n_clients, received.clone()).await;

        let mut socks = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            socks.push(sock);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for (cid, mut sock) in socks.into_iter().enumerate() {
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    if sock.write_all(&frame).await.is_err() { break; }
                }
                let _ = sock.shutdown().await;
            }));
        }
        for h in pubs { h.await.unwrap(); }

        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        t0.elapsed().as_nanos()
    })
}

// ── S8: M conns → SHARED tokio::mpsc → 1 sync decoder thread (TRUE MPSC) ─

fn run_s8(rt: &Runtime, cfg: Cfg) -> u128 {
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));

        // ONE shared mpsc, ONE sync decoder thread.
        let (frame_tx, mut frame_rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
        let received_t = received.clone();
        thread::spawn(move || {
            while let Some(frame) = frame_rx.blocking_recv() {
                let view = FrameView::new(&frame);
                let count = BatchIter::new(view.body()).count() as u64;
                received_t.fetch_add(count, Ordering::Relaxed);
            }
        });

        // Server: tokio acceptor + N readers, ALL using cloned frame_tx.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let n_clients_e = cfg.n_clients;
        let tx_for_acc = frame_tx.clone();
        tokio::spawn(async move {
            let mut accepted = 0;
            while accepted < n_clients_e {
                let (mut sock, _) = listener.accept().await.unwrap();
                sock.set_nodelay(true).ok();
                let tx = tx_for_acc.clone();
                tokio::spawn(async move {
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
                                    let _ = tx.send(frame);
                                }
                            }
                            Err(_) => break,
                        }
                    }
                });
                accepted += 1;
            }
        });
        drop(frame_tx);  // acceptor's clone keeps it alive while conns alive

        // Clients: tokio task con socket directo, fire frames.
        let mut socks = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            socks.push(sock);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for (cid, mut sock) in socks.into_iter().enumerate() {
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    if sock.write_all(&frame).await.is_err() { break; }
                }
                let _ = sock.shutdown().await;
            }));
        }
        for h in pubs { h.await.unwrap(); }

        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        t0.elapsed().as_nanos()
    })
}

// ── S9: M conns → SHARED kit::Mpmc → 1 sync decoder thread ──────────────

fn run_s9(rt: &Runtime, cfg: Cfg) -> u128 {
    // kit::Mpmc supports M up to 255 via chunked SignalSet (post 3cb6f6f).
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));

        // Mpmc<Bytes, RING_CAP>::new(M, 1) — M producers, 1 consumer.
        // RING_CAP=64 → con M=16 conns, total slots = 1024 (matches arbitro mpsc 4096
        // dividido entre M). Total: 16 × 64 = 1024 slots de backpressure agregada.
        let (producers, mut consumers, _shutdown) = Mpmc::<Bytes, 64>::new(cfg.n_clients, 1);
        let consumer = consumers.pop().unwrap();

        // Sync decoder thread.
        let received_t = received.clone();
        thread::spawn(move || {
            consumer.bind();
            while let Ok(frame) = consumer.recv() {
                let view = FrameView::new(&frame);
                let count = BatchIter::new(view.body()).count() as u64;
                received_t.fetch_add(count, Ordering::Relaxed);
            }
        });

        // Server: tokio acceptor. Each accepted socket gets ONE producer.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let producers_arc = Arc::new(Mutex::new(producers));
        let producers_for_acc = producers_arc.clone();
        let n_clients_e = cfg.n_clients;
        tokio::spawn(async move {
            let mut accepted = 0;
            while accepted < n_clients_e {
                let (mut sock, _) = listener.accept().await.unwrap();
                sock.set_nodelay(true).ok();
                let producer = producers_for_acc.lock().unwrap().pop().unwrap();
                tokio::spawn(async move {
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
                                    // Mpmc try_send + retry.
                                    let mut f = frame;
                                    loop {
                                        match producer.try_send(f) {
                                            Ok(_) => break,
                                            Err(returned) => {
                                                f = returned;
                                                tokio::task::yield_now().await;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                });
                accepted += 1;
            }
        });

        // Clients.
        let mut socks = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            socks.push(sock);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for (cid, mut sock) in socks.into_iter().enumerate() {
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    if sock.write_all(&frame).await.is_err() { break; }
                }
                let _ = sock.shutdown().await;
            }));
        }
        for h in pubs { h.await.unwrap(); }

        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        t0.elapsed().as_nanos()
    })
}

// ── ChunkedMpmc: M:1 con bitmap dinámico (Box<[AtomicU64]>) → soporta M arbitrario ──
//
// Reemplaza el SignalSet (1 AtomicU64, max 63 bits) por Vec<AtomicU64> chunks.
// 1 consumer thread, M productores ilimitados (limit = memoria).
// Usa kit::gate::Park para wake-up sin syscall en hot path.

mod chunked_mpmc {
    use std::cell::UnsafeCell;
    use std::mem::MaybeUninit;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;
    use arbitro_kit::gate::Park;

    #[repr(align(64))]
    struct PRing<T, const CAP: usize> {
        head: AtomicUsize,
        _pad0: [u8; 64 - 8],
        tail: AtomicUsize,
        _pad1: [u8; 64 - 8],
        slots: [UnsafeCell<MaybeUninit<T>>; CAP],
    }

    impl<T, const CAP: usize> PRing<T, CAP> {
        const MASK: usize = CAP - 1;
        fn new() -> Self {
            Self {
                head: AtomicUsize::new(0),
                _pad0: [0; 56],
                tail: AtomicUsize::new(0),
                _pad1: [0; 56],
                slots: std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit())),
            }
        }
        fn is_full(h: usize, t: usize) -> bool { h.wrapping_sub(t) >= CAP }
    }

    pub struct ChunkedMpmc<T: Send, const CAP: usize> {
        bitmap: Box<[AtomicU64]>,       // chunks de 64 bits cada uno
        rings: Box<[PRing<T, CAP>]>,    // M rings
        consumer_park: Park,
        shutdown: AtomicBool,
        m: usize,
    }

    unsafe impl<T: Send, const CAP: usize> Sync for ChunkedMpmc<T, CAP> {}
    unsafe impl<T: Send, const CAP: usize> Send for ChunkedMpmc<T, CAP> {}

    impl<T: Send, const CAP: usize> ChunkedMpmc<T, CAP> {
        pub fn new(m: usize) -> Arc<Self> {
            assert!(m > 0, "m must be > 0");
            assert!(CAP > 0 && CAP.is_power_of_two(), "CAP must be power of 2");
            let n_chunks = (m + 63) / 64;
            let bitmap: Vec<AtomicU64> = (0..n_chunks).map(|_| AtomicU64::new(0)).collect();
            let rings: Vec<PRing<T, CAP>> = (0..m).map(|_| PRing::new()).collect();
            Arc::new(ChunkedMpmc {
                bitmap: bitmap.into_boxed_slice(),
                rings: rings.into_boxed_slice(),
                consumer_park: Park::new(),
                shutdown: AtomicBool::new(false),
                m,
            })
        }

        pub fn bind_consumer(&self) {
            self.consumer_park.set_worker(std::thread::current());
        }

        /// Producer p envía a su ring. Retorna Err(value) si lleno.
        pub fn try_send(&self, p: usize, value: T) -> Result<(), T> {
            assert!(p < self.m);
            let ring = &self.rings[p];
            let h = ring.head.load(Ordering::Relaxed);
            let t = ring.tail.load(Ordering::Acquire);
            if PRing::<T, CAP>::is_full(h, t) { return Err(value); }
            unsafe {
                (*ring.slots[h & PRing::<T, CAP>::MASK].get()).write(value);
            }
            ring.head.store(h.wrapping_add(1), Ordering::Release);
            // Set bit in bitmap.
            let chunk = p / 64;
            let bit = p % 64;
            self.bitmap[chunk].fetch_or(1u64 << bit, Ordering::Release);
            // Wake consumer if parked.
            self.consumer_park.wake();
            Ok(())
        }

        /// Consumer drena. Returns Some(item) o None si todo vacío.
        pub fn try_recv(&self) -> Option<T> {
            for (chunk_idx, atomic) in self.bitmap.iter().enumerate() {
                let state = atomic.load(Ordering::Acquire);
                if state == 0 { continue; }
                let mut remaining = state;
                while remaining != 0 {
                    let bit_pos = remaining.trailing_zeros() as usize;
                    let p = chunk_idx * 64 + bit_pos;
                    let bit = 1u64 << bit_pos;
                    remaining &= !bit;
                    if p >= self.m { break; }
                    let ring = &self.rings[p];
                    let t = ring.tail.load(Ordering::Relaxed);
                    let h = ring.head.load(Ordering::Acquire);
                    if t == h {
                        // Stale bit; clear and continue.
                        atomic.fetch_and(!bit, Ordering::Relaxed);
                        continue;
                    }
                    let v = unsafe {
                        (*ring.slots[t & PRing::<T, CAP>::MASK].get())
                            .assume_init_read()
                    };
                    ring.tail.store(t.wrapping_add(1), Ordering::Release);
                    return Some(v);
                }
            }
            None
        }

        /// Blocking recv. Parks via consumer_park hasta que cualquier bit esté set.
        pub fn recv(&self) -> Result<T, ()> {
            loop {
                if let Some(v) = self.try_recv() { return Ok(v); }
                if self.shutdown.load(Ordering::Acquire) { return Err(()); }
                // Park hasta que haya data o shutdown.
                self.consumer_park.wait_until(|| {
                    if self.shutdown.load(Ordering::Acquire) { return true; }
                    for atomic in self.bitmap.iter() {
                        if atomic.load(Ordering::Acquire) != 0 { return true; }
                    }
                    false
                });
            }
        }

        pub fn signal_shutdown(&self) {
            self.shutdown.store(true, Ordering::Release);
            self.consumer_park.wake();
        }
    }
}

use chunked_mpmc::ChunkedMpmc;

// ── S11: M conns → ChunkedMpmc (M arbitrary) → 1 sync decoder ─────────────

fn run_s11(rt: &Runtime, cfg: Cfg) -> u128 {
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));
        let mpmc = ChunkedMpmc::<Bytes, 64>::new(cfg.n_clients);

        // Sync decoder thread.
        let mpmc_t = mpmc.clone();
        let received_t = received.clone();
        thread::spawn(move || {
            mpmc_t.bind_consumer();
            while let Ok(frame) = mpmc_t.recv() {
                let view = FrameView::new(&frame);
                let count = BatchIter::new(view.body()).count() as u64;
                received_t.fetch_add(count, Ordering::Relaxed);
            }
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mpmc_acc = mpmc.clone();
        let n_clients_e = cfg.n_clients;
        tokio::spawn(async move {
            let mut accepted = 0;
            while accepted < n_clients_e {
                let (mut sock, _) = listener.accept().await.unwrap();
                sock.set_nodelay(true).ok();
                let mpmc = mpmc_acc.clone();
                let producer_id = accepted;
                tokio::spawn(async move {
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
                                    let mut f = frame;
                                    loop {
                                        match mpmc.try_send(producer_id, f) {
                                            Ok(_) => break,
                                            Err(returned) => {
                                                f = returned;
                                                tokio::task::yield_now().await;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                });
                accepted += 1;
            }
        });

        let mut socks = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            socks.push(sock);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for (cid, mut sock) in socks.into_iter().enumerate() {
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    if sock.write_all(&frame).await.is_err() { break; }
                }
                let _ = sock.shutdown().await;
            }));
        }
        for h in pubs { h.await.unwrap(); }

        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        let elapsed = t0.elapsed().as_nanos();
        mpmc.signal_shutdown();
        elapsed
    })
}

// ── S12: M conns → kit::Hub (named ports, 1 inbound slot each) → 1 sync decoder
//
// Hub is the named-port M:1 primitive: M producers, each with its own port,
// N ≤ 63 (bit 63 reserved for shutdown). Hot-path: each port has a 1-slot
// inbound (NO ring), so the producer must wait for the drain to pick up
// the value before the next send. Compared to Mpmc (RING_CAP=64 per producer),
// Hub is high-backpressure / low-buffering — meant for low-N RPC, not
// high-throughput fan-in. We measure it here for completeness.

fn run_s12_hub(rt: &Runtime, cfg: Cfg) -> u128 {
    if cfg.n_clients > arbitro_kit::route::MAX_HUB_PORTS {
        // Hub caps at 63 ports (bit 63 reserved for shutdown). M > 63: skip.
        return u128::MAX;
    }
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));

        // Hub<In=Bytes, Out=()>: M ports, no reply payload (we ignore replies).
        let (drain, ports) = Hub::<Bytes, ()>::new(cfg.n_clients);
        let _shutdown = drain.shutdown_handle();

        // Sync decoder thread.
        let received_t = received.clone();
        thread::spawn(move || {
            drain.bind();
            loop {
                match drain.recv_batch(|_idx, frame, _reply| {
                    let view = FrameView::new(&frame);
                    let count = BatchIter::new(view.body()).count() as u64;
                    received_t.fetch_add(count, Ordering::Relaxed);
                    // Reply is dropped without sending — Out=() and the
                    // producer never calls recv_reply, so no backpressure
                    // through the outbound pipe. Inbound slot is freed by
                    // recv_batch before the closure runs.
                }) {
                    Ok(()) => {}
                    Err(_shutdown) => break,
                }
            }
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ports_arc = Arc::new(Mutex::new(ports));
        let ports_for_acc = ports_arc.clone();
        let n_clients_e = cfg.n_clients;
        tokio::spawn(async move {
            let mut accepted = 0;
            while accepted < n_clients_e {
                let (mut sock, _) = listener.accept().await.unwrap();
                sock.set_nodelay(true).ok();
                let port = ports_for_acc.lock().unwrap().pop().unwrap();
                tokio::spawn(async move {
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
                                    // Hub::try_send returns Err if port is busy
                                    // (1-slot inbound). Yield and retry.
                                    let mut f = frame;
                                    loop {
                                        match port.try_send(f) {
                                            Ok(()) => break,
                                            Err(returned) => {
                                                f = returned;
                                                tokio::task::yield_now().await;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                });
                accepted += 1;
            }
        });

        let mut socks = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            socks.push(sock);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for (cid, mut sock) in socks.into_iter().enumerate() {
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    if sock.write_all(&frame).await.is_err() { break; }
                }
                let _ = sock.shutdown().await;
            }));
        }
        for h in pubs { h.await.unwrap(); }

        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        t0.elapsed().as_nanos()
    })
}

// ── S13: M conns → LANE-HASHED kit::Mpmc with N producers behind Mutex → 1 sync decoder
//
// This models the realistic server-side integration: a fixed pool of N
// producers per shard, every connection hashes by conn_id to one lane,
// briefly locks the Mutex, calls try_send, releases. This is what the
// server gets if we adopt Option A (Mutex-protected producer lanes,
// keeping ShardHandle Clone). The Mutex hold is ~50 ns; under N=16 with
// M conns hashing across them, contention is rare.

fn run_s13_lanes(rt: &Runtime, cfg: Cfg) -> u128 {
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));

        // 16 producer lanes — pool size independent of conn count.
        const LANES: usize = 16;
        let (producers, mut consumers, _shutdown) =
            Mpmc::<Bytes, 64>::new(LANES, 1);
        let consumer = consumers.pop().unwrap();

        let lanes: Arc<Vec<std::sync::Mutex<arbitro_kit::route::MpmcProducer<Bytes, 64>>>> =
            Arc::new(producers.into_iter().map(std::sync::Mutex::new).collect());

        let received_t = received.clone();
        thread::spawn(move || {
            consumer.bind();
            while let Ok(frame) = consumer.recv() {
                let view = FrameView::new(&frame);
                let count = BatchIter::new(view.body()).count() as u64;
                received_t.fetch_add(count, Ordering::Relaxed);
            }
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let lanes_acc = lanes.clone();
        let n_clients_e = cfg.n_clients;
        tokio::spawn(async move {
            let mut accepted = 0;
            while accepted < n_clients_e {
                let (mut sock, _) = listener.accept().await.unwrap();
                sock.set_nodelay(true).ok();
                let lanes = lanes_acc.clone();
                let conn_id = accepted as u64;
                tokio::spawn(async move {
                    let lane_idx = (conn_id as usize) % LANES;
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
                                    let mut f = frame;
                                    loop {
                                        // Lock briefly, try_send, drop guard.
                                        let res = {
                                            let g = lanes[lane_idx].lock().unwrap();
                                            g.try_send(f)
                                        };
                                        match res {
                                            Ok(()) => break,
                                            Err(returned) => {
                                                f = returned;
                                                tokio::task::yield_now().await;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                });
                accepted += 1;
            }
        });

        let mut socks = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            socks.push(sock);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for (cid, mut sock) in socks.into_iter().enumerate() {
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    if sock.write_all(&frame).await.is_err() { break; }
                }
                let _ = sock.shutdown().await;
            }));
        }
        for h in pubs { h.await.unwrap(); }

        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        t0.elapsed().as_nanos()
    })
}

// ── S10: M conns → SHARDED kit::Mpmc (multiple instances of 63) → N decoder threads
//
// Workaround para el limit M ≤ 63 de kit::Mpmc:
// 1. Crea ceil(M / 63) instances de Mpmc, cada uno con producers=63 max.
// 2. Cada Mpmc tiene su propio decoder thread.
// 3. Todos los decoder threads incrementan un counter shared.
// Permite M arbitrario, a costo de múltiples decoder threads.

fn run_s10(rt: &Runtime, cfg: Cfg) -> u128 {
    rt.block_on(async move {
        let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as u64;
        let received = Arc::new(AtomicU64::new(0));

        const PER_CHUNK: usize = 63;
        let n_chunks = (cfg.n_clients + PER_CHUNK - 1) / PER_CHUNK;

        let mut all_producers: Vec<Box<dyn Send + Sync>> = Vec::with_capacity(cfg.n_clients);
        // Ad-hoc: collect producers across chunks. We use a Vec<Option<MpmcProducer>>
        // so each conn task can take its own producer.
        let mut producer_pool: Vec<Option<arbitro_kit::route::MpmcProducer<Bytes, 64>>> =
            Vec::with_capacity(cfg.n_clients);

        for chunk_idx in 0..n_chunks {
            let m_chunk = if chunk_idx == n_chunks - 1 {
                cfg.n_clients - chunk_idx * PER_CHUNK
            } else {
                PER_CHUNK
            };
            let (producers, mut consumers, _shutdown) =
                Mpmc::<Bytes, 64>::new(m_chunk, 1);
            for p in producers {
                producer_pool.push(Some(p));
            }
            let consumer = consumers.pop().unwrap();
            let received_t = received.clone();
            thread::spawn(move || {
                consumer.bind();
                while let Ok(frame) = consumer.recv() {
                    let view = FrameView::new(&frame);
                    let count = BatchIter::new(view.body()).count() as u64;
                    received_t.fetch_add(count, Ordering::Relaxed);
                }
            });
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let producer_pool_arc = Arc::new(Mutex::new(producer_pool));
        let pool_for_acc = producer_pool_arc.clone();
        let n_clients_e = cfg.n_clients;
        tokio::spawn(async move {
            let mut accepted = 0;
            while accepted < n_clients_e {
                let (mut sock, _) = listener.accept().await.unwrap();
                sock.set_nodelay(true).ok();
                let producer = pool_for_acc.lock().unwrap()[accepted].take().unwrap();
                tokio::spawn(async move {
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
                                    let mut f = frame;
                                    loop {
                                        match producer.try_send(f) {
                                            Ok(_) => break,
                                            Err(returned) => {
                                                f = returned;
                                                tokio::task::yield_now().await;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                });
                accepted += 1;
            }
        });

        let mut socks = Vec::with_capacity(cfg.n_clients);
        for _ in 0..cfg.n_clients {
            let sock = TcpStream::connect(addr).await.unwrap();
            sock.set_nodelay(true).ok();
            socks.push(sock);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let payload = vec![0u8; cfg.payload_len];
        let entry_hdr = PublishEntry {
            data_len:  U32::new(payload.len() as u32),
            subj_len:  U16::new(SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };

        let t0 = Instant::now();
        let mut pubs = Vec::with_capacity(cfg.n_clients);
        for (cid, mut sock) in socks.into_iter().enumerate() {
            let payload = payload.clone();
            pubs.push(tokio::spawn(async move {
                for _ in 0..cfg.frames {
                    let frame = build_frame(cfg, cid, &entry_hdr, &payload);
                    if sock.write_all(&frame).await.is_err() { break; }
                }
                let _ = sock.shutdown().await;
            }));
        }
        for h in pubs { h.await.unwrap(); }

        while received.load(Ordering::Acquire) < total_entries {
            tokio::task::yield_now().await;
        }
        t0.elapsed().as_nanos()
    })
}

fn main() {
    let cfg = Cfg {
        n_clients:   env_usize("BENCH_CONNS", 16),
        frames:      env_usize("BENCH_FRAMES", 100),
        k:           env_usize("BENCH_K", 256),
        payload_len: env_usize("BENCH_PAYLOAD", 256),
        rounds:      env_usize("BENCH_ROUNDS", 5),
    };
    let workers = env_usize("BENCH_WORKERS", 8);

    let entry_size = PUBLISH_ENTRY_SIZE + SUBJECT.len() + cfg.payload_len;
    let frame_size = ENVELOPE_SIZE + 4 + cfg.k * entry_size;
    let total_bytes = (cfg.n_clients * cfg.frames * frame_size) as f64;
    let total_entries = (cfg.n_clients * cfg.frames * cfg.k) as f64;

    println!("=== mpsc_overhead — costo de mpsc en cada lado de TCP ===");
    println!("clients={}  frames/client={}  K={}  payload={}B  rounds={}  workers={}",
             cfg.n_clients, cfg.frames, cfg.k, cfg.payload_len, cfg.rounds, workers);
    println!("frame size = {} B   total = {:.1} MB   total entries = {}",
             frame_size, total_bytes / (1024.0 * 1024.0), total_entries);
    println!();
    println!("{:<60} {:>10} {:>10} {:>14} {:>14}",
             "scenario", "min ms", "p50 ms", "GB/s (min)", "ent/s (min)");
    println!("{}", "─".repeat(116));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap();

    let _ = run_s1(&rt, cfg);
    let _ = run_s2(&rt, cfg);
    let _ = run_s3(&rt, cfg);
    let _ = run_s4(&rt, cfg);
    let _ = run_s5(&rt, cfg);
    let _ = run_s6(&rt, cfg);
    let _ = run_s7(&rt, cfg);
    let _ = run_s8(&rt, cfg);
    // Run S11 (PoC) before S9/S10 — the PoC validates the chunked-bitmap
    // approach in isolation, so if a later run hangs we still have a
    // baseline number for the M:1 scenario.
    let _ = run_s11(&rt, cfg);
    let _ = run_s9(&rt, cfg);
    let _ = run_s10(&rt, cfg);
    let _ = run_s12_hub(&rt, cfg);
    let _ = run_s13_lanes(&rt, cfg);

    let mut s1: Vec<u128> = (0..cfg.rounds).map(|_| run_s1(&rt, cfg)).collect();
    let mut s2: Vec<u128> = (0..cfg.rounds).map(|_| run_s2(&rt, cfg)).collect();
    let mut s3: Vec<u128> = (0..cfg.rounds).map(|_| run_s3(&rt, cfg)).collect();
    let mut s4: Vec<u128> = (0..cfg.rounds).map(|_| run_s4(&rt, cfg)).collect();
    let mut s5: Vec<u128> = (0..cfg.rounds).map(|_| run_s5(&rt, cfg)).collect();
    let mut s6: Vec<u128> = (0..cfg.rounds).map(|_| run_s6(&rt, cfg)).collect();
    let mut s7: Vec<u128> = (0..cfg.rounds).map(|_| run_s7(&rt, cfg)).collect();
    let mut s8:  Vec<u128> = (0..cfg.rounds).map(|_| run_s8(&rt, cfg)).collect();
    // PoC runs before kit::Mpmc/sharded so we always get a number for it.
    let mut s11: Vec<u128> = (0..cfg.rounds).map(|_| run_s11(&rt, cfg)).collect();
    let mut s9:  Vec<u128> = (0..cfg.rounds).map(|_| run_s9(&rt, cfg)).collect();
    let mut s10: Vec<u128> = (0..cfg.rounds).map(|_| run_s10(&rt, cfg)).collect();
    let mut s12: Vec<u128> = (0..cfg.rounds).map(|_| run_s12_hub(&rt, cfg)).collect();
    let mut s13: Vec<u128> = (0..cfg.rounds).map(|_| run_s13_lanes(&rt, cfg)).collect();
    s1.sort(); s2.sort(); s3.sort(); s4.sort(); s5.sort(); s6.sort(); s7.sort(); s8.sort(); s9.sort(); s10.sort(); s11.sort(); s12.sort(); s13.sort();

    let report = |name: &str, s: &[u128]| {
        let min_ns = s[0] as f64;
        let p50_ns = s[s.len() / 2] as f64;
        let gbs = total_bytes / min_ns;   // GB/s = bytes / ns
        let eps = total_entries * 1e9 / min_ns;
        println!("{:<60} {:>10.2} {:>10.2} {:>14.2} {:>14.0}",
                 name, min_ns / 1e6, p50_ns / 1e6, gbs, eps);
    };
    report("S1  encode → mpsc → TCP → mpsc → decode", &s1);
    report("S2  encode → TCP → mpsc → decode",        &s2);
    report("S3  encode → TCP → decode                (raw)", &s3);
    report("S4  encode → kit::Stream → TCP → kit::Stream → decode", &s4);
    report("S5  encode → kit::Ring → TCP → kit::Ring → decode", &s5);
    report("S6  encode → TCP → kit::Stream → decode (kit solo server)", &s6);
    report("S7  encode → TCP → kit::Ring → decode (kit::Ring solo server)", &s7);
    report("S8  M conns → SHARED tokio::mpsc → 1 sync decoder (REAL MPSC)", &s8);
    report("S9  M conns → SHARED kit::Mpmc   → 1 sync decoder (REAL MPSC)", &s9);
    report("S10 M conns → SHARDED kit::Mpmc (chunks of 63) → N decoders", &s10);
    report("S11 M conns → ChunkedMpmc (Box<[AtomicU64]>) → 1 decoder", &s11);
    if s12[0] != u128::MAX {
        report("S12 M conns → kit::Hub (named ports, 1-slot inbound) → 1 decoder", &s12);
    } else {
        println!("{:<60} {:>10} {:>10} {:>14} {:>14}",
                 "S12 M conns → kit::Hub (named ports, 1-slot inbound) → 1 decoder",
                 "skipped", "—", "—", "M>63");
    }
    report("S13 M conns → 16 LANE-HASHED kit::Mpmc producers under Mutex → 1 decoder", &s13);

    println!();
    let s1_v = s1[0] as f64;
    let s2_v = s2[0] as f64;
    let s3_v = s3[0] as f64;
    let s4_v = s4[0] as f64;
    let s5_v = s5[0] as f64;
    let s6_v = s6[0] as f64;
    let s7_v = s7[0] as f64;
    let s8_v = s8[0] as f64;
    let s9_v = s9[0] as f64;
    let s10_v = s10[0] as f64;
    let s11_v = s11[0] as f64;
    println!("Cost of CLIENT mpsc:    S1 vs S2 = {:.2}× ({:.1}% perdido)",
             s1_v / s2_v, (s1_v - s2_v) / s1_v * 100.0);
    println!("Cost of SERVER mpsc:    S2 vs S3 = {:.2}× ({:.1}% perdido)",
             s2_v / s3_v, (s2_v - s3_v) / s2_v * 100.0);
    println!("Cost of BOTH mpsc:      S1 vs S3 = {:.2}× ({:.1}% perdido)",
             s1_v / s3_v, (s1_v - s3_v) / s1_v * 100.0);
    println!("kit::Stream vs mpsc:    S4 vs S1 = {:.2}× ({:.1}% gain)",
             s1_v / s4_v, (s1_v - s4_v) / s1_v * 100.0);
    println!("kit::Stream vs raw TCP: S4 vs S3 = {:.2}× ({:.1}% lost vs raw)",
             s4_v / s3_v, (s4_v - s3_v) / s4_v * 100.0);
    println!("kit::Ring   vs mpsc:    S5 vs S1 = {:.2}× ({:.1}% gain)",
             s1_v / s5_v, (s1_v - s5_v) / s1_v * 100.0);
    println!("kit::Ring   vs raw TCP: S5 vs S3 = {:.2}× ({:.1}% lost vs raw)",
             s5_v / s3_v, (s5_v - s3_v) / s5_v * 100.0);
    println!("kit::Ring   vs Stream:  S5 vs S4 = {:.2}× ({:.1}%)",
             s4_v / s5_v, (s4_v - s5_v) / s4_v * 100.0);
    println!();
    println!("── Solo server ── (cliente directo a TCP, comparando primitivas server-side):");
    println!("S2 mpsc        : {:>10.2} ms  (baseline server-only)", s2_v / 1e6);
    println!("S6 kit::Stream : {:>10.2} ms  ({:.2}× vs S2, {:+.1}% gain)",
             s6_v / 1e6, s2_v / s6_v, (s2_v - s6_v) / s2_v * 100.0);
    println!("S7 kit::Ring   : {:>10.2} ms  ({:.2}× vs S2, {:+.1}% gain)",
             s7_v / 1e6, s2_v / s7_v, (s2_v - s7_v) / s2_v * 100.0);
    println!();
    println!("── REAL MPSC: M producers → 1 shared channel → 1 sync decoder ──");
    println!("S8 tokio mpsc shared : {:>10.2} ms  (baseline real-mpsc)", s8_v / 1e6);
    println!("S9 kit::Mpmc shared  : {:>10.2} ms  ({:.2}× vs S8, {:+.1}% gain)",
             s9_v / 1e6, s8_v / s9_v, (s8_v - s9_v) / s8_v * 100.0);
    println!("S10 sharded kit::Mpmc: {:>10.2} ms  ({:.2}× vs S8, {:+.1}% gain) [supports M > 63]",
             s10_v / 1e6, s8_v / s10_v, (s8_v - s10_v) / s8_v * 100.0);
    println!("S11 ChunkedMpmc      : {:>10.2} ms  ({:.2}× vs S8, {:+.1}% gain) [M arbitrario, 1 decoder]",
             s11_v / 1e6, s8_v / s11_v, (s8_v - s11_v) / s8_v * 100.0);
    if s12[0] != u128::MAX {
        let s12_v = s12[0] as f64;
        println!("S12 kit::Hub         : {:>10.2} ms  ({:.2}× vs S8, {:+.1}% gain) [named ports, M ≤ 63]",
                 s12_v / 1e6, s8_v / s12_v, (s8_v - s12_v) / s8_v * 100.0);
    } else {
        println!("S12 kit::Hub         :   skipped — Hub caps at 63 ports (current M={})",
                 cfg.n_clients);
    }
    let s13_v = s13[0] as f64;
    println!("S13 lanes-mutex      : {:>10.2} ms  ({:.2}× vs S8, {:+.1}% gain) [16 lanes, ShardHandle Clone]",
             s13_v / 1e6, s8_v / s13_v, (s8_v - s13_v) / s8_v * 100.0);
    println!("S13 vs S9 (mutex tax): {:.2}× ({:+.1}%)",
             s13_v / s9_v, (s13_v - s9_v) / s9_v * 100.0);
    println!("Done.");
}
