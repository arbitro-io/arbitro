//! Isolated bench — publish + ideal drain, full path with TCP.
//!
//! ## Scenario 1: PUBLISH (production 1:1)
//!   Client → TCP → server read_loop → dispatch_publish:
//!     BatchIter → store.lock().append_batch() → send_rep_ok → gate
//!   Server write_loop → TCP → client counts RepOk
//!
//! ## Scenario 2: DRAIN IDEAL
//!   Pre-populate store with N messages. Dedicated drain thread:
//!     gate wakes → store.lock().for_each() → batch entries into RepBatch
//!     frames (DRAIN_BATCH entries per frame) → try_send to channel.
//!   Server write_loop → TCP → client parses RepBatch, counts entries.
//!
//!   The drain is IDEAL: its own thread, its own loop, no commands,
//!   no engine, no shared loop. Only gate + store connect it to the world.
//!   Entries are grouped into batched RepBatch frames (like publish groups
//!   entries into batched Publish frames).
//!
//! Run: cargo bench --bench drain_isolated -p arbitro-e2e

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use arbitro_engine_v2::catalog::fnv1a_32;
use arbitro_proto::action::Action;
use arbitro_proto::wire::delivery::{
    DeliveryEntryHeader, RepBatchFixed, RepOkAction, DELIVERY_ENTRY_HEADER_SIZE,
    REP_BATCH_FIXED_SIZE,
};
use arbitro_proto::wire::envelope::{Envelope, FrameView, ENVELOPE_SIZE};
use arbitro_proto::wire::publish::{BatchIter, PublishEntry, PUBLISH_ENTRY_SIZE};
use arbitro_store::{EntryRef, MemoryStore, Store};

// ── Settings ────────────────────────────────────────────────────────────────

const PAYLOAD_SIZE: usize = 64;
const SUBJECT: &[u8] = b"bench.publish.subj";
const BATCH_SIZE: u32 = 256;
/// Drain groups this many entries per RepBatch frame.
const DRAIN_BATCH: usize = 256;
/// Max entries scanned per drain cycle (matches production max_feed).
const MAX_FEED: u64 = 8192;
/// Server write channel capacity — matches throughput bench (65536).
const WRITE_BUFFER_CAP: usize = 65536;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn fmt_rate(msgs: usize, elapsed: std::time::Duration) -> String {
    let rate = msgs as f64 / elapsed.as_secs_f64();
    if rate >= 1_000_000_000.0 {
        format!("{:.1}G", rate / 1_000_000_000.0)
    } else if rate >= 1_000_000.0 {
        format!("{:.1}M", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}K", rate / 1_000.0)
    } else {
        format!("{:.0}", rate)
    }
}

fn fmt_dur(elapsed: std::time::Duration) -> String {
    let ms = elapsed.as_secs_f64() * 1000.0;
    if ms >= 1000.0 {
        format!("{:.2}s", ms / 1000.0)
    } else {
        format!("{:.2}ms", ms)
    }
}

// ── Shared: server write_loop ───────────────────────────────────────────────

/// Server write loop — mirrors server.rs:288-321.
/// recv → coalesce via try_recv → write_all / write_all_vectored.
async fn server_write_loop(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut rx: mpsc::Receiver<Bytes>,
) {
    let mut batch: Vec<Bytes> = Vec::with_capacity(64);

    loop {
        match rx.recv().await {
            Some(frame) => batch.push(frame),
            None => break,
        }

        // Coalesce — drain all ready frames without blocking.
        while let Ok(frame) = rx.try_recv() {
            batch.push(frame);
        }

        let failed = if batch.len() == 1 {
            writer.write_all(&batch[0]).await.is_err()
        } else {
            let slices: Vec<std::io::IoSlice<'_>> =
                batch.iter().map(|f| std::io::IoSlice::new(f)).collect();
            writer.write_vectored(&slices).await.is_err()
        };

        if failed {
            break;
        }
        batch.clear();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SCENARIO 1: PUBLISH
// ═══════════════════════════════════════════════════════════════════════════

fn build_publish_frame(
    buf: &mut Vec<u8>,
    stream_id: u32,
    env_seq: u32,
    batch_size: u32,
    subject: &[u8],
    payload: &[u8],
) {
    buf.clear();
    buf.extend_from_slice(&[0u8; ENVELOPE_SIZE]);
    buf.extend_from_slice(&batch_size.to_le_bytes());

    for _ in 0..batch_size {
        let header = PublishEntry {
            data_len: U32::new(payload.len() as u32),
            subj_len: U16::new(subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0; 3],
        };
        buf.extend_from_slice(header.as_bytes());
        buf.extend_from_slice(subject);
        buf.extend_from_slice(payload);
    }

    let body_len = (buf.len() - ENVELOPE_SIZE) as u32;
    let envelope = Envelope::new(Action::Publish, stream_id, body_len, env_seq);
    buf[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());
}

#[inline(never)]
fn dispatch_publish(
    frame: &[u8],
    store: &Arc<Mutex<Box<dyn Store>>>,
    write_tx: &mpsc::Sender<Bytes>,
    gate_counter: &AtomicU64,
) {
    let view = FrameView::new(frame);
    let env_seq = view.envelope().env_seq.get();
    let body = view.body();

    let iter = BatchIter::new(body);
    let store_entries: Vec<EntryRef<'_>> = iter
        .map(|view| EntryRef {
            stream_id: 1,
            subject: view.subject(),
            payload: view.payload(),
            flags: 0,
        })
        .collect();

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let first_seq = store
        .lock()
        .unwrap()
        .append_batch(&store_entries, now_ms)
        .unwrap();

    let rep_envelope = Envelope::new(Action::RepOk, 0, 16, env_seq);
    let rep_body = RepOkAction {
        ref_seq: U64::new(first_seq),
        _pad: U64::new(0),
    };
    let mut rep_buf = BytesMut::with_capacity(ENVELOPE_SIZE + 16);
    rep_buf.extend_from_slice(rep_envelope.as_bytes());
    rep_buf.extend_from_slice(rep_body.as_bytes());
    let _ = write_tx.try_send(rep_buf.freeze());

    gate_counter.fetch_add(1, Ordering::Relaxed);
}

fn server_read_loop(
    mut reader: TcpStream,
    store: Arc<Mutex<Box<dyn Store>>>,
    write_tx: mpsc::Sender<Bytes>,
    gate_counter: Arc<AtomicU64>,
) {
    let mut buf = vec![0u8; 512 * 1024];
    let mut ring = Vec::with_capacity(1024 * 1024);

    loop {
        let n = match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        ring.extend_from_slice(&buf[..n]);

        loop {
            if ring.len() < ENVELOPE_SIZE {
                break;
            }
            let msg_len = u32::from_le_bytes([
                ring[8], ring[9], ring[10], ring[11],
            ]) as usize;
            let total = ENVELOPE_SIZE + msg_len;
            if ring.len() < total {
                break;
            }

            dispatch_publish(&ring[..total], &store, &write_tx, &gate_counter);
            ring.drain(..total);
        }
    }
}

fn client_read_replies(mut reader: TcpStream, expected: usize) -> usize {
    let mut buf = [0u8; 64 * 1024];
    let mut ring = Vec::with_capacity(64 * 1024);
    let mut count = 0usize;
    let frame_size = ENVELOPE_SIZE + 16;

    while count < expected {
        let n = match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        ring.extend_from_slice(&buf[..n]);

        while ring.len() >= frame_size {
            ring.drain(..frame_size);
            count += 1;
        }
    }
    count
}

fn scenario_publish(total_msgs: usize) {
    let batches = total_msgs / BATCH_SIZE as usize;
    let payload = vec![0x42u8; PAYLOAD_SIZE];

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client_stream = TcpStream::connect(addr).unwrap();
    client_stream.set_nodelay(true).unwrap();
    let (server_stream, _) = listener.accept().unwrap();
    server_stream.set_nodelay(true).unwrap();

    let store: Arc<Mutex<Box<dyn Store>>> =
        Arc::new(Mutex::new(Box::new(MemoryStore::new())));
    let gate_counter = Arc::new(AtomicU64::new(0));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();

    let (write_tx, write_rx) = mpsc::channel::<Bytes>(WRITE_BUFFER_CAP);

    let server_std_clone = server_stream.try_clone().unwrap();
    let server_tokio = rt.block_on(async {
        tokio::net::TcpStream::from_std(server_std_clone).unwrap()
    });
    let (_server_read_half, server_write_half) = server_tokio.into_split();

    let write_handle = std::thread::spawn(move || {
        rt.block_on(server_write_loop(server_write_half, write_rx));
    });

    let store_clone = Arc::clone(&store);
    let gate_clone = Arc::clone(&gate_counter);
    let read_handle = std::thread::spawn(move || {
        server_read_loop(server_stream, store_clone, write_tx, gate_clone);
    });

    let client_reader = client_stream.try_clone().unwrap();
    let reply_handle = std::thread::spawn(move || {
        client_read_replies(client_reader, batches)
    });

    let mut writer = client_stream;
    writer.set_nodelay(true).unwrap();
    let mut frame_buf = Vec::with_capacity(
        ENVELOPE_SIZE + 4 + BATCH_SIZE as usize * (PUBLISH_ENTRY_SIZE + SUBJECT.len() + PAYLOAD_SIZE),
    );

    let t0 = Instant::now();
    for i in 0..batches {
        build_publish_frame(&mut frame_buf, 1, i as u32, BATCH_SIZE, SUBJECT, &payload);
        writer.write_all(&frame_buf).unwrap();
    }
    drop(writer);
    let replies = reply_handle.join().unwrap();
    let elapsed = t0.elapsed();

    read_handle.join().unwrap();
    write_handle.join().unwrap();

    let published = batches * BATCH_SIZE as usize;
    let bytes = published * (PAYLOAD_SIZE + SUBJECT.len());
    let mb = bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0;
    println!(
        "    {total:>7}k  {time:>10}  {rate:>10} msg/s  {mb:>8.0} MB/s  (replies={replies})",
        total = total_msgs / 1000,
        time = fmt_dur(elapsed),
        rate = fmt_rate(published, elapsed),
        mb = mb,
        replies = replies,
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// SCENARIO 2: DRAIN IDEAL
// ═══════════════════════════════════════════════════════════════════════════

/// Build a batched RepBatch frame into `body`.
///
/// Wire: [16B envelope][8B RepBatchFixed][N × (18B DeliveryEntryHeader + subject + payload)]
///
/// This is the IDEAL frame builder: N entries per frame, like publish batches
/// N entries per Publish frame. The current production drain builds 1 entry
/// per frame — 500k frames instead of ~2k.
fn build_rep_batch(
    body: &mut BytesMut,
    stream_id: u32,
    consumer_id: u32,
    entries: &[DrainEntry<'_>],
) {
    body.clear();
    // Envelope placeholder.
    body.extend_from_slice(&[0u8; ENVELOPE_SIZE]);
    // RepBatchFixed header.
    body.extend_from_slice(
        RepBatchFixed {
            count: U16::new(entries.len() as u16),
            _pad: U16::new(0),
        }
        .as_bytes(),
    );
    // Entries.
    for e in entries {
        let subj_len = e.subject.len();
        let data_len = subj_len + e.payload.len();
        body.extend_from_slice(
            DeliveryEntryHeader {
                consumer_id: U32::new(consumer_id),
                seq: U64::new(e.seq),
                subj_len: U16::new(subj_len as u16),
                data_len: U32::new(data_len as u32),
                subject_hash: U32::new(e.subject_hash),
            }
            .as_bytes(),
        );
        body.extend_from_slice(e.subject);
        body.extend_from_slice(e.payload);
    }
    // Patch envelope.
    let body_len = body.len() - ENVELOPE_SIZE;
    let envelope = Envelope::new(Action::RepBatch, stream_id, body_len as u32, 0);
    body[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());
}

/// Temporary entry collected from store for batching.
struct DrainEntry<'a> {
    seq: u64,
    subject: &'a [u8],
    payload: &'a [u8],
    subject_hash: u32,
}

/// Ideal drain thread — its own thread, its own loop, no commands, no engine,
/// no shared loop. Only gate (AtomicBool) + store connect it to the world.
///
/// Reads from store in chunks of MAX_FEED. Groups entries into batched
/// RepBatch frames (DRAIN_BATCH entries per frame). try_send to channel.
/// When channel is Full, yields 50µs for the write_loop to drain.
fn drain_thread(
    store: Arc<Mutex<Box<dyn Store>>>,
    write_tx: mpsc::Sender<Bytes>,
    gate_open: Arc<AtomicBool>,
    total_expected: u64,
) {
    let mut cursor: u64 = 0;
    let mut body = BytesMut::with_capacity(
        ENVELOPE_SIZE
            + REP_BATCH_FIXED_SIZE
            + DRAIN_BATCH * (DELIVERY_ENTRY_HEADER_SIZE + SUBJECT.len() + PAYLOAD_SIZE),
    );

    // Wait for gate to open (store is populated).
    while !gate_open.load(Ordering::Acquire) {
        std::thread::park();
    }

    loop {
        // Lock store, read a chunk, collect entries for batching.
        // We must collect because store entries borrow from the lock guard.
        let guard = store.lock().unwrap();
        let info = guard.info();
        if info.last_seq <= cursor {
            break; // All done.
        }

        let start = cursor + 1;
        let end = (start + MAX_FEED).min(info.last_seq + 1);

        // Collect entries from this chunk into owned batches and send them.
        // We batch DRAIN_BATCH entries per RepBatch frame.
        let mut batch_seqs: Vec<u64> = Vec::with_capacity(DRAIN_BATCH);
        let mut batch_subjects: Vec<Vec<u8>> = Vec::with_capacity(DRAIN_BATCH);
        let mut batch_payloads: Vec<Vec<u8>> = Vec::with_capacity(DRAIN_BATCH);
        let mut batch_hashes: Vec<u32> = Vec::with_capacity(DRAIN_BATCH);

        guard
            .for_each(start, end, &mut |entry| {
                batch_seqs.push(entry.seq);
                batch_subjects.push(entry.subject.to_vec());
                batch_payloads.push(entry.payload.to_vec());
                batch_hashes.push(fnv1a_32(entry.subject));

                if batch_seqs.len() == DRAIN_BATCH {
                    // Build and send a full batch.
                    let entries: Vec<DrainEntry<'_>> = (0..batch_seqs.len())
                        .map(|i| DrainEntry {
                            seq: batch_seqs[i],
                            subject: &batch_subjects[i],
                            payload: &batch_payloads[i],
                            subject_hash: batch_hashes[i],
                        })
                        .collect();
                    build_rep_batch(&mut body, 1, 1, &entries);
                    let frozen = body.split().freeze();
                    while write_tx.try_send(frozen.clone()).is_err() {
                        std::thread::park_timeout(std::time::Duration::from_micros(50));
                    }
                    batch_seqs.clear();
                    batch_subjects.clear();
                    batch_payloads.clear();
                    batch_hashes.clear();
                }
            })
            .ok();

        // Flush remaining partial batch.
        if !batch_seqs.is_empty() {
            let entries: Vec<DrainEntry<'_>> = (0..batch_seqs.len())
                .map(|i| DrainEntry {
                    seq: batch_seqs[i],
                    subject: &batch_subjects[i],
                    payload: &batch_payloads[i],
                    subject_hash: batch_hashes[i],
                })
                .collect();
            build_rep_batch(&mut body, 1, 1, &entries);
            let frozen = body.split().freeze();
            while write_tx.try_send(frozen.clone()).is_err() {
                std::thread::park_timeout(std::time::Duration::from_micros(50));
            }
        }

        drop(guard);
        cursor = end - 1;

        if cursor >= total_expected {
            break;
        }
    }
}

/// Client reader for drain — parses RepBatch frames, counts entries.
fn client_read_drain(mut reader: TcpStream, expected: usize) -> usize {
    let mut buf = [0u8; 256 * 1024];
    let mut ring = Vec::with_capacity(512 * 1024);
    let mut count = 0usize;

    while count < expected {
        let n = match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        ring.extend_from_slice(&buf[..n]);

        // Parse complete RepBatch frames from ring.
        loop {
            if ring.len() < ENVELOPE_SIZE {
                break;
            }
            let msg_len = u32::from_le_bytes([
                ring[8], ring[9], ring[10], ring[11],
            ]) as usize;
            let total = ENVELOPE_SIZE + msg_len;
            if ring.len() < total {
                break;
            }

            // Parse entry count from RepBatchFixed.
            let batch_body = &ring[ENVELOPE_SIZE..total];
            if batch_body.len() >= REP_BATCH_FIXED_SIZE {
                let entry_count =
                    u16::from_le_bytes([batch_body[0], batch_body[1]]) as usize;
                count += entry_count;
            }

            ring.drain(..total);
        }
    }
    count
}

fn scenario_drain(total_msgs: usize) {
    let payload = vec![0x42u8; PAYLOAD_SIZE];

    // Pre-populate store.
    let store: Arc<Mutex<Box<dyn Store>>> =
        Arc::new(Mutex::new(Box::new(MemoryStore::new())));
    {
        let mut guard = store.lock().unwrap();
        // Batch append for speed — same as publish does.
        let entries_per_batch = BATCH_SIZE as usize;
        let batches = total_msgs / entries_per_batch;
        let remainder = total_msgs % entries_per_batch;
        let refs: Vec<EntryRef<'_>> = (0..entries_per_batch)
            .map(|_| EntryRef {
                stream_id: 1,
                subject: SUBJECT,
                payload: &payload,
                flags: 0,
            })
            .collect();
        for _ in 0..batches {
            guard.append_batch(&refs, 0).unwrap();
        }
        if remainder > 0 {
            let refs: Vec<EntryRef<'_>> = (0..remainder)
                .map(|_| EntryRef {
                    stream_id: 1,
                    subject: SUBJECT,
                    payload: &payload,
                    flags: 0,
                })
                .collect();
            guard.append_batch(&refs, 0).unwrap();
        }
    }

    // TCP pair.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client_stream = TcpStream::connect(addr).unwrap();
    client_stream.set_nodelay(true).unwrap();
    let (server_stream, _) = listener.accept().unwrap();
    server_stream.set_nodelay(true).unwrap();

    let gate_open = Arc::new(AtomicBool::new(false));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();

    let (write_tx, write_rx) = mpsc::channel::<Bytes>(WRITE_BUFFER_CAP);

    // Server write_loop on tokio.
    let server_std_clone = server_stream.try_clone().unwrap();
    let server_tokio = rt.block_on(async {
        tokio::net::TcpStream::from_std(server_std_clone).unwrap()
    });
    let (_server_read_half, server_write_half) = server_tokio.into_split();

    let write_handle = std::thread::spawn(move || {
        rt.block_on(server_write_loop(server_write_half, write_rx));
    });

    // Client reader.
    let reader_handle = std::thread::spawn(move || {
        client_read_drain(client_stream, total_msgs)
    });

    // Drain thread.
    let store_clone = Arc::clone(&store);
    let gate_clone = Arc::clone(&gate_open);
    let drain_handle = std::thread::spawn(move || {
        drain_thread(store_clone, write_tx, gate_clone, total_msgs as u64);
    });

    // ── Timed section: open the gate ────────────────────────────────────
    let t0 = Instant::now();
    gate_open.store(true, Ordering::Release);
    drain_handle.thread().unpark();

    // Wait for drain to finish and channel to close.
    drain_handle.join().unwrap();
    // write_tx dropped → write_loop exits → TCP closed → client reader exits.
    let received = reader_handle.join().unwrap();
    let elapsed = t0.elapsed();
    write_handle.join().unwrap();

    let bytes = total_msgs * (PAYLOAD_SIZE + SUBJECT.len());
    let mb = bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0;
    println!(
        "    {total:>7}k  {time:>10}  {rate:>10} msg/s  {mb:>8.0} MB/s  (received={received})",
        total = total_msgs / 1000,
        time = fmt_dur(elapsed),
        rate = fmt_rate(total_msgs, elapsed),
        mb = mb,
        received = received,
    );
}

// ── Main ────────────────────────────────────────────────────────────────────

fn main() {
    println!("Isolated bench — publish + ideal drain, full path with TCP");
    println!(
        "payload={}B, subject={}B, pub_batch={}, drain_batch={}, max_feed={}, write_buf={}",
        PAYLOAD_SIZE,
        SUBJECT.len(),
        BATCH_SIZE,
        DRAIN_BATCH,
        MAX_FEED,
        WRITE_BUFFER_CAP,
    );
    println!("{}", "=".repeat(90));

    println!("\n  PUBLISH (client → TCP → store → RepOk → TCP → client)");
    println!("    {:>7}   {:>10}  {:>14}  {:>8}", "msgs", "time", "throughput", "bandwidth");
    println!("    {}", "-".repeat(80));
    for &msgs in &[10_000, 100_000, 500_000, 1_000_000] {
        scenario_publish(msgs);
    }

    println!("\n  DRAIN IDEAL (store → batch RepBatch → channel → TCP → client)");
    println!("    {:>7}   {:>10}  {:>14}  {:>8}", "msgs", "time", "throughput", "bandwidth");
    println!("    {}", "-".repeat(80));
    for &msgs in &[10_000, 100_000, 500_000, 1_000_000] {
        scenario_drain(msgs);
    }
}
