//! Temporary bench: measure encode → send → receive → decode throughput
//! with all cores active (N producer threads, N consumer threads, N channels).
//!
//! Simulates the real server: each shard thread produces frames through its
//! own channel to a dedicated writer. Measures aggregate throughput.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use zerocopy::IntoBytes;

use arbitro_proto::action::Action;
use arbitro_proto::wire::delivery::{DeliveryEntryHeader, RepBatchFixed};
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};

use zerocopy::byteorder::little_endian::{U16, U32, U64};

const PAYLOAD_SIZE: usize = 64;
const BATCH_ENTRIES: usize = 256;
const TOTAL_MSGS_PER_CORE: usize = 1_000_000;
const CHANNEL_CAP: usize = 65536;

/// Build a single RepBatch frame with `count` entries of `payload_size` bytes.
fn build_frame(count: usize, payload_size: usize) -> Bytes {
    let subject = b"bench.test.subject";
    let payload = vec![0x42u8; payload_size];
    let subj_len = subject.len();
    let data_len = subj_len + payload.len();

    let mut body = BytesMut::with_capacity(
        8 + count * (std::mem::size_of::<DeliveryEntryHeader>() + data_len),
    );
    body.extend_from_slice(
        RepBatchFixed {
            count: U16::new(count as u16),
            _pad: U16::new(0),
        }
        .as_bytes(),
    );
    for seq in 0..count {
        let header = DeliveryEntryHeader {
            consumer_id: U32::new(1),
            seq: U64::new(seq as u64),
            subj_len: U16::new(subj_len as u16),
            data_len: U32::new(data_len as u32),
            subject_hash: U32::new(0),
        };
        body.extend_from_slice(header.as_bytes());
        body.extend_from_slice(subject);
        body.extend_from_slice(&payload);
    }

    let body_len = body.len();
    let envelope = Envelope::new(Action::RepBatch, 1, body_len as u32, 0);
    let mut frame = BytesMut::with_capacity(ENVELOPE_SIZE + body_len);
    frame.extend_from_slice(envelope.as_bytes());
    frame.extend_from_slice(&body);
    frame.freeze()
}

/// Decode a frame: validate envelope + walk entries (simulates client recv).
#[inline(never)]
fn decode_frame(data: &[u8]) -> usize {
    if data.len() < ENVELOPE_SIZE {
        return 0;
    }
    let body = &data[ENVELOPE_SIZE..];
    if body.len() < 8 {
        return 0;
    }
    let count = u16::from_le_bytes([body[4], body[5]]) as usize;
    let mut offset = 8;
    for _ in 0..count {
        if offset + 14 > body.len() {
            break;
        }
        let data_len = u32::from_le_bytes([
            body[offset + 10],
            body[offset + 11],
            body[offset + 12],
            body[offset + 13],
        ]) as usize;
        offset += 14 + data_len;
    }
    count
}

fn print_result(label: &str, cores: usize, total: u64, elapsed: std::time::Duration, extra: &str) {
    let msgs_per_sec = total as f64 / elapsed.as_secs_f64();
    let per_core = msgs_per_sec / cores as f64;
    println!(
        "{:<30} {:>12.0} msg/s | {:>7.2}ms | {:>8} msgs | {}/core: {:.0} msg/s | {}",
        label,
        msgs_per_sec,
        elapsed.as_secs_f64() * 1000.0,
        total,
        cores,
        per_core,
        extra,
    );
}

fn run_for_cores(num_cores: usize) {
    let batches_per_core = TOTAL_MSGS_PER_CORE / BATCH_ENTRIES;
    let total_expected = (TOTAL_MSGS_PER_CORE / BATCH_ENTRIES * BATCH_ENTRIES * num_cores) as u64;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cores)
        .enable_all()
        .build()
        .unwrap();

    println!(
        "\n{}M msgs/core × {} cores = {}M total, batch={}, cap={}",
        TOTAL_MSGS_PER_CORE / 1_000_000,
        num_cores,
        total_expected / 1_000_000,
        BATCH_ENTRIES,
        CHANNEL_CAP,
    );
    println!("{}", "-".repeat(110));

    // ── 1. Baseline: N threads, encode + decode only (no channel) ───────
    {
        let counter = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(num_cores + 1));

        let mut handles = Vec::new();
        for _ in 0..num_cores {
            let c = counter.clone();
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                let mut local = 0u64;
                for _ in 0..batches_per_core {
                    let f = build_frame(BATCH_ENTRIES, PAYLOAD_SIZE);
                    local += decode_frame(&f) as u64;
                }
                c.fetch_add(local, Ordering::Relaxed);
            }));
        }
        barrier.wait();
        let t0 = Instant::now();
        for h in handles {
            h.join().unwrap();
        }
        let elapsed = t0.elapsed();
        let total = counter.load(Ordering::Relaxed);
        print_result("[NO CHANNEL]", num_cores, total, elapsed, "encode+decode");
    }

    // ── 2. tokio mpsc: N sync producers (blocking_send) → N async consumers
    rt.block_on(async {
        let counter = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(num_cores + 1));

        let mut producer_handles = Vec::new();
        let mut consumer_futures = Vec::new();

        for _ in 0..num_cores {
            let (tx, mut rx) = mpsc::channel::<Bytes>(CHANNEL_CAP);
            let b = barrier.clone();

            producer_handles.push(std::thread::spawn(move || {
                b.wait();
                for _ in 0..batches_per_core {
                    let f = build_frame(BATCH_ENTRIES, PAYLOAD_SIZE);
                    tx.blocking_send(f).unwrap();
                }
            }));

            consumer_futures.push(tokio::spawn(async move {
                let mut local = 0u64;
                for _ in 0..batches_per_core {
                    let f = rx.recv().await.unwrap();
                    local += decode_frame(&f) as u64;
                }
                local
            }));
        }

        barrier.wait();
        let t0 = Instant::now();

        let mut total = 0u64;
        for fut in consumer_futures {
            total += fut.await.unwrap();
        }
        for h in producer_handles {
            h.join().unwrap();
        }
        let elapsed = t0.elapsed();
        print_result("[TOKIO blocking_send]", num_cores, total, elapsed, &format!("cap={}", CHANNEL_CAP));
    });

    // ── 3. crossbeam bounded: N sync producers → N sync consumers ───────
    {
        let counter = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(num_cores * 2 + 1));

        let mut handles = Vec::new();
        for _ in 0..num_cores {
            let (tx, rx) = crossbeam_channel::bounded::<Bytes>(CHANNEL_CAP);
            let b_p = barrier.clone();
            let b_c = barrier.clone();
            let c = counter.clone();

            handles.push(std::thread::spawn(move || {
                b_p.wait();
                for _ in 0..batches_per_core {
                    let f = build_frame(BATCH_ENTRIES, PAYLOAD_SIZE);
                    tx.send(f).unwrap();
                }
            }));

            handles.push(std::thread::spawn(move || {
                b_c.wait();
                let mut local = 0u64;
                for _ in 0..batches_per_core {
                    let f = rx.recv().unwrap();
                    local += decode_frame(&f) as u64;
                }
                c.fetch_add(local, Ordering::Relaxed);
            }));
        }

        barrier.wait();
        let t0 = Instant::now();
        for h in handles {
            h.join().unwrap();
        }
        let elapsed = t0.elapsed();
        let total = counter.load(Ordering::Relaxed);
        print_result("[CROSSBEAM bounded]", num_cores, total, elapsed, &format!("cap={}", CHANNEL_CAP));
    }

    // ── 4. flume bounded: N sync producers → N sync consumers ───────────
    {
        let counter = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(num_cores * 2 + 1));

        let mut handles = Vec::new();
        for _ in 0..num_cores {
            let (tx, rx) = flume::bounded::<Bytes>(CHANNEL_CAP);
            let b_p = barrier.clone();
            let b_c = barrier.clone();
            let c = counter.clone();

            handles.push(std::thread::spawn(move || {
                b_p.wait();
                for _ in 0..batches_per_core {
                    let f = build_frame(BATCH_ENTRIES, PAYLOAD_SIZE);
                    tx.send(f).unwrap();
                }
            }));

            handles.push(std::thread::spawn(move || {
                b_c.wait();
                let mut local = 0u64;
                for _ in 0..batches_per_core {
                    let f = rx.recv().unwrap();
                    local += decode_frame(&f) as u64;
                }
                c.fetch_add(local, Ordering::Relaxed);
            }));
        }

        barrier.wait();
        let t0 = Instant::now();
        for h in handles {
            h.join().unwrap();
        }
        let elapsed = t0.elapsed();
        let total = counter.load(Ordering::Relaxed);
        print_result("[FLUME bounded]", num_cores, total, elapsed, &format!("cap={}", CHANNEL_CAP));
    }
}

fn main() {
    println!("Channel overhead bench");
    println!("{}", "=".repeat(110));
    for cores in [2, 4, 6] {
        run_for_cores(cores);
    }
}
