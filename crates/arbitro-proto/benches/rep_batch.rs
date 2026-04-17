//! Micro-benchmark: RepBatch encode + decode cost.
//!
//! Measures:
//! - Encode: build a RepBatch frame with N entries (16B envelope + 8B fixed + N × entry)
//! - Decode: parse the frame via zero-copy RepBatchView iterator
//!
//! Run: cargo bench --bench rep_batch -p arbitro-proto

#![allow(unused)]

use std::hint::black_box;
use std::time::Instant;

use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::IntoBytes;

use arbitro_proto::wire::delivery::{
    DeliveryEntryHeader, RepBatchEntry, RepBatchFixed, RepBatchView,
    DELIVERY_ENTRY_HEADER_SIZE, REP_BATCH_FIXED_SIZE,
};
use arbitro_proto::wire::envelope::Envelope;
use arbitro_proto::action::Action;

const ENVELOPE_SIZE: usize = core::mem::size_of::<Envelope>();

// ── Helpers ─────────────────────────────────────────────────────────────────

fn build_rep_batch(buf: &mut Vec<u8>, consumer_id: u32, entries: &[(&[u8], &[u8])]) {
    buf.clear();

    // Envelope placeholder
    buf.extend_from_slice(&[0u8; ENVELOPE_SIZE]);

    // RepBatchFixed
    buf.extend_from_slice(
        RepBatchFixed {
            count: U16::new(entries.len() as u16),
            _pad: U16::new(0),
        }
        .as_bytes(),
    );

    // Entries
    for (seq_idx, &(subject, payload)) in entries.iter().enumerate() {
        let subj_len = subject.len();
        let data_len = subj_len + payload.len();
        buf.extend_from_slice(
            DeliveryEntryHeader {
                consumer_id: U32::new(consumer_id),
                seq: U64::new(seq_idx as u64 + 1),
                subj_len: U16::new(subj_len as u16),
                data_len: U32::new(data_len as u32),
                subject_hash: U32::new(0x12345678),
            }
            .as_bytes(),
        );
        buf.extend_from_slice(subject);
        buf.extend_from_slice(payload);
    }

    // Patch envelope
    let body_len = buf.len() - ENVELOPE_SIZE;
    let envelope = Envelope::new(Action::RepBatch, 1, body_len as u32, 0);
    buf[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());
}

fn decode_rep_batch(body: &[u8]) -> (u16, u64) {
    let view = RepBatchView::new(body);
    let count = view.count();
    let mut sum_seq: u64 = 0;
    for entry in view.entries() {
        sum_seq += entry.seq;
        black_box(entry.consumer_id);
        black_box(entry.subject);
        black_box(entry.payload);
        black_box(entry.subject_hash);
    }
    (count, sum_seq)
}

// ── Bench harness (no external deps) ────────────────────────────────────────

fn bench_encode(label: &str, count: usize, subject: &[u8], payload: &[u8], iters: u64) {
    let entries: Vec<(&[u8], &[u8])> = (0..count).map(|_| (subject, payload)).collect();
    let mut buf = Vec::with_capacity(64 * 1024);

    // Warmup
    for _ in 0..100 {
        build_rep_batch(&mut buf, 42, &entries);
        black_box(&buf);
    }

    let start = Instant::now();
    for _ in 0..iters {
        build_rep_batch(&mut buf, 42, &entries);
        black_box(&buf);
    }
    let elapsed = start.elapsed();

    let per_iter = elapsed / iters as u32;
    let per_entry = elapsed / (iters * count as u64) as u32;
    let frame_bytes = buf.len();
    let throughput = (iters * count as u64) as f64 / elapsed.as_secs_f64();

    println!(
        "  encode {label:30} | {count:4} entries | frame {frame_bytes:6}B | {per_iter:>8.1?}/frame | {per_entry:>6.1?}/entry | {throughput:>10.0} entries/s",
    );
}

fn bench_decode(label: &str, count: usize, subject: &[u8], payload: &[u8], iters: u64) {
    let entries: Vec<(&[u8], &[u8])> = (0..count).map(|_| (subject, payload)).collect();
    let mut buf = Vec::new();
    build_rep_batch(&mut buf, 42, &entries);
    let body = &buf[ENVELOPE_SIZE..]; // skip envelope, decode body only

    // Warmup
    for _ in 0..100 {
        let r = decode_rep_batch(body);
        black_box(r);
    }

    let start = Instant::now();
    for _ in 0..iters {
        let r = decode_rep_batch(body);
        black_box(r);
    }
    let elapsed = start.elapsed();

    let per_iter = elapsed / iters as u32;
    let per_entry = elapsed / (iters * count as u64) as u32;
    let throughput = (iters * count as u64) as f64 / elapsed.as_secs_f64();

    println!(
        "  decode {label:30} | {count:4} entries | body   {body_len:6}B | {per_iter:>8.1?}/frame | {per_entry:>6.1?}/entry | {throughput:>10.0} entries/s",
        body_len = body.len(),
    );
}

fn main() {
    println!("\nRepBatch encode/decode micro-benchmark");
    println!("=======================================\n");

    let subjects: &[(&str, &[u8])] = &[
        ("8B subj", b"orders.x"),
        ("19B subj", b"message.premium.x1"),
        ("40B subj", b"very.long.subject.name.for.testing.perf"),
    ];

    let payloads: &[(&str, &[u8])] = &[
        ("0B payload", b""),
        ("64B payload", &[0xAB; 64]),
        ("256B payload", &[0xCD; 256]),
        ("1KB payload", &[0xEF; 1024]),
    ];

    let counts = [1, 8, 32, 128, 256];
    let iters = 500_000u64;

    // Focused: 19B subject, 64B payload (typical case)
    println!("── Typical case: 19B subject + 64B payload ──\n");
    for &count in &counts {
        let subject = b"message.premium.x1";
        let payload = &[0xAB; 64];
        let label = format!("{count}×(19B+64B)");
        bench_encode(&label, count, subject, payload, iters);
    }
    println!();
    for &count in &counts {
        let subject = b"message.premium.x1";
        let payload = &[0xAB; 64];
        let label = format!("{count}×(19B+64B)");
        bench_decode(&label, count, subject, payload, iters);
    }

    // Matrix: subject × payload for count=32
    println!("\n── Matrix: subject × payload, 32 entries ──\n");
    for &(sname, subject) in subjects {
        for &(pname, payload) in payloads {
            let label = format!("32×({sname}+{pname})");
            bench_encode(&label, 32, subject, payload, iters);
        }
    }
    println!();
    for &(sname, subject) in subjects {
        for &(pname, payload) in payloads {
            let label = format!("32×({sname}+{pname})");
            bench_decode(&label, 32, subject, payload, iters);
        }
    }

    println!();
}
