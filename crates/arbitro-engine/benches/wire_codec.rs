//! Benchmark: zero-copy wire encode/decode — ns per operation.
//!
//! Measures the cost of casting between typed slices and raw bytes.
//! Expected: near-zero (pointer cast, no serialization).

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, black_box};
use arbitro_engine::batch::{ClaimedEntry, AckEntry};
use arbitro_engine::fanout::FanoutEntry;
use arbitro_engine::reply::RepPublish;
use arbitro_engine::wire;
use arbitro_engine::types::*;
use zerocopy::IntoBytes;

// ── Helpers ────────────────────────────────────────────────────────────────

fn make_fanout_entries(n: usize) -> Vec<FanoutEntry> {
    (0..n).map(|i| FanoutEntry::new(
        ConnectionId(i as u64 + 1),
        (i as u32).wrapping_mul(0x9E3779B9),
        i as u64 + 1000,
    )).collect()
}

fn make_claimed_entries(n: usize) -> Vec<ClaimedEntry> {
    (0..n).map(|i| ClaimedEntry {
        seq: i as u64 + 1,
        pending_id: PendingId(i as u32),
        subject_hash: (i as u32).wrapping_mul(0x9E3779B9),
    }).collect()
}

fn make_ack_entries(n: usize) -> Vec<AckEntry> {
    (0..n).map(|i| AckEntry { seq: i as u64 + 1 }).collect()
}

// ── 1. Encode: &[T] → &[u8] ───────────────────────────────────────────────

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_encode");

    for count in [1, 10, 100, 1000] {
        let fanout = make_fanout_entries(count);
        group.bench_with_input(
            BenchmarkId::new("fanout_entry", count),
            &count,
            |b, _| b.iter(|| black_box(fanout.as_bytes())),
        );

        let claimed = make_claimed_entries(count);
        group.bench_with_input(
            BenchmarkId::new("claimed_entry", count),
            &count,
            |b, _| b.iter(|| black_box(claimed.as_bytes())),
        );

        let ack = make_ack_entries(count);
        group.bench_with_input(
            BenchmarkId::new("ack_entry", count),
            &count,
            |b, _| b.iter(|| black_box(ack.as_bytes())),
        );
    }

    // RepPublish — single struct
    let rep = RepPublish::new(100);
    group.bench_function("rep_publish", |b| {
        b.iter(|| black_box(rep.as_bytes()))
    });

    group.finish();
}

// ── 2. Decode: &[u8] → &[T] ───────────────────────────────────────────────

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_decode");

    for count in [1, 10, 100, 1000] {
        let fanout = make_fanout_entries(count);
        let fanout_bytes = fanout.as_bytes().to_vec();
        group.bench_with_input(
            BenchmarkId::new("fanout_entry", count),
            &count,
            |b, _| b.iter(|| {
                let decoded = wire::decode_slice::<FanoutEntry>(black_box(&fanout_bytes));
                black_box(decoded)
            }),
        );

        let claimed = make_claimed_entries(count);
        let claimed_bytes = claimed.as_bytes().to_vec();
        group.bench_with_input(
            BenchmarkId::new("claimed_entry", count),
            &count,
            |b, _| b.iter(|| {
                let decoded = wire::decode_slice::<ClaimedEntry>(black_box(&claimed_bytes));
                black_box(decoded)
            }),
        );

        let ack = make_ack_entries(count);
        let ack_bytes = ack.as_bytes().to_vec();
        group.bench_with_input(
            BenchmarkId::new("ack_entry", count),
            &count,
            |b, _| b.iter(|| {
                let decoded = wire::decode_slice::<AckEntry>(black_box(&ack_bytes));
                black_box(decoded)
            }),
        );
    }

    // RepPublish — single struct
    let rep = RepPublish::new(100);
    let rep_bytes = rep.as_bytes().to_vec();
    group.bench_function("rep_publish", |b| {
        b.iter(|| {
            let decoded = wire::decode_ref::<RepPublish>(black_box(&rep_bytes));
            black_box(decoded)
        })
    });

    group.finish();
}

// ── 3. Roundtrip: encode → decode → read field ────────────────────────────

fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_roundtrip");

    for count in [1, 10, 100, 1000] {
        let fanout = make_fanout_entries(count);
        group.bench_with_input(
            BenchmarkId::new("fanout_entry", count),
            &count,
            |b, _| b.iter(|| {
                let bytes = fanout.as_bytes();
                let decoded = wire::decode_slice::<FanoutEntry>(black_box(bytes)).unwrap();
                black_box(decoded[0].connection_id);
                black_box(decoded[decoded.len() - 1].seq);
            }),
        );

        let claimed = make_claimed_entries(count);
        group.bench_with_input(
            BenchmarkId::new("claimed_entry", count),
            &count,
            |b, _| b.iter(|| {
                let bytes = claimed.as_bytes();
                let decoded = wire::decode_slice::<ClaimedEntry>(black_box(bytes)).unwrap();
                black_box(decoded[0].seq);
                black_box(decoded[decoded.len() - 1].pending_id);
            }),
        );
    }

    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode, bench_roundtrip);
criterion_main!(benches);
