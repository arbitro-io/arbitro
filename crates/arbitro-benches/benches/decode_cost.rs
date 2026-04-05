//! Decode cost: zerocopy overlay vs manual offset for subject extraction.
//!
//! Measures the cost of:
//! 1. zerocopy ref_from_bytes on a fixed entry header, then slice subject
//! 2. Manual u8 offset arithmetic to slice subject directly
//! 3. Two zerocopy decodes (envelope + entry header)
//!
//! All operate on the same pre-built buffer in memory — no I/O.

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

// ── Wire types ──────────────────────────────────────────────────────────────

/// 16B transport envelope.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct Envelope {
    action: U16,
    flags: u8,
    _rsv: u8,
    stream_id: U32,
    msg_len: U32,
    env_seq: U32,
}
const ENV: usize = std::mem::size_of::<Envelope>();
const _: () = assert!(ENV == 16);

/// 12B fixed entry header (subject + reply_to lengths always present).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct EntryHeader {
    data_len: U32,
    subj_len: U16,
    reply_len: U16,
    flags: u8,
    _pad: [u8; 3],
}
const EH: usize = std::mem::size_of::<EntryHeader>();
const _: () = assert!(EH == 12);

const ACT_PUBLISH: u16 = 0x0101;
const SUBJECT: &[u8] = b"orders.created";
const REPLY_TO: &[u8] = b"_INBOX.abc123";
const PAYLOAD: &[u8] = b"{\"order_id\":99}";

// ── Buffer builder ──────────────────────────────────────────────────────────

/// Builds: [Envelope 16B][count 2B][EntryHeader 12B][subject][reply_to][payload]
fn build_frame() -> Vec<u8> {
    let entry_body = SUBJECT.len() + REPLY_TO.len() + PAYLOAD.len();
    let msg_len = 2 + EH + entry_body; // count + 1 entry

    let mut buf = Vec::with_capacity(ENV + msg_len);

    // Envelope
    let env = Envelope {
        action: ACT_PUBLISH.into(),
        flags: 0,
        _rsv: 0,
        stream_id: 42u32.into(),
        msg_len: (msg_len as u32).into(),
        env_seq: 1u32.into(),
    };
    buf.extend_from_slice(env.as_bytes());

    // Count
    buf.extend_from_slice(&1u16.to_le_bytes());

    // Entry header
    let eh = EntryHeader {
        data_len: (PAYLOAD.len() as u32).into(),
        subj_len: (SUBJECT.len() as u16).into(),
        reply_len: (REPLY_TO.len() as u16).into(),
        flags: 0,
        _pad: [0; 3],
    };
    buf.extend_from_slice(eh.as_bytes());

    // Variable-length data
    buf.extend_from_slice(SUBJECT);
    buf.extend_from_slice(REPLY_TO);
    buf.extend_from_slice(PAYLOAD);

    buf
}

// ── Decode strategies ───────────────────────────────────────────────────────

/// Strategy 1: Manual offset — read u16 at known offset, slice subject.
#[inline(always)]
fn manual_subject(buf: &[u8]) -> &[u8] {
    // Skip envelope(16) + count(2) = 18, entry header starts at 18
    // subj_len is at offset 18+4 = 22 (2 bytes LE)
    let subj_len = u16::from_le_bytes([buf[22], buf[23]]) as usize;
    // Subject starts after envelope(16) + count(2) + entry_header(12) = 30
    &buf[30..30 + subj_len]
}

/// Strategy 2: One zerocopy decode — entry header overlay, then slice.
#[inline(always)]
fn zerocopy_entry_subject(buf: &[u8]) -> &[u8] {
    // Skip envelope(16) + count(2) = 18
    let eh = EntryHeader::ref_from_bytes(&buf[18..18 + EH]).unwrap();
    let subj_len = eh.subj_len.get() as usize;
    &buf[30..30 + subj_len]
}

/// Strategy 3: Two zerocopy decodes — envelope + entry header.
#[inline(always)]
fn zerocopy_both(buf: &[u8]) -> (&[u8], u16, u32) {
    let env = Envelope::ref_from_bytes(&buf[..ENV]).unwrap();
    let action = env.action.get();
    let stream_id = env.stream_id.get();

    let eh = EntryHeader::ref_from_bytes(&buf[18..18 + EH]).unwrap();
    let subj_len = eh.subj_len.get() as usize;
    let subject = &buf[30..30 + subj_len];

    (subject, action, stream_id)
}

/// Strategy 4: Manual everything — no zerocopy at all.
#[inline(always)]
fn manual_both(buf: &[u8]) -> (&[u8], u16, u32) {
    let action = u16::from_le_bytes([buf[0], buf[1]]);
    let stream_id = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);

    let subj_len = u16::from_le_bytes([buf[22], buf[23]]) as usize;
    let subject = &buf[30..30 + subj_len];

    (subject, action, stream_id)
}

// ── Benchmarks ──────────────────────────────────────────────────────────────

fn bench_decode(c: &mut Criterion) {
    let frame = build_frame();

    // Verify correctness
    assert_eq!(manual_subject(&frame), SUBJECT);
    assert_eq!(zerocopy_entry_subject(&frame), SUBJECT);
    let (s, a, sid) = zerocopy_both(&frame);
    assert_eq!(s, SUBJECT);
    assert_eq!(a, ACT_PUBLISH);
    assert_eq!(sid, 42);
    let (s2, a2, sid2) = manual_both(&frame);
    assert_eq!(s2, SUBJECT);
    assert_eq!(a2, ACT_PUBLISH);
    assert_eq!(sid2, 42);

    eprintln!("frame size: {}B", frame.len());

    let mut g = c.benchmark_group("decode_subject");
    g.throughput(Throughput::Elements(1));
    g.sample_size(100);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(3));

    g.bench_function("manual_subject_only", |b| {
        b.iter(|| black_box(manual_subject(black_box(&frame))))
    });

    g.bench_function("zerocopy_entry_subject", |b| {
        b.iter(|| black_box(zerocopy_entry_subject(black_box(&frame))))
    });

    g.bench_function("zerocopy_both_env_entry", |b| {
        b.iter(|| black_box(zerocopy_both(black_box(&frame))))
    });

    g.bench_function("manual_both", |b| {
        b.iter(|| black_box(manual_both(black_box(&frame))))
    });

    g.finish();
}

// ── Batch decode: N entries ─────────────────────────────────────────────────

const N: usize = 100;

fn build_batch_frame() -> Vec<u8> {
    let entry_body = SUBJECT.len() + REPLY_TO.len() + PAYLOAD.len();
    let msg_len = 2 + N * (EH + entry_body);

    let mut buf = Vec::with_capacity(ENV + msg_len);

    let env = Envelope {
        action: ACT_PUBLISH.into(),
        flags: 0,
        _rsv: 0,
        stream_id: 42u32.into(),
        msg_len: (msg_len as u32).into(),
        env_seq: 1u32.into(),
    };
    buf.extend_from_slice(env.as_bytes());
    buf.extend_from_slice(&(N as u16).to_le_bytes());

    for _ in 0..N {
        let eh = EntryHeader {
            data_len: (PAYLOAD.len() as u32).into(),
            subj_len: (SUBJECT.len() as u16).into(),
            reply_len: (REPLY_TO.len() as u16).into(),
            flags: 0,
            _pad: [0; 3],
        };
        buf.extend_from_slice(eh.as_bytes());
        buf.extend_from_slice(SUBJECT);
        buf.extend_from_slice(REPLY_TO);
        buf.extend_from_slice(PAYLOAD);
    }

    buf
}

/// Iterate N entries with zerocopy overlay per entry.
#[inline(always)]
fn zerocopy_iterate(buf: &[u8]) -> usize {
    let count = u16::from_le_bytes([buf[ENV], buf[ENV + 1]]) as usize;
    let mut offset = ENV + 2;
    let mut total_subj = 0usize;

    for _ in 0..count {
        let eh = EntryHeader::ref_from_bytes(&buf[offset..offset + EH]).unwrap();
        let subj_len = eh.subj_len.get() as usize;
        let reply_len = eh.reply_len.get() as usize;
        let data_len = eh.data_len.get() as usize;
        let subject = &buf[offset + EH..offset + EH + subj_len];
        total_subj += subject.len();
        offset += EH + subj_len + reply_len + data_len;
    }
    total_subj
}

/// Iterate N entries with manual offset arithmetic.
#[inline(always)]
fn manual_iterate(buf: &[u8]) -> usize {
    let count = u16::from_le_bytes([buf[ENV], buf[ENV + 1]]) as usize;
    let mut offset = ENV + 2;
    let mut total_subj = 0usize;

    for _ in 0..count {
        let data_len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
        let subj_len = u16::from_le_bytes(buf[offset + 4..offset + 6].try_into().unwrap()) as usize;
        let reply_len = u16::from_le_bytes(buf[offset + 6..offset + 8].try_into().unwrap()) as usize;
        let subject = &buf[offset + EH..offset + EH + subj_len];
        total_subj += subject.len();
        offset += EH + subj_len + reply_len + data_len;
    }
    total_subj
}

fn bench_batch_decode(c: &mut Criterion) {
    let frame = build_batch_frame();

    let z = zerocopy_iterate(&frame);
    let m = manual_iterate(&frame);
    assert_eq!(z, m);
    assert_eq!(z, N * SUBJECT.len());

    eprintln!("batch frame: {}B, {} entries", frame.len(), N);

    let mut g = c.benchmark_group("decode_batch");
    g.throughput(Throughput::Elements(N as u64));
    g.sample_size(100);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(3));

    g.bench_function("zerocopy_iterate", |b| {
        b.iter(|| black_box(zerocopy_iterate(black_box(&frame))))
    });

    g.bench_function("manual_iterate", |b| {
        b.iter(|| black_box(manual_iterate(black_box(&frame))))
    });

    g.finish();
}

criterion_group!(benches, bench_decode, bench_batch_decode);
criterion_main!(benches);
