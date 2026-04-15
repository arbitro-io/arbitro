//! Benchmark: variable-size entries with inline subject_len + payload_len,
//! read back as zerocopy DST (header + subject slice + payload slice).
//!
//! Goal: measure how fast an in-memory segmented log can scan entries when
//! the header is parsed via `zerocopy::Ref` (no alloc, no parse, pointer cast),
//! and subject/payload are returned as `&[u8]` slices over the same buffer.
//!
//! Layout per entry (packed, no padding between records):
//!
//!     [VarEntryHeader = 32 B][subject: subject_len B][payload: payload_len B]
//!
//! The header carries `subject_len` + `payload_len` so the reader can advance
//! to the next record without any external index. Optional: a `Vec<u32>` of
//! offsets lets you do O(1) random access by ordinal.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

// ── Entry layout ──────────────────────────────────────────────────────────
// Uses zerocopy's little-endian primitives which are `Unaligned`,
// so headers can be read from arbitrary offsets via pointer cast.

#[derive(Copy, Clone, IntoBytes, FromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct VarEntryHeader {
    pub seq: U64,           // 8
    pub ts: U64,            // 8
    pub subject_hash: U32,  // 4
    pub payload_len: U32,   // 4
    pub subject_len: U16,   // 2
    pub flags: U16,         // 2
    pub _pad: U32,          // 4
} // 32 B

const HEADER_SIZE: usize = core::mem::size_of::<VarEntryHeader>();
const _: () = assert!(HEADER_SIZE == 32);

#[inline(always)]
fn fnv1a_32(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

// ── In-memory segmented-log buffer ────────────────────────────────────────

pub struct VarLog {
    buf: Vec<u8>,
    offsets: Vec<u32>, // offset of each entry start (for O(1) random access)
    next_seq: u64,
    cursor: usize,
}

impl VarLog {
    pub fn with_capacity(bytes: usize, entries: usize) -> Self {
        Self {
            buf: vec![0u8; bytes],
            offsets: Vec::with_capacity(entries),
            next_seq: 1,
            cursor: 0,
        }
    }

    #[inline]
    pub fn append(&mut self, subject: &[u8], payload: &[u8], ts: u64) -> u64 {
        let seq = self.next_seq;
        let total = HEADER_SIZE + subject.len() + payload.len();
        debug_assert!(self.cursor + total <= self.buf.len(), "log overflow");

        let hdr = VarEntryHeader {
            seq: U64::new(seq),
            ts: U64::new(ts),
            subject_hash: U32::new(fnv1a_32(subject)),
            payload_len: U32::new(payload.len() as u32),
            subject_len: U16::new(subject.len() as u16),
            flags: U16::new(0),
            _pad: U32::new(0),
        };

        let start = self.cursor;
        // Zero-copy header write via zerocopy::IntoBytes
        self.buf[start..start + HEADER_SIZE].copy_from_slice(hdr.as_bytes());
        let subj_off = start + HEADER_SIZE;
        self.buf[subj_off..subj_off + subject.len()].copy_from_slice(subject);
        let pay_off = subj_off + subject.len();
        self.buf[pay_off..pay_off + payload.len()].copy_from_slice(payload);

        self.offsets.push(start as u32);
        self.cursor += total;
        self.next_seq += 1;
        seq
    }

    /// Zero-copy view over a single entry located at `offset`.
    /// Returns `(header_ref, subject_slice, payload_slice)` all borrowing `self.buf`.
    #[inline(always)]
    pub fn view_at(&self, offset: usize) -> (&VarEntryHeader, &[u8], &[u8]) {
        // Pointer cast via zerocopy — no parse, no copy, no alloc.
        let hdr = VarEntryHeader::ref_from_prefix(&self.buf[offset..])
            .expect("header zerocopy")
            .0;
        let subj_off = offset + HEADER_SIZE;
        let pay_off = subj_off + hdr.subject_len.get() as usize;
        let pay_end = pay_off + hdr.payload_len.get() as usize;
        let subject = &self.buf[subj_off..pay_off];
        let payload = &self.buf[pay_off..pay_end];
        (hdr, subject, payload)
    }

    /// Sequential scan — walks the whole log using only the inline lengths.
    /// No offset index needed; the scanner advances itself via each header.
    #[inline]
    pub fn scan_all<F: FnMut(&VarEntryHeader, &[u8], &[u8])>(&self, mut f: F) {
        let mut off = 0usize;
        let end = self.cursor;
        while off < end {
            let hdr = VarEntryHeader::ref_from_prefix(&self.buf[off..])
                .expect("header zerocopy")
                .0;
            let subj_off = off + HEADER_SIZE;
            let pay_off = subj_off + hdr.subject_len.get() as usize;
            let pay_end = pay_off + hdr.payload_len.get() as usize;
            f(hdr, &self.buf[subj_off..pay_off], &self.buf[pay_off..pay_end]);
            off = pay_end;
        }
    }
}

// ── Fixtures ──────────────────────────────────────────────────────────────

fn fixture(n: usize, payload_size: usize) -> VarLog {
    let subject = b"orders.created.v1".as_slice();
    let payload = vec![0xABu8; payload_size];
    let total_bytes = n * (HEADER_SIZE + subject.len() + payload_size) + 1024;
    let mut log = VarLog::with_capacity(total_bytes, n);
    for i in 0..n {
        log.append(subject, &payload, 1_700_000_000 + i as u64);
    }
    log
}

// ── Benches ───────────────────────────────────────────────────────────────

fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("var_entry_append");
    for &payload_size in &[16usize, 64, 256, 1024] {
        let subject = b"orders.created.v1".as_slice();
        let payload = vec![0xABu8; payload_size];
        let n = 10_000usize;
        let cap = n * (HEADER_SIZE + subject.len() + payload_size) + 1024;
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::new("append", payload_size),
            &payload_size,
            |b, _| {
                b.iter_batched(
                    || VarLog::with_capacity(cap, n),
                    |mut log| {
                        for i in 0..n {
                            log.append(subject, &payload, i as u64);
                        }
                        black_box(log.cursor)
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("var_entry_scan");
    for &payload_size in &[16usize, 64, 256, 1024] {
        let n = 100_000usize;
        let log = fixture(n, payload_size);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::new("scan_all", payload_size),
            &payload_size,
            |b, _| {
                b.iter(|| {
                    let mut sum = 0u64;
                    log.scan_all(|hdr, subj, pay| {
                        // Touch each field so optimizer can't elide the walk.
                        sum = sum
                            .wrapping_add(hdr.seq.get())
                            .wrapping_add(subj.len() as u64)
                            .wrapping_add(pay.len() as u64)
                            .wrapping_add(pay[0] as u64);
                    });
                    black_box(sum)
                });
            },
        );
    }
    group.finish();
}

fn bench_random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("var_entry_random_access");
    for &payload_size in &[16usize, 64, 256, 1024] {
        let n = 100_000usize;
        let log = fixture(n, payload_size);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::new("view_at_all", payload_size),
            &payload_size,
            |b, _| {
                b.iter(|| {
                    let mut sum = 0u64;
                    for &off in &log.offsets {
                        let (hdr, subj, pay) = log.view_at(off as usize);
                        sum = sum
                            .wrapping_add(hdr.seq.get())
                            .wrapping_add(subj.len() as u64)
                            .wrapping_add(pay.len() as u64)
                            .wrapping_add(pay[0] as u64);
                    }
                    black_box(sum)
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_append, bench_scan, bench_random_access);
criterion_main!(benches);
