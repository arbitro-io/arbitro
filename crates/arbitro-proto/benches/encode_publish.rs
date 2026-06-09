#![allow(clippy::uninit_vec)] // Bench intentionally uses set_len() for zero-overhead alloc measurement
//! encode_publish — measure the cost of building a publish batch body
//! (`[4 count][12 entry][subject][payload]`) using different encoding
//! strategies. Goal: find the cheapest path with no measurable cost
//! beyond the unavoidable byte-stream construction.
//!
//! The strategies:
//!
//! 1. **piecewise** — what `arbitro-client::Client::publish` does today.
//!    `Vec::with_capacity` + 4 × `extend_from_slice` calls
//!    (count, entry header, subject, payload).
//! 2. **fixed_struct** — collapse the count + `PublishEntry` (16 bytes)
//!    into a single zerocopy struct, write once via `as_bytes()`, then
//!    extend subject + payload. 1 fewer `extend_from_slice` than (1).
//! 3. **inplace** — pre-allocate the full buffer, then write each field
//!    directly via unaligned `*mut` stores. The variable-length suffix
//!    (subject + payload) goes via `copy_nonoverlapping`. Zero
//!    intermediate stack arrays, zero redundant memcpys.
//!
//! All three produce the same bytes. We measure the per-call cost in
//! nanoseconds so we know the compiler-optimized floor for this kind
//! of encoder.
//!
//! Run: `cargo bench --bench encode_publish`

use std::hint::black_box;
use std::time::Instant;

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use arbitro_proto::v2::header::Header;
use arbitro_proto::v2::ingress::pub_frame::PubFrame;
use arbitro_proto::wire::publish::PublishEntry;

// User's idea: combine the v2 Header + subject_len into a single SIZED
// struct. Build it as a struct literal, serialize via `as_bytes()`
// (zero-cost transmute on a sized #[repr(C)] type), then append the
// variable subject || payload. No DST, no runtime layout validation,
// no per-tail bounds checks.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct PublishFixed {
    header: Header,   // 16 B
    subject_len: U16, //  2 B  (LE u16)
    _pad: [u8; 6],    //  6 B  (align to 24 = multiple of 8)
}
const PUBLISH_FIXED_SIZE: usize = core::mem::size_of::<PublishFixed>();
const _: () = assert!(PUBLISH_FIXED_SIZE == 24);

// ── Variant 2 helper: combined "count + entry" header struct. ──────────
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct BatchHeader {
    count: U32,     // 4
    data_len: U32,  // 4
    subj_len: U16,  // 2
    reply_len: U16, // 2
    flags: u8,      // 1
    _pad: [u8; 3],  // 3
}
const BATCH_HEADER_SIZE: usize = core::mem::size_of::<BatchHeader>(); // = 16
const _: () = assert!(BATCH_HEADER_SIZE == 16);

// ── Variant 1: piecewise extend_from_slice (matches Client::publish). ──
#[inline]
fn encode_piecewise(subject: &[u8], payload: &[u8]) -> Vec<u8> {
    let entry = PublishEntry {
        data_len: U32::new(payload.len() as u32),
        subj_len: U16::new(subject.len() as u16),
        reply_len: U16::new(0),
        flags: 0,
        _pad: [0u8; 3],
    };
    let body_len = 4 + 12 + subject.len() + payload.len();
    let mut body = Vec::with_capacity(body_len);
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(entry.as_bytes());
    body.extend_from_slice(subject);
    body.extend_from_slice(payload);
    body
}

// ── Variant 2: build a single 16B header struct, write once. ──────────
#[inline]
fn encode_fixed_struct(subject: &[u8], payload: &[u8]) -> Vec<u8> {
    let hdr = BatchHeader {
        count: U32::new(1),
        data_len: U32::new(payload.len() as u32),
        subj_len: U16::new(subject.len() as u16),
        reply_len: U16::new(0),
        flags: 0,
        _pad: [0u8; 3],
    };
    let total = BATCH_HEADER_SIZE + subject.len() + payload.len();
    let mut body = Vec::with_capacity(total);
    body.extend_from_slice(hdr.as_bytes());
    body.extend_from_slice(subject);
    body.extend_from_slice(payload);
    body
}

// ── Variant 8: reuse a caller-owned `&mut Vec<u8>`. Eliminates the
// per-call malloc. Caller calls `clear()` between iterations and the
// Vec retains its capacity. Same write pattern as `sized_as_bytes`
// but without the `Vec::with_capacity` cost amortized over many
// frames. This is what a steady-state encoder loop should look like.
#[inline]
fn encode_into_existing_buf(out: &mut Vec<u8>, subject: &[u8], payload: &[u8]) {
    out.clear();
    let msg_len = (PUBLISH_FIXED_SIZE - 16 + subject.len() + payload.len()) as u32;
    let pf = PublishFixed {
        header: Header::new(0x0101, msg_len, 1).with_flags(0),
        subject_len: U16::new(subject.len() as u16),
        _pad: [0u8; 6],
    };
    out.extend_from_slice(pf.as_bytes());
    out.extend_from_slice(subject);
    out.extend_from_slice(payload);
}

// ── Variant 9: write into a pre-sized `&mut [u8]` — the
// `PubFrame::encode_into`-style API but using our simpler sized
// PublishFixed struct. Caller is responsible for sizing the slice
// (use `total_size(subject_len, payload_len)`). Zero allocation,
// zero bounds-check overhead from `Vec::extend`, zero unsafe.
#[inline]
fn encode_into_slice(out: &mut [u8], subject: &[u8], payload: &[u8]) -> usize {
    let total = PUBLISH_FIXED_SIZE + subject.len() + payload.len();
    debug_assert_eq!(out.len(), total);
    let msg_len = (PUBLISH_FIXED_SIZE - 16 + subject.len() + payload.len()) as u32;
    let pf = PublishFixed {
        header: Header::new(0x0101, msg_len, 1).with_flags(0),
        subject_len: U16::new(subject.len() as u16),
        _pad: [0u8; 6],
    };
    out[..PUBLISH_FIXED_SIZE].copy_from_slice(pf.as_bytes());
    let s_off = PUBLISH_FIXED_SIZE;
    out[s_off..s_off + subject.len()].copy_from_slice(subject);
    let p_off = s_off + subject.len();
    out[p_off..].copy_from_slice(payload);
    total
}

// ── Variant 7: libc memcpy via FFI. Build the same output as
// sized_as_bytes (header struct + subject + payload), but write into
// the destination Vec via raw `libc::memcpy` calls. Functionally
// equivalent to `std::ptr::copy_nonoverlapping` (both compile to the
// same memcpy intrinsic on x86_64) — included to show that "going
// through libc" is NOT a magic perf win and carries the same unsafe
// caveats as raw pointer writes.
unsafe extern "C" {
    fn memcpy(dest: *mut u8, src: *const u8, n: usize) -> *mut u8;
}

#[inline]
fn encode_libc_memcpy(subject: &[u8], payload: &[u8]) -> Vec<u8> {
    let msg_len = (PUBLISH_FIXED_SIZE - 16 + subject.len() + payload.len()) as u32;
    // Build the same 24B header on the stack via the zerocopy struct,
    // get a slice via as_bytes(), then memcpy into the heap.
    let pf = PublishFixed {
        header: Header::new(0x0101, msg_len, 1).with_flags(0),
        subject_len: U16::new(subject.len() as u16),
        _pad: [0u8; 6],
    };
    let pf_bytes = pf.as_bytes();

    let total = PUBLISH_FIXED_SIZE + subject.len() + payload.len();
    let mut buf = Vec::<u8>::with_capacity(total);
    // SAFETY: every byte is written via memcpy below before any read.
    unsafe {
        buf.set_len(total);
        let p = buf.as_mut_ptr();
        memcpy(p, pf_bytes.as_ptr(), PUBLISH_FIXED_SIZE);
        memcpy(p.add(PUBLISH_FIXED_SIZE), subject.as_ptr(), subject.len());
        memcpy(
            p.add(PUBLISH_FIXED_SIZE + subject.len()),
            payload.as_ptr(),
            payload.len(),
        );
    }
    buf
}

// ── Variant 6: SIZED struct + as_bytes() (the user's idea). Build a
// sized `PublishFixed { header, subject_len, _pad }` with a struct
// literal, then `as_bytes()` is a zero-cost view of the 24 stack
// bytes. No DST, no `mut_from_bytes`. Append subject || payload via
// extend_from_slice. Output: [16B Header][2B subj_len][6B pad][subject][payload]
// = 24 + subject + payload bytes.
#[inline]
fn encode_sized_as_bytes(subject: &[u8], payload: &[u8]) -> Vec<u8> {
    let msg_len = (PUBLISH_FIXED_SIZE - 16 + subject.len() + payload.len()) as u32;
    let pf = PublishFixed {
        header: Header::new(0x0101 /*Action::Publish*/, msg_len, 1).with_flags(0),
        subject_len: U16::new(subject.len() as u16),
        _pad: [0u8; 6],
    };
    let total = PUBLISH_FIXED_SIZE + subject.len() + payload.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(pf.as_bytes()); // ← 24 B header in one go
    buf.extend_from_slice(subject);
    buf.extend_from_slice(payload);
    buf
}

// ── Variant 5: v2 RAW — same output bytes as encode_v2_pubframe but
// without the zerocopy DST machinery. Direct unaligned `*mut` writes
// to each field, `copy_nonoverlapping` for the variable suffix.
// Lets us isolate the overhead of `mut_from_bytes` + slice bounds
// checks from the actual cost of writing the bytes. If this is
// significantly faster than v2_pubframe_zc, the zerocopy overhead is
// the gap; if equal, the compiler is already eliminating that work.
#[inline]
fn encode_v2_raw(subject: &[u8], payload: &[u8]) -> Vec<u8> {
    // Same layout as PubFrame: [16B Header][8B PubBody][tail].
    // Action::Publish = 0x0101.
    const HEADER_SIZE: usize = 16;
    const PUB_BODY_FIXED: usize = 8;
    let total = HEADER_SIZE + PUB_BODY_FIXED + subject.len() + payload.len();
    let msg_len = (PUB_BODY_FIXED + subject.len() + payload.len()) as u32;

    let mut buf = Vec::<u8>::with_capacity(total);
    // SAFETY: every byte is written below before any read.
    unsafe {
        buf.set_len(total);
        let p = buf.as_mut_ptr();

        // ── Header (16B) ────────────────────────────────────────────
        // offset  0..2:  action       u16 LE = 0x0101
        // offset  2:     flags        u8     = 0
        // offset  3:     entry_flags  u8     = 0
        // offset  4..8:  msg_len      u32 LE
        // offset  8..16: seq          u64 LE = 1
        (p as *mut u16).write_unaligned(0x0101u16.to_le());
        *p.add(2) = 0; // flags
        *p.add(3) = 0; // entry_flags
        (p.add(4) as *mut u32).write_unaligned(msg_len.to_le());
        (p.add(8) as *mut u64).write_unaligned(1u64.to_le());

        // ── PubBody (8B) ────────────────────────────────────────────
        // offset 16..20: stream_id   u32 LE = 0
        // offset 20..22: subject_len u16 LE
        // offset 22..24: _pad        u16 LE = 0
        (p.add(16) as *mut u32).write_unaligned(0u32);
        (p.add(20) as *mut u16).write_unaligned((subject.len() as u16).to_le());
        (p.add(22) as *mut u16).write_unaligned(0u16);

        // ── Tail (subject || payload) ──────────────────────────────
        let tail_off = HEADER_SIZE + PUB_BODY_FIXED;
        std::ptr::copy_nonoverlapping(subject.as_ptr(), p.add(tail_off), subject.len());
        std::ptr::copy_nonoverlapping(
            payload.as_ptr(),
            p.add(tail_off + subject.len()),
            payload.len(),
        );
    }
    buf
}

// ── Variant 4: v2 zerocopy `PubFrame::encode_into` (DIFFERENT FORMAT!) ─
// Builds a full v2 PUB frame: [16B Header][8B PubBody][subject][payload]
// vs the legacy body format used by V1-V3: [4B count][12B PublishEntry][subject][payload].
#[inline]
fn encode_v2_pubframe(subject: &[u8], payload: &[u8]) -> Vec<u8> {
    let size = PubFrame::wire_size(subject.len(), 0, payload.len());
    // SAFETY: encode_into writes every byte before any read.
    let mut buf = Vec::<u8>::with_capacity(size);
    unsafe {
        buf.set_len(size);
    }
    let _ = PubFrame::encode_into(
        &mut buf,
        /*seq*/ 1,
        /*stream_id*/ 0,
        /*flags*/ 0,
        /*entry_flags*/ 0,
        subject,
        /*msg_id*/ &[],
        payload,
    );
    buf
}

// ── Variant 3: in-place direct writes via unaligned pointer stores. ───
#[inline]
fn encode_inplace(subject: &[u8], payload: &[u8]) -> Vec<u8> {
    let total = 16 + subject.len() + payload.len();
    let mut body = Vec::<u8>::with_capacity(total);
    // SAFETY: we set len() to the full capacity and write every byte
    // (including the flags + pad bytes) before any read.
    unsafe {
        body.set_len(total);
        let p = body.as_mut_ptr();
        // count + data_len + subj_len + reply_len in one shot, four LE
        // unaligned stores. No `[u8; 4]` stack temporaries.
        (p as *mut u32).write_unaligned(1u32.to_le());
        (p.add(4) as *mut u32).write_unaligned((payload.len() as u32).to_le());
        (p.add(8) as *mut u16).write_unaligned((subject.len() as u16).to_le());
        (p.add(10) as *mut u16).write_unaligned(0u16.to_le());
        // flags + 3 pad bytes — single u32 zero store.
        (p.add(12) as *mut u32).write_unaligned(0u32);
        // Variable-length suffix.
        std::ptr::copy_nonoverlapping(subject.as_ptr(), p.add(16), subject.len());
        std::ptr::copy_nonoverlapping(payload.as_ptr(), p.add(16 + subject.len()), payload.len());
    }
    body
}

// ── Bench harness — warmup + N runs, report min + mean per call. ──────
fn bench_one(name: &str, payload_size: usize, iters: usize, runs: usize) {
    let subject = b"bench.subject.path";
    let payload = vec![0xAAu8; payload_size];

    // Sanity check: V1/V2/V3 produce the same legacy body bytes.
    // V4 (PubFrame::encode_into) and V5 (v2_raw) produce a different
    // format (full v2 frame including 16B Header + 10B PubBody) but
    // they MUST produce identical bytes between themselves.
    let a = encode_piecewise(subject, &payload);
    let b = encode_fixed_struct(subject, &payload);
    let c = encode_inplace(subject, &payload);
    let d = encode_v2_pubframe(subject, &payload);
    let e = encode_v2_raw(subject, &payload);
    assert_eq!(a, b, "piecewise vs fixed_struct mismatch");
    assert_eq!(a, c, "piecewise vs inplace mismatch");
    assert_eq!(
        d.len(),
        16 + 8 + subject.len() + payload.len(),
        "v2 pubframe wire size mismatch"
    );
    assert_eq!(
        d, e,
        "v2_pubframe (zerocopy) vs v2_raw (unsafe ptr) mismatch"
    );

    fn run(iters: usize, f: impl Fn() -> Vec<u8>) -> u128 {
        // Warmup
        for _ in 0..1000 {
            let v = f();
            black_box(v);
        }
        let t0 = Instant::now();
        for _ in 0..iters {
            let v = f();
            black_box(v);
        }
        t0.elapsed().as_nanos()
    }

    println!("\n── {name} (payload={payload_size}B, iters={iters}) ──");
    println!(
        "{:<18} {:>10} {:>14} {:>14}",
        "variant", "min ns/op", "mean ns/op", "ops/sec"
    );
    println!("{}", "─".repeat(60));

    for (label, f) in [
        (
            "piecewise",
            &(encode_piecewise as fn(&[u8], &[u8]) -> Vec<u8>),
        ),
        ("fixed_struct", &(encode_fixed_struct as _)),
        ("inplace", &(encode_inplace as _)),
        ("v2_pubframe_zc", &(encode_v2_pubframe as _)),
        ("v2_raw", &(encode_v2_raw as _)),
        ("sized_as_bytes", &(encode_sized_as_bytes as _)),
        ("libc_memcpy", &(encode_libc_memcpy as _)),
    ] {
        let mut samples: Vec<u128> = Vec::with_capacity(runs);
        for _ in 0..runs {
            let ns = run(iters, || f(subject, &payload));
            samples.push(ns);
        }
        samples.sort();
        let min = samples[0] as f64 / iters as f64;
        let mean = samples.iter().sum::<u128>() as f64 / runs as f64 / iters as f64;
        let ops_per_sec = 1.0e9 / min;
        println!("{label:<18} {min:>10.2} {mean:>14.2} {ops_per_sec:>14.0}");
    }

    // ── Variants 8 + 9: zero-alloc per call. Outer loop reuses one buffer.
    // ── Variant 8: `encode_into_existing_buf` — reuse a Vec<u8>.
    {
        let mut samples: Vec<u128> = Vec::with_capacity(runs);
        for _ in 0..runs {
            let mut reuse =
                Vec::<u8>::with_capacity(PUBLISH_FIXED_SIZE + subject.len() + payload_size);
            // Warmup
            for _ in 0..1000 {
                encode_into_existing_buf(&mut reuse, subject, &payload);
                black_box(&reuse);
            }
            let t0 = Instant::now();
            for _ in 0..iters {
                encode_into_existing_buf(&mut reuse, subject, &payload);
                black_box(&reuse);
            }
            samples.push(t0.elapsed().as_nanos());
        }
        samples.sort();
        let min = samples[0] as f64 / iters as f64;
        let mean = samples.iter().sum::<u128>() as f64 / runs as f64 / iters as f64;
        let ops_per_sec = 1.0e9 / min;
        println!(
            "{:<18} {:>10.2} {:>14.2} {:>14.0}",
            "reuse_vec", min, mean, ops_per_sec
        );
    }

    // ── Variant 9: `encode_into_slice` — pre-sized &mut [u8].
    {
        let total = PUBLISH_FIXED_SIZE + subject.len() + payload.len();
        let mut samples: Vec<u128> = Vec::with_capacity(runs);
        for _ in 0..runs {
            let mut reuse = vec![0u8; total];
            // Warmup
            for _ in 0..1000 {
                encode_into_slice(&mut reuse, subject, &payload);
                black_box(&reuse);
            }
            let t0 = Instant::now();
            for _ in 0..iters {
                encode_into_slice(&mut reuse, subject, &payload);
                black_box(&reuse);
            }
            samples.push(t0.elapsed().as_nanos());
        }
        samples.sort();
        let min = samples[0] as f64 / iters as f64;
        let mean = samples.iter().sum::<u128>() as f64 / runs as f64 / iters as f64;
        let ops_per_sec = 1.0e9 / min;
        println!(
            "{:<18} {:>10.2} {:>14.2} {:>14.0}",
            "into_slice", min, mean, ops_per_sec
        );
    }
}

fn main() {
    println!("=== encode_publish — encoding strategy comparison ===");
    println!("Builds `[4 count][12 PublishEntry][subject][payload]`.");
    println!();

    let iters = 100_000;
    let runs = 7;
    let subject_b = b"bench.subject.path"; // 18 bytes
    let _ = subject_b;

    for &payload_size in &[64usize, 256, 1024, 4096] {
        bench_one("encode body", payload_size, iters, runs);
    }
    println!("\nDone.");
}
