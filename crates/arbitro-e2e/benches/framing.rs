//! framing — encode/decode comparison: libc-style vs Rust-safe vs bytes::BufMut.
//!
//! Frame layout (14-byte header + variable body):
//!   [0..4]   total_len  : u32 BE (includes header)
//!   [4]      msg_type   : u8
//!   [5..9]   seq        : u32 BE
//!   [9..13]  conn_id    : u32 BE
//!   [13]     subject_len: u8
//!   [14..14+slen]        subject bytes
//!   [14+slen..total_len] payload bytes
//!
//! Three encoders + three decoders:
//!   1. libc-style — unsafe, libc::memcpy, raw pointer walks, C-flavored
//!   2. rust-safe  — slices, u32::to_be_bytes, copy_from_slice
//!   3. bytes      — bytes::BufMut / Buf (the one arbitro uses in prod)
//!
//! Expectation: after LLVM optimization all three should be ~identical. The
//! bench exists to *prove* it and detect regressions if that ever changes.
//!
//! Run (testing.md):
//!   cargo bench --bench framing -p arbitro-e2e --no-run
//!   cp target/release/deps/framing-<hash> /tmp/arbitro/
//!   cd /tmp/arbitro && timeout 120 ./framing-<hash> --bench 2>&1 | tee /tmp/bench.log

use bytes::{Buf, BufMut};
use std::time::{Duration, Instant};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};
use zerocopy::big_endian::U32 as BeU32;

// ── Zerocopy header: the 14-byte fixed prefix as a typed view ────────────
//
// #[repr(C, packed)] → no padding, exactly 14 bytes on the wire.
// Immutable + KnownLayout + Unaligned → allow ref_from_bytes on any buffer.
// IntoBytes → header.as_bytes() returns &[u8] (a pointer cast, no copy).
// FromBytes  → FrameHeader::ref_from_prefix(buf) is a ptr cast (no copy).
//
// BeU32 stores bytes big-endian in memory; .get() / ::new() do the byteswap
// at the boundary (compiles to `bswap` on x86).
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
struct FrameHeader {
    total_len: BeU32,
    msg_type: u8,
    seq: BeU32,
    conn_id: BeU32,
    subject_len: u8,
}
const _: () = assert!(std::mem::size_of::<FrameHeader>() == HEADER);

const N: usize = 2_000_000;                  // 2M frames per variant
const RUNS: usize = 3;
const SUBJECT: &[u8] = b"stream.events.orders.v1";
const PAYLOAD_LEN: usize = 64;               // realistic small-msg size
const HEADER: usize = 14;
const FRAME_LEN: usize = HEADER + SUBJECT.len() + PAYLOAD_LEN;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Frame<'a> {
    total_len: u32,
    msg_type: u8,
    seq: u32,
    conn_id: u32,
    subject: &'a [u8],
    payload: &'a [u8],
}

// ── 1. LIBC-STYLE ENCODE ─────────────────────────────────────────────────
//
// C-flavored: raw ptr, libc::memcpy, manual byteswap via u32::to_be() (which
// is htonl on LE hosts). No bounds checks, no slice magic.
#[inline(never)]
fn encode_libc(buf: &mut [u8], f: &Frame) -> usize {
    unsafe {
        let p = buf.as_mut_ptr();
        let tl = f.total_len.to_be();
        libc::memcpy(p as *mut _, &tl as *const u32 as *const _, 4);
        *p.add(4) = f.msg_type;
        let seq = f.seq.to_be();
        libc::memcpy(p.add(5) as *mut _, &seq as *const u32 as *const _, 4);
        let cid = f.conn_id.to_be();
        libc::memcpy(p.add(9) as *mut _, &cid as *const u32 as *const _, 4);
        *p.add(13) = f.subject.len() as u8;
        libc::memcpy(
            p.add(HEADER) as *mut _,
            f.subject.as_ptr() as *const _,
            f.subject.len(),
        );
        libc::memcpy(
            p.add(HEADER + f.subject.len()) as *mut _,
            f.payload.as_ptr() as *const _,
            f.payload.len(),
        );
    }
    f.total_len as usize
}

// ── 2. RUST-SAFE ENCODE ──────────────────────────────────────────────────
//
// Pure stdlib: slice indexing, to_be_bytes, copy_from_slice. Bounds-checked
// (but elided by the optimizer when it can prove the access is in-range).
#[inline(never)]
fn encode_rust(buf: &mut [u8], f: &Frame) -> usize {
    buf[0..4].copy_from_slice(&f.total_len.to_be_bytes());
    buf[4] = f.msg_type;
    buf[5..9].copy_from_slice(&f.seq.to_be_bytes());
    buf[9..13].copy_from_slice(&f.conn_id.to_be_bytes());
    buf[13] = f.subject.len() as u8;
    let slen = f.subject.len();
    buf[HEADER..HEADER + slen].copy_from_slice(f.subject);
    buf[HEADER + slen..HEADER + slen + f.payload.len()].copy_from_slice(f.payload);
    f.total_len as usize
}

// ── 3. bytes::BufMut ENCODE ──────────────────────────────────────────────
//
// What arbitro-server/shard/drain.rs actually uses on the hot path.
#[inline(never)]
fn encode_bytes(buf: &mut Vec<u8>, f: &Frame) -> usize {
    buf.clear();
    buf.put_u32(f.total_len);
    buf.put_u8(f.msg_type);
    buf.put_u32(f.seq);
    buf.put_u32(f.conn_id);
    buf.put_u8(f.subject.len() as u8);
    buf.put_slice(f.subject);
    buf.put_slice(f.payload);
    buf.len()
}

// ── 4. ZEROCOPY ENCODE ───────────────────────────────────────────────────
//
// Build the header as a typed struct, then one memcpy of 14 bytes for the
// whole header (vs 5 small writes in the other variants). Subject + payload
// are still copy_from_slice — there's no way around those without changing
// the frame format to avoid the copies entirely.
#[inline(never)]
fn encode_zerocopy(buf: &mut [u8], f: &Frame) -> usize {
    let h = FrameHeader {
        total_len: BeU32::new(f.total_len),
        msg_type: f.msg_type,
        seq: BeU32::new(f.seq),
        conn_id: BeU32::new(f.conn_id),
        subject_len: f.subject.len() as u8,
    };
    buf[..HEADER].copy_from_slice(h.as_bytes());
    let slen = f.subject.len();
    buf[HEADER..HEADER + slen].copy_from_slice(f.subject);
    buf[HEADER + slen..HEADER + slen + f.payload.len()].copy_from_slice(f.payload);
    f.total_len as usize
}

// ── 1. LIBC-STYLE DECODE ─────────────────────────────────────────────────
#[inline(never)]
fn decode_libc<'a>(buf: &'a [u8]) -> Frame<'a> {
    unsafe {
        let p = buf.as_ptr();
        let mut tl: u32 = 0;
        libc::memcpy(&mut tl as *mut u32 as *mut _, p as *const _, 4);
        let total_len = u32::from_be(tl);
        let msg_type = *p.add(4);
        let mut seq_raw: u32 = 0;
        libc::memcpy(&mut seq_raw as *mut u32 as *mut _, p.add(5) as *const _, 4);
        let seq = u32::from_be(seq_raw);
        let mut cid_raw: u32 = 0;
        libc::memcpy(&mut cid_raw as *mut u32 as *mut _, p.add(9) as *const _, 4);
        let conn_id = u32::from_be(cid_raw);
        let slen = *p.add(13) as usize;
        // Slices are still safe constructs — construct via std::slice::from_raw_parts
        let subject = std::slice::from_raw_parts(p.add(HEADER), slen);
        let payload_off = HEADER + slen;
        let payload_len = total_len as usize - payload_off;
        let payload = std::slice::from_raw_parts(p.add(payload_off), payload_len);
        Frame { total_len, msg_type, seq, conn_id, subject, payload }
    }
}

// ── 2. RUST-SAFE DECODE ──────────────────────────────────────────────────
#[inline(never)]
fn decode_rust<'a>(buf: &'a [u8]) -> Frame<'a> {
    let total_len = u32::from_be_bytes(buf[0..4].try_into().unwrap());
    let msg_type = buf[4];
    let seq = u32::from_be_bytes(buf[5..9].try_into().unwrap());
    let conn_id = u32::from_be_bytes(buf[9..13].try_into().unwrap());
    let slen = buf[13] as usize;
    let subject = &buf[HEADER..HEADER + slen];
    let payload = &buf[HEADER + slen..total_len as usize];
    Frame { total_len, msg_type, seq, conn_id, subject, payload }
}

// ── 3. bytes::Buf DECODE ─────────────────────────────────────────────────
#[inline(never)]
fn decode_bytes<'a>(buf: &'a [u8]) -> Frame<'a> {
    let mut cur = &buf[..];
    let total_len = cur.get_u32();
    let msg_type = cur.get_u8();
    let seq = cur.get_u32();
    let conn_id = cur.get_u32();
    let slen = cur.get_u8() as usize;
    // bytes::Buf doesn't borrow a slice nicely without Bytes; we index manually
    // for the tails. This is still what we do in production.
    let subject = &buf[HEADER..HEADER + slen];
    let payload = &buf[HEADER + slen..total_len as usize];
    Frame { total_len, msg_type, seq, conn_id, subject, payload }
}

// ── 4. ZEROCOPY DECODE ───────────────────────────────────────────────────
//
// `ref_from_prefix` is a pure pointer cast + length check — no byte copy.
// The returned &FrameHeader IS the bytes of buf reinterpreted. .get() on
// each BeU32 byteswaps at read time (single `bswap` instr).
#[inline(never)]
fn decode_zerocopy<'a>(buf: &'a [u8]) -> Frame<'a> {
    let (h, rest) = FrameHeader::ref_from_prefix(buf).expect("header");
    let slen = h.subject_len as usize;
    let total_len = h.total_len.get();
    let subject = &rest[..slen];
    let payload_end = total_len as usize - HEADER;
    let payload = &rest[slen..payload_end];
    Frame {
        total_len,
        msg_type: h.msg_type,
        seq: h.seq.get(),
        conn_id: h.conn_id.get(),
        subject,
        payload,
    }
}

// ── helpers ──────────────────────────────────────────────────────────────
fn sample_frame<'a>(payload: &'a [u8]) -> Frame<'a> {
    Frame {
        total_len: FRAME_LEN as u32,
        msg_type: 0x03,
        seq: 0xDEADBEEF,
        conn_id: 0xCAFEBABE,
        subject: SUBJECT,
        payload,
    }
}

fn report(label: &str, wall: Duration, n: usize) {
    let per_op_ns = wall.as_nanos() as f64 / n as f64;
    let ops_per_s = n as f64 / wall.as_secs_f64();
    let gib_per_s = (n * FRAME_LEN) as f64 / wall.as_secs_f64() / (1024.0 * 1024.0 * 1024.0);
    println!(
        "  {:<24}  wall={:>7.1}ms  {:>7.2} ns/op  {:>8.2} M ops/s  {:>6.2} GiB/s",
        label,
        wall.as_secs_f64() * 1000.0,
        per_op_ns,
        ops_per_s / 1_000_000.0,
        gib_per_s,
    );
}

fn bench_encode<F>(label: &str, mut f: F)
where
    F: FnMut(&mut [u8], &Frame) -> usize,
{
    let payload = vec![0xABu8; PAYLOAD_LEN];
    let frame = sample_frame(&payload);
    let mut buf = vec![0u8; FRAME_LEN];

    // warmup
    for _ in 0..100_000 {
        let n = f(&mut buf, &frame);
        std::hint::black_box(n);
    }

    for run in 1..=RUNS {
        let t0 = Instant::now();
        for _ in 0..N {
            let n = f(&mut buf, &frame);
            std::hint::black_box(n);
        }
        let wall = t0.elapsed();
        report(&format!("{} [{}]", label, run), wall, N);
    }
}

fn bench_encode_bytes(label: &str) {
    let payload = vec![0xABu8; PAYLOAD_LEN];
    let frame = sample_frame(&payload);
    let mut buf: Vec<u8> = Vec::with_capacity(FRAME_LEN);

    for _ in 0..100_000 {
        let n = encode_bytes(&mut buf, &frame);
        std::hint::black_box(n);
    }

    for run in 1..=RUNS {
        let t0 = Instant::now();
        for _ in 0..N {
            let n = encode_bytes(&mut buf, &frame);
            std::hint::black_box(n);
        }
        let wall = t0.elapsed();
        report(&format!("{} [{}]", label, run), wall, N);
    }
}

fn bench_decode<F>(label: &str, mut f: F)
where
    F: for<'a> FnMut(&'a [u8]) -> Frame<'a>,
{
    let payload = vec![0xABu8; PAYLOAD_LEN];
    let frame = sample_frame(&payload);
    let mut buf = vec![0u8; FRAME_LEN];
    encode_rust(&mut buf, &frame);

    for _ in 0..100_000 {
        let f = f(&buf);
        std::hint::black_box(f);
    }

    for run in 1..=RUNS {
        let t0 = Instant::now();
        for _ in 0..N {
            let f = f(&buf);
            std::hint::black_box(f);
        }
        let wall = t0.elapsed();
        report(&format!("{} [{}]", label, run), wall, N);
    }
}

fn main() {
    println!("══════════════════════════════════════════════════════════════════════════════════════");
    println!(
        " framing — frame={}B (header={}B + subject={}B + payload={}B), N={} frames, {} runs",
        FRAME_LEN, HEADER, SUBJECT.len(), PAYLOAD_LEN, N, RUNS,
    );
    println!("══════════════════════════════════════════════════════════════════════════════════════");

    // Sanity: all three encoders produce the same bytes, all three decoders
    // parse back to the same struct. If this fails, benchmark numbers are
    // meaningless — bail early.
    {
        let payload = vec![0xABu8; PAYLOAD_LEN];
        let frame = sample_frame(&payload);
        let mut a = vec![0u8; FRAME_LEN];
        let mut b = vec![0u8; FRAME_LEN];
        let mut c: Vec<u8> = Vec::with_capacity(FRAME_LEN);
        let mut d = vec![0u8; FRAME_LEN];
        encode_libc(&mut a, &frame);
        encode_rust(&mut b, &frame);
        encode_bytes(&mut c, &frame);
        encode_zerocopy(&mut d, &frame);
        assert_eq!(a, b, "libc encode != rust encode");
        assert_eq!(a, c.as_slice(), "libc encode != bytes encode");
        assert_eq!(a, d, "libc encode != zerocopy encode");
        let da = decode_libc(&a);
        let db = decode_rust(&a);
        let dc = decode_bytes(&a);
        let dd = decode_zerocopy(&a);
        assert_eq!(da, db);
        assert_eq!(da, dc);
        assert_eq!(da, dd);
        println!("[sanity] all four encoders/decoders agree ✓\n");
    }

    println!("── encode ─────────────────────────────────────────────────────────");
    bench_encode("libc memcpy", encode_libc);
    bench_encode("rust copy_from_slice", encode_rust);
    bench_encode_bytes("bytes::BufMut");
    bench_encode("zerocopy header", encode_zerocopy);

    println!("\n── decode ─────────────────────────────────────────────────────────");
    bench_decode("libc memcpy", decode_libc);
    bench_decode("rust slice", decode_rust);
    bench_decode("bytes::Buf", decode_bytes);
    bench_decode("zerocopy ref_from", decode_zerocopy);

    println!("\n[done]");
}
