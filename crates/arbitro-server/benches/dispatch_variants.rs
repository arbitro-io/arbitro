//! Dispatch strategies — raw routing cost, 3 variants.
//!
//! ## What this measures
//!
//! The ONLY cost under test is **routing a frame to its handler**. Handlers
//! are intentionally trivial (one `black_box` on a counter) so the bench
//! isolates the dispatch itself, not the work after dispatch.
//!
//! ## The 3 variants
//!
//! **V1 — Per-action code (current arbitro)**
//!   Envelope has a dedicated `action: U16` per operation (20 variants).
//!   Outer `match env.action` routes directly.
//!
//! **V2 — Single entry + inner discriminator**
//!   Envelope's `action` is always `Action::Ops`. Body's first byte is an
//!   `op_type: u8` that the handler reads with zerocopy and matches inside.
//!
//! **V3 — Multi-op per frame vs per-op frames (same total work)**
//!   V3a: one MultiOps envelope with N ops packed inline.
//!   V3b: N separate single-op frames, same V1 dispatch applied N times.
//!   Compares wire+dispatch amortisation.
//!
//! ## Env vars
//!
//!   BENCH_DISPATCH_ITERS    default 10_000_000
//!   BENCH_DISPATCH_OPS_PER_MULTIFRAME   default 8
//!
//! ## Run
//!
//! ```bash
//! wsl bash -lc "cd /mnt/.../arbitro && \
//!   cargo bench --bench dispatch_variants -p arbitro-server --no-run"
//! wsl bash -lc "cp .../target/release/deps/dispatch_variants-* /tmp/arbitro-bench/ \
//!   && cd /tmp/arbitro-bench && ./dispatch_variants-* --bench"
//! ```

#![allow(unused)]

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

const ITERS: usize = 10_000_000;
const OPS_PER_MULTIFRAME: usize = 8;

fn env_usize(var: &str, fallback: usize) -> usize {
    std::env::var(var).ok().and_then(|s| s.parse().ok()).unwrap_or(fallback)
}

// ── Wire formats ─────────────────────────────────────────────────────────────

/// Shared 16-byte envelope for V1 and V2.
#[repr(C)]
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Copy, Clone)]
struct Envelope {
    action: U16,
    flags: u8,
    rsv: u8,
    stream_id: U32,
    msg_len: U32,
    env_seq: U32,
}
const ENVELOPE_SIZE: usize = std::mem::size_of::<Envelope>();

/// Per-op header for MultiOps frames (V3a). 8 bytes aligned.
#[repr(C)]
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Copy, Clone)]
struct OpHeader {
    op_type: u8,
    _pad: [u8; 3],
    op_len: U32,
}
const OP_HEADER_SIZE: usize = std::mem::size_of::<OpHeader>();

// Action codes for V1 (20 distinct variants).
const ACTION_CODES_V1: [u16; 20] = [
    0x0100, 0x0101, 0x0102, 0x0103, 0x0104,
    0x0200, 0x0201, 0x0202, 0x0203, 0x0204,
    0x0300, 0x0301, 0x0302, 0x0303, 0x0304,
    0x0400, 0x0401, 0x0402, 0x0403, 0x0404,
];

// In V2 / V3, the envelope uses this single action code:
const ACTION_OPS: u16 = 0x0FFF;

// ── Handlers (do the same trivial work for all variants) ─────────────────────

// A single global counter — black_box prevents the compiler from erasing
// the handler body, so the dispatch cost is real.
static SINK: AtomicU64 = AtomicU64::new(0);

#[inline(always)]
fn do_handler_work(op: u8, body: &[u8]) {
    // One branch-predictable atomic op + one memory touch — simulates
    // the minimum work any real handler would do.
    SINK.fetch_add(op as u64, Relaxed);
    if !body.is_empty() {
        black_box(body[0]);
    }
}

// ── V1 — Per-action dispatch ─────────────────────────────────────────────────

#[inline(always)]
fn dispatch_v1(buf: &[u8]) {
    let env = Envelope::ref_from_bytes(&buf[..ENVELOPE_SIZE]).unwrap();
    let body = &buf[ENVELOPE_SIZE..];
    let action = env.action.get();

    // Compiler produces a jump table for this match (20 variants, u16).
    match action {
        0x0100 => do_handler_work(0, body),
        0x0101 => do_handler_work(1, body),
        0x0102 => do_handler_work(2, body),
        0x0103 => do_handler_work(3, body),
        0x0104 => do_handler_work(4, body),
        0x0200 => do_handler_work(5, body),
        0x0201 => do_handler_work(6, body),
        0x0202 => do_handler_work(7, body),
        0x0203 => do_handler_work(8, body),
        0x0204 => do_handler_work(9, body),
        0x0300 => do_handler_work(10, body),
        0x0301 => do_handler_work(11, body),
        0x0302 => do_handler_work(12, body),
        0x0303 => do_handler_work(13, body),
        0x0304 => do_handler_work(14, body),
        0x0400 => do_handler_work(15, body),
        0x0401 => do_handler_work(16, body),
        0x0402 => do_handler_work(17, body),
        0x0403 => do_handler_work(18, body),
        0x0404 => do_handler_work(19, body),
        _ => {}
    }
}

// ── V2 — Single entry point, inner discriminator byte ───────────────────────

#[inline(always)]
fn dispatch_v2(buf: &[u8]) {
    let env = Envelope::ref_from_bytes(&buf[..ENVELOPE_SIZE]).unwrap();
    // In V2 the outer action is always Ops; no outer branch.
    debug_assert_eq!(env.action.get(), ACTION_OPS);
    let body = &buf[ENVELOPE_SIZE..];
    // First byte of body is the op discriminator.
    let op = body[0];
    let inner_body = &body[1..];

    match op {
        0 => do_handler_work(0, inner_body),
        1 => do_handler_work(1, inner_body),
        2 => do_handler_work(2, inner_body),
        3 => do_handler_work(3, inner_body),
        4 => do_handler_work(4, inner_body),
        5 => do_handler_work(5, inner_body),
        6 => do_handler_work(6, inner_body),
        7 => do_handler_work(7, inner_body),
        8 => do_handler_work(8, inner_body),
        9 => do_handler_work(9, inner_body),
        10 => do_handler_work(10, inner_body),
        11 => do_handler_work(11, inner_body),
        12 => do_handler_work(12, inner_body),
        13 => do_handler_work(13, inner_body),
        14 => do_handler_work(14, inner_body),
        15 => do_handler_work(15, inner_body),
        16 => do_handler_work(16, inner_body),
        17 => do_handler_work(17, inner_body),
        18 => do_handler_work(18, inner_body),
        19 => do_handler_work(19, inner_body),
        _ => {}
    }
}

// ── V3a — Multi-ops per frame ───────────────────────────────────────────────

#[inline(always)]
fn dispatch_v3_multiframe(buf: &[u8]) {
    let env = Envelope::ref_from_bytes(&buf[..ENVELOPE_SIZE]).unwrap();
    let body_len = env.msg_len.get() as usize;
    let mut offset = ENVELOPE_SIZE;
    let end = offset + body_len;

    while offset + OP_HEADER_SIZE <= end {
        let oph = OpHeader::ref_from_bytes(&buf[offset..offset + OP_HEADER_SIZE]).unwrap();
        let op_type = oph.op_type;
        let op_len = oph.op_len.get() as usize;
        let op_body_start = offset + OP_HEADER_SIZE;
        let op_body_end = op_body_start + op_len;
        if op_body_end > end {
            break;
        }
        do_handler_work(op_type, &buf[op_body_start..op_body_end]);
        offset = op_body_end;
    }
}

// ── Frame builders ──────────────────────────────────────────────────────────

fn build_v1_frame(action: u16, body_payload: &[u8]) -> Vec<u8> {
    let mut buf = vec![0u8; ENVELOPE_SIZE + body_payload.len()];
    let env = Envelope {
        action: U16::new(action),
        flags: 0,
        rsv: 0,
        stream_id: U32::new(1),
        msg_len: U32::new(body_payload.len() as u32),
        env_seq: U32::new(0),
    };
    buf[..ENVELOPE_SIZE].copy_from_slice(env.as_bytes());
    buf[ENVELOPE_SIZE..].copy_from_slice(body_payload);
    buf
}

fn build_v2_frame(op_type: u8, inner_body: &[u8]) -> Vec<u8> {
    let body_len = 1 + inner_body.len();
    let mut buf = vec![0u8; ENVELOPE_SIZE + body_len];
    let env = Envelope {
        action: U16::new(ACTION_OPS),
        flags: 0,
        rsv: 0,
        stream_id: U32::new(1),
        msg_len: U32::new(body_len as u32),
        env_seq: U32::new(0),
    };
    buf[..ENVELOPE_SIZE].copy_from_slice(env.as_bytes());
    buf[ENVELOPE_SIZE] = op_type;
    buf[ENVELOPE_SIZE + 1..].copy_from_slice(inner_body);
    buf
}

/// V3a: one frame with N ops packed inside.
fn build_v3_multiframe(ops: &[(u8, &[u8])]) -> Vec<u8> {
    let body_len: usize = ops
        .iter()
        .map(|(_, b)| OP_HEADER_SIZE + b.len())
        .sum();
    let mut buf = vec![0u8; ENVELOPE_SIZE + body_len];
    let env = Envelope {
        action: U16::new(ACTION_OPS),
        flags: 0,
        rsv: 0,
        stream_id: U32::new(1),
        msg_len: U32::new(body_len as u32),
        env_seq: U32::new(0),
    };
    buf[..ENVELOPE_SIZE].copy_from_slice(env.as_bytes());

    let mut off = ENVELOPE_SIZE;
    for &(op_type, body) in ops {
        let oph = OpHeader {
            op_type,
            _pad: [0; 3],
            op_len: U32::new(body.len() as u32),
        };
        buf[off..off + OP_HEADER_SIZE].copy_from_slice(oph.as_bytes());
        off += OP_HEADER_SIZE;
        buf[off..off + body.len()].copy_from_slice(body);
        off += body.len();
    }
    buf
}

// ── Benches ──────────────────────────────────────────────────────────────────

fn bench_v1(iters: usize) -> f64 {
    // Pre-build a batch of frames covering all 20 actions (distributed evenly).
    let payload = vec![0xAAu8; 8];
    let frames: Vec<Vec<u8>> = ACTION_CODES_V1
        .iter()
        .map(|a| build_v1_frame(*a, &payload))
        .collect();

    // Warmup.
    for _ in 0..10_000 {
        for f in &frames {
            dispatch_v1(f);
        }
    }

    let start = Instant::now();
    let mut done = 0;
    while done < iters {
        for f in &frames {
            dispatch_v1(f);
            done += 1;
            if done >= iters {
                break;
            }
        }
    }
    let elapsed = start.elapsed();
    elapsed.as_nanos() as f64 / iters as f64
}

fn bench_v2(iters: usize) -> f64 {
    let payload = vec![0xAAu8; 8];
    let frames: Vec<Vec<u8>> =
        (0..20u8).map(|op| build_v2_frame(op, &payload)).collect();

    for _ in 0..10_000 {
        for f in &frames {
            dispatch_v2(f);
        }
    }

    let start = Instant::now();
    let mut done = 0;
    while done < iters {
        for f in &frames {
            dispatch_v2(f);
            done += 1;
            if done >= iters {
                break;
            }
        }
    }
    let elapsed = start.elapsed();
    elapsed.as_nanos() as f64 / iters as f64
}

fn bench_v3_multiframe(iters: usize, ops_per_frame: usize) -> f64 {
    // Build ONE frame with `ops_per_frame` different ops.
    let payload = vec![0xAAu8; 8];
    let ops_spec: Vec<(u8, &[u8])> =
        (0..ops_per_frame).map(|i| (i as u8, payload.as_slice())).collect();
    let frame = build_v3_multiframe(&ops_spec);

    for _ in 0..10_000 {
        dispatch_v3_multiframe(&frame);
    }

    // Measure per-op cost: each frame contains ops_per_frame ops.
    let frames_needed = iters / ops_per_frame;
    let start = Instant::now();
    for _ in 0..frames_needed {
        dispatch_v3_multiframe(&frame);
    }
    let elapsed = start.elapsed();
    let total_ops = (frames_needed * ops_per_frame) as f64;
    elapsed.as_nanos() as f64 / total_ops
}

fn bench_v3_separate(iters: usize, ops_per_frame: usize) -> f64 {
    // Same ops but each in its OWN v1-style frame (= per-op frames).
    let payload = vec![0xAAu8; 8];
    let frames: Vec<Vec<u8>> = (0..ops_per_frame)
        .map(|i| build_v1_frame(ACTION_CODES_V1[i], &payload))
        .collect();

    for _ in 0..10_000 {
        for f in &frames {
            dispatch_v1(f);
        }
    }

    let frames_needed = iters / ops_per_frame;
    let start = Instant::now();
    for _ in 0..frames_needed {
        for f in &frames {
            dispatch_v1(f);
        }
    }
    let elapsed = start.elapsed();
    let total_ops = (frames_needed * ops_per_frame) as f64;
    elapsed.as_nanos() as f64 / total_ops
}

// ── Part 3 — TCP loopback (real syscalls) ─────────────────────────────────

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

/// Runs a receiver thread that reads frames and dispatches using `dispatch`.
/// The sender thread pushes `n_frames` frames over a loopback TCP socket.
/// Returns the per-op wall-clock cost (includes write + network + dispatch).
fn bench_tcp_loopback<F>(
    frames: &[Vec<u8>],
    n_iters: usize,
    ops_per_frame: usize,
    dispatch: F,
) -> f64
where
    F: Fn(&[u8]) + Send + Copy + 'static,
{
    // Bind loopback listener.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // Receiver thread: accept 1 connection, parse frames, dispatch.
    let receiver = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        // Read stream buffer — accumulate then process frames.
        let mut buf = Vec::with_capacity(1 << 16);
        let mut tmp = [0u8; 64 * 1024];
        loop {
            match sock.read(&mut tmp) {
                Ok(0) => break,           // EOF, sender closed
                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                Err(_) => break,
            }
            // Process all complete frames in buffer.
            loop {
                if buf.len() < ENVELOPE_SIZE {
                    break;
                }
                let env = Envelope::ref_from_bytes(&buf[..ENVELOPE_SIZE]).unwrap();
                let body_len = env.msg_len.get() as usize;
                let total = ENVELOPE_SIZE + body_len;
                if buf.len() < total {
                    break;
                }
                dispatch(&buf[..total]);
                buf.drain(..total);
            }
        }
    });

    let mut sender = TcpStream::connect(addr).unwrap();
    sender.set_nodelay(true).unwrap();

    // Warmup — send a few frames.
    for _ in 0..64 {
        for f in frames {
            sender.write_all(f).unwrap();
        }
    }
    // Let receiver catch up before starting the timer.
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Measure: send n_iters / ops_per_frame "batches", one write per frame.
    // For V1 separate: frames.len() == ops_per_frame, each frame = 1 op.
    // For V3a multi: frames.len() == 1, each frame = ops_per_frame ops.
    let batches = n_iters / ops_per_frame;
    let start = Instant::now();
    for _ in 0..batches {
        for f in frames {
            sender.write_all(f).unwrap();
        }
    }
    // Close the sender to signal EOF; wait receiver to finish.
    drop(sender);
    receiver.join().unwrap();
    let elapsed = start.elapsed();

    let total_ops = (batches * ops_per_frame) as f64;
    elapsed.as_nanos() as f64 / total_ops
}

fn bench_v1_tcp(iters: usize, ops_per_frame: usize) -> f64 {
    let payload = vec![0xAAu8; 8];
    let frames: Vec<Vec<u8>> = (0..ops_per_frame)
        .map(|i| build_v1_frame(ACTION_CODES_V1[i], &payload))
        .collect();
    bench_tcp_loopback(&frames, iters, ops_per_frame, |buf| dispatch_v1(buf))
}

fn bench_v3_tcp_multiframe(iters: usize, ops_per_frame: usize) -> f64 {
    let payload = vec![0xAAu8; 8];
    let ops_spec: Vec<(u8, &[u8])> =
        (0..ops_per_frame).map(|i| (i as u8, payload.as_slice())).collect();
    let frame = build_v3_multiframe(&ops_spec);
    let frames = vec![frame];
    bench_tcp_loopback(&frames, iters, ops_per_frame, |buf| {
        dispatch_v3_multiframe(buf)
    })
}

fn main() {
    let iters = env_usize("BENCH_DISPATCH_ITERS", ITERS);
    let opf = env_usize("BENCH_DISPATCH_OPS_PER_MULTIFRAME", OPS_PER_MULTIFRAME);

    println!();
    println!("========================================================");
    println!("                 Dispatch variants bench");
    println!("========================================================");
    println!(
        "  iters={iters}   multi-frame pack={opf} ops   payload=8 B"
    );
    println!();

    println!("--------------------------------------------------------");
    println!("  Part 1 — raw dispatch cost (per operation)");
    println!("--------------------------------------------------------");

    let t_v1 = bench_v1(iters);
    println!("  V1  per-action (outer jump table, u16)   : {t_v1:>5.2} ns/op");
    let t_v2 = bench_v2(iters);
    println!("  V2  inner byte discriminator (single Ops): {t_v2:>5.2} ns/op");

    println!();
    println!("--------------------------------------------------------");
    println!("  Part 2 — multi-op per frame vs separate frames");
    println!("--------------------------------------------------------");

    let t_multi = bench_v3_multiframe(iters, opf);
    println!(
        "  V3a multi-op frame ({opf} ops packed)       : {t_multi:>5.2} ns/op"
    );
    let t_sep = bench_v3_separate(iters, opf);
    println!(
        "  V3b separate frames (1 op each, {opf} frames) : {t_sep:>5.2} ns/op"
    );

    let speedup = t_sep / t_multi;
    println!();
    println!(
        "  multi/separate speedup: {speedup:.2}x (>1 = multi wins, <1 = separate wins)"
    );

    // ── Part 3 — TCP loopback (real syscalls) ─────────────────────────
    println!();
    println!("--------------------------------------------------------");
    println!("  Part 3 — TCP loopback (write+network+dispatch per op)");
    println!("--------------------------------------------------------");

    // Use fewer iters for network — syscalls dominate, 1M is plenty.
    let net_iters = iters.min(1_000_000);

    let t_v1_tcp = bench_v1_tcp(net_iters, opf);
    println!(
        "  V1 separate frames ({opf} writes per batch) : {t_v1_tcp:>6.1} ns/op"
    );
    let t_v3_tcp = bench_v3_tcp_multiframe(net_iters, opf);
    println!(
        "  V3a multi-op frame  (1 write  per batch)    : {t_v3_tcp:>6.1} ns/op"
    );

    let speedup_tcp = t_v1_tcp / t_v3_tcp;
    println!();
    println!(
        "  multi/separate over TCP: {speedup_tcp:.2}x (>1 = multi wins)"
    );

    println!();

    // Keep SINK alive to defeat DCE.
    black_box(SINK.load(Relaxed));
}
