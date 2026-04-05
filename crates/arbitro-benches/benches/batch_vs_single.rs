//! TCP benchmark: batch-as-standard vs single-frame.
//!
//! Measures full TCP roundtrip for 1000 messages across 5 strategies:
//! write×N, writev×1, batch(1)×N, batch(N)×1, uring batch(N).
//!
//! Wire types defined from scratch — no external protocol deps.

use std::hint::black_box;
use std::io::{IoSlice, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use bytes::BufMut;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

const N: usize = 2_730; // ~64KB per batch frame
const SUBJECT: &[u8] = b"orders.created";
const PAYLOAD: &[u8] = b"{}";

// ── Wire types (defined from scratch) ────────────────────────────────────────

/// Current protocol: 32B fixed header.
#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct Header32 {
    magic: U32,
    version: u8,
    flags: u8,
    action: U16,
    crc32c: U32,
    length: U32,
    sequence: U64,
    timestamp: U64,
}

const H32: usize = std::mem::size_of::<Header32>();
const _: () = assert!(H32 == 32);

const MAGIC: u32 = 0xA1B2_C3D4;
const VER: u8 = 0x02;
const ACT_PUBLISH: u16 = 0x0101;
const ACT_REPOK: u16 = 0x0203;

/// Proposed protocol: 16B envelope.
/// [2 action][1 flags][1 rsv][4 stream_id][4 msg_len][4 env_seq]
const ENV: usize = 16;

/// Proposed batch entry: 8B per message.
/// [4 data_len][2 subj_len][1 flags][1 pad]
const ENTRY: usize = 8;

// ── Frame builders ───────────────────────────────────────────────────────────

fn build_single(seq: u64) -> Vec<u8> {
    let plen = 2 + SUBJECT.len() + PAYLOAD.len();
    let hdr = Header32 {
        magic: MAGIC.into(), version: VER, flags: 0x01, // NO_ACK
        action: ACT_PUBLISH.into(),
        crc32c: 0u32.into(), length: (plen as u32).into(),
        sequence: seq.into(), timestamp: 0u64.into(),
    };
    let mut buf = Vec::with_capacity(H32 + plen);
    buf.extend_from_slice(hdr.as_bytes());
    buf.extend_from_slice(&(SUBJECT.len() as u16).to_le_bytes());
    buf.extend_from_slice(SUBJECT);
    buf.extend_from_slice(PAYLOAD);
    buf
}

fn build_batch(count: u16) -> Vec<u8> {
    let entry_sz = ENTRY + SUBJECT.len() + PAYLOAD.len();
    let msg_len = 2 + entry_sz * count as usize;
    let mut buf = Vec::with_capacity(ENV + msg_len);
    // Envelope
    buf.put_u16_le(ACT_PUBLISH);
    buf.put_u8(0); buf.put_u8(0);
    buf.put_u32_le(0);
    buf.put_u32_le(msg_len as u32);
    buf.put_u32_le(0);
    // Count
    buf.put_u16_le(count);
    // Entries
    for _ in 0..count {
        buf.put_u32_le(PAYLOAD.len() as u32);
        buf.put_u16_le(SUBJECT.len() as u16);
        buf.put_u8(0); buf.put_u8(0);
        buf.extend_from_slice(SUBJECT);
        buf.extend_from_slice(PAYLOAD);
    }
    buf
}

fn repok_32() -> [u8; H32] {
    let hdr = Header32 {
        magic: MAGIC.into(), version: VER, flags: 0,
        action: ACT_REPOK.into(),
        crc32c: 0u32.into(), length: 0u32.into(),
        sequence: 0u64.into(), timestamp: 0u64.into(),
    };
    let mut out = [0u8; H32];
    out.copy_from_slice(hdr.as_bytes());
    out
}

fn repok_16() -> [u8; ENV] {
    let mut out = [0u8; ENV];
    out[0..2].copy_from_slice(&ACT_REPOK.to_le_bytes());
    out
}

// ── Server ───────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum Mode { Current, Proposed }

fn spawn_server(mode: Mode) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            thread::spawn(move || serve(stream, mode));
        }
    });
    port
}

fn serve(mut s: TcpStream, mode: Mode) {
    s.set_nodelay(true).unwrap();
    let rok32 = repok_32();
    let rok16 = repok_16();
    let mut hdr = [0u8; H32];
    let mut payload = vec![0u8; 64 * 1024];
    loop {
        match mode {
            Mode::Current => {
                if s.read_exact(&mut hdr).is_err() { return }
                let len = u32::from_le_bytes(hdr[12..16].try_into().unwrap()) as usize;
                if len > 0 && s.read_exact(&mut payload[..len]).is_err() { return }
                if s.write_all(&rok32).is_err() { return }
            }
            Mode::Proposed => {
                if s.read_exact(&mut hdr[..ENV]).is_err() { return }
                let len = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
                if len > 0 && s.read_exact(&mut payload[..len]).is_err() { return }
                if s.write_all(&rok16).is_err() { return }
            }
        }
    }
}

fn connect(port: u16) -> TcpStream {
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_nodelay(true).unwrap();
    s
}

/// writev that handles partial writes by advancing through the iovec list.
fn writev_all(s: &mut TcpStream, bufs: &[Vec<u8>]) {
    let mut written = 0usize;
    let total: usize = bufs.iter().map(|b| b.len()).sum();
    while written < total {
        // Build IoSlice for remaining data
        let mut skip = written;
        let slices: Vec<IoSlice<'_>> = bufs.iter().filter_map(|b| {
            if skip >= b.len() {
                skip -= b.len();
                None
            } else {
                let slice = IoSlice::new(&b[skip..]);
                skip = 0;
                Some(slice)
            }
        }).collect();
        let n = s.write_vectored(&slices).unwrap();
        assert!(n > 0, "write_vectored returned 0");
        written += n;
    }
}

fn cfg(g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    g.throughput(Throughput::Elements(N as u64));
    g.sample_size(10);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(3));
}

// ── 01: Current protocol (32B header) ────────────────────────────────────────

fn bench_current(c: &mut Criterion) {
    let port = spawn_server(Mode::Current);
    let frames: Vec<Vec<u8>> = (0..N).map(|i| build_single(i as u64)).collect();

    let tx: usize = frames.iter().map(|f| f.len()).sum();
    eprintln!("current: tx={}B rx={}B total={}B", tx, H32 * N, tx + H32 * N);

    let mut g = c.benchmark_group("01_current");
    cfg(&mut g);

    // 1: write() × N
    g.bench_function("write_x_N", |b| {
        let mut s = connect(port);
        let mut rbuf = vec![0u8; H32 * N];
        b.iter(|| {
            for f in &frames { s.write_all(f).unwrap(); }
            s.read_exact(&mut rbuf).unwrap();
            black_box(&rbuf);
        });
    });

    // 2: writev() — scatter-gather (handles partial writes)
    g.bench_function("writev_x_1", |b| {
        let mut s = connect(port);
        let mut rbuf = vec![0u8; H32 * N];
        b.iter(|| {
            writev_all(&mut s, &frames);
            s.read_exact(&mut rbuf).unwrap();
            black_box(&rbuf);
        });
    });

    g.finish();
}

// ── 02: Proposed protocol (16B envelope, batch) ──────────────────────────────

fn bench_proposed(c: &mut Criterion) {
    let port = spawn_server(Mode::Proposed);
    let batch_n = build_batch(N as u16);
    let batch1s: Vec<Vec<u8>> = (0..N).map(|_| build_batch(1)).collect();

    eprintln!("proposed batch({}): tx={}B rx={}B", N, batch_n.len(), ENV);
    let tx1: usize = batch1s.iter().map(|f| f.len()).sum();
    eprintln!("proposed batch(1)x{}: tx={}B rx={}B", N, tx1, ENV * N);

    let mut g = c.benchmark_group("02_proposed");
    cfg(&mut g);

    // 3: batch(1) write × N
    g.bench_function("batch1_write_x_N", |b| {
        let mut s = connect(port);
        let mut rbuf = vec![0u8; ENV * N];
        b.iter(|| {
            for f in &batch1s { s.write_all(f).unwrap(); }
            s.read_exact(&mut rbuf).unwrap();
            black_box(&rbuf);
        });
    });

    // 4: batch(N) write × 1
    g.bench_function("batch_N_x_1", |b| {
        let mut s = connect(port);
        let mut rbuf = [0u8; ENV];
        b.iter(|| {
            s.write_all(&batch_n).unwrap();
            s.read_exact(&mut rbuf).unwrap();
            black_box(&rbuf);
        });
    });

    // batch(1) writev — scatter-gather (handles partial writes)
    g.bench_function("batch1_writev_x_1", |b| {
        let mut s = connect(port);
        let mut rbuf = vec![0u8; ENV * N];
        b.iter(|| {
            writev_all(&mut s, &batch1s);
            s.read_exact(&mut rbuf).unwrap();
            black_box(&rbuf);
        });
    });

    g.finish();
}

// ── 03: tokio-uring ──────────────────────────────────────────────────────────

async fn uring_read_exact(stream: &tokio_uring::net::TcpStream, n: usize) -> Vec<u8> {
    let mut result = Vec::with_capacity(n);
    while result.len() < n {
        let buf = vec![0u8; n - result.len()];
        let (res, buf) = stream.read(buf).await;
        let read = res.unwrap();
        assert!(read > 0, "unexpected EOF at {}/{}", result.len(), n);
        result.extend_from_slice(&buf[..read]);
    }
    result
}

fn bench_uring(c: &mut Criterion) {
    let port_c = spawn_server(Mode::Current);
    let port_p = spawn_server(Mode::Proposed);

    let frames: Vec<Vec<u8>> = (0..N).map(|i| build_single(i as u64)).collect();
    let mut concat = Vec::with_capacity(frames.iter().map(|f| f.len()).sum());
    for f in &frames { concat.extend_from_slice(f); }
    let batch_n = build_batch(N as u16);

    let mut g = c.benchmark_group("03_uring");
    cfg(&mut g);

    // 5a: uring — current concat write
    g.bench_function("current_concat", |b| {
        let data = concat.clone();
        b.iter(|| {
            tokio_uring::start(async {
                let s = tokio_uring::net::TcpStream::connect(
                    format!("127.0.0.1:{port_c}").parse().unwrap()
                ).await.unwrap();
                s.set_nodelay(true).unwrap();
                let (res, _) = s.write_all(data.clone()).await;
                res.unwrap();
                black_box(uring_read_exact(&s, H32 * N).await);
            });
        });
    });

    // 5b: uring — proposed batch(N)
    g.bench_function("proposed_batch_N", |b| {
        let frame = batch_n.clone();
        b.iter(|| {
            tokio_uring::start(async {
                let s = tokio_uring::net::TcpStream::connect(
                    format!("127.0.0.1:{port_p}").parse().unwrap()
                ).await.unwrap();
                s.set_nodelay(true).unwrap();
                let (res, _) = s.write_all(frame.clone()).await;
                res.unwrap();
                black_box(uring_read_exact(&s, ENV).await);
            });
        });
    });

    // 5c: uring — persistent connection batch(N), manual timing
    g.bench_function("batch_N_persistent", |b| {
        let frame = batch_n.clone();
        tokio_uring::start(async {
            let s = tokio_uring::net::TcpStream::connect(
                format!("127.0.0.1:{port_p}").parse().unwrap()
            ).await.unwrap();
            s.set_nodelay(true).unwrap();
            let iters = 200u64;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                let (res, _) = s.write_all(frame.clone()).await;
                res.unwrap();
                uring_read_exact(&s, ENV).await;
            }
            let elapsed = start.elapsed();
            eprintln!(
                "uring persistent batch({}): {}us/iter, {:.1}M msg/s",
                N, (elapsed / iters as u32).as_micros(),
                (N as f64 * iters as f64) / elapsed.as_secs_f64() / 1_000_000.0,
            );
        });
        b.iter(|| black_box(0));
    });

    g.finish();
}

// ── Wire stats ───────────────────────────────────────────────────────────────

fn bench_wire_stats(c: &mut Criterion) {
    let single = H32 + 2 + SUBJECT.len() + PAYLOAD.len();
    let batch1 = ENV + 2 + ENTRY + SUBJECT.len() + PAYLOAD.len();
    let batchn = ENV + 2 + N * (ENTRY + SUBJECT.len() + PAYLOAD.len());

    eprintln!("\n=== Wire bytes for {} messages (tx) ===", N);
    eprintln!("current  single x{N}: {}B ({}/msg)", single * N, single);
    eprintln!("proposed batch1 x{N}: {}B ({}/msg)", batch1 * N, batch1);
    eprintln!("proposed batchN x1:   {}B ({}/msg)", batchn, batchn / N);
    eprintln!("batch1 vs single: {:.0}% savings", (1.0 - batch1 as f64 / single as f64) * 100.0);
    eprintln!("batchN vs single: {:.0}% savings", (1.0 - batchn as f64 / (single * N) as f64) * 100.0);
    eprintln!("===\n");

    let mut g = c.benchmark_group("00_wire_stats");
    g.sample_size(10);
    g.bench_function("noop", |b| b.iter(|| black_box(0)));
    g.finish();
}

criterion_group!(benches, bench_wire_stats, bench_current, bench_proposed, bench_uring);
criterion_main!(benches);
