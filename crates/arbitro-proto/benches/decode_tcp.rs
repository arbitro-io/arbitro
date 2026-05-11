//! decode_tcp — compare server-side PubFrame v2 decode strategies over a real
//! TCP loopback connection, **using tokio** (matching arbitro-server's actual
//! runtime).
//!
//! ## Strategies measured
//!
//! 1. **`naive_two_read`** — textbook async:
//!    - `AsyncReadExt::read_exact(16B)` → parse Header → extract `msg_len`
//!    - `BytesMut::with_capacity(msg_len)` + `unsafe set_len` + `read_exact` + `freeze`
//!    - **One heap allocation per frame**, **two `read_exact().await` per frame**.
//!
//! 2. **`bufreader_inplace`** — `tokio::io::BufReader<TcpStream>`:
//!    - 64 KiB internal buffer, parses Header + body in-place via `buffer()`.
//!    - `consume()` advances cursor. Slow path falls back to `read_exact` for
//!      frames that straddle a buffer boundary.
//!    - **0 allocations** in fast path.
//!
//! 3. **`prealloc_split`** — manual ring buffer (the user's design):
//!    - One `Vec<u8>` per connection, read into the tail, parse via sub-slice,
//!      compact-on-demand. Dispatch by `action` → `<Struct>::ref_from_bytes(slice)`.
//!    - **0 allocations** for the whole connection (after init).
//!    - Most explicit zero-alloc / zero-Arc design. No BufReader borrow contract.
//!
//! ## Run (per .agent/rules/testing.md — WSL only, native FS)
//!
//! ```bash
//! wsl bash -lc "cd /mnt/d/.../arbitro && cargo bench -p arbitro-proto --bench decode_tcp --no-run"
//! wsl bash -lc "
//!   mkdir -p /tmp/arbitro &&
//!   cp -a target/release/deps/decode_tcp-* /tmp/arbitro/ &&
//!   cd /tmp/arbitro &&
//!   timeout 120 ./decode_tcp-* --bench 2>&1 | tee /tmp/bench_decode_tcp.log
//! "
//! ```

use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Builder;

// ---------- workload size ----------------------------------------------------

const N_FRAMES:    usize = 100_000;  // explicit user override of the 1000-cap
const SUBJECT_LEN: usize = 16;
const PAYLOAD_LEN: usize = 256;

const HEADER_SIZE:   usize = 16;
const PUB_BODY_SIZE: usize = 8;
const TOTAL_FRAME_SIZE: usize = HEADER_SIZE + PUB_BODY_SIZE + SUBJECT_LEN + PAYLOAD_LEN;

const ITERATIONS: usize = 20;
const WARMUP:     usize = 3;

// ---------- frame builder (manual LE bytes — independent of struct layout) ---

fn encode_pub_frame(out: &mut Vec<u8>, seq: u64) {
    let body_len = (PUB_BODY_SIZE + SUBJECT_LEN + PAYLOAD_LEN) as u32;

    // Header: action u16 LE | flags u8 | entry_flags u8 | msg_len u32 LE | seq u64 LE
    out.extend_from_slice(&0x0101u16.to_le_bytes());
    out.push(0);
    out.push(0);
    out.extend_from_slice(&body_len.to_le_bytes());
    out.extend_from_slice(&seq.to_le_bytes());

    // PubBody: stream_id u32 LE | subject_len u16 LE | _pad u16 LE
    out.extend_from_slice(&7u32.to_le_bytes());
    out.extend_from_slice(&(SUBJECT_LEN as u16).to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());

    // Tail
    out.extend_from_slice(&[b's'; SUBJECT_LEN]);
    out.extend_from_slice(&[b'p'; PAYLOAD_LEN]);
}

fn build_blob() -> Vec<u8> {
    let mut blob = Vec::with_capacity(N_FRAMES * TOTAL_FRAME_SIZE);
    for seq in 0..N_FRAMES as u64 {
        encode_pub_frame(&mut blob, seq);
    }
    debug_assert_eq!(blob.len(), N_FRAMES * TOTAL_FRAME_SIZE);
    blob
}

// ---------- header field accessors -------------------------------------------

#[inline(always)]
fn parse_msg_len(header: &[u8]) -> usize {
    u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize
}

#[inline(always)]
fn parse_subject_len(body: &[u8]) -> usize {
    u16::from_le_bytes([body[4], body[5]]) as usize
}

#[inline(always)]
fn parse_action(header: &[u8]) -> u16 {
    u16::from_le_bytes([header[0], header[1]])
}

// ---------- strategy 1: tokio naive (two read_exact + per-frame BytesMut) ----

async fn run_naive(mut stream: TcpStream) -> usize {
    let mut header_buf = [0u8; HEADER_SIZE];
    let mut frames: Vec<Bytes> = Vec::with_capacity(N_FRAMES);

    for _ in 0..N_FRAMES {
        stream.read_exact(&mut header_buf).await.expect("read header");
        let msg_len = parse_msg_len(&header_buf);

        let mut body = BytesMut::with_capacity(msg_len);
        // SAFETY: read_exact below writes msg_len bytes before any read.
        unsafe { body.set_len(msg_len); }
        stream.read_exact(&mut body[..]).await.expect("read body");
        let body: Bytes = body.freeze();

        let sl = parse_subject_len(&body);
        black_box(sl);
        frames.push(body);
    }

    black_box(&frames);
    frames.len()
}

// ---------- strategy 2: tokio BufReader, in-place parse ----------------------

async fn run_bufreader(stream: TcpStream) -> usize {
    let mut reader = BufReader::with_capacity(64 * 1024, stream);
    let mut count: usize = 0;
    let mut staging: Vec<u8> = Vec::with_capacity(TOTAL_FRAME_SIZE);

    while count < N_FRAMES {
        // Fast path: drain whole frames already buffered.
        loop {
            let buf = reader.buffer();
            if buf.len() < HEADER_SIZE { break; }
            let msg_len = parse_msg_len(&buf[..HEADER_SIZE]);
            let total   = HEADER_SIZE + msg_len;
            if buf.len() < total { break; }

            let body = &buf[HEADER_SIZE..total];
            let sl = parse_subject_len(body);
            black_box(sl);
            black_box(body);

            reader.consume(total);
            count += 1;
            if count >= N_FRAMES { return count; }
        }

        // Slow path: not enough buffered for the next frame — refill.
        // `fill_buf` only reads if the buffer is empty; for a partial buffer
        // we fall through to `read_exact` which uses leftovers + reads more.
        if reader.buffer().is_empty() {
            let buf = reader.fill_buf().await.expect("fill_buf");
            if buf.is_empty() { break; }
        } else {
            // Frame straddles. One read into reusable staging vec.
            let mut header_buf = [0u8; HEADER_SIZE];
            if reader.read_exact(&mut header_buf).await.is_err() { break; }
            let msg_len = parse_msg_len(&header_buf);
            if staging.capacity() < msg_len {
                staging.reserve(msg_len - staging.capacity());
            }
            // SAFETY: read_exact below writes msg_len bytes.
            unsafe { staging.set_len(msg_len); }
            reader.read_exact(&mut staging[..msg_len]).await.expect("read body");
            let sl = parse_subject_len(&staging[..msg_len]);
            black_box(sl);
            black_box(&staging[..msg_len]);
            count += 1;
        }
    }
    count
}

// ---------- strategy 3: pre-allocated Vec ring + sub-slice + cast-by-action --

async fn run_prealloc_split(mut stream: TcpStream) -> usize {
    let mut buf: Vec<u8> = vec![0u8; 4 * 1024];
    let mut filled:   usize = 0;
    let mut consumed: usize = 0;
    let mut count:    usize = 0;

    while count < N_FRAMES {
        // Fast path: drain whole frames out of the live region.
        loop {
            let avail = filled - consumed;
            if avail < HEADER_SIZE { break; }

            let header  = &buf[consumed..consumed + HEADER_SIZE];
            let action  = parse_action(header);
            let msg_len = parse_msg_len(header);
            let total   = HEADER_SIZE + msg_len;

            if avail < total { break; }

            let frame_slice = &buf[consumed..consumed + total];

            // Dispatch by action — equivalent to `<Struct>::ref_from_bytes(slice)`.
            match action {
                0x0101 /* Publish */ => {
                    let body = &frame_slice[HEADER_SIZE..];
                    let sl = parse_subject_len(body);
                    black_box(sl);
                    black_box(frame_slice);
                }
                _ => unreachable!("only Publish in this bench"),
            }

            consumed += total;
            count += 1;
            if count >= N_FRAMES { return count; }
        }

        // Compact partial frame to the front.
        if consumed > 0 {
            let live = filled - consumed;
            if live > 0 {
                buf.copy_within(consumed..filled, 0);
            }
            filled = live;
            consumed = 0;
        }

        // Grow if a single frame exceeds buffer.
        if filled == buf.len() {
            buf.resize(buf.len() * 2, 0);
        }

        // One async read into the free tail.
        let n = stream.read(&mut buf[filled..]).await.expect("read");
        if n == 0 { break; }
        filled += n;
    }

    count
}

// ---------- strategy 4: BytesMut accumulator + read_buf + O(1) split_to -----
//
// Idea: one `BytesMut` accumulator per connection. `read_buf` writes into the
// spare capacity (and the BytesMut grows automatically when we `reserve`).
// Each complete frame is peeled off with `split_to(total)`, which is **O(1)**:
// just an Arc bump and a few index updates — the underlying allocation is
// shared.
//
// vs `prealloc_split`:
//   - Same conceptual flow (accumulate → drain → repeat).
//   - `BytesMut` handles compaction/growth internally. No manual `copy_within`.
//   - Each frame becomes an owned `BytesMut` (tiny stack header + shared Arc),
//     which is convenient for downstream APIs that want a `Bytes`/`BytesMut`
//     instead of a `&[u8]`.
//
// vs `naive_two_read`:
//   - Single shared allocation that grows as needed (vs N allocations).
//   - No `set_len` unsafe — `read_buf` writes into spare capacity safely.
//   - Same `read_exact` pattern conceptually, but amortized over many frames.

async fn run_read_buf_split(mut stream: TcpStream) -> usize {
    // Initial capacity sized for a few frames; grows automatically if a single
    // huge frame arrives (capped in production with a hard MAX_MSG_LEN check).
    let mut acc = BytesMut::with_capacity(64 * 1024);
    let mut count: usize = 0;

    while count < N_FRAMES {
        // Fast path: drain whole frames out of the accumulator.
        loop {
            if acc.len() < HEADER_SIZE { break; }
            let msg_len = parse_msg_len(&acc[..HEADER_SIZE]);
            let total = HEADER_SIZE + msg_len;
            if acc.len() < total { break; }

            // O(1): bumps Arc, sets indices. No memcpy, no heap alloc.
            // `frame` owns bytes [0..total]; `acc` keeps [total..].
            let frame: BytesMut = acc.split_to(total);

            // Equivalent to `<Frame>::ref_from_bytes(&frame[..])` + accessor:
            let body = &frame[HEADER_SIZE..];
            let sl = parse_subject_len(body);
            black_box(sl);
            black_box(&frame);

            count += 1;
            if count >= N_FRAMES { return count; }
        }

        // Need more bytes — ensure spare capacity, then read_buf.
        // `reserve` will compact and/or grow the underlying allocation as
        // needed. Spare capacity = capacity - len.
        if acc.capacity() - acc.len() < TOTAL_FRAME_SIZE {
            acc.reserve(TOTAL_FRAME_SIZE);
        }
        let n = stream.read_buf(&mut acc).await.expect("read_buf");
        if n == 0 { break; } // EOF
    }
    count
}

// ---------- harness (one tokio runtime, reused across all measurements) ------

async fn run_one_iter<F, Fut>(blob: Vec<u8>, strategy: F) -> Duration
where
    F: FnOnce(TcpStream) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = usize> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().unwrap();

    // Producer task.
    let producer = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        sock.set_nodelay(true).ok();
        sock.write_all(&blob).await.expect("write_all");
        sock.shutdown().await.ok();
    });

    let stream = TcpStream::connect(addr).await.expect("connect");
    stream.set_nodelay(true).ok();

    let start = Instant::now();
    let n = strategy(stream).await;
    let elapsed = start.elapsed();
    assert_eq!(n, N_FRAMES, "strategy didn't drain all frames");

    producer.await.ok();
    elapsed
}

fn measure<F, Fut>(rt: &tokio::runtime::Runtime, name: &str, blob: &[u8], strategy: F)
where
    F: Fn(TcpStream) -> Fut + Copy + Send + 'static,
    Fut: std::future::Future<Output = usize> + Send + 'static,
{
    for _ in 0..WARMUP {
        let blob_owned = blob.to_vec();
        rt.block_on(run_one_iter(blob_owned, strategy));
    }

    let mut samples = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let blob_owned = blob.to_vec();
        let d = rt.block_on(run_one_iter(blob_owned, strategy));
        samples.push(d);
    }
    samples.sort();

    let min     = samples[0];
    let median  = samples[ITERATIONS / 2];
    let p99_idx = ((ITERATIONS as f64) * 0.99) as usize;
    let p99     = samples[p99_idx.min(ITERATIONS - 1)];

    let bytes_total      = (N_FRAMES * TOTAL_FRAME_SIZE) as f64;
    let throughput_mbs   = bytes_total / median.as_secs_f64() / 1e6;
    let frames_per_sec_m = (N_FRAMES as f64) / median.as_secs_f64() / 1e6;
    let per_frame_ns     = median.as_nanos() as f64 / N_FRAMES as f64;

    println!(
        "{:<22} min={:>9.2}ms  median={:>9.2}ms  p99={:>9.2}ms   {:>7.1}ns/frame  {:>5.2} M msg/s  {:>5.0} MB/s",
        name,
        min.as_secs_f64()    * 1e3,
        median.as_secs_f64() * 1e3,
        p99.as_secs_f64()    * 1e3,
        per_frame_ns,
        frames_per_sec_m,
        throughput_mbs,
    );
}

fn main() {
    let _bench = std::env::args().any(|a| a == "--bench");

    println!();
    println!("decode_tcp (TOKIO) — PubFrame v2 read strategies over loopback TCP");
    println!("  N_FRAMES        = {}", N_FRAMES);
    println!("  SUBJECT_LEN     = {}", SUBJECT_LEN);
    println!("  PAYLOAD_LEN     = {}", PAYLOAD_LEN);
    println!("  frame size      = {} B", TOTAL_FRAME_SIZE);
    println!("  total blob      = {} KiB", N_FRAMES * TOTAL_FRAME_SIZE / 1024);
    println!("  iterations      = {} (+{} warmup)", ITERATIONS, WARMUP);
    println!("  runtime         = tokio multi_thread (2 workers)");
    println!();

    // Multi-thread runtime so producer + consumer run in parallel,
    // matching how arbitro-server actually spawns per-connection tasks.
    let rt = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build runtime");

    let blob = build_blob();

    measure(&rt, "naive_two_read",    &blob, |s| run_naive(s));
    measure(&rt, "bufreader_inplace", &blob, |s| run_bufreader(s));
    measure(&rt, "prealloc_split",    &blob, |s| run_prealloc_split(s));
    measure(&rt, "read_buf_split_to", &blob, |s| run_read_buf_split(s));

    println!();
    println!("Notes:");
    println!("  * `naive_two_read`   : 2 read_exact().await + 1 BytesMut alloc per frame.");
    println!("  * `bufreader_inplace`: tokio::io::BufReader 64 KiB, in-place parse,");
    println!("                          0 allocs in fast path. Slow path = staging vec.");
    println!("  * `prealloc_split`   : 1 Vec<u8> for the whole conn, sub-slice + cast,");
    println!("                          compact-on-demand. Dispatches by action.");
    println!("  * `read_buf_split_to`: 1 BytesMut accumulator, read_buf grows it on demand,");
    println!("                          split_to(total) is O(1) per frame (Arc-shared).");
}
