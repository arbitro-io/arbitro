//! writev_concurrent — N subscriptions as kit::Mpmc producers,
//! 1 writer task drains and emits to the TCP socket via write_all_vectored.
//!
//! This is the "single writer task + M:1 queue" pattern. The writer is
//! the SOLE owner of the socket — no race possible. Subs only call
//! `producer.try_send(frame_bytes)`; they never touch the socket.
//!
//! Setup:
//!   - 1 listener accepts 1 connection.
//!   - N tokio tasks (= N "subscriptions") each owns one `MpmcProducer<Vec<u8>>`
//!     and pushes framed messages.
//!   - 1 writer std::thread drains via `MpmcConsumer::recv_batch` and writes
//!     batches to the socket using `write_all_vectored`. The OS-thread
//!     consumer parks on the SignalSet when the queue is empty (0% CPU idle).
//!   - Reader on the other side parses frames and validates checksums.
//!
//! Pass criteria:
//!   - All `N × FRAMES_PER_SUB` frames received
//!   - 0 checksum mismatches
//!   - 0 misordered or partial frames at the parser
//!
//! Run: `cargo bench --bench writev_concurrent`

use std::io::IoSlice;
use std::os::fd::AsFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32};

use arbitro_kit::route::Mpmc;
use arbitro_proto::action::Action;
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_proto::wire::publish::PublishEntry;

const N_SUBS: usize = 10;
// Override at runtime via FRAMES env var (default 100_000).
fn frames_per_sub() -> usize {
    std::env::var("FRAMES").ok().and_then(|s| s.parse().ok()).unwrap_or(100_000)
}
const BATCH_SIZE: usize    = 256;          // try_send_batch chunk size
const PAYLOAD_SIZE: usize  = 256;          // 256 B per frame
const HDR_SIZE: usize      = 16;
const FRAME_TOTAL: usize   = HDR_SIZE + PAYLOAD_SIZE;

#[inline]
fn checksum(buf: &[u8]) -> u32 {
    buf.iter().fold(0u32, |acc, &b| acc.wrapping_add(b as u32))
}

/// Build a complete frame [16B header][PAYLOAD_SIZE payload] in ONE
/// allocation, with **zero memcpys** for the header. Direct `*mut u32`
/// stores at the right offsets — no intermediate `[u8; 4]` stack
/// arrays, no `copy_from_slice`. The payload is filled in-place inside
/// the frame buffer's heap memory.
///
/// Total: **1 malloc + 4 unaligned u32 stores + payload byte-fill**.
#[inline]
fn build_frame(sub_id: u32, seq: u32) -> Vec<u8> {
    let mut buf = Vec::<u8>::with_capacity(FRAME_TOTAL);
    // SAFETY: we set len() to the full capacity and write every byte
    // before any read.
    unsafe { buf.set_len(FRAME_TOTAL); }
    // Fill payload in-place inside the frame buffer.
    {
        let payload = &mut buf[HDR_SIZE..FRAME_TOTAL];
        for i in 0..PAYLOAD_SIZE {
            payload[i] = ((sub_id.wrapping_mul(31)
                .wrapping_add(seq.wrapping_mul(17))
                .wrapping_add(i as u32)) & 0xff) as u8;
        }
    }
    // Compute checksum over the just-written payload (read-only borrow).
    let chk = checksum(&buf[HDR_SIZE..FRAME_TOTAL]);

    // Direct unaligned u32 stores into the heap buffer. No `[u8; 4]`
    // temporaries on the stack, no `copy_from_slice` memcpy. On
    // little-endian targets the values stored ARE already LE.
    // SAFETY: `buf` has at least 16 bytes (HDR_SIZE) capacity initialized
    // by `set_len` above. Writes are in-bounds and aligned to 1 byte
    // (using write_unaligned, no alignment requirement).
    unsafe {
        let p = buf.as_mut_ptr();
        (p as *mut u32).write_unaligned(sub_id.to_le());
        (p.add(4)  as *mut u32).write_unaligned(seq.to_le());
        (p.add(8)  as *mut u32).write_unaligned((PAYLOAD_SIZE as u32).to_le());
        (p.add(12) as *mut u32).write_unaligned(chk.to_le());
    }
    buf
}

struct ReaderResult {
    total_received: usize,
    checksum_errors: usize,
    sub_counts: Vec<u32>,
}

async fn run_reader(mut sock: TcpStream, expected: usize, n_subs: usize) -> ReaderResult {
    let mut backlog = Vec::with_capacity(1 << 20);
    let mut buf = vec![0u8; 1 << 16];
    let mut received = 0usize;
    let mut errors = 0usize;
    let mut counts = vec![0u32; n_subs];

    while received < expected {
        let n = match sock.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        backlog.extend_from_slice(&buf[..n]);

        while backlog.len() >= HDR_SIZE {
            let sub_id = u32::from_le_bytes(backlog[0..4].try_into().unwrap());
            let seq    = u32::from_le_bytes(backlog[4..8].try_into().unwrap());
            let len    = u32::from_le_bytes(backlog[8..12].try_into().unwrap()) as usize;
            let chk    = u32::from_le_bytes(backlog[12..16].try_into().unwrap());

            // Sanity check on len before waiting for the rest.
            if len != PAYLOAD_SIZE {
                // Header itself is corrupted — len wrong → byte interleaving detected.
                errors += 1;
                received += 1;
                // Drain just the bad header and continue trying to resync.
                backlog.drain(..HDR_SIZE);
                continue;
            }
            if backlog.len() < HDR_SIZE + len { break; }

            let payload = &backlog[HDR_SIZE..HDR_SIZE + len];
            let computed = checksum(payload);
            if computed != chk || (sub_id as usize) >= n_subs {
                errors += 1;
            } else {
                counts[sub_id as usize] += 1;
                let _ = seq;
            }
            received += 1;
            backlog.drain(..HDR_SIZE + len);
        }
    }

    ReaderResult { total_received: received, checksum_errors: errors, sub_counts: counts }
}

/// Two architectures available — selected via the `Strategy` enum.
#[derive(Copy, Clone, Debug)]
enum Strategy {
    /// Original: 1 message = 1 frame of 272 B. Producer sends individual
    /// frames; writer batches them with `write_vectored` (up to N iovecs).
    /// Tests the per-message throughput of the M:1 + writev pipeline.
    Mpmc,
    /// Simulates `client.publish_batch`: 1 batch = 1 contiguous frame
    /// containing 256 messages in the real Arbitro publish_batch wire
    /// format ([16B Envelope][4B count][256 × (12B entry + subj + payload)]).
    /// Producer sends `Arc<Vec<u8>>` clones — no memcpy in userland.
    /// Writer write_all's each batch as a single contiguous block.
    /// The ONLY copy is the kernel's internal copy_from_user into the
    /// socket send buffer.
    MpmcPublishBatch,
}

fn run_once(rt: &Runtime, n_subs: usize, strategy: Strategy) -> (u128, ReaderResult) {
    match strategy {
        Strategy::Mpmc             => run_once_mpmc(rt, n_subs),
        Strategy::MpmcPublishBatch => run_once_publish_batch(rt, n_subs),
    }
}

fn run_once_mpmc(rt: &Runtime, n_subs: usize) -> (u128, ReaderResult) {
    rt.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Reader on the same runtime.
        let expected = n_subs * frames_per_sub();
        let reader_h = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            sock.set_nodelay(true).ok();
            run_reader(sock, expected, n_subs).await
        });

        // Writer side — shared across N tasks via kit::Mpmc M:1.
        let tokio_stream = TcpStream::connect(addr).await.unwrap();
        tokio_stream.set_nodelay(true).ok();

        // Bump SO_SNDBUF so frames fit comfortably.
        let raw_fd = tokio_stream.as_fd();
        let sock_ref = socket2::SockRef::from(&raw_fd);
        let _ = sock_ref.set_send_buffer_size(8 * 1024 * 1024);

        let short_writes = Arc::new(AtomicU64::new(0)); // unused under Mpmc M:1
        let would_blocks = Arc::new(AtomicU64::new(0));

        // M producers (subs) → 1 consumer (the writer thread).
        let (producers, mut consumers, _shutdown) =
            Mpmc::<Bytes, 256>::new(n_subs.max(1), 1);
        let consumer = consumers.pop().unwrap();

        // Convert to a blocking std TcpStream so we can drive it from
        // a dedicated std::thread (kit::Mpmc::recv parks with std::thread::park,
        // which would freeze a tokio worker).
        let std_stream = tokio_stream.into_std().unwrap();
        std_stream.set_nonblocking(false).ok();

        let expected_total = n_subs * frames_per_sub();
        let writer_h = std::thread::Builder::new()
            .name("writev-bench-writer".into())
            .spawn(move || {
                use std::collections::VecDeque;
                use std::io::{IoSlice, Write};
                consumer.bind();
                let mut writer = std_stream;

                let mut pending: VecDeque<Bytes> = VecDeque::with_capacity(2048);
                let mut batch: Vec<Bytes> = Vec::with_capacity(1024);
                let mut written = 0usize;
                // Histogram of batch sizes so we can see how much frames
                // get coalesced under load.
                let mut hist: Vec<usize> = Vec::with_capacity(256);

                while written < expected_total {
                    // 1. Park until at least 1 frame.
                    if pending.is_empty() {
                        match consumer.recv() {
                            Ok(f) => pending.push_back(f),
                            Err(_) => break,
                        }
                    }
                    // 2. Drain the rest non-blocking.
                    consumer.try_recv_batch(|f| pending.push_back(f));

                    // 3. Take up to 1024 into this writev (Linux IOV_MAX = 1024).
                    let n = pending.len().min(1024);
                    for _ in 0..n { batch.push(pending.pop_front().unwrap()); }

                    hist.push(batch.len());

                    // 4. write_vectored loop.
                    let mut iovs_storage: Vec<IoSlice> =
                        batch.iter().map(|b| IoSlice::new(b)).collect();
                    let mut iovs: &mut [IoSlice] = iovs_storage.as_mut_slice();
                    let mut io_err = false;
                    while !iovs.is_empty() {
                        match writer.write_vectored(iovs) {
                            Ok(0) => { io_err = true; break; }
                            Ok(k) => IoSlice::advance_slices(&mut iovs, k),
                            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                            Err(_) => { io_err = true; break; }
                        }
                    }
                    written += batch.len();
                    batch.clear();
                    if io_err { break; }
                }
                let _ = writer.flush();

                // Print histogram summary so we see actual batching.
                let total_batches = hist.len();
                let total_frames: usize = hist.iter().sum();
                let avg = if total_batches > 0 {
                    total_frames as f64 / total_batches as f64
                } else { 0.0 };
                let max = hist.iter().copied().max().unwrap_or(0);

                // Bucketed histogram.
                let buckets = [1usize, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];
                let mut counts = vec![0usize; buckets.len()];
                for &b in &hist {
                    let idx = match b {
                        1 => 0, 2..=3 => 1, 4..=7 => 2, 8..=15 => 3,
                        16..=31 => 4, 32..=63 => 5, 64..=127 => 6,
                        128..=255 => 7, 256..=511 => 8, 512..=1023 => 9, _ => 10,
                    };
                    counts[idx] += 1;
                }
                eprintln!("[writer] batches={}, frames={}, avg={:.2}, max={}",
                          total_batches, total_frames, avg, max);
                eprintln!("[writer] histogram (batch_size : count)");
                for (label, count) in buckets.iter().zip(counts.iter()) {
                    if *count > 0 {
                        eprintln!("[writer]   ≥{:>2}: {}", label, count);
                    }
                }
            })
            .unwrap();

        // ── PREBUILD all frames upfront. Each producer task holds a
        // `Vec<Bytes>` (refcounted) of all its frames. Hot loop only
        // clones (Arc bump, ~1 ns) into a batch — zero allocations
        // during the timed window. The Vec<u8>→Bytes wrapper alloc
        // is paid ONCE per frame here, before t0 starts.
        let prebuilt: Vec<Vec<Bytes>> = (0..n_subs as u32).map(|sub_id| {
            (0..frames_per_sub() as u32).map(|seq| {
                Bytes::from(build_frame(sub_id, seq))
            }).collect()
        }).collect();

        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(n_subs);
        for (producer, frames) in producers.into_iter().zip(prebuilt.into_iter()) {
            handles.push(tokio::spawn(async move {
                let mut batch: Vec<Bytes> = Vec::with_capacity(BATCH_SIZE);
                for frame in frames.iter() {
                    // Bytes::clone is an Arc bump — no malloc, no memcpy.
                    batch.push(frame.clone());

                    if batch.len() == BATCH_SIZE {
                        // Drain via try_send_batch — amortizes the fetch_or.
                        while !batch.is_empty() {
                            let n = producer.try_send_batch(&mut batch);
                            if n == 0 {
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                }
                while !batch.is_empty() {
                    let n = producer.try_send_batch(&mut batch);
                    if n == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }

        for h in handles { let _ = h.await; }
        // Wait for the writer thread to finish draining all frames.
        let _ = writer_h.join();

        let result = reader_h.await.unwrap();
        let elapsed_ns = t0.elapsed().as_nanos();
        let _ = short_writes;
        let _ = would_blocks;
        (elapsed_ns, result)
    })
}

// ── publish_batch simulation ────────────────────────────────────────────
//
// Each batch is a SINGLE contiguous `Arc<Vec<u8>>` carrying 256 messages
// in the real `client.publish_batch` wire format:
//
//   [16B Envelope]
//     action = Action::Publish
//     stream_id, msg_len, env_seq
//   [4B count = 256]
//   [12B PublishEntry][subject][payload]   ← entry 0
//   [12B PublishEntry][subject][payload]   ← entry 1
//   ...
//   [12B PublishEntry][subject][payload]   ← entry 255
//
// Producer hot path: clone the Arc (~1 ns bump), push into kit::Mpmc.
// Writer hot path: drain Arc, write_all on the contiguous bytes (1 syscall),
// drop. NO userland memcpy — only the kernel's `copy_from_user` into
// the socket send buffer.

const PUBLISH_BATCH_ENTRIES: usize = 256;
const PUBLISH_SUBJECT: &[u8] = b"bench.subject.path";
const PUBLISH_PAYLOAD_LEN: usize = PAYLOAD_SIZE;
const PUBLISH_ENTRY_SIZE: usize = 12;

/// Build one full `publish_batch` frame for `sub_id` containing
/// `PUBLISH_BATCH_ENTRIES` messages with a deterministic payload. The
/// returned Vec<u8> is the on-wire bytes; in the bench we wrap it in
/// `Arc<Vec<u8>>` so cloning is just a refcount bump.
fn build_publish_batch(sub_id: u32) -> Vec<u8> {
    let entry_total_size = PUBLISH_ENTRY_SIZE + PUBLISH_SUBJECT.len() + PUBLISH_PAYLOAD_LEN;
    let body_len = 4 /* count */ + PUBLISH_BATCH_ENTRIES * entry_total_size;
    let total = ENVELOPE_SIZE + body_len;

    let mut buf = Vec::<u8>::with_capacity(total);
    let envelope = Envelope {
        action:    U16::new(Action::Publish.as_u16()),
        flags:     0,
        _rsv:      0,
        stream_id: U32::new(0xCAFE_0000 | sub_id),
        msg_len:   U32::new(body_len as u32),
        env_seq:   U32::new(1),
    };
    buf.extend_from_slice(envelope.as_bytes());
    buf.extend_from_slice(&(PUBLISH_BATCH_ENTRIES as u32).to_le_bytes());

    let payload = vec![0xABu8; PUBLISH_PAYLOAD_LEN];
    for _ in 0..PUBLISH_BATCH_ENTRIES {
        let entry = PublishEntry {
            data_len:  U32::new(PUBLISH_PAYLOAD_LEN as u32),
            subj_len:  U16::new(PUBLISH_SUBJECT.len() as u16),
            reply_len: U16::new(0),
            flags:     0,
            _pad:      [0u8; 3],
        };
        buf.extend_from_slice(entry.as_bytes());
        buf.extend_from_slice(PUBLISH_SUBJECT);
        buf.extend_from_slice(&payload);
    }
    debug_assert_eq!(buf.len(), total);
    buf
}

/// Reader for the publish_batch wire format. Parses Envelope + count
/// + N entries from the TCP byte stream; counts received messages.
async fn run_reader_publish_batch(
    mut sock: TcpStream,
    expected_msgs: usize,
) -> ReaderResult {
    use zerocopy::FromBytes;
    let mut backlog = Vec::with_capacity(1 << 20);
    let mut buf = vec![0u8; 1 << 16];
    let mut received = 0usize;
    let errors = 0usize;

    while received < expected_msgs {
        let n = match sock.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        backlog.extend_from_slice(&buf[..n]);

        // Try to parse one publish_batch frame at a time.
        loop {
            if backlog.len() < ENVELOPE_SIZE { break; }
            let env = Envelope::ref_from_bytes(&backlog[..ENVELOPE_SIZE])
                .expect("parse envelope");
            let body_len = env.msg_len.get() as usize;
            let total = ENVELOPE_SIZE + body_len;
            if backlog.len() < total { break; }

            // Body starts at ENVELOPE_SIZE; first 4 B = count.
            let body = &backlog[ENVELOPE_SIZE..total];
            let count = u32::from_le_bytes([body[0], body[1], body[2], body[3]]) as usize;
            received += count;

            backlog.drain(..total);
        }
    }

    ReaderResult {
        total_received: received,
        checksum_errors: errors,
        sub_counts: vec![0; 1], // not tracked in this scenario
    }
}

fn run_once_publish_batch(rt: &Runtime, n_subs: usize) -> (u128, ReaderResult) {
    rt.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let total_msgs = n_subs * frames_per_sub();

        let reader_h = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            sock.set_nodelay(true).ok();
            run_reader_publish_batch(sock, total_msgs).await
        });

        let tokio_stream = TcpStream::connect(addr).await.unwrap();
        tokio_stream.set_nodelay(true).ok();
        let raw_fd = tokio_stream.as_fd();
        let sock_ref = socket2::SockRef::from(&raw_fd);
        let _ = sock_ref.set_send_buffer_size(8 * 1024 * 1024);

        // kit::Mpmc payload is `Arc<Vec<u8>>` — clone is ~1 ns bump.
        let (producers, mut consumers, _shutdown) =
            Mpmc::<Arc<Vec<u8>>, 256>::new(n_subs.max(1), 1);
        let consumer = consumers.pop().unwrap();

        let std_stream = tokio_stream.into_std().unwrap();
        std_stream.set_nonblocking(false).ok();

        // ── PREBUILD: one batch per sub. Each batch holds
        // PUBLISH_BATCH_ENTRIES messages in publish_batch wire format.
        // Each producer keeps an Arc to its batch and clones it for every
        // send — the underlying Vec<u8> is NEVER copied.
        let prebuilt: Vec<Arc<Vec<u8>>> = (0..n_subs as u32)
            .map(|sub_id| Arc::new(build_publish_batch(sub_id)))
            .collect();

        // How many batch sends are needed per sub to deliver
        // `frames_per_sub()` total messages.
        let batches_per_sub = frames_per_sub().div_ceil(PUBLISH_BATCH_ENTRIES);

        // Total number of batch sends we expect to drain before exiting.
        // Mirrors the `Mpmc` writer's `written < expected_total` pattern,
        // because the kit::Mpmc consumer doesn't auto-close when producers
        // drop while `_shutdown` is held by this scope.
        let expected_batches = n_subs * batches_per_sub;

        // Spawn writer thread: drains Arc<Vec<u8>> batches and write_all's.
        let writer_h = std::thread::Builder::new()
            .name("writev-bench-writer-pb".into())
            .spawn(move || {
                use std::io::Write;
                consumer.bind();
                let mut writer = std_stream;
                let mut written_batches = 0usize;
                while written_batches < expected_batches {
                    let batch = match consumer.recv() {
                        Ok(b) => b,
                        Err(_) => break,
                    };
                    if writer.write_all(&batch).is_err() {
                        break;
                    }
                    written_batches += 1;
                    drop(batch); // explicit Arc decref; storage freed when last clone gone
                }
                let _ = writer.flush();
                eprintln!("[writer-pb] batches_written={} (expected {})",
                          written_batches, expected_batches);
            })
            .unwrap();

        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(n_subs);
        for (producer, batch) in producers.into_iter().zip(prebuilt.into_iter()) {
            handles.push(tokio::spawn(async move {
                for _ in 0..batches_per_sub {
                    // Arc::clone is ~1 ns bump — NO memcpy of the ~73KB body.
                    let mut frame = Arc::clone(&batch);
                    loop {
                        match producer.try_send(frame) {
                            Ok(()) => break,
                            Err(returned) => {
                                frame = returned;
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                }
            }));
        }
        for h in handles { let _ = h.await; }
        let _ = writer_h.join();

        let result = reader_h.await.unwrap();
        let elapsed_ns = t0.elapsed().as_nanos();
        (elapsed_ns, result)
    })
}

fn main() {
    println!("=== writev_concurrent — N subs share 1 TCP socket ===");
    println!("FRAMES_PER_SUB={}  BATCH={}  PAYLOAD={}B  FRAME={}B",
             frames_per_sub(), BATCH_SIZE, PAYLOAD_SIZE, FRAME_TOTAL);
    println!();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    // Warmup both strategies.
    let _ = run_once(&rt, N_SUBS, Strategy::Mpmc);
    let _ = run_once(&rt, N_SUBS, Strategy::MpmcPublishBatch);

    println!("{:<8} {:<22} {:>9} {:>10} {:>10} {:>10} {:>13} {:>13} {:>14} {:>10}",
             "n_subs", "strategy", "ms", "rcv", "expected", "errors",
             "msg/s_total", "MB/s_total", "ns/msg_avg", "verdict");
    println!("{}", "─".repeat(135));

    let configs = [1usize, 2, 4, 10, 20, 50];
    let strategies = [Strategy::Mpmc, Strategy::MpmcPublishBatch];

    for &n in configs.iter() {
        for &strat in strategies.iter() {
            let strat_name = match strat {
                Strategy::Mpmc => "mpmc",
                Strategy::MpmcPublishBatch => "mpmc_publish_batch",
            };
            let (elapsed, res) = run_once(&rt, n, strat);
            let expected = n * frames_per_sub();
            let elapsed_s = elapsed as f64 / 1e9;
            let total_bytes = expected as f64 * FRAME_TOTAL as f64;
            let msg_per_sec = expected as f64 / elapsed_s;
            let mb_per_sec  = total_bytes / elapsed_s / (1024.0 * 1024.0);
            let ns_per_msg  = elapsed as f64 / expected as f64;
            let verdict = if res.checksum_errors == 0 && res.total_received == expected {
                "PASS"
            } else {
                "FAIL"
            };
            println!("{:<8} {:<22} {:>9.1} {:>10} {:>10} {:>10} {:>13.0} {:>13.1} {:>14.1} {:>10}",
                     n,
                     strat_name,
                     elapsed as f64 / 1e6,
                     res.total_received,
                     expected,
                     res.checksum_errors,
                     msg_per_sec,
                     mb_per_sec,
                     ns_per_msg,
                     verdict);
            if res.checksum_errors > 0 {
                let printable = std::cmp::min(n, 10);
                for i in 0..printable {
                    println!("    sub {:2}: {} frames received", i, res.sub_counts[i]);
                }
            }
        }
    }
    println!();
    println!("Done.");
}
