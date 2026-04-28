//! release_zerocopy — temporary bench: zerocopy struct vs `Bytes::from(Vec)`.
//!
//! Goal: quantify the cost of the current header-build path
//! (`Vec::with_capacity` + 4× `extend_from_slice` + `Bytes::from(vec)`)
//! versus a `#[repr(C)] zerocopy::IntoBytes` struct shipped to the
//! kernel as iovecs. The struct travels by value through the SPSC ring
//! (zerocopy types are `Copy`); subject + payload are separate iovecs.
//!
//! Two scopes:
//!
//! - **encode-only**: build the header N times; consume each output
//!   with `black_box` so the compiler can't elide the work.
//! - **encode+transit**: build → push into `MpscAsync` → consumer pops
//!   and reads every byte (defeats DCE). No TCP — both variants would
//!   call the same `write_vectored`, so the I/O is held constant and
//!   only what we hand to the syscall changes.
//!
//! Subject = 16 B, payload = 128 B (`Bytes::clone` = refcount bump).
//!
//! Run from `/tmp/arbitro/` per `.agent/rules/testing.md`.

use std::hint::black_box;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWrite};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::IntoBytes;

use arbitro_proto::action::Action;
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_proto::wire::publish::{PublishEntry, PUBLISH_ENTRY_SIZE};

use arbitro_kit::route::{MpmcAsync, MpscAsync};

// ─── Constants ────────────────────────────────────────────────────────────

const SUBJECT_LEN: usize = 16;
const PAYLOAD_LEN: usize = 128;
const RING_CAP: usize = 4096;
const HDR_BYTES: usize = ENVELOPE_SIZE + 4 + PUBLISH_ENTRY_SIZE; // 32

// ─── Variant A: current (Vec → Bytes::from) ──────────────────────────────

#[inline(always)]
fn build_header_a(seq: u32, stream_id: u32, subj_len: usize, payload_len: usize) -> Vec<u8> {
    let body_len: u32 = (4 + 12 + subj_len + payload_len) as u32;
    // No subject copy here — the production path passes subject as a
    // separate iovec too. We measure only the *header* construction.
    let total = ENVELOPE_SIZE + 4 + PUBLISH_ENTRY_SIZE;
    let mut buf = Vec::with_capacity(total);

    let env = Envelope {
        action: U16::new(Action::Publish.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(body_len),
        env_seq: U32::new(seq),
    };
    buf.extend_from_slice(env.as_bytes());
    buf.extend_from_slice(&1u32.to_le_bytes());
    let entry = PublishEntry {
        data_len: U32::new(payload_len as u32),
        subj_len: U16::new(subj_len as u16),
        reply_len: U16::new(0),
        flags: 0,
        _pad: [0u8; 3],
    };
    buf.extend_from_slice(entry.as_bytes());
    debug_assert_eq!(buf.len(), total);
    buf
}

// ─── Variant B: zerocopy struct (stack, no alloc, no Bytes::from) ────────

/// Fixed prefix of a single-entry publish: envelope + count + entry.
/// Subject is variable-length and travels as a separate iovec.
#[derive(IntoBytes, zerocopy::Immutable, Clone, Copy)]
#[repr(C)]
struct PubSinglePrefix {
    envelope: Envelope,
    count: U32,
    entry: PublishEntry,
}

const _: () = assert!(core::mem::size_of::<PubSinglePrefix>() == ENVELOPE_SIZE + 4 + PUBLISH_ENTRY_SIZE);

#[inline(always)]
fn build_header_b(seq: u32, stream_id: u32, subj_len: u16, payload_len: u32) -> PubSinglePrefix {
    let body_len: u32 = (4 + 12 + subj_len as u32 + payload_len) as u32;
    PubSinglePrefix {
        envelope: Envelope {
            action: U16::new(Action::Publish.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(stream_id),
            msg_len: U32::new(body_len),
            env_seq: U32::new(seq),
        },
        count: U32::new(1),
        entry: PublishEntry {
            data_len: U32::new(payload_len),
            subj_len: U16::new(subj_len),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0u8; 3],
        },
    }
}

// ─── Variant C: iovec from pre-allocated arena (zero per-op alloc) ───────
//
// The producer owns an arena of `RING_CAP` `PubSinglePrefix` slots; on
// each iteration it writes to slot `i % RING_CAP` and ships only the
// raw `(*const u8, len)` pair through the channel. The arena outlives
// the consumer (back-pressure on the SPSC ring guarantees the slot is
// not overwritten before the consumer drains it). Zero allocs/op,
// zero memcpy of header bytes — the prefix is built in place and the
// pointer is what travels.

#[derive(Clone, Copy)]
struct IovDesc {
    ptr: *const u8,
    len: usize,
}
unsafe impl Send for IovDesc {}

// ─── Variant D: pure libc::malloc + libc::memcpy ─────────────────────────
//
// Bypasses Rust's allocator wrapper and Vec metadata. Each iter:
//   malloc(32) + 3× memcpy → raw `(ptr, len)` through channel → free.
// Drop guard ensures free runs even if the consumer panics.

struct LibcBuf {
    ptr: *mut u8,
    len: usize,
}
unsafe impl Send for LibcBuf {}
impl Drop for LibcBuf {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { libc::free(self.ptr as *mut libc::c_void); }
        }
    }
}

#[inline(always)]
fn build_header_d(seq: u32, stream_id: u32, subj_len: usize, payload_len: usize) -> LibcBuf {
    unsafe {
        let ptr = libc::malloc(HDR_BYTES) as *mut u8;
        let body_len: u32 = (4 + 12 + subj_len + payload_len) as u32;
        let env = Envelope {
            action: U16::new(Action::Publish.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(stream_id),
            msg_len: U32::new(body_len),
            env_seq: U32::new(seq),
        };
        let entry = PublishEntry {
            data_len: U32::new(payload_len as u32),
            subj_len: U16::new(subj_len as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0u8; 3],
        };
        let one: u32 = 1;

        libc::memcpy(ptr as *mut _, env.as_bytes().as_ptr() as *const _, ENVELOPE_SIZE);
        libc::memcpy(ptr.add(ENVELOPE_SIZE) as *mut _, &one as *const _ as *const _, 4);
        libc::memcpy(
            ptr.add(ENVELOPE_SIZE + 4) as *mut _,
            entry.as_bytes().as_ptr() as *const _,
            PUBLISH_ENTRY_SIZE,
        );
        LibcBuf { ptr, len: HDR_BYTES }
    }
}

// ─── Encode-only microbench ──────────────────────────────────────────────

fn bench_encode_only_a(iters: u64) -> Duration {
    let start = Instant::now();
    for i in 0..iters {
        let h = build_header_a(i as u32, 1, SUBJECT_LEN, PAYLOAD_LEN);
        black_box(h);
    }
    start.elapsed()
}

fn bench_encode_only_b(iters: u64) -> Duration {
    // subject is borrowed in the real path — we just measure the prefix build.
    let start = Instant::now();
    for i in 0..iters {
        let h = build_header_b(i as u32, 1, SUBJECT_LEN as u16, PAYLOAD_LEN as u32);
        black_box(h);
    }
    start.elapsed()
}

fn bench_encode_only_c(iters: u64) -> Duration {
    // Pre-allocated arena of structs; each iter writes in place + emits
    // a raw `(ptr, len)` pair. This is the production-shape iovec path.
    let mut arena: Vec<PubSinglePrefix> = (0..RING_CAP)
        .map(|_| build_header_b(0, 0, 0, 0))
        .collect();
    let start = Instant::now();
    for i in 0..iters {
        let slot = (i as usize) & (RING_CAP - 1);
        arena[slot] = build_header_b(i as u32, 1, SUBJECT_LEN as u16, PAYLOAD_LEN as u32);
        let desc = IovDesc {
            ptr: arena[slot].as_bytes().as_ptr(),
            len: HDR_BYTES,
        };
        black_box(desc);
    }
    start.elapsed()
}

fn bench_encode_only_d(iters: u64) -> Duration {
    let start = Instant::now();
    for i in 0..iters {
        let h = build_header_d(i as u32, 1, SUBJECT_LEN, PAYLOAD_LEN);
        black_box(&h); // forces the malloc'd buffer to be observed before drop
        drop(h);
    }
    start.elapsed()
}

// ─── Encode + channel transit ────────────────────────────────────────────
//
// Producer → MpscAsync → Consumer reads every byte. No TCP (both
// variants use the same syscall). Sums every byte into an atomic so
// the compiler can't elide the work. Single producer, single consumer
// — the contention model is identical across variants.

// Frame carries ONLY the header. Subject + payload are constants in
// production too (Bytes refcount-bumped from caller storage); they
// would add identical noise to both variants. Exclude to isolate the
// header-encode delta.
type FrameA = Vec<u8>;
type FrameB = PubSinglePrefix;

fn bench_transit_a(iters: u64) -> Duration {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (mut prods, mut cons, _shutdown) =
            MpscAsync::<FrameA, RING_CAP>::new(1);
        let prod = prods.pop().unwrap();
        let sink = Arc::new(AtomicU64::new(0));

        let cons_h = {
            let sink = sink.clone();
            tokio::spawn(async move {
                let mut acc: u64 = 0;
                let mut received: u64 = 0;
                while let Ok(h) = cons.recv_async().await {
                    for b in h.iter() { acc = acc.wrapping_add(*b as u64); }
                    received += 1;
                    if received >= iters { break; }
                }
                sink.store(acc, Ordering::Relaxed);
            })
        };

        let start = Instant::now();
        for i in 0..iters {
            let mut frame: FrameA = build_header_a(i as u32, 1, SUBJECT_LEN, PAYLOAD_LEN);
            loop {
                match prod.try_send(frame) {
                    Ok(()) => break,
                    Err(returned) => {
                        frame = returned;
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
        let elapsed = start.elapsed();
        let _ = cons_h.await;
        black_box(sink.load(Ordering::Relaxed));
        elapsed
    })
}

fn bench_transit_b(iters: u64) -> Duration {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (mut prods, mut cons, _shutdown) =
            MpscAsync::<FrameB, RING_CAP>::new(1);
        let prod = prods.pop().unwrap();
        let sink = Arc::new(AtomicU64::new(0));

        let cons_h = {
            let sink = sink.clone();
            tokio::spawn(async move {
                let mut acc: u64 = 0;
                let mut received: u64 = 0;
                while let Ok(prefix) = cons.recv_async().await {
                    for b in prefix.as_bytes().iter() { acc = acc.wrapping_add(*b as u64); }
                    received += 1;
                    if received >= iters { break; }
                }
                sink.store(acc, Ordering::Relaxed);
            })
        };

        let start = Instant::now();
        for i in 0..iters {
            let mut frame: FrameB =
                build_header_b(i as u32, 1, SUBJECT_LEN as u16, PAYLOAD_LEN as u32);
            loop {
                match prod.try_send(frame) {
                    Ok(()) => break,
                    Err(returned) => {
                        frame = returned;
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
        let elapsed = start.elapsed();
        let _ = cons_h.await;
        black_box(sink.load(Ordering::Relaxed));
        elapsed
    })
}

type FrameC = IovDesc;
type FrameD = LibcBuf;

fn bench_transit_c(iters: u64) -> Duration {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (mut prods, mut cons, _shutdown) =
            MpscAsync::<FrameC, RING_CAP>::new(1);
        let prod = prods.pop().unwrap();
        let sink = Arc::new(AtomicU64::new(0));

        // Arena lives for the whole bench. Back-pressure of the ring
        // (capacity = RING_CAP) prevents the producer from overwriting
        // a slot the consumer hasn't drained yet.
        let mut arena: Vec<PubSinglePrefix> = (0..RING_CAP)
            .map(|_| build_header_b(0, 0, 0, 0))
            .collect();

        let cons_h = {
            let sink = sink.clone();
            tokio::spawn(async move {
                let mut acc: u64 = 0;
                let mut received: u64 = 0;
                while let Ok(desc) = cons.recv_async().await {
                    let slice = unsafe { std::slice::from_raw_parts(desc.ptr, desc.len) };
                    for b in slice.iter() { acc = acc.wrapping_add(*b as u64); }
                    received += 1;
                    if received >= iters { break; }
                }
                sink.store(acc, Ordering::Relaxed);
            })
        };

        let start = Instant::now();
        for i in 0..iters {
            let slot = (i as usize) & (RING_CAP - 1);
            arena[slot] = build_header_b(i as u32, 1, SUBJECT_LEN as u16, PAYLOAD_LEN as u32);
            let mut frame = IovDesc {
                ptr: arena[slot].as_bytes().as_ptr(),
                len: HDR_BYTES,
            };
            loop {
                match prod.try_send(frame) {
                    Ok(()) => break,
                    Err(returned) => {
                        frame = returned;
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
        let elapsed = start.elapsed();
        let _ = cons_h.await;
        black_box(sink.load(Ordering::Relaxed));
        drop(arena);
        elapsed
    })
}

fn bench_transit_d(iters: u64) -> Duration {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (mut prods, mut cons, _shutdown) =
            MpscAsync::<FrameD, RING_CAP>::new(1);
        let prod = prods.pop().unwrap();
        let sink = Arc::new(AtomicU64::new(0));

        let cons_h = {
            let sink = sink.clone();
            tokio::spawn(async move {
                let mut acc: u64 = 0;
                let mut received: u64 = 0;
                while let Ok(buf) = cons.recv_async().await {
                    let slice = unsafe { std::slice::from_raw_parts(buf.ptr, buf.len) };
                    for b in slice.iter() { acc = acc.wrapping_add(*b as u64); }
                    drop(buf); // libc::free
                    received += 1;
                    if received >= iters { break; }
                }
                sink.store(acc, Ordering::Relaxed);
            })
        };

        let start = Instant::now();
        for i in 0..iters {
            let mut frame = build_header_d(i as u32, 1, SUBJECT_LEN, PAYLOAD_LEN);
            loop {
                match prod.try_send(frame) {
                    Ok(()) => break,
                    Err(returned) => {
                        frame = returned;
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
        let elapsed = start.elapsed();
        let _ = cons_h.await;
        black_box(sink.load(Ordering::Relaxed));
        elapsed
    })
}

// ─── Variant G: same-thread try_send/try_recv (no parking, no cross-core) ─
//
// Producer and consumer alternate on the same OS thread. No tokio,
// no waker, no cache coherency penalty. Pure ring slot transit cost.
// This is the floor — anything above this number on the cross-thread
// scopes is *physics* (cache coherency + scheduler), not channel logic.

fn bench_transit_g_same_thread(iters: u64) -> Duration {
    use arbitro_kit::route::Mpsc;
    let (mut prods, cons, _shutdown) =
        Mpsc::<PubSinglePrefix, RING_CAP>::new(1);
    let prod = prods.pop().unwrap();
    let mut acc: u64 = 0;
    let start = Instant::now();
    for i in 0..iters {
        let frame = build_header_b(i as u32, 1, SUBJECT_LEN as u16, PAYLOAD_LEN as u32);
        if prod.try_send(frame).is_err() { panic!("ring full at cap=4096"); }
        let received = cons.try_recv().expect("just sent");
        for b in received.as_bytes().iter() {
            acc = acc.wrapping_add(*b as u64);
        }
    }
    let elapsed = start.elapsed();
    black_box(acc);
    elapsed
}

// ─── Variant F: zerocopy struct over tokio::sync::mpsc (1P → 1C) ─────────
//
// Same struct B payload, but the kit `MpscAsync` is replaced by
// `tokio::sync::mpsc::channel` (the canonical tokio MPSC). Lets us
// compare the kit's per-producer-SPSC mini-rings against tokio's
// linked-list / atomic-stack queue under identical encoder + payload
// conditions.

fn bench_transit_f_tokio_mpsc(iters: u64) -> Duration {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<PubSinglePrefix>(RING_CAP);
        let sink = Arc::new(AtomicU64::new(0));

        let cons_h = {
            let sink = sink.clone();
            tokio::spawn(async move {
                let mut acc: u64 = 0;
                let mut received: u64 = 0;
                while let Some(prefix) = rx.recv().await {
                    for b in prefix.as_bytes().iter() { acc = acc.wrapping_add(*b as u64); }
                    received += 1;
                    if received >= iters { break; }
                }
                sink.store(acc, Ordering::Relaxed);
            })
        };

        let start = Instant::now();
        for i in 0..iters {
            let frame = build_header_b(i as u32, 1, SUBJECT_LEN as u16, PAYLOAD_LEN as u32);
            // tokio::mpsc has its own backpressure (await on send); using
            // try_send + yield matches the production-shape pattern of
            // the other transit variants (no waker on the producer side).
            let mut f = frame;
            loop {
                match tx.try_send(f) {
                    Ok(()) => break,
                    Err(tokio::sync::mpsc::error::TrySendError::Full(returned)) => {
                        f = returned;
                        tokio::task::yield_now().await;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return Duration::default(),
                }
            }
        }
        let elapsed = start.elapsed();
        let _ = cons_h.await;
        black_box(sink.load(Ordering::Relaxed));
        elapsed
    })
}

fn bench_transit_f_tokio_mpsc_mp(producers: usize, total_iters: u64) -> Duration {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(producers + 2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<PubSinglePrefix>(RING_CAP);
        let per_producer = total_iters / producers as u64;
        let total = per_producer * producers as u64;
        let sink = Arc::new(AtomicU64::new(0));

        let cons_h = {
            let sink = sink.clone();
            tokio::spawn(async move {
                let mut acc: u64 = 0;
                let mut received: u64 = 0;
                while let Some(prefix) = rx.recv().await {
                    for b in prefix.as_bytes().iter() { acc = acc.wrapping_add(*b as u64); }
                    received += 1;
                    if received >= total { break; }
                }
                sink.store(acc, Ordering::Relaxed);
            })
        };

        let start = Instant::now();
        let mut js = tokio::task::JoinSet::new();
        for _ in 0..producers {
            let tx = tx.clone();
            js.spawn(async move {
                for i in 0..per_producer {
                    let mut f = build_header_b(
                        i as u32, 1, SUBJECT_LEN as u16, PAYLOAD_LEN as u32,
                    );
                    loop {
                        match tx.try_send(f) {
                            Ok(()) => break,
                            Err(tokio::sync::mpsc::error::TrySendError::Full(returned)) => {
                                f = returned;
                                tokio::task::yield_now().await;
                            }
                            Err(_) => return,
                        }
                    }
                }
            });
        }
        drop(tx);
        while js.join_next().await.is_some() {}
        let elapsed = start.elapsed();
        let _ = cons_h.await;
        black_box(sink.load(Ordering::Relaxed));
        elapsed
    })
}

// ─── Variant E: zerocopy struct over MpmcAsync (M producers → 1 consumer) ─
//
// Same B (zerocopy struct on stack) but routed through `MpmcAsync`
// with M producer shards fanning into a single consumer. Models the
// fan-in pressure of multiple client tasks publishing concurrently
// from inside the same process. `total_iters` is split evenly across
// the M producers so the total work matches the SPSC scopes above.

fn bench_transit_e_mpmc(producers: usize, total_iters: u64) -> Duration {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(producers + 2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (prods, mut cons_v, _shutdown) =
            MpmcAsync::<PubSinglePrefix, RING_CAP>::new(producers, 1);
        let cons = cons_v.pop().unwrap();
        let per_producer = total_iters / producers as u64;
        let total = per_producer * producers as u64;

        let start = Instant::now();

        // Producers — `MpmcProducer` is Send so each can run on its own task.
        let mut js = tokio::task::JoinSet::new();
        for prod in prods {
            js.spawn(async move {
                for i in 0..per_producer {
                    let mut frame = build_header_b(
                        i as u32, 1, SUBJECT_LEN as u16, PAYLOAD_LEN as u32,
                    );
                    loop {
                        match prod.try_send(frame) {
                            Ok(()) => break,
                            Err(returned) => {
                                frame = returned;
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                }
            });
        }

        // Consumer — runs on this main task (no `tokio::spawn`) because
        // `MpmcConsumer` is `!Sync`, so its `recv_async` future is `!Send`.
        let consumer_fut = async {
            let mut acc: u64 = 0;
            for _ in 0..total {
                match cons.recv_async().await {
                    Ok(prefix) => {
                        for b in prefix.as_bytes().iter() {
                            acc = acc.wrapping_add(*b as u64);
                        }
                    }
                    Err(_) => break,
                }
            }
            acc
        };
        let producers_fut = async {
            while js.join_next().await.is_some() {}
        };
        let (acc, _) = tokio::join!(consumer_fut, producers_fut);

        let elapsed = start.elapsed();
        black_box(acc);
        elapsed
    })
}

// ─── TCP scope: struct → write_vectored → kernel ─────────────────────────
//
// This is the production-shape final test. The producer builds the
// `PubSinglePrefix` on stack, ships it through `MpscAsync` together
// with a refcount-bumped `Bytes` payload. The writer task drains the
// channel and emits each frame via `poll_write_vectored([prefix.as_bytes(),
// payload])` — exactly what `arbitro-client::conn::write_loop` would
// do once migrated. A drain task on the other end of the TCP socket
// reads + counts bytes (the broker is the kernel + a black-hole reader).
//
// Goal: confirm that `struct.as_bytes()` interoperates with `IoSlice`
// without surprises and that throughput in a real syscall path is at
// least as good as the no-TCP transit (the syscall is the ceiling).

type TcpFrame = (PubSinglePrefix, Bytes);

async fn write_all_vectored2_async(
    writer: &mut OwnedWriteHalf,
    header: &[u8],
    payload: &[u8],
) -> std::io::Result<()> {
    use std::future::poll_fn;
    use std::io::IoSlice;
    use tokio::io::AsyncWriteExt;

    let mut header_written = 0usize;
    while header_written < header.len() {
        let h = &header[header_written..];
        let bufs = [IoSlice::new(h), IoSlice::new(payload)];
        let n = poll_fn(|cx| Pin::new(&mut *writer).poll_write_vectored(cx, &bufs)).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "poll_write_vectored returned 0",
            ));
        }
        if n <= h.len() {
            header_written += n;
        } else {
            let payload_done = n - h.len();
            return writer.write_all(&payload[payload_done..]).await;
        }
    }
    writer.write_all(payload).await
}

fn bench_tcp_struct(iters: u64) -> Duration {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let total_bytes: u64 = iters * (HDR_BYTES as u64 + PAYLOAD_LEN as u64);

        // Drain task: count bytes read from the socket. When `total_bytes`
        // are seen, exit.
        let drain = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = sock.set_nodelay(true);
            let mut buf = vec![0u8; 64 * 1024];
            let mut got: u64 = 0;
            while got < total_bytes {
                match sock.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => got += n as u64,
                    Err(_) => break,
                }
            }
            got
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let _ = stream.set_nodelay(true);
        let (_read_half, write_half) = stream.into_split();

        let (mut prods, mut cons, shutdown) =
            MpscAsync::<TcpFrame, RING_CAP>::new(1);
        let prod = prods.pop().unwrap();

        // Writer task: drain the Mpsc, call write_vectored.
        let writer_h = tokio::spawn(async move {
            let mut wh = write_half;
            while let Ok((prefix, payload)) = cons.recv_async().await {
                if write_all_vectored2_async(&mut wh, prefix.as_bytes(), &payload)
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });

        let payload: Bytes = Bytes::from(vec![0u8; PAYLOAD_LEN]);
        let start = Instant::now();
        for i in 0..iters {
            let prefix =
                build_header_b(i as u32, 1, SUBJECT_LEN as u16, PAYLOAD_LEN as u32);
            let mut frame: TcpFrame = (prefix, payload.clone());
            loop {
                match prod.try_send(frame) {
                    Ok(()) => break,
                    Err(returned) => {
                        frame = returned;
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
        // Wait for the drain to confirm all bytes hit the kernel — that
        // is the actual end of the throughput measurement.
        let got = drain.await.unwrap_or(0);
        let elapsed = start.elapsed();
        // Now tear down: drop producer, signal shutdown so the writer
        // task's `recv_async` returns `Err(Shutdown)` and exits.
        drop(prod);
        shutdown.signal();
        let _ = writer_h.await;
        assert_eq!(got, total_bytes, "drain saw {} of {} bytes", got, total_bytes);
        elapsed
    })
}

// ─── Driver ───────────────────────────────────────────────────────────────

fn fmt_row(name: &str, iters: u64, dur: Duration) {
    let ns = dur.as_nanos() as f64 / iters as f64;
    let mps = iters as f64 / dur.as_secs_f64();
    println!(
        "  {name:30} | {iters:>8} | {:>9.2}ms | {ns:>8.2} ns/op | {mps:>11.0} op/s",
        dur.as_secs_f64() * 1000.0,
    );
}

fn main() {
    let iters_encode: u64 = std::env::var("BENCH_ENCODE_ITERS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(2_000_000);
    let iters_transit: u64 = std::env::var("BENCH_TRANSIT_ITERS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(200_000);

    println!("release_zerocopy — Bytes::from(Vec) vs zerocopy::IntoBytes struct");
    println!("subject={SUBJECT_LEN}B  payload={PAYLOAD_LEN}B  ring={RING_CAP}\n");
    println!("  {:30} | {:>8} | {:>9}  | {:>8}     | {}", "scope", "iters", "elapsed", "ns/op", "op/s");
    println!("  {}", "-".repeat(95));

    // Warm caches.
    let _ = bench_encode_only_a(50_000);
    let _ = bench_encode_only_b(50_000);
    let _ = bench_encode_only_c(50_000);
    let _ = bench_encode_only_d(50_000);

    println!("\n[encode-only — pure CPU, no channel]");
    fmt_row("A. Vec+extend_from_slice",   iters_encode, bench_encode_only_a(iters_encode));
    fmt_row("B. zerocopy struct (stack)", iters_encode, bench_encode_only_b(iters_encode));
    fmt_row("C. iovec (arena ptr+len)",   iters_encode, bench_encode_only_c(iters_encode));
    fmt_row("D. libc malloc+memcpy",      iters_encode, bench_encode_only_d(iters_encode));

    println!("\n[FLOOR: same-thread try_send + try_recv (no parking, no cross-core)]");
    fmt_row("G. same-thread try_send/recv", iters_transit, bench_transit_g_same_thread(iters_transit));

    println!("\n[encode + Mpsc transit (1 producer, 1 consumer, full byte read)]");
    fmt_row("A. Vec+extend_from_slice",   iters_transit, bench_transit_a(iters_transit));
    fmt_row("B. zerocopy struct (stack)", iters_transit, bench_transit_b(iters_transit));
    fmt_row("C. iovec (arena ptr+len)",   iters_transit, bench_transit_c(iters_transit));
    fmt_row("D. libc malloc+memcpy",      iters_transit, bench_transit_d(iters_transit));

    println!("\n[encode + Mpmc transit — M producers → 1 consumer (struct B)]");
    for &m in &[2usize, 4, 8] {
        let dur = bench_transit_e_mpmc(m, iters_transit);
        fmt_row(&format!("E. Mpmc M={m}, struct B"), iters_transit, dur);
    }

    println!("\n[encode + tokio::sync::mpsc transit (struct B)]");
    fmt_row("F. tokio::mpsc 1P→1C",     iters_transit, bench_transit_f_tokio_mpsc(iters_transit));
    for &m in &[2usize, 4, 8] {
        let dur = bench_transit_f_tokio_mpsc_mp(m, iters_transit);
        fmt_row(&format!("F. tokio::mpsc M={m}→1C"), iters_transit, dur);
    }

    println!("\n[encode + Mpsc + TCP write_vectored (production shape, struct → kernel)]");
    let iters_tcp: u64 = std::env::var("BENCH_TCP_ITERS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(50_000);
    let dur = bench_tcp_struct(iters_tcp);
    fmt_row("B+TCP. struct → writev",    iters_tcp, dur);
    let bytes = iters_tcp * (HDR_BYTES as u64 + PAYLOAD_LEN as u64);
    let mibps = (bytes as f64) / dur.as_secs_f64() / (1024.0 * 1024.0);
    println!(
        "       throughput = {:.0} MiB/s ({} bytes/op = {} hdr + {} payload)",
        mibps, HDR_BYTES + PAYLOAD_LEN, HDR_BYTES, PAYLOAD_LEN,
    );
}
