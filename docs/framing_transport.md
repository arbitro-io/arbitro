# Framing & Transport — "Let the stream be the one that copies"

> **Core principle**: userland never copies payload bytes. The only memcpy
> that happens is the one the kernel does when writing the socket buffer.
> Every other step works with references, typed views, and pointer casts.

This document captures the findings from a series of benchmarks comparing
encoding/framing strategies over TCP, proposes an ideal structure for
messages traveling between ingress and egress, and sketches a batching
architecture that defers all copies to the final write syscall.

---

## 1. Benchmark findings

Four benches in `crates/arbitro-e2e/benches/` feed this document:

- `tcp_send.rs` — raw TCP throughput (tokio vs tokio-uring vs libc).
- `framing.rs` — encode/decode micro-benchmark (libc vs rust-safe vs
  `bytes::BufMut` vs zerocopy).
- `tcp_framed.rs` — end-to-end frame write strategies over a real TCP
  connection, variants A–G.
- `chan_byte.rs` — channel latency (context, not framing).

### 1.1 Raw TCP ceiling (single conn, loopback, tokio current_thread)

| Variant | GB/s | Notes |
|---|---|---|
| tokio current_thread + 1 conn | 8.6 | epoll, non-blocking |
| tokio-uring default + 1 conn | 8.6 | io_uring, same regime |
| tokio-uring + SQPOLL | 3.7 | worse; SQPOLL hurts serial await |
| libc raw blocking + 1 conn | 7.1 | blocking write ≈ 18% slower |

**Conclusion**: for loopback single-conn, tokio and tokio-uring tie at
~8.6 GB/s. libc blocking is *slower* because the fill-then-sleep pattern
hits socket-buffer-full more often than tokio's EAGAIN + epoll loop.

### 1.2 Parallel saturation (8 conns)

| Variant | GB/s | Notes |
|---|---|---|
| **tokio multi_thread(4) × 8 conns** | **21.4** | workers = physical cores |
| tokio multi_thread(8) × 8 conns | 15.9 | too many workers |
| tokio multi_thread(24) × 8 conns | 12.4 | oversubscription |
| libc raw × 8 conns (16 threads) | 12.7 | thread oversubscription |
| tokio-uring × 8 conns | 6.7 | single-threaded runtime limit |

**Conclusion**: tokio multi_thread with `worker_threads = physical cores`
is the best scaling model for a broker. `tokio-uring` is capped by its
single-ring single-thread design and cannot scale horizontally without
manual sharding.

### 1.3 Framing micro-bench (101-byte frame, encode)

| Strategy | ns/op | GiB/s |
|---|---|---|
| libc memcpy (unsafe) | 2.9 | 33.0 |
| **zerocopy header** | 3.3 | 28.0 |
| rust `copy_from_slice` | 7.6 | 12.3 |
| `bytes::BufMut` | 9.8 | 9.6 |

**Conclusion**: `bytes::BufMut` (the current idiom in `drain.rs`) is **3×
slower** than a zerocopy struct encode. Migrating the header build site to
a zerocopy `#[repr(C, packed)]` struct matches `libc::memcpy` performance
with zero `unsafe`.

### 1.4 TCP framed writes (1038 B frame, varying batch strategies)

| Variant | ns/frame | GB/s | Speedup |
|---|---|---|---|
| A. `BytesMut` per-frame | 1521 | 0.68 | 1.0× (baseline) |
| B. Prealloc `Vec<u8>` + encode + write_all | 1450 | 0.72 | 1.05× |
| C. zerocopy + writev (1 frame) | 1514 | 0.68 | ~same |
| D. zerocopy header → prealloc + write_all | 1452 | 0.71 | 1.04× |
| E. batch=512 zerocopy + writev (2N IoSlices) | 133 | 7.76 | 11.4× |
| F. batch=512 FixedFrame `slice.as_bytes()` | 105 | 9.91 | 14.6× |
| **G. batch=512 DST `mut_from_bytes()` + write_all** | **103** | **10.06** | **14.8×** |

**Conclusions**:

1. Without batching the bottleneck is the syscall (~1.4 µs each). Any
   encode strategy is noise next to that.
2. Batching with `write_vectored` reduces per-frame syscall cost from
   ~1400 ns to ~3 ns (amortized over 512 frames).
3. A contiguous buffer (`write_all(&buf)`) beats `write_vectored` with
   many small `IoSlice`s because the kernel performs one large memcpy
   instead of many small ones.
4. The DST zerocopy pattern (`#[repr(C, packed)] struct Frame { header,
   payload: [u8] }`) ties with fixed-size structs while supporting
   variable payloads and typed field access.
5. 10 GB/s is within ~15% of the memcpy loopback ceiling — this is
   effectively the physical floor for userspace TCP on this machine.

---

## 2. Ideal structure between ingress and egress

### 2.1 Design principles

The benchmark results lead to four non-negotiable rules for the hot path:

1. **Payload lives in one place, from ingress to egress.** A message is
   admitted to a buffer once (when read from the ingress socket) and
   leaves that buffer via a kernel memcpy to the egress socket. Userland
   does not copy payload bytes.

2. **Headers are typed views, never dynamic encoders.** Use
   `#[repr(C, packed)]` + zerocopy `IntoBytes`/`FromBytes` so that header
   mutation compiles to a single store and header writes are a ptr cast.

3. **Batching is mandatory in the egress path.** Under no circumstances
   should the drain writer issue one syscall per frame. A batch of 32–512
   frames is the right ballpark — 32 captures 85% of the win, 512 is
   within 1% of the ceiling.

4. **Let the stream be the one that copies.** The only memcpy is
   `copy_from_user` done by the kernel during `write()`/`writev()`.

### 2.2 Frame layout

```rust
use zerocopy::{FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned};
use zerocopy::big_endian::{U16, U32, U64};

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub struct FrameHeader {
    pub total_len:   U32,   // full frame including header
    pub version:     u8,
    pub msg_type:    u8,    // Publish / Ack / Ping / ...
    pub flags:       U16,
    pub seq:         U64,
    pub conn_id:     U32,
    pub stream_id:   U32,
    pub ingress_ns:  U64,   // stamped at ingress, never mutated downstream
    pub egress_ns:   U64,   // stamped at egress by drain writer
    pub subject_len: u8,
    pub reply_len:   u8,
    pub header_len:  U16,   // user headers region
    pub payload_len: U32,
}

const _: () = assert!(std::mem::size_of::<FrameHeader>() == 48);

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub struct Frame {
    pub header: FrameHeader,
    pub tail:   [u8],       // DST — subject + reply + headers + payload
}
```

The `tail` is opaque bytes with a known sub-layout reconstructable from
the header (`subject_len`, `reply_len`, `header_len`, `payload_len`
determine the splits). Routers and drain writers that only need the
header **never parse** the tail.

### 2.3 End-to-end flow

```
┌──────────────────────────────────────────────────────────────────────┐
│                         INGRESS SOCKET                               │
│  read() → ingress_buf[N bytes]                                       │
│           ↓                                                          │
│  Frame::ref_from_bytes(&ingress_buf[..total_len])   (ptr cast, 0 ns) │
│           ↓                                                          │
│  frame.header.ingress_ns = now_ns();   (one u64 store)               │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     SHARD ROUTING (same thread)                      │
│  shard_id = hash(frame.header.stream_id)                             │
│  payload never touched, subject never copied                         │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│              EGRESS BATCH RING (one per drain writer)                │
│                                                                      │
│   [ frame0 | frame1 | frame2 | ... | frameN ]  ← contiguous Vec<u8>  │
│    ^head                              ^tail                          │
│                                                                      │
│  Append: one copy_from_slice of the ingress bytes into ring tail.    │
│  This is the ONE copy that moves the payload from ingress to egress. │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         EGRESS SOCKET                                │
│  When batch full (N frames or T µs timeout):                         │
│    for slot in frames_iter(&mut ring):                               │
│        frame = Frame::mut_from_bytes(slot);                          │
│        frame.header.egress_ns = now_ns();                            │
│    stream.write_all(&ring[..tail])  ← ONE syscall, ONE kernel memcpy │
│    ring.clear()                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

Copies in this path:

1. **Kernel → ingress_buf** (`read()`): kernel copies from socket buffer
   to user buffer. Unavoidable.
2. **ingress_buf → egress ring** (`copy_from_slice`): moves the frame
   from the ingress reader's buffer to the shard-local egress ring. This
   is the only userland memcpy, and it is per-frame.
3. **egress ring → kernel** (`write_all`): kernel copies from user
   buffer to socket buffer. One call per batch.

Three memcpys total for N frames, two of which are kernel-side and the
third is the routing copy between two user buffers. Everything else is
ptr casts and length checks.

---

## 3. Working examples

### 3.1 Header-only parse, full forward

```rust
use std::io::IoSlice;

fn route_and_forward(
    incoming: &[u8],
    egress: &mut impl std::io::Write,
) -> std::io::Result<()> {
    // Parse the header without touching the payload.
    let frame = Frame::ref_from_bytes(incoming).expect("valid frame");
    let shard = shard_for(frame.header.stream_id.get());
    let _ = shard;

    // Forward the whole frame verbatim. as_bytes() is a ptr cast over
    // the original `incoming` slice — zero copy from our side.
    egress.write_all(frame.as_bytes())
}
```

### 3.2 Mutate egress timestamp, write the whole frame

```rust
fn stamp_and_write(
    buf: &mut [u8],
    egress: &mut std::net::TcpStream,
) -> std::io::Result<()> {
    let frame = Frame::mut_from_bytes(buf).expect("valid frame");
    frame.header.egress_ns = U64::new(now_ns());
    // as_bytes() covers header + tail; one memcpy on the kernel side.
    egress.write_all(frame.as_bytes())
}
```

### 3.3 Batched drain writer

```rust
struct DrainWriter {
    ring: Vec<u8>,             // contiguous egress buffer
    frame_offsets: Vec<usize>, // start of each frame in `ring`
    batch_target: usize,       // typically 64–128
}

impl DrainWriter {
    fn push(&mut self, incoming_frame: &[u8]) {
        self.frame_offsets.push(self.ring.len());
        self.ring.extend_from_slice(incoming_frame);
    }

    async fn flush(&mut self, sock: &mut tokio::net::TcpStream)
        -> std::io::Result<()>
    {
        use tokio::io::AsyncWriteExt;

        // Stamp egress time on every frame via DST views.
        // No copies — just header stores.
        let ts = now_ns();
        for &off in &self.frame_offsets {
            let slot = &mut self.ring[off..];
            let frame = Frame::mut_from_bytes(slot).unwrap();
            frame.header.egress_ns = U64::new(ts);
        }

        // One syscall, one kernel memcpy for the whole batch.
        sock.write_all(&self.ring).await?;

        self.ring.clear();
        self.frame_offsets.clear();
        Ok(())
    }
}
```

### 3.4 Parse selectively — enum validated via `TryFromBytes`

```rust
use zerocopy::TryFromBytes;

#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(u8)]
pub enum MsgType {
    Publish    = 0x01,
    Subscribe  = 0x02,
    Ack        = 0x03,
    Ping       = 0x04,
    Pong       = 0x05,
}

fn decode_kind(buf: &[u8]) -> Option<(MsgType, &[u8])> {
    MsgType::try_read_from_prefix(&buf[9..]).ok()
}
```

Byte patterns outside the declared discriminants are rejected at parse
time without `unsafe`.

---

## 4. Benefits

| Aspect | Current (`bytes::BufMut`) | Proposed (zerocopy DST + batched ring) |
|---|---|---|
| Encode cost | ~10 ns/frame (5 writes) | ~3 ns/frame (1 header memcpy) |
| Allocations per frame | 1 (`BytesMut::with_capacity`) | 0 (ring is pre-allocated) |
| Syscalls per N frames | N (one per frame) | 1 (one per batch) |
| Userland payload copies | 1 (into `BytesMut`) + 1 (into kernel) | 1 (ingress → ring) + 1 (kernel) |
| Header mutation | Builder pattern, no typed view | `frame.header.field = value` |
| Layout changes | Manual everywhere | Compile-time `assert!` |
| `unsafe` | 0 | 0 |
| Throughput (1 KB frames) | 0.68 GB/s | ~10 GB/s |
| CPU / 1 M msg/s | ~150% of a core | ~10% of a core |

---

## 5. Hypothesis — struct with list for batching, copy only at write time

### 5.1 Problem statement

We want a type that:

- Accepts a sequence of frames from ingress (one at a time, across threads).
- Holds them in a form that is writeable in a single syscall.
- Never copies the payload bytes in userland until the write itself.
- Supports variable-size payloads.

The benchmark winner (variant G) used a single contiguous `Vec<u8>`
populated by `extend_from_slice`. That already does "copy once" — from the
ingress buffer into the ring. The hypothesis below explores going one step
further: **can we avoid even that copy** by holding references?

### 5.2 Hypothesis A — Owned ring buffer (baseline, proven)

```rust
pub struct EgressBatch {
    /// Contiguous byte buffer. Each frame is appended via copy_from_slice
    /// when the shard hands it off. Headers mutable via DST views. When
    /// the batch is ready, write_all(&ring[..tail]) in a single syscall.
    ring: Vec<u8>,
    offsets: Vec<usize>,
}

impl EgressBatch {
    pub fn push(&mut self, frame_bytes: &[u8]) {
        self.offsets.push(self.ring.len());
        self.ring.extend_from_slice(frame_bytes);  // <-- THE one userland copy
    }

    pub async fn flush<W: AsyncWrite + Unpin>(&mut self, w: &mut W)
        -> io::Result<()>
    {
        w.write_all(&self.ring).await?;
        self.ring.clear();
        self.offsets.clear();
        Ok(())
    }
}
```

- Throughput: **~10 GB/s** (measured in variant G).
- Copies per frame in userland: **1** (the `extend_from_slice`).
- Complexity: low.

This is the floor for a correct implementation. Anything fancier must beat
it to be worth the complexity cost.

### 5.3 Hypothesis B — Zero-copy via retained `Bytes` references

```rust
use bytes::Bytes;

pub struct EgressBatchRef {
    /// Each frame is held by its own reference-counted buffer. No copy
    /// into a central ring; we build IoSlices at flush time.
    frames: Vec<Bytes>,
}

impl EgressBatchRef {
    pub fn push(&mut self, frame: Bytes) {
        self.frames.push(frame);     // Arc increment, no memcpy
    }

    pub async fn flush<W: AsyncWrite + Unpin>(&mut self, w: &mut W)
        -> io::Result<()>
    {
        let slices: Vec<IoSlice> =
            self.frames.iter().map(|b| IoSlice::new(b)).collect();
        w.write_vectored(&slices).await?;  // kernel memcpys N regions
        self.frames.clear();
        Ok(())
    }
}
```

- Throughput: **~7.8 GB/s** (measured in variant E with batch=512).
- Copies per frame in userland: **0**.
- Complexity: medium (refcounting, IoSlice vector allocation per flush).

Worse than A by ~20%. Reason: the kernel performs many small memcpys
(one per `IoSlice`) rather than one big memcpy. The *userland* saving of
skipping the `extend_from_slice` is smaller than the *kernel-side* cost
of scatter-gather for small regions.

**Verdict**: for frames ≤ ~4 KB, hypothesis A wins. For very large frames
(≥ 64 KB) hypothesis B becomes competitive or wins because per-region
kernel memcpy cost amortizes.

### 5.4 Hypothesis C — Hybrid with size threshold

```rust
pub struct EgressBatchHybrid {
    /// Small frames live inline in `ring` (copied once at push time).
    ring: Vec<u8>,
    ring_offsets: Vec<usize>,
    /// Large frames are kept as `Bytes` references (no copy).
    large_refs: Vec<Bytes>,
    /// Flush order: interleave by arrival using an enum tag.
    order: Vec<Slot>,
}

enum Slot {
    Inline { off: usize, len: usize },
    Large  { idx: usize },
}

const INLINE_THRESHOLD: usize = 4 * 1024;
```

Push: `if frame.len() <= INLINE_THRESHOLD { ring.extend_from_slice(...) }
else { large_refs.push(frame) }`.

Flush: walk `order`, build `Vec<IoSlice>` where inline slots reference
`&ring[off..off+len]` and large slots reference `&large_refs[idx]`.
Single `write_vectored`.

- Throughput: expected ~9 GB/s for mixed workloads.
- Copies per frame: **0** for large, **1** for small.
- Complexity: high (slot ordering, dual storage).

**Verdict**: worth implementing only if profile shows a bimodal payload
size distribution with a significant fraction > 4 KB.

### 5.5 Hypothesis D — SPSC ring per drain writer

For the shard → drain handoff specifically, a single-producer
single-consumer ring avoids the `Arc` overhead of `Bytes` and the lock
overhead of `Mutex<Vec>`:

```rust
/// SPSC byte ring. The shard (producer) writes frames contiguously into
/// the buffer via `reserve(len) -> &mut [u8]` + header mutation. The
/// drain writer (consumer) reads `ready_slice() -> &[u8]` and calls
/// `write_all` on it. Indices are atomics; no allocation per frame,
/// no memcpy of the header-sized prefix.
pub struct SpscFrameRing {
    buf: Box<[u8]>,
    head: AtomicUsize,    // consumer
    tail: AtomicUsize,    // producer
    mask: usize,          // len - 1, requires power-of-two capacity
}

impl SpscFrameRing {
    /// Producer: reserve `len` contiguous bytes. Returns a writable slice
    /// OR None if the ring is full (back-pressure signal).
    pub fn reserve(&self, len: usize) -> Option<&mut [u8]> { /* ... */ }

    /// Producer: commit the reservation. Advances `tail`.
    pub fn commit(&self, len: usize) { /* ... */ }

    /// Consumer: read the contiguous ready region.
    pub fn ready_slice(&self) -> &[u8] { /* ... */ }

    /// Consumer: advance `head` after write.
    pub fn release(&self, len: usize) { /* ... */ }
}
```

Producer usage (in the shard):

```rust
// Instead of: ingress_buf -> copy into central Vec
// Do: reserve directly in the ring, write the frame in place
let slot = ring.reserve(frame_len).ok_or(BackPressure)?;
let frame = Frame::mut_from_bytes(slot).unwrap();
frame.header.total_len = U32::new(frame_len as u32);
frame.header.msg_type  = 0x01;
// ... fill rest
frame.tail[..subject.len()].copy_from_slice(subject);
frame.tail[subject.len()..].copy_from_slice(payload);
ring.commit(frame_len);
```

Consumer usage (in the drain writer):

```rust
let bytes = ring.ready_slice();
if !bytes.is_empty() {
    sock.write_all(bytes).await?;
    ring.release(bytes.len());
}
```

- Throughput: expected **~10 GB/s** (matches hypothesis A but without
  the intermediate Vec push).
- Copies per frame in userland: **1** (subject + payload copied directly
  into the ring slot during `reserve`). If the subject/payload already
  live in zerocopy buffers upstream, this is still one memcpy. If the
  shard builds them from scratch, there's no avoidable copy anyway.
- Complexity: medium (SPSC atomics, wrap-around handling, back-pressure).

**Verdict**: this is the structurally cleanest approach. The ring *is*
the egress buffer; there is no separate "batch then write" step because
the ring continuously accumulates and the writer continuously drains.
The stream *is* the one that copies, in the literal sense: the kernel
reads bytes from the ring during `write()` and no intermediate staging
ever happens.

### 5.6 Recommendation

| Workload | Recommended | Why |
|---|---|---|
| Small frames (≤4 KB), single shard | A (`EgressBatch`) | Simplest, 10 GB/s |
| Small frames, multi-shard → writer | D (SPSC ring) | Avoids handoff copy |
| Mixed size distribution | C (hybrid) | Retains large-payload zero-copy |
| Very large frames (≥64 KB) | B (`Bytes` list + writev) | Per-region cost amortizes |

For arbitro specifically (broker with small-to-medium messages, shard
→ drain writer fan-in), **Hypothesis D is the target**. It matches the
throughput ceiling of the simpler `A`, scales to arbitrary frame sizes
without branches, eliminates the intermediate `Vec<u8>` allocation per
batch, and models the "stream is the one that copies" principle as
directly as the Linux API allows.

---

## 6. Migration checklist

1. Introduce `FrameHeader` + `Frame` (DST) types in `arbitro-proto` with
   zerocopy derives and `const _: () = assert!` size guards.
2. Replace the `bytes::BufMut` encode site in `shard/drain.rs` with a
   typed `Frame::mut_from_bytes` view on the current buffer. Expect
   **3× encode speedup** (~10 ns → ~3 ns per frame) from this alone.
3. Batch the drain writer to 64–128 frames per `write_all` call. Expect
   **10× throughput** (~0.7 GB/s → ~7-10 GB/s) for small-message
   workloads.
4. Once step 3 is stable and measured, evaluate Hypothesis D for the
   shard → drain handoff. The payoff is marginal vs a well-tuned step 3,
   but it removes one allocation per batch and simplifies the
   back-pressure story.

Each step must be gated behind a reproducible benchmark run and
compared against the baseline captured in `/tmp/bench.log`.

---

## 7. References

- Benches: `crates/arbitro-e2e/benches/{tcp_send,framing,tcp_framed}.rs`
- Zerocopy docs: https://docs.rs/zerocopy/0.8
- Linux `writev(2)`: <https://man7.org/linux/man-pages/man2/writev.2.html>
- Kernel memcpy ceiling on loopback for this host: ~10 GB/s single
  connection, ~22 GB/s with 8 parallel conns on 4 workers.
