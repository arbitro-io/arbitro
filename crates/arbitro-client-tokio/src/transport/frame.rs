//! `WriteFrame` — work item for the single-writer task — and the pool of
//! producer rings that feed it.
//!
//! Three frame variants cover every outbound shape:
//! - `Inline`   : frame ≤ `INLINE_CAP` bytes stored inline in the ring slot —
//!                **zero heap allocation** on the producer hot path.
//! - `Mono`     : pre-encoded `Bytes` for frames that exceed `INLINE_CAP`
//!                (admin / ack / sub / hello / large pub).
//! - `PubBatch` : single contiguous `Bytes` (batch pub).
//!
//! ## Why `Inline`?
//!
//! A bench comparison of `vec![0u8; 92]` → `Bytes::from(buf)` → `try_send`
//! vs a pre-allocated ptr → `try_send` (same ring) showed:
//!
//!   alloc-per-msg : 148 ns/op  (malloc + zero + dealloc)
//!   ptr-reuse     :  12 ns/op  (encode + copy into ring slot)
//!
//! `Inline` closes that gap by encoding directly into a stack array and
//! letting `try_send` copy it into the ring slot in one memcpy — no heap
//! operation on the producer side.  `INLINE_CAP = 128` covers the common
//! case: 16B header + 8B PubBody + subject ≤ 40B + payload ≤ 64B = 128B.
//! Larger frames fall back to `Mono(Bytes)`.
//!
//! ## Two traffic classes, two depths
//!
//! Everything outbound funnels into one fan-in channel and one writer task,
//! but not everything wants the same ring depth. Publishes are bursty and
//! want a deep ring; the four long-lived control tasks — heartbeat, the ack
//! and nack batchers, session replay — send a frame at a time and would waste
//! a deep one.
//!
//! [`MpscLazy`] makes that a runtime choice: depth is picked per producer at
//! claim time and the slot array is allocated on the first claim, so an
//! unclaimed ring costs its skeleton and nothing else. That is why
//! [`ClientConfig::write_queue_capacity`](crate::ClientConfig::write_queue_capacity)
//! is a real knob now rather than a value `connect` had to reject.

use std::sync::Arc;

use bytes::Bytes;

use arbitro_kit::route::{AcquireError, LazyConsumer, LazyProducer, MpscLazy, MAX_LAZY_PRODUCERS};
use arbitro_kit::NotifyWaiter;

/// Maximum frame size stored inline in the ring slot (bytes).
/// Covers: 16B header + 8B body + ≤40B subject + ≤64B payload.
pub const INLINE_CAP: usize = 128;

/// Default depth of a publish producer's ring, in frames. Overridable per
/// client through [`ClientConfig::write_queue_capacity`](crate::ClientConfig::write_queue_capacity).
pub const WRITE_QUEUE_CAP: usize = 4096;

/// Depth of a control producer's ring, in frames.
///
/// Heartbeat, the ack and nack batchers and session replay each hold a lease
/// for the life of the connection and send one frame at a time. A deep ring
/// would buy them nothing: if the writer task is so far behind that 64 control
/// frames are outstanding, 4096 would only defer the same problem.
pub const CONTROL_QUEUE_CAP: usize = 64;

/// Max concurrent producers — i.e. max simultaneous `Client` clones plus the
/// long-lived control leases.
///
/// Rings are allocated on first claim, so an unused producer slot costs its
/// skeleton (five cache lines) rather than its slots. Raising this is cheap
/// in a way it was not when every ring was preallocated at connect.
pub(crate) const MAX_WRITE_PRODUCERS: usize = MAX_LAZY_PRODUCERS;

/// Receiving end drained by the single writer task.
pub(crate) type WriteConsumer = LazyConsumer<WriteFrame, NotifyWaiter>;

/// A leased producer ring. Returns it to the pool on drop; frames already
/// queued stay there and the writer task drains them.
pub(crate) type WriteLease = LazyProducer<WriteFrame, NotifyWaiter>;

/// Shared pool of write producers leased out to publish/manage callers and to
/// the long-lived control tasks.
///
/// Thin wrapper over [`MpscLazy`] that remembers which depth each traffic
/// class asked for, so call sites keep saying `acquire()` and
/// `acquire_control()` instead of repeating a capacity everywhere.
pub(crate) struct WritePool {
    inner: Arc<MpscLazy<WriteFrame, NotifyWaiter>>,
    publish_depth: usize,
    control_depth: usize,
}

impl WritePool {
    /// Build a pool with room for `producers` rings, publish leases sized at
    /// `publish_depth`. Allocates the skeletons only — no slot storage until
    /// something is claimed.
    ///
    /// `producers` and `publish_depth` are validated by
    /// [`Client::connect`](crate::Client::connect); `publish_depth` must be a
    /// non-zero power of two.
    pub(crate) fn new(producers: usize, publish_depth: usize) -> (Arc<Self>, WriteConsumer) {
        let (inner, consumer) = MpscLazy::<WriteFrame, NotifyWaiter>::new(producers);
        (
            Arc::new(Self {
                inner,
                publish_depth,
                control_depth: CONTROL_QUEUE_CAP,
            }),
            consumer,
        )
    }

    /// Lease a producer for publish traffic.
    ///
    /// The error is kept, not collapsed to an `Option`: `Exhausted` is
    /// transient — every ring is leased right now — while `Closed` is
    /// terminal. A caller that cannot tell them apart retries forever on a
    /// dead channel, or gives up on a live one.
    #[inline]
    pub(crate) fn acquire(&self) -> Result<WriteLease, AcquireError> {
        self.inner.acquire(self.publish_depth)
    }

    /// Lease a producer for one of the long-lived control tasks.
    #[inline]
    pub(crate) fn acquire_control(&self) -> Result<WriteLease, AcquireError> {
        self.inner.acquire(self.control_depth)
    }

    /// Producer slots not currently leased.
    #[inline]
    pub(crate) fn available(&self) -> usize {
        self.inner.available()
    }

    /// Slots actually allocated across every ring claimed at least once —
    /// the number that says what this client is really paying for.
    #[inline]
    pub(crate) fn allocated_slots(&self) -> usize {
        self.inner.allocated_slots()
    }

    /// Shut the channel down: parked producers and the writer task wake and
    /// see a closed channel.
    #[inline]
    pub(crate) fn close(&self) {
        self.inner.close();
    }
}

/// Work item enqueued by producers and drained by the single writer task.
#[derive(Debug)]
pub enum WriteFrame {
    /// Small frame stored inline — no heap allocation on the producer side.
    /// The `u16` is the valid byte count within the fixed-size array.
    Inline([u8; INLINE_CAP], u16),
    /// Pre-encoded heap buffer for frames that exceed `INLINE_CAP`.
    Mono(Bytes),
    /// Batch-pub: single contiguous heap buffer.
    PubBatch(Bytes),
}

impl WriteFrame {
    /// Returns the wire bytes to write, regardless of variant.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            WriteFrame::Inline(data, len) => &data[..*len as usize],
            WriteFrame::Mono(b) | WriteFrame::PubBatch(b) => b.as_ref(),
        }
    }
}
