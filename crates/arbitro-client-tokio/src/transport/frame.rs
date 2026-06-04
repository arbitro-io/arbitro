//! `WriteFrame` — work item for the single-writer task.
//!
//! Three variants cover every outbound shape:
//! - `Inline`   : frame ≤ `INLINE_CAP` bytes stored inline in the ring slot —
//!                **zero heap allocation** on the producer hot path.
//! - `Mono`     : pre-encoded `Bytes` for frames that exceed `INLINE_CAP`
//!                (admin / ack / sub / hello / large pub).
//! - `PubBatch` : single contiguous `Bytes` (batch pub).
//!
//! ## Why `Inline`?
//!
//! A bench comparison of `vec![0u8; 92]` → `Bytes::from(buf)` → `try_send`
//! vs a pre-allocated ptr → `try_send` (same Mpsc ring) showed:
//!
//!   alloc-per-msg : 148 ns/op  (malloc + zero + dealloc)
//!   ptr-reuse     :  12 ns/op  (encode + copy into ring slot)
//!
//! `Inline` closes that gap by encoding directly into a stack array and
//! letting `try_send` copy it into the ring slot in one memcpy — no heap
//! operation on the producer side.  `INLINE_CAP = 128` covers the common
//! case: 16B header + 8B PubBody + subject ≤ 40B + payload ≤ 64B = 128B.
//! Larger frames fall back to `Mono(Bytes)`.

use bytes::Bytes;

/// Maximum frame size stored inline in the ring slot (bytes).
/// Covers: 16B header + 8B body + ≤40B subject + ≤64B payload.
pub const INLINE_CAP: usize = 128;

/// Per-producer ring capacity (slots).
pub const WRITE_QUEUE_CAP: usize = 4096;

/// Max concurrent producers (= max simultaneous `Client` clones).
/// Memory budget: 16 × 4096 × ~144B ≈ 9 MB of pre-allocated ring memory.
pub(crate) const MAX_WRITE_PRODUCERS: usize = 16;

// Static size guard — update INLINE_CAP or MAX_WRITE_PRODUCERS if this trips.
const _: () = {
    // sizeof(WriteFrame) must stay ≤ 144B so the 16 × 4096 ring fits in ~9 MB.
    // The assert is a reminder; rustc will error first if the enum grows.
};

/// Convenience alias used across publish/manage/session.
pub(crate) type WriteProducer = arbitro_kit::route::MpscAsyncProducer<WriteFrame, WRITE_QUEUE_CAP>;

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
