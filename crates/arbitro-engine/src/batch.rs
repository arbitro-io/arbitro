//! Batch types — all runtime operations are batch-first.
//!
//! Level 1 — depends on `types`, `error` only.
//!
//! Single message = batch(count=1). One code path, no branching.
//! All batch types use borrowed slices — the engine never takes
//! ownership of the caller's data.

use crate::types::*;
use bytes::Bytes;
use zerocopy::{IntoBytes, FromBytes, Immutable, KnownLayout, TryFromBytes};

// ── Publish ──────────────────────────────────────────────────────────────────

/// A batch of messages to publish to a stream.
pub struct PublishBatch<'a> {
    pub stream_id: StreamId,
    pub entries: &'a [PublishEntry<'a>],
    pub now: Timestamp,
}

/// A single entry in a publish batch.
pub struct PublishEntry<'a> {
    pub subject_hash: u32,
    /// Raw subject bytes for pattern matching. `&[u8]` — never UTF-8 validated on hot path.
    pub subject: &'a [u8],
    pub payload: PayloadRef<'a>,
    pub idempotency_key: u64,
    pub credits_cost: u16,
}

// ── Publish (owned, cross-thread) ────────────────────────────────────────────
//
// `PublishEntry<'a>` borrows. That's the right shape inside the engine thread,
// where the wire buffer is alive and zero copy is the goal. But the message
// has to *get* to the engine thread first — usually from a TCP reader task on
// a different thread. The wire buffer there dies when the parser task ends,
// so something has to own subject + payload while the publish crosses the
// channel.
//
// `PublishEntryOwned` is that something. Both fields are `Bytes`, which:
//   • is `Send + 'static` — can sit in an mpsc command,
//   • is refcount, not memcpy — `clone()` is ~5 ns,
//   • slices without copying — `bytes.slice(range)` shares the same alloc.
//
// At dispatch time the shard worker calls `view_into(&mut scratch)` to build
// a borrowed `PublishBatch<'_>` over the same `Bytes`, with **zero copies of
// payload bytes**. The scratch Vec lives on the worker, reused across calls.
//
// This is the *only* "owned mirror" the engine ships. Every other batch type
// is either `Copy` (claim/ack/nack) or already owned by construction
// (configs). Callers should never invent parallel struct hierarchies — use
// these directly across the channel, then `view_into` on the worker side.

/// Owned counterpart of [`PublishEntry`] — survives crossing a channel.
///
/// Holds subject and payload as [`Bytes`] (refcount, not memcpy). Convert to
/// the borrowed form with [`Self::as_borrowed`] when handing it to the engine.
#[derive(Debug, Clone)]
pub struct PublishEntryOwned {
    pub subject_hash: u32,
    pub subject: Bytes,
    pub payload: Bytes,
    pub idempotency_key: u64,
    pub credits_cost: u16,
}

impl PublishEntryOwned {
    /// Borrowed view — zero-copy. The returned [`PublishEntry`] borrows from
    /// `self`, so it must not outlive this owned entry.
    #[inline]
    pub fn as_borrowed(&self) -> PublishEntry<'_> {
        PublishEntry {
            subject_hash: self.subject_hash,
            subject: &self.subject,
            payload: PayloadRef::Borrowed(&self.payload),
            idempotency_key: self.idempotency_key,
            credits_cost: self.credits_cost,
        }
    }
}

/// Owned counterpart of [`PublishBatch`] — survives crossing a channel.
///
/// `entries` is a plain `Vec` so callers can build it from any iterator
/// without pulling in extra container deps.
#[derive(Debug, Clone)]
pub struct PublishBatchOwned {
    pub stream_id: StreamId,
    pub entries: Vec<PublishEntryOwned>,
    pub now: Timestamp,
}

impl PublishBatchOwned {
    /// Build a borrowed [`PublishBatch`] view into a caller-provided scratch
    /// `Vec`. The scratch is cleared and refilled — keep one per shard worker
    /// and reuse it across calls so steady-state allocates nothing.
    ///
    /// ```ignore
    /// // On the shard worker:
    /// let batch = owned.view_into(&mut self.scratch_publish);
    /// engine.publish(&batch);
    /// // scratch is implicitly reused next call
    /// ```
    #[inline]
    pub fn view_into<'a>(
        &'a self,
        scratch: &'a mut Vec<PublishEntry<'a>>,
    ) -> PublishBatch<'a> {
        scratch.clear();
        scratch.reserve(self.entries.len());
        for e in &self.entries {
            scratch.push(e.as_borrowed());
        }
        PublishBatch {
            stream_id: self.stream_id,
            entries: scratch.as_slice(),
            now: self.now,
        }
    }
}

// ── Claim ────────────────────────────────────────────────────────────────────

/// Request to claim (deliver) messages from a queue.
pub struct ClaimBatch {
    pub queue_id: QueueId,
    pub connection_id: ConnectionId,
    pub consumer_id: ConsumerId,
    pub max_items: u16,
    pub now: Timestamp,
}

/// A single claimed entry returned to the caller.
/// Zero-copy: cast `&[ClaimedEntry]` ↔ `&[u8]` directly.
#[derive(Debug, Clone, Copy, IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct ClaimedEntry {
    pub seq: u64,            // 8
    pub pending_id: PendingId, // 4
    pub subject_hash: u32,  // 4
}
// 16 bytes, no padding.
const _: () = assert!(std::mem::size_of::<ClaimedEntry>() == 16);

// ── Ack ──────────────────────────────────────────────────────────────────────

/// A batch of acknowledgments.
pub struct AckBatch<'a> {
    pub consumer_id: ConsumerId,
    pub entries: &'a [AckEntry],
    pub now: Timestamp,
}

/// A single ack entry.
/// Zero-copy: cast `&[AckEntry]` ↔ `&[u8]` directly.
#[derive(Debug, Clone, Copy, IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct AckEntry {
    pub seq: u64,
}

/// Result of a single ack operation.
/// Zero-copy: cast `&[AckResult]` ↔ `&[u8]` directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq,
         IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum AckResult {
    /// Successfully acknowledged and released.
    Acked = 0,
    /// Pending not found — already acked or timed out.
    NotFound = 1,
}

// ── Nack ─────────────────────────────────────────────────────────────────────

/// A batch of negative acknowledgments (redelivery requests).
pub struct NackBatch<'a> {
    pub consumer_id: ConsumerId,
    pub entries: &'a [NackEntry],
    pub now: Timestamp,
}

/// A single nack entry.
#[derive(Debug, Clone, Copy)]
pub struct NackEntry {
    pub seq: u64,
    /// Optional: schedule retry at this time instead of immediate requeue.
    pub retry_at: Option<Timestamp>,
}

/// Result of a single nack operation.
/// Zero-copy: cast `&[NackResult]` ↔ `&[u8]` directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq,
         IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum NackResult {
    /// Successfully nacked and requeued.
    Requeued = 0,
    /// Pending not found.
    NotFound = 1,
}

// ── Bind ─────────────────────────────────────────────────────────────────────

/// A batch of bind operations (subscription ↔ connection).
pub struct BindBatch<'a> {
    pub entries: &'a [BindEntry],
    pub now: Timestamp,
}

/// A single bind entry. Management path — no zerocopy needed.
#[derive(Debug, Clone, Copy)]
pub struct BindEntry {
    pub connection_id: ConnectionId,
    pub subscription_id: SubscriptionId,
}

// ── Drain ────────────────────────────────────────────────────────────────────

/// Request to drain a connection.
pub struct DrainConnectionReq {
    pub connection_id: ConnectionId,
    pub mode: DrainMode,
    pub now: Timestamp,
}

/// Report from a drain operation.
#[derive(Debug, Clone, Copy, Default)]
pub struct DrainReport {
    pub pending_released: u32,
    pub pending_requeued: u32,
    pub bindings_removed: u32,
}

// ── Open Connection ──────────────────────────────────────────────────────────

/// Request to open a new connection.
pub struct OpenConnectionReq {
    pub connection_id: ConnectionId,
    pub node_id: NodeId,
    pub now: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_batch_borrows() {
        let entries = [PublishEntry {

            subject_hash: 0xDEAD,
            subject: b"orders.created",
            payload: PayloadRef::Borrowed(b"hello"),
            idempotency_key: 0,
            credits_cost: 1,
        }];
        let batch = PublishBatch {
            stream_id: StreamId(10),
            entries: &entries,
            now: Timestamp::new(0),
        };
        assert_eq!(batch.entries.len(), 1);
        assert_eq!(batch.entries[0].payload.as_bytes(), b"hello");
    }

    #[test]
    fn ack_batch_borrows() {
        let entries = [AckEntry { seq: 100 }, AckEntry { seq: 200 }];
        let batch = AckBatch {
            consumer_id: ConsumerId(1),
            entries: &entries,
            now: Timestamp::new(0),
        };
        assert_eq!(batch.entries.len(), 2);
    }

    #[test]
    fn drain_report_default() {
        let r = DrainReport::default();
        assert_eq!(r.pending_released, 0);
        assert_eq!(r.pending_requeued, 0);
        assert_eq!(r.bindings_removed, 0);
    }

    #[test]
    fn publish_owned_as_borrowed_zero_copy() {
        let subject = Bytes::from_static(b"orders.created");
        let payload = Bytes::from_static(b"hello world");
        let owned = PublishEntryOwned {
            subject_hash: 0xCAFE,
            subject: subject.clone(),
            payload: payload.clone(),
            idempotency_key: 42,
            credits_cost: 1,
        };

        let borrowed = owned.as_borrowed();
        assert_eq!(borrowed.subject_hash, 0xCAFE);
        assert_eq!(borrowed.subject, b"orders.created");
        assert_eq!(borrowed.payload.as_bytes(), b"hello world");
        assert_eq!(borrowed.idempotency_key, 42);
        assert_eq!(borrowed.credits_cost, 1);

        // Same backing buffer — zero copy.
        assert_eq!(
            borrowed.subject.as_ptr(),
            owned.subject.as_ptr(),
            "as_borrowed must not copy subject bytes"
        );
        assert_eq!(
            borrowed.payload.as_bytes().as_ptr(),
            owned.payload.as_ptr(),
            "as_borrowed must not copy payload bytes"
        );
    }

    #[test]
    fn publish_batch_owned_view_into_reuses_scratch() {
        let owned = PublishBatchOwned {
            stream_id: StreamId(7),
            entries: vec![
                PublishEntryOwned {
                    subject_hash: 1,
                    subject: Bytes::from_static(b"a.b"),
                    payload: Bytes::from_static(b"p1"),
                    idempotency_key: 0,
                    credits_cost: 1,
                },
                PublishEntryOwned {
                    subject_hash: 2,
                    subject: Bytes::from_static(b"c.d"),
                    payload: Bytes::from_static(b"p2"),
                    idempotency_key: 0,
                    credits_cost: 1,
                },
            ],
            now: Timestamp::new(1_000),
        };

        let mut scratch: Vec<PublishEntry<'_>> = Vec::with_capacity(8);
        let view = owned.view_into(&mut scratch);
        assert_eq!(view.stream_id, StreamId(7));
        assert_eq!(view.entries.len(), 2);
        assert_eq!(view.entries[0].subject, b"a.b");
        assert_eq!(view.entries[1].subject, b"c.d");
        assert_eq!(view.entries[1].payload.as_bytes(), b"p2");
        assert_eq!(view.now, Timestamp::new(1_000));
        // Same backing buffers — view_into is zero-copy.
        assert_eq!(view.entries[0].subject.as_ptr(), owned.entries[0].subject.as_ptr());
        assert_eq!(view.entries[1].payload.as_bytes().as_ptr(), owned.entries[1].payload.as_ptr());
    }

    #[test]
    fn publish_owned_clone_is_refcount() {
        let subject = Bytes::from_static(b"orders.x");
        let payload = Bytes::from_static(b"payload-bytes");
        let a = PublishEntryOwned {
            subject_hash: 9,
            subject: subject.clone(),
            payload: payload.clone(),
            idempotency_key: 0,
            credits_cost: 1,
        };
        let b = a.clone();

        // Both clones point at the same backing allocation.
        assert_eq!(a.subject.as_ptr(), b.subject.as_ptr());
        assert_eq!(a.payload.as_ptr(), b.payload.as_ptr());
    }
}
