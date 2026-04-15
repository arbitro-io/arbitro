//! Reply frames — batch-shaped result types for every operation.
//!
//! Level 1 — depends on `types`, `error` only.
//!
//! Hot path replies use pre-allocated scratch buffers via `ScratchReply<T>`.
//! The buffer is owned by EngineContext and recycled with `.clear()` —
//! zero heap allocation on steady-state.
//!
//! Cold path replies (bind, admin) use `RepOk<T>` with a fresh Vec.

use crate::error::ErrorCode;
use zerocopy::{IntoBytes, FromBytes, Immutable, KnownLayout, TryFromBytes};

// ── Operation Kind ───────────────────────────────────────────────────────────

/// Identifies the operation type in a reply frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq,
         IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum OperationKind {
    Publish = 0,
    Claim = 1,
    Ack = 2,
    Nack = 3,
    Bind = 4,
    Drain = 5,
    OpenConnection = 6,
}

// ── ScratchReply (hot path — zero alloc) ────────────────────────────────────

/// Pre-allocated reply buffer for hot path operations.
///
/// Owns a `Vec<T>` that is **never dropped** — the engine recycles it
/// via `reset()` + `clear()`, preserving capacity across calls.
/// Steady-state: zero heap allocations.
pub struct ScratchReply<T> {
    pub op: OperationKind,
    pub accepted: u32,
    pub rejected: u32,
    buf: Vec<T>,
}

impl<T> ScratchReply<T> {
    /// Create with initial capacity. Called once at engine init.
    pub fn new(op: OperationKind, capacity: usize) -> Self {
        Self {
            op,
            accepted: 0,
            rejected: 0,
            buf: Vec::with_capacity(capacity),
        }
    }

    /// Reset counters and clear buffer (keeps capacity).
    #[inline]
    pub fn reset(&mut self) {
        self.accepted = 0;
        self.rejected = 0;
        self.buf.clear();
    }

    /// Push an accepted entry into the scratch buffer.
    #[inline]
    pub fn accept(&mut self, entry: T) {
        self.accepted += 1;
        self.buf.push(entry);
    }

    /// Count a rejected entry (no data stored).
    #[inline]
    pub fn reject(&mut self) {
        self.rejected += 1;
    }

    /// Read the result entries as a slice. Zero-copy.
    #[inline]
    pub fn entries(&self) -> &[T] {
        &self.buf
    }

    /// Number of entries in the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn total(&self) -> u32 {
        self.accepted + self.rejected
    }
}

impl<T: IntoBytes + Immutable> ScratchReply<T> {
    /// View entries as raw bytes. Zero-copy cast, no allocation.
    /// `&[T]` → `&[u8]` directly.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        zerocopy::IntoBytes::as_bytes(self.entries())
    }
}

// ── RepOk (cold path — management operations) ──────────────────────────────

/// Batch reply for management/cold-path operations (bind, admin).
///
/// Allocates a Vec per call. Acceptable for operations that run
/// once per session, not per message.
#[derive(Debug)]
pub struct RepOk<T> {
    pub op: OperationKind,
    pub accepted: u32,
    pub rejected: u32,
    pub entries: Vec<T>,
}

impl<T> RepOk<T> {
    pub fn new(op: OperationKind) -> Self {
        Self {
            op,
            accepted: 0,
            rejected: 0,
            entries: Vec::new(),
        }
    }

    /// Create with pre-allocated capacity.
    pub fn with_capacity(op: OperationKind, cap: usize) -> Self {
        Self {
            op,
            accepted: 0,
            rejected: 0,
            entries: Vec::with_capacity(cap),
        }
    }

    #[inline]
    pub fn accept(&mut self, entry: T) {
        self.accepted += 1;
        self.entries.push(entry);
    }

    #[inline]
    pub fn reject(&mut self) {
        self.rejected += 1;
    }

    pub fn total(&self) -> u32 {
        self.accepted + self.rejected
    }
}

// ── RepError ─────────────────────────────────────────────────────────────────

/// Error reply for a failed operation.
#[derive(Debug)]
pub struct RepError {
    pub op: OperationKind,
    pub code: ErrorCode,
    pub message: String,
    pub failed_indexes: Vec<u32>,
}

impl RepError {
    pub fn new(op: OperationKind, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            op,
            code,
            message: message.into(),
            failed_indexes: Vec::new(),
        }
    }

    pub fn with_indexes(mut self, indexes: Vec<u32>) -> Self {
        self.failed_indexes = indexes;
        self
    }
}

// ── RepPublish ───────────────────────────────────────────────────────────────

/// Publish result — stats only, no delivery data.
///
/// Fanout is fire-and-forget: notifications live in ctx.fanout queue,
/// NOT in the reply. The protocol layer drains the fanout queue
/// independently — publish never blocks on delivery.
/// Publish result — stats only, no delivery data.
/// Zero-copy: cast `&RepPublish` ↔ `&[u8]` directly. 20 bytes.
#[derive(Debug, Clone, Copy, IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct RepPublish {
    /// How many entries the publisher sent.
    pub source_entries: u32,
    /// Entries skipped by idempotency dedup.
    pub duplicates_skipped: u32,
    /// Consumers notified via fanout queue (fire-and-forget).
    pub notified: u32,
    /// Consumers without binding — ready queue only (pull model).
    pub queued: u32,
}
const _: () = assert!(std::mem::size_of::<RepPublish>() == 16);

impl RepPublish {
    #[inline]
    pub fn new(source_entries: u32) -> Self {
        Self {
            source_entries,
            duplicates_skipped: 0,
            notified: 0,
            queued: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_reply_zero_alloc_reuse() {
        let mut scratch = ScratchReply::<u64>::new(OperationKind::Ack, 8);
        scratch.accept(100);
        scratch.accept(200);
        scratch.reject();

        assert_eq!(scratch.accepted, 2);
        assert_eq!(scratch.rejected, 1);
        assert_eq!(scratch.total(), 3);
        assert_eq!(scratch.entries(), &[100, 200]);

        // Reset preserves capacity, zero alloc on next use
        let cap_before = scratch.buf.capacity();
        scratch.reset();
        assert_eq!(scratch.accepted, 0);
        assert_eq!(scratch.entries(), &[]);
        assert_eq!(scratch.buf.capacity(), cap_before);
    }

    #[test]
    fn rep_ok_cold_path() {
        let mut rep = RepOk::<u64>::new(OperationKind::Bind);
        rep.accept(100);
        rep.accept(200);
        rep.reject();

        assert_eq!(rep.accepted, 2);
        assert_eq!(rep.rejected, 1);
        assert_eq!(rep.total(), 3);
        assert_eq!(rep.entries, vec![100, 200]);
    }

    #[test]
    fn rep_publish_stats() {
        let mut rep = RepPublish::new(10);
        rep.duplicates_skipped = 2;
        rep.notified = 5;
        rep.queued = 3;

        assert_eq!(rep.source_entries, 10);
        assert_eq!(rep.duplicates_skipped, 2);
        assert_eq!(rep.notified, 5);
        assert_eq!(rep.queued, 3);
    }

    #[test]
    fn rep_error_construction() {
        let err = RepError::new(OperationKind::Publish, ErrorCode::CreditExhausted, "no credits")
            .with_indexes(vec![2, 5, 7]);

        assert_eq!(err.code, ErrorCode::CreditExhausted);
        assert_eq!(err.failed_indexes, vec![2, 5, 7]);
    }
}
