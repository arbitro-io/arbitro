//! All ID newtypes, Timestamp, PayloadRef, CreditScope.
//!
//! Level 0 — no internal dependencies.
//!
//! All fixed-layout types derive `zerocopy::{IntoBytes, FromBytes}` for
//! zero-copy cast between `&[T]` ↔ `&[u8]`. No serialization, no copies.

use bytes::Bytes;
use zerocopy::{IntoBytes, FromBytes, Immutable, KnownLayout, TryFromBytes};

// ── ID Newtypes ──────────────────────────────────────────────────────────────

macro_rules! id_newtype_u32 {
    ($($(#[$meta:meta])* $name:ident),+ $(,)?) => {
        $(
            $(#[$meta])*
            #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
                     IntoBytes, FromBytes, Immutable, KnownLayout)]
            #[repr(transparent)]
            pub struct $name(pub u32);

            impl $name {
                #[inline]
                pub const fn new(v: u32) -> Self { Self(v) }

                #[inline]
                pub const fn raw(self) -> u32 { self.0 }
            }

            impl From<u32> for $name {
                #[inline]
                fn from(v: u32) -> Self { Self(v) }
            }
        )+
    };
}

id_newtype_u32!(
    /// Unique identifier for a pending (in-flight) message.
    PendingId,
    /// Unique identifier for a consumer.
    ConsumerId,
    /// Unique identifier for a queue.
    QueueId,
    /// Unique identifier for a subscription.
    SubscriptionId,
    /// Unique identifier for a binding (subscription ↔ connection).
    BindingId,
    /// Unique identifier for a stream.
    StreamId,
    /// Unique identifier for a broker node.
    NodeId,
);

/// Unique identifier for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
         IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(transparent)]
pub struct ConnectionId(pub u64);

impl ConnectionId {
    #[inline]
    pub const fn new(v: u64) -> Self { Self(v) }

    #[inline]
    pub const fn raw(self) -> u64 { self.0 }
}

impl From<u64> for ConnectionId {
    #[inline]
    fn from(v: u64) -> Self { Self(v) }
}

// ── Slab Key ─────────────────────────────────────────────────────────────────

/// Generational key into a `TypedSlab`.
///
/// Combines a slot index with a generation counter for ABA protection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash,
         IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct SlabKey {
    pub index: u32,
    pub generation: u32,
}

impl SlabKey {
    #[inline]
    pub const fn new(index: u32, generation: u32) -> Self {
        Self { index, generation }
    }

    /// Sentinel value representing an invalid / uninitialized key.
    pub const DANGLING: Self = Self { index: u32::MAX, generation: 0 };
}

// ── Timestamp ────────────────────────────────────────────────────────────────

/// Monotonic nanosecond timestamp. Passed from caller, never read from clock
/// on the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
         IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(transparent)]
pub struct Timestamp(pub u64);

impl Timestamp {
    #[inline]
    pub const fn new(ns: u64) -> Self { Self(ns) }

    #[inline]
    pub const fn as_ns(self) -> u64 { self.0 }

    #[inline]
    pub const fn as_ms(self) -> u64 { self.0 / 1_000_000 }
}

// ── PayloadRef ───────────────────────────────────────────────────────────────

/// Zero-copy payload container.
///
/// - `Borrowed`: at ingress, borrows from the wire buffer. Zero copy.
/// - `Owned`: after store, Arc-backed Bytes. Clone = 3ns.
pub enum PayloadRef<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

impl<'a> PayloadRef<'a> {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            PayloadRef::Borrowed(b) => b,
            PayloadRef::Owned(b) => b,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            PayloadRef::Borrowed(b) => b.len(),
            PayloadRef::Owned(b) => b.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

// ── CreditScope ──────────────────────────────────────────────────────────────

/// Scope for multi-level credit tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash,
         IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum CreditScope {
    Node = 0,
    Connection = 1,
    Subject = 2,
}

/// A single credit reservation stored inline in PendingNode.
#[derive(Debug, Clone, Copy, PartialEq, Eq,
         IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct CreditEntry {
    pub scope: CreditScope,
    pub _pad: [u8; 3],
    pub counter_idx: u32,
}

/// Maximum number of credit scopes per pending message.
pub const MAX_CREDITS_PER_PENDING: usize = 3;

const _: () = assert!(std::mem::size_of::<CreditEntry>() == 8);

// ── DrainMode ────────────────────────────────────────────────────────────────

/// Policy for handling in-flight messages during drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainMode {
    /// Release resources and requeue messages for redelivery.
    ReleaseAndRequeue,
    /// Release resources and drop messages permanently.
    ReleaseAndDrop,
    /// Release resources and schedule retry at a specific time.
    ReleaseAndRetryScheduled { retry_at: Timestamp },
    /// Release resources and immediately requeue for retry.
    ReleaseAndRetryNow,
}

// ── AckPolicy ────────────────────────────────────────────────────────────────

/// Acknowledgment policy for a consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum AckPolicy {
    /// Fire-and-forget — no ack required.
    None,
    /// Explicit ack required per message (or batch).
    Explicit,
}

// ── DeliverMode ──────────────────────────────────────────────────────────────

/// How messages are distributed among consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum DeliverMode {
    /// Every consumer receives every message.
    Fanout,
    /// Messages are load-balanced across consumers in a queue group.
    Queue,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credit_entry_size() {
        assert_eq!(std::mem::size_of::<CreditEntry>(), 8);
    }

    #[test]
    fn slab_key_dangling() {
        let k = SlabKey::DANGLING;
        assert_eq!(k.index, u32::MAX);
        assert_eq!(k.generation, 0);
    }

    #[test]
    fn id_newtypes_are_distinct() {
        let a = PendingId(1);
        let b = ConsumerId(1);
        // They have the same raw value but are different types —
        // this won't compile if you try `a == b`.
        assert_eq!(a.raw(), b.raw());
    }

    #[test]
    fn payload_ref_borrowed() {
        let data = b"hello";
        let pr = PayloadRef::Borrowed(data);
        assert_eq!(pr.as_bytes(), b"hello");
        assert_eq!(pr.len(), 5);
        assert!(!pr.is_empty());
    }

    #[test]
    fn payload_ref_owned() {
        let data = Bytes::from_static(b"world");
        let pr = PayloadRef::Owned(data);
        assert_eq!(pr.as_bytes(), b"world");
    }

    #[test]
    fn timestamp_conversions() {
        let ts = Timestamp::new(5_000_000_000); // 5 seconds
        assert_eq!(ts.as_ns(), 5_000_000_000);
        assert_eq!(ts.as_ms(), 5_000);
    }
}
