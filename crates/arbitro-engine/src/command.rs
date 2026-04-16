//! Kernel command vocabulary — post-success mutations the worker sends
//! to the engine after I/O succeeds.
//!
//! Level 1. Depends only on `types`.
//!
//! `Command` is NOT a wire type — it never crosses the network. The wire
//! format is `arbitro-proto` envelopes. `Command` is the single vocabulary
//! `ArbitroEngine::execute` accepts, designed to carry zero-copy references.
//!
//! Key difference from the legacy engine: `Delivered` is emitted AFTER
//! `try_send` succeeds in the drain loop. The engine bumps credits/inflight
//! AFTER the send worked — never speculatively.

use crate::types::{BindingId, ConnectionId, ConsumerId, StreamId};

/// Entry recording a successful delivery.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct DeliveredEntry {
    /// Store sequence of the delivered message.
    pub seq: u64,
    /// FNV-1a hash of the subject — for O(1) credit arithmetic on ack.
    pub subject_hash: u32,
    pub _pad: u32,
}
const _: () = assert!(core::mem::size_of::<DeliveredEntry>() == 16);

/// Entry in an ack/nack command. The engine resolves `subject_hash`
/// from the `Pending` found by `seq` — no wire echo needed.
#[derive(Debug, Clone, Copy)]
pub struct AckEntry {
    pub stream_id: StreamId,
    pub seq: u64,
}

/// Reason a `Tombstone` command was emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DropReason {
    /// `timestamp + max_age_ms <= now_ms` at drain time.
    Expired = 0,
    /// Explicit tombstone bit set on the stored entry.
    Tombstoned = 1,
    /// No live subscribers for this subject at drain time.
    NoSubscribers = 2,
}

/// Kernel command — the single vocabulary `ArbitroEngine::execute` accepts.
///
/// All variants borrow from scratch buffers owned by the caller (drainer
/// or inbound translator). The engine neither retains nor reallocates
/// these references.
#[derive(Debug)]
pub enum Command<'a> {
    /// Notify engine that entries were successfully delivered to a binding.
    /// Emitted AFTER `try_send` succeeds in the drain loop.
    Delivered {
        stream_id: StreamId,
        binding_id: BindingId,
        entries: &'a [DeliveredEntry],
    },

    /// Positive acknowledgement from a consumer.
    Ack {
        consumer_id: ConsumerId,
        entries: &'a [AckEntry],
    },

    /// Negative acknowledgement — request redelivery.
    Nack {
        consumer_id: ConsumerId,
        entries: &'a [AckEntry],
    },

    /// Store accepted a publish — seq assigned. Engine tracks metrics.
    PublishAccepted {
        stream_id: StreamId,
        seq: u64,
        connection_id: ConnectionId,
        env_seq: u32,
    },

    /// Entry dropped — expired, tombstoned, or no subscribers.
    Tombstone {
        stream_id: StreamId,
        seq: u64,
        reason: DropReason,
    },
}
