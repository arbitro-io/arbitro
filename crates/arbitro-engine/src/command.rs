//! Kernel command vocabulary — the internal dispatch type that the shard
//! drainer and inbound translator hand to `engine.execute`.
//!
//! Level 1. Depends only on `types`, `bytes`.
//!
//! `Command` is **NOT** a wire type. It never crosses the network — the
//! wire format is still the existing `arbitro-proto` envelopes. `Command`
//! is the single vocabulary `execute` understands, and it is designed to
//! carry zero-copy references (`&[u8]` for subject, `Bytes` clone for
//! payload) so that constructing one is a few pointer copies and an Arc
//! bump per entry.
//!
//! Variants map 1:1 to the kernel model in the migration plan:
//! - `Fanout`   — one command per connection: all entries fan out to all
//!                consumers sharing that conn.
//! - `Queue`    — one command per winning consumer in a queue group.
//! - `Ack`/`Nack` — batches of (stream, seq) from a consumer.
//! - `RepOk`    — publish acknowledgement back to a producer.
//! - `Tombstone` — inline drop notification (expired / tombstoned / no subs).

use bytes::Bytes;

use crate::types::{ConnectionId, ConsumerId, QueueId, StreamId, SubscriptionId};

/// Borrowed-or-Arc reference to a single stored message.
///
/// Zero-copy by construction:
/// - `subject` borrows from the store buffer for the lifetime of the
///   command (the drainer holds the store window alive).
/// - `payload` is an Arc-backed `Bytes`; cloning is an Arc bump (~3ns).
#[derive(Debug, Clone)]
pub struct MsgRef<'a> {
    /// Store-assigned sequence number for this entry.
    pub seq: u64,
    /// FNV-1a hash of `subject` — precomputed by the store/router.
    pub subject_hash: u32,
    /// Subject bytes borrowed from the store window.
    pub subject: &'a [u8],
    /// Payload — Arc-backed, cheap to clone.
    pub payload: Bytes,
}

/// Reason a `Tombstone` command was emitted.
///
/// Encoded as `#[repr(u8)]` so future wire adapters can cast it directly.
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

/// Pair of (stream, seq) used by Ack/Nack/RepOk batches.
#[derive(Debug, Clone, Copy)]
pub struct StreamSeq {
    /// Stream this entry belongs to.
    pub stream_id: StreamId,
    /// Sequence number inside that stream.
    pub seq: u64,
}

/// Kernel command — the single vocabulary `ArbitroEngine::execute` accepts.
///
/// All variants borrow from scratch buffers owned by the caller (drainer
/// or inbound translator). The engine neither retains nor reallocates
/// these references.
#[derive(Debug)]
pub enum Command<'a> {
    /// Deliver every entry to every consumer sharing one connection.
    ///
    /// The sender expands this to exactly one `FanoutBatch` frame +
    /// one `try_send` on the target connection's mpsc.
    Fanout {
        /// Stream the entries belong to.
        stream_id: StreamId,
        /// Target connection — one command per conn.
        connection_id: ConnectionId,
        /// Consumers on this connection that should receive the batch.
        consumers: &'a [ConsumerId],
        /// Messages to deliver — shared across all `consumers`.
        entries: &'a [MsgRef<'a>],
    },

    /// Deliver one entry to one specific consumer inside a queue group.
    ///
    /// Queue fan-out is resolved by the drainer (`pick_queue_winner`)
    /// before this command is built, so the sender has no routing work.
    Queue {
        /// Stream the entry belongs to.
        stream_id: StreamId,
        /// Queue group this delivery was selected from.
        queue_id: QueueId,
        /// Winning consumer.
        consumer_id: ConsumerId,
        /// Subscription on the winner — needed for client-side demux.
        subscription_id: SubscriptionId,
        /// Connection to dispatch to.
        connection_id: ConnectionId,
        /// The single entry being delivered.
        entry: MsgRef<'a>,
    },

    /// Positive acknowledgement — release pendings matching (stream, seq).
    Ack {
        /// (stream, seq) pairs being acked.
        entries: &'a [StreamSeq],
    },

    /// Negative acknowledgement — requeue or retry per drain policy.
    Nack {
        /// (stream, seq) pairs being nacked.
        entries: &'a [StreamSeq],
    },

    /// Publish acknowledgement heading back to the producer.
    RepOk {
        /// Producer connection to reply on.
        connection_id: ConnectionId,
        /// Envelope sequence from the original publish frame — echoed.
        env_seq: u32,
        /// (stream, seq) assigned by the store for each accepted entry.
        entries: &'a [StreamSeq],
    },

    /// Emit a drop notification — the entry won't be delivered.
    Tombstone {
        /// Stream the dropped entry lives in.
        stream_id: StreamId,
        /// Sequence of the dropped entry.
        seq: u64,
        /// Why it was dropped.
        reason: DropReason,
    },
}
