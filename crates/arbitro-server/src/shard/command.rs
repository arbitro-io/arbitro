//! Shard commands — owned types that cross the mpsc channel boundary.
//!
//! Rule: engine types travel as-is. Only own data that must cross the channel.

use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine_v2::types::*;
use arbitro_engine_v2::ConsumerStateSnapshot;

use bytes::Bytes;
use tokio::sync::oneshot;

// Re-export engine AckEntry for use in ack/nack commands.
pub use arbitro_engine_v2::AckEntry;

// ── Shard command enum ──────────────────────────────────────────────────────

/// Commands dispatched to a shard worker via mpsc channel.
///
/// Publish used to be deliberately absent here — it wrote straight to the
/// shared store from the dispatch layer, which is what forced the store to
/// be `Arc<Mutex<_>>`: any connection thread could reach any shard's
/// journal. `Publish` exists now so the store can stop crossing threads at
/// all. What crosses is this message; the journal stays inside its shard.
pub enum ShardCommand {
    // Hot path
    /// Append entries to this shard's journal and wake its drain.
    ///
    /// The reply carries the first assigned sequence, which the publisher
    /// owes its client. It is a `oneshot` rather than a fire-and-forget
    /// because `publish_batch_wait` cannot answer without it.
    Publish(PublishCmd),
    Ack(AckCmd),
    Nack(NackCmd),
    /// Terminate — ack + tombstone (never redeliver to any consumer).
    AckTerm(AckCmd),

    // Subscription management
    Subscribe(SubscribeCmd),
    Unsubscribe(UnsubscribeCmd),

    // Stream management
    CreateStream(CreateStreamCmd),
    DeleteStream(DeleteStreamCmd),

    // Consumer management
    CreateConsumer(CreateConsumerCmd),
    DeleteConsumer(DeleteConsumerCmd),

    // Connection lifecycle
    OpenConnection(OpenConnectionCmd),
    DrainConnection(DrainConnectionCmd),

    // Bind (subscribe a subscription to a connection)
    Bind(BindCmd),

    // Admin
    PauseConsumer(PauseConsumerCmd),
    ResumeConsumer(ResumeConsumerCmd),

    // Stream content management
    PurgeStream(PurgeStreamCmd),
    DrainSubject(DrainSubjectCmd),
    DeleteMessage(DeleteMessageCmd),

    // Query
    ListStreams(ListStreamsCmd),
    ListConsumers(ListConsumersCmd),
    StoreInfo(StoreInfoCmd),
    ConsumerStates(ConsumerStatesCmd),
    ConsumerPending(ConsumerPendingCmd),

    // System
    Shutdown,

    /// Rebuild one stream's dedup tracker by scanning this shard's journal.
    ///
    /// Startup only. It moved here from `recovery` for the same reason
    /// everything else did: the scan reads the journal, and the journal is
    /// only reachable from the shard's own thread.
    RebuildIdempotency(RebuildIdempotencyCmd),

    /// Load the stream_lifecycle.bin sidecar after replay completes.
    /// Patches `created_at_seq` in stream_retention and rebuilds snapshot.
    LoadStreamLifecycle,
}

// ── Hot path commands ───────────────────────────────────────────────────────

/// Owned publish entry — subject and payload cross the channel.
///
/// `flags` and `deliver_at_ms` are carried too, and that is not incidental:
/// without them the routed path would rebuild every entry as a plain
/// immediate publish, silently dropping header flags and turning a delayed
/// message into an instant one. Nothing would error; the message would just
/// arrive at the wrong time, or without its headers.
pub struct PublishEntryOwned {
    pub subject: Bytes,
    pub payload: Bytes,
    pub flags: u8,
    pub deliver_at_ms: u64,
}

impl PublishEntryOwned {
    /// Build an owned entry from a wire view, sharing the underlying frame
    /// buffer via `Bytes::slice_ref` (zero-copy — refcount on the same Arc).
    #[inline]
    pub fn from_wire(view: &arbitro_proto::wire::publish::PublishView<'_>, frame: &Bytes) -> Self {
        Self {
            subject: frame.slice_ref(view.subject()),
            payload: frame.slice_ref(view.payload()),
            flags: 0,
            deliver_at_ms: 0,
        }
    }
}

/// Append to this shard's journal, from a thread that is not the shard's.
///
/// Nothing here borrows the store. The payloads are owned `Bytes`, which
/// for the dispatch path means a copy — see `owned_entries` for why slicing
/// the frame is not safe there. So routing a publish costs the wake, the
/// reply AND the payload copy. That is the price of publishing to a shard
/// this thread does not own, and it is why the local door exists.
pub struct PublishCmd {
    pub stream_id: StreamId,
    pub entries: Vec<PublishEntryOwned>,
    pub now_ms: u64,
    /// Who to answer, and under which request sequence.
    ///
    /// The SHARD sends this reply, not the publisher. That is the whole
    /// reason this field exists instead of a `oneshot`: awaiting the
    /// sequence back would turn a fire-and-forget publish into a lockstep
    /// round trip, and a client publishing in a tight loop fills its
    /// outgoing ring and starts failing with `QueueFull`. Measured, not
    /// theorised — it broke `drop_client_cancels_all_tasks_under_500ms`
    /// deterministically until the await came out.
    /// `None` for an internal append with no client waiting — the delayed
    /// journal republishing a matured entry, for instance. A sentinel
    /// conn_id would have been a silent way to send a reply to connection
    /// zero.
    pub reply_to: Option<(u64, u64)>,
}

/// Rebuild a stream's dedup tracker from the shard's journal at startup.
/// The reply is how many entries were re-recorded.
pub struct RebuildIdempotencyCmd {
    pub stream_id: StreamId,
    pub window_ms: u32,
    pub now_ms: u64,
    pub reply: oneshot::Sender<u64>,
}

/// Acknowledge messages. Uses engine's AckEntry (stream_id + seq).
pub struct AckCmd {
    pub consumer_id: ConsumerId,
    pub conn_id: u64,
    pub entries: Vec<AckEntry>,
    pub reply: oneshot::Sender<AckReply>,
}

/// Ack reply — zero alloc, inline u32s.
pub struct AckReply {
    pub accepted: u32,
    pub rejected: u32,
}

/// Negative acknowledge (requeue). Same entry type as ack.
pub struct NackCmd {
    pub consumer_id: ConsumerId,
    pub conn_id: u64,
    pub entries: Vec<AckEntry>,
    /// Delay in ms before redelivery. 0 = immediate cursor rewind.
    pub delay_ms: u32,
    pub reply: oneshot::Sender<NackReply>,
}

/// Nack reply — zero alloc, inline u32s.
pub struct NackReply {
    pub requeued: u32,
    pub not_found: u32,
}

// ── Subscription management ─────────────────────────────────────────────────

/// Subscribe: ensure stream + consumer + subscription + bind.
pub struct SubscribeCmd {
    pub stream_config: StreamConfig,
    pub consumer_config: ConsumerConfig,
    pub subscription_config: SubscriptionConfig,
    pub connection_id: ConnectionId,
    /// Deliver policy — determines cursor positioning for this consumer.
    pub deliver_policy: u8,
    /// Start sequence for `DeliverPolicy::ByStartSeq`.
    pub start_seq: u64,
    pub reply: oneshot::Sender<bool>,
}

/// Unsubscribe: retire bindings for this subscription.
pub struct UnsubscribeCmd {
    pub subscription_id: SubscriptionId,
    pub reply: oneshot::Sender<bool>,
}

// ── Stream management ───────────────────────────────────────────────────────

pub struct CreateStreamCmd {
    pub config: StreamConfig,
    /// Maximum number of messages to retain per stream (0 = unlimited).
    pub max_msgs: u64,
    /// Maximum total bytes to retain per stream (0 = unlimited).
    pub max_bytes: u64,
    /// Age-based eviction threshold in milliseconds (0 = disabled).
    pub max_age_ms: u64,
    pub reply: oneshot::Sender<bool>,
}

pub struct DeleteStreamCmd {
    pub stream_id: StreamId,
    /// When true, purge on-disk data. False during recovery replay.
    pub purge_disk: bool,
    pub reply: oneshot::Sender<bool>,
}

// ── Consumer management ─────────────────────────────────────────────────────

pub struct CreateConsumerCmd {
    pub config: ConsumerConfig,
    /// Per-subject inflight limits: (pattern, limit). Applied after consumer creation.
    pub max_subject_inflights: Vec<(Vec<u8>, u32)>,
    pub reply: oneshot::Sender<CreateConsumerReply>,
}

/// CreateConsumer reply.
pub struct CreateConsumerReply {
    /// - `0` = consumer already existed (same config — idempotent)
    /// - `1` = newly created
    /// - `2` = consumer already exists with different config (GAP-3)
    pub code: u8,
    /// Shard journal `last_seq` read by the command thread alongside the
    /// create. For a `DeliverPolicy::New` consumer this is its deliver
    /// floor — the dispatch stamps it into the NameRegistry `start_seq`
    /// slot so every subscribe (and the recovery record) carries the
    /// consumer's creation position.
    pub journal_tail: u64,
}

pub struct DeleteConsumerCmd {
    pub consumer_id: ConsumerId,
    pub reply: oneshot::Sender<bool>,
}

// ── Query ──────────────────────────────────────────────────────────────

pub struct ListStreamsCmd {
    pub reply: oneshot::Sender<ListStreamsReply>,
}

/// Each entry is (stream_id_raw, name).
pub struct ListStreamsReply {
    pub streams: Vec<(u32, Vec<u8>)>,
}

pub struct ListConsumersCmd {
    pub reply: oneshot::Sender<ListConsumersReply>,
}

/// Each entry is (consumer_id, stream_id, queue_id, paused).
pub struct ListConsumersReply {
    pub consumers: Vec<(u32, u32, u32, bool)>,
}

pub struct StoreInfoCmd {
    pub stream_id: StreamId,
    pub reply: oneshot::Sender<StoreInfoReply>,
}

pub struct StoreInfoReply {
    pub messages: u64,
    pub bytes: u64,
}

/// Snapshot every consumer's live state on this shard (id, stream, queue,
/// paused, `ack_pending`). One Vec per shard — caller aggregates across
/// shards. Use this for NATS-style `num_ack_pending` reporting.
pub struct ConsumerStatesCmd {
    pub reply: oneshot::Sender<Vec<ConsumerStateSnapshot>>,
}

/// Get the live pending-ack count for a single consumer. Replies with
/// 0 if the consumer doesn't exist on this shard.
pub struct ConsumerPendingCmd {
    pub consumer_id: ConsumerId,
    pub reply: oneshot::Sender<u64>,
}

// ── Connection lifecycle ────────────────────────────────────────────────────

pub struct OpenConnectionCmd {
    pub connection_id: ConnectionId,
    pub node_id: NodeId,
    pub reply: oneshot::Sender<()>,
}

pub struct DrainConnectionCmd {
    pub connection_id: ConnectionId,
    pub reply: oneshot::Sender<()>,
}

// ── Bind ────────────────────────────────────────────────────────────────────

pub struct BindCmd {
    pub connection_id: ConnectionId,
    pub subscription_id: SubscriptionId,
    pub reply: oneshot::Sender<()>,
}

// ── Stream content management ────────────────────────────────────────────────

/// Purge all messages from a stream's store. Stream entity survives.
/// Returns the number of messages deleted.
pub struct PurgeStreamCmd {
    pub stream_id: StreamId,
    pub reply: oneshot::Sender<u64>,
}

/// Delete all messages whose subject matches a pattern. Returns the count.
pub struct DrainSubjectCmd {
    pub stream_id: StreamId,
    pub subject: Vec<u8>,
    pub reply: oneshot::Sender<u64>,
}

/// Tombstone a single message by sequence. Returns true if found.
pub struct DeleteMessageCmd {
    pub seq: u64,
    pub reply: oneshot::Sender<bool>,
}

// ── Admin ───────────────────────────────────────────────────────────────────

pub struct PauseConsumerCmd {
    pub consumer_id: ConsumerId,
    pub reply: oneshot::Sender<bool>,
}

pub struct ResumeConsumerCmd {
    pub consumer_id: ConsumerId,
    pub reply: oneshot::Sender<bool>,
}
