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
/// Publish is NOT here — it goes directly to the store from the
/// dispatch layer, bypassing the shard worker entirely.
pub enum ShardCommand {
    // Hot path
    PublishAccumulate(PublishCmd),
    Ack(AckCmd),
    Nack(NackCmd),

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

    // Query
    ListStreams(ListStreamsCmd),
    ListConsumers(ListConsumersCmd),
    StoreInfo(StoreInfoCmd),
    ConsumerStates(ConsumerStatesCmd),
    ConsumerPending(ConsumerPendingCmd),

    // System
    Shutdown,
}

// ── Hot path commands ───────────────────────────────────────────────────────

/// Publish entries to a stream. Fire & forget — shard replies directly.
pub struct PublishCmd {
    pub stream_id: StreamId,
    pub conn_id: u64,
    pub env_seq: u32,
    pub entries: Vec<PublishEntryOwned>,
}

/// Owned publish entry — subject and payload cross the channel.
pub struct PublishEntryOwned {
    pub subject: Bytes,
    pub payload: Bytes,
}

impl PublishEntryOwned {
    /// Build an owned entry from a wire view, sharing the underlying frame
    /// buffer via `Bytes::slice_ref` (zero-copy — refcount on the same Arc).
    #[inline]
    pub fn from_wire(view: &arbitro_proto::wire::publish::PublishView<'_>, frame: &Bytes) -> Self {
        Self {
            subject: frame.slice_ref(view.subject()),
            payload: frame.slice_ref(view.payload()),
        }
    }
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

/// CreateConsumer reply codes:
/// - `0` = consumer already existed (same config — idempotent)
/// - `1` = newly created
/// - `2` = consumer already exists with different config (GAP-3)
pub struct CreateConsumerCmd {
    pub config: ConsumerConfig,
    /// Per-subject inflight limits: (pattern, limit). Applied after consumer creation.
    pub max_subject_inflights: Vec<(Vec<u8>, u32)>,
    pub reply: oneshot::Sender<u8>,
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

// ── Admin ───────────────────────────────────────────────────────────────────

pub struct PauseConsumerCmd {
    pub consumer_id: ConsumerId,
    pub reply: oneshot::Sender<bool>,
}

pub struct ResumeConsumerCmd {
    pub consumer_id: ConsumerId,
    pub reply: oneshot::Sender<bool>,
}
