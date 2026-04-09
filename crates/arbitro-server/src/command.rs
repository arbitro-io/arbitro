//! Shard commands — owned types that cross the mpsc channel boundary.
//!
//! Rule: engine types travel as-is (Vec<FanoutEntry>, Vec<ClaimedEntry>).
//! Never define owned mirror types. Only own data that must cross the channel.

use arbitro_engine_v2::batch::{AckEntry, ClaimedEntry, DrainReport, NackEntry};
use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine_v2::types::*;

use bytes::Bytes;
use tokio::sync::oneshot;

// ── Shard command enum ──────────────────────────────────────────────────────

/// Commands dispatched to a shard worker via mpsc channel.
pub enum ShardCommand {
    // Hot path
    Publish(PublishCmd),
    Claim(ClaimCmd),
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

    // Bind
    Bind(BindCmd),

    // Admin
    PauseConsumer(PauseConsumerCmd),
    ResumeConsumer(ResumeConsumerCmd),

    // Delivery
    DrainDeliver,

    // Query
    ListStreams(ListStreamsCmd),
    ListConsumers(ListConsumersCmd),
    StoreInfo(StoreInfoCmd),

    // Recovery
    SeedStores(SeedStoresCmd),

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

/// Claim messages from a queue.
pub struct ClaimCmd {
    pub queue_id: QueueId,
    pub connection_id: ConnectionId,
    pub consumer_id: ConsumerId,
    pub max_items: u16,
    pub now: Timestamp,
    pub reply: oneshot::Sender<Vec<ClaimedEntry>>,
}

/// Acknowledge messages. Engine AckEntry directly — no conversion needed.
pub struct AckCmd {
    pub consumer_id: ConsumerId,
    pub entries: Vec<AckEntry>,
    pub now: Timestamp,
    pub reply: oneshot::Sender<AckReply>,
}

/// Ack reply — zero alloc, inline u32s.
pub struct AckReply {
    pub accepted: u32,
    pub rejected: u32,
}

/// Negative acknowledge (requeue). Engine NackEntry directly.
pub struct NackCmd {
    pub consumer_id: ConsumerId,
    pub entries: Vec<NackEntry>,
    pub now: Timestamp,
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
    pub now: Timestamp,
    pub reply: oneshot::Sender<bool>,
}

/// Unsubscribe: drain subscription.
pub struct UnsubscribeCmd {
    pub subscription_id: SubscriptionId,
    pub mode: DrainMode,
    pub now: Timestamp,
    pub reply: oneshot::Sender<DrainReport>,
}

// ── Stream management ───────────────────────────────────────────────────────

pub struct CreateStreamCmd {
    pub config: StreamConfig,
    pub journal_kind: u8,
    pub reply: oneshot::Sender<bool>,
}

pub struct DeleteStreamCmd {
    pub stream_id: StreamId,
    pub mode: DrainMode,
    /// When true, delete on-disk store data (segment files).
    /// False during recovery replay — the store reflects the final state.
    pub purge_disk: bool,
    pub reply: oneshot::Sender<DrainReport>,
}

// ── Consumer management ─────────────────────────────────────────────────────

pub struct CreateConsumerCmd {
    pub config: ConsumerConfig,
    /// Per-subject inflight limits: (pattern, limit). Applied after consumer creation.
    pub max_subject_inflights: Vec<(Vec<u8>, u32)>,
    pub reply: oneshot::Sender<bool>,
}

pub struct DeleteConsumerCmd {
    pub consumer_id: ConsumerId,
    pub mode: DrainMode,
    pub reply: oneshot::Sender<DrainReport>,
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

pub struct StoreInfoCmd {
    pub stream_id: StreamId,
    pub reply: oneshot::Sender<StoreInfoReply>,
}

pub struct StoreInfoReply {
    pub messages: u64,
    pub bytes: u64,
}

/// Each entry is (consumer_id, stream_id, queue_id, paused).
pub struct ListConsumersReply {
    pub consumers: Vec<(u32, u32, u32, bool)>,
}

// ── Connection lifecycle ────────────────────────────────────────────────────

pub struct OpenConnectionCmd {
    pub connection_id: ConnectionId,
    pub node_id: NodeId,
    pub now: Timestamp,
    pub reply: oneshot::Sender<()>,
}

pub struct DrainConnectionCmd {
    pub connection_id: ConnectionId,
    pub mode: DrainMode,
    pub now: Timestamp,
    pub reply: oneshot::Sender<DrainReport>,
}

// ── Bind ────────────────────────────────────────────────────────────────────

pub struct BindCmd {
    pub connection_id: ConnectionId,
    pub subscription_id: SubscriptionId,
    pub now: Timestamp,
    pub reply: oneshot::Sender<()>,
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

// ── Recovery ───────────────────────────────────────────────────────────────────

/// Seed engine queues from stores that have existing messages (recovery).
pub struct SeedStoresCmd {
    pub reply: oneshot::Sender<u64>,
}
