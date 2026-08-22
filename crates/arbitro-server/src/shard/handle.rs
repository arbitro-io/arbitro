//! ShardHandle — async API wrapping mpsc::Sender + oneshot per command.
//!
//! Each method builds an owned command, sends it to the shard's channel,
//! and awaits the oneshot reply. Backpressure if channel is full.

use std::fmt;
use std::sync::Arc;

use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine_v2::types::*;
use arbitro_engine_v2::{ConsumerStateSnapshot, EngineMetrics, MetricsSnapshot};
use arbitro_store::EntryRef;
use tokio::sync::{mpsc, oneshot};

use crate::sink::StreamSink;
use crate::common::reply_v2::send_rep_ok_v2;
use crate::common::Gate;
use crate::shard::command::*;
use crate::transport::ConnectionRegistry;

/// Async handle to a shard worker.
#[derive(Clone)]
pub struct ShardHandle {
    shard_id: u32,
    tx: mpsc::Sender<ShardCommand>,
    /// Shared gate — publish notifies drain after store append.
    gate: Arc<Gate>,
    /// Connection registry — publish replies directly to the client.
    registry: ConnectionRegistry,
    /// Shared metrics — read directly via atomic loads (F9), no shard round-trip.
    metrics: Arc<EngineMetrics>,
}

impl ShardHandle {
    pub fn new(
        shard_id: u32,
        tx: mpsc::Sender<ShardCommand>,
        gate: Arc<Gate>,
        registry: ConnectionRegistry,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        Self {
            shard_id,
            tx,
            gate,
            registry,
            metrics,
        }
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    // ── Hot path ────────────────────────────────────────────────────────

    /// Append through the shard's own worker instead of reaching into its
    /// store from here.
    ///
    /// This is the routed path: the caller's thread never touches the
    /// journal, so the journal does not have to be shareable. The entries
    /// are `Bytes` slices of the original frame, so what crosses the
    /// channel is a refcount, not the payload.
    ///
    /// Returns the first assigned sequence. `None` means the shard refused
    /// the append (quota) — distinct from `Err`, which means the shard is
    /// gone.
    /// Returns as soon as the command is queued. The shard answers the
    /// client directly — see `PublishCmd::conn_id` for why waiting here
    /// would be a throughput bug rather than a nicety.
    pub async fn publish_routed(
        &self,
        stream_id: StreamId,
        entries: Vec<PublishEntryOwned>,
        now_ms: u64,
        reply_to: Option<(u64, u64)>,
    ) -> Result<(), SendError> {
        self.send(ShardCommand::Publish(crate::shard::command::PublishCmd {
            stream_id,
            entries,
            now_ms,
            reply_to,
        }))
        .await
    }

    /// Startup: ask this shard to rebuild a stream's dedup tracker from its
    /// own journal. Returns how many entries were re-recorded.
    pub async fn rebuild_idempotency(
        &self,
        stream_id: StreamId,
        window_ms: u32,
        now_ms: u64,
    ) -> Result<u64, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::RebuildIdempotency(RebuildIdempotencyCmd {
            stream_id,
            window_ms,
            now_ms,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn ack(
        &self,
        consumer_id: ConsumerId,
        conn_id: u64,
        entries: Vec<AckEntry>,
    ) -> Result<AckReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Ack(AckCmd {
            consumer_id,
            conn_id,
            entries,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    /// Ack + tombstone — permanently kill the message for all consumers.
    pub async fn ack_term(
        &self,
        consumer_id: ConsumerId,
        conn_id: u64,
        entries: Vec<AckEntry>,
    ) -> Result<AckReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::AckTerm(AckCmd {
            consumer_id,
            conn_id,
            entries,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn nack(
        &self,
        consumer_id: ConsumerId,
        conn_id: u64,
        entries: Vec<AckEntry>,
        delay_ms: u32,
    ) -> Result<NackReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Nack(NackCmd {
            consumer_id,
            conn_id,
            entries,
            delay_ms,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Subscription management ─────────────────────────────────────────

    pub async fn subscribe(
        &self,
        stream_config: StreamConfig,
        consumer_config: ConsumerConfig,
        subscription_config: SubscriptionConfig,
        connection_id: ConnectionId,
        deliver_policy: u8,
        start_seq: u64,
    ) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Subscribe(SubscribeCmd {
            stream_config,
            consumer_config,
            subscription_config,
            connection_id,
            deliver_policy,
            start_seq,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn unsubscribe(&self, subscription_id: SubscriptionId) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Unsubscribe(UnsubscribeCmd {
            subscription_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Stream management ───────────────────────────────────────────────

    pub async fn create_stream(
        &self,
        config: StreamConfig,
        max_msgs: u64,
        max_bytes: u64,
        max_age_ms: u64,
    ) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::CreateStream(CreateStreamCmd {
            config,
            max_msgs,
            max_bytes,
            max_age_ms,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    /// Purge all messages from a stream's store. Returns the deleted count.
    pub async fn purge_stream(&self, stream_id: StreamId) -> Result<u64, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::PurgeStream(PurgeStreamCmd {
            stream_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    /// Drain all messages matching `subject` from a stream's store.
    /// Returns the deleted count.
    pub async fn drain_subject(
        &self,
        stream_id: StreamId,
        subject: Vec<u8>,
    ) -> Result<u64, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::DrainSubject(DrainSubjectCmd {
            stream_id,
            subject,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    /// Tombstone a single message by sequence. Returns true if found.
    pub async fn delete_message(&self, seq: u64) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::DeleteMessage(DeleteMessageCmd {
            seq,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn delete_stream(
        &self,
        stream_id: StreamId,
        purge_disk: bool,
    ) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::DeleteStream(DeleteStreamCmd {
            stream_id,
            purge_disk,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Consumer management ─────────────────────────────────────────────

    /// Create or ensure a consumer. See [`CreateConsumerReply`] for the
    /// reply codes and the journal tail carried alongside them.
    pub async fn create_consumer(
        &self,
        config: ConsumerConfig,
        max_subject_inflights: Vec<(Vec<u8>, u32)>,
    ) -> Result<CreateConsumerReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::CreateConsumer(CreateConsumerCmd {
            config,
            max_subject_inflights,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn delete_consumer(&self, consumer_id: ConsumerId) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::DeleteConsumer(DeleteConsumerCmd {
            consumer_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Connection lifecycle ────────────────────────────────────────────

    pub async fn open_connection(
        &self,
        connection_id: ConnectionId,
        node_id: NodeId,
    ) -> Result<(), SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::OpenConnection(OpenConnectionCmd {
            connection_id,
            node_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn drain_connection(&self, connection_id: ConnectionId) -> Result<(), SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::DrainConnection(DrainConnectionCmd {
            connection_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Bind ────────────────────────────────────────────────────────────

    pub async fn bind(
        &self,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> Result<(), SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Bind(BindCmd {
            connection_id,
            subscription_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Query ───────────────────────────────────────────────────────────

    pub async fn list_streams(&self) -> Result<ListStreamsReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::ListStreams(ListStreamsCmd { reply: tx }))
            .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn list_consumers(&self) -> Result<ListConsumersReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::ListConsumers(ListConsumersCmd { reply: tx }))
            .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn store_info(&self, stream_id: StreamId) -> Result<StoreInfoReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::StoreInfo(StoreInfoCmd {
            stream_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    /// Snapshot this shard's engine metrics. Sync — reads Arc<EngineMetrics>
    /// directly via Relaxed loads, no shard command round-trip (F9).
    #[inline]
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Snapshot per-consumer live state (pending ACKs, paused flag, etc.).
    /// One round-trip per shard — operators aggregate across shards.
    pub async fn consumer_states(&self) -> Result<Vec<ConsumerStateSnapshot>, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::ConsumerStates(ConsumerStatesCmd {
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    /// Get the live pending-ack count for a single consumer. Returns 0 if
    /// the consumer doesn't exist on this shard.
    pub async fn consumer_pending(&self, consumer_id: ConsumerId) -> Result<u64, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::ConsumerPending(ConsumerPendingCmd {
            consumer_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Admin ───────────────────────────────────────────────────────────

    pub async fn pause_consumer(&self, consumer_id: ConsumerId) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::PauseConsumer(PauseConsumerCmd {
            consumer_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn resume_consumer(&self, consumer_id: ConsumerId) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::ResumeConsumer(ResumeConsumerCmd {
            consumer_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Internal ────────────────────────────────────────────────────────

    pub async fn send(&self, cmd: ShardCommand) -> Result<(), SendError> {
        crate::lifecycle_trace!("07_handle_send_enter", 0, 0, "frame_loop");
        self.tx.send(cmd).await.map_err(|_| SendError::SHARD_DOWN)?;
        crate::lifecycle_trace!("08_handle_send_done", 0, 0, "frame_loop");
        Ok(())
    }

    /// Signal the shard to load the stream_lifecycle sidecar after replay.
    pub async fn load_stream_lifecycle(&self) -> Result<(), SendError> {
        self.send(ShardCommand::LoadStreamLifecycle).await
    }

    pub fn send_shutdown(&self) {
        let _ = self.tx.try_send(ShardCommand::Shutdown);
        self.gate.release();
    }
}

/// Error when the shard worker has exited.
#[derive(Debug)]
pub struct SendError;

impl SendError {
    pub const SHARD_DOWN: Self = Self;
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "shard worker has exited")
    }
}

impl std::error::Error for SendError {}
