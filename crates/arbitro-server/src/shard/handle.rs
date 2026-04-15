//! ShardHandle — async API wrapping mpsc::Sender + oneshot per command.
//!
//! Each method builds an owned command, sends it to the shard's channel,
//! and awaits the oneshot reply. Backpressure if channel is full.

use std::fmt;

use arbitro_engine_v2::batch::{AckEntry, ClaimedEntry, DrainReport, NackEntry};
use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine_v2::types::*;
use tokio::sync::{mpsc, oneshot};

use crate::shard::command::*;

/// Async handle to a shard worker.
#[derive(Clone)]
pub struct ShardHandle {
    shard_id: u32,
    tx: mpsc::Sender<ShardCommand>,
    shard_thread: std::thread::Thread,
}

impl ShardHandle {
    pub fn new(shard_id: u32, tx: mpsc::Sender<ShardCommand>, shard_thread: std::thread::Thread) -> Self {
        Self { shard_id, tx, shard_thread }
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    // ── Hot path ────────────────────────────────────────────────────────

    /// Fire & forget — shard replies directly via ConnectionRegistry.
    pub async fn publish(
        &self,
        stream_id: StreamId,
        conn_id: u64,
        env_seq: u32,
        entries: Vec<PublishEntryOwned>,
    ) -> Result<(), SendError> {
        self.send(ShardCommand::Publish(PublishCmd {
            stream_id,
            conn_id,
            env_seq,
            entries,
        })).await
    }

    /// Fire & forget — shard accumulates entries, flushes with append_batch after
    /// 5ms deadline or 1024-entry threshold, replies directly via ConnectionRegistry.
    pub async fn publish_accumulate(
        &self,
        stream_id: StreamId,
        conn_id: u64,
        env_seq: u32,
        entries: Vec<PublishEntryOwned>,
    ) -> Result<(), SendError> {
        self.send(ShardCommand::PublishAccumulate(PublishCmd {
            stream_id,
            conn_id,
            env_seq,
            entries,
        })).await
    }

    pub async fn ack(
        &self,
        consumer_id: ConsumerId,
        entries: Vec<AckEntry>,
        now: Timestamp,
    ) -> Result<AckReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Ack(AckCmd {
            consumer_id,
            entries,
            now,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    /// Test-only direct claim probe. Production delivery flows through the
    /// drainer (`handle_drain_deliver`) and `RepBatch` frames — this bypass
    /// exists so integration tests can inspect engine state synchronously.
    pub async fn claim(
        &self,
        queue_id: QueueId,
        connection_id: ConnectionId,
        consumer_id: ConsumerId,
        max_items: u16,
        now: Timestamp,
    ) -> Result<Vec<ClaimedEntry>, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Claim(ClaimCmd {
            queue_id,
            connection_id,
            consumer_id,
            max_items,
            now,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn nack(
        &self,
        consumer_id: ConsumerId,
        entries: Vec<NackEntry>,
        now: Timestamp,
    ) -> Result<NackReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Nack(NackCmd {
            consumer_id,
            entries,
            now,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Subscription management ─────────────────────────────────────────

    pub async fn subscribe(
        &self,
        stream_config: StreamConfig,
        consumer_config: ConsumerConfig,
        subscription_config: SubscriptionConfig,
        connection_id: ConnectionId,
        now: Timestamp,
    ) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Subscribe(SubscribeCmd {
            stream_config,
            consumer_config,
            subscription_config,
            connection_id,
            now,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn unsubscribe(
        &self,
        subscription_id: SubscriptionId,
        mode: DrainMode,
        now: Timestamp,
    ) -> Result<DrainReport, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Unsubscribe(UnsubscribeCmd {
            subscription_id,
            mode,
            now,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Stream management ───────────────────────────────────────────────

    pub async fn create_stream(
        &self,
        config: StreamConfig,
        journal_kind: u8,
    ) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::CreateStream(CreateStreamCmd {
            config,
            journal_kind,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn delete_stream(
        &self,
        stream_id: StreamId,
        mode: DrainMode,
        purge_disk: bool,
    ) -> Result<DrainReport, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::DeleteStream(DeleteStreamCmd {
            stream_id,
            mode,
            purge_disk,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Consumer management ─────────────────────────────────────────────

    pub async fn create_consumer(
        &self,
        config: ConsumerConfig,
        max_subject_inflights: Vec<(Vec<u8>, u32)>,
    ) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::CreateConsumer(CreateConsumerCmd {
            config,
            max_subject_inflights,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn delete_consumer(
        &self,
        consumer_id: ConsumerId,
        mode: DrainMode,
    ) -> Result<DrainReport, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::DeleteConsumer(DeleteConsumerCmd {
            consumer_id,
            mode,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Query ───────────────────────────────────────────────────────────

    pub async fn list_streams(&self) -> Result<ListStreamsReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::ListStreams(ListStreamsCmd {
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn list_consumers(&self) -> Result<ListConsumersReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::ListConsumers(ListConsumersCmd {
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn store_info(&self, stream_id: StreamId) -> Result<StoreInfoReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::StoreInfo(StoreInfoCmd {
            stream_id,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Connection lifecycle ────────────────────────────────────────────

    pub async fn open_connection(
        &self,
        connection_id: ConnectionId,
        node_id: NodeId,
        now: Timestamp,
    ) -> Result<(), SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::OpenConnection(OpenConnectionCmd {
            connection_id,
            node_id,
            now,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn drain_connection(
        &self,
        connection_id: ConnectionId,
        mode: DrainMode,
        now: Timestamp,
    ) -> Result<DrainReport, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::DrainConnection(DrainConnectionCmd {
            connection_id,
            mode,
            now,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Bind ────────────────────────────────────────────────────────────

    pub async fn bind(
        &self,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
        now: Timestamp,
    ) -> Result<(), SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Bind(BindCmd {
            connection_id,
            subscription_id,
            now,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Admin ───────────────────────────────────────────────────────────

    pub async fn pause_consumer(&self, consumer_id: ConsumerId) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::PauseConsumer(PauseConsumerCmd {
            consumer_id,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn resume_consumer(&self, consumer_id: ConsumerId) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::ResumeConsumer(ResumeConsumerCmd {
            consumer_id,
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Recovery ────────────────────────────────────────────────────────

    /// Seed engine queues from stores with existing data (recovery path).
    /// Returns total number of messages seeded.
    pub async fn seed_stores(&self) -> Result<u64, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::SeedStores(SeedStoresCmd {
            reply: tx,
        })).await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Internal ────────────────────────────────────────────────────────

    pub async fn send(&self, cmd: ShardCommand) -> Result<(), SendError> {
        crate::lifecycle_trace!("07_handle_send_enter", 0, 0, "frame_loop");
        self.tx.send(cmd).await.map_err(|_| SendError::SHARD_DOWN)?;
        self.shard_thread.unpark();
        crate::lifecycle_trace!("08_handle_send_done", 0, 0, "frame_loop");
        Ok(())
    }

    pub fn send_shutdown(&self) {
        let _ = self.tx.try_send(ShardCommand::Shutdown);
        self.shard_thread.unpark();
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
