//! ShardHandle — async API wrapping mpsc::Sender + oneshot per command.
//!
//! Each method builds an owned command, sends it to the shard's channel,
//! and awaits the oneshot reply. Backpressure if channel is full.

use std::fmt;
use std::sync::Arc;

use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine_v2::types::*;
use arbitro_store::EntryRef;
use tokio::sync::{mpsc, oneshot};

use crate::common::Gate;
use crate::common::reply_v2::send_rep_ok_v2;
use crate::shard::command::*;
use crate::shard::router::SharedStore;
use crate::transport::ConnectionRegistry;

/// Async handle to a shard worker.
#[derive(Clone)]
pub struct ShardHandle {
    shard_id: u32,
    tx: mpsc::Sender<ShardCommand>,
    shard_thread: std::thread::Thread,
    /// Shared store — publish writes directly, bypassing the shard worker.
    store: SharedStore,
    /// Shared gate — publish notifies drain after store append.
    gate: Arc<Gate>,
    /// Connection registry — publish replies directly to the client.
    registry: ConnectionRegistry,
}

impl ShardHandle {
    pub fn new(
        shard_id: u32,
        tx: mpsc::Sender<ShardCommand>,
        shard_thread: std::thread::Thread,
        store: SharedStore,
        gate: Arc<Gate>,
        registry: ConnectionRegistry,
    ) -> Self {
        Self {
            shard_id,
            tx,
            shard_thread,
            store,
            gate,
            registry,
        }
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    // ── Hot path ────────────────────────────────────────────────────────

    /// Fire & forget — writes directly to the shared store, signals gate.
    /// Does NOT go through the shard worker. Publish and drain are
    /// independent services connected only by store and gate.
    pub async fn publish(
        &self,
        stream_id: StreamId,
        conn_id: u64,
        env_seq: u32,
        entries: Vec<PublishEntryOwned>,
    ) -> Result<(), SendError> {
        let store_entries: Vec<EntryRef<'_>> = entries
            .iter()
            .map(|e| EntryRef {
                stream_id: stream_id.raw(),
                subject: &e.subject,
                payload: &e.payload,
                flags: 0,
            })
            .collect();

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let first_seq = self
            .store
            .lock()
            .unwrap()
            .append_batch(&store_entries, now_ms)
            .map_err(|_| SendError::SHARD_DOWN)?;

        send_rep_ok_v2(&self.registry, conn_id, env_seq as u64, first_seq);
        self.gate.release();
        self.shard_thread.unpark();
        Ok(())
    }

    /// Fire & forget — shard accumulates entries, flushes with append_batch.
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
        }))
        .await
    }

    pub async fn ack(
        &self,
        consumer_id: ConsumerId,
        entries: Vec<AckEntry>,
    ) -> Result<AckReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Ack(AckCmd {
            consumer_id,
            entries,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn nack(
        &self,
        consumer_id: ConsumerId,
        entries: Vec<AckEntry>,
    ) -> Result<NackReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Nack(NackCmd {
            consumer_id,
            entries,
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
    ) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Subscribe(SubscribeCmd {
            stream_config,
            consumer_config,
            subscription_config,
            connection_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn unsubscribe(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<bool, SendError> {
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
    ) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::CreateStream(CreateStreamCmd {
            config,
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
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn delete_consumer(
        &self,
        consumer_id: ConsumerId,
    ) -> Result<bool, SendError> {
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

    pub async fn drain_connection(
        &self,
        connection_id: ConnectionId,
    ) -> Result<(), SendError> {
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

    pub async fn list_streams(
        &self,
    ) -> Result<ListStreamsReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::ListStreams(ListStreamsCmd {
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn list_consumers(
        &self,
    ) -> Result<ListConsumersReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::ListConsumers(ListConsumersCmd {
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn store_info(
        &self,
        stream_id: StreamId,
    ) -> Result<StoreInfoReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::StoreInfo(StoreInfoCmd {
            stream_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Admin ───────────────────────────────────────────────────────────

    pub async fn pause_consumer(
        &self,
        consumer_id: ConsumerId,
    ) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::PauseConsumer(PauseConsumerCmd {
            consumer_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    pub async fn resume_consumer(
        &self,
        consumer_id: ConsumerId,
    ) -> Result<bool, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::ResumeConsumer(ResumeConsumerCmd {
            consumer_id,
            reply: tx,
        }))
        .await?;
        rx.await.map_err(|_| SendError::SHARD_DOWN)
    }

    // ── Internal ────────────────────────────────────────────────────────

    pub async fn send(
        &self,
        cmd: ShardCommand,
    ) -> Result<(), SendError> {
        crate::lifecycle_trace!(
            "07_handle_send_enter",
            0,
            0,
            "frame_loop"
        );
        self.tx
            .send(cmd)
            .await
            .map_err(|_| SendError::SHARD_DOWN)?;
        crate::lifecycle_trace!(
            "08_handle_send_done",
            0,
            0,
            "frame_loop"
        );
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
