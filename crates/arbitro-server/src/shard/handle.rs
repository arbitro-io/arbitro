//! ShardHandle — async API wrapping kit::Mpsc (M producers) + oneshot per command.
//!
//! Each method builds an owned command, sends it via the kit::Mpsc lane
//! (round-robin across M producers, each behind a parking_lot::Mutex), and
//! awaits the oneshot reply. Backpressure: if all rings are full, the call
//! awaits a backpressure Notify that the CommandWorker triggers after
//! draining items via `recv_batch_async_send`.
//!
//! ## Why this design
//!
//! `kit::MpscProducer` is `!Sync` by design (each clone owns a unique ring).
//! arbitro's pattern is "any tokio task may send" → requires Sync access.
//! Wrapping each producer in `parking_lot::Mutex` makes the producer set
//! shareable. With M=8 producers, lock contention is amortised across 8
//! lanes; the CommandWorker drains all 8 in one `recv_batch_async_send`
//! await — that's where the 2× over `tokio::mpsc::recv_many` comes from.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine_v2::types::*;
use arbitro_store::EntryRef;
use tokio::sync::oneshot;

use arbitro_kit::route::MpscProducer;
use arbitro_kit::waiter::NotifyWaiter;

use crate::common::Gate;
use crate::common::reply_v2::send_rep_ok_v2;
use crate::shard::command::*;
use crate::shard::router::SharedStore;
use crate::transport::ConnectionRegistry;

/// Producer type used by ShardHandle. Each shard has `SHARD_M` producers,
/// each wrapped in a `parking_lot::Mutex` so any tokio task can send via
/// the round-robin lane chooser.
pub(super) type ShardProducer =
    MpscProducer<ShardCommand, { crate::shard::router::SHARD_RING_CAP }, NotifyWaiter>;

/// Lane = `Mutex<MpscProducer>`. Send path: lock + try_send + unlock.
/// Never holds the lock across `.await`.
pub(super) type ShardLane = parking_lot::Mutex<ShardProducer>;

/// Async handle to a shard worker.
#[derive(Clone)]
pub struct ShardHandle {
    shard_id: u32,
    lanes: Arc<[ShardLane]>,
    /// Round-robin counter for lane selection.
    next_idx: Arc<AtomicUsize>,
    /// Wakes parked senders after the CommandWorker drains items.
    backpressure: Arc<tokio::sync::Notify>,
    /// Set to `true` by the CommandWorker on shutdown so `send()` callers
    /// can short-circuit instead of looping forever on full lanes that
    /// will never drain. Replaces the `Err(SendError)` semantics of
    /// `tokio::mpsc::Sender` when the receiver is dropped.
    consumer_alive: Arc<std::sync::atomic::AtomicBool>,
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
        lanes: Arc<[ShardLane]>,
        backpressure: Arc<tokio::sync::Notify>,
        consumer_alive: Arc<std::sync::atomic::AtomicBool>,
        store: SharedStore,
        gate: Arc<Gate>,
        registry: ConnectionRegistry,
    ) -> Self {
        Self {
            shard_id,
            lanes,
            next_idx: Arc::new(AtomicUsize::new(0)),
            backpressure,
            consumer_alive,
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
        delay_ms: u32,
    ) -> Result<NackReply, SendError> {
        let (tx, rx) = oneshot::channel();
        self.send(ShardCommand::Nack(NackCmd {
            consumer_id,
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
        config:     StreamConfig,
        max_msgs:   u64,
        max_bytes:  u64,
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
    pub async fn purge_stream(
        &self,
        stream_id: StreamId,
    ) -> Result<u64, SendError> {
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

    /// Send a command to the shard. Picks a lane round-robin, tries each
    /// in turn. If all lanes are full, awaits the backpressure Notify
    /// (signalled by CommandWorker after each drain pass) and retries.
    ///
    /// **Never** holds a lane lock across `.await` — would block the
    /// tokio worker. Each lock is taken, try_send is attempted, lock is
    /// released, before any await happens.
    pub async fn send(
        &self,
        mut cmd: ShardCommand,
    ) -> Result<(), SendError> {
        crate::lifecycle_trace!("07_handle_send_enter", 0, 0, "frame_loop");
        let n = self.lanes.len();
        let start = self.next_idx.fetch_add(1, Ordering::Relaxed) % n;
        loop {
            if !self.consumer_alive.load(Ordering::Acquire) {
                return Err(SendError::SHARD_DOWN);
            }
            // Try every lane starting from `start`. Each iteration acquires
            // and releases the lock immediately — no await while held.
            for i in 0..n {
                let idx = (start + i) % n;
                let res = self.lanes[idx].lock().try_send(cmd);
                match res {
                    Ok(()) => {
                        crate::lifecycle_trace!("08_handle_send_done", 0, 0, "frame_loop");
                        return Ok(());
                    }
                    Err(returned) => {
                        cmd = returned;
                    }
                }
            }
            // All lanes full — wait for CommandWorker to drain at least
            // one item, then retry. Capture notified() BEFORE the retry
            // sweep to prevent lost-wake.
            let notify = self.backpressure.notified();
            // One more pass before parking (consumer might have drained
            // while we were building the notified()).
            for i in 0..n {
                let idx = (start + i) % n;
                let res = self.lanes[idx].lock().try_send(cmd);
                match res {
                    Ok(()) => return Ok(()),
                    Err(returned) => cmd = returned,
                }
            }
            if !self.consumer_alive.load(Ordering::Acquire) {
                return Err(SendError::SHARD_DOWN);
            }
            notify.await;
        }
    }

    pub fn send_shutdown(&self) {
        // Best-effort: try every lane. Shutdown is in-band; the worker
        // breaks out of the loop when it sees ShardCommand::Shutdown.
        let mut cmd = Some(ShardCommand::Shutdown);
        for lane in self.lanes.iter() {
            if let Some(c) = cmd.take() {
                match lane.lock().try_send(c) {
                    Ok(()) => break,
                    Err(returned) => cmd = Some(returned),
                }
            }
        }
        // Drop any unsent command (shutdown is best-effort if every lane
        // is full — the channel will close via Drop of senders anyway).
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
