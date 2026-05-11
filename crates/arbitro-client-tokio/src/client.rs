//! Public `Client` facade.
//!
//! Each `Client` instance owns a dedicated kit `MpscAsyncProducer` — no
//! lock on the publish hot path.  Cloning pops a producer from the pool
//! in `Inner`; dropping returns it.  `Inner` holds only cold / shared state.

use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::config::ClientConfig;
use crate::conn::session::spawn_connection;
use crate::consume::SubscriptionHandle;
use crate::error::ClientError;
use crate::state::Inner;
use crate::state::pending::Pending;
use crate::state::seq::SeqAllocator;
use crate::state::subscriptions::Subscriptions;
use crate::transport::frame::{WriteFrame, WriteProducer, MAX_WRITE_PRODUCERS, WRITE_QUEUE_CAP};

/// One entry of a batch publish: `{ subject: &[u8], payload: Bytes }`.
pub use crate::transport::encode::BatchEntry;

/// Handle to a tokio-driven Arbitro connection.
///
/// Each instance owns a dedicated writer producer — publish is lock-free
/// on the hot path.  Clone pops a producer from the shared pool; drop
/// returns it.  Panics if the pool is exhausted (> `MAX_WRITE_PRODUCERS - 2`
/// concurrent clones — 2 slots are reserved for the admin producer and
/// the initial client handle).
pub struct Client {
    pub(crate) inner:    Arc<Inner>,
    pub(crate) producer: Option<WriteProducer>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").field("addr", &self.inner.cfg.addr).finish()
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        let producer = self.inner.producer_pool
            .lock().unwrap()
            .pop()
            .expect("producer pool exhausted — reduce concurrent Client clones or increase MAX_WRITE_PRODUCERS");
        Self { inner: Arc::clone(&self.inner), producer: Some(producer) }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(p) = self.producer.take() {
            if let Ok(mut pool) = self.inner.producer_pool.lock() {
                pool.push(p);
            }
        }
    }
}

impl Client {
    /// Connect to the broker described by `cfg`.
    ///
    /// Resolves once the initial TCP dial + HELLO handshake succeeds.
    /// All subsequent reconnects happen transparently in the background.
    pub async fn connect(cfg: ClientConfig) -> Result<Self, ClientError> {
        use arbitro_kit::route::MpscAsync;

        // Allocate the shared MPSC ring.
        let (mut producers, consumer, _shutdown) =
            MpscAsync::<WriteFrame, WRITE_QUEUE_CAP>::new(MAX_WRITE_PRODUCERS);

        // Reserve two dedicated slots up front.
        let my_producer    = producers.remove(0);   // this Client handle
        let admin_producer = producers.remove(0);   // ack-batcher + heartbeat + sub-replay

        // Ack-batcher and nack-batcher channels (tokio mpsc — Sender is Clone + Sync).
        let (ack_tx, ack_rx)   = tokio::sync::mpsc::channel(4096);
        let (nack_tx, nack_rx) = tokio::sync::mpsc::channel(4096);

        let cancel = CancellationToken::new();

        let inner = Arc::new(Inner {
            cfg:            cfg.clone(),
            producer_pool:  Mutex::new(producers),          // 14 slots
            pending:        Arc::new(Pending::new()),
            seq_alloc:      SeqAllocator::new(),
            cancel:         cancel.clone(),
            subscriptions:  Arc::new(Subscriptions::new()),
            admin_producer: Mutex::new(admin_producer),
            ack_tx,
            nack_tx,
            last_pong_ns:   AtomicU64::new(Inner::now_ns()),
            metrics:        Arc::new(crate::metrics::ClientMetrics::new()),
        });

        // Spawn the ack-batcher and nack-batcher — both live for the Client lifetime.
        tokio::spawn(crate::consume::ack_batcher_task(
            ack_rx,
            Arc::clone(&inner),
            cancel.clone(),
        ));
        tokio::spawn(crate::consume::nack_batcher_task(
            nack_rx,
            Arc::clone(&inner),
            cancel.clone(),
        ));

        // Establish the first connection; background loop handles reconnects.
        spawn_connection(consumer, Arc::clone(&inner)).await?;

        Ok(Self {
            inner,
            producer: Some(my_producer),
        })
    }

    /// Cancel every spawned task immediately.  Idempotent.
    pub fn close(&self) {
        self.inner.cancel.cancel();
    }

    #[inline]
    fn producer(&self) -> &WriteProducer {
        self.producer.as_ref().expect("producer missing")
    }

    // ── publish ───────────────────────────────────────────────────────────────

    #[inline]
    pub fn publish(
        &self,
        stream_id: u32,
        subject:   &[u8],
        payload:   Bytes,
    ) -> Result<(), ClientError> {
        let r = crate::publish::publish_async(
            self.producer(), &self.inner.seq_alloc, stream_id, subject, payload,
        );
        if r.is_ok() {
            self.inner.metrics.publishes_sent
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        r
    }

    #[inline]
    pub fn publish_bytes(
        &self,
        stream_id: u32,
        subject:   &[u8],
        payload:   &[u8],
    ) -> Result<(), ClientError> {
        self.publish(stream_id, subject, Bytes::copy_from_slice(payload))
    }

    #[inline]
    pub fn publish_sync(
        &self,
        stream_id: u32,
        subject:   &[u8],
        payload:   Bytes,
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        crate::publish::publish_sync_async(
            self.producer(),
            &self.inner.pending,
            &self.inner.seq_alloc,
            stream_id, subject, payload,
        )
    }

    pub fn publish_batch(
        &self,
        stream_id: u32,
        entries:   &[BatchEntry<'_>],
    ) -> Result<(), ClientError> {
        let r = crate::publish::publish_batch_async(
            self.producer(), &self.inner.seq_alloc, stream_id, entries,
        );
        if r.is_ok() {
            self.inner.metrics.publish_batch_entries
                .fetch_add(entries.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        r
    }

    pub fn publish_batch_sync(
        &self,
        stream_id: u32,
        entries:   &[BatchEntry<'_>],
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        crate::publish::publish_batch_sync_async(
            self.producer(),
            &self.inner.pending,
            &self.inner.seq_alloc,
            stream_id, entries,
        )
    }

    /// Publish a message with a reply-to subject (request/reply RPC pattern).
    ///
    /// The broker stores the entry with the `reply_to` metadata. Consumers
    /// receive it as part of the delivery and can publish a response to the
    /// specified reply_to subject. Returns the broker's first_seq confirmation.
    ///
    /// The caller is responsible for subscribing to the `reply_to` subject
    /// (typically `_INBOX.<token>`) before calling this method.
    pub fn publish_with_reply(
        &self,
        stream_id: u32,
        subject:   &[u8],
        reply_to:  &[u8],
        payload:   Bytes,
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        crate::publish::publish_with_reply_async(
            self.producer(),
            &self.inner.pending,
            &self.inner.seq_alloc,
            stream_id, subject, reply_to, payload,
        )
    }

    // ── subscribe ─────────────────────────────────────────────────────────────

    /// Subscribe to messages delivered to `consumer_id`.
    ///
    /// Registers the subscription locally before sending the `SubFrame` so
    /// any `Deliver` frames arriving during the broker round-trip are
    /// buffered.  Awaits the `RepOk` reply before returning.
    pub fn subscribe(
        &self,
        stream_id:   u32,
        consumer_id: u32,
        filter:      &[u8],
    ) -> impl std::future::Future<Output = Result<SubscriptionHandle, ClientError>> + Send {
        crate::consume::subscribe_async(
            Arc::clone(&self.inner),
            stream_id,
            consumer_id,
            filter,
        )
    }

    // ── manage ────────────────────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub async fn create_stream(
        &self,
        name: &[u8], filter: &[u8],
        max_msgs: u64, max_bytes: u64, max_age_secs: u64,
        replicas: u8, journal_kind: u8, retention: u8, discard: u8,
    ) -> Result<Bytes, ClientError> {
        crate::manage::create_stream(
            self.producer(), &self.inner.pending, &self.inner.seq_alloc,
            name, filter, max_msgs, max_bytes, max_age_secs,
            replicas, journal_kind, retention, discard,
        ).await
    }

    pub async fn delete_stream(&self, name: &[u8]) -> Result<Bytes, ClientError> {
        crate::manage::delete_stream(
            self.producer(), &self.inner.pending, &self.inner.seq_alloc, name,
        ).await
    }

    pub async fn get_stream(&self, name: &[u8]) -> Result<Bytes, ClientError> {
        crate::manage::get_stream(
            self.producer(), &self.inner.pending, &self.inner.seq_alloc, name,
        ).await
    }

    pub async fn purge_stream(&self, name: &[u8]) -> Result<Bytes, ClientError> {
        crate::manage::purge_stream(
            self.producer(), &self.inner.pending, &self.inner.seq_alloc, name,
        ).await
    }

    pub async fn drain_subject(
        &self, name: &[u8], subject: &[u8],
    ) -> Result<Bytes, ClientError> {
        crate::manage::drain_subject(
            self.producer(), &self.inner.pending, &self.inner.seq_alloc, name, subject,
        ).await
    }

    pub async fn list_streams(
        &self, offset: u32, limit: u32,
    ) -> Result<Bytes, ClientError> {
        crate::manage::list_streams(
            self.producer(), &self.inner.pending, &self.inner.seq_alloc, offset, limit,
        ).await
    }

    /// Create a consumer with no per-subject inflight limits.
    ///
    /// This is the common case. Use [`Client::create_consumer_with_limits`]
    /// to attach per-subject `max_inflight` caps. Per-subject limits are
    /// only enforced by the server with `ack_policy == AckPolicy::Explicit
    /// (1)`; setting them on a fire-and-forget consumer is silently
    /// dropped server-side.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_consumer(
        &self,
        stream_id: u32, name: &[u8], group: &[u8], subject: &[u8],
        max_inflight: u16, ack_policy: u8, deliver_policy: u8, deliver_mode: u8,
        ack_wait_ms: u32, start_seq: u64,
    ) -> Result<Bytes, ClientError> {
        self.create_consumer_with_limits(
            stream_id, name, group, subject, max_inflight,
            ack_policy, deliver_policy, deliver_mode, ack_wait_ms, start_seq,
            &[],
        ).await
    }

    /// Create a consumer with per-subject inflight limits.
    ///
    /// Each `(pattern, max_inflight)` entry caps the number of in-flight
    /// (delivered, unacked) messages per subject matching `pattern`. Wildcards
    /// (`*`, `>`) are supported. Only effective with `ack_policy ==
    /// AckPolicy::Explicit (1)`; otherwise silently ignored by the server.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_consumer_with_limits(
        &self,
        stream_id: u32, name: &[u8], group: &[u8], subject: &[u8],
        max_inflight: u16, ack_policy: u8, deliver_policy: u8, deliver_mode: u8,
        ack_wait_ms: u32, start_seq: u64,
        subject_limits: &[arbitro_proto::v2::manager::SubjectLimit<'_>],
    ) -> Result<Bytes, ClientError> {
        crate::manage::create_consumer(
            self.producer(), &self.inner.pending, &self.inner.seq_alloc,
            stream_id, name, group, subject, max_inflight,
            ack_policy, deliver_policy, deliver_mode, ack_wait_ms, start_seq,
            subject_limits,
        ).await
    }

    pub async fn delete_consumer(&self, consumer_id: u32) -> Result<Bytes, ClientError> {
        crate::manage::delete_consumer(
            self.producer(), &self.inner.pending, &self.inner.seq_alloc, consumer_id,
        ).await
    }

    pub async fn get_consumer(
        &self, stream_id: u32, name: &[u8],
    ) -> Result<Bytes, ClientError> {
        crate::manage::get_consumer(
            self.producer(), &self.inner.pending, &self.inner.seq_alloc, stream_id, name,
        ).await
    }

    pub async fn list_consumers(
        &self, stream_id: u32, offset: u32, limit: u32,
    ) -> Result<Bytes, ClientError> {
        crate::manage::list_consumers(
            self.producer(), &self.inner.pending, &self.inner.seq_alloc,
            stream_id, offset, limit,
        ).await
    }

    /// Live pending-ack count for one consumer (broker round-trip).
    ///
    /// Returns the number of messages this consumer has been delivered
    /// but not yet acked. Equivalent to NATS JetStream's
    /// `num_ack_pending`. Reads engine state via one O(1) Vec lookup
    /// per shard; cheap enough to poll from a dashboard.
    pub async fn get_pending(&self, consumer_id: u32) -> Result<u64, ClientError> {
        let resp = crate::manage::consumer_stats(
            self.producer(), &self.inner.pending, &self.inner.seq_alloc, consumer_id,
        ).await?;
        // RepOk body is 8 bytes — server packs the u64 count in place of ref_seq.
        if resp.len() < 8 {
            return Err(ClientError::Proto(arbitro_proto::error::ProtoError::BufferTooShort {
                need: 8,
                have: resp.len() as u32,
            }));
        }
        let count = u64::from_le_bytes(resp[..8].try_into().unwrap());
        Ok(count)
    }

    // ── observability ─────────────────────────────────────────────────────────

    /// Borrow the live atomic counters shared with every task on this
    /// client. Mostly useful for testing — operators should prefer
    /// [`Client::metrics_snapshot`] for periodic sampling.
    #[inline]
    pub fn metrics(&self) -> &crate::metrics::ClientMetrics {
        &self.inner.metrics
    }

    /// Point-in-time snapshot of all client counters. Cheap — just
    /// `Relaxed` loads on a few atomics. Call on a timer to chart
    /// throughput, ack rates, reconnects, active subscriptions, etc.
    #[inline]
    pub fn metrics_snapshot(&self) -> crate::metrics::ClientMetricsSnapshot {
        self.inner.metrics.snapshot()
    }
}
