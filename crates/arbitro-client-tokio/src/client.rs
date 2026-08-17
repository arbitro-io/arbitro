//! Public `Client` facade.
//!
//! `Client` is a thin `Arc<Inner>` handle — publish/manage helpers lease a
//! producer from the shared `WritePool` for the duration of a single
//! `try_send`.  `Inner` holds all cold / shared state.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::config::ClientConfig;
use crate::conn::session::spawn_connection;
use crate::consume::SubscriptionHandle;
use crate::error::ClientError;
use crate::queue::QueueOptions;
use crate::state::pending::Pending;
use crate::state::seq::SeqAllocator;
use crate::state::subscriptions::Subscriptions;
use crate::state::Inner;
use crate::transport::frame::{WritePool, MAX_WRITE_PRODUCERS};

/// One entry of a batch publish: `{ subject: &[u8], payload: Bytes }`.
pub use crate::transport::encode::BatchEntry;

/// Handle to a tokio-driven Arbitro connection.
///
/// Cloning is a pure `Arc` bump — every publish/manage call leases a
/// producer from the shared `WritePool` for the duration of a single
/// `try_send`, so there is no per-clone producer to hand out or reclaim.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<Inner>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("addr", &self.inner.cfg.addr)
            .finish()
    }
}

impl Client {
    /// Connect to the broker described by `cfg`.
    ///
    /// Resolves once the initial TCP dial + HELLO handshake succeeds.
    /// All subsequent reconnects happen transparently in the background.
    ///
    /// Redelivery dedup follows
    /// [`ClientConfig::ack_store`](crate::config::ClientConfig::ack_store):
    /// `None` (the default) disables it; `Some(WalConfig)` opens a durable WAL
    /// at the configured — or platform-default — directory. For a custom
    /// [`Store`](crate::ackstore::Store) implementation use
    /// [`Client::connect_with_ackstore`].
    pub async fn connect(cfg: ClientConfig) -> Result<Self, ClientError> {
        Self::connect_with(cfg, None).await
    }

    /// Connect with a durable redelivery-dedup [`Store`](crate::ackstore::Store).
    ///
    /// The store is keyed by `(stream_name, consumer_name, seq)` and persists
    /// which messages were processed+acked. After a restart, a redelivery of an
    /// already-acked message is recognized and silently re-acked instead of
    /// re-running the user handler. Pass a [`Wal`](crate::ackstore::wal::Wal)
    /// for durability or a [`MemoryStore`](crate::ackstore::memory::MemoryStore)
    /// for in-process dedup only.
    ///
    /// Dedup only activates for subscriptions created via
    /// [`Client::subscribe_dedup`] (which carries the stream/consumer names the
    /// store needs to resolve a slot).
    pub async fn connect_with_ackstore(
        cfg: ClientConfig,
        store: Arc<dyn crate::ackstore::Store>,
    ) -> Result<Self, ClientError> {
        // Rebuild in-memory state from the log before any delivery arrives.
        store
            .restore()
            .map_err(|e| ClientError::InvalidConfig(format!("ackstore restore: {e}")))?;
        Self::connect_with(cfg, Some(store)).await
    }

    async fn connect_with(
        cfg: ClientConfig,
        ack_store: Option<Arc<dyn crate::ackstore::Store>>,
    ) -> Result<Self, ClientError> {
        // Reject an oversized token here, before any socket work. The broker
        // closes with InvalidLength — a generic code the reader does NOT treat
        // as terminal — so with the default unbounded reconnect policy this
        // would redial forever. A local error names the actual problem.
        if let Some(t) = &cfg.auth_token {
            if t.len() > crate::config::MAX_AUTH_TOKEN_LEN {
                return Err(ClientError::InvalidConfig(format!(
                    "auth_token is {} bytes, the broker accepts at most {}",
                    t.len(),
                    crate::config::MAX_AUTH_TOKEN_LEN
                )));
            }
        }

        // An explicitly supplied store wins; otherwise honour the WAL declared
        // on the config (storage path included). Opening happens here, before
        // any connection work, so a bad directory / already-locked directory
        // fails fast instead of after a successful handshake.
        let ack_store = match ack_store {
            Some(s) => Some(s),
            None => match &cfg.ack_store {
                Some(wcfg) => {
                    let wal = crate::ackstore::wal::Wal::open(wcfg.clone()).map_err(|e| {
                        ClientError::InvalidConfig(format!("ackstore open: {e}"))
                    })?;
                    let store: Arc<dyn crate::ackstore::Store> = wal;
                    store.restore().map_err(|e| {
                        ClientError::InvalidConfig(format!("ackstore restore: {e}"))
                    })?;
                    Some(store)
                }
                None => None,
            },
        };

        // Ring depth is a runtime value now; the ring indexes with a mask, so
        // it has to be a non-zero power of two.
        if cfg.write_queue_capacity == 0 || !cfg.write_queue_capacity.is_power_of_two() {
            return Err(ClientError::InvalidConfig(format!(
                "write_queue_capacity must be a non-zero power of two, got {}",
                cfg.write_queue_capacity
            )));
        }
        // heartbeat, ack, nack and session-replay each hold a lease for the
        // client's lifetime, so publish/manage needs at least one slot beyond
        // them or the first `acquire()` below fails.
        const RESERVED_LEASES: usize = 4;
        if cfg.max_write_producers <= RESERVED_LEASES
            || cfg.max_write_producers > MAX_WRITE_PRODUCERS
        {
            return Err(ClientError::InvalidConfig(format!(
                "max_write_producers must be {}..={MAX_WRITE_PRODUCERS} ({RESERVED_LEASES} are \
                 reserved for background tasks), got {}",
                RESERVED_LEASES + 1,
                cfg.max_write_producers
            )));
        }

        // One leasable ring per producer slot. Slots are allocated on first
        // claim, so an unused producer costs its skeleton, not its ring.
        let (pool, consumer) =
            WritePool::new(cfg.max_write_producers, cfg.write_queue_capacity);

        // Ack-batcher and nack-batcher channels (tokio mpsc — Sender is Clone + Sync).
        let (ack_tx, ack_rx) = tokio::sync::mpsc::channel(cfg.ack_queue_capacity);
        let (nack_tx, nack_rx) = tokio::sync::mpsc::channel(cfg.ack_queue_capacity);

        let cancel = CancellationToken::new();

        let inner = Arc::new(Inner {
            cfg: cfg.clone(),
            pool: Arc::clone(&pool),
            pending: Arc::new(Pending::new()),
            seq_alloc: SeqAllocator::new(),
            sub_id_alloc: crate::state::seq::SubIdAllocator::new(),
            cancel: cancel.clone(),
            subscriptions: Arc::new(Subscriptions::new()),
            ack_tx,
            nack_tx,
            last_pong_ns: AtomicU64::new(Inner::now_ns()),
            metrics: Arc::new(crate::metrics::ClientMetrics::new()),
            cron_state: crate::cron::CronState::new(),
            session_cancel: std::sync::Mutex::new(None),
            ackrel: Arc::new(crate::ackrel::AckRelay::new()),
            ack_store,
        });

        // Dedicated leases for the long-lived background tasks — carved out
        // up front so they never contend with ad-hoc publish/manage leases.
        let heartbeat_lease = pool
            .acquire_control()
            .expect("validated: max_write_producers > RESERVED_LEASES");
        let ack_lease = pool
            .acquire_control()
            .expect("validated: max_write_producers > RESERVED_LEASES");
        let nack_lease = pool
            .acquire_control()
            .expect("validated: max_write_producers > RESERVED_LEASES");
        let session_replay_lease = pool
            .acquire_control()
            .expect("validated: max_write_producers > RESERVED_LEASES");

        // Heartbeat runs for the Client lifetime (not per-session) so it can
        // hold a single dedicated lease across reconnects.
        tokio::spawn(crate::conn::heartbeat::heartbeat_task(
            Arc::clone(&inner),
            cfg.keep_alive.clone(),
            cancel.clone(),
            heartbeat_lease,
        ));

        // Spawn the ack-batcher and nack-batcher — both live for the Client lifetime.
        tokio::spawn(crate::consume::ack_batcher_task(
            ack_rx,
            Arc::clone(&inner),
            cancel.clone(),
            ack_lease,
        ));
        tokio::spawn(crate::consume::nack_batcher_task(
            nack_rx,
            Arc::clone(&inner),
            cancel.clone(),
            nack_lease,
        ));

        // Establish the first connection; background loop handles reconnects.
        if let Err(e) = spawn_connection(consumer, Arc::clone(&inner), session_replay_lease).await {
            // The caller never receives a Client and so can never `close()` it.
            // Cancel the background tasks (each holds an `Arc<Inner>`, which
            // owns the store) and release the store's directory lock — without
            // this, one transient dial failure would turn every subsequent
            // connect attempt on that directory into `StoreError::Locked`.
            inner.cancel.cancel();
            if let Some(store) = &inner.ack_store {
                let _ = store.close();
            }
            return Err(e);
        }

        Ok(Self { inner })
    }

    /// Cancel every spawned task immediately.  Idempotent.
    ///
    /// If a durable ackstore is configured, its buffered records are flushed
    /// and the log is closed (final fsync) before returning.
    pub fn close(&self) {
        // Cancel the background tasks FIRST so the ack-batcher's periodic
        // `store.sync()` can't race a spurious call against a store we're
        // about to close, then do the final flush + close.
        self.inner.cancel.cancel();
        if let Some(store) = &self.inner.ack_store {
            let _ = store.sync();
            let _ = store.close();
        }
    }

    // ── publish ───────────────────────────────────────────────────────────────

    /// Fire-and-forget publish (no msg_id, no broker-side dedup).
    ///
    /// For idempotent publishes on streams with a non-zero
    /// `idempotency_window_ms`, use [`Client::publish_with_id`] /
    /// [`Client::publish_wait_with_id`].
    #[inline]
    pub fn publish(
        &self,
        stream_id: u32,
        subject: &[u8],
        payload: Bytes,
    ) -> Result<(), ClientError> {
        self.publish_with_id(stream_id, subject, &[], payload)
    }

    /// Fire-and-forget publish with an explicit `msg_id` for broker
    /// dedup. Pass an empty `msg_id` to match [`Client::publish`].
    #[inline]
    pub fn publish_with_id(
        &self,
        stream_id: u32,
        subject: &[u8],
        msg_id: &[u8],
        payload: Bytes,
    ) -> Result<(), ClientError> {
        let r = crate::publish::publish_async(
            &self.inner.pool,
            &self.inner.seq_alloc,
            stream_id,
            subject,
            msg_id,
            payload,
        );
        if r.is_ok() {
            self.inner
                .metrics
                .publishes_sent
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        r
    }

    #[inline]
    pub fn publish_bytes(
        &self,
        stream_id: u32,
        subject: &[u8],
        payload: &[u8],
    ) -> Result<(), ClientError> {
        self.publish(stream_id, subject, Bytes::copy_from_slice(payload))
    }

    /// Publish and **wait for the broker's OK** (RepOk with first_seq).
    ///
    /// "wait" = wait for the broker to acknowledge receipt — NOT a disk fsync.
    /// Whether that OK implies durability (in-memory accept vs fsync-to-disk) is
    /// decided by the target stream's journal/persistence policy, not this call.
    #[inline]
    pub fn publish_wait(
        &self,
        stream_id: u32,
        subject: &[u8],
        payload: Bytes,
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        self.publish_wait_with_id(stream_id, subject, &[], payload)
    }

    /// Publish and wait for the broker's OK, with an explicit `msg_id` for
    /// broker dedup. Returns `Err(ErrorCode::IdempotencyDuplicate)` when the
    /// broker has already seen this `(stream_id, msg_id)` within the stream's
    /// `idempotency_window_ms`.
    #[inline]
    pub fn publish_wait_with_id(
        &self,
        stream_id: u32,
        subject: &[u8],
        msg_id: &[u8],
        payload: Bytes,
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        crate::publish::publish_wait_async(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            self.inner.cfg.max_block,
            stream_id,
            subject,
            msg_id,
            payload,
        )
    }

    pub fn publish_batch(
        &self,
        stream_id: u32,
        entries: &[BatchEntry<'_>],
    ) -> Result<(), ClientError> {
        let r = crate::publish::publish_batch_async(
            &self.inner.pool,
            &self.inner.seq_alloc,
            stream_id,
            entries,
        );
        if r.is_ok() {
            self.inner
                .metrics
                .publish_batch_entries
                .fetch_add(entries.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        r
    }

    /// Batch publish and wait for the broker's OK (RepOk with the batch's
    /// `first_seq`). "wait" = wait for acknowledgement, not a disk fsync — the
    /// stream's persistence policy decides what the OK guarantees.
    pub fn publish_batch_wait(
        &self,
        stream_id: u32,
        entries: &[BatchEntry<'_>],
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        crate::publish::publish_batch_wait_async(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            self.inner.cfg.max_block,
            stream_id,
            entries,
        )
    }

    // ── Deprecated `*_sync` aliases ─────────────────────────────────────────
    // Renamed to `*_wait`: "sync" wrongly implied disk-sync, but these only
    // wait for the broker's OK (receipt). Kept for backward compatibility.

    /// Deprecated alias for [`publish_wait`](Self::publish_wait).
    #[deprecated(note = "renamed to `publish_wait` — it waits for the broker OK, not a disk fsync")]
    #[inline]
    pub fn publish_sync(
        &self,
        stream_id: u32,
        subject: &[u8],
        payload: Bytes,
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        self.publish_wait(stream_id, subject, payload)
    }

    /// Deprecated alias for [`publish_wait_with_id`](Self::publish_wait_with_id).
    #[deprecated(note = "renamed to `publish_wait_with_id`")]
    #[inline]
    pub fn publish_sync_with_id(
        &self,
        stream_id: u32,
        subject: &[u8],
        msg_id: &[u8],
        payload: Bytes,
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        self.publish_wait_with_id(stream_id, subject, msg_id, payload)
    }

    /// Deprecated alias for [`publish_batch_wait`](Self::publish_batch_wait).
    #[deprecated(note = "renamed to `publish_batch_wait`")]
    #[inline]
    pub fn publish_batch_sync(
        &self,
        stream_id: u32,
        entries: &[BatchEntry<'_>],
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        self.publish_batch_wait(stream_id, entries)
    }

    /// Publish a message with a delivery delay.
    ///
    /// The broker parks the message in its delayed journal and delivers it
    /// to consumers after `delay_ms` milliseconds. Returns the broker's
    /// confirmation (RepOk). The `ref_seq` in the reply is 0 because the
    /// message doesn't receive a store sequence until maturation.
    pub fn publish_delayed(
        &self,
        stream_id: u32,
        subject: &[u8],
        payload: Bytes,
        delay_ms: u64,
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        crate::publish::publish_delayed_async(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            self.inner.cfg.max_block,
            stream_id,
            subject,
            payload,
            delay_ms,
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
        subject: &[u8],
        reply_to: &[u8],
        payload: Bytes,
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        crate::publish::publish_with_reply_async(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            self.inner.cfg.max_block,
            stream_id,
            subject,
            reply_to,
            b"",
            payload,
        )
    }

    /// M10: PublishWithReply with idempotency dedup. Non-empty `msg_id`
    /// routes the publish through the per-stream `IdempotencyTracker` so
    /// retries from the same client return the original RepOk without a
    /// second write to the store.
    pub fn publish_with_reply_msg_id(
        &self,
        stream_id: u32,
        subject: &[u8],
        reply_to: &[u8],
        msg_id: &[u8],
        payload: Bytes,
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        crate::publish::publish_with_reply_async(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            self.inner.cfg.max_block,
            stream_id,
            subject,
            reply_to,
            msg_id,
            payload,
        )
    }

    // ── publish with headers ────────────────────────────────────────────────

    /// Publish a message with arbitrary key-value headers. Awaits broker
    /// confirmation (RepOk). Headers are persisted alongside the payload
    /// and stripped on delivery — consumers receive only the user payload.
    ///
    /// If a header with key `b"msg-id"` is present, it is used for
    /// broker-side idempotency dedup (equivalent to `publish_wait_with_id`).
    pub fn publish_with_headers(
        &self,
        stream_id: u32,
        subject: &[u8],
        headers: &[(&[u8], &[u8])],
        payload: Bytes,
    ) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
        crate::publish::publish_with_headers_sync_async(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            self.inner.cfg.max_block,
            stream_id,
            subject,
            headers,
            payload,
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
        stream_id: u32,
        consumer_id: u32,
        filter: &[u8],
    ) -> impl std::future::Future<Output = Result<SubscriptionHandle, ClientError>> + Send {
        crate::consume::subscribe_async(
            Arc::clone(&self.inner),
            stream_id,
            consumer_id,
            filter,
            None,
        )
    }

    /// Open N filtered subscriptions on `consumer_id` in ONE round-trip.
    ///
    /// A hundred [`Client::subscribe`] calls cost a hundred round-trips; the
    /// broker's work per subscription is a filter check and a binding, so the
    /// trip is nearly the whole cost. Every entry runs the same admission
    /// rules as a single subscribe.
    ///
    /// An empty filter inherits the consumer's, exactly as `subscribe` does.
    /// Entries the broker refuses — a filter outside the consumer's slice —
    /// come back in [`SubscribeBatchOutcome::rejected`] naming their index,
    /// while their legal peers stay open. A whole-frame refusal is an `Err`.
    ///
    /// Capped at 1024 entries by the broker.
    pub fn subscribe_batch(
        &self,
        stream_id: u32,
        consumer_id: u32,
        filters: Vec<Vec<u8>>,
    ) -> impl std::future::Future<Output = Result<crate::SubscribeBatchOutcome, ClientError>> + Send
    {
        crate::consume::subscribe_batch_async(
            Arc::clone(&self.inner),
            stream_id,
            consumer_id,
            filters,
        )
    }

    /// Subscribe with durable redelivery dedup keyed by
    /// `(stream_name, consumer_name)`.
    ///
    /// Identical to [`Client::subscribe`], but resolves an [`ackstore`] slot
    /// so that after a restart an already-processed message is recognized and
    /// re-acked instead of re-delivered to your handler. Requires the client
    /// to have been created with [`Client::connect_with_ackstore`]; without a
    /// store, this behaves exactly like `subscribe` (no dedup).
    ///
    /// The names MUST match those used to create the stream and consumer — the
    /// numeric `consumer_id` is ephemeral (it changes on delete+recreate) and
    /// cannot be a durable dedup key, whereas `seq` is stream-scoped so
    /// `(stream_name, consumer_name, seq)` is stable across consumer recreation.
    ///
    /// [`ackstore`]: crate::ackstore
    pub fn subscribe_dedup<'a>(
        &self,
        stream_name: &'a str,
        consumer_name: &'a str,
        stream_id: u32,
        consumer_id: u32,
        filter: &'a [u8],
    ) -> impl std::future::Future<Output = Result<SubscriptionHandle, ClientError>> + Send {
        crate::consume::subscribe_async(
            Arc::clone(&self.inner),
            stream_id,
            consumer_id,
            filter,
            Some((stream_name, consumer_name)),
        )
    }

    /// Join the durable work queue `queue` on `stream_id` in ONE call.
    ///
    /// The NATS-style convenience: instead of creating a consumer and then
    /// subscribing to it, every worker calls `queue_subscribe` with the same
    /// queue name and the broker load-balances between them — each message is
    /// delivered to exactly ONE member of the queue.
    ///
    /// The queue name doubles as the durable consumer name AND the queue
    /// group, which is what makes this safe to call concurrently from N
    /// workers: they all resolve to the same durable consumer (the create is
    /// idempotent) and therefore the same round-robin queue. Because the two
    /// names always move together, two workers can never disagree on the
    /// config and trip `InvalidConsumerConfig`.
    ///
    /// Passing a DIFFERENT queue name gives you a separate, independent
    /// durable queue over the same stream — its own cursor, its own pending
    /// set, its own copy of the messages.
    ///
    /// The consumer is **durable** (`AckPolicy::Explicit`): it survives broker
    /// restarts and worker disconnects, keeps its cursor, and redelivers any
    /// message a worker took but never acked. Messages must be acked —
    /// that is the point of a work queue.
    ///
    /// This is pure client-side sugar over
    /// [`Client::create_consumer_with_limits`] + [`Client::subscribe`]; it
    /// sends the exact same two frames and adds no broker-side concepts.
    ///
    /// ```ignore
    /// // Run this from as many workers as you like:
    /// let mut sub = client.queue_subscribe(stream_id, b"workers", b"orders.>").await?;
    /// while let Some(msg) = sub.recv().await {
    ///     handle(&msg);
    ///     msg.ack();
    /// }
    /// ```
    pub async fn queue_subscribe(
        &self,
        stream_id: u32,
        queue: &[u8],
        filter: &[u8],
    ) -> Result<SubscriptionHandle, ClientError> {
        self.queue_subscribe_with(stream_id, queue, QueueOptions::new().filter(filter))
            .await
    }

    /// [`Client::queue_subscribe`] with tuning knobs — redelivery deadline,
    /// in-flight cap, where a brand-new queue starts reading.
    ///
    /// [`QueueOptions`] is a value, so it can be built conditionally or
    /// reused across queues:
    ///
    /// ```ignore
    /// let mut opts = QueueOptions::new().filter(b"orders.>");
    /// if slow_worker { opts = opts.ack_wait_ms(120_000); }
    /// client.queue_subscribe_with(stream_id, b"workers", opts).await?;
    /// ```
    ///
    /// `deliver_policy` / `start_seq` only take effect the first time the
    /// queue is created — a later join finds the durable cursor already in
    /// place and resumes from it.
    pub async fn queue_subscribe_with(
        &self,
        stream_id: u32,
        queue: &[u8],
        opts: QueueOptions,
    ) -> Result<SubscriptionHandle, ClientError> {
        use arbitro_proto::config::{AckPolicy, DeliverMode};

        // `queue` is both the durable consumer name AND the queue group,
        // so an empty one would send an empty name and an empty group.
        // There is no stream name here to fall back to (the API is keyed
        // by `stream_id`), and an empty group means "the anonymous shared
        // queue" broker-side — fail locally instead.
        if queue.is_empty() {
            return Err(ClientError::InvalidConfig(
                "queue_subscribe requires a non-empty queue name — it is \
                 both the durable consumer name and the queue group"
                    .into(),
            ));
        }

        let limits: Vec<arbitro_proto::v2::manager::SubjectLimit<'_>> = opts
            .subject_limits
            .iter()
            .map(
                |(pattern, limit)| arbitro_proto::v2::manager::SubjectLimit {
                    pattern: pattern.as_slice(),
                    limit: *limit,
                },
            )
            .collect();

        // name == group is the invariant that makes concurrent joins
        // idempotent rather than a config clash: both always move together,
        // so two workers can never disagree on the consumer config.
        let resp = self
            .create_consumer_with_limits(
                stream_id,
                queue,
                queue,
                b"", // the subject filter belongs to the subscription
                opts.max_inflight,
                AckPolicy::Explicit as u8,
                opts.deliver_policy as u8,
                DeliverMode::Queue as u8,
                opts.ack_wait_ms,
                opts.start_seq,
                &limits,
            )
            .await?;
        if resp.len() < 8 {
            return Err(ClientError::InvalidConfig(
                "broker reply shorter than expected u64 consumer_id".into(),
            ));
        }
        let consumer_id = u64::from_le_bytes(resp[..8].try_into().expect("8 bytes")) as u32;

        // The subject filter is applied on the subscription, not on the
        // consumer — matching how every other call site scopes delivery, and
        // letting workers on one queue narrow independently.
        self.subscribe(stream_id, consumer_id, &opts.filter).await
    }

    // ── manage ────────────────────────────────────────────────────────────────

    /// Create a stream.
    ///
    /// `idempotency_window_ms = 0` disables idempotency for this
    /// stream (legacy default — every publish is accepted, no dedup).
    /// A non-zero value turns on per-stream dedup: publishes carrying
    /// a `msg_id` header that the broker has seen for THIS stream
    /// within the window are rejected with `ErrorCode::IdempotencyDuplicate`.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_stream(
        &self,
        name: &[u8],
        filter: &[u8],
        max_msgs: u64,
        max_bytes: u64,
        max_age_secs: u64,
        replicas: u8,
        journal_kind: u8,
        retention: u8,
        discard: u8,
        idempotency_window_ms: u32,
    ) -> Result<Bytes, ClientError> {
        crate::manage::create_stream(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            name,
            filter,
            max_msgs,
            max_bytes,
            max_age_secs,
            replicas,
            journal_kind,
            retention,
            discard,
            idempotency_window_ms,
        )
        .await
    }

    pub async fn delete_stream(&self, name: &[u8]) -> Result<Bytes, ClientError> {
        crate::manage::delete_stream(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            name,
        )
        .await
    }

    pub async fn get_stream(&self, name: &[u8]) -> Result<Bytes, ClientError> {
        crate::manage::get_stream(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            name,
        )
        .await
    }

    pub async fn purge_stream(&self, name: &[u8]) -> Result<Bytes, ClientError> {
        crate::manage::purge_stream(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            name,
        )
        .await
    }

    pub async fn drain_subject(&self, name: &[u8], subject: &[u8]) -> Result<Bytes, ClientError> {
        crate::manage::drain_subject(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            name,
            subject,
        )
        .await
    }

    /// Tombstone a single message by sequence number.
    ///
    /// The broker marks the entry as deleted so it will never be delivered
    /// to any consumer. Returns `Ok` with `ref_seq = 1` if found and
    /// tombstoned, `ref_seq = 0` if the sequence was not found or already
    /// tombstoned.
    pub async fn delete_message(&self, name: &[u8], seq: u64) -> Result<Bytes, ClientError> {
        crate::manage::delete_message(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            name,
            seq,
        )
        .await
    }

    pub async fn list_streams(&self, offset: u32, limit: u32) -> Result<Bytes, ClientError> {
        crate::manage::list_streams(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            offset,
            limit,
        )
        .await
    }

    /// Create a consumer with no per-subject inflight limits.
    ///
    /// This is the common case. Use [`Client::create_consumer_with_limits`]
    /// to attach per-subject `max_inflight` caps. Per-subject limits are
    /// only enforced by the server with `ack_policy == AckPolicy::Explicit
    /// (1)`; setting them on a fire-and-forget consumer is silently
    /// dropped server-side.
    ///
    /// `group` must be non-empty — it is passed through verbatim and the
    /// broker rejects an empty one. See
    /// [`Client::create_consumer_with_limits`] for why the default is not
    /// applied here, and [`crate::ConsumerBuilder`] for the path that does
    /// apply it.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_consumer(
        &self,
        stream_id: u32,
        name: &[u8],
        group: &[u8],
        subject: &[u8],
        max_inflight: u16,
        ack_policy: u8,
        deliver_policy: u8,
        deliver_mode: u8,
        ack_wait_ms: u32,
        start_seq: u64,
    ) -> Result<Bytes, ClientError> {
        self.create_consumer_with_limits(
            stream_id,
            name,
            group,
            subject,
            max_inflight,
            ack_policy,
            deliver_policy,
            deliver_mode,
            ack_wait_ms,
            start_seq,
            &[],
        )
        .await
    }

    /// Create a consumer with per-subject inflight limits.
    ///
    /// Each `(pattern, max_inflight)` entry caps the number of in-flight
    /// (delivered, unacked) messages per subject matching `pattern`. Wildcards
    /// (`*`, `>`) are supported. Only effective with `ack_policy ==
    /// AckPolicy::Explicit (1)`; otherwise silently ignored by the server.
    ///
    /// **`group` is mandatory and is NOT defaulted here.** This method is
    /// the raw wire-level escape hatch: every argument is encoded
    /// verbatim, so `b""` really does put an empty group on the wire, and
    /// the broker rejects it with `InvalidConsumerConfig`. Filling the
    /// group in is the client's job — use [`crate::ConsumerBuilder`],
    /// which applies the shared rule (group, else consumer name, else
    /// stream name), or pass a group explicitly.
    ///
    /// The rule cannot live here without also making the broker's
    /// empty-group guard unreachable from Rust, and the default it would
    /// apply (`name`) is exactly what `ConsumerBuilder` already does one
    /// layer up.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_consumer_with_limits(
        &self,
        stream_id: u32,
        name: &[u8],
        group: &[u8],
        subject: &[u8],
        max_inflight: u16,
        ack_policy: u8,
        deliver_policy: u8,
        deliver_mode: u8,
        ack_wait_ms: u32,
        start_seq: u64,
        subject_limits: &[arbitro_proto::v2::manager::SubjectLimit<'_>],
    ) -> Result<Bytes, ClientError> {
        crate::manage::create_consumer(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            stream_id,
            name,
            group,
            subject,
            max_inflight,
            ack_policy,
            deliver_policy,
            deliver_mode,
            ack_wait_ms,
            start_seq,
            subject_limits,
        )
        .await
    }

    /// Delete a consumer server-side and retire its local routes.
    ///
    /// The routing table is dropped only after the broker confirms, so a
    /// failed delete leaves a consumer that still exists reachable. Leaving
    /// the routes behind would strand them until teardown, and let them catch
    /// deliveries if the id were ever recycled.
    pub async fn delete_consumer(&self, consumer_id: u32) -> Result<Bytes, ClientError> {
        let resp = crate::manage::delete_consumer(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            consumer_id,
        )
        .await?;
        self.inner.subscriptions.remove_consumer(consumer_id);
        Ok(resp)
    }

    /// Pause delivery for a consumer (broker action `0x0506`). The broker
    /// stops dispatching until [`Client::resume_consumer`] is called.
    pub async fn pause_consumer(&self, consumer_id: u32) -> Result<Bytes, ClientError> {
        crate::manage::pause_consumer(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            consumer_id,
        )
        .await
    }

    /// Resume delivery for a paused consumer (broker action `0x0507`).
    pub async fn resume_consumer(&self, consumer_id: u32) -> Result<Bytes, ClientError> {
        crate::manage::resume_consumer(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            consumer_id,
        )
        .await
    }

    pub async fn get_consumer(&self, stream_id: u32, name: &[u8]) -> Result<Bytes, ClientError> {
        crate::manage::get_consumer(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            stream_id,
            name,
        )
        .await
    }

    pub async fn list_consumers(
        &self,
        stream_id: u32,
        offset: u32,
        limit: u32,
    ) -> Result<Bytes, ClientError> {
        crate::manage::list_consumers(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            stream_id,
            offset,
            limit,
        )
        .await
    }

    // ── L13: convenience helpers mirroring the TS client ──────────────

    /// L13: `true` iff the broker has a stream registered under `name`.
    /// Wraps `get_stream` and treats `StreamNotFound` as a negative answer
    /// (any other wire error or transport failure propagates).
    pub async fn stream_exists(&self, name: &[u8]) -> Result<bool, ClientError> {
        match self.get_stream(name).await {
            Ok(_) => Ok(true),
            Err(ClientError::Broker {
                code: arbitro_proto::error::ErrorCode::StreamNotFound,
            }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// L13: `true` iff the broker has a consumer registered under
    /// `(stream_id, name)`. Same StreamNotFound/ConsumerNotFound mapping
    /// as `stream_exists`.
    pub async fn consumer_exists(&self, stream_id: u32, name: &[u8]) -> Result<bool, ClientError> {
        match self.get_consumer(stream_id, name).await {
            Ok(_) => Ok(true),
            Err(ClientError::Broker {
                code: arbitro_proto::error::ErrorCode::ConsumerNotFound,
            }) => Ok(false),
            Err(ClientError::Broker {
                code: arbitro_proto::error::ErrorCode::StreamNotFound,
            }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// L13: create-or-return-existing variant of `create_stream`. Returns
    /// `Ok(true)` when the stream was freshly created, `Ok(false)` when an
    /// existing definition was returned. `StreamAlreadyExists` is treated
    /// as the idempotent path.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_stream(
        &self,
        name: &[u8],
        filter: &[u8],
        max_msgs: u64,
        max_bytes: u64,
        max_age_secs: u64,
        replicas: u8,
        journal_kind: u8,
        retention: u8,
        discard: u8,
        idempotency_window_ms: u32,
    ) -> Result<bool, ClientError> {
        match self
            .create_stream(
                name,
                filter,
                max_msgs,
                max_bytes,
                max_age_secs,
                replicas,
                journal_kind,
                retention,
                discard,
                idempotency_window_ms,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(ClientError::Broker {
                code: arbitro_proto::error::ErrorCode::StreamAlreadyExists,
            }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// L13: create-or-return-existing variant of `create_consumer`. Same
    /// `Ok(true)`/`Ok(false)` semantics as `upsert_stream`. Treats
    /// `ConsumerAlreadyExists` as the idempotent path.
    ///
    /// Like [`Client::create_consumer`], `group` is passed through
    /// verbatim and must be non-empty.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_consumer(
        &self,
        stream_id: u32,
        name: &[u8],
        group: &[u8],
        subject: &[u8],
        max_inflight: u16,
        ack_policy: u8,
        deliver_policy: u8,
        deliver_mode: u8,
        ack_wait_ms: u32,
        start_seq: u64,
    ) -> Result<bool, ClientError> {
        match self
            .create_consumer(
                stream_id,
                name,
                group,
                subject,
                max_inflight,
                ack_policy,
                deliver_policy,
                deliver_mode,
                ack_wait_ms,
                start_seq,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(ClientError::Broker {
                code: arbitro_proto::error::ErrorCode::ConsumerAlreadyExists,
            }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Live pending-ack count for one consumer (broker round-trip).
    ///
    /// Returns the number of messages this consumer has been delivered
    /// but not yet acked. Equivalent to NATS JetStream's
    /// `num_ack_pending`. Reads engine state via one O(1) Vec lookup
    /// per shard; cheap enough to poll from a dashboard.
    pub async fn get_pending(&self, consumer_id: u32) -> Result<u64, ClientError> {
        let resp = crate::manage::consumer_stats(
            &self.inner.pool,
            &self.inner.pending,
            &self.inner.seq_alloc,
            consumer_id,
        )
        .await?;
        // RepOk body is 8 bytes — server packs the u64 count in place of ref_seq.
        if resp.len() < 8 {
            return Err(ClientError::Proto(
                arbitro_proto::error::ProtoError::BufferTooShort {
                    need: 8,
                    have: resp.len() as u32,
                },
            ));
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

    /// Total deferred-ack entries currently buffered in the client-side
    /// ack-reliability hot layer, across all consumers.
    #[inline]
    pub fn pending_hot_count(&self) -> u64 {
        self.inner.ackrel.hot_count()
    }

    /// Age in milliseconds of the oldest deferred ack still buffered in
    /// the hot layer. `0` if nothing is pending.
    #[inline]
    pub fn oldest_pending_age_ms(&self) -> u64 {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.inner.ackrel.oldest_pending_age_ms(now_ms)
    }

    // ── Cron scheduling ────────────────────────────────────────────────

    /// Start building a cron job registration.
    ///
    /// ```rust,no_run
    /// # use arbitro_client_tokio::Client;
    /// # async fn example(client: &Client) {
    /// let cron = client.cron(b"billing")
    ///     .every(b"0 0 1 * *")
    ///     .run(|ctx| async move {
    ///         println!("fire #{}", ctx.fire_count);
    ///     })
    ///     .await
    ///     .unwrap();
    /// cron.stop().await.unwrap();
    /// # }
    /// ```
    pub fn cron<'a>(&'a self, name: &[u8]) -> crate::cron::CronBuilder<'a> {
        crate::cron::CronBuilder::new(self, name)
    }

    // ── Workflow orchestration ─────────────────────────────────────────

    /// Start building a workflow pipeline. Uses streams + consumer groups
    /// internally — the broker has no workflow-specific code.
    pub fn workflow(&self, name: &[u8]) -> crate::workflow::WorkflowBuilder {
        crate::workflow::WorkflowBuilder::new(self.clone(), name)
    }
}
