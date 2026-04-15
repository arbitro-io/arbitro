//! ArbitroDB — graph-based runtime engine.
//!
//! Root module. Level 8 — depends on everything, nothing depends on it.

// Level 0 — no internal deps
pub mod common;
pub mod error;
pub mod types;

// Level 2-3 — graph + edges
pub mod edge;
pub mod graph;

// Level 3 — mechanisms
pub mod idempotency;
pub mod inflight;
pub mod ready;

// Level 0 — metrics (atomic counters, leaf module)
pub mod metrics;

// Level 1 — batch + reply + fanout + wire + command vocabulary
pub mod batch;
pub mod command;
pub mod fanout;
pub mod reply;
pub mod wire;

// Level 4 — plugins
pub mod plugin;

// Level 5 — catalog
pub mod catalog;

// Level 6 — context
pub mod context;

// Level 7 — runtime + admin
pub mod admin;
pub mod runtime;

// ── Re-exports for ergonomic access from the protocol layer ────────────────
//
// Things callers reach for constantly, hoisted to the crate root so they
// don't have to know the internal module layout.

pub use batch::{PublishBatchOwned, PublishEntryOwned};
pub use command::{Command, DropReason, MsgRef, StreamSeq};
pub use inflight::InFlightScope;
pub use metrics::{EngineMetrics, MetricsSnapshot};

// ── ArbitroEngine — ergonomic facade ────────────────────────────────────────

use context::EngineContext;
use types::*;

/// ArbitroEngine builder. Registers plugins, edges, and initial catalog.
pub struct EngineBuilder {
    scheduler_tick_ms: u64,
}

impl EngineBuilder {
    pub fn new() -> Self {
        Self {
            scheduler_tick_ms: 100,
        }
    }

    /// Set scheduler tick resolution in milliseconds (default 100ms).
    pub fn scheduler_tick_ms(mut self, ms: u64) -> Self {
        self.scheduler_tick_ms = ms;
        self
    }

    /// Build the engine with all subsystems initialized.
    pub fn build(self) -> ArbitroEngine {
        // Core subsystems (credit, events, scheduler) live as direct
        // fields on `EngineContext`. No registry, no TypeId dispatch —
        // hot-path access is a single field load.
        let ctx = EngineContext::with_scheduler_tick_ms(self.scheduler_tick_ms);
        ArbitroEngine { ctx }
    }
}

impl Default for EngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// The ArbitroEngine — graph-based runtime engine.
///
/// All operations go through this struct. Single-threaded engine core:
/// `&mut self` is the synchronization.
pub struct ArbitroEngine {
    ctx: EngineContext,
}

impl ArbitroEngine {
    /// Create a new engine with default settings.
    pub fn new() -> Self {
        EngineBuilder::new().build()
    }

    /// Access the engine context directly (for advanced use).
    #[inline]
    pub fn ctx(&self) -> &EngineContext {
        &self.ctx
    }

    /// Access the engine context mutably (for advanced use).
    #[inline]
    pub fn ctx_mut(&mut self) -> &mut EngineContext {
        &mut self.ctx
    }

    // ── Catalog (management path) ───────────────────────────────────────

    /// Create or ensure a stream exists.
    pub fn ensure_stream(&mut self, config: catalog::StreamConfig) -> error::EngineResult<SlabKey> {
        self.ctx.catalog.ensure_stream(&mut self.ctx.graph, config)
    }

    /// Remove a stream from the catalog (lightweight — does NOT cascade).
    /// Prefer remove_stream_full() which also cleans consumers, subscriptions,
    /// queues, idempotency, and ready state.
    pub fn remove_stream(&mut self, stream_id: StreamId) -> error::EngineResult<()> {
        self.ctx
            .catalog
            .remove_stream(&mut self.ctx.graph, stream_id)
    }

    /// Fully remove a stream and all owned entities (consumers, subscriptions,
    /// queues, idempotency window, ready state). Cascading delete.
    ///
    /// For each consumer on this stream:
    ///   1. drain_consumer() — release pending messages and bindings
    ///   2. remove_consumer() — delete from catalog, graph, edges, match table
    ///
    /// Then drain and remove all queues, clean ready state and idempotency.
    pub fn remove_stream_full(
        &mut self,
        stream_id: StreamId,
        mode: DrainMode,
    ) -> batch::DrainReport {
        let mut report = batch::DrainReport::default();

        // 1. Collect consumer IDs for this stream (via edge index)
        let consumer_keys = self.ctx.edges.consumers_by_stream.take(&stream_id);

        // Resolve ConsumerId + QueueId from each consumer key before mutating
        let mut consumer_ids = Vec::with_capacity(consumer_keys.len());
        let mut queue_ids = std::collections::HashSet::new();
        for key in &consumer_keys {
            if let Ok(node) = self.ctx.graph.get_consumer(*key) {
                consumer_ids.push(node.consumer_id);
                queue_ids.insert(node.queue_id);
            }
        }

        // 2. Drain + remove each consumer (cascades to subscriptions)
        for consumer_id in consumer_ids {
            let sub = runtime::drain::drain_consumer(&mut self.ctx, consumer_id, mode);
            report.pending_released += sub.pending_released;
            report.pending_requeued += sub.pending_requeued;
            report.bindings_removed += sub.bindings_removed;
            let _ = self.ctx.catalog.remove_consumer(
                &mut self.ctx.graph,
                &mut self.ctx.edges,
                consumer_id,
            );
        }

        // 3. Drain + remove each queue
        for queue_id in queue_ids {
            let sub = runtime::drain::drain_queue(&mut self.ctx, queue_id, mode);
            report.pending_released += sub.pending_released;
            report.pending_requeued += sub.pending_requeued;
            // Remove ready ring entirely (not just clear)
            self.ctx.ready.remove_queue(queue_id);
            // Remove queue from catalog + graph
            let _ = self.ctx.catalog.remove_queue(&mut self.ctx.graph, queue_id);
        }

        // 4. Remove stream from catalog + graph + match table
        let _ = self
            .ctx
            .catalog
            .remove_stream(&mut self.ctx.graph, stream_id);

        // 5. Remove idempotency window (~4MB per stream)
        self.ctx.idempotency.remove(&stream_id);

        report
    }

    /// Create or ensure a consumer exists.
    pub fn ensure_consumer(
        &mut self,
        config: catalog::ConsumerConfig,
    ) -> error::EngineResult<SlabKey> {
        self.ctx
            .catalog
            .ensure_consumer(&mut self.ctx.graph, &mut self.ctx.edges, config)
    }

    /// Remove a consumer from the catalog, graph, and edge indexes.
    /// Call drain_consumer() first to release pending messages and bindings.
    pub fn remove_consumer(&mut self, consumer_id: ConsumerId) -> error::EngineResult<()> {
        self.ctx
            .catalog
            .remove_consumer(&mut self.ctx.graph, &mut self.ctx.edges, consumer_id)
    }

    /// Remove a subscription from the catalog, graph, edges, and match table.
    /// Call drain_subscription() first to release pending messages and bindings.
    pub fn remove_subscription(
        &mut self,
        subscription_id: SubscriptionId,
    ) -> error::EngineResult<()> {
        self.ctx.catalog.remove_subscription(
            &mut self.ctx.graph,
            &mut self.ctx.edges,
            subscription_id,
        )
    }

    /// Create or ensure a subscription exists.
    pub fn ensure_subscription(
        &mut self,
        config: catalog::SubscriptionConfig,
    ) -> error::EngineResult<SlabKey> {
        self.ctx
            .catalog
            .ensure_subscription(&mut self.ctx.graph, &mut self.ctx.edges, config)
    }

    // ── Runtime (hot path) ──────────────────────────────────────────────

    /// Publish a batch of messages. Fire-and-forget fanout.
    /// Fanout notifications are in ctx.fanout — drain separately.
    #[inline]
    pub fn publish(&mut self, batch: &batch::PublishBatch) -> reply::RepPublish {
        runtime::publish::on_publish_batch(&mut self.ctx, batch)
    }

    /// Drain pending fanout notifications for the protocol layer.
    #[inline]
    pub fn drain_fanout(&mut self) -> fanout::FanoutDrain<'_> {
        self.ctx.fanout.take()
    }

    /// Enqueue a store entry directly into the ready queue using its store seq.
    ///
    /// Used by `seed_from_store` to replay historical messages to a new
    /// consumer without re-publishing through the engine (which would
    /// reassign seqs). Looks up every queue matching the subject in the
    /// stream's match table and pushes to `ctx.ready`.
    ///
    /// **Correctness:** no cap on matched queues — every queue receives
    /// the seq. Delegates to `runtime::seed::enqueue_ready` which owns
    /// the split-borrow + dedup scratch.
    #[inline]
    pub fn enqueue_ready(
        &mut self,
        stream_id: types::StreamId,
        subject: &[u8],
        subject_hash: u32,
        seq: u64,
    ) -> usize {
        runtime::seed::enqueue_ready(&mut self.ctx, stream_id, subject, subject_hash, seq)
    }

    /// Batch-enqueue multiple store entries. Resolves patterns once per
    /// unique subject, resolves each queue ring once. Returns
    /// `(entries, no_match, queues_pushed)` for metrics flush.
    pub fn enqueue_ready_batch(
        &mut self,
        stream_id: types::StreamId,
        items: &[(&[u8], u32, u64)],
    ) -> (u64, u64, u64) {
        runtime::seed::enqueue_ready_batch(&mut self.ctx, stream_id, items)
    }

    /// Fast-path batch enqueue for subjects whose patterns are already resolved.
    /// Takes `(subject_hash, seq)` pairs — no subject bytes needed.
    pub fn enqueue_ready_seed_batch(
        &mut self,
        stream_id: types::StreamId,
        items: &[(u32, u64)],
    ) -> (u64, u64, u64) {
        runtime::seed::enqueue_ready_seed_batch(&mut self.ctx, stream_id, items)
    }

    /// Flush accumulated seed metrics in a single batch.
    pub fn flush_seed_metrics(&self, entries: u64, no_match: u64, queues_pushed: u64) {
        runtime::seed::flush_seed_metrics(&self.ctx, entries, no_match, queues_pushed);
    }

    /// Claim (deliver) messages from a queue.
    ///
    /// Callers MUST supply cached `subscription_id` and `binding_id`. The
    /// shard drainer caches both in `ActiveBinding` at subscribe time;
    /// tests / cold-path probes resolve them via
    /// [`runtime::claim::resolve_ids_for_batch`].
    ///
    /// **Cache invalidation contract:** if the caller performs any
    /// subscription/binding topology change (`unbind`, `remove_subscription`,
    /// etc.), it MUST refresh / drop its cached hints before the next
    /// `claim` call. Debug builds panic via `debug_assert_eq!` on stale
    /// hints; release builds would silently wire pending entries to the
    /// wrong edge indexes.
    ///
    /// Returns a reference to the pre-allocated scratch buffer.
    #[inline]
    pub fn claim(
        &mut self,
        batch: &batch::ClaimBatch,
        subscription_id: SubscriptionId,
        binding_id: BindingId,
    ) -> &reply::ScratchReply<batch::ClaimedEntry> {
        runtime::claim::on_claim_batch(&mut self.ctx, batch, subscription_id, binding_id)
    }

    /// Acknowledge messages.
    /// Returns a reference to the pre-allocated scratch buffer.
    #[inline]
    pub fn ack(&mut self, batch: &batch::AckBatch) -> &reply::ScratchReply<batch::AckResult> {
        runtime::ack::on_ack_batch(&mut self.ctx, batch)
    }

    /// Negatively acknowledge messages (requeue for redelivery).
    /// Returns a reference to the pre-allocated scratch buffer.
    #[inline]
    pub fn nack(&mut self, batch: &batch::NackBatch) -> &reply::ScratchReply<batch::NackResult> {
        runtime::ack::on_nack_batch(&mut self.ctx, batch)
    }

    /// Bind subscriptions to connections.
    #[inline]
    pub fn bind(&mut self, batch: &batch::BindBatch) -> reply::RepOk<SlabKey> {
        runtime::bind::on_bind_batch(&mut self.ctx, batch)
    }

    // ── Admin (management path) ─────────────────────────────────────────

    /// Open a new connection.
    pub fn open_connection(&mut self, req: &batch::OpenConnectionReq) -> SlabKey {
        admin::open_connection(&mut self.ctx, req)
    }

    /// Drain a connection: release all pending, remove bindings.
    pub fn drain_connection(&mut self, req: &batch::DrainConnectionReq) -> batch::DrainReport {
        runtime::drain::drain_connection(&mut self.ctx, req)
    }

    /// Remove a connection from the graph and edge indexes. O(1).
    /// Call drain_connection() first to release pending messages and bindings.
    pub fn remove_connection(&mut self, connection_id: ConnectionId) {
        if let Some(key) = self.ctx.connection_keys.remove(&connection_id) {
            if let Ok(node) = self.ctx.graph.remove_connection(key) {
                self.ctx
                    .edges
                    .connections_by_node
                    .remove(&node.node_id, &key);
            }
        }
    }

    /// Drain a subscription: release pending, remove bindings.
    pub fn drain_subscription(
        &mut self,
        subscription_id: SubscriptionId,
        mode: DrainMode,
    ) -> batch::DrainReport {
        runtime::drain::drain_subscription(&mut self.ctx, subscription_id, mode)
    }

    /// Drain a consumer: release all pending, remove bindings.
    pub fn drain_consumer(
        &mut self,
        consumer_id: ConsumerId,
        mode: DrainMode,
    ) -> batch::DrainReport {
        runtime::drain::drain_consumer(&mut self.ctx, consumer_id, mode)
    }

    /// Drain a queue: release all pending, clear ready state.
    pub fn drain_queue(&mut self, queue_id: QueueId, mode: DrainMode) -> batch::DrainReport {
        runtime::drain::drain_queue(&mut self.ctx, queue_id, mode)
    }

    /// Drain a node: drain all connections on this node.
    pub fn drain_node(
        &mut self,
        node_id: NodeId,
        mode: DrainMode,
        now: Timestamp,
    ) -> batch::DrainReport {
        runtime::drain::drain_node(&mut self.ctx, node_id, mode, now)
    }

    /// Set max inflight per subject by pattern on a stream. Management path.
    ///
    /// Example: `set_max_subject_inflight(stream, b"message.qr.>", 1)` means each
    /// concrete subject matching `message.qr.>` can have at most 1 in-flight.
    pub fn set_max_subject_inflight(
        &mut self,
        stream_id: StreamId,
        pattern: &[u8],
        max_inflight: u32,
    ) -> error::EngineResult<()> {
        self.ctx
            .catalog
            .set_max_subject_inflight(stream_id, pattern, max_inflight)?;
        // Flip the subject-tracking gate on InFlightCounters so inc/dec_pending
        // start maintaining the subject HashMap. Sticky — never unset.
        self.ctx.inflight.enable_subject_tracking();
        Ok(())
    }

    /// List all streams on this engine. Returns `(StreamId, name)` pairs.
    pub fn list_streams(&self) -> Vec<(StreamId, Vec<u8>)> {
        let ids = self.ctx.catalog.stream_ids();
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Ok(key) = self.ctx.catalog.stream_key(id) {
                if let Ok(node) = self.ctx.graph.get_stream(key) {
                    out.push((id, node.name.clone()));
                }
            }
        }
        out
    }

    /// List all consumers on this engine.
    /// Returns `(consumer_id, stream_id, queue_id, paused)` tuples.
    pub fn list_consumers(&self) -> Vec<(ConsumerId, StreamId, QueueId, bool)> {
        let ids = self.ctx.catalog.consumer_ids();
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Ok(key) = self.ctx.catalog.consumer_key(id) {
                if let Ok(node) = self.ctx.graph.get_consumer(key) {
                    out.push((id, node.stream_id, node.queue_id, node.paused));
                }
            }
        }
        out
    }

    /// Pause a consumer.
    pub fn pause_consumer(&mut self, consumer_id: ConsumerId) -> bool {
        admin::pause_consumer(&mut self.ctx, consumer_id)
    }

    /// Resume a consumer.
    pub fn resume_consumer(&mut self, consumer_id: ConsumerId) -> bool {
        admin::resume_consumer(&mut self.ctx, consumer_id)
    }

    /// Drain event bus.
    pub fn drain_events(&mut self) -> Vec<plugin::event_bus::EngineEvent> {
        self.ctx.events.drain()
    }

    /// Tick the scheduler and return expired deadlines.
    pub fn tick(&mut self, now_ms: u64, expired: &mut Vec<plugin::scheduler::ExpiredDeadline>) {
        self.ctx.scheduler.tick(now_ms, expired);
    }

    // ── Inflight introspection (worker / drain hot path) ───────────────
    //
    // The worker loop checks capacity before calling `claim()` so it can
    // skip a no-op batch entirely. All accessors here are O(1) and split
    // into two tiers:
    //
    //   1. **Limit lookups** (`*_max_inflight`) hit the catalog + graph
    //      chain — ~10-20 ns. The worker should call them ONCE at
    //      subscribe time and cache the result in its own `ActiveBinding`.
    //      Limits change only on management operations.
    //
    //   2. **Live counter reads** (`*_inflight`, `*_has_capacity`) hit a
    //      single Vec or HashMap slot — ~2-10 ns. Safe to call inside the
    //      hot loop on every iteration.
    //
    // Typical worker loop:
    // ```ignore
    // // cached at subscribe:
    // let max = engine.consumer_max_inflight(cid).unwrap_or(u32::MAX);
    //
    // loop {
    //     if !engine.consumer_has_capacity(cid, max) { break; }
    //     engine.claim(&batch, sub, bind);
    // }
    // ```

    /// Live consumer inflight count. **O(1) Vec read, ~2 ns.** Safe to
    /// call from the worker hot loop. Returns 0 for unknown consumers.
    #[inline]
    pub fn consumer_inflight(&self, consumer_id: types::ConsumerId) -> u32 {
        self.ctx
            .inflight
            .get(inflight::InFlightScope::Consumer, consumer_id.raw())
    }

    /// Live queue inflight count. O(1) Vec read.
    #[inline]
    pub fn queue_inflight(&self, queue_id: types::QueueId) -> u32 {
        self.ctx
            .inflight
            .get(inflight::InFlightScope::Queue, queue_id.raw())
    }

    /// Live subject inflight count. O(1) HashMap read, ~5-10 ns.
    /// Returns 0 when subject tracking is disabled (no `set_max_subject_inflight`
    /// call has happened anywhere) — the worker can call this unconditionally.
    #[inline]
    pub fn subject_inflight(&self, subject_hash: u32) -> u32 {
        self.ctx
            .inflight
            .get(inflight::InFlightScope::Subject, subject_hash)
    }

    /// Configured `max_inflight` for a consumer. **Cold path** — catalog
    /// lookup + graph chain. Caller MUST cache the result; do not call
    /// inside the worker loop. Returns `None` if the consumer is unknown.
    pub fn consumer_max_inflight(&self, consumer_id: types::ConsumerId) -> Option<u32> {
        let key = self.ctx.catalog.consumer_key(consumer_id).ok()?;
        self.ctx
            .graph
            .get_consumer(key)
            .ok()
            .map(|n| n.max_inflight)
    }

    /// Configured `paused` flag for a consumer. **Cold path** — catalog +
    /// graph. The hot loop normally caches the unpaused state at the start
    /// of a batch; this is for the worker's "should I poll at all?" check.
    pub fn consumer_paused(&self, consumer_id: types::ConsumerId) -> bool {
        match self.ctx.catalog.consumer_key(consumer_id) {
            Ok(key) => self
                .ctx
                .graph
                .get_consumer(key)
                .map(|n| n.paused)
                .unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Configured `max_subject_inflight` for a subject hash on a stream.
    /// **Cold path** — match table lookup. Caller should cache this
    /// per (stream, subject_hash) the first time it's seen.
    /// Returns `None` if no pattern on that stream limits the subject.
    pub fn subject_max_inflight(
        &self,
        stream_id: types::StreamId,
        subject_hash: u32,
    ) -> Option<u32> {
        self.ctx
            .catalog
            .max_subject_inflight(stream_id, subject_hash)
    }

    /// Fast capacity check: `consumer_inflight < max`. **~3 ns** (one Vec
    /// read + compare). Caller passes the cached `max_inflight` so the
    /// hot loop never re-reads the catalog.
    #[inline]
    pub fn consumer_has_capacity(&self, consumer_id: types::ConsumerId, max_inflight: u32) -> bool {
        self.ctx.inflight.has_capacity(
            inflight::InFlightScope::Consumer,
            consumer_id.raw(),
            max_inflight,
        )
    }

    /// Spare consumer capacity (`max - inflight`, saturated). **~3 ns.**
    /// Useful when the worker wants to size its next claim batch:
    /// `batch.max_items = engine.consumer_capacity_remaining(cid, max)`.
    #[inline]
    pub fn consumer_capacity_remaining(
        &self,
        consumer_id: types::ConsumerId,
        max_inflight: u32,
    ) -> u32 {
        max_inflight.saturating_sub(self.consumer_inflight(consumer_id))
    }

    /// Fast subject capacity check: `subject_inflight < max`. ~5-10 ns
    /// (HashMap read + compare). Caller passes the cached per-subject max.
    #[inline]
    pub fn subject_has_capacity(&self, subject_hash: u32, max_inflight: u32) -> bool {
        self.ctx
            .inflight
            .has_capacity(inflight::InFlightScope::Subject, subject_hash, max_inflight)
    }

    /// Whether subject-scope inflight is being tracked at all. When
    /// `false`, every `subject_inflight` call returns 0 and the worker
    /// can skip the per-subject capacity check entirely. The flag is
    /// sticky (flipped once when the first `set_max_subject_inflight`
    /// is called) — safe to cache in the worker.
    #[inline]
    pub fn subject_tracking_enabled(&self) -> bool {
        self.ctx.inflight.is_tracking_subject()
    }

    // ── Observability ───────────────────────────────────────────────────

    /// Borrow the atomic counter set for cross-thread observability.
    ///
    /// `&metrics::EngineMetrics` is `Send + Sync`: a separate metrics
    /// thread can `snapshot()` while the engine thread keeps doing
    /// `fetch_add(_, Relaxed)`. This is the **only** sanctioned form of
    /// hot-path observability in the engine (`performance.md` §15).
    #[inline]
    pub fn metrics(&self) -> &metrics::EngineMetrics {
        &self.ctx.metrics
    }

    /// Point-in-time snapshot of all counters as plain `u64`s — convenient
    /// for pushing to Prometheus / StatsD / logs from the protocol layer.
    #[inline]
    pub fn metrics_snapshot(&self) -> metrics::MetricsSnapshot {
        self.ctx.metrics.snapshot()
    }

    // ── Command dispatch (kernel API, parallel to legacy path) ──────────
    //
    // `execute` / `execute_batch` are the forthcoming single entry point
    // for the Command-based kernel (see plan W3). They are wired in
    // parallel to `on_publish` / `on_ack` / `drain_fanout`; the server
    // drainer will switch to them in Fase 2 of the migration. Until then
    // these methods are observational only — they advance metrics, they
    // do not mutate graph / inflight / ready state.

    /// Apply a single `Command` to engine state.
    #[inline]
    pub fn execute(&mut self, cmd: &command::Command<'_>) {
        runtime::execute::apply(&mut self.ctx, cmd);
    }

    /// Apply a slice of `Command`s in order.
    #[inline]
    pub fn execute_batch(&mut self, cmds: &[command::Command<'_>]) {
        runtime::execute::apply_batch(&mut self.ctx, cmds);
    }
}

impl Default for ArbitroEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod engine_tests {
    use super::*;

    #[test]
    fn full_publish_claim_ack_cycle() {
        let mut engine = ArbitroEngine::new();

        // Setup catalog
        engine
            .ensure_stream(catalog::StreamConfig {
                id: StreamId(1),
                name: b"orders".to_vec(),
            })
            .unwrap();

        engine
            .ensure_consumer(catalog::ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 1000,
            })
            .unwrap();

        engine
            .ensure_subscription(catalog::SubscriptionConfig {
                id: SubscriptionId(1),
                stream_id: StreamId(1),
                consumer_id: ConsumerId(1),
                filters: vec![],
            })
            .unwrap();

        // Open connection
        engine.open_connection(&batch::OpenConnectionReq {
            connection_id: ConnectionId(100),
            node_id: NodeId(1),
            now: Timestamp::new(0),
        });

        // Bind
        let bind_entries = [batch::BindEntry {
            connection_id: ConnectionId(100),
            subscription_id: SubscriptionId(1),
        }];
        engine.bind(&batch::BindBatch {
            entries: &bind_entries,
            now: Timestamp::new(0),
        });

        // Publish 10 messages
        let subjects: Vec<Vec<u8>> = (0..10).map(|i| format!("order.{i}").into_bytes()).collect();
        let pub_entries: Vec<_> = (0..10)
            .map(|i| batch::PublishEntry {
                subject_hash: catalog::fnv1a_32(&subjects[i]),
                subject: &subjects[i],
                payload: PayloadRef::Borrowed(b"test-payload"),
                idempotency_key: 0,
                credits_cost: 1,
            })
            .collect();

        let result = engine.publish(&batch::PublishBatch {
            stream_id: StreamId(1),
            entries: &pub_entries,
            now: Timestamp::new(1_000_000),
        });

        // 10 fire-and-forget notifications
        assert_eq!(result.notified, 10);
        assert_eq!(result.queued, 0);

        // Protocol layer drains fanout queue — 1 per connection per message
        let drain = engine.drain_fanout();
        assert_eq!(drain.len(), 10);
        for entry in drain.entries() {
            assert_eq!(entry.connection_id, ConnectionId(100));
        }
        drop(drain);

        // Claim: protocol layer claims on behalf of connection.
        // Read result and release borrow before calling ack.
        let ack_entries: Vec<batch::AckEntry>;
        {
            let claim_batch = batch::ClaimBatch {
                queue_id: QueueId(1),
                connection_id: ConnectionId(100),
                consumer_id: ConsumerId(1),
                max_items: 10,
                now: Timestamp::new(2_000_000),
            };
            let (sub, bind) = runtime::claim::resolve_ids_for_batch(engine.ctx(), &claim_batch);
            let claimed = engine.claim(&claim_batch, sub, bind);
            assert_eq!(claimed.accepted, 10);
            ack_entries = claimed
                .entries()
                .iter()
                .map(|e| batch::AckEntry { seq: e.seq })
                .collect();
        }

        let ack_result = engine.ack(&batch::AckBatch {
            consumer_id: ConsumerId(1),
            entries: &ack_entries,
            now: Timestamp::new(3_000_000),
        });
        assert_eq!(ack_result.accepted, 10);

        // Verify clean state
        assert_eq!(
            engine
                .ctx()
                .inflight
                .get(inflight::InFlightScope::Consumer, 1),
            0
        );
        assert_eq!(
            engine.ctx().inflight.get(inflight::InFlightScope::Queue, 1),
            0
        );
    }

    #[test]
    fn builder_custom_scheduler() {
        // Just verifies the builder wires the tick_ms through and the
        // three core subsystems are reachable via direct field access.
        let mut engine = EngineBuilder::new().scheduler_tick_ms(50).build();
        let _ = &engine.ctx().credit;
        let _ = &engine.ctx().events;
        let _ = &engine.ctx().scheduler;
        let _ = engine.ctx_mut(); // sanity: mutable access compiles
    }
}
