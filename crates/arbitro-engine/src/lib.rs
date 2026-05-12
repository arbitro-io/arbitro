//! ArbitroEngine — pure oracle engine.
//!
//! Root module. `&mut self`, sync, zero locks, zero async, zero I/O,
//! zero threads. Only `AtomicU64::fetch_add(Relaxed)` for metrics.
//!
//! The engine answers queries (O(1): `has_demand`, `has_any_demand`,
//! `consumer_has_capacity`, `consumer_inflight`) and accepts mutations
//! via `execute(Command) -> DeltaEvents`. Admin operations (create/delete
//! stream/consumer, subscribe/unsubscribe, connection management) also
//! return `DeltaEvents`.
//!
//! Subject inflight + paused-state are tracked by the server (per-consumer
//! ownership) and never via the engine, so the engine exposes neither.

// Level 0 — no internal deps
pub mod common;
pub mod error;
pub mod types;

// Level 0 — metrics (atomic counters, leaf module)
pub mod metrics;

// Level 0 — events
pub mod events;

// Level 1 — command vocabulary
pub mod command;

// Level 3 — inflight counters (credits)
pub mod inflight;

// Level 5 — catalog (entity storage + match tables + bindings)
pub mod catalog;

// Level 6 — context
pub mod context;

// Level 7 — runtime
pub mod runtime;

// ── Re-exports for ergonomic access ─────────────────────────────────────────

pub use command::{AckEntry, Command, DeliveredEntry, DropReason};
pub use events::DeltaEvents;
pub use inflight::InFlightScope;
pub use metrics::{EngineMetrics, MetricsSnapshot};

/// Per-consumer state gauge — point-in-time picture of one consumer's
/// load. Sent over the shard mpsc by `consumer_states_snapshot()`.
///
/// `ack_pending` is the count of messages delivered to the consumer
/// but not yet acked. Use it as a NATS-style `num_ack_pending` gauge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerStateSnapshot {
    pub consumer_id: u32,
    pub stream_id:   u32,
    pub queue_id:    u32,
    pub paused:      bool,
    pub ack_pending: u32,
}

// ── ArbitroEngine — oracle facade ───────────────────────────────────────────

use context::EngineContext;
use types::*;

/// The ArbitroEngine — pure oracle, single-threaded.
///
/// All operations go through this struct. `&mut self` is the
/// synchronization — no locks needed.
pub struct ArbitroEngine {
    ctx: EngineContext,
}

impl ArbitroEngine {
    /// Create a new engine with default settings.
    pub fn new() -> Self {
        Self {
            ctx: EngineContext::new(),
        }
    }

    // ── Oracle queries (hot path, O(1)) ─────────────────────────────────

    /// Any stream has ≥1 active binding.
    #[inline]
    pub fn has_any_demand(&self) -> bool {
        self.ctx.catalog.has_any_demand()
    }

    /// This stream has ≥1 active binding.
    #[inline]
    pub fn has_demand(&self, stream_id: StreamId) -> bool {
        self.ctx.catalog.has_demand(stream_id)
    }

    /// Consumer has capacity for more in-flight messages. ~3 ns.
    #[inline]
    pub fn consumer_has_capacity(
        &self,
        consumer_id: ConsumerId,
        max_inflight: u32,
    ) -> bool {
        self.ctx.inflight.has_capacity(
            inflight::InFlightScope::Consumer,
            consumer_id.raw(),
            max_inflight,
        )
    }

    /// Live consumer inflight count. O(1) Vec read, ~2 ns.
    #[inline]
    pub fn consumer_inflight(&self, consumer_id: ConsumerId) -> u32 {
        self.ctx
            .inflight
            .get(inflight::InFlightScope::Consumer, consumer_id.raw())
    }

    // ── Execute (hot-path mutation, returns events) ─────────────────────

    /// Apply a single `Command` to engine state. Returns events for
    /// the worker to react to.
    #[must_use]
    #[inline]
    pub fn execute(&mut self, cmd: &command::Command<'_>) -> DeltaEvents {
        runtime::execute::apply(&mut self.ctx, cmd)
    }

    // ── Catalog admin (cold path, returns events) ───────────────────────

    /// Create or ensure a stream exists.
    pub fn create_stream(
        &mut self,
        config: catalog::StreamConfig,
    ) -> error::EngineResult<()> {
        self.ctx.catalog.ensure_stream(config)
    }

    /// Delete a stream. Retires all bindings on this stream, releasing
    /// inflight credits. Returns events with `bindings_retired` +
    /// `demand_became_idle` + `consumers_removed`.
    ///
    /// Cascade: every consumer attached to the deleted stream is also
    /// fully removed (entity, subscriptions, bindings). Their ids are
    /// reported in `events.consumers_removed` so the server can mirror
    /// the cleanup into the `NameRegistry` (drop wire-name → id +
    /// reverse indexes). Without that cascade, the next CreateConsumer
    /// with the same name on a recreated stream silently aliases to a
    /// defunct id pointing at a catalog slot this method just removed.
    #[must_use]
    pub fn delete_stream(&mut self, id: StreamId) -> DeltaEvents {
        let mut events = DeltaEvents::default();

        // Snapshot the consumer ids BEFORE we start removing entities —
        // the iteration runs on the catalog state at call time.
        let consumer_ids = self.ctx.catalog.consumers_for_stream(id);

        // Remove each consumer's entity + subscriptions + retire its
        // bindings. We do this via the same path that `delete_consumer`
        // uses so the invariants (bindings retired → inflight released)
        // stay consistent regardless of whether the trigger was an
        // explicit DeleteConsumer wire frame or a cascade.
        for cid in &consumer_ids {
            runtime::retire::retire_bindings_for_consumer(&mut self.ctx, *cid, &mut events);
            let sub_ids = self.ctx.catalog.subscriptions_for_consumer(*cid);
            for sid in sub_ids {
                let _ = self.ctx.catalog.remove_subscription_entity(sid);
            }
            let _ = self.ctx.catalog.remove_consumer_entity(*cid);
        }
        events.consumers_removed.extend(consumer_ids);

        // Retire any remaining stream-level bindings (defensive — most
        // bindings should already be retired through the per-consumer
        // path above; this catches anything the engine ties to the
        // stream itself rather than to a specific consumer).
        runtime::retire::retire_bindings_for_stream(&mut self.ctx, id, &mut events);

        // Finally, drop the stream entity.
        let _ = self.ctx.catalog.remove_stream_entity(id);
        events
    }

    /// Create or ensure a consumer exists.
    pub fn create_consumer(
        &mut self,
        config: catalog::ConsumerConfig,
    ) -> error::EngineResult<()> {
        self.ctx.catalog.ensure_consumer(config)
    }

    /// Delete a consumer. Retires all bindings + subscriptions for this
    /// consumer. Reports the removed id in `events.consumers_removed`
    /// so the server mirrors the cleanup into NameRegistry — same
    /// signal the cascade from `delete_stream` produces.
    #[must_use]
    pub fn delete_consumer(&mut self, id: ConsumerId) -> DeltaEvents {
        let mut events = DeltaEvents::default();

        // Retire bindings first (releases inflight).
        runtime::retire::retire_bindings_for_consumer(&mut self.ctx, id, &mut events);

        // Remove subscriptions for this consumer.
        let sub_ids = self.ctx.catalog.subscriptions_for_consumer(id);
        for sid in sub_ids {
            let _ = self.ctx.catalog.remove_subscription_entity(sid);
        }

        // Remove consumer entity.
        if self.ctx.catalog.remove_consumer_entity(id).is_ok() {
            events.consumers_removed.push(id);
        }

        events
    }

    /// Create or ensure a subscription exists (subject filter → consumer).
    pub fn create_subscription(
        &mut self,
        config: catalog::SubscriptionConfig,
    ) -> error::EngineResult<()> {
        self.ctx.catalog.ensure_subscription(config)
    }

    /// Subscribe: bind a subscription to a connection. Creates a binding,
    /// updates match table with connection_id, increments demand.
    #[must_use]
    pub fn subscribe(
        &mut self,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
    ) -> (error::EngineResult<BindingId>, DeltaEvents) {
        let mut events = DeltaEvents::default();
        let result =
            self.ctx
                .catalog
                .subscribe(connection_id, subscription_id, &mut events);
        (result, events)
    }

    /// Unsubscribe: retire a binding.
    #[must_use]
    pub fn unsubscribe(&mut self, binding_id: BindingId) -> DeltaEvents {
        let mut events = DeltaEvents::default();
        runtime::retire::retire_binding(&mut self.ctx, binding_id, &mut events);
        events
    }

    /// Register a new connection.
    pub fn open_connection(&mut self, connection_id: ConnectionId, node_id: NodeId) {
        self.ctx.catalog.open_connection(connection_id, node_id);
    }

    /// Mark a connection as dead. Retires all bindings on this connection.
    #[must_use]
    pub fn mark_connection_dead(&mut self, connection_id: ConnectionId) -> DeltaEvents {
        let mut events = DeltaEvents::default();
        runtime::retire::retire_bindings_for_connection(
            &mut self.ctx,
            connection_id,
            &mut events,
        );
        self.ctx.catalog.remove_connection_entity(connection_id);
        events
    }

    /// Pause a consumer.
    pub fn pause_consumer(&mut self, id: ConsumerId) -> bool {
        self.ctx.catalog.pause_consumer(id)
    }

    /// Resume a consumer.
    pub fn resume_consumer(&mut self, id: ConsumerId) -> bool {
        self.ctx.catalog.resume_consumer(id)
    }

    /// Set max inflight per subject by pattern on a stream.
    ///
    /// The limit lives on the stream's match table; the actual
    /// enforcement happens on the server's drain thread (per-consumer
    /// `ConsumerSubjects.can`). The engine just stores the policy.
    pub fn set_max_subject_inflight(
        &mut self,
        stream_id: StreamId,
        pattern: &[u8],
        max_inflight: u32,
    ) -> error::EngineResult<()> {
        self.ctx
            .catalog
            .set_max_subject_inflight(stream_id, pattern, max_inflight)
    }

    // ── Listing (cold path) ─────────────────────────────────────────────

    /// List all streams.
    pub fn list_streams(&self) -> Vec<(StreamId, Vec<u8>)> {
        self.ctx.catalog.list_streams()
    }

    /// List all consumers.
    pub fn list_consumers(&self) -> Vec<(ConsumerId, StreamId, QueueId, bool)> {
        self.ctx.catalog.list_consumers()
    }

    /// Get consumer info.
    pub fn consumer(&self, id: ConsumerId) -> Option<&catalog::ConsumerInfo> {
        self.ctx.catalog.consumer(id)
    }

    // ── Observability ───────────────────────────────────────────────────

    /// Per-consumer live state — pending ACKs (`ack_pending`) and paused
    /// flag. The result is materialized once (no iterator into engine
    /// internals), so callers can safely send it across thread boundaries.
    ///
    /// `ack_pending` is the count of messages delivered to the consumer
    /// that haven't been acked yet (the equivalent of NATS JetStream's
    /// `num_ack_pending`). Sums across all consumers give the broker's
    /// total in-flight load — useful as a saturation gauge.
    pub fn consumer_states_snapshot(&self) -> Vec<ConsumerStateSnapshot> {
        self.list_consumers()
            .into_iter()
            .map(|(consumer_id, stream_id, queue_id, paused)| ConsumerStateSnapshot {
                consumer_id: consumer_id.raw(),
                stream_id:   stream_id.raw(),
                queue_id:    queue_id.raw(),
                paused,
                ack_pending: self.consumer_inflight(consumer_id),
            })
            .collect()
    }

    /// Point-in-time snapshot of all counters.
    #[inline]
    pub fn metrics_snapshot(&self) -> metrics::MetricsSnapshot {
        self.ctx.metrics.snapshot()
    }

    // ── Internal access (for server integration) ────────────────────────

    /// Direct access to the engine context (for advanced/server use).
    #[inline]
    pub fn ctx(&self) -> &EngineContext {
        &self.ctx
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
    fn oracle_lifecycle() {
        let mut engine = ArbitroEngine::new();

        // Setup catalog.
        engine
            .create_stream(catalog::StreamConfig {
                id: StreamId(1),
                name: b"orders".to_vec(),
            })
            .unwrap();

        engine
            .create_consumer(catalog::ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 1000,
                ack_wait_ms: 0,
            })
            .unwrap();

        engine
            .create_subscription(catalog::SubscriptionConfig {
                id: SubscriptionId(1),
                stream_id: StreamId(1),
                consumer_id: ConsumerId(1),
                filters: vec![],
            })
            .unwrap();

        // No demand yet — no binding.
        assert!(!engine.has_any_demand());

        // Open connection + subscribe.
        engine.open_connection(ConnectionId(100), NodeId(1));
        let (result, events) =
            engine.subscribe(ConnectionId(100), SubscriptionId(1));
        let binding_id = result.unwrap();
        assert!(!events.is_empty());
        assert!(engine.has_any_demand());
        assert!(engine.has_demand(StreamId(1)));

        // Simulate delivery via execute(Delivered).
        let delivered = [command::DeliveredEntry {
            seq: 1,
            subject_hash: 0xBEEF,
            _pad: 0,
        }];
        let _events = engine.execute(&command::Command::Delivered {
            stream_id: StreamId(1),
            binding_id,
            entries: &delivered,
        });

        // Consumer inflight bumped.
        assert_eq!(engine.consumer_inflight(ConsumerId(1)), 1);
        assert!(engine.consumer_has_capacity(ConsumerId(1), 1000));

        // Ack the message.
        let acks = [command::AckEntry {
            stream_id: StreamId(1),
            seq: 1,
        }];
        let _events = engine.execute(&command::Command::Ack {
            consumer_id: ConsumerId(1),
            entries: &acks,
        });

        // Inflight back to 0.
        assert_eq!(engine.consumer_inflight(ConsumerId(1)), 0);

        // Unsubscribe.
        let events = engine.unsubscribe(binding_id);
        assert!(!engine.has_any_demand());
        assert_eq!(events.bindings_retired.len(), 1);
        assert_eq!(events.demand_became_idle.len(), 1);
    }

    #[test]
    fn mark_connection_dead_retires_bindings() {
        let mut engine = ArbitroEngine::new();

        engine
            .create_stream(catalog::StreamConfig {
                id: StreamId(1),
                name: b"s".to_vec(),
            })
            .unwrap();
        engine
            .create_consumer(catalog::ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100,
                ack_wait_ms: 0,
            })
            .unwrap();
        engine
            .create_subscription(catalog::SubscriptionConfig {
                id: SubscriptionId(1),
                stream_id: StreamId(1),
                consumer_id: ConsumerId(1),
                filters: vec![],
            })
            .unwrap();

        engine.open_connection(ConnectionId(42), NodeId(1));
        let (result, _) =
            engine.subscribe(ConnectionId(42), SubscriptionId(1));
        let _bid = result.unwrap();
        assert!(engine.has_demand(StreamId(1)));

        // Kill connection.
        let events = engine.mark_connection_dead(ConnectionId(42));
        assert!(!engine.has_demand(StreamId(1)));
        assert_eq!(events.bindings_retired.len(), 1);
    }

    #[test]
    fn delete_stream_cascades() {
        let mut engine = ArbitroEngine::new();

        engine
            .create_stream(catalog::StreamConfig {
                id: StreamId(1),
                name: b"s".to_vec(),
            })
            .unwrap();
        engine
            .create_consumer(catalog::ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100,
                ack_wait_ms: 0,
            })
            .unwrap();
        engine
            .create_subscription(catalog::SubscriptionConfig {
                id: SubscriptionId(1),
                stream_id: StreamId(1),
                consumer_id: ConsumerId(1),
                filters: vec![],
            })
            .unwrap();
        engine.open_connection(ConnectionId(1), NodeId(1));
        let (r, _) =
            engine.subscribe(ConnectionId(1), SubscriptionId(1));
        r.unwrap();

        // Deliver + leave pending.
        let _events = engine.execute(&command::Command::Delivered {
            stream_id: StreamId(1),
            binding_id: BindingId(1),
            entries: &[command::DeliveredEntry {
                seq: 1,
                subject_hash: 0xABC,
                _pad: 0,
            }],
        });
        assert_eq!(engine.consumer_inflight(ConsumerId(1)), 1);

        // Delete stream — retires binding, releases inflight.
        let events = engine.delete_stream(StreamId(1));
        assert_eq!(events.bindings_retired.len(), 1);
        assert_eq!(engine.consumer_inflight(ConsumerId(1)), 0);
    }
}
