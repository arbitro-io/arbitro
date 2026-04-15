//! EngineContext — unified accessor for all engine subsystems.
//!
//! Level 6 — depends on Level 0-5. Provides access, no logic.
//!
//! EngineContext is a struct, not a trait. It provides `&mut` access
//! to the engine's subsystems without owning their logic.
//! Runtime operations borrow fields from context — never call
//! runtime methods from inside context.

use crate::graph::GraphStore;
use crate::edge::BuiltinEdges;
use crate::plugin::credit::CreditPlugin;
use crate::plugin::event_bus::EventBus;
use crate::plugin::scheduler::Scheduler;
use crate::inflight::InFlightCounters;
use crate::ready::ReadyState;
use crate::idempotency::IdempotencyWindow;
use crate::catalog::Catalog;
use crate::fanout::FanoutQueue;
use crate::reply::{ScratchReply, OperationKind};
use crate::batch::{ClaimedEntry, AckResult, NackResult};
use crate::metrics::EngineMetrics;
use crate::types::{ConnectionId, QueueId, SlabKey, StreamId};
use std::collections::HashMap;

/// Unified accessor for all engine subsystems.
///
/// Owns everything the engine needs. Runtime operations take
/// `&mut EngineContext` and access fields directly — no getters,
/// no indirection, no locking.
pub struct EngineContext {
    /// The entity graph: typed slabs for all node types.
    pub graph: GraphStore,

    /// Edge indexes: concrete struct with direct field access. No TypeId
    /// dispatch, no `Box<dyn Any>` downcast — hot-path edge lookups are a
    /// single field load (`performance.md` §11).
    pub edges: BuiltinEdges,

    /// Credit counters (Node / Connection / Subject scopes).
    ///
    /// Direct field — no registry indirection. Hot-path access in
    /// `claim` and `ack` is a single field load (~1 ns) instead of a
    /// TypeId HashMap lookup + `Box<dyn Any>` downcast (~15-20 ns).
    /// See `performance.md` §11 — slab/fields over HashMap for hot-path.
    pub credit: CreditPlugin,

    /// Event bus — buffered engine events drained by the protocol layer.
    pub events: EventBus,

    /// Timer wheel for pending deadlines.
    pub scheduler: Scheduler,

    /// InFlight counters: subject, consumer, queue.
    pub inflight: InFlightCounters,

    /// Per-queue ready state with round-robin delivery.
    pub ready: ReadyState,

    /// Per-stream idempotency windows.
    pub idempotency: HashMap<StreamId, IdempotencyWindow, ahash::RandomState>,

    /// ConnectionId → SlabKey lookup. O(1) for remove_connection.
    pub connection_keys: HashMap<ConnectionId, SlabKey, ahash::RandomState>,

    /// Catalog: stream/consumer/subscription lifecycle + match tables.
    pub catalog: Catalog,

    /// Monotonic sequence counter for message ordering.
    pub next_seq: u64,

    /// Monotonic binding ID counter.
    pub next_binding_id: u32,

    // ── Fanout (fire-and-forget notification buffer) ─────────────────────

    /// Fanout queue: publish pushes notifications, protocol layer drains.
    /// No grouping on hot path — protocol layer groups by connection.
    pub fanout: FanoutQueue,

    // ── Scratch reply buffers (hot path — zero alloc steady-state) ──────

    /// Pre-allocated reply buffer for claim operations.
    pub reply_claim: ScratchReply<ClaimedEntry>,

    /// Pre-allocated reply buffer for ack operations.
    pub reply_ack: ScratchReply<AckResult>,

    /// Pre-allocated reply buffer for nack operations.
    pub reply_nack: ScratchReply<NackResult>,

    // ── Observability ────────────────────────────────────────────────────

    /// Atomic broker counters — the **only** permitted form of hot-path
    /// observability (`performance.md` §15, `code-anti-patterns.md`).
    /// Exposed as `&EngineMetrics` to the protocol layer for cross-thread
    /// snapshots; engine thread only does `fetch_add(_, Relaxed)`.
    pub metrics: EngineMetrics,

    // ── Seed scratch (cold path — replay from store) ─────────────────────

    /// Reusable queue-id scratch for `runtime::seed::enqueue_ready`.
    /// Replay matches a subject against every queue in the stream's match
    /// table; this buffer dedupes without a hard cap, so no queue is ever
    /// silently dropped (correctness invariant: broker never loses a
    /// replayed message).
    pub seed_scratch: Vec<QueueId>,

    /// Reusable per-queue batch buffer for `runtime::seed::enqueue_ready_batch`.
    /// Maps QueueId → Vec<(subject_hash, seq)>, reused across calls.
    pub seed_batch_queues: HashMap<QueueId, Vec<(u32, u64)>, ahash::RandomState>,
}

impl EngineContext {
    /// Create a new engine context with all subsystems initialized.
    ///
    /// `scheduler_tick_ms` controls timer wheel resolution. 100 ms is the
    /// default used by `EngineBuilder` unless overridden.
    pub fn with_scheduler_tick_ms(scheduler_tick_ms: u64) -> Self {
        Self {
            graph: GraphStore::new(),
            edges: BuiltinEdges::new(),
            credit: CreditPlugin::new(),
            events: EventBus::new(),
            scheduler: Scheduler::new(scheduler_tick_ms),
            inflight: InFlightCounters::new(),
            ready: ReadyState::new(),
            idempotency: HashMap::with_hasher(ahash::RandomState::new()),
            connection_keys: HashMap::with_hasher(ahash::RandomState::new()),
            catalog: Catalog::new(),
            next_seq: 1,
            next_binding_id: 1,
            fanout: FanoutQueue::new(256),
            reply_claim: ScratchReply::new(OperationKind::Claim, 64),
            reply_ack: ScratchReply::new(OperationKind::Ack, 64),
            reply_nack: ScratchReply::new(OperationKind::Nack, 64),
            metrics: EngineMetrics::new(),
            seed_scratch: Vec::with_capacity(16),
            seed_batch_queues: HashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    /// Create a new engine context with default scheduler tick (100 ms).
    #[inline]
    pub fn new() -> Self {
        Self::with_scheduler_tick_ms(100)
    }

    /// Get or create an idempotency window for a stream.
    #[inline]
    pub fn idempotency_for(&mut self, stream_id: StreamId) -> &mut IdempotencyWindow {
        self.idempotency
            .entry(stream_id)
            .or_insert_with(IdempotencyWindow::default_5min)
    }
}

impl Default for EngineContext {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::StreamConfig;

    #[test]
    fn context_creation() {
        let ctx = EngineContext::new();
        assert_eq!(ctx.inflight.get(crate::inflight::InFlightScope::Subject, 0), 0);
    }

    #[test]
    fn context_catalog_integration() {
        let mut ctx = EngineContext::new();

        ctx.catalog.ensure_stream(&mut ctx.graph, StreamConfig {
            id: StreamId(1),
            name: b"test".to_vec(),
        }).unwrap();

        assert!(ctx.catalog.stream_key(StreamId(1)).is_ok());
        assert!(ctx.catalog.match_table(StreamId(1)).is_some());
    }

    #[test]
    fn context_idempotency_per_stream() {
        let mut ctx = EngineContext::new();

        // First access creates the window
        let _w1 = ctx.idempotency_for(StreamId(1));
        let _w2 = ctx.idempotency_for(StreamId(2));

        assert_eq!(ctx.idempotency.len(), 2);
    }
}
