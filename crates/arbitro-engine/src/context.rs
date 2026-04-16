//! EngineContext — unified accessor for oracle engine subsystems.
//!
//! Level 6 — depends on Level 0-5. Provides access, no logic.
//!
//! Drastically simplified from the legacy graph-based context:
//! * No `GraphStore`, `BuiltinEdges`, `ReadyState`, `FanoutQueue`.
//! * No `ScratchReply` buffers (no claim/ack reply protocol).
//! * No `IdempotencyWindow` (handled at store level).
//! * No `CreditPlugin`, `EventBus`, `Scheduler` (plugins removed).
//!
//! What remains: `Catalog` (entity storage + match tables + bindings),
//! `InFlightCounters` (subject/consumer/queue credits), `EngineMetrics`
//! (atomic counters for cross-thread observability).

use crate::catalog::Catalog;
use crate::inflight::InFlightCounters;
use crate::metrics::EngineMetrics;

/// Unified accessor for all engine subsystems.
///
/// Owns everything the engine needs. Runtime operations take
/// `&mut EngineContext` and access fields directly — no getters,
/// no indirection, no locking.
pub struct EngineContext {
    /// Catalog: streams, consumers, subscriptions, bindings, match tables.
    pub catalog: Catalog,

    /// InFlight counters: subject, consumer, queue.
    pub inflight: InFlightCounters,

    /// Atomic broker counters — the **only** permitted form of hot-path
    /// observability (`performance.md` §15).
    pub metrics: EngineMetrics,
}

impl EngineContext {
    /// Create a new engine context with all subsystems initialized.
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
            inflight: InFlightCounters::new(),
            metrics: EngineMetrics::new(),
        }
    }
}

impl Default for EngineContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::StreamConfig;
    use crate::inflight::InFlightScope;
    use crate::types::StreamId;

    #[test]
    fn context_creation() {
        let ctx = EngineContext::new();
        assert_eq!(ctx.inflight.get(InFlightScope::Subject, 0), 0);
    }

    #[test]
    fn context_catalog_integration() {
        let mut ctx = EngineContext::new();
        ctx.catalog
            .ensure_stream(StreamConfig {
                id: StreamId(1),
                name: b"test".to_vec(),
            })
            .unwrap();
        assert!(ctx.catalog.stream_exists(StreamId(1)));
        assert!(ctx.catalog.match_table(StreamId(1)).is_some());
    }
}
