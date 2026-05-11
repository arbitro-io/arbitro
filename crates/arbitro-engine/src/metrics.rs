//! Broker metrics — lock-free atomic counters read from the protocol layer.
//!
//! Level 0 (leaf). No internal deps. `&EngineMetrics` is `Send + Sync` so
//! the protocol layer can snapshot from a metrics thread independently of
//! the engine thread.
//!
//! Rule compliance (`performance.md` §15, `code-anti-patterns.md`): the
//! **only** permitted form of hot-path observability in this crate. All
//! increments use `Ordering::Relaxed` — counters have no ordering deps
//! (`code-concurrency.md` §6).
//!
//! Layout: `#[repr(C, align(64))]` with explicit padding between counter
//! groups prevents false sharing when the protocol layer reads snapshots
//! from a different core than the engine thread.

use std::sync::atomic::{AtomicU64, Ordering};

/// Broker metrics — one cache line per counter group.
///
/// Safe to share across threads as `&EngineMetrics`. Engine thread only
/// does `fetch_add(_, Relaxed)`; protocol/metrics thread only reads via
/// `snapshot()`.
#[repr(C, align(64))]
pub struct EngineMetrics {
    // ── Publish ───────────────────────────────
    pub publish_entries_accepted:   AtomicU64,
    pub publish_duplicates_skipped: AtomicU64,
    pub publish_no_match:           AtomicU64,
    pub publish_queues_pushed:      AtomicU64,
    pub publish_fanout_notified:    AtomicU64,
    _pad0: [u8; 24],

    // ── Claim ─────────────────────────────────
    pub claim_batches:                 AtomicU64,
    pub claim_entries_delivered:       AtomicU64,
    pub claim_skipped_consumer_paused: AtomicU64,
    pub claim_skipped_max_inflight:    AtomicU64,
    pub claim_skipped_subject_limit:   AtomicU64,
    pub claim_skipped_credit_conn:     AtomicU64,
    pub claim_skipped_credit_subject:  AtomicU64,
    pub claim_empty_pop:               AtomicU64,

    // ── Ack / Nack ────────────────────────────
    pub ack_accepted:  AtomicU64,
    pub ack_not_found: AtomicU64,
    pub nack_accepted: AtomicU64,
    _pad2: [u8; 40],

    // ── Seed / Replay ─────────────────────────
    pub seed_entries:       AtomicU64,
    pub seed_queues_pushed: AtomicU64,
    pub seed_no_match:      AtomicU64,
    _pad3: [u8; 40],

    // ── Drain ─────────────────────────────────
    pub drain_pending_removed: AtomicU64,
    pub drain_connections:     AtomicU64,
    pub drain_consumers:       AtomicU64,
    _pad4: [u8; 40],
}

// Cache-line alignment guard — protects against accidental layout regressions.
const _: () = assert!(std::mem::align_of::<EngineMetrics>() == 64);

impl EngineMetrics {
    /// Create a zeroed counter set.
    pub const fn new() -> Self {
        Self {
            publish_entries_accepted:   AtomicU64::new(0),
            publish_duplicates_skipped: AtomicU64::new(0),
            publish_no_match:           AtomicU64::new(0),
            publish_queues_pushed:      AtomicU64::new(0),
            publish_fanout_notified:    AtomicU64::new(0),
            _pad0: [0; 24],

            claim_batches:                 AtomicU64::new(0),
            claim_entries_delivered:       AtomicU64::new(0),
            claim_skipped_consumer_paused: AtomicU64::new(0),
            claim_skipped_max_inflight:    AtomicU64::new(0),
            claim_skipped_subject_limit:   AtomicU64::new(0),
            claim_skipped_credit_conn:     AtomicU64::new(0),
            claim_skipped_credit_subject:  AtomicU64::new(0),
            claim_empty_pop:               AtomicU64::new(0),

            ack_accepted:  AtomicU64::new(0),
            ack_not_found: AtomicU64::new(0),
            nack_accepted: AtomicU64::new(0),
            _pad2: [0; 40],

            seed_entries:       AtomicU64::new(0),
            seed_queues_pushed: AtomicU64::new(0),
            seed_no_match:      AtomicU64::new(0),
            _pad3: [0; 40],

            drain_pending_removed: AtomicU64::new(0),
            drain_connections:     AtomicU64::new(0),
            drain_consumers:       AtomicU64::new(0),
            _pad4: [0; 40],
        }
    }

    /// Point-in-time snapshot. Loads each counter with `Relaxed` —
    /// per-counter consistent, not consistent across counters.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        MetricsSnapshot {
            publish_entries_accepted:   l(&self.publish_entries_accepted),
            publish_duplicates_skipped: l(&self.publish_duplicates_skipped),
            publish_no_match:           l(&self.publish_no_match),
            publish_queues_pushed:      l(&self.publish_queues_pushed),
            publish_fanout_notified:    l(&self.publish_fanout_notified),

            claim_batches:                 l(&self.claim_batches),
            claim_entries_delivered:       l(&self.claim_entries_delivered),
            claim_skipped_consumer_paused: l(&self.claim_skipped_consumer_paused),
            claim_skipped_max_inflight:    l(&self.claim_skipped_max_inflight),
            claim_skipped_subject_limit:   l(&self.claim_skipped_subject_limit),
            claim_skipped_credit_conn:     l(&self.claim_skipped_credit_conn),
            claim_skipped_credit_subject:  l(&self.claim_skipped_credit_subject),
            claim_empty_pop:               l(&self.claim_empty_pop),

            ack_accepted:  l(&self.ack_accepted),
            ack_not_found: l(&self.ack_not_found),
            nack_accepted: l(&self.nack_accepted),

            seed_entries:       l(&self.seed_entries),
            seed_queues_pushed: l(&self.seed_queues_pushed),
            seed_no_match:      l(&self.seed_no_match),

            drain_pending_removed: l(&self.drain_pending_removed),
            drain_connections:     l(&self.drain_connections),
            drain_consumers:       l(&self.drain_consumers),
        }
    }
}

impl Default for EngineMetrics {
    fn default() -> Self { Self::new() }
}

/// Point-in-time snapshot of `EngineMetrics`. Plain `u64` fields for
/// formatting into Prometheus/StatsD/etc. in the protocol layer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub publish_entries_accepted:   u64,
    pub publish_duplicates_skipped: u64,
    pub publish_no_match:           u64,
    pub publish_queues_pushed:      u64,
    pub publish_fanout_notified:    u64,

    pub claim_batches:                 u64,
    pub claim_entries_delivered:       u64,
    pub claim_skipped_consumer_paused: u64,
    pub claim_skipped_max_inflight:    u64,
    pub claim_skipped_subject_limit:   u64,
    pub claim_skipped_credit_conn:     u64,
    pub claim_skipped_credit_subject:  u64,
    pub claim_empty_pop:               u64,

    pub ack_accepted:  u64,
    pub ack_not_found: u64,
    pub nack_accepted: u64,

    pub seed_entries:       u64,
    pub seed_queues_pushed: u64,
    pub seed_no_match:      u64,

    pub drain_pending_removed: u64,
    pub drain_connections:     u64,
    pub drain_consumers:       u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_zero() {
        let m = EngineMetrics::new();
        let s = m.snapshot();
        assert_eq!(s, MetricsSnapshot::default());
    }

    #[test]
    fn snapshot_captures_current_values() {
        let m = EngineMetrics::new();
        m.publish_entries_accepted.fetch_add(7, Ordering::Relaxed);
        m.claim_entries_delivered.fetch_add(3, Ordering::Relaxed);
        m.ack_accepted.fetch_add(2, Ordering::Relaxed);

        let s = m.snapshot();
        assert_eq!(s.publish_entries_accepted, 7);
        assert_eq!(s.claim_entries_delivered, 3);
        assert_eq!(s.ack_accepted, 2);
        assert_eq!(s.publish_no_match, 0);
    }

    #[test]
    fn send_sync_bounds() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EngineMetrics>();
    }
}
