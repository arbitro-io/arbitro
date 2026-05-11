//! Client metrics — atomic counters readable from any thread without
//! a lock. The hot publish/deliver paths only do `fetch_add(Relaxed)`,
//! so observability cost is one cache-line write per event.
//!
//! Wired into `Inner` and exposed via `Client::metrics()`. Operators
//! poll `snapshot()` on a timer (or log it on shutdown) to see traffic.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Live atomic counters held by `Inner`. `Send + Sync` by construction.
#[derive(Debug, Default)]
pub struct ClientMetrics {
    // ── Publish ─────────────────────────────────────────────────────
    /// `publish` + `publish_sync` calls (one per logical message).
    pub publishes_sent:        AtomicU64,
    /// Entries inside `publish_batch` calls (summed across batches).
    pub publish_batch_entries: AtomicU64,
    /// `publish_sync` calls that returned an error from the broker.
    pub publish_errors:        AtomicU64,

    // ── Subscribe / deliver ─────────────────────────────────────────
    /// `Deliver` frames received from the broker (one per message).
    pub deliveries_received:   AtomicU64,
    /// Currently-open subscriptions. Gauge — incremented at `subscribe()`,
    /// decremented at `SubscriptionHandle::drop`.
    pub active_subscriptions:  AtomicUsize,

    // ── Ack / Nack ──────────────────────────────────────────────────
    pub acks_sent:             AtomicU64,
    pub nacks_sent:            AtomicU64,

    // ── Manage (CRUD requests) ──────────────────────────────────────
    pub manage_requests_sent:  AtomicU64,

    // ── Connection lifecycle ────────────────────────────────────────
    /// Times the session successfully reconnected after a drop.
    pub reconnects:            AtomicU64,
    /// Last Pong RTT observation, nanoseconds. 0 = no pong seen yet.
    pub last_pong_rtt_ns:      AtomicU64,
}

impl ClientMetrics {
    pub const fn new() -> Self {
        Self {
            publishes_sent:        AtomicU64::new(0),
            publish_batch_entries: AtomicU64::new(0),
            publish_errors:        AtomicU64::new(0),
            deliveries_received:   AtomicU64::new(0),
            active_subscriptions:  AtomicUsize::new(0),
            acks_sent:             AtomicU64::new(0),
            nacks_sent:            AtomicU64::new(0),
            manage_requests_sent:  AtomicU64::new(0),
            reconnects:            AtomicU64::new(0),
            last_pong_rtt_ns:      AtomicU64::new(0),
        }
    }

    /// Point-in-time snapshot. Per-field consistent, not cross-field.
    pub fn snapshot(&self) -> ClientMetricsSnapshot {
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        ClientMetricsSnapshot {
            publishes_sent:        l(&self.publishes_sent),
            publish_batch_entries: l(&self.publish_batch_entries),
            publish_errors:        l(&self.publish_errors),
            deliveries_received:   l(&self.deliveries_received),
            active_subscriptions:  self.active_subscriptions.load(Ordering::Relaxed),
            acks_sent:             l(&self.acks_sent),
            nacks_sent:            l(&self.nacks_sent),
            manage_requests_sent:  l(&self.manage_requests_sent),
            reconnects:            l(&self.reconnects),
            last_pong_rtt_ns:      l(&self.last_pong_rtt_ns),
        }
    }
}

/// Plain owned snapshot suitable for serialization, logging, or returning
/// from a sync function.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientMetricsSnapshot {
    pub publishes_sent:        u64,
    pub publish_batch_entries: u64,
    pub publish_errors:        u64,
    pub deliveries_received:   u64,
    pub active_subscriptions:  usize,
    pub acks_sent:             u64,
    pub nacks_sent:            u64,
    pub manage_requests_sent:  u64,
    pub reconnects:            u64,
    pub last_pong_rtt_ns:      u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_snapshot_is_zero() {
        let m = ClientMetrics::new();
        assert_eq!(m.snapshot(), ClientMetricsSnapshot::default());
    }

    #[test]
    fn captures_increments() {
        let m = ClientMetrics::new();
        m.publishes_sent.fetch_add(3, Ordering::Relaxed);
        m.acks_sent.fetch_add(7, Ordering::Relaxed);
        m.active_subscriptions.fetch_add(2, Ordering::Relaxed);
        let s = m.snapshot();
        assert_eq!(s.publishes_sent, 3);
        assert_eq!(s.acks_sent, 7);
        assert_eq!(s.active_subscriptions, 2);
    }
}
