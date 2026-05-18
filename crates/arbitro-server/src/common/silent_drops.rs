//! H10 — counters for every "silent drop" site in the server.
//!
//! The hot paths use `try_send` / `enqueue` and intentionally drop the
//! frame when the downstream channel is full. Until now those drops were
//! invisible: a slow consumer or a saturated drain→cmd ring just leaked
//! messages with zero operator signal. `SilentDrops` aggregates each
//! drop site into a single `AtomicU64` that the periodic `metrics_loop`
//! prints once per interval.
//!
//! Counters are Relaxed — observability only, never feeds back into
//! control flow.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct SilentDrops {
    /// Bumped when `ConnectionRegistry::enqueue` saw a full
    /// per-connection mpsc and dropped the outbound frame.
    pub conn_write: AtomicU64,
    /// Bumped when the drain → command notification ring was full and
    /// the producer (drain thread) had to drop the notification.
    pub notify_ring: AtomicU64,
    /// Bumped when the command → drain event ring was full and the
    /// producer (command worker) had to drop the event.
    pub drain_evt: AtomicU64,
}

impl SilentDrops {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn inc_conn_write(&self) {
        self.conn_write.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_notify_ring(&self) {
        self.notify_ring.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_drain_evt(&self) {
        self.drain_evt.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot the current counter values — used by the metrics loop
    /// to log deltas vs the previous tick.
    pub fn snapshot(&self) -> SilentDropsSnapshot {
        SilentDropsSnapshot {
            conn_write: self.conn_write.load(Ordering::Relaxed),
            notify_ring: self.notify_ring.load(Ordering::Relaxed),
            drain_evt: self.drain_evt.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default, Clone, Copy)]
pub struct SilentDropsSnapshot {
    pub conn_write: u64,
    pub notify_ring: u64,
    pub drain_evt: u64,
}
