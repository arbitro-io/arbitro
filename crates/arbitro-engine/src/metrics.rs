//! Metrics — atomic counters, zero-alloc on hot path.
//!
//! `fetch_add(Relaxed)` on publish/deliver. Snapshot only on StatsRequest (cold).

use core::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub struct Metrics {
    pub connections: AtomicU64,
    pub msgs_in: AtomicU64,
    pub msgs_out: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub streams: AtomicU64,
    pub consumers: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            connections: AtomicU64::new(0),
            msgs_in: AtomicU64::new(0),
            msgs_out: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            streams: AtomicU64::new(0),
            consumers: AtomicU64::new(0),
        }
    }

    /// Cold path — build snapshot for StatsResponse.
    #[inline]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            connections: self.connections.load(Relaxed),
            msgs_in: self.msgs_in.load(Relaxed),
            msgs_out: self.msgs_out.load(Relaxed),
            bytes_in: self.bytes_in.load(Relaxed),
            bytes_out: self.bytes_out.load(Relaxed),
            streams: self.streams.load(Relaxed),
            consumers: self.consumers.load(Relaxed),
        }
    }
}

/// Frozen point-in-time metrics for StatsResponse.
#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    pub connections: u64,
    pub msgs_in: u64,
    pub msgs_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub streams: u64,
    pub consumers: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_and_snapshot() {
        let m = Metrics::new();
        m.msgs_in.fetch_add(100, Relaxed);
        m.msgs_out.fetch_add(50, Relaxed);
        m.bytes_in.fetch_add(4096, Relaxed);
        m.connections.fetch_add(3, Relaxed);

        let s = m.snapshot();
        assert_eq!(s.msgs_in, 100);
        assert_eq!(s.msgs_out, 50);
        assert_eq!(s.bytes_in, 4096);
        assert_eq!(s.connections, 3);
        assert_eq!(s.bytes_out, 0);
        assert_eq!(s.streams, 0);
        assert_eq!(s.consumers, 0);
    }
}
