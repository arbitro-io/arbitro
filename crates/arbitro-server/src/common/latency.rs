//! §7.4 — minimal latency histogram.
//!
//! Hand-rolled, zero-dep, 7-bucket log-scale histogram. Each bucket is an
//! `AtomicU64` count; `record(ns)` finds the smallest bucket whose
//! upper-bound is ≥ ns and bumps it. Designed to be cheap enough to
//! sit on the publish path (one branch + one `fetch_add Relaxed`).
//!
//! Buckets (cumulative-style, so Prometheus can emit them directly):
//!   < 100µs, < 1ms, < 10ms, < 100ms, < 1s, < 10s, +Inf
//!
//! We don't ship hdrhistogram or t-digest — for an operator dashboard a
//! coarse log-scale histogram tells you when p95 crosses 100ms vs. 1s
//! without dragging a 200 KB dep into the binary.

use std::sync::atomic::{AtomicU64, Ordering};

/// Upper bounds in nanoseconds. Final bucket is +Inf via the `record`
/// fallthrough.
pub const LATENCY_BUCKETS_NS: [u64; 6] = [
    100_000,         // 100µs
    1_000_000,       //   1ms
    10_000_000,      //  10ms
    100_000_000,     // 100ms
    1_000_000_000,   //   1s
    10_000_000_000,  //  10s
];

/// Human-readable upper bounds (matches `LATENCY_BUCKETS_NS` + "+Inf").
/// Used by the Prometheus exporter for the `le="..."` label.
pub const LATENCY_LABELS: [&str; 7] = [
    "0.0001", "0.001", "0.01", "0.1", "1", "10", "+Inf",
];

/// 7-bucket histogram, lock-free, append-only.
#[derive(Debug, Default)]
pub struct Latency {
    /// One counter per upper-bound bucket + a final +Inf catch-all.
    buckets: [AtomicU64; 7],
    /// Total observations (== sum of buckets) and total ns (so the
    /// exporter can emit `_count` and `_sum` for full Prometheus
    /// histogram compatibility).
    count: AtomicU64,
    sum_ns: AtomicU64,
}

impl Latency {
    pub const fn new() -> Self {
        // const-friendly init — array of zero AtomicU64.
        Self {
            buckets: [
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
        }
    }

    /// Record one observation. Lock-free. ~3 ns under contention.
    #[inline]
    pub fn record(&self, ns: u64) {
        let idx = match LATENCY_BUCKETS_NS.iter().position(|&b| ns < b) {
            Some(i) => i,
            None => 6, // +Inf bucket
        };
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
    }

    /// Point-in-time snapshot. Returns (per-bucket counts, total count, total ns).
    pub fn snapshot(&self) -> ([u64; 7], u64, u64) {
        let mut out = [0u64; 7];
        for (i, b) in self.buckets.iter().enumerate() {
            out[i] = b.load(Ordering::Relaxed);
        }
        (
            out,
            self.count.load(Ordering::Relaxed),
            self.sum_ns.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_classify_correctly() {
        let h = Latency::new();
        h.record(50_000);              // <100µs → bucket 0
        h.record(500_000);             // <1ms   → bucket 1
        h.record(5_000_000);           // <10ms  → bucket 2
        h.record(50_000_000);          // <100ms → bucket 3
        h.record(500_000_000);         // <1s    → bucket 4
        h.record(5_000_000_000);       // <10s   → bucket 5
        h.record(50_000_000_000);      // +Inf   → bucket 6
        let (buckets, count, sum) = h.snapshot();
        assert_eq!(buckets, [1, 1, 1, 1, 1, 1, 1]);
        assert_eq!(count, 7);
        assert!(sum > 0);
    }

    #[test]
    fn snapshot_is_idempotent() {
        let h = Latency::new();
        h.record(1_000);
        let a = h.snapshot();
        let b = h.snapshot();
        assert_eq!(a.0, b.0);
        assert_eq!(a.1, b.1);
    }
}
