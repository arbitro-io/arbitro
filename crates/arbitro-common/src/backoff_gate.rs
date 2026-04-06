//! BackoffGate — progressive-backoff wait with instant signal reset.
//!
//! `wait()` sleeps for the current interval, advancing through the table
//! on each call. `signal()` resets the index back to 0 so the next `wait()`
//! uses the shortest interval again.
//!
//! ```text
//! intervals = [1, 200, 300, 500] (µs)
//!
//! wait() → sleep 1µs,   index → 1
//! wait() → sleep 200µs, index → 2
//! wait() → sleep 300µs, index → 3
//! wait() → sleep 500µs, index stays at 3 (clamped)
//!
//! signal() → index = 0  (next wait() is 1µs again)
//! ```
//!
//! Designed for drain loops: fast response when work arrives (signal resets
//! to 1µs), low CPU when idle (backs off to 500µs between polls).
//!
//! No threads. No futex. No allocation. Pure atomic + OS sleep.

use std::sync::atomic::{AtomicUsize, Ordering::*};
use std::time::Duration;

/// Progressive-backoff gate for drain loops.
///
/// Call `wait()` at the bottom of the loop. Call `signal()` when new work
/// arrives (e.g., from a spawned task or a publish). The next `wait()` after
/// a `signal()` will use the shortest interval.
pub struct BackoffGate {
    /// Index into `intervals`. Advances on each `wait()`, resets on `signal()`.
    index:     AtomicUsize,
    intervals: &'static [u64], // microseconds
}

impl BackoffGate {
    /// Create a gate with the given interval table (microseconds).
    ///
    /// Example: `BackoffGate::new(&[1, 200, 300, 500])`
    pub const fn new(intervals: &'static [u64]) -> Self {
        Self {
            index:     AtomicUsize::new(0),
            intervals,
        }
    }

    /// Sleep for the current interval, then advance the index.
    ///
    /// Called at the bottom of the drain loop. Blocks the current thread
    /// (use the async variant `wait_async` inside tokio tasks).
    #[inline]
    pub fn wait(&self) {
        let idx = self.index.load(Relaxed);
        let us  = self.intervals[idx.min(self.intervals.len() - 1)];
        // Advance index (clamped at last entry)
        let next = (idx + 1).min(self.intervals.len() - 1);
        self.index.store(next, Relaxed);
        std::thread::sleep(Duration::from_micros(us));
    }

    /// Reset the backoff index to 0 — next `wait()` uses the shortest interval.
    ///
    /// Call this when new work arrives: from spawned task completions, publishes,
    /// or any event that makes the loop productive again.
    #[inline]
    pub fn signal(&self) {
        self.index.store(0, Release);
    }

    /// Current interval that the next `wait()` will use (µs).
    #[inline]
    pub fn current_interval_us(&self) -> u64 {
        let idx = self.index.load(Relaxed);
        self.intervals[idx.min(self.intervals.len() - 1)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn advances_through_intervals() {
        let gate = BackoffGate::new(&[1, 200, 300, 500]);
        assert_eq!(gate.current_interval_us(), 1);
        gate.wait(); // uses 1µs, index → 1
        assert_eq!(gate.current_interval_us(), 200);
        gate.wait(); // uses 200µs, index → 2
        assert_eq!(gate.current_interval_us(), 300);
    }

    #[test]
    fn signal_resets_index() {
        let gate = BackoffGate::new(&[1, 200, 300, 500]);
        gate.wait(); // 1µs
        gate.wait(); // 200µs
        assert_eq!(gate.current_interval_us(), 300);
        gate.signal();
        assert_eq!(gate.current_interval_us(), 1);
    }

    #[test]
    fn clamps_at_last_interval() {
        let gate = BackoffGate::new(&[1, 200, 300, 500]);
        for _ in 0..10 {
            gate.wait();
        }
        assert_eq!(gate.current_interval_us(), 500);
    }

}
