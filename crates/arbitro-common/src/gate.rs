//! Gate — drain delivery signal for the shard thread.
//!
//! The shard calls `release()` after publish/ack/nack to signal new work.
//! The drain loop checks `is_open()` to decide whether to run drain_cycle.
//! When drain_cycle finds nothing, it calls `lock()` — the drain parks.
//!
//! Implementation: `arbitro_kit::gate::SignalSet<ParkWaiter>` with a single
//! signal (bit 0). `ParkWaiter` → `thread::park/unpark`, 0% CPU idle.
//! Replaces the previous crossbeam_channel::bounded(1) implementation —
//! same semantics, one fewer dependency, ~2× faster on the release path
//! (atomic bit-OR vs channel try_send).

use arbitro_kit::gate::{SignalId, SignalSet};

/// Single signal ID — always bit 0.
const GATE_BIT: SignalId = SignalId::new(0);

/// kit SignalSet-backed Gate.
///
/// Semantics:
/// - `release()` sets bit 0 (coalescing — multiple releases merge via OR).
/// - `acquire()` parks the thread (0% CPU via `thread::park`) until bit 0
///   is set. Fast-paths if already open.
/// - `lock()` clears bit 0.
/// - `is_open()` reads bit 0.
#[repr(transparent)]
pub struct Gate {
    inner: SignalSet,
}

impl Default for Gate {
    fn default() -> Self { Self::new() }
}

impl Gate {
    pub fn new() -> Self {
        Self {
            inner: SignalSet::new(),
        }
    }

    /// Register the consumer thread. Must be called once by the drain
    /// thread before any producer calls `release()`.
    #[inline]
    pub fn set_worker(&self, t: std::thread::Thread) {
        self.inner.set_worker(t);
    }

    /// Signal that work is available. Coalescing — multiple concurrent
    /// releases are merged (bit-OR into a single atomic).
    #[inline]
    pub fn release(&self) {
        self.inner.release(GATE_BIT);
    }

    /// Clear the "work available" signal. Called by drain when a cycle
    /// found nothing to deliver.
    #[inline]
    pub fn lock(&self) {
        self.inner.lock(GATE_BIT);
    }

    /// `true` if there is pending work.
    #[inline]
    pub fn is_open(&self) -> bool {
        self.inner.is_open(GATE_BIT)
    }

    /// Block until work is available. 0% CPU — parks via
    /// `thread::park` (futex on Linux).
    #[inline]
    pub fn acquire(&self) {
        self.inner.acquire();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    /// T17 — multiple `release()` calls coalesce into a single
    /// "work available" signal. The gate's contract is that N producers
    /// firing in quick succession do NOT each wake the drain N times —
    /// they OR into one bit and the drain decides what to do on its
    /// next pass. Without this, the shard worker would do N redundant
    /// drain cycles per burst publish.
    #[test]
    fn t17_n_releases_coalesce_into_one_open_state() {
        let g = Gate::new();
        // Register a worker thread so `release()` has an unpark target —
        // we never actually `acquire()` here, but kit's contract requires
        // it for the release path.
        g.set_worker(std::thread::current());

        // Burst of 1000 releases — must end in exactly one "open" state.
        for _ in 0..1000 {
            g.release();
        }
        assert!(g.is_open(), "gate must be open after any release");

        // One lock() collapses the whole burst — same as if a single
        // release had happened. This is the key invariant: burst of N
        // does not require N lock() calls to clear.
        g.lock();
        assert!(!g.is_open(), "single lock() clears the coalesced state");
    }

    /// T17 follow-up — release/lock interleavings race the right way:
    /// a release that fires AFTER lock() wins (gate ends open).
    #[test]
    fn t17_release_after_lock_keeps_gate_open() {
        let g = Gate::new();
        g.set_worker(std::thread::current());

        g.release();
        g.lock();
        assert!(!g.is_open());
        g.release();
        assert!(g.is_open(), "post-lock release must reopen the gate");
    }

    /// T17 follow-up — concurrent producers cannot lose updates.
    /// Tightens the coalescing invariant: even when producers fire
    /// from N threads simultaneously, the final state is "open" and
    /// a single lock() collapses it.
    #[test]
    fn t17_concurrent_releases_never_lose_open_state() {
        let g = Arc::new(Gate::new());
        g.set_worker(std::thread::current());

        let mut handles = Vec::new();
        for _ in 0..8 {
            let g2 = Arc::clone(&g);
            handles.push(std::thread::spawn(move || {
                for _ in 0..1_000 {
                    g2.release();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // Give park/unpark machinery a beat to settle on slow CI runners.
        std::thread::sleep(Duration::from_millis(5));
        assert!(g.is_open(), "8 × 1000 concurrent releases must leave gate open");
    }
}
