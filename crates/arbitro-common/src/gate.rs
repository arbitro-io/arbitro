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
