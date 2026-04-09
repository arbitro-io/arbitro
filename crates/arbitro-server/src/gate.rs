//! Gate — drain delivery signal for the shard thread.
//!
//! The shard calls `release()` after publish/ack/nack to signal new work.
//! The shard loop checks `is_open()` to decide whether to run drain_deliver.
//! When drain_deliver finds nothing, it calls `lock()` — the shard parks.
//!
//! Benchmarked: ~80ns under load, 0% CPU when idle.
//! Spin 512× (~1µs) then park. Wakes via unpark().

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};

/// Shard drain gate — controls when delivery runs.
///
/// Lives inside `ShardWorker` (one per shard thread). Not Clone, not Arc.
/// External wake comes from `ShardHandle` calling `thread.unpark()`.
#[repr(align(64))]
pub struct Gate {
    locked: AtomicBool,
    parked: AtomicBool,
    worker: UnsafeCell<Option<std::thread::Thread>>,
}

// Safety: only the shard thread reads `worker` after set_worker().
// `locked` and `parked` are AtomicBool — safe across threads.
unsafe impl Sync for Gate {}

impl Gate {
    pub fn new() -> Self {
        Self {
            locked: AtomicBool::new(true),
            parked: AtomicBool::new(false),
            worker: UnsafeCell::new(None),
        }
    }

    /// Called once at shard thread startup.
    pub fn set_worker(&self, t: std::thread::Thread) {
        unsafe { *self.worker.get() = Some(t); }
    }

    /// Signal that drain work is available. Non-blocking, O(1).
    /// If the shard thread is parked, wakes it via unpark().
    #[inline]
    pub fn release(&self) {
        self.locked.store(false, Ordering::Relaxed);
        if self.parked.load(Ordering::Relaxed) {
            unsafe {
                if let Some(t) = &*self.worker.get() {
                    t.unpark();
                }
            }
        }
    }

    /// Mark gate as closed — no drain work pending.
    /// Called by drain_deliver when it finds nothing to deliver.
    #[inline]
    pub fn lock(&self) {
        self.locked.store(true, Ordering::Relaxed);
    }

    /// Check if drain work is available (non-blocking).
    #[inline]
    pub fn is_open(&self) -> bool {
        !self.locked.load(Ordering::Relaxed)
    }

    /// Spin-wait then park. Used as the idle-wait step in the shard loop.
    ///
    /// Spins 512× (~1µs) checking if gate opens — avoids park syscall under load.
    /// If still locked after spin, parks once and returns on any unpark
    /// (gate.release or ShardHandle.unpark). The caller's loop re-checks both sources.
    #[inline]
    pub fn acquire(&self) {
        // Fast path
        if !self.locked.load(Ordering::Relaxed) { return; }
        // Spin phase — absorbs latency under load
        for _ in 0..512 {
            if !self.locked.load(Ordering::Relaxed) { return; }
            std::hint::spin_loop();
        }
        // Park phase — single park, returns on any unpark
        self.parked.store(true, Ordering::Relaxed);
        std::thread::park();
        self.parked.store(false, Ordering::Relaxed);
    }
}
