//! Shared monotonic millisecond clock — replaces per-publish
//! `SystemTime::now()` syscalls (F7 optimisation in OPTIMIZATION.md).
//!
//! A single `tokio` task updates an `AtomicU64` once per millisecond
//! (1000 Hz). Hot paths read the value with one `Relaxed` load (~1 ns).
//! The previous syscall chain
//! (`SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()`)
//! costs ~25–50 ns on Linux and substantially more on Windows.
//!
//! Resolution is 1 ms which is fine for timestamping store entries —
//! age-based eviction and idempotency windows operate in seconds.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Shared cached "now" in milliseconds since the UNIX epoch.
///
/// `Arc`-cloneable: server, router and dispatchers all hold copies.
/// A single tokio task is responsible for keeping it in sync.
#[derive(Debug, Clone, Default)]
pub struct SharedClock {
    inner: Arc<AtomicU64>,
}

impl SharedClock {
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self { inner: Arc::new(AtomicU64::new(now)) }
    }

    /// Read the cached "now" — single relaxed atomic load.
    #[inline(always)]
    pub fn now_ms(&self) -> u64 {
        self.inner.load(Ordering::Relaxed)
    }

    /// Refresh the cached value from the real clock. Called by the
    /// updater task.
    #[inline]
    pub fn refresh(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.inner.store(now, Ordering::Relaxed);
    }
}
