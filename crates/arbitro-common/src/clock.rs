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
use std::time::Instant;

/// Shared cached "now" in milliseconds since the UNIX epoch.
///
/// `Arc`-cloneable: server, router and dispatchers all hold copies.
/// A single tokio task is responsible for keeping it in sync.
///
/// L3: previously used `SystemTime::now().duration_since(UNIX_EPOCH)
/// .unwrap_or_default()` on each `refresh()`. A clock skew that pushed
/// the system clock *backwards* across UNIX_EPOCH (rare but observable
/// during NTP corrections, container migrations, suspend/resume) made
/// `duration_since` return Err and silently coerced the cached time to
/// **0**, which corrupts age-based eviction (every entry "ages out").
///
/// We now anchor a `start_unix_ms` + `start_instant` at construction
/// and derive `now_ms = start_unix_ms + start_instant.elapsed().as_millis()`.
/// `Instant` is monotonic on every supported OS — the result can never
/// go backwards, and there is no syscall failure mode to coerce.
#[derive(Debug, Clone)]
pub struct SharedClock {
    inner: Arc<ClockInner>,
}

#[derive(Debug)]
struct ClockInner {
    /// UNIX-epoch milliseconds at construction (latched once).
    start_unix_ms: u64,
    /// Monotonic anchor — `start_instant.elapsed()` is the time since boot.
    start_instant: Instant,
    /// Cached `start_unix_ms + elapsed`, updated by the refresh task.
    now_ms: AtomicU64,
}

impl Default for SharedClock {
    fn default() -> Self { Self::new() }
}

impl SharedClock {
    pub fn new() -> Self {
        let start_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let start_instant = Instant::now();
        Self {
            inner: Arc::new(ClockInner {
                start_unix_ms,
                start_instant,
                now_ms: AtomicU64::new(start_unix_ms),
            }),
        }
    }

    /// Read the cached "now" — single relaxed atomic load.
    #[inline(always)]
    pub fn now_ms(&self) -> u64 {
        self.inner.now_ms.load(Ordering::Relaxed)
    }

    /// Refresh the cached value from the monotonic anchor. Called by
    /// the updater task.
    ///
    /// L3: `Instant::elapsed()` cannot fail and cannot move backwards,
    /// so the cached value is always `>= start_unix_ms`.
    #[inline]
    pub fn refresh(&self) {
        let elapsed_ms = self.inner.start_instant.elapsed().as_millis() as u64;
        let now = self.inner.start_unix_ms.saturating_add(elapsed_ms);
        self.inner.now_ms.store(now, Ordering::Relaxed);
    }
}
