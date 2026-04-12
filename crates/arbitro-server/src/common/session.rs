//! Session — per-connection transport handle.
//!
//! The engine owns connection lifecycle (open/drain/bindings/pending).
//! Session is ONLY the TCP transport layer: write channel + keepalive timer.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::mpsc;

/// Per-connection transport handle. NOT lifecycle — engine owns that.
pub struct Session {
    /// Bounded write channel — backpressure on slow consumers.
    pub tx: mpsc::Sender<Bytes>,
    /// Last activity timestamp — for idle timeout / keepalive.
    pub last_activity: Instant,
}

/// Atomic connection ID generator.
pub struct ConnIdGen {
    next: AtomicU64,
}

impl Default for ConnIdGen {
    fn default() -> Self { Self::new() }
}

impl ConnIdGen {
    pub fn new() -> Self {
        Self { next: AtomicU64::new(1) }
    }

    #[inline]
    pub fn next(&self) -> u64 {
        self.next.fetch_add(1, Relaxed)
    }
}
