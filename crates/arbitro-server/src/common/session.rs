//! Session — per-connection transport handle.
//!
//! The engine owns connection lifecycle (open/drain/bindings/pending).
//! Session is ONLY the TCP transport layer: shared writer handle +
//! keepalive timer. The drain writes directly via `try_write` and waits
//! on tokio's `writable()` notifications (backed by the reactor) when
//! the kernel buffer fills — no intermediate mpsc channel, no writer task.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Instant;

use tokio::net::tcp::OwnedWriteHalf;

/// Per-connection transport handle. NOT lifecycle — engine owns that.
pub struct Session {
    /// Shared owned write-half of the TCP socket. Both the registry
    /// (admin replies) and the drain (RepBatch frames) hold Arc clones
    /// and call `try_write` / `writable()` directly. No Mutex needed —
    /// both methods take `&self`.
    pub writer: Arc<OwnedWriteHalf>,
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
