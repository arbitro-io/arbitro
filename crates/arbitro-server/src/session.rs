//! Session — per-connection state on the server side.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::mpsc;

use arbitro_proto::ids::ConnId;

/// Per-connection session on the server.
pub struct Session {
    pub conn_id: ConnId,
    /// Bounded write channel — backpressure on slow consumers.
    pub tx: mpsc::Sender<Bytes>,
    /// Last activity timestamp — for idle timeout.
    pub last_activity: Instant,
    /// Connection state.
    pub state: SessionState,
}

/// Connection lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Waiting for Connect frame.
    Connecting,
    /// Authenticated and active.
    Active,
    /// Server is shutting down, draining write buffer.
    Draining,
    /// Closed — about to be removed.
    Closed,
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
    pub fn next(&self) -> ConnId {
        self.next.fetch_add(1, Relaxed)
    }
}
