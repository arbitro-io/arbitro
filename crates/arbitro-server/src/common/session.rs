//! Session — per-connection transport handle.
//!
//! Each connection owns a dedicated async writer task that drains an
//! MPSC channel of pre-encoded `Bytes` frames and calls `write_all`
//! on `OwnedWriteHalf`. All send paths (dispatch, drain, keepalive) are
//! non-blocking `try_send` — backpressure drops the frame if the per-conn
//! queue is full, preventing deadlocks in the shared tokio runtime.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;

/// Outbound frame queue capacity per connection.
pub const CONN_WRITE_CAP: usize = 4096;

/// Per-connection transport handle. NOT lifecycle — engine owns that.
pub struct Session {
    /// Sender half of the per-connection frame queue. Non-blocking
    /// `try_send` pushes frames; the writer task drains them.
    pub write_tx: mpsc::Sender<Bytes>,
    /// Last activity timestamp (epoch-millis since UNIX_EPOCH) — for
    /// idle timeout / keepalive. **F8**: AtomicU64 instead of `Instant`
    /// so `touch()` doesn't need to take the registry mutex; readers
    /// (idle sweep + keepalive sweep) load with Relaxed.
    pub last_activity: AtomicU64,
    /// **M8**: writer feedback — set to `true` by the writer task when
    /// `write_all` hits an I/O error. The drain path reads this with
    /// `Relaxed` to detect dead connections before wasting frames into
    /// the channel. Shared via `Arc` so the writer task can outlive the
    /// session map entry during shutdown races.
    pub write_failed: Arc<AtomicBool>,
    /// **M8**: total frames successfully written to the socket. The
    /// writer task increments after each `write_all` success. Used for
    /// observability and back-pressure detection (compare with frames
    /// enqueued via `try_send`).
    pub frames_written: Arc<AtomicU64>,
    /// Who this connection authenticated as. Written once, by the handshake,
    /// immediately after credentials are accepted; never mutated afterwards
    /// (there is no re-auth — credential rotation means reconnecting).
    ///
    /// This is the authorization seam. A per-action permission check is
    /// `registry.identity(conn_id)` inside the `match action` in
    /// `transport::dispatch_v2` — which already holds `conn_id` and the
    /// registry, so no signature anywhere needs to change. `Arc` so readers
    /// clone a pointer, not a `Vec<Permission>`, while holding the map lock.
    pub identity: Arc<crate::auth::Identity>,
    /// Which shard's listener accepted this connection, or `None` for the
    /// bootstrap socket (and for every connection when per-shard listeners
    /// are off).
    ///
    /// Recorded, not enforced. Routing is still per stream, so a connection
    /// that arrived on shard 3's port can still publish to a stream on
    /// shard 0 — this only makes that fact observable. Without it the extra
    /// ports are indistinguishable doors to the same path, and nothing
    /// downstream could ever tell a client dialed the right shard.
    pub listener_shard: Option<u16>,
}

/// Atomic connection ID generator.
pub struct ConnIdGen {
    next: AtomicU64,
}

impl Default for ConnIdGen {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnIdGen {
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    #[inline]
    pub fn next(&self) -> u64 {
        self.next.fetch_add(1, Relaxed)
    }
}
