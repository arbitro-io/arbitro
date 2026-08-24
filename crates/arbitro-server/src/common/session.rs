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
    pub last_activity: Arc<AtomicU64>,
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

/// Everything the connection's own read loop needs, handed over once at
/// registration.
///
/// This exists so the hot path never looks itself up. `touch` used to take
/// the registry's global `Mutex<HashMap<Session>>` and a `RwLock` on the
/// clock — on EVERY frame, from every connection of every shard, against
/// one lock. Instrumentation put that phase at ~880 ns/frame, which is not
/// the cost of splitting a frame; it is the cost of contending for a map
/// the caller did not need to consult.
///
/// A connection knows who it is. Under share-nothing there is nothing to
/// coordinate here, so there is nothing to lock.
#[derive(Clone)]
pub struct ConnHandle {
    pub conn_id: u64,
    /// This connection's outbound queue, held directly.
    ///
    /// The reply path used to reach it through the registry's global
    /// `Mutex<HashMap<Session>>` — on every reply, from every connection
    /// of every shard, against one lock. A connection replying to itself
    /// has no reason to consult a map of everyone.
    write_tx: mpsc::Sender<Bytes>,
    last_activity: Arc<AtomicU64>,
    /// `None` only in unit tests that build a registry without a clock;
    /// those pay a `SystemTime::now()` per touch, which is fine on a path
    /// nothing measures.
    clock: Option<arbitro_common::SharedClock>,
    /// The shard this connection lives on.
    ///
    /// Not an `Option`: a connection is accepted BY a shard and belongs to
    /// it. Holding it is what lets a reply write straight to the socket
    /// instead of asking anyone whether it may.
    shard: std::rc::Rc<crate::shard::shard::Shard>,
}

impl ConnHandle {
    pub fn new(
        conn_id: u64,
        write_tx: mpsc::Sender<Bytes>,
        last_activity: Arc<AtomicU64>,
        clock: Option<arbitro_common::SharedClock>,
        shard: std::rc::Rc<crate::shard::shard::Shard>,
    ) -> Self {
        Self {
            conn_id,
            write_tx,
            last_activity,
            clock,
            shard,
        }
    }

    /// The shard that owns this connection.
    #[inline]
    pub fn shard(&self) -> &std::rc::Rc<crate::shard::shard::Shard> {
        &self.shard
    }

    /// Queue a frame for this connection. No lock, no lookup.
    ///
    /// `false` means the outbound queue is full and the frame was
    /// dropped — the same behaviour the registry had, surfaced to the
    /// caller instead of buried.
    #[inline]
    pub fn send(&self, frame: Bytes) -> bool {
        self.write_tx.try_send(frame).is_ok()
    }

    /// Mark this connection alive. One relaxed store — no lock, no lookup.
    ///
    /// Relaxed is right: the only reader is the idle sweep, which asks "was
    /// this touched within the last N seconds". A sweep that sees a
    /// slightly stale value re-checks seconds later, and being off by a few
    /// microseconds cannot make a live connection look idle.
    #[inline]
    pub fn touch(&self) {
        let now = match &self.clock {
            Some(c) => c.now_ms(),
            None => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        self.last_activity
            .store(now, std::sync::atomic::Ordering::Relaxed);
    }
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
