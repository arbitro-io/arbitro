//! Shared client state — `Inner` plus all sub-modules.
//!
//! `Inner` is the single owner of all mutable state (producers, pending
//! map, subscriptions, ack channel, heartbeat timestamp).  It lives in
//! an `Arc` shared by every `Client` clone and every spawned task.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;

use crate::config::ClientConfig;
use crate::consume::message::{AckCmd, NackCmd};
use crate::metrics::ClientMetrics;
use crate::state::pending::Pending;
use crate::state::seq::{SeqAllocator, SubIdAllocator};
use crate::state::subscriptions::Subscriptions;
use crate::transport::frame::WritePool;

pub(crate) mod pending;
pub(crate) mod seq;
pub(crate) mod subscriptions;

/// All mutable state shared across sessions and tasks.
pub(crate) struct Inner {
    /// Static configuration (addr, reconnect policy, keep-alive timings).
    pub(crate) cfg: ClientConfig,
    /// Shared pool of write producers.  Ad-hoc callers (publish/manage/cron)
    /// lease a slot for the duration of a single `try_send`; the long-lived
    /// background tasks (heartbeat, ack/nack batchers, session replay) hold
    /// a dedicated lease for their whole lifetime.
    pub(crate) pool: Arc<WritePool>,
    /// In-flight request-reply correlation (`seq → OneShotAsyncSender`).
    pub(crate) pending: Arc<Pending>,
    /// Monotonic u64 sequence counter.
    pub(crate) seq_alloc: SeqAllocator,
    /// Monotonic subscription-id counter, connection-scoped.
    pub(crate) sub_id_alloc: SubIdAllocator,
    /// Root cancellation token — cancelled on `Client::close()` or drop.
    pub(crate) cancel: CancellationToken,
    /// Session-scoped cancel — set by the reconnect loop on each new session,
    /// cancelled by the heartbeat watchdog on stale pong. Never cancels the
    /// client-level `cancel`, so reconnect keeps running.
    pub(crate) session_cancel: Mutex<Option<CancellationToken>>,
    /// Active subscriptions: `consumer_id → delivery channel + replay body`.
    pub(crate) subscriptions: Arc<Subscriptions>,
    /// Sender into the ack-batcher task.  Cloned cheaply into every `Message`.
    pub(crate) ack_tx: tokio::sync::mpsc::Sender<AckCmd>,
    /// Sender into the nack-batcher task.  Cloned cheaply into every `Message`.
    pub(crate) nack_tx: tokio::sync::mpsc::Sender<NackCmd>,
    /// Nanoseconds since the Unix epoch of the last received `Pong` (or
    /// the time the session was established).  Updated by reader task;
    /// read by heartbeat watchdog.
    pub(crate) last_pong_ns: AtomicU64,
    /// Atomic client counters — shared with hot-path tasks for cheap
    /// `fetch_add(Relaxed)` observability. See [`crate::metrics`].
    pub(crate) metrics: Arc<ClientMetrics>,
    /// Active cron job handlers — keyed by name, used by dispatch + reconnect.
    pub(crate) cron_state: crate::cron::CronState,
    /// Ack-reliability hot layer: per-consumer deferred-ack tracking.
    pub(crate) ackrel: Arc<crate::ackrel::AckRelay>,
    /// Optional durable redelivery-dedup store (WAL). `None` unless the caller
    /// enables persistence via [`crate::config::ClientConfig::ack_store`].
    /// Replaces the old SQLite cold tier.
    pub(crate) ack_store: Option<Arc<dyn crate::ackstore::Store>>,
}

impl Inner {
    /// Current time as nanoseconds since the Unix epoch.
    #[inline]
    pub(crate) fn now_ns() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.pending.drain_disconnected();
    }
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("addr", &self.cfg.addr)
            .finish()
    }
}
