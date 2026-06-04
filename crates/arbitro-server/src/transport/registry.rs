//! ConnectionRegistry — shared handles to per-connection outbound queues.
//!
//! Each connection gets a dedicated async writer task that owns the
//! writer half (plain TCP or TLS) and drains an `mpsc::channel<Bytes>`.
//! All send paths use non-blocking `try_send` — no `block_in_place`, no
//! mutex around the socket. If the queue is full the frame is dropped
//! (connection too slow); the tokio reactor is never starved.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use arbitro_common::SharedClock;
use arbitro_proto::lifecycle::LifeCycle;

use crate::common::session::{ConnIdGen, Session, CONN_WRITE_CAP};

/// Minimum allowed per-connection outbound buffer capacity. A value of 0
/// from `ARBITRO_WRITE_BUFFER_CAP` would deadlock every write
/// (`try_send` always returns `Full`), so we clamp to a safe floor.
const MIN_WRITE_BUFFER_CAP: usize = 16;

/// **F8 helper** — current "now" in milliseconds. Reads the shared
/// clock if one is set on the registry, otherwise falls back to a
/// per-call `SystemTime::now()` (used in unit-tests / standalone).
#[inline]
fn now_ms(clock: &Option<SharedClock>) -> u64 {
    match clock {
        Some(c) => c.now_ms(),
        None => std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    }
}

/// **F36**: monomorphic writer enum — eliminates vtable indirection.
/// Each variant holds the concrete type, so `write_all` inlines to a
/// direct method call instead of an indirect function pointer.
pub enum ConnWriter {
    Plain(tokio::net::tcp::OwnedWriteHalf),
    #[cfg(feature = "tls")]
    Tls(tokio::io::WriteHalf<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>),
}

impl tokio::io::AsyncWrite for ConnWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ConnWriter::Plain(w) => std::pin::Pin::new(w).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            ConnWriter::Tls(w) => std::pin::Pin::new(w).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnWriter::Plain(w) => std::pin::Pin::new(w).poll_flush(cx),
            #[cfg(feature = "tls")]
            ConnWriter::Tls(w) => std::pin::Pin::new(w).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnWriter::Plain(w) => std::pin::Pin::new(w).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            ConnWriter::Tls(w) => std::pin::Pin::new(w).poll_shutdown(cx),
        }
    }
}

/// **F36**: monomorphic reader enum — eliminates vtable indirection.
pub enum ConnReader {
    Plain(tokio::net::tcp::OwnedReadHalf),
    #[cfg(feature = "tls")]
    Tls(tokio::io::ReadHalf<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>),
}

impl tokio::io::AsyncRead for ConnReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ConnReader::Plain(r) => std::pin::Pin::new(r).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            ConnReader::Tls(r) => std::pin::Pin::new(r).poll_read(cx, buf),
        }
    }
}

/// TCP transport. Clone-friendly — backed by Arc.
#[derive(Clone)]
pub struct ConnectionRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    /// **F8**: `parking_lot::Mutex` (no poison handling, faster
    /// uncontested path) wrapping the sessions map. `touch()` no
    /// longer takes this mutex thanks to the AtomicU64 `last_activity`
    /// inside `Session` — the only consumers of the lock are
    /// register / remove / get_write_tx / enqueue / sweep, all of which
    /// either run on the cold path or do a quick map lookup.
    sessions: parking_lot::Mutex<HashMap<u64, Session, foldhash::fast::FixedState>>,
    conn_id_gen: ConnIdGen,
    /// Optional shared millisecond clock for last-activity reads.
    /// Server wires it in `set_clock()`; tests can leave it None and
    /// pay a per-call `SystemTime::now()` (rare paths).
    clock: parking_lot::RwLock<Option<SharedClock>>,
    /// H13: per-connection mpsc capacity. Was previously ignored — every
    /// channel allocated `CONN_WRITE_CAP` (4096) regardless of
    /// `ARBITRO_WRITE_BUFFER_CAP`. Now honoured at `register()` time.
    write_buffer_cap: usize,
    /// H10: counter bumped every time an `enqueue()` dropped a frame
    /// because the per-connection mpsc was full. Optional so tests can
    /// continue to construct a registry without wiring it up.
    silent_drops: Option<Arc<crate::common::SilentDrops>>,
}

impl ConnectionRegistry {
    pub fn new(write_buffer_cap: usize) -> Self {
        // 0 would mean "every try_send returns Full" — clamp to a safe
        // floor. Callers who genuinely want the historical 4096 default
        // still get it via `CONN_WRITE_CAP` (config.rs uses 8192).
        let cap = write_buffer_cap.max(MIN_WRITE_BUFFER_CAP);
        Self {
            inner: Arc::new(Inner {
                sessions: parking_lot::Mutex::new(HashMap::with_hasher(
                    foldhash::fast::FixedState::default(),
                )),
                conn_id_gen: ConnIdGen::new(),
                clock: parking_lot::RwLock::new(None),
                write_buffer_cap: cap,
                silent_drops: None,
            }),
        }
    }

    /// H10: wire the silent-drop counters. Called by the server after
    /// construction so the registry can bump `conn_write` on every
    /// dropped outbound frame.
    pub fn set_silent_drops(&mut self, sd: Arc<crate::common::SilentDrops>) {
        // We hold the only Arc<Inner> at this point (called from
        // `ArbitroServer::new` before any clone). Use `get_mut` for a
        // safe in-place update.
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.silent_drops = Some(sd);
        }
    }

    /// Wire a shared millisecond clock so `touch` / sweeps avoid a
    /// per-call `SystemTime::now()` syscall.
    pub fn set_clock(&self, clock: SharedClock) {
        *self.inner.clock.write() = Some(clock);
    }

    /// Register a new connection. Spawns a writer task that owns `writer`
    /// and drains the per-connection frame queue. Returns the `conn_id`.
    ///
    /// Accepts any `AsyncWrite` — plain TCP (`OwnedWriteHalf`) or TLS.
    pub fn register(&self, writer: ConnWriter) -> u64 {
        let conn_id = self.inner.conn_id_gen.next();
        // H13: honour the configured per-connection capacity. Fallback
        // is the historical default if the field is unset (zero).
        let cap = if self.inner.write_buffer_cap == 0 {
            CONN_WRITE_CAP
        } else {
            self.inner.write_buffer_cap
        };
        let (tx, rx) = mpsc::channel::<Bytes>(cap);
        // M8: shared feedback atomics — writer task signals failures and
        // counts successful writes so the drain path skips dead connections.
        let write_failed = Arc::new(AtomicBool::new(false));
        let frames_written = Arc::new(AtomicU64::new(0));
        // H6: writer task removes the session from the registry on
        // write error so a half-dead peer cannot pile up forever.
        let inner = Arc::clone(&self.inner);
        // M15: supervise the writer task — panics here would silently
        // strand the connection's mpsc receiver. Watcher logs and exits
        // when the child resolves (normal or panic).
        let writer_handle = tokio::spawn(conn_writer_task(
            rx,
            writer,
            conn_id,
            inner,
            Arc::clone(&write_failed),
            Arc::clone(&frames_written),
        ));
        let cid_for_log = conn_id;
        tokio::spawn(async move {
            match writer_handle.await {
                Ok(()) => {}
                Err(e) if e.is_panic() => {
                    tracing::error!(
                        target = "supervisor",
                        conn = cid_for_log,
                        "conn writer task panicked: {e}"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        target = "supervisor",
                        conn = cid_for_log,
                        "conn writer join error: {e}"
                    );
                }
            }
        });
        let clock = self.inner.clock.read().clone();
        let session = Session {
            write_tx: tx,
            last_activity: std::sync::atomic::AtomicU64::new(now_ms(&clock)),
            write_failed,
            frames_written,
        };
        self.inner.sessions.lock().insert(conn_id, session);
        conn_id
    }

    /// Remove a session — drops the Sender, which closes the writer task.
    pub fn remove(&self, conn_id: u64) {
        self.inner.sessions.lock().remove(&conn_id);
    }

    /// Update last activity timestamp. **F8**: no longer takes the
    /// outer sessions mutex — we briefly hold the lock to look up the
    /// session, then store directly into the per-session AtomicU64.
    /// `Instant::now()` on every frame is replaced by an Atomic load
    /// from the shared clock.
    pub fn touch(&self, conn_id: u64) {
        let clock = self.inner.clock.read().clone();
        let now = now_ms(&clock);
        let sessions = self.inner.sessions.lock();
        if let Some(s) = sessions.get(&conn_id) {
            s.last_activity.store(now, Relaxed);
        }
    }

    /// Clone the write sender for a connection. Used by the shard to cache
    /// the sender in `ActiveBinding` at subscribe time.
    pub fn get_write_tx(&self, conn_id: u64) -> Option<mpsc::Sender<Bytes>> {
        let sessions = self.inner.sessions.lock();
        sessions.get(&conn_id).map(|s| s.write_tx.clone())
    }

    /// **M8**: clone the writer-feedback flag for a connection so the drain
    /// snapshot can check it inline without taking the registry lock.
    pub fn get_write_failed(&self, conn_id: u64) -> Option<Arc<AtomicBool>> {
        let sessions = self.inner.sessions.lock();
        sessions.get(&conn_id).map(|s| Arc::clone(&s.write_failed))
    }

    /// Number of active sessions.
    pub fn active_count(&self) -> usize {
        self.inner.sessions.lock().len()
    }

    pub fn idle_connections(&self, timeout: std::time::Duration) -> Vec<u64> {
        let clock = self.inner.clock.read().clone();
        let now = now_ms(&clock);
        let to_ms = timeout.as_millis() as u64;
        let sessions = self.inner.sessions.lock();
        sessions
            .iter()
            .filter(|(_, s)| now.saturating_sub(s.last_activity.load(Relaxed)) > to_ms)
            .map(|(&id, _)| id)
            .collect()
    }

    pub fn connections_needing_ping(&self, interval: std::time::Duration) -> Vec<u64> {
        let clock = self.inner.clock.read().clone();
        let now = now_ms(&clock);
        let iv_ms = interval.as_millis() as u64;
        let sessions = self.inner.sessions.lock();
        sessions
            .iter()
            .filter(|(_, s)| now.saturating_sub(s.last_activity.load(Relaxed)) > iv_ms)
            .map(|(&id, _)| id)
            .collect()
    }

    pub fn all_conn_ids(&self) -> Vec<u64> {
        let sessions = self.inner.sessions.lock();
        sessions.keys().copied().collect()
    }

    /// **M8**: check whether the writer task for `conn_id` is still healthy.
    /// Returns `false` if the writer has signalled an I/O error or if the
    /// connection doesn't exist. Drain uses this to skip dead connections
    /// before wasting frames into the channel.
    #[inline]
    pub fn is_conn_healthy(&self, conn_id: u64) -> bool {
        let sessions = self.inner.sessions.lock();
        match sessions.get(&conn_id) {
            Some(s) => !s.write_failed.load(Relaxed),
            None => false,
        }
    }

    /// **M8**: number of frames successfully written to the socket for
    /// `conn_id`. Returns 0 if the connection doesn't exist.
    #[inline]
    pub fn frames_written(&self, conn_id: u64) -> u64 {
        let sessions = self.inner.sessions.lock();
        match sessions.get(&conn_id) {
            Some(s) => s.frames_written.load(Relaxed),
            None => 0,
        }
    }

    /// Enqueue frame parts as a single `Bytes`. Non-blocking — drops if full.
    #[inline]
    pub fn send_parts(&self, conn_id: u64, parts: &[&[u8]]) -> bool {
        let total: usize = parts.iter().map(|p| p.len()).sum();
        let mut buf = bytes::BytesMut::with_capacity(total);
        for part in parts {
            buf.extend_from_slice(part);
        }
        self.enqueue(conn_id, buf.freeze())
    }

    /// F34: Enqueue a small zerocopy frame without BytesMut allocation.
    /// `Bytes::copy_from_slice` uses an inline representation for buffers
    /// ≤ ~31 bytes (RepOkFrame = 24B, Header = 12B), avoiding the heap
    /// entirely on the publish-reply hot path.
    #[inline]
    pub fn send_inline(&self, conn_id: u64, data: &[u8]) -> bool {
        self.enqueue(conn_id, Bytes::copy_from_slice(data))
    }

    /// Enqueue an already-built `Bytes`. Non-blocking — drops if full.
    #[inline]
    pub fn send_bytes(&self, conn_id: u64, data: Bytes) -> bool {
        self.enqueue(conn_id, data)
    }

    #[inline]
    fn enqueue(&self, conn_id: u64, frame: Bytes) -> bool {
        let sessions = self.inner.sessions.lock();
        match sessions.get(&conn_id) {
            Some(s) => match s.write_tx.try_send(frame) {
                Ok(_) => true,
                Err(_) => {
                    // H10: count every dropped frame so operators see
                    // the slow-consumer signal in the metrics line
                    // instead of guessing from broken application logs.
                    if let Some(sd) = &self.inner.silent_drops {
                        sd.inc_conn_write();
                    }
                    false
                }
            },
            None => false,
        }
    }
}

/// Per-connection writer task — owns the writer (plain TCP or TLS), drains the channel.
///
/// H6: on a write error we synchronously remove the session from the
/// registry. Otherwise the `Sender` half kept by the registry would
/// keep the channel open forever and stale entries would accumulate
/// for every disconnect that hit a write error before the read loop
/// noticed the EOF.
async fn conn_writer_task(
    mut rx: mpsc::Receiver<Bytes>,
    mut w: ConnWriter,
    conn_id: u64,
    inner: Arc<Inner>,
    write_failed: Arc<AtomicBool>,
    frames_written: Arc<AtomicU64>,
) {
    let mut err = false;
    while let Some(frame) = rx.recv().await {
        if w.write_all(&frame).await.is_err() {
            // M8: signal the drain path that this connection is dead.
            write_failed.store(true, Relaxed);
            err = true;
            break;
        }
        // M8: bump written count so observers can compare enqueued vs.
        // written for back-pressure and delivery-confirmation.
        frames_written.fetch_add(1, Relaxed);
    }
    let _ = w.shutdown().await;
    if err {
        inner.sessions.lock().remove(&conn_id);
    }
}

impl LifeCycle for ConnectionRegistry {
    fn on_init(&mut self) {
        tracing::info!("ConnectionRegistry: init (async per-conn writer tasks)");
    }

    fn on_shutdown(&mut self) {
        let mut sessions = self.inner.sessions.lock();
        let count = sessions.len();
        sessions.clear(); // drops all Senders → writer tasks exit
        tracing::info!("ConnectionRegistry: shutdown, closed {} sessions", count);
    }
}
