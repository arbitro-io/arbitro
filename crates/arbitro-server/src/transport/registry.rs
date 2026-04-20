//! ConnectionRegistry — shared handles to per-connection TCP sockets.
//!
//! Each connection stores an `Arc<OwnedWriteHalf>` — no intermediate
//! channel, no writer task. The drain and admin reply paths use
//! `try_write` + `Handle::block_on(writable())` to cooperate with the
//! tokio reactor when the kernel send buffer is full.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::runtime::Handle;

use arbitro_proto::lifecycle::LifeCycle;

use crate::common::session::{ConnIdGen, Session};

/// TCP transport. Clone-friendly — backed by Arc.
#[derive(Clone)]
pub struct ConnectionRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    // conn_id is unbounded-dense (ConnIdGen monotonic counter). Memory
    // footprint of a Vec<Option<Session>> would grow without bound, so we
    // use HashMap + ahash per the dense/sparse rule (performance.md).
    sessions: Mutex<HashMap<u64, Session, foldhash::fast::FixedState>>,
    conn_id_gen: ConnIdGen,
    #[allow(dead_code)]
    write_buffer_cap: usize,
    /// Tokio runtime handle — cached at construction so non-async callers
    /// (drain OS thread, admin replies) can `block_on(writable())` without
    /// capturing the handle per call.
    runtime: Handle,
}

impl ConnectionRegistry {
    pub fn new(write_buffer_cap: usize) -> Self {
        let runtime = Handle::try_current()
            .expect("ConnectionRegistry::new must be called inside a tokio runtime");
        Self {
            inner: Arc::new(Inner {
                sessions: Mutex::new(HashMap::with_hasher(foldhash::fast::FixedState::default())),
                conn_id_gen: ConnIdGen::new(),
                write_buffer_cap,
                runtime,
            }),
        }
    }

    /// Register a new connection. Caller supplies the shared writer half.
    pub fn register(&self, writer: Arc<OwnedWriteHalf>) -> u64 {
        let conn_id = self.inner.conn_id_gen.next();
        let session = Session {
            writer,
            last_activity: Instant::now(),
        };
        self.inner
            .sessions
            .lock()
            .unwrap()
            .insert(conn_id, session);
        conn_id
    }

    /// Remove a session.
    pub fn remove(&self, conn_id: u64) {
        let mut sessions = self.inner.sessions.lock().unwrap();
        sessions.remove(&conn_id);
    }

    /// Update last activity timestamp.
    pub fn touch(&self, conn_id: u64) {
        let mut sessions = self.inner.sessions.lock().unwrap();
        if let Some(s) = sessions.get_mut(&conn_id) {
            s.last_activity = Instant::now();
        }
    }

    /// Clone the shared writer half for a connection. O(1) — one Arc
    /// refcount bump. Used by the shard worker to cache the writer in
    /// `ActiveBinding` at subscribe time so the drainer hot path can
    /// write directly without touching the registry Mutex.
    pub fn get_writer(&self, conn_id: u64) -> Option<Arc<OwnedWriteHalf>> {
        let sessions = self.inner.sessions.lock().unwrap();
        sessions.get(&conn_id).map(|s| Arc::clone(&s.writer))
    }

    /// Clone the cached tokio runtime handle (cheap — Arc bump).
    pub fn runtime_handle(&self) -> Handle {
        self.inner.runtime.clone()
    }

    /// Number of active sessions.
    pub fn active_count(&self) -> usize {
        self.inner.sessions.lock().unwrap().len()
    }

    pub fn idle_connections(&self, timeout: std::time::Duration) -> Vec<u64> {
        let now = Instant::now();
        let sessions = self.inner.sessions.lock().unwrap();
        sessions
            .iter()
            .filter(|(_, s)| now.duration_since(s.last_activity) > timeout)
            .map(|(&id, _)| id)
            .collect()
    }

    pub fn connections_needing_ping(&self, interval: std::time::Duration) -> Vec<u64> {
        let now = Instant::now();
        let sessions = self.inner.sessions.lock().unwrap();
        sessions
            .iter()
            .filter(|(_, s)| now.duration_since(s.last_activity) > interval)
            .map(|(&id, _)| id)
            .collect()
    }

    pub fn all_conn_ids(&self) -> Vec<u64> {
        let sessions = self.inner.sessions.lock().unwrap();
        sessions.keys().copied().collect()
    }

    /// Send frame parts to a connection. Exactly ONE alloc+copy.
    #[inline]
    pub fn send_parts(&self, conn_id: u64, parts: &[&[u8]]) -> bool {
        let total: usize = parts.iter().map(|p| p.len()).sum();
        let mut buf = BytesMut::with_capacity(total);
        for part in parts {
            buf.extend_from_slice(part);
        }
        self.write_to(conn_id, &buf.freeze())
    }

    /// Zero-copy send — writes an already-built `Bytes`.
    #[inline]
    pub fn send_bytes(&self, conn_id: u64, data: Bytes) -> bool {
        self.write_to(conn_id, &data)
    }

    /// Blocking send — used by the shard worker for acks and replies.
    /// Applies natural backpressure via `writable().await` when the kernel
    /// buffer is full. Returns `false` on closed / dead connection.
    #[inline]
    pub fn send_bytes_blocking(&self, conn_id: u64, data: Bytes) -> bool {
        self.write_to(conn_id, &data)
    }

    fn write_to(&self, conn_id: u64, frame: &[u8]) -> bool {
        let writer = {
            let sessions = self.inner.sessions.lock().unwrap();
            match sessions.get(&conn_id) {
                Some(s) => Arc::clone(&s.writer),
                None => return false,
            }
        };
        write_all_blocking(&writer, frame, &self.inner.runtime)
    }
}

/// Write every byte of `frame` to the socket. Non-blocking `try_write`
/// until `WouldBlock`; then `Handle::block_on(writer.writable())` to park
/// the caller in the tokio reactor until the kernel buffer has space.
///
/// Returns `false` on a non-recoverable error (closed / reset socket).
/// Safe to call from any thread — including OS threads not owned by the
/// tokio runtime (the `Handle::block_on` drives the reactor for us).
pub fn write_all_blocking(writer: &OwnedWriteHalf, frame: &[u8], handle: &Handle) -> bool {
    let mut off = 0;
    while off < frame.len() {
        match writer.try_write(&frame[off..]) {
            Ok(0) => return false,
            Ok(n) => off += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // If called from inside a tokio worker (admin replies, pings,
                // keepalive), `Handle::block_on` would panic — use
                // `block_in_place` to temporarily move the thread out of the
                // worker pool. From the drain OS thread, `try_current` fails
                // and we use `block_on` directly.
                let res = if Handle::try_current().is_ok() {
                    tokio::task::block_in_place(|| handle.block_on(writer.writable()))
                } else {
                    handle.block_on(writer.writable())
                };
                if res.is_err() {
                    return false;
                }
            }
            Err(_) => return false,
        }
    }
    true
}

impl LifeCycle for ConnectionRegistry {
    fn on_init(&mut self) {
        tracing::info!("ConnectionRegistry: init (direct-write, no channel)");
    }

    fn on_shutdown(&mut self) {
        let mut sessions = self.inner.sessions.lock().unwrap();
        let count = sessions.len();
        sessions.clear();
        tracing::info!(
            "ConnectionRegistry: shutdown, closed {} sessions",
            count
        );
    }
}
