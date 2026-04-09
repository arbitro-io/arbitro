//! ConnectionRegistry — manages per-connection write channels and session state.
//!
//! Each connection has a bounded write channel. Send methods enqueue frames.
//! The write loop (spawned per connection) drains and writes to TCP with
//! write_vectored.
//!
//! ## Send discipline
//!
//! All frame sending goes through `send_parts`. Callers pass zerocopy struct
//! slices (`envelope.as_bytes()`, `body.as_bytes()`) — NO intermediate stack
//! buffer. `send_parts` performs exactly ONE `BytesMut` allocation+copy to
//! produce the owned `Bytes` required by the mpsc channel. This is the
//! minimum possible with async channels.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;

use arbitro_proto::lifecycle::LifeCycle;

use crate::session::{ConnIdGen, Session};

/// TCP transport backed by per-connection bounded channels.
///
/// Clone-friendly — backed by Arc. Multiple shards share the same registry.
#[derive(Clone)]
pub struct ConnectionRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    sessions: Mutex<HashMap<u64, Session>>,
    conn_id_gen: ConnIdGen,
    write_buffer_cap: usize,
}

impl ConnectionRegistry {
    pub fn new(write_buffer_cap: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                sessions: Mutex::new(HashMap::new()),
                conn_id_gen: ConnIdGen::new(),
                write_buffer_cap,
            }),
        }
    }

    /// Allocate a new connection ID and create its write channel.
    /// Returns (conn_id, receiver) — caller spawns the write loop with rx.
    pub fn register(&self) -> (u64, mpsc::Receiver<Bytes>) {
        let conn_id = self.inner.conn_id_gen.next();
        let (tx, rx) = mpsc::channel(self.inner.write_buffer_cap);

        let session = Session {
            tx,
            last_activity: Instant::now(),
        };

        let mut sessions = self.inner.sessions.lock().unwrap();
        sessions.insert(conn_id, session);

        (conn_id, rx)
    }

    /// Remove a session. Dropping the Sender closes the write loop.
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

    /// Number of active sessions.
    pub fn active_count(&self) -> usize {
        self.inner.sessions.lock().unwrap().len()
    }

    /// Collect connection IDs that have been idle longer than the given duration.
    pub fn idle_connections(&self, timeout: std::time::Duration) -> Vec<u64> {
        let now = Instant::now();
        let sessions = self.inner.sessions.lock().unwrap();
        sessions.iter()
            .filter(|(_, s)| now.duration_since(s.last_activity) > timeout)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Collect connection IDs that need a keepalive ping.
    pub fn connections_needing_ping(&self, interval: std::time::Duration) -> Vec<u64> {
        let now = Instant::now();
        let sessions = self.inner.sessions.lock().unwrap();
        sessions.iter()
            .filter(|(_, s)| now.duration_since(s.last_activity) > interval)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Get all connection IDs.
    pub fn all_conn_ids(&self) -> Vec<u64> {
        let sessions = self.inner.sessions.lock().unwrap();
        sessions.keys().copied().collect()
    }

    /// Send frame parts to a connection. Exactly ONE alloc+copy.
    ///
    /// Callers pass zerocopy `as_bytes()` slices directly — no intermediate
    /// stack buffer needed:
    /// ```ignore
    /// registry.send_parts(conn_id, &[envelope.as_bytes(), body.as_bytes()]);
    /// ```
    #[inline]
    pub fn send_parts(&self, conn_id: u64, parts: &[&[u8]]) -> bool {
        let total: usize = parts.iter().map(|p| p.len()).sum();
        let mut buf = BytesMut::with_capacity(total);
        for part in parts {
            buf.extend_from_slice(part);
        }
        self.try_send_to(conn_id, buf.freeze())
    }

    /// Zero-copy send — takes ownership of an already-built Bytes.
    /// Use for variable-length frames built externally.
    #[inline]
    pub fn send_bytes(&self, conn_id: u64, data: Bytes) -> bool {
        self.try_send_to(conn_id, data)
    }

    fn try_send_to(&self, conn_id: u64, frame: Bytes) -> bool {
        let sessions = self.inner.sessions.lock().unwrap();
        if let Some(session) = sessions.get(&conn_id) {
            match session.tx.try_send(frame) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => false,
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        } else {
            false
        }
    }
}

impl LifeCycle for ConnectionRegistry {
    fn on_init(&mut self) {
        tracing::info!("ConnectionRegistry: init (write_buffer_cap={})", self.inner.write_buffer_cap);
    }

    fn on_shutdown(&mut self) {
        let mut sessions = self.inner.sessions.lock().unwrap();
        let count = sessions.len();
        sessions.clear();
        tracing::info!("ConnectionRegistry: shutdown, closed {} sessions", count);
    }
}
