//! TokioTransport — implements arbitro_engine::Transport over TCP.
//!
//! Each connection has a bounded write channel. The Transport trait's
//! send/send_parts methods enqueue frames into the channel.
//! The write loop drains the channel and writes to TCP.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::mpsc;

use arbitro_engine::Transport;
use arbitro_proto::ids::ConnId;

use crate::session::{ConnIdGen, Session, SessionState};

/// TCP transport backed by per-connection bounded channels.
pub struct TokioTransport {
    sessions: Mutex<HashMap<ConnId, Session>>,
    conn_id_gen: ConnIdGen,
    write_buffer_cap: usize,
}

impl TokioTransport {
    pub fn new(write_buffer_cap: usize) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            conn_id_gen: ConnIdGen::new(),
            write_buffer_cap,
        }
    }

    /// Allocate a new connection ID and create its write channel.
    /// Returns (conn_id, receiver) — caller spawns the write loop with rx.
    pub fn register(&self) -> (ConnId, mpsc::Receiver<Bytes>) {
        let conn_id = self.conn_id_gen.next();
        let (tx, rx) = mpsc::channel(self.write_buffer_cap);

        let session = Session {
            conn_id,
            tx,
            last_activity: Instant::now(),
            state: SessionState::Connecting,
        };

        let mut sessions = self.sessions.lock().unwrap();
        sessions.insert(conn_id, session);

        (conn_id, rx)
    }

    /// Remove a session. Dropping the Sender closes the write loop.
    pub fn remove(&self, conn_id: ConnId) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.remove(&conn_id);
    }

    /// Update last activity timestamp.
    pub fn touch(&self, conn_id: ConnId) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(s) = sessions.get_mut(&conn_id) {
            s.last_activity = Instant::now();
        }
    }

    /// Mark a session as active (authenticated).
    pub fn activate(&self, conn_id: ConnId) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(s) = sessions.get_mut(&conn_id) {
            s.state = SessionState::Active;
        }
    }

    /// Number of active sessions.
    pub fn active_count(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }

    /// Collect connection IDs that have been idle longer than the given duration.
    pub fn idle_connections(&self, timeout: std::time::Duration) -> Vec<ConnId> {
        let now = Instant::now();
        let sessions = self.sessions.lock().unwrap();
        sessions.values()
            .filter(|s| now.duration_since(s.last_activity) > timeout)
            .map(|s| s.conn_id)
            .collect()
    }

    /// Collect connection IDs that need a keepalive ping.
    pub fn connections_needing_ping(&self, interval: std::time::Duration) -> Vec<ConnId> {
        let now = Instant::now();
        let sessions = self.sessions.lock().unwrap();
        sessions.values()
            .filter(|s| s.state == SessionState::Active
                && now.duration_since(s.last_activity) > interval)
            .map(|s| s.conn_id)
            .collect()
    }

    /// Send ServerShuttingDown to all connections and mark as draining.
    pub fn drain_all(&self) {
        let mut sessions = self.sessions.lock().unwrap();
        for session in sessions.values_mut() {
            session.state = SessionState::Draining;
        }
    }

    /// Get all connection IDs.
    pub fn all_conn_ids(&self) -> Vec<ConnId> {
        let sessions = self.sessions.lock().unwrap();
        sessions.keys().copied().collect()
    }
}

impl Transport for TokioTransport {
    fn send(&self, conn_id: ConnId, data: &[u8]) -> bool {
        let sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.get(&conn_id) {
            // try_send for backpressure — if full, slow consumer
            match session.tx.try_send(Bytes::copy_from_slice(data)) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(conn_id, "slow consumer, dropping");
                    false
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        } else {
            false
        }
    }

    fn send_parts(&self, conn_id: ConnId, parts: &[&[u8]]) -> bool {
        // Concatenate parts into a single Bytes for the channel.
        // This is on the engine thread, not the hot path of the transport itself.
        let total: usize = parts.iter().map(|p| p.len()).sum();
        let mut buf = Vec::with_capacity(total);
        for part in parts {
            buf.extend_from_slice(part);
        }
        self.send(conn_id, &buf)
    }

    fn close(&self, conn_id: ConnId) {
        self.remove(conn_id);
    }
}
