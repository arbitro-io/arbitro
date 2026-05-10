//! ConnectionRegistry — shared handles to per-connection outbound queues.
//!
//! Each connection gets a dedicated async writer task that owns the
//! writer half (plain TCP or TLS) and drains an `mpsc::channel<Bytes>`.
//! All send paths use non-blocking `try_send` — no `block_in_place`, no
//! mutex around the socket. If the queue is full the frame is dropped
//! (connection too slow); the tokio reactor is never starved.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use arbitro_proto::lifecycle::LifeCycle;

use crate::common::session::{ConnIdGen, Session, CONN_WRITE_CAP};

/// Trait-object writer — works for both plain TCP and TLS.
pub type BoxedWriter = Box<dyn tokio::io::AsyncWrite + Send + Unpin>;
/// Trait-object reader — works for both plain TCP and TLS.
pub type BoxedReader = Box<dyn tokio::io::AsyncRead + Send + Unpin>;

/// TCP transport. Clone-friendly — backed by Arc.
#[derive(Clone)]
pub struct ConnectionRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    sessions: Mutex<HashMap<u64, Session, foldhash::fast::FixedState>>,
    conn_id_gen: ConnIdGen,
}

impl ConnectionRegistry {
    pub fn new(_write_buffer_cap: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                sessions: Mutex::new(HashMap::with_hasher(foldhash::fast::FixedState::default())),
                conn_id_gen: ConnIdGen::new(),
            }),
        }
    }

    /// Register a new connection. Spawns a writer task that owns `writer`
    /// and drains the per-connection frame queue. Returns the `conn_id`.
    ///
    /// Accepts any `AsyncWrite` — plain TCP (`OwnedWriteHalf`) or TLS.
    pub fn register(&self, writer: BoxedWriter) -> u64 {
        let conn_id = self.inner.conn_id_gen.next();
        let (tx, rx) = mpsc::channel::<Bytes>(CONN_WRITE_CAP);
        tokio::spawn(conn_writer_task(rx, writer));
        let session = Session {
            write_tx: tx,
            last_activity: Instant::now(),
        };
        self.inner.sessions.lock().unwrap().insert(conn_id, session);
        conn_id
    }

    /// Remove a session — drops the Sender, which closes the writer task.
    pub fn remove(&self, conn_id: u64) {
        self.inner.sessions.lock().unwrap().remove(&conn_id);
    }

    /// Update last activity timestamp.
    pub fn touch(&self, conn_id: u64) {
        let mut sessions = self.inner.sessions.lock().unwrap();
        if let Some(s) = sessions.get_mut(&conn_id) {
            s.last_activity = Instant::now();
        }
    }

    /// Clone the write sender for a connection. Used by the shard to cache
    /// the sender in `ActiveBinding` at subscribe time.
    pub fn get_write_tx(&self, conn_id: u64) -> Option<mpsc::Sender<Bytes>> {
        let sessions = self.inner.sessions.lock().unwrap();
        sessions.get(&conn_id).map(|s| s.write_tx.clone())
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

    /// Enqueue an already-built `Bytes`. Non-blocking — drops if full.
    #[inline]
    pub fn send_bytes(&self, conn_id: u64, data: Bytes) -> bool {
        self.enqueue(conn_id, data)
    }

    #[inline]
    fn enqueue(&self, conn_id: u64, frame: Bytes) -> bool {
        let sessions = self.inner.sessions.lock().unwrap();
        match sessions.get(&conn_id) {
            Some(s) => s.write_tx.try_send(frame).is_ok(),
            None => false,
        }
    }
}

/// Per-connection writer task — owns the writer (plain TCP or TLS), drains the channel.
async fn conn_writer_task(mut rx: mpsc::Receiver<Bytes>, mut w: BoxedWriter) {
    while let Some(frame) = rx.recv().await {
        if w.write_all(&frame).await.is_err() {
            break;
        }
    }
    let _ = w.shutdown().await;
}

impl LifeCycle for ConnectionRegistry {
    fn on_init(&mut self) {
        tracing::info!("ConnectionRegistry: init (async per-conn writer tasks)");
    }

    fn on_shutdown(&mut self) {
        let mut sessions = self.inner.sessions.lock().unwrap();
        let count = sessions.len();
        sessions.clear(); // drops all Senders → writer tasks exit
        tracing::info!(
            "ConnectionRegistry: shutdown, closed {} sessions",
            count
        );
    }
}
