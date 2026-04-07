//! Context — shared broker state passed to all engine handlers.
//!
//! No global drains Mutex. Drain lives inside StreamSlot, accessed
//! via the stream's shard lock. Single lock per stream (R19).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arbitro_proto::config::ConsumerConfig;
use arbitro_proto::ids::ConnId;

use arbitro_metadata::MetadataLog;
use crate::auth::Auth;
use crate::drain::signal::{DrainSignal, NullSignal};
use crate::metrics::Metrics;
use crate::stream::StreamMap;
use crate::transport::Transport;

/// Factory for creating drain signals. The server provides Gate; tests use NullSignal.
pub type SignalFactory = Box<dyn Fn(u32) -> Arc<dyn DrainSignal> + Send + Sync>;

/// Default factory — NullSignal (no async drain task).
pub fn null_signal_factory() -> SignalFactory {
    Box::new(|_stream_id| Arc::new(NullSignal))
}

/// Shared broker state — passed to all engine handlers.
pub struct Context {
    pub streams: Arc<StreamMap>,
    pub transport: Box<dyn Transport>,
    pub auth: Box<dyn Auth>,
    pub metrics: Metrics,
    /// Factory for creating drain signals per stream.
    pub signal_factory: SignalFactory,
    /// Consumer configs indexed by (stream_id, consumer_id).
    pub consumers: Mutex<HashMap<(u32, u32), ConsumerConfig>>,
    /// Next consumer_id counter.
    pub next_consumer_id: Mutex<u32>,
    /// Track which connections are active.
    pub connections: Mutex<HashMap<ConnId, ConnState>>,
    /// Persistent log for metadata. Interior mutability allows enabling it after replay.
    pub metadata: parking_lot::RwLock<Option<Arc<MetadataLog>>>,
}

/// Per-connection state.
pub struct ConnState {
    pub conn_id: ConnId,
    pub authenticated: bool,
    /// (stream_id, consumer_id) pairs bound to this connection.
    pub subscriptions: Vec<(u32, u32)>,
}

impl Context {
    pub fn new(transport: Box<dyn Transport>, auth: Box<dyn Auth>) -> Self {
        Self {
            streams: Arc::new(StreamMap::new()),
            transport,
            auth,
            metrics: Metrics::new(),
            signal_factory: null_signal_factory(),
            consumers: Mutex::new(HashMap::new()),
            next_consumer_id: Mutex::new(1),
            connections: Mutex::new(HashMap::new()),
            metadata: parking_lot::RwLock::new(None),
        }
    }

    /// Allocate a new consumer_id.
    pub fn alloc_consumer_id(&self) -> u32 {
        let mut id = self.next_consumer_id.lock().unwrap();
        let v = *id;
        *id += 1;
        v
    }
}
