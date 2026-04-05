//! Context — shared broker state passed to all engine handlers.

use std::collections::HashMap;
use std::sync::Mutex;

use arbitro_proto::config::ConsumerConfig;
use arbitro_proto::ids::ConnId;

use crate::auth::Auth;
use crate::drain::ReactiveDrain;
use crate::metrics::Metrics;
use crate::stream::StreamMap;
use crate::transport::Transport;

/// Shared broker state — passed to all engine handlers.
pub struct Context {
    pub streams: StreamMap,
    /// drain_id keyed by stream_id
    pub drains: Mutex<HashMap<u32, ReactiveDrain>>,
    pub transport: Box<dyn Transport>,
    pub auth: Box<dyn Auth>,
    pub metrics: Metrics,
    /// Consumer configs indexed by (stream_id, consumer_id).
    pub consumers: Mutex<HashMap<(u32, u32), ConsumerConfig>>,
    /// Next consumer_id counter.
    pub next_consumer_id: Mutex<u32>,
    /// Track which connections are active.
    pub connections: Mutex<HashMap<ConnId, ConnState>>,
}

/// Per-connection state.
pub struct ConnState {
    pub conn_id: ConnId,
    pub authenticated: bool,
}

impl Context {
    pub fn new(transport: Box<dyn Transport>, auth: Box<dyn Auth>) -> Self {
        Self {
            streams: StreamMap::new(),
            drains: Mutex::new(HashMap::new()),
            transport,
            auth,
            metrics: Metrics::new(),
            consumers: Mutex::new(HashMap::new()),
            next_consumer_id: Mutex::new(1),
            connections: Mutex::new(HashMap::new()),
        }
    }

    /// Allocate a new consumer_id.
    pub fn alloc_consumer_id(&self) -> u32 {
        let mut id = self.next_consumer_id.lock().unwrap();
        let v = *id;
        *id += 1;
        v
    }

    /// Get or create a drain for a stream.
    pub fn get_or_create_drain(&self, stream_id: u32) -> bool {
        let mut drains = self.drains.lock().unwrap();
        if drains.contains_key(&stream_id) {
            return false;
        }
        drains.insert(stream_id, ReactiveDrain::new(stream_id));
        true
    }
}
