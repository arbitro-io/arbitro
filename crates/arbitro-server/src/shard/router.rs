//! ShardRouter — spawn shard workers, route commands by stream_id.
//!
//! Each shard has two independent services connected only by store + gate:
//! - **Publish**: dispatch layer writes directly to the store, signals gate.
//! - **Drain**: shard worker reads from store, delivers via engine oracle.
//! They do not know each other.

use std::sync::{Arc, Mutex};

use arbitro_engine_v2::types::StreamId;
use arbitro_engine_v2::ArbitroEngine;
use arbitro_store::MemoryStore;
use tokio::sync::mpsc;

use crate::common::{Gate, NameRegistry};
use crate::config::Config;
use crate::shard::handle::ShardHandle;
use crate::shard::worker::ShardWorker;
use crate::transport::ConnectionRegistry;

/// Shared store handle — publish writes, drain reads.
pub type SharedStore = Arc<Mutex<Box<dyn arbitro_store::Store>>>;

/// Routes commands to the correct shard worker by stream_id.
/// Clone-friendly — backed by Arc.
#[derive(Clone)]
pub struct ShardRouter {
    shards: Arc<[ShardHandle]>,
    /// Per-shard shared store — publish writes, drain reads.
    stores: Arc<[SharedStore]>,
    /// Per-shard gate — publish signals, drain consumes.
    gates: Arc<[Arc<Gate>]>,
    /// Server-wide name → small-int registry. Required because the engine
    /// catalog uses `StreamId` and `ConsumerId` as direct `Vec` indices, so
    /// the server cannot derive them by hashing names. See
    /// `common::name_registry` for full rationale.
    names: Arc<NameRegistry>,
}

impl ShardRouter {
    /// Spawn N shard workers on dedicated OS threads.
    pub fn spawn(config: &Config, registry: &ConnectionRegistry) -> Self {
        let shard_count = config.shard_count;
        let channel_capacity = config.channel_capacity;

        let mut handles = Vec::with_capacity(shard_count);
        let mut stores = Vec::with_capacity(shard_count);
        let mut gates = Vec::with_capacity(shard_count);
        let names = Arc::new(NameRegistry::new());

        for id in 0..shard_count {
            let (tx, rx) = mpsc::channel(channel_capacity);
            let engine = ArbitroEngine::new();
            let gate = Arc::new(Gate::new());

            // Single store per shard — stream-agnostic.
            let store: Box<dyn arbitro_store::Store> = match &config.data_dir {
                Some(dir) => {
                    let path = std::path::Path::new(dir)
                        .join("shards")
                        .join(id.to_string());
                    Box::new(arbitro_store::TolerantStore::new(path))
                }
                None => Box::new(MemoryStore::new()),
            };
            let shared_store: SharedStore = Arc::new(Mutex::new(store));

            let worker = ShardWorker::new(
                engine,
                Arc::clone(&shared_store),
                rx,
                Arc::clone(&gate),
                registry.clone(),
                config.data_dir.clone(),
                Arc::clone(&names),
                config.max_feed_per_cycle,
                config.drain_batch_size,
            );

            stores.push(Arc::clone(&shared_store));
            gates.push(Arc::clone(&gate));

            // Named thread — mandatory per concurrency.md
            let join_handle = std::thread::Builder::new()
                .name(format!("shard-{id}"))
                .spawn(move || worker.run())
                .expect("failed to spawn shard thread");

            let shard_thread = join_handle.thread().clone();
            handles.push(ShardHandle::new(
                id as u32,
                tx,
                shard_thread,
                Arc::clone(&shared_store),
                Arc::clone(&gate),
                registry.clone(),
            ));
        }

        Self {
            shards: handles.into(),
            stores: stores.into(),
            gates: gates.into(),
            names,
        }
    }

    /// Shared name → small-int registry.
    #[inline]
    pub fn names(&self) -> &Arc<NameRegistry> {
        &self.names
    }

    /// Shared store for a stream — used by publish (dispatch layer).
    #[inline]
    pub fn store_for(&self, stream_id: StreamId) -> &SharedStore {
        let idx = stream_id.raw() as usize % self.stores.len();
        &self.stores[idx]
    }

    /// Shared gate for a stream — used by publish to notify drain.
    #[inline]
    pub fn gate_for(&self, stream_id: StreamId) -> &Arc<Gate> {
        let idx = stream_id.raw() as usize % self.gates.len();
        &self.gates[idx]
    }

    /// Route to the shard that owns this stream.
    /// Deterministic: stream_id.raw() % shard_count.
    #[inline]
    pub fn shard_for(&self, stream_id: StreamId) -> &ShardHandle {
        let idx = stream_id.raw() as usize % self.shards.len();
        &self.shards[idx]
    }

    /// Get shard by index.
    #[inline]
    pub fn shard(&self, index: usize) -> &ShardHandle {
        &self.shards[index]
    }

    /// Number of shards.
    #[inline]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Graceful shutdown — send Shutdown to all shards.
    pub fn shutdown(&self) {
        for shard in self.shards.iter() {
            shard.send_shutdown();
        }
    }
}
