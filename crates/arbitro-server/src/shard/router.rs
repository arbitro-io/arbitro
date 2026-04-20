//! ShardRouter — spawn shard workers, route commands by stream_id.
//!
//! Each shard has THREE independent services:
//! - **Publish**: dispatch layer writes directly to the store, signals gate.
//! - **Drain**: dedicated OS thread reads from store, delivers via atomics.
//! - **Commands**: tokio task processes ack/nack/subscribe/admin.
//!
//! **Zero Mutex between drain and commands.** Shared state is atomics +
//! ArcSwap snapshots.

use std::sync::Arc;

use arbitro_engine_v2::types::StreamId;
use arbitro_engine_v2::ArbitroEngine;
use arbitro_store::MemoryStore;
use tokio::sync::mpsc;

use crate::common::{Gate, NameRegistry};
use crate::config::Config;
use crate::shard::handle::ShardHandle;
use crate::shard::shared::{DrainSnapshot, SharedCounters, SnapshotSwap};
use crate::shard::worker::{CommandWorker, DrainWorker, FlusherConfig};
use crate::transport::ConnectionRegistry;

/// Shared store handle — publish writes, drain reads.
pub type SharedStore = Arc<std::sync::Mutex<Box<dyn arbitro_store::Store>>>;

/// Routes commands to the correct shard worker by stream_id.
#[derive(Clone)]
pub struct ShardRouter {
    shards: Arc<[ShardHandle]>,
    stores: Arc<[SharedStore]>,
    gates: Arc<[Arc<Gate>]>,
    names: Arc<NameRegistry>,
}

impl ShardRouter {
    /// Spawn N shard workers. Per shard: one drain OS thread + one command tokio task.
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

            let store: Box<dyn arbitro_store::Store> = match &config.data_dir {
                Some(dir) => {
                    let path = std::path::Path::new(dir)
                        .join("shards")
                        .join(id.to_string());
                    Box::new(arbitro_store::TolerantStore::new(path))
                }
                None => Box::new(MemoryStore::new()),
            };
            let shared_store: SharedStore = Arc::new(std::sync::Mutex::new(store));

            // Shared atomics — zero Mutex, zero contention.
            let counters = Arc::new(SharedCounters::new());

            // Snapshot for drain — updated by command thread on structural changes.
            let snapshot = Arc::new(SnapshotSwap::new(DrainSnapshot::empty()));

            let running = Arc::new(std::sync::atomic::AtomicBool::new(true));

            // Notification channel: drain → command (deliveries + dead connections).
            let (notify_tx, notify_rx) = mpsc::channel(8192);

            // ── Drain thread — pure: gate.acquire → drain_cycle ──────
            let drain_worker = DrainWorker {
                counters: Arc::clone(&counters),
                snapshot: Arc::clone(&snapshot),
                store: Arc::clone(&shared_store),
                gate: Arc::clone(&gate),
                names: Arc::clone(&names),
                drain_config: super::drain::DrainConfig {
                    max_feed: config.max_feed_per_cycle,
                    max_age_ms: 0,
                    batch_size: config.drain_batch_size,
                },
                drain_scratch: super::drain::DrainScratch::new(),
                running: Arc::clone(&running),
                notify_tx,
            };

            let drain_handle = std::thread::Builder::new()
                .name(format!("drain-{id}"))
                .spawn(move || drain_worker.run())
                .expect("failed to spawn drain thread");

            let drain_thread = drain_handle.thread().clone();

            // ── Command task — tokio::spawn, owns engine ────────────
            let cmd_worker = CommandWorker {
                engine,
                counters: Arc::clone(&counters),
                snapshot: Arc::clone(&snapshot),
                store: Arc::clone(&shared_store),
                gate: Arc::clone(&gate),
                registry: registry.clone(),
                names: Arc::clone(&names),
                rx,
                notify_rx,
                running: Arc::clone(&running),
                flusher_config: FlusherConfig::default(),
                accum_streams: std::collections::HashMap::with_hasher(
                    foldhash::fast::FixedState::default(),
                ),
                accum_deadline: None,
                accum_total: 0,
                accum_bytes: 0,
                drain_config_batch_size: config.drain_batch_size,
                bindings: Vec::new(),
            };

            tokio::spawn(cmd_worker.run());

            stores.push(Arc::clone(&shared_store));
            gates.push(Arc::clone(&gate));

            handles.push(ShardHandle::new(
                id as u32,
                tx,
                drain_thread,
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

    #[inline]
    pub fn names(&self) -> &Arc<NameRegistry> {
        &self.names
    }

    #[inline]
    pub fn store_for(&self, stream_id: StreamId) -> &SharedStore {
        let idx = stream_id.raw() as usize % self.stores.len();
        &self.stores[idx]
    }

    #[inline]
    pub fn gate_for(&self, stream_id: StreamId) -> &Arc<Gate> {
        let idx = stream_id.raw() as usize % self.gates.len();
        &self.gates[idx]
    }

    #[inline]
    pub fn shard_for(&self, stream_id: StreamId) -> &ShardHandle {
        let idx = stream_id.raw() as usize % self.shards.len();
        &self.shards[idx]
    }

    #[inline]
    pub fn shard(&self, index: usize) -> &ShardHandle {
        &self.shards[index]
    }

    #[inline]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shutdown(&self) {
        for shard in self.shards.iter() {
            shard.send_shutdown();
        }
    }
}
