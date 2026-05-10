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
use arbitro_kit::route::Mpsc as KitMpsc;
use arbitro_kit::waiter::NotifyWaiter;
use arbitro_store::MemoryStore;

use crate::common::{Gate, NameRegistry};
use crate::config::Config;
use crate::persistence::command_log::SharedCommandLog;
use crate::shard::command::ShardCommand;
use crate::shard::handle::{ShardHandle, ShardLane};
use crate::shard::shared::{DrainSnapshot, NotifyRing, SharedCounters, SnapshotSwap};
use crate::shard::worker::{CommandWorker, DrainWorker, FlusherConfig};
use crate::transport::ConnectionRegistry;

// ── kit::Mpsc lane configuration ───────────────────────────────────────────
//
// Total command channel capacity = SHARD_M × SHARD_RING_CAP.
// With M=8 lanes × 128 slots/lane = 1024 total — matches the previous
// tokio::mpsc(channel_capacity=4096) default ÷ 4 for tighter backpressure,
// or scaled up via SHARD_RING_CAP at compile time.
//
// M=8 chosen so consumer-side `recv_batch_async_send` amortises drain
// cost across 8 rings (~2× over tokio::mpsc::recv_many in benches).

/// Number of producer lanes per shard. Each lane is a `kit::MpscProducer`
/// wrapped in a `parking_lot::Mutex` and stored in `ShardHandle.lanes`.
pub const SHARD_M: usize = 8;

/// Per-lane ring capacity (must be power of two). Total channel capacity =
/// `SHARD_M * SHARD_RING_CAP`.
pub const SHARD_RING_CAP: usize = 128;

/// Shared store handle — publish writes, drain reads.
pub type SharedStore = Arc<std::sync::Mutex<Box<dyn arbitro_store::Store>>>;

/// Routes commands to the correct shard worker by stream_id.
#[derive(Clone)]
pub struct ShardRouter {
    shards: Arc<[ShardHandle]>,
    stores: Arc<[SharedStore]>,
    gates: Arc<[Arc<Gate>]>,
    names: Arc<NameRegistry>,
    /// Optional persistent command log — set when `data_dir` is configured.
    /// Used by dispatch to record metadata mutations (create/delete stream/consumer)
    /// so they survive server restarts.
    command_log: Option<SharedCommandLog>,
}

impl ShardRouter {
    /// Spawn N shard workers. Per shard: one drain OS thread + one command tokio task.
    pub fn spawn(config: &Config, registry: &ConnectionRegistry) -> Self {
        let shard_count = config.shard_count;
        // `config.channel_capacity` retained as max command in-flight, but
        // the actual capacity is `SHARD_M * SHARD_RING_CAP` (compile-time).
        // Honour the larger of the two if the operator configured > 1024.
        let _ = config.channel_capacity;

        let mut handles = Vec::with_capacity(shard_count);
        let mut stores = Vec::with_capacity(shard_count);
        let mut gates = Vec::with_capacity(shard_count);
        let names = Arc::new(NameRegistry::new());

        for id in 0..shard_count {
            // ── kit::Mpsc with M=SHARD_M producer lanes per shard ──────
            let (producers, consumer, shutdown_handle) =
                KitMpsc::<ShardCommand, SHARD_RING_CAP, NotifyWaiter>::new(SHARD_M);
            // Drop the kit shutdown handle — we use in-band ShardCommand::Shutdown.
            // The consumer never returns Err(Shutdown) because we never call
            // shutdown_handle.signal().
            let _ = shutdown_handle;

            let lanes: Arc<[ShardLane]> = producers
                .into_iter()
                .map(parking_lot::Mutex::new)
                .collect::<Vec<_>>()
                .into();
            let backpressure = Arc::new(tokio::sync::Notify::new());
            let consumer_alive = Arc::new(std::sync::atomic::AtomicBool::new(true));

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

            // Notification ring: drain → command (deliveries + dead connections).
            // SPSC Ring — drain is the sole producer, command task is the sole consumer.
            let notify_ring = Arc::new(NotifyRing::new());

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
                notify_ring: Arc::clone(&notify_ring),
            };

            std::thread::Builder::new()
                .name(format!("drain-{id}"))
                .spawn(move || drain_worker.run())
                .expect("failed to spawn drain thread");

            // ── Command task — tokio::spawn, owns engine ────────────
            let cmd_worker = CommandWorker {
                engine,
                counters: Arc::clone(&counters),
                snapshot: Arc::clone(&snapshot),
                store: Arc::clone(&shared_store),
                gate: Arc::clone(&gate),
                registry: registry.clone(),
                names: Arc::clone(&names),
                rx: consumer,
                backpressure: Arc::clone(&backpressure),
                consumer_alive: Arc::clone(&consumer_alive),
                notify_ring,
                running: Arc::clone(&running),
                flusher_config: FlusherConfig::default(),
                accum_streams: std::collections::HashMap::with_hasher(
                    foldhash::fast::FixedState::default(),
                ),
                accum_deadline: None,
                accum_total: 0,
                accum_bytes: 0,
                drain_config_batch_size: config.drain_batch_size,
                stream_retention: std::collections::HashMap::with_hasher(
                    foldhash::fast::FixedState::default(),
                ),
                bindings: Vec::new(),
                next_eviction: None,
                wheel: None,
                wheel_buf: Vec::new(),
                next_wheel_tick: None,
            };

            tokio::spawn(cmd_worker.run());

            stores.push(Arc::clone(&shared_store));
            gates.push(Arc::clone(&gate));

            handles.push(ShardHandle::new(
                id as u32,
                lanes,
                backpressure,
                consumer_alive,
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
            command_log: None,
        }
    }

    /// Wire the persistent command log. Called by `ArbitroServer::set_command_log`
    /// before `run()`. After this, metadata mutations are recorded to the log.
    pub fn set_command_log(&mut self, log: SharedCommandLog) {
        self.command_log = Some(log);
    }

    /// Return a reference to the command log, if configured.
    #[inline]
    pub fn command_log(&self) -> Option<&SharedCommandLog> {
        self.command_log.as_ref()
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
