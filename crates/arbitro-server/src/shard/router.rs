//! ShardRouter — spawn shard workers, route commands by stream_id.
//!
//! Each shard has THREE independent services:
//! - **Publish**: dispatch layer writes directly to the store, signals gate.
//! - **Drain**: dedicated OS thread reads from store, delivers via atomics.
//! - **Commands**: tokio task processes ack/nack/subscribe/admin.
//!
//! **Zero Mutex between drain and commands.** Shared state is atomics +
//! ArcSwap snapshots.

use std::collections::HashMap;
use std::sync::Arc;

use arbitro_engine_v2::types::StreamId;
use arbitro_engine_v2::ArbitroEngine;
use arbitro_store::MemoryStore;
use tokio::sync::mpsc;

use crate::common::{Gate, NameRegistry};
use arbitro_common::SharedClock;
use crate::config::Config;
use crate::persistence::command_log::SharedCommandLog;
use crate::shard::drain_events::DrainEventRing;
use crate::shard::handle::ShardHandle;
use crate::shard::shared::{DrainSnapshot, NotifyRing, SharedCounters, SnapshotSwap};
use crate::shard::worker::{CommandWorker, DrainWorker, FlusherConfig};
use crate::transport::ConnectionRegistry;

/// Shared store handle — publish writes, drain reads.
/// Shared store handle — publish writes, drain reads.
/// **F2**: `parking_lot::Mutex` for faster uncontested path; no
/// `block_in_place` wrapper needed (append is a sub-µs mmap memcpy).
pub type SharedStore = Arc<parking_lot::Mutex<Box<dyn arbitro_store::Store>>>;

/// Routes commands to the correct shard worker by stream_id.
#[derive(Clone)]
pub struct ShardRouter {
    shards: Arc<[ShardHandle]>,
    stores: Arc<[SharedStore]>,
    gates: Arc<[Arc<Gate>]>,
    names: Arc<NameRegistry>,
    /// H5: drain task join handles, one per shard. After the migration
    /// from `std::thread` to `tokio::spawn`, these are `tokio::task::JoinHandle`
    /// — `shutdown()` awaits them so all per-shard state is released
    /// (mmaps closed, drain ring empty) before the function returns.
    /// Tokio's `JoinHandle` is `Send` so the surrounding
    /// `parking_lot::Mutex<Option<...>>` is only there to let `shutdown`
    /// take ownership of the handles from a `&self` method.
    drain_joins: Arc<parking_lot::Mutex<Vec<Option<tokio::task::JoinHandle<()>>>>>,
    /// Per-shard "running" flags, used by `shutdown` to flip drain
    /// tasks off so they exit their inner loop cleanly.
    drain_running: Arc<[Arc<std::sync::atomic::AtomicBool>]>,
    /// Per-shard idempotency dedup state. Each entry is a
    /// lazily-allocated tracker (`Option<...>` starts None, fills in
    /// on first idempotent publish for that shard). Shared between
    /// the dispatch publish path (membership check + record) and the
    /// shard worker's tick loop (expiration sweep).
    idempotency: Arc<[crate::shard::idempotency::SharedIdempotency]>,
    /// Per-shard "tracker allocated" flag (F10) — flipped to `true` the
    /// first time the publish hot path lazily allocates the idempotency
    /// tracker for that shard. The command worker reads this with a
    /// single relaxed atomic load in its `tokio::select!` predicate
    /// instead of locking the shared `Arc<Mutex<Option<...>>>`.
    has_idempotency: Arc<[Arc<std::sync::atomic::AtomicBool>]>,
    /// Optional persistent command log — set when `data_dir` is configured.
    /// Used by dispatch to record metadata mutations (create/delete stream/consumer)
    /// so they survive server restarts.
    command_log: Option<SharedCommandLog>,
    /// Shared monotonic millisecond clock (F7). Replaces per-publish
    /// `SystemTime::now()` syscalls with a single relaxed atomic load.
    clock: SharedClock,
    /// H10: shared silent-drop counters across all shards. Bumped at
    /// every `try_send` failure on the conn-write / notify-ring /
    /// drain-event paths and surfaced in the periodic metrics log.
    silent_drops: Arc<crate::common::SilentDrops>,
    /// F37: 1-second TTL cache for `list_streams` / `list_consumers`
    /// fan-out replies. Cold-path RPCs that fan out to every shard;
    /// caching avoids paying the 16-shard round-trip on every call from
    /// dashboards / health checks. Lock contention is irrelevant —
    /// these RPCs are not on any hot path.
    list_cache: Arc<parking_lot::Mutex<ListCache>>,
}

/// F37 — list_streams / list_consumers TTL cache. 1-second freshness
/// window. Built lazily on read; the cache is invalidated by the next
/// expiry, no explicit eviction on writes (create/delete) needed.
#[derive(Default)]
struct ListCache {
    streams: Option<(std::time::Instant, Arc<Vec<(u32, Vec<u8>)>>)>,
    consumers: Option<(std::time::Instant, Arc<Vec<(u32, u32, u32, bool)>>)>,
}

impl ShardRouter {
    /// F37: cache duration. Picked so that operator dashboards (1Hz
    /// poll) see ~1 cache miss per second per shard cluster, but
    /// scripts hammering `list_streams` don't fan out N×.
    const LIST_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(1);

    /// Return a cached `list_streams` aggregate if fresh.
    /// The caller falls back to the fan-out path on `None`.
    #[inline]
    pub fn cached_list_streams(&self) -> Option<Arc<Vec<(u32, Vec<u8>)>>> {
        let guard = self.list_cache.lock();
        if let Some((ts, v)) = guard.streams.as_ref() {
            if ts.elapsed() < Self::LIST_CACHE_TTL {
                return Some(Arc::clone(v));
            }
        }
        None
    }

    /// Store a freshly-built aggregate. Replaces any existing entry.
    #[inline]
    pub fn store_list_streams(&self, v: Vec<(u32, Vec<u8>)>) -> Arc<Vec<(u32, Vec<u8>)>> {
        let v = Arc::new(v);
        self.list_cache.lock().streams =
            Some((std::time::Instant::now(), Arc::clone(&v)));
        v
    }

    /// Cached `list_consumers` aggregate if fresh.
    #[inline]
    pub fn cached_list_consumers(&self) -> Option<Arc<Vec<(u32, u32, u32, bool)>>> {
        let guard = self.list_cache.lock();
        if let Some((ts, v)) = guard.consumers.as_ref() {
            if ts.elapsed() < Self::LIST_CACHE_TTL {
                return Some(Arc::clone(v));
            }
        }
        None
    }

    #[inline]
    pub fn store_list_consumers(
        &self,
        v: Vec<(u32, u32, u32, bool)>,
    ) -> Arc<Vec<(u32, u32, u32, bool)>> {
        let v = Arc::new(v);
        self.list_cache.lock().consumers =
            Some((std::time::Instant::now(), Arc::clone(&v)));
        v
    }

    /// Invalidate both list caches. Called from create/delete paths so
    /// the next `list_streams` / `list_consumers` reflects the write.
    #[inline]
    pub fn invalidate_list_cache(&self) {
        let mut g = self.list_cache.lock();
        g.streams = None;
        g.consumers = None;
    }
}

impl ShardRouter {
    /// Spawn N shard workers. Per shard: one drain OS thread + one command tokio task.
    pub fn spawn(config: &Config, registry: &ConnectionRegistry) -> Self {
        let shard_count = config.shard_count;
        let channel_capacity = config.channel_capacity;

        let mut handles = Vec::with_capacity(shard_count);
        let mut stores = Vec::with_capacity(shard_count);
        let mut gates = Vec::with_capacity(shard_count);
        let mut idempotency = Vec::with_capacity(shard_count);
        let mut has_idempotency = Vec::with_capacity(shard_count);
        let mut drain_joins = Vec::with_capacity(shard_count);
        let mut drain_running = Vec::with_capacity(shard_count);
        let names = Arc::new(NameRegistry::new());
        // H10: one SilentDrops shared by every shard + the registry.
        let silent_drops = Arc::new(crate::common::SilentDrops::new());

        for id in 0..shard_count {
            let (tx, rx) = mpsc::channel(channel_capacity);
            let engine = ArbitroEngine::new();
            let shard_metrics = engine.metrics_arc();
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
            let shared_store: SharedStore = Arc::new(parking_lot::Mutex::new(store));

            // Shared atomics — zero Mutex, zero contention.
            let counters = Arc::new(SharedCounters::new());

            // Snapshot for drain — updated by command thread on structural changes.
            let snapshot = Arc::new(SnapshotSwap::new(DrainSnapshot::empty()));

            let running = Arc::new(std::sync::atomic::AtomicBool::new(true));

            // Per-shard idempotency tracker handle. None inside the
            // Arc<Mutex<>> means not allocated yet — the publish hot
            // path allocates on first idempotent stream. Both the
            // command worker (tick loop) and dispatch_v2 (publish
            // check + record) hold clones of this Arc.
            let shard_idempotency = super::idempotency::new_shared_idempotency();
            let shard_has_idempotency = Arc::new(std::sync::atomic::AtomicBool::new(false));

            // Notification ring: drain → command (deliveries + dead connections).
            // SPSC Ring — drain is the sole producer, command task is the sole consumer.
            let notify_ring = Arc::new(NotifyRing::new());

            // Drain-event ring: command → drain (ack-driven subject-inflight
            // decrements + consumer-removed cleanup). SPSC.
            let drain_evt_ring = Arc::new(DrainEventRing::new());

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
                drain_evt_rx: Arc::clone(&drain_evt_ring),
                consumer_subjects: Vec::new(),
                silent_drops: Arc::clone(&silent_drops),
            };

            // H5: keep the JoinHandle. shutdown() will flip `running`
            // to false, release the gate, and await. After migrating to
            // an async drain, this is a tokio task — no OS-thread join,
            // no `spawn_blocking`, no impedance mismatch with the runtime.
            let join = tokio::spawn(drain_worker.run());
            drain_joins.push(Some(join));
            drain_running.push(Arc::clone(&running));

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
                notify_ring,
                drain_evt_tx: drain_evt_ring,
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
                idempotency_tracker: Arc::clone(&shard_idempotency),
                has_idempotency: Arc::clone(&shard_has_idempotency),
                flush_stream_ids: Vec::new(),
                silent_drops: Arc::clone(&silent_drops),
                pending_consumer_remove: Vec::new(),
                last_wheel_tick: None,
                evict_resume_seq: 0,
                stream_oldest_ts: HashMap::default(),
            };

            // M15: supervise the command-worker task — if it panics
            // we want a loud log line in operators' eyes instead of a
            // silently-dead shard. The `JoinHandle` is awaited in a
            // watcher task that logs and exits when the child resolves.
            let shard_id_for_log = id;
            let cmd_handle = tokio::spawn(cmd_worker.run());
            tokio::spawn(async move {
                match cmd_handle.await {
                    Ok(()) => {
                        tracing::debug!(target = "supervisor", shard = shard_id_for_log, "command worker exited cleanly");
                    }
                    Err(e) if e.is_panic() => {
                        tracing::error!(target = "supervisor", shard = shard_id_for_log, "command worker panicked: {e}");
                    }
                    Err(e) => {
                        tracing::warn!(target = "supervisor", shard = shard_id_for_log, "command worker join error: {e}");
                    }
                }
            });

            stores.push(Arc::clone(&shared_store));
            gates.push(Arc::clone(&gate));
            idempotency.push(Arc::clone(&shard_idempotency));
            has_idempotency.push(Arc::clone(&shard_has_idempotency));

            handles.push(ShardHandle::new(
                id as u32,
                tx,
                Arc::clone(&shared_store),
                Arc::clone(&gate),
                registry.clone(),
                shard_metrics,
            ));
        }

        // Spawn the SharedClock updater task — refreshes the cached
        // millisecond timestamp at ~1000 Hz. Hot paths read with one
        // relaxed atomic load (~1 ns) instead of paying the
        // `SystemTime::now()` syscall (~25–50 ns on Linux, more on
        // Windows). See `arbitro_common::clock`.
        let clock = SharedClock::new();
        {
            let clk = clock.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(
                    std::time::Duration::from_millis(1),
                );
                loop {
                    interval.tick().await;
                    clk.refresh();
                }
            });
        }

        Self {
            shards: handles.into(),
            stores: stores.into(),
            gates: gates.into(),
            names,
            drain_joins: Arc::new(parking_lot::Mutex::new(drain_joins)),
            drain_running: drain_running.into(),
            idempotency: idempotency.into(),
            has_idempotency: has_idempotency.into(),
            command_log: None,
            clock,
            silent_drops,
            // F37: empty cache; first list_streams / list_consumers
            // populates it.
            list_cache: Arc::new(parking_lot::Mutex::new(ListCache::default())),
        }
    }

    /// H10: handle to the shared silent-drop counters. The metrics loop
    /// snapshots this every interval; tests can read it for assertions.
    #[inline]
    pub fn silent_drops(&self) -> Arc<crate::common::SilentDrops> {
        Arc::clone(&self.silent_drops)
    }

    /// Cached "now" in milliseconds since the UNIX epoch. Hot path —
    /// one relaxed atomic load. See `arbitro_common::SharedClock`.
    #[inline(always)]
    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    /// Clone the shared clock — used by callers that want their own handle.
    #[inline]
    pub fn clock(&self) -> SharedClock {
        self.clock.clone()
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

    /// Per-shard idempotency tracker handle. The publish hot path
    /// (`dispatch_v2::v2_publish`) uses this to check + record
    /// dedup state when the stream has `idempotency_window_ms > 0`.
    /// `Option<...>` inside the Mutex is `None` until the first
    /// idempotent publish allocates it lazily.
    #[inline]
    pub fn idempotency_for(
        &self,
        stream_id: StreamId,
    ) -> &super::idempotency::SharedIdempotency {
        let idx = stream_id.raw() as usize % self.idempotency.len();
        &self.idempotency[idx]
    }

    /// Per-shard "tracker allocated" flag — flip to `true` after the
    /// publish hot path lazily allocates the idempotency tracker so
    /// the command worker's `select!` predicate can stop locking the
    /// Arc just to call `Option::is_some()` (F10).
    #[inline]
    pub fn mark_idempotency_allocated(&self, stream_id: StreamId) {
        let idx = stream_id.raw() as usize % self.has_idempotency.len();
        self.has_idempotency[idx]
            .store(true, std::sync::atomic::Ordering::Relaxed);
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

    pub async fn shutdown(&self) {
        for shard in self.shards.iter() {
            shard.send_shutdown();
        }
        // H5: flip every drain's `running` flag, release the gate so
        // the thread unparks past `gate.acquire()`, and join. The
        // command worker also sets running=false on its Shutdown arm
        // (worker.rs::handle_or_shutdown), but it does so AFTER the
        // shard channel drains. Flipping it here too is idempotent and
        // guarantees a clean join even if the command task aborted.
        for r in self.drain_running.iter() {
            r.store(false, std::sync::atomic::Ordering::Relaxed);
        }
        for g in self.gates.iter() {
            g.release();
        }
        // Drain ownership out of the mutex first, then drop the lock
        // before awaiting. Each drain is a tokio task — `await` is
        // cooperative and does not block the worker.
        let handles: Vec<tokio::task::JoinHandle<()>> = {
            let mut joins = self.drain_joins.lock();
            joins.iter_mut().filter_map(|s| s.take()).collect()
        };
        for h in handles {
            if let Err(e) = h.await {
                tracing::warn!(?e, "drain task join failed");
            }
        }
    }
}
