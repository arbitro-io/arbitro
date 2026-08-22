//! ShardRouter — spawn shard workers, route commands by stream_id.
//!
//! Each shard has THREE independent services:
//! - **Publish**: dispatch layer writes directly to the store, signals gate.
//! - **Drain**: tokio task reads from store, delivers via atomics.
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
use crate::config::Config;
use crate::persistence::command_log::SharedCommandLog;
use crate::shard::drain_events::DrainEventRing;
use crate::shard::handle::ShardHandle;
use crate::shard::shared::{DrainSnapshot, NotifyRing, SharedCounters, SnapshotSwap};
use crate::shard::drain_probe::{ProbeOff, ProbeOn};
use crate::shard::worker::{CommandWorker, DrainWorker};
use crate::transport::ConnectionRegistry;
use arbitro_common::SharedClock;


/// What happened to an append, and — the part callers must respect — who
/// owes the client an answer.
///
/// Getting this wrong sends two replies for one request, which desynchronises
/// the client's sequence bookkeeping far more confusingly than sending none.
pub enum Append {
    /// Appended on this thread. The CALLER still owes the client its RepOk.
    Stored(u64),
    /// Handed to the owning shard, which will answer the client itself.
    /// The caller must send nothing.
    Delegated,
    /// Refused — quota, or the shard is gone. The caller owes the error.
    Refused,
}

/// Routes commands to the correct shard worker by stream_id.
#[derive(Clone)]
pub struct ShardRouter {
    shards: Arc<[ShardHandle]>,
    /// How many shards exist. Was `stores.len()`; the stores themselves
    /// now live on their own threads and the router never holds one.
    shard_count: usize,
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
    /// Per-shard listening port, published once the sockets are bound and
    /// read back by `Action::ShardTopology`. Empty until then, and empty
    /// forever when `shard_listeners` is off — the answer in that case is
    /// "no port of my own", not a guess.
    shard_ports: Arc<std::sync::OnceLock<Vec<u16>>>,
    /// Per-shard runtimes, empty unless `shard_runtimes` is on. Held here
    /// ONLY to keep them alive: dropping a `ShardRuntime` stops its thread,
    /// so leaving these in `spawn` would kill every shard the moment it
    /// returned. `Arc` because `ShardRouter` is cloned per connection.
    _shard_runtimes: Arc<[super::runtime::ShardRuntime]>,
    /// Optional replication sender — set when clustered. Shard workers
    /// send `ReplicationBatch`es through this channel; the replication
    /// loop forwards them to followers via the cluster transport.
    #[cfg(feature = "cluster")]
    replication_tx: Arc<
        parking_lot::Mutex<Option<mpsc::Sender<crate::cluster::replication::ReplicationBatch>>>,
    >,
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
        self.list_cache.lock().streams = Some((std::time::Instant::now(), Arc::clone(&v)));
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
        self.list_cache.lock().consumers = Some((std::time::Instant::now(), Arc::clone(&v)));
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
    /// Spawn N shard workers. Per shard: one drain tokio task + one command tokio task.
    pub fn spawn(config: &Config, registry: &ConnectionRegistry) -> Self {
        let shard_count = config.shard_count;
        let channel_capacity = config.channel_capacity;

        let mut handles = Vec::with_capacity(shard_count);
        let mut gates = Vec::with_capacity(shard_count);
        let mut idempotency = Vec::with_capacity(shard_count);
        let mut has_idempotency = Vec::with_capacity(shard_count);
        let mut drain_joins = Vec::with_capacity(shard_count);
        let mut drain_running = Vec::with_capacity(shard_count);
        // One runtime per shard, always — the journal is MOVED onto that
        // thread and lives there. There is no second model to fall back to:
        // a shard whose journal sat in a shared `Arc<Mutex<_>>` would put
        // the lock back, and a broker running half of each would carry the
        // cost of both and the benefit of neither.
        let mut shard_runtimes: Vec<super::runtime::ShardRuntime> =
            Vec::with_capacity(shard_count);
        let names = Arc::new(NameRegistry::new());
        // H10: one SilentDrops shared by every shard + the registry.
        let silent_drops = Arc::new(crate::common::SilentDrops::new());
        // Cluster replication sender — shared between the router and all
        // shard workers. Starts as None; `set_replication_tx` fills it
        // during cluster boot. Workers check it lazily on flush.
        #[cfg(feature = "cluster")]
        let replication_tx: Arc<
            parking_lot::Mutex<Option<mpsc::Sender<crate::cluster::replication::ReplicationBatch>>>,
        > = Arc::new(parking_lot::Mutex::new(None));

        for id in 0..shard_count {
            let (tx, rx) = mpsc::channel(channel_capacity);
            let engine = ArbitroEngine::new();
            let shard_metrics = engine.metrics_arc();
            let gate = Arc::new(Gate::new());

            let shard_data_path: Option<std::path::PathBuf> = config.data_dir.as_ref().map(|dir| {
                std::path::Path::new(dir)
                    .join("shards")
                    .join(id.to_string())
            });
            let store: Box<dyn arbitro_store::Store> = match &shard_data_path {
                Some(path) => {
                    // SEC-8: propagate the operator-configured fsync
                    // policy into the message journal. Previously the
                    // TolerantStore defaulted to `FsyncPolicy::None`
                    // and `set_fsync_policy` was never called, so
                    // ARBITRO_FSYNC_POLICY=every only affected the
                    // command log + delayed journal — the actual
                    // message data was silently at OS-buffer mercy.
                    let store_fsync = match config.fsync_policy {
                        crate::config::FsyncPolicy::Every => arbitro_store::FsyncPolicy::EveryWrite,
                        crate::config::FsyncPolicy::None => arbitro_store::FsyncPolicy::None,
                    };
                    let mut tolerant = arbitro_store::TolerantStore::new(path.clone());
                    tolerant.set_fsync_policy(store_fsync);
                    Box::new(tolerant)
                }
                None => Box::new(MemoryStore::new()),
            };
            // The journal moves onto the shard's own thread here and never
            // comes back. Everything that touches it from now on is a task
            // on that thread, which is why none of them takes a lock.
            let runtime = super::runtime::ShardRuntime::start(id, store)
                .unwrap_or_else(|e| panic!("shard {id} runtime failed to start: {e}"));
            let rt = runtime.handle().clone();
            shard_runtimes.push(runtime);

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
            // SPSC — drain owns the single producer, command task owns the consumer.
            // Modelled as Mpsc(m=1) so the consumer's async recv future is
            // explicitly `Send` (see rust-lang/rust#100013).
            let (mut notify_producers, notify_rx, _notify_shutdown) = NotifyRing::new(1);
            let notify_tx = notify_producers
                .pop()
                .expect("NotifyRing::new(1) yields exactly one producer");

            // Drain-event ring: command → drain (ack-driven subject-inflight
            // decrements + consumer-removed cleanup). SPSC — command owns
            // the producer, drain owns the consumer.
            let (drain_evt_tx, drain_evt_rx) = DrainEventRing::new();

            // ── Drain task — pure: gate.acquire → fill/dispatch/deliver ──
            let drain_worker = DrainWorker {
                shard_id: id as u32,
                counters: Arc::clone(&counters),
                snapshot: Arc::clone(&snapshot),
                gate: Arc::clone(&gate),
                names: Arc::clone(&names),
                drain_config: super::drain::DrainConfig {
                    max_feed: config.max_feed_per_cycle,
                    max_age_ms: 0,
                    batch_size: config.drain_batch_size,
                    stall_evict_ms: config.drain_stall_evict_ms,
                },
                drain_scratch: super::drain::DrainScratch::new(),
                staged: super::drain::Staged::default(),
                running: Arc::clone(&running),
                notify_ring: notify_tx,
                drain_evt_rx,
                consumer_subjects: Vec::new(),
                silent_drops: Arc::clone(&silent_drops),
            };

            // H5: keep the JoinHandle. shutdown() will flip `running`
            // to false, release the gate, and await. After migrating to
            // an async drain, this is a tokio task — no OS-thread join,
            // no `spawn_blocking`, no impedance mismatch with the runtime.
            //
            // Probe monomorph selected ONCE here: recording drains carry
            // `ProbeOn` (also serves the legacy ARBITRO_CHAOS_DEBUG drain
            // prints); default drains compile every probe call to nothing.
            // ARBITRO_DRAIN_PROBE keeps the cycle ring only — the quiet
            // instrument for chasing a race. ARBITRO_CHAOS_DEBUG additionally
            // restores the legacy per-frame prints, which cost a write(2)
            // each and shift the timing they are meant to observe.
            let verbose = std::env::var("ARBITRO_CHAOS_DEBUG").is_ok();
            let probe_on = verbose || std::env::var("ARBITRO_DRAIN_PROBE").is_ok();
            let join = if probe_on {
                rt.spawn(drain_worker.run(ProbeOn::new(verbose)))
            } else {
                rt.spawn(drain_worker.run(ProbeOff))
            };
            drain_joins.push(Some(join));
            drain_running.push(Arc::clone(&running));

            // ── Command task — tokio::spawn, owns engine ────────────
            let cmd_worker = CommandWorker {
                engine,
                shard_id: id,
                counters: Arc::clone(&counters),
                snapshot: Arc::clone(&snapshot),
                gate: Arc::clone(&gate),
                registry: registry.clone(),
                names: Arc::clone(&names),
                rx,
                notify_ring: notify_rx,
                drain_evt_tx,
                running: Arc::clone(&running),
                drain_config_batch_size: config.drain_batch_size,
                stream_retention: std::collections::HashMap::with_hasher(
                    foldhash::fast::FixedState::default(),
                ),
                bindings: Vec::new(),
                next_eviction: None,
                wheel: None,
                wheel_buf: Vec::new(),
                next_timer_ms: None,
                epoch: std::time::Instant::now(),
                last_idempotency_ms: 0,
                idempotency_tracker: Arc::clone(&shard_idempotency),
                has_idempotency: Arc::clone(&shard_has_idempotency),
                silent_drops: Arc::clone(&silent_drops),
                pending_consumer_remove: Vec::new(),
                pending_drain_acks: Vec::new(),
                ack_floors: crate::shard::ack_floor::AckFloors::new(),
                evict_resume_seq: 0,
                stream_oldest_ts: HashMap::default(),
                dlq_nack_counts: std::collections::HashMap::with_hasher(
                    foldhash::fast::FixedState::default(),
                ),
                data_path: shard_data_path,
                replay_mode: true,
                #[cfg(feature = "cluster")]
                replication_tx: Arc::clone(&replication_tx),
            };

            // M15: supervise the command-worker task — if it panics
            // we want a loud log line in operators' eyes instead of a
            // silently-dead shard. The `JoinHandle` is awaited in a
            // watcher task that logs and exits when the child resolves.
            let shard_id_for_log = id;
            // Same runtime as this shard's drain — that pairing IS the
            // change. Two tasks on one thread cannot contend for the store.
            let cmd_handle = rt.spawn(cmd_worker.run());
            tokio::spawn(async move {
                match cmd_handle.await {
                    Ok(()) => {
                        tracing::debug!(
                            target = "supervisor",
                            shard = shard_id_for_log,
                            "command worker exited cleanly"
                        );
                    }
                    Err(e) if e.is_panic() => {
                        tracing::error!(
                            target = "supervisor",
                            shard = shard_id_for_log,
                            "command worker panicked: {e}"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            target = "supervisor",
                            shard = shard_id_for_log,
                            "command worker join error: {e}"
                        );
                    }
                }
            });

            gates.push(Arc::clone(&gate));
            idempotency.push(Arc::clone(&shard_idempotency));
            has_idempotency.push(Arc::clone(&shard_has_idempotency));

            handles.push(ShardHandle::new(
                id as u32,
                tx,
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
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(1));
                loop {
                    interval.tick().await;
                    clk.refresh();
                }
            });
        }

        Self {
            shards: handles.into(),
            shard_count,
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
            shard_ports: Arc::new(std::sync::OnceLock::new()),
            _shard_runtimes: shard_runtimes.into(),
            #[cfg(feature = "cluster")]
            replication_tx,
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

    /// One stream's storage, with the lock, the gate and the shard index
    /// hidden. This is what callers should use; `store_for` and `gate_for`
    /// remain for the paths that genuinely need the raw handles.
    ///
    /// Free to build — it borrows the `Arc`s already held here.
    ///
    /// Resolves the shard ONCE. Going through `store_for` + `gate_for`
    /// looked equivalent but asked the registry twice per publish frame,
    /// and the store and the gate must belong to the same shard anyway —
    /// two independent lookups is the shape that lets them disagree.

    /// The runtime owning `shard`, when shards have private runtimes.
    ///
    /// This is how a connection gets pinned to the thread that owns the
    /// store it will publish to. Until a connection runs here, its publish
    /// arrives from an arbitrary pool thread and the store's lock is what
    /// keeps that sound — so this is the prerequisite for the lock ever
    /// going away, not a scheduling nicety.
    pub(crate) fn runtime_for_shard(&self, shard: u16) -> Option<tokio::runtime::Handle> {
        self._shard_runtimes
            .get(shard as usize)
            .map(|r| r.handle().clone())
    }

    /// Publish the per-shard listening ports. Called once, after the
    /// sockets are bound; later calls are ignored rather than racing.
    pub fn set_shard_ports(&self, ports: Vec<u16>) {
        let _ = self.shard_ports.set(ports);
    }

    /// Port per shard, indexed by shard number. Empty when the server runs
    /// on a single listener.
    pub fn shard_ports(&self) -> &[u16] {
        self.shard_ports.get().map(Vec::as_slice).unwrap_or(&[])
    }

    /// THE routing decision, in one place.
    ///
    /// A recorded placement wins; the modulo is the fallback for streams
    /// created before placement was recorded. Every `*_for` below goes
    /// through here — that is the point. When these each computed their own
    /// `stream_id % len`, changing one and not the others meant publish
    /// wrote to one shard's store while the gate woke another's drain: the
    /// messages are stored and never delivered, with no error anywhere.
    /// The rule itself, taking the placement rather than fetching it, so a
    /// caller that already holds a catalog snapshot does not pay a second
    /// guard for it. THE one copy — `shard_index` and `sink_with` are two
    /// doors into this, never two implementations of it.
    #[inline]
    fn place(placement: Option<u16>, stream_id: StreamId, len: usize) -> usize {
        match placement {
            Some(shard) => (shard as usize).min(len - 1),
            None => stream_id.raw() as usize % len,
        }
    }

    /// Append, through whichever door this thread is entitled to.
    ///
    /// Same-thread is a direct call into the journal — no lock, no channel.
    /// Otherwise the entries go to the owning shard as a command and the
    /// sequence comes back on a oneshot. `owned` is a closure so the fast
    /// path never builds the owned `Vec` it would not use.
    ///
    /// `None` means the append was refused (quota) or the shard is gone;
    /// both are "no sequence to report", which is all the caller can act on.
    pub async fn append(
        &self,
        cat: &arbitro_common::name_registry::Snapshot<'_>,
        stream_id: StreamId,
        entries: &[arbitro_store::EntryRef<'_>],
        owned: impl FnOnce() -> Vec<crate::shard::command::PublishEntryOwned>,
        now_ms: u64,
        reply_to: crate::shard::command::PublishReply,
    ) -> Append {
        use crate::sink::StreamSink;
        let idx = Self::place(cat.stream_shard(stream_id), stream_id, self.shard_count);
        if super::local::owns(idx) {
            return match crate::sink::LocalSink::new(idx, &self.gates[idx])
                .publish(entries, now_ms)
            {
                Ok(seq) => Append::Stored(seq),
                Err(_) => Append::Refused,
            };
        }
        // Deliberately NOT awaited. The shard answers the client itself;
        // waiting here for the sequence just to forward it would serialise
        // a fire-and-forget publisher against its own broker.
        match self.shards[idx]
            .publish_routed(stream_id, owned(), now_ms, reply_to)
            .await
        {
            Ok(()) => Append::Delegated,
            Err(_) => Append::Refused,
        }
    }

    /// `append`, for the callers that need the assigned sequence itself.
    ///
    /// This one WAITS on the routed path, which is exactly what `append`
    /// refuses to do — so it is not the default and should stay rare.
    /// Replication is the legitimate case: it cannot ack its leader without
    /// the number. `None` means refused or the shard is gone.
    pub async fn append_for_seq(
        &self,
        cat: &arbitro_common::name_registry::Snapshot<'_>,
        stream_id: StreamId,
        entries: &[arbitro_store::EntryRef<'_>],
        owned: impl FnOnce() -> Vec<crate::shard::command::PublishEntryOwned>,
        now_ms: u64,
    ) -> Option<u64> {
        use crate::sink::StreamSink;
        let idx = Self::place(cat.stream_shard(stream_id), stream_id, self.shard_count);
        if super::local::owns(idx) {
            return crate::sink::LocalSink::new(idx, &self.gates[idx])
                .publish(entries, now_ms)
                .ok();
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.shards[idx]
            .publish_routed(
                stream_id,
                owned(),
                now_ms,
                crate::shard::command::PublishReply::Seq(tx),
            )
            .await
            .ok()?;
        rx.await.ok().flatten()
    }

    /// Journal stats for a stream, through whichever door applies.
    ///
    /// Async for the same reason `append` is: off-thread callers have to ask
    /// the owning shard. Quota pre-checks read this, so it is on the publish
    /// path — but only for streams that actually declare a quota.
    pub async fn store_stats(
        &self,
        cat: &arbitro_common::name_registry::Snapshot<'_>,
        stream_id: StreamId,
    ) -> arbitro_store::StoreInfo {
        use crate::sink::StreamSink;
        let idx = Self::place(cat.stream_shard(stream_id), stream_id, self.shard_count);
        if super::local::owns(idx) {
            return crate::sink::LocalSink::new(idx, &self.gates[idx]).info();
        }
        match self.shards[idx].store_info(stream_id).await {
            Ok(r) => arbitro_store::StoreInfo {
                messages: r.messages,
                bytes: r.bytes,
                ..Default::default()
            },
            Err(_) => arbitro_store::StoreInfo::default(),
        }
    }

    /// `store_stats` for callers that hold no catalog snapshot — the cold
    /// ack-state paths, which read this once per request rather than per
    /// entry and have no reason to carry one.
    pub async fn store_stats_for(&self, stream_id: StreamId) -> arbitro_store::StoreInfo {
        let cat = self.names.snapshot();
        self.store_stats(&cat, stream_id).await
    }

    /// The lock-free sink, when THIS thread owns the stream's shard.
    ///
    /// `None` means the caller is somewhere else — the shared pool, the
    /// accept loop, or another shard's thread — and must take the routed
    /// path instead. Returning an `Option` rather than falling back to the
    /// shared sink is deliberate: the two doors cost different things, and
    /// a caller that cannot see which one it went through cannot be
    /// measured or reasoned about.
    #[inline]
    pub fn local_sink(&self, stream_id: StreamId) -> Option<crate::sink::LocalSink<'_>> {
        let idx = self.shard_index(stream_id, self.shard_count);
        if !super::local::owns(idx) {
            return None;
        }
        Some(crate::sink::LocalSink::new(idx, &self.gates[idx]))
    }


    #[inline]
    fn shard_index(&self, stream_id: StreamId, len: usize) -> usize {
        Self::place(self.names.stream_shard(stream_id), stream_id, len)
    }

    /// Where a NEWLY created stream should live. Same rule the routing uses
    /// for an unplaced stream, so recording this value cannot disagree with
    /// where the bytes actually go.
    ///
    /// Deliberately still the modulo: it spreads streams evenly with no
    /// state to maintain. What changes is that the answer is now WRITTEN
    /// DOWN, so a later change to the shard count cannot move the stream.
    #[inline]
    pub fn shard_index_for_new(&self, stream_id: StreamId) -> u16 {
        (stream_id.raw() as usize % self.shard_count) as u16
    }


    #[inline]
    pub fn gate_for(&self, stream_id: StreamId) -> &Arc<Gate> {
        let idx = self.shard_index(stream_id, self.gates.len());
        &self.gates[idx]
    }

    /// Per-shard idempotency tracker handle. The publish hot path
    /// (`dispatch_v2::v2_publish`) uses this to check + record
    /// dedup state when the stream has `idempotency_window_ms > 0`.
    /// `Option<...>` inside the Mutex is `None` until the first
    /// idempotent publish allocates it lazily.
    #[inline]
    pub fn idempotency_for(&self, stream_id: StreamId) -> &super::idempotency::SharedIdempotency {
        let idx = self.shard_index(stream_id, self.idempotency.len());
        &self.idempotency[idx]
    }

    /// Per-shard "tracker allocated" flag — flip to `true` after the
    /// publish hot path lazily allocates the idempotency tracker so
    /// the command worker's `select!` predicate can stop locking the
    /// Arc just to call `Option::is_some()` (F10).
    #[inline]
    pub fn mark_idempotency_allocated(&self, stream_id: StreamId) {
        let idx = self.shard_index(stream_id, self.has_idempotency.len());
        self.has_idempotency[idx].store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn shard_for(&self, stream_id: StreamId) -> &ShardHandle {
        let idx = self.shard_index(stream_id, self.shards.len());
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

    /// Set the replication sender. Called once during cluster boot in
    /// `server.rs` after the `replication_loop` task is spawned. The
    /// sender is stored inside the shared `Arc<Mutex<Option<...>>>`
    /// that every shard worker already holds a clone of, so workers
    /// see it immediately on their next flush without any explicit
    /// propagation.
    #[cfg(feature = "cluster")]
    pub fn set_replication_tx(
        &self,
        tx: mpsc::Sender<crate::cluster::replication::ReplicationBatch>,
    ) {
        *self.replication_tx.lock() = Some(tx);
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
