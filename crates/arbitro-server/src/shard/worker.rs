//! Shard workers — drain thread + command thread, **zero Mutex**.
//!
//! **Drain thread** (`drain-N`) — pure dedicated loop:
//! ```text
//! loop {
//!     gate.acquire();
//!     while gate.is_open() { drain_cycle(); }
//! }
//! ```
//! Reads `SharedCounters` (atomics) + `SnapshotSwap<DrainSnapshot>` (Arc).
//! Never touches the engine. Never blocks.
//!
//! **Command thread** (`cmd-N`) — owns `ArbitroEngine` exclusively:
//! subscribe, ack, nack, pause, accumulator, admin. Mutates engine with
//! `&mut self`. Updates `SharedCounters` atomically. Swaps `DrainSnapshot`
//! on structural changes (subscribe/unsubscribe/bind).
//!
//! **Zero Mutex between threads.** Drain and commands run fully in parallel.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_engine_v2::types::*;
use arbitro_engine_v2::ArbitroEngine;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::common::Gate;
use crate::shard::command::*;
use crate::shard::router::SharedStore;
use crate::shard::shared::{
    DrainNotification, DrainSnapshot, SharedCounters, SnapshotSwap,
};
use crate::transport::ConnectionRegistry;

// ── Cross-handler private types ──────────────────────────────────────────────

/// A bound consumer↔connection pair for delivery.
///
/// Created by `handle_subscribe` / `handle_bind`, iterated by
/// `drain::drain_cycle`, filtered on unsubscribe / delete.
pub struct ActiveBinding {
    pub(super) binding_id: BindingId,
    pub(super) connection_id: ConnectionId,
    pub(super) consumer_id: ConsumerId,
    pub(super) stream_id: StreamId,
    pub(super) queue_id: QueueId,
    /// Configured `max_inflight` cached at subscribe time.
    pub(super) max_inflight: u32,
    /// `AckPolicy::None` — skip inflight tracking and capacity checks.
    pub(super) fire_and_forget: bool,
    /// Cached shared writer half — cloned once from the registry at
    /// subscribe time (~3 ns). The drain writes directly to the socket
    /// via `try_write` + `writable()` for backpressure — no intermediate
    /// channel, no writer task.
    pub(super) writer: Arc<tokio::net::tcp::OwnedWriteHalf>,
    /// Tokio runtime handle — needed so the drain OS thread can
    /// `block_on(writer.writable())` when the kernel buffer fills.
    pub(super) runtime: tokio::runtime::Handle,
}

// ── Accumulator private types ────────────────────────────────────────────────

/// Flusher configuration — controls when accumulated entries are flushed.
pub(super) struct FlusherConfig {
    pub(super) max_size: usize,
    pub(super) max_bytes: usize,
    pub(super) interval_ms: u64,
}

impl Default for FlusherConfig {
    fn default() -> Self {
        Self {
            max_size: 1024,
            max_bytes: 4 * 1024 * 1024,
            interval_ms: 5,
        }
    }
}

/// Tracks who to reply to after an accumulated flush.
pub(super) struct AccumCaller {
    pub(super) conn_id: u64,
    pub(super) env_seq: u32,
    pub(super) entry_count: u32,
}

/// Per-stream accumulation buffer.
pub(super) struct StreamAccum {
    pub(super) store_entries: Vec<PublishEntryOwned>,
    pub(super) callers: Vec<AccumCaller>,
    pub(super) bytes: usize,
}

// ── Drain worker ────────────────────────────────────────────────────────────

/// Pure drain thread — gate.acquire → drain_cycle → loop.
/// Nothing else runs here. No commands, no engine, no Mutex.
///
/// Reads atomics (`SharedCounters`) and snapshots (`SnapshotSwap`)
/// for all decisions. After delivery, increments atomic inflight and
/// pushes notifications to the command thread via lock-free channel.
pub struct DrainWorker {
    pub(super) counters: Arc<SharedCounters>,
    pub(super) snapshot: Arc<SnapshotSwap<DrainSnapshot>>,
    pub(super) store: SharedStore,
    pub(super) gate: Arc<Gate>,
    pub(super) names: Arc<crate::common::NameRegistry>,
    pub(super) drain_config: super::drain::DrainConfig,
    pub(super) drain_scratch: super::drain::DrainScratch,
    pub(super) running: Arc<std::sync::atomic::AtomicBool>,
    /// Notifications to command thread (deliveries + dead connections).
    pub(super) notify_tx: mpsc::Sender<DrainNotification>,
}

impl DrainWorker {
    /// Pure drain loop — nothing else runs on this thread.
    pub fn run(mut self) {
        self.gate.set_worker(std::thread::current());

        // ── Store init ───────────────────────────────────────────────────
        {
            let mut store_guard = self.store.lock().unwrap();
            if let Err(e) = store_guard.init() {
                tracing::error!(error = ?e, "store init failed");
            }
            let info = store_guard.info();
            if info.last_seq > 0 {
                self.counters.set_cursor(info.last_seq);
            }
        }

        loop {
            self.gate.acquire();

            if !self.running.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }

            while self.gate.is_open() {
                crate::lifecycle_trace!("20_gate_open_detected", 0, 0, "shard");

                let now_ms = if self.drain_config.max_age_ms > 0 {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64
                } else {
                    0
                };

                // Load snapshot — Arc clone (~3ns), no lock on engine.
                let snap = self.snapshot.load();
                let prev_cursor = self.counters.cursor();

                // Check for rewind signal from command thread.
                if let Some(rw) = self.counters.take_rewind() {
                    let cur = self.counters.cursor();
                    if rw > 0 && rw - 1 < cur {
                        self.counters.set_cursor(rw - 1);
                    }
                }

                {
                    let store_guard = self.store.lock().unwrap();
                    super::drain::drain_cycle(
                        &self.counters,
                        &snap,
                        &**store_guard,
                        &self.gate,
                        &self.names,
                        &self.drain_config,
                        &mut self.drain_scratch,
                        &self.notify_tx,
                        now_ms,
                    );
                }

                let stalled = self.counters.cursor() == prev_cursor;

                // Backpressure: cursor didn't advance → downstream full.
                if stalled && self.gate.is_open() {
                    std::thread::park_timeout(
                        std::time::Duration::from_micros(50),
                    );
                    break;
                }
            }
        }
    }
}

// ── Command worker ──────────────────────────────────────────────────────────

/// Command task — owns `ArbitroEngine` exclusively. No Mutex.
///
/// Processes all ShardCommands as a tokio::spawn task. After engine
/// mutations, updates `SharedCounters` atomically and swaps
/// `DrainSnapshot` for structural changes.
pub struct CommandWorker {
    /// Engine — owned exclusively. `&mut self`, no sharing, no lock.
    pub(super) engine: ArbitroEngine,
    /// Atomic counters shared with drain.
    pub(super) counters: Arc<SharedCounters>,
    /// Structural snapshot shared with drain.
    pub(super) snapshot: Arc<SnapshotSwap<DrainSnapshot>>,
    pub(super) store: SharedStore,
    pub(super) gate: Arc<Gate>,
    pub(super) registry: ConnectionRegistry,
    pub(super) names: Arc<crate::common::NameRegistry>,
    pub(super) rx: mpsc::Receiver<ShardCommand>,
    /// Notifications from drain thread (deliveries + dead connections).
    pub(super) notify_rx: mpsc::Receiver<DrainNotification>,
    pub(super) running: Arc<std::sync::atomic::AtomicBool>,
    // Accumulator
    pub(super) flusher_config: FlusherConfig,
    // StreamId is dense but admin-path (publish accumulation), so HashMap is
    // acceptable here — but we opt into ahash per the dense/sparse rule
    // (performance.md): non-std hashers for any keyed lookup.
    pub(super) accum_streams: HashMap<StreamId, StreamAccum, rustc_hash::FxBuildHasher>,
    pub(super) accum_deadline: Option<Instant>,
    pub(super) accum_total: usize,
    pub(super) accum_bytes: usize,
    pub(super) drain_config_batch_size: u16,
    /// Local bindings list — command thread's copy. Cloned into
    /// `DrainSnapshot` on structural changes.
    pub(super) bindings: Vec<ActiveBinding>,
}

impl CommandWorker {
    /// Async command loop — runs as a `tokio::spawn` task.
    pub async fn run(mut self) {
        loop {
            // Process any pending drain notifications first (non-blocking).
            self.drain_notifications();

            if self.accum_total > 0 {
                let timeout = self.accum_deadline
                    .map(|d| d.saturating_duration_since(Instant::now()))
                    .unwrap_or(Duration::from_millis(
                        self.flusher_config.interval_ms,
                    ));

                tokio::select! {
                    cmd = self.rx.recv() => {
                        match cmd {
                            Some(cmd) => {
                                if self.handle_or_shutdown(cmd) {
                                    return;
                                }
                            }
                            None => return,
                        }
                    }
                    notif = self.notify_rx.recv() => {
                        if let Some(n) = notif {
                            self.handle_notification(n);
                        }
                    }
                    _ = tokio::time::sleep(timeout) => {
                        self.flush_accumulator();
                    }
                }
            } else {
                tokio::select! {
                    cmd = self.rx.recv() => {
                        match cmd {
                            Some(cmd) => {
                                if self.handle_or_shutdown(cmd) {
                                    return;
                                }
                            }
                            None => return,
                        }
                    }
                    notif = self.notify_rx.recv() => {
                        if let Some(n) = notif {
                            self.handle_notification(n);
                        }
                    }
                }
            }
        }
    }

    /// Process drain notifications (non-blocking batch drain).
    pub(super) fn drain_notifications(&mut self) {
        while let Ok(n) = self.notify_rx.try_recv() {
            self.handle_notification(n);
        }
    }

    /// Handle a single drain notification.
    fn handle_notification(&mut self, notif: DrainNotification) {
        match notif {
            DrainNotification::Delivered {
                binding_id,
                entries,
                ..
            } => {
                // Update engine's pending list for future ack/retire.
                use arbitro_engine_v2::command::Command;
                let stream_id = self
                    .engine
                    .ctx()
                    .catalog
                    .binding(binding_id)
                    .map(|b| b.stream_id)
                    .unwrap_or(StreamId(0));
                let _ = self.engine.execute(&Command::Delivered {
                    stream_id,
                    binding_id,
                    entries: &entries,
                });
            }
            DrainNotification::ConnectionDead(conn_id) => {
                let delta = self.engine.mark_connection_dead(conn_id);
                self.apply_delta_and_sync(&delta);
            }
        }
    }

    /// Returns `true` if shutdown was requested.
    fn handle_or_shutdown(&mut self, cmd: ShardCommand) -> bool {
        if matches!(cmd, ShardCommand::Shutdown) {
            self.flush_accumulator();
            if let Err(e) = self.store.lock().unwrap().shutdown() {
                tracing::error!(error = ?e, "store shutdown failed");
            }
            self.running.store(false, std::sync::atomic::Ordering::Relaxed);
            self.gate.release();
            return true;
        }
        self.dispatch_command(cmd);
        false
    }

    pub(super) fn check_accumulator_flush(&mut self) {
        if self.accum_total == 0 {
            return;
        }
        let force = self.accum_total >= self.flusher_config.max_size
            || self.accum_bytes >= self.flusher_config.max_bytes;
        let expired = self
            .accum_deadline
            .is_some_and(|d| Instant::now() >= d);
        if force || expired {
            self.flush_accumulator();
        }
    }

    /// Dispatch a single command to its handler.
    fn dispatch_command(&mut self, cmd: ShardCommand) {
        match cmd {
            ShardCommand::PublishAccumulate(cmd) => self.handle_publish_accumulate(cmd),
            ShardCommand::Ack(cmd) => self.handle_ack(cmd),
            ShardCommand::Nack(cmd) => self.handle_nack(cmd),
            ShardCommand::Subscribe(cmd) => self.handle_subscribe(cmd),
            ShardCommand::Unsubscribe(cmd) => self.handle_unsubscribe(cmd),
            ShardCommand::CreateStream(cmd) => self.handle_create_stream(cmd),
            ShardCommand::DeleteStream(cmd) => self.handle_delete_stream(cmd),
            ShardCommand::CreateConsumer(cmd) => self.handle_create_consumer(cmd),
            ShardCommand::DeleteConsumer(cmd) => self.handle_delete_consumer(cmd),
            ShardCommand::OpenConnection(cmd) => self.handle_open_connection(cmd),
            ShardCommand::DrainConnection(cmd) => self.handle_drain_connection(cmd),
            ShardCommand::Bind(cmd) => self.handle_bind(cmd),
            ShardCommand::ListStreams(cmd) => self.handle_list_streams(cmd),
            ShardCommand::ListConsumers(cmd) => self.handle_list_consumers(cmd),
            ShardCommand::StoreInfo(cmd) => self.handle_store_info(cmd),
            ShardCommand::PauseConsumer(cmd) => self.handle_pause_consumer(cmd),
            ShardCommand::ResumeConsumer(cmd) => self.handle_resume_consumer(cmd),
            ShardCommand::Shutdown => {}
        }
    }

    // ── Snapshot sync ──────────────────────────────────────────────────

    /// Apply engine delta events and sync shared state.
    /// Called after engine mutations that may retire bindings.
    pub(super) fn apply_delta_and_sync(&mut self, delta: &arbitro_engine_v2::DeltaEvents) {
        if !delta.demand_became_available.is_empty() {
            self.gate.release();
        }
        // Decrement subject inflight for acked/retired pendings.
        // Key is (consumer_id, subject_hash) — per-consumer isolation.
        for &(cid, sh) in &delta.subject_hashes_acked {
            self.counters.dec_subject(cid, sh);
        }
        // Demand atomics are already updated by subscribe/unsubscribe handlers.
        // DeltaEvents demand_became_available/idle are informational only here.
        // Remove retired bindings
        for &bid in &delta.bindings_retired {
            self.bindings.retain(|b| b.binding_id != bid);
        }
        if !delta.bindings_retired.is_empty() {
            self.rebuild_and_swap_snapshot();
        }
    }

    /// Rebuild the drain snapshot from current bindings + engine match tables
    /// and swap it into the shared SnapshotSwap.
    ///
    /// Fase C.2: the snapshot's match_tables get their `binding_idx`
    /// fields **stamped** with the server-layer binding index, so the
    /// drain can fetch the binding via `bindings[match_entry.binding_idx]`
    /// — a direct Vec index — instead of a `(consumer_id, connection_id)`
    /// HashMap lookup on every match.
    pub(super) fn rebuild_and_swap_snapshot(&self) {
        // We need to clone bindings for the snapshot because drain holds Arc
        // while we might modify our local copy later.
        let snap_bindings: Vec<ActiveBinding> = self
            .bindings
            .iter()
            .map(|b| ActiveBinding {
                binding_id: b.binding_id,
                connection_id: b.connection_id,
                consumer_id: b.consumer_id,
                stream_id: b.stream_id,
                queue_id: b.queue_id,
                max_inflight: b.max_inflight,
                fire_and_forget: b.fire_and_forget,
                writer: Arc::clone(&b.writer),
                runtime: b.runtime.clone(),
            })
            .collect();

        // Build per-connection writer index. Dedup by connection_id —
        // multiple consumers on the same conn share a single writer.
        // HashMap+ahash: connection_id is unbounded-monotonic, direct
        // Vec<Option<T>> would leak memory, and HashMap beats binary_search.
        let mut writers_by_conn: std::collections::HashMap<
            u64, crate::shard::shared::WriterIndexEntry, rustc_hash::FxBuildHasher,
        > = std::collections::HashMap::with_capacity_and_hasher(
            self.bindings.len(), rustc_hash::FxBuildHasher::default(),
        );
        for b in &self.bindings {
            writers_by_conn
                .entry(b.connection_id.0)
                .or_insert_with(|| crate::shard::shared::WriterIndexEntry {
                    writer: Arc::clone(&b.writer),
                    runtime: b.runtime.clone(),
                });
        }

        // Clone match tables from engine catalog (deep clone — the
        // stamping mutates this copy, NOT the engine's canonical state).
        let catalog = &self.engine.ctx().catalog;
        let mut match_tables = catalog.clone_match_tables();

        // Stamp `binding_idx` onto match entries by walking self.bindings.
        // For each active binding at server-index `i`, find all match
        // entries on its stream's match table that correspond to
        // `(consumer_id, connection_id)` and stamp `binding_idx = i`.
        // Match entries not covered here retain BINDING_IDX_UNBOUND —
        // drain skips them defensively.
        for (i, b) in self.bindings.iter().enumerate() {
            let stream_idx = b.stream_id.0 as usize;
            if let Some(Some(mt)) = match_tables.get_mut(stream_idx) {
                mt.set_binding_idx_for(
                    b.consumer_id,
                    b.connection_id,
                    i as u32,
                );
            }
        }

        self.snapshot.store(DrainSnapshot {
            bindings: snap_bindings,
            writers_by_conn,
            match_tables,
        });
    }
}
