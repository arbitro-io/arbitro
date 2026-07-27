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

use tokio::sync::mpsc;

use crate::common::Gate;
use crate::shard::command::*;
use crate::shard::consumer_subjects::ConsumerSubjects;
use crate::shard::drain_events::DrainEvent;
use crate::shard::router::SharedStore;
use crate::shard::shared::{
    DrainNotification, DrainSnapshot, SharedCounters, SnapshotSwap,
};
use crate::transport::ConnectionRegistry;

// ── Per-stream retention config ──────────────────────────────────────────────

/// Retention limits stored in the command worker and propagated into
/// `DrainSnapshot` for zero-copy access by the drain thread.
#[derive(Clone, Copy, Default)]
pub(super) struct StreamRetention {
    /// Max messages per stream (0 = unlimited).
    pub max_msgs: u64,
    /// Max bytes per stream (0 = unlimited).
    pub max_bytes: u64,
    /// Age-based eviction threshold in milliseconds (0 = disabled).
    pub max_age_ms: u64,
    /// Global journal seq at which this stream incarnation was created.
    /// Drain skips entries with seq < created_at_seq for this stream_id.
    /// 0 = no filter (backward compat for streams created before this feature).
    pub created_at_seq: u64,
}

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
    /// Ack deadline in milliseconds. 0 = no timeout (no wheel entry).
    pub(super) ack_wait_ms: u32,
    /// Sender to the per-connection async writer task. `try_send` is
    /// non-blocking — no `block_in_place`, no write lock, no runtime handle.
    pub(super) write_tx: tokio::sync::mpsc::Sender<bytes::Bytes>,
    /// **M8**: writer feedback — `true` when the writer task has hit an
    /// I/O error. Shared with the writer task via Arc.
    pub(super) write_failed: std::sync::Arc<std::sync::atomic::AtomicBool>,
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
    /// SPSC — drain owns the sole producer half (this task).
    pub(super) notify_ring: crate::shard::shared::NotifyProducer,
    /// Drain-event ring: command → drain (ack-driven subject inflight decs).
    /// SPSC — drain owns the sole consumer half (this task).
    /// Drained at the top of every drain cycle via non-blocking `try_recv`.
    pub(super) drain_evt_rx: crate::shard::drain_events::DrainEventConsumer,
    /// Per-consumer subject inflight, indexed by `ConsumerId.raw()`. Slot
    /// is lazily allocated on first inc; reset to `None` on
    /// `DrainEvent::ConsumerRemoved`. Single-thread owned by drain — no
    /// locks, no atomics. Replaces `SharedCounters.subject` (papaya).
    pub(super) consumer_subjects: Vec<Option<ConsumerSubjects>>,
    /// H10: shared silent-drop counters. Wired into `drain::drain_deliver`
    /// so the drain → cmd notify-ring drop sites bump
    /// `silent_drops.notify_ring` instead of failing silently.
    pub(super) silent_drops: Arc<crate::common::SilentDrops>,
}

impl DrainWorker {
    /// Pure drain loop — runs as a tokio task. `gate.acquire().await`
    /// suspends the task on `tokio::sync::Notify` (via kit's
    /// `NotifyWaiter`). No `std::thread`, no `JoinHandle<()>` of OS threads.
    pub async fn run(mut self) {
        // ── Store init ───────────────────────────────────────────────────
        {
            let mut store_guard = self.store.lock();
            if let Err(e) = store_guard.init() {
                tracing::error!(error = ?e, "store init failed");
            }
            let info = store_guard.info();
            if info.last_seq > 0 {
                self.counters.set_cursor(info.last_seq);
            }
        }

        loop {
            crate::lifecycle_trace!("19_1_gate_waiting", 0, 0, "shard");
            self.gate.acquire().await;
            crate::lifecycle_trace!("19_2_gate_acquired", 0, 0, "shard");

            if !self.running.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }

            loop {
                // Shutdown liveness: when any entry is capacity-blocked,
                // `more_pending` re-opens the gate every cycle so this inner
                // loop never parks on `acquire()` — making the outer loop's
                // `running` check (above) unreachable. If the acks that would
                // free that capacity never arrive (e.g. the command worker has
                // already processed Shutdown), the drain would spin until the
                // process dies and `ShardRouter::shutdown()`'s JoinHandle await
                // would deadlock. One relaxed load per cycle guarantees the
                // drain always terminates within a cycle on shutdown.
                if !self.running.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }

                crate::lifecycle_trace!("20_gate_open_detected", 0, 0, "shard");

                // ── Clear the gate BEFORE reading the store ───────────────
                //
                // This closes a tail-message lost-wakeup. A publish/ack does
                // `append_batch → send ack → gate.release()` (see
                // dispatch_v2). The drain's OLD end-of-cycle `gate.lock()`
                // could wipe a `release()` that raced in just before it,
                // stranding the entry in the store. Normally the next publish
                // re-wakes the drain, but the FINAL message of a burst (no
                // later publish — e.g. the last message before a server kill,
                // or the last message of the whole run) would sit undelivered
                // forever → delivery loss.
                //
                // By clearing here, at the TOP of the cycle:
                //   * a `release()` that arrives AFTER this clear re-opens the
                //     gate → the `is_open()` check at the bottom loops again;
                //   * a `release()` CLOBBERED by this clear necessarily
                //     appended its entry BEFORE releasing (publish orders
                //     append-under-store-lock strictly before release), and we
                //     take the store lock AFTER this clear, so the store read
                //     below is guaranteed to observe that entry and deliver it.
                // `drain_deliver` now only ever RE-OPENS the gate (on
                // `more_pending`); it never clears it. Clearing is owned here.
                self.gate.lock();

                // Drain the command→drain event ring before deciding any
                // delivery. This applies acks (subject inflight decs) and
                // consumer removals so the upcoming dispatch sees current
                // per-consumer state.
                drain_event_ring(&mut self.drain_evt_rx, &mut self.consumer_subjects);

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

                // Split drain into two phases so the store lock is held
                // ONLY during the for_each walk (Phase 1). TCP delivery
                // and bookkeeping (Phase 2+3) run lock-free.
                let read_result = {
                    let store_guard = self.store.lock();
                    super::drain::drain_read(
                        &self.counters,
                        &snap,
                        &**store_guard,
                        &self.drain_config,
                        &mut self.drain_scratch,
                        &mut self.consumer_subjects,
                        now_ms,
                    )
                };
                // Store lock released — publish can proceed concurrently.
                // `drain_deliver` re-opens the gate iff there is more work
                // (`more_pending`); the `None` branch (no work this cycle)
                // leaves the gate cleared by the top-of-loop `lock()`.
                if let Some(result) = read_result {
                    super::drain::drain_deliver(
                        &self.counters,
                        &snap,
                        &self.gate,
                        &self.names,
                        &mut self.drain_scratch,
                        &mut self.consumer_subjects,
                        &mut self.notify_ring,
                        &self.silent_drops,
                        self.drain_config.stall_evict_ms,
                        result,
                    );
                }

                let stalled = self.counters.cursor() == prev_cursor;

                // Backpressure: work remains (gate re-opened via `more_pending`)
                // but the cursor didn't advance → downstream channel full.
                // Yield briefly so the writer task can drain before we retry.
                if stalled && self.gate.is_open() {
                    tokio::time::sleep(std::time::Duration::from_micros(50)).await;
                }

                // No pending signal → genuinely idle: go back to sleep on
                // `acquire()`. Any concurrent `release()` (publish/ack/rewind)
                // that landed after the top-of-loop clear keeps the gate open
                // and makes us loop instead.
                if !self.gate.is_open() {
                    crate::lifecycle_trace!("33_drainer_exit_locked", 0, 0, "shard");
                    break;
                }
            }
        }
    }
}

/// Apply every pending [`DrainEvent`] in the ring to the per-consumer
/// subject inflight slots. Non-blocking; returns as soon as the ring is
/// empty. Called at the top of every drain cycle.
#[inline]
fn drain_event_ring(
    rx: &mut crate::shard::drain_events::DrainEventConsumer,
    consumer_subjects: &mut Vec<Option<ConsumerSubjects>>,
) {
    while let Ok(evt) = rx.try_recv() {
        match evt {
            DrainEvent::Ack {
                consumer_id,
                subject_hash,
                ack_floor,
            } => {
                let idx = consumer_id.raw() as usize;
                if let Some(Some(cs)) = consumer_subjects.get_mut(idx) {
                    cs.dec(subject_hash);
                    cs.raise_ack_floor(ack_floor);
                }
            }
            DrainEvent::ConsumerRemoved { consumer_id } => {
                let idx = consumer_id.raw() as usize;
                if let Some(slot) = consumer_subjects.get_mut(idx) {
                    *slot = None;
                }
            }
        }
    }
}

/// BUG2: rewind the drain cursor so seqs released by a retired binding get
/// redelivered. Mirrors `handle_bind`'s protocol: move the cursor back for
/// the common case (drain sleeping) AND `signal_rewind` so a mid-flight
/// drain cycle can't clobber the cursor when it writes `new_cursor` at the
/// end of its cycle. `signal_rewind` takes the min, so a smaller pending
/// rewind (more replay) still wins — redelivery is safe, message loss is not.
#[inline]
fn rewind_released(counters: &SharedCounters, min_seq: u64) {
    if min_seq == 0 {
        return;
    }
    let cur = counters.cursor();
    if min_seq - 1 < cur {
        counters.set_cursor(min_seq - 1);
    }
    counters.signal_rewind(min_seq);
}

/// Mutable accessor for a consumer's subject inflight, creating the slot
/// on demand. Slot index = `ConsumerId.raw() as usize`.
#[inline]
pub(in crate::shard) fn consumer_subjects_slot_mut(
    consumer_subjects: &mut Vec<Option<ConsumerSubjects>>,
    consumer_id: u32,
) -> &mut ConsumerSubjects {
    let idx = consumer_id as usize;
    if idx >= consumer_subjects.len() {
        consumer_subjects.resize_with(idx + 1, || None);
    }
    consumer_subjects[idx].get_or_insert_with(ConsumerSubjects::new)
}

/// Read-only accessor. Returns `None` if the consumer has no tracked
/// subjects yet — caller treats that as "0 inflight" (always has room).
#[inline]
pub(in crate::shard) fn consumer_subjects_slot(
    consumer_subjects: &[Option<ConsumerSubjects>],
    consumer_id: u32,
) -> Option<&ConsumerSubjects> {
    consumer_subjects
        .get(consumer_id as usize)
        .and_then(|s| s.as_ref())
}

// ── Command worker ──────────────────────────────────────────────────────────

/// Command task — owns `ArbitroEngine` exclusively. No Mutex.
///
/// Processes all ShardCommands as a tokio::spawn task. After engine
/// mutations, updates `SharedCounters` atomically and swaps
/// `DrainSnapshot` for structural changes.
#[allow(dead_code)] // `names`, `drain_config_batch_size` kept for upcoming features
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
    /// SPSC — command owns the sole consumer half (this task).
    pub(super) notify_ring: crate::shard::shared::NotifyConsumer,
    /// Drain-event ring shared with DrainWorker — command owns the sole
    /// producer half (this task), drain thread is the sole consumer.
    /// Used to push ack-driven subject inflight decrements + consumer
    /// cleanup events.
    pub(super) drain_evt_tx: crate::shard::drain_events::DrainEventProducer,
    pub(super) running: Arc<std::sync::atomic::AtomicBool>,
    // Accumulator
    pub(super) flusher_config: FlusherConfig,
    // StreamId is dense but admin-path (publish accumulation), so HashMap is
    // acceptable here — but we opt into foldhash per the dense/sparse rule
    // (performance.md): non-std hashers for any keyed lookup.
    pub(super) accum_streams: HashMap<StreamId, StreamAccum, foldhash::fast::FixedState>,
    pub(super) accum_deadline: Option<Instant>,
    pub(super) accum_total: usize,
    pub(super) accum_bytes: usize,
    pub(super) drain_config_batch_size: u16,
    /// Per-stream retention limits. Set at CreateStream, cleared at DeleteStream.
    /// Propagated into `DrainSnapshot` during snapshot rebuild.
    pub(super) stream_retention: HashMap<StreamId, StreamRetention, foldhash::fast::FixedState>,
    /// Local bindings list — command thread's copy. Cloned into
    /// `DrainSnapshot` on structural changes.
    pub(super) bindings: Vec<ActiveBinding>,
    /// Next time to run max_age eviction (cold path, every 5 seconds).
    pub(super) next_eviction: Option<Instant>,
    /// Timing wheel for ack deadlines and nack-with-delay.
    /// Created lazily on first consumer with ack_wait_ms > 0.
    /// Resolution: 1 second per tick, 120 buckets = covers up to 120s.
    pub(super) wheel: Option<arbitro_common::TimingWheel<arbitro_common::WheelEntry>>,
    /// Scratch buffer reused across wheel ticks to avoid allocation.
    pub(super) wheel_buf: Vec<arbitro_common::WheelEntry>,
    /// Next time to advance the wheel (1 tick per second).
    pub(super) next_wheel_tick: Option<Instant>,
    /// Per-shard idempotency dedup, shared with `dispatch_v2` so the
    /// publish hot path can check membership and record new entries
    /// (publishes don't go through this worker — they hit the store
    /// directly via `ShardRouter::store_for`). Wrapped in `Arc<Mutex>`
    /// for the same reason `SharedStore` is: the publish path locks,
    /// the worker's tick loop also locks (1Hz), uncontended in normal
    /// operation.
    ///
    /// `Option<...>` inside the Mutex stays `None` until the first
    /// publish that hits an idempotent stream owned by this shard
    /// (lazy allocation). Cost when None: zero — the publish hot path
    /// fast-bails via `NameRegistry::stream_idempotency_window_ms`
    /// before touching this Arc.
    pub(super) idempotency_tracker: crate::shard::idempotency::SharedIdempotency,
    /// F10 — cached "has idempotency tracker been allocated" flag.
    /// Used in `tokio::select!` predicates to avoid locking the shared
    /// `Arc<Mutex<Option<IdempotencyTracker>>>` on every iteration just
    /// to call `Option::is_some()`. Flipped to `true` the first time the
    /// publish hot path allocates the tracker; never goes back to false
    /// in steady state (the tracker only drops when the shard shuts down).
    pub(super) has_idempotency: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// F27 — reusable buffer for accumulator flush. Avoids one Vec
    /// allocation per `flush_accumulator()` call (200/s at the default
    /// 5 ms interval per shard).
    pub(super) flush_stream_ids: Vec<StreamId>,
    /// H10: shared silent-drop counters.
    pub(super) silent_drops: Arc<crate::common::SilentDrops>,
    /// H11: ConsumerRemoved events that lost a `try_send` to the drain
    /// ring. Drained at the top of every command loop iteration so a
    /// transient ring-full doesn't leak the per-consumer subject slot.
    pub(super) pending_consumer_remove: Vec<ConsumerId>,
    /// Per-consumer contiguous-acked floor (temporal isolation). Fed in
    /// `handle_ack` with engine-matched seqs only; the current floor is
    /// piggybacked on every `DrainEvent::Ack` so the drain-owned
    /// `ConsumerSubjects` slot can skip re-delivering acked seqs on a
    /// cursor rewind. See `shard/ack_floor.rs`.
    pub(super) ack_floors: crate::shard::ack_floor::AckFloors,
    /// DLQ nack counter: `(consumer_id, seq) → nack_count`. Tracks how
    /// many times each message has been nacked by a given consumer. When
    /// the count exceeds the consumer's `max_nack` threshold, the message
    /// is published to the DLQ stream and acked from the original.
    pub(super) dlq_nack_counts: HashMap<(u32, u64), u32, foldhash::fast::FixedState>,
    /// Shard data directory (for sidecar files like stream_lifecycle.bin).
    /// `None` when running in-memory (no `data_dir` configured).
    pub(super) data_path: Option<std::path::PathBuf>,
    /// True during replay — suppresses sidecar writes that would overwrite
    /// the persisted created_at_seq with wrong values computed from the
    /// already-populated store.
    pub(super) replay_mode: bool,

    /// H12: timestamp of the last `wheel_tick + idempotency tick` pass.
    /// The `tokio::select!` sleep arm can be starved when commands are
    /// arriving continuously; this field lets `dispatch_command` force
    /// a tick after every WHEEL_TICK_INTERVAL of wall time. The
    /// `Option` stays `None` until the first command runs, so the
    /// behaviour for a quiescent shard is unchanged.
    pub(super) last_wheel_tick: Option<Instant>,
    /// F16 — incremental eviction resume cursor. evict_expired walks the
    /// store at most `EVICT_WALK_CAP` entries per call and stores the
    /// next-to-scan seq here so the following call picks up from where
    /// it left off. Reset back to `0` when the walk completes or the
    /// store is rotated.
    pub(super) evict_resume_seq: u64,
    /// F16 — per-stream oldest known timestamp cache. If a stream's
    /// cached oldest_ts >= the cutoff, the eviction walk skips it
    /// entirely (no entries can be expired). Invalidated on truncation
    /// and when streams are deleted.
    pub(super) stream_oldest_ts: HashMap<StreamId, u64, foldhash::fast::FixedState>,
    /// Optional replication sender — shared with the `ShardRouter`. The
    /// router's `set_replication_tx` fills the `Option` inside the Mutex
    /// during cluster boot; the shard worker reads it lazily on the first
    /// flush that needs replication. The `Arc<Mutex<Option<Sender>>>` is
    /// the same instance stored in the router, so no manual propagation.
    #[cfg(feature = "cluster")]
    pub(super) replication_tx: std::sync::Arc<parking_lot::Mutex<Option<tokio::sync::mpsc::Sender<crate::cluster::replication::ReplicationBatch>>>>,
}

impl CommandWorker {
    /// Eviction interval — cold path, runs every 5 seconds.
    const EVICTION_INTERVAL: Duration = Duration::from_secs(5);
    /// Wheel tick interval — 1 second resolution.
    pub(super) const WHEEL_TICK_INTERVAL: Duration = Duration::from_secs(1);
    /// Number of wheel buckets (max delay in seconds). Exposed to handlers
    /// so M6 can clamp nack delay_ms without wrapping the bucket index.
    pub(super) const WHEEL_BUCKETS: usize = 120;

    /// Async command loop — runs as a `tokio::spawn` task.
    pub async fn run(mut self) {
        // Initialize eviction timer.
        self.next_eviction = Some(Instant::now() + Self::EVICTION_INTERVAL);

        loop {
            // Process any pending drain notifications first (non-blocking).
            self.drain_notifications();

            // H11: retry any ConsumerRemoved events the previous cycle
            // couldn't push because the drain-event ring was full. We
            // keep retrying until the ring has room — losing a
            // ConsumerRemoved leaks the consumer's subject inflight
            // slot inside the drain thread forever.
            if !self.pending_consumer_remove.is_empty() {
                let mut i = 0;
                while i < self.pending_consumer_remove.len() {
                    let cid = self.pending_consumer_remove[i];
                    if self
                        .drain_evt_tx
                        .try_send(crate::shard::drain_events::DrainEvent::ConsumerRemoved {
                            consumer_id: cid,
                        })
                        .is_ok()
                    {
                        self.pending_consumer_remove.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }
            }

            // Check if eviction is due.
            let eviction_sleep = self
                .next_eviction
                .map(|t| t.saturating_duration_since(Instant::now()))
                .unwrap_or(Self::EVICTION_INTERVAL);

            // Wheel tick sleep — only active when wheel exists.
            let wheel_sleep = self
                .next_wheel_tick
                .map(|t| t.saturating_duration_since(Instant::now()))
                .unwrap_or(Self::EVICTION_INTERVAL); // dormant if no wheel

            if self.accum_total > 0 {
                let timeout = self
                    .accum_deadline
                    .map(|d| d.saturating_duration_since(Instant::now()))
                    .unwrap_or(Duration::from_millis(self.flusher_config.interval_ms));

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
                    Ok(n) = self.notify_ring.recv_async_send() => {
                        self.handle_notification(n);
                    }
                    _ = tokio::time::sleep(timeout) => {
                        self.flush_accumulator();
                    }
                    _ = tokio::time::sleep(eviction_sleep) => {
                        self.evict_expired();
                        self.next_eviction = Some(Instant::now() + Self::EVICTION_INTERVAL);
                    }
                    _ = tokio::time::sleep(wheel_sleep), if self.wheel.is_some() || self.has_idempotency.load(std::sync::atomic::Ordering::Relaxed) => {
                        // Both timers run at the same 1-second cadence;
                        // one tokio::sleep drives both. Wheel tick is a
                        // no-op when wheel is None. Idempotency tick
                        // locks the shared Arc<Mutex<>>; the publish
                        // path also locks it on idempotent publishes,
                        // but contention is negligible (publish lock
                        // hold time = HashMap lookup, sub-microsecond).
                        self.wheel_tick();
                        // F26: tick every per-stream tracker. Outer
                        // read lock is uncontended in steady state;
                        // each per-stream Mutex is taken only briefly.
                        let guard = self.idempotency_tracker.read();
                        for tracker_arc in guard.values() {
                            tracker_arc.lock().tick();
                        }
                        drop(guard);
                        self.next_wheel_tick = Some(Instant::now() + Self::WHEEL_TICK_INTERVAL);
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
                    Ok(n) = self.notify_ring.recv_async_send() => {
                        self.handle_notification(n);
                    }
                    _ = tokio::time::sleep(eviction_sleep) => {
                        self.evict_expired();
                        self.next_eviction = Some(Instant::now() + Self::EVICTION_INTERVAL);
                    }
                    _ = tokio::time::sleep(wheel_sleep), if self.wheel.is_some() || self.has_idempotency.load(std::sync::atomic::Ordering::Relaxed) => {
                        // Both timers run at the same 1-second cadence;
                        // one tokio::sleep drives both. Wheel tick is a
                        // no-op when wheel is None. Idempotency tick
                        // locks the shared Arc<Mutex<>>; the publish
                        // path also locks it on idempotent publishes,
                        // but contention is negligible (publish lock
                        // hold time = HashMap lookup, sub-microsecond).
                        self.wheel_tick();
                        // F26: tick every per-stream tracker. Outer
                        // read lock is uncontended in steady state;
                        // each per-stream Mutex is taken only briefly.
                        let guard = self.idempotency_tracker.read();
                        for tracker_arc in guard.values() {
                            tracker_arc.lock().tick();
                        }
                        drop(guard);
                        self.next_wheel_tick = Some(Instant::now() + Self::WHEEL_TICK_INTERVAL);
                    }
                }
            }
        }
    }

    /// Process drain notifications (non-blocking batch drain).
    pub(super) fn drain_notifications(&mut self) {
        while let Some(n) = self.notify_ring.try_recv() {
            self.handle_notification(n);
        }
    }

    /// Handle a single drain notification.
    fn handle_notification(&mut self, notif: DrainNotification) {
        match notif {
            DrainNotification::Delivered {
                binding_id,
                consumer_id,
                queue_id,
                entries,
            } => {
                // Update engine's pending list for future ack/retire.
                use arbitro_engine_v2::command::Command;
                // A3/ROB-12 reconciliation: the drain incremented the shared
                // inflight once per delivered entry, but the engine SKIPS
                // re-adding a seq that is already pending on this binding
                // (redelivery after a cursor rewind — nack, resubscribe,
                // ack-timeout). Without correction those duplicates become
                // phantom inflight and the consumer permanently loses
                // capacity. Pre-scan the pending map with the exact check
                // the engine uses and reverse the blind increments below.
                // `dup_hashes` only allocates on an actual redelivery.
                let mut stream_id = StreamId(0);
                let mut dup_hashes: Vec<u32> = Vec::new();
                if let Some(b) = self.engine.ctx().catalog.binding(binding_id) {
                    stream_id = b.stream_id;
                    if !b.fire_and_forget {
                        for e in entries.iter() {
                            if b.pending.contains_key(&e.seq) {
                                dup_hashes.push(e.subject_hash);
                            }
                        }
                    }
                }
                let _ = self.engine.execute(&Command::Delivered {
                    stream_id,
                    binding_id,
                    entries: &entries,
                });

                if !dup_hashes.is_empty() {
                    self.counters.dec_inflight_bulk(
                        consumer_id.0,
                        queue_id.0,
                        dup_hashes.len() as u32,
                    );
                    // The drain also bumped the per-(consumer, subject)
                    // counter for each duplicate; send one Ack event per
                    // duplicate so the drain-owned `ConsumerSubjects` book
                    // reconciles in lock-step (same channel the real ack
                    // path uses in apply_delta_and_sync).
                    let ack_floor = self.ack_floors.floor(consumer_id.raw());
                    for &sh in &dup_hashes {
                        if self
                            .drain_evt_tx
                            .try_send(DrainEvent::Ack {
                                consumer_id,
                                subject_hash: sh,
                                ack_floor,
                            })
                            .is_err()
                        {
                            self.silent_drops.inc_drain_evt();
                        }
                    }
                    // Capacity was freed — wake the drain so a
                    // capacity-blocked cursor resumes promptly.
                    self.gate.release();
                }

                // Insert delivered entries into the ack-timeout wheel.
                self.wheel_insert_delivered(consumer_id, &entries);
            }
            DrainNotification::ConnectionDead(conn_id) => {
                let delta = self.engine.mark_connection_dead(conn_id);
                self.apply_delta_and_sync(&delta);
            }
        }
    }

    // ── Timing wheel ─────────────────────────────────────────────────────────

    /// Ensure the wheel is initialized. Called lazily on first need.
    pub(super) fn ensure_wheel(&mut self) {
        if self.wheel.is_none() {
            self.wheel = Some(arbitro_common::TimingWheel::new(Self::WHEEL_BUCKETS));
            self.next_wheel_tick = Some(Instant::now() + Self::WHEEL_TICK_INTERVAL);
        }
    }

    /// Insert delivered entries into the wheel for ack-timeout tracking.
    /// Only inserts if the consumer has `ack_wait_ms > 0`.
    fn wheel_insert_delivered(
        &mut self,
        consumer_id: ConsumerId,
        entries: &[arbitro_engine_v2::command::DeliveredEntry],
    ) {
        // Look up ack_wait_ms from the consumer info.
        let ack_wait_ms = self
            .engine
            .consumer(consumer_id)
            .map(|c| c.ack_wait_ms)
            .unwrap_or(0);
        if ack_wait_ms == 0 {
            return; // no timeout configured
        }

        self.ensure_wheel();
        let delay_ticks = (ack_wait_ms / 1000).max(1); // at least 1 tick

        let wheel = self.wheel.as_mut().unwrap();
        for entry in entries {
            // M5: explicit kind tag — no more "subject_hash == 0" hack.
            wheel.insert(
                arbitro_common::WheelEntry {
                    seq: entry.seq,
                    consumer_id: consumer_id.0,
                    subject_hash: entry.subject_hash,
                    kind: arbitro_common::WheelEntryKind::AckTimeout,
                },
                delay_ticks,
            );
        }
    }

    /// Advance the wheel by one tick. Process expired entries: verify
    /// still pending → auto-nack (cursor rewind + gate release).
    fn wheel_tick(&mut self) {
        let wheel = match self.wheel.as_mut() {
            Some(w) => w,
            None => return,
        };
        wheel.advance_into(&mut self.wheel_buf);
        if self.wheel_buf.is_empty() {
            return;
        }

        // Process expired entries.
        // Two kinds of entries hit the wheel:
        //   1. Ack-timeout: message still pending → nack + dec inflight + rewind.
        //   2. Nack-delay: message already nacked (not pending) → just rewind cursor.
        // For (1) "lazy cancel" means: if acked since insertion → skip entirely.
        // For (2) entry is never pending (already nacked) → always rewind.
        //
        // M5: explicit `entry.kind` tag distinguishes the two paths.
        let mut min_rewind: Option<u64> = None;
        let mut expired_count: u32 = 0;

        for entry in &self.wheel_buf {
            let consumer_id = ConsumerId(entry.consumer_id);
            let is_nack_delay = matches!(entry.kind, arbitro_common::WheelEntryKind::NackDelay,);

            if is_nack_delay {
                // Nack-delay: message was already nacked, just rewind cursor.
                min_rewind = Some(min_rewind.map_or(entry.seq, |m: u64| m.min(entry.seq)));
                expired_count += 1;
                continue;
            }

            // Ack-timeout path: check if entry is still pending.
            let still_pending = self
                .engine
                .ctx()
                .catalog
                .bindings_for_consumer(consumer_id)
                .iter()
                .any(|&bid| {
                    self.engine
                        .ctx()
                        .catalog
                        .binding(bid)
                        .map(|b| b.is_pending(entry.seq))
                        .unwrap_or(false)
                });

            if !still_pending {
                continue; // already acked — stale entry, lazy cancel
            }

            // Auto-nack: remove from pending, dec inflight, track rewind.
            use arbitro_engine_v2::command::{AckEntry, Command};
            let stream_id = self
                .engine
                .consumer(consumer_id)
                .map(|c| c.stream_id)
                .unwrap_or(StreamId(0));
            let ack_entry = AckEntry {
                stream_id,
                seq: entry.seq,
            };
            let _ = self.engine.execute(&Command::Nack {
                consumer_id,
                entries: &[ack_entry],
            });

            // Decrement atomic inflight.
            if let Some(consumer) = self.engine.consumer(consumer_id) {
                self.counters
                    .dec_inflight(consumer_id.0, consumer.queue_id.0);
            }

            // Track minimum seq for cursor rewind.
            min_rewind = Some(min_rewind.map_or(entry.seq, |m: u64| m.min(entry.seq)));
            expired_count += 1;
        }

        if expired_count > 0 {
            // Rewind cursor and wake drain for redelivery.
            if let Some(min_seq) = min_rewind {
                let cur = self.counters.cursor();
                self.counters.set_cursor(cur.min(min_seq.saturating_sub(1)));
                self.counters.clear_rewind();
            }
            self.gate.release();
        }
    }

    /// H12: run the wheel + idempotency tick if at least
    /// `WHEEL_TICK_INTERVAL` has elapsed since the last pass. Called
    /// after every dispatched command so a busy shard doesn't starve
    /// the timer arm of the `tokio::select!`. Idle shards still fall
    /// back to the sleep-driven branch in `run()`.
    fn maybe_tick_periodic(&mut self) {
        let now = Instant::now();
        let due = match self.last_wheel_tick {
            Some(last) => now.duration_since(last) >= Self::WHEEL_TICK_INTERVAL,
            None => true,
        };
        if !due {
            return;
        }
        if !(self.wheel.is_some()
            || self
                .has_idempotency
                .load(std::sync::atomic::Ordering::Relaxed))
        {
            self.last_wheel_tick = Some(now);
            return;
        }
        self.wheel_tick();
        let guard = self.idempotency_tracker.read();
        for tracker_arc in guard.values() {
            tracker_arc.lock().tick();
        }
        drop(guard);
        self.last_wheel_tick = Some(now);
        self.next_wheel_tick = Some(now + Self::WHEEL_TICK_INTERVAL);
    }

    /// Returns `true` if shutdown was requested.
    fn handle_or_shutdown(&mut self, cmd: ShardCommand) -> bool {
        if matches!(cmd, ShardCommand::Shutdown) {
            if crate::shard::drain::chaos_debug() {
                let info = self.store.lock().info();
                eprintln!(
                    "[SHUTDOWN-BEGIN] accum_total={} store_first={} store_last={} messages={}",
                    self.accum_total, info.first_seq, info.last_seq, info.messages
                );
            }
            self.flush_accumulator();
            if let Err(e) = self.store.lock().shutdown() {
                tracing::error!(error = ?e, "store shutdown failed");
            }
            if crate::shard::drain::chaos_debug() {
                let info = self.store.lock().info();
                eprintln!(
                    "[SHUTDOWN-DONE] store_first={} store_last={} messages={}",
                    info.first_seq, info.last_seq, info.messages
                );
            }
            self.running
                .store(false, std::sync::atomic::Ordering::Relaxed);
            self.gate.release();
            return true;
        }
        self.dispatch_command(cmd);
        // H12: keep the periodic tick deterministic under sustained load.
        self.maybe_tick_periodic();
        false
    }

    pub(super) fn check_accumulator_flush(&mut self) {
        if self.accum_total == 0 {
            return;
        }
        let force = self.accum_total >= self.flusher_config.max_size
            || self.accum_bytes >= self.flusher_config.max_bytes;
        let expired = self.accum_deadline.is_some_and(|d| Instant::now() >= d);
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
            ShardCommand::PurgeStream(cmd) => self.handle_purge_stream(cmd),
            ShardCommand::DrainSubject(cmd) => self.handle_drain_subject(cmd),
            ShardCommand::DeleteMessage(cmd) => self.handle_delete_message(cmd),
            ShardCommand::AckTerm(cmd) => self.handle_ack_term(cmd),
            ShardCommand::CreateConsumer(cmd) => self.handle_create_consumer(cmd),
            ShardCommand::DeleteConsumer(cmd) => self.handle_delete_consumer(cmd),
            ShardCommand::OpenConnection(cmd) => self.handle_open_connection(cmd),
            ShardCommand::DrainConnection(cmd) => self.handle_drain_connection(cmd),
            ShardCommand::Bind(cmd) => self.handle_bind(cmd),
            ShardCommand::ListStreams(cmd) => self.handle_list_streams(cmd),
            ShardCommand::ListConsumers(cmd) => self.handle_list_consumers(cmd),
            ShardCommand::StoreInfo(cmd) => self.handle_store_info(cmd),
            ShardCommand::ConsumerStates(cmd) => {
                let _ = cmd.reply.send(self.engine.consumer_states_snapshot());
            }
            ShardCommand::ConsumerPending(cmd) => {
                let count = self.engine.consumer_inflight(cmd.consumer_id) as u64;
                let _ = cmd.reply.send(count);
            }
            ShardCommand::PauseConsumer(cmd) => self.handle_pause_consumer(cmd),
            ShardCommand::ResumeConsumer(cmd) => self.handle_resume_consumer(cmd),
            ShardCommand::LoadStreamLifecycle => {
                self.load_stream_lifecycle();
                self.replay_mode = false;
                self.rebuild_and_swap_snapshot();
            }
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
        // Push subject-inflight decs to the drain via SPSC ring. Drain
        // owns the per-(consumer, subject) counters (`ConsumerSubjects`)
        // and applies these at the top of its next cycle. Ring overflow
        // is silently dropped — see `drain_events.rs` overflow policy.
        if !delta.subject_hashes_acked.is_empty() {
            for &(cid, sh) in &delta.subject_hashes_acked {
                if self
                    .drain_evt_tx
                    .try_send(DrainEvent::Ack {
                        consumer_id: ConsumerId(cid),
                        subject_hash: sh,
                        // Piggyback the contiguous-acked floor so the
                        // drain slot stays current (handle_ack records
                        // matched seqs BEFORE the engine execute whose
                        // delta lands here, so the value is fresh).
                        ack_floor: self.ack_floors.floor(cid),
                    })
                    .is_err()
                {
                    self.silent_drops.inc_drain_evt();
                }
            }
            // Wake drain so it processes the ring even if no new publishes
            // arrive. Multiple releases coalesce via `fetch_or`.
            self.gate.release();
        }
        // Demand atomics are already updated by subscribe/unsubscribe handlers.
        // DeltaEvents demand_became_available/idle are informational only here.
        // BUG2: in-flight (delivered-but-unacked) seqs released by a retired
        // binding must be redelivered. Rewind the drain cursor to the lowest
        // released seq NOW — the drain owns redelivery and DeliverPolicy is
        // re-evaluated at dispatch, so there's no reason to defer to the next
        // bind. signal_rewind guards a mid-flight drain cycle from clobbering
        // the cursor at cycle end (same protocol as handle_bind).
        if !delta.pending_seqs_released.is_empty() {
            if let Some(&min_seq) = delta.pending_seqs_released.iter().min() {
                rewind_released(&self.counters, min_seq);
                self.gate.release();
            }
        }
        // Consumer entities removed (explicit DeleteConsumer or a
        // delete_stream cascade): drop the command-side ack-floor slot
        // AND the drain-side `ConsumerSubjects` slot. Consumer ids are
        // pool-recycled — a stale contiguous-acked floor inherited by a
        // recycled id would silently skip delivery for the new consumer
        // (message loss), so this cleanup is correctness-critical on
        // every removal path (handle_delete_consumer only covers the
        // explicit one). H11 retry on ring-full, same as the handler.
        for &cid in &delta.consumers_removed {
            self.ack_floors.remove(cid.raw());
            if self
                .drain_evt_tx
                .try_send(DrainEvent::ConsumerRemoved { consumer_id: cid })
                .is_err()
            {
                self.silent_drops.inc_drain_evt();
                self.pending_consumer_remove.push(cid);
            }
        }
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
                ack_wait_ms: b.ack_wait_ms,
                write_tx: b.write_tx.clone(),
                write_failed: std::sync::Arc::clone(&b.write_failed),
            })
            .collect();

        // Build per-connection writer index. Dedup by connection_id —
        // multiple consumers on the same conn share a single writer.
        // HashMap+foldhash: connection_id is unbounded-monotonic, direct
        // Vec<Option<T>> would leak memory, and HashMap beats binary_search.
        let mut writers_by_conn: std::collections::HashMap<
            u64,
            crate::shard::shared::WriterIndexEntry,
            foldhash::fast::FixedState,
        > = std::collections::HashMap::with_capacity_and_hasher(
            self.bindings.len(),
            foldhash::fast::FixedState::default(),
        );
        for b in &self.bindings {
            writers_by_conn.entry(b.connection_id.0).or_insert_with(|| {
                crate::shard::shared::WriterIndexEntry {
                    write_tx: b.write_tx.clone(),
                    write_failed: std::sync::Arc::clone(&b.write_failed),
                }
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
                mt.set_binding_idx_for(b.consumer_id, b.connection_id, i as u32);
            }
        }

        // Build per-stream max_age_ms vec, indexed by StreamId.raw().
        // Drain looks up by stream_id.raw() — O(1) array access.
        let max_stream_idx = self
            .stream_retention
            .keys()
            .map(|s| s.0 as usize)
            .max()
            .unwrap_or(0);
        let mut stream_max_age_ms = vec![0u64; max_stream_idx + 1];
        for (sid, r) in &self.stream_retention {
            if let Some(slot) = stream_max_age_ms.get_mut(sid.0 as usize) {
                *slot = r.max_age_ms;
            }
        }

        // Build per-stream created_at_seq vec, indexed by StreamId.raw().
        // Drain skips entries with seq < created_at_seq for the stream.
        let mut stream_created_at_seq = vec![0u64; max_stream_idx + 1];
        for (sid, r) in &self.stream_retention {
            if let Some(slot) = stream_created_at_seq.get_mut(sid.0 as usize) {
                *slot = r.created_at_seq;
            }
        }

        self.snapshot.store(DrainSnapshot {
            // F19: wrap once into an Arc<[T]> so the drain thread sees
            // an immutable cheaply-cloneable slice instead of a Vec.
            bindings: Arc::from(snap_bindings.into_boxed_slice()),
            writers_by_conn,
            match_tables,
            stream_max_age_ms,
            stream_created_at_seq,
        });
    }

    // ── Stream lifecycle sidecar persistence ────────────────────────────

    /// Save stream lifecycle data (created_at_seq per stream) to a sidecar
    /// file. Format: repeated `[stream_id: u32 LE][created_at_seq: u64 LE]`
    /// (12 bytes per entry). Called after create/delete stream.
    pub(super) fn save_stream_lifecycle(&self) {
        let Some(ref dir) = self.data_path else { return };
        let path = dir.join("stream_lifecycle.bin");
        let mut buf = Vec::with_capacity(self.stream_retention.len() * 12);
        for (sid, r) in &self.stream_retention {
            buf.extend_from_slice(&sid.0.to_le_bytes());
            buf.extend_from_slice(&r.created_at_seq.to_le_bytes());
        }
        if let Err(e) = std::fs::write(&path, &buf) {
            tracing::warn!(error = %e, "failed to save stream_lifecycle sidecar");
        }
    }

    /// Load stream lifecycle data from the sidecar file and patch
    /// `stream_retention` entries with the persisted `created_at_seq`.
    /// Must be called AFTER command log replay has populated stream_retention.
    pub(super) fn load_stream_lifecycle(&mut self) {
        let Some(ref dir) = self.data_path else { return };
        let path = dir.join("stream_lifecycle.bin");
        let Ok(bytes) = std::fs::read(&path) else { return };
        if bytes.len() % 12 != 0 {
            return;
        }
        for chunk in bytes.chunks_exact(12) {
            let sid = StreamId(u32::from_le_bytes(chunk[0..4].try_into().unwrap()));
            let created_at_seq = u64::from_le_bytes(chunk[4..12].try_into().unwrap());
            if let Some(r) = self.stream_retention.get_mut(&sid) {
                r.created_at_seq = created_at_seq;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// BUG2 — retiring a binding with pending (in-flight, unacked) seqs
    /// must rewind the drain cursor so those seqs get redelivered. Before
    /// the fix, `min(pending_seqs_released)` was recorded into a write-only
    /// `deferred_rewind_seq` field that nothing ever read → the seqs were
    /// lost. `rewind_released` now moves the cursor back AND signals the
    /// rewind so a mid-flight drain can't clobber it.
    #[test]
    fn released_pending_seqs_rewind_cursor_and_signal() {
        let counters = SharedCounters::new();
        counters.set_cursor(100);

        // A dead binding released pending seqs; the lowest was 40.
        rewind_released(&counters, 40);

        assert_eq!(counters.cursor(), 39, "cursor rewound to min_seq - 1");
        // Durable signal so a mid-flight drain honours the rewind at the
        // top of its next cycle instead of the clobbered cursor.
        assert_eq!(counters.take_rewind(), Some(40));
    }

    /// `rewind_released` never drags the cursor forward — if the cursor is
    /// already behind the released seq, only the (durable) rewind signal is
    /// posted so the drain's `min`-based `take_rewind` stays authoritative.
    #[test]
    fn released_pending_seqs_never_advance_cursor() {
        let counters = SharedCounters::new();
        counters.set_cursor(10);

        rewind_released(&counters, 40); // min_seq - 1 == 39 > 10

        assert_eq!(counters.cursor(), 10, "cursor must not move forward");
        assert_eq!(counters.take_rewind(), Some(40));
    }

    /// seq 0 is the "no messages" sentinel — `rewind_released` is a no-op.
    #[test]
    fn released_seq_zero_is_noop() {
        let counters = SharedCounters::new();
        counters.set_cursor(5);
        rewind_released(&counters, 0);
        assert_eq!(counters.cursor(), 5);
        assert_eq!(counters.take_rewind(), None);
    }
}
