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
use crate::shard::drain_events::{DrainEvent, DrainEventRing};
use crate::shard::router::SharedStore;
use crate::shard::shared::{DrainNotification, DrainSnapshot, NotifyRing, SharedCounters, SnapshotSwap};
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
    /// SPSC Ring — drain is the sole producer, command task is the sole consumer.
    pub(super) notify_ring: Arc<NotifyRing>,
    /// Drain-event ring: command → drain (ack-driven subject inflight decs).
    /// SPSC — drain is the sole consumer, command task is the sole producer.
    /// Drained at the top of every drain cycle via non-blocking `try_recv`.
    pub(super) drain_evt_rx: Arc<DrainEventRing>,
    /// Per-consumer subject inflight, indexed by `ConsumerId.raw()`. Slot
    /// is lazily allocated on first inc; reset to `None` on
    /// `DrainEvent::ConsumerRemoved`. Single-thread owned by drain — no
    /// locks, no atomics. Replaces `SharedCounters.subject` (papaya).
    pub(super) consumer_subjects: Vec<Option<ConsumerSubjects>>,
}

impl DrainWorker {
    /// Pure drain loop — nothing else runs on this thread.
    pub fn run(mut self) {
        self.gate.set_worker(std::thread::current());

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
            self.gate.acquire();
            crate::lifecycle_trace!("19_2_gate_acquired", 0, 0, "shard");

            if !self.running.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }

            while self.gate.is_open() {
                crate::lifecycle_trace!("20_gate_open_detected", 0, 0, "shard");

                // Drain the command→drain event ring before deciding any
                // delivery. This applies acks (subject inflight decs) and
                // consumer removals so the upcoming dispatch sees current
                // per-consumer state.
                drain_event_ring(&self.drain_evt_rx, &mut self.consumer_subjects);

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
                match read_result {
                    Some(result) => super::drain::drain_deliver(
                        &self.counters,
                        &snap,
                        &self.gate,
                        &self.names,
                        &mut self.drain_scratch,
                        &mut self.consumer_subjects,
                        &self.notify_ring,
                        result,
                    ),
                    None => {
                        self.gate.lock();
                        crate::lifecycle_trace!("33_drainer_exit_locked", 0, 0, "shard");
                    }
                }

                let stalled = self.counters.cursor() == prev_cursor;

                // Backpressure: cursor didn't advance → downstream full.
                if stalled && self.gate.is_open() {
                    std::thread::park_timeout(std::time::Duration::from_micros(50));
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
    rx: &DrainEventRing,
    consumer_subjects: &mut Vec<Option<ConsumerSubjects>>,
) {
    while let Some(evt) = rx.try_recv() {
        match evt {
            DrainEvent::Ack { consumer_id, subject_hash } => {
                let idx = consumer_id.raw() as usize;
                if let Some(Some(cs)) = consumer_subjects.get_mut(idx) {
                    cs.dec(subject_hash);
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

/// Mutable accessor for a consumer's subject inflight, creating the slot
/// on demand. Slot index = `ConsumerId.raw() as usize`.
#[inline]
pub(in crate::shard) fn consumer_subjects_slot_mut<'a>(
    consumer_subjects: &'a mut Vec<Option<ConsumerSubjects>>,
    consumer_id: u32,
) -> &'a mut ConsumerSubjects {
    let idx = consumer_id as usize;
    if idx >= consumer_subjects.len() {
        consumer_subjects.resize_with(idx + 1, || None);
    }
    consumer_subjects[idx].get_or_insert_with(ConsumerSubjects::new)
}

/// Read-only accessor. Returns `None` if the consumer has no tracked
/// subjects yet — caller treats that as "0 inflight" (always has room).
#[inline]
pub(in crate::shard) fn consumer_subjects_slot<'a>(
    consumer_subjects: &'a [Option<ConsumerSubjects>],
    consumer_id: u32,
) -> Option<&'a ConsumerSubjects> {
    consumer_subjects.get(consumer_id as usize).and_then(|s| s.as_ref())
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
    /// SPSC Ring shared with DrainWorker — command task is the sole consumer.
    pub(super) notify_ring: Arc<NotifyRing>,
    /// Drain-event ring shared with DrainWorker — command task is the
    /// sole producer, drain thread is the sole consumer. Used to push
    /// ack-driven subject inflight decrements + consumer cleanup events.
    pub(super) drain_evt_tx: Arc<DrainEventRing>,
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
}

impl CommandWorker {
    /// Eviction interval — cold path, runs every 5 seconds.
    const EVICTION_INTERVAL: Duration = Duration::from_secs(5);
    /// Wheel tick interval — 1 second resolution.
    const WHEEL_TICK_INTERVAL: Duration = Duration::from_secs(1);
    /// Number of wheel buckets (max delay in seconds).
    const WHEEL_BUCKETS: usize = 120;

    /// Async command loop — runs as a `tokio::spawn` task.
    pub async fn run(mut self) {
        // Initialize eviction timer.
        self.next_eviction = Some(Instant::now() + Self::EVICTION_INTERVAL);

        loop {
            // Process any pending drain notifications first (non-blocking).
            self.drain_notifications();

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
                    n = self.notify_ring.recv_async_send() => {
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
                        if let Some(t) = self
                            .idempotency_tracker
                            .lock()
                            .expect("idempotency mutex poisoned")
                            .as_mut()
                        {
                            t.tick();
                        }
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
                    n = self.notify_ring.recv_async_send() => {
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
                        if let Some(t) = self
                            .idempotency_tracker
                            .lock()
                            .expect("idempotency mutex poisoned")
                            .as_mut()
                        {
                            t.tick();
                        }
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
            // subject_hash != 0 signals ack-timeout entry to wheel_tick.
            // Ensure it's never 0 (extremely rare: only if FNV-1a produces 0).
            let hash = if entry.subject_hash == 0 { 1 } else { entry.subject_hash };
            wheel.insert(
                arbitro_common::WheelEntry {
                    seq: entry.seq,
                    consumer_id: consumer_id.0,
                    subject_hash: hash,
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
        // Distinguish: subject_hash == 0 ⇒ nack-delay entry (guaranteed not
        // pending, just needs cursor rewind). subject_hash != 0 ⇒ ack-timeout.
        let mut min_rewind: Option<u64> = None;
        let mut expired_count: u32 = 0;

        for entry in &self.wheel_buf {
            let consumer_id = ConsumerId(entry.consumer_id);
            let is_nack_delay = entry.subject_hash == 0;

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
                        .map(|b| b.pending.iter().any(|p| p.seq == entry.seq))
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

    /// Returns `true` if shutdown was requested.
    fn handle_or_shutdown(&mut self, cmd: ShardCommand) -> bool {
        if matches!(cmd, ShardCommand::Shutdown) {
            self.flush_accumulator();
            if let Err(e) = self.store.lock().shutdown() {
                tracing::error!(error = ?e, "store shutdown failed");
            }
            self.running
                .store(false, std::sync::atomic::Ordering::Relaxed);
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
            ShardCommand::PurgeStream(cmd)  => self.handle_purge_stream(cmd),
            ShardCommand::DrainSubject(cmd) => self.handle_drain_subject(cmd),
            ShardCommand::CreateConsumer(cmd) => self.handle_create_consumer(cmd),
            ShardCommand::DeleteConsumer(cmd) => self.handle_delete_consumer(cmd),
            ShardCommand::OpenConnection(cmd) => self.handle_open_connection(cmd),
            ShardCommand::DrainConnection(cmd) => self.handle_drain_connection(cmd),
            ShardCommand::Bind(cmd) => self.handle_bind(cmd),
            ShardCommand::ListStreams(cmd) => self.handle_list_streams(cmd),
            ShardCommand::ListConsumers(cmd) => self.handle_list_consumers(cmd),
            ShardCommand::StoreInfo(cmd) => self.handle_store_info(cmd),
            ShardCommand::Metrics(cmd) => {
                let _ = cmd.reply.send(self.engine.metrics_snapshot());
            }
            ShardCommand::ConsumerStates(cmd) => {
                let _ = cmd.reply.send(self.engine.consumer_states_snapshot());
            }
            ShardCommand::ConsumerPending(cmd) => {
                let count = self.engine.consumer_inflight(cmd.consumer_id) as u64;
                let _ = cmd.reply.send(count);
            }
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
        // Push subject-inflight decs to the drain via SPSC ring. Drain
        // owns the per-(consumer, subject) counters (`ConsumerSubjects`)
        // and applies these at the top of its next cycle. Ring overflow
        // is silently dropped — see `drain_events.rs` overflow policy.
        if !delta.subject_hashes_acked.is_empty() {
            for &(cid, sh) in &delta.subject_hashes_acked {
                let _ = self.drain_evt_tx.try_send(DrainEvent::Ack {
                    consumer_id: ConsumerId(cid),
                    subject_hash: sh,
                });
            }
            // Wake drain so it processes the ring even if no new publishes
            // arrive. Multiple releases coalesce via `fetch_or`.
            self.gate.release();
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
                ack_wait_ms: b.ack_wait_ms,
                write_tx: b.write_tx.clone(),
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
        let max_stream_idx = self.stream_retention.keys()
            .map(|s| s.0 as usize)
            .max()
            .unwrap_or(0);
        let mut stream_max_age_ms = vec![0u64; max_stream_idx + 1];
        for (sid, r) in &self.stream_retention {
            if let Some(slot) = stream_max_age_ms.get_mut(sid.0 as usize) {
                *slot = r.max_age_ms;
            }
        }

        self.snapshot.store(DrainSnapshot {
            bindings: snap_bindings,
            writers_by_conn,
            match_tables,
            stream_max_age_ms,
        });
    }
}
