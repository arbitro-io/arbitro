//! Shard workers — drain thread + command thread, **zero Mutex**.
//!
//! **Drain thread** (`drain-N`) — pure dedicated loop:
//! ```text
//! loop {
//!     gate.acquire();
//!     while gate.is_open() { drain_read(); drain_deliver(); }
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
use crate::shard::drain_probe::{DrainProbe, ParkVerdict, ReadVerdict, RewindApplied};
use crate::shard::router::SharedStore;
use crate::shard::shared::{DrainNotification, DrainSnapshot, SharedCounters, SnapshotSwap};
use crate::transport::ConnectionRegistry;

// ── Per-stream retention config ──────────────────────────────────────────────

/// Retention limits stored in the command worker and propagated into
/// `DrainSnapshot` for zero-copy access by the drain thread.
#[derive(Clone, Copy, Default)]
pub(super) struct StreamRetention {
    /// Age-based eviction threshold in milliseconds (0 = disabled).
    ///
    /// Size limits (`max_msgs` / `max_bytes`) are NOT here: they are
    /// enforced as a pre-append quota rejection in `dispatch_v2`, against
    /// the engine's own quota. This struct once carried them for a
    /// FIFO-eviction path (`DiscardPolicy::Old` — drop oldest to make
    /// room) that lived only in the removed publish accumulator and was
    /// therefore never reachable. Re-adding them means implementing that
    /// eviction, not just restoring the fields.
    pub max_age_ms: u64,
    /// Global journal seq at which this stream incarnation was created.
    /// Drain skips entries with seq < created_at_seq for this stream_id.
    /// 0 = no filter (backward compat for streams created before this feature).
    pub created_at_seq: u64,
}

// ── Cross-handler private types ──────────────────────────────────────────────

/// A bound consumer↔connection pair for delivery.
///
/// Created by `handle_subscribe` / `handle_bind`, iterated by the
/// drain cycle (`drain::drain_read`), filtered on unsubscribe / delete.
pub struct ActiveBinding {
    pub(super) binding_id: BindingId,
    pub(super) connection_id: ConnectionId,
    pub(super) consumer_id: ConsumerId,
    /// One binding is one subscription — the key the match table stamps by.
    pub(super) subscription_id: SubscriptionId,
    /// Dense index of this binding's `(connection, consumer)` pair — the
    /// fanout-collapse key. Bindings that share a pair share an index, so
    /// the drain marks one slot per message instead of scanning a list of
    /// pairs. Assigned by `rebuild_and_swap_snapshot`; snapshot-local.
    pub(super) group_idx: u32,
    /// The id the client chose. Stamped on every delivery so the client can
    /// route by a number it recognises.
    pub(super) external_sub_id: u32,
    pub(super) stream_id: StreamId,
    pub(super) queue_id: QueueId,
    /// Configured `max_inflight` cached at subscribe time.
    pub(super) max_inflight: u32,
    /// `AckPolicy::None` — skip inflight tracking and capacity checks.
    pub(super) fire_and_forget: bool,
    /// Ack deadline in milliseconds. 0 = no timeout (no wheel entry).
    pub(super) ack_wait_ms: u32,
    /// Deliver floor: entries with `seq <= deliver_floor` are never
    /// delivered on this binding (DeliverPolicy::New = journal tail at
    /// consumer creation; ByStartSeq = start_seq - 1; All = 0). Rides in
    /// the DrainSnapshot so the drain sees the binding and its floor as
    /// one atomic unit — no ring event, no ordering hazard.
    pub(super) deliver_floor: u64,
    /// Sender to the per-connection async writer task. `try_send` is
    /// non-blocking — no `block_in_place`, no write lock, no runtime handle.
    pub(super) write_tx: tokio::sync::mpsc::Sender<bytes::Bytes>,
    /// **M8**: writer feedback — `true` when the writer task has hit an
    /// I/O error. Shared with the writer task via Arc.
    pub(super) write_failed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

// ── Drain worker ────────────────────────────────────────────────────────────

/// Pure drain thread — gate.acquire → drain_read/drain_deliver → loop.
/// Nothing else runs here. No commands, no engine, no Mutex.
///
/// Reads atomics (`SharedCounters`) and snapshots (`SnapshotSwap`)
/// for all decisions. After delivery, increments atomic inflight and
/// pushes notifications to the command thread via lock-free channel.
pub struct DrainWorker {
    /// Diagnostics only: every drain logs under the same `"shard"` thread
    /// label, so without this the interleaved trace cannot be split per shard.
    /// Read solely by `lifecycle_trace!`, which compiles to nothing when its
    /// feature is off — hence unread in default builds.
    #[cfg_attr(not(feature = "lifecycle_trace"), allow(dead_code))]
    pub(super) shard_id: u32,
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
    /// `NotifyWaiter`). Generic over the observability probe, chosen once
    /// at spawn (router.rs); `ProbeOff` compiles every probe call away.
    pub(in crate::shard) async fn run<P: DrainProbe>(mut self, mut probe: P) {
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

        // Previous cycle's snapshot — held so the `Arc::ptr_eq` compare is
        // ABA-safe; gates the scratch-cache clear in `drain_read`.
        let mut prev_snap: Option<Arc<DrainSnapshot>> = None;

        loop {
            crate::lifecycle_trace!("19_1_gate_waiting", self.shard_id as u64, 0, "shard");
            self.gate.acquire().await;
            crate::lifecycle_trace!("19_2_gate_acquired", self.shard_id as u64, 0, "shard");

            if !self.running.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }

            loop {
                // Shutdown liveness: `more_pending` can re-open the gate
                // every cycle (capacity-blocked entry), so `acquire()` may
                // never park again — this load is the guaranteed exit.
                if !self.running.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }

                crate::lifecycle_trace!("20_gate_open_detected", self.shard_id as u64, 0, "shard");

                // INVARIANT: clear the gate at the TOP, before any store
                // read. A `release()` wiped by this clear appended its entry
                // first and the store is read after, so the walk sees it; a
                // later `release()` re-opens. `drain_deliver` only re-opens.
                self.gate.lock();

                // INVARIANT: rewind BEFORE the event ring — see `apply_rewind`.
                let rewind = apply_rewind(&self.counters);
                drain_event_ring(&mut self.drain_evt_rx, &mut self.consumer_subjects);
                probe.cycle_start(&self.counters, rewind);

                let now_ms = cycle_now_ms(&self.drain_config);

                // Snapshot load — Arc clone (~3ns), no lock on engine.
                let snap = self.snapshot.load();
                let snap_changed = prev_snap.as_ref().is_none_or(|p| !Arc::ptr_eq(p, &snap));
                let prev_cursor = self.counters.cursor();

                // Phase split: store lock held for the walk only; delivery
                // and bookkeeping run lock-free.
                let verdict = {
                    let store_guard = self.store.lock();
                    super::drain::drain_read(
                        &self.counters,
                        &snap,
                        &**store_guard,
                        &self.drain_config,
                        &mut self.drain_scratch,
                        &mut self.consumer_subjects,
                        now_ms,
                        snap_changed,
                    )
                };
                probe.read_verdict(verdict);
                // `drain_deliver` re-opens the gate iff more work remains;
                // non-`Fed` verdicts leave it cleared by the top-of-loop lock.
                if let ReadVerdict::Fed(result) = verdict {
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
                        &mut probe,
                    );
                }
                prev_snap = Some(snap);

                let stalled = self.counters.cursor() == prev_cursor;

                // Backpressure: work remains but the cursor didn't advance →
                // downstream writer full. Yield so it can drain. (First gate
                // read; the park decision below takes its own.)
                if stalled && self.gate.is_open() {
                    tokio::time::sleep(std::time::Duration::from_micros(50)).await;
                }

                // INVARIANT: `park_verdict` is a SECOND, separate gate load,
                // after the possible sleep — a concurrent `release()` during
                // it must be observed. Never merge with the read above.
                let park = park_verdict(&self.gate);
                probe.park(&self.counters, park, stalled);
                if park == ParkVerdict::Park {
                    crate::lifecycle_trace!(
                        "33_drainer_exit_locked",
                        self.shard_id as u64,
                        0,
                        "shard"
                    );
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
                seq,
                op,
            } => {
                let idx = consumer_id.raw() as usize;
                if let Some(Some(cs)) = consumer_subjects.get_mut(idx) {
                    cs.dec(subject_hash);
                    // Raise first: an in-order ack is absorbed by the
                    // floor and never touches the suppression set.
                    cs.raise_ack_floor(ack_floor);
                    match op {
                        crate::shard::drain_events::SuppressOp::Acked => cs.suppress(seq),
                        crate::shard::drain_events::SuppressOp::Released => cs.release(seq),
                        crate::shard::drain_events::SuppressOp::None => {}
                    }
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

/// Consume the rewind signal and move the cursor back. MUST run before
/// draining the event ring: `signal_rewind` (Release) is sequenced after
/// the ring push and `take_rewind` is Acquire, so a visible signal
/// implies its `Released` events are already in the ring.
#[inline]
fn apply_rewind(counters: &SharedCounters) -> RewindApplied {
    match counters.take_rewind() {
        None => RewindApplied::None,
        Some(rw) => {
            let cur = counters.cursor();
            if rw > 0 && rw - 1 < cur {
                counters.set_cursor(rw - 1);
                RewindApplied::Applied { to: rw - 1 }
            } else {
                RewindApplied::AlreadyBehind { signal: rw }
            }
        }
    }
}

/// Wall clock for TTL filtering — taken only when max-age is armed
/// (0 on the server path, router.rs).
#[inline]
fn cycle_now_ms(cfg: &super::drain::DrainConfig) -> u64 {
    if cfg.max_age_ms > 0 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    } else {
        0
    }
}

/// The park decision — one dedicated gate load, SEPARATE from the
/// pre-sleep backpressure read. Collapsing the two reads reintroduces a
/// lost wake: a `release()` during the 50µs sleep must be observed here.
#[inline]
fn park_verdict(gate: &Gate) -> ParkVerdict {
    if gate.is_open() {
        ParkVerdict::Continue
    } else {
        ParkVerdict::Park
    }
}

/// BUG2: rewind the drain cursor so seqs released by a retired binding get
/// redelivered. Mirrors `handle_bind`'s protocol: move the cursor back for
/// the common case (drain sleeping) AND `signal_rewind` so a mid-flight
/// drain cycle can't clobber the cursor when it writes `new_cursor` at the
/// end of its cycle. `signal_rewind` takes the min, so a smaller pending
/// rewind (more replay) still wins — redelivery is safe, message loss is not.
#[inline]
pub(in crate::shard) fn rewind_released(counters: &SharedCounters, min_seq: u64) {
    if min_seq == 0 {
        return;
    }
    let cur = counters.cursor();
    if min_seq - 1 < cur {
        counters.set_cursor(min_seq - 1);
    }
    counters.signal_rewind(min_seq);
}

/// Audit #10: drop every per-(consumer, seq) DLQ nack counter belonging to
/// `consumer_id`. Entries are removed on ack and on reaching the max_nack
/// threshold, but a consumer deleted mid-flight left its counters behind
/// forever — unbounded growth under nack-heavy churn when `max_nack > 0`.
/// Called from `handle_delete_consumer`. Cold path.
#[inline]
pub(in crate::shard) fn clear_consumer_nack_counts(
    counts: &mut HashMap<(u32, u64), u32, foldhash::fast::FixedState>,
    consumer_id: u32,
) {
    counts.retain(|(cid, _), _| *cid != consumer_id);
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
    pub(super) wheel:
        Option<arbitro_common::HierarchicalTimingWheel<arbitro_common::WheelEntry>>,
    /// Scratch buffer reused across wheel ticks to avoid allocation.
    pub(super) wheel_buf: Vec<arbitro_common::WheelEntry>,
    /// When the timer arm next runs, `epoch`-relative. `None` disables
    /// the arm: driven by deadlines rather than a tick, an idle shard
    /// arms nothing.
    pub(super) next_timer_ms: Option<u64>,
    /// Origin for every millisecond the worker deals in.
    pub(super) epoch: Instant,
    /// When the idempotency trackers were last advanced. The difference
    /// against now is what they get, so their windows follow wall time
    /// rather than a count of wakeups.
    pub(super) last_idempotency_ms: u64,
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
    /// H10: shared silent-drop counters.
    pub(super) silent_drops: Arc<crate::common::SilentDrops>,
    /// H11: ConsumerRemoved events that lost a `try_send` to the drain
    /// ring. Drained at the top of every command loop iteration so a
    /// transient ring-full doesn't leak the per-consumer subject slot.
    pub(super) pending_consumer_remove: Vec<ConsumerId>,
    /// `DrainEvent::Ack`s that lost a `try_send` because the drain-event
    /// ring was full — reachable deterministically by a bulk nack or a
    /// connection death with more than `DRAIN_EVENT_CAP` pendings while
    /// the drain is parked (`apply_delta_and_sync` pushes the whole
    /// delta before `gate.release()`, so nothing drains mid-loop).
    /// Retried at the top of every command-loop iteration, same pattern
    /// as `pending_consumer_remove`. These events are correctness-
    /// critical, NOT droppable: a lost `Released` leaves its seq in the
    /// drain's suppression set forever — the contiguous floor can never
    /// rise past it (it is unacked) and resubscribe replays from above
    /// it — permanent starvation of that message for that consumer. A
    /// lost `Acked` leaks a subject-inflight credit. Entries for a
    /// consumer are purged when its `ConsumerRemoved` is emitted, so a
    /// stale event can never be applied to a pool-recycled consumer id.
    pub(super) pending_drain_acks: Vec<crate::shard::drain_events::DrainEvent>,
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
    pub(super) replication_tx: std::sync::Arc<
        parking_lot::Mutex<
            Option<tokio::sync::mpsc::Sender<crate::cluster::replication::ReplicationBatch>>,
        >,
    >,
}

impl CommandWorker {
    /// Eviction interval — cold path, runs every 5 seconds.
    const EVICTION_INTERVAL: Duration = Duration::from_secs(5);
    /// Base resolution of the ack-timeout / nack-delay wheel: how late a
    /// deadline can fire, never how early. The hierarchical wheel arms no
    /// wakeups while idle, so this is chosen for the precision a delay
    /// deserves, not for what the shard can afford to wake for.
    pub(super) const WHEEL_TICK_MS: u64 = 100;

    /// How often dedup windows are retired. Coarser than the wheel on
    /// purpose: a window is minutes long, the tracker buckets in seconds,
    /// and there is one tracker per stream.
    const IDEMPOTENCY_INTERVAL_MS: u64 = 1_000;

    /// Async command loop — runs as a `tokio::spawn` task.
    pub async fn run(mut self) {
        // Initialize eviction timer.
        self.next_eviction = Some(Instant::now() + Self::EVICTION_INTERVAL);

        loop {
            // Process any pending drain notifications first (non-blocking).
            self.drain_notifications();

            // Retry `DrainEvent::Ack`s that lost a `try_send` because the
            // drain-event ring was full (bulk nack / connection death with
            // > DRAIN_EVENT_CAP pendings). Retained until the ring accepts
            // them — a lost `Released` permanently starves its seq (see
            // the `pending_drain_acks` field doc). For every re-sent
            // `Released` the cursor rewind is re-signalled: the rewind
            // signalled alongside the original (overflowed) push may have
            // been consumed by a drain cycle that ran before the event
            // reached the ring — that walk skipped the still-suppressed
            // seq without `track_skipped`, so without a fresh rewind the
            // cursor never comes back to it. Retry order within the queue
            // is irrelevant: `dec` is commutative, the floor is a
            // monotonic max, and per-seq suppress/release inversions are
            // impossible while an event is queued (an `Acked` for a seq
            // requires a redelivery, which requires its `Released` to have
            // been applied first).
            if !self.pending_drain_acks.is_empty() {
                let mut sent_any = false;
                let mut min_released: Option<u64> = None;
                while let Some(&evt) = self.pending_drain_acks.first() {
                    if self.drain_evt_tx.try_send(evt).is_err() {
                        // Ring full again — keep the rest for the next pass.
                        break;
                    }
                    if let crate::shard::drain_events::DrainEvent::Ack {
                        seq,
                        op: crate::shard::drain_events::SuppressOp::Released,
                        ..
                    } = evt
                    {
                        min_released = Some(min_released.map_or(seq, |m| m.min(seq)));
                    }
                    sent_any = true;
                    self.pending_drain_acks.swap_remove(0);
                }
                if let Some(min_seq) = min_released {
                    rewind_released(&self.counters, min_seq);
                }
                if sent_any {
                    self.gate.release();
                }
            }

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

            // Every pass, not just where timers are inserted: the
            // idempotency trackers are allocated lazily by the publish
            // path, which runs elsewhere and cannot re-arm anything.
            // A few bit ops and one relaxed load.
            self.rearm_timer();

            // The wheel's own next deadline, not a tick.
            let timer_sleep = self
                .next_timer_ms
                .map(|due| Duration::from_millis(due.saturating_sub(self.now_ms())))
                .unwrap_or(Self::EVICTION_INTERVAL);

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
                _ = tokio::time::sleep(timer_sleep), if self.next_timer_ms.is_some() => {
                    // One sleep, two wheels, no shared cadence: the
                    // deadline is whichever comes first and each gets
                    // the wall time that actually elapsed.
                    self.run_timers();
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
                // Retired-binding reversal: the binding died (connection
                // death, unsubscribe, delete-consumer/stream cascade)
                // BETWEEN the drain's flush and this notification being
                // applied. At flush time the drain already recorded the
                // delivery — `inc_inflight` on the shared counters,
                // `cs.inc(subject)` + `cs.suppress(seq)` in its own book —
                // but the engine never registered a pending for these seqs
                // (`Command::Delivered` no-ops on an unknown binding), so
                // the retirement walk over `binding.pending` could not have
                // released them, and no ack / nack / ack-timeout ever will
                // (nothing matches a pending that does not exist). Without
                // this reversal the suppression holds forever: every future
                // walk skips the seq via `is_suppressed` WITHOUT
                // `track_skipped`, the cursor advances past it, and the
                // message is permanently starved for this consumer —
                // measured as the chaos bench losing exactly the seq in
                // flight at a consumer force-disconnect. Reverse everything
                // the drain did and re-arm redelivery.
                if self.engine.ctx().catalog.binding(binding_id).is_none() {
                    // 1. Reverse the drain's blind shared-counter
                    //    increments — no pending was registered, so no
                    //    ack/nack/retire path will ever decrement these.
                    self.counters.dec_inflight_bulk(
                        consumer_id.0,
                        queue_id.0,
                        entries.len() as u32,
                    );
                    // 2. Reverse the drain-side per-subject counters AND
                    //    un-suppress each seq (`SuppressOp::Released`) so a
                    //    rewound walk may redeliver it — to a queue-group
                    //    sibling now, or to this consumer when it
                    //    resubscribes. Same channel + ring-full handling as
                    //    `apply_delta_and_sync`; the retry flush at the top
                    //    of the command loop re-signals the rewind for any
                    //    queued event. A stale event for a removed consumer
                    //    is harmless: `drain_event_ring` ignores events for
                    //    a dropped slot, and `apply_delta_and_sync` purges
                    //    queued events when a `ConsumerRemoved` is emitted.
                    let ack_floor = self.ack_floors.floor(consumer_id.raw());
                    let mut min_seq: Option<u64> = None;
                    for e in entries.iter() {
                        min_seq = Some(min_seq.map_or(e.seq, |m: u64| m.min(e.seq)));
                        let evt = DrainEvent::Ack {
                            consumer_id,
                            subject_hash: e.subject_hash,
                            ack_floor,
                            seq: e.seq,
                            op: crate::shard::drain_events::SuppressOp::Released,
                        };
                        if self.drain_evt_tx.try_send(evt).is_err() {
                            self.silent_drops.inc_drain_evt();
                            self.pending_drain_acks.push(evt);
                        }
                    }
                    // 3. Rewind so the drain re-walks the released seqs.
                    //    Ordering contract (see the drain's `take_rewind`
                    //    comment): Released events are pushed BEFORE the
                    //    rewind is signalled, and the drain consumes the
                    //    rewind before draining the event ring, so a
                    //    visible signal implies the releases are visible
                    //    too.
                    if let Some(min_seq) = min_seq {
                        rewind_released(&self.counters, min_seq);
                    }
                    self.gate.release();
                    // 4. NO wheel insert — there is no pending to time
                    //    out, and a stale ack-timeout entry would only
                    //    lazy-cancel later without releasing anything.
                    return;
                }
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
                                // Counter reconciliation for a duplicate
                                // redelivery — nothing was acked and
                                // nothing was released: the suppression
                                // set must not change.
                                seq: 0,
                                op: crate::shard::drain_events::SuppressOp::None,
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
                self.apply_delta_and_sync(&delta, false);
            }
        }
    }

    // ── Timing wheel ─────────────────────────────────────────────────────────

    /// Milliseconds since this worker's epoch — the single source of
    /// time for both wheels, monotonic because `Instant` is.
    #[inline]
    pub(super) fn now_ms(&self) -> u64 {
        Instant::now().duration_since(self.epoch).as_millis() as u64
    }

    /// Ensure the wheel is initialized. Called lazily on first need.
    pub(super) fn ensure_wheel(&mut self) {
        if self.wheel.is_none() {
            let now_ms = self.now_ms();
            self.wheel = Some(arbitro_common::HierarchicalTimingWheel::new(
                Self::WHEEL_TICK_MS,
                now_ms,
            ));
        }
    }

    /// The earlier of the wheel's next deadline and the next idempotency
    /// sweep. `None` disables the arm — which is the point of asking the
    /// wheel instead of ticking: a shard with nothing scheduled sleeps
    /// until real work arrives, however fine [`Self::WHEEL_TICK_MS`] is.
    pub(super) fn rearm_timer(&mut self) {
        let wheel_due = self.wheel.as_ref().and_then(|w| w.next_expiry_ms());
        let idempotency_due = self
            .has_idempotency
            .load(std::sync::atomic::Ordering::Relaxed)
            .then(|| self.last_idempotency_ms + Self::IDEMPOTENCY_INTERVAL_MS);
        self.next_timer_ms = match (wheel_due, idempotency_due) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (only, None) | (None, only) => only,
        };
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
        // The instant ack_wait actually runs out. This was
        // `ack_wait_ms / 1000` whole ticks, which truncated: 1500ms
        // became one tick, so the broker auto-nacked inside the first
        // second and redelivered a message whose ack_wait had not
        // expired. Nothing left to round now.
        let now_ms = self.now_ms();
        let deadline_ms = now_ms + u64::from(ack_wait_ms);

        let wheel = self.wheel.as_mut().unwrap();
        for entry in entries {
            // M5: explicit kind tag — no more "subject_hash == 0" hack.
            let outcome = wheel.insert(
                arbitro_common::WheelEntry {
                    seq: entry.seq,
                    consumer_id: consumer_id.0,
                    subject_hash: entry.subject_hash,
                    kind: arbitro_common::WheelEntryKind::AckTimeout,
                },
                deadline_ms,
            );
            debug_assert_eq!(
                outcome,
                arbitro_common::Insert::Scheduled,
                "ack_wait_ms > 0 was checked above"
            );
        }
        self.rearm_timer();
    }

    /// Move the wheel to `now_ms`. Process expired entries: verify
    /// still pending → auto-nack (cursor rewind + gate release).
    fn wheel_advance(&mut self, now_ms: u64) {
        let wheel = match self.wheel.as_mut() {
            Some(w) => w,
            None => return,
        };
        wheel.advance_to(now_ms, &mut self.wheel_buf);
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
        // Auto-nack deltas accumulated across the expired batch. They
        // MUST reach `apply_delta_and_sync` (previously the delta was
        // discarded): each released pending emits a `DrainEvent::Ack`
        // that (a) decrements the drain-owned per-subject inflight —
        // dropping it leaked the subject cap forever — and (b) removes
        // the seq from the drain's delivered-suppression set so the
        // rewind below can actually redeliver it.
        let mut merged_delta = arbitro_engine_v2::DeltaEvents::default();

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
                // Broker-side auto-nack: no client frame, so no id.
                sub_id: 0,
            };
            let delta = self.engine.execute(&Command::Nack {
                // Broker-side auto-nack: no client frame, no connection.
                conn_id: ConnectionId(0),
                consumer_id,
                entries: &[ack_entry],
            });
            merged_delta.merge(delta);

            // Decrement atomic inflight.
            if let Some(consumer) = self.engine.consumer(consumer_id) {
                self.counters
                    .dec_inflight(consumer_id.0, consumer.queue_id.0);
            }

            // Track minimum seq for cursor rewind.
            min_rewind = Some(min_rewind.map_or(entry.seq, |m: u64| m.min(entry.seq)));
            expired_count += 1;
        }

        if !merged_delta.is_empty() {
            // Auto-nack releases: sync drain-side subject counters and
            // un-suppress the timed-out seqs (false = not acks).
            self.apply_delta_and_sync(&merged_delta, false);
        }

        if expired_count > 0 {
            // Rewind cursor and wake drain for redelivery.
            //
            // BUG3/M3: same protocol as handle_bind / retirement
            // (`rewind_released`: set_cursor + signal_rewind), NOT a bare
            // set_cursor + clear_rewind. A drain cycle already past its
            // top-of-cycle `take_rewind` writes `new_cursor` forward at
            // cycle end, clobbering a bare set_cursor — and the old
            // unconditional `clear_rewind()` wiped any co-pending rewind
            // signalled by a retirement or resubscribe, skipping those
            // seqs forever (message loss). `signal_rewind` is durable
            // (consumed at the top of the drain's next cycle) and
            // min-composes with concurrent signals.
            if let Some(min_seq) = min_rewind {
                rewind_released(&self.counters, min_seq);
            }
            self.gate.release();
        }
    }

    /// Move the wheel to `now`, retire whatever dedup windows ran out,
    /// re-arm. Safe at any moment: both wheels take an instant, so an
    /// early call moves nothing and a late one catches up in full.
    fn run_timers(&mut self) {
        let now_ms = self.now_ms();
        self.wheel_advance(now_ms);

        // F26: every per-stream tracker. Handing over elapsed wall time
        // rather than a tick count keeps the windows honest even though
        // this arm competes with command traffic and is regularly late.
        let elapsed = now_ms.saturating_sub(self.last_idempotency_ms);
        if elapsed >= Self::IDEMPOTENCY_INTERVAL_MS
            && self
                .has_idempotency
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            let guard = self.idempotency_tracker.read();
            for tracker_arc in guard.values() {
                tracker_arc.lock().advance_by_ms(elapsed);
            }
            drop(guard);
            self.last_idempotency_ms = now_ms;
        }

        self.rearm_timer();
    }

    /// H12: run a pass if its deadline has gone by. Called after every
    /// dispatched command so a busy shard doesn't starve the `select!`
    /// timer arm. Idle shards fall back to the sleep-driven branch.
    fn maybe_tick_periodic(&mut self) {
        let Some(due_ms) = self.next_timer_ms else {
            return;
        };
        if self.now_ms() < due_ms {
            return;
        }
        self.run_timers();
    }

    /// Returns `true` if shutdown was requested.
    fn handle_or_shutdown(&mut self, cmd: ShardCommand) -> bool {
        if matches!(cmd, ShardCommand::Shutdown) {
            if crate::shard::drain::chaos_debug() {
                let info = self.store.lock().info();
                eprintln!(
                    "[SHUTDOWN-BEGIN] store_first={} store_last={} messages={}",
                    info.first_seq, info.last_seq, info.messages
                );
            }
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

    /// Dispatch a single command to its handler.
    fn dispatch_command(&mut self, cmd: ShardCommand) {
        match cmd {
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
    ///
    /// `releases_are_acks` — true ONLY when the delta comes from
    /// `Command::Ack` (handle_ack / ack_term): every released pending
    /// entry was genuinely acked, so its seq rides the `DrainEvent::Ack`
    /// as `SuppressOp::Acked` and stays in the drain's redelivery-
    /// suppression set. Nack, ack-timeout and retirement releases pass
    /// false (`SuppressOp::Released`) — those seqs are owed again and
    /// must become deliverable.
    pub(super) fn apply_delta_and_sync(
        &mut self,
        delta: &arbitro_engine_v2::DeltaEvents,
        releases_are_acks: bool,
    ) {
        if !delta.demand_became_available.is_empty() {
            self.gate.release();
        }
        // Push subject-inflight decs to the drain via SPSC ring. Drain
        // owns the per-(consumer, subject) counters (`ConsumerSubjects`)
        // and applies these at the top of its next cycle. Ring overflow
        // queues the event on `pending_drain_acks` for retry — these
        // events are correctness-critical (a lost `Released` starves its
        // seq forever), see `drain_events.rs` overflow policy.
        if !delta.subject_hashes_acked.is_empty() {
            for &(cid, sh, seq) in &delta.subject_hashes_acked {
                let evt = DrainEvent::Ack {
                    consumer_id: ConsumerId(cid),
                    subject_hash: sh,
                    // Piggyback the contiguous-acked floor so the
                    // drain slot stays current (handle_ack records
                    // matched seqs BEFORE the engine execute whose
                    // delta lands here, so the value is fresh).
                    ack_floor: self.ack_floors.floor(cid),
                    seq,
                    // A true ack marks the seq done forever; a
                    // nack / ack-timeout / retirement release makes
                    // it owed (deliverable) again.
                    op: if releases_are_acks {
                        crate::shard::drain_events::SuppressOp::Acked
                    } else {
                        crate::shard::drain_events::SuppressOp::Released
                    },
                };
                if self.drain_evt_tx.try_send(evt).is_err() {
                    // Ring full (drain parked + bulk release > cap).
                    // Queue for retry at the top of the command loop —
                    // same pattern as `pending_consumer_remove` (H11).
                    // The counter stays as the ring-overflow degradation
                    // signal even though the event is no longer lost.
                    self.silent_drops.inc_drain_evt();
                    self.pending_drain_acks.push(evt);
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
        // Mirror the inflight credit the engine released via binding
        // retirement. The ack, nack and ack-timeout handlers decrement
        // `counters` themselves; the four retirement handlers
        // (delete_stream, delete_consumer, unsubscribe, drain_connection)
        // did not, so the engine's count dropped to zero while the
        // mirror kept the credit. Consumer ids are pool-recycled, so the
        // next consumer to claim the id inherited the residue and
        // `consumer_has_capacity` refused it delivery forever.
        //
        // Only `runtime::retire::retire_binding` populates this vec, and
        // the ack/nack paths never reach it — applying unconditionally
        // here cannot double-decrement.
        //
        // Exact subtraction, never a reset: the drain runs in parallel
        // off a snapshot that still lists this consumer until the next
        // `publish_snapshot`, so it can land a legitimate `inc_inflight`
        // mid-cleanup. That delivery is real and is released by its own
        // path (`Delivered` with a vanished binding reverses it); a blind
        // zero here would erase it and re-create the residue one step later.
        for &(consumer_raw, queue_raw, count) in &delta.inflight_released {
            self.counters
                .dec_inflight_bulk(consumer_raw, queue_raw, count);
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
            // A removed consumer's queued Ack retries are moot — and must
            // never be applied AFTER its ConsumerRemoved lands: consumer
            // ids are pool-recycled, and a stale floor-raise or dec on the
            // recycled id would silently corrupt the new consumer's slot.
            // Purge before queueing/sending the removal (the ring is SPSC
            // FIFO, so events already IN the ring are safely ordered
            // before the removal).
            self.pending_drain_acks.retain(|e| {
                !matches!(e, DrainEvent::Ack { consumer_id, .. } if consumer_id.raw() == cid.raw())
            });
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
        // Fanout-collapse groups: one dense index per distinct
        // `(connection, consumer)`. Siblings of the same consumer on the
        // same socket collapse to one wire copy, so they share an index.
        // Built here, on the cold path, so the drain never hashes it.
        let mut group_ids: std::collections::HashMap<(u64, u32), u32, foldhash::fast::FixedState> =
            std::collections::HashMap::with_capacity_and_hasher(
                self.bindings.len(),
                foldhash::fast::FixedState::default(),
            );
        let mut next_group = 0u32;
        let group_of: Vec<u32> = self
            .bindings
            .iter()
            .map(|b| {
                *group_ids
                    .entry((b.connection_id.0, b.consumer_id.0))
                    .or_insert_with(|| {
                        let g = next_group;
                        next_group += 1;
                        g
                    })
            })
            .collect();

        // We need to clone bindings for the snapshot because drain holds Arc
        // while we might modify our local copy later.
        let snap_bindings: Vec<ActiveBinding> = self
            .bindings
            .iter()
            .enumerate()
            .map(|(i, b)| ActiveBinding {
                binding_id: b.binding_id,
                connection_id: b.connection_id,
                consumer_id: b.consumer_id,
                subscription_id: b.subscription_id,
                group_idx: group_of[i],
                external_sub_id: b.external_sub_id,
                stream_id: b.stream_id,
                queue_id: b.queue_id,
                max_inflight: b.max_inflight,
                fire_and_forget: b.fire_and_forget,
                ack_wait_ms: b.ack_wait_ms,
                deliver_floor: b.deliver_floor,
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
                mt.set_binding_idx_for(b.consumer_id, b.connection_id, b.subscription_id, i as u32);
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
        let Some(ref dir) = self.data_path else {
            return;
        };
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
        let Some(ref dir) = self.data_path else {
            return;
        };
        let path = dir.join("stream_lifecycle.bin");
        let Ok(bytes) = std::fs::read(&path) else {
            return;
        };
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

    /// Build a bare CommandWorker for direct handler-level tests, plus
    /// the drain-side consumer half of the drain-event ring so a test
    /// can observe exactly what the drain thread would apply. Mirrors
    /// `ShardRouter::spawn` field-for-field with in-memory defaults.
    fn test_worker() -> (
        CommandWorker,
        crate::shard::drain_events::DrainEventConsumer,
    ) {
        let (_tx, rx) = mpsc::channel(4);
        let (mut notify_producers, notify_rx, _notify_shutdown) =
            crate::shard::shared::NotifyRing::new(1);
        let _notify_tx = notify_producers.pop();
        let (drain_evt_tx, drain_evt_rx) = crate::shard::drain_events::DrainEventRing::new();
        let worker = CommandWorker {
            engine: ArbitroEngine::new(),
            counters: Arc::new(SharedCounters::new()),
            snapshot: Arc::new(SnapshotSwap::new(DrainSnapshot::empty())),
            store: Arc::new(parking_lot::Mutex::new(Box::new(
                arbitro_store::MemoryStore::new(),
            ))),
            gate: Arc::new(Gate::new()),
            registry: crate::transport::ConnectionRegistry::new(64),
            names: Arc::new(crate::common::NameRegistry::new()),
            rx,
            notify_ring: notify_rx,
            drain_evt_tx,
            running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            drain_config_batch_size: 64,
            stream_retention: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            bindings: Vec::new(),
            next_eviction: None,
            wheel: None,
            wheel_buf: Vec::new(),
            next_timer_ms: None,
            epoch: Instant::now(),
            last_idempotency_ms: 0,
            idempotency_tracker: crate::shard::idempotency::new_shared_idempotency(),
            has_idempotency: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            silent_drops: Arc::new(crate::common::SilentDrops::new()),
            pending_consumer_remove: Vec::new(),
            pending_drain_acks: Vec::new(),
            ack_floors: crate::shard::ack_floor::AckFloors::new(),
            evict_resume_seq: 0,
            stream_oldest_ts: HashMap::default(),
            dlq_nack_counts: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            data_path: None,
            replay_mode: false,
            #[cfg(feature = "cluster")]
            replication_tx: Arc::new(parking_lot::Mutex::new(None)),
        };
        (worker, drain_evt_rx)
    }

    /// Chaos regression — a `Delivered` notification whose binding was
    /// retired before it applied (consumer force-disconnect while the
    /// delivery was in flight on the notify ring) must REVERSE the
    /// drain's flush-time bookkeeping and re-arm redelivery.
    ///
    /// The race: the drain suppresses the seq synchronously at flush
    /// (`cs.suppress`), but `binding.pending` — what retirement walks
    /// to emit `Released` — is only registered when the notification is
    /// applied. If `mark_connection_dead` runs in that window, the late
    /// notification meets a retired binding: before the fix it fell
    /// through as a no-op (engine `Command::Delivered` skips unknown
    /// bindings), so nothing ever released the suppression — every
    /// future walk skipped the seq without `track_skipped` and the
    /// message was permanently starved (chaos bench: drain stalls one
    /// short, always the seq in flight at the force-disconnect).
    ///
    /// This drives `handle_notification` directly with the exact
    /// post-race state (catalog has no such binding), which is
    /// identical no matter how the binding disappeared — connection
    /// death, unsubscribe, or delete cascade.
    #[test]
    fn late_delivered_for_retired_binding_reverses_and_releases() {
        let (mut w, mut drain_evt_rx) = test_worker();
        let consumer_raw = 3u32;
        let subject_hash = 0xABCDu32;
        let seq = 7u64;

        // Simulate the drain's flush-time bookkeeping for one ack-mode
        // delivery (drain.rs Phase 3): shared inflight inc + drain-side
        // subject inc + suppression.
        w.counters.inc_inflight(consumer_raw, 0);
        let mut consumer_subjects: Vec<Option<ConsumerSubjects>> = Vec::new();
        {
            let cs = consumer_subjects_slot_mut(&mut consumer_subjects, consumer_raw);
            cs.inc(subject_hash);
            cs.suppress(seq);
            assert!(cs.is_suppressed(seq), "delivery memory recorded at flush");
        }
        // Drain cursor has moved on past the seq.
        w.counters.set_cursor(42);

        // The binding retired before the notification applied — the
        // catalog knows no binding 99.
        w.handle_notification(DrainNotification::Delivered {
            binding_id: arbitro_engine_v2::types::BindingId(99),
            consumer_id: ConsumerId(consumer_raw),
            queue_id: QueueId(0),
            entries: vec![arbitro_engine_v2::command::DeliveredEntry {
                seq,
                subject_hash,
                _pad: 0,
            }],
        });

        // 1. The drain's blind shared-counter increment was reversed —
        //    nothing else will ever decrement it (no pending exists).
        assert_eq!(
            w.counters.consumer_inflight(consumer_raw),
            0,
            "shared inflight must be reversed for a retired binding"
        );

        // 2. A `Released` reached the drain ring: applying the ring
        //    decrements the subject counter AND un-suppresses the seq,
        //    so a rewound walk may redeliver it.
        drain_event_ring(&mut drain_evt_rx, &mut consumer_subjects);
        let cs = consumer_subjects_slot(&consumer_subjects, consumer_raw)
            .expect("slot still exists");
        assert!(
            !cs.is_suppressed(seq),
            "suppression must be released — otherwise the seq is starved forever"
        );
        assert_eq!(cs.get(subject_hash), 0, "subject inflight reversed");

        // 3. The cursor was rewound with a durable signal so the drain
        //    actually re-walks the released seq.
        assert_eq!(w.counters.cursor(), seq - 1, "cursor rewound to seq - 1");
        assert_eq!(w.counters.take_rewind(), Some(seq), "durable rewind signal");

        // 4. No pending was registered, so nothing may sit in the
        //    ack-timeout wheel (a stale entry would only lazy-cancel).
        assert!(w.wheel.is_none(), "no wheel entry for a reversed delivery");
    }

    /// Audit #10 — deleting a consumer must drop ALL of its per-(consumer,
    /// seq) DLQ nack counters and none of its siblings'. Before the fix,
    /// `handle_delete_consumer` never touched the map, so a nack-heavy
    /// consumer that was deleted leaked its entries forever.
    #[test]
    fn delete_consumer_clears_its_dlq_nack_counts() {
        let mut counts: HashMap<(u32, u64), u32, foldhash::fast::FixedState> =
            HashMap::with_hasher(foldhash::fast::FixedState::default());
        // Consumer 7: three tracked seqs. Consumer 9: two tracked seqs.
        counts.insert((7, 100), 2);
        counts.insert((7, 101), 1);
        counts.insert((7, 250), 4);
        counts.insert((9, 100), 1);
        counts.insert((9, 300), 3);

        clear_consumer_nack_counts(&mut counts, 7);

        assert!(
            counts.keys().all(|(cid, _)| *cid != 7),
            "all consumer-7 entries must be gone"
        );
        assert_eq!(counts.len(), 2, "consumer-9 entries must be untouched");
        assert_eq!(counts.get(&(9, 100)), Some(&1));
        assert_eq!(counts.get(&(9, 300)), Some(&3));
    }
}
