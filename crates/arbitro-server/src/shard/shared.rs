//! Lock-free shared state between drain and command threads.
//!
//! **Zero Mutex, zero contention.** Drain and commands run 100% in parallel.
//!
//! - `SharedCounters`: atomic inflight, demand, paused, cursor, rewind.
//! - `SnapshotSwap<T>`: RwLock<Arc<T>> for structural snapshots (bindings,
//!   match tables). Lock held ~5ns for Arc clone/swap — effectively lock-free.
//! - `DrainNotification`: lock-free queue from drain → command for delivery
//!   tracking and dead connection cleanup.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use arbitro_engine_v2::catalog::match_table::MatchTable;
use arbitro_engine_v2::command::DeliveredEntry;
use arbitro_engine_v2::types::*;
use crate::shard::worker::ActiveBinding;

// ── Constants ──────────────────────────────────────────────────────────────

/// Max slots for consumer/queue/stream counters. Pre-allocated at startup.
/// Indices beyond this panic — resize if needed for huge deployments.
const SLOT_COUNT: usize = 4096;

// Per-(consumer, subject) inflight no longer lives here. It moved to
// `crate::shard::consumer_subjects::ConsumerSubjects`, owned exclusively
// by the drain thread. Acks travel command → drain via the SPSC
// `DrainEventRing` (`crate::shard::drain_events`). Single-thread
// ownership beats the old papaya design 3-4× on the head-to-head bench
// while also removing the per-slot AtomicU32 footprint.

/// Sentinel value for "no rewind requested".
const NO_REWIND: u64 = u64::MAX;

// ── SharedCounters ───────────────────────────────────���─────────────────────

/// Atomic counters shared between drain and command threads.
///
/// Drain does `fetch_add` on delivery. ACK does `fetch_sub` on ack.
/// Zero locks, zero contention. Both threads run fully in parallel.
pub struct SharedCounters {
    /// Per-consumer inflight count. Index = ConsumerId.raw().
    consumer: Box<[AtomicU32]>,
    /// Per-queue inflight count. Index = QueueId.raw().
    queue: Box<[AtomicU32]>,
    /// Per-stream demand (number of active bindings). Index = StreamId.raw().
    demand: Box<[AtomicU32]>,
    /// Total demand across all streams — for O(1) `has_any_demand()`.
    total_demand: AtomicU32,
    /// Per-consumer paused flag. Index = ConsumerId.raw().
    paused: Box<[AtomicBool]>,
    /// Drain cursor position. Written by drain, read by command for rewind.
    cursor: AtomicU64,
    /// Rewind signal from command → drain. `NO_REWIND` = no rewind.
    /// Command writes (min of current + new), drain reads and clears.
    rewind: AtomicU64,
}

impl Default for SharedCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedCounters {
    pub fn new() -> Self {
        let mk_u32 = || -> Box<[AtomicU32]> {
            (0..SLOT_COUNT)
                .map(|_| AtomicU32::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice()
        };
        let mk_bool = || -> Box<[AtomicBool]> {
            (0..SLOT_COUNT)
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>()
                .into_boxed_slice()
        };
        Self {
            consumer: mk_u32(),
            queue: mk_u32(),
            demand: mk_u32(),
            total_demand: AtomicU32::new(0),
            paused: mk_bool(),
            cursor: AtomicU64::new(0),
            rewind: AtomicU64::new(NO_REWIND),
        }
    }

    // ── Inflight (drain: add, ack: sub) ──────────────────────────────

    /// Increment consumer + queue inflight after successful delivery.
    /// Called by drain thread.
    #[inline]
    pub fn inc_inflight(&self, consumer_id: u32, queue_id: u32) {
        self.consumer[consumer_id as usize].fetch_add(1, Ordering::Relaxed);
        self.queue[queue_id as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement consumer + queue inflight after ack.
    /// Called by command thread.
    #[inline]
    pub fn dec_inflight(&self, consumer_id: u32, queue_id: u32) {
        self.consumer[consumer_id as usize].fetch_sub(1, Ordering::Relaxed);
        self.queue[queue_id as usize].fetch_sub(1, Ordering::Relaxed);
    }

    /// Bulk decrement consumer + queue inflight (for retire_binding).
    #[inline]
    pub fn dec_inflight_bulk(&self, consumer_id: u32, queue_id: u32, count: u32) {
        self.consumer[consumer_id as usize].fetch_sub(count, Ordering::Relaxed);
        self.queue[queue_id as usize].fetch_sub(count, Ordering::Relaxed);
    }

    /// Current consumer inflight count.
    #[inline]
    pub fn consumer_inflight(&self, consumer_id: u32) -> u32 {
        self.consumer[consumer_id as usize].load(Ordering::Relaxed)
    }

    /// Check if consumer has capacity for more messages.
    #[inline]
    pub fn consumer_has_capacity(&self, consumer_id: u32, max_inflight: u32) -> bool {
        self.consumer_inflight(consumer_id) < max_inflight
    }

    // ── Demand (subscribe: add, unsubscribe: sub) ────────────────────

    /// True if any stream has at least one active binding.
    #[inline]
    pub fn has_any_demand(&self) -> bool {
        self.total_demand.load(Ordering::Relaxed) > 0
    }

    /// True if this stream has at least one active binding.
    #[inline]
    pub fn has_demand(&self, stream_id: u32) -> bool {
        self.demand[stream_id as usize].load(Ordering::Relaxed) > 0
    }

    /// Increment demand for a stream. Returns previous count.
    #[inline]
    pub fn inc_demand(&self, stream_id: u32) -> u32 {
        let prev = self.demand[stream_id as usize].fetch_add(1, Ordering::Relaxed);
        self.total_demand.fetch_add(1, Ordering::Relaxed);
        prev
    }

    /// Decrement demand for a stream. Returns previous count.
    #[inline]
    pub fn dec_demand(&self, stream_id: u32) -> u32 {
        let prev = self.demand[stream_id as usize].fetch_sub(1, Ordering::Relaxed);
        self.total_demand.fetch_sub(1, Ordering::Relaxed);
        prev
    }

    // ── Paused (command: set, drain: read) ─────���─────────────────────

    #[inline]
    pub fn is_paused(&self, consumer_id: u32) -> bool {
        self.paused[consumer_id as usize].load(Ordering::Relaxed)
    }

    #[inline]
    pub fn set_paused(&self, consumer_id: u32, val: bool) {
        self.paused[consumer_id as usize].store(val, Ordering::Relaxed);
    }

    // ── Cursor (drain: write, command: read for rewind) ──────────────

    #[inline]
    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn set_cursor(&self, val: u64) {
        self.cursor.store(val, Ordering::Relaxed);
    }

    // ── Rewind (command: signal, drain: consume) ─────────────────────

    /// Signal drain to rewind to `target`. Takes the min of current
    /// rewind and target (CAS loop for concurrent safety).
    pub fn signal_rewind(&self, target: u64) {
        loop {
            let current = self.rewind.load(Ordering::Relaxed);
            let new = current.min(target);
            if new == current {
                break;
            }
            match self.rewind.compare_exchange_weak(
                current,
                new,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(_) => continue,
            }
        }
    }

    /// Consume the rewind signal. Returns `Some(target)` if a rewind
    /// was requested, `None` otherwise.
    pub fn take_rewind(&self) -> Option<u64> {
        let val = self.rewind.swap(NO_REWIND, Ordering::Relaxed);
        if val == NO_REWIND {
            None
        } else {
            Some(val)
        }
    }

    /// Reset rewind signal — but ONLY if the slot still holds the value
    /// the caller observed. M3: the previous unconditional `store` lost a
    /// concurrent rewind signal that came in between the caller's read
    /// and its clear (e.g., wheel_tick consumes, finishes its work, then
    /// blindly stores NO_REWIND, wiping out a rewind another command
    /// signalled in the meantime). The CAS now leaves a later signal
    /// alone.
    pub fn clear_rewind_if_eq(&self, expected: u64) -> bool {
        self.rewind
            .compare_exchange(expected, NO_REWIND, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    /// Unconditional reset (e.g., on shard restart / clean shutdown).
    /// Hot-path callers should prefer `clear_rewind_if_eq`.
    pub fn clear_rewind(&self) {
        self.rewind.store(NO_REWIND, Ordering::Relaxed);
    }
}

// ── DrainSnapshot ──────────────────────────────────────────────────────────

/// Read-only snapshot for the drain thread. Updated via `SnapshotSwap`
/// by the command thread on subscribe/unsubscribe/bind.
///
/// Drain loads this once per cycle (~3ns Arc clone). The command thread
/// builds a new snapshot and swaps it in on structural changes (rare).
///
/// **Fase C.2 note**: `binding_index` was removed. The drain no longer
/// looks up bindings via `(consumer_id, connection_id)` HashMap. Instead,
/// `MatchEntry.binding_idx` is stamped directly during snapshot rebuild
/// (see `worker.rs::rebuild_and_swap_snapshot`), so per-match dispatch
/// is a direct `bindings[match_entry.binding_idx]` Vec access.
pub struct DrainSnapshot {
    /// Active bindings — iterated by drain for delivery.
    /// F19: `Arc<[T]>` instead of `Vec<T>`. Snapshot rebuild builds a
    /// fresh boxed slice and wraps it in `Arc`, so cloning the
    /// `DrainSnapshot` into a new tokio task or reading from the drain
    /// thread doesn't pay a Vec-clone per swap. Deref to `&[T]` keeps
    /// every drain call site (`&snap.bindings`, `snap.bindings[idx]`)
    /// unchanged.
    pub bindings: Arc<[ActiveBinding]>,
    /// Per-connection writer index. One entry per connection (dedup'd
    /// from bindings — multiple consumers share a writer).
    /// HashMap+foldhash: connection_id is unbounded-monotonic, so direct
    /// Vec<Option<T>> would leak memory for closed conns, and binary
    /// search is 2× slower than HashMap at all sizes (2.6 ns vs 3-15 ns).
    pub writers_by_conn: HashMap<u64, WriterIndexEntry, foldhash::fast::FixedState>,
    /// Match tables — indexed by StreamId.raw(). `None` = no stream.
    /// `MatchEntry.binding_idx` is stamped with the server-layer
    /// binding index during rebuild.
    pub match_tables: Vec<Option<MatchTable>>,
    /// Per-stream age eviction limit (milliseconds). Indexed by StreamId.raw().
    /// 0 = no age limit for that stream. Populated by CommandWorker from
    /// `stream_retention` on snapshot rebuild.
    pub stream_max_age_ms: Vec<u64>,
}

/// Per-connection writer handle, deduplicated from bindings (one entry
/// per connection even if multiple consumers live on the same conn).
#[derive(Clone)]
pub struct WriterIndexEntry {
    pub write_tx: tokio::sync::mpsc::Sender<bytes::Bytes>,
    /// **M8**: writer feedback — `true` when the writer task has hit an
    /// I/O error. Drain checks this before `try_send` to skip dead
    /// connections without wasting frames into the channel.
    pub write_failed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl DrainSnapshot {
    pub fn empty() -> Self {
        Self {
            bindings: Arc::from(Vec::<ActiveBinding>::new().into_boxed_slice()),
            writers_by_conn: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            match_tables: Vec::new(),
            stream_max_age_ms: Vec::new(),
        }
    }
}

/// Writer lookup for a given connection. HashMap+foldhash, O(1) amortised.
#[inline]
pub fn find_writer(
    index: &HashMap<u64, WriterIndexEntry, foldhash::fast::FixedState>,
    connection_id: u64,
) -> Option<&WriterIndexEntry> {
    index.get(&connection_id)
}

// ── SnapshotSwap ───────────────────────────────────────────────────────────

/// Lock-free atomic snapshot swap backed by `arc_swap::ArcSwap` (F20).
///
/// Drain: `load()` → ~1–2 ns lock-free Arc load.
/// Command: `store()` → atomic pointer swap, old Arc drops when refcount = 0.
///
/// Replaces the previous `RwLock<Arc<T>>` pattern, which paid a read-lock
/// acquire on every drain cycle (~5 ns); at 100k+ cycles/s the savings
/// add up — and the API is unchanged for callers (`.load() -> Arc<T>`,
/// `.store(val)`).
pub struct SnapshotSwap<T> {
    inner: arc_swap::ArcSwap<T>,
}

impl<T> SnapshotSwap<T> {
    pub fn new(val: T) -> Self {
        Self {
            inner: arc_swap::ArcSwap::from_pointee(val),
        }
    }

    /// Load the current snapshot — lock-free, ~1–2 ns.
    #[inline]
    pub fn load(&self) -> Arc<T> {
        // `load_full` returns an owned `Arc<T>`, matching the prior
        // signature `RwLock::read().clone()`.
        self.inner.load_full()
    }

    /// Replace the snapshot — single atomic pointer swap.
    pub fn store(&self, val: T) {
        self.inner.store(Arc::new(val));
    }
}

// ── DrainNotification ──────────────────────────────────────────────────────

/// SPSC notification ring: drain OS thread (producer) → command tokio task (consumer).
/// Capacity is power-of-two (8192 = 2^13). Drain uses `try_send` (non-blocking),
/// command uses `recv_async` (async) + `try_recv` (batch drain).
pub type NotifyRing = arbitro_kit::stream::Ring<DrainNotification, 8192, arbitro_kit::NotifyWaiter>;

/// Messages from drain thread → command thread.
///
/// Sent via SPSC `Ring` (drain uses `try_send`, command uses `recv_async`).
/// Preserves ordering: all deliveries before a `ConnectionDead` are guaranteed
/// to be processed first — so `retire_binding` sees complete pending data.
pub enum DrainNotification {
    /// Entries successfully delivered to a binding. Command thread updates
    /// the engine's pending list for future ack/retire.
    Delivered {
        binding_id: BindingId,
        consumer_id: ConsumerId,
        queue_id: QueueId,
        entries: Vec<DeliveredEntry>,
    },
    /// Connection detected dead (try_send returned Closed). Command thread
    /// calls `engine.mark_connection_dead()` to retire bindings.
    ConnectionDead(ConnectionId),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T19 — `signal_rewind` takes the MIN of the existing pending
    /// value and the new target. The CAS loop must converge even under
    /// concurrent contention.
    #[test]
    fn t19_signal_rewind_takes_min() {
        let c = SharedCounters::new();
        c.signal_rewind(100);
        c.signal_rewind(50); // smaller wins
        c.signal_rewind(75); // larger ignored
        assert_eq!(c.take_rewind(), Some(50));
        // After take, sentinel restored.
        assert_eq!(c.take_rewind(), None);
    }

    /// T19 — `clear_rewind_if_eq` only clears when the observed value
    /// matches. A racing signal that arrived after the observation
    /// must survive the clear. This is the M3 race: wheel_tick
    /// observes `R1`, processes, then clears — but in the meantime
    /// the command thread signalled `R2 < R1`. The CAS form rejects
    /// the clear and leaves `R2` intact.
    #[test]
    fn t19_clear_rewind_if_eq_does_not_clobber_concurrent_signal() {
        let c = SharedCounters::new();
        c.signal_rewind(100);
        // Observer reads 100, races: another producer signals a smaller
        // value (50) before the observer's CAS-clear runs.
        c.signal_rewind(50);
        // Observer's clear, scoped to its observed value, must FAIL.
        assert!(
            !c.clear_rewind_if_eq(100),
            "clear with stale expected must be a no-op",
        );
        // 50 survives.
        assert_eq!(c.take_rewind(), Some(50));
    }

    /// T19 — when observed value matches, `clear_rewind_if_eq` clears.
    #[test]
    fn t19_clear_rewind_if_eq_clears_on_match() {
        let c = SharedCounters::new();
        c.signal_rewind(42);
        assert!(c.clear_rewind_if_eq(42));
        assert_eq!(c.take_rewind(), None);
    }

    /// T19 — unconditional `clear_rewind` always clears (used on
    /// shutdown/restart paths). Documents the contract difference
    /// from `clear_rewind_if_eq` in a test that fails if someone
    /// makes the two methods equivalent by accident.
    #[test]
    fn t19_clear_rewind_unconditional_wipes_any_signal() {
        let c = SharedCounters::new();
        c.signal_rewind(10);
        c.signal_rewind(20); // 10 wins via min
        c.clear_rewind();
        assert_eq!(c.take_rewind(), None);
    }
}
