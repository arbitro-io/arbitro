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
use std::sync::{Arc, RwLock};

use papaya::HashMap as PapayaMap;

use arbitro_engine_v2::catalog::match_table::MatchTable;
use arbitro_engine_v2::command::DeliveredEntry;
use arbitro_engine_v2::types::*;
use crate::shard::worker::ActiveBinding;

// ── Constants ──────────────────────────────────────────────────────────────

/// Max slots for consumer/queue/stream counters. Pre-allocated at startup.
/// Indices beyond this panic — resize if needed for huge deployments.
const SLOT_COUNT: usize = 4096;

// Subject inflight uses papaya::HashMap<(u32,u32), AtomicU32, foldhash> for
// true lock-free concurrent access. No RwLock — drain readers never block
// on writer inserts/removes. Measured benefits under write-churn:
//   RwLock<HashMap>: 137 ns/read, 7.2M reads/s    (91% degradation)
//   papaya::HashMap:  29 ns/read, 34M reads/s     (75% degradation)
// See crates/arbitro-server/benches/subject_inflight.rs workload E.

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
    /// Subject inflight — exact per-(consumer, subject) tracking via
    /// lock-free papaya::HashMap.
    /// Key = (consumer_id, subject_hash). Value = AtomicU32 counter.
    /// Keying by consumer_id is required for per-consumer isolation:
    /// two consumers on the same stream publishing the same subject must
    /// not share a counter (otherwise their max_subject_inflight budgets
    /// would collide and starve one of them).
    /// Entry removed when counter reaches 0 (bounded memory to working set).
    /// Lock-free: drain readers never block on writer inserts/removes.
    subject: PapayaMap<(u32, u32), AtomicU32, foldhash::fast::FixedState>,
    /// Drain cursor position. Written by drain, read by command for rewind.
    cursor: AtomicU64,
    /// Rewind signal from command → drain. `NO_REWIND` = no rewind.
    /// Command writes (min of current + new), drain reads and clears.
    rewind: AtomicU64,
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
            subject: PapayaMap::builder()
                .hasher(foldhash::fast::FixedState::default())
                .build(),
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

    // ── Subject inflight (drain: add, ack: sub) ─────────────────────

    /// Increment subject inflight for a (consumer, subject) pair.
    /// Called by drain after delivery.
    #[inline]
    pub fn inc_subject(&self, consumer_id: u32, subject_hash: u32) {
        let key = (consumer_id, subject_hash);
        let g = self.subject.pin();
        match g.get(&key) {
            Some(c) => {
                c.fetch_add(1, Ordering::Relaxed);
            }
            None => {
                // get_or_insert_with returns the final entry (inserted or
                // raced by another thread). Always safe to fetch_add on it.
                let c = g.get_or_insert_with(key, || AtomicU32::new(0));
                c.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Decrement subject inflight for a (consumer, subject) pair.
    /// Called by command thread after ack.
    ///
    /// NOTE: we DO NOT remove the entry at zero. papaya's remove is
    /// vulnerable to the ABA race: dec goes 1→0, another thread incs to 1,
    /// our remove deletes the entry losing the increment. Leaving zero
    /// entries in the map is safe (has_room returns true), at cost of
    /// bounded memory growth per distinct (consumer, subject) pair seen.
    #[inline]
    pub fn dec_subject(&self, consumer_id: u32, subject_hash: u32) {
        let key = (consumer_id, subject_hash);
        let g = self.subject.pin();
        if let Some(c) = g.get(&key) {
            c.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Check if the (consumer, subject) pair has room for more inflight.
    /// Missing key = 0 inflight = always has room.
    #[inline]
    pub fn subject_has_room(&self, consumer_id: u32, subject_hash: u32, max: u32) -> bool {
        let g = self.subject.pin();
        match g.get(&(consumer_id, subject_hash)) {
            Some(c) => c.load(Ordering::Relaxed) < max,
            None => true,
        }
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

    /// Reset rewind signal (e.g., after cursor set to 0 on subscribe).
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
    pub bindings: Vec<ActiveBinding>,
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
}

/// Per-connection writer handle, deduplicated from bindings (one entry
/// per connection even if multiple consumers live on the same conn).
#[derive(Clone)]
pub struct WriterIndexEntry {
    pub writer: std::sync::Arc<tokio::net::tcp::OwnedWriteHalf>,
    pub runtime: tokio::runtime::Handle,
}

impl DrainSnapshot {
    pub fn empty() -> Self {
        Self {
            bindings: Vec::new(),
            writers_by_conn: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            match_tables: Vec::new(),
        }
    }
}

/// Writer lookup for a given connection. HashMap+foldhash, O(1) amortised.
#[inline]
pub fn find_writer<'a>(
    index: &'a HashMap<u64, WriterIndexEntry, foldhash::fast::FixedState>,
    connection_id: u64,
) -> Option<&'a WriterIndexEntry> {
    index.get(&connection_id)
}

// ── SnapshotSwap ───────────────────────────────────────────────────────────

/// Atomic snapshot swap. `RwLock<Arc<T>>` held for ~5ns (pointer swap only).
///
/// Drain: `load()` → Arc clone (~3ns), read-only access for entire cycle.
/// Command: `store()` → swap Arc pointer (~5ns), old Arc drops when refcount=0.
pub struct SnapshotSwap<T> {
    inner: RwLock<Arc<T>>,
}

impl<T> SnapshotSwap<T> {
    pub fn new(val: T) -> Self {
        Self {
            inner: RwLock::new(Arc::new(val)),
        }
    }

    /// Load the current snapshot. Returns an Arc — caller owns a reference.
    /// RwLock read held for ~3ns (Arc clone only).
    #[inline]
    pub fn load(&self) -> Arc<T> {
        self.inner.read().unwrap().clone()
    }

    /// Replace the snapshot. RwLock write held for ~5ns (pointer swap only).
    pub fn store(&self, val: T) {
        *self.inner.write().unwrap() = Arc::new(val);
    }
}

// ── DrainNotification ──────────────────────────────────────────────────────

/// Messages from drain thread → command thread.
///
/// Sent via `tokio::sync::mpsc` (drain uses `try_send`, command uses `recv`).
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
