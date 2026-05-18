//! Per-shard message-id dedup with bounded-window expiration.
//!
//! Each shard worker owns one optional `IdempotencyTracker` allocated
//! lazily on the first stream that opts in (via `CreateStream` with a
//! non-zero `idempotency_window_ms`). Shards without any
//! idempotent stream pay literal zero cost — the field is `None`,
//! the HashMap is never built, the wheel never ticks.
//!
//! ## Contract
//!
//! For a stream with `idempotency_window_ms = W`:
//!  - `contains(stream, msg_id_hash)` returns `true` iff the broker
//!    has seen this `(stream, msg_id_hash)` pair within the last W ms.
//!  - `record(stream, msg_id_hash)` inserts the pair with an
//!    expiration W ms in the future. Repeated record of the same key
//!    is a no-op (the original expiration stands; we DO NOT slide the
//!    window — that's intentional, prevents an adversary from keeping
//!    an entry alive forever by replaying).
//!  - `tick()` advances the wheel by one bucket (1 s) and drops
//!    entries that crossed their expiration line. O(entries-in-bucket).
//!
//! ## Why HashMap + Wheel and not just HashMap with `expires_at`?
//!
//! Either works for membership. The wheel is the cheap GC path —
//! O(1) advance, only touches the entries that are about to expire.
//! A naive HashMap with periodic-sweep GC pays O(N) on every sweep
//! and pauses the publish path. A BinaryHeap pays O(log N) per
//! insert. The wheel is O(1) per insert AND amortised O(1) per
//! expiration. For 10 k publishes/s sustained × 60 s window =
//! 600 k entries; wheel = ~3 MB heap + 60 buckets vs. HashMap-only
//! at the same scale = ~24 MB if you also want sweep-free expiry.
//!
//! ## Memory budget
//!
//! Per entry: `(u32 stream_id, u64 msg_id_hash, u64 expires_at_ms)`
//! plus HashMap overhead → ~48 B (entry + hash slot). At 10 k/s ×
//! 60 s window = 600 k × 48 B ≈ 29 MB per shard. The shard owns it
//! exclusively (no cross-shard sharing), so on an 8-shard broker
//! the worst-case total is ~230 MB at peak — comparable to a single
//! medium-sized message store.

use std::collections::HashMap;
use std::sync::Arc;

use arbitro_engine_v2::types::StreamId;
use arbitro_common::{TimingWheel, foldhash::fast::FixedState};

/// Shared handle to a shard's idempotency state.
///
/// **F26 / TODO H4**: this used to be `Arc<Mutex<Option<IdempotencyTracker>>>`
/// — a single per-shard lock that serialised every idempotent publish
/// on the shard. Under load on one hot stream, every cold stream on
/// the same shard would also stall.
///
/// Now: outer `RwLock` over a per-stream map of small per-stream
/// trackers, each behind its own `parking_lot::Mutex`. The publish
/// hot path:
///   1. read-lock the outer map (lock-free in steady state),
///   2. clone the per-stream `Arc<Mutex<Tracker>>` if present,
///   3. drop the outer read lock, then lock the per-stream mutex.
///
/// Different streams take different locks → no false sharing. The
/// worker tick iterates the map under a read lock and `tick()`s each
/// tracker. The outer write-lock is only taken on first publish for a
/// given stream (lazy allocation) and on stream deletion.
pub type SharedIdempotency = Arc<parking_lot::RwLock<
    HashMap<u32, Arc<parking_lot::Mutex<IdempotencyTracker>>, FixedState>,
>>;

/// Build a fresh shared handle with no trackers allocated yet.
pub fn new_shared_idempotency() -> SharedIdempotency {
    Arc::new(parking_lot::RwLock::new(HashMap::with_hasher(FixedState::default())))
}

/// Get-or-create the per-stream tracker handle. Cheap when the entry
/// already exists (read lock + Arc clone); takes the write lock once
/// per fresh stream to insert. Caller then locks the returned Mutex.
#[inline]
pub fn idempotency_for_stream(
    shared: &SharedIdempotency,
    stream: StreamId,
) -> Arc<parking_lot::Mutex<IdempotencyTracker>> {
    let key = stream.0;
    // Fast path: existing entry.
    {
        let g = shared.read();
        if let Some(t) = g.get(&key) {
            return Arc::clone(t);
        }
    }
    // Slow path: insert under write lock.
    let mut g = shared.write();
    Arc::clone(g.entry(key).or_insert_with(|| {
        Arc::new(parking_lot::Mutex::new(IdempotencyTracker::new()))
    }))
}

/// Wheel resolution: each bucket represents one second. We round
/// `idempotency_window_ms` UP to the nearest second when inserting.
pub const TICK_MS: u64 = 1000;

/// Maximum window the tracker accepts, in milliseconds. Streams that
/// request a longer window are silently clamped here. Five minutes is
/// the same default NATS JetStream uses for the upper bound of
/// per-stream dedup.
///
/// Why a cap: without one, a hostile or buggy client could request a
/// 24-hour window and pin tens of millions of entries in RAM.
pub const MAX_WINDOW_MS: u32 = 5 * 60 * 1000; // 300_000

/// Number of wheel buckets. Equals the cap in seconds — every
/// supported window fits in the wheel.
pub const WHEEL_BUCKETS: usize = (MAX_WINDOW_MS as usize) / (TICK_MS as usize);

/// Compact entry that lives in the timing wheel. The wheel returns
/// these on `advance()` and we look them up in `seen` to remove the
/// mapping. 16 B, `Copy`, fits the wheel's `T: Copy` bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct IdempotencyEntry {
    pub stream_id: u32,
    pub _pad: u32,
    pub msg_id_hash: u64,
}
const _: () = assert!(core::mem::size_of::<IdempotencyEntry>() == 16);

/// Per-shard dedup state. Holds a membership map keyed by
/// `(stream, hash)` and a wheel that schedules each entry's removal.
///
/// `seen` and `wheel` are kept in lock-step: every insert hits both,
/// every wheel-advanced entry is removed from `seen`. A bug in one
/// without the other would either leak memory (entry in `seen` not
/// in any wheel bucket) or break the dedup contract (wheel entry not
/// in `seen`). The two-line invariant is tested below.
///
/// **M2**: the value side stores the full `msg_id` bytes so a 64-bit
/// FoldHash collision between two distinct ids does not silently
/// drop the second publish. On a hash hit we compare bytes; only an
/// exact match counts as a duplicate. The slot is a `SmallVec`-style
/// `Vec<Vec<u8>>` to handle the rare case where multiple ids genuinely
/// collide — typically length 1.
pub struct IdempotencyTracker {
    seen: HashMap<(StreamId, u64), Vec<Vec<u8>>, FixedState>,
    wheel: TimingWheel<IdempotencyEntry>,
    /// Scratch buffer reused on every `tick()` to avoid allocation.
    drain_buf: Vec<IdempotencyEntry>,
}

impl IdempotencyTracker {
    pub fn new() -> Self {
        Self {
            seen: HashMap::with_hasher(FixedState::default()),
            wheel: TimingWheel::new(WHEEL_BUCKETS),
            drain_buf: Vec::with_capacity(256),
        }
    }

    /// True if `(stream, msg_id_hash, msg_id_bytes)` was recorded
    /// recently and hasn't expired yet. M2: requires the caller pass
    /// the full id so hash collisions don't cause false dedup.
    #[inline]
    pub fn contains(&self, stream: StreamId, msg_id_hash: u64, msg_id: &[u8]) -> bool {
        match self.seen.get(&(stream, msg_id_hash)) {
            Some(ids) => ids.iter().any(|id| id.as_slice() == msg_id),
            None => false,
        }
    }

    /// Insert `(stream, msg_id_hash, msg_id_bytes)` with an expiration
    /// `window_ms` in the future. Returns `true` if this is the first
    /// time we've seen the id, `false` if it was already recorded.
    ///
    /// M2: hash collisions between distinct ids are resolved by storing
    /// every full id that landed in the same hash slot and comparing
    /// bytes on lookup. In the common case (no collision) the slot
    /// holds exactly one id and the cost is one Vec push + one
    /// `[u8]::eq`.
    ///
    /// The window is clamped to `MAX_WINDOW_MS`. Repeat-records do
    /// NOT extend the existing entry's lifetime — the original
    /// expiration stands.
    #[inline]
    pub fn record(
        &mut self,
        stream: StreamId,
        msg_id_hash: u64,
        msg_id: &[u8],
        window_ms: u32,
    ) -> bool {
        use std::collections::hash_map::Entry;
        let is_new = match self.seen.entry((stream, msg_id_hash)) {
            Entry::Occupied(mut slot) => {
                let ids = slot.get_mut();
                if ids.iter().any(|id| id.as_slice() == msg_id) {
                    return false; // exact duplicate
                }
                // Genuine hash collision between distinct ids — keep
                // both. Tested in `hash_collision_does_not_dedup`.
                ids.push(msg_id.to_vec());
                true
            }
            Entry::Vacant(slot) => {
                slot.insert(vec![msg_id.to_vec()]);
                true
            }
        };
        if is_new {
            // Schedule removal. Round window up to whole seconds
            // (wheel resolution is 1s). NameRegistry already clamped
            // the window at create time (F39); apply `.min()` here
            // defensively in case a caller wires the tracker
            // without the registry.
            let clamped_ms = window_ms.min(MAX_WINDOW_MS);
            let ticks = clamped_ms.div_ceil(TICK_MS as u32);
            self.wheel.insert(
                IdempotencyEntry {
                    stream_id: stream.0,
                    _pad: 0,
                    msg_id_hash,
                },
                ticks,
            );
        }
        is_new
    }

    /// Forcibly remove a `(stream, msg_id_hash, msg_id)` mapping from
    /// the membership map, regardless of its expiration. Used to roll
    /// back records inserted during a batch publish when a later
    /// entry in the same batch turns out to be a duplicate — the
    /// batch is atomic, so partial inserts must be undone.
    ///
    /// The wheel entry is left in place; when its bucket fires
    /// `tick()` will look up the key, miss, and silently move on.
    /// Leaving phantom wheel entries is fine — they cost one HashMap
    /// lookup at expiration time and don't break the dedup contract.
    #[inline]
    pub fn forget(&mut self, stream: StreamId, msg_id_hash: u64, msg_id: &[u8]) {
        if let Some(ids) = self.seen.get_mut(&(stream, msg_id_hash)) {
            ids.retain(|id| id.as_slice() != msg_id);
            if ids.is_empty() {
                self.seen.remove(&(stream, msg_id_hash));
            }
        }
    }

    /// Advance the wheel by one tick (1 s). Removes from `seen` every
    /// entry that just expired. Designed to be called from the shard
    /// worker's existing tick loop — same cadence as the ack wheel,
    /// no new timer.
    ///
    /// M2 collision behaviour: when multiple distinct msg_ids share a
    /// hash slot they each get their own wheel entry, but the wheel
    /// entry only carries the hash — not the full id. On expiration
    /// we pop one id from the front of the slot (FIFO). For two ids
    /// inserted close in time this is correct to within ±1 tick; for
    /// ids inserted far apart the lagging id may be retired up to
    /// `wheel.span()` ticks early. Acceptable: the collision rate at
    /// 64-bit foldhash on bounded id-space (msg_ids per stream within
    /// a 5-minute window) is dominated by the birthday bound — even at
    /// 10M ids/window the expected collision count is ~3 × 10⁻⁶.
    pub fn tick(&mut self) {
        self.wheel.advance_into(&mut self.drain_buf);
        for e in self.drain_buf.drain(..) {
            let key = (StreamId(e.stream_id), e.msg_id_hash);
            if let Some(ids) = self.seen.get_mut(&key) {
                if !ids.is_empty() {
                    ids.remove(0); // FIFO retire — see doc note
                }
                if ids.is_empty() {
                    self.seen.remove(&key);
                }
            }
        }
    }

    /// Number of live (not-yet-expired) hash slots. For metrics. M2:
    /// post hash-collision handling this is "slots", not "ids" — they
    /// differ only when distinct ids landed in the same slot, which is
    /// astronomically rare at FoldHash64 strength.
    #[inline]
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// Number of live tracked ids (sum across collision lists). For
    /// tests that need to assert the post-collision shape.
    #[inline]
    pub fn id_count(&self) -> usize {
        self.seen.values().map(|v| v.len()).sum()
    }

    /// True when nothing is being tracked. Lets the shard worker
    /// drop the tracker back to `None` after a long idle period if
    /// it ever wants to (we don't do that today — the tracker stays
    /// alive once allocated).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

impl Default for IdempotencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(n: u32) -> StreamId {
        StreamId(n)
    }

    #[test]
    fn first_record_returns_true_repeat_returns_false() {
        let mut t = IdempotencyTracker::new();
        assert!(t.record(s(1), 0xABC, b"id-1", 1000), "first must be true");
        assert!(!t.record(s(1), 0xABC, b"id-1", 1000), "duplicate must be false");
        assert!(t.contains(s(1), 0xABC, b"id-1"));
    }

    #[test]
    fn different_streams_isolated() {
        let mut t = IdempotencyTracker::new();
        t.record(s(1), 0xABC, b"id", 1000);
        // Same hash but different stream — must NOT collide.
        assert!(t.record(s(2), 0xABC, b"id", 1000), "different stream is independent");
        assert!(t.contains(s(1), 0xABC, b"id"));
        assert!(t.contains(s(2), 0xABC, b"id"));
    }

    #[test]
    fn entry_expires_after_window() {
        let mut t = IdempotencyTracker::new();
        // Window 1500 ms → 2 ticks (rounded up). Should still be
        // present after 1 tick, gone after 2.
        t.record(s(1), 0xDEAD, b"x", 1500);
        assert!(t.contains(s(1), 0xDEAD, b"x"));
        t.tick();
        assert!(t.contains(s(1), 0xDEAD, b"x"), "still alive after 1s of a 1.5s window");
        t.tick();
        assert!(!t.contains(s(1), 0xDEAD, b"x"), "must be gone after 2s of a 1.5s window");
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn repeat_record_does_not_slide_window() {
        // An attacker that keeps re-publishing the same id mustn't
        // be able to extend its lifetime. Once recorded with a
        // window, it expires on the original schedule regardless of
        // subsequent record calls.
        let mut t = IdempotencyTracker::new();
        t.record(s(1), 0xFEED, b"a", 1500); // expires at tick 2
        t.tick();                                     // tick 1
        let _ = t.record(s(1), 0xFEED, b"a", 9999); // sliding attempt
        t.tick();                                     // tick 2
        assert!(!t.contains(s(1), 0xFEED, b"a"), "must expire on the ORIGINAL schedule");
    }

    #[test]
    fn window_clamped_to_max() {
        let mut t = IdempotencyTracker::new();
        // Ask for a 1-year window. Tracker must clamp to MAX_WINDOW_MS.
        t.record(s(1), 0xFFFF, b"x", u32::MAX);
        // Advance past the clamp + 1.
        for _ in 0..((MAX_WINDOW_MS as usize / TICK_MS as usize) + 1) {
            t.tick();
        }
        assert!(
            !t.contains(s(1), 0xFFFF, b"x"),
            "window must be clamped to MAX_WINDOW_MS = {}ms",
            MAX_WINDOW_MS,
        );
    }

    #[test]
    fn many_entries_tick_drains_correct_bucket() {
        let mut t = IdempotencyTracker::new();
        // Three entries with three different windows.
        t.record(s(1), 1, b"a", 1000); // tick 1
        t.record(s(1), 2, b"b", 2000); // tick 2
        t.record(s(1), 3, b"c", 3000); // tick 3
        assert_eq!(t.len(), 3);

        t.tick(); // tick 1 — entry 1 expires
        assert!(!t.contains(s(1), 1, b"a"));
        assert!(t.contains(s(1), 2, b"b"));
        assert!(t.contains(s(1), 3, b"c"));
        assert_eq!(t.len(), 2);

        t.tick(); // tick 2 — entry 2 expires
        assert!(!t.contains(s(1), 2, b"b"));
        assert!(t.contains(s(1), 3, b"c"));
        assert_eq!(t.len(), 1);

        t.tick(); // tick 3 — entry 3 expires
        assert!(!t.contains(s(1), 3, b"c"));
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn empty_tick_is_a_noop() {
        let mut t = IdempotencyTracker::new();
        for _ in 0..10 {
            t.tick();
        }
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn hash_collision_does_not_dedup() {
        // M2: two distinct msg_ids that happen to hash to the same u64
        // must both be tracked as independent ids — neither one
        // shadows the other. We simulate a collision by passing the
        // same hash for two different id strings.
        let mut t = IdempotencyTracker::new();
        let hash = 0xC011_1510u64;
        assert!(t.record(s(1), hash, b"id-A", 1000));
        // Second call: same hash, different id. Must NOT be reported
        // as a duplicate — that would silently drop a legitimate
        // publish.
        assert!(t.record(s(1), hash, b"id-B", 1000), "distinct id at same hash must record");
        assert!(t.contains(s(1), hash, b"id-A"));
        assert!(t.contains(s(1), hash, b"id-B"));
        assert!(!t.contains(s(1), hash, b"id-C"));
        // One hash slot, two ids stored.
        assert_eq!(t.len(), 1);
        assert_eq!(t.id_count(), 2);

        // A *true* duplicate of either id is still detected.
        assert!(!t.record(s(1), hash, b"id-A", 1000));
        assert!(!t.record(s(1), hash, b"id-B", 1000));
    }

    #[test]
    fn forget_only_removes_matching_id_in_collision_slot() {
        let mut t = IdempotencyTracker::new();
        let hash = 0xDEAD_BEEFu64;
        t.record(s(1), hash, b"a", 1000);
        t.record(s(1), hash, b"b", 1000);
        t.forget(s(1), hash, b"a");
        assert!(!t.contains(s(1), hash, b"a"));
        assert!(t.contains(s(1), hash, b"b"), "non-matching id stays");
    }

    /// T18 — `forget()` leaves the wheel entry in place by design (see
    /// the docstring). The wheel must NOT panic or break the dedup
    /// contract when it later fires for a key that's already gone from
    /// `seen` because the caller forgot it. The two structures are
    /// allowed to drift on `forget`, but `tick()` must remain a
    /// no-op-on-miss instead of corrupting state.
    #[test]
    fn t18_forget_does_not_desync_wheel_tick() {
        let mut t = IdempotencyTracker::new();
        // Record + forget the same key immediately. The wheel still
        // holds a phantom entry for it; `seen` is empty.
        t.record(s(1), 0xAAA1, b"x", 1000);
        t.forget(s(1), 0xAAA1, b"x");
        assert!(!t.contains(s(1), 0xAAA1, b"x"));
        assert_eq!(t.len(), 0);
        assert_eq!(t.id_count(), 0);

        // Tick past the wheel slot — the wheel will pop the phantom
        // entry. It must NOT panic and must NOT corrupt `seen` (which
        // could re-introduce the forgotten id with a wrong count).
        t.tick();
        assert!(!t.contains(s(1), 0xAAA1, b"x"));
        assert_eq!(t.len(), 0);
        assert_eq!(t.id_count(), 0);
    }

    /// T18 follow-up — forget one id from a collision slot, leaving
    /// the other. When the wheel later fires it must retire ONE entry
    /// (the surviving id), not double-retire.
    #[test]
    fn t18_forget_one_of_two_then_tick_retires_survivor() {
        let mut t = IdempotencyTracker::new();
        let hash = 0xBEEF_0001u64;
        t.record(s(1), hash, b"alpha", 1000);
        t.record(s(1), hash, b"beta", 1000);
        // Forget the first one. Wheel still has TWO entries (one for
        // each `record` call). `seen` has one id under the hash key.
        t.forget(s(1), hash, b"alpha");
        assert_eq!(t.id_count(), 1);
        // Tick past both. First tick pops the phantom (alpha) — should
        // FIFO-retire the surviving id (beta), per the documented
        // contract in `tick()`. Then the second tick pops the real beta
        // entry but the slot is already gone, so it's a no-op.
        t.tick();
        t.tick();
        assert!(!t.contains(s(1), hash, b"beta"));
        assert_eq!(t.id_count(), 0);
    }
}
