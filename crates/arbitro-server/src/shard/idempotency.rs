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

use arbitro_engine_v2::types::StreamId;
use arbitro_common::{TimingWheel, foldhash::fast::FixedState};

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
pub struct IdempotencyTracker {
    seen: HashMap<(StreamId, u64), (), FixedState>,
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

    /// True if `(stream, msg_id_hash)` was recorded recently and
    /// hasn't expired yet. O(1) average — single HashMap lookup.
    #[inline]
    pub fn contains(&self, stream: StreamId, msg_id_hash: u64) -> bool {
        self.seen.contains_key(&(stream, msg_id_hash))
    }

    /// Insert `(stream, msg_id_hash)` with an expiration `window_ms`
    /// in the future. Returns `true` if this is the first time we've
    /// seen the key, `false` if it was already recorded (duplicate).
    ///
    /// The window is clamped to `MAX_WINDOW_MS`. Repeat-records do
    /// NOT extend the existing entry's lifetime — the original
    /// expiration stands.
    #[inline]
    pub fn record(&mut self, stream: StreamId, msg_id_hash: u64, window_ms: u32) -> bool {
        // O(1) check + insert in one operation. `try_insert` would be
        // cleaner but it's nightly-only; `entry().or_insert` is the
        // stable equivalent that avoids the double lookup.
        use std::collections::hash_map::Entry;
        match self.seen.entry((stream, msg_id_hash)) {
            Entry::Occupied(_) => false,
            Entry::Vacant(slot) => {
                slot.insert(());
                // Schedule removal. Round window up to whole seconds
                // (wheel resolution is 1s).
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
                true
            }
        }
    }

    /// Advance the wheel by one tick (1 s). Removes from `seen` every
    /// entry that just expired. Designed to be called from the shard
    /// worker's existing tick loop — same cadence as the ack wheel,
    /// no new timer.
    pub fn tick(&mut self) {
        self.wheel.advance_into(&mut self.drain_buf);
        for e in self.drain_buf.drain(..) {
            self.seen.remove(&(StreamId(e.stream_id), e.msg_id_hash));
        }
    }

    /// Number of live (not-yet-expired) entries. For metrics.
    #[inline]
    pub fn len(&self) -> usize {
        self.seen.len()
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
        assert!(t.record(s(1), 0xABC, 1000), "first must be true");
        assert!(!t.record(s(1), 0xABC, 1000), "duplicate must be false");
        assert!(t.contains(s(1), 0xABC));
    }

    #[test]
    fn different_streams_isolated() {
        let mut t = IdempotencyTracker::new();
        t.record(s(1), 0xABC, 1000);
        // Same hash but different stream — must NOT collide.
        assert!(t.record(s(2), 0xABC, 1000), "different stream is independent");
        assert!(t.contains(s(1), 0xABC));
        assert!(t.contains(s(2), 0xABC));
    }

    #[test]
    fn entry_expires_after_window() {
        let mut t = IdempotencyTracker::new();
        // Window 1500 ms → 2 ticks (rounded up). Should still be
        // present after 1 tick, gone after 2.
        t.record(s(1), 0xDEAD, 1500);
        assert!(t.contains(s(1), 0xDEAD));
        t.tick();
        assert!(t.contains(s(1), 0xDEAD), "still alive after 1s of a 1.5s window");
        t.tick();
        assert!(!t.contains(s(1), 0xDEAD), "must be gone after 2s of a 1.5s window");
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn repeat_record_does_not_slide_window() {
        // An attacker that keeps re-publishing the same id mustn't
        // be able to extend its lifetime. Once recorded with a
        // window, it expires on the original schedule regardless of
        // subsequent record calls.
        let mut t = IdempotencyTracker::new();
        t.record(s(1), 0xFEED, 1500); // expires at tick 2
        t.tick();                     // tick 1
        let _ = t.record(s(1), 0xFEED, 9999); // sliding attempt
        t.tick();                     // tick 2
        assert!(!t.contains(s(1), 0xFEED), "must expire on the ORIGINAL schedule");
    }

    #[test]
    fn window_clamped_to_max() {
        let mut t = IdempotencyTracker::new();
        // Ask for a 1-year window. Tracker must clamp to MAX_WINDOW_MS.
        t.record(s(1), 0xFFFF, u32::MAX);
        // Advance past the clamp + 1.
        for _ in 0..((MAX_WINDOW_MS as usize / TICK_MS as usize) + 1) {
            t.tick();
        }
        assert!(
            !t.contains(s(1), 0xFFFF),
            "window must be clamped to MAX_WINDOW_MS = {}ms",
            MAX_WINDOW_MS,
        );
    }

    #[test]
    fn many_entries_tick_drains_correct_bucket() {
        let mut t = IdempotencyTracker::new();
        // Three entries with three different windows.
        t.record(s(1), 1, 1000); // tick 1
        t.record(s(1), 2, 2000); // tick 2
        t.record(s(1), 3, 3000); // tick 3
        assert_eq!(t.len(), 3);

        t.tick(); // tick 1 — entry 1 expires
        assert!(!t.contains(s(1), 1));
        assert!(t.contains(s(1), 2));
        assert!(t.contains(s(1), 3));
        assert_eq!(t.len(), 2);

        t.tick(); // tick 2 — entry 2 expires
        assert!(!t.contains(s(1), 2));
        assert!(t.contains(s(1), 3));
        assert_eq!(t.len(), 1);

        t.tick(); // tick 3 — entry 3 expires
        assert!(!t.contains(s(1), 3));
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
}
