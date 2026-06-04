//! ConsumerSubjects — per-consumer subject inflight tracker.
//!
//! Owned by the drain OS thread; mutated only on the drain thread. The
//! command thread sends decrement events via `DrainEvent::Ack` through the
//! drain-event ring (`drain_events.rs`), so neither atomics nor locks are
//! needed on the hot path.
//!
//! Subject hashes are already `foldhash::wire_hash_32` outputs, so the map
//! uses `nohash_hasher::BuildNoHashHasher<u32>` to skip a second hash.
//!
//! Encapsulated API (`can` / `inc` / `dec` / `total`…) is the only entry
//! point from drain/worker — internal representation can change later
//! (e.g. ahash, slot table, intrusive list) without touching callers.

use nohash_hasher::BuildNoHashHasher;
use std::collections::HashMap;

/// Per-consumer subject inflight counters.
///
/// Two invariants:
/// 1. Entries are removed when their count reaches 0 (bounded by
///    working-set, not lifetime).
/// 2. `total` is the sum of all entry counts (kept in lockstep with
///    every inc/dec/dec_by/clear for O(1) reads).
#[derive(Default)]
pub struct ConsumerSubjects {
    inflight: HashMap<u32, u32, BuildNoHashHasher<u32>>,
    total: u32,
}

impl ConsumerSubjects {
    pub fn new() -> Self {
        Self::default()
    }

    /// True if this subject has capacity for one more delivery under
    /// `max`. Missing key = 0 inflight = always has capacity.
    #[inline]
    pub fn can(&self, hash: u32, max: u32) -> bool {
        self.get(hash) < max
    }

    /// Current inflight count for `hash`. 0 if not tracked.
    #[inline]
    pub fn get(&self, hash: u32) -> u32 {
        self.inflight.get(&hash).copied().unwrap_or(0)
    }

    /// Total inflight across all subjects for this consumer.
    #[inline]
    pub fn total(&self) -> u32 {
        self.total
    }

    /// Number of distinct subjects currently tracked. O(1).
    #[inline]
    pub fn distinct_subjects(&self) -> usize {
        self.inflight.len()
    }

    /// Increment subject count by 1. Returns the new count.
    #[inline]
    pub fn inc(&mut self, hash: u32) -> u32 {
        let entry = self.inflight.entry(hash).or_insert(0);
        *entry += 1;
        self.total += 1;
        *entry
    }

    /// Decrement subject count by 1. Removes the entry on 0. Returns the
    /// new count (0 if the entry was missing).
    #[inline]
    pub fn dec(&mut self, hash: u32) -> u32 {
        let Some(c) = self.inflight.get_mut(&hash) else {
            return 0;
        };
        if *c == 0 {
            return 0;
        }
        *c -= 1;
        self.total -= 1;
        let new = *c;
        if new == 0 {
            self.inflight.remove(&hash);
        }
        new
    }

    /// Decrement subject count by `n`, clamped at 0. Removes entry on 0.
    /// Returns the new count.
    #[inline]
    pub fn dec_by(&mut self, hash: u32, n: u32) -> u32 {
        let Some(c) = self.inflight.get_mut(&hash) else {
            return 0;
        };
        let delta = (*c).min(n);
        *c -= delta;
        self.total = self.total.saturating_sub(delta);
        let new = *c;
        if new == 0 {
            self.inflight.remove(&hash);
        }
        new
    }

    /// Drop all tracked subjects for this consumer. Used on retire.
    pub fn clear(&mut self) {
        self.inflight.clear();
        self.total = 0;
    }

    /// Materialise current state as `(subject_hash, count)` pairs.
    /// Cold path only — used by `getConsumerStats` wire op.
    pub fn snapshot(&self) -> Vec<(u32, u32)> {
        self.inflight.iter().map(|(&h, &c)| (h, c)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_state_is_zero() {
        let s = ConsumerSubjects::new();
        assert_eq!(s.total(), 0);
        assert_eq!(s.distinct_subjects(), 0);
        assert_eq!(s.get(0xBEEF), 0);
        assert!(s.can(0xBEEF, 1));
    }

    #[test]
    fn inc_increments_and_totals() {
        let mut s = ConsumerSubjects::new();
        assert_eq!(s.inc(0xBEEF), 1);
        assert_eq!(s.inc(0xBEEF), 2);
        assert_eq!(s.inc(0xDEAD), 1);
        assert_eq!(s.get(0xBEEF), 2);
        assert_eq!(s.get(0xDEAD), 1);
        assert_eq!(s.total(), 3);
        assert_eq!(s.distinct_subjects(), 2);
    }

    #[test]
    fn dec_decrements_and_removes_on_zero() {
        let mut s = ConsumerSubjects::new();
        s.inc(0xBEEF);
        s.inc(0xBEEF);
        assert_eq!(s.dec(0xBEEF), 1);
        assert_eq!(s.total(), 1);
        assert_eq!(s.distinct_subjects(), 1);
        assert_eq!(s.dec(0xBEEF), 0);
        assert_eq!(s.total(), 0);
        assert_eq!(s.distinct_subjects(), 0, "entry must be removed at zero");
    }

    #[test]
    fn dec_on_missing_is_noop() {
        let mut s = ConsumerSubjects::new();
        assert_eq!(s.dec(0xBEEF), 0);
        assert_eq!(s.total(), 0);
    }

    #[test]
    fn dec_by_clamps_at_zero() {
        let mut s = ConsumerSubjects::new();
        s.inc(0xBEEF);
        s.inc(0xBEEF);
        s.inc(0xBEEF);
        assert_eq!(s.dec_by(0xBEEF, 5), 0, "clamped at zero");
        assert_eq!(s.total(), 0);
        assert_eq!(s.distinct_subjects(), 0, "removed at zero");
    }

    #[test]
    fn can_respects_max() {
        let mut s = ConsumerSubjects::new();
        assert!(s.can(0xBEEF, 1));
        s.inc(0xBEEF);
        assert!(!s.can(0xBEEF, 1));
        assert!(s.can(0xBEEF, 2));
    }

    #[test]
    fn snapshot_lists_all_entries() {
        let mut s = ConsumerSubjects::new();
        s.inc(0x1);
        s.inc(0x1);
        s.inc(0x2);
        let mut snap = s.snapshot();
        snap.sort();
        assert_eq!(snap, vec![(0x1, 2), (0x2, 1)]);
    }

    #[test]
    fn clear_resets_state() {
        let mut s = ConsumerSubjects::new();
        s.inc(0x1);
        s.inc(0x2);
        s.clear();
        assert_eq!(s.total(), 0);
        assert_eq!(s.distinct_subjects(), 0);
    }
}
