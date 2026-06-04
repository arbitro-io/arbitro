//! IdPool — dense slot allocator with free list + generation tags.
//!
//! Level 0 — zero dependencies beyond std.
//!
//! ## Why this exists
//!
//! Arbitro's sequential IDs (stream, consumer, queue, connection, binding)
//! are used as **direct Vec indices** in the hot path:
//!
//! ```text
//! counters.consumer[consumer_id as usize].fetch_add(1)   // ~1 ns
//! match_tables[stream_id as usize]                        // ~1 ns
//! bindings[binding_idx as usize]                          // ~1 ns
//! ```
//!
//! Without recycling, every create/delete cycle grows the slot space
//! monotonically. After enough churn the Vec's length outgrows the working
//! set, wasting memory and eventually saturating.
//!
//! `IdPool` solves this by maintaining a **free list** of released slots
//! and recycling them on the next `alloc`. Dense indexing is preserved —
//! `active_count()` ≤ `slot_count()` and slot_count is bounded by peak
//! working set, not by total lifetime ops.
//!
//! ## Generation tags (anti-ABA)
//!
//! Reuse alone is unsafe when IDs are embedded in persisted data (e.g.
//! `stream_id` in a store entry). If stream slot 5 is freed and
//! reallocated to a new stream, an in-flight message from the old stream
//! must be rejected. The pool bumps `generations[slot]` on every alloc
//! from the free list so callers can embed `(slot, gen)` pairs and
//! validate freshness via `is_current(slot, gen)`.
//!
//! ## Invariants
//!
//! - `generations.len() == slot_count()` (one gen per existing slot)
//! - `free_slots ⊆ 0..generations.len()` (no dangling slots in free list)
//! - Same slot appears at most once in `free_slots` (no double-free)
//! - `generations[slot]` only ever increases (monotonic per-slot)

use std::collections::HashSet;

/// Error returned when the pool cannot allocate a new slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolError {
    /// All `max_slots` slots are currently allocated and none are free.
    Exhausted,
    /// Slot index is out of range for this pool.
    OutOfRange,
    /// Attempted to free a slot that was already free or never allocated.
    DoubleFree,
}

/// A dense slot allocator with recycling and generation tags.
///
/// **Not thread-safe.** Callers must provide external synchronization
/// (the engine owns the pool exclusively via `&mut self`).
#[derive(Debug, Clone)]
pub struct IdPool {
    /// Next slot index to hand out when the free list is empty.
    next_slot: u32,
    /// Slots returned via `free()`, available for reuse. LIFO for
    /// cache-warmth (most-recently-freed is most likely hot in L1).
    free_slots: Vec<u32>,
    /// Current generation per slot. Index = slot. Incremented on each
    /// alloc from the free list (slot reuse). Initial slots get gen=0.
    generations: Vec<u32>,
    /// Hard cap on total slots. `alloc` returns `Exhausted` if
    /// `next_slot == max_slots && free_slots.is_empty()`.
    max_slots: u32,
}

impl IdPool {
    /// Create a new pool with the given max capacity.
    #[inline]
    pub fn new(max_slots: u32) -> Self {
        Self {
            next_slot: 0,
            free_slots: Vec::new(),
            generations: Vec::new(),
            max_slots,
        }
    }

    /// Create a pool pre-sized for typical working set (avoids early
    /// `generations` reallocations).
    pub fn with_capacity(max_slots: u32, initial_capacity: usize) -> Self {
        Self {
            next_slot: 0,
            free_slots: Vec::with_capacity(initial_capacity / 4),
            generations: Vec::with_capacity(initial_capacity),
            max_slots,
        }
    }

    /// Allocate a slot. Prefers recycling freed slots; only grows
    /// `next_slot` when the free list is empty.
    ///
    /// Returns `(slot, generation)`. Callers that need anti-ABA should
    /// persist both and validate via `is_current` on use.
    pub fn alloc(&mut self) -> Result<(u32, u32), PoolError> {
        if let Some(slot) = self.free_slots.pop() {
            // Reuse: bump generation BEFORE handing out, so any residual
            // reference with the old gen is immediately stale.
            let idx = slot as usize;
            self.generations[idx] = self.generations[idx].wrapping_add(1);
            return Ok((slot, self.generations[idx]));
        }

        if self.next_slot >= self.max_slots {
            return Err(PoolError::Exhausted);
        }

        let slot = self.next_slot;
        self.next_slot += 1;
        self.generations.push(0);
        Ok((slot, 0))
    }

    /// Release a slot back to the pool. The next `alloc` may hand it out
    /// again with an incremented generation.
    ///
    /// Returns `DoubleFree` if the slot is already in the free list or
    /// was never allocated. `OutOfRange` if `slot >= slot_count()`.
    pub fn free(&mut self, slot: u32) -> Result<(), PoolError> {
        if (slot as usize) >= self.generations.len() {
            return Err(PoolError::OutOfRange);
        }
        // Linear scan is O(free_list.len()); acceptable for low-frequency
        // delete ops. For very large churn a HashSet could be used but
        // adds memory overhead.
        if self.free_slots.contains(&slot) {
            return Err(PoolError::DoubleFree);
        }
        self.free_slots.push(slot);
        Ok(())
    }

    /// True if `(slot, generation)` matches the current generation for
    /// `slot`. Used for anti-ABA validation on persisted IDs.
    ///
    /// Returns `false` if slot is out of range (never allocated).
    #[inline]
    pub fn is_current(&self, slot: u32, generation: u32) -> bool {
        self.generations
            .get(slot as usize)
            .copied()
            .is_some_and(|g| g == generation)
    }

    /// Current generation for a slot, or `None` if out of range.
    #[inline]
    pub fn generation(&self, slot: u32) -> Option<u32> {
        self.generations.get(slot as usize).copied()
    }

    /// Total slots ever handed out (including currently-free ones).
    #[inline]
    pub fn slot_count(&self) -> usize {
        self.generations.len()
    }

    /// Slots currently allocated (not in the free list).
    #[inline]
    pub fn active_count(&self) -> usize {
        self.generations.len() - self.free_slots.len()
    }

    /// Max capacity configured at construction.
    #[inline]
    pub fn max_slots(&self) -> u32 {
        self.max_slots
    }

    /// True if the free list has recyclable slots.
    #[inline]
    pub fn has_free(&self) -> bool {
        !self.free_slots.is_empty()
    }

    /// Snapshot for persistence / compaction. Callers serialize this
    /// and restore via `restore_from`.
    pub fn snapshot(&self) -> PoolSnapshot {
        PoolSnapshot {
            next_slot: self.next_slot,
            free_slots: self.free_slots.clone(),
            generations: self.generations.clone(),
            max_slots: self.max_slots,
        }
    }

    /// Restore from a snapshot. Validates invariants.
    pub fn restore_from(snapshot: PoolSnapshot) -> Result<Self, PoolError> {
        // Invariant checks
        if snapshot.next_slot as usize != snapshot.generations.len() {
            return Err(PoolError::OutOfRange);
        }
        let seen: HashSet<u32> = snapshot.free_slots.iter().copied().collect();
        if seen.len() != snapshot.free_slots.len() {
            // Duplicate in free list
            return Err(PoolError::DoubleFree);
        }
        if snapshot
            .free_slots
            .iter()
            .any(|&s| (s as usize) >= snapshot.generations.len())
        {
            return Err(PoolError::OutOfRange);
        }
        Ok(Self {
            next_slot: snapshot.next_slot,
            free_slots: snapshot.free_slots,
            generations: snapshot.generations,
            max_slots: snapshot.max_slots,
        })
    }

    /// Replay helper: apply a create op with the given `(slot, generation)`
    /// as observed in a command log record. Grows internal state as needed
    /// to match. Used during recovery.
    ///
    /// This is idempotent — calling twice with the same slot is a no-op
    /// on the second call (generation is already set).
    pub fn replay_alloc(&mut self, slot: u32, generation: u32) -> Result<(), PoolError> {
        if slot >= self.max_slots {
            return Err(PoolError::OutOfRange);
        }
        let idx = slot as usize;
        // Grow generations to cover this slot.
        while self.generations.len() <= idx {
            self.generations.push(0);
        }
        self.generations[idx] = generation;
        // Remove from free list if present (replay order may alloc after free).
        self.free_slots.retain(|&s| s != slot);
        // Bump next_slot to keep it the high-water mark.
        if slot + 1 > self.next_slot {
            self.next_slot = slot + 1;
        }
        Ok(())
    }

    /// Replay helper: apply a free op. Slot must have been replay_alloc'd
    /// first (otherwise `OutOfRange`). Generation is NOT bumped here —
    /// the next `alloc` bumps it when handed out.
    pub fn replay_free(&mut self, slot: u32) -> Result<(), PoolError> {
        if (slot as usize) >= self.generations.len() {
            return Err(PoolError::OutOfRange);
        }
        if self.free_slots.contains(&slot) {
            return Err(PoolError::DoubleFree);
        }
        self.free_slots.push(slot);
        Ok(())
    }
}

/// Serializable snapshot of a pool's state.
#[derive(Debug, Clone)]
pub struct PoolSnapshot {
    pub next_slot: u32,
    pub free_slots: Vec<u32>,
    pub generations: Vec<u32>,
    pub max_slots: u32,
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_sequential_from_empty_pool() {
        let mut pool = IdPool::new(10);
        assert_eq!(pool.alloc().unwrap(), (0, 0));
        assert_eq!(pool.alloc().unwrap(), (1, 0));
        assert_eq!(pool.alloc().unwrap(), (2, 0));
        assert_eq!(pool.active_count(), 3);
        assert_eq!(pool.slot_count(), 3);
    }

    #[test]
    fn free_then_alloc_recycles_with_bumped_generation() {
        let mut pool = IdPool::new(10);
        let (s0, g0) = pool.alloc().unwrap();
        assert_eq!((s0, g0), (0, 0));

        pool.free(s0).unwrap();
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool.slot_count(), 1);
        assert!(pool.has_free());

        // Reuse: same slot, incremented gen
        let (s1, g1) = pool.alloc().unwrap();
        assert_eq!((s1, g1), (0, 1));
        assert_eq!(pool.active_count(), 1);
        assert!(!pool.has_free());
    }

    #[test]
    fn is_current_detects_stale_generation() {
        let mut pool = IdPool::new(10);
        let (slot, gen) = pool.alloc().unwrap();
        assert!(pool.is_current(slot, gen));

        pool.free(slot).unwrap();
        pool.alloc().unwrap(); // reuse — bumps gen to 1

        // Old gen no longer current
        assert!(!pool.is_current(slot, gen));
        assert!(pool.is_current(slot, gen + 1));
    }

    #[test]
    fn exhaustion_returns_error() {
        let mut pool = IdPool::new(3);
        pool.alloc().unwrap();
        pool.alloc().unwrap();
        pool.alloc().unwrap();
        assert_eq!(pool.alloc().unwrap_err(), PoolError::Exhausted);
    }

    #[test]
    fn double_free_detected() {
        let mut pool = IdPool::new(10);
        let (s, _) = pool.alloc().unwrap();
        pool.free(s).unwrap();
        assert_eq!(pool.free(s).unwrap_err(), PoolError::DoubleFree);
    }

    #[test]
    fn out_of_range_free() {
        let mut pool = IdPool::new(10);
        assert_eq!(pool.free(5).unwrap_err(), PoolError::OutOfRange);
    }

    #[test]
    fn recycled_slots_preferred_over_new() {
        let mut pool = IdPool::new(10);
        let (s0, _) = pool.alloc().unwrap();
        let (s1, _) = pool.alloc().unwrap();
        let (_s2, _) = pool.alloc().unwrap();

        pool.free(s1).unwrap();
        pool.free(s0).unwrap();

        // Free list is LIFO — last freed first
        let (r0, _) = pool.alloc().unwrap();
        assert_eq!(r0, s0);
        let (r1, _) = pool.alloc().unwrap();
        assert_eq!(r1, s1);
        // Next is a fresh slot
        let (r2, _) = pool.alloc().unwrap();
        assert_eq!(r2, 3);

        // s2 was never freed — still unavailable
        assert_eq!(pool.generations.len(), 4);
    }

    #[test]
    fn snapshot_and_restore_preserves_state() {
        let mut pool = IdPool::new(10);
        pool.alloc().unwrap();
        pool.alloc().unwrap();
        pool.alloc().unwrap();
        pool.free(1).unwrap();
        pool.alloc().unwrap(); // reuse slot 1 → gen 1
        pool.free(0).unwrap();

        let snap = pool.snapshot();
        let restored = IdPool::restore_from(snap).unwrap();

        assert_eq!(restored.slot_count(), pool.slot_count());
        assert_eq!(restored.active_count(), pool.active_count());
        assert_eq!(restored.generation(0), pool.generation(0));
        assert_eq!(restored.generation(1), pool.generation(1));
        assert_eq!(restored.generation(2), pool.generation(2));
        assert_eq!(restored.has_free(), pool.has_free());
    }

    #[test]
    fn restore_rejects_invalid_snapshots() {
        // next_slot != generations.len()
        let bad = PoolSnapshot {
            next_slot: 5,
            free_slots: vec![],
            generations: vec![0, 0, 0],
            max_slots: 10,
        };
        assert!(IdPool::restore_from(bad).is_err());

        // Duplicate in free_slots
        let bad = PoolSnapshot {
            next_slot: 3,
            free_slots: vec![1, 1],
            generations: vec![0, 0, 0],
            max_slots: 10,
        };
        assert!(IdPool::restore_from(bad).is_err());

        // free_slots contains out-of-range slot
        let bad = PoolSnapshot {
            next_slot: 2,
            free_slots: vec![5],
            generations: vec![0, 0],
            max_slots: 10,
        };
        assert!(IdPool::restore_from(bad).is_err());
    }

    #[test]
    fn replay_reconstructs_from_log() {
        // Simulate replaying a command log:
        //   CreateStream{slot=0, gen=0}
        //   CreateStream{slot=1, gen=0}
        //   CreateStream{slot=2, gen=0}
        //   DeleteStream{slot=1}   → free 1
        //   CreateStream{slot=1, gen=1}  → reuse
        let mut pool = IdPool::new(10);
        pool.replay_alloc(0, 0).unwrap();
        pool.replay_alloc(1, 0).unwrap();
        pool.replay_alloc(2, 0).unwrap();
        pool.replay_free(1).unwrap();
        pool.replay_alloc(1, 1).unwrap();

        assert_eq!(pool.slot_count(), 3);
        assert_eq!(pool.active_count(), 3);
        assert_eq!(pool.generation(0), Some(0));
        assert_eq!(pool.generation(1), Some(1));
        assert_eq!(pool.generation(2), Some(0));
        assert!(!pool.has_free());
    }

    #[test]
    fn replay_then_alloc_continues_correctly() {
        let mut pool = IdPool::new(10);
        pool.replay_alloc(0, 0).unwrap();
        pool.replay_alloc(1, 0).unwrap();
        pool.replay_free(0).unwrap();

        // Now alloc should reuse slot 0 with gen 1
        let (slot, gen) = pool.alloc().unwrap();
        assert_eq!((slot, gen), (0, 1));

        // Next alloc extends
        let (slot, gen) = pool.alloc().unwrap();
        assert_eq!((slot, gen), (2, 0));
    }

    #[test]
    fn max_slots_bounds_allocation() {
        let mut pool = IdPool::new(2);
        pool.alloc().unwrap();
        pool.alloc().unwrap();
        assert!(pool.alloc().is_err());

        // After free, can alloc again
        pool.free(0).unwrap();
        assert!(pool.alloc().is_ok());
    }

    #[test]
    fn generation_wraps_on_overflow() {
        // Not a real concern (u32 = 4B reuses) but test the behavior
        let mut pool = IdPool::new(1);
        let (slot, _) = pool.alloc().unwrap();

        // Manually set generation near max to test wrap
        pool.generations[slot as usize] = u32::MAX;
        pool.free(slot).unwrap();
        let (_, gen) = pool.alloc().unwrap();
        assert_eq!(gen, 0); // wrapped
    }
}
