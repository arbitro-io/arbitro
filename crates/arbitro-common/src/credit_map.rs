//! `CreditMap` — per-subject credit ledger with O(1) inflight tracking.
//!
//! ## Design
//!
//! - **Pre-compiled patterns**: compiled once at subscribe time, matched N times.
//! - **InFlightRing**: power-of-two ring, O(1) insert/remove by `seq & mask`.
//! - **CreditSlot**: cache-line aligned (64B), one per pattern.
//! - **No `Instant::now()`** on the hot path.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};

use crate::subject::subject_matches;

// ── InFlightRing ────────────────────────────────────────────────────────────

/// O(1) seq → slot_idx map. Power-of-two capacity, zero-probe guaranteed
/// within the sliding window `[ack_seq, ack_seq + max_pending]`.
struct InFlightRing {
    seqs:   Box<[AtomicU64]>,
    groups: Box<[AtomicU8]>,
    conns:  Box<[AtomicU64]>, // Tracks ConnId per sequence
    mask:   usize,
}

impl InFlightRing {
    fn new(max_pending: u32) -> Self {
        let cap = (max_pending as usize * 2).next_power_of_two().max(16);
        Self {
            seqs:   (0..cap).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice(),
            groups: (0..cap).map(|_| AtomicU8::new(0)).collect::<Vec<_>>().into_boxed_slice(),
            conns:  (0..cap).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice(),
            mask:   cap - 1,
        }
    }

    #[inline]
    fn insert(&self, seq: u64, idx: u8, conn_id: u64) {
        let s = (seq as usize) & self.mask;
        self.groups[s].store(idx, Ordering::Relaxed);
        self.conns[s].store(conn_id, Ordering::Relaxed);
        self.seqs[s].store(seq, Ordering::Release);
    }

    #[inline]
    fn remove(&self, seq: u64) -> Option<u8> {
        let s = (seq as usize) & self.mask;
        self.seqs[s]
            .compare_exchange(seq, 0, Ordering::AcqRel, Ordering::Relaxed)
            .ok()
            .map(|_| self.groups[s].load(Ordering::Relaxed))
    }
}

// ── CreditSlot (one cache line) ────────────────────────────────────────────

/// Hot counters per pattern — exactly one L1 cache line (64 bytes).
#[repr(C, align(64))]
struct CreditSlot {
    pending:     AtomicU32,
    max_pending: u32,
    _pad:        [u8; 56],
}

const _: () = assert!(core::mem::size_of::<CreditSlot>() == 64);

/// Cold metadata — accessed only during pattern matching.
struct CreditSlotCold {
    pattern: Box<[u8]>,
}

// ── CreditMap ───────────────────────────────────────────────────────────────

/// Per-pattern credit ledger. Created once at subscribe time.
/// After construction all fields are immutable except atomic counters.
pub struct CreditMap {
    slots:     Box<[CreditSlot]>,
    cold:      Box<[CreditSlotCold]>,
    in_flight: InFlightRing,
}

impl CreditMap {
    /// Build from subject limit rules. Management path only.
    pub fn new(rules: &[(impl AsRef<[u8]>, u32)], max_pending: u32) -> Self {
        assert!(rules.len() <= 255, "CreditMap: max 255 patterns (slot index is u8)");
        let mut slots = Vec::with_capacity(rules.len());
        let mut cold = Vec::with_capacity(rules.len());
        for (pattern, limit) in rules {
            slots.push(CreditSlot {
                pending:     AtomicU32::new(0),
                max_pending: *limit,
                _pad:        [0u8; 56],
            });
            cold.push(CreditSlotCold {
                pattern: Box::from(pattern.as_ref()),
            });
        }
        CreditMap {
            slots:     slots.into_boxed_slice(),
            cold:      cold.into_boxed_slice(),
            in_flight: InFlightRing::new(max_pending),
        }
    }

    /// Build from SubjectLimit config.
    pub fn from_limits(limits: &[arbitro_proto::config::SubjectLimit], max_pending: u32) -> Self {
        let rules: Vec<(&[u8], u32)> = limits.iter()
            .map(|sl| (sl.pattern.as_ref(), sl.limit))
            .collect();
        Self::new(&rules, max_pending)
    }

    /// Find the first slot whose pattern matches `subject`.
    #[inline]
    fn find_slot(&self, subject: &[u8]) -> Option<usize> {
        self.cold.iter().position(|c| subject_matches(&c.pattern, subject))
    }

    /// Check if `subject` has room (peek, no mutation).
    #[inline]
    pub fn has_credit(&self, subject: &[u8]) -> bool {
        match self.find_slot(subject) {
            Some(idx) => {
                let s = &self.slots[idx];
                s.pending.load(Ordering::Relaxed) < s.max_pending
            }
            None => true,
        }
    }

    pub fn try_acquire(&self, subject: &[u8], seq: u64, conn_id: u64) -> bool {
        let Some(idx) = self.find_slot(subject) else { return true };
        let slot = &self.slots[idx];
        let ok = slot.pending
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |v| {
                if v < slot.max_pending { Some(v + 1) } else { None }
            })
            .is_ok();
        if ok {
            self.in_flight.insert(seq, idx as u8, conn_id);
        }
        ok
    }

    /// Scavenge credit for all sequences owned by `conn_id`.
    /// Used when a client disconnects without ACKing.
    /// Returns a list of rescued sequences (to be moved to nacked queue).
    pub fn scavenge(&self, conn_id: u64) -> Vec<u64> {
        let mut rescued = Vec::new();
        for s in 0..=self.in_flight.mask {
            let actual_conn = self.in_flight.conns[s].load(Ordering::Relaxed);
            if actual_conn == conn_id {
                let seq = self.in_flight.seqs[s].swap(0, Ordering::AcqRel);
                if seq != 0 {
                    let slot_idx = self.in_flight.groups[s].load(Ordering::Relaxed);
                    self.slots[slot_idx as usize].pending.fetch_sub(1, Ordering::AcqRel);
                    rescued.push(seq);
                }
            }
        }
        rescued
    }

    /// Release credit for `seq`. O(1): ring lookup → fetch_sub.
    #[inline]
    pub fn release(&self, seq: u64) {
        if let Some(idx) = self.in_flight.remove(seq) {
            self.slots[idx as usize].pending.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Number of patterns.
    #[inline]
    pub fn is_empty(&self) -> bool { self.slots.is_empty() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules() -> Vec<(&'static [u8], u32)> {
        vec![
            (b"orders.created".as_slice(), 5),
            (b"orders.updated".as_slice(), 5),
            (b"orders.>".as_slice(), 20),
        ]
    }

    #[test]
    fn has_credit_initially() {
        let cm = CreditMap::new(&rules(), 128);
        assert!(cm.has_credit(b"orders.created"));
    }

    #[test]
    fn unmatched_always_has_credit() {
        let cm = CreditMap::new(&rules(), 128);
        assert!(cm.has_credit(b"payments.done"));
    }

    #[test]
    fn acquire_blocks_at_limit() {
        let cm = CreditMap::new(&rules(), 128);
        for seq in 1..=5u64 {
            assert!(cm.try_acquire(b"orders.created", seq, 42));
        }
        assert!(!cm.try_acquire(b"orders.created", 6, 42));
        assert!(cm.try_acquire(b"orders.updated", 7, 42));
    }

    #[test]
    fn release_restores_credit() {
        let cm = CreditMap::new(&rules(), 128);
        for seq in 1..=5u64 {
            assert!(cm.try_acquire(b"orders.created", seq, 42));
        }
        assert!(!cm.try_acquire(b"orders.created", 6, 42));
        cm.release(3);
        assert!(cm.try_acquire(b"orders.created", 7, 42));
    }

    #[test]
    fn release_unknown_is_noop() {
        let cm = CreditMap::new(&rules(), 128);
        cm.release(9999);
        assert!(cm.has_credit(b"orders.created"));
    }

    #[test]
    fn ring_no_collision_within_window() {
        let ring = InFlightRing::new(64);
        for seq in 1..=64u64 {
            ring.insert(seq, (seq % 4) as u8, 42);
        }
        for seq in 1..=64u64 {
            assert_eq!(ring.remove(seq), Some((seq % 4) as u8));
        }
    }
}
