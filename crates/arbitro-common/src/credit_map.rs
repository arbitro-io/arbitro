use core::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use crate::subject::subject_matches;

/// FNV-1a hash → u32. Deterministic, zero-alloc.
#[inline(always)]
fn hash_subject(data: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in data {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    if h == 0 { 1 } else { h } // 0 is reserved for empty slot
}

// ── InFlightRing ────────────────────────────────────────────────────────────

/// O(1) seq → slot_idx map. Power-of-two capacity.
struct InFlightRing {
    seqs:   Box<[AtomicU64]>,
    slots:  Box<[AtomicU32]>, // Maps to HashTable index
    conns:  Box<[AtomicU64]>,
    mask:   usize,
}

impl InFlightRing {
    fn new(max_pending: u32) -> Self {
        let cap = (max_pending as usize * 2).next_power_of_two().max(16);
        Self {
            seqs:   (0..cap).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice(),
            slots:  (0..cap).map(|_| AtomicU32::new(0)).collect::<Vec<_>>().into_boxed_slice(),
            conns:  (0..cap).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice(),
            mask:   cap - 1,
        }
    }

    #[inline]
    fn insert(&self, seq: u64, slot_idx: u32, conn_id: u64) {
        let s = (seq as usize) & self.mask;
        self.slots[s].store(slot_idx, Ordering::Relaxed);
        self.conns[s].store(conn_id, Ordering::Relaxed);
        self.seqs[s].store(seq, Ordering::Release);
    }

    #[inline]
    fn remove(&self, seq: u64) -> Option<u32> {
        let s = (seq as usize) & self.mask;
        self.seqs[s]
            .compare_exchange(seq, 0, Ordering::AcqRel, Ordering::Relaxed)
            .ok()
            .map(|_| self.slots[s].load(Ordering::Relaxed))
    }
}

// ── SubjectSlot (one cache line) ────────────────────────────────────────────

/// Dynamic slot for an active subject.
#[repr(C, align(64))]
struct SubjectSlot {
    hash:        AtomicU32, // 0 = empty
    pending:     AtomicU32,
    pattern_idx: AtomicU8,  // 255 = no policy
    _pad:        [u8; 55],
}

/// Cold metadata — only for policy matching.
struct CreditSlotCold {
    pattern: Box<[u8]>,
    limit:   u32,
}

// ── CreditMap ───────────────────────────────────────────────────────────────

/// Dynamic CreditMap with Per-Subject Isolation.
pub struct CreditMap {
    in_flight: InFlightRing,
    slots:     Box<[SubjectSlot]>,
    mask:      u32,
    cold:      Box<[CreditSlotCold]>,
}

impl CreditMap {
    pub fn new(rules: &[(impl AsRef<[u8]>, u32)], max_pending: u32) -> Self {
        let mut cold = Vec::with_capacity(rules.len());
        for (pattern, limit) in rules {
            cold.push(CreditSlotCold {
                pattern: Box::from(pattern.as_ref()),
                limit:   *limit,
            });
        }

        let size = (max_pending * 2).next_power_of_two().max(64);
        let mut slots = Vec::with_capacity(size as usize);
        for _ in 0..size {
            slots.push(SubjectSlot {
                hash:        AtomicU32::new(0),
                pending:     AtomicU32::new(0),
                pattern_idx: AtomicU8::new(255),
                _pad:        [0u8; 55],
            });
        }

        Self {
            in_flight: InFlightRing::new(max_pending),
            slots:     slots.into_boxed_slice(),
            mask:      size - 1,
            cold:      cold.into_boxed_slice(),
        }
    }

    pub fn from_limits(limits: &[arbitro_proto::config::SubjectLimit], max_pending: u32) -> Self {
        let rules: Vec<(&[u8], u32)> = limits.iter()
            .map(|sl| (sl.pattern.as_ref(), sl.limit))
            .collect();
        Self::new(&rules, max_pending)
    }

    pub fn try_acquire(&self, subject: &[u8], seq: u64, conn_id: u64) -> bool {
        let h = hash_subject(subject);
        let mut idx = (h & self.mask) as usize;

        for _ in 0..16 { // Linear probing
            let slot = &self.slots[idx];
            let existing = slot.hash.load(Ordering::Acquire);

            if existing == h {
                return self.acquire_in_slot(idx, seq, conn_id);
            }

            if existing == 0 {
                // Claim empty slot
                if slot.hash.compare_exchange(0, h, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                    let p_idx = self.find_policy_idx(subject);
                    slot.pattern_idx.store(p_idx, Ordering::Relaxed);
                    return self.acquire_in_slot(idx, seq, conn_id);
                }
            }
            idx = (idx + 1) & (self.mask as usize);
        }
        false
    }

    #[inline(always)]
    fn acquire_in_slot(&self, idx: usize, seq: u64, conn_id: u64) -> bool {
        let slot = &self.slots[idx];
        let p_idx = slot.pattern_idx.load(Ordering::Relaxed);
        let limit = if p_idx == 255 {
            u32::MAX
        } else {
            self.cold[p_idx as usize].limit
        };

        let mut v = slot.pending.load(Ordering::Acquire);
        loop {
            if v >= limit { return false }
            match slot.pending.compare_exchange_weak(v, v + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    self.in_flight.insert(seq, idx as u32, conn_id);
                    return true;
                }
                Err(next_v) => v = next_v,
            }
        }
    }

    #[inline]
    pub fn release(&self, seq: u64) {
        if let Some(idx) = self.in_flight.remove(seq) {
            let slot = &self.slots[idx as usize];
            let prev = slot.pending.fetch_sub(1, Ordering::AcqRel);
            
            // RECYCLING: Clear slot if this was the last inflight message for this subject
            if prev == 1 {
                // Optimization: store 0 to hash to allow others to use the slot
                slot.hash.store(0, Ordering::Release);
            }
        }
    }

    #[inline]
    pub fn has_credit(&self, subject: &[u8]) -> bool {
        let h = hash_subject(subject);
        let mut idx = (h & self.mask) as usize;
        for _ in 0..16 {
            let slot = &self.slots[idx];
            let existing = slot.hash.load(Ordering::Acquire);
            if existing == h {
                let p_idx = slot.pattern_idx.load(Ordering::Relaxed);
                let limit = if p_idx == 255 { u32::MAX } else { self.cold[p_idx as usize].limit };
                return slot.pending.load(Ordering::Acquire) < limit;
            }
            if existing == 0 { return true }
            idx = (idx + 1) & (self.mask as usize);
        }
        true
    }

    fn find_policy_idx(&self, subject: &[u8]) -> u8 {
        for (i, c) in self.cold.iter().enumerate() {
            if subject_matches(&c.pattern, subject) {
                return i as u8;
            }
        }
        255
    }

    pub fn scavenge(&self, conn_id: u64) -> Vec<u64> {
        let mut rescued = Vec::new();
        for s in 0..=self.in_flight.mask {
            let actual_conn = self.in_flight.conns[s].load(Ordering::Relaxed);
            if actual_conn == conn_id {
                let seq = self.in_flight.seqs[s].swap(0, Ordering::AcqRel);
                if seq != 0 {
                    let slot_idx = self.in_flight.slots[s].load(Ordering::Relaxed);
                    let slot = &self.slots[slot_idx as usize];
                    let prev = slot.pending.fetch_sub(1, Ordering::AcqRel);
                    if prev == 1 {
                        slot.hash.store(0, Ordering::Release);
                    }
                    rescued.push(seq);
                }
            }
        }
        rescued
    }

    #[inline]
    pub fn is_empty(&self) -> bool { self.cold.is_empty() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules() -> Vec<(&'static [u8], u32)> {
        vec![
            (b"orders.premium.>".as_slice(), 2),
            (b"orders.basic.>".as_slice(), 1),
        ]
    }

    #[test]
    fn subject_isolation_under_same_pattern() {
        let cm = CreditMap::new(&rules(), 128);
        
        // user_1 takes their 2 credits
        assert!(cm.try_acquire(b"orders.premium.user_1", 1, 42));
        assert!(cm.try_acquire(b"orders.premium.user_1", 2, 42));
        assert!(!cm.try_acquire(b"orders.premium.user_1", 3, 42)); // reached limited

        // user_2 matches SAME pattern but should have OWN 2 credits
        assert!(cm.try_acquire(b"orders.premium.user_2", 4, 42));
        assert!(cm.try_acquire(b"orders.premium.user_2", 5, 42));
        assert!(!cm.try_acquire(b"orders.premium.user_2", 6, 42));
    }

    #[test]
    fn state_recycling_on_release() {
        let cm = CreditMap::new(&rules(), 128);
        
        // Acquire and release
        assert!(cm.try_acquire(b"orders.basic.user_1", 1, 42));
        assert!(!cm.try_acquire(b"orders.basic.user_1", 2, 42)); // limit 1
        
        cm.release(1);
        
        // Slot should be recycled and available again
        assert!(cm.try_acquire(b"orders.basic.user_1", 3, 42));
    }

    #[test]
    fn ring_u32_slots() {
        let ring = InFlightRing::new(64);
        for seq in 1..=64u64 {
            ring.insert(seq, seq as u32, 42);
        }
        for seq in 1..=64u64 {
            assert_eq!(ring.remove(seq), Some(seq as u32));
        }
    }

    #[test]
    fn collision_probing() {
        // Very small table to force collisions
        let cm = CreditMap::new(&rules(), 4); // size 16
        
        for i in 0..10 {
            let subj = format!("user_{}", i);
            assert!(cm.try_acquire(subj.as_bytes(), i as u64, 42));
        }
    }

    #[test]
    fn test_fast_massive_isolation() {
        let cm = CreditMap::new(&rules(), 10000);
        
        // Rapid 10k subject pass
        for i in 1..=10000u64 {
            let subj = format!("user_{}", i);
            assert!(cm.try_acquire(subj.as_bytes(), i, 42));
        }

        // Complete release/recycling pass
        for i in 1..=10000u64 {
            cm.release(i);
        }

        // Verify isolation is still functioning after massive recycling
        assert!(cm.try_acquire(b"user_new", 99999, 42));
    }
}
