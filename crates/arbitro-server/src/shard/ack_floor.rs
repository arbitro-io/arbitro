//! AckFloors — per-consumer contiguous-acked floor (temporal isolation).
//!
//! Owned by the command thread. For each consumer it tracks the highest
//! `floor` such that **every** seq `<= floor` has been acked by that
//! consumer, gap-free. The drain uses the floor (mirrored into the
//! drain-owned `ConsumerSubjects` slot via `DrainEvent::Ack`) to skip
//! re-delivering already-acked seqs when the shard cursor rewinds for a
//! different consumer's replay (late `DeliverPolicy::All` / `ByStartSeq`
//! join, bind, nack, ack-timeout).
//!
//! ## Why the contiguous prefix (and not `consumer_cursor`)
//!
//! `NameRegistry::consumer_cursor` is the MAX acked seq. Under
//! out-of-order acks, seqs below the max can still be unacked (delivered
//! and pending, nacked awaiting redelivery, or skipped for capacity) —
//! filtering on the max would skip owed messages and lose them. The
//! contiguous prefix is safe by construction: any owed seq is unacked,
//! an unacked seq is a gap, and a gap pins the floor below it. So a
//! floor-based skip can never suppress a legitimate (re)delivery.
//!
//! ## Feeding rule (correctness-critical)
//!
//! Callers must record ONLY seqs that matched a pending delivery (the
//! engine's own ack criterion). Feeding raw client input would let a
//! bogus ack for a never-delivered seq advance the floor past an owed
//! message.
//!
//! Slots are indexed by `ConsumerId.raw()` (dense-ID rule) and MUST be
//! removed when the consumer entity is deleted — consumer ids are
//! pool-recycled, and a stale floor on a recycled id would silently
//! starve the new consumer.

use std::collections::BTreeSet;

/// Cap on the out-of-order set. A consumer whose subject filter never
/// matches some seqs has a permanent gap: its floor stalls there and
/// acked seqs above keep accumulating. Beyond the cap we stop inserting
/// — the floor simply stops advancing (conservative: extra redeliveries
/// on a rewind, never message loss).
const OOO_CAP: usize = 1024;

#[derive(Default)]
struct FloorSlot {
    /// All seqs `<= floor` are acked, gap-free.
    floor: u64,
    /// Acked seqs `> floor + 1`, waiting for the gap to close.
    ooo: BTreeSet<u64>,
}

impl FloorSlot {
    fn record(&mut self, seq: u64) {
        if seq <= self.floor {
            return; // duplicate ack of an already-absorbed seq
        }
        if seq == self.floor + 1 {
            self.floor = seq;
            // Absorb any consecutive run buffered out of order.
            while let Some(&next) = self.ooo.first() {
                if next != self.floor + 1 {
                    break;
                }
                self.ooo.pop_first();
                self.floor = next;
            }
        } else if self.ooo.len() < OOO_CAP {
            self.ooo.insert(seq);
        }
        // else: cap reached — floor stalls (safe, conservative).
    }
}

/// Per-consumer contiguous-acked floors, indexed by `ConsumerId.raw()`.
#[derive(Default)]
pub struct AckFloors {
    slots: Vec<Option<FloorSlot>>,
}

impl AckFloors {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one acked seq for `consumer_id`. The seq MUST have matched
    /// a pending delivery (see module docs).
    pub fn record_acked(&mut self, consumer_id: u32, seq: u64) {
        let idx = consumer_id as usize;
        if idx >= self.slots.len() {
            self.slots.resize_with(idx + 1, || None);
        }
        self.slots[idx]
            .get_or_insert_with(FloorSlot::default)
            .record(seq);
    }

    /// Seed the floor at a consumer's start position (DeliverPolicy::New
    /// / ByStartSeq deliver floor). Seqs at or below the deliver floor
    /// are never delivered to this consumer, so "nothing unacked at or
    /// below the floor" holds by construction — without the seed, the
    /// first real ack (floor + 1) would stall forever in the
    /// out-of-order set because seqs 1..=floor never arrive. Monotonic
    /// max: never lowers an existing floor. Absorbs any buffered run the
    /// raised floor reaches.
    pub fn seed(&mut self, consumer_id: u32, floor: u64) {
        let idx = consumer_id as usize;
        if idx >= self.slots.len() {
            self.slots.resize_with(idx + 1, || None);
        }
        let slot = self.slots[idx].get_or_insert_with(FloorSlot::default);
        if floor > slot.floor {
            slot.floor = floor;
            slot.ooo = slot.ooo.split_off(&(floor + 1));
            while let Some(&next) = slot.ooo.first() {
                if next != slot.floor + 1 {
                    break;
                }
                slot.ooo.pop_first();
                slot.floor = next;
            }
        }
    }

    /// Current floor for `consumer_id`. 0 = nothing acked contiguously.
    #[inline]
    pub fn floor(&self, consumer_id: u32) -> u64 {
        self.slots
            .get(consumer_id as usize)
            .and_then(|s| s.as_ref())
            .map_or(0, |s| s.floor)
    }

    /// Drop all floor state for a deleted consumer (id may be recycled).
    pub fn remove(&mut self, consumer_id: u32) {
        if let Some(slot) = self.slots.get_mut(consumer_id as usize) {
            *slot = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_floor_is_zero() {
        let f = AckFloors::new();
        assert_eq!(f.floor(7), 0);
    }

    #[test]
    fn in_order_acks_advance_floor() {
        let mut f = AckFloors::new();
        for s in 1..=5 {
            f.record_acked(1, s);
        }
        assert_eq!(f.floor(1), 5);
    }

    /// Requirement: out-of-order acks must NOT advance the floor past an
    /// unacked gap — the gap seq (3) stays deliverable.
    #[test]
    fn out_of_order_acks_pin_floor_below_gap() {
        let mut f = AckFloors::new();
        for s in [1u64, 2, 4, 5] {
            f.record_acked(1, s);
        }
        assert_eq!(f.floor(1), 2, "floor must stop at the gap (seq 3 unacked)");
        // The gap closes (e.g. seq 3 nacked, redelivered, re-acked):
        f.record_acked(1, 3);
        assert_eq!(f.floor(1), 5, "closing the gap absorbs the buffered run");
    }

    #[test]
    fn duplicate_acks_are_noops() {
        let mut f = AckFloors::new();
        f.record_acked(1, 1);
        f.record_acked(1, 1);
        f.record_acked(1, 2);
        f.record_acked(1, 2);
        assert_eq!(f.floor(1), 2);
    }

    #[test]
    fn consumers_are_independent() {
        let mut f = AckFloors::new();
        f.record_acked(1, 1);
        f.record_acked(2, 1);
        f.record_acked(2, 2);
        assert_eq!(f.floor(1), 1);
        assert_eq!(f.floor(2), 2);
        assert_eq!(f.floor(3), 0);
    }

    /// A seeded floor (deliver floor of a New / ByStartSeq consumer)
    /// makes the first real ack contiguous instead of out-of-order.
    #[test]
    fn seed_makes_first_ack_contiguous() {
        let mut f = AckFloors::new();
        f.seed(1, 10);
        assert_eq!(f.floor(1), 10);
        f.record_acked(1, 11);
        assert_eq!(f.floor(1), 11, "ack at seed + 1 must advance the floor");
    }

    /// Seeding is monotonic and absorbs a buffered out-of-order run.
    #[test]
    fn seed_is_monotonic_and_absorbs_ooo_run() {
        let mut f = AckFloors::new();
        // Acks 12, 13 buffered out of order (floor still 0).
        f.record_acked(1, 12);
        f.record_acked(1, 13);
        assert_eq!(f.floor(1), 0);
        // Seed to 11 — the buffered run 12, 13 becomes contiguous.
        f.seed(1, 11);
        assert_eq!(f.floor(1), 13);
        // A lower re-seed (resubscribe) never lowers the floor.
        f.seed(1, 5);
        assert_eq!(f.floor(1), 13);
    }

    /// Recycled consumer ids must start from a clean floor.
    #[test]
    fn remove_resets_floor_for_recycled_id() {
        let mut f = AckFloors::new();
        for s in 1..=10 {
            f.record_acked(1, s);
        }
        assert_eq!(f.floor(1), 10);
        f.remove(1);
        assert_eq!(f.floor(1), 0);
    }

    /// Beyond the cap the floor stalls instead of growing the set —
    /// conservative, never lossy.
    #[test]
    fn ooo_cap_stalls_floor_without_unbounded_growth() {
        let mut f = AckFloors::new();
        // Permanent gap at seq 1; ack 2..=OOO_CAP+100 out of order.
        for s in 2..=(OOO_CAP as u64 + 100) {
            f.record_acked(1, s);
        }
        assert_eq!(f.floor(1), 0);
        let slot = f.slots[1].as_ref().unwrap();
        assert!(slot.ooo.len() <= OOO_CAP);
    }
}
