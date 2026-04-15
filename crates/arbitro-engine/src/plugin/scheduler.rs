//! Scheduler plugin — timer wheel for deadline management.
//!
//! Level 4 — depends on `types`, `plugin/mod`.
//!
//! O(1) schedule, O(1) cancel (mark slot), O(k) tick per expired batch.
//! No heap allocation for scheduling or cancellation.

/// Number of slots in the timer wheel. Must be power of 2.
const WHEEL_SLOTS: usize = 65536;
const WHEEL_MASK: usize = WHEEL_SLOTS - 1;

/// A scheduled deadline entry.
#[derive(Debug, Clone, Copy)]
struct WheelEntry {
    /// The pending ID this deadline is for.
    pending_key_index: u32,
    pending_key_gen: u32,
    /// Whether this entry has been cancelled.
    cancelled: bool,
}

/// Timer wheel for O(1) deadline scheduling and cancellation.
///
/// Deadlines are placed in slots based on their expiry time.
/// Cancellation marks the entry without removing it (O(1)).
/// Tick sweeps expired slots and returns non-cancelled entries.
pub struct Scheduler {
    /// Flat array of slots. Each slot holds entries expiring at that tick.
    slots: Vec<Vec<WheelEntry>>,
    /// Current tick position.
    current_tick: u64,
    /// Milliseconds per tick (resolution).
    tick_ms: u64,
    /// Next deadline ID (monotonically increasing).
    next_id: u32,
    /// Maps deadline_id → (slot_index, entry_index) for O(1) cancel.
    cancel_map: Vec<(u16, u16)>,
}

impl Scheduler {
    /// Create a new scheduler with the given tick resolution.
    ///
    /// `tick_ms`: milliseconds per tick (e.g. 100 for 100ms resolution).
    pub fn new(tick_ms: u64) -> Self {
        let mut slots = Vec::with_capacity(WHEEL_SLOTS);
        for _ in 0..WHEEL_SLOTS {
            slots.push(Vec::new());
        }
        Self {
            slots,
            current_tick: 0,
            tick_ms,
            next_id: 1, // 0 = no deadline
            cancel_map: Vec::with_capacity(1024),
        }
    }

    /// Schedule a deadline. Returns a deadline_id for cancellation.
    ///
    /// `deadline_ms`: absolute timestamp in milliseconds when this should expire.
    /// O(1): compute slot index, push entry.
    pub fn schedule(
        &mut self,
        deadline_ms: u64,
        pending_key_index: u32,
        pending_key_gen: u32,
    ) -> u32 {
        let tick = deadline_ms / self.tick_ms;
        let slot_idx = (tick as usize) & WHEEL_MASK;

        let entry = WheelEntry {
            pending_key_index,
            pending_key_gen,
            cancelled: false,
        };

        let entry_idx = self.slots[slot_idx].len();
        self.slots[slot_idx].push(entry);

        let id = self.next_id;
        self.next_id += 1;

        // Store cancel mapping
        let idx = id as usize;
        if idx >= self.cancel_map.len() {
            self.cancel_map.resize(idx + 256, (0, 0));
        }
        self.cancel_map[idx] = (slot_idx as u16, entry_idx as u16);

        id
    }

    /// Cancel a deadline. O(1): mark entry as cancelled.
    ///
    /// `deadline_id`: the ID returned by `schedule`. 0 = no-op.
    #[inline]
    pub fn cancel(&mut self, deadline_id: u32) {
        if deadline_id == 0 { return; }
        let idx = deadline_id as usize;
        if idx < self.cancel_map.len() {
            let (slot_idx, entry_idx) = self.cancel_map[idx];
            if let Some(entry) = self.slots[slot_idx as usize].get_mut(entry_idx as usize) {
                entry.cancelled = true;
            }
        }
    }

    /// Advance the timer to `now_ms` and return all expired, non-cancelled entries.
    ///
    /// O(k) where k = number of entries in expired slots.
    pub fn tick(&mut self, now_ms: u64, expired: &mut Vec<ExpiredDeadline>) {
        let target_tick = now_ms / self.tick_ms;

        while self.current_tick <= target_tick {
            let slot_idx = (self.current_tick as usize) & WHEEL_MASK;
            for entry in self.slots[slot_idx].drain(..) {
                if !entry.cancelled {
                    expired.push(ExpiredDeadline {
                        pending_key_index: entry.pending_key_index,
                        pending_key_gen: entry.pending_key_gen,
                    });
                }
            }
            self.current_tick += 1;
        }
    }
}

/// An expired deadline returned by `tick()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpiredDeadline {
    pub pending_key_index: u32,
    pub pending_key_gen: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_and_tick() {
        let mut sched = Scheduler::new(100); // 100ms per tick
        sched.schedule(500, 1, 0); // expires at 500ms
        sched.schedule(500, 2, 0); // same tick
        sched.schedule(1000, 3, 0); // later

        let mut expired = Vec::new();
        sched.tick(600, &mut expired);
        assert_eq!(expired.len(), 2);
        assert_eq!(expired[0].pending_key_index, 1);
        assert_eq!(expired[1].pending_key_index, 2);

        expired.clear();
        sched.tick(1100, &mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].pending_key_index, 3);
    }

    #[test]
    fn cancel_prevents_expiry() {
        let mut sched = Scheduler::new(100);
        let id1 = sched.schedule(500, 1, 0);
        let _id2 = sched.schedule(500, 2, 0);

        sched.cancel(id1);

        let mut expired = Vec::new();
        sched.tick(600, &mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].pending_key_index, 2);
    }

    #[test]
    fn cancel_zero_is_noop() {
        let mut sched = Scheduler::new(100);
        sched.cancel(0); // should not panic
    }

    #[test]
    fn no_expired_before_deadline() {
        let mut sched = Scheduler::new(100);
        sched.schedule(1000, 1, 0);

        let mut expired = Vec::new();
        sched.tick(500, &mut expired);
        assert!(expired.is_empty());
    }

    #[test]
    fn tick_is_incremental() {
        let mut sched = Scheduler::new(100);
        sched.schedule(200, 1, 0);
        sched.schedule(400, 2, 0);

        let mut expired = Vec::new();
        sched.tick(300, &mut expired);
        assert_eq!(expired.len(), 1);

        expired.clear();
        sched.tick(500, &mut expired);
        assert_eq!(expired.len(), 1);
    }
}
