//! Hierarchical timing wheel, Kafka-shaped, driven by absolute time.
//!
//! Levels stack by a factor of `WHEEL_SIZE`: a timer too far out for one
//! level is handed upward, and handed back down (cascaded) as its
//! deadline approaches. Resolution stays at `tick_ms` while the span
//! grows geometrically — ~9 KB reaches years.
//!
//! Contrast [`crate::wheel::TimingWheel`], which counts ticks: its
//! memory is `span / resolution`, and a deadline past the ring is
//! silently clamped.
//!
//! No clock inside. The caller passes an instant, not a tick count, so
//! an oversleeping driver catches up in one call instead of drifting:
//!
//! ```text
//! advance_to(now, &mut out)  → what is due
//! next_expiry_ms()           → when to come back, None if nothing is
//! ```
//!
//! `None` means the driver arms no timer, which is what decouples
//! resolution from wake rate: an idle wheel is free at any `tick_ms`.
//!
//! Two deliberate divergences from Kafka:
//!
//! - **Never early.** Kafka flushes a bucket at its start, so a timer can
//!   fire up to a tick early — harmless at 1ms, not for an ack timeout,
//!   where early means redelivering a message whose `ack_wait` had not
//!   run out. Level 0 here flushes bucket `v` at `now ≥ (v+1)·tick_ms`,
//!   so firing lands in `[deadline, deadline + tick_ms]`. Upper levels
//!   keep Kafka's rule; they cascade rather than fire, and the bucket
//!   start is what leaves a full lower level of room underneath.
//! - **No clamping.** A deadline past the top level parks in its furthest
//!   bucket and is re-placed when that bucket comes due.
//!
//! Cancellation is lazy, as in the flat wheel: buckets stay flat `Vec`s
//! and the ack path never touches the wheel. Kafka cancels in O(1) with
//! intrusive lists, but that costs an allocation per timer, and here
//! nearly every timer is acked before it expires.
//!
//! Memory: `WHEEL_SIZE × 24` bytes of `Vec` headers per level, levels
//! created on demand. Bucket and scratch capacity is trimmed after a
//! drain with hysteresis, so steady load keeps its buffers and a spike is
//! walked back down; [`HierarchicalTimingWheel::shrink_to_fit`] releases
//! the rest for a driver that knows it has gone idle. `T: Copy` means
//! `T: !Drop` — nothing here can leak, and there is no `unsafe`.
//!
//! Not thread safe, and does not need to be: the task that inserts is the
//! task that advances. That is also why there is no `DelayQueue` — Kafka
//! signals a sleeping timer thread, whereas here the inserting loop just
//! re-reads `next_expiry_ms()` on its next pass.

/// Buckets per level. Fixed at 64 so occupancy fits one `u64`, which is
/// what makes [`HierarchicalTimingWheel::next_expiry_ms`] a couple of bit
/// instructions per level instead of a scan.
pub const WHEEL_SIZE: usize = 64;

const WHEEL_BITS: u64 = WHEEL_SIZE as u64;

/// Hard cap on levels. At `tick_ms = 1` the top already spans ~2.2 years.
pub const MAX_LEVELS: usize = 6;

const BUCKET_RETAIN_CAP: usize = 16;
const SCRATCH_RETAIN_CAP: usize = WHEEL_SIZE * 4;

/// Give back capacity a spike left behind, with hysteresis.
///
/// Trimming to a fixed floor after every drain would be worse than not
/// trimming: a bucket holding a steady thousand timers would regrow from
/// the floor through ~7 reallocs every rotation. Keeping twice the last
/// drain, and acting only past twice that, leaves steady load alone.
#[inline]
fn trim<E>(buf: &mut Vec<E>, last_len: usize, floor: usize) {
    let keep = last_len.saturating_mul(2).max(floor);
    if buf.capacity() > keep.saturating_mul(2) {
        buf.shrink_to(keep);
    }
}

/// Level 0 lags a bucket so it never fires early; upper levels cascade at
/// the bucket start. See the module docs.
const BOTTOM_LAG: u64 = 1;
const UPPER_LAG: u64 = 0;

/// What [`HierarchicalTimingWheel::insert`] did with a timer. `Elapsed`
/// was not stored, and dropping that return is how a deadline vanishes.
#[must_use = "an Elapsed timer was not scheduled — the caller must fire it"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Insert {
    /// Stored. It will come out of a later `advance_to`.
    Scheduled,
    /// The deadline was at or before the wheel's current time, so there
    /// was nothing to wait for. Not stored — act on it now.
    Elapsed,
}

/// A scheduled value plus the instant it is due. The deadline is
/// absolute because cascading re-places against the current time, which
/// a relative offset can no longer express.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Timer<T: Copy> {
    value: T,
    expire_at_ms: u64,
}

/// One ring of the hierarchy.
struct Level<T: Copy> {
    /// Milliseconds one bucket covers.
    tick_ms: u64,
    /// Fixed-size so the compiler can prove `vid % WHEEL_BITS` is in
    /// bounds and drop the check on every push and drain.
    buckets: Box<[Vec<Timer<T>>; WHEEL_SIZE]>,
    /// Bit `i` set ⇔ `buckets[i]` is non-empty. Kept in step with the
    /// buckets on every push and drain, and cross-checked against the
    /// buckets themselves by this file's `assert_consistent` helper.
    occupied: u64,
    /// First virtual bucket id not yet drained. Held as "next" rather
    /// than "last drained" so a clock starting at zero is representable:
    /// the bottom level lags a bucket, so `last_drained` would be −1.
    next_vid: u64,
    len: usize,
}

impl<T: Copy> Level<T> {
    fn new(tick_ms: u64, now_ms: u64, lag: u64) -> Self {
        debug_assert!(tick_ms > 0);
        Self {
            tick_ms,
            buckets: Box::new(core::array::from_fn(|_| Vec::new())),
            occupied: 0,
            // Starts where the first legal insert slot begins, so
            // nothing counts as overdue at construction. `+ (1 - lag)`
            // and not `+ 1 - lag`: with a 1ms tick the intermediate can
            // overflow u64 even though the result cannot.
            next_vid: now_ms / tick_ms + (1 - lag),
            len: 0,
        }
    }

    /// Virtual bucket id of an instant at this level's resolution.
    #[inline]
    fn vid(&self, at_ms: u64) -> u64 {
        at_ms / self.tick_ms
    }

    /// Live window: the 64 ids from the cursor. Below it the bucket is
    /// past; at or above `+ WHEEL_SIZE` the id wraps onto another
    /// rotation's bucket.
    #[inline]
    fn accepts(&self, vid: u64) -> bool {
        vid >= self.next_vid && vid < self.next_vid + WHEEL_BITS
    }

    #[inline]
    fn push(&mut self, vid: u64, timer: Timer<T>) {
        debug_assert!(self.accepts(vid));
        let idx = (vid % WHEEL_BITS) as usize;
        self.buckets[idx].push(timer);
        self.occupied |= 1u64 << idx;
        self.len += 1;
    }

    /// Move the cursor to `now_ms`, handing every bucket it passes to
    /// `out`. `lag` picks the flush rule: 1 at the bottom (bucket end,
    /// never early), 0 above (bucket start, so the cascade has room).
    fn drain_due(&mut self, now_ms: u64, lag: u64, out: &mut Vec<Timer<T>>) {
        // See `Level::new` for why the constants are folded.
        let limit = self.vid(now_ms) + (1 - lag);
        if limit <= self.next_vid {
            return;
        }
        if self.occupied == 0 {
            // Most levels of a grown hierarchy are idle most of the
            // time, and this branch is their whole cost.
            self.next_vid = limit;
            return;
        }
        // A jump longer than one rotation would revisit buckets; every
        // bucket is due in that case, so one rotation covers it.
        let span = (limit - self.next_vid).min(WHEEL_BITS);
        let first = limit - span;
        for vid in first..limit {
            let idx = (vid % WHEEL_BITS) as usize;
            if self.occupied & (1u64 << idx) == 0 {
                continue;
            }
            let bucket = &mut self.buckets[idx];
            let drained = bucket.len();
            self.len -= drained;
            out.append(bucket);
            self.occupied &= !(1u64 << idx);
            // `append` leaves the capacity behind.
            trim(bucket, drained, BUCKET_RETAIN_CAP);
        }
        self.next_vid = limit;
    }

    /// When this level next has something to hand over. Rotating the
    /// mask so the cursor sits on bit 0 turns "nearest non-empty bucket"
    /// into one `trailing_zeros`.
    #[inline]
    fn next_due_ms(&self, lag: u64) -> Option<u64> {
        if self.occupied == 0 {
            return None;
        }
        let rot = (self.next_vid % WHEEL_BITS) as u32;
        let ahead = self.occupied.rotate_right(rot).trailing_zeros() as u64;
        // Handed over once `now` reaches `(vid + lag) * tick`.
        Some((self.next_vid + ahead + lag) * self.tick_ms)
    }

    fn clear(&mut self) {
        for bucket in self.buckets.iter_mut() {
            bucket.clear();
            if bucket.capacity() > BUCKET_RETAIN_CAP {
                bucket.shrink_to(BUCKET_RETAIN_CAP);
            }
        }
        self.occupied = 0;
        self.len = 0;
    }
}

/// Hierarchical timing wheel over absolute milliseconds. See the module
/// docs for the shape and the firing guarantee.
///
/// ```
/// use arbitro_common::hierarchical_wheel::{HierarchicalTimingWheel, Insert};
///
/// // 100 ms resolution, clock starting at 0.
/// let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(100, 0);
/// assert_eq!(wheel.insert(7, 250), Insert::Scheduled);
///
/// // Nothing is due before the deadline — and the wheel says so.
/// let mut fired = Vec::new();
/// assert_eq!(wheel.next_expiry_ms(), Some(300));
/// wheel.advance_to(299, &mut fired);
/// assert!(fired.is_empty());
///
/// // It fires after the deadline, never before it.
/// wheel.advance_to(300, &mut fired);
/// assert_eq!(fired, vec![7]);
/// assert_eq!(wheel.next_expiry_ms(), None);
/// ```
pub struct HierarchicalTimingWheel<T: Copy> {
    levels: Vec<Level<T>>,
    tick_ms: u64,
    now_ms: u64,
    len: usize,
    /// Reused so cascading allocates nothing in steady state.
    scratch: Vec<Timer<T>>,
}

impl<T: Copy> HierarchicalTimingWheel<T> {
    /// Build a wheel with `tick_ms` resolution, clock starting at
    /// `start_ms`. The wheel only compares instants, so the origin is
    /// arbitrary as long as it matches what [`Self::advance_to`] gets.
    ///
    /// # Panics
    ///
    /// If `tick_ms` is zero — every bucket calculation divides by it.
    pub fn new(tick_ms: u64, start_ms: u64) -> Self {
        assert!(tick_ms > 0, "tick_ms must be at least 1ms");
        Self {
            levels: vec![Level::new(tick_ms, start_ms, BOTTOM_LAG)],
            tick_ms,
            now_ms: start_ms,
            len: 0,
            scratch: Vec::new(),
        }
    }

    /// Schedule `value` for `expire_at_ms`.
    ///
    /// [`Insert::Elapsed`] stores nothing — the deadline was already at
    /// or before now. Otherwise it surfaces from a later `advance_to`,
    /// never before its deadline and at most one `tick_ms` after.
    ///
    /// O(1) for a deadline inside level 0, O(levels) worst case.
    pub fn insert(&mut self, value: T, expire_at_ms: u64) -> Insert {
        if expire_at_ms <= self.now_ms {
            return Insert::Elapsed;
        }
        self.place(
            Timer {
                value,
                expire_at_ms,
            },
            0,
        );
        self.len += 1;
        Insert::Scheduled
    }

    /// Move the clock to `now_ms` and collect what is due into `out`,
    /// which is cleared first. Timers surface exactly once.
    ///
    /// Time only moves forward; a `now_ms` at or below the current one is
    /// ignored, so a non-monotonic clock cannot rewind or double-fire.
    /// Jumps of any size catch up in this one call — which is what lets
    /// the driver sleep to [`Self::next_expiry_ms`] instead of ticking.
    pub fn advance_to(&mut self, now_ms: u64, out: &mut Vec<T>) {
        out.clear();
        if now_ms <= self.now_ms {
            return;
        }
        self.now_ms = now_ms;

        let mut scratch = core::mem::take(&mut self.scratch);
        debug_assert!(scratch.is_empty(), "scratch is emptied before storing");

        // Every cursor moves to `now` BEFORE anything is re-placed, and
        // that ordering is the correctness argument. A timer handed down
        // is measured against the receiving level's cursor; a stale
        // cursor reads it as too far ahead and bounces it back up — after
        // a long sleep, into the top level's parking bucket. Kafka avoids
        // the same trap by setting every level's clock before flushing.
        for li in 0..self.levels.len() {
            let lag = if li == 0 { BOTTOM_LAG } else { UPPER_LAG };
            self.levels[li].drain_due(now_ms, lag, &mut scratch);
        }

        let drained = scratch.len();
        for timer in scratch.iter().copied() {
            if timer.expire_at_ms <= now_ms {
                out.push(timer.value);
                self.len -= 1;
            } else {
                // Only from an upper level — level 0 lags a bucket so
                // everything it releases is due. With cursors current
                // this lands one pass down, no further cascade needed.
                self.place(timer, 0);
            }
        }

        scratch.clear();
        // Without this, scratch keeps the largest catch-up it ever did
        // for the life of the process, and the per-bucket trimming above
        // would just be moving the burst in here.
        trim(&mut scratch, drained, SCRATCH_RETAIN_CAP);
        self.scratch = scratch;
    }

    /// When to call [`Self::advance_to`] next, or `None` if nothing is
    /// scheduled — which is the point: an idle wheel arms no timer, so
    /// its cost does not follow `tick_ms`.
    ///
    /// This is the earliest instant *some* level has work; upstairs that
    /// work is a cascade, not a fire, so a wake may produce an empty
    /// `out`. Firing still lands within a tick of the deadline.
    ///
    /// O(levels), a couple of bit instructions each.
    pub fn next_expiry_ms(&self) -> Option<u64> {
        if self.len == 0 {
            return None;
        }
        let mut best: Option<u64> = None;
        for (li, level) in self.levels.iter().enumerate() {
            let lag = if li == 0 { BOTTOM_LAG } else { UPPER_LAG };
            if let Some(due) = level.next_due_ms(lag) {
                best = Some(best.map_or(due, |b| b.min(due)));
            }
        }
        best
    }

    /// [`Self::next_expiry_ms`] as a duration from `now_ms`, saturating
    /// at zero when something is already due.
    pub fn sleep_for_ms(&self, now_ms: u64) -> Option<u64> {
        self.next_expiry_ms().map(|due| due.saturating_sub(now_ms))
    }

    /// Timers currently scheduled.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The wheel's current time.
    #[inline]
    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }

    /// Base resolution. Firing is late by at most this much, never early.
    #[inline]
    pub fn tick_ms(&self) -> u64 {
        self.tick_ms
    }

    /// Levels allocated. Grows when a deadline reaches past the top,
    /// never shrinks.
    #[inline]
    pub fn levels(&self) -> usize {
        self.levels.len()
    }

    /// Span the current levels cover, in ms. A deadline past this parks
    /// and is re-placed later — not clamped, not lost.
    pub fn span_ms(&self) -> u64 {
        self.levels
            .last()
            .map_or(0, |lv| lv.tick_ms.saturating_mul(WHEEL_BITS))
    }

    /// Drop every scheduled timer, keeping the allocated levels.
    pub fn clear(&mut self) {
        for level in self.levels.iter_mut() {
            level.clear();
        }
        self.len = 0;
    }

    /// Hand back every byte the wheel is not using.
    ///
    /// Draining trims with hysteresis, which needs rotations to work; a
    /// wheel that has gone quiet has none left. Purely an optimisation —
    /// skipping it costs only memory.
    pub fn shrink_to_fit(&mut self) {
        for level in self.levels.iter_mut() {
            for bucket in level.buckets.iter_mut() {
                bucket.shrink_to_fit();
            }
        }
        self.scratch.shrink_to_fit();
    }

    /// Store `timer` at the finest level at or above `from_level` that
    /// can hold it, growing the hierarchy if the deadline outruns the
    /// top. Callers guarantee `expire_at_ms > now_ms`.
    fn place(&mut self, timer: Timer<T>, from_level: usize) {
        debug_assert!(timer.expire_at_ms > self.now_ms, "a due timer must fire");
        for li in from_level..self.levels.len() {
            let vid = self.levels[li].vid(timer.expire_at_ms);
            if self.levels[li].accepts(vid) {
                self.levels[li].push(vid, timer);
                return;
            }
        }

        // Past the top: add coarser levels until it fits, or hit the cap.
        while self.levels.len() < MAX_LEVELS {
            let top_tick = self.levels[self.levels.len() - 1].tick_ms;
            let Some(next_tick) = top_tick.checked_mul(WHEEL_BITS) else {
                break;
            };
            self.levels
                .push(Level::new(next_tick, self.now_ms, UPPER_LAG));
            let li = self.levels.len() - 1;
            let vid = self.levels[li].vid(timer.expire_at_ms);
            if self.levels[li].accepts(vid) {
                self.levels[li].push(vid, timer);
                return;
            }
        }

        // Still too far at the cap. Park in the top level's furthest
        // bucket rather than clamp: when it comes due the timer is
        // re-placed against the advanced clock and either fits or parks
        // again. One pass covers the top level's whole span.
        let li = self.levels.len() - 1;
        let park = self.levels[li].next_vid + WHEEL_BITS - 1;
        self.levels[li].push(park, timer);
    }
}

impl<T: Copy + core::fmt::Debug> core::fmt::Debug for HierarchicalTimingWheel<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HierarchicalTimingWheel")
            .field("tick_ms", &self.tick_ms)
            .field("now_ms", &self.now_ms)
            .field("len", &self.len)
            .field("levels", &self.levels.len())
            .field("span_ms", &self.span_ms())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random source. A seeded LCG keeps the
    /// property tests reproducible without pulling in `rand`.
    struct Lcg(u64);

    impl Lcg {
        fn new(seed: u64) -> Self {
            Self(seed)
        }
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 11
        }
        fn in_range(&mut self, lo: u64, hi: u64) -> u64 {
            lo + self.next() % (hi - lo)
        }
    }

    /// Every bucket's occupancy bit agrees with whether it holds
    /// anything, and the cached lengths agree with the buckets. The
    /// occupancy mask is what `next_expiry_ms` trusts, so a mask that
    /// drifts out of step would silently mis-schedule wakeups.
    fn assert_consistent<T: Copy>(wheel: &HierarchicalTimingWheel<T>) {
        let mut total = 0usize;
        for (li, level) in wheel.levels.iter().enumerate() {
            let mut level_total = 0usize;
            for (idx, bucket) in level.buckets.iter().enumerate() {
                let bit_set = level.occupied & (1u64 << idx) != 0;
                assert_eq!(
                    bit_set,
                    !bucket.is_empty(),
                    "level {li} bucket {idx}: occupancy bit {bit_set} but {} entries",
                    bucket.len()
                );
                level_total += bucket.len();
            }
            assert_eq!(level_total, level.len, "level {li} cached len is stale");
            total += level_total;
        }
        assert_eq!(total, wheel.len, "wheel cached len is stale");
    }

    #[test]
    fn a_fresh_wheel_holds_nothing_and_asks_for_no_wakeup() {
        let wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(100, 0);
        assert!(wheel.is_empty());
        assert_eq!(wheel.len(), 0);
        assert_eq!(wheel.next_expiry_ms(), None);
        assert_eq!(wheel.sleep_for_ms(0), None);
        assert_eq!(wheel.levels(), 1);
    }

    #[test]
    fn a_deadline_at_or_before_now_is_reported_not_stored() {
        let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(100, 1_000);
        assert_eq!(wheel.insert(1, 999), Insert::Elapsed);
        assert_eq!(wheel.insert(2, 1_000), Insert::Elapsed);
        assert_eq!(wheel.insert(3, 1_001), Insert::Scheduled);
        assert_eq!(wheel.len(), 1, "only the future deadline was stored");
    }

    #[test]
    fn a_timer_never_fires_before_its_deadline() {
        // The whole reason this wheel exists: an ack timeout that fires
        // early redelivers a message whose ack_wait had not run out.
        let mut wheel: HierarchicalTimingWheel<u64> = HierarchicalTimingWheel::new(100, 0);
        let mut rng = Lcg::new(0xA11CE);
        let mut deadlines = std::collections::HashMap::new();

        for id in 0..2_000u64 {
            let deadline = rng.in_range(1, 90_000);
            if wheel.insert(id, deadline) == Insert::Scheduled {
                deadlines.insert(id, deadline);
            }
        }

        let mut out = Vec::new();
        let mut fired = 0usize;
        for now in (0..=120_000).step_by(37) {
            wheel.advance_to(now, &mut out);
            for &id in out.iter() {
                let deadline = deadlines[&id];
                assert!(
                    deadline <= now,
                    "timer {id} fired at {now} but was due at {deadline}"
                );
                fired += 1;
            }
        }
        assert_eq!(fired, deadlines.len(), "every timer fired exactly once");
        assert!(wheel.is_empty());
        assert_consistent(&wheel);
    }

    #[test]
    fn a_timer_fires_within_one_tick_after_its_deadline() {
        const TICK: u64 = 100;
        let mut wheel: HierarchicalTimingWheel<u64> = HierarchicalTimingWheel::new(TICK, 0);
        let mut rng = Lcg::new(0xB0B);
        let mut deadlines = std::collections::HashMap::new();

        // Level spans at this tick: L0 6.4s, L1 409.6s, L2 26_214s.
        // The range has to clear L1 or the bound is only ever checked on
        // a single cascade, and a two-hop handover that lost a tick on
        // the way down would go unnoticed.
        const HORIZON: u64 = 5_000_000;
        for id in 0..2_000u64 {
            let deadline = rng.in_range(1, HORIZON);
            if wheel.insert(id, deadline) == Insert::Scheduled {
                deadlines.insert(id, deadline);
            }
        }
        assert!(wheel.levels() >= 3, "the range must reach level 2");

        // Driven one tick at a time, so "fired at" is the tightest
        // observation the resolution allows.
        let mut out = Vec::new();
        let mut seen = 0usize;
        let mut now = 0;
        while now <= HORIZON + TICK {
            now += TICK;
            wheel.advance_to(now, &mut out);
            for &id in out.iter() {
                let deadline = deadlines[&id];
                assert!(deadline <= now, "timer {id} fired early");
                assert!(
                    now - deadline <= TICK,
                    "timer {id} due at {deadline} fired at {now}, later than one tick"
                );
                seen += 1;
            }
        }
        assert_eq!(seen, deadlines.len());
    }

    #[test]
    fn levels_appear_only_when_a_deadline_reaches_that_far() {
        let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(1, 0);
        assert_eq!(wheel.levels(), 1, "level 0 covers 64ms");

        assert_eq!(wheel.insert(1, 30), Insert::Scheduled);
        assert_eq!(wheel.levels(), 1);

        assert_eq!(wheel.insert(2, 1_000), Insert::Scheduled); // past 64ms, inside 64² = 4096ms
        assert_eq!(wheel.levels(), 2);

        assert_eq!(wheel.insert(3, 100_000), Insert::Scheduled); // past 4096ms
        assert_eq!(wheel.levels(), 3);

        assert_consistent(&wheel);
    }

    #[test]
    fn a_timer_parked_upstairs_cascades_down_and_fires_once() {
        let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(1, 0);
        // 500_000 ms needs level 3 (64³ = 262_144 ms).
        assert_eq!(wheel.insert(42, 500_000), Insert::Scheduled);
        assert!(wheel.levels() >= 3, "deadline reached past level 2");

        let mut out = Vec::new();
        let mut fires = 0;
        let mut now = 0u64;
        // Step in units unrelated to any level boundary so the walk down
        // is exercised rather than landing on aligned instants.
        while now < 600_000 {
            now += 997;
            wheel.advance_to(now, &mut out);
            for &value in out.iter() {
                assert_eq!(value, 42);
                assert!(now >= 500_000, "cascaded timer fired early at {now}");
                assert!(now - 500_000 <= 997, "fired far later than the step");
                fires += 1;
            }
        }
        assert_eq!(fires, 1, "cascading must not duplicate or drop the timer");
        assert!(wheel.is_empty());
        assert_consistent(&wheel);
    }

    #[test]
    fn a_deadline_past_the_top_level_parks_and_still_fires_on_time() {
        // 1ms base caps the hierarchy at 64⁶ ms; ask for beyond that so
        // the re-park path runs instead of the growth path.
        let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(1, 0);
        let far = WHEEL_BITS.pow(MAX_LEVELS as u32) * 3;
        assert_eq!(wheel.insert(9, far), Insert::Scheduled);
        assert_eq!(wheel.levels(), MAX_LEVELS, "grew to the cap, no further");
        let span = wheel.span_ms();
        assert!(far > span, "the deadline really does outrun the top level");

        // The parking bucket comes due at the top level's whole span,
        // nowhere near the deadline. This is the assertion that separates
        // parking from clamping: a wheel that clamped would fire here.
        let mut out = Vec::new();
        wheel.advance_to(span, &mut out);
        assert!(out.is_empty(), "parking fired at the parking bucket");
        assert_eq!(wheel.len(), 1, "and the timer is still scheduled");

        // Walk to the deadline in steps aligned to no level boundary, so
        // however many parking rounds it takes, none may leak the timer
        // out early.
        let step = far / 7;
        let mut now = span;
        let mut fires = 0usize;
        while now < far * 2 {
            now += step;
            wheel.advance_to(now, &mut out);
            for &value in out.iter() {
                assert_eq!(value, 9);
                assert!(now >= far, "a timer due at {far} fired at {now}");
                fires += 1;
            }
        }
        assert_eq!(fires, 1, "parking must not duplicate or drop the timer");
        assert!(wheel.is_empty());
        assert_consistent(&wheel);
    }

    #[test]
    fn next_expiry_is_the_exact_instant_the_wheel_has_work() {
        let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(100, 0);
        assert_eq!(wheel.insert(1, 250), Insert::Scheduled);

        let due = wheel.next_expiry_ms().expect("one timer is scheduled");
        assert_eq!(due, 300, "bucket 2 flushes once the clock passes 300");

        let mut out = Vec::new();
        wheel.advance_to(due - 1, &mut out);
        assert!(out.is_empty(), "there was nothing to do one ms earlier");

        wheel.advance_to(due, &mut out);
        assert_eq!(out, vec![1]);
        assert_eq!(wheel.next_expiry_ms(), None);
    }

    #[test]
    fn next_expiry_reports_the_nearest_of_many_across_levels() {
        let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(100, 0);
        assert_eq!(wheel.insert(1, 50_000), Insert::Scheduled); // upper level
        assert_eq!(wheel.insert(2, 900), Insert::Scheduled); // level 0
        assert_eq!(wheel.insert(3, 20_000), Insert::Scheduled); // upper level

        // The nearest is the level-0 timer, flushed at the end of its
        // bucket: bucket 9 covers [900, 1000), so 1000.
        assert_eq!(wheel.next_expiry_ms(), Some(1_000));
        assert_eq!(wheel.sleep_for_ms(400), Some(600));
        assert_eq!(wheel.sleep_for_ms(5_000), Some(0), "already due saturates");
    }

    #[test]
    fn sleeping_to_the_next_expiry_fires_exactly_what_ticking_does() {
        // The equivalence the design rests on: a driver that sleeps to
        // next_expiry_ms must observe the same timers, in the same
        // order, as one that wakes every tick — otherwise the CPU saving
        // would come at the cost of behaviour.
        const TICK: u64 = 100;
        // Past level 1 (409.6s), so the equivalence covers multi-level
        // cascades and not just the timers that never left level 0.
        const HORIZON: u64 = 5_000_000;
        let mut rng = Lcg::new(0xC0FFEE);
        let schedule: Vec<(u32, u64)> = (0..500u32)
            .map(|id| (id, rng.in_range(1, HORIZON)))
            .collect();

        let mut ticked: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(TICK, 0);
        let mut slept: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(TICK, 0);
        for (id, deadline) in schedule.iter().copied() {
            assert_eq!(ticked.insert(id, deadline), Insert::Scheduled);
            assert_eq!(slept.insert(id, deadline), Insert::Scheduled);
        }
        assert!(ticked.levels() >= 3, "the horizon must reach level 2");

        let mut by_tick: Vec<(u64, u32)> = Vec::new();
        let mut out = Vec::new();
        let mut now = 0u64;
        while now < HORIZON + TICK * 2 {
            now += TICK;
            ticked.advance_to(now, &mut out);
            by_tick.extend(out.iter().map(|&id| (now, id)));
        }

        let mut by_sleep: Vec<(u64, u32)> = Vec::new();
        let mut now = 0u64;
        let mut wakeups = 0usize;
        while let Some(due) = slept.next_expiry_ms() {
            assert!(due > now, "a wakeup must move the clock forward");
            now = due;
            wakeups += 1;
            slept.advance_to(now, &mut out);
            by_sleep.extend(out.iter().map(|&id| (now, id)));
        }

        assert_eq!(by_tick, by_sleep, "sleeping changed what fired or when");
        assert!(
            wakeups < (HORIZON / TICK) as usize,
            "sleeping used {wakeups} wakeups, no better than ticking"
        );
        assert!(slept.is_empty() && ticked.is_empty());
    }

    #[test]
    fn an_idle_wheel_asks_for_no_wakeups_at_any_resolution() {
        // The property that makes fine resolution free: cost follows the
        // number of scheduled timers, not the tick size.
        for tick_ms in [1, 10, 100, 1_000] {
            let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(tick_ms, 0);
            assert_eq!(wheel.next_expiry_ms(), None);

            assert_eq!(wheel.insert(1, 10_000), Insert::Scheduled);
            let mut out = Vec::new();
            wheel.advance_to(20_000, &mut out);
            assert_eq!(out, vec![1]);
            assert_eq!(
                wheel.next_expiry_ms(),
                None,
                "tick_ms={tick_ms}: a drained wheel must arm nothing"
            );
        }
    }

    #[test]
    fn a_clock_that_goes_backwards_is_ignored() {
        let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(100, 0);
        assert_eq!(wheel.insert(1, 500), Insert::Scheduled);

        let mut out = Vec::new();
        wheel.advance_to(1_000, &mut out);
        assert_eq!(out, vec![1]);

        wheel.advance_to(200, &mut out);
        assert!(out.is_empty(), "rewinding must not replay a fired timer");
        assert_eq!(wheel.now_ms(), 1_000, "and must not rewind the clock");

        wheel.advance_to(1_000, &mut out);
        assert!(out.is_empty(), "nor may standing still refire it");
    }

    #[test]
    fn a_jump_past_everything_fires_everything_in_one_call() {
        // A driver blocked on other work hands over the real time on its
        // next pass; catching up must not need one call per tick.
        let mut wheel: HierarchicalTimingWheel<u64> = HierarchicalTimingWheel::new(1, 0);
        let mut rng = Lcg::new(0xD00D);
        let mut expected = Vec::new();
        for id in 0..1_000u64 {
            let deadline = rng.in_range(1, 5_000_000);
            if wheel.insert(id, deadline) == Insert::Scheduled {
                expected.push(id);
            }
        }

        let mut out = Vec::new();
        wheel.advance_to(10_000_000, &mut out);
        out.sort_unstable();
        expected.sort_unstable();
        assert_eq!(out, expected, "a single jump must drain the whole wheel");
        assert!(wheel.is_empty());
        assert_consistent(&wheel);
    }

    #[test]
    fn timers_sharing_a_bucket_all_come_out() {
        let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(1_000, 0);
        // 1s buckets: these three land together in bucket 5.
        for (id, deadline) in [(1u32, 5_001u64), (2, 5_500), (3, 5_999)] {
            assert_eq!(wheel.insert(id, deadline), Insert::Scheduled);
        }
        assert_eq!(wheel.len(), 3);

        let mut out = Vec::new();
        wheel.advance_to(5_999, &mut out);
        assert!(out.is_empty(), "the bucket flushes at its end, not before");

        wheel.advance_to(6_000, &mut out);
        out.sort_unstable();
        assert_eq!(out, vec![1, 2, 3]);
        assert!(wheel.is_empty());
    }

    #[test]
    fn clear_drops_every_timer_and_keeps_the_wheel_usable() {
        let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(100, 0);
        for id in 0..100u32 {
            assert_eq!(wheel.insert(id, 1_000 + u64::from(id) * 500), Insert::Scheduled);
        }
        assert_eq!(wheel.len(), 100);

        wheel.clear();
        assert!(wheel.is_empty());
        assert_eq!(wheel.next_expiry_ms(), None);
        assert_consistent(&wheel);

        let mut out = Vec::new();
        wheel.advance_to(1_000_000, &mut out);
        assert!(out.is_empty(), "cleared timers must not resurface");

        assert_eq!(wheel.insert(7, 1_000_100), Insert::Scheduled);
        wheel.advance_to(1_000_200, &mut out);
        assert_eq!(out, vec![7], "the wheel still works after a clear");
    }

    #[test]
    fn a_burst_does_not_pin_its_high_water_mark() {
        // Buckets are Vecs; without trimming, one spike would hold its
        // peak capacity for the life of the process.
        let mut wheel: HierarchicalTimingWheel<u64> = HierarchicalTimingWheel::new(100, 0);
        for id in 0..50_000u64 {
            assert_eq!(wheel.insert(id, 1_000 + (id % 10) * 100), Insert::Scheduled);
        }
        let mut out = Vec::new();
        wheel.advance_to(10_000, &mut out);
        assert_eq!(out.len(), 50_000);
        assert!(wheel.is_empty());

        // Hysteresis means the spike's own rotation keeps its capacity —
        // at that moment a spike is indistinguishable from a load that
        // is about to repeat. What must hold is that ordinary traffic
        // afterwards walks it back down.
        let retained_now: usize = wheel
            .levels
            .iter()
            .flat_map(|lv| lv.buckets.iter())
            .map(|b| b.capacity())
            .sum();

        // One light timer per level-0 bucket, so every bucket the burst
        // touched gets drained again.
        let start_vid = 100u64;
        for k in 0..WHEEL_BITS {
            let deadline = (start_vid + k) * 100 + 50;
            assert_eq!(wheel.insert(1_000_000 + k, deadline), Insert::Scheduled);
        }
        wheel.advance_to((start_vid + WHEEL_BITS + 1) * 100, &mut out);
        assert_eq!(out.len(), WHEEL_SIZE, "every level-0 bucket was drained");

        let retained_after: usize = wheel
            .levels
            .iter()
            .flat_map(|lv| lv.buckets.iter())
            .map(|b| b.capacity())
            .sum();
        let ceiling = wheel.levels() * WHEEL_SIZE * BUCKET_RETAIN_CAP;
        assert!(
            retained_after < retained_now,
            "the burst's capacity was never given back: {retained_now} → {retained_after}"
        );
        assert!(
            retained_after <= ceiling,
            "buckets kept {retained_after} slots, above the {ceiling} ceiling"
        );
    }

    #[test]
    fn the_structure_stays_consistent_under_mixed_traffic() {
        // Insert and advance interleaved, the way a live shard drives it,
        // checking the occupancy mask and cached lengths throughout.
        let mut wheel: HierarchicalTimingWheel<u64> = HierarchicalTimingWheel::new(50, 0);
        let mut rng = Lcg::new(0xFEED);
        let mut scheduled = std::collections::HashMap::new();
        let mut out = Vec::new();
        let mut now = 0u64;
        let mut next_id = 0u64;

        for round in 0..500 {
            for _ in 0..20 {
                let deadline = now + rng.in_range(1, 30_000);
                if wheel.insert(next_id, deadline) == Insert::Scheduled {
                    scheduled.insert(next_id, deadline);
                }
                next_id += 1;
            }

            now += rng.in_range(1, 700);
            wheel.advance_to(now, &mut out);
            for &id in out.iter() {
                let deadline = scheduled
                    .remove(&id)
                    .unwrap_or_else(|| panic!("timer {id} fired twice"));
                assert!(deadline <= now, "timer {id} fired early in round {round}");
            }
            assert_eq!(wheel.len(), scheduled.len(), "round {round}: len drifted");
            assert_consistent(&wheel);
        }

        // Drain the tail so nothing is left unaccounted for.
        wheel.advance_to(now + 60_000, &mut out);
        for &id in out.iter() {
            scheduled.remove(&id).expect("timer fired twice");
        }
        assert!(scheduled.is_empty(), "{} timers never fired", scheduled.len());
        assert!(wheel.is_empty());
    }

    #[test]
    fn a_wheel_starting_at_a_late_clock_behaves_like_one_starting_at_zero() {
        // The origin is arbitrary — real drivers hand over a monotonic
        // clock whose zero is process start, not epoch.
        let base = 1_700_000_000_000u64;
        let mut shifted: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(100, base);
        let mut zeroed: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(100, 0);

        let mut rng = Lcg::new(0x5EED);
        for id in 0..300u32 {
            let offset = rng.in_range(1, 200_000);
            assert_eq!(shifted.insert(id, base + offset), Insert::Scheduled);
            assert_eq!(zeroed.insert(id, offset), Insert::Scheduled);
        }

        let mut a = Vec::new();
        let mut b = Vec::new();
        for step in (0..250_000).step_by(311) {
            shifted.advance_to(base + step, &mut a);
            zeroed.advance_to(step, &mut b);
            a.sort_unstable();
            b.sort_unstable();
            assert_eq!(a, b, "the clock origin changed what fired at +{step}");
        }
    }

    #[test]
    fn a_sleeping_driver_does_not_oversleep_a_parked_timer() {
        // The parked path and the sleep path together: a wrong wake
        // instant for the parking bucket would show up as lateness past
        // the one-tick bound, which a fixed-step walk cannot detect.
        const TICK: u64 = 100;
        let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(TICK, 0);
        let far = wheel.span_ms().saturating_mul(2) + 12_345;
        assert_eq!(wheel.insert(4, far), Insert::Scheduled);

        let mut out = Vec::new();
        let mut now = 0u64;
        let mut wakeups = 0usize;
        let mut fired_at = None;
        while let Some(due) = wheel.next_expiry_ms() {
            assert!(due > now, "a wakeup must move the clock forward");
            now = due;
            wakeups += 1;
            wheel.advance_to(now, &mut out);
            for &value in out.iter() {
                assert_eq!(value, 4);
                fired_at = Some(now);
            }
        }

        let fired_at = fired_at.expect("the timer never fired");
        assert!(fired_at >= far, "fired early: {fired_at} < {far}");
        assert!(
            fired_at - far <= TICK,
            "sleeping overslept: due {far}, fired {fired_at}"
        );
        // Re-parking costs one extra wake per top-level span; anything
        // near the tick count would mean the sleep path degenerated.
        assert!(wakeups < 16, "{wakeups} wakeups for a single timer");
    }

    #[test]
    fn a_deadline_on_the_seam_between_two_levels_still_fires_once() {
        // The exact instant level 0 stops accepting and level 1 starts.
        // A one-off error at either window edge drops the timer through
        // the gap, and only a boundary-exact input can catch it.
        const TICK: u64 = 100;
        let level0_span = TICK * WHEEL_BITS;
        for offset in [-2i64, -1, 0, 1, 2] {
            let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(TICK, 0);
            let deadline = (level0_span as i64 + offset) as u64;
            assert_eq!(wheel.insert(1, deadline), Insert::Scheduled);

            let mut out = Vec::new();
            let mut now = 0u64;
            let mut fires = 0usize;
            while now < level0_span * 3 {
                now += TICK;
                wheel.advance_to(now, &mut out);
                for &value in out.iter() {
                    assert_eq!(value, 1);
                    assert!(now >= deadline, "seam timer fired early at offset {offset}");
                    assert!(now - deadline <= TICK, "seam timer fired late");
                    fires += 1;
                }
            }
            assert_eq!(fires, 1, "seam offset {offset} did not fire exactly once");
        }
    }

    #[test]
    fn the_end_of_the_clock_does_not_overflow() {
        // u64::MAX is the obvious "flush everything" sentinel, and the
        // cursor arithmetic runs a division by a tick as small as 1ms —
        // an intermediate `+ 1` there would panic in debug and wrap in
        // release, which is worse than either.
        let mut wheel: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(1, 0);
        assert_eq!(wheel.insert(1, 5), Insert::Scheduled);
        assert_eq!(wheel.insert(2, u64::MAX), Insert::Scheduled);

        let mut out = Vec::new();
        wheel.advance_to(u64::MAX, &mut out);
        out.sort_unstable();
        assert_eq!(out, vec![1, 2], "both timers are due at the end of time");
        assert!(wheel.is_empty());
        assert_consistent(&wheel);
    }

    #[test]
    fn a_burst_does_not_pin_the_cascade_buffer_either() {
        // Trimming the buckets alone would just move a burst's footprint
        // into the scratch buffer, where it would stay for the life of
        // the process.
        let mut wheel: HierarchicalTimingWheel<u64> = HierarchicalTimingWheel::new(100, 0);
        let mut out = Vec::new();
        for id in 0..50_000u64 {
            assert_eq!(wheel.insert(id, 1_000 + (id % 10) * 100), Insert::Scheduled);
        }
        wheel.advance_to(10_000, &mut out);
        assert_eq!(out.len(), 50_000);

        // Steady low traffic afterwards must walk the buffer back down.
        for round in 0..8u64 {
            let base = 20_000 + round * 1_000;
            for id in 0..10u64 {
                assert_eq!(wheel.insert(id, base + 100), Insert::Scheduled);
            }
            wheel.advance_to(base + 500, &mut out);
        }
        assert!(
            wheel.scratch.capacity() <= SCRATCH_RETAIN_CAP * 2,
            "scratch still holds {} slots after the burst drained",
            wheel.scratch.capacity()
        );

        wheel.shrink_to_fit();
        assert_eq!(wheel.scratch.capacity(), 0, "shrink_to_fit left capacity");
    }

    #[test]
    fn a_steady_load_is_not_shrunk_and_regrown_every_rotation() {
        // Hysteresis: trimming back to a fixed floor after each drain
        // would make a steady thousand-per-bucket load realloc its way
        // up from 16 on every single rotation.
        let mut wheel: HierarchicalTimingWheel<u64> = HierarchicalTimingWheel::new(100, 0);
        let mut out = Vec::new();
        let mut now = 0u64;
        let mut capacity_after = Vec::new();

        for round in 0..6u64 {
            let base = now + 200;
            for id in 0..1_000u64 {
                assert_eq!(wheel.insert(round * 1_000 + id, base), Insert::Scheduled);
            }
            now = base + 200;
            wheel.advance_to(now, &mut out);
            assert_eq!(out.len(), 1_000);
            let idx = ((base / 100) % WHEEL_BITS) as usize;
            capacity_after.push(wheel.levels[0].buckets[idx].capacity());
        }

        // After the first round the bucket has its working capacity and
        // must keep it: a drop back toward the floor is the thrash.
        for (round, cap) in capacity_after.iter().enumerate().skip(1) {
            assert!(
                *cap >= 1_000,
                "round {round}: bucket shrank to {cap}, so the next \
                 rotation reallocates its way back up"
            );
        }
    }

    #[test]
    #[should_panic(expected = "tick_ms must be at least 1ms")]
    fn a_wheel_without_resolution_is_refused() {
        let _: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(0, 0);
    }
}
