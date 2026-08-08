//! Hierarchical Timing Wheel — Kafka-shaped, driven by absolute time.
//!
//! The flat [`crate::wheel::TimingWheel`] counts ticks: one array of
//! buckets, one bucket per tick, `advance()` moves the cursor by one. Its
//! memory is proportional to `span / resolution`, so a finer resolution
//! costs buckets, and a deadline past the end of the ring is silently
//! clamped to the last bucket.
//!
//! This wheel is the other shape. Levels stack: level 0 has `tick_ms`
//! resolution, level 1 has `tick_ms × WHEEL_SIZE`, level 2 squares that,
//! and so on. A timer that does not fit in a level is handed upward; when
//! an upper bucket comes due its timers are handed back down (the
//! *cascade*), landing at a finer level each pass until level 0 fires
//! them. Resolution stays at `tick_ms` while the span grows
//! geometrically, so the whole structure is ~9 KB and reaches years.
//!
//! # It has no clock
//!
//! Same contract as the flat wheel, for the same reason — deterministic
//! tests, and a single owner deciding when time moves. The difference is
//! *what* the caller passes: an **instant**, not a count of elapsed ticks.
//!
//! ```text
//! driver → wheel : "it is now T, give me what is due"   advance_to(T, &mut out)
//! wheel  → driver: "the next thing is due at T2"        next_expiry_ms()
//! driver         : sleep until T2, repeat
//! ```
//!
//! Passing an instant is what removes tick counting, and with it drift: a
//! driver that oversleeps, or that gets preempted by other work in its
//! `select!`, hands over the real time on the next pass and the wheel
//! catches up in one call. Nothing accumulates.
//!
//! `next_expiry_ms()` returning `None` means nothing is scheduled, so the
//! driver arms no timer at all. That decouples resolution from wake rate:
//! an idle wheel costs zero wakeups whether `tick_ms` is 1000 or 1.
//!
//! # Firing is never early
//!
//! Kafka flushes a bucket when the clock reaches the bucket's *start*, so
//! a timer can fire up to one tick before its deadline. At `tick_ms = 1`
//! nobody notices. Arbitro uses this for ack timeouts, where firing early
//! means redelivering a message whose `ack_wait` has not actually run
//! out — a duplicate the consumer was promised it would not get.
//!
//! So level 0 here flushes bucket `v` when `now ≥ (v + 1) · tick_ms`,
//! one bucket behind Kafka. Every timer in that bucket has a deadline
//! below `(v + 1) · tick_ms`, hence at or before `now`. A timer fires at
//! an instant in `[deadline, deadline + tick_ms]` — **never early**, and
//! late by at most one tick. Upper levels keep Kafka's rule —
//! they do not fire, they cascade, and cascading at the bucket start is
//! exactly what leaves a full lower-level span of room underneath.
//!
//! # Deadlines beyond the top level
//!
//! They are not clamped. A timer that outruns the top level parks in that
//! level's furthest bucket; when the bucket comes due the timer is
//! re-placed against the new time and either fits or parks again. Each
//! pass advances by the top level's whole span, so with the default
//! shape one pass covers more than any caller will ask for — and the
//! answer stays correct rather than quietly wrong.
//!
//! # Cancellation
//!
//! There is none, deliberately — same **lazy cancel** as the flat wheel.
//! Kafka can cancel in O(1) because its buckets are intrusive linked
//! lists, which costs an allocation per timer and a pointer chase per
//! fire. Arbitro's ack path is the hot path and the overwhelming majority
//! of timers are acked before they expire, so the cheaper trade is to
//! leave them: buckets stay flat `Vec`s, insert is a `push`, and the
//! driver drops fired timers whose work already completed.
//!
//! # Memory
//!
//! - Buckets: `WHEEL_SIZE × 24` bytes per level (empty `Vec` headers),
//!   ≈ 1.5 KB, so ≈ 9 KB with all [`MAX_LEVELS`] levels — and levels are
//!   created only when a deadline reaches that far.
//! - Timers: [`Timer<T>`] is `T` plus a `u64` deadline, heap-resident
//!   only while scheduled.
//! - Bucket capacity is trimmed back to [`BUCKET_RETAIN_CAP`] after a
//!   drain, so a one-off burst does not pin its high-water mark forever.
//!
//! `T: Copy` means `T: !Drop`: the wheel owns nothing that can leak, only
//! its own `Vec`s. There is no `unsafe`, no interior mutability, and no
//! reference cycle, so "no leaks" is a property of the types rather than
//! a claim about the code.
//!
//! # Thread safety
//!
//! Not needed, same as the flat wheel. It lives inside the single-owner
//! shard worker: the task that inserts is the task that advances. That is
//! also why there is no `DelayQueue` and no signalling — Kafka needs to
//! wake a sleeping timer thread when an earlier deadline arrives, while
//! here the loop that inserted simply recomputes `next_expiry_ms()` on
//! its next pass and re-arms.

/// Buckets per level.
///
/// Fixed at 64 so per-level occupancy fits one `u64`, which is what makes
/// [`HierarchicalTimingWheel::next_expiry_ms`] a couple of bit
/// instructions per level instead of a scan over buckets.
pub const WHEEL_SIZE: usize = 64;

/// Bit width of the occupancy mask; identical to [`WHEEL_SIZE`], named
/// separately where the intent is "bits" rather than "buckets".
const WHEEL_BITS: u64 = WHEEL_SIZE as u64;

/// Hard cap on levels, so a nonsense deadline cannot grow the structure
/// without bound. With `tick_ms = 1` the top level already spans
/// `64⁶ ms` ≈ 2.2 years; with `tick_ms = 100` it spans centuries.
pub const MAX_LEVELS: usize = 6;

/// Capacity a bucket keeps after being drained. Bounds the memory a
/// traffic burst leaves behind while still avoiding a realloc per insert
/// in steady state.
const BUCKET_RETAIN_CAP: usize = 16;

/// Level 0 lags one bucket behind the clock so it never fires early; see
/// the module docs. Upper levels use Kafka's rule and cascade at the
/// bucket start.
const BOTTOM_LAG: u64 = 1;
const UPPER_LAG: u64 = 0;

/// What [`HierarchicalTimingWheel::insert`] did with a timer.
///
/// `#[must_use]`: an `Elapsed` timer was **not** stored, and silently
/// dropping that return is how a deadline goes missing.
#[must_use = "an Elapsed timer was not scheduled — the caller must fire it"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Insert {
    /// Stored. It will come out of a later `advance_to`.
    Scheduled,
    /// The deadline was at or before the wheel's current time, so there
    /// was nothing to wait for. Not stored — act on it now.
    Elapsed,
}

/// A scheduled value plus the instant it is due.
///
/// The absolute deadline rides with the timer because cascading needs
/// it: a timer handed down from an upper level has to be re-placed
/// against the *current* time, which a relative offset can no longer
/// express.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Timer<T: Copy> {
    value: T,
    expire_at_ms: u64,
}

/// One ring of the hierarchy.
struct Level<T: Copy> {
    /// Milliseconds one bucket covers.
    tick_ms: u64,
    buckets: Box<[Vec<Timer<T>>]>,
    /// Bit `i` set ⇔ `buckets[i]` is non-empty. Kept in step with the
    /// buckets on every push and drain; `debug_assert`ed in tests.
    occupied: u64,
    /// First virtual bucket id not yet drained — the cursor.
    ///
    /// Held as "next" rather than "last drained" so a wheel whose clock
    /// starts at zero has a representable cursor: the bottom level lags a
    /// bucket, and `last_drained` would have to be −1.
    next_vid: u64,
    len: usize,
}

impl<T: Copy> Level<T> {
    fn new(tick_ms: u64, now_ms: u64, lag: u64) -> Self {
        debug_assert!(tick_ms > 0);
        let buckets: Vec<Vec<Timer<T>>> = (0..WHEEL_SIZE).map(|_| Vec::new()).collect();
        Self {
            tick_ms,
            buckets: buckets.into_boxed_slice(),
            occupied: 0,
            // Nothing has been drained yet, and nothing may be treated as
            // overdue at construction: the cursor starts exactly where
            // the first legal insert slot begins.
            next_vid: now_ms / tick_ms + 1 - lag,
            len: 0,
        }
    }

    /// Virtual bucket id of an instant at this level's resolution.
    #[inline]
    fn vid(&self, at_ms: u64) -> u64 {
        at_ms / self.tick_ms
    }

    /// Can a timer due at `vid` be stored here without aliasing a bucket
    /// that is already spoken for?
    ///
    /// The live window is the 64 ids starting at the cursor. Below it the
    /// bucket is already past; at or above `+ WHEEL_SIZE` the id wraps
    /// onto a bucket belonging to a different rotation.
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

    /// Move the cursor to `now_ms` and hand every bucket it passes to
    /// `out`. `lag` selects the flush rule: 1 for the bottom level (flush
    /// at the bucket's end, never early), 0 for upper levels (flush at
    /// the start, so the cascade lands with a full span of room).
    fn drain_due(&mut self, now_ms: u64, lag: u64, out: &mut Vec<Timer<T>>) {
        let limit = self.vid(now_ms) + 1 - lag;
        if limit <= self.next_vid {
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
            self.len -= bucket.len();
            out.append(bucket);
            self.occupied &= !(1u64 << idx);
            // `append` leaves the capacity behind; give back anything a
            // burst grabbed so the high-water mark is not permanent.
            if bucket.capacity() > BUCKET_RETAIN_CAP {
                bucket.shrink_to(BUCKET_RETAIN_CAP);
            }
        }
        self.next_vid = limit;
    }

    /// Instant at which this level next has something to hand over, or
    /// `None` when it holds nothing.
    ///
    /// Rotating the occupancy mask so the cursor lands on bit 0 turns
    /// "find the nearest non-empty bucket" into one `trailing_zeros`.
    #[inline]
    fn next_due_ms(&self, lag: u64) -> Option<u64> {
        if self.occupied == 0 {
            return None;
        }
        let rot = (self.next_vid % WHEEL_BITS) as u32;
        let ahead = self.occupied.rotate_right(rot).trailing_zeros() as u64;
        // Bucket `vid` is handed over once `now / tick + 1 - lag > vid`,
        // that is once `now` reaches `(vid + lag) * tick`.
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

/// Hierarchical timing wheel over absolute milliseconds.
///
/// See the module docs for the shape, the firing guarantee and the
/// reasoning behind lazy cancel.
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
    /// Base resolution, and the resolution of level 0.
    tick_ms: u64,
    now_ms: u64,
    len: usize,
    /// Reused across `advance_to` calls so cascading allocates nothing in
    /// steady state.
    scratch: Vec<Timer<T>>,
}

impl<T: Copy> HierarchicalTimingWheel<T> {
    /// Build a wheel with `tick_ms` resolution whose clock starts at
    /// `start_ms`.
    ///
    /// `start_ms` should come from the same monotonic source later fed to
    /// [`Self::advance_to`]; the wheel only ever compares instants, so
    /// the origin is arbitrary as long as it is consistent.
    ///
    /// # Panics
    ///
    /// If `tick_ms` is zero — a wheel with no resolution has no meaning,
    /// and every bucket calculation divides by it.
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

    /// Schedule `value` to come due at `expire_at_ms`.
    ///
    /// Returns [`Insert::Elapsed`] — and stores nothing — when the
    /// deadline is at or before the wheel's current time. Otherwise the
    /// timer is stored and will surface from an `advance_to` at or after
    /// its deadline, never before, and by at most one `tick_ms` after.
    ///
    /// O(levels) worst case, and O(1) for the common case of a deadline
    /// inside level 0.
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

    /// Move the clock to `now_ms` and collect everything due into `out`,
    /// which is cleared first.
    ///
    /// Time only moves forward: a `now_ms` at or below the current time
    /// is ignored, so a non-monotonic clock cannot rewind the wheel or
    /// double-fire a timer. Jumps of any size are fine — the wheel
    /// catches up in this one call, which is what lets the driver sleep
    /// to [`Self::next_expiry_ms`] instead of ticking.
    ///
    /// Timers surface exactly once. Cascading from upper levels happens
    /// here, so a single call can both hand a timer down several levels
    /// and fire it.
    pub fn advance_to(&mut self, now_ms: u64, out: &mut Vec<T>) {
        out.clear();
        if now_ms <= self.now_ms {
            return;
        }
        self.now_ms = now_ms;

        // Taken out so levels can be drained while others are written;
        // put back before returning so the buffer is reused.
        let mut scratch = core::mem::take(&mut self.scratch);
        scratch.clear();

        // Every cursor moves to `now` *before* anything is re-placed.
        //
        // This ordering is the whole correctness argument. A timer handed
        // down from an upper level is measured against the receiving
        // level's cursor; if that cursor were still at the previous
        // instant it would read the timer as "too far ahead", reject it,
        // and bounce it back up — after a long sleep, all the way to the
        // top level's parking bucket, where it would sit for years.
        // Kafka avoids the same trap by having `advanceClock` set the
        // time on every level before flushing a single bucket.
        for li in 0..self.levels.len() {
            let lag = if li == 0 { BOTTOM_LAG } else { UPPER_LAG };
            self.levels[li].drain_due(now_ms, lag, &mut scratch);
        }

        for timer in scratch.iter().copied() {
            if timer.expire_at_ms <= now_ms {
                out.push(timer.value);
                self.len -= 1;
            } else {
                // Only reachable from an upper level: level 0 lags a full
                // bucket precisely so everything it releases is due.
                // With all cursors current this lands one pass down and
                // needs no further cascade in this call.
                self.place(timer, 0);
            }
        }

        scratch.clear();
        self.scratch = scratch;
    }

    /// The instant the driver must call [`Self::advance_to`] again, or
    /// `None` when nothing is scheduled.
    ///
    /// `None` is the whole point of the design: with nothing pending the
    /// driver arms no timer, so an idle wheel costs nothing no matter how
    /// fine `tick_ms` is.
    ///
    /// The returned instant is the earliest at which *some* level has
    /// work — for upper levels that work is a cascade, not a fire, so a
    /// wake can legitimately produce an empty `out`. Firing still
    /// happens no later than one `tick_ms` past the deadline.
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

    /// How long the driver should sleep from `now_ms`, saturating at zero
    /// when something is already due. `None` when nothing is scheduled.
    ///
    /// Convenience over [`Self::next_expiry_ms`] for drivers that hold a
    /// duration rather than a deadline.
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

    /// Levels currently allocated. Grows only when a deadline reaches
    /// past the existing top, never shrinks.
    #[inline]
    pub fn levels(&self) -> usize {
        self.levels.len()
    }

    /// Span the current levels cover from `now`, in milliseconds. A
    /// deadline past this parks and is re-placed later — it is not
    /// clamped, and not lost.
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

    /// Store `timer` at the finest level at or above `from_level` that
    /// can hold it, growing the hierarchy when the deadline outruns the
    /// current top.
    ///
    /// Callers guarantee `timer.expire_at_ms > self.now_ms`; `insert`
    /// checks it and the cascade re-checks it per timer.
    fn place(&mut self, timer: Timer<T>, from_level: usize) {
        debug_assert!(timer.expire_at_ms > self.now_ms, "a due timer must fire");
        for li in from_level..self.levels.len() {
            let vid = self.levels[li].vid(timer.expire_at_ms);
            if self.levels[li].accepts(vid) {
                self.levels[li].push(vid, timer);
                return;
            }
        }

        // Past the top: add coarser levels until it fits, or until the
        // cap says stop.
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

        // Still too far even at the cap. Park in the top level's furthest
        // bucket rather than clamp: when that bucket comes due the timer
        // is re-placed against the advanced clock, and either fits or
        // parks again. Late by construction, never early, never wrong —
        // one pass here covers the top level's entire span.
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

        for id in 0..1_000u64 {
            // Reach past level 0 (6.4 s) so cascaded timers are covered
            // by the bound too, not just the ones that never moved.
            let deadline = rng.in_range(1, 400_000);
            if wheel.insert(id, deadline) == Insert::Scheduled {
                deadlines.insert(id, deadline);
            }
        }

        // Driven one tick at a time, so "fired at" is the tightest
        // observation the resolution allows.
        let mut out = Vec::new();
        let mut seen = 0usize;
        let mut now = 0;
        while now <= 500_000 {
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
        const HORIZON: u64 = 300_000;
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

        let retained: usize = wheel
            .levels
            .iter()
            .flat_map(|lv| lv.buckets.iter())
            .map(|b| b.capacity())
            .sum();
        let ceiling = wheel.levels() * WHEEL_SIZE * BUCKET_RETAIN_CAP;
        assert!(
            retained <= ceiling,
            "buckets kept {retained} slots, above the {ceiling} ceiling"
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
    #[should_panic(expected = "tick_ms must be at least 1ms")]
    fn a_wheel_without_resolution_is_refused() {
        let _: HierarchicalTimingWheel<u32> = HierarchicalTimingWheel::new(0, 0);
    }
}
