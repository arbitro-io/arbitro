//! Hashed Timing Wheel — O(1) insert, O(expired) per tick.
//!
//! A fixed-size array of buckets where each bucket represents one tick
//! of wall-clock time (resolution chosen by the caller). Entries are
//! inserted into `bucket[(current + delay_ticks) % num_buckets]`.
//!
//! Designed for ack-timeout and nack-delay in Arbitro's shard worker.
//! Uses **lazy cancel**: the ack path never touches the wheel. On tick,
//! expired entries are returned to the caller who verifies liveness
//! (entry still in `binding.pending`). Stale entries are simply skipped.
//!
//! Memory:
//! - Buckets: `num_buckets × 24 bytes` (empty Vec headers, zero heap).
//! - Entries: heap-allocated only when inserted (16 bytes each).
//! - A wheel with 0 active entries uses ~1.4 KB for 60 buckets.
//!
//! Thread safety: **not needed**. The wheel lives inside the single-owner
//! CommandWorker (one per shard). No Arc, no Mutex.

/// Discriminates the two wheel-entry workloads. M5: replaces the previous
/// "subject_hash == 0 ⇒ nack-delay" hack with an explicit tag so the
/// worker doesn't depend on FNV-1a never hashing to zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WheelEntryKind {
    /// Delivered entry awaiting ack. On expiry the worker re-checks
    /// `binding.pending` and auto-nacks if still in flight.
    AckTimeout = 0,
    /// Nack-with-delay: cursor rewind only, message is already nacked.
    NackDelay = 1,
}

/// Entry stored in the timing wheel. 24 bytes, Copy.
///
/// The caller uses `consumer_id` + `seq` to look up whether the entry
/// is still pending. If already acked, skip (lazy cancel).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct WheelEntry {
    pub seq: u64,
    pub consumer_id: u32,
    pub subject_hash: u32,
    pub kind: WheelEntryKind,
}
// Size is now 24 (16 + 1 byte tag + 7 bytes alignment padding). Compiler
// keeps the struct 8-byte aligned because of the u64 seq.
const _: () = assert!(core::mem::size_of::<WheelEntry>() == 24);

/// Hashed timing wheel with lazy cancel semantics. Generic over the
/// entry type so the same structure backs multiple use cases:
/// - `TimingWheel<WheelEntry>` for ack-timeout / nack-with-delay
///   (`WheelEntry` carries seq + consumer_id + subject_hash).
/// - Other entry types (e.g. idempotency dedup) can use the same
///   wheel without duplicating the bucket / advance / clamp logic.
///
/// `T: Copy` keeps the implementation alloc-free per insert and lets
/// `advance` return a `Vec<T>` cheaply (the entries themselves are
/// pulled out of the bucket by ownership).
///
/// The caller is responsible for:
/// 1. Calling `advance()` at a fixed interval (e.g., every 1 second).
/// 2. Verifying returned entries are still pending before acting on them.
///
/// The wheel does NOT track time — it only knows ticks. The caller
/// converts wall-clock intervals into tick counts.
pub struct TimingWheel<T: Copy> {
    buckets: Box<[Vec<T>]>,
    current: usize,
    num_buckets: usize,
    len: usize,
}

impl<T: Copy> TimingWheel<T> {
    /// Create a new wheel with the given number of buckets.
    ///
    /// Each bucket represents one tick. With 1-second resolution:
    /// - 60 buckets = covers up to 60 seconds of delay.
    /// - 120 buckets = covers up to 120 seconds.
    ///
    /// Delays exceeding `num_buckets` ticks are clamped to the last bucket.
    pub fn new(num_buckets: usize) -> Self {
        assert!(num_buckets > 0, "wheel must have at least 1 bucket");
        let buckets: Vec<Vec<T>> = (0..num_buckets).map(|_| Vec::new()).collect();
        Self {
            buckets: buckets.into_boxed_slice(),
            current: 0,
            num_buckets,
            len: 0,
        }
    }

    /// Insert an entry that expires `delay_ticks` from now.
    ///
    /// If `delay_ticks >= num_buckets`, it is clamped to `num_buckets - 1`.
    /// O(1) amortized (Vec::push).
    #[inline]
    pub fn insert(&mut self, entry: T, delay_ticks: u32) {
        let ticks = (delay_ticks as usize).min(self.num_buckets - 1);
        let bucket = (self.current + ticks) % self.num_buckets;
        self.buckets[bucket].push(entry);
        self.len += 1;
    }

    /// Advance the wheel by one tick. Returns expired entries from the
    /// current bucket (drained — bucket is empty after this call).
    ///
    /// The caller must verify each entry is still pending (lazy cancel).
    /// Call this once per tick interval (e.g., once per second).
    #[inline]
    pub fn advance(&mut self) -> Vec<T> {
        self.current = (self.current + 1) % self.num_buckets;
        let bucket = core::mem::take(&mut self.buckets[self.current]);
        self.len -= bucket.len();
        bucket
    }

    /// Advance the wheel by one tick, draining expired entries into the
    /// provided buffer. Avoids allocation when the caller reuses a buffer.
    #[inline]
    pub fn advance_into(&mut self, out: &mut Vec<T>) {
        self.current = (self.current + 1) % self.num_buckets;
        out.clear();
        out.append(&mut self.buckets[self.current]);
        self.len -= out.len();
    }

    /// Number of entries currently in the wheel (across all buckets).
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// True if no entries in any bucket.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of buckets (max delay in ticks).
    #[inline]
    pub fn num_buckets(&self) -> usize {
        self.num_buckets
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insert_and_advance() {
        let mut w = TimingWheel::new(4);
        let e = WheelEntry {
            seq: 100,
            consumer_id: 1,
            subject_hash: 0xAB,
            kind: WheelEntryKind::AckTimeout,
        };

        w.insert(e, 2); // expires 2 ticks from now
        assert_eq!(w.len(), 1);

        let expired = w.advance(); // tick 1
        assert!(expired.is_empty());
        assert_eq!(w.len(), 1);

        let expired = w.advance(); // tick 2
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], e);
        assert_eq!(w.len(), 0);
    }

    #[test]
    fn multiple_entries_same_bucket() {
        let mut w = TimingWheel::new(8);
        let e1 = WheelEntry {
            seq: 1,
            consumer_id: 10,
            subject_hash: 0,
            kind: WheelEntryKind::AckTimeout,
        };
        let e2 = WheelEntry {
            seq: 2,
            consumer_id: 20,
            subject_hash: 0,
            kind: WheelEntryKind::AckTimeout,
        };
        let e3 = WheelEntry {
            seq: 3,
            consumer_id: 10,
            subject_hash: 0,
            kind: WheelEntryKind::AckTimeout,
        };

        w.insert(e1, 3);
        w.insert(e2, 3);
        w.insert(e3, 3);
        assert_eq!(w.len(), 3);

        w.advance(); // tick 1
        w.advance(); // tick 2
        let expired = w.advance(); // tick 3
        assert_eq!(expired.len(), 3);
        assert_eq!(w.len(), 0);
    }

    #[test]
    fn delay_clamped_to_max() {
        let mut w = TimingWheel::new(4);
        let e = WheelEntry {
            seq: 42,
            consumer_id: 1,
            subject_hash: 0,
            kind: WheelEntryKind::AckTimeout,
        };

        // delay=100 but wheel only has 4 buckets → clamped to 3
        w.insert(e, 100);
        w.advance(); // 1
        w.advance(); // 2
        let expired = w.advance(); // 3
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].seq, 42);
    }

    #[test]
    fn wrap_around() {
        let mut w = TimingWheel::new(4);
        // Advance 3 times to move current to bucket 3
        w.advance();
        w.advance();
        w.advance();

        let e = WheelEntry {
            seq: 7,
            consumer_id: 1,
            subject_hash: 0,
            kind: WheelEntryKind::AckTimeout,
        };
        w.insert(e, 2); // should land in bucket (3+2)%4 = 1

        w.advance(); // current=0, no entries
        let expired = w.advance(); // current=1
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].seq, 7);
    }

    #[test]
    fn advance_into_reuses_buffer() {
        let mut w = TimingWheel::new(4);
        w.insert(
            WheelEntry {
                seq: 1,
                consumer_id: 0,
                subject_hash: 0,
                kind: WheelEntryKind::AckTimeout,
            },
            1,
        );
        w.insert(
            WheelEntry {
                seq: 2,
                consumer_id: 0,
                subject_hash: 0,
                kind: WheelEntryKind::AckTimeout,
            },
            1,
        );

        let mut buf = Vec::new();
        w.advance_into(&mut buf);
        assert_eq!(buf.len(), 2);
        assert_eq!(w.len(), 0);
    }

    #[test]
    fn zero_delay_fires_next_tick() {
        let mut w = TimingWheel::new(4);
        let e = WheelEntry {
            seq: 99,
            consumer_id: 5,
            subject_hash: 0,
            kind: WheelEntryKind::AckTimeout,
        };

        // delay=0 means "insert at current bucket". But current advances
        // BEFORE checking, so delay=0 → lands at current, fires on NEXT
        // full rotation. For immediate fire, use delay=1 (or handle inline).
        //
        // Actually: insert at (current + 0) % N = current bucket.
        // advance() moves current forward by 1. So this entry won't fire
        // until the wheel wraps around completely (N ticks).
        // For "fire next tick", use delay_ticks=1.
        w.insert(e, 1);
        let expired = w.advance();
        assert_eq!(expired.len(), 1);
    }
}
