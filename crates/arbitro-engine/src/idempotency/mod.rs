//! IdempotencyWindow — time-bucketed exact hash set for deduplication.
//!
//! Level 3 — depends on `types` only.
//!
//! O(1) check-and-insert per message. Zero false positives (exact match).
//! Memory: configurable, default 8 buckets × 65536 slots × 8 bytes = 4 MB.

/// Time-bucketed exact hash set for window-based idempotency.
///
/// Messages carry a `u64` idempotency key. If the key was seen within
/// the window duration, the message is rejected as duplicate.
///
/// Key = 0 means "no idempotency" and is always accepted.
pub struct IdempotencyWindow {
    buckets: Box<[Bucket]>,
    bucket_count: usize,
    bucket_mask: usize,
    bucket_duration_ms: u64,
    current_bucket: usize,
    current_bucket_start_ms: u64,
}

struct Bucket {
    slots: Box<[u64]>,
    mask: usize,
    count: u32,
}

impl Bucket {
    fn new(slot_count: usize) -> Self {
        assert!(slot_count.is_power_of_two());
        Self {
            slots: vec![0u64; slot_count].into_boxed_slice(),
            mask: slot_count - 1,
            count: 0,
        }
    }

    fn clear(&mut self) {
        for slot in self.slots.iter_mut() {
            *slot = 0;
        }
        self.count = 0;
    }

    /// Check if key exists in this bucket. O(1) with linear probe (max 16).
    #[inline]
    fn contains(&self, key: u64) -> bool {
        let start = (key as usize) & self.mask;
        for i in 0..16 {
            let idx = (start + i) & self.mask;
            let slot = self.slots[idx];
            if slot == key { return true; }
            if slot == 0 { return false; }
        }
        false
    }

    /// Insert key into this bucket. Returns true if inserted, false if full probe.
    #[inline]
    fn insert(&mut self, key: u64) -> bool {
        let start = (key as usize) & self.mask;
        for i in 0..16 {
            let idx = (start + i) & self.mask;
            if self.slots[idx] == 0 {
                self.slots[idx] = key;
                self.count += 1;
                return true;
            }
            if self.slots[idx] == key {
                return true; // already present
            }
        }
        false // probe chain full — rare, slot_count should be large enough
    }
}

impl IdempotencyWindow {
    /// Create a new idempotency window.
    ///
    /// - `window_duration_ms`: total window duration (e.g. 300_000 for 5 minutes).
    /// - `bucket_count`: number of time buckets (must be power of 2, default 8).
    /// - `slots_per_bucket`: number of hash slots per bucket (must be power of 2, default 65536).
    pub fn new(window_duration_ms: u64, bucket_count: usize, slots_per_bucket: usize) -> Self {
        assert!(bucket_count.is_power_of_two());
        assert!(slots_per_bucket.is_power_of_two());

        let buckets: Vec<Bucket> = (0..bucket_count)
            .map(|_| Bucket::new(slots_per_bucket))
            .collect();

        Self {
            buckets: buckets.into_boxed_slice(),
            bucket_count,
            bucket_mask: bucket_count - 1,
            bucket_duration_ms: window_duration_ms / bucket_count as u64,
            current_bucket: 0,
            current_bucket_start_ms: 0,
        }
    }

    /// Create with default settings: 5 minute window, 8 buckets, 65536 slots each.
    pub fn default_5min() -> Self {
        Self::new(300_000, 8, 65536)
    }

    /// Check if a key is duplicate AND insert if new.
    ///
    /// Returns `true` if duplicate (reject), `false` if new (accept).
    /// Key = 0 is always accepted (no idempotency).
    ///
    /// O(1): bucket rotation + probe across all buckets.
    #[inline]
    pub fn check_and_insert(&mut self, key: u64, now_ms: u64) -> bool {
        if key == 0 { return false; }

        self.maybe_rotate(now_ms);

        // Check all buckets for existing key
        for i in 0..self.bucket_count {
            let idx = (self.current_bucket.wrapping_sub(i)) & self.bucket_mask;
            if self.buckets[idx].contains(key) {
                return true; // duplicate
            }
        }

        // Not found — insert into current bucket
        self.buckets[self.current_bucket].insert(key);
        false
    }

    /// Advance to the next bucket if enough time has elapsed.
    #[inline]
    fn maybe_rotate(&mut self, now_ms: u64) {
        if now_ms < self.current_bucket_start_ms + self.bucket_duration_ms {
            return;
        }

        // How many buckets to advance
        let elapsed = now_ms.saturating_sub(self.current_bucket_start_ms);
        let advance = (elapsed / self.bucket_duration_ms).min(self.bucket_count as u64) as usize;

        for _ in 0..advance {
            self.current_bucket = (self.current_bucket + 1) & self.bucket_mask;
            self.buckets[self.current_bucket].clear();
        }

        self.current_bucket_start_ms = now_ms - (now_ms % self.bucket_duration_ms);
    }

    /// Total number of keys stored across all buckets.
    pub fn total_keys(&self) -> u32 {
        self.buckets.iter().map(|b| b.count).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_zero_always_accepted() {
        let mut w = IdempotencyWindow::new(1000, 4, 64);
        assert!(!w.check_and_insert(0, 0));
        assert!(!w.check_and_insert(0, 0));
        assert!(!w.check_and_insert(0, 0));
    }

    #[test]
    fn duplicate_detected() {
        let mut w = IdempotencyWindow::new(1000, 4, 64);
        assert!(!w.check_and_insert(42, 0)); // new
        assert!(w.check_and_insert(42, 0));  // duplicate
        assert!(w.check_and_insert(42, 100)); // still within window
    }

    #[test]
    fn different_keys_accepted() {
        let mut w = IdempotencyWindow::new(1000, 4, 64);
        assert!(!w.check_and_insert(1, 0));
        assert!(!w.check_and_insert(2, 0));
        assert!(!w.check_and_insert(3, 0));
        assert_eq!(w.total_keys(), 3);
    }

    #[test]
    fn rotation_expires_old_keys() {
        // 1000ms window, 4 buckets → 250ms per bucket
        let mut w = IdempotencyWindow::new(1000, 4, 64);

        assert!(!w.check_and_insert(42, 0));
        assert!(w.check_and_insert(42, 100)); // still in window

        // Advance past the full window
        assert!(!w.check_and_insert(42, 1100)); // key expired, accepted as new
    }

    #[test]
    fn partial_rotation() {
        // 1000ms window, 4 buckets → 250ms per bucket
        let mut w = IdempotencyWindow::new(1000, 4, 64);

        assert!(!w.check_and_insert(42, 0));
        // Advance 2 buckets (500ms)
        assert!(w.check_and_insert(42, 500)); // still within 4-bucket window
    }

    #[test]
    fn many_keys_no_collision() {
        let mut w = IdempotencyWindow::new(10000, 4, 1024);
        for i in 1..500u64 {
            assert!(!w.check_and_insert(i, 0), "key {i} falsely detected as duplicate");
        }
        for i in 1..500u64 {
            assert!(w.check_and_insert(i, 0), "key {i} not detected as duplicate");
        }
    }

    #[test]
    fn default_5min() {
        let w = IdempotencyWindow::default_5min();
        assert_eq!(w.bucket_count, 8);
        assert_eq!(w.bucket_duration_ms, 37500);
    }
}
