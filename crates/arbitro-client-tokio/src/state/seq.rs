//! Atomic u64 seq allocator. v2 wire uses `Header.seq: U64`.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct SeqAllocator(AtomicU64);

impl SeqAllocator {
    pub fn new() -> Self {
        // Start at 1 so seq=0 is reserved as "no request" sentinel.
        Self(AtomicU64::new(1))
    }

    /// Returns the next seq, wrapping after `u64::MAX` (~584y at 1 ns/op).
    #[inline]
    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for SeqAllocator {
    fn default() -> Self {
        Self::new()
    }
}
