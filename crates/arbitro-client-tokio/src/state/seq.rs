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

/// Per-connection subscription-id allocator. The broker refuses `0` and
/// namespaces what we send by connection, so a plain local counter is enough
/// to keep this client's subscriptions distinct from one another.
#[derive(Debug)]
pub struct SubIdAllocator(std::sync::atomic::AtomicU32);

impl SubIdAllocator {
    pub fn new() -> Self {
        // 0 is the rejected sentinel — never hand it out.
        Self(std::sync::atomic::AtomicU32::new(1))
    }

    #[inline]
    pub fn next(&self) -> u32 {
        let id = self.0.fetch_add(1, Ordering::Relaxed);
        // Wrap skips 0 rather than emitting the sentinel.
        if id == 0 {
            self.0.fetch_add(1, Ordering::Relaxed)
        } else {
            id
        }
    }
}

impl Default for SubIdAllocator {
    fn default() -> Self {
        Self::new()
    }
}
