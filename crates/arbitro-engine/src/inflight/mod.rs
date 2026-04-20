//! InFlight counters — dense Vec for consumer/queue, HashMap for subject.
//!
//! Level 3 — depends on `types`, `error`.
//!
//! Tracks how many in-flight (pending ack) messages exist per scope.
//! Single-threaded engine core — no atomics needed internally.
//!
//! ## Storage choice
//!
//! `ConsumerId` and `QueueId` are assigned monotonically by the catalog
//! (dense + bounded, ~10k in a realistic deployment). That enables direct
//! `Vec<u32>` indexing by raw ID — **zero hashing, one load, one store**
//! per inc/dec/get. Hot-path cost drops from ~10-15 ns (HashMap lookup +
//! bucket walk + entry API) to ~2 ns (cache-line load + add + store).
//!
//! `subject_hash` is a 32-bit hash of an arbitrary subject string — sparse
//! across the full `u32` range. A Vec would need 16 GB. Stays as HashMap.
//! See `performance.md` §11 (slab/array over HashMap for hot-path lookups).
//!
//! ## Auto-grow
//!
//! Writes grow the Vec if the key is beyond the current len. At steady
//! state (post-startup, when all consumers/queues are registered) the
//! resize branch never fires and the CPU branch predictor eats it for
//! free. Reads outside the range return 0 without allocating.

use std::collections::HashMap;

/// Scope for inflight counting. Mirrors the three dimensions we track.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InFlightScope {
    Subject,
    Consumer,
    Queue,
}

/// Per-scope inflight counter storage. See module docs for rationale.
pub struct InFlightCounters {
    /// Subject inflight: sparse u32 hash → HashMap.
    subject: HashMap<u32, u32, foldhash::fast::FixedState>,
    /// Consumer inflight: dense consumer_id → Vec indexed by raw id.
    consumer: Vec<u32>,
    /// Queue inflight: dense queue_id → Vec indexed by raw id.
    queue: Vec<u32>,
    /// Subject tracking gate. Starts `false`. Flipped to `true` (sticky)
    /// by `enable_subject_tracking` when the first subject-inflight limit
    /// is configured anywhere in the engine.
    ///
    /// Why: the subject HashMap is ONLY read to enforce
    /// `Catalog::max_subject_inflight`. With zero limits configured, the
    /// entire subject write path is dead — the `HashMap::entry` API
    /// alone is ~10 ns per call, and `inc_pending`/`dec_pending` pay this
    /// on every single claim and ack. Gating behind a branch-predictable
    /// bool eliminates ~18-22 ns/msg in the common (no-limits) case.
    ///
    /// Transition safety: pendings claimed BEFORE the flag flips were
    /// never tracked in the subject map; their eventual ack hits a
    /// no-op `dec_subject` (empty-key guarded). Counts stay consistent.
    track_subject: bool,
}

impl InFlightCounters {
    pub fn new() -> Self {
        Self {
            subject: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            // Start with a modest capacity; auto-grows on first write past len.
            consumer: Vec::with_capacity(64),
            queue: Vec::with_capacity(64),
            track_subject: false,
        }
    }

    /// Enable subject-scope inflight tracking. Sticky — never flips back.
    /// Called by the catalog when the first subject-inflight limit is set.
    #[inline]
    pub fn enable_subject_tracking(&mut self) {
        self.track_subject = true;
    }

    /// Fast-path: is subject-scope inflight being tracked?
    #[inline(always)]
    pub fn is_tracking_subject(&self) -> bool {
        self.track_subject
    }

    /// Ensure the consumer/queue Vec can hold `idx`. Grows by chunks of 16
    /// beyond the requested index to amortize resizes during startup.
    #[inline(always)]
    fn ensure_len(vec: &mut Vec<u32>, idx: usize) {
        if idx >= vec.len() {
            vec.resize(idx + 16, 0);
        }
    }

    /// Increment the inflight count for an entity. O(1).
    #[inline]
    pub fn inc(&mut self, scope: InFlightScope, key: u32) {
        match scope {
            InFlightScope::Subject => {
                *self.subject.entry(key).or_insert(0) += 1;
            }
            InFlightScope::Consumer => {
                let i = key as usize;
                Self::ensure_len(&mut self.consumer, i);
                self.consumer[i] += 1;
            }
            InFlightScope::Queue => {
                let i = key as usize;
                Self::ensure_len(&mut self.queue, i);
                self.queue[i] += 1;
            }
        }
    }

    /// Decrement the inflight count for an entity. O(1).
    /// Saturates at zero — never underflows.
    #[inline]
    pub fn dec(&mut self, scope: InFlightScope, key: u32) {
        match scope {
            InFlightScope::Subject => dec_subject(&mut self.subject, key),
            InFlightScope::Consumer => {
                let i = key as usize;
                if let Some(c) = self.consumer.get_mut(i) {
                    *c = c.saturating_sub(1);
                }
            }
            InFlightScope::Queue => {
                let i = key as usize;
                if let Some(c) = self.queue.get_mut(i) {
                    *c = c.saturating_sub(1);
                }
            }
        }
    }

    /// Get the current inflight count. O(1).
    #[inline]
    pub fn get(&self, scope: InFlightScope, key: u32) -> u32 {
        match scope {
            InFlightScope::Subject => self.subject.get(&key).copied().unwrap_or(0),
            InFlightScope::Consumer => self.consumer.get(key as usize).copied().unwrap_or(0),
            InFlightScope::Queue => self.queue.get(key as usize).copied().unwrap_or(0),
        }
    }

    /// Check if inflight count is below a limit. O(1).
    #[inline]
    pub fn has_capacity(&self, scope: InFlightScope, key: u32, limit: u32) -> bool {
        self.get(scope, key) < limit
    }

    /// Reset counter for an entity to zero. Used during drain.
    #[inline]
    pub fn reset(&mut self, scope: InFlightScope, key: u32) {
        match scope {
            InFlightScope::Subject => { self.subject.remove(&key); }
            InFlightScope::Consumer => {
                if let Some(c) = self.consumer.get_mut(key as usize) { *c = 0; }
            }
            InFlightScope::Queue => {
                if let Some(c) = self.queue.get_mut(key as usize) { *c = 0; }
            }
        }
    }

    /// Decrement subject, consumer, and queue in one call.
    /// Hot path for `release_pending` — skips the 3× scope match.
    /// The subject branch is gated on `track_subject` — common path is
    /// two Vec writes (~2-3 ns) instead of HashMap::get_mut + Vec×2.
    #[inline]
    pub fn dec_pending(&mut self, subject_hash: u32, consumer_id: u32, queue_id: u32) {
        if self.track_subject {
            dec_subject(&mut self.subject, subject_hash);
        }
        let ci = consumer_id as usize;
        if let Some(c) = self.consumer.get_mut(ci) {
            *c = c.saturating_sub(1);
        }
        let qi = queue_id as usize;
        if let Some(c) = self.queue.get_mut(qi) {
            *c = c.saturating_sub(1);
        }
    }

    /// Increment subject, consumer, and queue in one call.
    /// Hot path for `claim` — skips the 3× scope match.
    /// The subject branch is gated on `track_subject` — common path is
    /// two Vec writes (~2-3 ns) instead of HashMap::entry + Vec×2.
    #[inline]
    pub fn inc_pending(&mut self, subject_hash: u32, consumer_id: u32, queue_id: u32) {
        if self.track_subject {
            *self.subject.entry(subject_hash).or_insert(0) += 1;
        }
        let ci = consumer_id as usize;
        Self::ensure_len(&mut self.consumer, ci);
        self.consumer[ci] += 1;
        let qi = queue_id as usize;
        Self::ensure_len(&mut self.queue, qi);
        self.queue[qi] += 1;
    }
}

impl Default for InFlightCounters {
    fn default() -> Self { Self::new() }
}

/// Decrement a subject counter in the HashMap, removing entry at zero.
#[inline]
fn dec_subject(map: &mut HashMap<u32, u32, foldhash::fast::FixedState>, key: u32) {
    if let Some(count) = map.get_mut(&key) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            map.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inc_dec_basic() {
        let mut c = InFlightCounters::new();
        assert_eq!(c.get(InFlightScope::Subject, 10), 0);

        c.inc(InFlightScope::Subject, 10);
        c.inc(InFlightScope::Subject, 10);
        assert_eq!(c.get(InFlightScope::Subject, 10), 2);

        c.dec(InFlightScope::Subject, 10);
        assert_eq!(c.get(InFlightScope::Subject, 10), 1);

        c.dec(InFlightScope::Subject, 10);
        assert_eq!(c.get(InFlightScope::Subject, 10), 0);
    }

    #[test]
    fn dec_saturates_at_zero() {
        let mut c = InFlightCounters::new();
        c.dec(InFlightScope::Consumer, 5);
        assert_eq!(c.get(InFlightScope::Consumer, 5), 0);
    }

    #[test]
    fn has_capacity() {
        let mut c = InFlightCounters::new();
        assert!(c.has_capacity(InFlightScope::Queue, 1, 10));

        for _ in 0..10 {
            c.inc(InFlightScope::Queue, 1);
        }
        assert!(!c.has_capacity(InFlightScope::Queue, 1, 10));
        assert!(c.has_capacity(InFlightScope::Queue, 1, 11));
    }

    #[test]
    fn dec_pending_convenience() {
        let mut c = InFlightCounters::new();
        c.enable_subject_tracking();
        c.inc_pending(0xBEEF, 20, 100);
        c.inc_pending(0xBEEF, 20, 100);

        assert_eq!(c.get(InFlightScope::Subject, 0xBEEF), 2);
        assert_eq!(c.get(InFlightScope::Consumer, 20), 2);
        assert_eq!(c.get(InFlightScope::Queue, 100), 2);

        c.dec_pending(0xBEEF, 20, 100);
        assert_eq!(c.get(InFlightScope::Subject, 0xBEEF), 1);
        assert_eq!(c.get(InFlightScope::Consumer, 20), 1);
        assert_eq!(c.get(InFlightScope::Queue, 100), 1);
    }

    #[test]
    fn subject_gate_off_by_default_skips_subject_map() {
        let mut c = InFlightCounters::new();
        assert!(!c.is_tracking_subject());
        c.inc_pending(0xBEEF, 20, 100);
        c.inc_pending(0xBEEF, 20, 100);
        // Subject map remains empty; consumer/queue still tracked.
        assert_eq!(c.get(InFlightScope::Subject, 0xBEEF), 0);
        assert_eq!(c.get(InFlightScope::Consumer, 20), 2);
        assert_eq!(c.get(InFlightScope::Queue, 100), 2);
        // dec is also a no-op on subject — no underflow, stays consistent.
        c.dec_pending(0xBEEF, 20, 100);
        assert_eq!(c.get(InFlightScope::Subject, 0xBEEF), 0);
        assert_eq!(c.get(InFlightScope::Consumer, 20), 1);
    }

    #[test]
    fn reset_clears_counter() {
        let mut c = InFlightCounters::new();
        c.inc(InFlightScope::Subject, 42);
        c.inc(InFlightScope::Subject, 42);
        c.inc(InFlightScope::Subject, 42);
        assert_eq!(c.get(InFlightScope::Subject, 42), 3);

        c.reset(InFlightScope::Subject, 42);
        assert_eq!(c.get(InFlightScope::Subject, 42), 0);
    }

    #[test]
    fn scopes_are_independent() {
        let mut c = InFlightCounters::new();
        c.inc(InFlightScope::Subject, 1);
        c.inc(InFlightScope::Consumer, 1);
        c.inc(InFlightScope::Queue, 1);

        assert_eq!(c.get(InFlightScope::Subject, 1), 1);
        assert_eq!(c.get(InFlightScope::Consumer, 1), 1);
        assert_eq!(c.get(InFlightScope::Queue, 1), 1);

        c.dec(InFlightScope::Subject, 1);
        assert_eq!(c.get(InFlightScope::Subject, 1), 0);
        assert_eq!(c.get(InFlightScope::Consumer, 1), 1);
    }

    #[test]
    fn dense_id_autogrow() {
        // Writing to a high ID grows the Vec; reads in the gap return 0.
        let mut c = InFlightCounters::new();
        c.inc(InFlightScope::Consumer, 500);
        assert_eq!(c.get(InFlightScope::Consumer, 500), 1);
        assert_eq!(c.get(InFlightScope::Consumer, 250), 0);
        assert_eq!(c.get(InFlightScope::Consumer, 10_000), 0);
    }

    #[test]
    fn dense_dec_out_of_range_is_noop() {
        let mut c = InFlightCounters::new();
        c.dec(InFlightScope::Queue, 9999);
        c.dec_pending(0, 9999, 9999);
        assert_eq!(c.get(InFlightScope::Queue, 9999), 0);
        assert_eq!(c.get(InFlightScope::Consumer, 9999), 0);
    }
}
