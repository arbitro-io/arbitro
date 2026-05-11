//! InFlight counters — dense Vec for consumer/queue.
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
//! Per-(consumer, subject) inflight used to live here as a sparse
//! `HashMap<u32, u32>`. It now lives in the server, owned exclusively
//! by the drain thread
//! (`arbitro_server::shard::consumer_subjects::ConsumerSubjects`) — see
//! commit history under `refactor/consumer-owned-counters` for the
//! rationale + head-to-head bench.
//!
//! ## Auto-grow
//!
//! Writes grow the Vec if the key is beyond the current len. At steady
//! state (post-startup, when all consumers/queues are registered) the
//! resize branch never fires and the CPU branch predictor eats it for
//! free. Reads outside the range return 0 without allocating.

/// Scope for inflight counting. Subject scope was removed — it now lives
/// in the server as drain-owned `ConsumerSubjects`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InFlightScope {
    Consumer,
    Queue,
}

/// Per-scope inflight counter storage. See module docs for rationale.
pub struct InFlightCounters {
    /// Consumer inflight: dense consumer_id → Vec indexed by raw id.
    consumer: Vec<u32>,
    /// Queue inflight: dense queue_id → Vec indexed by raw id.
    queue: Vec<u32>,
}

impl InFlightCounters {
    pub fn new() -> Self {
        Self {
            // Start with a modest capacity; auto-grows on first write past len.
            consumer: Vec::with_capacity(64),
            queue: Vec::with_capacity(64),
        }
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
        let vec = match scope {
            InFlightScope::Consumer => &mut self.consumer,
            InFlightScope::Queue => &mut self.queue,
        };
        let i = key as usize;
        Self::ensure_len(vec, i);
        vec[i] += 1;
    }

    /// Decrement the inflight count for an entity. O(1).
    /// Saturates at zero — never underflows.
    #[inline]
    pub fn dec(&mut self, scope: InFlightScope, key: u32) {
        let vec = match scope {
            InFlightScope::Consumer => &mut self.consumer,
            InFlightScope::Queue => &mut self.queue,
        };
        if let Some(c) = vec.get_mut(key as usize) {
            *c = c.saturating_sub(1);
        }
    }

    /// Get the current inflight count. O(1).
    #[inline]
    pub fn get(&self, scope: InFlightScope, key: u32) -> u32 {
        let vec = match scope {
            InFlightScope::Consumer => &self.consumer,
            InFlightScope::Queue => &self.queue,
        };
        vec.get(key as usize).copied().unwrap_or(0)
    }

    /// Check if inflight count is below a limit. O(1).
    #[inline]
    pub fn has_capacity(&self, scope: InFlightScope, key: u32, limit: u32) -> bool {
        self.get(scope, key) < limit
    }

    /// Reset counter for an entity to zero. Used during drain.
    #[inline]
    pub fn reset(&mut self, scope: InFlightScope, key: u32) {
        let vec = match scope {
            InFlightScope::Consumer => &mut self.consumer,
            InFlightScope::Queue => &mut self.queue,
        };
        if let Some(c) = vec.get_mut(key as usize) {
            *c = 0;
        }
    }

    /// Decrement consumer + queue in one call.
    /// Hot path for `release_pending` — skips the per-scope match.
    #[inline]
    pub fn dec_pending(&mut self, consumer_id: u32, queue_id: u32) {
        let ci = consumer_id as usize;
        if let Some(c) = self.consumer.get_mut(ci) {
            *c = c.saturating_sub(1);
        }
        let qi = queue_id as usize;
        if let Some(c) = self.queue.get_mut(qi) {
            *c = c.saturating_sub(1);
        }
    }

    /// Increment consumer + queue in one call.
    /// Hot path for `claim` — skips the per-scope match.
    #[inline]
    pub fn inc_pending(&mut self, consumer_id: u32, queue_id: u32) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inc_dec_basic() {
        let mut c = InFlightCounters::new();
        assert_eq!(c.get(InFlightScope::Consumer, 10), 0);

        c.inc(InFlightScope::Consumer, 10);
        c.inc(InFlightScope::Consumer, 10);
        assert_eq!(c.get(InFlightScope::Consumer, 10), 2);

        c.dec(InFlightScope::Consumer, 10);
        assert_eq!(c.get(InFlightScope::Consumer, 10), 1);

        c.dec(InFlightScope::Consumer, 10);
        assert_eq!(c.get(InFlightScope::Consumer, 10), 0);
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
        c.inc_pending(20, 100);
        c.inc_pending(20, 100);

        assert_eq!(c.get(InFlightScope::Consumer, 20), 2);
        assert_eq!(c.get(InFlightScope::Queue, 100), 2);

        c.dec_pending(20, 100);
        assert_eq!(c.get(InFlightScope::Consumer, 20), 1);
        assert_eq!(c.get(InFlightScope::Queue, 100), 1);
    }

    #[test]
    fn reset_clears_counter() {
        let mut c = InFlightCounters::new();
        c.inc(InFlightScope::Consumer, 42);
        c.inc(InFlightScope::Consumer, 42);
        c.inc(InFlightScope::Consumer, 42);
        assert_eq!(c.get(InFlightScope::Consumer, 42), 3);

        c.reset(InFlightScope::Consumer, 42);
        assert_eq!(c.get(InFlightScope::Consumer, 42), 0);
    }

    #[test]
    fn scopes_are_independent() {
        let mut c = InFlightCounters::new();
        c.inc(InFlightScope::Consumer, 1);
        c.inc(InFlightScope::Queue, 1);

        assert_eq!(c.get(InFlightScope::Consumer, 1), 1);
        assert_eq!(c.get(InFlightScope::Queue, 1), 1);

        c.dec(InFlightScope::Consumer, 1);
        assert_eq!(c.get(InFlightScope::Consumer, 1), 0);
        assert_eq!(c.get(InFlightScope::Queue, 1), 1);
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
        c.dec_pending(9999, 9999);
        assert_eq!(c.get(InFlightScope::Queue, 9999), 0);
        assert_eq!(c.get(InFlightScope::Consumer, 9999), 0);
    }
}
