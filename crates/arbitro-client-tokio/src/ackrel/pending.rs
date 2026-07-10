//! Per-consumer hot-path pending-ack set. Tracks which delivered `seq`s
//! have not yet been confirmed by the broker, with an O(1) reject gate
//! (`min_seq`/`max_seq`/`count`) in front of the `BTreeSet` lock so the
//! common "not pending" case never touches the mutex.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;

/// Compare-exchange loop for a running minimum. `AtomicU64::fetch_min`
/// isn't stable, so we roll our own with `Relaxed` ordering — this map
/// is a hint structure, not a synchronization point.
fn atomic_min(a: &AtomicU64, v: u64) {
    let mut cur = a.load(Ordering::Relaxed);
    while v < cur {
        match a.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => cur = observed,
        }
    }
}

/// Compare-exchange loop for a running maximum. See [`atomic_min`].
fn atomic_max(a: &AtomicU64, v: u64) {
    let mut cur = a.load(Ordering::Relaxed);
    while v > cur {
        match a.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => cur = observed,
        }
    }
}

/// Deferred (not-yet-broker-persisted) sequence numbers for one consumer.
/// Reconnect bumps `generation`; a stale generation is purged wholesale
/// by the owning [`super::AckRelay`] rather than reconciled entry-by-entry.
pub(crate) struct ConsumerPending {
    pub generation: u32,
    pub count: AtomicU32,
    pub min_seq: AtomicU64,
    pub max_seq: AtomicU64,
    pub pending: Mutex<BTreeSet<u64>>,
    pub oldest_ts_ms: AtomicU64,
}

impl ConsumerPending {
    pub fn new(generation: u32) -> Self {
        Self {
            generation,
            count: AtomicU32::new(0),
            min_seq: AtomicU64::new(u64::MAX),
            max_seq: AtomicU64::new(0),
            pending: Mutex::new(BTreeSet::new()),
            oldest_ts_ms: AtomicU64::new(0),
        }
    }

    /// Records `seq` as pending. Returns `true` if it was newly inserted
    /// (the caller should count it toward the hot-set gauge).
    pub fn record(&self, seq: u64, now_ms: u64) -> bool {
        let newly_inserted = self.pending.lock().unwrap().insert(seq);
        if newly_inserted {
            self.count.fetch_add(1, Ordering::Relaxed);
            atomic_min(&self.min_seq, seq);
            atomic_max(&self.max_seq, seq);
            let _ = self
                .oldest_ts_ms
                .compare_exchange(0, now_ms, Ordering::Relaxed, Ordering::Relaxed);
        }
        newly_inserted
    }

    /// GATE CASCADE: range check on atomics before ever taking the lock.
    pub fn is_pending(&self, seq: u64) -> bool {
        if self.count.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let mn = self.min_seq.load(Ordering::Relaxed);
        let mx = self.max_seq.load(Ordering::Relaxed);
        if seq < mn || seq > mx {
            return false;
        }
        self.pending.lock().unwrap().contains(&seq)
    }

    /// Drops every entry `<= new_cursor` (broker-confirmed cumulative ack).
    /// Returns the number removed.
    pub fn purge_up_to(&self, new_cursor: u64) -> u32 {
        let mut g = self.pending.lock().unwrap();
        let old_len = g.len();
        let kept = g.split_off(&new_cursor.saturating_add(1));
        let removed = (old_len - kept.len()) as u32;
        *g = kept;
        self.count.store(g.len() as u32, Ordering::Relaxed);
        match (g.iter().next(), g.iter().next_back()) {
            (Some(&mn), Some(&mx)) => {
                self.min_seq.store(mn, Ordering::Relaxed);
                self.max_seq.store(mx, Ordering::Relaxed);
            }
            _ => {
                self.min_seq.store(u64::MAX, Ordering::Relaxed);
                self.max_seq.store(0, Ordering::Relaxed);
                self.oldest_ts_ms.store(0, Ordering::Relaxed);
            }
        }
        removed
    }

    /// Up to `max` smallest pending seqs, ascending. Non-destructive —
    /// removal only happens via [`Self::purge_up_to`] on broker confirm.
    pub fn drain_ascending(&self, max: usize) -> Vec<u64> {
        self.pending.lock().unwrap().iter().take(max).copied().collect()
    }

    /// TTL sweep. We only track the oldest timestamp in the set (not
    /// per-seq), so once that oldest entry crosses `cutoff_ms` the whole
    /// set is considered expired and dropped together.
    pub fn expire_older_than(&self, cutoff_ms: u64, _now_ms: u64) -> u32 {
        let oldest = self.oldest_ts_ms.load(Ordering::Relaxed);
        if oldest == 0 || oldest > cutoff_ms {
            return 0;
        }
        let mut g = self.pending.lock().unwrap();
        let removed = g.len() as u32;
        g.clear();
        self.count.store(0, Ordering::Relaxed);
        self.min_seq.store(u64::MAX, Ordering::Relaxed);
        self.max_seq.store(0, Ordering::Relaxed);
        self.oldest_ts_ms.store(0, Ordering::Relaxed);
        removed
    }
}
