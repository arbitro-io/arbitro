//! Edge storage primitives: `HashEdge` (one-to-many) and `UniqueEdge`
//! (one-to-one).
//!
//! Level 3 — depends on `types`, `error`. Does NOT know about specific edges.
//!
//! `BuiltinEdges` in [`super::builtin`] composes these primitives as named
//! fields. There is no `TypeId`-keyed registry, no `Box<dyn Any>`, and no
//! virtual dispatch — the whole point of the refactor that replaced the
//! old `EdgeRegistry` is that edge access must be a single direct field
//! load on the hot path (`performance.md` §11).

use std::collections::{HashMap, VecDeque};

use crate::types::{ConsumerId, SlabKey};

/// One-to-many edge index backed by `HashMap<K, Vec<V>>`.
///
/// Each parent key maps to a small `Vec` of child values. Remove is
/// swap-remove + linear scan (typical child count ≤ a handful).
pub struct HashEdge<K, V> {
    map: HashMap<K, Vec<V>, ahash::RandomState>,
    empty: Vec<V>,
}

impl<K, V> HashEdge<K, V>
where
    K: Eq + std::hash::Hash + Copy,
    V: Copy + PartialEq,
{
    pub fn new() -> Self {
        Self {
            map: HashMap::with_hasher(ahash::RandomState::new()),
            empty: Vec::new(),
        }
    }

    /// Insert a mapping: parent → child. O(1) amortized.
    #[inline]
    pub fn insert(&mut self, parent: &K, child: V) {
        self.map.entry(*parent).or_default().push(child);
    }

    /// Remove a specific child from a parent. O(k) in siblings (typically ≤ 3).
    #[inline]
    pub fn remove(&mut self, parent: &K, child: &V) {
        if let Some(children) = self.map.get_mut(parent) {
            if let Some(pos) = children.iter().position(|c| c == child) {
                children.swap_remove(pos);
            }
            if children.is_empty() {
                self.map.remove(parent);
            }
        }
    }

    /// Get all children for a parent (shared reference). O(1).
    #[inline]
    pub fn get(&self, parent: &K) -> &[V] {
        self.map.get(parent).map(|v| v.as_slice()).unwrap_or(&self.empty)
    }

    /// Take (drain) all children for a parent, removing them from the index. O(1).
    #[inline]
    pub fn take(&mut self, parent: &K) -> Vec<V> {
        self.map.remove(parent).unwrap_or_default()
    }

    /// Get a single child for a parent, if exactly one exists.
    #[inline]
    pub fn get_one(&self, parent: &K) -> Option<V> {
        let items = self.get(parent);
        if items.len() == 1 { Some(items[0]) } else { None }
    }

    /// Check if a parent has any children.
    #[inline]
    pub fn contains_key(&self, parent: &K) -> bool {
        !self.get(parent).is_empty()
    }

    /// Number of parent keys in this index.
    #[inline]
    pub fn len(&self) -> usize { self.map.len() }

    /// Whether this index is empty.
    #[inline]
    pub fn is_empty(&self) -> bool { self.map.is_empty() }
}

impl<K, V> Default for HashEdge<K, V>
where
    K: Eq + std::hash::Hash + Copy,
    V: Copy + PartialEq,
{
    fn default() -> Self { Self::new() }
}

/// One-to-one edge index: each key maps to exactly one value.
///
/// Used for edges like `pending_by_consumer_seq` where the composite key
/// `(ConsumerId, seq)` maps to exactly one `SlabKey`.
pub struct UniqueEdge<K, V> {
    map: HashMap<K, V, ahash::RandomState>,
}

impl<K, V> UniqueEdge<K, V>
where
    K: Eq + std::hash::Hash + Copy,
    V: Copy + PartialEq,
{
    pub fn new() -> Self {
        Self {
            map: HashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    #[inline]
    pub fn insert_unique(&mut self, key: K, value: V) {
        self.map.insert(key, value);
    }

    #[inline]
    pub fn get_unique(&self, key: &K) -> Option<V> {
        self.map.get(key).copied()
    }

    #[inline]
    pub fn remove_unique(&mut self, key: &K) -> Option<V> {
        self.map.remove(key)
    }

    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    #[inline]
    pub fn len(&self) -> usize { self.map.len() }

    #[inline]
    pub fn is_empty(&self) -> bool { self.map.is_empty() }
}

impl<K, V> Default for UniqueEdge<K, V>
where
    K: Eq + std::hash::Hash + Copy,
    V: Copy + PartialEq,
{
    fn default() -> Self { Self::new() }
}

/// Per-consumer `(seq → SlabKey)` index, replacing a `HashMap<(ConsumerId,
/// u64), SlabKey>`. Each consumer owns a small `VecDeque<(seq, SlabKey)>`
/// that is a de-facto FIFO of live pending entries.
///
/// ## Why a Vec-of-deques instead of a HashMap
///
/// Ack is the **only** hot-path lookup on this edge (publish and claim
/// do not read it; they only write it). In real workloads ack happens
/// in-order far more often than not — queues are FIFO per subject, and
/// clients typically ack as they finish. The fast path here is:
///
///   1. `deques[consumer_id].front()` — O(1) pointer deref
///   2. Compare `front.seq == target_seq` — 1 cmp
///   3. `pop_front()` — O(1)
///
/// That is ~3-5 ns vs ~15-20 ns for a HashMap tuple-key lookup. Out-of-
/// order acks fall back to a linear scan bounded by `max_inflight` (a
/// few cache lines; for typical max_inflight ≤ 256 this is still faster
/// than a HashMap hit).
///
/// ## Invariants
///
/// - `deques[i]` is ALWAYS sorted ascending by seq, because claim pushes
///   in global-seq order and we never reorder on remove.
/// - The outer `Vec` is grown lazily on first `insert` for a new
///   consumer; empty slots are `VecDeque::new()` (0 alloc).
pub struct ConsumerSeqEdge {
    deques: Vec<VecDeque<(u64, SlabKey)>>,
}

impl ConsumerSeqEdge {
    #[inline]
    pub fn new() -> Self {
        Self { deques: Vec::new() }
    }

    /// Ensure the outer vec has a slot for `consumer_id`. Cold-path —
    /// called only when a consumer first claims.
    #[inline(never)]
    #[cold]
    fn grow_for(&mut self, idx: usize) {
        // Round up to the next power of two for amortized growth.
        let new_len = (idx + 1).max(8).next_power_of_two();
        self.deques.resize_with(new_len, VecDeque::new);
    }

    /// Push `(seq, key)` to the back of the consumer's deque. Seqs must
    /// be monotonically non-decreasing per consumer (invariant guaranteed
    /// by claim which pops from the ready queue in seq order).
    #[inline]
    pub fn insert(&mut self, consumer_id: ConsumerId, seq: u64, key: SlabKey) {
        let idx = consumer_id.0 as usize;
        if idx >= self.deques.len() {
            self.grow_for(idx);
        }
        // SAFETY: just grew above.
        unsafe { self.deques.get_unchecked_mut(idx) }.push_back((seq, key));
    }

    /// Look up the SlabKey for `(consumer_id, seq)`. Fast path is a
    /// front-of-deque match; falls back to a bounded linear scan.
    #[inline]
    pub fn get(&self, consumer_id: ConsumerId, seq: u64) -> Option<SlabKey> {
        let dq = self.deques.get(consumer_id.0 as usize)?;
        // In-order ack fast path.
        if let Some(&(front_seq, key)) = dq.front() {
            if front_seq == seq {
                return Some(key);
            }
        }
        // Out-of-order — linear scan. Bounded by live entries for this
        // consumer (≤ max_inflight).
        dq.iter().find(|&&(s, _)| s == seq).map(|&(_, k)| k)
    }

    /// Remove `(consumer_id, seq)` and return the associated SlabKey.
    /// Same fast-path as `get`.
    #[inline]
    pub fn remove(&mut self, consumer_id: ConsumerId, seq: u64) -> Option<SlabKey> {
        let dq = self.deques.get_mut(consumer_id.0 as usize)?;
        // In-order: pop front.
        if let Some(&(front_seq, _)) = dq.front() {
            if front_seq == seq {
                return dq.pop_front().map(|(_, k)| k);
            }
        }
        // Out-of-order: scan + remove. O(k) in both the scan and the
        // internal VecDeque shift, but k ≤ max_inflight and out-of-order
        // acks are rare in real workloads.
        let pos = dq.iter().position(|&(s, _)| s == seq)?;
        dq.remove(pos).map(|(_, k)| k)
    }

    /// Test-only: does `(consumer_id, seq)` exist in the index?
    #[inline]
    pub fn contains(&self, consumer_id: ConsumerId, seq: u64) -> bool {
        self.get(consumer_id, seq).is_some()
    }
}

impl Default for ConsumerSeqEdge {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_edge_insert_get_remove() {
        let mut edge = HashEdge::<u32, u32>::new();
        edge.insert(&1, 10);
        edge.insert(&1, 20);
        edge.insert(&2, 30);

        assert_eq!(edge.get(&1), &[10, 20]);
        assert_eq!(edge.get(&2), &[30]);
        assert_eq!(edge.get(&99), &[]);

        edge.remove(&1, &10);
        assert_eq!(edge.get(&1), &[20]);

        let taken = edge.take(&1);
        assert_eq!(taken, vec![20]);
        assert_eq!(edge.get(&1), &[]);
    }

    #[test]
    fn unique_edge_operations() {
        let mut edge = UniqueEdge::<(u32, u64), u32>::new();
        edge.insert_unique((10, 100), 42);
        edge.insert_unique((10, 200), 43);

        assert_eq!(edge.get_unique(&(10, 100)), Some(42));
        assert_eq!(edge.get_unique(&(10, 200)), Some(43));
        assert_eq!(edge.get_unique(&(10, 300)), None);

        assert_eq!(edge.remove_unique(&(10, 100)), Some(42));
        assert_eq!(edge.get_unique(&(10, 100)), None);
    }

    #[test]
    fn consumer_seq_in_order_fast_path() {
        let mut edge = ConsumerSeqEdge::new();
        let c = ConsumerId(3);

        edge.insert(c, 10, SlabKey::new(100, 0));
        edge.insert(c, 11, SlabKey::new(101, 0));
        edge.insert(c, 12, SlabKey::new(102, 0));

        assert_eq!(edge.get(c, 10), Some(SlabKey::new(100, 0)));
        assert_eq!(edge.get(c, 12), Some(SlabKey::new(102, 0)));
        assert_eq!(edge.get(c, 99), None);

        // In-order acks: front pops each time
        assert_eq!(edge.remove(c, 10), Some(SlabKey::new(100, 0)));
        assert_eq!(edge.remove(c, 11), Some(SlabKey::new(101, 0)));
        assert_eq!(edge.remove(c, 12), Some(SlabKey::new(102, 0)));
        assert_eq!(edge.get(c, 10), None);
    }

    #[test]
    fn consumer_seq_out_of_order() {
        let mut edge = ConsumerSeqEdge::new();
        let c = ConsumerId(7);

        edge.insert(c, 1, SlabKey::new(10, 0));
        edge.insert(c, 2, SlabKey::new(20, 0));
        edge.insert(c, 3, SlabKey::new(30, 0));

        // Ack middle first
        assert_eq!(edge.remove(c, 2), Some(SlabKey::new(20, 0)));
        // Front is still 1, back is 3
        assert_eq!(edge.remove(c, 1), Some(SlabKey::new(10, 0)));
        assert_eq!(edge.remove(c, 3), Some(SlabKey::new(30, 0)));
    }

    #[test]
    fn consumer_seq_unknown_consumer_is_none() {
        let edge = ConsumerSeqEdge::new();
        assert_eq!(edge.get(ConsumerId(999), 1), None);
    }

    #[test]
    fn consumer_seq_grows_for_high_id() {
        let mut edge = ConsumerSeqEdge::new();
        edge.insert(ConsumerId(1000), 42, SlabKey::new(1, 0));
        assert_eq!(edge.get(ConsumerId(1000), 42), Some(SlabKey::new(1, 0)));
        assert_eq!(edge.get(ConsumerId(500), 42), None);
    }

    #[test]
    fn hash_edge_empty_key_cleanup() {
        let mut edge = HashEdge::<u32, u32>::new();
        edge.insert(&1, 10);
        edge.remove(&1, &10);
        // Parent key is cleaned up when no children remain
        assert_eq!(edge.len(), 0);
    }
}
