//! Intrusive pending edge — head-pointer HashMap over a doubly-linked list
//! threaded through `PendingNode::edge_prev` / `edge_next`.
//!
//! Level 3. O(1) insert/remove regardless of list length or HashMap size.
//! Replaces `HashEdge<K, SlabKey>` for the 6 pending-hot-path edges.
//!
//! The old `HashMap<K, Vec<SlabKey>>` layout made `release_pending` O(log N)
//! in practice — the per-parent Vec pushed data out of cache as the inflight
//! population grew. At 100k inflight the 6 edge removes cost ~7 µs. With
//! intrusive lists the per-release edge work is a handful of cache-line
//! writes on the two neighbors + (optionally) a HashMap update if the
//! removed node was the head. See `release_breakdown` bench.

use std::collections::HashMap;
use crate::graph::node::PendingNode;
use crate::graph::slab::TypedSlab;
use crate::types::SlabKey;

/// Head-pointer index over an intrusive doubly-linked list. `K` is the
/// parent key (e.g. `ConnectionId`); list nodes live inside `PendingNode`
/// in the `edge_prev[i]` / `edge_next[i]` slots identified by `edge_idx`.
pub struct PendingEdge<K> {
    heads: HashMap<K, SlabKey, ahash::RandomState>,
    edge_idx: usize,
}

impl<K: Eq + std::hash::Hash + Copy> PendingEdge<K> {
    /// `edge_idx` must be one of the `pending_edge_idx::*` constants and
    /// must match the field order in `BuiltinEdges` — two edges cannot
    /// share an index or they will corrupt each other's lists.
    pub fn new(edge_idx: usize) -> Self {
        Self {
            heads: HashMap::with_hasher(ahash::RandomState::new()),
            edge_idx,
        }
    }

    /// Insert `node_key` at the head of the list for `parent`. O(1).
    ///
    /// `slab` must already contain `node_key`. Writes `node.edge_prev[i]`
    /// / `edge_next[i]` and patches the previous head's `prev` pointer.
    #[inline]
    pub fn insert_head(
        &mut self,
        slab: &mut TypedSlab<PendingNode>,
        parent: K,
        node_key: SlabKey,
    ) {
        let i = self.edge_idx;
        let old_head = self
            .heads
            .insert(parent, node_key)
            .unwrap_or(SlabKey::DANGLING);

        // SAFETY: caller guarantees node_key was just inserted into slab.
        let node = slab.get_mut(node_key).expect("insert_head: node missing");
        node.edge_prev[i] = SlabKey::DANGLING;
        node.edge_next[i] = old_head;

        if old_head.index != u32::MAX {
            // invariant: old_head came from self.heads, so it must still
            // be in the slab until it is explicitly removed.
            let prev_node = slab
                .get_mut(old_head)
                .expect("insert_head: stale head in PendingEdge");
            prev_node.edge_prev[i] = node_key;
        }
    }

    /// Replace the head pointer for `parent` with `new_head`, returning
    /// the previous head (or `DANGLING` if there was none). Does **not**
    /// touch the slab — caller is responsible for patching link slots.
    ///
    /// Used by [`crate::edge::BuiltinEdges::insert_pending_all`] to fold
    /// the per-edge HashMap update into one consolidated insert path that
    /// shares a single `slab.get_mut(node_key)` across all 4 pending edges.
    #[inline]
    pub fn swap_head(&mut self, parent: K, new_head: SlabKey) -> SlabKey {
        self.heads.insert(parent, new_head).unwrap_or(SlabKey::DANGLING)
    }

    /// Edge index this `PendingEdge` writes into on `PendingNode`.
    /// Used by consolidated insert paths that need to know which slot
    /// of `edge_prev[]` / `edge_next[]` to patch on neighbor nodes.
    #[inline]
    pub fn edge_idx(&self) -> usize {
        self.edge_idx
    }

    /// Unlink `node_key` from the list for `parent`. O(1).
    ///
    /// Caller supplies `prev` and `next` already read from the node (the
    /// node may already be removed from the slab by this point). `slab`
    /// must still contain any non-DANGLING neighbors.
    #[inline]
    pub fn unlink(
        &mut self,
        slab: &mut TypedSlab<PendingNode>,
        parent: K,
        _node_key: SlabKey,
        prev: SlabKey,
        next: SlabKey,
    ) {
        let i = self.edge_idx;

        if prev.index != u32::MAX {
            // invariant: `prev` is still live; it's the node that pointed
            // to us via edge_next[i] and we were not its head.
            if let Ok(prev_node) = slab.get_mut(prev) {
                prev_node.edge_next[i] = next;
            }
        } else {
            // We were the head. Update the head map.
            if next.index == u32::MAX {
                self.heads.remove(&parent);
            } else {
                self.heads.insert(parent, next);
            }
        }

        if next.index != u32::MAX {
            if let Ok(next_node) = slab.get_mut(next) {
                next_node.edge_prev[i] = prev;
            }
        }
    }

    /// Drain all entries for `parent`. Walks the list, collects keys,
    /// nulls out prev/next for this edge on each visited node, and
    /// removes the heads entry. Caller then typically calls release_pending
    /// on each key — that release will see prev=next=DANGLING for this
    /// edge and do nothing, but will still unlink from the OTHER 5 edges.
    pub fn take(
        &mut self,
        slab: &mut TypedSlab<PendingNode>,
        parent: K,
    ) -> Vec<SlabKey> {
        let i = self.edge_idx;
        let mut out = Vec::new();
        let mut cur = self.heads.remove(&parent).unwrap_or(SlabKey::DANGLING);
        while cur.index != u32::MAX {
            let next = match slab.get_mut(cur) {
                Ok(node) => {
                    let n = node.edge_next[i];
                    node.edge_prev[i] = SlabKey::DANGLING;
                    node.edge_next[i] = SlabKey::DANGLING;
                    n
                }
                // Defensive: list diverged from slab reality. Stop.
                Err(_) => break,
            };
            out.push(cur);
            cur = next;
        }
        out
    }

    /// Read-only: does this parent have any entries?
    #[inline]
    pub fn contains_key(&self, parent: &K) -> bool {
        self.heads.contains_key(parent)
    }

    /// Get the head (or DANGLING if empty).
    #[inline]
    pub fn head(&self, parent: &K) -> SlabKey {
        self.heads.get(parent).copied().unwrap_or(SlabKey::DANGLING)
    }

    /// Walk the list from head and count entries for `parent`. O(k).
    /// Used by tests and cold-path introspection only.
    pub fn len_for(&self, slab: &TypedSlab<PendingNode>, parent: &K) -> usize {
        let i = self.edge_idx;
        let mut cur = self.head(parent);
        let mut n = 0;
        while cur.index != u32::MAX {
            n += 1;
            match slab.get(cur) {
                Ok(node) => cur = node.edge_next[i],
                Err(_) => break,
            }
        }
        n
    }

    /// Test-only introspection: walk the list and return all keys.
    #[cfg(test)]
    pub fn collect(&self, slab: &TypedSlab<PendingNode>, parent: &K) -> Vec<SlabKey> {
        let i = self.edge_idx;
        let mut out = Vec::new();
        let mut cur = self.head(parent);
        while cur.index != u32::MAX {
            out.push(cur);
            match slab.get(cur) {
                Ok(node) => cur = node.edge_next[i],
                Err(_) => break,
            }
        }
        out
    }

    /// Number of parent keys tracked (not total nodes).
    #[inline]
    pub fn len(&self) -> usize { self.heads.len() }

    /// Whether any parent has entries.
    #[inline]
    pub fn is_empty(&self) -> bool { self.heads.is_empty() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::node::{pending_edge_idx, PendingNode};
    use crate::types::*;

    fn make_pending(seq: u64) -> PendingNode {
        PendingNode {
            pending_id: PendingId(seq as u32),
            seq,
            queue_id: QueueId(1),
            consumer_id: ConsumerId(1),
            subscription_id: SubscriptionId(1),
            binding_id: BindingId(1),
            connection_id: ConnectionId(1),
            subject_hash: 0,
            credits: [CreditEntry { scope: CreditScope::Node, _pad: [0; 3], counter_idx: 0 }; 3],
            credit_count: 0,
            deadline_id: 0,
            delivered_at: Timestamp::new(0),
            ack_wait_ns: 0,
            edge_prev: [SlabKey::DANGLING; pending_edge_idx::COUNT],
            edge_next: [SlabKey::DANGLING; pending_edge_idx::COUNT],
        }
    }

    #[test]
    fn insert_then_head_matches() {
        let mut slab: TypedSlab<PendingNode> = TypedSlab::new();
        let mut edge: PendingEdge<u32> = PendingEdge::new(pending_edge_idx::CONNECTION);

        let k = slab.insert(make_pending(1));
        edge.insert_head(&mut slab, 42, k);

        assert_eq!(edge.head(&42), k);
        assert_eq!(edge.len_for(&slab, &42), 1);
    }

    #[test]
    fn insert_two_unlinks_middle() {
        let mut slab: TypedSlab<PendingNode> = TypedSlab::new();
        let mut edge: PendingEdge<u32> = PendingEdge::new(pending_edge_idx::CONSUMER);
        let i = pending_edge_idx::CONSUMER;

        let a = slab.insert(make_pending(1));
        let b = slab.insert(make_pending(2));
        let c = slab.insert(make_pending(3));
        edge.insert_head(&mut slab, 7, a);
        edge.insert_head(&mut slab, 7, b);
        edge.insert_head(&mut slab, 7, c);

        // Head order after 3 inserts-at-head: c -> b -> a
        assert_eq!(edge.collect(&slab, &7), vec![c, b, a]);

        // Unlink the middle (b)
        let prev = slab.get(b).unwrap().edge_prev[i];
        let next = slab.get(b).unwrap().edge_next[i];
        edge.unlink(&mut slab, 7u32, b, prev, next);
        assert_eq!(edge.collect(&slab, &7), vec![c, a]);
    }

    #[test]
    fn unlink_last_removes_heads_entry() {
        let mut slab: TypedSlab<PendingNode> = TypedSlab::new();
        let mut edge: PendingEdge<u32> = PendingEdge::new(pending_edge_idx::QUEUE);
        let i = pending_edge_idx::QUEUE;

        let a = slab.insert(make_pending(1));
        edge.insert_head(&mut slab, 9, a);
        assert!(edge.contains_key(&9));

        let prev = slab.get(a).unwrap().edge_prev[i];
        let next = slab.get(a).unwrap().edge_next[i];
        edge.unlink(&mut slab, 9u32, a, prev, next);

        assert!(!edge.contains_key(&9));
        assert!(edge.is_empty());
    }

    #[test]
    fn take_empties_everything() {
        let mut slab: TypedSlab<PendingNode> = TypedSlab::new();
        let mut edge: PendingEdge<u32> = PendingEdge::new(pending_edge_idx::SUBSCRIPTION);

        let a = slab.insert(make_pending(1));
        let b = slab.insert(make_pending(2));
        edge.insert_head(&mut slab, 3, a);
        edge.insert_head(&mut slab, 3, b);

        let out = edge.take(&mut slab, 3u32);
        assert_eq!(out.len(), 2);
        assert!(out.contains(&a));
        assert!(out.contains(&b));
        assert!(!edge.contains_key(&3));
    }

    #[test]
    fn take_nulls_out_pointers_for_visited_nodes() {
        let mut slab: TypedSlab<PendingNode> = TypedSlab::new();
        let mut edge: PendingEdge<u32> = PendingEdge::new(pending_edge_idx::CONNECTION);
        let i = pending_edge_idx::CONNECTION;

        let a = slab.insert(make_pending(1));
        let b = slab.insert(make_pending(2));
        edge.insert_head(&mut slab, 5, a);
        edge.insert_head(&mut slab, 5, b);

        edge.take(&mut slab, 5u32);

        for k in [a, b] {
            let n = slab.get(k).unwrap();
            assert_eq!(n.edge_prev[i], SlabKey::DANGLING);
            assert_eq!(n.edge_next[i], SlabKey::DANGLING);
        }
    }
}
