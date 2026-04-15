//! Per-queue ready state management.
//!
//! Level 3 — depends on `types`, `ready/ring`.

pub mod ring;

use std::collections::HashMap;
use crate::types::QueueId;
use ring::ReadySubjectRing;

/// Manages ready state for all queues.
///
/// Each queue has its own `ReadySubjectRing` for fair round-robin
/// delivery across subjects (anti-HOL blocking).
pub struct ReadyState {
    rings: HashMap<QueueId, ReadySubjectRing, ahash::RandomState>,
}

impl ReadyState {
    pub fn new() -> Self {
        Self {
            rings: HashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    /// Enqueue a sequence number for delivery. O(1).
    #[inline]
    pub fn push(&mut self, queue_id: QueueId, subject_hash: u32, seq: u64) {
        self.rings
            .entry(queue_id)
            .or_default()
            .push(subject_hash, seq);
    }

    /// Enqueue a batch of (subject_hash, seq) pairs into a single queue.
    /// Resolves the ring once — avoids N HashMap lookups on `rings`.
    #[inline]
    pub fn push_batch(&mut self, queue_id: QueueId, items: &[(u32, u64)]) {
        let ring = self.rings.entry(queue_id).or_default();
        for &(subject_hash, seq) in items {
            ring.push(subject_hash, seq);
        }
    }

    /// Enqueue a nacked sequence for priority redelivery. O(1).
    #[inline]
    pub fn push_nacked(&mut self, queue_id: QueueId, subject_hash: u32, seq: u64) {
        self.rings
            .entry(queue_id)
            .or_default()
            .push_nacked(subject_hash, seq);
    }

    /// Pop the next ready (subject, seq) from a queue. O(1).
    #[inline]
    pub fn pop(&mut self, queue_id: QueueId) -> Option<(u32, u64)> {
        self.rings.get_mut(&queue_id)?.pop()
    }

    /// Peek the current front subject for a queue. O(1).
    #[inline]
    pub fn peek_subject(&self, queue_id: QueueId) -> Option<u32> {
        self.rings.get(&queue_id)?.peek_subject()
    }

    /// Skip the current subject for a queue (inflight limit hit). O(1).
    #[inline]
    pub fn skip_current(&mut self, queue_id: QueueId) {
        if let Some(ring) = self.rings.get_mut(&queue_id) {
            ring.skip_current();
        }
    }

    /// Whether a queue has any ready work.
    #[inline]
    pub fn has_ready(&self, queue_id: QueueId) -> bool {
        self.rings.get(&queue_id).map_or(false, |r| !r.is_empty())
    }

    /// Total ready entries for a queue.
    pub fn total_ready(&self, queue_id: QueueId) -> usize {
        self.rings.get(&queue_id).map_or(0, |r| r.total_ready())
    }

    /// Clear all ready work for a queue. Used by purge.
    pub fn clear_queue(&mut self, queue_id: QueueId) {
        if let Some(ring) = self.rings.get_mut(&queue_id) {
            ring.clear();
        }
    }

    /// Remove a queue entirely.
    pub fn remove_queue(&mut self, queue_id: QueueId) {
        self.rings.remove(&queue_id);
    }
}

impl Default for ReadyState {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_queue_isolation() {
        let mut ready = ReadyState::new();
        let q1 = QueueId(1);
        let q2 = QueueId(2);

        ready.push(q1, 10, 100);
        ready.push(q2, 20, 200);

        assert_eq!(ready.pop(q1), Some((10, 100)));
        assert_eq!(ready.pop(q2), Some((20, 200)));
        assert_eq!(ready.pop(q1), None);
    }

    #[test]
    fn nacked_priority() {
        let mut ready = ReadyState::new();
        let q = QueueId(1);

        ready.push(q, 10, 100);
        ready.push(q, 10, 200);
        ready.push_nacked(q, 10, 50);

        assert_eq!(ready.pop(q), Some((10, 50)));
        assert_eq!(ready.pop(q), Some((10, 100)));
    }

    #[test]
    fn has_ready() {
        let mut ready = ReadyState::new();
        let q = QueueId(1);

        assert!(!ready.has_ready(q));
        ready.push(q, 10, 1);
        assert!(ready.has_ready(q));
        ready.pop(q);
        assert!(!ready.has_ready(q));
    }

    #[test]
    fn clear_queue() {
        let mut ready = ReadyState::new();
        let q = QueueId(1);
        ready.push(q, 10, 1);
        ready.push(q, 20, 2);
        ready.clear_queue(q);
        assert!(!ready.has_ready(q));
    }
}
