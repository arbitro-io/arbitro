//! ReadySubjectRing — round-robin delivery over subjects to prevent HOL blocking.
//!
//! Level 3 — depends on `types`.
//!
//! Each queue has a ring of subjects with ready work. When delivering,
//! the ring advances to the next subject, preventing a single subject
//! with many messages from starving others.

use std::collections::{HashMap, VecDeque};
use std::collections::hash_map::Entry;

/// Round-robin ring over subjects that have ready work for a single queue.
///
/// - `push(subject, seq)`: O(1) — add seq to subject's deque, add subject to ring if new.
/// - `pop() -> Option<(subject, seq)>`: O(1) — advance ring, pop from subject's deque.
/// - `skip_current()`: O(1) — advance ring past current subject (e.g. inflight limit hit).
pub struct ReadySubjectRing {
    /// Round-robin order of subjects. Front = next to deliver.
    subjects: VecDeque<u32>,
    /// Per-subject FIFO of ready sequence numbers.
    /// Also serves as ring membership check via `contains_key()`.
    per_subject: HashMap<u32, VecDeque<u64>, ahash::RandomState>,
}

impl ReadySubjectRing {
    pub fn new() -> Self {
        Self {
            subjects: VecDeque::new(),
            per_subject: HashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    /// Enqueue a sequence number for a subject. O(1).
    ///
    /// If the subject is not yet in the ring, it is added to the back.
    #[inline]
    pub fn push(&mut self, subject_hash: u32, seq: u64) {
        match self.per_subject.entry(subject_hash) {
            Entry::Occupied(mut e) => {
                e.get_mut().push_back(seq);
            }
            Entry::Vacant(e) => {
                let mut deque = VecDeque::new();
                deque.push_back(seq);
                e.insert(deque);
                self.subjects.push_back(subject_hash);
            }
        }
    }

    /// Pop the next ready (subject, seq) pair. O(1) amortized.
    ///
    /// Advances the ring: pops front subject, takes its first seq,
    /// then moves subject to back of ring (if it still has work).
    pub fn pop(&mut self) -> Option<(u32, u64)> {
        loop {
            let subject = self.subjects.pop_front()?;

            if let Some(deque) = self.per_subject.get_mut(&subject) {
                if let Some(seq) = deque.pop_front() {
                    // Subject still has work → move to back of ring
                    if !deque.is_empty() {
                        self.subjects.push_back(subject);
                    } else {
                        // No more work for this subject
                        self.per_subject.remove(&subject);
                    }
                    return Some((subject, seq));
                }
            }

            // Subject in ring but no work — clean up and try next
            self.per_subject.remove(&subject);
        }
    }

    /// Peek at the next subject without removing. O(1).
    pub fn peek_subject(&self) -> Option<u32> {
        self.subjects.front().copied()
    }

    /// Skip the current front subject (e.g. inflight limit reached).
    /// Moves it to the back of the ring. O(1).
    pub fn skip_current(&mut self) {
        if let Some(subject) = self.subjects.pop_front() {
            self.subjects.push_back(subject);
        }
    }

    /// Push a nacked sequence to the FRONT of its subject's deque (priority redelivery).
    pub fn push_nacked(&mut self, subject_hash: u32, seq: u64) {
        let is_new = !self.per_subject.contains_key(&subject_hash);
        self.per_subject
            .entry(subject_hash)
            .or_default()
            .push_front(seq);

        if is_new {
            self.subjects.push_back(subject_hash);
        }
    }

    /// Number of subjects with ready work.
    #[inline]
    pub fn subject_count(&self) -> usize {
        self.subjects.len()
    }

    /// Total number of ready sequence numbers across all subjects.
    pub fn total_ready(&self) -> usize {
        self.per_subject.values().map(|d| d.len()).sum()
    }

    /// Whether there is any ready work.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.subjects.is_empty()
    }

    /// Clear all ready work.
    pub fn clear(&mut self) {
        self.subjects.clear();
        self.per_subject.clear();
    }
}

impl Default for ReadySubjectRing {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_push_pop() {
        let mut ring = ReadySubjectRing::new();
        ring.push(10, 1);
        ring.push(10, 2);
        ring.push(20, 3);

        // Round-robin: subject 10 first, then 20, then back to 10
        assert_eq!(ring.pop(), Some((10, 1)));
        assert_eq!(ring.pop(), Some((20, 3)));
        assert_eq!(ring.pop(), Some((10, 2)));
        assert_eq!(ring.pop(), None);
    }

    #[test]
    fn single_subject() {
        let mut ring = ReadySubjectRing::new();
        ring.push(10, 1);
        ring.push(10, 2);
        ring.push(10, 3);

        assert_eq!(ring.pop(), Some((10, 1)));
        assert_eq!(ring.pop(), Some((10, 2)));
        assert_eq!(ring.pop(), Some((10, 3)));
        assert_eq!(ring.pop(), None);
    }

    #[test]
    fn skip_current_advances_ring() {
        let mut ring = ReadySubjectRing::new();
        ring.push(10, 1);
        ring.push(20, 2);
        ring.push(30, 3);

        // Skip subject 10
        assert_eq!(ring.peek_subject(), Some(10));
        ring.skip_current();
        assert_eq!(ring.peek_subject(), Some(20));

        assert_eq!(ring.pop(), Some((20, 2)));
        assert_eq!(ring.pop(), Some((30, 3)));
        assert_eq!(ring.pop(), Some((10, 1))); // 10 was moved to back
    }

    #[test]
    fn nacked_gets_priority() {
        let mut ring = ReadySubjectRing::new();
        ring.push(10, 100);
        ring.push(10, 200);

        // Nack seq 50 — goes to front of subject 10's deque
        ring.push_nacked(10, 50);

        assert_eq!(ring.pop(), Some((10, 50))); // nacked first
        assert_eq!(ring.pop(), Some((10, 100)));
        assert_eq!(ring.pop(), Some((10, 200)));
    }

    #[test]
    fn three_subjects_fair_interleave() {
        let mut ring = ReadySubjectRing::new();
        for seq in 0..3 { ring.push(1, seq); }
        for seq in 10..13 { ring.push(2, seq); }
        for seq in 20..23 { ring.push(3, seq); }

        let mut results = Vec::new();
        while let Some((subj, seq)) = ring.pop() {
            results.push((subj, seq));
        }

        // Should interleave: 1,2,3,1,2,3,1,2,3
        assert_eq!(results.len(), 9);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[1].0, 2);
        assert_eq!(results[2].0, 3);
        assert_eq!(results[3].0, 1);
    }

    #[test]
    fn total_ready_and_subject_count() {
        let mut ring = ReadySubjectRing::new();
        ring.push(10, 1);
        ring.push(10, 2);
        ring.push(20, 3);

        assert_eq!(ring.subject_count(), 2);
        assert_eq!(ring.total_ready(), 3);
    }

    #[test]
    fn clear() {
        let mut ring = ReadySubjectRing::new();
        ring.push(10, 1);
        ring.push(20, 2);
        ring.clear();
        assert!(ring.is_empty());
        assert_eq!(ring.pop(), None);
    }

    #[test]
    fn push_after_drain() {
        let mut ring = ReadySubjectRing::new();
        ring.push(10, 1);
        assert_eq!(ring.pop(), Some((10, 1)));
        assert!(ring.is_empty());

        // Push again after fully drained
        ring.push(10, 2);
        assert_eq!(ring.pop(), Some((10, 2)));
    }
}
