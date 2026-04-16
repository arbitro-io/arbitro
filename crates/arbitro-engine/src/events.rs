//! Reactive delta events — engine mutations produce typed events for the worker.
//!
//! Level 0 — depends only on `types`.
//!
//! `#[must_use]` ensures callers never silently ignore events. The worker
//! inspects each return: `demand_became_available` → `gate.release()`,
//! `bindings_retired` → cleanup cached tx handles.

use crate::types::{BindingId, StreamId};

/// Events produced by `execute(Command)` and admin mutations.
///
/// NOT a `futures::Stream` — pure event-sourced struct. The worker decides
/// park/unpark based on the contents.
#[must_use]
#[derive(Default, Debug)]
pub struct DeltaEvents {
    /// Streams that transitioned from 0 → ≥1 active bindings.
    pub demand_became_available: Vec<StreamId>,
    /// Streams that transitioned from ≥1 → 0 active bindings.
    pub demand_became_idle: Vec<StreamId>,
    /// Bindings retired by `delete_stream`, `delete_consumer`, or
    /// `mark_connection_dead`.
    pub bindings_retired: Vec<BindingId>,
    /// Subject hashes whose inflight was decremented by ack.
    /// Used by the handler to sync `SharedCounters::dec_subject`.
    pub subject_hashes_acked: Vec<u32>,
}

impl DeltaEvents {
    /// Merge another set of events into this one.
    #[inline]
    pub fn merge(&mut self, other: DeltaEvents) {
        self.demand_became_available
            .extend(other.demand_became_available);
        self.demand_became_idle.extend(other.demand_became_idle);
        self.bindings_retired.extend(other.bindings_retired);
        self.subject_hashes_acked.extend(other.subject_hashes_acked);
    }

    /// True if no events were emitted.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.demand_became_available.is_empty()
            && self.demand_became_idle.is_empty()
            && self.bindings_retired.is_empty()
            && self.subject_hashes_acked.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_empty() {
        let e = DeltaEvents::default();
        assert!(e.is_empty());
    }

    #[test]
    fn merge_combines() {
        let mut a = DeltaEvents::default();
        a.demand_became_available.push(StreamId(1));

        let mut b = DeltaEvents::default();
        b.demand_became_idle.push(StreamId(2));
        b.bindings_retired.push(BindingId(3));

        a.merge(b);
        assert!(!a.is_empty());
        assert_eq!(a.demand_became_available.len(), 1);
        assert_eq!(a.demand_became_idle.len(), 1);
        assert_eq!(a.bindings_retired.len(), 1);
    }
}
