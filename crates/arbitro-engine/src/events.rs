//! Reactive delta events — engine mutations produce typed events for the worker.
//!
//! Level 0 — depends only on `types`.
//!
//! `#[must_use]` ensures callers never silently ignore events. The worker
//! inspects each return: `demand_became_available` → `gate.release()`,
//! `bindings_retired` → cleanup cached tx handles.

use crate::types::{BindingId, ConsumerId, QueueId, StreamId};

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
    /// (consumer_id, subject_hash, seq) triples whose pending entries
    /// were released by ack/nack/retire. The server forwards each as a
    /// `DrainEvent::Ack` so the drain-owned `ConsumerSubjects` slot
    /// decrements in lock-step. The seq drives the drain's redelivery
    /// suppression set: an ack keeps the seq suppressed forever, while
    /// a nack/timeout/retirement release removes it so the seq becomes
    /// deliverable again (the server distinguishes by call-site).
    pub subject_hashes_acked: Vec<(u32, u32, u64)>,
    /// Consumer ids whose entities were removed as part of this
    /// mutation. Populated by `delete_consumer` (single id) and by
    /// `delete_stream` (every consumer attached to the stream).
    ///
    /// The server uses this to mirror the engine cascade into the
    /// `NameRegistry`: for each id, call `remove_consumer_by_id` so the
    /// wire-name → id mapping (and reverse indexes) are cleared. Without
    /// it, a same-named recreate on a fresh stream silently aliases to
    /// the old (now-defunct) ConsumerId.
    /// Carries the queue alongside the id: a consumer's inflight is also
    /// counted against its queue, and the server has to release both. The
    /// queue is a field of the consumer entity, so the engine reads it for
    /// free right before removing it — while the only other way to recover
    /// it afterwards is scanning the shard's bindings, which is a linear
    /// walk per removed consumer and quadratic on a `delete_stream` cascade.
    pub consumers_removed: Vec<(ConsumerId, QueueId)>,
    /// Seqs of pending entries released by binding retirement (disconnect).
    /// The shard worker rewinds the drain cursor to min(seqs) so these
    /// entries get redelivered to other group members.
    pub pending_seqs_released: Vec<u64>,
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
        self.consumers_removed.extend(other.consumers_removed);
        self.pending_seqs_released
            .extend(other.pending_seqs_released);
    }

    /// True if no events were emitted.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.demand_became_available.is_empty()
            && self.demand_became_idle.is_empty()
            && self.bindings_retired.is_empty()
            && self.subject_hashes_acked.is_empty()
            && self.consumers_removed.is_empty()
            && self.pending_seqs_released.is_empty()
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
