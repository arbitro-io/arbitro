//! Delivery memory for fire-and-forget bindings.
//!
//! Ack-mode bindings remember what they were sent in `ConsumerSubjects`.
//! Fire-and-forget bindings never ack, so nothing remembered it: whenever a
//! sibling pinned the shard cursor, every re-walk of the window sent them the
//! same entries again — measured at 64,071 deliveries for 50 messages.
//!
//! The floor is the highest seq already flushed to a binding's socket. It is
//! indexed by the binding's position in the snapshot, so the hot-path check
//! is one load and one compare. It only rises after the frame carrying the
//! entry flushed `Ok`; a frame that bounced leaves it where it was, and the
//! entry is retried like any other backpressured one.

use std::collections::HashMap;

use arbitro_engine_v2::types::{BindingId, ConnectionId, StreamId};

use crate::shard::worker::ActiveBinding;

#[derive(Default)]
pub(in crate::shard) struct FireAndForgetFloors {
    /// Highest seq flushed, by snapshot binding index.
    floor: Vec<u64>,
    /// The binding each `floor` slot belongs to, to carry floors across a
    /// snapshot swap that reorders or drops bindings.
    ids: Vec<BindingId>,
    /// Highest seq staged this cycle, by binding index; 0 = none.
    staged: Vec<u64>,
    /// Indices with a staged seq, so committing never walks every binding.
    touched: Vec<u32>,
}

impl FireAndForgetFloors {
    /// Re-key the floors to a new snapshot. Cold: runs on snapshot swaps.
    pub(in crate::shard) fn remap(&mut self, bindings: &[ActiveBinding]) {
        let old: HashMap<BindingId, u64, foldhash::fast::FixedState> = self
            .ids
            .iter()
            .copied()
            .zip(self.floor.iter().copied())
            .collect();
        self.ids.clear();
        self.floor.clear();
        for b in bindings {
            self.ids.push(b.binding_id);
            self.floor.push(old.get(&b.binding_id).copied().unwrap_or(0));
        }
        self.staged.clear();
        self.staged.resize(bindings.len(), 0);
        self.touched.clear();
    }

    /// Whether `seq` already reached this binding's socket.
    #[inline]
    pub(in crate::shard) fn already_sent(&self, idx: usize, seq: u64) -> bool {
        self.floor.get(idx).is_some_and(|&f| seq <= f)
    }

    /// Note that `seq` was framed for this binding this cycle.
    #[inline]
    pub(in crate::shard) fn stage(&mut self, idx: usize, seq: u64) {
        if let Some(s) = self.staged.get_mut(idx) {
            if *s == 0 {
                self.touched.push(idx as u32);
            }
            *s = seq;
        }
    }

    /// Raise the floor of every binding whose frame went out.
    pub(in crate::shard) fn commit(
        &mut self,
        bindings: &[ActiveBinding],
        flushed: impl Fn(ConnectionId, StreamId) -> bool,
    ) {
        for &idx in &self.touched {
            let idx = idx as usize;
            let seq = std::mem::take(&mut self.staged[idx]);
            if let (Some(b), Some(f)) = (bindings.get(idx), self.floor.get_mut(idx)) {
                if flushed(b.connection_id, b.stream_id) && seq > *f {
                    *f = seq;
                }
            }
        }
        self.touched.clear();
    }
}
