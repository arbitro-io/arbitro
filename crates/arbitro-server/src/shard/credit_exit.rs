//! Early exit for the drain walk once no binding can take another entry.
//!
//! A cursor pinned by a capped consumer makes every ack-woken cycle re-walk
//! the same window: one entry fits the freed credit, the other 255 are read,
//! resolved and skipped. Bindings only lose credit within a cycle (local
//! deliveries); credit freed by an ack releases the gate for the next cycle.

use std::sync::atomic::Ordering;

use crate::shard::shared::SharedCounters;
use crate::shard::worker::ActiveBinding;

#[derive(Default)]
pub(in crate::shard) struct CreditExit {
    cycle: u64,
    /// `out[idx] == cycle`: binding `idx` is out of credit this cycle.
    out: Vec<u64>,
    /// Bindings that can still take an entry; `None` until the cycle's first credit skip.
    live: Option<usize>,
}

impl CreditExit {
    pub(in crate::shard) fn begin_cycle(&mut self, bindings: usize) {
        self.cycle += 1;
        self.live = None;
        if self.out.len() < bindings {
            self.out.resize(bindings, 0);
        }
    }

    #[inline]
    pub(in crate::shard) fn exhausted(&self) -> bool {
        self.live == Some(0)
    }

    /// Binding `idx` was skipped for lack of credit (paused or at `max_inflight`).
    #[inline]
    pub(in crate::shard) fn out_of_credit(
        &mut self,
        idx: usize,
        bindings: &[ActiveBinding],
        counters: &SharedCounters,
        local_inflight: &[(u32, u32)],
    ) {
        if self.out[idx] == self.cycle {
            return;
        }
        let live = match self.live {
            Some(live) => {
                self.out[idx] = self.cycle;
                live.saturating_sub(1)
            }
            None => self.count_live(bindings, counters, local_inflight),
        };
        self.live = Some(live);
    }

    /// Once per cycle, at its first credit skip.
    fn count_live(
        &mut self,
        bindings: &[ActiveBinding],
        counters: &SharedCounters,
        local_inflight: &[(u32, u32)],
    ) -> usize {
        let mut live = 0;
        for (idx, b) in bindings.iter().enumerate() {
            if has_credit(b, counters, local_inflight) {
                live += 1;
            } else {
                self.out[idx] = self.cycle;
            }
        }
        live
    }
}

/// Mirrors the drain's own eligibility checks, in the same order.
fn has_credit(b: &ActiveBinding, counters: &SharedCounters, local_inflight: &[(u32, u32)]) -> bool {
    if b.write_failed.load(Ordering::Relaxed) || counters.is_paused(b.consumer_id.0) {
        return false;
    }
    if b.fire_and_forget {
        return true;
    }
    let cid = b.consumer_id.0;
    let pending = local_inflight.iter().find(|e| e.0 == cid).map_or(0, |e| e.1);
    pending < b.max_inflight && counters.consumer_has_capacity(cid, b.max_inflight - pending)
}
