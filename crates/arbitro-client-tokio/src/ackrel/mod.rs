//! Client-side ack-reliability hot layer: per-consumer deferred-ack
//! tracking ([`pending`]). Pure data structures — wiring into
//! `Inner`/session/batcher is a separate step.
//!
//! Redelivery dedup now lives in the durable [`crate::ackstore`] WAL, which
//! replaced both the old in-memory `SeenCache` and the SQLite cold tier.

pub(crate) mod pending;

use std::sync::atomic::Ordering;
use std::sync::RwLock;

use pending::ConsumerPending;

/// Top-level ack-reliability state, one instance per `Inner`.
pub(crate) struct AckRelay {
    per_consumer: RwLock<Vec<Option<Box<ConsumerPending>>>>,
}

impl AckRelay {
    pub fn new() -> Self {
        Self {
            per_consumer: RwLock::new(Vec::new()),
        }
    }

    /// Ensures a slot for `cid` exists and is on `generation`. A
    /// generation bump (reconnect) replaces the slot wholesale — this
    /// is the purge, matching the reconnect invariant that deferred acks
    /// don't survive a session change.
    pub fn ensure(&self, cid: u32, generation: u32) {
        let mut g = self.per_consumer.write().unwrap();
        if g.len() <= cid as usize {
            g.resize_with(cid as usize + 1, || None);
        }
        let needs_replace = match &g[cid as usize] {
            Some(slot) => slot.generation != generation,
            None => true,
        };
        if needs_replace {
            g[cid as usize] = Some(Box::new(ConsumerPending::new(generation)));
        }
    }

    pub fn record_deferred(&self, cid: u32, generation: u32, seq: u64, now_ms: u64) -> bool {
        self.ensure(cid, generation);
        let g = self.per_consumer.read().unwrap();
        g[cid as usize].as_ref().unwrap().record(seq, now_ms)
    }

    /// Current generation for `cid`, or `0` if no slot exists yet (never
    /// deferred an ack for this consumer). The reconnect flow bumps
    /// generations via [`Self::ensure`].
    pub fn generation_of(&self, cid: u32) -> u32 {
        let g = self.per_consumer.read().unwrap();
        match g.get(cid as usize) {
            Some(Some(slot)) => slot.generation,
            _ => 0,
        }
    }

    /// GATE step 1: no slot means never deferred, never pending.
    #[allow(dead_code)] // query counterpart of the gate API, kept for completeness
    pub fn is_pending(&self, cid: u32, seq: u64) -> bool {
        let g = self.per_consumer.read().unwrap();
        match g.get(cid as usize) {
            Some(Some(slot)) => slot.is_pending(seq),
            _ => false,
        }
    }

    pub fn purge_up_to(&self, cid: u32, new_cursor: u64) -> u32 {
        let g = self.per_consumer.read().unwrap();
        match g.get(cid as usize) {
            Some(Some(slot)) => slot.purge_up_to(new_cursor),
            _ => 0,
        }
    }

    pub fn drain_ascending(&self, cid: u32, max: usize) -> Vec<u64> {
        let g = self.per_consumer.read().unwrap();
        match g.get(cid as usize) {
            Some(Some(slot)) => slot.drain_ascending(max),
            _ => Vec::new(),
        }
    }

    /// `(consumer_id, generation)` for every slot with outstanding
    /// deferred acks — feeds the reconnect replay flow.
    pub fn active_consumers(&self) -> Vec<(u32, u32)> {
        let g = self.per_consumer.read().unwrap();
        g.iter()
            .enumerate()
            .filter_map(|(cid, slot)| slot.as_ref().map(|s| (cid as u32, s)))
            .filter(|(_, s)| s.count.load(Ordering::Relaxed) > 0)
            .map(|(cid, s)| (cid, s.generation))
            .collect()
    }

    /// Gauge: total deferred-ack entries across all consumers.
    pub fn hot_count(&self) -> u64 {
        let g = self.per_consumer.read().unwrap();
        g.iter()
            .filter_map(|slot| slot.as_ref())
            .map(|s| s.count.load(Ordering::Relaxed) as u64)
            .sum()
    }

    /// Gauge: age in ms of the oldest deferred ack across all consumers.
    pub fn oldest_pending_age_ms(&self, now_ms: u64) -> u64 {
        let g = self.per_consumer.read().unwrap();
        g.iter()
            .filter_map(|slot| slot.as_ref())
            .map(|s| s.oldest_ts_ms.load(Ordering::Relaxed))
            .filter(|&ts| ts > 0)
            .map(|ts| now_ms.saturating_sub(ts))
            .max()
            .unwrap_or(0)
    }

    pub fn expire(&self, cutoff_ms: u64, now_ms: u64) -> u32 {
        let g = self.per_consumer.read().unwrap();
        g.iter()
            .filter_map(|slot| slot.as_ref())
            .map(|s| s.expire_older_than(cutoff_ms, now_ms))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_cascade_rejects_out_of_range() {
        let relay = AckRelay::new();
        relay.record_deferred(0, 1, 100, 1_000);
        assert!(!relay.is_pending(0, 50));
        assert!(!relay.is_pending(0, 200));
        assert!(relay.is_pending(0, 100));
    }

    #[test]
    fn purge_up_to_removes_le_cursor() {
        let relay = AckRelay::new();
        for seq in [10, 20, 30, 40] {
            relay.record_deferred(0, 1, seq, 1_000);
        }
        let removed = relay.purge_up_to(0, 20);
        assert_eq!(removed, 2);
        assert!(!relay.is_pending(0, 10));
        assert!(!relay.is_pending(0, 20));
        assert!(relay.is_pending(0, 30));
        assert!(relay.is_pending(0, 40));
    }

    #[test]
    fn drain_is_ascending() {
        let relay = AckRelay::new();
        for seq in [40, 10, 30, 20] {
            relay.record_deferred(0, 1, seq, 1_000);
        }
        assert_eq!(relay.drain_ascending(0, 3), vec![10, 20, 30]);
    }

    #[test]
    fn generation_change_purges() {
        let relay = AckRelay::new();
        relay.ensure(0, 1);
        relay.record_deferred(0, 1, 100, 1_000);
        assert!(relay.is_pending(0, 100));
        relay.ensure(0, 2);
        assert!(!relay.is_pending(0, 100));
    }
}
