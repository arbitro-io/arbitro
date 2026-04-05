//! Consumer — registered consumer with delivery state.
//!
//! Each consumer tracks: which connection it's on, its filters,
//! ack state, and credit map for flow control.

use arbitro_common::credit_map::CreditMap;
use arbitro_common::subject::subject_matches;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverMode};
use arbitro_proto::ids::{ConnId, Sequence};

/// A registered consumer attached to a stream.
pub struct Consumer {
    pub config: ConsumerConfig,
    /// Connection this consumer is currently bound to (0 = not connected).
    pub conn_id: ConnId,
    /// Next sequence to deliver from the journal.
    pub deliver_seq: Sequence,
    /// Credit map for per-subject flow control (only with AckPolicy::Explicit).
    pub credit_map: Option<CreditMap>,
    /// Total messages delivered (not yet acked).
    pub pending_count: u32,
}

impl Consumer {
    pub fn new(config: ConsumerConfig, start_seq: Sequence) -> Self {
        let credit_map = if config.ack_policy == AckPolicy::Explicit
            && !config.subject_limits.is_empty()
        {
            Some(CreditMap::from_limits(&config.subject_limits, config.max_inflight as u32))
        } else {
            None
        };

        Self {
            config,
            conn_id: 0,
            deliver_seq: start_seq,
            credit_map,
            pending_count: 0,
        }
    }

    /// Does this consumer's filters match the given subject?
    #[inline]
    pub fn matches(&self, subject: &[u8]) -> bool {
        // No filters = match everything
        if self.config.filters.is_empty() {
            return true;
        }
        self.config.filters.iter().any(|f| subject_matches(f, subject))
    }

    /// Is this consumer connected and ready to receive?
    #[inline]
    pub fn is_active(&self) -> bool {
        self.conn_id != 0
    }

    /// Check if we have credit to deliver a message for this subject.
    #[inline]
    pub fn has_credit(&self, subject: &[u8]) -> bool {
        if self.config.ack_policy == AckPolicy::None {
            return true;
        }
        // Global inflight check
        if self.config.max_inflight > 0 && self.pending_count >= self.config.max_inflight as u32 {
            return false;
        }
        // Per-subject check
        if let Some(ref cm) = self.credit_map {
            return cm.has_credit(subject);
        }
        true
    }

    /// Acquire credit for delivery. Returns false if no credit available.
    #[inline]
    pub fn try_acquire(&mut self, seq: Sequence, subject: &[u8]) -> bool {
        if self.config.ack_policy == AckPolicy::None {
            return true;
        }
        if self.config.max_inflight > 0 && self.pending_count >= self.config.max_inflight as u32 {
            return false;
        }
        if let Some(ref mut cm) = self.credit_map {
            if !cm.try_acquire(subject, seq) {
                return false;
            }
        }
        self.pending_count += 1;
        true
    }

    /// Release credit on ack.
    #[inline]
    pub fn release(&mut self, seq: Sequence) {
        if self.config.ack_policy == AckPolicy::None {
            return;
        }
        if self.pending_count > 0 {
            self.pending_count -= 1;
        }
        if let Some(ref mut cm) = self.credit_map {
            cm.release(seq);
        }
    }

    #[inline]
    pub fn is_queue(&self) -> bool {
        self.config.deliver_mode == DeliverMode::Queue
    }

    #[inline]
    pub fn is_fanout(&self) -> bool {
        self.config.deliver_mode == DeliverMode::Fanout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arbitro_proto::config::ConsumerConfig;

    fn fanout_consumer(name: &[u8], stream: &[u8]) -> Consumer {
        let cfg = ConsumerConfig::new(name, stream)
            .filter(b"orders.>")
            .ack_policy(AckPolicy::None)
            .build();
        Consumer::new(cfg, 1)
    }

    fn explicit_consumer(name: &[u8], stream: &[u8]) -> Consumer {
        let cfg = ConsumerConfig::new(name, stream)
            .filter(b"orders.>")
            .ack_policy(AckPolicy::Explicit)
            .max_inflight(10)
            .build();
        Consumer::new(cfg, 1)
    }

    #[test]
    fn matches_filter() {
        let c = fanout_consumer(b"c1", b"ORDERS");
        assert!(c.matches(b"orders.created"));
        assert!(c.matches(b"orders.updated"));
        assert!(!c.matches(b"payments.done"));
    }

    #[test]
    fn no_filter_matches_all() {
        let cfg = ConsumerConfig::new(b"c1", b"ORDERS")
            .ack_policy(AckPolicy::None)
            .build();
        let c = Consumer::new(cfg, 1);
        assert!(c.matches(b"anything"));
    }

    #[test]
    fn fire_and_forget_always_has_credit() {
        let c = fanout_consumer(b"c1", b"ORDERS");
        assert!(c.has_credit(b"orders.created"));
    }

    #[test]
    fn explicit_inflight_limit() {
        let mut c = explicit_consumer(b"c1", b"ORDERS");
        c.config.max_inflight = 2;

        assert!(c.try_acquire(1, b"orders.created"));
        assert!(c.try_acquire(2, b"orders.created"));
        assert!(!c.try_acquire(3, b"orders.created")); // at limit

        c.release(1);
        assert!(c.try_acquire(3, b"orders.created")); // credit restored
    }

    #[test]
    fn inactive_by_default() {
        let c = fanout_consumer(b"c1", b"ORDERS");
        assert!(!c.is_active());
    }

    #[test]
    fn active_when_connected() {
        let mut c = fanout_consumer(b"c1", b"ORDERS");
        c.conn_id = 42;
        assert!(c.is_active());
    }
}
