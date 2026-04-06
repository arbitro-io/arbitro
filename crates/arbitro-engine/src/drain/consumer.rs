//! Consumer — registered consumer with delivery state.
//!
//! Each consumer tracks: its subscriptions (connections), filters,
//! ack state, credit map, nacked queue, and deferred queue.

use std::collections::VecDeque;

use arbitro_common::credit_map::CreditMap;
use arbitro_common::subject::subject_matches;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverMode};
use arbitro_proto::ids::{ConnId, Sequence};

use super::subscription::Subscription;

/// A registered consumer attached to a stream.
pub struct Consumer {
    pub config: ConsumerConfig,
    /// Active subscriptions (connections receiving from this consumer).
    pub subscriptions: Vec<Subscription>,
    /// Next sequence to deliver from the journal.
    pub deliver_seq: Sequence,
    /// Credit map for per-subject flow control (only with AckPolicy::Explicit).
    pub credit_map: Option<CreditMap>,
    /// Total messages delivered but not yet acked.
    pub pending_count: u32,
    /// Sequences nacked — need redelivery. O(1) pop_front.
    pub nacked: VecDeque<u64>,
    /// Sequences deferred due to per-subject credit limits.
    /// Retry when credit frees (ack releases subject slot).
    pub deferred: VecDeque<u64>,
    /// Fast-path: true if filter is ">" or empty, matching every subject.
    pub matches_all: bool,
}

impl Consumer {
    pub fn new(config: ConsumerConfig, start_seq: Sequence) -> Self {
        let credit_map =
            if config.ack_policy == AckPolicy::Explicit && !config.subject_limits.is_empty() {
                Some(CreditMap::from_limits(
                    &config.subject_limits,
                    config.max_inflight as u32,
                ))
            } else {
                None
            };

        let matches_all =
            config.filters.is_empty() || config.filters.iter().any(|f| f.as_ref() == b">");

        Self {
            config,
            subscriptions: Vec::new(),
            deliver_seq: start_seq,
            credit_map,
            pending_count: 0,
            nacked: VecDeque::new(),
            deferred: VecDeque::new(),
            matches_all,
        }
    }

    /// Add a subscription binding this consumer to a connection.
    pub fn add_subscription(&mut self, sub: Subscription) {
        self.subscriptions.push(sub);
    }

    /// Remove all subscriptions for a given connection.
    /// Returns true if any were removed.
    pub fn remove_conn(&mut self, conn_id: ConnId) -> bool {
        let before = self.subscriptions.len();
        self.subscriptions.retain(|s| s.conn_id != conn_id);
        self.subscriptions.len() < before
    }

    /// Does this consumer's filters match the given subject?
    #[inline]
    pub fn matches(&self, subject: &[u8]) -> bool {
        self.matches_all
            || self
                .config
                .filters
                .iter()
                .any(|f| subject_matches(f, subject))
    }

    /// Is this consumer connected and ready to receive?
    #[inline]
    pub fn is_active(&self) -> bool {
        !self.subscriptions.is_empty()
    }

    /// Check global inflight credit only (ignores per-subject limits).
    #[inline]
    pub fn has_global_credit(&self) -> bool {
        if self.config.ack_policy == AckPolicy::None {
            return true;
        }
        self.config.max_inflight == 0 || self.pending_count < self.config.max_inflight as u32
    }

    /// Check if we have credit to deliver a message for this subject.
    #[inline]
    pub fn has_credit(&self, subject: &[u8]) -> bool {
        if !self.has_global_credit() {
            return false;
        }
        if let Some(ref cm) = self.credit_map {
            return cm.has_credit(subject);
        }
        true
    }

    /// Acquire credit for delivery. Returns false if no credit available.
    #[inline]
    pub fn try_acquire(&mut self, seq: Sequence, subject: &[u8], conn_id: u64) -> bool {
        if self.config.ack_policy == AckPolicy::None {
            return true;
        }
        if self.config.max_inflight > 0 && self.pending_count >= self.config.max_inflight as u32 {
            return false;
        }
        if let Some(ref mut mut_cm) = self.credit_map {
            if !mut_cm.try_acquire(subject, seq, conn_id) {
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
        assert!(c.has_global_credit());
    }

    #[test]
    fn explicit_inflight_limit() {
        let mut c = explicit_consumer(b"c1", b"ORDERS");
        c.config.max_inflight = 2;

        assert!(c.try_acquire(1, b"orders.created", 42));
        assert!(c.try_acquire(2, b"orders.created", 42));
        assert!(!c.try_acquire(3, b"orders.created", 42)); // at limit

        c.release(1);
        assert!(c.try_acquire(3, b"orders.created", 42)); // credit restored
    }

    #[test]
    fn inactive_by_default() {
        let c = fanout_consumer(b"c1", b"ORDERS");
        assert!(!c.is_active());
    }

    #[test]
    fn active_when_subscribed() {
        let mut c = fanout_consumer(b"c1", b"ORDERS");
        c.add_subscription(Subscription::new(42));
        assert!(c.is_active());
    }

    #[test]
    fn remove_conn_clears_subscriptions() {
        let mut c = fanout_consumer(b"c1", b"ORDERS");
        c.add_subscription(Subscription::new(42));
        c.add_subscription(Subscription::new(42));
        c.add_subscription(Subscription::new(99));

        assert!(c.remove_conn(42));
        assert_eq!(c.subscriptions.len(), 1);
        assert_eq!(c.subscriptions[0].conn_id, 99);
    }

    #[test]
    fn nacked_is_vecdeque() {
        let mut c = fanout_consumer(b"c1", b"ORDERS");
        c.nacked.push_back(1);
        c.nacked.push_back(2);
        c.nacked.push_back(3);
        assert_eq!(c.nacked.pop_front(), Some(1)); // O(1)
        assert_eq!(c.nacked.len(), 2);
    }
}
