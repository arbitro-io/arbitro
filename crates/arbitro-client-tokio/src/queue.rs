//! Tuning knobs for [`Client::queue_subscribe_with`].
//!
//! [`QueueOptions`] is a plain value, not a consuming builder: you can store
//! it, reuse it across several queues, or assemble it conditionally —
//!
//! ```ignore
//! let mut opts = QueueOptions::new().filter(b"orders.>");
//! if slow_worker { opts = opts.ack_wait_ms(120_000); }
//! if low_memory  { opts = opts.max_inflight(10); }
//! client.queue_subscribe_with(stream_id, b"workers", opts).await?;
//! ```
//!
//! Every field already carries a sane default, so there is no
//! `..Default::default()` tail to write and nothing to fill in for the
//! common case.
//!
//! [`AckPolicy`] is deliberately absent: a work queue is always
//! `AckPolicy::Explicit`. Allowing `None` here would let a caller build a
//! queue that silently drops jobs when a worker dies, which is the one thing
//! a queue exists to prevent.
//!
//! [`Client::queue_subscribe_with`]: crate::Client::queue_subscribe_with
//! [`AckPolicy`]: arbitro_proto::config::AckPolicy

use arbitro_proto::config::DeliverPolicy;

/// Optional settings for a durable work queue. Start from
/// [`QueueOptions::new`] and set only what you need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueOptions {
    pub(crate) filter: Vec<u8>,
    pub(crate) ack_wait_ms: u32,
    pub(crate) max_inflight: u16,
    pub(crate) deliver_policy: DeliverPolicy,
    pub(crate) start_seq: u64,
    pub(crate) subject_limits: Vec<(Vec<u8>, u32)>,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            // No filter — the queue sees every subject in the stream. The
            // filter lives on the subscription, not the consumer, so two
            // workers on the same queue may narrow differently without ever
            // colliding on consumer config.
            filter: Vec::new(),
            // 0 = broker default redelivery deadline.
            ack_wait_ms: 0,
            // 0 = unlimited in-flight.
            max_inflight: 0,
            deliver_policy: DeliverPolicy::All,
            start_seq: 0,
            subject_limits: Vec::new(),
        }
    }
}

impl QueueOptions {
    /// All defaults. Chain setters for the ones you care about.
    pub fn new() -> Self {
        Self::default()
    }

    /// Subject filter for this subscription. `b">"` matches everything.
    ///
    /// Applied per-subscription, so workers sharing a queue can each narrow
    /// to a different slice of the stream.
    pub fn filter(mut self, filter: &[u8]) -> Self {
        self.filter = filter.to_vec();
        self
    }

    /// How long the broker waits for an ack before redelivering the job to
    /// the queue. `0` keeps the broker default.
    ///
    /// Raise it for handlers that legitimately take a long time — otherwise
    /// a slow success looks identical to a crashed worker and the job is
    /// handed to someone else while the first worker is still on it.
    pub fn ack_wait_ms(mut self, ms: u32) -> Self {
        self.ack_wait_ms = ms;
        self
    }

    /// Cap on delivered-but-unacked messages for the whole queue. `0` is
    /// unlimited. This is the queue's backpressure valve.
    pub fn max_inflight(mut self, n: u16) -> Self {
        self.max_inflight = n;
        self
    }

    /// Where the queue starts reading when it is first created. Ignored on
    /// later joins — the durable cursor already exists by then.
    pub fn deliver_policy(mut self, p: DeliverPolicy) -> Self {
        self.deliver_policy = p;
        self
    }

    /// Starting sequence for [`DeliverPolicy::ByStartSeq`].
    pub fn start_seq(mut self, seq: u64) -> Self {
        self.start_seq = seq;
        self
    }

    /// Cap in-flight messages for every subject matching `pattern`. Each
    /// distinct subject keeps its own counter. Multiple calls accumulate.
    pub fn max_subject_inflight(mut self, pattern: &[u8], limit: u32) -> Self {
        self.subject_limits.push((pattern.to_vec(), limit));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_permissive_ones() {
        let o = QueueOptions::new();
        assert!(o.filter.is_empty(), "no filter — the queue sees everything");
        assert_eq!(o.ack_wait_ms, 0, "0 defers to the broker default");
        assert_eq!(o.max_inflight, 0, "0 means unlimited in-flight");
        assert_eq!(o.deliver_policy, DeliverPolicy::All);
        assert_eq!(o.start_seq, 0);
        assert!(o.subject_limits.is_empty());
    }

    /// The point of a value type over a consuming builder: options can be
    /// assembled conditionally and reused.
    #[test]
    fn options_are_a_reusable_value() {
        let base = QueueOptions::new().filter(b"orders.>");

        let mut slow = base.clone();
        slow = slow.ack_wait_ms(120_000);

        assert_eq!(base.ack_wait_ms, 0, "the original is untouched");
        assert_eq!(slow.ack_wait_ms, 120_000);
        assert_eq!(slow.filter, b"orders.>".to_vec(), "shared prefix survives");
    }

    #[test]
    fn subject_limits_accumulate() {
        let o = QueueOptions::new()
            .max_subject_inflight(b"orders.basic.>", 1)
            .max_subject_inflight(b"orders.premium.>", 5);
        assert_eq!(o.subject_limits.len(), 2);
        assert_eq!(o.subject_limits[1].1, 5);
    }
}
