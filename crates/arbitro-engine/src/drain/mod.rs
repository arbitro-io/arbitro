//! ReactiveDrain — inline delivery after journal append.
//!
//! No channels, no background tasks. Append + deliver under the same lock.
//! The drain owns all consumers for a stream and dispatches to them.

pub mod consumer;
pub mod frame_builder;

use consumer::Consumer;
use frame_builder::build_delivery_envelope;

use arbitro_proto::config::{ConsumerConfig, DeliverMode};
use arbitro_proto::ids::ConnId;
use arbitro_store::Entry;

use crate::transport::Transport;

/// Max queue consumers per stream for stack-allocated candidate array.
const MAX_QUEUE_CANDIDATES: usize = 64;

/// Reactive drain — delivers messages inline after journal append.
pub struct ReactiveDrain {
    stream_id: u32,
    consumers: Vec<Consumer>,
    /// Round-robin index for Queue mode delivery.
    queue_idx: usize,
}

impl ReactiveDrain {
    pub fn new(stream_id: u32) -> Self {
        Self {
            stream_id,
            consumers: Vec::new(),
            queue_idx: 0,
        }
    }

    /// Register a consumer. Returns consumer_id.
    pub fn add_consumer(&mut self, config: ConsumerConfig, start_seq: u64) -> u32 {
        let id = config.consumer_id;
        self.consumers.push(Consumer::new(config, start_seq));
        id
    }

    /// Remove a consumer by id.
    pub fn remove_consumer(&mut self, consumer_id: u32) -> bool {
        let before = self.consumers.len();
        self.consumers.retain(|c| c.config.consumer_id != consumer_id);
        self.consumers.len() < before
    }

    /// Bind a consumer to a connection (subscribe).
    pub fn bind(&mut self, consumer_id: u32, conn_id: ConnId) -> bool {
        if let Some(c) = self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            c.conn_id = conn_id;
            true
        } else {
            false
        }
    }

    /// Unbind a consumer from its connection (unsubscribe).
    pub fn unbind(&mut self, consumer_id: u32) -> bool {
        if let Some(c) = self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            c.conn_id = 0;
            true
        } else {
            false
        }
    }

    /// Unbind all consumers on a given connection (disconnect cleanup).
    pub fn unbind_conn(&mut self, conn_id: ConnId) {
        for c in &mut self.consumers {
            if c.conn_id == conn_id {
                c.conn_id = 0;
            }
        }
    }

    /// Deliver a single entry to matching consumers.
    /// Called inline after journal append, under the stream lock.
    /// Zero allocations: stack array for queue candidates, send_parts for scatter write.
    #[inline]
    pub fn deliver(&mut self, entry: &Entry, transport: &dyn Transport) -> u32 {
        let subject = &entry.subject;
        let mut delivered = 0u32;

        // Stack-allocated queue candidate indices — no Vec
        let mut queue_buf = [0usize; MAX_QUEUE_CANDIDATES];
        let mut queue_count = 0usize;

        for (i, consumer) in self.consumers.iter_mut().enumerate() {
            if !consumer.is_active() || !consumer.matches(subject) {
                continue;
            }

            match consumer.config.deliver_mode {
                DeliverMode::Fanout => {
                    if Self::send_entry(consumer, entry, transport) {
                        delivered += 1;
                    }
                }
                DeliverMode::Queue => {
                    if consumer.has_credit(subject) && queue_count < MAX_QUEUE_CANDIDATES {
                        queue_buf[queue_count] = i;
                        queue_count += 1;
                    }
                }
            }
        }

        // Queue mode: round-robin pick one
        if queue_count > 0 {
            let idx = self.queue_idx % queue_count;
            self.queue_idx = self.queue_idx.wrapping_add(1);
            let ci = queue_buf[idx];
            let consumer = &mut self.consumers[ci];
            if Self::send_entry(consumer, entry, transport) {
                delivered += 1;
            }
        }

        delivered
    }

    /// Deliver a batch of entries.
    #[inline]
    pub fn deliver_batch(&mut self, entries: &[Entry], transport: &dyn Transport) -> u32 {
        let mut total = 0;
        for entry in entries {
            total += self.deliver(entry, transport);
        }
        total
    }

    /// Process an ack from a consumer.
    pub fn ack(&mut self, consumer_id: u32, seq: u64) -> bool {
        if let Some(c) = self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            c.release(seq);
            true
        } else {
            false
        }
    }

    /// Send a single entry to a consumer. Zero heap allocation.
    /// Uses send_parts for scatter write: envelope + subj_len + subject + payload.
    #[inline]
    fn send_entry(consumer: &mut Consumer, entry: &Entry, transport: &dyn Transport) -> bool {
        if !consumer.try_acquire(entry.seq, &entry.subject) {
            return false;
        }

        let body_len = 2 + entry.subject.len() + entry.payload.len();
        let env = build_delivery_envelope(
            consumer.config.stream_id,
            body_len as u32,
            entry.seq as u32,
        );
        let subj_len_bytes = (entry.subject.len() as u16).to_le_bytes();

        // Scatter write — no concatenation, no Vec
        transport.send_parts(consumer.conn_id, &[
            &env,
            &subj_len_bytes,
            &entry.subject,
            &entry.payload,
        ])
    }

    pub fn consumer_count(&self) -> usize {
        self.consumers.len()
    }

    pub fn find_consumer(&self, consumer_id: u32) -> Option<&Consumer> {
        self.consumers.iter().find(|c| c.config.consumer_id == consumer_id)
    }

    /// Iterate all consumers — used for overlap checks (cold path).
    pub fn iter_consumers(&self) -> impl Iterator<Item = &Consumer> {
        self.consumers.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arbitro_proto::config::{AckPolicy, ConsumerConfig};
    use arbitro_store::Entry;
    use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

    /// Counting transport — tracks how many sends happened.
    struct CountTransport {
        count: AtomicU32,
    }

    impl CountTransport {
        fn new() -> Self { Self { count: AtomicU32::new(0) } }
        fn sent(&self) -> u32 { self.count.load(Relaxed) }
    }

    impl Transport for CountTransport {
        fn send(&self, _conn_id: ConnId, _data: &[u8]) -> bool {
            self.count.fetch_add(1, Relaxed);
            true
        }
        fn close(&self, _conn_id: ConnId) {}
    }

    fn test_entry(seq: u64, subject: &[u8], payload: &[u8]) -> Entry {
        Entry {
            seq,
            timestamp: 0,
            subject: Box::from(subject),
            payload: Box::from(payload),
        }
    }

    fn consumer_cfg(name: &[u8], id: u32, mode: DeliverMode) -> ConsumerConfig {
        let mut cfg = ConsumerConfig::new(name, b"TEST")
            .filter(b"orders.>")
            .ack_policy(AckPolicy::None)
            .deliver_mode(mode)
            .build();
        cfg.consumer_id = id;
        cfg
    }

    #[test]
    fn fanout_delivers_to_all() {
        let transport = CountTransport::new();
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        drain.add_consumer(consumer_cfg(b"c2", 2, DeliverMode::Fanout), 1);
        drain.bind(1, 100);
        drain.bind(2, 200);

        let entry = test_entry(1, b"orders.created", b"{}");
        let delivered = drain.deliver(&entry, &transport);

        assert_eq!(delivered, 2);
        assert_eq!(transport.sent(), 2);
    }

    #[test]
    fn queue_delivers_to_one() {
        let transport = CountTransport::new();
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Queue), 1);
        drain.add_consumer(consumer_cfg(b"c2", 2, DeliverMode::Queue), 1);
        drain.bind(1, 100);
        drain.bind(2, 200);

        let entry = test_entry(1, b"orders.created", b"{}");
        let delivered = drain.deliver(&entry, &transport);

        assert_eq!(delivered, 1);
        assert_eq!(transport.sent(), 1);
    }

    #[test]
    fn queue_round_robin() {
        let transport = CountTransport::new();
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Queue), 1);
        drain.add_consumer(consumer_cfg(b"c2", 2, DeliverMode::Queue), 1);
        drain.bind(1, 100);
        drain.bind(2, 200);

        for i in 1..=4 {
            let entry = test_entry(i, b"orders.created", b"{}");
            drain.deliver(&entry, &transport);
        }

        // 4 messages, round-robin across 2 consumers = 4 total sends
        assert_eq!(transport.sent(), 4);
    }

    #[test]
    fn inactive_consumer_skipped() {
        let transport = CountTransport::new();
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        // c1 not bound — inactive

        let entry = test_entry(1, b"orders.created", b"{}");
        assert_eq!(drain.deliver(&entry, &transport), 0);
    }

    #[test]
    fn non_matching_subject_skipped() {
        let transport = CountTransport::new();
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        drain.bind(1, 100);

        let entry = test_entry(1, b"payments.done", b"{}");
        assert_eq!(drain.deliver(&entry, &transport), 0);
    }

    #[test]
    fn unbind_conn_clears_all() {
        let transport = CountTransport::new();
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        drain.add_consumer(consumer_cfg(b"c2", 2, DeliverMode::Fanout), 1);
        drain.bind(1, 100);
        drain.bind(2, 100); // same conn

        drain.unbind_conn(100);

        let entry = test_entry(1, b"orders.created", b"{}");
        assert_eq!(drain.deliver(&entry, &transport), 0);
    }

    #[test]
    fn remove_consumer() {
        let mut drain = ReactiveDrain::new(1);
        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        assert_eq!(drain.consumer_count(), 1);

        assert!(drain.remove_consumer(1));
        assert_eq!(drain.consumer_count(), 0);
    }

    #[test]
    fn ack_releases_credit() {
        let transport = CountTransport::new();
        let mut drain = ReactiveDrain::new(1);

        let mut cfg = ConsumerConfig::new(b"c1", b"TEST")
            .filter(b"orders.>")
            .ack_policy(AckPolicy::Explicit)
            .max_inflight(1)
            .build();
        cfg.consumer_id = 1;

        drain.add_consumer(cfg, 1);
        drain.bind(1, 100);

        let e1 = test_entry(1, b"orders.created", b"{}");
        assert_eq!(drain.deliver(&e1, &transport), 1);

        // At inflight limit — next delivery blocked
        let e2 = test_entry(2, b"orders.created", b"{}");
        assert_eq!(drain.deliver(&e2, &transport), 0);

        // Ack releases credit
        drain.ack(1, 1);
        assert_eq!(drain.deliver(&e2, &transport), 1);
    }
}
