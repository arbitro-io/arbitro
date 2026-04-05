//! ReactiveDrain — delivery engine per stream.
//!
//! The drain is the ONLY component that knows about consumers and delivery.
//! Publish just appends + signals. Ack just releases credit + signals.
//! The drain reacts: reads from store, delivers to connections.
//!
//! For Fanout: groups consumers by conn_id, sends once per connection.
//! The CLIENT demultiplexes locally to matching subscribers.
//!
//! For Queue: round-robin pick one consumer per entry.

pub mod consumer;
pub mod frame_builder;

use consumer::Consumer;
use frame_builder::build_delivery_envelope;

use arbitro_proto::config::{ConsumerConfig, DeliverMode};
use arbitro_proto::ids::ConnId;
use arbitro_store::{Entry, Store};

use crate::transport::Transport;

/// Max queue consumers per stream for stack-allocated candidate array.
const MAX_QUEUE_CANDIDATES: usize = 64;

/// Reactive drain — owns all delivery logic for a single stream.
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

    // ── Consumer management (cold path) ─────────────────────────────────

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

    /// Bind a consumer to a connection and deliver backlog.
    pub fn on_bind(
        &mut self,
        consumer_id: u32,
        conn_id: ConnId,
        store: &dyn Store,
        transport: &dyn Transport,
    ) -> bool {
        if let Some(c) = self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            c.conn_id = conn_id;
            // Deliver backlog from where this consumer left off
            let info = store.info();
            if c.deliver_seq <= info.last_seq {
                let entries = store.read_range(c.deliver_seq, info.last_seq + 1)
                    .unwrap_or_default();
                if !entries.is_empty() {
                    self.deliver_entries(&entries, transport);
                }
            }
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

    // ── Reactive wake (hot path) ────────────────────────────────────────

    /// Called after publish appends new entries. Reads from store, delivers.
    /// This is THE delivery trigger for new messages.
    #[inline]
    pub fn wake(
        &mut self,
        store: &dyn Store,
        transport: &dyn Transport,
        first_seq: u64,
        count: usize,
    ) -> u32 {
        let entries = store.read_range(first_seq, first_seq + count as u64)
            .unwrap_or_default();
        if entries.is_empty() {
            return 0;
        }
        self.deliver_entries(&entries, transport)
    }

    /// Called on ack — releases credit, then tries to deliver pending entries
    /// that were blocked by inflight limits.
    pub fn on_ack(
        &mut self,
        consumer_id: u32,
        seq: u64,
        store: &dyn Store,
        transport: &dyn Transport,
    ) -> bool {
        let consumer = match self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            Some(c) => c,
            None => return false,
        };
        consumer.release(seq);

        // After releasing credit, try to deliver next pending entry for this consumer
        if consumer.is_active() && consumer.has_credit(b"") {
            let info = store.info();
            if consumer.deliver_seq <= info.last_seq {
                let entries = store.read_range(consumer.deliver_seq, consumer.deliver_seq + 1)
                    .unwrap_or_default();
                if let Some(entry) = entries.first() {
                    if consumer.matches(&entry.subject) && consumer.has_credit(&entry.subject) {
                        Self::send_entry(consumer, entry, self.stream_id, transport);
                    }
                }
            }
        }
        true
    }

    /// Called on nack — release credit, add to nacked list for redelivery.
    pub fn on_nack(
        &mut self,
        consumer_id: u32,
        seq: u64,
        store: &dyn Store,
        transport: &dyn Transport,
    ) -> bool {
        let consumer = match self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            Some(c) => c,
            None => return false,
        };
        consumer.release(seq);
        consumer.nacked.push(seq);

        // Try to redeliver nacked entries immediately
        self.drain_nacked(consumer_id, store, transport);
        true
    }

    /// Fetch (pull mode) — read up to max_msgs entries for a specific consumer.
    pub fn fetch(
        &mut self,
        consumer_id: u32,
        max_msgs: u32,
        store: &dyn Store,
        transport: &dyn Transport,
    ) -> u32 {
        let consumer = match self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            Some(c) => c,
            None => return 0,
        };

        if !consumer.is_active() {
            return 0;
        }

        let info = store.info();
        if consumer.deliver_seq > info.last_seq {
            return 0;
        }

        let end = core::cmp::min(
            consumer.deliver_seq + max_msgs as u64,
            info.last_seq + 1,
        );
        let entries = store.read_range(consumer.deliver_seq, end)
            .unwrap_or_default();

        let mut delivered = 0u32;
        for entry in &entries {
            if !consumer.matches(&entry.subject) || !consumer.has_credit(&entry.subject) {
                continue;
            }
            if Self::send_entry(consumer, entry, self.stream_id, transport) {
                delivered += 1;
            }
        }
        delivered
    }

    // ── Internal delivery ───────────────────────────────────────────────

    /// Deliver a batch of entries to matching consumers.
    /// Fanout: groups by conn_id, sends once per connection.
    /// Queue: round-robin pick one.
    fn deliver_entries(&mut self, entries: &[Entry], transport: &dyn Transport) -> u32 {
        let mut delivered = 0u32;

        for entry in entries {
            let subject = &entry.subject;

            // Stack-allocated queue candidate indices
            let mut queue_buf = [0usize; MAX_QUEUE_CANDIDATES];
            let mut queue_count = 0usize;

            // Track which conn_ids we've already sent to for fanout
            let mut fanout_conns = [0u64; MAX_QUEUE_CANDIDATES];
            let mut fanout_conn_count = 0usize;

            for (i, consumer) in self.consumers.iter_mut().enumerate() {
                if !consumer.is_active() || !consumer.matches(subject) {
                    continue;
                }

                match consumer.config.deliver_mode {
                    DeliverMode::Fanout => {
                        use arbitro_proto::config::AckPolicy;
                        if consumer.config.ack_policy == AckPolicy::None {
                            // Fire-and-forget: send once per connection, client demuxes
                            let conn = consumer.conn_id;
                            let already_sent = fanout_conns[..fanout_conn_count].contains(&conn);
                            if !already_sent {
                                Self::send_entry_fanout(conn, entry, self.stream_id, transport);
                                if fanout_conn_count < MAX_QUEUE_CANDIDATES {
                                    fanout_conns[fanout_conn_count] = conn;
                                    fanout_conn_count += 1;
                                }
                                delivered += 1;
                            }
                            consumer.deliver_seq = entry.seq + 1;
                        } else {
                            // Explicit ack: per-consumer credit tracking
                            if Self::send_entry(consumer, entry, self.stream_id, transport) {
                                delivered += 1;
                            }
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
                if Self::send_entry(consumer, entry, self.stream_id, transport) {
                    delivered += 1;
                }
            }
        }

        delivered
    }

    /// Send a single entry to a specific consumer with credit tracking.
    #[inline]
    fn send_entry(consumer: &mut Consumer, entry: &Entry, stream_id: u32, transport: &dyn Transport) -> bool {
        if !consumer.try_acquire(entry.seq, &entry.subject) {
            return false;
        }
        consumer.deliver_seq = entry.seq + 1;

        let body_len = 2 + entry.subject.len() + entry.payload.len();
        let env = build_delivery_envelope(stream_id, body_len as u32, entry.seq as u32);
        let subj_len_bytes = (entry.subject.len() as u16).to_le_bytes();

        // Scatter write — no concatenation
        transport.send_parts(consumer.conn_id, &[
            &env,
            &subj_len_bytes,
            &entry.subject,
            &entry.payload,
        ])
    }

    /// Send entry to a connection without credit tracking (fanout, one per conn).
    #[inline]
    fn send_entry_fanout(conn_id: ConnId, entry: &Entry, stream_id: u32, transport: &dyn Transport) {
        let body_len = 2 + entry.subject.len() + entry.payload.len();
        let env = build_delivery_envelope(stream_id, body_len as u32, entry.seq as u32);
        let subj_len_bytes = (entry.subject.len() as u16).to_le_bytes();

        transport.send_parts(conn_id, &[
            &env,
            &subj_len_bytes,
            &entry.subject,
            &entry.payload,
        ]);
    }

    /// Drain nacked entries for a consumer — redeliver them.
    fn drain_nacked(&mut self, consumer_id: u32, store: &dyn Store, transport: &dyn Transport) {
        let consumer = match self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            Some(c) => c,
            None => return,
        };

        if !consumer.is_active() || consumer.nacked.is_empty() {
            return;
        }

        // Drain nacked sequences one at a time while we have credit
        while let Some(&seq) = consumer.nacked.first() {
            if !consumer.has_credit(b"") {
                break;
            }
            if let Ok(Some(entry)) = store.read(seq) {
                if consumer.matches(&entry.subject) && consumer.has_credit(&entry.subject)
                    && Self::send_entry(consumer, &entry, self.stream_id, transport)
                {
                    consumer.nacked.remove(0);
                    continue;
                }
            }
            // Entry not found or no credit — stop
            consumer.nacked.remove(0);
        }
    }

    // ── Query ───────────────────────────────────────────────────────────

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
    use arbitro_store::{MemoryStore, EntryRef};
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

    fn consumer_cfg(name: &[u8], id: u32, mode: DeliverMode) -> ConsumerConfig {
        let mut cfg = ConsumerConfig::new(name, b"TEST")
            .filter(b"orders.>")
            .ack_policy(AckPolicy::None)
            .deliver_mode(mode)
            .build();
        cfg.consumer_id = id;
        cfg
    }

    fn make_store_with_entries(count: u64) -> MemoryStore {
        let mut store = MemoryStore::new();
        for i in 0..count {
            store.append(
                EntryRef { subject: b"orders.created", payload: b"{}" },
                1000 + i,
            ).unwrap();
        }
        store
    }

    #[test]
    fn wake_delivers_to_fanout() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        drain.add_consumer(consumer_cfg(b"c2", 2, DeliverMode::Fanout), 1);
        drain.consumers[0].conn_id = 100;
        drain.consumers[1].conn_id = 200;

        let seq = store.append(
            EntryRef { subject: b"orders.created", payload: b"{}" },
            1000,
        ).unwrap();

        let delivered = drain.wake(&store, &transport, seq, 1);
        assert_eq!(delivered, 2);
        assert_eq!(transport.sent(), 2);
    }

    #[test]
    fn fanout_sends_once_per_connection() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        // Two consumers on SAME connection
        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        drain.add_consumer(consumer_cfg(b"c2", 2, DeliverMode::Fanout), 1);
        drain.consumers[0].conn_id = 100;
        drain.consumers[1].conn_id = 100; // same conn

        let seq = store.append(
            EntryRef { subject: b"orders.created", payload: b"{}" },
            1000,
        ).unwrap();

        let delivered = drain.wake(&store, &transport, seq, 1);
        // Only 1 send — same connection, client demuxes
        assert_eq!(delivered, 1);
        assert_eq!(transport.sent(), 1);
    }

    #[test]
    fn queue_delivers_to_one() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Queue), 1);
        drain.add_consumer(consumer_cfg(b"c2", 2, DeliverMode::Queue), 1);
        drain.consumers[0].conn_id = 100;
        drain.consumers[1].conn_id = 200;

        let seq = store.append(
            EntryRef { subject: b"orders.created", payload: b"{}" },
            1000,
        ).unwrap();

        let delivered = drain.wake(&store, &transport, seq, 1);
        assert_eq!(delivered, 1);
        assert_eq!(transport.sent(), 1);
    }

    #[test]
    fn queue_round_robin() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Queue), 1);
        drain.add_consumer(consumer_cfg(b"c2", 2, DeliverMode::Queue), 1);
        drain.consumers[0].conn_id = 100;
        drain.consumers[1].conn_id = 200;

        for i in 0..4u64 {
            store.append(
                EntryRef { subject: b"orders.created", payload: b"{}" },
                1000 + i,
            ).unwrap();
            drain.wake(&store, &transport, i + 1, 1);
        }

        assert_eq!(transport.sent(), 4);
    }

    #[test]
    fn inactive_consumer_skipped() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        // c1 not bound — inactive

        let seq = store.append(
            EntryRef { subject: b"orders.created", payload: b"{}" },
            1000,
        ).unwrap();

        assert_eq!(drain.wake(&store, &transport, seq, 1), 0);
    }

    #[test]
    fn non_matching_subject_skipped() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        drain.consumers[0].conn_id = 100;

        let seq = store.append(
            EntryRef { subject: b"payments.done", payload: b"{}" },
            1000,
        ).unwrap();

        assert_eq!(drain.wake(&store, &transport, seq, 1), 0);
    }

    #[test]
    fn unbind_conn_clears_all() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        drain.add_consumer(consumer_cfg(b"c2", 2, DeliverMode::Fanout), 1);
        drain.consumers[0].conn_id = 100;
        drain.consumers[1].conn_id = 100;

        drain.unbind_conn(100);

        let seq = store.append(
            EntryRef { subject: b"orders.created", payload: b"{}" },
            1000,
        ).unwrap();

        assert_eq!(drain.wake(&store, &transport, seq, 1), 0);
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
    fn ack_releases_credit_and_delivers() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        let mut cfg = ConsumerConfig::new(b"c1", b"TEST")
            .filter(b"orders.>")
            .ack_policy(AckPolicy::Explicit)
            .max_inflight(1)
            .build();
        cfg.consumer_id = 1;

        drain.add_consumer(cfg, 1);
        drain.consumers[0].conn_id = 100;

        // Append 2 entries
        store.append(EntryRef { subject: b"orders.created", payload: b"1" }, 1000).unwrap();
        store.append(EntryRef { subject: b"orders.created", payload: b"2" }, 1001).unwrap();

        // Wake delivers first entry (inflight = 1)
        assert_eq!(drain.wake(&store, &transport, 1, 2), 1);

        // Ack releases credit, delivers next
        assert!(drain.on_ack(1, 1, &store, &transport));
        assert_eq!(transport.sent(), 2); // total sends
    }

    #[test]
    fn fetch_pulls_entries() {
        let transport = CountTransport::new();
        let store = make_store_with_entries(5);
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        drain.consumers[0].conn_id = 100;

        let fetched = drain.fetch(1, 3, &store, &transport);
        assert_eq!(fetched, 3);
        assert_eq!(transport.sent(), 3);
    }

    #[test]
    fn fetch_respects_available() {
        let transport = CountTransport::new();
        let store = make_store_with_entries(2);
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        drain.consumers[0].conn_id = 100;

        // Ask for 10 but only 2 exist
        let fetched = drain.fetch(1, 10, &store, &transport);
        assert_eq!(fetched, 2);
    }

    #[test]
    fn on_bind_delivers_backlog() {
        let transport = CountTransport::new();
        let store = make_store_with_entries(3);
        let mut drain = ReactiveDrain::new(1);

        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);
        // Consumer starts at seq 1, has backlog of 3 entries

        drain.on_bind(1, 100, &store, &transport);
        // Should deliver all 3 backlog entries
        assert_eq!(transport.sent(), 3);
    }
}
