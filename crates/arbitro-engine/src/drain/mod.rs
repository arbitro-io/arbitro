//! ReactiveDrain — delivery engine per stream.
//!
//! The drain is the ONLY component that reads from the store and delivers
//! to connections. Publish just appends + signals. Ack just releases credit + signals.
//!
//! Zero-alloc on hot path: scratch buffers reused across cycles,
//! callback-based store reads (get/for_each) borrow directly from storage.
//!
//! For Fanout: each subscription on the consumer receives every matching entry.
//! For Queue: round-robin across subscriptions within the consumer.

pub mod consumer;
pub mod frame_builder;
pub mod signal;
pub mod subscription;

use consumer::Consumer;
use frame_builder::{
    build_rep_batch_envelope, build_rep_batch_fixed, build_entry_header,
    build_delivery_envelope,
};
use subscription::Subscription;

use bytes::BytesMut;

use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverMode};
use arbitro_proto::ids::ConnId;
use arbitro_proto::wire::envelope::ENVELOPE_SIZE;
use arbitro_store::{Entry, Store};

use crate::transport::Transport;

/// Max entries per delivery cycle per consumer.
const BATCH_SIZE: usize = 256;

/// Scratch buffers reused across delivery cycles. Capacity grows monotonically.
struct DrainScratch {
    /// Reusable buffer for building batch frames. BytesMut so we can freeze → Bytes zero-copy.
    frame_buf: BytesMut,
}

impl DrainScratch {
    fn new() -> Self {
        Self {
            frame_buf: BytesMut::with_capacity(16 * 1024), // 16KB initial, grows monotonically
        }
    }
}

/// Reactive drain — owns all delivery logic for a single stream.
///
/// Lives inside StreamSlot. Accessed via the stream's shard lock.
/// The server's drain task calls `deliver_cycle()` in a loop, gated by
/// the stream's DrainSignal (Gate/Notify).
pub struct ReactiveDrain {
    stream_id: u32,
    consumers: Vec<Consumer>,
    /// Round-robin index for Queue mode delivery.
    queue_idx: usize,
    /// Reusable scratch buffers — zero alloc after warmup.
    scratch: DrainScratch,
}

impl ReactiveDrain {
    pub fn new(stream_id: u32) -> Self {
        Self {
            stream_id,
            consumers: Vec::new(),
            queue_idx: 0,
            scratch: DrainScratch::new(),
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

    /// Bind a consumer to a connection (add subscription).
    pub fn bind(&mut self, consumer_id: u32, conn_id: ConnId) -> bool {
        if let Some(c) = self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            c.add_subscription(Subscription::new(conn_id));
            true
        } else {
            false
        }
    }

    /// Unbind a consumer from a specific connection.
    pub fn unbind(&mut self, consumer_id: u32, conn_id: ConnId) -> bool {
        if let Some(c) = self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            c.remove_conn(conn_id)
        } else {
            false
        }
    }

    /// Unbind all consumers from a given connection (disconnect cleanup).
    pub fn unbind_conn(&mut self, conn_id: ConnId) {
        for c in &mut self.consumers {
            c.remove_conn(conn_id);
        }
    }

    /// Release credit on ack. Returns true if consumer found.
    pub fn on_ack(&mut self, consumer_id: u32, seq: u64) -> bool {
        if let Some(c) = self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            c.release(seq);
            true
        } else {
            false
        }
    }

    /// Release credit for a batch of acked sequences. Returns number of acks processed.
    pub fn on_batch_ack(&mut self, consumer_id: u32, seqs: &[u64]) -> u32 {
        if let Some(c) = self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            let mut count = 0u32;
            for &seq in seqs {
                c.release(seq);
                count += 1;
            }
            count
        } else {
            0
        }
    }

    /// Release credit on nack, queue for redelivery. Returns true if consumer found.
    pub fn on_nack(&mut self, consumer_id: u32, seq: u64) -> bool {
        if let Some(c) = self.consumers.iter_mut().find(|c| c.config.consumer_id == consumer_id) {
            c.release(seq);
            c.nacked.push_back(seq);
            true
        } else {
            false
        }
    }

    // ── Delivery (hot path) ─────────────────────────────────────────────

    /// Run one delivery cycle. Returns true if any progress was made.
    ///
    /// Called from the drain task (async, gated by DrainSignal) or
    /// directly in tests (synchronous).
    ///
    /// Zero-alloc: uses scratch buffers, callback-based store reads.
    /// No Vec::new, no clone, no Bytes::copy_from_slice.
    pub fn deliver_cycle(
        &mut self,
        store: &dyn Store,
        transport: &dyn Transport,
        _now_ts: u64,
    ) -> bool {
        let mut any_progress = false;
        let stream_id = self.stream_id;

        for ci in 0..self.consumers.len() {
            let consumer = &mut self.consumers[ci];
            if consumer.subscriptions.is_empty() { continue; }
            if !consumer.has_global_credit() { continue; }

            // Single-pass building: Start building the frame immediately
            let mut delivered_count = 0u16;
            let frame_buf = &mut self.scratch.frame_buf;
            frame_buf.clear();

            // Reserve space for envelope [16B]
            frame_buf.extend_from_slice(&[0u8; ENVELOPE_SIZE]);

            // Placeholder for RepBatchFixed [8B]
            let fixed_pos = frame_buf.len();
            frame_buf.extend_from_slice(&[0u8; 8]);

            // 1. Process Nacked/Deferred first (Individual lookups)
            // (Keeping it simple for these non-contiguous entries)
            process_priority_entries(store, consumer, BATCH_SIZE, &mut delivered_count, frame_buf);

            // 2. Continuous Read (Single-Pass for_each)
            if delivered_count < BATCH_SIZE as u16 && consumer.deliver_seq < store.info().last_seq + 1 {
                let remaining = BATCH_SIZE - delivered_count as usize;
                let scan_end = (consumer.deliver_seq + (remaining * 2) as u64).min(store.info().last_seq + 1);
                
                let is_none_policy = consumer.config.ack_policy == AckPolicy::None;
                let is_queue = consumer.config.deliver_mode == DeliverMode::Queue;
                let sub_count = consumer.subscriptions.len();
                
                let mut current_delivered = delivered_count;
                let queue_idx = &mut self.queue_idx;

                store.for_each(consumer.deliver_seq, scan_end, &mut |entry| {
                    if current_delivered >= BATCH_SIZE as u16 { return; }
                    if !is_none_policy && !consumer.has_global_credit() { return; }

                    // Advance cursor
                    consumer.deliver_seq = entry.seq + 1;

                    if consumer.matches(&entry.subject) {
                        if is_none_policy || consumer.try_acquire(entry.seq, &entry.subject) {
                            if is_queue && sub_count > 1 {
                                // Queue with multiple subs: send immediately to balance load
                                let conn_id = consumer.subscriptions[*queue_idx % sub_count].conn_id;
                                *queue_idx = queue_idx.wrapping_add(1);
                                send_entry(conn_id, entry, stream_id, transport);
                            } else {
                                // Fanout or Single-sub Queue: write to batch frame
                                write_entry_to_buf(entry, frame_buf);
                            }
                            current_delivered += 1;
                        } else if consumer.has_global_credit() {
                            consumer.deferred.push_back(entry.seq);
                        }
                    }
                }).ok();
                delivered_count = current_delivered;
            }

            if delivered_count == 0 { continue; }
            any_progress = true;

            // Finalize and Dispatch Batch Frame if any entries were batched
            // (In Queue mode with sub_count > 1, delivered_count messages were sent but 0 were batched)
            let batched_in_buf = if consumer.config.deliver_mode == DeliverMode::Queue && consumer.subscriptions.len() > 1 {
                0
            } else {
                delivered_count
            };

            if batched_in_buf > 0 {
                finalize_batch_frame(frame_buf, fixed_pos, consumer.config.consumer_id, batched_in_buf, stream_id);
                let frame = frame_buf.split().freeze();
                dispatch_frame(consumer, &mut self.queue_idx, frame, transport);
            }
        }

        any_progress
    }

    /// Fetch (pull mode) — deliver up to max_msgs entries for a specific consumer.
    pub fn fetch(
        &mut self,
        consumer_id: u32,
        max_msgs: u32,
        store: &dyn Store,
        transport: &dyn Transport,
        now_ts: u64,
    ) -> u32 {
        let ci = match self.consumers.iter().position(|c| c.config.consumer_id == consumer_id) {
            Some(i) => i,
            None => return 0,
        };

        if self.consumers[ci].subscriptions.is_empty() {
            return 0;
        }

        // Note: fetch logic remains separate as it is pull-based
        let mut seqs = Vec::with_capacity(max_msgs as usize);
        get_next_messages(
            store,
            &mut self.consumers[ci],
            now_ts,
            max_msgs as usize,
            &mut seqs,
        );

        let stream_id = self.stream_id;
        let num_seqs = seqs.len();
        let mut delivered = 0u32;

        for si in 0..num_seqs {
            let seq = seqs[si];
            // Fetch sends to the first subscription
            let conn_id = self.consumers[ci].subscriptions[0].conn_id;
            store.get(seq, &mut |entry| {
                send_entry(conn_id, entry, stream_id, transport);
            }).ok();
            delivered += 1;
        }

        delivered
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

// ── Free functions (hot path) ───────────────────────────────────────────

fn process_priority_entries(
    store: &dyn Store,
    consumer: &mut Consumer,
    batch_size: usize,
    delivered_count: &mut u16,
    buf: &mut BytesMut,
) {
    let is_none_policy = consumer.config.ack_policy == AckPolicy::None;

    // 1. Nacked
    while !consumer.nacked.is_empty() && (*delivered_count as usize) < batch_size {
        if !is_none_policy && !consumer.has_global_credit() { break; }
        let seq = *consumer.nacked.front().unwrap();
        let mut delivered = false;
        let found = store.get(seq, &mut |entry| {
            if is_none_policy || consumer.try_acquire(seq, &entry.subject) {
                write_entry_to_buf(entry, buf);
                *delivered_count += 1;
                delivered = true;
            }
        }).unwrap_or(false);
        if delivered || !found { consumer.nacked.pop_front(); } else { break; }
    }

    // 2. Deferred
    let mut i = 0;
    while i < consumer.deferred.len() && (*delivered_count as usize) < batch_size {
        if !consumer.has_global_credit() { break; }
        let seq = consumer.deferred[i];
        let mut resolved = false;
        let found = store.get(seq, &mut |entry| {
            if consumer.try_acquire(seq, &entry.subject) {
                write_entry_to_buf(entry, buf);
                *delivered_count += 1;
                resolved = true;
            }
        }).unwrap_or(false);
        if resolved || !found { consumer.deferred.remove(i); } else { i += 1; }
    }
}

#[inline]
fn write_entry_to_buf(entry: &Entry, buf: &mut BytesMut) {
    let subj_len = entry.subject.len() as u16;
    let data_len = (entry.subject.len() + entry.payload.len()) as u32;
    let hdr = build_entry_header(entry.seq, subj_len, data_len);
    buf.extend_from_slice(&hdr);
    buf.extend_from_slice(&entry.subject);
    buf.extend_from_slice(&entry.payload);
}

#[inline]
fn finalize_batch_frame(buf: &mut BytesMut, fixed_pos: usize, consumer_id: u32, count: u16, stream_id: u32) {
    let fixed = build_rep_batch_fixed(consumer_id, count);
    buf[fixed_pos..fixed_pos + 8].copy_from_slice(&fixed);
    let msg_len = (buf.len() - ENVELOPE_SIZE) as u32;
    let env = build_rep_batch_envelope(stream_id, msg_len);
    buf[..ENVELOPE_SIZE].copy_from_slice(&env);
}

#[inline]
fn dispatch_frame(consumer: &mut Consumer, queue_idx: &mut usize, frame: bytes::Bytes, transport: &dyn Transport) {
    match consumer.config.deliver_mode {
        DeliverMode::Fanout => {
            for sub in &consumer.subscriptions {
                transport.send_bytes(sub.conn_id, frame.clone());
            }
        }
        DeliverMode::Queue => {
            if !consumer.subscriptions.is_empty() {
                let idx = *queue_idx % consumer.subscriptions.len();
                transport.send_bytes(consumer.subscriptions[idx].conn_id, frame);
                *queue_idx += 1;
            }
        }
    }
}

/// Collect up to `batch_size` deliverable seq numbers for this consumer.
/// Priority: (1) nacked, (2) deferred, (3) new from journal cursor.
fn get_next_messages(
    store: &dyn Store,
    consumer: &mut Consumer,
    _now_ts: u64,
    batch_size: usize,
    seqs: &mut Vec<u64>,
) {
    let info = store.info();
    let head = info.last_seq + 1;
    let is_none_policy = consumer.config.ack_policy == AckPolicy::None;

    while !consumer.nacked.is_empty() && seqs.len() < batch_size {
        if !is_none_policy && !consumer.has_global_credit() { break; }
        let seq = *consumer.nacked.front().unwrap();
        let mut delivered = false;
        let found = store.get(seq, &mut |entry| {
            if is_none_policy || consumer.try_acquire(seq, &entry.subject) {
                seqs.push(seq);
                delivered = true;
            }
        }).unwrap_or(false);
        if delivered || !found { consumer.nacked.pop_front(); } else { break; }
    }

    let mut i = 0;
    while i < consumer.deferred.len() && seqs.len() < batch_size {
        if !consumer.has_global_credit() { break; }
        let seq = consumer.deferred[i];
        let mut resolved = false;
        let found = store.get(seq, &mut |entry| {
            if consumer.try_acquire(seq, &entry.subject) {
                seqs.push(seq);
                resolved = true;
            }
        }).unwrap_or(false);
        if resolved || !found { consumer.deferred.remove(i); } else { i += 1; }
    }

    while consumer.deliver_seq < head && seqs.len() < batch_size {
        if !is_none_policy && !consumer.has_global_credit() { break; }
        let seq = consumer.deliver_seq;
        consumer.deliver_seq += 1;
        store.get(seq, &mut |entry| {
            if !consumer.matches(&entry.subject) { return; }
            if is_none_policy || consumer.try_acquire(seq, &entry.subject) {
                seqs.push(seq);
            } else if consumer.has_global_credit() {
                consumer.deferred.push_back(seq);
            }
        }).ok();
    }
}

/// Send a single entry to a connection. Stack envelope + scatter write.
/// Used as fallback for Queue mode with multiple subscriptions.
#[inline]
fn send_entry(conn_id: ConnId, entry: &Entry, stream_id: u32, transport: &dyn Transport) {
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

    /// Helper: add consumer and bind to conn_id.
    fn add_and_bind(drain: &mut ReactiveDrain, cfg: ConsumerConfig, start_seq: u64, conn_id: ConnId) {
        let id = cfg.consumer_id;
        drain.add_consumer(cfg, start_seq);
        drain.bind(id, conn_id);
    }

    #[test]
    fn deliver_cycle_fanout() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        add_and_bind(&mut drain, consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1, 100);
        add_and_bind(&mut drain, consumer_cfg(b"c2", 2, DeliverMode::Fanout), 1, 200);

        store.append(
            EntryRef { subject: b"orders.created", payload: b"{}" },
            1000,
        ).unwrap();

        let progress = drain.deliver_cycle(&store, &transport, 0);
        assert!(progress);
        assert_eq!(transport.sent(), 2); // one per consumer's subscription
    }

    #[test]
    fn deliver_cycle_queue_round_robin() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        // Queue consumer with two subscriptions
        let cfg = consumer_cfg(b"c1", 1, DeliverMode::Queue);
        drain.add_consumer(cfg, 1);
        drain.bind(1, 100);
        drain.bind(1, 200);

        for i in 0..4u64 {
            store.append(
                EntryRef { subject: b"orders.created", payload: b"{}" },
                1000 + i,
            ).unwrap();
        }

        drain.deliver_cycle(&store, &transport, 0);
        // 4 entries, round-robin across 2 subs = 4 sends total
        assert_eq!(transport.sent(), 4);
    }

    #[test]
    fn deliver_cycle_inactive_skipped() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        // Consumer without subscription = inactive
        drain.add_consumer(consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1);

        store.append(
            EntryRef { subject: b"orders.created", payload: b"{}" },
            1000,
        ).unwrap();

        let progress = drain.deliver_cycle(&store, &transport, 0);
        assert!(!progress);
        assert_eq!(transport.sent(), 0);
    }

    #[test]
    fn deliver_cycle_non_matching_skipped() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        add_and_bind(&mut drain, consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1, 100);

        store.append(
            EntryRef { subject: b"payments.done", payload: b"{}" },
            1000,
        ).unwrap();

        let progress = drain.deliver_cycle(&store, &transport, 0);
        assert!(!progress);
    }

    #[test]
    fn unbind_conn_clears_all() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        add_and_bind(&mut drain, consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1, 100);
        add_and_bind(&mut drain, consumer_cfg(b"c2", 2, DeliverMode::Fanout), 1, 100);

        drain.unbind_conn(100);

        store.append(
            EntryRef { subject: b"orders.created", payload: b"{}" },
            1000,
        ).unwrap();

        assert!(!drain.deliver_cycle(&store, &transport, 0));
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
    fn ack_releases_credit_and_enables_delivery() {
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
        drain.bind(1, 100);

        // Append 2 entries
        store.append(EntryRef { subject: b"orders.created", payload: b"1" }, 1000).unwrap();
        store.append(EntryRef { subject: b"orders.created", payload: b"2" }, 1001).unwrap();

        // First cycle delivers 1 (inflight = 1, limit = 1)
        drain.deliver_cycle(&store, &transport, 0);
        assert_eq!(transport.sent(), 1);

        // Ack releases credit
        assert!(drain.on_ack(1, 1));

        // Second cycle delivers next
        drain.deliver_cycle(&store, &transport, 0);
        assert_eq!(transport.sent(), 2);
    }

    #[test]
    fn nack_redelivers() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        let mut cfg = ConsumerConfig::new(b"c1", b"TEST")
            .filter(b"orders.>")
            .ack_policy(AckPolicy::Explicit)
            .max_inflight(2)
            .build();
        cfg.consumer_id = 1;

        drain.add_consumer(cfg, 1);
        drain.bind(1, 100);

        store.append(EntryRef { subject: b"orders.created", payload: b"1" }, 1000).unwrap();

        // Deliver first entry
        drain.deliver_cycle(&store, &transport, 0);
        assert_eq!(transport.sent(), 1);

        // Nack it — releases credit and queues for redelivery
        assert!(drain.on_nack(1, 1));

        // Next cycle redelivers the nacked entry
        drain.deliver_cycle(&store, &transport, 0);
        assert_eq!(transport.sent(), 2);
    }

    #[test]
    fn fetch_pulls_entries() {
        let transport = CountTransport::new();
        let store = make_store_with_entries(5);
        let mut drain = ReactiveDrain::new(1);

        add_and_bind(&mut drain, consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1, 100);

        let fetched = drain.fetch(1, 3, &store, &transport, 0);
        assert_eq!(fetched, 3);
        assert_eq!(transport.sent(), 3);
    }

    #[test]
    fn fetch_respects_available() {
        let transport = CountTransport::new();
        let store = make_store_with_entries(2);
        let mut drain = ReactiveDrain::new(1);

        add_and_bind(&mut drain, consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1, 100);

        let fetched = drain.fetch(1, 10, &store, &transport, 0);
        assert_eq!(fetched, 2);
    }

    #[test]
    fn deliver_cycle_advances_cursor() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        add_and_bind(&mut drain, consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1, 100);

        // Append 3 entries
        for i in 0..3u64 {
            store.append(EntryRef { subject: b"orders.created", payload: b"{}" }, 1000 + i).unwrap();
        }

        // First cycle delivers all 3 as one batch
        drain.deliver_cycle(&store, &transport, 0);
        assert_eq!(transport.sent(), 1); // 1 batch send

        // Second cycle: no new entries, no progress
        assert!(!drain.deliver_cycle(&store, &transport, 0));

        // Append 2 more
        store.append(EntryRef { subject: b"orders.created", payload: b"{}" }, 2000).unwrap();
        store.append(EntryRef { subject: b"orders.created", payload: b"{}" }, 2001).unwrap();

        // Third cycle delivers the 2 new ones as one batch
        assert!(drain.deliver_cycle(&store, &transport, 0));
        assert_eq!(transport.sent(), 2); // 2 total batch sends
    }

    #[test]
    fn multiple_consumers_independent() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        // Two consumers starting at different positions
        add_and_bind(&mut drain, consumer_cfg(b"c1", 1, DeliverMode::Fanout), 1, 100);

        // Append 3 entries
        for i in 0..3u64 {
            store.append(EntryRef { subject: b"orders.created", payload: b"{}" }, 1000 + i).unwrap();
        }

        // c1 delivers 3 as one batch
        drain.deliver_cycle(&store, &transport, 0);
        assert_eq!(transport.sent(), 1); // 1 batch send

        // Now add c2 starting from seq 1 — it has backlog
        add_and_bind(&mut drain, consumer_cfg(b"c2", 2, DeliverMode::Fanout), 1, 200);

        // c2 delivers its backlog (3 as one batch), c1 has nothing new
        drain.deliver_cycle(&store, &transport, 0);
        assert_eq!(transport.sent(), 2); // 1 + 1 batch sends
    }
}
