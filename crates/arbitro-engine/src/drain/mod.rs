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
pub mod signal;
pub mod subscription;

use consumer::Consumer;
use subscription::Subscription;

use std::collections::HashMap;

use bytes::BytesMut;

use arbitro_proto::action::Action;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverMode};
use arbitro_proto::ids::ConnId;
use arbitro_proto::wire::delivery::RepBatchFixed;
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_proto::wire::headers::{DeliverBatchHeader, DeliveryEntryHeader};
use arbitro_store::{Entry, Store};
use zerocopy::{
    byteorder::little_endian::{U16, U32, U64},
    IntoBytes,
};

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
    consumers: HashMap<u32, Consumer>,
    /// Round-robin index for Queue mode delivery.
    queue_idx: usize,
    /// Reusable scratch buffers — zero alloc after warmup.
    scratch: DrainScratch,
}

impl ReactiveDrain {
    pub fn new(stream_id: u32) -> Self {
        Self {
            stream_id,
            consumers: HashMap::new(),
            queue_idx: 0,
            scratch: DrainScratch::new(),
        }
    }

    // ── Consumer management (cold path) ─────────────────────────────────

    /// Register a consumer. Returns consumer_id.
    pub fn add_consumer(&mut self, config: ConsumerConfig, start_seq: u64) -> u32 {
        let id = config.consumer_id;
        self.consumers.insert(id, Consumer::new(config, start_seq));
        id
    }

    /// Remove a consumer by id.
    pub fn remove_consumer(&mut self, consumer_id: u32) -> bool {
        self.consumers.remove(&consumer_id).is_some()
    }

    /// Bind a consumer to a connection (add subscription).
    pub fn bind(&mut self, consumer_id: u32, conn_id: ConnId) -> bool {
        if let Some(c) = self.consumers.get_mut(&consumer_id) {
            c.add_subscription(Subscription::new(conn_id));
            true
        } else {
            false
        }
    }

    /// Unbind a consumer from a specific connection.
    pub fn unbind(&mut self, consumer_id: u32, conn_id: ConnId) -> bool {
        if let Some(c) = self.consumers.get_mut(&consumer_id) {
            c.remove_conn(conn_id)
        } else {
            false
        }
    }

    /// Unbind all consumers from a given connection (disconnect cleanup).
    pub fn unbind_conn(&mut self, conn_id: ConnId) {
        for consumer in self.consumers.values_mut() {
            if let Some(ref cm) = consumer.credit_map {
                let rescued = cm.scavenge(conn_id);
                for seq in rescued {
                    consumer.nacked.push_back(seq);
                    if consumer.pending_count > 0 {
                        consumer.pending_count -= 1;
                    }
                }
            }
            consumer.remove_conn(conn_id);
        }
    }

    /// Release credit on ack. Returns true if consumer found.
    pub fn on_ack(&mut self, consumer_id: u32, seq: u64) -> bool {
        if let Some(c) = self.consumers.get_mut(&consumer_id) {
            c.release(seq);
            true
        } else {
            false
        }
    }

    /// Release credit for a batch of acked sequences. Returns number of acks processed.
    pub fn on_batch_ack(&mut self, consumer_id: u32, seqs: &[u64]) -> u32 {
        if let Some(c) = self.consumers.get_mut(&consumer_id) {
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
        if let Some(c) = self.consumers.get_mut(&consumer_id) {
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

        for consumer in self.consumers.values_mut() {
            if consumer.subscriptions.is_empty() {
                continue;
            }
            if !consumer.has_global_credit() {
                continue;
            }

            // Single-pass building using DeliverBatchHeader (24 bytes)
            let mut delivered_count = 0u16;
            let frame_buf = &mut self.scratch.frame_buf;
            frame_buf.clear();

            // Reserve space for the composite header [24B]
            const HEADER_SIZE: usize = core::mem::size_of::<DeliverBatchHeader>();
            frame_buf.extend_from_slice(&[0u8; HEADER_SIZE]);

            // 1. Process Nacked/Deferred first (Individual lookups)
            // (Pass first conn_id as representative owner for the batch)
            let batch_conn_id = consumer.subscriptions[0].conn_id;
            process_priority_entries(store, consumer, BATCH_SIZE, &mut delivered_count, frame_buf, batch_conn_id);

            // 2. Continuous Read (Single-Pass for_each)
            if delivered_count < BATCH_SIZE as u16
                && consumer.deliver_seq < store.info().last_seq + 1
            {
                let remaining = BATCH_SIZE - delivered_count as usize;
                let scan_end =
                    (consumer.deliver_seq + (remaining * 2) as u64).min(store.info().last_seq + 1);

                let is_none_policy = consumer.config.ack_policy == AckPolicy::None;
                let is_queue = consumer.config.deliver_mode == DeliverMode::Queue;
                let sub_count = consumer.subscriptions.len();

                let mut current_delivered = delivered_count;
                let queue_idx = &mut self.queue_idx;

                store
                    .for_each(consumer.deliver_seq, scan_end, &mut |entry| {
                        if current_delivered >= BATCH_SIZE as u16 {
                            return;
                        }
                        if !is_none_policy && !consumer.has_global_credit() {
                            return;
                        }

                        // Advance cursor
                        consumer.deliver_seq = entry.seq + 1;

                        if consumer.matches(entry.subject) {
                            // Assign ownership to current subscription in Queue mode, or first for Fanout batching.
                            let conn_id = if is_queue && sub_count > 1 {
                                consumer.subscriptions[*queue_idx % sub_count].conn_id
                            } else {
                                consumer.subscriptions[0].conn_id
                            };

                            if is_none_policy || consumer.try_acquire(entry.seq, entry.subject, conn_id) {
                                if is_queue && sub_count > 1 {
                                    // Queue with multiple subs: already picked conn_id above
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
                    })
                    .ok();
                delivered_count = current_delivered;
            }

            if delivered_count == 0 {
                continue;
            }
            any_progress = true;

            // Finalize and Dispatch Batch Frame if any entries were batched
            // (In Queue mode with sub_count > 1, delivered_count messages were sent but 0 were batched)
            let batched_in_buf = if consumer.config.deliver_mode == DeliverMode::Queue
                && consumer.subscriptions.len() > 1
            {
                0
            } else {
                delivered_count
            };

            if batched_in_buf > 0 {
                let action = match consumer.config.deliver_mode {
                    DeliverMode::Fanout => Action::FanoutBatch,
                    _ => Action::RepBatch,
                };
                finalize_batch_frame(
                    frame_buf,
                    consumer.config.consumer_id,
                    batched_in_buf,
                    stream_id,
                    action,
                );
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
        conn_id: u64,
    ) -> u32 {
        let consumer = match self.consumers.get_mut(&consumer_id) {
            Some(c) => c,
            None => return 0,
        };

        if consumer.subscriptions.is_empty() {
            return 0;
        }

        // Note: fetch logic remains separate as it is pull-based
        let mut seqs = Vec::with_capacity(max_msgs as usize);
        get_next_messages(
            store,
            consumer,
            now_ts,
            max_msgs as usize,
            &mut seqs,
            conn_id,
        );

        let stream_id = self.stream_id;
        let num_seqs = seqs.len();
        let mut delivered = 0u32;

        for si in 0..num_seqs {
            let seq = seqs[si];
            // Fetch sends to the first subscription
            let conn_id = consumer.subscriptions[0].conn_id;
            store
                .get(seq, &mut |entry| {
                    send_entry(conn_id, entry, stream_id, transport);
                })
                .ok();
            delivered += 1;
        }

        delivered
    }

    // ── Query ───────────────────────────────────────────────────────────

    pub fn consumer_count(&self) -> usize {
        self.consumers.len()
    }

    pub fn find_consumer(&self, consumer_id: u32) -> Option<&Consumer> {
        self.consumers.get(&consumer_id)
    }

    /// Iterate all consumers — used for overlap checks (cold path).
    pub fn iter_consumers(&self) -> impl Iterator<Item = &Consumer> {
        self.consumers.values()
    }

    /// Calculate the global low water mark across all registered consumers.
    /// Used for WorkQueue and Interest lazy scavenging.
    pub fn lowest_unacked_seq(&self) -> u64 {
        if self.consumers.is_empty() {
            return 0; // No consumers means we keep data until someone connects
        }
        let mut min_seq = u64::MAX;
        for c in self.consumers.values() {
            min_seq = min_seq.min(c.lowest_unacked_seq());
        }
        if min_seq == u64::MAX { 0 } else { min_seq }
    }
}

// ── Free functions (hot path) ───────────────────────────────────────────

fn process_priority_entries(
    store: &dyn Store,
    consumer: &mut Consumer,
    batch_size: usize,
    delivered_count: &mut u16,
    buf: &mut BytesMut,
    conn_id: u64,
) {
    let is_none_policy = consumer.config.ack_policy == AckPolicy::None;

    // 1. Nacked
    while !consumer.nacked.is_empty() && (*delivered_count as usize) < batch_size {
        if !is_none_policy && !consumer.has_global_credit() {
            break;
        }
        let seq = *consumer.nacked.front().unwrap();
        let mut delivered = false;
        let found = store
            .get(seq, &mut |entry| {
                if is_none_policy || consumer.try_acquire(seq, entry.subject, conn_id) {
                    write_entry_to_buf(entry, buf);
                    *delivered_count += 1;
                    delivered = true;
                }
            })
            .unwrap_or(false);
        if delivered || !found {
            consumer.nacked.pop_front();
        } else {
            break;
        }
    }

    // 2. Deferred
    let num_deferred = consumer.deferred.len();
    for _ in 0..num_deferred {
        if (*delivered_count as usize) >= batch_size || !consumer.has_global_credit() {
            break;
        }

        let seq = consumer.deferred.pop_front().unwrap();
        let mut resolved = false;

        let found = store.get(seq, &mut |entry| {
            if consumer.try_acquire(seq, entry.subject, conn_id) {
                write_entry_to_buf(entry, buf);
                *delivered_count += 1;
                resolved = true;
            }
        }).unwrap_or(false);

        // Re-buffer if message exists but still blocked by subject credit
        if !resolved && found {
            consumer.deferred.push_back(seq);
        }
    }
}

#[inline]
fn write_entry_to_buf(entry: &Entry<'_>, buf: &mut BytesMut) {
    let hdr = DeliveryEntryHeader {
        seq: U64::new(entry.seq),
        subj_len: U16::new(entry.subject.len() as u16),
        data_len: U32::new((entry.subject.len() + entry.payload.len()) as u32),
    };
    buf.extend_from_slice(hdr.as_bytes());
    buf.extend_from_slice(entry.subject);
    buf.extend_from_slice(entry.payload);
}

#[inline]
fn finalize_batch_frame(
    buf: &mut BytesMut,
    consumer_id: u32,
    count: u16,
    stream_id: u32,
    action: Action,
) {
    let msg_len = (buf.len() - ENVELOPE_SIZE) as u32;
    let header = DeliverBatchHeader {
        env: Envelope {
            action: U16::new(action.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(stream_id),
            msg_len: U32::new(msg_len),
            env_seq: U32::new(0),
        },
        batch: RepBatchFixed {
            consumer_id: U32::new(consumer_id),
            count: U16::new(count),
            _pad: U16::new(0),
        },
    };
    buf[..core::mem::size_of::<DeliverBatchHeader>()].copy_from_slice(header.as_bytes());
}

#[inline]
fn dispatch_frame(
    consumer: &mut Consumer,
    queue_idx: &mut usize,
    frame: bytes::Bytes,
    transport: &dyn Transport,
) {
    match consumer.config.deliver_mode {
        DeliverMode::Fanout => {
            // Deduplicate by ConnId to avoid redundant bytes on the same socket
            let mut sent_conns = [0u64; 16];
            let mut sent_count = 0;

            for sub in &consumer.subscriptions {
                let mut found = false;
                for i in 0..sent_count {
                    if sent_conns[i] == sub.conn_id {
                        found = true;
                        break;
                    }
                }
                if !found {
                    transport.send_bytes(sub.conn_id, frame.clone());
                    if sent_count < 16 {
                        sent_conns[sent_count] = sub.conn_id;
                        sent_count += 1;
                    }
                }
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
    conn_id: u64,
) {
    let info = store.info();
    let head = info.last_seq + 1;
    let is_none_policy = consumer.config.ack_policy == AckPolicy::None;

    while !consumer.nacked.is_empty() && seqs.len() < batch_size {
        if !is_none_policy && !consumer.has_global_credit() {
            break;
        }
        let seq = *consumer.nacked.front().unwrap();
        let mut delivered = false;
        let found = store
            .get(seq, &mut |entry| {
                if is_none_policy || consumer.try_acquire(seq, &entry.subject, conn_id) {
                    seqs.push(seq);
                    delivered = true;
                }
            })
            .unwrap_or(false);
        if delivered || !found {
            consumer.nacked.pop_front();
        } else {
            break;
        }
    }

    let mut i = 0;
    while i < consumer.deferred.len() && seqs.len() < batch_size {
        if !consumer.has_global_credit() {
            break;
        }
        let seq = consumer.deferred[i];
        let mut resolved = false;
        let found = store
            .get(seq, &mut |entry| {
                if consumer.try_acquire(seq, &entry.subject, conn_id) {
                    seqs.push(seq);
                    resolved = true;
                }
            })
            .unwrap_or(false);
        if resolved || !found {
            consumer.deferred.remove(i);
        } else {
            i += 1;
        }
    }

    while consumer.deliver_seq < head && seqs.len() < batch_size {
        if !is_none_policy && !consumer.has_global_credit() {
            break;
        }
        let seq = consumer.deliver_seq;
        consumer.deliver_seq += 1;
        store
            .get(seq, &mut |entry| {
                if !consumer.matches(entry.subject) {
                    return;
                }
                if is_none_policy || consumer.try_acquire(seq, entry.subject, conn_id) {
                    seqs.push(seq);
                } else if consumer.has_global_credit() {
                    consumer.deferred.push_back(seq);
                }
            })
            .ok();
    }
}

#[inline]
fn send_entry(conn_id: ConnId, entry: &Entry<'_>, stream_id: u32, transport: &dyn Transport) {
    let body_len = 2 + entry.subject.len() + entry.payload.len();
    let env = Envelope {
        action: U16::new(Action::Deliver.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(body_len as u32),
        env_seq: U32::new(entry.seq as u32),
    };
    let subj_len_bytes = (entry.subject.len() as u16).to_le_bytes();

    transport.send_parts(
        conn_id,
        &[
            env.as_bytes(),
            &subj_len_bytes,
            entry.subject,
            entry.payload,
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use arbitro_proto::config::{AckPolicy, ConsumerConfig, SubjectLimit};
    use arbitro_store::{EntryRef, MemoryStore};
    use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

    /// Counting transport — tracks how many sends happened.
    struct CountTransport {
        count: AtomicU32,
    }

    impl CountTransport {
        fn new() -> Self {
            Self {
                count: AtomicU32::new(0),
            }
        }
        fn sent(&self) -> u32 {
            self.count.load(Relaxed)
        }
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
            store
                .append(
                    EntryRef {
                        subject: b"orders.created",
                        payload: b"{}",
                    },
                    1000 + i,
                )
                .unwrap();
        }
        store
    }

    /// Helper: add consumer and bind to conn_id.
    fn add_and_bind(
        drain: &mut ReactiveDrain,
        cfg: ConsumerConfig,
        start_seq: u64,
        conn_id: ConnId,
    ) {
        let id = cfg.consumer_id;
        drain.add_consumer(cfg, start_seq);
        drain.bind(id, conn_id);
    }

    #[test]
    fn deliver_cycle_fanout() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        add_and_bind(
            &mut drain,
            consumer_cfg(b"c1", 1, DeliverMode::Fanout),
            1,
            100,
        );
        add_and_bind(
            &mut drain,
            consumer_cfg(b"c2", 2, DeliverMode::Fanout),
            1,
            200,
        );

        store
            .append(
                EntryRef {
                    subject: b"orders.created",
                    payload: b"{}",
                },
                1000,
            )
            .unwrap();

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
            store
                .append(
                    EntryRef {
                        subject: b"orders.created",
                        payload: b"{}",
                    },
                    1000 + i,
                )
                .unwrap();
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

        store
            .append(
                EntryRef {
                    subject: b"orders.created",
                    payload: b"{}",
                },
                1000,
            )
            .unwrap();

        let progress = drain.deliver_cycle(&store, &transport, 0);
        assert!(!progress);
        assert_eq!(transport.sent(), 0);
    }

    #[test]
    fn deliver_cycle_non_matching_skipped() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        add_and_bind(
            &mut drain,
            consumer_cfg(b"c1", 1, DeliverMode::Fanout),
            1,
            100,
        );

        store
            .append(
                EntryRef {
                    subject: b"payments.done",
                    payload: b"{}",
                },
                1000,
            )
            .unwrap();

        let progress = drain.deliver_cycle(&store, &transport, 0);
        assert!(!progress);
    }

    #[test]
    fn unbind_conn_clears_all() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        add_and_bind(
            &mut drain,
            consumer_cfg(b"c1", 1, DeliverMode::Fanout),
            1,
            100,
        );
        add_and_bind(
            &mut drain,
            consumer_cfg(b"c2", 2, DeliverMode::Fanout),
            1,
            100,
        );

        drain.unbind_conn(100);

        store
            .append(
                EntryRef {
                    subject: b"orders.created",
                    payload: b"{}",
                },
                1000,
            )
            .unwrap();

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
        store
            .append(
                EntryRef {
                    subject: b"orders.created",
                    payload: b"1",
                },
                1000,
            )
            .unwrap();
        store
            .append(
                EntryRef {
                    subject: b"orders.created",
                    payload: b"2",
                },
                1001,
            )
            .unwrap();

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

        store
            .append(
                EntryRef {
                    subject: b"orders.created",
                    payload: b"1",
                },
                1000,
            )
            .unwrap();

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

        add_and_bind(
            &mut drain,
            consumer_cfg(b"c1", 1, DeliverMode::Fanout),
            1,
            100,
        );

        let fetched = drain.fetch(1, 3, &store, &transport, 0, 100);
        assert_eq!(fetched, 3);
        assert_eq!(transport.sent(), 3);
    }

    #[test]
    fn fetch_respects_available() {
        let transport = CountTransport::new();
        let store = make_store_with_entries(2);
        let mut drain = ReactiveDrain::new(1);

        add_and_bind(
            &mut drain,
            consumer_cfg(b"c1", 1, DeliverMode::Fanout),
            1,
            100,
        );

        let fetched = drain.fetch(1, 10, &store, &transport, 0, 100);
        assert_eq!(fetched, 2);
    }

    #[test]
    fn deliver_cycle_advances_cursor() {
        let transport = CountTransport::new();
        let mut store = MemoryStore::new();
        let mut drain = ReactiveDrain::new(1);

        add_and_bind(
            &mut drain,
            consumer_cfg(b"c1", 1, DeliverMode::Fanout),
            1,
            100,
        );

        // Append 3 entries
        for i in 0..3u64 {
            store
                .append(
                    EntryRef {
                        subject: b"orders.created",
                        payload: b"{}",
                    },
                    1000 + i,
                )
                .unwrap();
        }

        // First cycle delivers all 3 as one batch
        drain.deliver_cycle(&store, &transport, 0);
        assert_eq!(transport.sent(), 1); // 1 batch send

        // Second cycle: no new entries, no progress
        assert!(!drain.deliver_cycle(&store, &transport, 0));

        // Append 2 more
        store
            .append(
                EntryRef {
                    subject: b"orders.created",
                    payload: b"{}",
                },
                2000,
            )
            .unwrap();
        store
            .append(
                EntryRef {
                    subject: b"orders.created",
                    payload: b"{}",
                },
                2001,
            )
            .unwrap();

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
        add_and_bind(
            &mut drain,
            consumer_cfg(b"c1", 1, DeliverMode::Fanout),
            1,
            100,
        );

        // Append 3 entries
        for i in 0..3u64 {
            store
                .append(
                    EntryRef {
                        subject: b"orders.created",
                        payload: b"{}",
                    },
                    1000 + i,
                )
                .unwrap();
        }

        // c1 delivers 3 as one batch
        drain.deliver_cycle(&store, &transport, 0);
        assert_eq!(transport.sent(), 1); // 1 batch send

        // Now add c2 starting from seq 1 — it has backlog
        add_and_bind(
            &mut drain,
            consumer_cfg(b"c2", 2, DeliverMode::Fanout),
            1,
            200,
        );

        // c2 delivers its backlog (3 as one batch), c1 has nothing new
        drain.deliver_cycle(&store, &transport, 0);
        assert_eq!(transport.sent(), 2); // 1 + 1 batch sends
    }

    #[test]
    fn unbind_recovers_credits() {
        let transport = CountTransport::new();
        let store = make_store_with_entries(10);
        let mut drain = ReactiveDrain::new(1);
        let conn_id = 12345u64;

        // 1. Add consumer with strict limit (max_inflight = 2)
        let mut cfg = consumer_cfg(b"c1", 1, DeliverMode::Queue);
        cfg.max_inflight = 2;
        cfg.ack_policy = AckPolicy::Explicit;
        // MUST add a limit rule for scavenging to work (it tracks sequence -> conn_id in the Ring)
        cfg.subject_limits = vec![SubjectLimit { pattern: b"orders.>".to_vec().into_boxed_slice(), limit: 10 }].into_boxed_slice();
        add_and_bind(&mut drain, cfg, 0, conn_id);

        // 2. First cycle delivers 2 messages (hits limit)
        drain.deliver_cycle(&store, &transport, 0);
        assert_eq!(transport.sent(), 1, "Should have sent 1 batch");
        {
            let c = drain.find_consumer(1).unwrap();
            assert_eq!(c.pending_count, 2, "Pending count should be 2 after delivery");
        }

        // 3. Second cycle should do nothing (no credit)
        drain.deliver_cycle(&store, &transport, 0);
        assert_eq!(transport.sent(), 1, "Should not have sent a second batch"); 

        // 4. Disconnect client
        drain.unbind_conn(conn_id);
        
        {
            let c = drain.find_consumer(1).unwrap();
            assert_eq!(c.pending_count, 0, "Pending count should be 0 after scavenger runs");
            assert_eq!(c.nacked.len(), 2, "Should have rescued 2 messages"); 
        }

        // 5. Re-bind the connection so messages can be delivered again
        drain.bind(1, conn_id);

        // 6. Next cycle should deliver them again (since credits were recovered)
        let prog = drain.deliver_cycle(&store, &transport, 0);
        assert!(prog, "Should have made progress after recovery");
        assert_eq!(transport.sent(), 2, "Should have delivered a new batch after recovery");
    }
}
