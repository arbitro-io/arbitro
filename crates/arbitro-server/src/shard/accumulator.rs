//! Frame accumulator — packs wire bytes into one `RepBatch` frame per
//! `(connection, stream)` pair active in a drain cycle.
//!
//! **Single responsibility: wire-level grouping.**
//!
//! Does not validate. Does not touch demand, inflight, subject limits,
//! paused state, match tables, or ack bookkeeping. All of that is the
//! drain's responsibility and already has working mechanisms elsewhere
//! (`engine::inflight::InFlightCounters`, `catalog::Binding.pending`,
//! `DrainNotification::Delivered` → `Command::Delivered`).
//!
//! Lifecycle per drain cycle:
//!
//! ```ignore
//! acc.clear();
//! for entry in store.for_each(..) {
//!     // drain-side validation + match resolution already decided
//!     // these (conn, consumer) targets are valid.
//!     for target in targets {
//!         acc.add(target.conn, stream, target.consumer, seq, subject, subject_hash, payload);
//!     }
//! }
//! acc.for_each(names, |frame| write_all_blocking(&writer, &frame.bytes));
//! ```
//!
//! Buckets are pooled. `BytesMut` capacity is reused across cycles — no
//! reallocation in steady state.

use std::sync::Arc;

use arbitro_engine_v2::types::{ConnectionId, ConsumerId, StreamId};
use arbitro_proto::action::Action;
use arbitro_proto::wire::delivery::{
    DeliveryEntryHeader, RepBatchFixed, DELIVERY_ENTRY_HEADER_SIZE,
};
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use bytes::{Bytes, BytesMut};
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::IntoBytes;

use crate::common::NameRegistry;

// ── Bucket ────────────────────────────────────────────────────────────────

struct Bucket {
    body: BytesMut,
    count: u16,
    first_seq: u64,
    stream_id: StreamId,
    connection_id: ConnectionId,
    in_use: bool,
}

impl Bucket {
    fn new_blank() -> Self {
        Self {
            body: BytesMut::with_capacity(64 * 1024),
            count: 0,
            first_seq: 0,
            stream_id: StreamId(0),
            connection_id: ConnectionId(0),
            in_use: false,
        }
    }

    fn activate(&mut self, conn: ConnectionId, stream: StreamId, first_seq: u64) {
        self.body.clear();
        self.body.extend_from_slice(&[0u8; ENVELOPE_SIZE]);
        self.body.extend_from_slice(
            RepBatchFixed { count: U16::new(0), _pad: U16::new(0) }.as_bytes(),
        );
        self.count = 0;
        self.first_seq = first_seq;
        self.stream_id = stream;
        self.connection_id = conn;
        self.in_use = true;
    }

    fn release(&mut self) {
        self.body.clear();
        self.count = 0;
        self.in_use = false;
    }

    #[inline]
    fn push_entry_bytes(
        &mut self,
        consumer_id: u32,
        seq: u64,
        subject_hash: u32,
        subject: &[u8],
        payload: &[u8],
    ) {
        const ENTRY_SCRATCH_SIZE: usize = 4096;
        let subj_len = subject.len();
        let data_len = subj_len + payload.len();
        let total = DELIVERY_ENTRY_HEADER_SIZE + data_len;

        let header = DeliveryEntryHeader {
            consumer_id: U32::new(consumer_id),
            seq: U64::new(seq),
            subj_len: U16::new(subj_len as u16),
            data_len: U32::new(data_len as u32),
            subject_hash: U32::new(subject_hash),
        };

        if total <= ENTRY_SCRATCH_SIZE {
            let mut scratch = [0u8; ENTRY_SCRATCH_SIZE];
            scratch[..DELIVERY_ENTRY_HEADER_SIZE].copy_from_slice(header.as_bytes());
            let subj_end = DELIVERY_ENTRY_HEADER_SIZE + subj_len;
            scratch[DELIVERY_ENTRY_HEADER_SIZE..subj_end].copy_from_slice(subject);
            scratch[subj_end..total].copy_from_slice(payload);
            self.body.extend_from_slice(&scratch[..total]);
        } else {
            self.body.extend_from_slice(header.as_bytes());
            self.body.extend_from_slice(subject);
            self.body.extend_from_slice(payload);
        }

        self.count = self.count.saturating_add(1);
        if self.count == 1 {
            self.first_seq = seq;
        }
    }
}

// ── Accumulator ───────────────────────────────────────────────────────────

/// Mechanical wire grouper. Reused across drain cycles.
pub struct Accumulator {
    buckets: Vec<Bucket>,
    /// `(conn_raw, stream_raw, bucket_idx)` — linear scan over the
    /// handful of connections touched in a cycle.
    active: Vec<(u64, u32, usize)>,
}

impl Default for Accumulator {
    fn default() -> Self { Self::new() }
}

/// Frame handed to the flush callback. Bytes already have the envelope
/// and `RepBatchFixed` header patched — ready to write to the socket.
pub struct Frame {
    pub connection_id: ConnectionId,
    pub stream_id: StreamId,
    pub first_seq: u64,
    pub count: u16,
    pub bytes: Bytes,
}

impl Accumulator {
    pub fn new() -> Self {
        Self {
            buckets: Vec::with_capacity(16),
            active: Vec::with_capacity(16),
        }
    }

    /// Reset for a new cycle. Does not deallocate — `BytesMut` capacity
    /// stays resident for reuse.
    pub fn clear(&mut self) {
        for b in &mut self.buckets {
            if b.in_use { b.release(); }
        }
        self.active.clear();
    }

    /// Append one entry's wire bytes to the bucket for `(conn, stream)`.
    /// The bucket is created on first touch per cycle; subsequent calls
    /// for the same `(conn, stream)` append to the same `BytesMut`.
    ///
    /// `consumer_id` is written verbatim into the `DeliveryEntryHeader`.
    /// Use `0` for broadcast (client resolves locally).
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn add(
        &mut self,
        conn: ConnectionId,
        stream: StreamId,
        consumer: ConsumerId,
        seq: u64,
        subject: &[u8],
        subject_hash: u32,
        payload: &[u8],
    ) {
        let idx = self.acquire_bucket(conn, stream, seq);
        self.buckets[idx].push_entry_bytes(consumer.0, seq, subject_hash, subject, payload);
    }

    /// Iterate each active bucket, patch envelope + count, hand the
    /// prepared frame to `flush`. Bucket is released after the callback
    /// returns, regardless of its result.
    pub fn for_each<F>(&mut self, names: &Arc<NameRegistry>, mut flush: F)
    where
        F: FnMut(Frame) -> bool,
    {
        for &(_conn_raw, _stream_raw, idx) in self.active.iter() {
            let b = &mut self.buckets[idx];
            if b.count == 0 {
                b.release();
                continue;
            }

            let count_off = ENVELOPE_SIZE;
            b.body[count_off..count_off + 2].copy_from_slice(&b.count.to_le_bytes());

            let body_len = b.body.len() - ENVELOPE_SIZE;
            let wire_stream_id = names
                .stream_wire(b.stream_id)
                .unwrap_or_else(|| b.stream_id.raw());
            let envelope = Envelope::new(Action::RepBatch, wire_stream_id, body_len as u32, 0);
            b.body[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());

            let frame = Frame {
                connection_id: b.connection_id,
                stream_id: b.stream_id,
                first_seq: b.first_seq,
                count: b.count,
                bytes: b.body.split().freeze(),
            };
            let _ = flush(frame);
            b.release();
        }
        self.active.clear();
    }

    fn acquire_bucket(&mut self, conn: ConnectionId, stream: StreamId, first_seq: u64) -> usize {
        for &(c, s, idx) in self.active.iter() {
            if c == conn.0 && s == stream.raw() {
                return idx;
            }
        }
        let idx = match self.buckets.iter().position(|b| !b.in_use) {
            Some(i) => i,
            None => {
                self.buckets.push(Bucket::new_blank());
                self.buckets.len() - 1
            }
        };
        self.buckets[idx].activate(conn, stream, first_seq);
        self.active.push((conn.0, stream.raw(), idx));
        idx
    }

    // ── Test helpers ─────────────────────────────────────────────────────

    #[cfg(test)]
    pub(crate) fn active_count(&self) -> usize { self.active.len() }

    #[cfg(test)]
    pub(crate) fn bucket_count_for(&self, conn: ConnectionId, stream: StreamId) -> Option<u16> {
        self.active.iter().find_map(|&(c, s, idx)| {
            if c == conn.0 && s == stream.raw() { Some(self.buckets[idx].count) } else { None }
        })
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const SUBJECT: &[u8] = b"demo.topic";
    const PAYLOAD: &[u8] = b"hello";

    fn names() -> Arc<NameRegistry> { Arc::new(NameRegistry::default()) }

    #[test]
    fn single_conn_many_entries_one_bucket() {
        let mut acc = Accumulator::new();
        acc.clear();
        for seq in 1..=10u64 {
            acc.add(ConnectionId(100), StreamId(1), ConsumerId(0),
                    seq, SUBJECT, 0xDEAD, PAYLOAD);
        }
        assert_eq!(acc.active_count(), 1);
        assert_eq!(acc.bucket_count_for(ConnectionId(100), StreamId(1)), Some(10));
    }

    #[test]
    fn multi_conn_one_bucket_each() {
        let mut acc = Accumulator::new();
        acc.clear();
        for conn in 1u64..=4 {
            for seq in 1..=5u64 {
                acc.add(ConnectionId(conn), StreamId(1), ConsumerId(0),
                        seq, SUBJECT, 0xDEAD, PAYLOAD);
            }
        }
        assert_eq!(acc.active_count(), 4);
        for conn in 1u64..=4 {
            assert_eq!(acc.bucket_count_for(ConnectionId(conn), StreamId(1)), Some(5));
        }
    }

    #[test]
    fn same_conn_different_streams_distinct_buckets() {
        let mut acc = Accumulator::new();
        acc.clear();
        acc.add(ConnectionId(100), StreamId(1), ConsumerId(0), 1, SUBJECT, 0xDEAD, PAYLOAD);
        acc.add(ConnectionId(100), StreamId(2), ConsumerId(0), 1, SUBJECT, 0xDEAD, PAYLOAD);
        assert_eq!(acc.active_count(), 2);
    }

    #[test]
    fn clear_reuses_pool() {
        let mut acc = Accumulator::new();
        for _ in 0..3 {
            acc.clear();
            for conn in 1u64..=4 {
                acc.add(ConnectionId(conn), StreamId(1), ConsumerId(0),
                        1, SUBJECT, 0xDEAD, PAYLOAD);
            }
            assert_eq!(acc.active_count(), 4);
        }
        // Pool caps at 4 — every cycle reuses the same buckets.
        assert_eq!(acc.buckets.len(), 4);
    }

    #[test]
    fn for_each_emits_one_frame_per_active_bucket() {
        let mut acc = Accumulator::new();
        acc.clear();
        for conn in 1u64..=3 {
            for seq in 1..=4u64 {
                acc.add(ConnectionId(conn), StreamId(1), ConsumerId(0),
                        seq, SUBJECT, 0xDEAD, PAYLOAD);
            }
        }

        let mut seen: Vec<(u64, u16)> = Vec::new();
        acc.for_each(&names(), |frame| {
            seen.push((frame.connection_id.0, frame.count));
            true
        });

        seen.sort();
        assert_eq!(seen, vec![(1, 4), (2, 4), (3, 4)]);
        assert_eq!(acc.active_count(), 0);
    }

    #[test]
    fn frame_bytes_contain_envelope_and_rep_batch_header() {
        let mut acc = Accumulator::new();
        acc.clear();
        acc.add(ConnectionId(100), StreamId(1), ConsumerId(0),
                42, SUBJECT, 0xBEEF, PAYLOAD);

        let mut seen = false;
        acc.for_each(&names(), |frame| {
            assert_eq!(frame.connection_id, ConnectionId(100));
            assert_eq!(frame.count, 1);
            assert_eq!(frame.first_seq, 42);
            assert!(frame.bytes.len() > ENVELOPE_SIZE + 4);
            seen = true;
            true
        });
        assert!(seen);
    }

    /// Scenario: drain already resolved matches and decided to
    /// broadcast to one conn. Exactly one `add()` call produces one
    /// entry in the bucket — the 40× saving vs pushing one entry per
    /// match is realized by the *caller* choosing broadcast.
    #[test]
    fn broadcast_collapse_shows_1_entry_per_broadcast_call() {
        let mut acc = Accumulator::new();
        acc.clear();
        acc.add(ConnectionId(100), StreamId(1),
                ConsumerId(0),            // broadcast
                1, SUBJECT, 0xDEAD, PAYLOAD);
        assert_eq!(acc.bucket_count_for(ConnectionId(100), StreamId(1)), Some(1));
    }
}
