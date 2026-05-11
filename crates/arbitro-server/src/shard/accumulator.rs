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
//!         acc.add(target.conn, stream, target.consumer, seq, subject, subject_hash, reply_to, payload);
//!     }
//! }
//! acc.for_each(names, |frame| write_all_blocking(&writer, &frame.bytes));
//! ```
//!
//! Buckets are pooled. `BytesMut` capacity is reused across cycles — no
//! reallocation in steady state.

use std::collections::HashMap;
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
    /// `true` → emit as `Action::FanoutBatch` (1 entry per msg, per-conn,
    /// `consumer_id = 0`, client demuxes via local SubjectTrie).
    /// `false` → emit as `Action::RepBatch` (1 entry per (msg × consumer),
    /// `consumer_id` populated, client routes by consumer_id).
    /// Wire body layout is identical for both — only the envelope action
    /// code differs at flush time.
    is_fanout: bool,
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
            is_fanout: false,
        }
    }

    fn activate(
        &mut self,
        conn: ConnectionId,
        stream: StreamId,
        first_seq: u64,
        is_fanout: bool,
    ) {
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
        self.is_fanout = is_fanout;
    }

    fn release(&mut self) {
        self.body.clear();
        self.count = 0;
        self.in_use = false;
        self.is_fanout = false;
    }

    #[inline]
    fn push_entry_bytes(
        &mut self,
        consumer_id: u32,
        seq: u64,
        subject_hash: u32,
        subject: &[u8],
        reply_to: &[u8],
        payload: &[u8],
    ) {
        let subj_len = subject.len();
        let reply_len = reply_to.len();
        let data_len = subj_len + reply_len + payload.len();
        let total = DELIVERY_ENTRY_HEADER_SIZE + data_len;

        let header = DeliveryEntryHeader {
            consumer_id: U32::new(consumer_id),
            seq: U64::new(seq),
            subj_len: U16::new(subj_len as u16),
            reply_len: U16::new(reply_len as u16),
            data_len: U32::new(data_len as u32),
            subject_hash: U32::new(subject_hash),
        };

        // Reserve once, then extend directly into `body` — no intermediate
        // 4 KB stack scratch. The previous "fast path" (scratch buffer)
        // copied subject+payload through stack (arena → scratch → body,
        // two memcpys per data byte). Direct extend does one memcpy per
        // source slice, half the memory bandwidth for the same work.
        // Measured impact: +10% throughput, 5× less run-to-run variance
        // (the 4 KB stack scratch caused per-call zero-init spikes on
        // cold cache lines).
        self.body.reserve(total);
        self.body.extend_from_slice(header.as_bytes());
        self.body.extend_from_slice(subject);
        self.body.extend_from_slice(reply_to);
        self.body.extend_from_slice(payload);

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
    /// Ordered list of bucket indices active in the current cycle.
    /// Preserves first-touch order so `for_each` emits frames deterministically.
    active: Vec<usize>,
    /// `(conn_raw, stream_raw, is_fanout) -> bucket_idx` — O(1) bucket
    /// lookup during `add`/`add_fanout`. `is_fanout` is part of the key so
    /// a single (conn, stream) can simultaneously hold a per-consumer
    /// `RepBatch` bucket AND a `FanoutBatch` bucket: the dispatcher emits
    /// ack-mode consumers as `RepBatch` entries and fire-and-forget
    /// consumers as one collapsed `FanoutBatch` entry.
    ///
    /// Replaces the previous linear-scan `Vec<(u64, u32, bool, usize)>`.
    /// Bench (`drain_full_scenario`) showed +51% throughput at 64 active
    /// buckets × ~668 emits/cycle — linear scan was the dominant cost in
    /// acquire_bucket. foldhash `FixedState` per `.agent/rules/performance.md`.
    index: HashMap<(u64, u32, bool), usize, foldhash::fast::FixedState>,
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
            index: HashMap::with_capacity_and_hasher(
                16,
                foldhash::fast::FixedState::default(),
            ),
        }
    }

    /// Reset for a new cycle. Does not deallocate — `BytesMut` capacity
    /// stays resident for reuse.
    pub fn clear(&mut self) {
        for b in &mut self.buckets {
            if b.in_use { b.release(); }
        }
        self.active.clear();
        self.index.clear();
    }

    /// Append a per-consumer entry. Routes to the `RepBatch` bucket for
    /// `(conn, stream)`. Each call adds one wire entry with the supplied
    /// `consumer_id`; the client routes by consumer.
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
        reply_to: &[u8],
        payload: &[u8],
    ) {
        let idx = self.acquire_bucket(conn, stream, seq, /* is_fanout */ false);
        self.buckets[idx].push_entry_bytes(consumer.0, seq, subject_hash, subject, reply_to, payload);
    }

    /// Append a broadcast entry. Routes to the `FanoutBatch` bucket for
    /// `(conn, stream)`. Each call adds **one** wire entry per message
    /// regardless of how many consumers on this connection match — the
    /// client demultiplexes locally via its SubjectTrie.
    ///
    /// Wire `consumer_id` is hard-zero (broadcast marker). Caller must
    /// only invoke this when ALL matched consumers on `(conn, stream)`
    /// for this entry are fire-and-forget; otherwise per-consumer ack
    /// tracking breaks. The dispatcher in `drain.rs` enforces this.
    #[inline]
    pub fn add_fanout(
        &mut self,
        conn: ConnectionId,
        stream: StreamId,
        seq: u64,
        subject: &[u8],
        subject_hash: u32,
        reply_to: &[u8],
        payload: &[u8],
    ) {
        let idx = self.acquire_bucket(conn, stream, seq, /* is_fanout */ true);
        self.buckets[idx].push_entry_bytes(0, seq, subject_hash, subject, reply_to, payload);
    }

    /// Iterate each active bucket, patch envelope + count, hand the
    /// prepared frame to `flush`. Bucket is released after the callback
    /// returns, regardless of its result.
    pub fn for_each<F>(&mut self, names: &Arc<NameRegistry>, mut flush: F)
    where
        F: FnMut(Frame) -> bool,
    {
        for &idx in self.active.iter() {
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
            let action = if b.is_fanout {
                Action::FanoutBatch
            } else {
                Action::RepBatch
            };
            let envelope = Envelope::new(action, wire_stream_id, body_len as u32, 0);
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
        self.index.clear();
    }

    #[inline]
    fn acquire_bucket(
        &mut self,
        conn: ConnectionId,
        stream: StreamId,
        first_seq: u64,
        is_fanout: bool,
    ) -> usize {
        let key = (conn.0, stream.raw(), is_fanout);
        if let Some(&idx) = self.index.get(&key) {
            return idx;
        }
        let idx = match self.buckets.iter().position(|b| !b.in_use) {
            Some(i) => i,
            None => {
                self.buckets.push(Bucket::new_blank());
                self.buckets.len() - 1
            }
        };
        self.buckets[idx].activate(conn, stream, first_seq, is_fanout);
        self.active.push(idx);
        self.index.insert(key, idx);
        idx
    }

    // ── Test helpers ─────────────────────────────────────────────────────

    #[cfg(test)]
    pub(crate) fn active_count(&self) -> usize { self.active.len() }

    /// Count of entries in the per-consumer (`RepBatch`) bucket for
    /// `(conn, stream)`. `None` if no such bucket is active this cycle.
    #[cfg(test)]
    pub(crate) fn bucket_count_for(&self, conn: ConnectionId, stream: StreamId) -> Option<u16> {
        self.index
            .get(&(conn.0, stream.raw(), false))
            .map(|&idx| self.buckets[idx].count)
    }

    /// Count of entries in the fanout (`FanoutBatch`) bucket for
    /// `(conn, stream)`. `None` if no fanout bucket is active this cycle.
    #[cfg(test)]
    pub(crate) fn fanout_bucket_count_for(
        &self,
        conn: ConnectionId,
        stream: StreamId,
    ) -> Option<u16> {
        self.index
            .get(&(conn.0, stream.raw(), true))
            .map(|&idx| self.buckets[idx].count)
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
                    seq, SUBJECT, 0xDEAD, &[], PAYLOAD);
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
                        seq, SUBJECT, 0xDEAD, &[], PAYLOAD);
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
        acc.add(ConnectionId(100), StreamId(1), ConsumerId(0), 1, SUBJECT, 0xDEAD, &[], PAYLOAD);
        acc.add(ConnectionId(100), StreamId(2), ConsumerId(0), 1, SUBJECT, 0xDEAD, &[], PAYLOAD);
        assert_eq!(acc.active_count(), 2);
    }

    #[test]
    fn clear_reuses_pool() {
        let mut acc = Accumulator::new();
        for _ in 0..3 {
            acc.clear();
            for conn in 1u64..=4 {
                acc.add(ConnectionId(conn), StreamId(1), ConsumerId(0),
                        1, SUBJECT, 0xDEAD, &[], PAYLOAD);
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
                        seq, SUBJECT, 0xDEAD, &[], PAYLOAD);
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
                42, SUBJECT, 0xBEEF, &[], PAYLOAD);

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
                ConsumerId(0),            // broadcast (consumer_id=0 in RepBatch)
                1, SUBJECT, 0xDEAD, &[], PAYLOAD);
        assert_eq!(acc.bucket_count_for(ConnectionId(100), StreamId(1)), Some(1));
    }

    // ── FanoutBatch (per-bucket action) ───────────────────────────────────

    /// `add_fanout` produces a separate bucket from `add` even on the
    /// same `(conn, stream)` pair. Per-consumer ack-mode entries can
    /// coexist with a single fire-and-forget broadcast entry.
    #[test]
    fn add_fanout_uses_separate_bucket_from_add() {
        let mut acc = Accumulator::new();
        acc.clear();

        // 2 ack-mode consumers on this conn
        acc.add(ConnectionId(100), StreamId(1), ConsumerId(7),
                1, SUBJECT, 0xDEAD, &[], PAYLOAD);
        acc.add(ConnectionId(100), StreamId(1), ConsumerId(8),
                1, SUBJECT, 0xDEAD, &[], PAYLOAD);

        // 1 broadcast entry on the same conn (collapsed fire-and-forget)
        acc.add_fanout(ConnectionId(100), StreamId(1),
                       1, SUBJECT, 0xDEAD, &[], PAYLOAD);

        assert_eq!(acc.active_count(), 2, "RepBatch + FanoutBatch buckets");
        assert_eq!(acc.bucket_count_for(ConnectionId(100), StreamId(1)), Some(2));
        assert_eq!(acc.fanout_bucket_count_for(ConnectionId(100), StreamId(1)), Some(1));
    }

    /// Multiple `add_fanout` calls with the same `(conn, stream)` collapse
    /// into one bucket — exactly the 1-entry-per-msg invariant.
    #[test]
    fn add_fanout_same_conn_stream_collapses_to_one_bucket() {
        let mut acc = Accumulator::new();
        acc.clear();
        for seq in 1..=10u64 {
            acc.add_fanout(ConnectionId(100), StreamId(1), seq, SUBJECT, 0xDEAD, &[], PAYLOAD);
        }
        assert_eq!(acc.active_count(), 1);
        assert_eq!(acc.fanout_bucket_count_for(ConnectionId(100), StreamId(1)), Some(10));
    }

    /// Fanout bucket flushes with `Action::FanoutBatch` (`0x0207`) in
    /// the envelope; per-consumer bucket flushes with `Action::RepBatch`
    /// (`0x0205`). Confirms the per-bucket action selection in
    /// `for_each`.
    #[test]
    fn for_each_emits_correct_action_per_bucket_kind() {
        use arbitro_proto::wire::envelope::Envelope;
        use zerocopy::FromBytes;

        let mut acc = Accumulator::new();
        acc.clear();
        acc.add(ConnectionId(100), StreamId(1), ConsumerId(7),
                1, SUBJECT, 0xDEAD, &[], PAYLOAD);
        acc.add_fanout(ConnectionId(100), StreamId(1),
                       2, SUBJECT, 0xBEEF, &[], PAYLOAD);

        let mut actions: Vec<u16> = Vec::new();
        acc.for_each(&names(), |frame| {
            let env = Envelope::ref_from_bytes(&frame.bytes[..ENVELOPE_SIZE]).unwrap();
            actions.push(env.action.get());
            true
        });
        actions.sort_unstable();
        assert_eq!(
            actions,
            vec![Action::RepBatch.as_u16(), Action::FanoutBatch.as_u16()],
            "one frame per bucket kind, with the right action code each",
        );
    }

    /// Wire `consumer_id` is forced to `0` for fanout entries —
    /// regardless of which consumer ids matched on the server side.
    /// Client uses `0` as the broadcast marker that triggers local
    /// SubjectTrie demultiplexing.
    #[test]
    fn fanout_entries_carry_consumer_id_zero() {
        use arbitro_proto::wire::delivery::RepBatchView;

        let mut acc = Accumulator::new();
        acc.clear();
        acc.add_fanout(ConnectionId(100), StreamId(1),
                       42, SUBJECT, 0xBEEF, &[], PAYLOAD);

        let mut saw = false;
        acc.for_each(&names(), |frame| {
            // Skip the envelope; RepBatchView parses `[count][pad] entries…`
            let body = &frame.bytes[ENVELOPE_SIZE..];
            let view = RepBatchView::new(body);
            for e in view.entries() {
                assert_eq!(e.consumer_id, 0, "fanout marker on the wire");
                assert_eq!(e.seq, 42);
                saw = true;
            }
            true
        });
        assert!(saw);
    }
}
