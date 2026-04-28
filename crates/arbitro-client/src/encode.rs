//! Wire encoding helpers for outbound publish frames.
//!
//! The publish hot path uses `encode_publish_split`: it builds an
//! **interleaved iovec list** matching the broker's wire layout
//! `[envelope][count] [hdr_1][subj_1][pay_1] … [hdr_N][subj_N][pay_N]`.
//! Slot 0 holds the envelope+count+first-entry-meta, slot 1 holds the
//! first payload (Arc-shared), slot 2 holds the second entry meta,
//! slot 3 holds the second payload, etc. The write loop ships the
//! whole list to the kernel via `write_vectored`, so no payload bytes
//! are ever copied in userspace past the API boundary.

use bytes::Bytes;
use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::IntoBytes;

use arbitro_proto::action::Action;
use arbitro_proto::v2::header::{Header, HEADER_SIZE};
use arbitro_proto::v2::ingress::{
    BATCH_PUB_ENTRY_HEADER_SIZE, BatchPubFrame, PUB_BODY_FIXED, PubBody,
};
use arbitro_proto::v2::manager::{
    CreateConsumerFrame, CreateStreamFrame, DeleteConsumerFrame, DeleteStreamFrame,
    DrainSubjectFrame, GetConsumerFrame, GetStreamFrame, ListConsumersFrame, ListStreamsFrame,
    PurgeStreamFrame,
};
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_proto::wire::publish::PublishEntry;

use crate::client::BatchEntry;

// ──────────────────────────────────────────────────────────────────────
// v2 encoders — pure zerocopy, no payload memcpy.
// ──────────────────────────────────────────────────────────────────────

/// Stack-only prefix for a v2 single PUB frame: `[Header 16B][PubBody 8B] = 24B`.
///
/// Built as ONE struct literal, emitted via ONE `as_bytes()`. The `Bytes`
/// returned by [`encode_pub_v2`] copies this 24B prefix once (unavoidable
/// — it must be owned to ship through the channel) but never touches the
/// payload. Subject is small metadata (≤256B typical); payload is the
/// caller's `Bytes` cloned via Arc bump.
#[derive(
    Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::Immutable,
    zerocopy::KnownLayout, zerocopy::Unaligned,
)]
#[repr(C)]
struct PubV2Prefix {
    header: Header,
    body:   PubBody,
}
const _: () = assert!(core::mem::size_of::<PubV2Prefix>() == HEADER_SIZE + PUB_BODY_FIXED);
const _: () = assert!(core::mem::size_of::<PubV2Prefix>() == 24);

/// Build a v2 PUB frame as **3 iovecs**: `[prefix 24B][subject][payload]`.
///
/// Zero payload memcpy: the caller's `Bytes` is cloned (Arc bump only) and
/// shipped as a separate iovec. Subject is small metadata copied once.
/// Prefix is a 24B stack struct → one tiny `Bytes::copy_from_slice` for
/// channel ownership.
///
/// Concatenation `prefix || subject || payload` is byte-identical to a
/// `PubFrame` parseable by `PubFrame::ref_from_bytes`.
#[inline(always)]
pub(crate) fn encode_pub_v2(
    seq: u64,
    stream_id: u32,
    entry_flags: u8,
    subject: &[u8],
    payload: &Bytes,
) -> (Bytes /* prefix 24B */, Bytes /* subject */, Bytes /* payload */) {
    let msg_len = (PUB_BODY_FIXED + subject.len() + payload.len()) as u32;
    let prefix = PubV2Prefix {
        header: Header::new(Action::Publish.as_u16(), msg_len, seq)
            .with_entry_flags(entry_flags),
        body: PubBody {
            stream_id:   U32::new(stream_id),
            subject_len: U16::new(subject.len() as u16),
            _pad:        U16::new(0),
        },
    };
    (
        Bytes::copy_from_slice(prefix.as_bytes()),
        Bytes::copy_from_slice(subject),
        payload.clone(),
    )
}

/// Build a v2 BATCH-PUB frame in **one contiguous `Bytes`** (single iovec).
///
/// Uses [`BatchPubFrame::encode_into`] (inline canonical encoder from
/// `arbitro-proto`) to write directly into a pre-sized `Vec<u8>`. The
/// payload zero-copy guarantee is sacrificed here on purpose — batch is
/// the throughput path and one syscall beats `1 + 3·N` iovecs (which can
/// overflow `IOV_MAX` for large N). All payloads are memcpy'd once into
/// the buffer; Arc allocations remain alive for the duration of the call.
pub(crate) fn encode_pub_batch_v2(
    seq: u64,
    stream_id: u32,
    entry_flags: u8,
    entries: &[BatchEntry<'_>],
) -> Bytes {
    let mut tail_bytes = 0usize;
    for e in entries {
        tail_bytes += BATCH_PUB_ENTRY_HEADER_SIZE + e.subject.len() + e.payload.len();
    }
    let size = BatchPubFrame::wire_size(tail_bytes);
    let mut buf = vec![0u8; size];

    // Adapt to encode_into's `&[(subject, payload)]`.
    let tuples: Vec<(&[u8], &[u8])> = entries
        .iter()
        .map(|e| (e.subject, &e.payload[..]))
        .collect();

    BatchPubFrame::encode_into(&mut buf, seq, stream_id, 0, entry_flags, &tuples);
    Bytes::from(buf)
}

// ──────────────────────────────────────────────────────────────────────
// v2 manager encoders — cold path, single-iovec `Bytes`.
//
// Each builds one pre-sized buffer via the proto crate's inline
// `encode_into` helpers. No payload to memcpy; just metadata + names.
// ──────────────────────────────────────────────────────────────────────

/// CreateStream request frame.
pub(crate) fn encode_create_stream_v2(
    seq: u64,
    name: &[u8],
    filter: &[u8],
    max_msgs: u64,
    max_bytes: u64,
    max_age_secs: u64,
    replicas: u8,
    journal_kind: u8,
    retention: u8,
    discard: u8,
) -> Bytes {
    let size = CreateStreamFrame::wire_size(name.len(), filter.len());
    let mut buf = vec![0u8; size];
    CreateStreamFrame::encode_into(
        &mut buf, seq, name, filter, max_msgs, max_bytes, max_age_secs, replicas, journal_kind,
        retention, discard,
    );
    Bytes::from(buf)
}

/// DeleteStream request frame (8B body + name tail).
pub(crate) fn encode_delete_stream_v2(seq: u64, name: &[u8]) -> Bytes {
    let size = DeleteStreamFrame::wire_size(name.len());
    let mut buf = vec![0u8; size];
    DeleteStreamFrame::encode_into(&mut buf, seq, name);
    Bytes::from(buf)
}

/// GetStream request frame.
pub(crate) fn encode_get_stream_v2(seq: u64, name: &[u8]) -> Bytes {
    let size = GetStreamFrame::wire_size(name.len());
    let mut buf = vec![0u8; size];
    GetStreamFrame::encode_into(&mut buf, seq, name);
    Bytes::from(buf)
}

/// PurgeStream request frame.
pub(crate) fn encode_purge_stream_v2(seq: u64, name: &[u8]) -> Bytes {
    let size = PurgeStreamFrame::wire_size(name.len());
    let mut buf = vec![0u8; size];
    PurgeStreamFrame::encode_into(&mut buf, seq, name);
    Bytes::from(buf)
}

/// DrainSubject request frame.
pub(crate) fn encode_drain_subject_v2(seq: u64, name: &[u8], subject: &[u8]) -> Bytes {
    let size = DrainSubjectFrame::wire_size(name.len(), subject.len());
    let mut buf = vec![0u8; size];
    DrainSubjectFrame::encode_into(&mut buf, seq, name, subject);
    Bytes::from(buf)
}

/// ListStreams request frame (sized).
pub(crate) fn encode_list_streams_v2(seq: u64, offset: u32, limit: u32) -> Bytes {
    let f = ListStreamsFrame::new(seq, offset, limit);
    Bytes::copy_from_slice(f.as_bytes())
}

/// CreateConsumer request frame.
pub(crate) fn encode_create_consumer_v2(
    seq: u64,
    stream_id: u32,
    name: &[u8],
    group: &[u8],
    subject: &[u8],
    max_inflight: u16,
    ack_policy: u8,
    deliver_policy: u8,
    deliver_mode: u8,
    ack_wait_ms: u32,
    start_seq: u64,
) -> Bytes {
    let size = CreateConsumerFrame::wire_size(name.len(), group.len(), subject.len());
    let mut buf = vec![0u8; size];
    CreateConsumerFrame::encode_into(
        &mut buf, seq, stream_id, name, group, subject, max_inflight, ack_policy, deliver_policy,
        deliver_mode, ack_wait_ms, start_seq,
    );
    Bytes::from(buf)
}

/// DeleteConsumer request frame (sized, 8B body).
pub(crate) fn encode_delete_consumer_v2(seq: u64, consumer_id: u32) -> Bytes {
    let f = DeleteConsumerFrame::new(seq, consumer_id);
    Bytes::copy_from_slice(f.as_bytes())
}

/// GetConsumer request frame.
pub(crate) fn encode_get_consumer_v2(seq: u64, stream_id: u32, name: &[u8]) -> Bytes {
    let size = GetConsumerFrame::wire_size(name.len());
    let mut buf = vec![0u8; size];
    GetConsumerFrame::encode_into(&mut buf, seq, stream_id, name);
    Bytes::from(buf)
}

/// ListConsumers request frame (sized).
pub(crate) fn encode_list_consumers_v2(
    seq: u64,
    stream_id: u32,
    offset: u32,
    limit: u32,
) -> Bytes {
    let f = ListConsumersFrame::new(seq, stream_id, offset, limit);
    Bytes::copy_from_slice(f.as_bytes())
}

// ──────────────────────────────────────────────────────────────────────
// v1 encoder (legacy, retired in Step 5).
// ──────────────────────────────────────────────────────────────────────

/// Build a zero-copy publish frame: an interleaved iovec list.
///
/// Returns `Vec<Bytes>` where:
/// - slot 0 = `[envelope 16][count u32][hdr_1 12][subj_1]` (first-entry
///   meta inlined right after the envelope/count so the wire layout
///   matches the broker's expected `[hdr][subj][pay]` per-entry order),
/// - slot 1 = payload of entry 1 (Arc-shared, never memcpy'd),
/// - slot 2 = `[hdr_2 12][subj_2]`,
/// - slot 3 = payload of entry 2,
/// - …
///
/// The total wire body length (count + per-entry hdr + subj + payload)
/// goes into `Envelope.msg_len` so the broker can dispatch the frame.
///
/// For an empty `entries` slice the result is a single-slot list with
/// just envelope+count.
pub(crate) fn encode_publish_split(
    seq: u32,
    action: Action,
    stream_id: u32,
    entries: &[BatchEntry<'_>],
) -> Vec<Bytes> {
    // Total body bytes the broker will see on the wire (count + per-entry
    // header + per-entry subject + per-entry payload), summed across
    // every entry.
    let body_len: u32 = (4 + entries
        .iter()
        .map(|e| 12 + e.subject.len() + e.payload.len())
        .sum::<usize>()) as u32;

    let envelope = Envelope {
        action: U16::new(action.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(body_len),
        env_seq: U32::new(seq),
    };

    // 1 slot for envelope+count(+first entry meta), then 2 slots per
    // remaining entry (meta + payload). When `entries` is empty we still
    // emit the envelope+count slot.
    let cap = if entries.is_empty() { 1 } else { 2 * entries.len() };
    let mut chunks: Vec<Bytes> = Vec::with_capacity(cap);

    // Slot 0: envelope + count + entry-1 meta (if any).
    let first_meta_len = entries.first().map(|e| 12 + e.subject.len()).unwrap_or(0);
    let mut head = Vec::with_capacity(ENVELOPE_SIZE + 4 + first_meta_len);
    head.extend_from_slice(envelope.as_bytes());
    head.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    if let Some(first) = entries.first() {
        let hdr = PublishEntry {
            data_len: U32::new(first.payload.len() as u32),
            subj_len: U16::new(first.subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0u8; 3],
        };
        head.extend_from_slice(hdr.as_bytes());
        head.extend_from_slice(first.subject);
    }
    chunks.push(Bytes::from(head));

    if let Some(first) = entries.first() {
        chunks.push(first.payload.clone());
    }

    // Remaining entries: each contributes (meta, payload) — two iovec slots.
    for entry in entries.iter().skip(1) {
        let hdr = PublishEntry {
            data_len: U32::new(entry.payload.len() as u32),
            subj_len: U16::new(entry.subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0u8; 3],
        };
        let mut meta = Vec::with_capacity(12 + entry.subject.len());
        meta.extend_from_slice(hdr.as_bytes());
        meta.extend_from_slice(entry.subject);
        chunks.push(Bytes::from(meta));
        chunks.push(entry.payload.clone());
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_publish_split_layout() {
        let entries = vec![
            BatchEntry {
                subject: b"foo",
                payload: Bytes::from_static(b"hello"),
            },
            BatchEntry {
                subject: b"bar.baz",
                payload: Bytes::from_static(b"world!!"),
            },
        ];
        let chunks = encode_publish_split(42, Action::Publish, 0xAABBCCDD, &entries);

        // 1 envelope+count+meta1 slot, then payload1, meta2, payload2 = 4 slots total.
        assert_eq!(chunks.len(), 4);

        let head = &chunks[0];
        // envelope(16) + count(4) + entry1 meta (12 + 3) = 35
        assert_eq!(head.len(), ENVELOPE_SIZE + 4 + 12 + 3);

        // Envelope.env_seq @ offset 12.
        assert_eq!(
            u32::from_le_bytes([head[12], head[13], head[14], head[15]]),
            42
        );
        // count after envelope.
        assert_eq!(
            u32::from_le_bytes([
                head[ENVELOPE_SIZE],
                head[ENVELOPE_SIZE + 1],
                head[ENVELOPE_SIZE + 2],
                head[ENVELOPE_SIZE + 3],
            ]),
            2
        );

        // payload of entry 1 (arc-shared).
        assert_eq!(&chunks[1][..], b"hello");
        // entry-2 meta = 12 + 7
        assert_eq!(chunks[2].len(), 12 + 7);
        // payload of entry 2 (arc-shared).
        assert_eq!(&chunks[3][..], b"world!!");

        // Concatenating all chunks reproduces the contiguous wire body
        // expected by the broker's BatchIter decoder.
        let mut wire = Vec::new();
        for c in &chunks {
            wire.extend_from_slice(c);
        }
        // Skip envelope to get the body.
        let body = &wire[ENVELOPE_SIZE..];
        let count = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
        assert_eq!(count, 2);
    }

    #[test]
    fn encode_publish_split_payload_is_arc_shared() {
        let original = Bytes::from(vec![1u8, 2, 3, 4, 5]);
        let entries = vec![BatchEntry {
            subject: b"s",
            payload: original.clone(),
        }];
        let chunks = encode_publish_split(1, Action::Publish, 0, &entries);
        // Same allocation on both sides — Arc bump, no memcpy.
        // Slot 1 is the payload of entry 1.
        assert_eq!(original.as_ptr(), chunks[1].as_ptr());
    }

    // ── v2 encoder tests ──────────────────────────────────────────────

    use arbitro_proto::v2::ingress::{BatchPubFrame as ProtoBatchPubFrame, PubFrame};
    use zerocopy::FromBytes;

    #[test]
    fn encode_pub_v2_three_iovec_payload_is_arc_shared() {
        let original = Bytes::from(vec![1u8, 2, 3, 4, 5, 6, 7, 8]);
        let (prefix, subject, payload) =
            encode_pub_v2(42, 0xCAFEBABE, 0, b"orders.eu", &original);

        // The payload Bytes shares the same allocation — Arc bump only.
        assert_eq!(payload.as_ptr(), original.as_ptr());
        assert_eq!(payload.len(), original.len());

        // Concatenate the 3 iovecs and parse back as a v2 PubFrame.
        let mut wire = Vec::with_capacity(prefix.len() + subject.len() + payload.len());
        wire.extend_from_slice(&prefix);
        wire.extend_from_slice(&subject);
        wire.extend_from_slice(&payload);
        assert_eq!(prefix.len(), 24);

        let frame = PubFrame::ref_from_bytes(&wire).expect("v2 PubFrame layout");
        assert_eq!(frame.header.seq.get(), 42);
        assert_eq!(frame.header.action.get(), Action::Publish.as_u16());
        assert_eq!(frame.body.stream_id.get(), 0xCAFEBABE);
        assert_eq!(frame.subject(), b"orders.eu");
        assert_eq!(frame.payload(), &[1u8, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn encode_pub_batch_v2_roundtrip_via_proto() {
        let entries = vec![
            BatchEntry {
                subject: b"a.b",
                payload: Bytes::from_static(b"PING"),
            },
            BatchEntry {
                subject: b"orders.eu.42",
                payload: Bytes::from_static(b"hello world"),
            },
            BatchEntry {
                subject: b"x",
                payload: Bytes::from(vec![0xCC; 32]),
            },
        ];
        let wire = encode_pub_batch_v2(99, 0xDEADBEEF, 0, &entries);

        let frame = ProtoBatchPubFrame::ref_from_bytes(&wire).expect("v2 BatchPubFrame layout");
        assert_eq!(frame.header.seq.get(), 99);
        assert_eq!(frame.header.action.get(), Action::PublishBatch.as_u16());
        assert_eq!(frame.body.stream_id.get(), 0xDEADBEEF);
        assert_eq!(frame.count(), 3);

        let collected: Vec<(Vec<u8>, Vec<u8>)> = frame
            .iter()
            .map(|v| (v.subject().to_vec(), v.payload().to_vec()))
            .collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].0, b"a.b");
        assert_eq!(collected[0].1, b"PING");
        assert_eq!(collected[1].0, b"orders.eu.42");
        assert_eq!(collected[2].1, vec![0xCC; 32]);
    }

    // ── v1 legacy tests (kept until Step 5 cleanup) ───────────────────

    #[test]
    fn encode_publish_split_msg_len_includes_payloads() {
        let entries = vec![BatchEntry {
            subject: b"sub",
            payload: Bytes::from_static(b"payload-12bytes"),
        }];
        let chunks = encode_publish_split(1, Action::Publish, 0, &entries);
        let head = &chunks[0];
        // Envelope.msg_len @ offset 8 = 4 (count) + 12 (entry hdr) + 3 (subj) + 15 (payload) = 34
        let msg_len = u32::from_le_bytes([head[8], head[9], head[10], head[11]]);
        assert_eq!(msg_len, 4 + 12 + 3 + 15);
    }
}
