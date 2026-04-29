//! Wire encoders for outbound v2 frames.
//!
//! Manager + publish encoders are pure shells around
//! `arbitro_proto::v2::*::encode_into` helpers. They were previously
//! re-exported from the legacy `arbitro-client` crate; now lifted in
//! place so this crate stands alone. When `arbitro-proto` exposes these
//! as inherent `Frame` methods the file can shrink again.

use bytes::Bytes;
use zerocopy::IntoBytes;

use arbitro_proto::v2::header::{Header, HEADER_SIZE};
use arbitro_proto::v2::ingress::ack_frame::{AckFrame, BatchAckFrame};
use arbitro_proto::v2::ingress::hello::{HelloFrame, Role};
use arbitro_proto::v2::ingress::sub_frame::SubFrame;
use arbitro_proto::v2::ingress::{
    BATCH_PUB_ENTRY_HEADER_SIZE, BatchPubFrame, PUB_BODY_FIXED, PubBody,
};
use arbitro_proto::v2::manager::{
    CreateConsumerFrame, CreateStreamFrame, DeleteConsumerFrame, DeleteStreamFrame,
    DrainSubjectFrame, GetConsumerFrame, GetStreamFrame, ListConsumersFrame, ListStreamsFrame,
    PurgeStreamFrame,
};
use arbitro_proto::action::Action;

use zerocopy::byteorder::little_endian::{U16, U32};

// ─── BatchEntry ───────────────────────────────────────────────────────

/// One entry of a batch publish: a borrowed subject plus an owned
/// payload. The payload is `Bytes` so it can travel as a separate
/// iovec without an extra userspace copy.
#[derive(Debug, Clone)]
pub struct BatchEntry<'a> {
    pub subject: &'a [u8],
    pub payload: Bytes,
}

impl<'a> BatchEntry<'a> {
    /// Convenience constructor.
    #[inline]
    pub fn new(subject: &'a [u8], payload: Bytes) -> Self {
        Self { subject, payload }
    }
}

// ─── v2 publish encoders ──────────────────────────────────────────────

/// Stack-only prefix for a v2 single PUB frame: `[Header 16B][PubBody 8B] = 24B`.
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

/// Build a v2 PUB frame as 3 iovecs: `[prefix 24B][subject][payload]`.
///
/// Zero payload memcpy: the caller's `Bytes` is cloned (Arc bump only)
/// and shipped as a separate iovec. Subject is small metadata copied
/// once. Prefix is a 24B stack struct — one tiny `Bytes::copy_from_slice`
/// for channel ownership.
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

/// Build a v2 BATCH-PUB frame in one contiguous `Bytes` (single iovec).
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

// ─── v2 manager encoders ──────────────────────────────────────────────

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

/// DeleteStream request frame.
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

// ─── Tokio-only frames (no legacy counterpart) ────────────────────────

#[inline]
pub(crate) fn encode_hello_v2(role: Role, caps: u16) -> Bytes {
    let f = HelloFrame::new(role, caps);
    Bytes::copy_from_slice(f.as_bytes())
}

#[inline]
pub(crate) fn encode_ack_v2(seq: u64, consumer_id: u32, ack_seq: u64, subject_hash: u32) -> Bytes {
    let f = AckFrame::new(seq, consumer_id, ack_seq, subject_hash);
    Bytes::copy_from_slice(f.as_bytes())
}

#[inline]
pub(crate) fn encode_batch_ack_v2(
    seq: u64,
    consumer_id: u32,
    entries: &[(u64, u32)],
) -> Bytes {
    let size = BatchAckFrame::wire_size(entries.len());
    let mut buf = vec![0u8; size];
    BatchAckFrame::encode_into(&mut buf, seq, consumer_id, entries);
    Bytes::from(buf)
}

#[inline]
pub(crate) fn encode_sub_v2(
    seq: u64,
    conn_id: u32,
    consumer_id: u32,
    options_flags: u16,
    filter: &[u8],
) -> Bytes {
    let size = SubFrame::wire_size(filter.len());
    let mut buf = vec![0u8; size];
    SubFrame::encode_into(&mut buf, seq, conn_id, consumer_id, options_flags, filter);
    Bytes::from(buf)
}
