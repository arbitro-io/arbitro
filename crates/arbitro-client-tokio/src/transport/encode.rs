//! Wire encoders for outbound v2 frames.
//!
//! Manager + publish encoders are pure shells around
//! `arbitro_proto::v2::*::encode_into` helpers. They were previously
//! re-exported from the legacy `arbitro-client` crate; now lifted in
//! place so this crate stands alone. When `arbitro-proto` exposes these
//! as inherent `Frame` methods the file can shrink again.

use bytes::Bytes;
use zerocopy::IntoBytes;

use arbitro_proto::v2::ingress::ack_frame::{AckFrame, BatchAckFrame};
use arbitro_proto::v2::ingress::nack_frame::{NackFrame, BatchNackFrame};
use arbitro_proto::v2::ingress::hello::{HelloFrame, Role};
use arbitro_proto::v2::ingress::pub_with_reply::PubWithReplyFrame;
use arbitro_proto::v2::ingress::sub_frame::SubFrame;
use arbitro_proto::v2::ingress::{
    BATCH_PUB_ENTRY_HEADER_SIZE, BatchPubFrame,
};
use arbitro_proto::v2::manager::{
    CreateConsumerFrame, CreateStreamFrame, DeleteConsumerFrame, DeleteStreamFrame,
    DrainSubjectFrame, GetConsumerFrame, GetStreamFrame, ListConsumersFrame, ListStreamsFrame,
    PurgeStreamFrame,
};

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

    BatchPubFrame::encode_into_iter(
        &mut buf, seq, stream_id, 0, entry_flags,
        entries.len() as u32, tail_bytes,
        entries.iter().map(|e| (e.subject, e.payload.as_ref())),
    );
    Bytes::from(buf)
}

/// Build a v2 PUB-WITH-REPLY frame in one contiguous `Bytes`.
pub(crate) fn encode_pub_with_reply_v2(
    seq: u64,
    stream_id: u32,
    subject: &[u8],
    reply_to: &[u8],
    payload: &[u8],
) -> Bytes {
    let size = PubWithReplyFrame::wire_size(subject.len(), reply_to.len(), payload.len());
    let mut buf = vec![0u8; size];
    PubWithReplyFrame::encode_into(&mut buf, seq, stream_id, 0, 0, subject, reply_to, payload);
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

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arbitro_proto::v2::ingress::pub_frame::PubFrame;
    use arbitro_proto::action::Action;

    /// Encode a single-pub frame; verify wire size and action byte.
    #[test]
    fn pub_single_v2_roundtrip() {
        let subject = b"orders.created";
        let payload = b"hello world";
        let expected_size = PubFrame::wire_size(subject.len(), payload.len());

        let mut data = vec![0u8; expected_size];
        PubFrame::encode_into(&mut data, 1, 42, 0, 0, subject, payload);

        // Action bytes [0..2] must be Publish = 0x0101 (little-endian)
        assert_eq!(u16::from_le_bytes([data[0], data[1]]), Action::Publish.as_u16());
        // frame length must match wire_size
        assert_eq!(data.len(), expected_size);
        // payload appears at the end
        let payload_off = data.len() - payload.len();
        assert_eq!(&data[payload_off..], payload);
    }

    /// Encode a batch-pub frame with 3 entries; verify total size and action.
    #[test]
    fn pub_batch_v2_roundtrip() {
        let entries = vec![
            BatchEntry::new(b"a.b", Bytes::from_static(b"p1")),
            BatchEntry::new(b"c.d", Bytes::from_static(b"payload2")),
            BatchEntry::new(b"e.f.g", Bytes::from_static(b"x")),
        ];
        let frame = encode_pub_batch_v2(7, 1, 0, &entries);

        // Action = PublishBatch = 0x0103
        assert_eq!(
            u16::from_le_bytes([frame[0], frame[1]]),
            Action::PublishBatch.as_u16()
        );
        // Frame must not be empty and must contain all payloads
        assert!(frame.len() > 32);
        assert!(frame.windows(2).any(|w| w == b"p1"));
        assert!(frame.windows(8).any(|w| w == b"payload2"));
    }
}

/// Single nack — same wire layout as `encode_ack_v2`, action = Nack.
#[inline]
pub(crate) fn encode_nack_v2(seq: u64, consumer_id: u32, nack_seq: u64, subject_hash: u32) -> Bytes {
    let f = NackFrame::new(seq, consumer_id, nack_seq, subject_hash);
    Bytes::copy_from_slice(zerocopy::IntoBytes::as_bytes(&f))
}

/// Batch nack — entries are `(seq, subject_hash, delay_ms)`.
pub(crate) fn encode_batch_nack_v2(
    seq: u64,
    consumer_id: u32,
    entries: &[(u64, u32, u32)],
) -> Bytes {
    let size = BatchNackFrame::wire_size(entries.len());
    let mut buf = vec![0u8; size];
    BatchNackFrame::encode_into(&mut buf, seq, consumer_id, entries);
    Bytes::from(buf)
}

/// Unsubscribe frame — same body shape as SubFrame, action = Unsubscribe,
/// filter is empty. Server routes by `consumer_id` only. Total: 28B (≤ INLINE_CAP).
#[inline]
pub(crate) fn encode_unsub_v2(seq: u64, consumer_id: u32) -> Bytes {
    // Wire layout: [Header 16B][SubBody 12B] — filter_len = 0, no tail bytes.
    let size = SubFrame::wire_size(0);
    let mut buf = vec![0u8; size];
    // Re-use SubFrame::encode_into which writes Action::Subscribe, then patch action.
    SubFrame::encode_into(&mut buf, seq, 0, consumer_id, 0, b"");
    // Patch the action bytes (LE u16 at offset 0) to Unsubscribe = 0x0302.
    let action = arbitro_proto::action::Action::Unsubscribe.as_u16();
    buf[0..2].copy_from_slice(&action.to_le_bytes());
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
