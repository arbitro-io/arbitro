//! Wire encoders for outbound v2 frames.
//!
//! Manager + publish encoders are pure shells around
//! `arbitro_proto::v2::*::encode_into` helpers. They were previously
//! re-exported from the legacy `arbitro-client` crate; now lifted in
//! place so this crate stands alone. When `arbitro-proto` exposes these
//! as inherent `Frame` methods the file can shrink again.

use bytes::Bytes;
use zerocopy::IntoBytes;

use arbitro_proto::v2::cold::{
    ColdBody, ConsumerStats, CreateConsumer as CreateConsumerCold,
    CreateStream as CreateStreamCold, DeleteConsumer, DeleteMessage, DeleteStream, DrainSubject,
    GetConsumer, GetStream, ListConsumers, ListStreams, PurgeStream,
    SubjectLimit as ColdSubjectLimit, Unsubscribe,
};
use arbitro_proto::v2::ingress::ack_frame::{AckFrame, BatchAckFrame};
use arbitro_proto::v2::ingress::hello::{HelloFrame, Role};
use arbitro_proto::v2::ingress::nack_frame::{BatchNackFrame, NackFrame};
use arbitro_proto::v2::ingress::pub_with_reply::PubWithReplyFrame;
use arbitro_proto::v2::ingress::{BatchPubFrame, BATCH_PUB_ENTRY_HEADER_SIZE};
use arbitro_proto::v2::manager::SubjectLimit;

// ─── BatchEntry ───────────────────────────────────────────────────────

/// One entry of a batch publish: a borrowed subject plus an owned
/// payload, with an optional per-entry `msg_id` for broker-side
/// idempotency dedup. The payload is `Bytes` so it can travel as a
/// separate iovec without an extra userspace copy.
///
/// `msg_id` is opaque — the broker treats it as a hash key for the
/// stream's dedup window. Empty `msg_id` means "no dedup for this
/// entry" (mixing dedup + non-dedup entries in the same batch is
/// allowed).
#[derive(Debug, Clone)]
pub struct BatchEntry<'a> {
    pub subject: &'a [u8],
    pub msg_id: &'a [u8],
    pub payload: Bytes,
}

impl<'a> BatchEntry<'a> {
    /// Convenience constructor — no msg_id, legacy / non-dedup entry.
    #[inline]
    pub fn new(subject: &'a [u8], payload: Bytes) -> Self {
        Self {
            subject,
            msg_id: &[],
            payload,
        }
    }

    /// Constructor with an explicit `msg_id` for dedup-enabled streams.
    #[inline]
    pub fn with_msg_id(subject: &'a [u8], msg_id: &'a [u8], payload: Bytes) -> Self {
        Self {
            subject,
            msg_id,
            payload,
        }
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
        tail_bytes +=
            BATCH_PUB_ENTRY_HEADER_SIZE + e.subject.len() + e.msg_id.len() + e.payload.len();
    }
    let size = BatchPubFrame::wire_size(tail_bytes);
    let mut buf = vec![0u8; size];

    BatchPubFrame::encode_into_iter(
        &mut buf,
        seq,
        stream_id,
        0,
        entry_flags,
        entries.len() as u32,
        tail_bytes,
        entries
            .iter()
            .map(|e| (e.subject, e.msg_id, e.payload.as_ref())),
    );
    Bytes::from(buf)
}

/// Build a v2 PUB-WITH-REPLY frame in one contiguous `Bytes`.
///
/// M10: `msg_id` is an optional idempotency token. Empty = no dedup (legacy
/// behaviour). When non-empty the broker routes the publish through the
/// per-stream `IdempotencyTracker`.
pub(crate) fn encode_pub_with_reply_v2(
    seq: u64,
    stream_id: u32,
    subject: &[u8],
    reply_to: &[u8],
    msg_id: &[u8],
    payload: &[u8],
) -> Bytes {
    let size =
        PubWithReplyFrame::wire_size(subject.len(), reply_to.len(), msg_id.len(), payload.len());
    let mut buf = vec![0u8; size];
    PubWithReplyFrame::encode_into(
        &mut buf, seq, stream_id, 0, 0, subject, reply_to, msg_id, payload,
    );
    Bytes::from(buf)
}

/// Build a v2 PUB-DELAYED frame in one contiguous `Bytes`.
pub(crate) fn encode_pub_delayed_v2(
    seq: u64,
    stream_id: u32,
    subject: &[u8],
    payload: &[u8],
    delay_ms: u64,
) -> Bytes {
    use arbitro_proto::v2::ingress::pub_delayed_frame::PubDelayedFrame;
    let size = PubDelayedFrame::wire_size(subject.len(), 0, payload.len());
    let mut buf = vec![0u8; size];
    PubDelayedFrame::encode_into(
        &mut buf,
        seq,
        stream_id,
        0,
        0,
        subject,
        &[],
        payload,
        delay_ms,
    );
    Bytes::from(buf)
}

// ─── v2 manager encoders ──────────────────────────────────────────────

/// CreateStream request frame.
///
/// `idempotency_window_ms = 0` disables idempotency for the stream
/// (the default, matching pre-feature behaviour). A non-zero value
/// turns on per-stream dedup: duplicate `msg_id` publishes within the
/// window are rejected with `ErrorCode::IdempotencyDuplicate (203)`.
#[allow(clippy::too_many_arguments)]
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
    idempotency_window_ms: u32,
) -> Bytes {
    CreateStreamCold {
        name: name.to_vec(),
        filter: filter.to_vec(),
        max_msgs,
        max_bytes,
        max_age_secs,
        replicas,
        journal_kind,
        retention,
        discard,
        idempotency_window_ms,
    }
    .encode(seq)
}

/// DeleteStream / GetStream / PurgeStream / DrainSubject — cold-path
/// frames migrated to `v2::cold`. These thin shims keep the caller
/// API unchanged while the bodies now ride as JSON.
pub(crate) fn encode_delete_stream_v2(seq: u64, name: &[u8]) -> Bytes {
    DeleteStream {
        name: name.to_vec(),
    }
    .encode(seq)
}

pub(crate) fn encode_get_stream_v2(seq: u64, name: &[u8]) -> Bytes {
    GetStream {
        name: name.to_vec(),
    }
    .encode(seq)
}

pub(crate) fn encode_purge_stream_v2(seq: u64, name: &[u8]) -> Bytes {
    PurgeStream {
        name: name.to_vec(),
    }
    .encode(seq)
}

pub(crate) fn encode_drain_subject_v2(seq: u64, name: &[u8], subject: &[u8]) -> Bytes {
    DrainSubject {
        name: name.to_vec(),
        subject: subject.to_vec(),
    }
    .encode(seq)
}

/// DeleteMessage request frame — tombstone a single message by sequence.
pub(crate) fn encode_delete_message_v2(seq: u64, name: &[u8], msg_seq: u64) -> Bytes {
    DeleteMessage {
        name: name.to_vec(),
        seq: msg_seq,
    }
    .encode(seq)
}

/// ListStreams request frame — cold path (v2::cold).
pub(crate) fn encode_list_streams_v2(seq: u64, offset: u32, limit: u32) -> Bytes {
    ListStreams { offset, limit }.encode(seq)
}

/// CreateConsumer request frame.
///
/// `subject_limits` is an optional list of `(pattern, max_inflight)` pairs.
/// Per-subject limits are enforced by the server only with `ack_policy ==
/// Explicit`; the server silently drops them otherwise.
#[allow(clippy::too_many_arguments)]
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
    subject_limits: &[SubjectLimit<'_>],
) -> Bytes {
    let owned_limits: Vec<ColdSubjectLimit> = subject_limits
        .iter()
        .map(|s| ColdSubjectLimit {
            pattern: s.pattern.to_vec(),
            limit: s.limit,
        })
        .collect();
    CreateConsumerCold {
        stream_id,
        name: name.to_vec(),
        group: group.to_vec(),
        subject: subject.to_vec(),
        max_inflight,
        ack_policy,
        deliver_policy,
        deliver_mode,
        ack_wait_ms,
        start_seq,
        subject_limits: owned_limits,
        max_nack: None,
    }
    .encode(seq)
}

/// DeleteConsumer — cold-path frame (v2::cold).
pub(crate) fn encode_delete_consumer_v2(seq: u64, consumer_id: u32) -> Bytes {
    DeleteConsumer { consumer_id }.encode(seq)
}

/// ConsumerStats request frame — cold path (v2::cold).
pub(crate) fn encode_consumer_stats_v2(seq: u64, consumer_id: u32) -> Bytes {
    ConsumerStats { consumer_id }.encode(seq)
}

/// GetConsumer — cold-path frame (v2::cold).
pub(crate) fn encode_get_consumer_v2(seq: u64, stream_id: u32, name: &[u8]) -> Bytes {
    GetConsumer {
        stream_id,
        name: name.to_vec(),
    }
    .encode(seq)
}

/// ListConsumers request frame — cold path (v2::cold).
pub(crate) fn encode_list_consumers_v2(seq: u64, stream_id: u32, offset: u32, limit: u32) -> Bytes {
    ListConsumers {
        stream_id,
        offset,
        limit,
    }
    .encode(seq)
}

// ─── Tokio-only frames (no legacy counterpart) ────────────────────────

#[inline]
pub(crate) fn encode_hello_v2(role: Role) -> Bytes {
    let f = HelloFrame::new(role);
    Bytes::copy_from_slice(f.as_bytes())
}

#[inline]
pub(crate) fn encode_ack_v2(seq: u64, consumer_id: u32, ack_seq: u64, subject_hash: u32) -> Bytes {
    let f = AckFrame::new(seq, consumer_id, ack_seq, subject_hash);
    Bytes::copy_from_slice(f.as_bytes())
}

#[inline]
pub(crate) fn encode_batch_ack_v2(seq: u64, consumer_id: u32, entries: &[(u64, u32)]) -> Bytes {
    let size = BatchAckFrame::wire_size(entries.len());
    let mut buf = vec![0u8; size];
    BatchAckFrame::encode_into(&mut buf, seq, consumer_id, entries);
    Bytes::from(buf)
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arbitro_proto::action::Action;
    use arbitro_proto::v2::ingress::pub_frame::PubFrame;

    /// Encode a single-pub frame; verify wire size and action byte.
    #[test]
    fn pub_single_v2_roundtrip() {
        let subject = b"orders.created";
        let payload = b"hello world";
        let expected_size = PubFrame::wire_size(subject.len(), 0, payload.len());

        let mut data = vec![0u8; expected_size];
        PubFrame::encode_into(&mut data, 1, 42, 0, 0, subject, &[], payload);

        // Action bytes [0..2] must be Publish = 0x0101 (little-endian)
        assert_eq!(
            u16::from_le_bytes([data[0], data[1]]),
            Action::Publish.as_u16()
        );
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
pub(crate) fn encode_nack_v2(
    seq: u64,
    consumer_id: u32,
    nack_seq: u64,
    subject_hash: u32,
) -> Bytes {
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
    Unsubscribe { consumer_id }.encode(seq)
}

#[inline]
pub(crate) fn encode_sub_v2(
    seq: u64,
    _conn_id: u32,
    consumer_id: u32,
    _options_flags: u16,
    filter: &[u8],
) -> Bytes {
    use arbitro_proto::v2::cold::Subscribe as SubscribeCold;
    // Legacy callers pass a single filter; future multi-filter users
    // will call a `_with_filters` variant. `subscription_id == 0`
    // selects the legacy "subscription_id == consumer_id" path on the
    // server.
    let filters = if filter.is_empty() {
        Vec::new()
    } else {
        vec![filter.to_vec()]
    };
    SubscribeCold {
        consumer_id,
        subscription_id: 0,
        filters,
    }
    .encode(seq)
}
