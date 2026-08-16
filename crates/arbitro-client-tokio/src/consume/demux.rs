//! Deliver / batch-deliver frame demux.
//!
//! Called synchronously from the reader task — no allocation on the
//! single-frame path (the subject copy into `Box<[u8]>` is unavoidable).
//!
//! ## Wire formats
//!
//! The server uses two different 16-byte header formats:
//!
//! **RepBatch** (batch delivery from the accumulator) — **Envelope** format:
//! ```text
//! [16B Envelope]   action | flags | rsv | stream_id(4B) | msg_len(4B) | env_seq(4B)
//! [4B  RepBatchFixed]  count(u16) | pad(u16)
//! [N × entry]
//!   [4B consumer_id][8B seq][2B subj_len][2B reply_len][4B data_len][4B sub_id]
//!   [subj_len bytes subject]
//!   [reply_len bytes reply_to]
//!   [payload …]
//! ```
//!
//! **Deliver** (single delivery, v2 proto path) — **v2 Header** format:
//! ```text
//! [16B Header]     action | flags | entry_flags | msg_len(4B) | seq(8B)
//! [12B DeliverBody]  consumer_id(4B) | subscription_id(4B) | subject_len(2B) | pad(2B)
//! [subject_len bytes subject][payload …]
//! ```

use bytes::Bytes;
use zerocopy::FromBytes;

// RepBatch uses the server's internal wire types (Envelope + DeliveryEntryHeader).
use arbitro_proto::wire::{
    DeliveryEntryHeader, RepBatchFixed, DELIVERY_ENTRY_HEADER_SIZE, REP_BATCH_FIXED_SIZE,
};
// Single Deliver still uses v2 egress types.
use arbitro_proto::v2::egress::{DeliverBody, DELIVER_BODY_FIXED};
use arbitro_proto::v2::header::{Header, HEADER_SIZE};

use std::sync::Arc;

use crate::consume::message::Message;
use crate::state::subscriptions::Target;
use crate::state::Inner;

/// Envelope size is the same as HEADER_SIZE (16B), but the field layout differs.
const ENVELOPE_SIZE: usize = HEADER_SIZE;

/// Dispatch a single `Deliver` frame to its subscriber channel.
///
/// Frame layout: `[Header 16B][DeliverBody 12B][subject subject_len B][payload …]`
pub(crate) async fn dispatch_deliver(frame: Bytes, inner: &Arc<Inner>) {
    let hdr = match Header::ref_from_bytes(&frame[..HEADER_SIZE]) {
        Ok(h) => h,
        Err(_) => return,
    };
    let deliver_seq = hdr.seq.get();

    let body_end = HEADER_SIZE + DELIVER_BODY_FIXED;
    if frame.len() < body_end {
        return;
    }
    let body = match DeliverBody::ref_from_bytes(&frame[HEADER_SIZE..body_end]) {
        Ok(b) => b,
        Err(_) => return,
    };

    let consumer_id = body.consumer_id.get();
    let sub_id = body.subscription_id.get();
    let subject_len = body.subject_len.get() as usize;
    let payload_off = body_end + subject_len;

    if frame.len() < payload_off {
        return;
    }

    let subject = &frame[body_end..payload_off];
    let payload = frame.slice(payload_off..);
    let mut targets = Vec::new();
    deliver(
        inner,
        sub_id,
        consumer_id,
        deliver_seq,
        subject,
        Bytes::new(), // single Deliver carries no reply_to
        payload,
        &mut targets,
    )
    .await;
}

/// Route one delivered entry and hand it to every subscription it belongs
/// to, then clear `targets` for the next entry.
///
/// The dedup probe runs ONCE, before the fan-out. Per subscription it would
/// let the first sibling mark the seq and the rest discard the message as a
/// duplicate — the slot is keyed `(stream, consumer)`, not per subscription.
#[allow(clippy::too_many_arguments)]
async fn deliver(
    inner: &Arc<Inner>,
    sub_id: u32,
    consumer_id: u32,
    seq: u64,
    subject: &[u8],
    reply_to: Bytes,
    payload: Bytes,
    targets: &mut Vec<Target>,
) {
    // Unknown subscription: the handle was dropped, or this arrived before a
    // reconnect replay re-registered it. Nothing to route it to.
    let Some(d) = inner.subscriptions.route(sub_id, subject, targets) else {
        return;
    };

    if let Some(s) = &d.dedup {
        if s.seen(seq) {
            inner
                .metrics
                .dedup_hits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let _ = inner.ack_tx.try_send(crate::consume::message::AckCmd {
                seq,
                consumer_id,
                sub_id,
            });
            targets.clear();
            return;
        }
    }

    for t in targets.iter() {
        let msg = Message::new(
            seq,
            consumer_id,
            d.stream_id,
            t.sub_id,
            Box::from(subject),
            reply_to.clone(),
            payload.clone(),
            inner.ack_tx.clone(),
            inner.nack_tx.clone(),
            Arc::clone(inner),
            d.dedup.clone(),
        );
        inner
            .metrics
            .deliveries_received
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _ = t.tx.send(msg).await;
    }
    targets.clear();
}

/// Dispatch a batch-deliver frame (action = `RepBatch`) to subscriber channels.
///
/// The server sends RepBatch frames using the **Envelope** wire format (not v2 Header):
/// ```text
/// [16B Envelope]       action | flags | rsv | stream_id(4B) | msg_len(4B) | env_seq(4B)
/// [4B  RepBatchFixed]  count(u16) | pad(u16)
/// [N × (24B DeliveryEntryHeader + subject + reply_to + payload)]
///   consumer_id(4B) | seq(8B) | subj_len(2B) | reply_len(2B) | data_len(4B) | sub_id(4B)
/// ```
pub(crate) async fn dispatch_batch_deliver(frame: Bytes, inner: &Arc<Inner>) {
    // The Envelope (16B) precedes the batch header.
    let bh_start = ENVELOPE_SIZE;
    let bh_end = bh_start + REP_BATCH_FIXED_SIZE; // 16 + 4 = 20
    if frame.len() < bh_end {
        return;
    }
    let batch_hdr = match RepBatchFixed::ref_from_bytes(&frame[bh_start..bh_end]) {
        Ok(h) => h,
        Err(_) => return,
    };

    let count = batch_hdr.count.get() as usize;
    let mut off = bh_end; // start of first entry
    // Reused across every entry in this frame — the fan-out never allocates.
    let mut targets = Vec::new();

    for _ in 0..count {
        let entry_end = off + DELIVERY_ENTRY_HEADER_SIZE; // +24
        if frame.len() < entry_end {
            break;
        }
        let entry = match DeliveryEntryHeader::ref_from_bytes(&frame[off..entry_end]) {
            Ok(e) => e,
            Err(_) => break,
        };

        let consumer_id = entry.consumer_id.get();
        let deliver_seq = entry.seq.get();
        let subj_len = entry.subj_len.get() as usize;
        let reply_len = entry.reply_len.get() as usize;
        let data_len = entry.data_len.get() as usize;
        let sub_id = entry.sub_id.get();
        off = entry_end;

        // Validate the interior layout before slicing: a malformed entry with
        // `subj_len`/`reply_len` exceeding `data_len` (or a `data_len` past the
        // frame) would otherwise panic the reader task on the slice below.
        if frame.len() < off + data_len || subj_len + reply_len > data_len {
            break;
        }
        let subject = &frame[off..off + subj_len];
        let reply_to = if reply_len > 0 {
            frame.slice(off + subj_len..off + subj_len + reply_len)
        } else {
            Bytes::new()
        };
        let payload_start = off + subj_len + reply_len;
        let payload = frame.slice(payload_start..off + data_len);
        let next = off + data_len;

        deliver(
            inner,
            sub_id,
            consumer_id,
            deliver_seq,
            subject,
            reply_to,
            payload,
            &mut targets,
        )
        .await;
        off = next;
    }
}
