//! Delivered message type and fire-and-forget acknowledgement / nack.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::error::ClientError;
use crate::state::Inner;

#[inline]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Magic byte prefix for encoded reply_to addresses that carry a stream_id.
/// Format: `[0xFF][stream_id: 4B LE][subject bytes]`
pub const REPLY_TO_MAGIC: u8 = 0xFF;

/// Minimum length of an encoded reply_to with stream_id.
pub const REPLY_TO_MIN_LEN: usize = 5; // 1 magic + 4 stream_id

/// Encode a reply_to address with embedded stream_id.
#[inline]
pub fn encode_reply_to(stream_id: u32, subject: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(REPLY_TO_MIN_LEN + subject.len());
    buf.push(REPLY_TO_MAGIC);
    buf.extend_from_slice(&stream_id.to_le_bytes());
    buf.extend_from_slice(subject);
    buf
}

/// Parse an encoded reply_to: returns (stream_id, subject) or None if not encoded.
#[inline]
pub fn decode_reply_to(reply_to: &[u8]) -> Option<(u32, &[u8])> {
    if reply_to.len() >= REPLY_TO_MIN_LEN && reply_to[0] == REPLY_TO_MAGIC {
        let stream_id = u32::from_le_bytes([reply_to[1], reply_to[2], reply_to[3], reply_to[4]]);
        Some((stream_id, &reply_to[REPLY_TO_MIN_LEN..]))
    } else {
        None
    }
}

/// Internal command enqueued by `Message::ack()` and drained by the
/// ack-batcher task.  Never exposed publicly.
pub(crate) struct AckCmd {
    pub seq: u64,
    pub consumer_id: u32,
    pub subject_hash: u32,
}

/// Internal command enqueued by `Message::nack()` / `nack_delay()` and
/// drained by the nack-batcher task.
pub(crate) struct NackCmd {
    pub seq: u64,
    pub consumer_id: u32,
    pub subject_hash: u32,
    /// Delay in milliseconds before redelivery. 0 = immediate requeue.
    pub delay_ms: u32,
}

/// A message delivered from a broker consumer subscription.
///
/// Holds a zero-copy `Bytes` slice of the original read-buffer frame.
/// The subject is always copied into a `Box<[u8]>` once per delivery.
pub struct Message {
    /// Delivery sequence number (monotonically increasing per stream).
    pub seq: u64,
    /// The consumer that received this delivery.
    pub consumer_id: u32,
    /// Stream the consumer belongs to (informational — not required for ack).
    pub stream_id: u32,
    /// FNV-1a hash of the subject, echoed back in ack/nack frames.
    pub subject_hash: u32,
    subject: Box<[u8]>,
    reply_to: Bytes,
    payload: Bytes,
    ack_tx: mpsc::Sender<AckCmd>,
    nack_tx: mpsc::Sender<NackCmd>,
    inner: Arc<Inner>,
    /// Durable dedup slot for this delivery's `(stream, consumer)`, resolved
    /// once at subscribe time. `None` when persistence is disabled. On ack the
    /// message's `seq` is recorded here so a redelivery after restart is
    /// recognized and skipped. See [`crate::ackstore`].
    slot: Option<Arc<dyn crate::ackstore::SlotRef>>,
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("seq", &self.seq)
            .field("consumer_id", &self.consumer_id)
            .field("stream_id", &self.stream_id)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

impl Message {
    /// Construct a new `Message`.  Called from the demux path only.
    #[inline]
    pub(crate) fn new(
        seq: u64,
        consumer_id: u32,
        stream_id: u32,
        subject_hash: u32,
        subject: Box<[u8]>,
        reply_to: Bytes,
        payload: Bytes,
        ack_tx: mpsc::Sender<AckCmd>,
        nack_tx: mpsc::Sender<NackCmd>,
        inner: Arc<Inner>,
        slot: Option<Arc<dyn crate::ackstore::SlotRef>>,
    ) -> Self {
        Self {
            seq,
            consumer_id,
            stream_id,
            subject_hash,
            subject,
            reply_to,
            payload,
            ack_tx,
            nack_tx,
            inner,
            slot,
        }
    }

    /// Borrow the subject bytes.
    #[inline]
    pub fn subject(&self) -> &[u8] {
        &self.subject
    }

    /// Reply-to address bytes (raw, including stream_id prefix if encoded).
    #[inline]
    pub fn reply_to_raw(&self) -> &[u8] {
        &self.reply_to
    }

    /// Returns `true` if this message has a reply_to address.
    #[inline]
    pub fn has_reply_to(&self) -> bool {
        !self.reply_to.is_empty()
    }

    /// Clone the payload `Bytes` handle (zero-copy — bumps an Arc ref-count).
    #[inline]
    pub fn payload(&self) -> Bytes {
        self.payload.clone()
    }

    /// Borrow the payload bytes without cloning the Bytes handle.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.payload
    }

    /// Reply to the sender of this message.
    ///
    /// The reply_to field must be encoded with stream_id (0xFF prefix format)
    /// for this to work. Returns `ClientError::NoReplyAddress` if reply_to
    /// is empty or not in the encoded format.
    ///
    /// This is a fire-and-forget publish via a leased producer — it does
    /// not wait for broker confirmation.
    pub fn reply(&self, payload: &[u8]) -> Result<(), ClientError> {
        let (target_stream_id, subject) = decode_reply_to(&self.reply_to)
            .ok_or(ClientError::NoReplyAddress)?;

        let seq = self.inner.seq_alloc.next();
        let frame = crate::publish::encode_pub_frame_for_reply(seq, target_stream_id, subject, payload);
        crate::publish::enqueue(&self.inner.pool, frame)
    }

    /// Fire-and-forget acknowledgement.
    ///
    /// Enqueues an `AckCmd` into the ack-batcher task. If the channel is
    /// full or the batcher is gone, the ack is not dropped — it is
    /// recorded in the ack-reliability hot layer and flushed by the
    /// batcher's periodic sweep (or replayed after reconnect).
    #[inline]
    pub fn ack(&self) {
        // Durable dedup: record this seq as processed so a redelivery (even
        // after a client restart) is recognized and skipped. Append is
        // buffered — a periodic `Store::sync` makes it durable. Idempotent.
        if let Some(slot) = &self.slot {
            match slot.record(self.seq) {
                Ok(()) => {
                    self.inner.metrics.ackstore_records.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    self.inner.metrics.ackstore_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(seq = self.seq, error = %e, "ackstore: record failed");
                }
            }
        }
        let cmd = AckCmd {
            seq: self.seq,
            consumer_id: self.consumer_id,
            subject_hash: self.subject_hash,
        };
        if self.ack_tx.try_send(cmd).is_err() {
            let generation = self.inner.ackrel.generation_of(self.consumer_id);
            self.inner
                .ackrel
                .record_deferred(self.consumer_id, generation, self.seq, now_ms());
            self.inner.metrics.acks_deferred.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Fire-and-forget negative acknowledgement (immediate requeue).
    ///
    /// Enqueues a `NackCmd` into the nack-batcher task. The broker will
    /// requeue the message for redelivery to this consumer. Silently drops
    /// if the internal channel is full or the client has disconnected.
    #[inline]
    pub fn nack(&self) {
        let _ = self.nack_tx.try_send(NackCmd {
            seq: self.seq,
            consumer_id: self.consumer_id,
            subject_hash: self.subject_hash,
            delay_ms: 0,
        });
    }

    /// Negative acknowledgement with delayed redelivery.
    ///
    /// The broker will wait `delay_ms` milliseconds before making this
    /// message available for redelivery. Maximum delay = 120 seconds
    /// (clamped by the server's timing wheel resolution).
    #[inline]
    pub fn nack_delay(&self, delay_ms: u32) {
        let _ = self.nack_tx.try_send(NackCmd {
            seq: self.seq,
            consumer_id: self.consumer_id,
            subject_hash: self.subject_hash,
            delay_ms,
        });
    }
}
