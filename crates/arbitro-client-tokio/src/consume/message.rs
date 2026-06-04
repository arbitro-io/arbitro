//! Delivered message type and fire-and-forget acknowledgement / nack.

use bytes::Bytes;
use tokio::sync::mpsc;

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
        }
    }

    /// Borrow the subject bytes.
    #[inline]
    pub fn subject(&self) -> &[u8] {
        &self.subject
    }

    /// Reply-to subject for request/reply RPC. Empty when not an RPC message.
    #[inline]
    pub fn reply_to(&self) -> &[u8] {
        &self.reply_to
    }

    /// Returns `true` if this message has a reply_to subject (is an RPC request).
    #[inline]
    pub fn has_reply_to(&self) -> bool {
        !self.reply_to.is_empty()
    }

    /// Clone the payload `Bytes` handle (zero-copy — bumps an Arc ref-count).
    #[inline]
    pub fn payload(&self) -> Bytes {
        self.payload.clone()
    }

    /// Fire-and-forget acknowledgement.
    ///
    /// Enqueues an `AckCmd` into the ack-batcher task.  Silently drops if
    /// the internal channel is full or the client has disconnected.
    #[inline]
    pub fn ack(self) {
        let _ = self.ack_tx.try_send(AckCmd {
            seq: self.seq,
            consumer_id: self.consumer_id,
            subject_hash: self.subject_hash,
        });
    }

    /// Fire-and-forget negative acknowledgement (immediate requeue).
    ///
    /// Enqueues a `NackCmd` into the nack-batcher task. The broker will
    /// requeue the message for redelivery to this consumer. Silently drops
    /// if the internal channel is full or the client has disconnected.
    #[inline]
    pub fn nack(self) {
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
    pub fn nack_delay(self, delay_ms: u32) {
        let _ = self.nack_tx.try_send(NackCmd {
            seq: self.seq,
            consumer_id: self.consumer_id,
            subject_hash: self.subject_hash,
            delay_ms,
        });
    }
}
