//! Message delivered to a consumer.

use bytes::Bytes;
use tokio::sync::mpsc;

/// A message received from the broker.
pub struct Message {
    pub seq: u64,
    pub subject: Box<[u8]>,
    pub payload: Bytes,
    pub(crate) consumer_id: u32,
    pub(crate) stream_id: u32,
    pub(crate) ack_tx: mpsc::Sender<AckCmd>,
}

/// Command sent back to the connection for ack/nack.
pub(crate) enum AckCmd {
    Ack { stream_id: u32, consumer_id: u32, seq: u64 },
    Nack { stream_id: u32, consumer_id: u32, seq: u64 },
}

impl Message {
    /// Acknowledge this message — fire-and-forget.
    pub fn ack(&self) {
        let _ = self.ack_tx.try_send(AckCmd::Ack {
            stream_id: self.stream_id,
            consumer_id: self.consumer_id,
            seq: self.seq,
        });
    }

    /// Negative-acknowledge — request redelivery.
    pub fn nack(&self) {
        let _ = self.ack_tx.try_send(AckCmd::Nack {
            stream_id: self.stream_id,
            consumer_id: self.consumer_id,
            seq: self.seq,
        });
    }
}
