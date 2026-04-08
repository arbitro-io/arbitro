//! Message delivered to a consumer.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;

use arbitro_proto::action::Action;

use crate::error::ClientError;
use crate::inner::Inner;

/// A message received from the broker.
pub struct Message {
    pub seq: u64,
    pub subject: Box<[u8]>,
    pub payload: Bytes,
    pub(crate) consumer_id: u32,
    pub(crate) stream_id: u32,
    pub(crate) ack_tx: mpsc::Sender<AckCmd>,
    pub(crate) inner: Arc<Inner>,
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

    /// Acknowledge this message and wait for broker confirmation.
    ///
    /// Same pattern as `Client::publish` — direct `inner.request()`, no intermediate hops.
    pub async fn ack_sync(&self) -> Result<(), ClientError> {
        let mut body = [0u8; 16];
        body[..8].copy_from_slice(&self.seq.to_le_bytes());
        body[8..12].copy_from_slice(&self.consumer_id.to_le_bytes());

        self.inner
            .request(Action::AckSync, self.stream_id, &body)
            .await?;
        Ok(())
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
