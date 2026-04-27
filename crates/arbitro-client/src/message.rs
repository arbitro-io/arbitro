//! Message delivered to a consumer.

use std::sync::{Arc, Mutex};

use bytes::Bytes;

use arbitro_kit::route::MpscProducer;

use arbitro_proto::action::Action;

use crate::error::ClientError;
use crate::inner::{Inner, ACK_RING_CAP};

/// Producer handle for the ack channel — shared across `Message` instances.
/// `MpscProducer` is `Send + !Sync`; we wrap it in `Arc<Mutex<…>>` so it can
/// be cloned freely and pushed into from any message-owning task.
pub(crate) type AckProducer = Arc<Mutex<MpscProducer<AckCmd, ACK_RING_CAP>>>;

/// A message received from the broker.
pub struct Message {
    pub seq: u64,
    pub subject: Box<[u8]>,
    pub payload: Bytes,
    pub(crate) consumer_id: u32,
    pub(crate) stream_id: u32,
    pub(crate) ack_tx: AckProducer,
    pub(crate) inner: Arc<Inner>,
}

/// Command sent back to the connection for ack/nack.
pub(crate) enum AckCmd {
    Ack { stream_id: u32, consumer_id: u32, seq: u64 },
    Nack { stream_id: u32, consumer_id: u32, seq: u64 },
}

impl Message {
    /// Acknowledge this message — fire-and-forget. Drops silently on
    /// backpressure (ring full) or if the ack channel was reset by a
    /// reconnect (stale clone, correct).
    pub fn ack(&self) {
        let cmd = AckCmd::Ack {
            stream_id: self.stream_id,
            consumer_id: self.consumer_id,
            seq: self.seq,
        };
        let _ = self.ack_tx.lock().unwrap().try_send(cmd);
    }

    /// Acknowledge this message and wait for broker confirmation.
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
        let cmd = AckCmd::Nack {
            stream_id: self.stream_id,
            consumer_id: self.consumer_id,
            seq: self.seq,
        };
        let _ = self.ack_tx.lock().unwrap().try_send(cmd);
    }
}
