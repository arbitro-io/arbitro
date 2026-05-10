//! Consume API — `SubscriptionHandle`, `subscribe_async`, and the ack-batcher.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::consume::message::{AckCmd, NackCmd, Message};
use crate::error::ClientError;
use crate::state::Inner;
use crate::transport::encode::{encode_batch_ack_v2, encode_batch_nack_v2, encode_nack_v2, encode_sub_v2, encode_unsub_v2};
use crate::transport::frame::{WriteFrame, INLINE_CAP};

pub mod demux;
pub mod message;

// ── SubscriptionHandle ────────────────────────────────────────────────────────

/// Handle to an active subscription.
///
/// Dropping the handle unregisters the subscription locally.  The server
/// garbage-collects the consumer-side state when the connection drops or
/// an explicit `Unsubscribe` is sent (not yet implemented; the drop is
/// sufficient for correctness).
pub struct SubscriptionHandle {
    pub(crate) rx:          mpsc::Receiver<Message>,
    pub(crate) consumer_id: u32,
    pub(crate) inner:       Arc<Inner>,
}

impl std::fmt::Debug for SubscriptionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionHandle")
            .field("consumer_id", &self.consumer_id)
            .finish()
    }
}

impl SubscriptionHandle {
    /// Receive the next delivered message.
    ///
    /// Returns `None` when the client is closed or the connection is
    /// permanently lost.
    #[inline]
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        self.inner.subscriptions.remove(self.consumer_id);
        // Fire-and-forget Unsubscribe to the broker.
        // Lock held only for `try_send` (nanoseconds, no await).
        // Silently dropped if the channel is full or session is torn down.
        let seq = self.inner.seq_alloc.next();
        let frame = encode_unsub_v2(seq, self.consumer_id);
        let _ = self.inner.admin_producer.lock().unwrap()
            .try_send(WriteFrame::Mono(frame));
    }
}

// ── subscribe_async ───────────────────────────────────────────────────────────

/// Register a subscription locally, then send a `SubFrame` to the broker
/// and await the `RepOk` reply.
///
/// **All synchronous work** (channel registration, pending slot, frame
/// encode, `try_send`) happens before the `async move` block, so the
/// returned future is `Send` and no `&Inner` reference crosses an await.
pub(crate) fn subscribe_async(
    inner:       Arc<Inner>,
    stream_id:   u32,
    consumer_id: u32,
    filter:      &[u8],
) -> impl Future<Output = Result<SubscriptionHandle, ClientError>> + Send {
    let seq      = inner.seq_alloc.next();
    let sub_body = encode_sub_v2(seq, 0, consumer_id, 0, filter);

    // 1. Register channel BEFORE enqueuing the SubFrame.
    //    Any Deliver frames that arrive while the round-trip is in flight
    //    are buffered in the channel (capacity = 4096).
    let rx = inner.subscriptions.register(consumer_id, stream_id, sub_body.clone());

    // 2. Reserve a pending slot for the RepOk reply.
    let rx_pending = inner.pending.register(seq);

    // 3. Enqueue the SubFrame via the admin producer (sync — no await).
    let enqueue_result = inner
        .admin_producer
        .lock()
        .unwrap()
        .try_send(WriteFrame::Mono(sub_body))
        .map_err(|_| ClientError::ChannelClosed);

    let inner2 = Arc::clone(&inner);
    async move {
        let wire_result: Result<Bytes, ClientError> = {
            enqueue_result?;
            rx_pending
                .recv_async()
                .await
                .map_err(|_| ClientError::ChannelClosed)
                .and_then(|r| r)
        };
        match wire_result {
            Ok(_) => Ok(SubscriptionHandle { rx, consumer_id, inner: inner2 }),
            Err(e) => {
                inner2.subscriptions.remove(consumer_id);
                Err(e)
            }
        }
    }
}

// ── ack_batcher_task ──────────────────────────────────────────────────────────

/// Drains `AckCmd`s from `Message::ack()` calls, batches them, and
/// enqueues `AckFrame` / `BatchAckFrame` via the admin producer.
///
/// Runs for the **Client lifetime** (not per-session), so acks enqueued
/// during a reconnect window are preserved in the ring and flushed once
/// the new writer task starts.
///
/// Uses `recv().await` + `try_recv()` drain — zero spin loop.
pub(crate) async fn ack_batcher_task(
    mut rx: mpsc::Receiver<AckCmd>,
    inner:  Arc<Inner>,
    cancel: CancellationToken,
) {
    use arbitro_proto::v2::ingress::ack_frame::AckFrame;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            cmd = rx.recv() => {
                let Some(first) = cmd else { return };

                // Drain all immediately available — not a spin loop since
                // `recv()` already parked until at least one arrived.
                let mut batch: Vec<AckCmd> = vec![first];
                while let Ok(c) = rx.try_recv() {
                    batch.push(c);
                    if batch.len() >= 64 { break; }
                }

                // Group by consumer_id — each consumer gets its own ack frame.
                // Build the map first (no lock), then take the admin lock once
                // for the entire emit loop (nanoseconds; no await inside).
                let mut by_consumer: HashMap<u32, Vec<(u64, u32)>> = HashMap::new();
                for cmd in &batch {
                    by_consumer
                        .entry(cmd.consumer_id)
                        .or_default()
                        .push((cmd.seq, cmd.subject_hash));
                }

                let admin = inner.admin_producer.lock().unwrap();
                for (consumer_id, entries) in by_consumer {
                    let seq = inner.seq_alloc.next();
                    let frame = if entries.len() == 1 {
                        // Single ack — inline (AckFrame::WIRE_SIZE = 32B < INLINE_CAP).
                        let ack = AckFrame::new(seq, consumer_id, entries[0].0, entries[0].1);
                        let mut data = [0u8; INLINE_CAP];
                        let sz = AckFrame::WIRE_SIZE;
                        data[..sz].copy_from_slice(zerocopy::IntoBytes::as_bytes(&ack));
                        WriteFrame::Inline(data, sz as u16)
                    } else {
                        WriteFrame::Mono(encode_batch_ack_v2(seq, consumer_id, &entries))
                    };
                    let _ = admin.try_send(frame);
                }
            }
        }
    }
}

// ── nack_batcher_task ─────────────────────────────────────────────────────────

/// Drains `NackCmd`s from `Message::nack()` calls, batches them, and
/// enqueues `NackFrame` / `BatchNackFrame` via the admin producer.
///
/// Identical structure to `ack_batcher_task` — see its doc for rationale.
pub(crate) async fn nack_batcher_task(
    mut rx: mpsc::Receiver<NackCmd>,
    inner:  Arc<Inner>,
    cancel: CancellationToken,
) {
    use arbitro_proto::v2::ingress::nack_frame::NackFrame;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            cmd = rx.recv() => {
                let Some(first) = cmd else { return };

                let mut batch: Vec<NackCmd> = vec![first];
                while let Ok(c) = rx.try_recv() {
                    batch.push(c);
                    if batch.len() >= 64 { break; }
                }

                // Group by consumer_id then emit one nack frame per group.
                // Tuple: (seq, subject_hash, delay_ms).
                let mut by_consumer: HashMap<u32, Vec<(u64, u32, u32)>> = HashMap::new();
                for cmd in &batch {
                    by_consumer
                        .entry(cmd.consumer_id)
                        .or_default()
                        .push((cmd.seq, cmd.subject_hash, cmd.delay_ms));
                }

                let admin = inner.admin_producer.lock().unwrap();
                for (consumer_id, entries) in by_consumer {
                    let seq = inner.seq_alloc.next();
                    // Use single NackFrame only when no delay and single entry.
                    let frame = if entries.len() == 1 && entries[0].2 == 0 {
                        let nack = NackFrame::new(seq, consumer_id, entries[0].0, entries[0].1);
                        let mut data = [0u8; INLINE_CAP];
                        let sz = NackFrame::WIRE_SIZE;
                        data[..sz].copy_from_slice(zerocopy::IntoBytes::as_bytes(&nack));
                        WriteFrame::Inline(data, sz as u16)
                    } else {
                        WriteFrame::Mono(encode_batch_nack_v2(seq, consumer_id, &entries))
                    };
                    let _ = admin.try_send(frame);
                }
            }
        }
    }
}
