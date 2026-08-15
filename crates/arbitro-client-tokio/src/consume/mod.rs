//! Consume API — `SubscriptionHandle`, `subscribe_async`, and the ack-batcher.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::consume::message::{AckCmd, Message, NackCmd};
use crate::error::ClientError;
use crate::state::Inner;
use crate::transport::encode::{
    encode_ack_batch_v2, encode_ack_state_req_v2, encode_batch_ack_v2, encode_batch_nack_v2,
    encode_sub_v2, encode_unsub_v2,
};
use crate::transport::frame::{WriteFrame, WriteLease, INLINE_CAP};

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
    pub(crate) rx: mpsc::Receiver<Message>,
    pub(crate) consumer_id: u32,
    pub(crate) inner: Arc<Inner>,
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
        // Silently dropped if the pool is exhausted, full, or torn down.
        let seq = self.inner.seq_alloc.next();
        let frame = encode_unsub_v2(seq, self.consumer_id);
        let _ = crate::publish::enqueue(&self.inner.pool, WriteFrame::Mono(frame));
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
    inner: Arc<Inner>,
    stream_id: u32,
    consumer_id: u32,
    filter: &[u8],
    names: Option<(&str, &str)>,
) -> impl Future<Output = Result<SubscriptionHandle, ClientError>> + Send {
    let seq = inner.seq_alloc.next();
    // One id per subscription, not per consumer: several filtered subscriptions
    // on one consumer must not collapse onto a single broker binding.
    let subscription_id = inner.sub_id_alloc.next();
    let sub_body = encode_sub_v2(seq, 0, consumer_id, subscription_id, filter);

    // Resolve the durable dedup slot up front (cold path, sync). Only when a
    // store is configured AND the caller supplied the `(stream, consumer)`
    // names the key needs — the numeric consumer_id is ephemeral and unusable
    // as a durable key. A resolve error disables dedup for this subscription
    // rather than failing the subscribe.
    let slot = match (&inner.ack_store, names) {
        (Some(store), Some((stream, consumer))) => match store.slot(stream, consumer) {
            Ok(s) => Some(s),
            Err(e) => {
                inner
                    .metrics
                    .ackstore_errors
                    .fetch_add(1, Ordering::Relaxed);
                warn!(stream, consumer, error = %e, "ackstore: slot resolve failed; dedup disabled for this sub");
                None
            }
        },
        _ => None,
    };

    // 1. Register channel BEFORE enqueuing the SubFrame.
    //    Any Deliver frames that arrive while the round-trip is in flight
    //    are buffered in the channel (capacity = 4096).
    let has_slot = slot.is_some();
    let rx = inner
        .subscriptions
        .register(consumer_id, stream_id, sub_body.clone(), slot);

    // 2. Reserve a pending slot for the RepOk reply.
    let rx_pending = inner.pending.register(seq);

    // 3. Enqueue the SubFrame via a leased producer (sync — no await).
    let enqueue_result = crate::publish::enqueue(&inner.pool, WriteFrame::Mono(sub_body));

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
            Ok(_) => {
                // On-connect ackstore purge: the WAL may hold entries recorded
                // by a previous, dead session (its `AckBatchResp` never
                // arrived, so nothing confirmed them). Ask the broker for its
                // authoritative ack cursor ONCE, now that the subscribe is
                // confirmed (the consumer provably exists server-side); the
                // `AckStateRep` handler in `transport::reader` drops every WAL
                // entry at or below that cursor. Cold path — one 24 B frame
                // per subscribe, fire-and-forget, nothing on the delivery/ack
                // hot path. Reconnects are covered separately by
                // `conn::session::send_ack_state_reqs`.
                if has_slot {
                    let req_seq = inner2.seq_alloc.next();
                    let generation = inner2.ackrel.generation_of(consumer_id);
                    let _ = crate::publish::enqueue(
                        &inner2.pool,
                        WriteFrame::Mono(encode_ack_state_req_v2(req_seq, consumer_id, generation)),
                    );
                }
                Ok(SubscriptionHandle {
                    rx,
                    consumer_id,
                    inner: inner2,
                })
            }
            Err(e) => {
                inner2.subscriptions.remove(consumer_id);
                Err(e)
            }
        }
    }
}

// ── ack_batcher_task ──────────────────────────────────────────────────────────

#[inline]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// How often the deferred-ack sweep tick fires.
const SWEEP_TICK: Duration = Duration::from_millis(100);
/// TTL-expiry check runs once every this many ticks (~60s at `SWEEP_TICK`).
const EXPIRE_EVERY_TICKS: u32 = 600;

/// Drains `AckCmd`s from `Message::ack()` calls, batches them, and
/// enqueues `AckFrame` / `BatchAckFrame` via a dedicated lease. Also
/// services the ack-reliability hot layer: deferred acks (recorded when
/// the channel above was full) are flushed on a timer and TTL-expired
/// periodically.
///
/// Runs for the **Client lifetime** (not per-session), so acks enqueued
/// during a reconnect window are preserved in the ring and flushed once
/// the new writer task starts.
///
/// Uses `recv().await` + `try_recv()` drain plus a `tokio::time::interval`
/// tick — zero spin loop.
pub(crate) async fn ack_batcher_task(
    mut rx: mpsc::Receiver<AckCmd>,
    inner: Arc<Inner>,
    cancel: CancellationToken,
    mut lease: WriteLease,
) {
    use arbitro_proto::v2::ingress::ack_frame::AckFrame;

    let mut sweep = tokio::time::interval(SWEEP_TICK);
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ticks_since_expire: u32 = 0;

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
                let mut by_consumer: HashMap<u32, Vec<(u64, u32)>> = HashMap::new();
                for cmd in &batch {
                    by_consumer
                        .entry(cmd.consumer_id)
                        .or_default()
                        .push((cmd.seq, cmd.sub_id));
                }

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
                    let _ = lease.try_send(frame);
                }
            }
            _ = sweep.tick() => {
                // Flush every consumer with outstanding deferred acks. Not
                // removed from the hot set here — only `AckBatchResp`
                // (transport::reader dispatch) purges on broker confirm.
                for (consumer_id, generation) in inner.ackrel.active_consumers() {
                    let seqs = inner.ackrel.drain_ascending(consumer_id, 1000);
                    if seqs.is_empty() {
                        continue;
                    }
                    let seq = inner.seq_alloc.next();
                    let frame = WriteFrame::Mono(encode_ack_batch_v2(seq, consumer_id, generation, 0, &seqs));
                    let _ = lease.try_send(frame);
                }

                // Durable dedup: flush buffered ack records to disk each tick
                // so a crash loses at most `SWEEP_TICK` of recorded acks.
                if let Some(store) = &inner.ack_store {
                    if let Err(e) = store.sync() {
                        inner.metrics.ackstore_errors.fetch_add(1, Ordering::Relaxed);
                        warn!(error = %e, "ackstore: periodic sync failed");
                    }
                }

                ticks_since_expire += 1;
                if ticks_since_expire >= EXPIRE_EVERY_TICKS {
                    ticks_since_expire = 0;
                    let now = now_ms();
                    let cutoff = now.saturating_sub(inner.cfg.ack_pending_ttl.as_millis() as u64);
                    let expired = inner.ackrel.expire(cutoff, now);
                    if expired > 0 {
                        inner.metrics.acks_expired.fetch_add(expired as u64, Ordering::Relaxed);
                        warn!(expired, "ack-batcher: TTL-expired deferred acks dropped unpersisted");
                    }
                }
            }
        }
    }
}

// ── nack_batcher_task ─────────────────────────────────────────────────────────

/// Drains `NackCmd`s from `Message::nack()` calls, batches them, and
/// enqueues `NackFrame` / `BatchNackFrame` via a dedicated lease.
///
/// Identical structure to `ack_batcher_task` — see its doc for rationale.
pub(crate) async fn nack_batcher_task(
    mut rx: mpsc::Receiver<NackCmd>,
    inner: Arc<Inner>,
    cancel: CancellationToken,
    mut lease: WriteLease,
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
                // Tuple: (seq, sub_id, delay_ms).
                let mut by_consumer: HashMap<u32, Vec<(u64, u32, u32)>> = HashMap::new();
                for cmd in &batch {
                    by_consumer
                        .entry(cmd.consumer_id)
                        .or_default()
                        .push((cmd.seq, cmd.sub_id, cmd.delay_ms));
                }

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
                    let _ = lease.try_send(frame);
                }
            }
        }
    }
}
