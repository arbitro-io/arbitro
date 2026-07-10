//! Reader task — `BytesMut + read_buf + split_to`. v2 framing.
//!
//! Decodes by `Header.action` and routes to the correct handler:
//! - `RepOk` / `RepError`            → resolve the matching `Pending` slot.
//! - `ListStreams` / `ListConsumers`  → resolve `Pending` with the body bytes.
//! - `Deliver`                        → demux to subscriber channel.
//! - `RepBatch`                       → batch-deliver demux.
//! - `Pong`                           → update `last_pong_ns` heartbeat timestamp.
//! - everything else                  → silently drop.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use zerocopy::FromBytes;

use arbitro_proto::action::Action;
use arbitro_proto::v2::egress::ack_state::{AckBatchRespFrame, AckStateRepFrame};
use arbitro_proto::v2::egress::rep_frame::RepErrFrame;
use arbitro_proto::v2::header::{Header, HEADER_SIZE};

use crate::consume::demux;
use crate::error::ClientError;
use crate::state::Inner;
use crate::transport::encode::encode_ack_batch_v2;
use crate::transport::frame::WriteFrame;

/// Initial read buffer capacity.
const READ_BUF_INITIAL: usize = 64 * 1024;

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

pub(crate) async fn reader_task<R: AsyncRead + Unpin>(
    mut r: R,
    inner: Arc<Inner>,
    cancel: CancellationToken,
) -> Result<(), ClientError> {
    let mut buf = BytesMut::with_capacity(READ_BUF_INITIAL);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            res = r.read_buf(&mut buf) => {
                let n = res?;
                if n == 0 {
                    return Err(ClientError::Disconnected);
                }
                while buf.len() >= HEADER_SIZE {
                    // The server uses TWO different 16-byte header formats:
                    //
                    //  • v2 Header (management frames: RepOk, RepError, Pong, …):
                    //      bytes 4-7 = msg_len
                    //
                    //  • Envelope (delivery frames: RepBatch):
                    //      bytes 4-7 = stream_id   ← NOT msg_len
                    //      bytes 8-11 = msg_len
                    //
                    // Peek at the action first so we read msg_len from the right
                    // offset and split the buffer at the correct frame boundary.
                    let action = u16::from_le_bytes([buf[0], buf[1]]);
                    let msg_len: usize = if action == Action::RepBatch.as_u16()
                        || action == Action::FanoutBatch.as_u16()
                    {
                        // Envelope format: msg_len at bytes 8-11.
                        u32::from_le_bytes(
                            buf[8..12].try_into().expect("buf >= HEADER_SIZE >= 16"),
                        ) as usize
                    } else {
                        // v2 Header format: msg_len at bytes 4-7.
                        let h = match Header::ref_from_bytes(&buf[..HEADER_SIZE]) {
                            Ok(h) => h,
                            Err(_) => return Err(ClientError::Disconnected),
                        };
                        h.msg_len.get() as usize
                    };
                    let total = HEADER_SIZE + msg_len;
                    if buf.len() < total {
                        buf.reserve(total - buf.len());
                        break;
                    }
                    let frame = buf.split_to(total).freeze();
                    dispatch(&inner, frame).await;
                }
            }
        }
    }
}

async fn dispatch(inner: &Arc<Inner>, frame: Bytes) {
    // SAFETY: called only after verifying `frame.len() >= HEADER_SIZE`.
    let h = match Header::ref_from_bytes(&frame[..HEADER_SIZE]) {
        Ok(h) => h,
        Err(_) => return,
    };
    let action = h.action.get();
    let req_seq = h.seq.get();
    let body = frame.slice(HEADER_SIZE..);

    // ── Reply paths ────────────────────────────────────────────────────
    if action == Action::RepOk.as_u16() {
        inner.pending.complete_ok(req_seq, body);
        return;
    }

    if action == Action::RepError.as_u16() {
        if let Ok(rep) = RepErrFrame::ref_from_bytes(&frame[..core::mem::size_of::<RepErrFrame>()])
        {
            inner
                .pending
                .complete_err(req_seq, rep.body.error_code.get());
        } else {
            inner.pending.complete_err(req_seq, 0);
        }
        return;
    }

    // ListStreams / ListConsumers reply — body is the raw payload.
    if action == Action::ListStreams.as_u16() || action == Action::ListConsumers.as_u16() {
        inner.pending.complete_ok(req_seq, body);
        return;
    }

    // ── Deliver paths ──────────────────────────────────────────────────
    if action == Action::Deliver.as_u16() {
        demux::dispatch_deliver(frame, inner).await;
        return;
    }

    if action == Action::RepBatch.as_u16() || action == Action::FanoutBatch.as_u16() {
        demux::dispatch_batch_deliver(frame, inner).await;
        return;
    }

    // ── Heartbeat ──────────────────────────────────────────────────────
    if action == Action::Pong.as_u16() {
        inner.last_pong_ns.store(now_ns(), Ordering::Relaxed);
        return;
    }

    // ── Cron fire ──────────────────────────────────────────────────
    if action == Action::CronFire.as_u16() {
        crate::cron::dispatch_cron_fire(frame, inner).await;
        return;
    }

    // ── Ack-reliability ────────────────────────────────────────────
    if action == Action::AckStateRep.as_u16() {
        dispatch_ack_state_rep(inner, &frame);
        return;
    }

    if action == Action::AckBatchResp.as_u16() {
        dispatch_ack_batch_resp(inner, &frame);
        return;
    }

    // All other actions are silently dropped (system frames, etc.)
    let _ = action;
}

/// `AckStateRep`: the broker's authoritative cursor/retention snapshot for
/// one consumer, sent in response to `AckStateReq` (reconnect replay).
///
/// A generation mismatch means our local hot/cold state is stale relative
/// to the broker (e.g. consumer was recreated) — wipe it wholesale rather
/// than try to reconcile entry-by-entry. Otherwise, trim the deferred set
/// to the broker's confirmed/retained range and flush what survives.
fn dispatch_ack_state_rep(inner: &Arc<Inner>, frame: &Bytes) {
    let Ok(rep) = AckStateRepFrame::ref_from_bytes(&frame[..AckStateRepFrame::WIRE_SIZE]) else {
        return;
    };
    let consumer_id = rep.body.consumer_id.get();
    let generation = rep.body.generation.get();
    let cursor = rep.body.cursor.get();
    let low_seq = rep.body.low_seq.get();
    let high_seq = rep.body.high_seq.get();

    if generation != inner.ackrel.generation_of(consumer_id) {
        // `ensure` on a new generation replaces the slot wholesale — the
        // purge invariant for a session/consumer change.
        inner.ackrel.ensure(consumer_id, generation);
        #[cfg(feature = "ack-persistence")]
        if let Some(cold) = inner.cold.clone() {
            tokio::task::spawn_blocking(move || {
                let mut store = cold.blocking_lock();
                let _ = store.purge_generation(consumer_id);
            });
        }
        return;
    }

    inner.ackrel.purge_up_to(consumer_id, cursor);
    // Below the broker's retention floor — will never be confirmable.
    inner.ackrel.purge_up_to(consumer_id, low_seq.saturating_sub(1));

    let pending = inner.ackrel.drain_ascending(consumer_id, usize::MAX);
    if let Some(&max_seq) = pending.last() {
        if max_seq > high_seq {
            inner.metrics.suspicious_seq_over_high.fetch_add(1, Ordering::Relaxed);
            warn_rate_limited(consumer_id, max_seq, high_seq);
        }
    }
    for chunk in pending.chunks(1000) {
        let seq = inner.seq_alloc.next();
        let out = WriteFrame::Mono(encode_ack_batch_v2(seq, consumer_id, generation, 0, chunk));
        let _ = crate::publish::enqueue(&inner.pool, out);
    }
}

/// `AckBatchResp`: broker confirms cumulative ack up to `new_cursor` for one
/// consumer — purge the confirmed range from the hot (and, when enabled,
/// cold) tier. `still_pending` is informational only for now.
fn dispatch_ack_batch_resp(inner: &Arc<Inner>, frame: &Bytes) {
    let Ok(rep) = AckBatchRespFrame::ref_from_bytes(&frame[..AckBatchRespFrame::WIRE_SIZE]) else {
        return;
    };
    let consumer_id = rep.body.consumer_id.get();
    let new_cursor = rep.body.new_cursor.get();

    let purged = inner.ackrel.purge_up_to(consumer_id, new_cursor);
    inner.metrics.acks_confirmed.fetch_add(purged as u64, Ordering::Relaxed);

    #[cfg(feature = "ack-persistence")]
    if let Some(cold) = inner.cold.clone() {
        let generation = inner.ackrel.generation_of(consumer_id);
        tokio::task::spawn_blocking(move || {
            let mut store = cold.blocking_lock();
            // Best-effort summary: the hot tier doesn't expose per-consumer
            // min/max/count, so this purge writes a placeholder — safe
            // because `consumer_state` is only a startup-preload hint and
            // gets fully reconciled by the next `write_batch`/`ensure` pass.
            let summary = crate::ackrel::cold::StateSummary {
                consumer_id,
                generation,
                count: 0,
                min_seq: 0,
                max_seq: 0,
            };
            let _ = store.purge_up_to(consumer_id, generation, new_cursor, summary);
        });
    }
}

/// Rate-limits the "deferred ack above broker high-water mark" warning to
/// at most once per 5s across all consumers — this fires on a protocol
/// anomaly, not per-message, but is still on the reader hot path.
static LAST_SUSPICIOUS_WARN_NS: AtomicU64 = AtomicU64::new(0);
const SUSPICIOUS_WARN_INTERVAL_NS: u64 = 5_000_000_000;

fn warn_rate_limited(consumer_id: u32, seq: u64, high_seq: u64) {
    let now = now_ns();
    let last = LAST_SUSPICIOUS_WARN_NS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < SUSPICIOUS_WARN_INTERVAL_NS {
        return;
    }
    if LAST_SUSPICIOUS_WARN_NS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        warn!(consumer_id, seq, high_seq, "deferred ack seq above broker high-water mark");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientConfig;
    use crate::state::{pending::Pending, seq::SeqAllocator, subscriptions::Subscriptions, Inner};
    use crate::transport::frame::{WriteFrame, MAX_WRITE_PRODUCERS, WRITE_QUEUE_CAP};
    use arbitro_kit::route::MpscAsync;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    /// Build a minimal `Inner` for use in unit tests (no real connection).
    fn make_inner(cancel: CancellationToken) -> Arc<Inner> {
        let (pool, _consumer, _shutdown) =
            MpscAsync::<WriteFrame, WRITE_QUEUE_CAP>::producer_pool(MAX_WRITE_PRODUCERS);
        let (ack_tx, _ack_rx) = tokio::sync::mpsc::channel(16);
        let (nack_tx, _nack_rx) = tokio::sync::mpsc::channel(16);
        Arc::new(Inner {
            cfg: ClientConfig::default(),
            pool,
            pending: Arc::new(Pending::new()),
            seq_alloc: SeqAllocator::new(),
            cancel: cancel.clone(),
            subscriptions: Arc::new(Subscriptions::new()),
            ack_tx,
            nack_tx,
            last_pong_ns: AtomicU64::new(0),
            metrics: Arc::new(crate::metrics::ClientMetrics::new()),
            cron_state: crate::cron::CronState::new(),
            session_cancel: std::sync::Mutex::new(None),
            ackrel: Arc::new(crate::ackrel::AckRelay::new()),
            #[cfg(feature = "ack-persistence")]
            cold: None,
        })
    }

    /// Build a raw v2 frame bytes for the given action + seq + body.
    fn make_frame(action: u16, seq: u64, body: &[u8]) -> Vec<u8> {
        let msg_len = body.len() as u32;
        let mut buf = vec![0u8; HEADER_SIZE + body.len()];
        buf[0..2].copy_from_slice(&action.to_le_bytes());
        // [2] flags = 0, [3] entry_flags = 0
        buf[4..8].copy_from_slice(&msg_len.to_le_bytes());
        buf[8..16].copy_from_slice(&seq.to_le_bytes());
        buf[HEADER_SIZE..].copy_from_slice(body);
        buf
    }

    /// Feed a frame split across two writes; the reader must reassemble it
    /// into exactly one dispatch call and resolve the pending slot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partial_frame_split_to_handles_boundary() {
        use arbitro_proto::action::Action;
        use std::time::Duration;

        let cancel = CancellationToken::new();
        let inner = make_inner(cancel.clone());

        // Register a pending slot for seq=55.
        let rx = inner.pending.register(55);

        // Build a RepOk frame: 8-byte body (all zeros → ref_seq = 0).
        let frame = make_frame(Action::RepOk.as_u16(), 55, &[0u8; 8]);
        assert_eq!(frame.len(), 24);

        // Set up a loopback TCP pair.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_h = tokio::spawn(async move { listener.accept().await.unwrap().0 });
        let writer = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (r_half, _) = accept_h.await.unwrap().into_split();
        let (_, mut w_half) = writer.into_split();

        // Spawn the reader task.
        tokio::spawn(reader_task(r_half, Arc::clone(&inner), cancel.clone()));

        // Write the first 8 bytes (only part of the header).
        w_half.write_all(&frame[..8]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Write the remaining 16 bytes.
        w_half.write_all(&frame[8..]).await.unwrap();

        // Pending must resolve exactly once.
        let result = tokio::time::timeout(Duration::from_millis(500), rx.recv_async())
            .await
            .expect("timed out waiting for RepOk")
            .expect("oneshot closed")
            .expect("wire error");

        assert_eq!(result.len(), 8);
        cancel.cancel();
    }
}
