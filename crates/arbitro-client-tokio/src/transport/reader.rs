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

use arbitro_proto::error::ErrorCode;

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
        // Guard the fixed-size slice: a truncated RepError (msg_len smaller than
        // the RepErr body) would panic the reader task on the slice below.
        if frame.len() < core::mem::size_of::<RepErrFrame>() {
            return;
        }
        if let Ok(rep) = RepErrFrame::ref_from_bytes(&frame[..core::mem::size_of::<RepErrFrame>()])
        {
            let code = rep.body.error_code.get();
            // Auth rejections are terminal and must be classified by CODE,
            // not by correlation: the broker sends them from the handshake
            // with `seq = 0`, which matches no pending request, so routing
            // them normally drops them silently. The connection then looks
            // like an ordinary drop and the supervisor redials forever with
            // a credential that will never work — hammering the broker once
            // per backoff, with nothing in the logs explaining why.
            if code == ErrorCode::AuthFailed as u16 || code == ErrorCode::AuthRequired as u16 {
                tracing::error!(
                    error_code = format_args!("0x{code:04x}"),
                    "broker rejected authentication — not reconnecting; \
                     check `auth_token` / ARBITRO_TOKEN against the broker's \
                     ARBITRO_AUTH_TOKEN or ARBITRO_AUTH_USERS"
                );
                // Cancels the whole client, not just this session: the
                // reconnect loop checks this token and returns instead of
                // backing off. Retrying is pointless — only a config change
                // can fix a wrong token.
                inner.cancel.cancel();
                inner.pending.drain_disconnected();
                return;
            }
            inner.pending.complete_err(req_seq, code);
        } else {
            inner.pending.complete_err(req_seq, 0);
        }
        return;
    }

    // ListStreams / ListConsumers / RepSubscribeBatch / ShardTopology —
    // body is the raw payload, handed to the caller to decode. These carry
    // their own action code rather than RepOk, so they need naming here or
    // the round-trip never completes.
    if action == Action::ListStreams.as_u16()
        || action == Action::ListConsumers.as_u16()
        || action == Action::RepSubscribeBatch.as_u16()
        || action == Action::ShardTopology.as_u16()
    {
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
    use arbitro_proto::v2::ingress::ack_state::ACK_STATUS_OK;

    if frame.len() < AckStateRepFrame::WIRE_SIZE {
        return; // truncated frame — don't panic the reader on the slice
    }
    let Ok(rep) = AckStateRepFrame::ref_from_bytes(&frame[..AckStateRepFrame::WIRE_SIZE]) else {
        return;
    };
    let consumer_id = rep.body.consumer_id.get();
    let generation = rep.body.generation.get();
    let cursor = rep.body.cursor.get();
    let low_seq = rep.body.low_seq.get();
    let high_seq = rep.body.high_seq.get();
    let status = rep.body.status.get();

    // ── Durable dedup: on-connect WAL purge (cold path) ────────────────
    //
    // The broker's cursor is authoritative for the consumer currently
    // registered under this id: it never (re)delivers seqs <= cursor to
    // it, so dropping them from the WAL live set can never cause a
    // duplicate execution. This runs BEFORE (and independent of) the
    // ackrel generation check below: the WAL slot is keyed by durable
    // `(stream, consumer)` names, and a fresh process (in-memory ackrel
    // generation 0, broker generation possibly bumped by consumer-id
    // recycling) must still purge entries recorded by a previous, dead
    // session — that reconnect purge is the whole point of the
    // on-(re)connect `AckStateReq`.
    //
    // Deliberate conservatism: the purge only runs when the broker
    // vouches for the cursor (`status == OK`). On any other status —
    // notably `ACK_STATUS_CONSUMER_UNKNOWN` (consumer deleted, or broker
    // restarted without it) — we purge NOTHING, even though the entries
    // are probably useless: a recreated same-name consumer will answer a
    // later request with `OK` and its own cursor, and stale high-seq
    // entries are left to the store TTL. A wrongly kept entry costs a
    // little disk; a wrongly dropped one costs a duplicate execution of
    // real work.
    if status == ACK_STATUS_OK {
        if let Some(slot) = inner.subscriptions.dedup_of(consumer_id) {
            if let Err(e) = slot.confirm_up_to(cursor) {
                inner
                    .metrics
                    .ackstore_errors
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(consumer_id, error = %e, "ackstore: confirm_up_to failed");
            }
        }
    }

    if generation != inner.ackrel.generation_of(consumer_id) {
        // `ensure` on a new generation replaces the slot wholesale — the
        // purge invariant for a session/consumer change.
        inner.ackrel.ensure(consumer_id, generation);
        return;
    }

    inner.ackrel.purge_up_to(consumer_id, cursor);
    // Below the broker's retention floor — will never be confirmable.
    inner
        .ackrel
        .purge_up_to(consumer_id, low_seq.saturating_sub(1));

    let pending = inner.ackrel.drain_ascending(consumer_id, usize::MAX);
    if let Some(&max_seq) = pending.last() {
        if max_seq > high_seq {
            inner
                .metrics
                .suspicious_seq_over_high
                .fetch_add(1, Ordering::Relaxed);
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
/// consumer — purge the confirmed range from the hot ack tier and (when a
/// durable ackstore is configured) drop the confirmed seqs from its live set.
/// `still_pending` is informational only for now.
fn dispatch_ack_batch_resp(inner: &Arc<Inner>, frame: &Bytes) {
    if frame.len() < AckBatchRespFrame::WIRE_SIZE {
        return; // truncated frame — don't panic the reader on the slice
    }
    let Ok(rep) = AckBatchRespFrame::ref_from_bytes(&frame[..AckBatchRespFrame::WIRE_SIZE]) else {
        return;
    };
    let consumer_id = rep.body.consumer_id.get();
    let new_cursor = rep.body.new_cursor.get();

    let purged = inner.ackrel.purge_up_to(consumer_id, new_cursor);
    inner
        .metrics
        .acks_confirmed
        .fetch_add(purged as u64, Ordering::Relaxed);

    // Durable dedup cleanup: the broker confirmed cumulative ack up to
    // `new_cursor`, so those seqs are safe to drop from the WAL live set.
    if let Some(slot) = inner.subscriptions.dedup_of(consumer_id) {
        if let Err(e) = slot.confirm_up_to(new_cursor) {
            inner
                .metrics
                .ackstore_errors
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(consumer_id, error = %e, "ackstore: confirm_up_to failed");
        }
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
        warn!(
            consumer_id,
            seq, high_seq, "deferred ack seq above broker high-water mark"
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::state::subscriptions::Registration;
    use super::*;
    use crate::config::ClientConfig;
    use crate::state::{pending::Pending, seq::SeqAllocator, subscriptions::Subscriptions, Inner};
    use crate::transport::frame::{WritePool, WRITE_QUEUE_CAP};
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    /// Build a minimal `Inner` for use in unit tests (no real connection).
    fn make_inner(cancel: CancellationToken) -> Arc<Inner> {
        make_inner_with_store(cancel, None)
    }

    fn make_inner_with_store(
        cancel: CancellationToken,
        ack_store: Option<Arc<dyn crate::ackstore::Store>>,
    ) -> Arc<Inner> {
        let (pool, _consumer) = WritePool::new(8, WRITE_QUEUE_CAP);
        let (ack_tx, _ack_rx) = tokio::sync::mpsc::channel(16);
        let (nack_tx, _nack_rx) = tokio::sync::mpsc::channel(16);
        Arc::new(Inner {
            cfg: ClientConfig::default(),
            pool,
            pending: Arc::new(Pending::new()),
            seq_alloc: SeqAllocator::new(),
            sub_id_alloc: crate::state::seq::SubIdAllocator::new(),
            cancel: cancel.clone(),
            subscriptions: Arc::new(Subscriptions::new()),
            ack_tx,
            nack_tx,
            last_pong_ns: AtomicU64::new(0),
            metrics: Arc::new(crate::metrics::ClientMetrics::new()),
            cron_state: crate::cron::CronState::new(),
            session_cancel: std::sync::Mutex::new(None),
            ackrel: Arc::new(crate::ackrel::AckRelay::new()),
            ack_store,
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

    // ── AckStateRep → ackstore purge (reconnect-purge feature) ──────────

    /// Set up an `Inner` with a memory ackstore, one slotted subscription
    /// for `cid`, and the given pre-recorded seqs (simulating entries left
    /// by a dead session that was never confirmed).
    fn make_purge_fixture(
        cid: u32,
        seqs: &[u64],
    ) -> (Arc<Inner>, Arc<dyn crate::ackstore::SlotRef>) {
        let store: Arc<dyn crate::ackstore::Store> =
            Arc::new(crate::ackstore::memory::MemoryStore::new(0));
        let slot = store.slot("s", "c").unwrap();
        for &s in seqs {
            slot.record(s).unwrap();
        }
        let inner = make_inner_with_store(CancellationToken::new(), Some(store));
        let _rx = inner
            .subscriptions
            .register(Registration {
                consumer_id: cid,
                stream_id: 1,
                fanout: false,
                dedup: Some(slot.clone()),
                sub_id: cid,
                filter: b"",
                frame: Bytes::new(),
            });
        (inner, slot)
    }

    fn ack_state_rep_bytes(
        cid: u32,
        generation: u32,
        cursor: u64,
        low_seq: u64,
        high_seq: u64,
        status: u32,
    ) -> Bytes {
        let f = AckStateRepFrame::new(1, cid, generation, cursor, low_seq, high_seq, status);
        Bytes::copy_from_slice(zerocopy::IntoBytes::as_bytes(&f))
    }

    /// The on-connect purge must run even when the broker's generation does
    /// not match the local (fresh-process) ackrel generation: the WAL slot is
    /// keyed by durable names, so entries recorded by a dead session are
    /// dropped up to the broker's cursor while entries above it survive.
    #[test]
    fn ack_state_rep_purges_ackstore_despite_generation_mismatch() {
        let (inner, slot) = make_purge_fixture(3, &[5, 10, 15, 10_000]);

        // Broker generation 7 vs local ackrel generation 0 → mismatch.
        let frame = ack_state_rep_bytes(3, 7, 15, 1, 20, 0 /* ACK_STATUS_OK */);
        dispatch_ack_state_rep(&inner, &frame);

        let info = slot.info();
        assert_eq!(info.live, 1, "entries <= cursor(15) must be dropped");
        assert_eq!(info.min_seq, 10_000, "entries above the cursor must survive");
        assert!(!slot.seen(5) && !slot.seen(10) && !slot.seen(15));
        assert!(slot.seen(10_000));
    }

    /// When the broker does NOT vouch for the cursor (status != OK, e.g. the
    /// consumer no longer exists), nothing may be purged — even if the frame
    /// carries a non-zero cursor. Conservative by design: a wrongly kept
    /// entry costs disk, a wrongly dropped one costs a duplicate execution.
    #[test]
    fn ack_state_rep_non_ok_status_purges_nothing() {
        let (inner, slot) = make_purge_fixture(4, &[5, 10, 15, 10_000]);

        // Adversarial frame: CONSUMER_UNKNOWN but cursor = 10_000.
        let frame = ack_state_rep_bytes(
            4, 0, 10_000, 0, 0, 3, /* ACK_STATUS_CONSUMER_UNKNOWN */
        );
        dispatch_ack_state_rep(&inner, &frame);

        let info = slot.info();
        assert_eq!(info.live, 4, "no entry may be dropped without an OK status");
        assert!(slot.seen(5) && slot.seen(10) && slot.seen(15) && slot.seen(10_000));
    }

    /// Matching generation (steady-state reconnect) also purges — the
    /// original primary-path behavior must be preserved.
    #[test]
    fn ack_state_rep_matching_generation_still_purges() {
        let (inner, slot) = make_purge_fixture(5, &[1, 2, 3, 400]);

        // ackrel generation for an untouched consumer is 0; broker also 0.
        let frame = ack_state_rep_bytes(5, 0, 3, 1, 400, 0);
        dispatch_ack_state_rep(&inner, &frame);

        let info = slot.info();
        assert_eq!(info.live, 1);
        assert_eq!(info.min_seq, 400);
    }
}
