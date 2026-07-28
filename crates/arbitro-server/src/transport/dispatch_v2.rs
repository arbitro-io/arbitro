//! v2 frame dispatch — sole dispatcher in the server.
//!
//! HELLO is mandatory at the start of every connection (`server.rs::read_loop`
//! enforces it). All subsequent traffic is `Header`-prefixed v2 frames.
//!
//! Scope:
//!   * Hot path:  Publish, PublishBatch, PublishWithReply, Ack, BatchAck, Subscribe, Unsubscribe
//!   * Mgmt:      CreateStream/DeleteStream/GetStream/PurgeStream/DrainSubject/ListStreams
//!                CreateConsumer/DeleteConsumer/GetConsumer/ListConsumers
//!   * System:    Disconnect, Ping, Pong (no-op or trivial reply)
//!
//! ## Dropped (intentional v1→v2 regression)
//!
//!   * `Nack`/`BatchNack` — now implemented (Action::Nack/BatchNack handlers).
//!   * `AckSync`/`BatchAckSync` — collapsed into fire-and-forget Ack/BatchAck.
//!   * `PublishAccumulate` — accumulator path is v1-only for now.
//!   * `PublishWithReply` — now implemented (request/reply RPC).
//!   * `PublishWithHeaders` / `PublishBatchWithHeaders` — deleted §5.1.
//!
//! ## Notable wire-shape compromises
//!
//!   * **Unsubscribe** has no dedicated v2 frame. Clients send the body of a
//!     `SubFrame` with `Action::Unsubscribe` in the header — same shape, the
//!     decoder branches on `Action::from_u16(header.action)`.
//!   * **Ack/BatchAck** bodies do **not** carry `stream_id`. We recover it
//!     via `names().consumer_stream(consumer_id)`, populated by
//!     `CreateConsumer` (v2). If the consumer was never created via v2 the
//!     ack is silently dropped — fire-and-forget contract.

use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine_v2::types::*;
use arbitro_engine_v2::AckEntry;
use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::v2::header::{Header, HEADER_SIZE};
use arbitro_proto::v2::ingress::ack_frame::{AckFrame, BatchAckFrame};
use arbitro_proto::v2::ingress::ack_state::{
    AckBatchFrame, AckStateReqFrame, ACK_BATCH_MAX_SEQS, ACK_STATUS_BATCH_TOO_LARGE,
    ACK_STATUS_CONSUMER_UNKNOWN, ACK_STATUS_OK,
};
use arbitro_proto::v2::ingress::batch_pub_frame::BatchPubFrame;
use arbitro_proto::v2::ingress::nack_frame::{BatchNackFrame, NackFrame};
use arbitro_proto::v2::ingress::pub_delayed_frame::PubDelayedFrame;
use arbitro_proto::v2::ingress::pub_frame::PubFrame;
use arbitro_proto::v2::ingress::pub_with_reply::PubWithReplyFrame;
use arbitro_proto::v2::manager::consumer_mgmt::CreateConsumerFrame;
use arbitro_proto::v2::manager::stream_mgmt::CreateStreamFrame;
use bytes::{Bytes, BytesMut};
use serde_json;
use zerocopy::FromBytes;
use zerocopy::IntoBytes;

// Used by metadata match arms once the Raft propose path is wired.
#[cfg(feature = "cluster")]
#[allow(unused_imports)]
use crate::cluster::ClusterState;

use crate::common::reply_v2::{
    send_ack_batch_resp_v2, send_ack_state_rep_v2, send_error_v2, send_rep_ok_v2,
};
use crate::shard::router::ShardRouter;
use crate::transport::ConnectionRegistry;

use arbitro_proto::metadata::{
    build_create_consumer, build_create_stream, build_delete_consumer, build_delete_stream,
};

/// Dispatch one v2 frame. `frame` covers `[Header(16) || body(msg_len)]`.
pub async fn dispatch_frame_v2(
    conn_id: u64,
    frame: Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    cron_registry: &std::sync::Arc<crate::cron::CronRegistry>,
    delayed_journal: &Option<crate::delayed::SharedDelayedJournal>,
    #[cfg(feature = "cluster")] cluster_state: &std::sync::Arc<crate::cluster::ClusterState>,
) -> Result<(), ()> {
    if frame.len() < HEADER_SIZE {
        return Err(());
    }
    let header = match Header::ref_from_bytes(&frame[..HEADER_SIZE]) {
        Ok(h) => h,
        Err(_) => return Err(()),
    };
    let action = match Action::from_u16(header.action.get()) {
        Some(a) => a,
        None => {
            send_error_v2(
                registry,
                conn_id,
                header.seq.get(),
                ErrorCode::UnknownAction,
            );
            return Err(());
        }
    };
    let req_seq = header.seq.get();

    // H16: per-dispatch tracing event. Compiles to a near no-op when
    // the subscriber filter excludes TRACE (an atomic load + branch).
    // We use an event instead of a span guard so that nothing has to
    // be held across the `.await` points in the match arms (a Span
    // `Entered` guard is `!Send`). The event captures the same fields
    // a span would and is sufficient for per-dispatch tracing.
    tracing::event!(
        tracing::Level::TRACE,
        conn_id,
        req_seq,
        action = ?action,
        "dispatch_v2"
    );

    match action {
        // ── Hot path ────────────────────────────────────────────────
        Action::Publish => v2_publish(
            conn_id,
            req_seq,
            &frame,
            server,
            registry,
        ),
        Action::PublishBatch => v2_publish_batch(conn_id, req_seq, &frame, server, registry),
        Action::PublishWithReply => {
            v2_publish_with_reply(conn_id, req_seq, &frame, server, registry)
        }
        Action::Ack => v2_ack(conn_id, &frame, server).await,
        Action::AckTerm => v2_ack_term(conn_id, &frame, server).await,
        Action::BatchAck => v2_batch_ack(conn_id, &frame, server).await,
        Action::AckStateReq => v2_ack_state(conn_id, req_seq, &frame, server, registry).await,
        Action::AckBatch => v2_ack_batch(conn_id, req_seq, &frame, server, registry).await,
        Action::Nack => v2_nack(conn_id, &frame, server).await,
        Action::BatchNack => v2_batch_nack(conn_id, &frame, server).await,
        Action::Subscribe => v2_subscribe(conn_id, req_seq, &frame, server, registry).await,
        Action::Unsubscribe => v2_unsubscribe(conn_id, req_seq, &frame, server, registry).await,

        // ── Stream management ───────────────────────────────────────
        Action::CreateStream => {
            #[cfg(feature = "cluster")]
            if cluster_state.is_clustered() {
                v2_create_stream_raft(conn_id, req_seq, &frame, server, registry, cluster_state)
                    .await;
            } else {
                v2_create_stream(conn_id, req_seq, &frame, server, registry).await;
            }
            #[cfg(not(feature = "cluster"))]
            v2_create_stream(conn_id, req_seq, &frame, server, registry).await;
        }
        Action::DeleteStream => {
            #[cfg(feature = "cluster")]
            if cluster_state.is_clustered() {
                v2_delete_stream_raft(conn_id, req_seq, &frame, server, registry, cluster_state)
                    .await;
            } else {
                v2_delete_stream(conn_id, req_seq, &frame, server, registry).await;
            }
            #[cfg(not(feature = "cluster"))]
            v2_delete_stream(conn_id, req_seq, &frame, server, registry).await;
        }
        Action::GetStream => v2_get_stream(conn_id, req_seq, &frame, server, registry).await,
        Action::PurgeStream => v2_purge_stream(conn_id, req_seq, &frame, server, registry).await,
        Action::DrainSubject => v2_drain_subject(conn_id, req_seq, &frame, server, registry).await,
        Action::DeleteMessage => {
            v2_delete_message(conn_id, req_seq, &frame, server, registry).await
        }
        Action::ListStreams => v2_list_streams(conn_id, req_seq, &frame, server, registry).await,

        // ── Consumer management ─────────────────────────────────────
        Action::CreateConsumer => {
            #[cfg(feature = "cluster")]
            if cluster_state.is_clustered() {
                v2_create_consumer_raft(conn_id, req_seq, &frame, server, registry, cluster_state)
                    .await;
            } else {
                v2_create_consumer(conn_id, req_seq, &frame, server, registry).await;
            }
            #[cfg(not(feature = "cluster"))]
            v2_create_consumer(conn_id, req_seq, &frame, server, registry).await;
        }
        Action::DeleteConsumer => {
            #[cfg(feature = "cluster")]
            if cluster_state.is_clustered() {
                v2_delete_consumer_raft(conn_id, req_seq, &frame, server, registry, cluster_state)
                    .await;
            } else {
                v2_delete_consumer(conn_id, req_seq, &frame, server, registry).await;
            }
            #[cfg(not(feature = "cluster"))]
            v2_delete_consumer(conn_id, req_seq, &frame, server, registry).await;
        }
        Action::GetConsumer => v2_get_consumer(conn_id, req_seq, &frame, server, registry).await,
        Action::ListConsumers => {
            v2_list_consumers(conn_id, req_seq, &frame, server, registry).await
        }
        Action::ConsumerStats => {
            v2_consumer_stats(conn_id, req_seq, &frame, server, registry).await
        }
        Action::PauseConsumer => {
            v2_pause_consumer(conn_id, req_seq, &frame, server, registry).await
        }
        Action::ResumeConsumer => {
            v2_resume_consumer(conn_id, req_seq, &frame, server, registry).await
        }

        // ── Delayed publish ─────────────────────────────────────────
        Action::PublishDelayed => {
            v2_publish_delayed(conn_id, req_seq, &frame, server, registry, delayed_journal)
        }

        // ── Cron scheduling ─────────────────────────────────────────
        Action::CreateCron => v2_create_cron(conn_id, req_seq, &frame, registry, cron_registry),
        Action::DeleteCron => v2_delete_cron(conn_id, req_seq, &frame, registry, cron_registry),
        Action::ListCrons => v2_list_crons(conn_id, req_seq, registry, cron_registry),
        Action::CronAck => v2_cron_ack(&frame, cron_registry),
        Action::CronFire => { /* server→client only; ignore if received */ }

        // ── System ──────────────────────────────────────────────────
        Action::Disconnect => {
            v2_disconnect(conn_id, server, registry, cron_registry).await;
        }
        Action::Ping => v2_ping(conn_id, registry),
        // M17: count Pongs so the keepalive path is observable. The
        // counter lives on the connection registry — it's stable across
        // the lifetime of the conn and the read loop already touches
        // the registry on every frame.
        Action::Pong => {
            registry.touch(conn_id);
        }

        // L1 / L2: AckSync / BatchAckSync, PublishAccumulate,
        // FanoutBatch — have wire codes but no dispatcher. Reply
        // `Unimplemented` so the client gets a stable, distinct error
        // instead of UnknownAction.
        _ => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::Unimplemented);
            return Err(());
        }
    }
    Ok(())
}

// ── Hot path ───────────────────────────────────────────────────────────────

fn v2_publish(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let f = match PubFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::BufferTooShort);
            return;
        }
    };
    // B4: bounds-check subject_len + msg_id_len against tail BEFORE
    // touching subject() / msg_id() / payload(). Without this a crafted
    // frame with subject_len > tail.len() panics the broker.
    if let Err(code) = f.validate() {
        send_error_v2(registry, conn_id, req_seq, code);
        return;
    }
    let wire_stream = f.body.stream_id.get();
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound);
            return;
        }
    };

    // ── Idempotency check (fast-bail) ─────────────────────────────────
    //
    // Two early-outs make non-idempotent publishes free:
    //   1. The stream's window is 0  → skip (most streams).
    //   2. The frame carries no msg_id → skip (legacy publishers).
    //
    // Only when BOTH a window AND a msg_id exist do we hash the id
    // and consult the shared tracker. The lock is held for the
    // membership check + insert only — sub-microsecond on a hash
    // miss, single-digit microseconds on a hash hit.
    //
    // When the client sends a pre-encoded ExtendedPayload (entry_flags
    // has HAS_HEADERS), the msg-id lives inside the TLV block under
    // key HDR_MSG_ID, not in the frame's dedicated msg_id field. Fall
    // back to the payload header extraction so a client that stamps
    // msg-id via headers gets deduped just like a legacy publisher
    // that fills f.msg_id() directly.
    let frame_msg_id = f.msg_id();
    let has_headers_flag = f.header.entry_flags
        & arbitro_proto::v2::header::entry_flag::HAS_HEADERS
        != 0;
    let msg_id: &[u8] = if !frame_msg_id.is_empty() {
        frame_msg_id
    } else if has_headers_flag {
        arbitro_proto::wire::msg_headers::ExtendedPayload::ref_from_bytes(f.payload())
            .ok()
            .and_then(|ext| ext.headers_block())
            .and_then(|hdrs| hdrs.get(arbitro_proto::wire::msg_headers::HDR_MSG_ID))
            .unwrap_or(&[])
    } else {
        &[]
    };
    let window_ms = server.names().stream_idempotency_window_ms(seq_stream);
    if window_ms > 0 && !msg_id.is_empty() {
        let hash = idempotency_hash(msg_id);
        // F26: per-stream lock. Different streams contend on different
        // mutexes. The outer map read-lock + Arc clone is sub-µs in
        // steady state (no allocation, no contention).
        let shared = server.idempotency_for(seq_stream);
        let tracker_arc = crate::shard::idempotency::idempotency_for_stream(shared, seq_stream);
        let mut t = tracker_arc.lock();
        // F10: announce allocation so the worker's select! predicate
        // stops paying the lock to test Option::is_some.
        server.mark_idempotency_allocated(seq_stream);
        // M2: pass the full msg_id so a hash collision between two
        // distinct ids doesn't silently dedup the second publish.
        if !t.record(seq_stream, hash, msg_id, window_ms) {
            drop(t);
            send_error_v2(registry, conn_id, req_seq, ErrorCode::IdempotencyDuplicate);
            return;
        }
        drop(t);
    }

    // ── Stream quota pre-check (DiscardPolicy::New) ────────────────────
    // If the stream has DiscardPolicy::New (discard == 1) and the store
    // would exceed max_msgs or max_bytes, reject BEFORE appending.
    if let Some(quota) = server.names().stream_quota(seq_stream) {
        if quota.discard == 1 {
            let shared_store = server.store_for(seq_stream);
            let info = shared_store.lock().info();
            if quota.max_msgs > 0 && info.messages >= quota.max_msgs {
                send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
                return;
            }
            let entry_bytes = (f.subject().len() + f.payload().len()) as u64;
            if quota.max_bytes > 0 && info.bytes + entry_bytes > quota.max_bytes {
                send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
                return;
            }
        }
    }

    // ── Headers / ExtendedPayload resolution ────────────────────────────
    //
    // Three cases, checked in priority order:
    //  1. Client already sent pre-encoded ExtendedPayload (entry_flags has
    //     HAS_HEADERS) → store payload as-is. msg_id for dedup was already
    //     extracted from the frame field above.
    //  2. Frame carries a msg_id but no client-side headers → server wraps
    //     payload + msg_id header into ExtendedPayload for journal recovery.
    //  3. Neither → store raw payload, no flags.
    let mut ext_buf: smallvec::SmallVec<[u8; 256]> = smallvec::SmallVec::new();
    let (store_payload, store_flags): (&[u8], u8) =
        if f.header.entry_flags & arbitro_proto::v2::header::entry_flag::HAS_HEADERS != 0 {
            (f.payload(), arbitro_store::flags::HAS_HEADERS)
        } else if !msg_id.is_empty() {
            let hdrs: [(&[u8], &[u8]); 1] = [(
                arbitro_proto::wire::msg_headers::HDR_MSG_ID,
                msg_id,
            )];
            let section = arbitro_proto::wire::msg_headers::HeadersBlock::section_size(&hdrs);
            let wire_size = arbitro_proto::wire::msg_headers::ExtendedPayload::wire_size(
                f.payload().len(),
                section,
            );
            ext_buf.resize(wire_size, 0);
            arbitro_proto::wire::msg_headers::encode_extended_payload(
                &mut ext_buf,
                f.payload(),
                &hdrs,
            );
            (&ext_buf, arbitro_store::flags::HAS_HEADERS)
        } else {
            (&[], 0)
        };

    let entries = [arbitro_store::EntryRef {
        stream_id: seq_stream.raw(),
        subject: f.subject(),
        payload: if store_flags != 0 { store_payload } else { f.payload() },
        flags: store_flags,
        deliver_at_ms: 0,
    }];

    // F7: single relaxed atomic load instead of SystemTime::now() syscall.
    let now_ms = server.now_ms();

    // F2: drop block_in_place. The store mutex is uncontended in steady
    // state (drain takes it once per cycle); append_batch is a mmap memcpy
    // (sub-µs). The block_in_place wrapper costs more than the work it
    // guards. parking_lot::Mutex gives a faster uncontested path.
    let shared_store = server.store_for(seq_stream);
    let first_seq = match shared_store.lock().append_batch(&entries, now_ms) {
        Ok(seq) => seq,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
            return;
        }
    };

    send_rep_ok_v2(registry, conn_id, req_seq, first_seq);
    server.gate_for(seq_stream).release();
}

/// Hash an opaque `msg_id` for the idempotency tracker.
///
/// We don't need cryptographic strength — false positives on
/// `IdempotencyTracker::record` would only mis-reject a legitimate
/// publish (rare; the broker reports `IdempotencyDuplicate` and the
/// client can retry with a different id). foldhash is the same hasher
/// we use for all the broker's HashMaps; using it here keeps the
/// codebase consistent.
#[inline]
pub(crate) fn idempotency_hash(msg_id: &[u8]) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = arbitro_common::foldhash::fast::FixedState::default().build_hasher();
    h.write(msg_id);
    h.finish()
}

fn v2_publish_with_reply(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let f = match PubWithReplyFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::BufferTooShort);
            return;
        }
    };
    if let Err(code) = f.validate() {
        send_error_v2(registry, conn_id, req_seq, code);
        return;
    }
    let wire_stream = f.body.stream_id.get();
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound);
            return;
        }
    };

    // M10: idempotency for PublishWithReply — same pattern as v2_publish.
    // Fast-bail when no per-stream window or no msg_id.
    let msg_id = f.msg_id();
    let window_ms = server.names().stream_idempotency_window_ms(seq_stream);
    if window_ms > 0 && !msg_id.is_empty() {
        let hash = idempotency_hash(msg_id);
        let shared = server.idempotency_for(seq_stream);
        let tracker_arc = crate::shard::idempotency::idempotency_for_stream(shared, seq_stream);
        let mut t = tracker_arc.lock();
        server.mark_idempotency_allocated(seq_stream);
        if !t.record(seq_stream, hash, msg_id, window_ms) {
            drop(t);
            send_error_v2(registry, conn_id, req_seq, ErrorCode::IdempotencyDuplicate);
            return;
        }
        drop(t);
    }

    // F5: Encode reply_to into the payload prefix using a `SmallVec` —
    // most reply addresses + small payloads fit inline (no heap alloc).
    // Format: [reply_len:u16 LE][reply_to][payload]. The drain extracts
    // this using the HAS_REPLY_TO flag.
    let reply_to = f.reply_to();
    let payload = f.payload();
    let mut combined_payload: smallvec::SmallVec<[u8; 256]> =
        smallvec::SmallVec::with_capacity(2 + reply_to.len() + payload.len());
    combined_payload.extend_from_slice(&(reply_to.len() as u16).to_le_bytes());
    combined_payload.extend_from_slice(reply_to);
    combined_payload.extend_from_slice(payload);

    let entries = [arbitro_store::EntryRef {
        stream_id: seq_stream.raw(),
        subject: f.subject(),
        payload: &combined_payload,
        flags: arbitro_store::flags::HAS_REPLY_TO,
        deliver_at_ms: 0,
    }];

    // F7: SharedClock atomic load.
    let now_ms = server.now_ms();

    // F2: drop block_in_place; parking_lot::Mutex is uncontested fast.
    let shared_store = server.store_for(seq_stream);
    let first_seq = match shared_store.lock().append_batch(&entries, now_ms) {
        Ok(seq) => seq,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
            return;
        }
    };

    send_rep_ok_v2(registry, conn_id, req_seq, first_seq);
    server.gate_for(seq_stream).release();
}

fn v2_publish_batch(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let f = match BatchPubFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::BufferTooShort);
            return;
        }
    };
    // B3: walk the iterator once to count yielded entries; if fewer
    // than `count` come back, the frame's per-entry length fields are
    // inconsistent and we reject with InvalidEntryCount BEFORE any
    // store mutation. The iterator validates each entry safely (no
    // panic) — we just check it ran to completion.
    {
        let expected = f.body.count.get();
        let actual: u32 = f.iter().count() as u32;
        if actual != expected {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidEntryCount);
            return;
        }
    }
    let wire_stream = f.body.stream_id.get();
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound);
            return;
        }
    };

    // ── Stream quota pre-check (DiscardPolicy::New) ────────────────────
    //
    // v2_publish (single) rejects an over-quota publish on
    // `DiscardPolicy::New` (discard == 1) streams before appending. The
    // batch path must apply the same gate so single and batch behave
    // consistently — a discard=1 stream must not silently accept a batch
    // that exceeds its quota when the same messages sent one-by-one
    // would be rejected. Bytes are the sum of (subject_len + payload_len)
    // across the batch, mirroring the single-publish accounting.
    if let Some(quota) = server.names().stream_quota(seq_stream) {
        if quota.discard == 1 {
            let batch_count = f.body.count.get() as u64;
            let mut batch_bytes: u64 = 0;
            for v in f.iter() {
                batch_bytes += (v.subject().len() + v.payload().len()) as u64;
            }
            let shared_store = server.store_for(seq_stream);
            let info = shared_store.lock().info();
            if quota.max_msgs > 0 && info.messages + batch_count > quota.max_msgs {
                send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
                return;
            }
            if quota.max_bytes > 0 && info.bytes + batch_bytes > quota.max_bytes {
                send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
                return;
            }
        }
    }

    // ── Idempotency check (all-or-nothing) ────────────────────────────
    //
    // F6: stream-build EntryRef vec directly from `f.iter()` —
    // dropping the materialised entry_views Vec on the non-idempotent
    // fast path. The iterator (`BatchPubIter`) is `Copy`, so the
    // idempotency branch can iterate twice without an extra alloc.
    //
    // Fast-bail when the stream has no idempotency window.
    //
    // When the batch header carries HAS_HEADERS, every entry's payload
    // is a pre-encoded ExtendedPayload and the per-entry msg-id lives
    // inside its TLV block under HDR_MSG_ID rather than in the entry's
    // dedicated msg_id field (same asymmetry as the single-publish path,
    // BUG-8). `msg_id_of_view` centralizes that lookup so both the
    // has-any pre-check and the record loop see the same effective id.
    let batch_has_headers = f.header.entry_flags
        & arbitro_proto::v2::header::entry_flag::HAS_HEADERS
        != 0;
    // Named fn instead of a closure because the msg_id / payload slices
    // borrow from the frame's `'a` lifetime, which closures can't express
    // with a higher-rank bound.
    fn msg_id_of_view<'a>(
        v: &arbitro_proto::v2::ingress::batch_pub_frame::BatchPubEntryView<'a>,
        batch_has_headers: bool,
    ) -> &'a [u8] {
        let field = v.msg_id();
        if !field.is_empty() {
            return field;
        }
        if !batch_has_headers {
            return &[];
        }
        arbitro_proto::wire::msg_headers::ExtendedPayload::ref_from_bytes(v.payload())
            .ok()
            .and_then(|ext| ext.headers_block())
            .and_then(|hdrs| hdrs.get(arbitro_proto::wire::msg_headers::HDR_MSG_ID))
            .unwrap_or(&[])
    }
    let window_ms = server.names().stream_idempotency_window_ms(seq_stream);
    if window_ms > 0
        && f.iter()
            .any(|v| !msg_id_of_view(&v, batch_has_headers).is_empty())
    {
        let shared = server.idempotency_for(seq_stream);
        let tracker_arc = crate::shard::idempotency::idempotency_for_stream(shared, seq_stream);
        let mut tracker = tracker_arc.lock();
        server.mark_idempotency_allocated(seq_stream);

        // M2: track inserted `(hash, msg_id bytes)` for rollback on
        // duplicate. We hold the msg_id slice borrowed from `frame`
        // (lives for the duration of this dispatch), so the rollback
        // doesn't need owned copies — except `forget` expects a slice,
        // which we still have.
        let mut inserted: smallvec::SmallVec<[(u64, &[u8]); 16]> = smallvec::SmallVec::new();
        let mut duplicate = false;
        for v in f.iter() {
            let id = msg_id_of_view(&v, batch_has_headers);
            if id.is_empty() {
                continue;
            }
            let hash = idempotency_hash(id);
            if !tracker.record(seq_stream, hash, id, window_ms) {
                duplicate = true;
                break;
            }
            inserted.push((hash, id));
        }
        if duplicate {
            for (hash, id) in &inserted {
                tracker.forget(seq_stream, *hash, id);
            }
            drop(tracker);
            send_error_v2(registry, conn_id, req_seq, ErrorCode::IdempotencyDuplicate);
            return;
        }
        drop(tracker);
    }

    // Stream-build EntryRef vec — one allocation, no intermediate
    // entry_views Vec. SmallVec inline storage absorbs small batches.
    //
    // Storage flags mirror the single-publish resolution (audit #9 —
    // entries used to be stored with `flags: 0` unconditionally, so a
    // HAS_HEADERS batch delivered the raw ExtendedPayload TLV bytes to
    // consumers and the restart dedup rebuild missed its msg-ids):
    //  1. Batch carries HAS_HEADERS → every payload is a pre-encoded
    //     ExtendedPayload; store as-is WITH the HAS_HEADERS flag so the
    //     drain unwraps it and recovery finds the msg-ids.
    //  2. Entry carries a dedicated-field msg_id → server wraps
    //     payload + msg-id header into ExtendedPayload (same as the
    //     single-publish case 2) so dedup survives restart.
    //  3. Neither → raw payload, no flags.
    // The wrap buffers are only allocated when at least one entry
    // actually carries a dedicated-field msg_id (cold, dedup-only path).
    let wrapped_payloads: Vec<Option<Vec<u8>>> = if !batch_has_headers
        && f.iter().any(|v| !v.msg_id().is_empty())
    {
        f.iter()
            .map(|v| {
                let id = v.msg_id();
                if id.is_empty() {
                    None
                } else {
                    Some(arbitro_proto::wire::msg_headers::encode_extended_payload_vec(
                        v.payload(),
                        &[(arbitro_proto::wire::msg_headers::HDR_MSG_ID, id)],
                    ))
                }
            })
            .collect()
    } else {
        Vec::new()
    };
    let entries: smallvec::SmallVec<[arbitro_store::EntryRef<'_>; 16]> = f
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let (payload, flags): (&[u8], u8) = if batch_has_headers {
                (v.payload(), arbitro_store::flags::HAS_HEADERS)
            } else if let Some(Some(w)) = wrapped_payloads.get(i) {
                (w.as_slice(), arbitro_store::flags::HAS_HEADERS)
            } else {
                (v.payload(), 0)
            };
            arbitro_store::EntryRef {
                stream_id: seq_stream.raw(),
                subject: v.subject(),
                payload,
                flags,
                deliver_at_ms: 0,
            }
        })
        .collect();

    // F7: SharedClock atomic load.
    let now_ms = server.now_ms();

    // F2: drop block_in_place.
    let shared_store = server.store_for(seq_stream);
    let first_seq = match shared_store.lock().append_batch(&entries, now_ms) {
        Ok(seq) => seq,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
            return;
        }
    };

    send_rep_ok_v2(registry, conn_id, req_seq, first_seq);
    server.gate_for(seq_stream).release();
}

fn v2_publish_delayed(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    delayed_journal: &Option<crate::delayed::SharedDelayedJournal>,
) {
    let f = match PubDelayedFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::BufferTooShort);
            return;
        }
    };
    if let Err(code) = f.validate() {
        send_error_v2(registry, conn_id, req_seq, code);
        return;
    }
    let wire_stream = f.body.stream_id.get();
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound);
            return;
        }
    };

    let delay_ms = f.delay_ms();

    // ── Idempotency check (audit #9 — the delayed path used to bypass
    // the dedup window entirely, so a duplicate msg-id delayed publish
    // was accepted and matured into a second copy). Same per-stream
    // window check as v2_publish, applied BEFORE any journal/store
    // mutation. Note: the msg-id is recorded in the in-RAM tracker at
    // PUBLISH time; a broker restart before maturation loses it (the
    // rebuild only scans the main store) — documented in
    // ROBUSTNESS_AUDIT.md.
    let msg_id = f.msg_id();
    let window_ms = server.names().stream_idempotency_window_ms(seq_stream);
    if window_ms > 0 && !msg_id.is_empty() {
        let hash = idempotency_hash(msg_id);
        let shared = server.idempotency_for(seq_stream);
        let tracker_arc = crate::shard::idempotency::idempotency_for_stream(shared, seq_stream);
        let mut t = tracker_arc.lock();
        server.mark_idempotency_allocated(seq_stream);
        if !t.record(seq_stream, hash, msg_id, window_ms) {
            drop(t);
            send_error_v2(registry, conn_id, req_seq, ErrorCode::IdempotencyDuplicate);
            return;
        }
        drop(t);
    }

    // ── Stream quota pre-check (DiscardPolicy::New) — mirror of the
    // immediate path (audit #9: the delayed path used to skip it, so a
    // discard=1 stream silently accepted over-quota delayed publishes).
    // Semantics: the quota is evaluated at PUBLISH time against the
    // store's current occupancy, exactly like v2_publish. A delayed
    // message that passes here may still mature into a store that has
    // since filled up — maturation appends without re-checking
    // (documented in ROBUSTNESS_AUDIT.md).
    if let Some(quota) = server.names().stream_quota(seq_stream) {
        if quota.discard == 1 {
            let shared_store = server.store_for(seq_stream);
            let info = shared_store.lock().info();
            if quota.max_msgs > 0 && info.messages >= quota.max_msgs {
                send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
                return;
            }
            let entry_bytes = (f.subject().len() + f.payload().len()) as u64;
            if quota.max_bytes > 0 && info.bytes + entry_bytes > quota.max_bytes {
                send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
                return;
            }
        }
    }

    // ── msg-id wrap (mirror of the single-publish case 2) ─────────────
    // When the frame carries a msg_id, wrap payload + msg-id header into
    // an ExtendedPayload and stamp HAS_HEADERS so (a) the drain strips
    // the metadata before delivery and (b) the restart dedup rebuild
    // finds the id once the entry reaches the main store. Cold path —
    // the owned Vec allocation is fine here.
    let (store_payload, store_flags): (std::borrow::Cow<'_, [u8]>, u8) = if !msg_id.is_empty() {
        let hdrs: [(&[u8], &[u8]); 1] =
            [(arbitro_proto::wire::msg_headers::HDR_MSG_ID, msg_id)];
        (
            std::borrow::Cow::Owned(
                arbitro_proto::wire::msg_headers::encode_extended_payload_vec(
                    f.payload(),
                    &hdrs,
                ),
            ),
            arbitro_store::flags::HAS_HEADERS,
        )
    } else {
        (std::borrow::Cow::Borrowed(f.payload()), 0)
    };

    // If delay_ms == 0, treat as a normal publish (bypass the journal).
    if delay_ms == 0 {
        let entries = [arbitro_store::EntryRef {
            stream_id: seq_stream.raw(),
            subject: f.subject(),
            payload: &store_payload,
            flags: store_flags,
            deliver_at_ms: 0,
        }];
        let now_ms = server.now_ms();
        let shared_store = server.store_for(seq_stream);
        let first_seq = match shared_store.lock().append_batch(&entries, now_ms) {
            Ok(seq) => seq,
            Err(_) => {
                send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
                return;
            }
        };
        send_rep_ok_v2(registry, conn_id, req_seq, first_seq);
        server.gate_for(seq_stream).release();
        return;
    }

    // Delayed path — park in the delayed journal.
    let journal = match delayed_journal {
        Some(j) => j,
        None => {
            // No data_dir configured — delayed publish requires persistence.
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };

    let now_ms = server.now_ms();
    let deliver_at_ms = now_ms + delay_ms;

    let mut j = journal.lock();
    match j.append(
        now_ms,
        deliver_at_ms,
        seq_stream.raw(),
        f.subject(),
        &store_payload,
        store_flags,
    ) {
        Ok(()) => {
            // Reply with RepOk (ref_seq = 0 since there's no store sequence yet).
            send_rep_ok_v2(registry, conn_id, req_seq, 0);
        }
        Err(crate::delayed::DelayedAppendError::DelayTooLarge) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
        }
        Err(crate::delayed::DelayedAppendError::TooManyPending) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
        }
        Err(crate::delayed::DelayedAppendError::Io(e)) => {
            tracing::error!(error = %e, "delayed journal append failed");
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
        }
    }
}

async fn v2_ack(conn_id: u64, frame: &Bytes, server: &ShardRouter) {
    let f = match AckFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => return,
    };
    let consumer_id = ConsumerId(f.body.consumer_id.get());
    let seq_stream = match server.names().consumer_stream(consumer_id) {
        Some(s) => s,
        None => return, // consumer unknown — fire-and-forget, no reply
    };
    let shard = server.shard_for(seq_stream);
    let _ = shard
        .ack(
            consumer_id,
            conn_id,
            vec![AckEntry {
                stream_id: seq_stream,
                seq: f.body.ack_seq.get(),
            }],
        )
        .await;
}

async fn v2_batch_ack(conn_id: u64, frame: &Bytes, server: &ShardRouter) {
    let f = match BatchAckFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => return,
    };
    let consumer_id = ConsumerId(f.body.consumer_id.get());
    let seq_stream = match server.names().consumer_stream(consumer_id) {
        Some(s) => s,
        None => return,
    };
    let shard = server.shard_for(seq_stream);
    // B2: bounds-checked entries view — silently drop the frame if the
    // count field is lying. Fire-and-forget ack has no reply channel
    // to surface the InvalidEntryCount, so we just terminate the
    // current frame; the connection stays alive for subsequent frames.
    let Some(raw) = f.try_entries() else { return };
    let mut entries: Vec<AckEntry> = Vec::with_capacity(raw.len());
    for e in raw {
        entries.push(AckEntry {
            stream_id: seq_stream,
            seq: e.seq.get(),
        });
    }
    let _ = shard.ack(consumer_id, conn_id, entries).await;
}

/// AckStateReq — read-only cursor/retention query, no mutation.
async fn v2_ack_state(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let f = match AckStateReqFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => return,
    };
    let consumer_id = ConsumerId(f.body.consumer_id.get());
    // Informational today — a later watermark task will compare this
    // against the server's generation to detect a stale client cursor.
    let _req_gen = f.body.generation.get();
    let seq_stream = match server.names().consumer_stream(consumer_id) {
        Some(s) => s,
        None => {
            send_ack_state_rep_v2(
                registry,
                conn_id,
                req_seq,
                consumer_id.0,
                0,
                0,
                0,
                0,
                ACK_STATUS_CONSUMER_UNKNOWN,
            );
            return;
        }
    };
    let cursor = server.names().consumer_cursor(consumer_id).unwrap_or(0);
    let generation = server.names().consumer_generation(consumer_id).unwrap_or(0);
    let info = server.store_for(seq_stream).lock().info();
    send_ack_state_rep_v2(
        registry,
        conn_id,
        req_seq,
        consumer_id.0,
        generation,
        cursor,
        info.first_seq,
        info.last_seq,
        ACK_STATUS_OK,
    );
}

/// AckBatch — request/reply batch ack with per-batch outcome counters.
/// Reuses `shard.ack`'s full engine execute+cursor+persistence path;
/// pre-counts accepted/ignored/below_retention before dispatch since
/// `shard.ack` doesn't report per-entry outcomes.
async fn v2_ack_batch(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let f = match AckBatchFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => return,
    };
    let consumer_id = ConsumerId(f.body.consumer_id.get());
    // B2: bounds-checked seqs view — silently drop the frame if the
    // count field is lying, mirroring v2_batch_ack.
    let Some(seqs) = f.try_seqs() else { return };

    if seqs.len() > ACK_BATCH_MAX_SEQS {
        send_ack_batch_resp_v2(
            registry,
            conn_id,
            req_seq,
            consumer_id.0,
            0,
            0,
            0,
            0,
            0,
            ACK_STATUS_BATCH_TOO_LARGE,
        );
        return;
    }

    let seq_stream = match server.names().consumer_stream(consumer_id) {
        Some(s) => s,
        None => {
            send_ack_batch_resp_v2(
                registry,
                conn_id,
                req_seq,
                consumer_id.0,
                0,
                0,
                0,
                0,
                0,
                ACK_STATUS_CONSUMER_UNKNOWN,
            );
            return;
        }
    };

    let cursor_before = server.names().consumer_cursor(consumer_id).unwrap_or(0);
    let low = server.store_for(seq_stream).lock().info().first_seq;

    let mut accepted_entries: Vec<AckEntry> = Vec::with_capacity(seqs.len());
    let mut accepted: u32 = 0;
    let mut ignored: u32 = 0;
    let mut below_retention: u32 = 0;
    for s in seqs {
        let seq = s.get();
        if seq <= cursor_before {
            ignored += 1;
        } else if seq < low {
            below_retention += 1;
        } else {
            accepted += 1;
            accepted_entries.push(AckEntry {
                stream_id: seq_stream,
                seq,
            });
        }
    }

    let shard = server.shard_for(seq_stream);
    let _ = shard.ack(consumer_id, conn_id, accepted_entries).await;

    let new_cursor = server.names().consumer_cursor(consumer_id).unwrap_or(0);
    // still_pending: no watermark tracked yet — later server task.
    send_ack_batch_resp_v2(
        registry,
        conn_id,
        req_seq,
        consumer_id.0,
        new_cursor,
        accepted,
        ignored,
        below_retention,
        0,
        ACK_STATUS_OK,
    );
}

/// Single-entry NACK — fire-and-forget, no reply. Always immediate (delay=0).
async fn v2_nack(conn_id: u64, frame: &Bytes, server: &ShardRouter) {
    let f = match NackFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => return,
    };
    let consumer_id = ConsumerId(f.body.consumer_id.get());
    let seq_stream = match server.names().consumer_stream(consumer_id) {
        Some(s) => s,
        None => return, // consumer unknown — fire-and-forget, no reply
    };
    let shard = server.shard_for(seq_stream);
    let _ = shard
        .nack(
            consumer_id,
            conn_id,
            vec![AckEntry {
                stream_id: seq_stream,
                seq: f.body.nack_seq.get(),
            }],
            0, // single nack frame has no delay field
        )
        .await;
}

/// Batch NACK — fire-and-forget, no reply. Supports per-batch delay_ms.
async fn v2_batch_nack(conn_id: u64, frame: &Bytes, server: &ShardRouter) {
    let f = match BatchNackFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => return,
    };
    let consumer_id = ConsumerId(f.body.consumer_id.get());
    let seq_stream = match server.names().consumer_stream(consumer_id) {
        Some(s) => s,
        None => return,
    };
    let shard = server.shard_for(seq_stream);
    // B2: bounds-checked entries view — silently drop the frame on
    // lying count (fire-and-forget, no reply channel).
    let Some(raw) = f.try_entries() else { return };
    let entries: Vec<AckEntry> = raw
        .iter()
        .map(|e| AckEntry {
            stream_id: seq_stream,
            seq: e.seq.get(),
        })
        .collect();
    // All entries in a batch share the same delay — take max.
    let delay_ms = raw.iter().map(|e| e.delay_ms.get()).max().unwrap_or(0);
    let _ = shard.nack(consumer_id, conn_id, entries, delay_ms).await;
}

async fn v2_subscribe(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, Subscribe as SubscribeCold};
    let body = match SubscribeCold::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::BufferTooShort);
            return;
        }
    };
    // H1: every filter validated. Empty `filters` = catch-all (legacy
    // single-empty-filter behaviour); each non-empty entry must parse
    // as a valid subject pattern.
    for f in &body.filters {
        if !f.is_empty() && arbitro_proto::validate::validate_subject(f).is_err() {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
            return;
        }
    }
    let consumer_id = ConsumerId(body.consumer_id);
    let seq_stream = match server.names().consumer_stream(consumer_id) {
        Some(s) => s,
        None => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerNotFound);
            return;
        }
    };
    let queue_id = server
        .names()
        .consumer_queue(consumer_id)
        .unwrap_or_else(|| server.names().get_or_create_queue(seq_stream, b""));
    let shard = server.shard_for(seq_stream);

    // Drop empty filters (legacy "single empty-filter == catch-all"
    // contract). After filtering, an empty Vec also means catch-all —
    // the engine handles both forms identically.
    let filters: Vec<Vec<u8>> = body.filters.into_iter().filter(|f| !f.is_empty()).collect();

    // deliver_policy from consumer config (stored at CreateConsumer time).
    // Default: 0 = All (replay from beginning). The NameRegistry can hold
    // per-consumer deliver_policy for management-API consumers.
    let (deliver_policy, start_seq) = server
        .names()
        .consumer_deliver_policy(consumer_id)
        .unwrap_or((0, 0));

    let reply = shard
        .subscribe(
            StreamConfig {
                id: seq_stream,
                name: vec![],
            },
            ConsumerConfig {
                id: consumer_id,
                queue_id,
                stream_id: seq_stream,
                durable: true,
                // v2 SubFrame body has no ack-policy field; default to Explicit.
                ack_policy: AckPolicy::Explicit,
                max_inflight: u32::MAX,
                ack_wait_ms: 0,
                max_nack: 0,
            },
            SubscriptionConfig {
                // body.subscription_id == 0 means "legacy default": use
                // the consumer's own id as the subscription id (one sub
                // per consumer). A non-zero value lets a single
                // consumer host multiple subs in parallel, each with
                // its own filter set. Engine already supports this
                // (`subscriptions_for_consumer`); the wire bit was the
                // gate.
                id: SubscriptionId(if body.subscription_id == 0 {
                    consumer_id.0
                } else {
                    body.subscription_id
                }),
                stream_id: seq_stream,
                consumer_id,
                filters,
            },
            ConnectionId(conn_id),
            deliver_policy,
            start_seq,
        )
        .await;

    // M19: differentiate "shard returned `Ok(false)` (no such
    // consumer/binding)" from a transport-level SendError. The shard
    // reply is a `bool`; the only legitimate way to see `Ok(false)` is
    // an unknown consumer at this layer (everything else is reported as
    // a separate command outcome).
    //
    // F35: `ref_seq` on a successful Subscribe reply carries the bound
    // `consumer_id` (cast to u64). The previous shape echoed `req_seq`,
    // which was redundant — the client already correlated via
    // `header.seq`. Returning the consumer_id lets a client that
    // multi-subscribes (or follows a redirect) confirm WHICH consumer
    // is now active without an extra round-trip. Backward compatible
    // for clients that ignore `ref_seq`.
    match reply {
        Ok(true) => send_rep_ok_v2(registry, conn_id, req_seq, consumer_id.0 as u64),
        Ok(false) => send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerNotFound),
        Err(_) => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
    }
}

async fn v2_unsubscribe(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, Unsubscribe};
    let body = match Unsubscribe::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::BufferTooShort);
            return;
        }
    };
    let consumer_id = ConsumerId(body.consumer_id);
    let seq_stream = match server.names().consumer_stream(consumer_id) {
        Some(s) => s,
        None => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerNotFound);
            return;
        }
    };
    let shard = server.shard_for(seq_stream);

    match shard.unsubscribe(SubscriptionId(consumer_id.0)).await {
        Ok(_) => send_rep_ok_v2(registry, conn_id, req_seq, req_seq),
        Err(_) => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
    }
}

// ── Stream CRUD ────────────────────────────────────────────────────────────

async fn v2_create_stream(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, CreateStream as CreateStreamCold};
    // SEC-6: bound how many streams a single connection may create.
    if !registry.check_and_incr_quota(conn_id, crate::transport::registry::QuotaKind::Stream) {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
        return;
    }
    let body = match CreateStreamCold::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let name = body.name.as_slice();
    // H1: validate name + filter at the dispatch boundary BEFORE
    // allocating IDs. Rejects empty / oversized / weird-byte names so
    // catalog Vec indexes stay sane and DeleteStream/wire echoes
    // don't have to handle pathological input.
    if arbitro_proto::validate::validate_name(name).is_err() {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
        return;
    }
    let filter = body.filter.as_slice();
    if !filter.is_empty() && arbitro_proto::validate::validate_subject(filter).is_err() {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
        return;
    }
    let wire_stream = arbitro_engine_v2::common::wire_hash_32(name);
    // M7: collision-detecting variant. Two distinct names hashing to the
    // same u32 are rejected with StreamAlreadyExists rather than silently
    // merged. See `name_registry::STREAM_COLLISION_SENTINEL`.
    let (seq_stream, _created) = server.names().get_or_create_stream_named(wire_stream, name);
    if seq_stream.raw() == arbitro_common::name_registry::NameRegistry::STREAM_SLOT_FULL_SENTINEL {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
        return;
    }
    if seq_stream.raw() == arbitro_common::name_registry::NameRegistry::STREAM_COLLISION_SENTINEL {
        tracing::error!(
            wire_id = wire_stream,
            name = ?String::from_utf8_lossy(name),
            "wire_hash_32 collision — distinct stream name maps to an in-use wire id; rejected"
        );
        send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamAlreadyExists);
        return;
    }
    let shard = server.shard_for(seq_stream);

    let max_msgs = body.max_msgs;
    let max_bytes = body.max_bytes;
    let max_age_ms = body.max_age_secs.saturating_mul(1_000);
    let idempotency_window_ms = body.idempotency_window_ms;

    match shard
        .create_stream(
            StreamConfig {
                id: seq_stream,
                name: name.to_vec(),
            },
            max_msgs,
            max_bytes,
            max_age_ms,
        )
        .await
    {
        Ok(true) => {
            // Record the per-stream idempotency window in NameRegistry.
            // The publish hot path checks this with a single indexed
            // u32 load (see `stream_idempotency_window_ms`). 0 is the
            // legacy default = no dedup; any non-zero value activates
            // the dedup window on `v2_publish` / `v2_publish_batch`.
            server
                .names()
                .set_stream_idempotency(seq_stream, idempotency_window_ms);
            server
                .names()
                .set_stream_replicas(seq_stream, body.replicas);
            // Honesty: replicas > 1 does NOT yet mean acknowledged-durable
            // replication (ROBUSTNESS_AUDIT.md §2.5 / action #8).
            #[cfg(feature = "cluster")]
            if body.replicas > 1 {
                tracing::warn!(
                    stream = %String::from_utf8_lossy(name),
                    replicas = body.replicas,
                    "stream requests replicas > 1, but replication is \
                     best-effort: publishers are acknowledged before \
                     replication and ISR/high-watermark are not enforced — \
                     a leader failover may lose acknowledged data until \
                     catch-up + ISR enforcement land"
                );
            }

            // Store stream quota limits so the publish hot path can
            // pre-check and reject with StreamFull for DiscardPolicy::New.
            server
                .names()
                .set_stream_quota(seq_stream, max_msgs, max_bytes, body.discard);

            // F37: invalidate the list_streams / list_consumers TTL
            // cache so the next list-RPC reflects this new stream.
            server.invalidate_list_cache();

            // Persist to command log on cold path — idempotent on replay.
            // The metadata log keeps the legacy zerocopy body
            // (CreateStreamFixed) so the recovery applier
            // (CreateStreamView) is unchanged. Build it from the parsed
            // cold body via the existing frame encoder, then strip the
            // Header (CreateStreamFrame::encode_into produces
            // `Header + body + tail`; the log only stores body+tail).
            if let Some(log) = server.command_log() {
                let total = CreateStreamFrame::wire_size(name.len(), filter.len());
                let mut wire = vec![0u8; total];
                CreateStreamFrame::encode_into(
                    &mut wire,
                    0,
                    name,
                    filter,
                    max_msgs,
                    max_bytes,
                    body.max_age_secs,
                    body.replicas,
                    body.journal_kind,
                    body.retention,
                    body.discard,
                    idempotency_window_ms,
                );
                let cmd = build_create_stream(&wire[HEADER_SIZE..]);
                if let Err(e) = log.record(&cmd) {
                    tracing::warn!(error = %e, "command log: create_stream record failed");
                }
            }
            send_rep_ok_v2(registry, conn_id, req_seq, wire_stream as u64)
        }
        Ok(_) => send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamAlreadyExists),
        Err(_) => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
    }
}

async fn v2_delete_stream(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, DeleteStream};
    let body = match DeleteStream::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let name = body.name.as_slice();
    let wire_stream = arbitro_engine_v2::common::wire_hash_32(name);
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound);
            return;
        }
    };
    let shard = server.shard_for(seq_stream);

    // Snapshot the consumers attached to this stream BEFORE the engine
    // cascade removes them — we need their ids to mirror the cleanup
    // into NameRegistry. The engine's `delete_stream` cascade removes
    // the consumer ENTITIES but NameRegistry holds a separate wire-name
    // → ConsumerId mapping that must also be cleared, else a same-named
    // recreate on a fresh stream would silently alias to a defunct id.
    let cascaded_consumers = server.names().consumers_for_stream(seq_stream);

    match shard.delete_stream(seq_stream, true).await {
        Ok(_) => {
            // Cascade NameRegistry cleanup for every consumer the
            // engine removed, then drop the stream mapping itself.
            for cid in cascaded_consumers {
                server.names().remove_consumer_by_id(cid);
            }
            server.names().remove_stream(wire_stream);
            // F37: invalidate list caches — stream + cascaded consumers
            // are gone, both list RPCs must rebuild on next call.
            server.invalidate_list_cache();
            if let Some(log) = server.command_log() {
                // Metadata log keeps the legacy zerocopy body so the
                // recovery applier (DeleteStreamView) is unchanged.
                // Wire body is now JSON; we rebuild the on-disk body
                // here from the parsed name.
                let mut body = Vec::with_capacity(8 + name.len());
                body.extend_from_slice(&(name.len() as u16).to_le_bytes());
                body.extend_from_slice(&[0u8; 6]);
                body.extend_from_slice(name);
                let cmd = build_delete_stream(&body);
                if let Err(e) = log.record(&cmd) {
                    tracing::warn!(error = %e, "command log: delete_stream record failed");
                }
            }
            send_rep_ok_v2(registry, conn_id, req_seq, req_seq);
        }
        Err(_) => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
    }
}

async fn v2_get_stream(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, GetStream};
    let body = match GetStream::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let name = body.name.as_slice();
    let wire_stream = arbitro_engine_v2::common::wire_hash_32(name);
    match server.names().stream_seq(wire_stream) {
        Some(_) => send_rep_ok_v2(registry, conn_id, req_seq, wire_stream as u64),
        None => send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound),
    }
}

async fn v2_purge_stream(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, PurgeStream};
    let body = match PurgeStream::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let name = body.name.as_slice();
    let wire_stream = arbitro_engine_v2::common::wire_hash_32(name);
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound);
            return;
        }
    };
    let shard = server.shard_for(seq_stream);
    match shard.purge_stream(seq_stream).await {
        Ok(deleted) => send_rep_ok_v2(registry, conn_id, req_seq, deleted),
        Err(_) => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
    }
}

async fn v2_drain_subject(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, DrainSubject};
    let body = match DrainSubject::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let name = body.name.as_slice();
    let subject = body.subject;
    let wire_stream = arbitro_engine_v2::common::wire_hash_32(name);
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound);
            return;
        }
    };
    let shard = server.shard_for(seq_stream);
    match shard.drain_subject(seq_stream, subject).await {
        Ok(deleted) => send_rep_ok_v2(registry, conn_id, req_seq, deleted),
        Err(_) => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
    }
}

async fn v2_delete_message(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, DeleteMessage};
    let body = match DeleteMessage::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let wire_stream = arbitro_engine_v2::common::wire_hash_32(&body.name);
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound);
            return;
        }
    };
    let shard = server.shard_for(seq_stream);
    match shard.delete_message(body.seq).await {
        Ok(found) => send_rep_ok_v2(registry, conn_id, req_seq, u64::from(found)),
        Err(_) => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
    }
}

/// AckTerm — same wire shape as Ack, routes to `shard.ack_term()`.
async fn v2_ack_term(conn_id: u64, frame: &Bytes, server: &ShardRouter) {
    let f = match AckFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => return,
    };
    let consumer_id = ConsumerId(f.body.consumer_id.get());
    let seq_stream = match server.names().consumer_stream(consumer_id) {
        Some(s) => s,
        None => return,
    };
    let shard = server.shard_for(seq_stream);
    let _ = shard
        .ack_term(
            consumer_id,
            conn_id,
            vec![AckEntry {
                stream_id: seq_stream,
                seq: f.body.ack_seq.get(),
            }],
        )
        .await;
}

async fn v2_list_streams(
    conn_id: u64,
    req_seq: u64,
    _frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    // M20: a shard that errors silently dropped its streams from the
    // listing — operators saw a half-populated reply and never knew. We
    // now fail loud (InternalError) if any shard reports an error, so
    // partial views never reach the client. Trade-off: a single
    // crashed shard kills the whole `list_streams` reply, but that's
    // strictly safer than fabricating an incomplete list.
    //
    // F37: 1-second TTL cache short-circuits the 16-shard round-trip
    // when the cache is fresh. Invalidated explicitly by
    // create/delete (see v2_create_stream / v2_delete_stream).
    let all_streams: std::sync::Arc<Vec<(u32, Vec<u8>)>> =
        if let Some(cached) = server.cached_list_streams() {
            cached
        } else {
            let mut acc: Vec<(u32, Vec<u8>)> = Vec::new();
            for i in 0..server.shard_count() {
                match server.shard(i).list_streams().await {
                    Ok(reply) => acc.extend(reply.streams),
                    Err(_) => {
                        send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
                        return;
                    }
                }
            }
            server.store_list_streams(acc)
        };

    let body_len: usize = 4 + all_streams.iter().map(|(_, n)| 6 + n.len()).sum::<usize>();
    let total = HEADER_SIZE + body_len;
    let mut buf = BytesMut::with_capacity(total);

    let header = Header::new(Action::ListStreams.as_u16(), body_len as u32, req_seq);
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(&(all_streams.len() as u32).to_le_bytes());
    for (seq_id, name) in all_streams.iter() {
        let wire_id = server
            .names()
            .stream_wire(StreamId(*seq_id))
            .unwrap_or(*seq_id);
        buf.extend_from_slice(&wire_id.to_le_bytes());
        buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
        buf.extend_from_slice(name);
    }
    registry.send_bytes(conn_id, buf.freeze());
}

// ── Consumer CRUD ──────────────────────────────────────────────────────────

async fn v2_create_consumer(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, CreateConsumer as CreateConsumerCold};
    // The SEC-6 reservation happens further down, right before the engine
    // call, and is released again unless the create actually allocated a new
    // consumer. Reserving up here charged a unit for every malformed request
    // and for every idempotent re-join, so a worker that restarts often would
    // eventually be refused creates it is entitled to.
    let body = match CreateConsumerCold::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let wire_stream = body.stream_id;
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound);
            return;
        }
    };
    let name = body.name.as_slice();
    let group = body.group.as_slice();
    // H1: validate consumer name + (optional) group + (optional)
    // subject filter at the dispatch boundary. Same reasoning as
    // v2_create_stream — keep weird bytes from leaking into the
    // engine catalog / NameRegistry maps.
    if arbitro_proto::validate::validate_name(name).is_err() {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
        return;
    }
    if !group.is_empty() && arbitro_proto::validate::validate_name(group).is_err() {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
        return;
    }
    let subject_filter = body.subject.as_slice();
    if !subject_filter.is_empty()
        && arbitro_proto::validate::validate_subject(subject_filter).is_err()
    {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
        return;
    }

    let ack_policy = match body.ack_policy {
        0 => AckPolicy::None,
        _ => AckPolicy::Explicit,
    };

    // GAP-2: AckPolicy::None ignores max_inflight — fire-and-forget
    // bindings never increment inflight counters, so any limit is dead
    // weight. Force unlimited so operators aren't misled by a config
    // that has no effect.
    let effective_max_inflight: u16 = if ack_policy == AckPolicy::None {
        0 // 0 → u32::MAX (unlimited) at ConsumerConfig construction below
    } else {
        body.max_inflight
    };

    // GAP-5: Fanout mode ignores the group field — the group drives
    // queue-dedup semantics in the drain (QueueId != 0 triggers
    // round-robin). For Fanout consumers we assign QueueId(0) directly
    // instead of going through get_or_create_queue, because that
    // allocator starts at 1 and any non-zero id activates queue-mode
    // dedup in the drain worker.
    let is_fanout = body.deliver_mode == 0;

    // B6: subject limits are only honored under Explicit ack. The wire
    // body always carries the Vec, but we silently drop it for None-ack
    // consumers (legacy contract — predates serde migration).
    let subject_limits: Vec<(Vec<u8>, u32)> = if ack_policy == AckPolicy::Explicit {
        body.subject_limits
            .iter()
            .map(|s| (s.pattern.clone(), s.limit))
            .collect()
    } else {
        Vec::new()
    };

    // SEC-6: bound how many consumers a single connection may create. Taken
    // here, after validation, so only a request that reaches the engine can
    // spend it.
    use crate::transport::registry::QuotaKind;
    if !registry.check_and_incr_quota(conn_id, QuotaKind::Consumer) {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerAlreadyExists);
        return;
    }

    let (seq_consumer, _created) = server.names().get_or_create_consumer(seq_stream, name);
    // B1: registry refused — consumer slot pool exhausted.
    if seq_consumer.raw() == u32::MAX {
        registry.release_quota(conn_id, QuotaKind::Consumer);
        send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerAlreadyExists);
        return;
    }
    let shard = server.shard_for(seq_stream);

    let queue_id = if is_fanout {
        QueueId(0)
    } else {
        server.names().get_or_create_queue(seq_stream, group)
    };
    let create_res = shard
        .create_consumer(
            ConsumerConfig {
                id: seq_consumer,
                queue_id,
                stream_id: seq_stream,
                durable: true,
                ack_policy,
                max_inflight: if effective_max_inflight == 0 {
                    u32::MAX
                } else {
                    effective_max_inflight as u32
                },
                ack_wait_ms: body.ack_wait_ms,
                // DLQ is disabled by default. The broker-native DLQ publish
                // path is not yet wired (see handle_nack), so a non-zero
                // default would silently drop poison messages. Keep it opt-in
                // (0 = redeliver forever) until the DLQ is fully implemented.
                max_nack: body.max_nack.unwrap_or(0),
            },
            subject_limits,
        )
        .await;

    // Quota counts consumers this connection actually brought into being.
    // An idempotent re-join (Ok(0)), a config-mismatch rejection (Ok(2)) and
    // a dead shard all give the reserved unit back.
    if !matches!(create_res, Ok(1)) {
        registry.release_quota(conn_id, QuotaKind::Consumer);
    }

    // AUDIT-10 follow-up: mutate the NameRegistry ONLY after the engine
    // accepts the create (Ok(1) = new, Ok(0) = idempotent same-config).
    // These writes used to happen BEFORE the engine's config-mismatch
    // check, so a REJECTED re-create (Ok(2) → InvalidConsumerConfig)
    // silently overwrote deliver_policy/queue/stream for the existing
    // consumer — durable registry state diverged from the engine and a
    // later subscribe (or restart replay) used the rejected config.
    if matches!(create_res, Ok(0) | Ok(1)) {
        server.names().set_consumer_queue(seq_consumer, queue_id);
        server.names().set_consumer_stream(seq_consumer, seq_stream);
        server
            .names()
            .set_consumer_deliver_policy(seq_consumer, body.deliver_policy, body.start_seq);
    }

    match create_res {
        Ok(1) => {
            // F37: a new consumer must show up in list_consumers reply.
            server.invalidate_list_cache();
            // Metadata log keeps the legacy zerocopy
            // (CreateConsumerFixed + tail) body so recovery
            // (CreateConsumerView) is unchanged. Rebuild from the
            // parsed cold body via the existing frame encoder.
            if let Some(log) = server.command_log() {
                let limit_refs: Vec<arbitro_proto::v2::manager::SubjectLimit<'_>> = body
                    .subject_limits
                    .iter()
                    .map(|s| arbitro_proto::v2::manager::SubjectLimit {
                        pattern: s.pattern.as_slice(),
                        limit: s.limit,
                    })
                    .collect();
                let tail_len = arbitro_proto::v2::manager::subject_limits_tail_len(&limit_refs);
                let total = CreateConsumerFrame::wire_size(
                    name.len(),
                    group.len(),
                    subject_filter.len(),
                    tail_len,
                );
                let mut wire = vec![0u8; total];
                CreateConsumerFrame::encode_into(
                    &mut wire,
                    0,
                    wire_stream,
                    name,
                    group,
                    subject_filter,
                    effective_max_inflight,
                    body.ack_policy,
                    body.deliver_policy,
                    body.deliver_mode,
                    body.ack_wait_ms,
                    body.start_seq,
                    &limit_refs,
                );
                let cmd = build_create_consumer(&wire[HEADER_SIZE..]);
                if let Err(e) = log.record(&cmd) {
                    tracing::warn!(error = %e, "command log: create_consumer record failed");
                }
            }
            send_rep_ok_v2(registry, conn_id, req_seq, seq_consumer.0 as u64)
        }
        Ok(0) => {
            // Already existed with same config — idempotent, return id.
            send_rep_ok_v2(registry, conn_id, req_seq, seq_consumer.0 as u64)
        }
        Ok(2) => {
            // GAP-3: consumer exists with different config.
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidConsumerConfig)
        }
        Ok(_) => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
        Err(_) => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
    }
}

async fn v2_delete_consumer(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, DeleteConsumer};
    let body = match DeleteConsumer::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let consumer_id = ConsumerId(body.consumer_id);

    // F14: route directly to the owning shard when we know it.
    // Fall back to fanning out if the consumer→stream mapping is unknown
    // (recovery edge cases, manual control-plane calls).
    let candidate_shards: smallvec::SmallVec<[usize; 1]> =
        match server.names().consumer_stream(consumer_id) {
            Some(stream) => {
                let idx = stream.raw() as usize % server.shard_count();
                smallvec::smallvec![idx]
            }
            None => (0..server.shard_count()).collect(),
        };

    // ROB-21: `delete_consumer` returns `Ok(false)` when the shard has no
    // matching consumer — that is a lookup miss, not a success. Only
    // `Ok(true)` means an entry was actually removed; only then do we
    // cascade the NameRegistry cleanup, log the command, and reply RepOk.
    for i in candidate_shards {
        match server.shard(i).delete_consumer(consumer_id).await {
            Ok(true) => {
                // Mirror the cascade that `v2_delete_stream` does for streams:
                // drop the wire-name → id mapping (plus the consumer's reverse
                // queue / stream / deliver-policy indexes) from NameRegistry.
                // Without this, `GetConsumer` keeps returning `Ok` for a
                // consumer the engine has already removed, and the registry
                // leaks one entry per deleted consumer (the maps grow forever
                // under a create→delete→recreate workload).
                server.names().remove_consumer_by_id(consumer_id);
                // F37: invalidate list_consumers cache.
                server.invalidate_list_cache();

                if let Some(log) = server.command_log() {
                    // Metadata log keeps the legacy zerocopy body
                    // (DeleteConsumerAction: consumer_id u32 + _pad u32)
                    // so the recovery applier (DeleteConsumerView) is
                    // unchanged. Rebuild from the parsed consumer_id.
                    let mut body = Vec::with_capacity(8);
                    body.extend_from_slice(&consumer_id.0.to_le_bytes());
                    body.extend_from_slice(&[0u8; 4]);
                    let cmd = build_delete_consumer(&body);
                    if let Err(e) = log.record(&cmd) {
                        tracing::warn!(error = %e, "command log: delete_consumer record failed");
                    }
                }
                send_rep_ok_v2(registry, conn_id, req_seq, req_seq);
                return;
            }
            Ok(false) => {
                // Not found on this shard — keep trying the remaining
                // candidates (relevant in the fan-out fallback case).
            }
            Err(_) => {
                send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
                return;
            }
        }
    }
    send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerNotFound);
}

/// M11: pause delivery to a consumer. Routes to the owning shard via the
/// names registry when known; otherwise fans out. Reply = RepOk if any
/// shard reported success, else ConsumerNotFound.
async fn v2_pause_consumer(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, PauseConsumer};
    let body = match PauseConsumer::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let consumer_id = ConsumerId(body.consumer_id);
    let candidate_shards: smallvec::SmallVec<[usize; 1]> =
        match server.names().consumer_stream(consumer_id) {
            Some(stream) => {
                let idx = stream.raw() as usize % server.shard_count();
                smallvec::smallvec![idx]
            }
            None => (0..server.shard_count()).collect(),
        };
    for i in candidate_shards {
        if let Ok(true) = server.shard(i).pause_consumer(consumer_id).await {
            send_rep_ok_v2(registry, conn_id, req_seq, req_seq);
            return;
        }
    }
    send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerNotFound);
}

/// M11: resume delivery to a previously paused consumer.
async fn v2_resume_consumer(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, ResumeConsumer};
    let body = match ResumeConsumer::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let consumer_id = ConsumerId(body.consumer_id);
    let candidate_shards: smallvec::SmallVec<[usize; 1]> =
        match server.names().consumer_stream(consumer_id) {
            Some(stream) => {
                let idx = stream.raw() as usize % server.shard_count();
                smallvec::smallvec![idx]
            }
            None => (0..server.shard_count()).collect(),
        };
    for i in candidate_shards {
        if let Ok(true) = server.shard(i).resume_consumer(consumer_id).await {
            send_rep_ok_v2(registry, conn_id, req_seq, req_seq);
            return;
        }
    }
    send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerNotFound);
}

/// Get the live pending-ack count for one consumer. The reply is a
/// standard `RepOk` whose `ref_seq` body carries the count as a u64.
/// Routes by walking every shard until one reports a non-zero or until
/// all replied — the consumer lives on exactly one shard, but querying
/// stays simple by summing across (most return 0).
async fn v2_consumer_stats(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, ConsumerStats};
    let body = match ConsumerStats::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let consumer_id = ConsumerId(body.consumer_id);

    // F14: route directly to the owning shard via NameRegistry — the
    // consumer lives on exactly one shard, no need to fan out queries.
    let total = match server.names().consumer_stream(consumer_id) {
        Some(stream) => {
            let shard = server.shard_for(stream);
            shard.consumer_pending(consumer_id).await.unwrap_or(0)
        }
        None => 0,
    };
    send_rep_ok_v2(registry, conn_id, req_seq, total);
}

async fn v2_get_consumer(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, GetConsumer};
    let body = match GetConsumer::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let name = body.name.as_slice();
    // GAP-6: consumers are namespaced by stream — translate the wire
    // stream_id to the sequential engine id before lookup.
    let wire_stream = body.stream_id;
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound);
            return;
        }
    };
    match server.names().consumer_id(seq_stream, name) {
        Some(id) => send_rep_ok_v2(registry, conn_id, req_seq, id.0 as u64),
        None => send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerNotFound),
    }
}

async fn v2_list_consumers(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    use arbitro_proto::v2::cold::{ColdBody, ListConsumers};
    let body = match ListConsumers::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    // Wire stream_id is the client-side hash; 0 = list all consumers.
    // Translate to the sequential engine id used inside the shard reply.
    let wire_filter = body.stream_id;
    // None = no filter (return all); Some(seq) = filter by engine seq_id.
    // Unknown wire hash → Some(u32::MAX) which matches nothing → empty list.
    let seq_filter: Option<u32> = if wire_filter == 0 {
        None // no filter
    } else {
        Some(
            server
                .names()
                .stream_seq(wire_filter)
                .map(|s| s.raw())
                .unwrap_or(u32::MAX),
        )
    };

    // M20: fail loud on any shard error rather than returning a partial
    // listing. Same trade-off as `v2_list_streams` — one crashed shard
    // takes the whole reply, but the alternative is silently lying to
    // the operator about what consumers exist.
    //
    // F37: TTL cache covers the (always full, no client filter) fan-out
    // step; the per-request `stream_id` filter is applied on top of the
    // cached aggregate, so different filter values can still share the
    // same underlying snapshot.
    let all_consumers: std::sync::Arc<Vec<(u32, u32, u32, bool)>> =
        if let Some(cached) = server.cached_list_consumers() {
            cached
        } else {
            let mut acc: Vec<(u32, u32, u32, bool)> = Vec::new();
            for i in 0..server.shard_count() {
                match server.shard(i).list_consumers().await {
                    Ok(reply) => acc.extend(reply.consumers),
                    Err(_) => {
                        send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
                        return;
                    }
                }
            }
            server.store_list_consumers(acc)
        };

    // CQ-17: no filter is the common case (ListConsumers with stream_id
    // 0). Iterate the cached `Arc<Vec>` by reference and encode directly
    // instead of cloning the whole vector just to iterate it again.
    let iter: Box<dyn Iterator<Item = &(u32, u32, u32, bool)>> = match seq_filter {
        None => Box::new(all_consumers.iter()),
        Some(seq) => Box::new(all_consumers.iter().filter(move |(_, sid, _, _)| *sid == seq)),
    };
    let filtered_count = match seq_filter {
        None => all_consumers.len(),
        Some(seq) => all_consumers.iter().filter(|(_, sid, _, _)| *sid == seq).count(),
    };

    let entry_size = 13;
    let body_len = 4 + filtered_count * entry_size;
    let total = HEADER_SIZE + body_len;
    let mut buf = BytesMut::with_capacity(total);

    let header = Header::new(Action::ListConsumers.as_u16(), body_len as u32, req_seq);
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(&(filtered_count as u32).to_le_bytes());
    for (consumer_id, stream_id, queue_id, paused) in iter {
        buf.extend_from_slice(&consumer_id.to_le_bytes());
        buf.extend_from_slice(&stream_id.to_le_bytes());
        buf.extend_from_slice(&queue_id.to_le_bytes());
        buf.extend_from_slice(&[*paused as u8]);
    }
    registry.send_bytes(conn_id, buf.freeze());
}

// ── System ─────────────────────────────────────────────────────────────────

/// Server-side disconnect: drain across all shards, drop the connection.
///
/// H9: previously this iterated `0..shard_count()` serially and awaited
/// each `drain_connection` round-trip in turn. With N shards and a
/// p99 shard reply of a few hundred µs, a slow shard would gate the
/// entire disconnect path and create a window where a recycled
/// connection_id could see ack injections on a still-bound shard.
/// We now build per-shard futures up front and poll them concurrently
/// via `tokio::join!` semantics (collect + await each). Total wall
/// time becomes `max(per-shard)` instead of `sum(per-shard)`.
pub(crate) async fn v2_disconnect(
    conn_id: u64,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    cron_registry: &std::sync::Arc<crate::cron::CronRegistry>,
) {
    let shards = server.shard_count();
    let cid = ConnectionId(conn_id);
    tracing::debug!(
        target = "dispatch",
        conn = conn_id,
        shards,
        "v2_disconnect: draining all shards"
    );

    // Spawn each drain_connection onto the same runtime so they execute
    // concurrently. We must await all before removing the connection
    // from the registry (writer task still alive).
    let mut handles = Vec::with_capacity(shards);
    for i in 0..shards {
        let handle = server.shard(i).clone();
        handles.push(tokio::spawn(async move {
            let _ = handle.drain_connection(cid).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    // Remove this connection from all cron worker pools.
    cron_registry.remove_connection(conn_id);

    tracing::debug!(
        target = "dispatch",
        conn = conn_id,
        "v2_disconnect: drains complete"
    );
    registry.remove(conn_id);
}

fn v2_ping(conn_id: u64, registry: &ConnectionRegistry) {
    // Reply with a Pong header (no body). Header = 12B, fits inline.
    let header = Header::new(Action::Pong.as_u16(), 0, 0);
    registry.send_inline(conn_id, header.as_bytes());
}

// ── Cron ──────────────────────────────────────────────────────────────────

fn v2_create_cron(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    registry: &ConnectionRegistry,
    cron_registry: &std::sync::Arc<crate::cron::CronRegistry>,
) {
    // SEC-6: bound how many crons a single connection may create.
    if !registry.check_and_incr_quota(conn_id, crate::transport::registry::QuotaKind::Cron) {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
        return;
    }
    let body_bytes = &frame[HEADER_SIZE..];
    let body = match arbitro_proto::wire::cron::decode_create_cron(body_bytes) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    match cron_registry.create(
        Bytes::copy_from_slice(body.name.as_bytes()),
        &body.every,
        body.tz,
        body.timeout_ms,
        body.overlap,
        conn_id,
    ) {
        Ok(()) => send_rep_ok_v2(registry, conn_id, req_seq, 0),
        Err(msg) => {
            tracing::warn!(conn_id, name = %body.name, error = %msg, "create_cron failed");
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
        }
    }
}

fn v2_delete_cron(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    registry: &ConnectionRegistry,
    cron_registry: &std::sync::Arc<crate::cron::CronRegistry>,
) {
    let name = &frame[HEADER_SIZE..];
    if cron_registry.delete(name) {
        send_rep_ok_v2(registry, conn_id, req_seq, 0);
    } else {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
    }
}

fn v2_list_crons(
    conn_id: u64,
    req_seq: u64,
    registry: &ConnectionRegistry,
    cron_registry: &std::sync::Arc<crate::cron::CronRegistry>,
) {
    let infos = cron_registry.list();
    let json = serde_json::to_vec(&infos).unwrap_or_default();
    // Send as RepOk with JSON body in ref_seq position (0) + raw JSON
    // appended. The client reads the body after the standard RepOk header.
    let total = HEADER_SIZE + json.len();
    let mut buf = BytesMut::with_capacity(total);
    let header = Header::new(Action::ListCrons.as_u16(), json.len() as u32, req_seq);
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(&json);
    registry.send_bytes(conn_id, buf.freeze());
}

fn v2_cron_ack(frame: &Bytes, cron_registry: &std::sync::Arc<crate::cron::CronRegistry>) {
    let body = &frame[HEADER_SIZE..];
    if let Some(view) = arbitro_proto::wire::cron::decode_cron_ack(body) {
        cron_registry.ack(view.name, view.ok);
    }
}

// ── Cluster Raft-propose wrappers ─────────────────────────────────────────
//
// These parse the frame, build a ClusterCommand, propose it through Raft,
// and on success execute locally. Only compiled with feature = "cluster".

#[cfg(feature = "cluster")]
async fn v2_create_stream_raft(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    cluster: &std::sync::Arc<crate::cluster::ClusterState>,
) {
    use arbitro_proto::v2::cold::{ColdBody, CreateStream as CreateStreamCold};
    // SEC-6: bound how many streams a single connection may create.
    if !registry.check_and_incr_quota(conn_id, crate::transport::registry::QuotaKind::Stream) {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
        return;
    }
    let body = match CreateStreamCold::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let cmd = crate::cluster::state_machine::ClusterCommand::CreateStream {
        name: String::from_utf8_lossy(&body.name).to_string(),
        filter: String::from_utf8_lossy(&body.filter).to_string(),
        max_msgs: body.max_msgs,
        max_bytes: body.max_bytes,
        max_age_secs: body.max_age_secs,
        replicas: body.replicas,
        journal_kind: body.journal_kind,
        retention: body.retention,
        discard: body.discard,
        idempotency_window_ms: body.idempotency_window_ms,
    };
    match tokio::time::timeout(
        std::time::Duration::from_millis(500),
        crate::cluster::propose_command(cluster.client(), &cmd),
    )
    .await
    {
        Ok(Ok(())) => {
            // Raft committed — execute locally.
            v2_create_stream(conn_id, req_seq, frame, server, registry).await;
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "raft propose create_stream failed");
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
        }
        Err(_) => {
            tracing::warn!("raft propose create_stream timed out (node may not be leader)");
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
        }
    }
}

#[cfg(feature = "cluster")]
async fn v2_delete_stream_raft(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    cluster: &std::sync::Arc<crate::cluster::ClusterState>,
) {
    use arbitro_proto::v2::cold::{ColdBody, DeleteStream};
    let body = match DeleteStream::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let cmd = crate::cluster::state_machine::ClusterCommand::DeleteStream {
        name: String::from_utf8_lossy(&body.name).to_string(),
    };
    match tokio::time::timeout(
        std::time::Duration::from_millis(500),
        crate::cluster::propose_command(cluster.client(), &cmd),
    )
    .await
    {
        Ok(Ok(())) => {
            // Raft committed — execute locally via the standard path.
            v2_delete_stream(conn_id, req_seq, frame, server, registry).await;
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "raft propose delete_stream failed");
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
        }
        Err(_) => {
            tracing::warn!("raft propose delete_stream timed out (node may not be leader)");
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
        }
    }
}

#[cfg(feature = "cluster")]
async fn v2_create_consumer_raft(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    cluster: &std::sync::Arc<crate::cluster::ClusterState>,
) {
    use arbitro_proto::v2::cold::{ColdBody, CreateConsumer as CreateConsumerCold};
    // No SEC-6 reservation here: on success this delegates to
    // `v2_create_consumer`, which takes its own. Charging one here too made
    // every clustered create cost two units of the connection's quota.
    let body = match CreateConsumerCold::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    let cmd = crate::cluster::state_machine::ClusterCommand::CreateConsumer {
        stream_name: format!("{}", body.stream_id),
        name: String::from_utf8_lossy(&body.name).to_string(),
        group: String::from_utf8_lossy(&body.group).to_string(),
        filter: String::from_utf8_lossy(&body.subject).to_string(),
        max_inflight: body.max_inflight,
        ack_policy: body.ack_policy,
        deliver_policy: body.deliver_policy,
        deliver_mode: body.deliver_mode,
        ack_wait_ms: body.ack_wait_ms,
        start_seq: body.start_seq,
    };
    match tokio::time::timeout(
        std::time::Duration::from_millis(500),
        crate::cluster::propose_command(cluster.client(), &cmd),
    )
    .await
    {
        Ok(Ok(())) => {
            v2_create_consumer(conn_id, req_seq, frame, server, registry).await;
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "raft propose create_consumer failed");
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
        }
        Err(_) => {
            tracing::warn!("raft propose create_consumer timed out (node may not be leader)");
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
        }
    }
}

#[cfg(feature = "cluster")]
async fn v2_delete_consumer_raft(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    cluster: &std::sync::Arc<crate::cluster::ClusterState>,
) {
    use arbitro_proto::v2::cold::{ColdBody, DeleteConsumer};
    let body = match DeleteConsumer::decode_body(&frame[HEADER_SIZE..]) {
        Ok(b) => b,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
            return;
        }
    };
    // Resolve the leader-local `consumer_id` back to its wire-stable
    // `(stream_name, consumer_name)` pair BEFORE proposing to Raft.
    // Followers assign different sequential ids, so the raw id has no
    // meaning outside this node — replicating names lets each follower
    // do a local name → id lookup after apply. See BUG-7.
    let cid = arbitro_engine_v2::types::ConsumerId(body.consumer_id);
    let stream_name = server
        .names()
        .consumer_stream(cid)
        .and_then(|s| server.names().stream_name(s))
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .unwrap_or_default();
    let consumer_name = server
        .names()
        .consumer_name(cid)
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .unwrap_or_default();
    if stream_name.is_empty() || consumer_name.is_empty() {
        // The consumer no longer exists locally (already deleted or
        // never created on this leader). Nothing to replicate — reply
        // OK so a retrying client doesn't spin.
        tracing::debug!(
            consumer_id = body.consumer_id,
            "raft delete_consumer: consumer id unmapped on this leader — skipping propose"
        );
        send_rep_ok_v2(registry, conn_id, req_seq, 0);
        return;
    }
    let cmd = crate::cluster::state_machine::ClusterCommand::DeleteConsumer {
        stream_name,
        name: consumer_name,
    };
    match tokio::time::timeout(
        std::time::Duration::from_millis(500),
        crate::cluster::propose_command(cluster.client(), &cmd),
    )
    .await
    {
        Ok(Ok(())) => {
            v2_delete_consumer(conn_id, req_seq, frame, server, registry).await;
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "raft propose delete_consumer failed");
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
        }
        Err(_) => {
            tracing::warn!("raft propose delete_consumer timed out (node may not be leader)");
            send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
        }
    }
}

