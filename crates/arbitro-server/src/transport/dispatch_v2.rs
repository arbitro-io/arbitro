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
//!   * `PublishWithHeaders` — not implemented.
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

use arbitro_engine_v2::AckEntry;
use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine_v2::types::*;
use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::v2::header::{Header, HEADER_SIZE};
use arbitro_proto::v2::ingress::ack_frame::{AckFrame, BatchAckFrame};
use arbitro_proto::v2::ingress::nack_frame::{NackFrame, BatchNackFrame};
use arbitro_proto::v2::ingress::batch_pub_frame::BatchPubFrame;
use arbitro_proto::v2::ingress::pub_frame::PubFrame;
use arbitro_proto::v2::ingress::pub_with_reply::PubWithReplyFrame;
use arbitro_proto::v2::ingress::sub_frame::SubFrame;
use arbitro_proto::v2::manager::consumer_mgmt::{
    CreateConsumerFrame, ListConsumersFrame, ConsumerStatsFrame,
};
use arbitro_proto::v2::manager::stream_mgmt::CreateStreamFrame;
use bytes::{Bytes, BytesMut};
use zerocopy::FromBytes;
use zerocopy::IntoBytes;

use crate::common::reply_v2::{send_error_v2, send_rep_ok_v2};
use crate::shard::router::ShardRouter;
use crate::transport::ConnectionRegistry;

use arbitro_proto::metadata::{
    build_create_stream, build_delete_stream,
    build_create_consumer, build_delete_consumer,
};

/// Dispatch one v2 frame. `frame` covers `[Header(16) || body(msg_len)]`.
pub async fn dispatch_frame_v2(
    conn_id: u64,
    frame: Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
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
            send_error_v2(registry, conn_id, header.seq.get(), ErrorCode::UnknownAction);
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
        Action::Publish          => v2_publish(conn_id, req_seq, &frame, server, registry),
        Action::PublishBatch     => v2_publish_batch(conn_id, req_seq, &frame, server, registry),
        Action::PublishWithReply => v2_publish_with_reply(conn_id, req_seq, &frame, server, registry),
        Action::Ack            => v2_ack(conn_id, &frame, server).await,
        Action::BatchAck       => v2_batch_ack(conn_id, &frame, server).await,
        Action::Nack           => v2_nack(conn_id, &frame, server).await,
        Action::BatchNack      => v2_batch_nack(conn_id, &frame, server).await,
        Action::Subscribe      => v2_subscribe(conn_id, req_seq, &frame, server, registry).await,
        Action::Unsubscribe    => v2_unsubscribe(conn_id, req_seq, &frame, server, registry).await,

        // ── Stream management ───────────────────────────────────────
        Action::CreateStream   => v2_create_stream(conn_id, req_seq, &frame, server, registry).await,
        Action::DeleteStream   => v2_delete_stream(conn_id, req_seq, &frame, server, registry).await,
        Action::GetStream      => v2_get_stream(conn_id, req_seq, &frame, server, registry).await,
        Action::PurgeStream    => v2_purge_stream(conn_id, req_seq, &frame, server, registry).await,
        Action::DrainSubject   => v2_drain_subject(conn_id, req_seq, &frame, server, registry).await,
        Action::ListStreams    => v2_list_streams(conn_id, req_seq, &frame, server, registry).await,

        // ── Consumer management ─────────────────────────────────────
        Action::CreateConsumer => v2_create_consumer(conn_id, req_seq, &frame, server, registry).await,
        Action::DeleteConsumer => v2_delete_consumer(conn_id, req_seq, &frame, server, registry).await,
        Action::GetConsumer    => v2_get_consumer(conn_id, req_seq, &frame, server, registry).await,
        Action::ListConsumers  => v2_list_consumers(conn_id, req_seq, &frame, server, registry).await,
        Action::ConsumerStats  => v2_consumer_stats(conn_id, req_seq, &frame, server, registry).await,
        Action::PauseConsumer  => v2_pause_consumer(conn_id, req_seq, &frame, server, registry).await,
        Action::ResumeConsumer => v2_resume_consumer(conn_id, req_seq, &frame, server, registry).await,

        // ── System ──────────────────────────────────────────────────
        Action::Disconnect     => v2_disconnect(conn_id, server, registry).await,
        Action::Ping           => v2_ping(conn_id, registry),
        // M17: count Pongs so the keepalive path is observable. The
        // counter lives on the connection registry — it's stable across
        // the lifetime of the conn and the read loop already touches
        // the registry on every frame.
        Action::Pong           => { registry.touch(conn_id); }

        // L1 / L2: AckSync / BatchAckSync, PublishAccumulate,
        // PublishWithHeaders, PublishBatchWithHeaders, FanoutBatch — all
        // have wire codes but no dispatcher. Reply `Unimplemented` so the
        // client gets a stable, distinct error instead of UnknownAction.
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::BufferTooShort); return; }
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
        None => { send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound); return; }
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
    let msg_id = f.msg_id();
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

    let entries = [arbitro_store::EntryRef {
        stream_id: seq_stream.raw(),
        subject: f.subject(),
        payload: f.payload(),
        flags: 0,
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
fn idempotency_hash(msg_id: &[u8]) -> u64 {
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::BufferTooShort); return; }
    };
    if let Err(code) = f.validate() {
        send_error_v2(registry, conn_id, req_seq, code);
        return;
    }
    let wire_stream = f.body.stream_id.get();
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => { send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound); return; }
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::BufferTooShort); return; }
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
        None => { send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound); return; }
    };

    // ── Idempotency check (all-or-nothing) ────────────────────────────
    //
    // F6: stream-build EntryRef vec directly from `f.iter()` —
    // dropping the materialised entry_views Vec on the non-idempotent
    // fast path. The iterator (`BatchPubIter`) is `Copy`, so the
    // idempotency branch can iterate twice without an extra alloc.
    //
    // Fast-bail when the stream has no idempotency window.
    let window_ms = server.names().stream_idempotency_window_ms(seq_stream);
    if window_ms > 0 && f.iter().any(|v| !v.msg_id().is_empty()) {
        let shared = server.idempotency_for(seq_stream);
        let tracker_arc = crate::shard::idempotency::idempotency_for_stream(shared, seq_stream);
        let mut tracker = tracker_arc.lock();
        server.mark_idempotency_allocated(seq_stream);

        // M2: track inserted `(hash, msg_id bytes)` for rollback on
        // duplicate. We hold the msg_id slice borrowed from `frame`
        // (lives for the duration of this dispatch), so the rollback
        // doesn't need owned copies — except `forget` expects a slice,
        // which we still have.
        let mut inserted: smallvec::SmallVec<[(u64, &[u8]); 16]> =
            smallvec::SmallVec::new();
        let mut duplicate = false;
        for v in f.iter() {
            let id = v.msg_id();
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
    let entries: smallvec::SmallVec<[arbitro_store::EntryRef<'_>; 16]> = f
        .iter()
        .map(|v| arbitro_store::EntryRef {
            stream_id: seq_stream.raw(),
            subject: v.subject(),
            payload: v.payload(),
            flags: 0,
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
            vec![AckEntry { stream_id: seq_stream, seq: f.body.ack_seq.get() }],
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
        entries.push(AckEntry { stream_id: seq_stream, seq: e.seq.get() });
    }
    let _ = shard.ack(consumer_id, conn_id, entries).await;
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
            vec![AckEntry { stream_id: seq_stream, seq: f.body.nack_seq.get() }],
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
        .map(|e| AckEntry { stream_id: seq_stream, seq: e.seq.get() })
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
    let f = match SubFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::BufferTooShort); return; }
    };
    if let Err(code) = f.validate() {
        send_error_v2(registry, conn_id, req_seq, code);
        return;
    }
    // H1: subscribe filter validated too.
    let sub_filter = f.filter();
    if !sub_filter.is_empty()
        && arbitro_proto::validate::validate_subject(sub_filter).is_err()
    {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
        return;
    }
    let consumer_id = ConsumerId(f.body.consumer_id.get());
    let seq_stream = match server.names().consumer_stream(consumer_id) {
        Some(s) => s,
        None => { send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerNotFound); return; }
    };
    let queue_id = server
        .names()
        .consumer_queue(consumer_id)
        .unwrap_or_else(|| server.names().get_or_create_queue(seq_stream, b""));
    let shard = server.shard_for(seq_stream);

    let filter = f.filter().to_vec();
    let filters = if filter.is_empty() { vec![] } else { vec![filter] };

    // deliver_policy from consumer config (stored at CreateConsumer time).
    // Default: 0 = All (replay from beginning). The NameRegistry can hold
    // per-consumer deliver_policy for management-API consumers.
    let (deliver_policy, start_seq) = server
        .names()
        .consumer_deliver_policy(consumer_id)
        .unwrap_or((0, 0));

    let reply = shard
        .subscribe(
            StreamConfig { id: seq_stream, name: vec![] },
            ConsumerConfig {
                id: consumer_id,
                queue_id,
                stream_id: seq_stream,
                durable: true,
                // v2 SubFrame body has no ack-policy field; default to Explicit.
                ack_policy: AckPolicy::Explicit,
                max_inflight: u32::MAX,
                ack_wait_ms: 0,
            },
            SubscriptionConfig {
                id: SubscriptionId(consumer_id.0),
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
        Ok(true)  => send_rep_ok_v2(registry, conn_id, req_seq, consumer_id.0 as u64),
        Ok(false) => send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerNotFound),
        Err(_)    => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::BufferTooShort); return; }
    };
    let consumer_id = ConsumerId(body.consumer_id);
    let seq_stream = match server.names().consumer_stream(consumer_id) {
        Some(s) => s,
        None => { send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerNotFound); return; }
    };
    let shard = server.shard_for(seq_stream);

    match shard.unsubscribe(SubscriptionId(consumer_id.0)).await {
        Ok(_)  => send_rep_ok_v2(registry, conn_id, req_seq, req_seq),
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
    let f = match CreateStreamFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let name = f.name();
    // H1: validate name + filter at the dispatch boundary BEFORE
    // allocating IDs. Rejects empty / oversized / weird-byte names so
    // catalog Vec indexes stay sane and DeleteStream/wire echoes
    // don't have to handle pathological input.
    if arbitro_proto::validate::validate_name(name).is_err() {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
        return;
    }
    let filter = f.filter();
    if !filter.is_empty()
        && arbitro_proto::validate::validate_subject(filter).is_err()
    {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
        return;
    }
    let wire_stream = arbitro_engine_v2::catalog::wire_hash_32(name);
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

    let max_msgs   = f.body.max_msgs.get();
    let max_bytes  = f.body.max_bytes.get();
    let max_age_ms = f.body.max_age_secs.get().saturating_mul(1_000);
    let idempotency_window_ms = f.body.idempotency_window_ms.get();

    match shard
        .create_stream(
            StreamConfig { id: seq_stream, name: name.to_vec() },
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
            server.names().set_stream_idempotency(seq_stream, idempotency_window_ms);

            // F37: invalidate the list_streams / list_consumers TTL
            // cache so the next list-RPC reflects this new stream.
            server.invalidate_list_cache();

            // Persist to command log on cold path — idempotent on replay.
            if let Some(log) = server.command_log() {
                let cmd = build_create_stream(&frame[HEADER_SIZE..]);
                if let Err(e) = log.record(&cmd) {
                    tracing::warn!(error = %e, "command log: create_stream record failed");
                }
            }
            send_rep_ok_v2(registry, conn_id, req_seq, wire_stream as u64)
        }
        Ok(_)    => send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamAlreadyExists),
        Err(_)   => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let name = body.name.as_slice();
    let wire_stream = arbitro_engine_v2::catalog::wire_hash_32(name);
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => { send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound); return; }
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let name = body.name.as_slice();
    let wire_stream = arbitro_engine_v2::catalog::wire_hash_32(name);
    match server.names().stream_seq(wire_stream) {
        Some(_) => send_rep_ok_v2(registry, conn_id, req_seq, wire_stream as u64),
        None    => send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound),
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let name = body.name.as_slice();
    let wire_stream = arbitro_engine_v2::catalog::wire_hash_32(name);
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None    => { send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound); return; }
    };
    let shard = server.shard_for(seq_stream);
    match shard.purge_stream(seq_stream).await {
        Ok(deleted) => send_rep_ok_v2(registry, conn_id, req_seq, deleted),
        Err(_)      => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let name = body.name.as_slice();
    let subject = body.subject;
    let wire_stream = arbitro_engine_v2::catalog::wire_hash_32(name);
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None    => { send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound); return; }
    };
    let shard = server.shard_for(seq_stream);
    match shard.drain_subject(seq_stream, subject).await {
        Ok(deleted) => send_rep_ok_v2(registry, conn_id, req_seq, deleted),
        Err(_)      => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
    }
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
        let wire_id = server.names().stream_wire(StreamId(*seq_id)).unwrap_or(*seq_id);
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
    let f = match CreateConsumerFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let wire_stream = f.body.stream_id.get();
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => { send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound); return; }
    };
    let name = f.name();
    let group = f.group();
    // H1: validate consumer name + (optional) group + (optional)
    // subject filter at the dispatch boundary. Same reasoning as
    // v2_create_stream — keep weird bytes from leaking into the
    // engine catalog / NameRegistry maps.
    if arbitro_proto::validate::validate_name(name).is_err() {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
        return;
    }
    if !group.is_empty()
        && arbitro_proto::validate::validate_name(group).is_err()
    {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
        return;
    }
    let subject_filter = f.subject();
    if !subject_filter.is_empty()
        && arbitro_proto::validate::validate_subject(subject_filter).is_err()
    {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
        return;
    }

    let ack_policy = match f.body.ack_policy {
        0 => AckPolicy::None,
        _ => AckPolicy::Explicit,
    };

    // B6: parse subject_limits FIRST so a malformed trailer is rejected
    // BEFORE we allocate a ConsumerId / queue / index entries — the
    // previous order leaked one ConsumerId slot per malformed
    // CreateConsumer, and combined with the (now-also-fixed) SLOT_COUNT
    // panic turned a single hostile client into a 30-second DoS.
    let subject_limits = if ack_policy == AckPolicy::Explicit {
        match f.subject_limits() {
            Some(v) => v,
            None => {
                send_error_v2(registry, conn_id, req_seq, ErrorCode::InvalidLength);
                return;
            }
        }
    } else {
        Vec::new()
    };

    let (seq_consumer, _created) = server.names().get_or_create_consumer(name);
    // B1: registry refused — consumer slot pool exhausted.
    if seq_consumer.raw() == u32::MAX {
        send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerAlreadyExists);
        return;
    }
    let shard = server.shard_for(seq_stream);

    let queue_id = server.names().get_or_create_queue(seq_stream, group);
    server.names().set_consumer_queue(seq_consumer, queue_id);
    server.names().set_consumer_stream(seq_consumer, seq_stream);
    server.names().set_consumer_deliver_policy(
        seq_consumer,
        f.body.deliver_policy,
        f.body.start_seq.get(),
    );

    match shard
        .create_consumer(
            ConsumerConfig {
                id: seq_consumer,
                queue_id,
                stream_id: seq_stream,
                durable: true,
                ack_policy,
                max_inflight: if f.body.max_inflight.get() == 0 {
                    u32::MAX
                } else {
                    f.body.max_inflight.get() as u32
                },
                ack_wait_ms: f.body.ack_wait_ms.get(),
            },
            subject_limits,
        )
        .await
    {
        Ok(true) => {
            // F37: a new consumer must show up in list_consumers reply.
            server.invalidate_list_cache();
            if let Some(log) = server.command_log() {
                let cmd = build_create_consumer(&frame[HEADER_SIZE..]);
                if let Err(e) = log.record(&cmd) {
                    tracing::warn!(error = %e, "command log: create_consumer record failed");
                }
            }
            send_rep_ok_v2(registry, conn_id, req_seq, seq_consumer.0 as u64)
        }
        Ok(_)    => send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerAlreadyExists),
        Err(_)   => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let consumer_id = ConsumerId(body.consumer_id);

    // F14: route directly to the owning shard when we know it.
    // Fall back to fanning out if the consumer→stream mapping is unknown
    // (recovery edge cases, manual control-plane calls).
    let candidate_shards: smallvec::SmallVec<[usize; 1]> = match server
        .names()
        .consumer_stream(consumer_id)
    {
        Some(stream) => {
            let idx = stream.raw() as usize % server.shard_count();
            smallvec::smallvec![idx]
        }
        None => (0..server.shard_count()).collect(),
    };

    for i in candidate_shards {
        if let Ok(_) = server.shard(i).delete_consumer(consumer_id).await {
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
    }
    send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let consumer_id = ConsumerId(body.consumer_id);
    let candidate_shards: smallvec::SmallVec<[usize; 1]> = match server
        .names()
        .consumer_stream(consumer_id)
    {
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let consumer_id = ConsumerId(body.consumer_id);
    let candidate_shards: smallvec::SmallVec<[usize; 1]> = match server
        .names()
        .consumer_stream(consumer_id)
    {
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
    let f = match ConsumerStatsFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let consumer_id = ConsumerId(f.body.consumer_id.get());

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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let name = body.name.as_slice();
    match server.names().consumer_id(name) {
        Some(id) => send_rep_ok_v2(registry, conn_id, req_seq, id.0 as u64),
        None     => send_error_v2(registry, conn_id, req_seq, ErrorCode::ConsumerNotFound),
    }
}

async fn v2_list_consumers(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let f = match ListConsumersFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    // Wire stream_id is the client-side hash; 0 = list all consumers.
    // Translate to the sequential engine id used inside the shard reply.
    let wire_filter = f.body.stream_id.get();
    // None = no filter (return all); Some(seq) = filter by engine seq_id.
    // Unknown wire hash → Some(u32::MAX) which matches nothing → empty list.
    let seq_filter: Option<u32> = if wire_filter == 0 {
        None  // no filter
    } else {
        Some(server.names().stream_seq(wire_filter).map(|s| s.raw()).unwrap_or(u32::MAX))
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

    // Apply stream filter when the client requested it.
    let filtered: Vec<(u32, u32, u32, bool)> = match seq_filter {
        None      => all_consumers.as_ref().clone(),
        Some(seq) => all_consumers
            .iter()
            .copied()
            .filter(|(_, sid, _, _)| *sid == seq)
            .collect(),
    };

    let entry_size = 13;
    let body_len = 4 + filtered.len() * entry_size;
    let total = HEADER_SIZE + body_len;
    let mut buf = BytesMut::with_capacity(total);

    let header = Header::new(Action::ListConsumers.as_u16(), body_len as u32, req_seq);
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(&(filtered.len() as u32).to_le_bytes());
    for (consumer_id, stream_id, queue_id, paused) in &filtered {
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
) {
    let shards = server.shard_count();
    let cid = ConnectionId(conn_id);
    tracing::debug!(target = "dispatch", conn = conn_id, shards, "v2_disconnect: draining all shards");

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

    tracing::debug!(target = "dispatch", conn = conn_id, "v2_disconnect: drains complete");
    registry.remove(conn_id);
}

fn v2_ping(conn_id: u64, registry: &ConnectionRegistry) {
    // Reply with a Pong header (no body).
    let header = Header::new(Action::Pong.as_u16(), 0, 0);
    registry.send_parts(conn_id, &[header.as_bytes()]);
}
