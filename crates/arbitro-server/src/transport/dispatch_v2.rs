//! v2 frame dispatch — sole dispatcher in the server.
//!
//! HELLO is mandatory at the start of every connection (`server.rs::read_loop`
//! enforces it). All subsequent traffic is `Header`-prefixed v2 frames.
//!
//! Scope:
//!   * Hot path:  Publish, PublishBatch, Ack, BatchAck, Subscribe, Unsubscribe
//!   * Mgmt:      CreateStream/DeleteStream/GetStream/PurgeStream/DrainSubject/ListStreams
//!                CreateConsumer/DeleteConsumer/GetConsumer/ListConsumers
//!   * System:    Disconnect, Ping, Pong (no-op or trivial reply)
//!
//! ## Dropped (intentional v1→v2 regression)
//!
//!   * `Nack`           — no v2 frame yet, returns `UnknownAction`.
//!   * `AckSync`/`BatchAckSync` — collapsed into fire-and-forget Ack/BatchAck.
//!   * `PublishAccumulate` — accumulator path is v1-only for now.
//!   * `PublishWithReply` / `PublishWithHeaders` — not implemented.
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
use arbitro_proto::v2::ingress::batch_pub_frame::BatchPubFrame;
use arbitro_proto::v2::ingress::pub_frame::PubFrame;
use arbitro_proto::v2::ingress::sub_frame::SubFrame;
use arbitro_proto::v2::manager::consumer_mgmt::{
    CreateConsumerFrame, DeleteConsumerFrame, GetConsumerFrame, ListConsumersFrame,
};
use arbitro_proto::v2::manager::stream_mgmt::{
    CreateStreamFrame, DeleteStreamFrame, DrainSubjectFrame, GetStreamFrame,
    PurgeStreamFrame,
};
use bytes::{Bytes, BytesMut};
use zerocopy::FromBytes;
use zerocopy::IntoBytes;

use crate::common::reply_v2::{send_error_v2, send_rep_ok_v2};
use crate::shard::router::ShardRouter;
use crate::transport::ConnectionRegistry;

/// Dispatch one v2 frame. `frame` covers `[Header(16) || body(msg_len)]`.
pub async fn dispatch_frame_v2(
    conn_id: u64,
    frame: Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    if frame.len() < HEADER_SIZE {
        return;
    }
    let header = match Header::ref_from_bytes(&frame[..HEADER_SIZE]) {
        Ok(h) => h,
        Err(_) => return,
    };
    let action = match Action::from_u16(header.action.get()) {
        Some(a) => a,
        None => {
            send_error_v2(registry, conn_id, header.seq.get(), ErrorCode::UnknownAction);
            return;
        }
    };
    let req_seq = header.seq.get();

    match action {
        // ── Hot path ────────────────────────────────────────────────
        Action::Publish        => v2_publish(conn_id, req_seq, &frame, server, registry),
        Action::PublishBatch   => v2_publish_batch(conn_id, req_seq, &frame, server, registry),
        Action::Ack            => v2_ack(&frame, server).await,
        Action::BatchAck       => v2_batch_ack(&frame, server).await,
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

        // ── System ──────────────────────────────────────────────────
        Action::Disconnect     => v2_disconnect(conn_id, server, registry).await,
        Action::Ping           => v2_ping(conn_id, registry),
        Action::Pong           => {} // ignore
        Action::Connect        => {
            // v2 has no Connect — HELLO is the handshake. Ack quietly.
        }

        // Anything else (Nack, AckSync, accumulators, with-reply/with-headers, ...)
        // is unsupported in this iteration.
        _ => send_error_v2(registry, conn_id, req_seq, ErrorCode::UnknownAction),
    }
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let wire_stream = f.body.stream_id.get();
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => { send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound); return; }
    };

    let entries = [arbitro_store::EntryRef {
        stream_id: seq_stream.raw(),
        subject: f.subject(),
        payload: f.payload(),
        flags: 0,
    }];

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let shared_store = server.store_for(seq_stream);
    let first_seq = match shared_store.lock().unwrap().append_batch(&entries, now_ms) {
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let wire_stream = f.body.stream_id.get();
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => { send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound); return; }
    };

    // Collect entry views; subject/payload are zero-copy slices into `frame`.
    let entry_views: Vec<_> = f.iter().collect();
    let entries: Vec<arbitro_store::EntryRef<'_>> = entry_views
        .iter()
        .map(|v| arbitro_store::EntryRef {
            stream_id: seq_stream.raw(),
            subject: v.subject(),
            payload: v.payload(),
            flags: 0,
        })
        .collect();

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let shared_store = server.store_for(seq_stream);
    let first_seq = match shared_store.lock().unwrap().append_batch(&entries, now_ms) {
        Ok(seq) => seq,
        Err(_) => {
            send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamFull);
            return;
        }
    };

    send_rep_ok_v2(registry, conn_id, req_seq, first_seq);
    server.gate_for(seq_stream).release();
}

async fn v2_ack(frame: &Bytes, server: &ShardRouter) {
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
            vec![AckEntry { stream_id: seq_stream, seq: f.body.ack_seq.get() }],
        )
        .await;
}

async fn v2_batch_ack(frame: &Bytes, server: &ShardRouter) {
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
    let entries: Vec<AckEntry> = f
        .entries()
        .iter()
        .map(|e| AckEntry { stream_id: seq_stream, seq: e.seq.get() })
        .collect();
    let _ = shard.ack(consumer_id, entries).await;
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
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
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
            },
            SubscriptionConfig {
                id: SubscriptionId(consumer_id.0),
                stream_id: seq_stream,
                consumer_id,
                filters,
            },
            ConnectionId(conn_id),
        )
        .await;

    match reply {
        Ok(true) => send_rep_ok_v2(registry, conn_id, req_seq, req_seq),
        _        => send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError),
    }
}

async fn v2_unsubscribe(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    // Body shape is identical to Subscribe (see module comment).
    let f = match SubFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let consumer_id = ConsumerId(f.body.consumer_id.get());
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
    let wire_stream = arbitro_engine_v2::catalog::wire_hash_32(name);
    let (seq_stream, _created) = server.names().get_or_create_stream(wire_stream);
    let shard = server.shard_for(seq_stream);

    match shard
        .create_stream(StreamConfig { id: seq_stream, name: name.to_vec() })
        .await
    {
        Ok(true) => send_rep_ok_v2(registry, conn_id, req_seq, wire_stream as u64),
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
    let f = match DeleteStreamFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let name = f.name();
    let wire_stream = arbitro_engine_v2::catalog::wire_hash_32(name);
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => { send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound); return; }
    };
    let shard = server.shard_for(seq_stream);

    match shard.delete_stream(seq_stream, true).await {
        Ok(_) => {
            server.names().remove_stream(wire_stream);
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
    let f = match GetStreamFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let name = f.name();
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
    let f = match PurgeStreamFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let name = f.name();
    let wire_stream = arbitro_engine_v2::catalog::wire_hash_32(name);
    match server.names().stream_seq(wire_stream) {
        Some(_) => send_rep_ok_v2(registry, conn_id, req_seq, req_seq),
        None    => send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound),
    }
}

async fn v2_drain_subject(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let f = match DrainSubjectFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let name = f.name();
    let wire_stream = arbitro_engine_v2::catalog::wire_hash_32(name);
    match server.names().stream_seq(wire_stream) {
        Some(_) => send_rep_ok_v2(registry, conn_id, req_seq, req_seq),
        None    => send_error_v2(registry, conn_id, req_seq, ErrorCode::StreamNotFound),
    }
}

async fn v2_list_streams(
    conn_id: u64,
    req_seq: u64,
    _frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let mut all_streams: Vec<(u32, Vec<u8>)> = Vec::new();
    for i in 0..server.shard_count() {
        if let Ok(reply) = server.shard(i).list_streams().await {
            all_streams.extend(reply.streams);
        }
    }

    let body_len: usize = 4 + all_streams.iter().map(|(_, n)| 6 + n.len()).sum::<usize>();
    let total = HEADER_SIZE + body_len;
    let mut buf = BytesMut::with_capacity(total);

    let header = Header::new(Action::ListStreams.as_u16(), body_len as u32, req_seq);
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(&(all_streams.len() as u32).to_le_bytes());
    for (seq_id, name) in &all_streams {
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
    let (seq_consumer, _created) = server.names().get_or_create_consumer(name);
    let shard = server.shard_for(seq_stream);

    let queue_id = server.names().get_or_create_queue(seq_stream, group);
    server.names().set_consumer_queue(seq_consumer, queue_id);
    server.names().set_consumer_stream(seq_consumer, seq_stream);

    let ack_policy = match f.body.ack_policy {
        0 => AckPolicy::None,
        _ => AckPolicy::Explicit,
    };

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
            },
            Vec::new(),
        )
        .await
    {
        Ok(true) => send_rep_ok_v2(registry, conn_id, req_seq, seq_consumer.0 as u64),
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
    let f = match DeleteConsumerFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let consumer_id = ConsumerId(f.body.consumer_id.get());

    for i in 0..server.shard_count() {
        if let Ok(_) = server.shard(i).delete_consumer(consumer_id).await {
            send_rep_ok_v2(registry, conn_id, req_seq, req_seq);
            return;
        }
    }
    send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError);
}

async fn v2_get_consumer(
    conn_id: u64,
    req_seq: u64,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let f = match GetConsumerFrame::ref_from_bytes(&frame[..]) {
        Ok(f) => f,
        Err(_) => { send_error_v2(registry, conn_id, req_seq, ErrorCode::InternalError); return; }
    };
    let _stream_id = f.body.stream_id.get();
    let _name = f.name();
    send_rep_ok_v2(registry, conn_id, req_seq, req_seq);
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
    let _filter_stream = f.body.stream_id.get();

    let mut all_consumers: Vec<(u32, u32, u32, bool)> = Vec::new();
    for i in 0..server.shard_count() {
        if let Ok(reply) = server.shard(i).list_consumers().await {
            all_consumers.extend(reply.consumers);
        }
    }

    let entry_size = 13;
    let body_len = 4 + all_consumers.len() * entry_size;
    let total = HEADER_SIZE + body_len;
    let mut buf = BytesMut::with_capacity(total);

    let header = Header::new(Action::ListConsumers.as_u16(), body_len as u32, req_seq);
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(&(all_consumers.len() as u32).to_le_bytes());
    for (consumer_id, stream_id, queue_id, paused) in &all_consumers {
        buf.extend_from_slice(&consumer_id.to_le_bytes());
        buf.extend_from_slice(&stream_id.to_le_bytes());
        buf.extend_from_slice(&queue_id.to_le_bytes());
        buf.extend_from_slice(&[*paused as u8]);
    }
    registry.send_bytes(conn_id, buf.freeze());
}

// ── System ─────────────────────────────────────────────────────────────────

/// Server-side disconnect: drain across all shards, drop the connection.
pub(crate) async fn v2_disconnect(
    conn_id: u64,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    for i in 0..server.shard_count() {
        let _ = server.shard(i).drain_connection(ConnectionId(conn_id)).await;
    }
    registry.remove(conn_id);
}

fn v2_ping(conn_id: u64, registry: &ConnectionRegistry) {
    // Reply with a Pong header (no body).
    let header = Header::new(Action::Pong.as_u16(), 0, 0);
    registry.send_parts(conn_id, &[header.as_bytes()]);
}
