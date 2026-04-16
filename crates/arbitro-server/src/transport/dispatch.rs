//! Frame dispatch — parse wire frames and route to engine shards.
//!
//! Each incoming frame is parsed via arbitro-proto's zero-copy views,
//! converted to engine types, and routed to the correct shard by stream_id.
//!
//! ## Zero-copy discipline
//!
//! All reply builders use `send_parts(&[envelope.as_bytes(), body.as_bytes()])`.
//! Structs live on the stack, `as_bytes()` returns `&[u8]` pointing to them
//! IN PLACE (zerocopy cast, not a copy). `send_parts` does exactly ONE
//! alloc+copy into the Bytes for the mpsc channel. No intermediate `[u8; N]`.

use arbitro_engine_v2::AckEntry;
use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine_v2::types::*;
use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::wire::delivery::{AckView, BatchAckView, NackView};
use arbitro_proto::wire::envelope::{Envelope, FrameView, ENVELOPE_SIZE};
use arbitro_proto::wire::manager::{CreateConsumerView, DeleteConsumerView};
use arbitro_proto::wire::publish::BatchIter;
use arbitro_proto::wire::stream::{CreateStreamView, DeleteStreamView};
use arbitro_proto::wire::subscribe::{SubscribeView, UnsubscribeView};
use arbitro_proto::wire::system::{ConnectView, ConnectedAction, PingView, PongAction};
use bytes::{Bytes, BytesMut};
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::U64;

use crate::common::reply::{send_error, send_rep_ok};
use crate::persistence::command_log::SharedCommandLog;
use crate::shard::command::PublishEntryOwned;
use crate::shard::router::ShardRouter;
use crate::transport::ConnectionRegistry;

/// Dispatch a raw frame to the appropriate shard.
pub async fn dispatch_frame(
    conn_id: u64,
    frame: Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    command_log: Option<&SharedCommandLog>,
) {
    let view = FrameView::new(&frame);
    let action = match view.action() {
        Some(a) => a,
        None => {
            send_error(registry, conn_id, 0, ErrorCode::UnknownAction);
            return;
        }
    };

    let stream_id = view.stream_id();
    let env_seq = view.envelope().env_seq.get();
    let body = view.body();

    match action {
        // ── Hot path ────────────────────────────────────────────────
        Action::Publish => dispatch_publish(conn_id, stream_id, env_seq, &frame, server, registry),
        Action::PublishAccumulate => dispatch_publish_accumulate(conn_id, stream_id, env_seq, &frame, server, registry).await,
        Action::Ack => dispatch_ack(stream_id, body, server).await,
        Action::AckSync => dispatch_ack_sync(conn_id, stream_id, env_seq, body, server, registry).await,
        Action::Nack => dispatch_nack(stream_id, body, server).await,
        Action::BatchAck => dispatch_batch_ack(stream_id, body, server).await,
        Action::BatchAckSync => dispatch_batch_ack_sync(conn_id, stream_id, env_seq, body, server, registry).await,

        // ── Subscription ────────────────────────────────────────────
        Action::Subscribe => dispatch_subscribe(conn_id, stream_id, env_seq, body, server, registry).await,
        Action::Unsubscribe => dispatch_unsubscribe(conn_id, stream_id, env_seq, body, server, registry).await,

        // ── Stream management ───────────────────────────────────────
        Action::CreateStream => dispatch_create_stream(conn_id, env_seq, body, server, registry, command_log).await,
        Action::DeleteStream => dispatch_delete_stream(conn_id, env_seq, body, server, registry, command_log).await,
        Action::ListStreams => dispatch_list_streams(conn_id, env_seq, server, registry).await,

        Action::ListConsumers => dispatch_list_consumers(conn_id, env_seq, server, registry).await,

        // ── Consumer management ─────────────────────────────────────
        Action::CreateConsumer => dispatch_create_consumer(conn_id, env_seq, body, server, registry, command_log).await,
        Action::DeleteConsumer => dispatch_delete_consumer(conn_id, stream_id, env_seq, body, server, registry, command_log).await,

        // ── System ──────────────────────────────────────────────────
        Action::Connect => dispatch_connect(conn_id, body, server, registry).await,
        Action::Disconnect => dispatch_disconnect(conn_id, server, registry).await,
        Action::Ping => dispatch_ping(conn_id, body, registry),
        Action::Pong => {}

        _ => {
            send_error(registry, conn_id, env_seq, ErrorCode::UnknownAction);
        }
    }
}

// ── Hot path dispatchers ───────────────────────────────────────────────────

/// Translate the client-computed wire stream id to the engine seq id, or
/// reply `StreamNotFound` and return `None`. See `common::name_registry` for
/// why this lives at the dispatch boundary.
#[inline]
fn translate_stream_or_error(
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    conn_id: u64,
    env_seq: u32,
    wire_stream: u32,
) -> Option<StreamId> {
    match server.names().stream_seq(wire_stream) {
        Some(seq) => Some(seq),
        None => {
            send_error(registry, conn_id, env_seq, ErrorCode::StreamNotFound);
            None
        }
    }
}

/// Same as `translate_stream_or_error` but silent — for fire-and-forget
/// paths (`Ack`, `Nack`) where there is no caller waiting for a reply.
#[inline]
fn translate_stream_silent(server: &ShardRouter, wire_stream: u32) -> Option<StreamId> {
    server.names().stream_seq(wire_stream)
}

/// Fire & forget — writes directly to the shared store, signals gate.
/// Does NOT go through the shard worker — publish and drain are
/// independent services connected only by store (data) and gate (notification).
fn dispatch_publish(
    conn_id: u64,
    stream_id: u32,
    env_seq: u32,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    crate::lifecycle_trace!("05_dispatch_publish_enter", conn_id, 0, "frame_loop");
    let seq_stream = match translate_stream_or_error(server, registry, conn_id, env_seq, stream_id) {
        Some(s) => s,
        None => return,
    };
    let body = &frame[ENVELOPE_SIZE..];
    let entries: Vec<PublishEntryOwned> = BatchIter::new(body)
        .map(|view| PublishEntryOwned::from_wire(&view, frame))
        .collect();
    crate::lifecycle_trace!("06_wire_parsed", conn_id, entries.len() as u64, "frame_loop");

    // Build store entries — zero-copy refs into owned Bytes.
    let store_entries: Vec<arbitro_store::EntryRef<'_>> = entries
        .iter()
        .map(|e| arbitro_store::EntryRef {
            stream_id: seq_stream.raw(),
            subject: &e.subject,
            payload: &e.payload,
            flags: 0,
        })
        .collect();

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    crate::lifecycle_trace!("11_pub_store_append_start", conn_id, 0, "frame_loop");

    let shared_store = server.store_for(seq_stream);
    let first_seq = match shared_store.lock().unwrap().append_batch(&store_entries, now_ms) {
        Ok(seq) => seq,
        Err(_) => {
            send_error(registry, conn_id, env_seq, ErrorCode::StreamFull);
            return;
        }
    };

    crate::lifecycle_trace!("12_pub_store_append_done", conn_id, first_seq, "frame_loop");

    send_rep_ok(registry, conn_id, env_seq, first_seq);
    crate::lifecycle_trace!("13_pub_rep_ok_sent", conn_id, first_seq, "frame_loop");

    // Notify drain — gate is the ONLY connection between publish and drain.
    server.gate_for(seq_stream).release();
    crate::lifecycle_trace!("14_pub_gate_released", conn_id, first_seq, "frame_loop");
}

/// PublishAccumulate — shard accumulates entries, flushes after deadline/threshold.
/// Same wire format as Publish, different action code.
async fn dispatch_publish_accumulate(
    conn_id: u64,
    stream_id: u32,
    env_seq: u32,
    frame: &Bytes,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let seq_stream = match translate_stream_or_error(server, registry, conn_id, env_seq, stream_id) {
        Some(s) => s,
        None => return,
    };
    let shard = server.shard_for(seq_stream);
    let body = &frame[ENVELOPE_SIZE..];
    let entries: Vec<PublishEntryOwned> = BatchIter::new(body)
        .map(|view| PublishEntryOwned::from_wire(&view, frame))
        .collect();

    if shard.publish_accumulate(seq_stream, conn_id, env_seq, entries).await.is_err() {
        send_error(registry, conn_id, env_seq, ErrorCode::InternalError);
    }
}

async fn dispatch_ack(stream_id: u32, body: &[u8], server: &ShardRouter) {
    crate::lifecycle_trace!("a05_dispatch_ack_enter", 0, 0, "frame_loop");
    let seq_stream = match translate_stream_silent(server, stream_id) {
        Some(s) => s,
        None => return,
    };
    let view = AckView::new(body);
    let shard = server.shard_for(seq_stream);
    let _ = shard
        .ack(ConsumerId(view.consumer_id()), vec![AckEntry { stream_id: seq_stream, seq: view.sequence() }])
        .await;
    crate::lifecycle_trace!("a18_dispatch_ack_returned", 0, 0, "frame_loop");
}

async fn dispatch_nack(stream_id: u32, body: &[u8], server: &ShardRouter) {
    let seq_stream = match translate_stream_silent(server, stream_id) {
        Some(s) => s,
        None => return,
    };
    let view = NackView::new(body);
    let shard = server.shard_for(seq_stream);
    let _ = shard
        .nack(ConsumerId(view.consumer_id()), vec![AckEntry { stream_id: seq_stream, seq: view.sequence() }])
        .await;
}

async fn dispatch_ack_sync(
    conn_id: u64,
    stream_id: u32,
    env_seq: u32,
    body: &[u8],
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let seq_stream = match translate_stream_or_error(server, registry, conn_id, env_seq, stream_id) {
        Some(s) => s,
        None => return,
    };
    let view = AckView::new(body);
    let shard = server.shard_for(seq_stream);
    match shard
        .ack(ConsumerId(view.consumer_id()), vec![AckEntry { stream_id: seq_stream, seq: view.sequence() }])
        .await
    {
        Ok(reply) => send_rep_ok(registry, conn_id, env_seq, reply.accepted as u64),
        Err(_) => send_error(registry, conn_id, env_seq, ErrorCode::InternalError),
    }
}

async fn dispatch_batch_ack(stream_id: u32, body: &[u8], server: &ShardRouter) {
    let seq_stream = match translate_stream_silent(server, stream_id) {
        Some(s) => s,
        None => return,
    };
    let view = BatchAckView::new(body);
    let shard = server.shard_for(seq_stream);
    let entries: Vec<AckEntry> = view.entries().map(|(seq, _hash)| AckEntry { stream_id: seq_stream, seq }).collect();
    let _ = shard.ack(ConsumerId(view.consumer_id()), entries).await;
}

async fn dispatch_batch_ack_sync(
    conn_id: u64,
    stream_id: u32,
    env_seq: u32,
    body: &[u8],
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let seq_stream = match translate_stream_or_error(server, registry, conn_id, env_seq, stream_id) {
        Some(s) => s,
        None => return,
    };
    let view = BatchAckView::new(body);
    let shard = server.shard_for(seq_stream);
    let entries: Vec<AckEntry> = view.entries().map(|(seq, _hash)| AckEntry { stream_id: seq_stream, seq }).collect();
    match shard.ack(ConsumerId(view.consumer_id()), entries).await {
        Ok(reply) => send_rep_ok(registry, conn_id, env_seq, reply.accepted as u64),
        Err(_) => send_error(registry, conn_id, env_seq, ErrorCode::InternalError),
    }
}

// ── Subscription dispatchers ───────────────────────────────────────────────

async fn dispatch_subscribe(
    conn_id: u64,
    stream_id: u32,
    env_seq: u32,
    body: &[u8],
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let seq_stream = match translate_stream_or_error(server, registry, conn_id, env_seq, stream_id) {
        Some(s) => s,
        None => return,
    };
    let view = SubscribeView::new(body);
    let shard = server.shard_for(seq_stream);
    let consumer_id = view.consumer_id();

    let subject = view.subject().to_vec();
    // The Subscribe wire body carries no group, so we cannot re-derive
    // the queue here. We MUST recover the exact queue id that
    // `dispatch_create_consumer` allocated for this consumer, otherwise
    // `ensure_subscription` would register the match table against a
    // different queue than the binding reads from — every claim would
    // pop an empty ring. Fall back to a per-stream default for the
    // never-explicitly-created path (subscribe-only flow).
    let queue_id = server
        .names()
        .consumer_queue(ConsumerId(consumer_id))
        .unwrap_or_else(|| server.names().get_or_create_queue(seq_stream, b""));

    let reply = shard
        .subscribe(
            StreamConfig {
                id: seq_stream,
                name: vec![],
            },
            ConsumerConfig {
                id: ConsumerId(consumer_id),
                queue_id,
                stream_id: seq_stream,
                durable: true,
                ack_policy: if view.deliver_mode() == 0 {
                    AckPolicy::None
                } else {
                    AckPolicy::Explicit
                },
                max_inflight: if view.max_inflight() == 0 { u32::MAX } else { view.max_inflight() as u32 },
            },
            SubscriptionConfig {
                id: SubscriptionId(consumer_id),
                stream_id: seq_stream,
                consumer_id: ConsumerId(consumer_id),
                filters: if subject.is_empty() {
                    vec![]
                } else {
                    vec![subject]
                },
            },
            ConnectionId(conn_id),
        )
        .await;

    match reply {
        Ok(true) => send_rep_ok(registry, conn_id, env_seq, env_seq as u64),
        _ => send_error(registry, conn_id, env_seq, ErrorCode::InternalError),
    }
}

async fn dispatch_unsubscribe(
    conn_id: u64,
    stream_id: u32,
    env_seq: u32,
    body: &[u8],
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let seq_stream = match translate_stream_or_error(server, registry, conn_id, env_seq, stream_id) {
        Some(s) => s,
        None => return,
    };
    let view = UnsubscribeView::new(body);
    let shard = server.shard_for(seq_stream);

    match shard
        .unsubscribe(
            SubscriptionId(view.consumer_id()),
        )
        .await
    {
        Ok(_) => send_rep_ok(registry, conn_id, env_seq, env_seq as u64),
        Err(_) => send_error(registry, conn_id, env_seq, ErrorCode::InternalError),
    }
}

// ── Stream management dispatchers ──────────────────────────────────────────

async fn dispatch_create_stream(
    conn_id: u64,
    env_seq: u32,
    body: &[u8],
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    command_log: Option<&SharedCommandLog>,
) {
    let view = CreateStreamView::new(body);
    let name = view.name();
    // Wire id is what the client computes locally (and ships in subsequent
    // frame envelopes); seq id is what the engine indexes by. The registry
    // pairs them up so the rest of dispatch can look the seq up by wire id.
    let wire_stream = arbitro_engine_v2::catalog::fnv1a_32(name);
    let (seq_stream, _created) = server.names().get_or_create_stream(wire_stream);
    let shard = server.shard_for(seq_stream);

    match shard
        .create_stream(
            StreamConfig {
                id: seq_stream,
                name: name.to_vec(),
            },
        )
        .await
    {
        Ok(true) => {
            if let Some(log) = command_log {
                let cmd = arbitro_proto::metadata::build_create_stream(body);
                if let Err(e) = log.record(&cmd) {
                    tracing::error!(error = %e, "failed to record CreateStream to command log");
                }
            }
            // Reply with the wire id — the client never reads it back, but
            // sticking to the convention keeps observability tools sane.
            send_rep_ok(registry, conn_id, env_seq, wire_stream as u64);
        }
        Ok(_) => send_error(registry, conn_id, env_seq, ErrorCode::StreamAlreadyExists),
        Err(_) => send_error(registry, conn_id, env_seq, ErrorCode::InternalError),
    }
}

async fn dispatch_delete_stream(
    conn_id: u64,
    env_seq: u32,
    body: &[u8],
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    command_log: Option<&SharedCommandLog>,
) {
    let view = DeleteStreamView::new(body);
    let name = view.name();
    let wire_stream = arbitro_engine_v2::catalog::fnv1a_32(name);
    let seq_stream = match server.names().stream_seq(wire_stream) {
        Some(s) => s,
        None => {
            send_error(registry, conn_id, env_seq, ErrorCode::StreamNotFound);
            return;
        }
    };
    let shard = server.shard_for(seq_stream);

    match shard
        .delete_stream(seq_stream, true)
        .await
    {
        Ok(_) => {
            // Drop the registry mapping AFTER the engine confirms removal,
            // so an in-flight publish racing with delete still finds the
            // seq id while the engine is the authoritative source.
            server.names().remove_stream(wire_stream);
            if let Some(log) = command_log {
                let cmd = arbitro_proto::metadata::build_delete_stream(body);
                if let Err(e) = log.record(&cmd) {
                    tracing::error!(error = %e, "failed to record DeleteStream to command log");
                }
            }
            send_rep_ok(registry, conn_id, env_seq, env_seq as u64);
        }
        Err(_) => send_error(registry, conn_id, env_seq, ErrorCode::InternalError),
    }
}

async fn dispatch_list_streams(
    conn_id: u64,
    env_seq: u32,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    // Fan out to all shards and merge results (cold path — allocs acceptable)
    let mut all_streams: Vec<(u32, Vec<u8>)> = Vec::new();

    for i in 0..server.shard_count() {
        let shard = server.shard(i);
        if let Ok(reply) = shard.list_streams().await {
            all_streams.extend(reply.streams);
        }
    }

    // Variable-length response — build Bytes directly, send via send_bytes
    let body_len: usize = 4 + all_streams.iter().map(|(_, n)| 6 + n.len()).sum::<usize>();
    let total = ENVELOPE_SIZE + body_len;
    let mut buf = BytesMut::with_capacity(total);

    let envelope = Envelope::new(Action::ListStreams, 0, body_len as u32, env_seq);
    buf.extend_from_slice(envelope.as_bytes());
    buf.extend_from_slice(&(all_streams.len() as u32).to_le_bytes());
    for (seq_id, name) in &all_streams {
        // Send back the WIRE id (what the client computes locally with
        // fnv1a_32), not the engine seq id, so client-side caches stay
        // consistent across list/create/delete.
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

async fn dispatch_list_consumers(
    conn_id: u64,
    env_seq: u32,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    // Fan out to all shards and merge results (cold path — allocs acceptable)
    let mut all_consumers: Vec<(u32, u32, u32, bool)> = Vec::new();

    for i in 0..server.shard_count() {
        let shard = server.shard(i);
        if let Ok(reply) = shard.list_consumers().await {
            all_consumers.extend(reply.consumers);
        }
    }

    // Wire format: [4 count][per entry: 4 consumer_id, 4 stream_id, 4 queue_id, 1 paused]
    let entry_size = 13; // 4+4+4+1
    let body_len = 4 + all_consumers.len() * entry_size;
    let total = ENVELOPE_SIZE + body_len;
    let mut buf = BytesMut::with_capacity(total);

    let envelope = Envelope::new(Action::ListConsumers, 0, body_len as u32, env_seq);
    buf.extend_from_slice(envelope.as_bytes());
    buf.extend_from_slice(&(all_consumers.len() as u32).to_le_bytes());
    for (consumer_id, stream_id, queue_id, paused) in &all_consumers {
        buf.extend_from_slice(&consumer_id.to_le_bytes());
        buf.extend_from_slice(&stream_id.to_le_bytes());
        buf.extend_from_slice(&queue_id.to_le_bytes());
        buf.extend_from_slice(&[*paused as u8]);
    }

    registry.send_bytes(conn_id, buf.freeze());
}

// ── Consumer management dispatchers ────────────────────────────────────────

async fn dispatch_create_consumer(
    conn_id: u64,
    env_seq: u32,
    body: &[u8],
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    command_log: Option<&SharedCommandLog>,
) {
    let view = CreateConsumerView::new(body);
    let wire_stream = view.stream_id();
    let seq_stream = match translate_stream_or_error(server, registry, conn_id, env_seq, wire_stream) {
        Some(s) => s,
        None => return,
    };
    let consumer_name = view.name();
    // Allocate a small sequential consumer id by name. The integer the
    // client receives in the reply is what it will echo on subsequent
    // wire frames (subscribe/ack/delete), so the engine never sees a
    // huge fnv1a_32 hash on the ConsumerId Vec index path.
    let (seq_consumer, _created) = server.names().get_or_create_consumer(consumer_name);
    let shard = server.shard_for(seq_stream);

    // Queue id is content-addressed by `(seq_stream, group)` so two
    // consumers with the same group on the same stream resolve to the
    // SAME ready ring (queue-group round-robin semantics, see
    // `name_registry.rs`). The original code used `fnv1a_32(group)` for
    // this, but that produces ~4B integers — fine for the catalog's
    // HashMap-keyed QueueId, fatal if any path ever indexes by it. We
    // also remember the resolved queue per consumer so the subscribe
    // path (which carries no group on the wire) can recover it.
    let group = view.group();
    let queue_id = server.names().get_or_create_queue(seq_stream, group);
    server.names().set_consumer_queue(seq_consumer, queue_id);

    let ack_policy = match view.ack_policy() {
        0 => AckPolicy::None,
        _ => AckPolicy::Explicit,
    };

    // Collect per-subject inflight limits from wire trailer.
    let max_subject_inflights: Vec<(Vec<u8>, u32)> = view
        .subject_limits()
        .map(|e| (e.pattern.to_vec(), e.limit))
        .collect();

    match shard
        .create_consumer(
            ConsumerConfig {
                id: seq_consumer,
                queue_id,
                stream_id: seq_stream,
                durable: true,
                ack_policy,
                max_inflight: if view.max_inflight() == 0 { u32::MAX } else { view.max_inflight() as u32 },
            },
            max_subject_inflights,
        )
        .await
    {
        Ok(true) => {
            if let Some(log) = command_log {
                let cmd = arbitro_proto::metadata::build_create_consumer(body);
                if let Err(e) = log.record(&cmd) {
                    tracing::error!(error = %e, "failed to record CreateConsumer to command log");
                }
            }
            send_rep_ok(registry, conn_id, env_seq, seq_consumer.0 as u64);
        }
        Ok(_) => send_error(registry, conn_id, env_seq, ErrorCode::ConsumerAlreadyExists),
        Err(_) => send_error(registry, conn_id, env_seq, ErrorCode::InternalError),
    }
}

async fn dispatch_delete_consumer(
    conn_id: u64,
    stream_id: u32,
    env_seq: u32,
    body: &[u8],
    server: &ShardRouter,
    registry: &ConnectionRegistry,
    command_log: Option<&SharedCommandLog>,
) {
    let seq_stream = match translate_stream_or_error(server, registry, conn_id, env_seq, stream_id) {
        Some(s) => s,
        None => return,
    };
    let view = DeleteConsumerView::new(body);
    let shard = server.shard_for(seq_stream);

    match shard
        .delete_consumer(ConsumerId(view.consumer_id()))
        .await
    {
        Ok(_) => {
            if let Some(log) = command_log {
                let cmd = arbitro_proto::metadata::build_delete_consumer(body);
                if let Err(e) = log.record(&cmd) {
                    tracing::error!(error = %e, "failed to record DeleteConsumer to command log");
                }
            }
            send_rep_ok(registry, conn_id, env_seq, env_seq as u64);
        }
        Err(_) => send_error(registry, conn_id, env_seq, ErrorCode::InternalError),
    }
}

// ── System dispatchers ─────────────────────────────────────────────────────

async fn dispatch_connect(
    conn_id: u64,
    body: &[u8],
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    let _view = ConnectView::new(body);

    for i in 0..server.shard_count() {
        let shard = server.shard(i);
        let _ = shard.open_connection(ConnectionId(conn_id), NodeId(1)).await;
    }

    // Structs on stack, as_bytes() gives &[u8] pointing to them — no copy
    let envelope = Envelope::new(Action::Connected, 0, 16, 0);
    let connected = ConnectedAction {
        conn_id: U64::new(conn_id),
        proto_version: 1,
        flags: 0,
        _pad: [0u8; 6],
    };
    registry.send_parts(conn_id, &[envelope.as_bytes(), connected.as_bytes()]);
}

async fn dispatch_disconnect(
    conn_id: u64,
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) {
    for i in 0..server.shard_count() {
        let shard = server.shard(i);
        let _ = shard
            .drain_connection(ConnectionId(conn_id))
            .await;
    }

    registry.remove(conn_id);
}

fn dispatch_ping(conn_id: u64, body: &[u8], registry: &ConnectionRegistry) {
    let view = PingView::new(body);
    let envelope = Envelope::new(Action::Pong, 0, 8, 0);
    let pong = PongAction {
        ping_id: U64::new(view.ping_id()),
    };
    registry.send_parts(conn_id, &[envelope.as_bytes(), pong.as_bytes()]);
}

