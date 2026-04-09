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

use arbitro_engine_v2::batch::{AckEntry, NackEntry};
use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine_v2::types::*;
use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::wire::delivery::{
    AckView, BatchAckView, DeliveryEntryHeader, NackView, RepBatchFixed, RepErrorAction,
    RepOkAction, DELIVERY_ENTRY_HEADER_SIZE, REP_BATCH_FIXED_SIZE,
};
use arbitro_proto::wire::envelope::{Envelope, FrameView, ENVELOPE_SIZE};
use arbitro_proto::wire::manager::{CreateConsumerView, DeleteConsumerView};
use arbitro_proto::wire::publish::BatchIter;
use arbitro_proto::wire::stream::{CreateStreamView, DeleteStreamView};
use arbitro_proto::wire::subscribe::{FetchView, SubscribeView, UnsubscribeView};
use arbitro_proto::wire::system::{ConnectView, ConnectedAction, PingView, PongAction};
use bytes::{Bytes, BytesMut};
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use crate::command::PublishEntryOwned;
use crate::command_log::SharedCommandLog;
use crate::router::Server;
use crate::transport::ConnectionRegistry;

/// Dispatch a raw frame to the appropriate shard.
pub async fn dispatch_frame(
    conn_id: u64,
    frame: Bytes,
    server: &Server,
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
        Action::Publish => dispatch_publish(conn_id, stream_id, env_seq, &frame, server, registry).await,
        Action::Ack => dispatch_ack(stream_id, body, server).await,
        Action::AckSync => dispatch_ack_sync(conn_id, stream_id, env_seq, body, server, registry).await,
        Action::Nack => dispatch_nack(stream_id, body, server).await,
        Action::BatchAck => dispatch_batch_ack(stream_id, body, server).await,
        Action::BatchAckSync => dispatch_batch_ack_sync(conn_id, stream_id, env_seq, body, server, registry).await,

        // ── Subscription ────────────────────────────────────────────
        Action::Subscribe => dispatch_subscribe(conn_id, stream_id, env_seq, body, server, registry).await,
        Action::Unsubscribe => dispatch_unsubscribe(conn_id, stream_id, env_seq, body, server, registry).await,
        Action::Fetch => dispatch_fetch(conn_id, stream_id, body, server, registry).await,

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

/// Fire & forget — shard validates, stores, and replies directly.
async fn dispatch_publish(
    conn_id: u64,
    stream_id: u32,
    env_seq: u32,
    frame: &Bytes,
    server: &Server,
    registry: &ConnectionRegistry,
) {
    let shard = server.shard_for(StreamId(stream_id));
    let body = &frame[ENVELOPE_SIZE..];
    let iter = BatchIter::new(body);

    // Zero-copy: slice_ref returns a Bytes sharing the same Arc-backed buffer.
    let entries: Vec<PublishEntryOwned> = iter
        .map(|view| PublishEntryOwned {
            subject: frame.slice_ref(view.subject()),
            payload: frame.slice_ref(view.payload()),
        })
        .collect();

    // Shard handles: validate stream → store.append → RepOk + gate.release
    if let Err(_) = shard.publish(StreamId(stream_id), conn_id, env_seq, entries).await {
        send_error(registry, conn_id, env_seq, ErrorCode::InternalError);
    }
}

async fn dispatch_ack(stream_id: u32, body: &[u8], server: &Server) {
    let view = AckView::new(body);
    let shard = server.shard_for(StreamId(stream_id));
    let now = timestamp_now();
    let _ = shard
        .ack(ConsumerId(view.consumer_id()), vec![AckEntry { seq: view.sequence() }], now)
        .await;
}

async fn dispatch_nack(stream_id: u32, body: &[u8], server: &Server) {
    let view = NackView::new(body);
    let shard = server.shard_for(StreamId(stream_id));
    let now = timestamp_now();
    let _ = shard
        .nack(ConsumerId(view.consumer_id()), vec![NackEntry { seq: view.sequence(), retry_at: None }], now)
        .await;
}

async fn dispatch_ack_sync(
    conn_id: u64,
    stream_id: u32,
    env_seq: u32,
    body: &[u8],
    server: &Server,
    registry: &ConnectionRegistry,
) {
    let view = AckView::new(body);
    let shard = server.shard_for(StreamId(stream_id));
    let now = timestamp_now();
    match shard
        .ack(ConsumerId(view.consumer_id()), vec![AckEntry { seq: view.sequence() }], now)
        .await
    {
        Ok(reply) => send_rep_ok(registry, conn_id, env_seq, reply.accepted as u64),
        Err(_) => send_error(registry, conn_id, env_seq, ErrorCode::InternalError),
    }
}

async fn dispatch_batch_ack(stream_id: u32, body: &[u8], server: &Server) {
    let view = BatchAckView::new(body);
    let shard = server.shard_for(StreamId(stream_id));
    let now = timestamp_now();
    let entries: Vec<AckEntry> = view.sequences().map(|seq| AckEntry { seq }).collect();
    let _ = shard.ack(ConsumerId(view.consumer_id()), entries, now).await;
}

async fn dispatch_batch_ack_sync(
    conn_id: u64,
    stream_id: u32,
    env_seq: u32,
    body: &[u8],
    server: &Server,
    registry: &ConnectionRegistry,
) {
    let view = BatchAckView::new(body);
    let shard = server.shard_for(StreamId(stream_id));
    let now = timestamp_now();
    let entries: Vec<AckEntry> = view.sequences().map(|seq| AckEntry { seq }).collect();
    match shard.ack(ConsumerId(view.consumer_id()), entries, now).await {
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
    server: &Server,
    registry: &ConnectionRegistry,
) {
    let view = SubscribeView::new(body);
    let shard = server.shard_for(StreamId(stream_id));
    let consumer_id = view.consumer_id();
    let now = timestamp_now();

    let subject = view.subject().to_vec();
    let group = view.group();
    // group empty → default to stream_id (same as hashing the stream name)
    let queue_id = if group.is_empty() {
        QueueId(stream_id)
    } else {
        QueueId(arbitro_engine_v2::catalog::fnv1a_32(group))
    };

    let reply = shard
        .subscribe(
            StreamConfig {
                id: StreamId(stream_id),
                name: vec![],
            },
            ConsumerConfig {
                id: ConsumerId(consumer_id),
                queue_id,
                stream_id: StreamId(stream_id),
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
                stream_id: StreamId(stream_id),
                consumer_id: ConsumerId(consumer_id),
                filters: if subject.is_empty() {
                    vec![]
                } else {
                    vec![subject]
                },
            },
            ConnectionId(conn_id),
            now,
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
    server: &Server,
    registry: &ConnectionRegistry,
) {
    let view = UnsubscribeView::new(body);
    let shard = server.shard_for(StreamId(stream_id));
    let now = timestamp_now();

    match shard
        .unsubscribe(
            SubscriptionId(view.consumer_id()),
            DrainMode::ReleaseAndRequeue,
            now,
        )
        .await
    {
        Ok(_) => send_rep_ok(registry, conn_id, env_seq, env_seq as u64),
        Err(_) => send_error(registry, conn_id, env_seq, ErrorCode::InternalError),
    }
}

async fn dispatch_fetch(
    conn_id: u64,
    stream_id: u32,
    body: &[u8],
    server: &Server,
    registry: &ConnectionRegistry,
) {
    let view = FetchView::new(body);
    let shard = server.shard_for(StreamId(stream_id));
    let consumer_id = view.consumer_id();
    let now = timestamp_now();

    let result = shard
        .claim(
            QueueId(consumer_id),
            ConnectionId(conn_id),
            ConsumerId(consumer_id),
            view.max_msgs() as u16,
            now,
        )
        .await;

    match result {
        Ok(reply) => {
            send_rep_batch(registry, conn_id, consumer_id, &reply);
        }
        Err(_) => {
            send_error(registry, conn_id, 0, ErrorCode::InternalError);
        }
    }
}

// ── Stream management dispatchers ──────────────────────────────────────────

async fn dispatch_create_stream(
    conn_id: u64,
    env_seq: u32,
    body: &[u8],
    server: &Server,
    registry: &ConnectionRegistry,
    command_log: Option<&SharedCommandLog>,
) {
    let view = CreateStreamView::new(body);
    let name = view.name();
    let stream_id = arbitro_engine_v2::catalog::fnv1a_32(name);
    let shard = server.shard_for(StreamId(stream_id));

    match shard
        .create_stream(
            StreamConfig {
                id: StreamId(stream_id),
                name: name.to_vec(),
            },
            view.journal_kind(),
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
            send_rep_ok(registry, conn_id, env_seq, stream_id as u64);
        }
        Ok(_) => send_error(registry, conn_id, env_seq, ErrorCode::StreamAlreadyExists),
        Err(_) => send_error(registry, conn_id, env_seq, ErrorCode::InternalError),
    }
}

async fn dispatch_delete_stream(
    conn_id: u64,
    env_seq: u32,
    body: &[u8],
    server: &Server,
    registry: &ConnectionRegistry,
    command_log: Option<&SharedCommandLog>,
) {
    let view = DeleteStreamView::new(body);
    let name = view.name();
    let stream_id = arbitro_engine_v2::catalog::fnv1a_32(name);
    let shard = server.shard_for(StreamId(stream_id));

    match shard
        .delete_stream(StreamId(stream_id), DrainMode::ReleaseAndRequeue, true)
        .await
    {
        Ok(_) => {
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
    server: &Server,
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

    let envelope = Envelope {
        action: U16::new(Action::ListStreams.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(body_len as u32),
        env_seq: U32::new(env_seq),
    };
    buf.extend_from_slice(envelope.as_bytes());
    buf.extend_from_slice(&(all_streams.len() as u32).to_le_bytes());
    for (stream_id, name) in &all_streams {
        buf.extend_from_slice(&stream_id.to_le_bytes());
        buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
        buf.extend_from_slice(name);
    }

    registry.send_bytes(conn_id, buf.freeze());
}

async fn dispatch_list_consumers(
    conn_id: u64,
    env_seq: u32,
    server: &Server,
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

    let envelope = Envelope {
        action: U16::new(Action::ListConsumers.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(body_len as u32),
        env_seq: U32::new(env_seq),
    };
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
    server: &Server,
    registry: &ConnectionRegistry,
    command_log: Option<&SharedCommandLog>,
) {
    let view = CreateConsumerView::new(body);
    let stream_id = view.stream_id();
    let consumer_name = view.name();
    let consumer_id = arbitro_engine_v2::catalog::fnv1a_32(consumer_name);
    let shard = server.shard_for(StreamId(stream_id));

    let group = view.group();
    let queue_id = if group.is_empty() {
        QueueId(stream_id)
    } else {
        QueueId(arbitro_engine_v2::catalog::fnv1a_32(group))
    };

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
                id: ConsumerId(consumer_id),
                queue_id,
                stream_id: StreamId(stream_id),
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
            send_rep_ok(registry, conn_id, env_seq, consumer_id as u64);
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
    server: &Server,
    registry: &ConnectionRegistry,
    command_log: Option<&SharedCommandLog>,
) {
    let view = DeleteConsumerView::new(body);
    let shard = server.shard_for(StreamId(stream_id));

    match shard
        .delete_consumer(ConsumerId(view.consumer_id()), DrainMode::ReleaseAndRequeue)
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
    server: &Server,
    registry: &ConnectionRegistry,
) {
    let _view = ConnectView::new(body);
    let now = timestamp_now();

    for i in 0..server.shard_count() {
        let shard = server.shard(i);
        let _ = shard.open_connection(ConnectionId(conn_id), NodeId(1), now).await;
    }

    // Structs on stack, as_bytes() gives &[u8] pointing to them — no copy
    let envelope = Envelope {
        action: U16::new(Action::Connected.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(16),
        env_seq: U32::new(0),
    };
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
    server: &Server,
    registry: &ConnectionRegistry,
) {
    let now = timestamp_now();

    for i in 0..server.shard_count() {
        let shard = server.shard(i);
        let _ = shard
            .drain_connection(ConnectionId(conn_id), DrainMode::ReleaseAndRequeue, now)
            .await;
    }

    registry.remove(conn_id);
}

fn dispatch_ping(conn_id: u64, body: &[u8], registry: &ConnectionRegistry) {
    let view = PingView::new(body);
    let envelope = Envelope {
        action: U16::new(Action::Pong.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(8),
        env_seq: U32::new(0),
    };
    let pong = PongAction {
        ping_id: U64::new(view.ping_id()),
    };
    registry.send_parts(conn_id, &[envelope.as_bytes(), pong.as_bytes()]);
}

// ── Reply builders ─────────────────────────────────────────────────────────
//
// Pattern: build zerocopy structs on stack → send_parts with as_bytes().
// ONE alloc+copy in send_parts. No intermediate [u8; N] buffer.

/// Send RepOk. Client uses ref_seq differently per action:
/// - CreateConsumer → consumer_id
/// - Publish → first assigned sequence
/// - Others → echo of env_seq
#[inline]
fn send_rep_ok(registry: &ConnectionRegistry, conn_id: u64, env_seq: u32, ref_seq: u64) {
    let envelope = Envelope {
        action: U16::new(Action::RepOk.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(16),
        env_seq: U32::new(env_seq),
    };
    let body = RepOkAction {
        ref_seq: U64::new(ref_seq),
        _pad: U64::new(0),
    };
    registry.send_parts(conn_id, &[envelope.as_bytes(), body.as_bytes()]);
}

#[inline]
fn send_error(registry: &ConnectionRegistry, conn_id: u64, env_seq: u32, code: ErrorCode) {
    let envelope = Envelope {
        action: U16::new(Action::RepError.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(16),
        env_seq: U32::new(env_seq),
    };
    let body = RepErrorAction {
        ref_seq: U64::new(env_seq as u64),
        error_code: U16::new(code.as_u16()),
        _pad: [0u8; 6],
    };
    registry.send_parts(conn_id, &[envelope.as_bytes(), body.as_bytes()]);
}

/// Deliver notification (push mode). Minimal frame until drain_task
/// provides full payload via Store.
#[inline]
fn send_deliver(
    registry: &ConnectionRegistry,
    conn_id: u64,
    stream_id: u32,
    seq: u64,
) {
    let envelope = Envelope {
        action: U16::new(Action::Deliver.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(2),
        env_seq: U32::new(seq as u32),
    };
    let subj_len: [u8; 2] = [0, 0];
    registry.send_parts(conn_id, &[envelope.as_bytes(), &subj_len]);
}

/// RepBatch for Fetch results. Variable-length — builds Bytes directly.
fn send_rep_batch(
    registry: &ConnectionRegistry,
    conn_id: u64,
    consumer_id: u32,
    entries: &[arbitro_engine_v2::batch::ClaimedEntry],
) {
    if entries.is_empty() {
        return;
    }

    let body_len = REP_BATCH_FIXED_SIZE + entries.len() * DELIVERY_ENTRY_HEADER_SIZE;
    let total = ENVELOPE_SIZE + body_len;
    let mut buf = BytesMut::with_capacity(total);

    let envelope = Envelope {
        action: U16::new(Action::RepBatch.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(body_len as u32),
        env_seq: U32::new(0),
    };
    buf.extend_from_slice(envelope.as_bytes());

    let batch_fixed = RepBatchFixed {
        consumer_id: U32::new(consumer_id),
        count: U16::new(entries.len() as u16),
        _pad: U16::new(0),
    };
    buf.extend_from_slice(batch_fixed.as_bytes());

    for entry in entries {
        let header = DeliveryEntryHeader {
            seq: U64::new(entry.seq),
            subj_len: U16::new(0),
            data_len: U32::new(0),
        };
        buf.extend_from_slice(header.as_bytes());
    }

    registry.send_bytes(conn_id, buf.freeze());
}

#[inline]
fn timestamp_now() -> Timestamp {
    Timestamp::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    )
}
