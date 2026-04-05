//! Stream management handlers — all cold path.

use core::sync::atomic::Ordering::Relaxed;

use arbitro_proto::config::StreamConfig;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::ids::ConnId;

use super::context::Context;
use super::reply;

/// Create a stream. Cold path.
pub fn on_create_stream(ctx: &Context, conn_id: ConnId, env_seq: u32, config: StreamConfig) {
    let stream_id = config.stream_id;

    if ctx.streams.insert(config) {
        ctx.get_or_create_drain(stream_id);
        ctx.metrics.streams.fetch_add(1, Relaxed);
        reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0);
    } else {
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::StreamAlreadyExists);
    }
}

/// Delete a stream. Cold path.
pub fn on_delete_stream(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32) {
    if ctx.streams.remove(stream_id).is_some() {
        // Remove drain and all its consumers
        let mut drains = ctx.drains.lock().unwrap();
        drains.remove(&stream_id);

        // Remove consumer configs for this stream
        let mut consumers = ctx.consumers.lock().unwrap();
        consumers.retain(|&(sid, _), _| sid != stream_id);

        ctx.metrics.streams.fetch_sub(1, Relaxed);
        reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0);
    } else {
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::StreamNotFound);
    }
}

/// Purge all messages from a stream. Cold path.
pub fn on_purge_stream(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32) {
    let result = ctx.streams.with_mut(stream_id, |slot| {
        slot.store.purge()
    });

    match result {
        Some(deleted) => {
            reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, deleted);
        }
        None => {
            reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::StreamNotFound);
        }
    }
}

/// Drain messages by subject from a stream. Cold path.
pub fn on_drain_subject(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32, subject: &[u8]) {
    let result = ctx.streams.with_mut(stream_id, |slot| {
        slot.store.drain(subject)
    });

    match result {
        Some(deleted) => {
            reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, deleted);
        }
        None => {
            reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::StreamNotFound);
        }
    }
}
