//! Stream management handlers — all cold path.

use core::sync::atomic::Ordering::Relaxed;

use arbitro_proto::action::Action;
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

/// Get stream info. Cold path.
///
/// Response body (all little-endian):
/// [8 messages][8 bytes][8 first_seq][8 last_seq][8 max_msgs][8 max_bytes][8 max_age_secs][1 replicas][1 journal_kind][1 retention][1 pad][4 name_len][name...]
pub fn on_get_stream(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32) {
    let result = ctx.streams.with(stream_id, |slot| {
        let info = slot.store.info();
        let cfg = &slot.config;
        let name = &cfg.name;

        let fixed_len = 8 * 7 + 4 + 4; // 7 u64s + 4 flags bytes + 4 name_len
        let mut buf = Vec::with_capacity(fixed_len + name.len());

        buf.extend_from_slice(&info.messages.to_le_bytes());
        buf.extend_from_slice(&info.bytes.to_le_bytes());
        buf.extend_from_slice(&info.first_seq.to_le_bytes());
        buf.extend_from_slice(&info.last_seq.to_le_bytes());
        buf.extend_from_slice(&cfg.max_msgs.to_le_bytes());
        buf.extend_from_slice(&cfg.max_bytes.to_le_bytes());
        buf.extend_from_slice(&cfg.max_age_secs.to_le_bytes());
        buf.push(cfg.replicas);
        buf.push(cfg.journal_kind as u8);
        buf.push(cfg.retention as u8);
        buf.push(0); // pad
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name);
        buf
    });

    match result {
        Some(body) => {
            reply::send_data(ctx.transport.as_ref(), conn_id, Action::RepOk, stream_id, env_seq, &body);
        }
        None => {
            reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::StreamNotFound);
        }
    }
}

/// List all streams. Cold path.
///
/// Response body: [4 count][stream_info_0][stream_info_1]...
/// Each stream_info: [4 stream_id][8 messages][8 bytes][4 name_len][name...]
pub fn on_list_streams(ctx: &Context, conn_id: ConnId, env_seq: u32) {
    let configs = ctx.streams.list_configs();
    let mut buf = Vec::new();
    buf.extend_from_slice(&(configs.len() as u32).to_le_bytes());

    for cfg in &configs {
        let info = ctx.streams.with(cfg.stream_id, |slot| slot.store.info());
        let info = info.unwrap_or_default();

        buf.extend_from_slice(&cfg.stream_id.to_le_bytes());
        buf.extend_from_slice(&info.messages.to_le_bytes());
        buf.extend_from_slice(&info.bytes.to_le_bytes());
        buf.extend_from_slice(&(cfg.name.len() as u32).to_le_bytes());
        buf.extend_from_slice(&cfg.name);
    }

    reply::send_data(ctx.transport.as_ref(), conn_id, Action::RepOk, 0, env_seq, &buf);
}
