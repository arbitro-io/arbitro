//! Subscribe/unsubscribe and consumer management handlers.

use arbitro_proto::action::Action;
use arbitro_proto::config::{ConsumerConfig, DeliverMode, DeliverPolicy};
use arbitro_proto::error::ErrorCode;
use arbitro_proto::ids::ConnId;

use arbitro_common::subject::patterns_overlap;

use super::context::Context;
use super::reply;

/// Create a consumer on a stream. Cold path.
pub fn on_create_consumer(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32, mut config: ConsumerConfig) {
    // Assign consumer_id
    let consumer_id = ctx.alloc_consumer_id();
    config.consumer_id = consumer_id;
    config.stream_id = stream_id;

    // Verify stream exists
    let stream_exists = ctx.streams.with(stream_id, |_| ()).is_some();
    if !stream_exists {
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::StreamNotFound);
        return;
    }

    // Check filter overlap for Queue mode consumers
    if config.deliver_mode == DeliverMode::Queue {
        let drains = ctx.drains.lock().unwrap();
        if let Some(drain) = drains.get(&stream_id) {
            for existing in drain.iter_consumers() {
                if existing.config.deliver_mode != DeliverMode::Queue {
                    continue;
                }
                // Check each filter pair for overlap
                for new_filter in config.filters.iter() {
                    for existing_filter in existing.config.filters.iter() {
                        if patterns_overlap(new_filter, existing_filter) {
                            reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::ConsumerFilterOverlap);
                            return;
                        }
                    }
                }
            }
        }
    }

    // Determine start sequence
    let start_seq = match config.deliver_policy {
        DeliverPolicy::All => {
            ctx.streams.with(stream_id, |slot| slot.store.info().first_seq).unwrap_or(1)
        }
        DeliverPolicy::New => {
            ctx.streams.with(stream_id, |slot| slot.store.info().last_seq + 1).unwrap_or(1)
        }
        DeliverPolicy::ByStartSeq => config.start_seq,
    };

    // Register in drain
    {
        let mut drains = ctx.drains.lock().unwrap();
        if let Some(drain) = drains.get_mut(&stream_id) {
            drain.add_consumer(config.clone(), start_seq);
        }
    }

    // Store consumer config
    {
        let mut consumers = ctx.consumers.lock().unwrap();
        consumers.insert((stream_id, consumer_id), config);
    }

    ctx.metrics.consumers.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, consumer_id as u64);
}

/// Delete a consumer. Cold path.
pub fn on_delete_consumer(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32, consumer_id: u32) {
    let removed = {
        let mut drains = ctx.drains.lock().unwrap();
        if let Some(drain) = drains.get_mut(&stream_id) {
            drain.remove_consumer(consumer_id)
        } else {
            false
        }
    };

    if removed {
        let mut consumers = ctx.consumers.lock().unwrap();
        consumers.remove(&(stream_id, consumer_id));
        ctx.metrics.consumers.fetch_sub(1, core::sync::atomic::Ordering::Relaxed);
        reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, consumer_id as u64);
    } else {
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::ConsumerNotFound);
    }
}

/// Bind a consumer to a connection (subscribe). Cold path.
pub fn on_subscribe(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32, consumer_id: u32) {
    let bound = {
        let mut drains = ctx.drains.lock().unwrap();
        if let Some(drain) = drains.get_mut(&stream_id) {
            drain.bind(consumer_id, conn_id)
        } else {
            false
        }
    };

    if bound {
        reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, consumer_id as u64);
    } else {
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::ConsumerNotFound);
    }
}

/// Unbind a consumer from a connection (unsubscribe). Cold path.
pub fn on_unsubscribe(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32, consumer_id: u32) {
    let unbound = {
        let mut drains = ctx.drains.lock().unwrap();
        if let Some(drain) = drains.get_mut(&stream_id) {
            drain.unbind(consumer_id)
        } else {
            false
        }
    };

    if unbound {
        reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, consumer_id as u64);
    } else {
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::ConsumerNotFound);
    }
}

/// Get consumer info. Cold path.
///
/// Response body (all little-endian):
/// [4 consumer_id][4 stream_id][2 max_inflight][1 ack_policy][1 deliver_policy]
/// [1 deliver_mode][3 pad][4 ack_wait_ms][8 start_seq][4 pending_count][8 deliver_seq]
/// [4 name_len][name...][4 filter_count][filter_0_len][filter_0]...
pub fn on_get_consumer(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32, consumer_id: u32) {
    let consumers = ctx.consumers.lock().unwrap();
    let config = match consumers.get(&(stream_id, consumer_id)) {
        Some(cfg) => cfg.clone(),
        None => {
            drop(consumers);
            reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::ConsumerNotFound);
            return;
        }
    };
    drop(consumers);

    // Get runtime state from drain
    let (pending_count, deliver_seq) = {
        let drains = ctx.drains.lock().unwrap();
        if let Some(drain) = drains.get(&stream_id) {
            if let Some(c) = drain.find_consumer(consumer_id) {
                (c.pending_count, c.deliver_seq)
            } else {
                (0, 0)
            }
        } else {
            (0, 0)
        }
    };

    let buf = serialize_consumer_info(&config, pending_count, deliver_seq);
    reply::send_data(ctx.transport.as_ref(), conn_id, Action::RepOk, stream_id, env_seq, &buf);
}

/// List all consumers on a stream. Cold path.
///
/// Response body: [4 count][consumer_info_0][consumer_info_1]...
pub fn on_list_consumers(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32) {
    let consumers = ctx.consumers.lock().unwrap();
    let stream_consumers: Vec<_> = consumers.iter()
        .filter(|(&(sid, _), _)| sid == stream_id)
        .map(|(_, cfg)| cfg.clone())
        .collect();
    drop(consumers);

    let mut buf = Vec::new();
    buf.extend_from_slice(&(stream_consumers.len() as u32).to_le_bytes());

    let drains = ctx.drains.lock().unwrap();
    for cfg in &stream_consumers {
        let (pending, deliver_seq) = if let Some(drain) = drains.get(&stream_id) {
            if let Some(c) = drain.find_consumer(cfg.consumer_id) {
                (c.pending_count, c.deliver_seq)
            } else {
                (0, 0)
            }
        } else {
            (0, 0)
        };
        buf.extend_from_slice(&serialize_consumer_info(cfg, pending, deliver_seq));
    }
    drop(drains);

    reply::send_data(ctx.transport.as_ref(), conn_id, Action::RepOk, stream_id, env_seq, &buf);
}

/// Serialize a consumer config + runtime state into bytes. Cold path.
fn serialize_consumer_info(config: &ConsumerConfig, pending_count: u32, deliver_seq: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64 + config.name.len());

    buf.extend_from_slice(&config.consumer_id.to_le_bytes());
    buf.extend_from_slice(&config.stream_id.to_le_bytes());
    buf.extend_from_slice(&config.max_inflight.to_le_bytes());
    buf.push(config.ack_policy as u8);
    buf.push(config.deliver_policy as u8);
    buf.push(config.deliver_mode as u8);
    buf.extend_from_slice(&[0u8; 3]); // pad
    buf.extend_from_slice(&config.ack_wait_ms.to_le_bytes());
    buf.extend_from_slice(&config.start_seq.to_le_bytes());
    buf.extend_from_slice(&pending_count.to_le_bytes());
    buf.extend_from_slice(&deliver_seq.to_le_bytes());
    buf.extend_from_slice(&(config.name.len() as u32).to_le_bytes());
    buf.extend_from_slice(&config.name);

    buf.extend_from_slice(&(config.filters.len() as u32).to_le_bytes());
    for filter in config.filters.iter() {
        buf.extend_from_slice(&(filter.len() as u32).to_le_bytes());
        buf.extend_from_slice(filter);
    }

    buf
}
