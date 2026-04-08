//! Subscribe/unsubscribe and consumer management handlers.
//!
//! All drain access through streams.with_mut — no global drains Mutex.
//! Drain lives inside StreamSlot (R8, R19).

use arbitro_proto::action::Action;
use arbitro_proto::config::{ConsumerConfig, DeliverMode, DeliverPolicy};
use arbitro_proto::error::ErrorCode;
use arbitro_proto::ids::ConnId;
use arbitro_proto::metadata::MetadataCommand;

use arbitro_common::subject::patterns_overlap;

use super::context::Context;
use super::reply;

/// Create a consumer on a stream. Cold path.
pub fn on_create_consumer(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32, mut config: ConsumerConfig) {
    // 1. Check if a consumer with this name already exists for this stream
    let existing_id = {
        let consumers = ctx.consumers.lock().unwrap();
        consumers.iter()
            .find(|(&(sid, _), cfg)| sid == stream_id && cfg.name == config.name)
            .map(|(&( _, cid), _)| cid)
    };

    if let Some(id) = existing_id {
        // Reuse existing consumer ID
        ctx.metrics.consumers.fetch_add(0, core::sync::atomic::Ordering::Relaxed); // No increment
        reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, id as u64);
        return;
    }

    // 2. Not found: Assign new consumer_id
    let consumer_id = ctx.alloc_consumer_id();
    config.consumer_id = consumer_id;
    config.stream_id = stream_id;

    // Check filter overlap for Queue mode consumers (needs read access to drain)
    if config.deliver_mode == DeliverMode::Queue {
        let overlap = ctx.streams.with(stream_id, |slot| {
            for existing in slot.drain.iter_consumers() {
                if existing.config.deliver_mode != DeliverMode::Queue {
                    continue;
                }
                for new_filter in config.filters.iter() {
                    for existing_filter in existing.config.filters.iter() {
                        if patterns_overlap(new_filter, existing_filter) {
                            return true;
                        }
                    }
                }
            }
            false
        });

        match overlap {
            Some(true) => {
                reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::ConsumerFilterOverlap);
                return;
            }
            None => {
                reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::StreamNotFound);
                return;
            }
            Some(false) => {} // no overlap, proceed
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

    // Register in drain (inside StreamSlot)
    let registered = ctx.streams.with_mut(stream_id, |slot| {
        slot.drain.add_consumer(config.clone(), start_seq);
    });

    if registered.is_none() {
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::StreamNotFound);
        return;
    }

    // Store consumer config
    {
        let mut consumers = ctx.consumers.lock().unwrap();
        consumers.insert((stream_id, consumer_id), config.clone());
    }

    // Record to persistent log if present
    if let Some(log) = ctx.metadata.read().as_ref() {
        let _ = log.record(&MetadataCommand::CreateConsumer(config));
    }

    ctx.metrics.consumers.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, consumer_id as u64);
}

/// Delete a consumer. Cold path.
pub fn on_delete_consumer(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32, consumer_id: u32) {
    let removed = ctx.streams.with_mut(stream_id, |slot| {
        slot.drain.remove_consumer(consumer_id)
    }).unwrap_or(false);

    if removed {
        // Record to persistent log if present
        if let Some(log) = ctx.metadata.read().as_ref() {
            let _ = log.record(&MetadataCommand::DeleteConsumer { stream_id, consumer_id });
        }

        let mut consumers = ctx.consumers.lock().unwrap();
        consumers.remove(&(stream_id, consumer_id));
        ctx.metrics.consumers.fetch_sub(1, core::sync::atomic::Ordering::Relaxed);
        reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, consumer_id as u64);
    } else {
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::ConsumerNotFound);
    }
}

/// Bind a consumer to a connection (subscribe). Cold path.
/// Signals drain to deliver backlog.
pub fn on_subscribe(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32, consumer_id: u32) {
    let bound = ctx.streams.with_mut(stream_id, |slot| {
        let result = slot.drain.bind(consumer_id, conn_id);
        if result {
            slot.signal.release();
        }
        result
    }).unwrap_or(false);

    if bound {
        // Track subscription on connection state
        let mut conns = ctx.connections.lock().unwrap();
        if let Some(state) = conns.get_mut(&conn_id) {
            state.subscriptions.push((stream_id, consumer_id));
        }
        reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, consumer_id as u64);
    } else {
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::ConsumerNotFound);
    }
}

/// Unbind a consumer from a connection (unsubscribe). Cold path.
pub fn on_unsubscribe(ctx: &Context, conn_id: ConnId, stream_id: u32, env_seq: u32, consumer_id: u32) {
    let unbound = ctx.streams.with_mut(stream_id, |slot| {
        slot.drain.unbind(consumer_id, conn_id)
    }).unwrap_or(false);

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

    // Get runtime state from drain (inside StreamSlot)
    let (pending_count, deliver_seq) = ctx.streams.with(stream_id, |slot| {
        if let Some(c) = slot.drain.find_consumer(consumer_id) {
            (c.pending_count, c.deliver_seq)
        } else {
            (0, 0)
        }
    }).unwrap_or((0, 0));

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

    for cfg in &stream_consumers {
        let (pending, deliver_seq) = ctx.streams.with(stream_id, |slot| {
            if let Some(c) = slot.drain.find_consumer(cfg.consumer_id) {
                (c.pending_count, c.deliver_seq)
            } else {
                (0, 0)
            }
        }).unwrap_or((0, 0));
        buf.extend_from_slice(&serialize_consumer_info(cfg, pending, deliver_seq));
    }

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
