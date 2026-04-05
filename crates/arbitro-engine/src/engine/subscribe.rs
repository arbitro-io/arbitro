//! Subscribe/unsubscribe and consumer management handlers.

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
