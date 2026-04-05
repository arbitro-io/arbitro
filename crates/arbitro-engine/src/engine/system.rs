//! System handlers — connect, disconnect, ack, nack, ping, stats.
//!
//! Ack/nack access drain via streams.with_mut (shard lock) — no global Mutex.
//! Signal drain after credit change to trigger pending deliveries.

use core::sync::atomic::Ordering::Relaxed;

use arbitro_proto::error::ErrorCode;
use arbitro_proto::ids::ConnId;
use arbitro_proto::wire::delivery::{AckView, BatchAckView};
use arbitro_proto::wire::envelope::FrameView;

use super::context::{Context, ConnState};
use super::reply;

/// Handle a new connection.
pub fn on_connect(ctx: &Context, conn_id: ConnId, token: &[u8]) {
    if !ctx.auth.check(conn_id, token) {
        reply::send_error(ctx.transport.as_ref(), conn_id, 0, 0, 0, ErrorCode::AuthFailed);
        ctx.transport.close(conn_id);
        return;
    }

    let mut conns = ctx.connections.lock().unwrap();
    conns.insert(conn_id, ConnState {
        conn_id,
        authenticated: true,
        subscriptions: Vec::new(),
    });
    ctx.metrics.connections.fetch_add(1, Relaxed);
}

/// Handle a disconnection — cleanup all subscriptions.
pub fn on_disconnect(ctx: &Context, conn_id: ConnId) {
    // Get subscription list for this connection
    let subs = {
        let mut conns = ctx.connections.lock().unwrap();
        conns.remove(&conn_id)
            .map(|cs| cs.subscriptions)
            .unwrap_or_default()
    };

    // Unbind from each stream's drain (one shard lock per stream)
    // Collect unique stream_ids to avoid locking the same shard twice
    let mut seen_streams = Vec::new();
    for &(stream_id, _) in &subs {
        if !seen_streams.contains(&stream_id) {
            seen_streams.push(stream_id);
            ctx.streams.with_mut(stream_id, |slot| {
                slot.drain.unbind_conn(conn_id);
            });
        }
    }

    ctx.metrics.connections.fetch_sub(1, Relaxed);
}

/// Handle an Ack frame — release credit, signal drain for pending deliveries.
#[inline]
pub fn on_ack(ctx: &Context, conn_id: ConnId, frame: &FrameView<'_>) {
    let stream_id = frame.stream_id();
    let body = frame.body();
    let view = AckView::new(body);
    let consumer_id = view.consumer_id();
    let seq = view.sequence();

    // Single shard lock: release credit + signal drain
    let acked = ctx.streams.with_mut(stream_id, |slot| {
        let result = slot.drain.on_ack(consumer_id, seq);
        if result {
            slot.signal.release();
        }
        result
    }).unwrap_or(false);

    if !acked {
        let env_seq = frame.envelope().env_seq.get();
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, seq, ErrorCode::ConsumerNotFound);
    }

    ctx.metrics.msgs_out.fetch_add(1, Relaxed);
}

/// Handle a BatchAck frame — release credit for all sequences, signal drain.
#[inline]
pub fn on_batch_ack(ctx: &Context, _conn_id: ConnId, frame: &FrameView<'_>) {
    let stream_id = frame.stream_id();
    let body = frame.body();
    let view = BatchAckView::new(body);
    let consumer_id = view.consumer_id();

    // Collect sequences into a stack-allocated or small vec
    let seqs: Vec<u64> = view.sequences().collect();

    ctx.streams.with_mut(stream_id, |slot| {
        let count = slot.drain.on_batch_ack(consumer_id, &seqs);
        if count > 0 {
            slot.signal.release();
        }
    });

    ctx.metrics.msgs_out.fetch_add(seqs.len() as u64, Relaxed);
}

/// Handle a Nack frame — release credit, queue for redelivery, signal drain.
#[inline]
pub fn on_nack(ctx: &Context, conn_id: ConnId, frame: &FrameView<'_>) {
    let stream_id = frame.stream_id();
    let body = frame.body();
    let view = AckView::new(body);
    let consumer_id = view.consumer_id();
    let seq = view.sequence();

    // Single shard lock: release credit + nack + signal drain
    let nacked = ctx.streams.with_mut(stream_id, |slot| {
        let result = slot.drain.on_nack(consumer_id, seq);
        if result {
            slot.signal.release();
        }
        result
    }).unwrap_or(false);

    if !nacked {
        let env_seq = frame.envelope().env_seq.get();
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, seq, ErrorCode::ConsumerNotFound);
    }
}

/// Handle Stats request — respond with metrics snapshot.
pub fn on_stats(ctx: &Context, conn_id: ConnId, frame: &FrameView<'_>) {
    use arbitro_proto::action::Action;
    use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
    use arbitro_proto::wire::metrics::StatsResponse;
    use zerocopy::IntoBytes;
    use zerocopy::byteorder::little_endian::{U16, U32, U64};

    let env_seq = frame.envelope().env_seq.get();
    let snap = ctx.metrics.snapshot();

    let response = StatsResponse {
        request_id: U64::new(env_seq as u64),
        connections: U64::new(snap.connections),
        total_msgs_in: U64::new(snap.msgs_in),
        total_msgs_out: U64::new(snap.msgs_out),
        total_bytes_in: U64::new(snap.bytes_in),
        total_bytes_out: U64::new(snap.bytes_out),
        streams: U64::new(snap.streams),
        consumers: U64::new(snap.consumers),
    };

    let envelope = Envelope {
        action: U16::new(Action::StatsReply.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(core::mem::size_of::<StatsResponse>() as u32),
        env_seq: U32::new(env_seq),
    };

    let mut env_buf = [0u8; ENVELOPE_SIZE];
    env_buf.copy_from_slice(envelope.as_bytes());
    ctx.transport.send_parts(conn_id, &[&env_buf, response.as_bytes()]);
}

/// Handle Ping — respond with Pong.
pub fn on_ping(ctx: &Context, conn_id: ConnId, frame: &FrameView<'_>) {
    use arbitro_proto::action::Action;
    use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
    use zerocopy::IntoBytes;
    use zerocopy::byteorder::little_endian::{U16, U32};

    let envelope = Envelope {
        action: U16::new(Action::Pong.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(frame.msg_len()),
        env_seq: U32::new(frame.envelope().env_seq.get()),
    };
    let mut buf = [0u8; ENVELOPE_SIZE];
    buf.copy_from_slice(envelope.as_bytes());
    ctx.transport.send(conn_id, &buf);
}
