//! System handlers — connect, disconnect, ack, ping.

use core::sync::atomic::Ordering::Relaxed;

use arbitro_proto::error::ErrorCode;
use arbitro_proto::ids::ConnId;
use arbitro_proto::wire::delivery::AckView;
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
    });
    ctx.metrics.connections.fetch_add(1, Relaxed);
}

/// Handle a disconnection — cleanup all subscriptions.
pub fn on_disconnect(ctx: &Context, conn_id: ConnId) {
    // Unbind all consumers on this connection
    let mut drains = ctx.drains.lock().unwrap();
    for drain in drains.values_mut() {
        drain.unbind_conn(conn_id);
    }
    drop(drains);

    let mut conns = ctx.connections.lock().unwrap();
    conns.remove(&conn_id);
    ctx.metrics.connections.fetch_sub(1, Relaxed);
}

/// Handle an Ack frame.
#[inline]
pub fn on_ack(ctx: &Context, conn_id: ConnId, frame: &FrameView<'_>) {
    let stream_id = frame.stream_id();
    let body = frame.body();
    let view = AckView::new(body);
    let consumer_id = view.consumer_id();
    let seq = view.sequence();

    let acked = {
        let mut drains = ctx.drains.lock().unwrap();
        if let Some(drain) = drains.get_mut(&stream_id) {
            drain.ack(consumer_id, seq)
        } else {
            false
        }
    };

    if !acked {
        let env_seq = frame.envelope().env_seq.get();
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, seq, ErrorCode::ConsumerNotFound);
    }

    ctx.metrics.msgs_out.fetch_add(1, Relaxed);
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
    // Pong is the same envelope with action swapped to Pong
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
