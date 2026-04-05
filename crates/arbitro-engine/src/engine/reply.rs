//! Reply helpers — zero-alloc ok/error responses.
//!
//! All frames built on the stack. No Bytes::copy_from_slice.

use arbitro_proto::error::ErrorCode;
use arbitro_proto::ids::ConnId;

use crate::drain::frame_builder::{build_rep_ok, build_rep_error};
use crate::transport::Transport;

/// Send RepOk (32B stack frame) to a connection.
#[inline]
pub fn send_ok(transport: &dyn Transport, conn_id: ConnId, stream_id: u32, env_seq: u32, ref_seq: u64) {
    let frame = build_rep_ok(stream_id, env_seq, ref_seq);
    transport.send(conn_id, &frame);
}

/// Send RepError (32B stack frame) to a connection.
#[inline]
pub fn send_error(transport: &dyn Transport, conn_id: ConnId, stream_id: u32, env_seq: u32, ref_seq: u64, code: ErrorCode) {
    let frame = build_rep_error(stream_id, env_seq, ref_seq, code);
    transport.send(conn_id, &frame);
}

/// Send a RepOk envelope + variable-length body. Cold path only.
/// Used for info/query responses (GetStream, ListStreams, etc.).
pub fn send_data(transport: &dyn Transport, conn_id: ConnId, action: arbitro_proto::action::Action, stream_id: u32, env_seq: u32, body: &[u8]) {
    use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
    use zerocopy::IntoBytes;
    use zerocopy::byteorder::little_endian::{U16, U32};

    let envelope = Envelope {
        action: U16::new(action.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(body.len() as u32),
        env_seq: U32::new(env_seq),
    };
    let mut env_buf = [0u8; ENVELOPE_SIZE];
    env_buf.copy_from_slice(envelope.as_bytes());
    transport.send_parts(conn_id, &[&env_buf, body]);
}
