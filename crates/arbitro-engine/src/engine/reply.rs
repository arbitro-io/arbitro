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
