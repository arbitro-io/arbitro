//! v2 reply builders — `RepOkFrame` (24B) and `RepErrFrame` (32B).
//!
//! Same pattern as `reply.rs` (v1): zerocopy struct on the stack, single
//! `as_bytes()` slice into `send_parts`. ONE alloc+copy in the registry.
//!
//! `ref_seq` semantics mirror v1:
//! - CreateConsumer → consumer_id
//! - Publish        → first assigned sequence
//! - Others         → echo of the request `seq` (header.seq)

use arbitro_proto::error::ErrorCode;
use arbitro_proto::v2::egress::rep_frame::{RepErrFrame, RepOkFrame};
use zerocopy::IntoBytes;

use crate::transport::ConnectionRegistry;

/// Send a v2 `RepOk`. `req_seq` is the request's `header.seq` being answered.
#[inline]
pub fn send_rep_ok_v2(registry: &ConnectionRegistry, conn_id: u64, req_seq: u64, ref_seq: u64) {
    let frame = RepOkFrame::new(req_seq, ref_seq);
    registry.send_parts(conn_id, &[frame.as_bytes()]);
}

/// Send a v2 `RepError`.
#[inline]
pub fn send_error_v2(registry: &ConnectionRegistry, conn_id: u64, req_seq: u64, code: ErrorCode) {
    let frame = RepErrFrame::new(req_seq, req_seq, code.as_u16());
    registry.send_parts(conn_id, &[frame.as_bytes()]);
}
