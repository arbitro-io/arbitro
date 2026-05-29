//! v2 reply builders — `RepOkFrame` (24B) and `RepErrFrame` (32B).
//!
//! Zerocopy struct on the stack, `as_bytes()` slice passed to `send_inline`.
//! F34: uses inline `Bytes` (no heap alloc for frames ≤ 31B).
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
/// F34: uses `send_inline` — RepOkFrame is 24B, fits inline `Bytes` (no heap alloc).
#[inline]
pub fn send_rep_ok_v2(registry: &ConnectionRegistry, conn_id: u64, req_seq: u64, ref_seq: u64) {
    let frame = RepOkFrame::new(req_seq, ref_seq);
    registry.send_inline(conn_id, frame.as_bytes());
}

/// Send a v2 `RepError`.
/// F34: RepErrFrame is 32B — marginal; still benefits from avoiding BytesMut.
#[inline]
pub fn send_error_v2(registry: &ConnectionRegistry, conn_id: u64, req_seq: u64, code: ErrorCode) {
    let frame = RepErrFrame::new(req_seq, req_seq, code.as_u16());
    registry.send_inline(conn_id, frame.as_bytes());
}
