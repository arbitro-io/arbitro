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
use arbitro_proto::v2::egress::ack_state::{AckBatchRespFrame, AckStateRepFrame};
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

/// Same reply, sent through the connection's own handle.
///
/// No `sessions.lock()` and no map lookup: a connection answering itself
/// does not need a directory of every other one. The registry version
/// stays for callers that only hold a `conn_id` — the drain, the cron
/// registry — which are not on the publish path.
#[inline]
pub fn reply_ok(conn: &crate::common::session::ConnHandle, req_seq: u64, ref_seq: u64) -> bool {
    let frame = RepOkFrame::new(req_seq, ref_seq);
    write_frame(conn, frame.as_bytes())
}

/// Straight to the socket when this thread owns it; the queue otherwise.
///
/// The direct door skips the channel AND the allocation — `try_write`
/// takes a `&[u8]`, so a 32-byte reply never becomes a heap `Bytes`.
///
/// The fallback is not a nicety. A connection whose socket is not on this
/// thread has TWO writers — this reply and the shard's drain — and the
/// channel's writer task is what stops them interleaving mid-frame.
/// Bypassing it for one of them would corrupt the stream.
#[inline]
fn write_frame(conn: &crate::common::session::ConnHandle, bytes: &[u8]) -> bool {
    use crate::transport::egress::Delivery;
    match crate::shard::local::with_egress(conn.conn_id, |e| e.send_slice(bytes)) {
        Some(Delivery::Dead) => false,
        Some(_) => true,
        None => conn.send(bytes::Bytes::copy_from_slice(bytes)),
    }
}

/// Error reply through the connection's own handle.
#[inline]
pub fn reply_err(
    conn: &crate::common::session::ConnHandle,
    req_seq: u64,
    code: ErrorCode,
) -> bool {
    let frame = RepErrFrame::new(req_seq, req_seq, code.as_u16());
    write_frame(conn, frame.as_bytes())
}

/// Send a v2 `RepError`.
/// F34: RepErrFrame is 32B — marginal; still benefits from avoiding BytesMut.
#[inline]
pub fn send_error_v2(registry: &ConnectionRegistry, conn_id: u64, req_seq: u64, code: ErrorCode) {
    let frame = RepErrFrame::new(req_seq, req_seq, code.as_u16());
    registry.send_inline(conn_id, frame.as_bytes());
}

/// Send a v2 `AckStateRep`. 56B — exceeds the 31B inline threshold, so
/// this goes through `send_bytes` (heap-allocated `Bytes`).
#[inline]
#[allow(clippy::too_many_arguments)]
pub fn send_ack_state_rep_v2(
    registry: &ConnectionRegistry,
    conn_id: u64,
    req_seq: u64,
    consumer_id: u32,
    generation: u32,
    cursor: u64,
    low_seq: u64,
    high_seq: u64,
    status: u32,
) {
    let f = AckStateRepFrame::new(
        req_seq,
        consumer_id,
        generation,
        cursor,
        low_seq,
        high_seq,
        status,
    );
    registry.send_bytes(conn_id, bytes::Bytes::copy_from_slice(f.as_bytes()));
}

/// Send a v2 `AckBatchResp`. 48B — exceeds the 31B inline threshold, so
/// this goes through `send_bytes` (heap-allocated `Bytes`).
#[inline]
#[allow(clippy::too_many_arguments)]
pub fn send_ack_batch_resp_v2(
    registry: &ConnectionRegistry,
    conn_id: u64,
    req_seq: u64,
    consumer_id: u32,
    new_cursor: u64,
    accepted: u32,
    ignored: u32,
    below_retention: u32,
    still_pending: u32,
    status: u32,
) {
    let f = AckBatchRespFrame::new(
        req_seq,
        consumer_id,
        new_cursor,
        accepted,
        ignored,
        below_retention,
        still_pending,
        status,
    );
    registry.send_bytes(conn_id, bytes::Bytes::copy_from_slice(f.as_bytes()));
}
