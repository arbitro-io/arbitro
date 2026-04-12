//! Shared reply builders — single source of truth for `RepOk`/`RepError` framing
//! and the `now` timestamp helper.
//!
//! Pattern: build zerocopy structs on the stack → `send_parts` with `as_bytes()`.
//! ONE alloc+copy in `send_parts`. No intermediate `[u8; N]` buffer.

use arbitro_engine_v2::types::Timestamp;
use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::wire::delivery::{RepErrorAction, RepOkAction};
use arbitro_proto::wire::envelope::Envelope;
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U64};

use crate::transport::ConnectionRegistry;

/// Send `RepOk`. `ref_seq` semantics depend on the action being acknowledged:
/// - `CreateConsumer` → consumer_id
/// - `Publish` → first assigned sequence
/// - Others → echo of `env_seq`
#[inline]
pub fn send_rep_ok(registry: &ConnectionRegistry, conn_id: u64, env_seq: u32, ref_seq: u64) {
    let envelope = Envelope::new(Action::RepOk, 0, 16, env_seq);
    let body = RepOkAction {
        ref_seq: U64::new(ref_seq),
        _pad: U64::new(0),
    };
    registry.send_parts(conn_id, &[envelope.as_bytes(), body.as_bytes()]);
}

/// Send `RepError`.
#[inline]
pub fn send_error(registry: &ConnectionRegistry, conn_id: u64, env_seq: u32, code: ErrorCode) {
    let envelope = Envelope::new(Action::RepError, 0, 16, env_seq);
    let body = RepErrorAction {
        ref_seq: U64::new(env_seq as u64),
        error_code: U16::new(code.as_u16()),
        _pad: [0u8; 6],
    };
    registry.send_parts(conn_id, &[envelope.as_bytes(), body.as_bytes()]);
}

/// Wall-clock millis since UNIX epoch as a `Timestamp`.
///
/// Cold-path helper. **Never call inside the drainer hot loop** — hoist once
/// per drain cycle and pass `Timestamp` down.
#[inline]
pub fn timestamp_now() -> Timestamp {
    Timestamp::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    )
}
