//! Reply helpers — zero-alloc ok/error responses.
//!
//! All frames built on the stack. No Bytes::copy_from_slice.

use arbitro_proto::error::ErrorCode;
use arbitro_proto::ids::ConnId;
use arbitro_proto::wire::envelope::Envelope;
use arbitro_proto::wire::headers::{RepOkHeader, RepErrorHeader};
use arbitro_proto::wire::delivery::{RepOkAction, RepErrorAction};
use arbitro_proto::action::Action;
use crate::transport::Transport;
use zerocopy::{IntoBytes, byteorder::little_endian::{U16, U32, U64}};

/// Send RepOk (32B stack frame) to a connection.
#[inline]
pub fn send_ok(transport: &dyn Transport, conn_id: ConnId, stream_id: u32, env_seq: u32, ref_seq: u64) {
    let header = RepOkHeader {
        env: Envelope {
            action: U16::new(Action::RepOk.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(stream_id),
            msg_len: U32::new(16),
            env_seq: U32::new(env_seq),
        },
        body: RepOkAction {
            ref_seq: U64::new(ref_seq),
            _pad: U64::new(0),
        },
    };
    transport.send(conn_id, header.as_bytes());
}

/// Send RepError (32B stack frame) to a connection.
#[inline]
pub fn send_error(transport: &dyn Transport, conn_id: ConnId, stream_id: u32, env_seq: u32, ref_seq: u64, code: ErrorCode) {
    let header = RepErrorHeader {
        env: Envelope {
            action: U16::new(Action::RepError.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(stream_id),
            msg_len: U32::new(16),
            env_seq: U32::new(env_seq),
        },
        body: RepErrorAction {
            ref_seq: U64::new(ref_seq),
            error_code: U16::new(code.as_u16()),
            _pad: [0u8; 6],
        },
    };
    transport.send(conn_id, header.as_bytes());
}

/// Send a RepOk envelope + variable-length body. Cold path only.
/// Used for info/query responses (GetStream, ListStreams, etc.).
pub fn send_data(transport: &dyn Transport, conn_id: ConnId, action: Action, stream_id: u32, env_seq: u32, body: &[u8]) {

    let envelope = Envelope {
        action: U16::new(action.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(body.len() as u32),
        env_seq: U32::new(env_seq),
    };
    
    transport.send_parts(conn_id, &[envelope.as_bytes(), body]);
}
