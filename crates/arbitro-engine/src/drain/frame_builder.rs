//! Frame builder — construct reply and delivery frames on the stack.
//!
//! Zero heap allocation. All frames are built into caller-provided
//! or stack buffers, then sent via transport.

use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_proto::wire::delivery::{RepOkAction, RepErrorAction};

/// Build a RepOk frame (Envelope 16B + RepOkAction 16B = 32B) on the stack.
#[inline]
pub fn build_rep_ok(stream_id: u32, env_seq: u32, ref_seq: u64) -> [u8; 32] {
    let mut buf = [0u8; 32];

    let envelope = Envelope {
        action: U16::new(Action::RepOk.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(16), // RepOkAction is 16 bytes
        env_seq: U32::new(env_seq),
    };
    buf[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());

    let body = RepOkAction {
        ref_seq: U64::new(ref_seq),
        _pad: U64::new(0),
    };
    buf[ENVELOPE_SIZE..].copy_from_slice(body.as_bytes());

    buf
}

/// Build a RepError frame (Envelope 16B + RepErrorAction 16B = 32B) on the stack.
#[inline]
pub fn build_rep_error(stream_id: u32, env_seq: u32, ref_seq: u64, code: ErrorCode) -> [u8; 32] {
    let mut buf = [0u8; 32];

    let envelope = Envelope {
        action: U16::new(Action::RepError.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(16),
        env_seq: U32::new(env_seq),
    };
    buf[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());

    let body = RepErrorAction {
        ref_seq: U64::new(ref_seq),
        error_code: U16::new(code.as_u16()),
        _pad: [0u8; 6],
    };
    buf[ENVELOPE_SIZE..].copy_from_slice(body.as_bytes());

    buf
}

/// Build a delivery envelope (16B) for forwarding a stored entry to a consumer.
/// The caller appends the entry data after the envelope.
#[inline]
pub fn build_delivery_envelope(stream_id: u32, msg_len: u32, seq: u32) -> [u8; ENVELOPE_SIZE] {
    let envelope = Envelope {
        action: U16::new(Action::Deliver.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(msg_len),
        env_seq: U32::new(seq),
    };
    let mut buf = [0u8; ENVELOPE_SIZE];
    buf.copy_from_slice(envelope.as_bytes());
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use arbitro_proto::wire::envelope::FrameView;
    use arbitro_proto::wire::delivery::{RepOkView, RepErrorView};

    #[test]
    fn rep_ok_roundtrip() {
        let frame = build_rep_ok(42, 7, 100);
        let view = FrameView::new(&frame);

        assert_eq!(view.action(), Some(Action::RepOk));
        assert_eq!(view.stream_id(), 42);
        assert_eq!(view.msg_len(), 16);

        let ok = RepOkView::new(view.body());
        assert_eq!(ok.ref_seq(), 100);
    }

    #[test]
    fn rep_error_roundtrip() {
        let frame = build_rep_error(42, 7, 100, ErrorCode::StreamFull);
        let view = FrameView::new(&frame);

        assert_eq!(view.action(), Some(Action::RepError));

        let err = RepErrorView::new(view.body());
        assert_eq!(err.ref_seq(), 100);
        assert_eq!(err.error_code(), ErrorCode::StreamFull.as_u16());
    }

    #[test]
    fn delivery_envelope_fields() {
        let env = build_delivery_envelope(99, 256, 5);
        let view = FrameView::new(&env);

        assert_eq!(view.action(), Some(Action::Deliver));
        assert_eq!(view.stream_id(), 99);
        assert_eq!(view.msg_len(), 256);
    }
}
