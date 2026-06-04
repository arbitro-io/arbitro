//! Ingress PUB-WITH-REPLY frame — request/reply RPC over the broker.
//!
//! Used when the publisher wants the consumer to send back a response on a
//! reply subject (`_INBOX.<token>` style). The reply subject is part of
//! the persisted record so consumers see it on delivery.
//!
//! Wire layout:
//! ```text
//! [Header 16B]                          ← action = Action::PublishWithReply
//! [PubWithReplyBody 12B]
//!   offset 0:   stream_id    u32  (4B)
//!   offset 4:   subject_len  u16  (2B)
//!   offset 6:   reply_len    u16  (2B)
//!   offset 8:   msg_id_len   u16  (2B)  — M10: optional dedup token
//!   offset 10:  _pad         u16  (2B)  — reserved
//! [tail]
//!   [subject  subject_len bytes]
//!   [reply    reply_len    bytes]
//!   [msg_id   msg_id_len   bytes]   ← M10: empty = no dedup
//!   [payload  payload_len  bytes]   ← payload_len = msg_len - 12 - subject_len - reply_len - msg_id_len
//! ```

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct PubWithReplyBody {
    pub stream_id: U32,
    pub subject_len: U16,
    pub reply_len: U16,
    /// M10: optional idempotency token length. `0` = no dedup (legacy).
    pub msg_id_len: U16,
    pub _pad: U16,
}

pub const PUB_WITH_REPLY_BODY_FIXED: usize = core::mem::size_of::<PubWithReplyBody>();
const _: () = assert!(PUB_WITH_REPLY_BODY_FIXED == 12);

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct PubWithReplyFrame {
    pub header: Header,
    pub body: PubWithReplyBody,
    pub tail: [u8],
}

impl PubWithReplyFrame {
    /// **B4 safety**: `subject_len + reply_len + msg_id_len <= tail.len()`
    /// so the per-field slicing in `subject() / reply_to() / msg_id() /
    /// payload()` cannot underflow on a malicious header.
    #[inline]
    pub fn validate(&self) -> Result<(), crate::error::ErrorCode> {
        let s = self.body.subject_len.get() as usize;
        let r = self.body.reply_len.get() as usize;
        let m = self.body.msg_id_len.get() as usize;
        let head_total = s
            .checked_add(r)
            .ok_or(crate::error::ErrorCode::InvalidLength)?
            .checked_add(m)
            .ok_or(crate::error::ErrorCode::InvalidLength)?;
        if head_total > self.tail.len() {
            return Err(crate::error::ErrorCode::InvalidLength);
        }
        let msg = self.header.msg_len.get() as usize;
        let lower = PUB_WITH_REPLY_BODY_FIXED
            .checked_add(head_total)
            .ok_or(crate::error::ErrorCode::InvalidLength)?;
        if msg < lower {
            return Err(crate::error::ErrorCode::InvalidLength);
        }
        Ok(())
    }

    #[inline(always)]
    pub fn subject(&self) -> &[u8] {
        let s = self.body.subject_len.get() as usize;
        &self.tail[..s]
    }

    #[inline(always)]
    pub fn reply_to(&self) -> &[u8] {
        let s = self.body.subject_len.get() as usize;
        let r = self.body.reply_len.get() as usize;
        &self.tail[s..s + r]
    }

    /// M10: idempotency token (may be empty when `msg_id_len == 0`).
    #[inline(always)]
    pub fn msg_id(&self) -> &[u8] {
        let s = self.body.subject_len.get() as usize;
        let r = self.body.reply_len.get() as usize;
        let m = self.body.msg_id_len.get() as usize;
        &self.tail[s + r..s + r + m]
    }

    #[inline(always)]
    pub fn payload(&self) -> &[u8] {
        let s = self.body.subject_len.get() as usize;
        let r = self.body.reply_len.get() as usize;
        let m = self.body.msg_id_len.get() as usize;
        &self.tail[s + r + m..]
    }

    #[inline(always)]
    pub const fn wire_size(
        subject_len: usize,
        reply_len: usize,
        msg_id_len: usize,
        payload_len: usize,
    ) -> usize {
        HEADER_SIZE + PUB_WITH_REPLY_BODY_FIXED + subject_len + reply_len + msg_id_len + payload_len
    }

    #[allow(clippy::too_many_arguments)]
    pub fn encode_into<'a>(
        out: &'a mut [u8],
        seq: u64,
        stream_id: u32,
        flags: u8,
        entry_flags: u8,
        subject: &[u8],
        reply_to: &[u8],
        msg_id: &[u8],
        payload: &[u8],
    ) -> &'a mut Self {
        debug_assert_eq!(
            out.len(),
            Self::wire_size(subject.len(), reply_to.len(), msg_id.len(), payload.len())
        );
        let msg_len = (PUB_WITH_REPLY_BODY_FIXED
            + subject.len()
            + reply_to.len()
            + msg_id.len()
            + payload.len()) as u32;
        let frame = Self::mut_from_bytes(out).expect("PubWithReplyFrame layout");
        frame.header = Header::new(
            crate::action::Action::PublishWithReply.as_u16(),
            msg_len,
            seq,
        )
        .with_flags(flags)
        .with_entry_flags(entry_flags);
        frame.body = PubWithReplyBody {
            stream_id: U32::new(stream_id),
            subject_len: U16::new(subject.len() as u16),
            reply_len: U16::new(reply_to.len() as u16),
            msg_id_len: U16::new(msg_id.len() as u16),
            _pad: U16::new(0),
        };
        let s = subject.len();
        let r = reply_to.len();
        let m = msg_id.len();
        frame.tail[..s].copy_from_slice(subject);
        frame.tail[s..s + r].copy_from_slice(reply_to);
        frame.tail[s + r..s + r + m].copy_from_slice(msg_id);
        frame.tail[s + r + m..].copy_from_slice(payload);
        frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::Action;

    #[test]
    fn body_size_is_12() {
        assert_eq!(PUB_WITH_REPLY_BODY_FIXED, 12);
    }

    #[test]
    fn encode_then_parse_roundtrip() {
        let subject = b"orders.eu.42";
        let reply = b"_INBOX.abc123";
        let payload = vec![0xAB; 256];
        let size = PubWithReplyFrame::wire_size(subject.len(), reply.len(), 0, payload.len());
        let mut buf = vec![0u8; size];
        PubWithReplyFrame::encode_into(&mut buf, 777, 7, 0, 0, subject, reply, b"", &payload);

        let f = PubWithReplyFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(f.header.action.get(), Action::PublishWithReply.as_u16());
        assert_eq!(f.body.stream_id.get(), 7);
        assert_eq!(f.subject(), subject);
        assert_eq!(f.reply_to(), reply);
        assert_eq!(f.msg_id(), b"");
        assert_eq!(f.payload(), payload.as_slice());
    }

    #[test]
    fn m10_roundtrip_with_msg_id() {
        let subject = b"rpc.req";
        let reply = b"_INBOX.token";
        let msg_id = b"client-uuid-42";
        let payload = b"hello".to_vec();
        let size =
            PubWithReplyFrame::wire_size(subject.len(), reply.len(), msg_id.len(), payload.len());
        let mut buf = vec![0u8; size];
        PubWithReplyFrame::encode_into(&mut buf, 1, 1, 0, 0, subject, reply, msg_id, &payload);
        let f = PubWithReplyFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(f.subject(), subject);
        assert_eq!(f.reply_to(), reply);
        assert_eq!(f.msg_id(), msg_id);
        assert_eq!(f.payload(), payload.as_slice());
        assert!(f.validate().is_ok());
    }
}
