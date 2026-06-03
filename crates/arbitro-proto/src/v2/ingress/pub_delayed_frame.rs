//! Ingress PUB-DELAYED frame — single publish with a delivery delay.
//!
//! Wire layout:
//! ```text
//! [Header 16B]                          <- action = Action::PublishDelayed
//! [PubDelayedBody fixed part 16B]
//!   offset 0:  stream_id    u32  (4B)
//!   offset 4:  subject_len  u16  (2B)
//!   offset 6:  msg_id_len   u16  (2B)
//!   offset 8:  delay_ms     u64  (8B)  <- milliseconds to delay delivery
//! [tail ... variable]
//!   [subject   subject_len bytes]
//!   [msg_id    msg_id_len  bytes]
//!   [payload   payload_len bytes]      <- payload_len = msg_len - 16 - subject_len - msg_id_len
//! ```
//!
//! `delay_ms = 0` is technically valid but degenerates to a normal publish.
//! The server computes `deliver_at_ms = now_ms + delay_ms` and parks the
//! entry in the delayed journal.

use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct PubDelayedBody {
    pub stream_id:   U32,
    pub subject_len: U16,
    pub msg_id_len:  U16,
    pub delay_ms:    U64,
}

pub const PUB_DELAYED_BODY_FIXED: usize = core::mem::size_of::<PubDelayedBody>();
const _: () = assert!(PUB_DELAYED_BODY_FIXED == 16);

/// DST view over an entire PUB-DELAYED frame (header + body + tail).
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct PubDelayedFrame {
    pub header: Header,
    pub body:   PubDelayedBody,
    pub tail:   [u8], // subject || msg_id || payload
}

impl PubDelayedFrame {
    /// Validate body lengths against the tail.
    #[inline]
    pub fn validate(&self) -> Result<(), crate::error::ErrorCode> {
        let s = self.body.subject_len.get() as usize;
        let m = self.body.msg_id_len.get() as usize;
        let head_total = s.checked_add(m).ok_or(crate::error::ErrorCode::InvalidLength)?;
        if head_total > self.tail.len() {
            return Err(crate::error::ErrorCode::InvalidLength);
        }
        let msg = self.header.msg_len.get() as usize;
        let lower = PUB_DELAYED_BODY_FIXED.checked_add(head_total)
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
    pub fn msg_id(&self) -> &[u8] {
        let s = self.body.subject_len.get() as usize;
        let m = self.body.msg_id_len.get() as usize;
        &self.tail[s..s + m]
    }

    #[inline(always)]
    pub fn payload(&self) -> &[u8] {
        let s = self.body.subject_len.get() as usize;
        let m = self.body.msg_id_len.get() as usize;
        &self.tail[s + m..]
    }

    #[inline(always)]
    pub fn delay_ms(&self) -> u64 {
        self.body.delay_ms.get()
    }

    /// Wire size for given subject + msg_id + payload.
    #[inline(always)]
    pub const fn wire_size(subject_len: usize, msg_id_len: usize, payload_len: usize) -> usize {
        HEADER_SIZE + PUB_DELAYED_BODY_FIXED + subject_len + msg_id_len + payload_len
    }

    /// Encode a fresh PUB-DELAYED frame directly into `out`.
    #[allow(clippy::too_many_arguments)]
    pub fn encode_into<'a>(
        out: &'a mut [u8],
        seq: u64,
        stream_id: u32,
        flags: u8,
        entry_flags: u8,
        subject: &[u8],
        msg_id: &[u8],
        payload: &[u8],
        delay_ms: u64,
    ) -> &'a mut Self {
        debug_assert_eq!(
            out.len(),
            Self::wire_size(subject.len(), msg_id.len(), payload.len())
        );
        let msg_len =
            (PUB_DELAYED_BODY_FIXED + subject.len() + msg_id.len() + payload.len()) as u32;
        let frame = Self::mut_from_bytes(out).expect("PubDelayedFrame layout");
        frame.header = Header::new(crate::action::Action::PublishDelayed.as_u16(), msg_len, seq)
            .with_flags(flags)
            .with_entry_flags(entry_flags);
        frame.body = PubDelayedBody {
            stream_id:   U32::new(stream_id),
            subject_len: U16::new(subject.len() as u16),
            msg_id_len:  U16::new(msg_id.len() as u16),
            delay_ms:    U64::new(delay_ms),
        };
        let s = subject.len();
        let m = msg_id.len();
        frame.tail[..s].copy_from_slice(subject);
        frame.tail[s..s + m].copy_from_slice(msg_id);
        frame.tail[s + m..].copy_from_slice(payload);
        frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_size_is_16() {
        assert_eq!(PUB_DELAYED_BODY_FIXED, 16);
    }

    #[test]
    fn encode_then_parse_roundtrip() {
        let subject = b"orders.delayed";
        let payload = vec![0xAB; 64];
        let delay_ms = 5000u64;
        let size = PubDelayedFrame::wire_size(subject.len(), 0, payload.len());
        let mut buf = vec![0u8; size];

        PubDelayedFrame::encode_into(
            &mut buf, 42, 0xBEEF, 0, 0, subject, &[], &payload, delay_ms,
        );

        let frame = PubDelayedFrame::ref_from_bytes(&buf).expect("parse");
        assert_eq!(frame.header.seq.get(), 42);
        assert_eq!(frame.body.stream_id.get(), 0xBEEF);
        assert_eq!(frame.subject(), subject);
        assert_eq!(frame.msg_id(), &[] as &[u8]);
        assert_eq!(frame.payload(), payload.as_slice());
        assert_eq!(frame.delay_ms(), delay_ms);
    }

    #[test]
    fn validate_rejects_oversized_subject() {
        let payload = [0u8; 4];
        let size = PubDelayedFrame::wire_size(0, 0, payload.len());
        let mut buf = vec![0u8; size];
        PubDelayedFrame::encode_into(
            &mut buf, 1, 0, 0, 0, &[], &[], &payload, 1000,
        );
        // Forge subject_len out of bounds.
        let off = HEADER_SIZE + 4;
        buf[off] = 0xFF;
        buf[off + 1] = 0xFF;
        let f = PubDelayedFrame::ref_from_bytes(&buf).unwrap();
        assert!(f.validate().is_err());
    }
}
