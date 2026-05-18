//! Ingress PUB-WITH-HEADERS frame — publish with metadata block.
//!
//! Headers are an opaque byte block (broker does not parse them). They get
//! persisted with the entry and delivered intact to consumers — useful for
//! tracing IDs, content-type, idempotency-key, source app, etc.
//!
//! Recommended internal format (NOT enforced by the broker):
//! ```text
//!   repeat:
//!     key_len   u8
//!     key       [key_len]
//!     value_len u16  LE
//!     value     [value_len]
//! ```
//!
//! Wire layout:
//! ```text
//! [Header 16B]                          ← action = Action::PublishWithHeaders
//! [PubWithHeadersBody 12B]
//!   offset 0:   stream_id    u32  (4B)
//!   offset 4:   subject_len  u16  (2B)
//!   offset 6:   headers_len  u16  (2B)  — total bytes of the headers block
//!   offset 8:   header_count u16  (2B)  — informational; 0 = unknown/unused
//!   offset 10:  _pad         u16  (2B)
//! [tail]
//!   [subject  subject_len bytes]
//!   [headers  headers_len bytes]
//!   [payload  payload_len bytes]   ← payload_len = msg_len - 12 - subject_len - headers_len
//! ```

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct PubWithHeadersBody {
    pub stream_id:    U32,
    pub subject_len:  U16,
    pub headers_len:  U16,
    pub header_count: U16,
    pub _pad:         U16,
}

pub const PUB_WITH_HEADERS_BODY_FIXED: usize = core::mem::size_of::<PubWithHeadersBody>();
const _: () = assert!(PUB_WITH_HEADERS_BODY_FIXED == 12);

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct PubWithHeadersFrame {
    pub header: Header,
    pub body:   PubWithHeadersBody,
    pub tail:   [u8],
}

impl PubWithHeadersFrame {
    #[inline(always)]
    pub fn subject(&self) -> &[u8] {
        let s = self.body.subject_len.get() as usize;
        &self.tail[..s]
    }

    #[inline(always)]
    pub fn headers(&self) -> &[u8] {
        let s = self.body.subject_len.get() as usize;
        let h = self.body.headers_len.get() as usize;
        &self.tail[s..s + h]
    }

    #[inline(always)]
    pub fn payload(&self) -> &[u8] {
        let s = self.body.subject_len.get() as usize;
        let h = self.body.headers_len.get() as usize;
        &self.tail[s + h..]
    }

    #[inline(always)]
    pub const fn wire_size(subject_len: usize, headers_len: usize, payload_len: usize) -> usize {
        HEADER_SIZE + PUB_WITH_HEADERS_BODY_FIXED + subject_len + headers_len + payload_len
    }

    #[allow(clippy::too_many_arguments)]
    pub fn encode_into<'a>(
        out: &'a mut [u8],
        seq: u64,
        stream_id: u32,
        flags: u8,
        entry_flags: u8,
        header_count: u16,
        subject: &[u8],
        headers: &[u8],
        payload: &[u8],
    ) -> &'a mut Self {
        debug_assert_eq!(
            out.len(),
            Self::wire_size(subject.len(), headers.len(), payload.len())
        );
        let msg_len =
            (PUB_WITH_HEADERS_BODY_FIXED + subject.len() + headers.len() + payload.len()) as u32;
        let frame = Self::mut_from_bytes(out).expect("PubWithHeadersFrame layout");
        frame.header =
            Header::new(crate::action::Action::PublishWithHeaders.as_u16(), msg_len, seq)
                .with_flags(flags)
                .with_entry_flags(entry_flags);
        frame.body = PubWithHeadersBody {
            stream_id:    U32::new(stream_id),
            subject_len:  U16::new(subject.len() as u16),
            headers_len:  U16::new(headers.len() as u16),
            header_count: U16::new(header_count),
            _pad:         U16::new(0),
        };
        let s = subject.len();
        let h = headers.len();
        frame.tail[..s].copy_from_slice(subject);
        frame.tail[s..s + h].copy_from_slice(headers);
        frame.tail[s + h..].copy_from_slice(payload);
        frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::Action;

    #[test]
    fn body_size_is_12() {
        assert_eq!(PUB_WITH_HEADERS_BODY_FIXED, 12);
    }

    #[test]
    fn encode_then_parse_roundtrip() {
        let subject = b"orders.eu.42";
        let headers = b"\x06trace_iX\x00abc123XYZ";
        let payload = vec![0xCD; 128];
        let size = PubWithHeadersFrame::wire_size(subject.len(), headers.len(), payload.len());
        let mut buf = vec![0u8; size];
        PubWithHeadersFrame::encode_into(&mut buf, 9, 3, 0, 0, 1, subject, headers, &payload);

        let f = PubWithHeadersFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(f.header.action.get(), Action::PublishWithHeaders.as_u16());
        assert_eq!(f.body.stream_id.get(), 3);
        assert_eq!(f.body.header_count.get(), 1);
        assert_eq!(f.subject(), subject);
        assert_eq!(f.headers(), headers);
        assert_eq!(f.payload(), payload.as_slice());
    }
}
