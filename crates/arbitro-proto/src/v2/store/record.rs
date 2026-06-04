//! Persisted record — what lives in the batch body.
//!
//! ```text
//! [Header 16B]                    ← action = Store (domain discriminator)
//! [RecordBody 12B]
//!   offset 0:  stream_id     u32  (4B)
//!   offset 4:  subject_hash  u32  (4B)  ← foldhash u32, precomputed
//!   offset 8:  subject_len   u16  (2B)
//!   offset 10: record_flags  u16  (2B)  ← tombstone, compressed, ...
//! [tail]
//!   [subject  subject_len bytes]
//!   [payload  ...]                ← payload_len = msg_len - 12 - subject_len
//! ```
//!
//! **Byte-identical body layout is intentional**: the subject + payload
//! section is copied unchanged from an ingress `PubFrame` — only the
//! leading header + body fields are rewritten at promotion time.

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct RecordBody {
    pub stream_id: U32,
    pub subject_hash: U32,
    pub subject_len: U16,
    pub record_flags: U16,
}
pub const RECORD_BODY_FIXED: usize = core::mem::size_of::<RecordBody>();
const _: () = assert!(RECORD_BODY_FIXED == 12);

/// Record flag bits.
pub mod flag {
    pub const TOMBSTONE: u16 = 1 << 0;
    pub const COMPRESSED: u16 = 1 << 1;
    // bits 2..=15 reserved
}

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct Record {
    pub header: Header,
    pub body: RecordBody,
    pub tail: [u8],
}

impl Record {
    #[inline(always)]
    pub const fn wire_size(subject_len: usize, payload_len: usize) -> usize {
        HEADER_SIZE + RECORD_BODY_FIXED + subject_len + payload_len
    }

    #[inline(always)]
    pub fn subject(&self) -> &[u8] {
        let n = self.body.subject_len.get() as usize;
        &self.tail[..n]
    }

    #[inline(always)]
    pub fn payload(&self) -> &[u8] {
        let n = self.body.subject_len.get() as usize;
        &self.tail[n..]
    }

    #[inline(always)]
    pub fn payload_len(&self) -> usize {
        let msg = self.header.msg_len.get() as usize;
        let s = self.body.subject_len.get() as usize;
        msg - RECORD_BODY_FIXED - s
    }

    pub fn encode_into<'a>(
        out: &'a mut [u8],
        seq: u64,
        stream_id: u32,
        subject_hash: u32,
        record_flags: u16,
        subject: &[u8],
        payload: &[u8],
    ) -> &'a mut Self {
        debug_assert_eq!(out.len(), Self::wire_size(subject.len(), payload.len()));
        let msg_len = (RECORD_BODY_FIXED + subject.len() + payload.len()) as u32;
        let frame = Self::mut_from_bytes(out).expect("Record layout");
        // We co-opt Action::Publish as "Store" for now; a dedicated action
        // code can be added later without changing the wire layout.
        frame.header = Header::new(crate::action::Action::Publish.as_u16(), msg_len, seq);
        frame.body = RecordBody {
            stream_id: U32::new(stream_id),
            subject_hash: U32::new(subject_hash),
            subject_len: U16::new(subject.len() as u16),
            record_flags: U16::new(record_flags),
        };
        frame.tail[..subject.len()].copy_from_slice(subject);
        frame.tail[subject.len()..].copy_from_slice(payload);
        frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_size_is_12() {
        assert_eq!(RECORD_BODY_FIXED, 12);
    }

    #[test]
    fn record_roundtrip() {
        let subject = b"orders.eu.premium";
        let payload = vec![0x7F; 256];
        let size = Record::wire_size(subject.len(), payload.len());
        let mut buf = vec![0u8; size];
        Record::encode_into(&mut buf, 42, 1, 0xABCD1234, 0, subject, &payload);
        let r = Record::ref_from_bytes(&buf).unwrap();
        assert_eq!(r.header.seq.get(), 42);
        assert_eq!(r.body.stream_id.get(), 1);
        assert_eq!(r.body.subject_hash.get(), 0xABCD1234);
        assert_eq!(r.subject(), subject);
        assert_eq!(r.payload(), payload.as_slice());
        assert_eq!(r.payload_len(), payload.len());
    }

    #[test]
    fn tombstone_flag() {
        let subject = b"x";
        let size = Record::wire_size(subject.len(), 0);
        let mut buf = vec![0u8; size];
        Record::encode_into(&mut buf, 1, 1, 0, flag::TOMBSTONE, subject, &[]);
        let r = Record::ref_from_bytes(&buf).unwrap();
        assert_eq!(r.body.record_flags.get() & flag::TOMBSTONE, flag::TOMBSTONE);
        assert_eq!(r.payload(), &[] as &[u8]);
    }
}
