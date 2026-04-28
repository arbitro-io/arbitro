//! Egress DELIVER frame + batched form.
//!
//! Single DELIVER:
//! ```text
//! [Header 16B]                     ← action = Action::Deliver
//! [DeliverBody 12B]
//!   offset 0:  consumer_id   u32  (4B)
//!   offset 4:  subject_hash  u32  (4B)  ← echoed back in client ack
//!   offset 8:  subject_len   u16  (2B)
//!   offset 10: _pad          u16  (2B)
//! [tail]
//!   [subject  subject_len bytes]
//!   [payload  ...]
//! ```
//!
//! Batched DELIVER (writev-friendly):
//! ```text
//! [Header 16B]                     ← action = Action::RepBatch
//! [DeliverBatchHeader 8B]
//!   consumer_id u32  (4B)
//!   count       u32  (4B)
//! [entries...]                     ← back-to-back DeliverBatchEntry + subject+payload
//!   [DeliverBatchEntry 16B]
//!     deliver_seq  u64  (8B)
//!     subject_hash u32  (4B)
//!     subject_len  u16  (2B)
//!     payload_len  u16  (2B)       ← payload_len fits u16 (max 65 KiB per entry)
//!   [subject  subject_len bytes]
//!   [payload  payload_len bytes]
//! ```
//!
//! Iteration walks each entry with known sizes — no hidden length fields.

use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

// ── Single DELIVER ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DeliverBody {
    pub consumer_id:  U32,
    pub subject_hash: U32,
    pub subject_len:  U16,
    pub _pad:         U16,
}
pub const DELIVER_BODY_FIXED: usize = core::mem::size_of::<DeliverBody>();
const _: () = assert!(DELIVER_BODY_FIXED == 12);

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DeliverFrame {
    pub header: Header,
    pub body:   DeliverBody,
    pub tail:   [u8],
}

impl DeliverFrame {
    #[inline(always)]
    pub const fn wire_size(subject_len: usize, payload_len: usize) -> usize {
        HEADER_SIZE + DELIVER_BODY_FIXED + subject_len + payload_len
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

    /// Build only the header + body on stack (16 + 12 = 28 bytes), allowing
    /// zero-copy egress: emit via `writev([header_body, subject+payload])`
    /// where `subject+payload` points directly into the persisted record.
    #[inline(always)]
    pub fn build_prefix(
        deliver_seq: u64,
        consumer_id: u32,
        subject_hash: u32,
        subject_len: u16,
        payload_len: usize,
    ) -> DeliverPrefix {
        let msg_len = (DELIVER_BODY_FIXED + subject_len as usize + payload_len) as u32;
        DeliverPrefix {
            header: Header::new(
                crate::action::Action::Deliver.as_u16(),
                msg_len,
                deliver_seq,
            ),
            body: DeliverBody {
                consumer_id:  U32::new(consumer_id),
                subject_hash: U32::new(subject_hash),
                subject_len:  U16::new(subject_len),
                _pad:         U16::new(0),
            },
        }
    }

    pub fn encode_into<'a>(
        out: &'a mut [u8],
        deliver_seq: u64,
        consumer_id: u32,
        subject_hash: u32,
        subject: &[u8],
        payload: &[u8],
    ) -> &'a mut Self {
        debug_assert_eq!(out.len(), Self::wire_size(subject.len(), payload.len()));
        let prefix = Self::build_prefix(
            deliver_seq,
            consumer_id,
            subject_hash,
            subject.len() as u16,
            payload.len(),
        );
        let frame = Self::mut_from_bytes(out).expect("DeliverFrame layout");
        frame.header = prefix.header;
        frame.body = prefix.body;
        frame.tail[..subject.len()].copy_from_slice(subject);
        frame.tail[subject.len()..].copy_from_slice(payload);
        frame
    }
}

/// Stack-allocatable, sized prefix (28 B) used for zero-copy `writev` emission.
///
/// The caller pairs this with an external `&[u8]` region that already contains
/// `subject || payload` (e.g. a slice into a `Record::tail`).
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DeliverPrefix {
    pub header: Header,
    pub body:   DeliverBody,
}
const _: () = assert!(core::mem::size_of::<DeliverPrefix>() == HEADER_SIZE + DELIVER_BODY_FIXED);

// ── Batched DELIVER ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DeliverBatchHeader {
    pub consumer_id: U32,
    pub count:       U32,
}
pub const DELIVER_BATCH_HEADER_FIXED: usize = core::mem::size_of::<DeliverBatchHeader>();
const _: () = assert!(DELIVER_BATCH_HEADER_FIXED == 8);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DeliverBatchEntry {
    pub deliver_seq:  U64,
    pub subject_hash: U32,
    pub subject_len:  U16,
    pub payload_len:  U16,
}
pub const DELIVER_BATCH_ENTRY_FIXED: usize = core::mem::size_of::<DeliverBatchEntry>();
const _: () = assert!(DELIVER_BATCH_ENTRY_FIXED == 16);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deliver_body_size_is_12() {
        assert_eq!(DELIVER_BODY_FIXED, 12);
    }

    #[test]
    fn deliver_prefix_size_is_28() {
        assert_eq!(core::mem::size_of::<DeliverPrefix>(), 28);
    }

    #[test]
    fn deliver_roundtrip() {
        let subject = b"users.created";
        let payload = [0x55u8; 64];
        let size = DeliverFrame::wire_size(subject.len(), payload.len());
        let mut buf = vec![0u8; size];
        DeliverFrame::encode_into(&mut buf, 123, 77, 0xDEAD, subject, &payload);
        let f = DeliverFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(f.header.seq.get(), 123);
        assert_eq!(f.body.consumer_id.get(), 77);
        assert_eq!(f.body.subject_hash.get(), 0xDEAD);
        assert_eq!(f.subject(), subject);
        assert_eq!(f.payload(), &payload);
    }

    #[test]
    fn batch_entry_size_is_16() {
        assert_eq!(DELIVER_BATCH_ENTRY_FIXED, 16);
    }
}
