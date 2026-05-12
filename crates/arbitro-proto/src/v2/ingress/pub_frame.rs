//! Ingress PUB frame — single, no reply, optional msg_id for dedup.
//!
//! This is the **base** publish action (`Action::Publish`). For frames
//! that need a reply subject, use `Action::PublishWithReply` /
//! `PubWithReplyFrame`. For frames with headers, use
//! `Action::PublishWithHeaders` / `PubWithHeadersFrame`. There is no
//! discriminator byte inside the body — the action *is* the discriminator.
//!
//! Wire layout:
//! ```text
//! [Header 16B]                          ← action = Action::Publish
//! [PubBody fixed part 8B]
//!   offset 0:  stream_id    u32  (4B)  ← target stream (resolved by name registry)
//!   offset 4:  subject_len  u16  (2B)
//!   offset 6:  msg_id_len   u16  (2B)  ← 0 = no msg_id (legacy default)
//! [tail ... variable]
//!   [subject   subject_len bytes]
//!   [msg_id    msg_id_len  bytes]      ← when msg_id_len > 0; opaque to the broker
//!   [payload   payload_len bytes]      ← payload_len = msg_len - 8 - subject_len - msg_id_len
//! ```
//!
//! Total frame size = `16 + msg_len`. Per-message flags travel in
//! `header.entry_flags` (e.g. `RETAIN`, `COMPRESSED`) — no per-frame
//! `entry_flags + _pad` waste.
//!
//! **Wire compatibility**: the old format used the trailing 2 bytes of
//! the body as `_pad` (always zero). The new format reuses those bytes
//! as `msg_id_len`. Old clients sending `_pad = 0` still parse to
//! `msg_id_len = 0` = "no msg_id", which keeps the legacy semantics.
//! Old brokers receiving a new frame with `msg_id_len > 0` would
//! mis-interpret the msg_id bytes as part of the payload — that's a
//! breaking-on-upgrade behaviour we accept (both sides must rebuild).

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct PubBody {
    pub stream_id:   U32,
    pub subject_len: U16,
    /// Length of an opaque per-message id used for broker-side dedup
    /// when the target stream has idempotency enabled. `0` = no id
    /// (legacy publish, never deduped).
    pub msg_id_len:  U16,
}

pub const PUB_BODY_FIXED: usize = core::mem::size_of::<PubBody>();
const _: () = assert!(PUB_BODY_FIXED == 8);

/// DST view over an entire PUB frame (header + body + tail).
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct PubFrame {
    pub header: Header,
    pub body:   PubBody,
    pub tail:   [u8], // subject || msg_id || payload
}

impl PubFrame {
    #[inline(always)]
    pub fn subject(&self) -> &[u8] {
        let s = self.body.subject_len.get() as usize;
        &self.tail[..s]
    }

    /// Per-message id used for broker-side dedup. Empty slice when
    /// `msg_id_len == 0` (legacy publish, no dedup).
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
    pub fn payload_len(&self) -> usize {
        let msg = self.header.msg_len.get() as usize;
        let s = self.body.subject_len.get() as usize;
        let m = self.body.msg_id_len.get() as usize;
        msg - PUB_BODY_FIXED - s - m
    }

    /// Wire size for given subject + msg_id + payload.
    #[inline(always)]
    pub const fn wire_size(subject_len: usize, msg_id_len: usize, payload_len: usize) -> usize {
        HEADER_SIZE + PUB_BODY_FIXED + subject_len + msg_id_len + payload_len
    }

    /// Encode a fresh PUB frame directly into `out` (no intermediate buffer).
    ///
    /// `out.len()` must equal `wire_size(subject.len(), msg_id.len(), payload.len())`.
    /// `flags` are transport-level, `entry_flags` are per-message.
    /// Pass `msg_id = &[]` for the legacy / non-dedup case.
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
    ) -> &'a mut Self {
        debug_assert_eq!(
            out.len(),
            Self::wire_size(subject.len(), msg_id.len(), payload.len())
        );
        let msg_len =
            (PUB_BODY_FIXED + subject.len() + msg_id.len() + payload.len()) as u32;
        let frame = Self::mut_from_bytes(out).expect("PubFrame layout");
        frame.header = Header::new(crate::action::Action::Publish.as_u16(), msg_len, seq)
            .with_flags(flags)
            .with_entry_flags(entry_flags);
        frame.body = PubBody {
            stream_id:   U32::new(stream_id),
            subject_len: U16::new(subject.len() as u16),
            msg_id_len:  U16::new(msg_id.len() as u16),
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
    fn body_size_is_8() {
        assert_eq!(PUB_BODY_FIXED, 8);
    }

    #[test]
    fn encode_then_parse_roundtrip() {
        let subject = b"orders.eu.premium.42";
        let payload = vec![0xAB; 1024];
        let size = PubFrame::wire_size(subject.len(), 0, payload.len());
        let mut buf = vec![0u8; size];

        PubFrame::encode_into(&mut buf, 777, 0xCAFEBABE, 0, 0, subject, &[], &payload);

        let frame = PubFrame::ref_from_bytes(&buf).expect("parse");
        assert_eq!(frame.header.seq.get(), 777);
        assert_eq!(frame.header.total_len(), size);
        assert_eq!(frame.body.stream_id.get(), 0xCAFEBABE);
        assert_eq!(frame.subject(), subject);
        assert_eq!(frame.msg_id(), &[] as &[u8]);
        assert_eq!(frame.payload(), payload.as_slice());
        assert_eq!(frame.payload_len(), payload.len());
    }

    #[test]
    fn msg_id_roundtrips_and_isolates_subject_payload() {
        let subject = b"orders.new";
        let msg_id  = b"order-id-12345";
        let payload = b"some opaque payload";
        let size = PubFrame::wire_size(subject.len(), msg_id.len(), payload.len());
        let mut buf = vec![0u8; size];

        PubFrame::encode_into(&mut buf, 5, 7, 0, 0, subject, msg_id, payload);

        let f = PubFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(f.body.msg_id_len.get() as usize, msg_id.len());
        assert_eq!(f.subject(), subject, "subject must NOT bleed into msg_id");
        assert_eq!(f.msg_id(), msg_id);
        assert_eq!(f.payload(), payload, "payload must come AFTER msg_id");
        assert_eq!(f.payload_len(), payload.len());
    }

    #[test]
    fn zero_subject() {
        let payload = [0xFFu8; 32];
        let size = PubFrame::wire_size(0, 0, payload.len());
        let mut buf = vec![0u8; size];
        PubFrame::encode_into(&mut buf, 1, 0, 0, 0, &[], &[], &payload);
        let f = PubFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(f.subject(), &[] as &[u8]);
        assert_eq!(f.payload(), &payload);
    }

    #[test]
    fn header_byte_layout_is_first_16() {
        let payload = [1u8, 2, 3, 4];
        let size = PubFrame::wire_size(0, 0, payload.len());
        let mut buf = vec![0u8; size];
        PubFrame::encode_into(&mut buf, 9, 0, 0, 0, &[], &[], &payload);
        let h = Header::ref_from_bytes(&buf[..HEADER_SIZE]).unwrap();
        assert_eq!(h.seq.get(), 9);
        assert_eq!(h.msg_len.get() as usize, PUB_BODY_FIXED + payload.len());
    }

    #[test]
    fn entry_flags_in_header() {
        use crate::v2::header::entry_flag;
        let payload = [0u8; 4];
        let size = PubFrame::wire_size(0, 0, payload.len());
        let mut buf = vec![0u8; size];
        PubFrame::encode_into(
            &mut buf, 1, 0, 0,
            entry_flag::RETAIN | entry_flag::COMPRESSED,
            &[], &[], &payload,
        );
        let f = PubFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(f.header.entry_flags, 0b0000_0011);
    }
}
