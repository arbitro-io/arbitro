//! Stream management frames (v2): Create / Delete / Get / Purge /
//! DrainSubject / ListStreams.
//!
//! All requests share the v2 `Header(16B)` prefix. Bodies are
//! `#[repr(C)]` zerocopy structs, decoded via a single `ref_from_bytes`
//! call over `&body[..]`. Variable-length name/subject lives in the
//! frame's `tail: [u8]` (a DST trailer when present).

use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::action::Action;
use crate::v2::header::{Header, HEADER_SIZE};

// ── CreateStream ───────────────────────────────────────────────────────
//
// Body layout (32 B fixed + name + filter):
//   name_len      u16
//   filter_len    u16
//   max_msgs      u64
//   max_bytes     u64
//   max_age_secs  u64
//   replicas      u8
//   journal_kind  u8
//   retention     u8
//   discard       u8
// Tail: [name (name_len)][filter (filter_len)]

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct CreateStreamBody {
    pub name_len:     U16,
    pub filter_len:   U16,
    pub max_msgs:     U64,
    pub max_bytes:    U64,
    pub max_age_secs: U64,
    pub replicas:     u8,
    pub journal_kind: u8,
    pub retention:    u8,
    pub discard:      u8,
}
pub const CREATE_STREAM_BODY_FIXED: usize = core::mem::size_of::<CreateStreamBody>();
const _: () = assert!(CREATE_STREAM_BODY_FIXED == 32);

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct CreateStreamFrame {
    pub header: Header,
    pub body:   CreateStreamBody,
    pub tail:   [u8],
}

impl CreateStreamFrame {
    #[inline(always)]
    pub const fn wire_size(name_len: usize, filter_len: usize) -> usize {
        HEADER_SIZE + CREATE_STREAM_BODY_FIXED + name_len + filter_len
    }

    #[inline(always)]
    pub fn name(&self) -> &[u8] {
        let n = self.body.name_len.get() as usize;
        &self.tail[..n]
    }

    #[inline(always)]
    pub fn filter(&self) -> &[u8] {
        let n = self.body.name_len.get() as usize;
        let f = self.body.filter_len.get() as usize;
        &self.tail[n..n + f]
    }

    pub fn encode_into<'a>(
        out: &'a mut [u8],
        seq: u64,
        name: &[u8],
        filter: &[u8],
        max_msgs: u64,
        max_bytes: u64,
        max_age_secs: u64,
        replicas: u8,
        journal_kind: u8,
        retention: u8,
        discard: u8,
    ) -> &'a mut Self {
        debug_assert_eq!(out.len(), Self::wire_size(name.len(), filter.len()));
        let msg_len = (CREATE_STREAM_BODY_FIXED + name.len() + filter.len()) as u32;
        let frame = Self::mut_from_bytes(out).expect("CreateStreamFrame layout");
        frame.header = Header::new(Action::CreateStream.as_u16(), msg_len, seq);
        frame.body = CreateStreamBody {
            name_len:     U16::new(name.len() as u16),
            filter_len:   U16::new(filter.len() as u16),
            max_msgs:     U64::new(max_msgs),
            max_bytes:    U64::new(max_bytes),
            max_age_secs: U64::new(max_age_secs),
            replicas,
            journal_kind,
            retention,
            discard,
        };
        let n = name.len();
        frame.tail[..n].copy_from_slice(name);
        frame.tail[n..n + filter.len()].copy_from_slice(filter);
        frame
    }
}

// ── DeleteStream / GetStream / PurgeStream — same shape (8B + name) ────

macro_rules! simple_named_frame {
    ($body:ident, $frame:ident, $size_const:ident, $action:expr) => {
        #[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
        #[repr(C)]
        pub struct $body {
            pub name_len: U16,
            pub _pad:     [u8; 6],
        }
        pub const $size_const: usize = core::mem::size_of::<$body>();
        const _: () = assert!($size_const == 8);

        #[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
        #[repr(C)]
        pub struct $frame {
            pub header: Header,
            pub body:   $body,
            pub tail:   [u8],
        }

        impl $frame {
            #[inline(always)]
            pub const fn wire_size(name_len: usize) -> usize {
                HEADER_SIZE + $size_const + name_len
            }

            #[inline(always)]
            pub fn name(&self) -> &[u8] {
                let n = self.body.name_len.get() as usize;
                &self.tail[..n]
            }

            pub fn encode_into<'a>(out: &'a mut [u8], seq: u64, name: &[u8]) -> &'a mut Self {
                debug_assert_eq!(out.len(), Self::wire_size(name.len()));
                let msg_len = ($size_const + name.len()) as u32;
                let frame = Self::mut_from_bytes(out).expect("layout");
                frame.header = Header::new($action.as_u16(), msg_len, seq);
                frame.body = $body {
                    name_len: U16::new(name.len() as u16),
                    _pad:     [0u8; 6],
                };
                frame.tail[..name.len()].copy_from_slice(name);
                frame
            }
        }
    };
}

simple_named_frame!(DeleteStreamBody, DeleteStreamFrame, DELETE_STREAM_BODY_FIXED, Action::DeleteStream);
simple_named_frame!(GetStreamBody, GetStreamFrame, GET_STREAM_BODY_FIXED, Action::GetStream);
simple_named_frame!(PurgeStreamBody, PurgeStreamFrame, PURGE_STREAM_BODY_FIXED, Action::PurgeStream);

// ── DrainSubject — 8B body (name_len + subj_len) + name + subject ──────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DrainSubjectBody {
    pub name_len: U16,
    pub subj_len: U16,
    pub _pad:     [u8; 4],
}
pub const DRAIN_SUBJECT_BODY_FIXED: usize = core::mem::size_of::<DrainSubjectBody>();
const _: () = assert!(DRAIN_SUBJECT_BODY_FIXED == 8);

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DrainSubjectFrame {
    pub header: Header,
    pub body:   DrainSubjectBody,
    pub tail:   [u8],
}

impl DrainSubjectFrame {
    #[inline(always)]
    pub const fn wire_size(name_len: usize, subj_len: usize) -> usize {
        HEADER_SIZE + DRAIN_SUBJECT_BODY_FIXED + name_len + subj_len
    }

    #[inline(always)]
    pub fn name(&self) -> &[u8] {
        let n = self.body.name_len.get() as usize;
        &self.tail[..n]
    }

    #[inline(always)]
    pub fn subject(&self) -> &[u8] {
        let n = self.body.name_len.get() as usize;
        let s = self.body.subj_len.get() as usize;
        &self.tail[n..n + s]
    }

    pub fn encode_into<'a>(out: &'a mut [u8], seq: u64, name: &[u8], subject: &[u8]) -> &'a mut Self {
        debug_assert_eq!(out.len(), Self::wire_size(name.len(), subject.len()));
        let msg_len = (DRAIN_SUBJECT_BODY_FIXED + name.len() + subject.len()) as u32;
        let frame = Self::mut_from_bytes(out).expect("DrainSubjectFrame layout");
        frame.header = Header::new(Action::DrainSubject.as_u16(), msg_len, seq);
        frame.body = DrainSubjectBody {
            name_len: U16::new(name.len() as u16),
            subj_len: U16::new(subject.len() as u16),
            _pad:     [0u8; 4],
        };
        let n = name.len();
        frame.tail[..n].copy_from_slice(name);
        frame.tail[n..n + subject.len()].copy_from_slice(subject);
        frame
    }
}

// ── ListStreams — sized (no tail) ──────────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct ListStreamsBody {
    pub offset: U32,
    pub limit:  U32,
}
pub const LIST_STREAMS_BODY_SIZE: usize = core::mem::size_of::<ListStreamsBody>();
const _: () = assert!(LIST_STREAMS_BODY_SIZE == 8);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct ListStreamsFrame {
    pub header: Header,
    pub body:   ListStreamsBody,
}
const _: () = assert!(core::mem::size_of::<ListStreamsFrame>() == HEADER_SIZE + LIST_STREAMS_BODY_SIZE);

impl ListStreamsFrame {
    pub const WIRE_SIZE: usize = HEADER_SIZE + LIST_STREAMS_BODY_SIZE;

    #[inline(always)]
    pub fn new(seq: u64, offset: u32, limit: u32) -> Self {
        Self {
            header: Header::new(Action::ListStreams.as_u16(), LIST_STREAMS_BODY_SIZE as u32, seq),
            body:   ListStreamsBody { offset: U32::new(offset), limit: U32::new(limit) },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_stream_roundtrip() {
        let size = CreateStreamFrame::wire_size(5, 7);
        let mut buf = vec![0u8; size];
        CreateStreamFrame::encode_into(&mut buf, 1, b"hello", b"orders.", 100, 200, 300, 1, 2, 3, 4);
        let frame = CreateStreamFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(frame.header.action.get(), Action::CreateStream.as_u16());
        assert_eq!(frame.name(), b"hello");
        assert_eq!(frame.filter(), b"orders.");
        assert_eq!(frame.body.max_msgs.get(), 100);
        assert_eq!(frame.body.discard, 4);
        assert_eq!(frame.as_bytes(), &buf[..]);
    }

    #[test]
    fn delete_stream_roundtrip() {
        let size = DeleteStreamFrame::wire_size(3);
        let mut buf = vec![0u8; size];
        DeleteStreamFrame::encode_into(&mut buf, 9, b"abc");
        let frame = DeleteStreamFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(frame.header.action.get(), Action::DeleteStream.as_u16());
        assert_eq!(frame.name(), b"abc");
    }

    #[test]
    fn drain_subject_roundtrip() {
        let size = DrainSubjectFrame::wire_size(3, 5);
        let mut buf = vec![0u8; size];
        DrainSubjectFrame::encode_into(&mut buf, 7, b"str", b"a.b.c");
        let f = DrainSubjectFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(f.name(), b"str");
        assert_eq!(f.subject(), b"a.b.c");
    }

    #[test]
    fn list_streams_sized() {
        assert_eq!(ListStreamsFrame::WIRE_SIZE, HEADER_SIZE + 8);
        let f = ListStreamsFrame::new(1, 0, 100);
        let bytes = f.as_bytes();
        let p = ListStreamsFrame::ref_from_bytes(bytes).unwrap();
        assert_eq!(p.body.limit.get(), 100);
    }
}
