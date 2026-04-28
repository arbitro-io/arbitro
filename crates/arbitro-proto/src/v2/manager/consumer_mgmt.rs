//! Consumer management frames (v2): Create / Delete / Get / ListConsumers.

use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::action::Action;
use crate::v2::header::{Header, HEADER_SIZE};

// ── CreateConsumer ─────────────────────────────────────────────────────
//
// Body (28 B fixed) + tail = [name][group][subject].
//
//   name_len        u16
//   subj_len        u16
//   stream_id       u32
//   max_inflight    u16
//   ack_policy      u8
//   deliver_policy  u8
//   deliver_mode    u8
//   _pad            u8
//   group_len       u16
//   ack_wait_ms     u32
//   start_seq       u64

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct CreateConsumerBody {
    pub name_len:       U16,
    pub subj_len:       U16,
    pub stream_id:      U32,
    pub max_inflight:   U16,
    pub ack_policy:     u8,
    pub deliver_policy: u8,
    pub deliver_mode:   u8,
    pub _pad:           u8,
    pub group_len:      U16,
    pub ack_wait_ms:    U32,
    pub start_seq:      U64,
}
pub const CREATE_CONSUMER_BODY_FIXED: usize = core::mem::size_of::<CreateConsumerBody>();
const _: () = assert!(CREATE_CONSUMER_BODY_FIXED == 28);

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct CreateConsumerFrame {
    pub header: Header,
    pub body:   CreateConsumerBody,
    pub tail:   [u8],
}

impl CreateConsumerFrame {
    #[inline(always)]
    pub const fn wire_size(name_len: usize, group_len: usize, subj_len: usize) -> usize {
        HEADER_SIZE + CREATE_CONSUMER_BODY_FIXED + name_len + group_len + subj_len
    }

    #[inline(always)]
    pub fn name(&self) -> &[u8] {
        let n = self.body.name_len.get() as usize;
        &self.tail[..n]
    }

    #[inline(always)]
    pub fn group(&self) -> &[u8] {
        let n = self.body.name_len.get() as usize;
        let g = self.body.group_len.get() as usize;
        &self.tail[n..n + g]
    }

    #[inline(always)]
    pub fn subject(&self) -> &[u8] {
        let n = self.body.name_len.get() as usize;
        let g = self.body.group_len.get() as usize;
        let s = self.body.subj_len.get() as usize;
        &self.tail[n + g..n + g + s]
    }

    pub fn encode_into<'a>(
        out: &'a mut [u8],
        seq: u64,
        stream_id: u32,
        name: &[u8],
        group: &[u8],
        subject: &[u8],
        max_inflight: u16,
        ack_policy: u8,
        deliver_policy: u8,
        deliver_mode: u8,
        ack_wait_ms: u32,
        start_seq: u64,
    ) -> &'a mut Self {
        debug_assert_eq!(out.len(), Self::wire_size(name.len(), group.len(), subject.len()));
        let msg_len = (CREATE_CONSUMER_BODY_FIXED + name.len() + group.len() + subject.len()) as u32;
        let frame = Self::mut_from_bytes(out).expect("CreateConsumerFrame layout");
        frame.header = Header::new(Action::CreateConsumer.as_u16(), msg_len, seq);
        frame.body = CreateConsumerBody {
            name_len:       U16::new(name.len() as u16),
            subj_len:       U16::new(subject.len() as u16),
            stream_id:      U32::new(stream_id),
            max_inflight:   U16::new(max_inflight),
            ack_policy,
            deliver_policy,
            deliver_mode,
            _pad:           0,
            group_len:      U16::new(group.len() as u16),
            ack_wait_ms:    U32::new(ack_wait_ms),
            start_seq:      U64::new(start_seq),
        };
        let n = name.len();
        let g = group.len();
        frame.tail[..n].copy_from_slice(name);
        frame.tail[n..n + g].copy_from_slice(group);
        frame.tail[n + g..n + g + subject.len()].copy_from_slice(subject);
        frame
    }
}

// ── DeleteConsumer (sized, 8 B body) ───────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DeleteConsumerBody {
    pub consumer_id: U32,
    pub _pad:        U32,
}
pub const DELETE_CONSUMER_BODY_SIZE: usize = core::mem::size_of::<DeleteConsumerBody>();
const _: () = assert!(DELETE_CONSUMER_BODY_SIZE == 8);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct DeleteConsumerFrame {
    pub header: Header,
    pub body:   DeleteConsumerBody,
}
const _: () = assert!(core::mem::size_of::<DeleteConsumerFrame>() == HEADER_SIZE + DELETE_CONSUMER_BODY_SIZE);

impl DeleteConsumerFrame {
    pub const WIRE_SIZE: usize = HEADER_SIZE + DELETE_CONSUMER_BODY_SIZE;

    #[inline(always)]
    pub fn new(seq: u64, consumer_id: u32) -> Self {
        Self {
            header: Header::new(Action::DeleteConsumer.as_u16(), DELETE_CONSUMER_BODY_SIZE as u32, seq),
            body:   DeleteConsumerBody { consumer_id: U32::new(consumer_id), _pad: U32::new(0) },
        }
    }
}

// ── GetConsumer (4B id + variable name) ────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct GetConsumerBody {
    pub stream_id: U32,
    pub name_len:  U16,
    pub _pad:      [u8; 2],
}
pub const GET_CONSUMER_BODY_FIXED: usize = core::mem::size_of::<GetConsumerBody>();
const _: () = assert!(GET_CONSUMER_BODY_FIXED == 8);

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct GetConsumerFrame {
    pub header: Header,
    pub body:   GetConsumerBody,
    pub tail:   [u8],
}

impl GetConsumerFrame {
    #[inline(always)]
    pub const fn wire_size(name_len: usize) -> usize {
        HEADER_SIZE + GET_CONSUMER_BODY_FIXED + name_len
    }

    #[inline(always)]
    pub fn name(&self) -> &[u8] {
        let n = self.body.name_len.get() as usize;
        &self.tail[..n]
    }

    pub fn encode_into<'a>(out: &'a mut [u8], seq: u64, stream_id: u32, name: &[u8]) -> &'a mut Self {
        debug_assert_eq!(out.len(), Self::wire_size(name.len()));
        let msg_len = (GET_CONSUMER_BODY_FIXED + name.len()) as u32;
        let frame = Self::mut_from_bytes(out).expect("GetConsumerFrame layout");
        frame.header = Header::new(Action::GetConsumer.as_u16(), msg_len, seq);
        frame.body = GetConsumerBody {
            stream_id: U32::new(stream_id),
            name_len:  U16::new(name.len() as u16),
            _pad:      [0u8; 2],
        };
        frame.tail[..name.len()].copy_from_slice(name);
        frame
    }
}

// ── ListConsumers (sized) ──────────────────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct ListConsumersBody {
    pub stream_id: U32,
    pub offset:    U32,
    pub limit:     U32,
    pub _pad:      U32,
}
pub const LIST_CONSUMERS_BODY_SIZE: usize = core::mem::size_of::<ListConsumersBody>();
const _: () = assert!(LIST_CONSUMERS_BODY_SIZE == 16);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct ListConsumersFrame {
    pub header: Header,
    pub body:   ListConsumersBody,
}
const _: () = assert!(core::mem::size_of::<ListConsumersFrame>() == HEADER_SIZE + LIST_CONSUMERS_BODY_SIZE);

impl ListConsumersFrame {
    pub const WIRE_SIZE: usize = HEADER_SIZE + LIST_CONSUMERS_BODY_SIZE;

    #[inline(always)]
    pub fn new(seq: u64, stream_id: u32, offset: u32, limit: u32) -> Self {
        Self {
            header: Header::new(Action::ListConsumers.as_u16(), LIST_CONSUMERS_BODY_SIZE as u32, seq),
            body:   ListConsumersBody {
                stream_id: U32::new(stream_id),
                offset:    U32::new(offset),
                limit:     U32::new(limit),
                _pad:      U32::new(0),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_consumer_roundtrip() {
        let size = CreateConsumerFrame::wire_size(4, 0, 5);
        let mut buf = vec![0u8; size];
        CreateConsumerFrame::encode_into(&mut buf, 1, 7, b"name", b"", b"a.b.c", 16, 1, 2, 3, 30_000, 0);
        let f = CreateConsumerFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(f.header.action.get(), Action::CreateConsumer.as_u16());
        assert_eq!(f.name(), b"name");
        assert_eq!(f.group(), b"");
        assert_eq!(f.subject(), b"a.b.c");
        assert_eq!(f.body.stream_id.get(), 7);
        assert_eq!(f.body.max_inflight.get(), 16);
        assert_eq!(f.as_bytes(), &buf[..]);
    }

    #[test]
    fn delete_consumer_sized() {
        let f = DeleteConsumerFrame::new(1, 42);
        let bytes = f.as_bytes();
        let p = DeleteConsumerFrame::ref_from_bytes(bytes).unwrap();
        assert_eq!(p.body.consumer_id.get(), 42);
    }

    #[test]
    fn list_consumers_sized() {
        let f = ListConsumersFrame::new(1, 7, 0, 100);
        let p = ListConsumersFrame::ref_from_bytes(f.as_bytes()).unwrap();
        assert_eq!(p.body.stream_id.get(), 7);
        assert_eq!(p.body.limit.get(), 100);
    }
}
