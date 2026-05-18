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
// Body layout (40 B fixed + name + filter):
//   name_len               u16
//   filter_len             u16
//   max_msgs               u64
//   max_bytes              u64
//   max_age_secs           u64
//   replicas               u8
//   journal_kind           u8
//   retention              u8
//   discard                u8
//   idempotency_window_ms  u32   // 0 = idempotency DISABLED for this stream
//                                //     (no dedup tracker, no per-publish hash,
//                                //     fast-bail bool stays false)
//                                // >0 = window in ms during which a duplicate
//                                //      msg_id is rejected with code 203
//   _pad                   u32   // reserved for future flags
// Tail: [name (name_len)][filter (filter_len)]
//
// Backwards-compat note: extending the fixed body from 32 → 40 B is a
// wire-incompatible change. All clients must be updated together. We
// pick this over a separate "CreateStreamWithIdempotency" opcode for
// API simplicity — a single CreateStream covers both modes (window_ms
// = 0 is the legacy default).

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct CreateStreamBody {
    pub name_len:              U16,
    pub filter_len:            U16,
    pub max_msgs:              U64,
    pub max_bytes:             U64,
    pub max_age_secs:          U64,
    pub replicas:              u8,
    pub journal_kind:          u8,
    pub retention:             u8,
    pub discard:               u8,
    pub idempotency_window_ms: U32,
    pub _pad:                  U32,
}
pub const CREATE_STREAM_BODY_FIXED: usize = core::mem::size_of::<CreateStreamBody>();
const _: () = assert!(CREATE_STREAM_BODY_FIXED == 40);

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

    #[allow(clippy::too_many_arguments)]
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
        idempotency_window_ms: u32,
    ) -> &'a mut Self {
        debug_assert_eq!(out.len(), Self::wire_size(name.len(), filter.len()));
        let msg_len = (CREATE_STREAM_BODY_FIXED + name.len() + filter.len()) as u32;
        let frame = Self::mut_from_bytes(out).expect("CreateStreamFrame layout");
        frame.header = Header::new(Action::CreateStream.as_u16(), msg_len, seq);
        frame.body = CreateStreamBody {
            name_len:              U16::new(name.len() as u16),
            filter_len:            U16::new(filter.len() as u16),
            max_msgs:              U64::new(max_msgs),
            max_bytes:             U64::new(max_bytes),
            max_age_secs:          U64::new(max_age_secs),
            replicas,
            journal_kind,
            retention,
            discard,
            idempotency_window_ms: U32::new(idempotency_window_ms),
            _pad:                  U32::new(0),
        };
        let n = name.len();
        frame.tail[..n].copy_from_slice(name);
        frame.tail[n..n + filter.len()].copy_from_slice(filter);
        frame
    }
}

// DeleteStream / GetStream / PurgeStream / DrainSubject migrated to serde —
// see `v2::cold` module.

// ListStreams migrated to serde — see `v2::cold` module.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_stream_roundtrip() {
        let size = CreateStreamFrame::wire_size(5, 7);
        let mut buf = vec![0u8; size];
        CreateStreamFrame::encode_into(
            &mut buf, 1, b"hello", b"orders.",
            100, 200, 300, 1, 2, 3, 4, 60_000,
        );
        let frame = CreateStreamFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(frame.header.action.get(), Action::CreateStream.as_u16());
        assert_eq!(frame.name(), b"hello");
        assert_eq!(frame.filter(), b"orders.");
        assert_eq!(frame.body.max_msgs.get(), 100);
        assert_eq!(frame.body.discard, 4);
        assert_eq!(frame.body.idempotency_window_ms.get(), 60_000);
        assert_eq!(frame.as_bytes(), &buf[..]);
    }

    // delete_stream / drain_subject / list_streams tests removed —
    // frames migrated to v2::cold and tested there.
}
