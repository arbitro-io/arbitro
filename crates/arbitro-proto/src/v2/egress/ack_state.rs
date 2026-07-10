//! Egress ACK_STATE_REP / ACK_BATCH_RESP frames — ack-reliability protocol.
//!
//! Both are fixed-size (no tail).
//!
//! ```text
//! AckStateRep (40 B body):
//!   offset 0:  consumer_id  u32  (4B)
//!   offset 4:  generation   u32  (4B)
//!   offset 8:  cursor       u64  (8B)
//!   offset 16: low_seq      u64  (8B)   ← lowest seq still retained
//!   offset 24: high_seq     u64  (8B)   ← highest seq published
//!   offset 32: status       u32  (4B)
//!   offset 36: _pad         u32  (4B)
//!
//! AckBatchResp (32 B body):
//!   offset 0:  consumer_id      u32  (4B)
//!   offset 4:  new_cursor       u64  (8B)
//!   offset 12: accepted         u32  (4B)
//!   offset 16: ignored          u32  (4B)
//!   offset 20: below_retention  u32  (4B)
//!   offset 24: still_pending    u32  (4B)
//!   offset 28: status           u32  (4B)
//! ```

use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct AckStateRepBody {
    pub consumer_id: U32,
    pub generation: U32,
    pub cursor: U64,
    pub low_seq: U64,
    pub high_seq: U64,
    pub status: U32,
    pub _pad: U32,
}
pub const ACK_STATE_REP_BODY_SIZE: usize = core::mem::size_of::<AckStateRepBody>();
const _: () = assert!(ACK_STATE_REP_BODY_SIZE == 40);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct AckStateRepFrame {
    pub header: Header,
    pub body: AckStateRepBody,
}
const _: () =
    assert!(core::mem::size_of::<AckStateRepFrame>() == HEADER_SIZE + ACK_STATE_REP_BODY_SIZE);

impl AckStateRepFrame {
    pub const WIRE_SIZE: usize = HEADER_SIZE + ACK_STATE_REP_BODY_SIZE;

    #[inline(always)]
    pub fn new(
        seq: u64,
        consumer_id: u32,
        generation: u32,
        cursor: u64,
        low_seq: u64,
        high_seq: u64,
        status: u32,
    ) -> Self {
        Self {
            header: Header::new(
                crate::action::Action::AckStateRep.as_u16(),
                ACK_STATE_REP_BODY_SIZE as u32,
                seq,
            ),
            body: AckStateRepBody {
                consumer_id: U32::new(consumer_id),
                generation: U32::new(generation),
                cursor: U64::new(cursor),
                low_seq: U64::new(low_seq),
                high_seq: U64::new(high_seq),
                status: U32::new(status),
                _pad: U32::new(0),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct AckBatchRespBody {
    pub consumer_id: U32,
    pub new_cursor: U64,
    pub accepted: U32,
    pub ignored: U32,
    pub below_retention: U32,
    pub still_pending: U32,
    pub status: U32,
}
pub const ACK_BATCH_RESP_BODY_SIZE: usize = core::mem::size_of::<AckBatchRespBody>();
const _: () = assert!(ACK_BATCH_RESP_BODY_SIZE == 32);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct AckBatchRespFrame {
    pub header: Header,
    pub body: AckBatchRespBody,
}
const _: () =
    assert!(core::mem::size_of::<AckBatchRespFrame>() == HEADER_SIZE + ACK_BATCH_RESP_BODY_SIZE);

impl AckBatchRespFrame {
    pub const WIRE_SIZE: usize = HEADER_SIZE + ACK_BATCH_RESP_BODY_SIZE;

    #[inline(always)]
    pub fn new(
        seq: u64,
        consumer_id: u32,
        new_cursor: u64,
        accepted: u32,
        ignored: u32,
        below_retention: u32,
        still_pending: u32,
        status: u32,
    ) -> Self {
        Self {
            header: Header::new(
                crate::action::Action::AckBatchResp.as_u16(),
                ACK_BATCH_RESP_BODY_SIZE as u32,
                seq,
            ),
            body: AckBatchRespBody {
                consumer_id: U32::new(consumer_id),
                new_cursor: U64::new(new_cursor),
                accepted: U32::new(accepted),
                ignored: U32::new(ignored),
                below_retention: U32::new(below_retention),
                still_pending: U32::new(still_pending),
                status: U32::new(status),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_state_rep_size() {
        assert_eq!(AckStateRepFrame::WIRE_SIZE, 56);
    }

    #[test]
    fn ack_state_rep_roundtrip() {
        let f = AckStateRepFrame::new(1, 42, 3, 1000, 500, 2000, 0);
        let bytes = f.as_bytes();
        let p = AckStateRepFrame::ref_from_bytes(bytes).unwrap();
        assert_eq!(p.body.consumer_id.get(), 42);
        assert_eq!(p.body.generation.get(), 3);
        assert_eq!(p.body.cursor.get(), 1000);
        assert_eq!(p.body.low_seq.get(), 500);
        assert_eq!(p.body.high_seq.get(), 2000);
        assert_eq!(p.body.status.get(), 0);
    }

    #[test]
    fn ack_batch_resp_size() {
        assert_eq!(AckBatchRespFrame::WIRE_SIZE, 48);
    }

    #[test]
    fn ack_batch_resp_roundtrip() {
        let f = AckBatchRespFrame::new(2, 77, 3000, 10, 2, 1, 0, 0);
        let bytes = f.as_bytes();
        let p = AckBatchRespFrame::ref_from_bytes(bytes).unwrap();
        assert_eq!(p.body.consumer_id.get(), 77);
        assert_eq!(p.body.new_cursor.get(), 3000);
        assert_eq!(p.body.accepted.get(), 10);
        assert_eq!(p.body.ignored.get(), 2);
        assert_eq!(p.body.below_retention.get(), 1);
        assert_eq!(p.body.still_pending.get(), 0);
        assert_eq!(p.body.status.get(), 0);
    }
}
