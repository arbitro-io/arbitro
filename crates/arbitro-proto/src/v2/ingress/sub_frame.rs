//! Ingress SUBSCRIBE / UNSUBSCRIBE frames.
//!
//! ```text
//! [Header 16B]                          ← action = Subscribe | Unsubscribe
//! [SubBody fixed part 12B]
//!   offset 0:  conn_id        u32  (4B)
//!   offset 4:  consumer_id    u32  (4B)   ← 0 on Subscribe (server assigns)
//!   offset 8:  filter_len     u16  (2B)
//!   offset 10: options_flags  u16  (2B)   ← qos, durable, retain-pickup, ...
//! [tail]
//!   [filter  filter_len bytes]            ← subject pattern (e.g. "orders.*.eu")
//! ```

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct SubBody {
    pub conn_id:       U32,
    pub consumer_id:   U32,
    pub filter_len:    U16,
    /// M18: reserved 16 bits for future SubFrame options (qos, durable,
    /// retain-pickup, etc). The current `dispatch_v2::v2_subscribe`
    /// ignores every bit — kept on the wire to lock in the field
    /// position so a future release can wire flags without a
    /// renegotiation. Treat as 0 in tests and producers; clients that
    /// happen to set bits will see no behaviour change.
    pub options_flags: U16,
}
pub const SUB_BODY_FIXED: usize = core::mem::size_of::<SubBody>();
const _: () = assert!(SUB_BODY_FIXED == 12);

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct SubFrame {
    pub header: Header,
    pub body:   SubBody,
    pub tail:   [u8],  // filter
}

impl SubFrame {
    /// **B4 safety**: `filter_len <= tail.len()`.
    #[inline]
    pub fn validate(&self) -> Result<(), crate::error::ErrorCode> {
        let n = self.body.filter_len.get() as usize;
        if n > self.tail.len() {
            return Err(crate::error::ErrorCode::InvalidLength);
        }
        Ok(())
    }

    #[inline(always)]
    pub const fn wire_size(filter_len: usize) -> usize {
        HEADER_SIZE + SUB_BODY_FIXED + filter_len
    }

    #[inline(always)]
    pub fn filter(&self) -> &[u8] {
        let n = self.body.filter_len.get() as usize;
        &self.tail[..n]
    }

    pub fn encode_into<'a>(
        out: &'a mut [u8],
        seq: u64,
        conn_id: u32,
        consumer_id: u32,
        options_flags: u16,
        filter: &[u8],
    ) -> &'a mut Self {
        debug_assert_eq!(out.len(), Self::wire_size(filter.len()));
        let msg_len = (SUB_BODY_FIXED + filter.len()) as u32;
        let frame = Self::mut_from_bytes(out).expect("SubFrame layout");
        frame.header = Header::new(crate::action::Action::Subscribe.as_u16(), msg_len, seq);
        frame.body = SubBody {
            conn_id:       U32::new(conn_id),
            consumer_id:   U32::new(consumer_id),
            filter_len:    U16::new(filter.len() as u16),
            options_flags: U16::new(options_flags),
        };
        frame.tail[..filter.len()].copy_from_slice(filter);
        frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_size_is_12() {
        assert_eq!(SUB_BODY_FIXED, 12);
    }

    #[test]
    fn roundtrip() {
        let filter = b"orders.*.eu";
        let size = SubFrame::wire_size(filter.len());
        let mut buf = vec![0u8; size];
        SubFrame::encode_into(&mut buf, 5, 100, 0, 0x01, filter);
        let f = SubFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(f.body.conn_id.get(), 100);
        assert_eq!(f.body.options_flags.get(), 0x01);
        assert_eq!(f.filter(), filter);
    }
}
