//! Server replies — RepOk (success) and RepError (failure).
//!
//! Both are fixed-size (no tail). Used for command ack (CreateStream,
//! Subscribe response, etc.) — NOT on the publish hot path.
//!
//! ```text
//! RepOk (8 B body):
//!   offset 0: ref_seq     u64  (8B)   ← seq of the request being answered
//!
//! RepError (16 B body):
//!   offset 0: ref_seq     u64  (8B)
//!   offset 8: error_code  u16  (2B)
//!   offset 10: _pad       u8[6]
//! ```

use zerocopy::byteorder::little_endian::{U16, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct RepOkBody {
    pub ref_seq: U64,
}
pub const REP_OK_BODY_SIZE: usize = core::mem::size_of::<RepOkBody>();
const _: () = assert!(REP_OK_BODY_SIZE == 8);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct RepOkFrame {
    pub header: Header,
    pub body:   RepOkBody,
}
const _: () = assert!(core::mem::size_of::<RepOkFrame>() == HEADER_SIZE + REP_OK_BODY_SIZE);

impl RepOkFrame {
    pub const WIRE_SIZE: usize = HEADER_SIZE + REP_OK_BODY_SIZE;

    #[inline(always)]
    pub fn new(seq: u64, ref_seq: u64) -> Self {
        Self {
            header: Header::new(crate::action::Action::RepOk.as_u16(), REP_OK_BODY_SIZE as u32, seq),
            body:   RepOkBody { ref_seq: U64::new(ref_seq) },
        }
    }
}

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct RepErrBody {
    pub ref_seq:    U64,
    pub error_code: U16,
    pub _pad:       [u8; 6],
}
pub const REP_ERR_BODY_SIZE: usize = core::mem::size_of::<RepErrBody>();
const _: () = assert!(REP_ERR_BODY_SIZE == 16);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct RepErrFrame {
    pub header: Header,
    pub body:   RepErrBody,
}
const _: () = assert!(core::mem::size_of::<RepErrFrame>() == HEADER_SIZE + REP_ERR_BODY_SIZE);

impl RepErrFrame {
    pub const WIRE_SIZE: usize = HEADER_SIZE + REP_ERR_BODY_SIZE;

    #[inline(always)]
    pub fn new(seq: u64, ref_seq: u64, error_code: u16) -> Self {
        Self {
            header: Header::new(crate::action::Action::RepError.as_u16(), REP_ERR_BODY_SIZE as u32, seq),
            body:   RepErrBody {
                ref_seq:    U64::new(ref_seq),
                error_code: U16::new(error_code),
                _pad:       [0; 6],
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rep_ok_size() {
        assert_eq!(RepOkFrame::WIRE_SIZE, 24);
    }

    #[test]
    fn rep_err_size() {
        assert_eq!(RepErrFrame::WIRE_SIZE, 32);
    }

    #[test]
    fn rep_ok_roundtrip() {
        let f = RepOkFrame::new(1, 999);
        let bytes = f.as_bytes();
        let p = RepOkFrame::ref_from_bytes(bytes).unwrap();
        assert_eq!(p.body.ref_seq.get(), 999);
    }

    #[test]
    fn rep_err_roundtrip() {
        let f = RepErrFrame::new(2, 888, 42);
        let bytes = f.as_bytes();
        let p = RepErrFrame::ref_from_bytes(bytes).unwrap();
        assert_eq!(p.body.ref_seq.get(), 888);
        assert_eq!(p.body.error_code.get(), 42);
    }
}
