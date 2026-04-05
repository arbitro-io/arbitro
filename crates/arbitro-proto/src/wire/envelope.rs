use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::action::Action;
use crate::error::ProtoError;

/// 16B transport envelope — first thing read from every frame.
///
/// ```text
/// [2 action][1 flags][1 rsv][4 stream_id][4 msg_len][4 env_seq]
/// ```
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct Envelope {
    pub action: U16,
    pub flags: u8,
    pub _rsv: u8,
    pub stream_id: U32,
    pub msg_len: U32,
    pub env_seq: U32,
}

pub const ENVELOPE_SIZE: usize = core::mem::size_of::<Envelope>();
const _: () = assert!(ENVELOPE_SIZE == 16);

/// Lazy view over a raw frame buffer.
/// Decodes envelope on access, zero-copy.
pub struct FrameView<'a> {
    buf: &'a [u8],
}

impl<'a> FrameView<'a> {
    /// Wrap a raw buffer. Does NOT validate — call `validate()` at the border.
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    pub fn envelope(&self) -> &Envelope {
        Envelope::ref_from_bytes(&self.buf[..ENVELOPE_SIZE]).unwrap()
    }

    #[inline(always)]
    pub fn action_raw(&self) -> u16 {
        self.envelope().action.get()
    }

    #[inline(always)]
    pub fn action(&self) -> Option<Action> {
        Action::from_u16(self.action_raw())
    }

    #[inline(always)]
    pub fn flags(&self) -> u8 {
        self.envelope().flags
    }

    #[inline(always)]
    pub fn stream_id(&self) -> u32 {
        self.envelope().stream_id.get()
    }

    #[inline(always)]
    pub fn msg_len(&self) -> u32 {
        self.envelope().msg_len.get()
    }

    #[inline(always)]
    pub fn body(&self) -> &'a [u8] {
        &self.buf[ENVELOPE_SIZE..]
    }

    #[inline(always)]
    pub fn raw(&self) -> &'a [u8] {
        self.buf
    }

    /// Validate structural integrity (at connection border, not hot path).
    pub fn validate(&self) -> Result<(), ProtoError> {
        if self.buf.len() < ENVELOPE_SIZE {
            return Err(ProtoError::BufferTooShort {
                need: ENVELOPE_SIZE as u32,
                have: self.buf.len() as u32,
            });
        }
        let msg_len = self.msg_len() as usize;
        if self.buf.len() < ENVELOPE_SIZE + msg_len {
            return Err(ProtoError::BufferTooShort {
                need: (ENVELOPE_SIZE + msg_len) as u32,
                have: self.buf.len() as u32,
            });
        }
        if self.action().is_none() {
            return Err(ProtoError::UnknownAction(self.action_raw()));
        }
        Ok(())
    }
}
