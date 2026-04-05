use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// 16B — Acknowledge delivery of a message.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct AckAction {
    pub sequence: U64,
    pub consumer_id: U32,
    pub _pad: U32,
}
const _: () = assert!(core::mem::size_of::<AckAction>() == 16);

/// 16B — Negative ack (request redelivery).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct NackAction {
    pub sequence: U64,
    pub consumer_id: U32,
    pub delay_ms: U32,
}
const _: () = assert!(core::mem::size_of::<NackAction>() == 16);

/// 16B — Server confirms a request succeeded.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct RepOkAction {
    pub ref_seq: U64,
    pub _pad: U64,
}
const _: () = assert!(core::mem::size_of::<RepOkAction>() == 16);

/// 16B — Server reports an error.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct RepErrorAction {
    pub ref_seq: U64,
    pub error_code: U16,
    pub _pad: [u8; 6],
}
const _: () = assert!(core::mem::size_of::<RepErrorAction>() == 16);

// ── Lazy views ──────────────────────────────────────────────────────────────

pub struct AckView<'a> {
    buf: &'a [u8],
}

impl<'a> AckView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    fn inner(&self) -> &AckAction {
        AckAction::ref_from_bytes(&self.buf[..core::mem::size_of::<AckAction>()]).unwrap()
    }

    #[inline(always)]
    pub fn sequence(&self) -> u64 { self.inner().sequence.get() }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 { self.inner().consumer_id.get() }
}

pub struct NackView<'a> {
    buf: &'a [u8],
}

impl<'a> NackView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    fn inner(&self) -> &NackAction {
        NackAction::ref_from_bytes(&self.buf[..core::mem::size_of::<NackAction>()]).unwrap()
    }

    #[inline(always)]
    pub fn sequence(&self) -> u64 { self.inner().sequence.get() }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 { self.inner().consumer_id.get() }

    #[inline(always)]
    pub fn delay_ms(&self) -> u32 { self.inner().delay_ms.get() }
}

pub struct RepOkView<'a> {
    buf: &'a [u8],
}

impl<'a> RepOkView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    pub fn ref_seq(&self) -> u64 {
        RepOkAction::ref_from_bytes(&self.buf[..core::mem::size_of::<RepOkAction>()]).unwrap().ref_seq.get()
    }
}

pub struct RepErrorView<'a> {
    buf: &'a [u8],
}

impl<'a> RepErrorView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    fn inner(&self) -> &RepErrorAction {
        RepErrorAction::ref_from_bytes(&self.buf[..core::mem::size_of::<RepErrorAction>()]).unwrap()
    }

    #[inline(always)]
    pub fn ref_seq(&self) -> u64 { self.inner().ref_seq.get() }

    #[inline(always)]
    pub fn error_code(&self) -> u16 { self.inner().error_code.get() }
}
