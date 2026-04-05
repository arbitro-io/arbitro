use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

// ── Consumer management ─────────────────────────────────────────────────────

/// 24B fixed — Create a consumer. Variable name + subject follow.
///
/// ```text
/// [2 name_len][2 subj_len][4 stream_id][2 max_inflight][1 deliver_policy][1 deliver_mode][4 ack_wait_ms][8 start_seq]
/// ```
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct CreateConsumerFixed {
    pub name_len: U16,
    pub subj_len: U16,
    pub stream_id: U32,
    pub max_inflight: U16,
    pub deliver_policy: u8,
    pub deliver_mode: u8,
    pub ack_wait_ms: U32,
    pub start_seq: U64,
}

pub const CREATE_CONSUMER_FIXED_SIZE: usize = core::mem::size_of::<CreateConsumerFixed>();
const _: () = assert!(CREATE_CONSUMER_FIXED_SIZE == 24);

/// 8B — Delete a consumer by ID.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct DeleteConsumerAction {
    pub consumer_id: U32,
    pub _pad: U32,
}
const _: () = assert!(core::mem::size_of::<DeleteConsumerAction>() == 8);

/// 8B — Get consumer info by ID.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct GetConsumerAction {
    pub consumer_id: U32,
    pub _pad: U32,
}
const _: () = assert!(core::mem::size_of::<GetConsumerAction>() == 8);

/// 8B — List consumers for a stream.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct ListConsumersAction {
    pub stream_id: U32,
    pub _pad: U32,
}
const _: () = assert!(core::mem::size_of::<ListConsumersAction>() == 8);

/// 8B — List all streams. No variable data.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct ListStreamsAction {
    pub offset: U32,
    pub limit: U32,
}
const _: () = assert!(core::mem::size_of::<ListStreamsAction>() == 8);

// ── Lazy views ──────────────────────────────────────────────────────────────

pub struct CreateConsumerView<'a> {
    buf: &'a [u8],
}

impl<'a> CreateConsumerView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    fn fixed(&self) -> &CreateConsumerFixed {
        CreateConsumerFixed::ref_from_bytes(&self.buf[..CREATE_CONSUMER_FIXED_SIZE]).unwrap()
    }

    #[inline(always)]
    pub fn stream_id(&self) -> u32 { self.fixed().stream_id.get() }

    #[inline(always)]
    pub fn max_inflight(&self) -> u16 { self.fixed().max_inflight.get() }

    #[inline(always)]
    pub fn deliver_policy(&self) -> u8 { self.fixed().deliver_policy }

    #[inline(always)]
    pub fn deliver_mode(&self) -> u8 { self.fixed().deliver_mode }

    #[inline(always)]
    pub fn ack_wait_ms(&self) -> u32 { self.fixed().ack_wait_ms.get() }

    #[inline(always)]
    pub fn start_seq(&self) -> u64 { self.fixed().start_seq.get() }

    #[inline(always)]
    pub fn name(&self) -> &'a [u8] {
        let nl = self.fixed().name_len.get() as usize;
        &self.buf[CREATE_CONSUMER_FIXED_SIZE..CREATE_CONSUMER_FIXED_SIZE + nl]
    }

    #[inline(always)]
    pub fn subject(&self) -> &'a [u8] {
        let nl = self.fixed().name_len.get() as usize;
        let sl = self.fixed().subj_len.get() as usize;
        let start = CREATE_CONSUMER_FIXED_SIZE + nl;
        &self.buf[start..start + sl]
    }
}

pub struct DeleteConsumerView<'a> {
    buf: &'a [u8],
}

impl<'a> DeleteConsumerView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 {
        DeleteConsumerAction::ref_from_bytes(&self.buf[..core::mem::size_of::<DeleteConsumerAction>()]).unwrap().consumer_id.get()
    }
}

pub struct GetConsumerView<'a> {
    buf: &'a [u8],
}

impl<'a> GetConsumerView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 {
        GetConsumerAction::ref_from_bytes(&self.buf[..core::mem::size_of::<GetConsumerAction>()]).unwrap().consumer_id.get()
    }
}

pub struct ListConsumersView<'a> {
    buf: &'a [u8],
}

impl<'a> ListConsumersView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    pub fn stream_id(&self) -> u32 {
        ListConsumersAction::ref_from_bytes(&self.buf[..core::mem::size_of::<ListConsumersAction>()]).unwrap().stream_id.get()
    }
}

pub struct ListStreamsView<'a> {
    buf: &'a [u8],
}

impl<'a> ListStreamsView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    fn inner(&self) -> &ListStreamsAction {
        ListStreamsAction::ref_from_bytes(&self.buf[..core::mem::size_of::<ListStreamsAction>()]).unwrap()
    }

    #[inline(always)]
    pub fn offset(&self) -> u32 { self.inner().offset.get() }

    #[inline(always)]
    pub fn limit(&self) -> u32 { self.inner().limit.get() }
}
