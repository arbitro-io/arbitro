use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// 20B fixed — Subscribe to a subject. Variable subject follows.
///
/// ```text
/// [4 consumer_id][2 subj_len][2 max_inflight][1 deliver_policy][1 deliver_mode][2 pad][8 start_seq]
/// ```
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct SubscribeFixed {
    pub consumer_id: U32,
    pub subj_len: U16,
    pub max_inflight: U16,
    pub deliver_policy: u8,
    pub deliver_mode: u8,
    pub _pad: [u8; 2],
    pub start_seq: U64,
}

pub const SUBSCRIBE_FIXED_SIZE: usize = core::mem::size_of::<SubscribeFixed>();
const _: () = assert!(SUBSCRIBE_FIXED_SIZE == 20);

/// 8B — Unsubscribe from a consumer.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct UnsubscribeAction {
    pub consumer_id: U32,
    pub _pad: U32,
}
const _: () = assert!(core::mem::size_of::<UnsubscribeAction>() == 8);

// ── Lazy views ──────────────────────────────────────────────────────────────

pub struct SubscribeView<'a> {
    buf: &'a [u8],
}

impl<'a> SubscribeView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    fn fixed(&self) -> &SubscribeFixed {
        SubscribeFixed::ref_from_bytes(&self.buf[..SUBSCRIBE_FIXED_SIZE]).unwrap()
    }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 { self.fixed().consumer_id.get() }

    #[inline(always)]
    pub fn max_inflight(&self) -> u16 { self.fixed().max_inflight.get() }

    #[inline(always)]
    pub fn deliver_policy(&self) -> u8 { self.fixed().deliver_policy }

    #[inline(always)]
    pub fn deliver_mode(&self) -> u8 { self.fixed().deliver_mode }

    #[inline(always)]
    pub fn start_seq(&self) -> u64 { self.fixed().start_seq.get() }

    #[inline(always)]
    pub fn subject(&self) -> &'a [u8] {
        let sl = self.fixed().subj_len.get() as usize;
        &self.buf[SUBSCRIBE_FIXED_SIZE..SUBSCRIBE_FIXED_SIZE + sl]
    }
}

pub struct UnsubscribeView<'a> {
    buf: &'a [u8],
}

impl<'a> UnsubscribeView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 {
        UnsubscribeAction::ref_from_bytes(&self.buf[..core::mem::size_of::<UnsubscribeAction>()]).unwrap().consumer_id.get()
    }
}
