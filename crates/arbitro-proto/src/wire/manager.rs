use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

// ── Consumer management ─────────────────────────────────────────────────────

/// 28B fixed — Create a consumer. Variable name + subject follow.
///
/// ```text
/// [2 name_len][2 subj_len][4 stream_id][2 max_inflight][1 ack_policy][1 deliver_policy][1 deliver_mode][3 pad][4 ack_wait_ms][8 start_seq]
/// ```
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct CreateConsumerFixed {
    pub name_len: U16,
    pub subj_len: U16,
    pub stream_id: U32,
    pub max_inflight: U16,
    pub ack_policy: u8,
    pub deliver_policy: u8,
    pub deliver_mode: u8,
    pub _pad: [u8; 3],
    pub ack_wait_ms: U32,
    pub start_seq: U64,
}

pub const CREATE_CONSUMER_FIXED_SIZE: usize = core::mem::size_of::<CreateConsumerFixed>();
const _: () = assert!(CREATE_CONSUMER_FIXED_SIZE == 28);

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
    pub fn ack_policy(&self) -> u8 { self.fixed().ack_policy }

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

    /// Iterator over (pattern, limit) pairs from the variable trailer.
    /// Trailer format: [2 num_limits] then per limit: [4 limit][2 pattern_len][pattern bytes]
    /// Returns an empty iterator when num_limits == 0 (backward compat).
    pub fn limits(&self) -> LimitsIter<'a> {
        let nl = self.fixed().name_len.get() as usize;
        let sl = self.fixed().subj_len.get() as usize;
        let trailer_start = CREATE_CONSUMER_FIXED_SIZE + nl + sl;
        if self.buf.len() < trailer_start + 2 {
            return LimitsIter { buf: &[], remaining: 0, pos: 0 };
        }
        let num = u16::from_le_bytes([self.buf[trailer_start], self.buf[trailer_start + 1]]) as usize;
        LimitsIter { buf: self.buf, remaining: num, pos: trailer_start + 2 }
    }
}

/// Iterator yielding `(pattern: &[u8], limit: u32)` from the subject_limits trailer.
pub struct LimitsIter<'a> {
    buf:       &'a [u8],
    remaining: usize,
    pos:       usize,
}

impl<'a> Iterator for LimitsIter<'a> {
    type Item = (&'a [u8], u32);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 { return None; }
        // Need at least 6 bytes: [4 limit][2 pattern_len]
        if self.buf.len() < self.pos + 6 { return None; }
        let limit = u32::from_le_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        let plen = u16::from_le_bytes([self.buf[self.pos + 4], self.buf[self.pos + 5]]) as usize;
        self.pos += 6;
        if self.buf.len() < self.pos + plen { return None; }
        let pattern = &self.buf[self.pos..self.pos + plen];
        self.pos += plen;
        self.remaining -= 1;
        Some((pattern, limit))
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
