use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

// ── Consumer management ─────────────────────────────────────────────────────

/// 28B fixed — Create a consumer. Variable name + group + subject follow.
///
/// ```text
/// [2 name_len][2 subj_len][4 stream_id][2 max_inflight][1 ack_policy][1 deliver_policy][1 deliver_mode][1 pad][2 group_len][4 ack_wait_ms][8 start_seq]
/// ```
///
/// Variable data layout: `[name (name_len)][group (group_len)][subject (subj_len)]`
/// If `group_len == 0`, the server uses the stream name as default group.
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
    pub _pad: u8,
    pub group_len: U16,
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
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    fn fixed(&self) -> &CreateConsumerFixed {
        CreateConsumerFixed::ref_from_bytes(&self.buf[..CREATE_CONSUMER_FIXED_SIZE]).unwrap()
    }

    #[inline(always)]
    pub fn stream_id(&self) -> u32 {
        self.fixed().stream_id.get()
    }

    #[inline(always)]
    pub fn max_inflight(&self) -> u16 {
        self.fixed().max_inflight.get()
    }

    #[inline(always)]
    pub fn ack_policy(&self) -> u8 {
        self.fixed().ack_policy
    }

    #[inline(always)]
    pub fn deliver_policy(&self) -> u8 {
        self.fixed().deliver_policy
    }

    #[inline(always)]
    pub fn deliver_mode(&self) -> u8 {
        self.fixed().deliver_mode
    }

    #[inline(always)]
    pub fn ack_wait_ms(&self) -> u32 {
        self.fixed().ack_wait_ms.get()
    }

    #[inline(always)]
    pub fn start_seq(&self) -> u64 {
        self.fixed().start_seq.get()
    }

    #[inline(always)]
    pub fn group_len(&self) -> u16 {
        self.fixed().group_len.get()
    }

    #[inline(always)]
    pub fn name(&self) -> &'a [u8] {
        let nl = self.fixed().name_len.get() as usize;
        &self.buf[CREATE_CONSUMER_FIXED_SIZE..CREATE_CONSUMER_FIXED_SIZE + nl]
    }

    /// Queue group name. Empty slice means "use stream name as default".
    #[inline(always)]
    pub fn group(&self) -> &'a [u8] {
        let nl = self.fixed().name_len.get() as usize;
        let gl = self.fixed().group_len.get() as usize;
        let start = CREATE_CONSUMER_FIXED_SIZE + nl;
        &self.buf[start..start + gl]
    }

    #[inline(always)]
    pub fn subject(&self) -> &'a [u8] {
        let nl = self.fixed().name_len.get() as usize;
        let gl = self.fixed().group_len.get() as usize;
        let sl = self.fixed().subj_len.get() as usize;
        let start = CREATE_CONSUMER_FIXED_SIZE + nl + gl;
        &self.buf[start..start + sl]
    }

    /// Start of the variable trailer after fixed + name + group + subject.
    #[inline(always)]
    fn trailer_offset(&self) -> usize {
        let nl = self.fixed().name_len.get() as usize;
        let gl = self.fixed().group_len.get() as usize;
        let sl = self.fixed().subj_len.get() as usize;
        CREATE_CONSUMER_FIXED_SIZE + nl + gl + sl
    }

    /// Iterator over per-subject inflight limits from the wire trailer.
    ///
    /// Trailer format: `[2 count][ N × [4 limit][2 pattern_len][pattern] ]`
    pub fn subject_limits(&self) -> SubjectLimitIter<'a> {
        let off = self.trailer_offset();
        if off + 2 > self.buf.len() {
            return SubjectLimitIter {
                buf: &[],
                remaining: 0,
            };
        }
        let count = u16::from_le_bytes([self.buf[off], self.buf[off + 1]]) as usize;
        SubjectLimitIter {
            buf: &self.buf[off + 2..],
            remaining: count,
        }
    }
}

/// Iterates `[4 limit][2 pattern_len][pattern]` entries.
pub struct SubjectLimitIter<'a> {
    buf: &'a [u8],
    remaining: usize,
}

pub struct SubjectLimitEntry<'a> {
    pub pattern: &'a [u8],
    pub limit: u32,
}

impl<'a> Iterator for SubjectLimitIter<'a> {
    type Item = SubjectLimitEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 || self.buf.len() < 6 {
            return None;
        }
        let limit = u32::from_le_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
        let plen = u16::from_le_bytes([self.buf[4], self.buf[5]]) as usize;
        if 6 + plen > self.buf.len() {
            self.remaining = 0;
            return None;
        }
        let pattern = &self.buf[6..6 + plen];
        self.buf = &self.buf[6 + plen..];
        self.remaining -= 1;
        Some(SubjectLimitEntry { pattern, limit })
    }
}

pub struct DeleteConsumerView<'a> {
    buf: &'a [u8],
}

impl<'a> DeleteConsumerView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 {
        DeleteConsumerAction::ref_from_bytes(
            &self.buf[..core::mem::size_of::<DeleteConsumerAction>()],
        )
        .unwrap()
        .consumer_id
        .get()
    }
}
