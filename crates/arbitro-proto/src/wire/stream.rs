use zerocopy::byteorder::little_endian::{U16, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// 32B fixed — Create a stream. Variable name follows.
///
/// ```text
/// [2 name_len][2 pad][8 max_msgs][8 max_bytes][8 max_age_secs][1 replicas][1 journal_kind][1 retention][1 pad]
/// ```
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct CreateStreamFixed {
    pub name_len: U16,
    pub _pad: U16,
    pub max_msgs: U64,
    pub max_bytes: U64,
    pub max_age_secs: U64,
    pub replicas: u8,
    pub journal_kind: u8,
    pub retention: u8,
    pub _pad2: u8,
}

pub const CREATE_STREAM_FIXED_SIZE: usize = core::mem::size_of::<CreateStreamFixed>();
const _: () = assert!(CREATE_STREAM_FIXED_SIZE == 32);

/// 8B fixed — Delete a stream. Variable name follows.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct DeleteStreamFixed {
    pub name_len: U16,
    pub _pad: [u8; 6],
}

pub const DELETE_STREAM_FIXED_SIZE: usize = core::mem::size_of::<DeleteStreamFixed>();
const _: () = assert!(DELETE_STREAM_FIXED_SIZE == 8);

/// 8B fixed — Get stream info. Variable name follows.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct GetStreamFixed {
    pub name_len: U16,
    pub _pad: [u8; 6],
}
const _: () = assert!(core::mem::size_of::<GetStreamFixed>() == 8);

/// 8B fixed — Purge all messages from a stream. Variable name follows.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct PurgeStreamFixed {
    pub name_len: U16,
    pub _pad: [u8; 6],
}

pub const PURGE_STREAM_FIXED_SIZE: usize = core::mem::size_of::<PurgeStreamFixed>();
const _: () = assert!(PURGE_STREAM_FIXED_SIZE == 8);

/// 8B fixed — Drain messages by subject from a stream. Variable name + subject follow.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct DrainSubjectFixed {
    pub name_len: U16,
    pub subj_len: U16,
    pub _pad: [u8; 4],
}

pub const DRAIN_SUBJECT_FIXED_SIZE: usize = core::mem::size_of::<DrainSubjectFixed>();
const _: () = assert!(DRAIN_SUBJECT_FIXED_SIZE == 8);

// ── Lazy views ──────────────────────────────────────────────────────────────

pub struct CreateStreamView<'a> {
    buf: &'a [u8],
}

impl<'a> CreateStreamView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    fn fixed(&self) -> &CreateStreamFixed {
        CreateStreamFixed::ref_from_bytes(&self.buf[..CREATE_STREAM_FIXED_SIZE]).unwrap()
    }

    #[inline(always)]
    pub fn name(&self) -> &'a [u8] {
        let nl = self.fixed().name_len.get() as usize;
        &self.buf[CREATE_STREAM_FIXED_SIZE..CREATE_STREAM_FIXED_SIZE + nl]
    }

    #[inline(always)]
    pub fn max_msgs(&self) -> u64 { self.fixed().max_msgs.get() }

    #[inline(always)]
    pub fn max_bytes(&self) -> u64 { self.fixed().max_bytes.get() }

    #[inline(always)]
    pub fn max_age_secs(&self) -> u64 { self.fixed().max_age_secs.get() }

    #[inline(always)]
    pub fn replicas(&self) -> u8 { self.fixed().replicas }

    #[inline(always)]
    pub fn journal_kind(&self) -> u8 { self.fixed().journal_kind }

    #[inline(always)]
    pub fn retention(&self) -> u8 { self.fixed().retention }
}

pub struct DeleteStreamView<'a> {
    buf: &'a [u8],
}

impl<'a> DeleteStreamView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    pub fn name(&self) -> &'a [u8] {
        let f = DeleteStreamFixed::ref_from_bytes(&self.buf[..DELETE_STREAM_FIXED_SIZE]).unwrap();
        let nl = f.name_len.get() as usize;
        &self.buf[DELETE_STREAM_FIXED_SIZE..DELETE_STREAM_FIXED_SIZE + nl]
    }
}

pub struct GetStreamView<'a> {
    buf: &'a [u8],
}

impl<'a> GetStreamView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    pub fn name(&self) -> &'a [u8] {
        let f = GetStreamFixed::ref_from_bytes(&self.buf[..core::mem::size_of::<GetStreamFixed>()]).unwrap();
        let nl = f.name_len.get() as usize;
        &self.buf[core::mem::size_of::<GetStreamFixed>()..core::mem::size_of::<GetStreamFixed>() + nl]
    }
}

pub struct PurgeStreamView<'a> {
    buf: &'a [u8],
}

impl<'a> PurgeStreamView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    pub fn name(&self) -> &'a [u8] {
        let f = PurgeStreamFixed::ref_from_bytes(&self.buf[..PURGE_STREAM_FIXED_SIZE]).unwrap();
        let nl = f.name_len.get() as usize;
        &self.buf[PURGE_STREAM_FIXED_SIZE..PURGE_STREAM_FIXED_SIZE + nl]
    }
}

pub struct DrainSubjectView<'a> {
    buf: &'a [u8],
}

impl<'a> DrainSubjectView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    fn fixed(&self) -> &DrainSubjectFixed {
        DrainSubjectFixed::ref_from_bytes(&self.buf[..DRAIN_SUBJECT_FIXED_SIZE]).unwrap()
    }

    #[inline(always)]
    pub fn name(&self) -> &'a [u8] {
        let nl = self.fixed().name_len.get() as usize;
        &self.buf[DRAIN_SUBJECT_FIXED_SIZE..DRAIN_SUBJECT_FIXED_SIZE + nl]
    }

    #[inline(always)]
    pub fn subject(&self) -> &'a [u8] {
        let nl = self.fixed().name_len.get() as usize;
        let sl = self.fixed().subj_len.get() as usize;
        let start = DRAIN_SUBJECT_FIXED_SIZE + nl;
        &self.buf[start..start + sl]
    }
}
