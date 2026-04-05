use zerocopy::byteorder::little_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// 8B — Request broker stats.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct StatsRequest {
    pub request_id: U64,
}
const _: () = assert!(core::mem::size_of::<StatsRequest>() == 8);

/// 64B — Broker stats response (atomic counters snapshot).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct StatsResponse {
    pub request_id: U64,
    pub connections: U64,
    pub total_msgs_in: U64,
    pub total_msgs_out: U64,
    pub total_bytes_in: U64,
    pub total_bytes_out: U64,
    pub streams: U64,
    pub consumers: U64,
}
const _: () = assert!(core::mem::size_of::<StatsResponse>() == 64);

// ── Lazy views ──────────────────────────────────────────────────────────────

pub struct StatsRequestView<'a> {
    buf: &'a [u8],
}

impl<'a> StatsRequestView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    pub fn request_id(&self) -> u64 {
        StatsRequest::ref_from_bytes(&self.buf[..core::mem::size_of::<StatsRequest>()]).unwrap().request_id.get()
    }
}

pub struct StatsResponseView<'a> {
    buf: &'a [u8],
}

impl<'a> StatsResponseView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    fn inner(&self) -> &StatsResponse {
        StatsResponse::ref_from_bytes(&self.buf[..core::mem::size_of::<StatsResponse>()]).unwrap()
    }

    #[inline(always)]
    pub fn request_id(&self) -> u64 { self.inner().request_id.get() }

    #[inline(always)]
    pub fn connections(&self) -> u64 { self.inner().connections.get() }

    #[inline(always)]
    pub fn total_msgs_in(&self) -> u64 { self.inner().total_msgs_in.get() }

    #[inline(always)]
    pub fn total_msgs_out(&self) -> u64 { self.inner().total_msgs_out.get() }

    #[inline(always)]
    pub fn total_bytes_in(&self) -> u64 { self.inner().total_bytes_in.get() }

    #[inline(always)]
    pub fn total_bytes_out(&self) -> u64 { self.inner().total_bytes_out.get() }

    #[inline(always)]
    pub fn streams(&self) -> u64 { self.inner().streams.get() }

    #[inline(always)]
    pub fn consumers(&self) -> u64 { self.inner().consumers.get() }
}
