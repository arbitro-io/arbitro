use zerocopy::byteorder::little_endian::{U16, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// 8B — Ping (keepalive).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct PingAction {
    pub ping_id: U64,
}
const _: () = assert!(core::mem::size_of::<PingAction>() == 8);

/// 8B — Pong (response to ping).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct PongAction {
    pub ping_id: U64,
}
const _: () = assert!(core::mem::size_of::<PongAction>() == 8);

/// 16B — Client sends on connect. Variable auth_token may follow.
///
/// ```text
/// [1 proto_version][1 flags][2 auth_len][4 pad][8 pad]
/// ```
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct ConnectFixed {
    pub proto_version: u8,
    pub flags: u8,
    pub auth_len: U16,
    pub _pad: [u8; 4],
    pub _pad2: U64,
}

pub const CONNECT_FIXED_SIZE: usize = core::mem::size_of::<ConnectFixed>();
const _: () = assert!(CONNECT_FIXED_SIZE == 16);

/// 16B — Server sends after successful connect.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct ConnectedAction {
    pub conn_id: U64,
    pub proto_version: u8,
    pub flags: u8,
    pub _pad: [u8; 6],
}
const _: () = assert!(core::mem::size_of::<ConnectedAction>() == 16);

/// 8B — Graceful disconnect.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct DisconnectAction {
    pub reason_code: U16,
    pub _pad: [u8; 6],
}
const _: () = assert!(core::mem::size_of::<DisconnectAction>() == 8);

// ── Lazy views ──────────────────────────────────────────────────────────────

pub struct PingView<'a> {
    buf: &'a [u8],
}

impl<'a> PingView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    pub fn ping_id(&self) -> u64 {
        PingAction::ref_from_bytes(&self.buf[..core::mem::size_of::<PingAction>()]).unwrap().ping_id.get()
    }
}

pub struct ConnectView<'a> {
    buf: &'a [u8],
}

impl<'a> ConnectView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    fn fixed(&self) -> &ConnectFixed {
        ConnectFixed::ref_from_bytes(&self.buf[..CONNECT_FIXED_SIZE]).unwrap()
    }

    #[inline(always)]
    pub fn proto_version(&self) -> u8 { self.fixed().proto_version }

    #[inline(always)]
    pub fn flags(&self) -> u8 { self.fixed().flags }

    #[inline(always)]
    pub fn auth_token(&self) -> &'a [u8] {
        let al = self.fixed().auth_len.get() as usize;
        &self.buf[CONNECT_FIXED_SIZE..CONNECT_FIXED_SIZE + al]
    }
}

pub struct ConnectedView<'a> {
    buf: &'a [u8],
}

impl<'a> ConnectedView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    fn inner(&self) -> &ConnectedAction {
        ConnectedAction::ref_from_bytes(&self.buf[..core::mem::size_of::<ConnectedAction>()]).unwrap()
    }

    #[inline(always)]
    pub fn conn_id(&self) -> u64 { self.inner().conn_id.get() }

    #[inline(always)]
    pub fn proto_version(&self) -> u8 { self.inner().proto_version }
}

pub struct DisconnectView<'a> {
    buf: &'a [u8],
}

impl<'a> DisconnectView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self { Self { buf } }

    #[inline(always)]
    pub fn reason_code(&self) -> u16 {
        DisconnectAction::ref_from_bytes(&self.buf[..core::mem::size_of::<DisconnectAction>()]).unwrap().reason_code.get()
    }
}
