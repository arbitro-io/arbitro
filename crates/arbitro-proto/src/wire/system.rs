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
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: checks the buffer is large enough to hold
    /// a `PingAction` before wrapping it.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < core::mem::size_of::<PingAction>() {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    pub fn ping_id(&self) -> u64 {
        PingAction::ref_from_bytes(&self.buf[..core::mem::size_of::<PingAction>()])
            .unwrap()
            .ping_id
            .get()
    }

    /// Checked variant of `ping_id()`: returns `None` instead of
    /// panicking on a truncated buffer.
    #[inline(always)]
    pub fn try_ping_id(&self) -> Option<u64> {
        let bytes = self.buf.get(..core::mem::size_of::<PingAction>())?;
        Some(PingAction::ref_from_bytes(bytes).ok()?.ping_id.get())
    }
}

pub struct ConnectView<'a> {
    buf: &'a [u8],
}

impl<'a> ConnectView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: checks `buf.len() >= CONNECT_FIXED_SIZE`
    /// before wrapping it.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < CONNECT_FIXED_SIZE {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    fn fixed(&self) -> &ConnectFixed {
        ConnectFixed::ref_from_bytes(&self.buf[..CONNECT_FIXED_SIZE]).unwrap()
    }

    #[inline(always)]
    fn try_fixed(&self) -> Option<&ConnectFixed> {
        ConnectFixed::ref_from_bytes(self.buf.get(..CONNECT_FIXED_SIZE)?).ok()
    }

    #[inline(always)]
    pub fn proto_version(&self) -> u8 {
        self.fixed().proto_version
    }

    #[inline(always)]
    pub fn flags(&self) -> u8 {
        self.fixed().flags
    }

    #[inline(always)]
    pub fn auth_token(&self) -> &'a [u8] {
        let al = self.fixed().auth_len.get() as usize;
        &self.buf[CONNECT_FIXED_SIZE..CONNECT_FIXED_SIZE + al]
    }

    /// Checked variant of `auth_token()`: returns `None` instead of
    /// panicking on a truncated buffer.
    #[inline(always)]
    pub fn try_auth_token(&self) -> Option<&'a [u8]> {
        let al = self.try_fixed()?.auth_len.get() as usize;
        self.buf
            .get(CONNECT_FIXED_SIZE..CONNECT_FIXED_SIZE.checked_add(al)?)
    }
}

pub struct ConnectedView<'a> {
    buf: &'a [u8],
}

impl<'a> ConnectedView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: checks the buffer is large enough to hold
    /// a `ConnectedAction` before wrapping it.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < core::mem::size_of::<ConnectedAction>() {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    fn inner(&self) -> &ConnectedAction {
        ConnectedAction::ref_from_bytes(&self.buf[..core::mem::size_of::<ConnectedAction>()])
            .unwrap()
    }

    #[inline(always)]
    fn try_inner(&self) -> Option<&ConnectedAction> {
        ConnectedAction::ref_from_bytes(self.buf.get(..core::mem::size_of::<ConnectedAction>())?)
            .ok()
    }

    #[inline(always)]
    pub fn conn_id(&self) -> u64 {
        self.inner().conn_id.get()
    }

    #[inline(always)]
    pub fn proto_version(&self) -> u8 {
        self.inner().proto_version
    }

    /// Checked variant of `conn_id()`.
    #[inline(always)]
    pub fn try_conn_id(&self) -> Option<u64> {
        Some(self.try_inner()?.conn_id.get())
    }

    /// Checked variant of `proto_version()`.
    #[inline(always)]
    pub fn try_proto_version(&self) -> Option<u8> {
        Some(self.try_inner()?.proto_version)
    }
}

pub struct DisconnectView<'a> {
    buf: &'a [u8],
}

impl<'a> DisconnectView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: checks the buffer is large enough to hold
    /// a `DisconnectAction` before wrapping it.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < core::mem::size_of::<DisconnectAction>() {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    pub fn reason_code(&self) -> u16 {
        DisconnectAction::ref_from_bytes(&self.buf[..core::mem::size_of::<DisconnectAction>()])
            .unwrap()
            .reason_code
            .get()
    }

    /// Checked variant of `reason_code()`: returns `None` instead of
    /// panicking on a truncated buffer.
    #[inline(always)]
    pub fn try_reason_code(&self) -> Option<u16> {
        let bytes = self.buf.get(..core::mem::size_of::<DisconnectAction>())?;
        Some(
            DisconnectAction::ref_from_bytes(bytes)
                .ok()?
                .reason_code
                .get(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_view_try_new_rejects_short_buffer() {
        let size = core::mem::size_of::<PingAction>();
        let short = vec![0u8; size - 1];
        assert!(PingView::try_new(&short).is_none());
        let exact = vec![0u8; size];
        let view = PingView::try_new(&exact).unwrap();
        assert_eq!(view.try_ping_id(), Some(0));
    }

    #[test]
    fn connect_view_try_new_rejects_short_buffer() {
        assert!(ConnectView::try_new(&[0u8; CONNECT_FIXED_SIZE - 1]).is_none());
        let view = ConnectView::try_new(&[0u8; CONNECT_FIXED_SIZE]).unwrap();
        assert_eq!(view.try_auth_token(), Some(&[][..]));
    }

    #[test]
    fn connected_view_try_new_rejects_short_buffer() {
        let size = core::mem::size_of::<ConnectedAction>();
        let short = vec![0u8; size - 1];
        assert!(ConnectedView::try_new(&short).is_none());
        let exact = vec![0u8; size];
        let view = ConnectedView::try_new(&exact).unwrap();
        assert_eq!(view.try_conn_id(), Some(0));
        assert_eq!(view.try_proto_version(), Some(0));
    }

    #[test]
    fn disconnect_view_try_new_rejects_short_buffer() {
        let size = core::mem::size_of::<DisconnectAction>();
        let short = vec![0u8; size - 1];
        assert!(DisconnectView::try_new(&short).is_none());
        let exact = vec![0u8; size];
        let view = DisconnectView::try_new(&exact).unwrap();
        assert_eq!(view.try_reason_code(), Some(0));
    }
}
