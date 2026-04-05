//! Auth trait — cold path, checked once on connect.
//!
//! The engine calls `auth.check()` during the Connect handshake.
//! Hot path (publish/deliver) never touches auth.

use arbitro_proto::ids::ConnId;

/// Authentication contract.
pub trait Auth: Send + Sync {
    /// Initialize — load keys, connect to auth provider.
    /// Default: no-op.
    fn init(&self) {}

    /// Graceful shutdown — close connections to auth provider.
    /// Default: no-op.
    fn shutdown(&self) {}

    /// Check credentials on connect. Returns true if allowed.
    fn check(&self, conn_id: ConnId, token: &[u8]) -> bool;
}

/// Allow all connections — default for development/testing.
pub struct AllowAll;

impl Auth for AllowAll {
    #[inline(always)]
    fn check(&self, _conn_id: ConnId, _token: &[u8]) -> bool {
        true
    }
}
