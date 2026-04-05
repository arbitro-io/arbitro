//! Transport trait — how the engine sends frames to connections.
//!
//! The engine never touches TCP. It calls `transport.send(conn_id, &[u8])`.
//! The server provides a real implementation; tests use NoopTransport.

use arbitro_proto::ids::ConnId;

/// Outbound transport contract.
///
/// `send` pushes a contiguous frame. `send_parts` pushes scatter slices
/// (envelope + body parts) without concatenating — zero extra copies.
/// Both must be non-blocking — the actual TCP write happens in the server's write loop.
pub trait Transport: Send + Sync {
    // ── Lifecycle ────────────────────────────────────────────────────

    /// Initialize — start listener, allocate buffers.
    /// Called once before any sends. Default: no-op.
    fn init(&self) {}

    /// Graceful shutdown — close all connections, stop accepting.
    /// Called once on engine shutdown. Default: no-op.
    fn shutdown(&self) {}

    // ── Hot path ─────────────────────────────────��──────────────────

    /// Send a contiguous frame to a connection. Returns false if conn is gone.
    fn send(&self, conn_id: ConnId, data: &[u8]) -> bool;

    /// Send multiple parts as a single logical frame — no concatenation.
    /// Default implementation falls back to `send` with a temporary buffer.
    fn send_parts(&self, conn_id: ConnId, parts: &[&[u8]]) -> bool {
        let total: usize = parts.iter().map(|p| p.len()).sum();
        let mut buf = Vec::with_capacity(total);
        for part in parts {
            buf.extend_from_slice(part);
        }
        self.send(conn_id, &buf)
    }

    /// Close a single connection.
    fn close(&self, conn_id: ConnId);
}

/// No-op transport for testing and benchmarks.
pub struct NoopTransport;

impl Transport for NoopTransport {
    #[inline(always)]
    fn send(&self, _conn_id: ConnId, _data: &[u8]) -> bool {
        true
    }

    #[inline(always)]
    fn send_parts(&self, _conn_id: ConnId, _parts: &[&[u8]]) -> bool {
        true
    }

    #[inline(always)]
    fn close(&self, _conn_id: ConnId) {}
}
