//! Transport trait — how the engine sends frames to connections.
//!
//! The engine never touches TCP. It calls `transport.send(conn_id, &[u8])`.
//! The server provides a real implementation; tests use NoopTransport.

use bytes::Bytes;

use arbitro_proto::ids::ConnId;

/// Outbound transport contract.
///
/// `send` pushes a contiguous frame. `send_bytes` pushes an owned `Bytes`
/// (zero-copy — no `Bytes::copy_from_slice`). `send_parts` pushes scatter
/// slices (envelope + body parts) without concatenating.
/// All must be non-blocking — the actual TCP write happens in the server's write loop.
pub trait Transport: Send + Sync {
    // ── Lifecycle ────────────────────────────────────────────────────

    /// Initialize — start listener, allocate buffers.
    /// Called once before any sends. Default: no-op.
    fn init(&self) {}

    /// Graceful shutdown — close all connections, stop accepting.
    /// Called once on engine shutdown. Default: no-op.
    fn shutdown(&self) {}

    // ── Hot path ─────────────────────────────────────────────────────

    /// Send a contiguous frame to a connection. Returns false if conn is gone.
    fn send(&self, conn_id: ConnId, data: &[u8]) -> bool;

    /// Send an owned Bytes frame — zero-copy hot path.
    /// Default delegates to `send(&data)`. Override in real transports
    /// to avoid Bytes::copy_from_slice.
    fn send_bytes(&self, conn_id: ConnId, data: Bytes) -> bool {
        self.send(conn_id, &data)
    }

    /// Send multiple parts as a single logical frame — no concatenation.
    /// Default implementation builds BytesMut, freezes, sends via send_bytes.
    fn send_parts(&self, conn_id: ConnId, parts: &[&[u8]]) -> bool {
        use bytes::BytesMut;
        let total: usize = parts.iter().map(|p| p.len()).sum();
        let mut buf = BytesMut::with_capacity(total);
        for part in parts {
            buf.extend_from_slice(part);
        }
        self.send_bytes(conn_id, buf.freeze())
    }

    /// Close a single connection.
    fn close(&self, conn_id: ConnId);
}

/// Blanket impl so `Arc<T>` can be used wherever `Transport` is expected.
impl<T: Transport> Transport for std::sync::Arc<T> {
    fn init(&self) { (**self).init() }
    fn shutdown(&self) { (**self).shutdown() }

    #[inline]
    fn send(&self, conn_id: ConnId, data: &[u8]) -> bool {
        (**self).send(conn_id, data)
    }

    #[inline]
    fn send_bytes(&self, conn_id: ConnId, data: Bytes) -> bool {
        (**self).send_bytes(conn_id, data)
    }

    #[inline]
    fn send_parts(&self, conn_id: ConnId, parts: &[&[u8]]) -> bool {
        (**self).send_parts(conn_id, parts)
    }

    fn close(&self, conn_id: ConnId) { (**self).close(conn_id) }
}

/// No-op transport for testing and benchmarks.
pub struct NoopTransport;

impl Transport for NoopTransport {
    #[inline(always)]
    fn send(&self, _conn_id: ConnId, _data: &[u8]) -> bool {
        true
    }

    #[inline(always)]
    fn send_bytes(&self, _conn_id: ConnId, _data: Bytes) -> bool {
        true
    }

    #[inline(always)]
    fn send_parts(&self, _conn_id: ConnId, _parts: &[&[u8]]) -> bool {
        true
    }

    #[inline(always)]
    fn close(&self, _conn_id: ConnId) {}
}
