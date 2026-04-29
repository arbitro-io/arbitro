//! `WriteFrame` — work item for the single-writer task.
//!
//! Three variants cover every outbound shape:
//! - `Mono`        : pre-encoded `Bytes` (admin / ack / sub / hello).
//! - `PubSingle`   : 24B prefix (Header+PubBody) + subject + payload — 3 iovecs.
//! - `PubBatch`    : single contiguous `Bytes` (one iovec).

use bytes::Bytes;

#[derive(Debug)]
pub(crate) enum WriteFrame {
    /// Pre-encoded single-iovec frame.
    Mono(Bytes),
    /// v2 single PUB: prefix(24B) + subject + payload (3 iovecs, zero payload memcpy).
    PubSingle {
        prefix:  Bytes,
        subject: Bytes,
        payload: Bytes,
    },
    /// v2 batch PUB: contiguous wire image (1 iovec).
    PubBatch(Bytes),
}
