//! Stream events for asynchronous broadcast.
//!
//! Metadata-only to ensure zero-allocation during propagation.

use bytes::Bytes;

/// Events emitted by a stream.
#[derive(Clone, Debug)]
pub enum StreamEvent {
    /// New message(s) successfully appended to the journal.
    MessagePublished {
        stream_id: u32,
        first_seq: u64,
        count: u16,
    },
    /// Stream metadata or config changed.
    ConfigChanged {
        stream_id: u32,
    },
    /// Stream was purged or messages deleted.
    StreamPurged {
        stream_id: u32,
        count: u64,
    },
    /// A custom event with binary payload.
    Custom {
        stream_id: u32,
        subject: Bytes,
        payload: Bytes,
    }
}
