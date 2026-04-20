//! Zerocopy metadata commands — raft-compatible, zero serde.
//!
//! Every metadata mutation (create/delete stream/consumer) is encoded as raw bytes:
//!
//! ```text
//! [1 command_type][body...]
//! ```
//!
//! The body is the **same wire bytes** as the TCP frame body for that action.
//! This means `StateMachine::apply(&[u8])` from arbitro-raft receives bytes
//! that can be parsed with the existing zerocopy views (CreateStreamView, etc.)
//! with zero copies, zero serde, zero allocations.
//!
//! ## Command types
//!
//! | Code | Action | Body |
//! |------|--------|------|
//! | 0x01 | CreateStream | CreateStreamFixed + variable (name + filter) |
//! | 0x02 | DeleteStream | DeleteStreamFixed + variable (name) |
//! | 0x03 | CreateConsumer | CreateConsumerFixed + variable (name + group + subject + limits) |
//! | 0x04 | DeleteConsumer | DeleteConsumerAction (8B fixed) |

/// Command type discriminators.
pub const CMD_CREATE_STREAM: u8 = 0x01;
pub const CMD_DELETE_STREAM: u8 = 0x02;
pub const CMD_CREATE_CONSUMER: u8 = 0x03;
pub const CMD_DELETE_CONSUMER: u8 = 0x04;

/// Zero-copy view over a metadata command buffer.
///
/// The buffer layout is `[1 command_type][body...]`.
/// Body is identical to the wire frame body for that action.
pub struct MetadataCommandView<'a> {
    buf: &'a [u8],
}

impl<'a> MetadataCommandView<'a> {
    /// Wrap a raw byte slice. Returns `None` if buffer is empty.
    #[inline]
    pub fn new(buf: &'a [u8]) -> Option<Self> {
        if buf.is_empty() {
            return None;
        }
        Some(Self { buf })
    }

    /// The command type byte.
    #[inline]
    pub fn command_type(&self) -> u8 {
        self.buf[0]
    }

    /// The body bytes (everything after the 1-byte discriminator).
    /// This is the exact wire frame body — pass directly to Views.
    #[inline]
    pub fn body(&self) -> &'a [u8] {
        &self.buf[1..]
    }

    /// Total length of the command (1 + body).
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the command is empty (should never be true after new()).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The full raw bytes (type + body).
    #[inline]
    pub fn as_bytes(&self) -> &'a [u8] {
        self.buf
    }
}

// ── Builder helpers ────────────────────────────────────────────────────────

/// Build a metadata command by prepending the type byte to existing wire body bytes.
///
/// This is the only allocation in the metadata path — one Vec per management op.
/// Called on cold path only (create/delete stream/consumer).
#[inline]
pub fn build_command(command_type: u8, wire_body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + wire_body.len());
    buf.push(command_type);
    buf.extend_from_slice(wire_body);
    buf
}

/// Build a CreateStream metadata command from wire body bytes.
#[inline]
pub fn build_create_stream(wire_body: &[u8]) -> Vec<u8> {
    build_command(CMD_CREATE_STREAM, wire_body)
}

/// Build a DeleteStream metadata command from wire body bytes.
#[inline]
pub fn build_delete_stream(wire_body: &[u8]) -> Vec<u8> {
    build_command(CMD_DELETE_STREAM, wire_body)
}

/// Build a CreateConsumer metadata command from wire body bytes.
#[inline]
pub fn build_create_consumer(wire_body: &[u8]) -> Vec<u8> {
    build_command(CMD_CREATE_CONSUMER, wire_body)
}

/// Build a DeleteConsumer metadata command from wire body bytes.
#[inline]
pub fn build_delete_consumer(wire_body: &[u8]) -> Vec<u8> {
    build_command(CMD_DELETE_CONSUMER, wire_body)
}

// ── Trait — compatible with StateMachine::apply(&[u8]) ─────────────────────

/// Trait for applying raw metadata commands.
///
/// Compatible with arbitro-raft's `StateMachine::apply(&[u8])`.
/// The default file-based implementation appends raw bytes to a log file.
/// A raft implementation passes the same bytes through `LogEntry.payload`.
pub trait MetadataApplier {
    /// Apply a raw metadata command. The bytes are `[1 type][body...]`.
    fn apply(&mut self, command: &[u8]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::manager::{
        CreateConsumerFixed, CreateConsumerView, DeleteConsumerAction, DeleteConsumerView,
    };
    use crate::wire::stream::{CreateStreamFixed, CreateStreamView, DeleteStreamView};
    use zerocopy::byteorder::little_endian::{U16, U32, U64};
    use zerocopy::IntoBytes;

    #[test]
    fn roundtrip_create_stream() {
        // Build wire body: CreateStreamFixed + name + filter
        let fixed = CreateStreamFixed {
            name_len: U16::new(6),
            filter_len: U16::new(8),
            max_msgs: U64::new(1_000_000),
            max_bytes: U64::new(1 << 30),
            max_age_secs: U64::new(86400),
            replicas: 1,
            journal_kind: 0,
            retention: 0,
            discard: 0,
        };
        let mut body = Vec::new();
        body.extend_from_slice(fixed.as_bytes());
        body.extend_from_slice(b"orders");
        body.extend_from_slice(b"orders.>");

        let cmd = build_create_stream(&body);
        let view = MetadataCommandView::new(&cmd).unwrap();

        assert_eq!(view.command_type(), CMD_CREATE_STREAM);
        let stream_view = CreateStreamView::new(view.body());
        assert_eq!(stream_view.name(), b"orders");
        assert_eq!(stream_view.filter(), b"orders.>");
        assert_eq!(stream_view.max_msgs(), 1_000_000);
        assert_eq!(stream_view.replicas(), 1);
    }

    #[test]
    fn roundtrip_delete_stream() {
        let fixed = crate::wire::stream::DeleteStreamFixed {
            name_len: U16::new(6),
            _pad: [0; 6],
        };
        let mut body = Vec::new();
        body.extend_from_slice(fixed.as_bytes());
        body.extend_from_slice(b"orders");

        let cmd = build_delete_stream(&body);
        let view = MetadataCommandView::new(&cmd).unwrap();

        assert_eq!(view.command_type(), CMD_DELETE_STREAM);
        let del_view = DeleteStreamView::new(view.body());
        assert_eq!(del_view.name(), b"orders");
    }

    #[test]
    fn roundtrip_create_consumer() {
        let fixed = CreateConsumerFixed {
            name_len: U16::new(7),
            subj_len: U16::new(8),
            stream_id: U32::new(42),
            max_inflight: U16::new(100),
            ack_policy: 1,
            deliver_policy: 0,
            deliver_mode: 0,
            discard: 0,
            group_len: U16::new(0),
            ack_wait_ms: U32::new(30000),
            start_seq: U64::new(0),
        };
        let mut body = Vec::new();
        body.extend_from_slice(fixed.as_bytes());
        body.extend_from_slice(b"worker1"); // name
                                            // group_len=0 → no group bytes
        body.extend_from_slice(b"orders.>"); // subject

        let cmd = build_create_consumer(&body);
        let view = MetadataCommandView::new(&cmd).unwrap();

        assert_eq!(view.command_type(), CMD_CREATE_CONSUMER);
        let cv = CreateConsumerView::new(view.body());
        assert_eq!(cv.stream_id(), 42);
        assert_eq!(cv.name(), b"worker1");
        assert_eq!(cv.subject(), b"orders.>");
        assert_eq!(cv.max_inflight(), 100);
    }

    #[test]
    fn roundtrip_delete_consumer() {
        let fixed = DeleteConsumerAction {
            consumer_id: U32::new(99),
            _pad: U32::new(0),
        };
        let cmd = build_delete_consumer(fixed.as_bytes());
        let view = MetadataCommandView::new(&cmd).unwrap();

        assert_eq!(view.command_type(), CMD_DELETE_CONSUMER);
        let dv = DeleteConsumerView::new(view.body());
        assert_eq!(dv.consumer_id(), 99);
    }

    #[test]
    fn empty_buffer_returns_none() {
        assert!(MetadataCommandView::new(&[]).is_none());
    }
}
