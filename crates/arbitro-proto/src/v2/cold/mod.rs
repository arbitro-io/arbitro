//! Cold-path frames — serde-encoded bodies for management ops.
//!
//! ## Why a separate module
//!
//! `v2::ingress`, `v2::egress`, `v2::store` carry hot-path frames: publish,
//! ack, deliver. They are zerocopy DSTs because they fire millions of times
//! per second per shard. A 64 B `AckFrame` decoded zero-copy in ~3 ns.
//!
//! Management ops — create stream, list consumers, pause/resume, etc. —
//! fire 1–100 times per second on the busiest broker. The zerocopy
//! constraint costs ergonomics there: every optional field needs a
//! reserved byte, every list needs a hand-rolled iterator, every wire
//! evolution needs a versioned struct. The cost trade is wrong for
//! cold path.
//!
//! This module wraps cold bodies in `Header { action, msg_len, seq } +
//! serde_json(body)`. The 16 B `Header` stays untouched — same dispatcher
//! routing, same per-connection sequencing. Only the body changes from
//! a `#[repr(C)]` DST to a JSON document.
//!
//! ## Why serde_json (not bincode / postcard)
//!
//! - `serde_json` is already a workspace dep — no new crate
//! - TS / Python / Go clients decode with their stdlib `JSON.parse`, no
//!   hand-rolled binary codec
//! - Frames are 100s of bytes at most; the ~10 µs encode/decode is
//!   invisible at 1–100 ops/s
//! - `tcpdump` / `strace` of these frames is human-readable
//!
//! Hot-path frames stay zerocopy. This split is permanent.
//!
//! ## Contract
//!
//! Any cold-path body type implements `ColdBody` (just a `serde` blanket
//! impl). `encode(seq)` produces `Bytes` ready for `try_send`. The
//! dispatcher feeds the post-Header slice into `decode_body`.

use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};

use crate::action::Action;
use crate::v2::header::{Header, HEADER_SIZE};

/// A cold-path frame body. Auto-implemented for any
/// `Serialize + DeserializeOwned` type via the blanket impl below.
pub trait ColdBody: Serialize + DeserializeOwned {
    /// Wire action this body decodes as. Dispatcher uses
    /// `header.action` to pick the right `T::decode_body`.
    const ACTION: Action;

    /// Encode `self` as `Header + serde_json(self)`. Bytes are ready
    /// for `try_send` on a `WriteFrame::Mono`.
    fn encode(&self, seq: u64) -> Bytes {
        let body = serde_json::to_vec(self).expect("ColdBody serialize");
        let mut buf = Vec::with_capacity(HEADER_SIZE + body.len());
        let header = Header::new(Self::ACTION.as_u16(), body.len() as u32, seq);
        buf.extend_from_slice(zerocopy::IntoBytes::as_bytes(&header));
        buf.extend_from_slice(&body);
        Bytes::from(buf)
    }

    /// Decode the body slice (post-Header). The dispatcher slices
    /// `&frame[HEADER_SIZE..]` and hands it here.
    #[inline]
    fn decode_body(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

// ── Per-action body types ────────────────────────────────────────────────

/// `PauseConsumer` body. Replaces the zerocopy `PauseConsumerBody`
/// (`consumer_id: u32 + _pad: u32`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PauseConsumer {
    pub consumer_id: u32,
}

impl ColdBody for PauseConsumer {
    const ACTION: Action = Action::PauseConsumer;
}

/// `ResumeConsumer` body. Same shape as `PauseConsumer`, different action.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResumeConsumer {
    pub consumer_id: u32,
}

impl ColdBody for ResumeConsumer {
    const ACTION: Action = Action::ResumeConsumer;
}

#[cfg(test)]
mod tests {
    use super::*;
    use zerocopy::FromBytes;

    #[test]
    fn pause_roundtrip() {
        let original = PauseConsumer { consumer_id: 42 };
        let bytes = original.encode(99);
        // Header parses
        let header = Header::ref_from_bytes(&bytes[..HEADER_SIZE]).unwrap();
        assert_eq!(header.action.get(), Action::PauseConsumer.as_u16());
        assert_eq!(header.seq.get(), 99);
        assert_eq!(header.msg_len.get() as usize, bytes.len() - HEADER_SIZE);
        // Body decodes
        let decoded = PauseConsumer::decode_body(&bytes[HEADER_SIZE..]).unwrap();
        assert_eq!(decoded.consumer_id, 42);
    }

    #[test]
    fn resume_roundtrip() {
        let original = ResumeConsumer { consumer_id: 7 };
        let bytes = original.encode(1);
        let header = Header::ref_from_bytes(&bytes[..HEADER_SIZE]).unwrap();
        assert_eq!(header.action.get(), Action::ResumeConsumer.as_u16());
        let decoded = ResumeConsumer::decode_body(&bytes[HEADER_SIZE..]).unwrap();
        assert_eq!(decoded.consumer_id, 7);
    }

    /// Cross-action bytes don't decode silently — JSON tolerates
    /// extra fields but not missing required ones, so we wrap that
    /// invariant in a test so a future evolution doesn't break it.
    #[test]
    fn missing_required_field_is_an_error() {
        let body = br#"{}"#;
        let err = PauseConsumer::decode_body(body).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("consumer_id"), "msg = {msg}");
    }
}
