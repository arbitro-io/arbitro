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
//! ## Adding a new cold frame
//!
//! Use the [`cold_body!`] macro. One line per type:
//!
//! ```ignore
//! cold_body! {
//!     Action::MyNewAction => pub struct MyNewBody {
//!         pub field_a: u32,
//!         pub field_b: Vec<u8>,
//!     },
//! }
//! ```
//!
//! This expands to: the struct with `#[derive(Serialize, Deserialize)]`,
//! and an `impl ColdBody` wiring the action. Nothing else.

use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};

use crate::action::Action;
use crate::v2::header::{Header, HEADER_SIZE};

/// A cold-path frame body. The `encode` method wraps `self` in a v2
/// Header and serializes the body as JSON; `decode_body` parses the
/// reverse from the post-Header slice.
pub trait ColdBody: Serialize + DeserializeOwned {
    /// Wire action this body decodes as. The dispatcher uses
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

/// Declare one or more cold-path body types in a single block.
///
/// Each entry produces a `#[derive(Serialize, Deserialize)]` struct
/// plus an `impl ColdBody` wired to the given `Action`. All fields are
/// public; the struct is `Debug + Clone`.
///
/// Example:
/// ```ignore
/// cold_body! {
///     Action::DeleteConsumer => pub struct DeleteConsumer { pub consumer_id: u32 },
///     Action::DeleteStream   => pub struct DeleteStream   { pub name: Vec<u8> },
/// }
/// ```
#[macro_export]
macro_rules! cold_body {
    (
        $(
            $action:expr => pub struct $name:ident {
                $( pub $field:ident : $ty:ty ),* $(,)?
            }
        ),+ $(,)?
    ) => {
        $(
            #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
            pub struct $name {
                $( pub $field: $ty, )*
            }

            impl $crate::v2::cold::ColdBody for $name {
                const ACTION: $crate::action::Action = $action;
            }
        )+
    };
}

// ── Cold body types ──────────────────────────────────────────────────────
//
// Every management frame whose body is a plain struct lives here. Add new
// ones inside the single `cold_body!` block — keeps the wire surface and
// the action table in one place.

cold_body! {
    // ── Consumer lifecycle (Pause/Resume/Delete/Unsubscribe) ─────────
    //
    // Same shape — `consumer_id: u32`. The action discriminates the
    // semantics on the broker side. `Unsubscribe` lives in
    // `Action::Unsubscribe` (subscription family) but has identical
    // body, so we share the structure with a per-action newtype to
    // keep the dispatcher's match arms unambiguous.
    Action::PauseConsumer  => pub struct PauseConsumer  { pub consumer_id: u32 },
    Action::ResumeConsumer => pub struct ResumeConsumer { pub consumer_id: u32 },
    Action::DeleteConsumer => pub struct DeleteConsumer { pub consumer_id: u32 },
    Action::Unsubscribe    => pub struct Unsubscribe    { pub consumer_id: u32 },

    // ── Stream lookup / lifecycle (by name) ──────────────────────────
    //
    // `name` carries the wire-side stream name (UTF-8 or arbitrary
    // bytes). `Vec<u8>` keeps the API agnostic to encoding.
    Action::DeleteStream => pub struct DeleteStream { pub name: Vec<u8> },
    Action::GetStream    => pub struct GetStream    { pub name: Vec<u8> },
    Action::PurgeStream  => pub struct PurgeStream  { pub name: Vec<u8> },

    // ── Consumer lookup (by (stream_id, name)) ───────────────────────
    Action::GetConsumer => pub struct GetConsumer {
        pub stream_id: u32,
        pub name:      Vec<u8>,
    },

    // ── DrainSubject (stream name + subject pattern) ─────────────────
    Action::DrainSubject => pub struct DrainSubject {
        pub name:    Vec<u8>,
        pub subject: Vec<u8>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use zerocopy::FromBytes;

    /// Generic header invariant: every cold body roundtrips through
    /// `encode(seq) → Header parse → decode_body`. Done for one
    /// representative; the macro guarantees the rest follow.
    #[test]
    fn pause_roundtrip() {
        let original = PauseConsumer { consumer_id: 42 };
        let bytes = original.encode(99);
        let header = Header::ref_from_bytes(&bytes[..HEADER_SIZE]).unwrap();
        assert_eq!(header.action.get(), Action::PauseConsumer.as_u16());
        assert_eq!(header.seq.get(), 99);
        assert_eq!(header.msg_len.get() as usize, bytes.len() - HEADER_SIZE);
        let decoded = PauseConsumer::decode_body(&bytes[HEADER_SIZE..]).unwrap();
        assert_eq!(decoded.consumer_id, 42);
    }

    /// Tail-bearing body — confirms `Vec<u8>` survives encode/decode
    /// and the Header carries the correct action discriminator.
    #[test]
    fn drain_subject_roundtrip() {
        let original = DrainSubject {
            name:    b"orders".to_vec(),
            subject: b"orders.eu.*".to_vec(),
        };
        let bytes = original.encode(1);
        let header = Header::ref_from_bytes(&bytes[..HEADER_SIZE]).unwrap();
        assert_eq!(header.action.get(), Action::DrainSubject.as_u16());
        let decoded = DrainSubject::decode_body(&bytes[HEADER_SIZE..]).unwrap();
        assert_eq!(decoded.name, b"orders");
        assert_eq!(decoded.subject, b"orders.eu.*");
    }

    /// Missing required fields surface as decode errors — guards
    /// against silently accepting partial bodies after a wire change.
    #[test]
    fn missing_required_field_is_an_error() {
        let body = br#"{}"#;
        let err = PauseConsumer::decode_body(body).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("consumer_id"), "msg = {msg}");
    }
}
