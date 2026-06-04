//! Connection handshake — the only frame in v2 that does NOT start with
//! the 16-byte `Header`. Sent as the very first 8 bytes of every TCP
//! connection.
//!
//! Wire layout:
//! ```text
//! offset  field    size  value
//! ──────  ───────  ────  ──────────────────────────────────────────────
//!   0     magic    u32   = ARBITRO_MAGIC_V2 ("ARB2", 0x32425241 LE)
//!   4     version  u8    = CURRENT_VERSION (2 today)
//!   5     role     u8    = 0 client, 1 server
//!   6     _pad     u16   = 0 (reserved — see "Capabilities" below)
//!                      ─────
//!                      8 B
//! ```
//!
//! ### Why magic only here, not per-frame
//!
//! TCP gives us byte-stream integrity (checksum + sequence numbers).
//! Adding 4 bytes of magic to every frame would tax the hot path
//! permanently for zero new safety. Magic on the *first* frame catches:
//!   * clients connecting to the wrong port (HTTP/junk)
//!   * v1 clients hitting a v2 broker
//!   * port scanners sending garbage
//!
//! Once the magic is validated the rest of the connection is pure
//! `Header`-prefixed v2 frames.
//!
//! ### Capabilities (M9 — pruned)
//!
//! Earlier versions had a `caps: u16` bitfield where the client
//! announced features it supported (`HEADERS`, `REPLY`,
//! `BATCH_HEADERS`, `COMPRESSED_PAYLOAD`). The server never read those
//! bytes — they were wire bytes that lied.
//!
//! The honest pattern for a broker is **server-announced capabilities**
//! (cf. NATS `INFO`, MQTT 5 `CONNACK`, Kafka `ApiVersionsResponse`):
//! the server tells the client what it supports, the client adapts.
//! Until arbitro has that reply frame, the slot stays `_pad` — clients
//! must write `0`, server ignores. Any future feature negotiation will
//! land in a dedicated `HelloAck`/`Welcome` frame, not by overloading
//! these bits.

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::CURRENT_VERSION;
use crate::v2::magic::ARBITRO_MAGIC_V2;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct HelloFrame {
    pub magic: U32,
    pub version: u8,
    pub role: u8,
    /// Reserved (M9 — was `caps: u16`, removed). Must be 0. Reserved
    /// for a future `HelloAck` negotiation; see module docs.
    pub _pad: U16,
}

pub const HELLO_FRAME_SIZE: usize = core::mem::size_of::<HelloFrame>();
const _: () = assert!(HELLO_FRAME_SIZE == 8);

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Client = 0,
    Server = 1,
}

impl HelloFrame {
    /// Build a Hello with the current protocol version and given role.
    #[inline(always)]
    pub fn new(role: Role) -> Self {
        Self {
            magic: U32::new(ARBITRO_MAGIC_V2),
            version: CURRENT_VERSION,
            role: role as u8,
            _pad: U16::new(0),
        }
    }

    /// Parse from the first 8 bytes of a TCP connection.
    /// Returns `None` if the magic is wrong (not an arbitro v2 client).
    #[inline(always)]
    pub fn parse(buf: &[u8]) -> Option<&Self> {
        if buf.len() < HELLO_FRAME_SIZE {
            return None;
        }
        let f = HelloFrame::ref_from_bytes(&buf[..HELLO_FRAME_SIZE]).ok()?;
        if f.magic.get() != ARBITRO_MAGIC_V2 {
            return None;
        }
        Some(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_8() {
        assert_eq!(HELLO_FRAME_SIZE, 8);
    }

    #[test]
    fn roundtrip() {
        let h = HelloFrame::new(Role::Client);
        let bytes = h.as_bytes();
        assert_eq!(bytes.len(), 8);
        let parsed = HelloFrame::parse(bytes).expect("magic ok");
        assert_eq!(parsed.version, CURRENT_VERSION);
        assert_eq!(parsed.role, Role::Client as u8);
        assert_eq!(parsed._pad.get(), 0);
    }

    #[test]
    fn rejects_wrong_magic() {
        let mut bad = [0u8; 8];
        bad[..4].copy_from_slice(b"HTTP");
        assert!(HelloFrame::parse(&bad).is_none());
    }

    #[test]
    fn magic_bytes_readable() {
        let h = HelloFrame::new(Role::Server);
        let bytes = h.as_bytes();
        assert_eq!(&bytes[0..4], b"ARB2");
    }
}
