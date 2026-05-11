//! Connection handshake — the only frame in v2 that does NOT start with
//! the 16-byte `Header`. Sent as the very first 8 bytes of every TCP
//! connection (in both directions: client→server and server→client).
//!
//! Wire layout:
//! ```text
//! offset  field    size  value
//! ──────  ───────  ────  ──────────────────────────────────────────────
//!   0     magic    u32   = ARBITRO_MAGIC_V2 ("ARB2", 0x32425241 LE)
//!   4     version  u8    = CURRENT_VERSION (2 today)
//!   5     role     u8    = 0 client, 1 server
//!   6     caps     u16   bitfield (capability flags)
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
//! Once both sides exchange `HelloFrame`s and validate the magic, the
//! rest of the connection is pure `Header`-prefixed v2 frames.
//!
//! ### Capabilities
//!
//! `caps` is forward-compatible: unknown bits are ignored. Both sides
//! send what they support, the effective set is the bitwise AND.

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::magic::ARBITRO_MAGIC_V2;
use crate::v2::header::CURRENT_VERSION;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct HelloFrame {
    pub magic:   U32,
    pub version: u8,
    pub role:    u8,
    pub caps:    U16,
}

pub const HELLO_FRAME_SIZE: usize = core::mem::size_of::<HelloFrame>();
const _: () = assert!(HELLO_FRAME_SIZE == 8);

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Client = 0,
    Server = 1,
}

pub mod cap {
    pub const HEADERS:           u16 = 1 << 0; // PublishWithHeaders supported
    pub const REPLY:             u16 = 1 << 1; // PublishWithReply supported
    pub const BATCH_HEADERS:     u16 = 1 << 2; // PublishBatchWithHeaders supported
    pub const COMPRESSED_PAYLOAD:u16 = 1 << 3; // entry_flag::COMPRESSED honored
    // bits 4..15 reserved
}

impl HelloFrame {
    /// Build a Hello with the current protocol version and given role/caps.
    #[inline(always)]
    pub fn new(role: Role, caps: u16) -> Self {
        Self {
            magic:   U32::new(ARBITRO_MAGIC_V2),
            version: CURRENT_VERSION,
            role:    role as u8,
            caps:    U16::new(caps),
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
        let h = HelloFrame::new(Role::Client, cap::HEADERS | cap::REPLY);
        let bytes = h.as_bytes();
        assert_eq!(bytes.len(), 8);
        let parsed = HelloFrame::parse(bytes).expect("magic ok");
        assert_eq!(parsed.version, CURRENT_VERSION);
        assert_eq!(parsed.role, Role::Client as u8);
        assert_eq!(parsed.caps.get(), 0b0000_0011);
    }

    #[test]
    fn rejects_wrong_magic() {
        let mut bad = [0u8; 8];
        bad[..4].copy_from_slice(b"HTTP");
        assert!(HelloFrame::parse(&bad).is_none());
    }

    #[test]
    fn magic_bytes_readable() {
        let h = HelloFrame::new(Role::Server, 0);
        let bytes = h.as_bytes();
        assert_eq!(&bytes[0..4], b"ARB2");
    }
}
