//! v2 Header — 16 B, naturally aligned, little-endian.
//!
//! ```text
//! offset  field         size  notes
//! ──────  ────────────  ────  ─────────────────────────────────────────────
//!   0     action        u16   2  domain/variant discriminant (see action.rs)
//!   2     flags         u8    1  transport-level: ACK_REQ, DUP, PRIORITY_HIGH
//!   3     entry_flags   u8    1  per-message: RETAIN, COMPRESSED, NO_BACKPRESSURE
//!   4     msg_len       u32   4  body length (bytes after this header)
//!   8     seq           u64   8  domain-scoped monotonic sequence
//!                          ─────
//!                          16 B, align 8
//! ```
//!
//! ### Why no `version` field
//!
//! Protocol version is negotiated **once** in the connection handshake
//! (`HelloFrame` carries the magic + version bits). After the handshake
//! both ends know what version they're speaking — repeating that in every
//! one of millions of frames is pure tax. The byte that used to be
//! `version` is now `entry_flags`, available to every frame for free.
//!
//! ### Why `flags` and `entry_flags` are split
//!
//! - `flags` (offset 2) = **transport** semantics: how the broker routes
//!   the frame. Set by the publisher's wire layer.
//!   * bit 0  ACK_REQ        — publisher wants an explicit `RepOk`
//!   * bit 1  DUP            — duplicate-resend marker
//!   * bit 2  PRIORITY_HIGH  — bypass normal queue order
//!   * bits 3..7 reserved
//!
//! - `entry_flags` (offset 3) = **per-message** semantics: characteristics
//!   of the payload itself, persisted on disk with the entry.
//!   * bit 0  RETAIN         — last-value retention
//!   * bit 1  COMPRESSED     — payload is compressed
//!   * bit 2  NO_BACKPRESSURE — drop instead of block
//!   * bits 3..7 reserved
//!
//! Splitting them means the broker can ack-route on `flags` without
//! looking at the body, while consumers see `entry_flags` as part of the
//! delivered record.
//!
//! Alignment guarantees (when the enclosing buffer is 8-byte aligned):
//!   * `msg_len` at offset 4 → 4-byte aligned ✓
//!   * `seq`     at offset 8 → 8-byte aligned ✓
//!   * `size_of::<Header>() == 16` is a multiple of 8 → arrays stay aligned.

use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct Header {
    pub action: U16,
    pub flags: u8,
    pub entry_flags: u8,
    pub msg_len: U32,
    pub seq: U64,
}

pub const HEADER_SIZE: usize = 16;

/// Protocol version negotiated in the `HelloFrame` handshake.
/// Not stored per-frame.
pub const CURRENT_VERSION: u8 = 2;

const _: () = assert!(core::mem::size_of::<Header>() == HEADER_SIZE);

impl Header {
    #[inline(always)]
    pub fn new(action: u16, msg_len: u32, seq: u64) -> Self {
        Self {
            action: U16::new(action),
            flags: 0,
            entry_flags: 0,
            msg_len: U32::new(msg_len),
            seq: U64::new(seq),
        }
    }

    #[inline(always)]
    pub fn with_flags(mut self, flags: u8) -> Self {
        self.flags = flags;
        self
    }

    #[inline(always)]
    pub fn with_entry_flags(mut self, ef: u8) -> Self {
        self.entry_flags = ef;
        self
    }

    /// Total bytes this frame occupies on wire, including the header itself.
    #[inline(always)]
    pub fn total_len(&self) -> usize {
        HEADER_SIZE + self.msg_len.get() as usize
    }
}

// ── Transport flags (offset 2) ───────────────────────────────────────────
//
// TODO §5: every bit other than ACK_REQ is reserved for forward-compatible
// extensions. The broker IGNORES DUP / PRIORITY_HIGH today — they are
// kept to lock in the bit positions so a future feature can claim them
// without renegotiating the wire layout. The header tests reference these
// constants; do not delete them without renumbering reserved bits first.
pub mod flag {
    pub const ACK_REQ: u8 = 1 << 0;
    /// Reserved (TODO §5). Set by clients that detect retransmission;
    /// the broker does NOT use this bit for deduplication today.
    pub const DUP: u8 = 1 << 1;
    /// Reserved (TODO §5). No priority queue exists yet — the broker
    /// treats high-priority publishes identically to normal ones.
    pub const PRIORITY_HIGH: u8 = 1 << 2;
    // bits 3..7 reserved
}

// ── Per-message flags (offset 3, formerly `version`) ─────────────────────
//
// TODO §5: the broker honours none of these bits today. They are kept as
// reserved markers so a future release can wire them up without breaking
// existing clients that already happen to set them.
pub mod entry_flag {
    /// Reserved (TODO §5). No retain semantics implemented.
    pub const RETAIN: u8 = 1 << 0;
    /// Reserved (TODO §5). Broker never decompresses on the read path —
    /// payloads are stored exactly as received.
    pub const COMPRESSED: u8 = 1 << 1;
    /// Reserved (TODO §5). Broker has only one backpressure policy
    /// today (drop on per-conn mpsc full); this bit is not consulted.
    pub const NO_BACKPRESSURE: u8 = 1 << 2;
    // bits 3..7 reserved
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_16() {
        assert_eq!(core::mem::size_of::<Header>(), 16);
    }

    #[test]
    fn field_offsets_natural() {
        let h = Header::new(0xBEEF, 0x01020304, 0x0A0B0C0D_0E0F_1011);
        let bytes = h.as_bytes();

        assert_eq!(&bytes[0..2], &0xBEEFu16.to_le_bytes());
        assert_eq!(bytes[2], 0); // flags
        assert_eq!(bytes[3], 0); // entry_flags
        assert_eq!(&bytes[4..8], &0x01020304u32.to_le_bytes());
        assert_eq!(&bytes[8..16], &0x0A0B0C0D_0E0F_1011u64.to_le_bytes());
    }

    #[test]
    fn roundtrip_ref_from_bytes() {
        let h = Header::new(0x0101, 256, 42);
        let bytes = h.as_bytes().to_vec();
        let parsed = Header::ref_from_bytes(&bytes[..]).unwrap();
        assert_eq!(parsed.action.get(), 0x0101);
        assert_eq!(parsed.msg_len.get(), 256);
        assert_eq!(parsed.seq.get(), 42);
        assert_eq!(parsed.total_len(), 16 + 256);
    }

    #[test]
    fn flags_and_entry_flags_independent() {
        let h = Header::new(0, 0, 0)
            .with_flags(flag::ACK_REQ | flag::PRIORITY_HIGH)
            .with_entry_flags(entry_flag::RETAIN | entry_flag::COMPRESSED);
        assert_eq!(h.flags, 0b0000_0101);
        assert_eq!(h.entry_flags, 0b0000_0011);
    }
}
