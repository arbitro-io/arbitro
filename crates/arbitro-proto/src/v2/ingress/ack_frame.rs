//! Ingress ACK / BATCH_ACK frames.
//!
//! ACK body (16 B, no tail):
//! ```text
//!   offset 0:  consumer_id   u32  (4B)
//!   offset 4:  subject_hash  u32  (4B)   ← echoed from DeliveryEntry, O(1) credit release
//!   offset 8:  ack_seq       u64  (8B)   ← sequence being acknowledged
//! ```
//! (The frame's own `header.seq` is the per-connection ack frame counter.)
//!
//! BATCH_ACK body (fixed 8 B + N × 16 B entries):
//! ```text
//!   offset 0:  consumer_id   u32  (4B)
//!   offset 4:  count         u32  (4B)   ← number of entries that follow
//!   entries[..]: [seq u64][subject_hash u32][_pad u32]  (16 B each)
//! ```

use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

// ── Single ACK ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct AckBody {
    pub consumer_id: U32,
    pub subject_hash: U32,
    pub ack_seq: U64,
}

pub const ACK_BODY_SIZE: usize = core::mem::size_of::<AckBody>();
const _: () = assert!(ACK_BODY_SIZE == 16);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct AckFrame {
    pub header: Header,
    pub body: AckBody,
}

const _: () = assert!(core::mem::size_of::<AckFrame>() == HEADER_SIZE + ACK_BODY_SIZE);

impl AckFrame {
    pub const WIRE_SIZE: usize = HEADER_SIZE + ACK_BODY_SIZE;

    #[inline(always)]
    pub fn new(seq: u64, consumer_id: u32, ack_seq: u64, subject_hash: u32) -> Self {
        Self {
            header: Header::new(
                crate::action::Action::Ack.as_u16(),
                ACK_BODY_SIZE as u32,
                seq,
            ),
            body: AckBody {
                consumer_id: U32::new(consumer_id),
                subject_hash: U32::new(subject_hash),
                ack_seq: U64::new(ack_seq),
            },
        }
    }
}

// ── Batch ACK ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct BatchAckBody {
    pub consumer_id: U32,
    pub count: U32,
}
pub const BATCH_ACK_BODY_FIXED: usize = core::mem::size_of::<BatchAckBody>();
const _: () = assert!(BATCH_ACK_BODY_FIXED == 8);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct BatchAckEntry {
    pub seq: U64,
    pub subject_hash: U32,
    pub _pad: U32,
}
pub const BATCH_ACK_ENTRY_SIZE: usize = core::mem::size_of::<BatchAckEntry>();
const _: () = assert!(BATCH_ACK_ENTRY_SIZE == 16);

/// DST frame: `Header + BatchAckBody + entries[count]`.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct BatchAckFrame {
    pub header: Header,
    pub body: BatchAckBody,
    pub tail: [u8], // exactly count * BATCH_ACK_ENTRY_SIZE bytes
}

impl BatchAckFrame {
    #[inline(always)]
    pub const fn wire_size(count: usize) -> usize {
        HEADER_SIZE + BATCH_ACK_BODY_FIXED + count * BATCH_ACK_ENTRY_SIZE
    }

    /// Typed slice view over the entries — **panics** on a lying `count`.
    /// Hot-path callers MUST validate via `try_entries()` first.
    #[inline(always)]
    pub fn entries(&self) -> &[BatchAckEntry] {
        // Safe: tail is exactly count * 16 bytes, BatchAckEntry is align-1.
        let n = self.body.count.get() as usize;
        <[BatchAckEntry]>::ref_from_bytes(&self.tail[..n * BATCH_ACK_ENTRY_SIZE])
            .expect("BatchAckEntry layout")
    }

    /// **B2 safety**: same as `entries()` but returns `None` if `count`
    /// doesn't match the tail length. Dispatchers parsing untrusted
    /// network frames must use this — the panic-on-lying-count path is
    /// remote-trigerrable.
    #[inline]
    pub fn try_entries(&self) -> Option<&[BatchAckEntry]> {
        let n = self.body.count.get() as usize;
        let bytes = n.checked_mul(BATCH_ACK_ENTRY_SIZE)?;
        if bytes > self.tail.len() {
            return None;
        }
        <[BatchAckEntry]>::ref_from_bytes(&self.tail[..bytes]).ok()
    }

    pub fn encode_into<'a>(
        out: &'a mut [u8],
        seq: u64,
        consumer_id: u32,
        entries: &[(u64, u32)],
    ) -> &'a mut Self {
        let count = entries.len();
        debug_assert_eq!(out.len(), Self::wire_size(count));

        let msg_len = (BATCH_ACK_BODY_FIXED + count * BATCH_ACK_ENTRY_SIZE) as u32;
        let frame = Self::mut_from_bytes(out).expect("BatchAckFrame layout");
        frame.header = Header::new(crate::action::Action::BatchAck.as_u16(), msg_len, seq);
        frame.body = BatchAckBody {
            consumer_id: U32::new(consumer_id),
            count: U32::new(count as u32),
        };
        let entries_buf = &mut frame.tail[..count * BATCH_ACK_ENTRY_SIZE];
        let slots = <[BatchAckEntry]>::mut_from_bytes(entries_buf).expect("entries slice");
        for (slot, (seq, hash)) in slots.iter_mut().zip(entries.iter()) {
            slot.seq = U64::new(*seq);
            slot.subject_hash = U32::new(*hash);
            slot._pad = U32::new(0);
        }
        frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_wire_size() {
        assert_eq!(AckFrame::WIRE_SIZE, 32);
    }

    #[test]
    fn ack_roundtrip() {
        let f = AckFrame::new(1, 42, 999, 0xDEADBEEF);
        let bytes = f.as_bytes();
        let parsed = AckFrame::ref_from_bytes(bytes).unwrap();
        assert_eq!(parsed.body.consumer_id.get(), 42);
        assert_eq!(parsed.body.ack_seq.get(), 999);
        assert_eq!(parsed.body.subject_hash.get(), 0xDEADBEEF);
    }

    /// T1 — B2 regression: a frame with a `count` that's larger than
    /// what the tail can hold must NOT panic inside `try_entries`. The
    /// dispatcher relies on `try_entries` returning `None` to drop the
    /// frame cleanly without crashing the shard.
    #[test]
    fn batch_ack_try_entries_handles_lying_count() {
        // Build a valid 2-entry frame, then bump `count` to a huge
        // value. The tail has exactly 2*16 bytes — claiming 100k entries
        // must not panic or read out of bounds.
        let entries = [(1u64, 0u32), (2, 0)];
        let size = BatchAckFrame::wire_size(entries.len());
        let mut buf = vec![0u8; size];
        BatchAckFrame::encode_into(&mut buf, 0, 1, &entries);

        // Overwrite the count field (BatchAckBody.count is offset 4
        // inside the body, which starts at HEADER_SIZE).
        let count_off = HEADER_SIZE + 4;
        buf[count_off] = 0xFF;
        buf[count_off + 1] = 0xFF;
        buf[count_off + 2] = 0xFF;
        buf[count_off + 3] = 0xFF;
        let parsed = BatchAckFrame::ref_from_bytes(&buf).unwrap();
        assert!(
            parsed.try_entries().is_none(),
            "lying count must return None, not panic"
        );
    }

    /// T1 — also reject overflow on the multiplication itself.
    #[test]
    fn batch_ack_try_entries_handles_count_overflow() {
        let entries = [(1u64, 0u32)];
        let size = BatchAckFrame::wire_size(entries.len());
        let mut buf = vec![0u8; size];
        BatchAckFrame::encode_into(&mut buf, 0, 1, &entries);
        // count = usize::MAX clearly overflows count * BATCH_ACK_ENTRY_SIZE.
        let count_off = HEADER_SIZE + 4;
        buf[count_off..count_off + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let parsed = BatchAckFrame::ref_from_bytes(&buf).unwrap();
        assert!(parsed.try_entries().is_none());
    }

    #[test]
    fn batch_ack_roundtrip() {
        let entries = [(100u64, 0x11u32), (101, 0x22), (102, 0x33), (103, 0x44)];
        let size = BatchAckFrame::wire_size(entries.len());
        assert_eq!(size, 16 + 8 + 4 * 16);
        let mut buf = vec![0u8; size];
        BatchAckFrame::encode_into(&mut buf, 7, 77, &entries);
        let parsed = BatchAckFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(parsed.body.consumer_id.get(), 77);
        assert_eq!(parsed.body.count.get(), 4);
        let es = parsed.entries();
        assert_eq!(es.len(), 4);
        assert_eq!(es[0].seq.get(), 100);
        assert_eq!(es[3].subject_hash.get(), 0x44);
    }
}
