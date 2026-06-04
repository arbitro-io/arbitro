//! Ingress NACK / BATCH_NACK frames.
//!
//! Wire layout is identical to ACK / BATCH_ACK — only the action
//! discriminant in the header differs. This keeps the server-side
//! decoder simple: read body, look up consumer, call `engine.nack()`.
//!
//! NACK body (16 B, no tail):
//! ```text
//!   offset 0:  consumer_id   u32  (4B)
//!   offset 4:  subject_hash  u32  (4B)
//!   offset 8:  nack_seq      u64  (8B)   ← sequence being nacked (requeued)
//! ```
//!
//! BATCH_NACK body (fixed 8 B + N × 16 B entries):
//! ```text
//!   offset 0:  consumer_id   u32  (4B)
//!   offset 4:  count         u32  (4B)
//!   entries[..]: [seq u64][subject_hash u32][_pad u32]  (16 B each)
//! ```

use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

// ── Single NACK ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct NackBody {
    pub consumer_id: U32,
    pub subject_hash: U32,
    pub nack_seq: U64,
}

pub const NACK_BODY_SIZE: usize = core::mem::size_of::<NackBody>();
const _: () = assert!(NACK_BODY_SIZE == 16);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct NackFrame {
    pub header: Header,
    pub body: NackBody,
}

const _: () = assert!(core::mem::size_of::<NackFrame>() == HEADER_SIZE + NACK_BODY_SIZE);

impl NackFrame {
    pub const WIRE_SIZE: usize = HEADER_SIZE + NACK_BODY_SIZE;

    #[inline(always)]
    pub fn new(seq: u64, consumer_id: u32, nack_seq: u64, subject_hash: u32) -> Self {
        Self {
            header: Header::new(
                crate::action::Action::Nack.as_u16(),
                NACK_BODY_SIZE as u32,
                seq,
            ),
            body: NackBody {
                consumer_id: U32::new(consumer_id),
                subject_hash: U32::new(subject_hash),
                nack_seq: U64::new(nack_seq),
            },
        }
    }
}

// ── Batch NACK ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct BatchNackBody {
    pub consumer_id: U32,
    pub count: U32,
}
pub const BATCH_NACK_BODY_FIXED: usize = core::mem::size_of::<BatchNackBody>();
const _: () = assert!(BATCH_NACK_BODY_FIXED == 8);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct BatchNackEntry {
    pub seq: U64,
    pub subject_hash: U32,
    /// Delay in milliseconds before redelivery. 0 = immediate requeue.
    /// Old clients send 0 here (was `_pad`), so this is backward-compatible.
    pub delay_ms: U32,
}
pub const BATCH_NACK_ENTRY_SIZE: usize = core::mem::size_of::<BatchNackEntry>();
const _: () = assert!(BATCH_NACK_ENTRY_SIZE == 16);

/// DST frame: `Header + BatchNackBody + entries[count]`.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct BatchNackFrame {
    pub header: Header,
    pub body: BatchNackBody,
    pub tail: [u8], // exactly count * BATCH_NACK_ENTRY_SIZE bytes
}

impl BatchNackFrame {
    #[inline(always)]
    pub const fn wire_size(count: usize) -> usize {
        HEADER_SIZE + BATCH_NACK_BODY_FIXED + count * BATCH_NACK_ENTRY_SIZE
    }

    /// Typed slice view over the entries — **panics** on a lying `count`.
    /// Hot-path callers must validate via `try_entries()` first.
    #[inline(always)]
    pub fn entries(&self) -> &[BatchNackEntry] {
        let n = self.body.count.get() as usize;
        <[BatchNackEntry]>::ref_from_bytes(&self.tail[..n * BATCH_NACK_ENTRY_SIZE])
            .expect("BatchNackEntry layout")
    }

    /// **B2 safety**: bounds-checked entries view.
    #[inline]
    pub fn try_entries(&self) -> Option<&[BatchNackEntry]> {
        let n = self.body.count.get() as usize;
        let bytes = n.checked_mul(BATCH_NACK_ENTRY_SIZE)?;
        if bytes > self.tail.len() {
            return None;
        }
        <[BatchNackEntry]>::ref_from_bytes(&self.tail[..bytes]).ok()
    }

    /// Encode a batch nack frame. Each entry is `(seq, subject_hash, delay_ms)`.
    /// For backward compat, callers can pass `delay_ms = 0` for immediate requeue.
    pub fn encode_into<'a>(
        out: &'a mut [u8],
        seq: u64,
        consumer_id: u32,
        entries: &[(u64, u32, u32)],
    ) -> &'a mut Self {
        let count = entries.len();
        debug_assert_eq!(out.len(), Self::wire_size(count));

        let msg_len = (BATCH_NACK_BODY_FIXED + count * BATCH_NACK_ENTRY_SIZE) as u32;
        let frame = Self::mut_from_bytes(out).expect("BatchNackFrame layout");
        frame.header = Header::new(crate::action::Action::BatchNack.as_u16(), msg_len, seq);
        frame.body = BatchNackBody {
            consumer_id: U32::new(consumer_id),
            count: U32::new(count as u32),
        };
        let entries_buf = &mut frame.tail[..count * BATCH_NACK_ENTRY_SIZE];
        let slots = <[BatchNackEntry]>::mut_from_bytes(entries_buf).expect("entries slice");
        for (slot, &(entry_seq, hash, delay)) in slots.iter_mut().zip(entries.iter()) {
            slot.seq = U64::new(entry_seq);
            slot.subject_hash = U32::new(hash);
            slot.delay_ms = U32::new(delay);
        }
        frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nack_wire_size() {
        assert_eq!(NackFrame::WIRE_SIZE, 32);
    }

    #[test]
    fn nack_roundtrip() {
        let f = NackFrame::new(1, 42, 999, 0xDEADBEEF);
        let bytes = f.as_bytes();
        let parsed = NackFrame::ref_from_bytes(bytes).unwrap();
        assert_eq!(parsed.body.consumer_id.get(), 42);
        assert_eq!(parsed.body.nack_seq.get(), 999);
        assert_eq!(parsed.body.subject_hash.get(), 0xDEADBEEF);
        // Action must be Nack = 0x0202
        assert_eq!(
            parsed.header.action.get(),
            crate::action::Action::Nack.as_u16()
        );
    }

    #[test]
    fn batch_nack_roundtrip() {
        // (seq, subject_hash, delay_ms)
        let entries = [(100u64, 0x11u32, 0u32), (101, 0x22, 5000), (102, 0x33, 0)];
        let size = BatchNackFrame::wire_size(entries.len());
        assert_eq!(size, 16 + 8 + 3 * 16);
        let mut buf = vec![0u8; size];
        BatchNackFrame::encode_into(&mut buf, 7, 77, &entries);
        let parsed = BatchNackFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(parsed.body.consumer_id.get(), 77);
        assert_eq!(parsed.body.count.get(), 3);
        let es = parsed.entries();
        assert_eq!(es[0].seq.get(), 100);
        assert_eq!(es[1].delay_ms.get(), 5000);
        assert_eq!(es[2].subject_hash.get(), 0x33);
    }
}
