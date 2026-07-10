//! Ingress ACK_STATE_REQ / ACK_BATCH frames — ack-reliability protocol.
//!
//! ACK_STATE_REQ body (8 B, no tail):
//! ```text
//!   offset 0:  consumer_id  u32  (4B)
//!   offset 4:  generation   u32  (4B)   ← client's last-known cursor generation
//! ```
//!
//! ACK_BATCH body (fixed 16 B + N × 8 B seqs):
//! ```text
//!   offset 0:  consumer_id  u32  (4B)
//!   offset 4:  generation   u32  (4B)
//!   offset 8:  flags        u32  (4B)
//!   offset 12: seq_count    u32  (4B)   ← number of seqs that follow
//!   seqs[..]: [seq u64]  (8B each)
//! ```

use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

/// Maximum seqs allowed in a single `AckBatch` frame.
pub const ACK_BATCH_MAX_SEQS: usize = 1000;

// ── Status codes shared with `ack_state` egress responses ───────────────

pub const ACK_STATUS_OK: u32 = 0;
pub const ACK_STATUS_BATCH_TOO_LARGE: u32 = 1;
pub const ACK_STATUS_GENERATION_MISMATCH: u32 = 2;
pub const ACK_STATUS_CONSUMER_UNKNOWN: u32 = 3;

// ── AckStateReq ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct AckStateReqBody {
    pub consumer_id: U32,
    pub generation: U32,
}

pub const ACK_STATE_REQ_BODY_SIZE: usize = core::mem::size_of::<AckStateReqBody>();
const _: () = assert!(ACK_STATE_REQ_BODY_SIZE == 8);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct AckStateReqFrame {
    pub header: Header,
    pub body: AckStateReqBody,
}

const _: () =
    assert!(core::mem::size_of::<AckStateReqFrame>() == HEADER_SIZE + ACK_STATE_REQ_BODY_SIZE);

impl AckStateReqFrame {
    pub const WIRE_SIZE: usize = HEADER_SIZE + ACK_STATE_REQ_BODY_SIZE;

    #[inline(always)]
    pub fn new(seq: u64, consumer_id: u32, generation: u32) -> Self {
        Self {
            header: Header::new(
                crate::action::Action::AckStateReq.as_u16(),
                ACK_STATE_REQ_BODY_SIZE as u32,
                seq,
            ),
            body: AckStateReqBody {
                consumer_id: U32::new(consumer_id),
                generation: U32::new(generation),
            },
        }
    }
}

// ── AckBatch ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct AckBatchBody {
    pub consumer_id: U32,
    pub generation: U32,
    pub flags: U32,
    pub seq_count: U32,
}
pub const ACK_BATCH_BODY_FIXED: usize = core::mem::size_of::<AckBatchBody>();
const _: () = assert!(ACK_BATCH_BODY_FIXED == 16);

/// DST frame: `Header + AckBatchBody + seqs[seq_count]`.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct AckBatchFrame {
    pub header: Header,
    pub body: AckBatchBody,
    pub tail: [u8], // exactly seq_count * 8 bytes
}

impl AckBatchFrame {
    #[inline(always)]
    pub const fn wire_size(count: usize) -> usize {
        HEADER_SIZE + ACK_BATCH_BODY_FIXED + count * 8
    }

    /// Typed slice view over the seqs — **panics** on a lying `seq_count`.
    /// Hot-path callers MUST validate via `try_seqs()` first.
    #[inline(always)]
    pub fn seqs(&self) -> &[U64] {
        let n = self.body.seq_count.get() as usize;
        <[U64]>::ref_from_bytes(&self.tail[..n * 8]).expect("U64 seq layout")
    }

    /// **B2 safety**: same as `seqs()` but returns `None` if `seq_count`
    /// doesn't match the tail length. Dispatchers parsing untrusted
    /// network frames must use this — the panic-on-lying-count path is
    /// remote-trigerrable.
    #[inline]
    pub fn try_seqs(&self) -> Option<&[U64]> {
        let n = self.body.seq_count.get() as usize;
        let bytes = n.checked_mul(8)?;
        if bytes > self.tail.len() {
            return None;
        }
        <[U64]>::ref_from_bytes(&self.tail[..bytes]).ok()
    }

    pub fn encode_into<'a>(
        out: &'a mut [u8],
        seq: u64,
        consumer_id: u32,
        generation: u32,
        flags: u32,
        seqs: &[u64],
    ) -> &'a mut Self {
        let count = seqs.len();
        debug_assert_eq!(out.len(), Self::wire_size(count));

        let msg_len = (ACK_BATCH_BODY_FIXED + count * 8) as u32;
        let frame = Self::mut_from_bytes(out).expect("AckBatchFrame layout");
        frame.header = Header::new(crate::action::Action::AckBatch.as_u16(), msg_len, seq);
        frame.body = AckBatchBody {
            consumer_id: U32::new(consumer_id),
            generation: U32::new(generation),
            flags: U32::new(flags),
            seq_count: U32::new(count as u32),
        };
        let seqs_buf = &mut frame.tail[..count * 8];
        let slots = <[U64]>::mut_from_bytes(seqs_buf).expect("seqs slice");
        for (slot, s) in slots.iter_mut().zip(seqs.iter()) {
            *slot = U64::new(*s);
        }
        frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_state_req_wire_size() {
        assert_eq!(AckStateReqFrame::WIRE_SIZE, 24);
    }

    #[test]
    fn ack_state_req_roundtrip() {
        let f = AckStateReqFrame::new(1, 42, 7);
        let bytes = f.as_bytes();
        let parsed = AckStateReqFrame::ref_from_bytes(bytes).unwrap();
        assert_eq!(parsed.body.consumer_id.get(), 42);
        assert_eq!(parsed.body.generation.get(), 7);
    }

    #[test]
    fn ack_batch_roundtrip() {
        let seqs = [100u64, 101, 102];
        let size = AckBatchFrame::wire_size(seqs.len());
        assert_eq!(size, 16 + 16 + 3 * 8);
        let mut buf = vec![0u8; size];
        AckBatchFrame::encode_into(&mut buf, 7, 77, 3, 0, &seqs);
        let parsed = AckBatchFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(parsed.body.consumer_id.get(), 77);
        assert_eq!(parsed.body.generation.get(), 3);
        assert_eq!(parsed.body.seq_count.get(), 3);
        let s = parsed.seqs();
        assert_eq!(s.len(), 3);
        assert_eq!(s[0].get(), 100);
        assert_eq!(s[2].get(), 102);
    }

    /// B2 regression: a frame with a `seq_count` that's larger than what
    /// the tail can hold must NOT panic inside `try_seqs`. The dispatcher
    /// relies on `try_seqs` returning `None` to drop the frame cleanly
    /// without crashing the shard.
    #[test]
    fn ack_batch_try_seqs_handles_lying_count() {
        let seqs = [1u64, 2];
        let size = AckBatchFrame::wire_size(seqs.len());
        let mut buf = vec![0u8; size];
        AckBatchFrame::encode_into(&mut buf, 0, 1, 0, 0, &seqs);

        // seq_count is offset 12 inside the body, which starts at HEADER_SIZE.
        let count_off = HEADER_SIZE + 12;
        buf[count_off..count_off + 4].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        let parsed = AckBatchFrame::ref_from_bytes(&buf).unwrap();
        assert!(
            parsed.try_seqs().is_none(),
            "lying count must return None, not panic"
        );
    }

    /// Also reject overflow on the multiplication itself.
    #[test]
    fn ack_batch_try_seqs_handles_count_overflow() {
        let seqs = [1u64];
        let size = AckBatchFrame::wire_size(seqs.len());
        let mut buf = vec![0u8; size];
        AckBatchFrame::encode_into(&mut buf, 0, 1, 0, 0, &seqs);
        let count_off = HEADER_SIZE + 12;
        buf[count_off..count_off + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let parsed = AckBatchFrame::ref_from_bytes(&buf).unwrap();
        assert!(parsed.try_seqs().is_none());
    }
}
