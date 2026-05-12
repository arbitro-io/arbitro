//! Ingress BATCH-PUB frame — homogeneous batch of plain publishes.
//!
//! Wire layout:
//! ```text
//! [Header 16B]                          ← action = Action::PublishBatch
//! [BatchPubBody fixed part 8B]
//!   offset 0:  stream_id  u32  (4B)    ← all entries target the same stream
//!   offset 4:  count      u32  (4B)    ← number of entries that follow
//! [tail ... variable]                   ← `count` entries, each:
//!   [BatchPubEntryHeader 8B]
//!     offset 0:  subject_len  u16  (2B)
//!     offset 2:  _pad         u16  (2B)
//!     offset 4:  payload_len  u32  (4B)
//!   [subject  subject_len bytes]
//!   [payload  payload_len bytes]
//! ```
//!
//! ### Why no per-entry `entry_flags`
//!
//! In a batch, every entry shares the same `header.entry_flags` (RETAIN,
//! COMPRESSED, …). If you need heterogeneous flags per-entry, send
//! multiple batches — keeping the wire shape uniform makes the decode
//! loop branch-free.
//!
//! ### Why no reply / headers
//!
//! Plain batch only. For batches with headers use `Action::PublishBatchWithHeaders`
//! (a separate frame type with a wider per-entry header). Reply-batches
//! are not supported on purpose — RPC semantics don't compose with batching.

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::header::{Header, HEADER_SIZE};

// ── Fixed body (8 B) ───────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct BatchPubBody {
    pub stream_id: U32,
    pub count:     U32,
}
pub const BATCH_PUB_BODY_FIXED: usize = core::mem::size_of::<BatchPubBody>();
const _: () = assert!(BATCH_PUB_BODY_FIXED == 8);

// ── Per-entry fixed header (8 B) ───────────────────────────────────────

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct BatchPubEntryHeader {
    pub subject_len: U16,
    /// Length of an opaque per-entry id used for broker-side dedup
    /// when the target stream has idempotency enabled. `0` = no id
    /// for this entry (mixing dedup + non-dedup entries in the same
    /// batch is allowed; the broker checks each independently).
    pub msg_id_len:  U16,
    pub payload_len: U32,
}
pub const BATCH_PUB_ENTRY_HEADER_SIZE: usize = core::mem::size_of::<BatchPubEntryHeader>();
const _: () = assert!(BATCH_PUB_ENTRY_HEADER_SIZE == 8);

// ── DST view ───────────────────────────────────────────────────────────

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct BatchPubFrame {
    pub header: Header,
    pub body:   BatchPubBody,
    pub tail:   [u8], // count × (BatchPubEntryHeader + subject + payload)
}

impl BatchPubFrame {
    /// Wire size for a batch where total `tail_bytes` is the sum across
    /// all entries of `(8 + subject_len + payload_len)`.
    #[inline(always)]
    pub const fn wire_size(tail_bytes: usize) -> usize {
        HEADER_SIZE + BATCH_PUB_BODY_FIXED + tail_bytes
    }

    #[inline(always)]
    pub fn count(&self) -> u32 {
        self.body.count.get()
    }

    #[inline(always)]
    pub fn iter(&self) -> BatchPubIter<'_> {
        BatchPubIter {
            buf:       &self.tail,
            offset:    0,
            remaining: self.body.count.get(),
        }
    }

    /// Encode a fresh BATCH_PUB frame into `out`.
    ///
    /// Each entry = `(subject, msg_id, payload)`. Pass an empty
    /// `msg_id` slice for entries that should not be deduped (legacy
    /// behaviour). Pre-size `out` to `wire_size(total_tail_bytes)`.
    pub fn encode_into<'a>(
        out: &'a mut [u8],
        seq: u64,
        stream_id: u32,
        flags: u8,
        entry_flags: u8,
        entries: &[(&'a [u8], &'a [u8], &'a [u8])],
    ) -> &'a mut Self {
        let mut tail_bytes: usize = 0;
        for (s, m, p) in entries {
            tail_bytes += BATCH_PUB_ENTRY_HEADER_SIZE + s.len() + m.len() + p.len();
        }
        Self::encode_into_iter(
            out, seq, stream_id, flags, entry_flags,
            entries.len() as u32, tail_bytes,
            entries.iter().copied(),
        )
    }

    /// Like `encode_into` but accepts any iterator — avoids an intermediate
    /// `Vec` when the caller already holds entries in a different form.
    ///
    /// `count` and `tail_bytes` must be pre-computed by the caller.
    /// `out.len()` must equal `wire_size(tail_bytes)`.
    #[allow(clippy::too_many_arguments)]
    pub fn encode_into_iter<'a, I>(
        out: &'a mut [u8],
        seq: u64,
        stream_id: u32,
        flags: u8,
        entry_flags: u8,
        count: u32,
        tail_bytes: usize,
        entries: I,
    ) -> &'a mut Self
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8], &'a [u8])>,
    {
        debug_assert_eq!(out.len(), Self::wire_size(tail_bytes));

        let msg_len = (BATCH_PUB_BODY_FIXED + tail_bytes) as u32;
        let frame = Self::mut_from_bytes(out).expect("BatchPubFrame layout");
        frame.header = Header::new(crate::action::Action::PublishBatch.as_u16(), msg_len, seq)
            .with_flags(flags)
            .with_entry_flags(entry_flags);
        frame.body = BatchPubBody {
            stream_id: U32::new(stream_id),
            count:     U32::new(count),
        };

        let mut off = 0usize;
        for (subject, msg_id, payload) in entries {
            let hdr_end = off + BATCH_PUB_ENTRY_HEADER_SIZE;
            let entry_hdr = BatchPubEntryHeader {
                subject_len: U16::new(subject.len() as u16),
                msg_id_len:  U16::new(msg_id.len() as u16),
                payload_len: U32::new(payload.len() as u32),
            };
            frame.tail[off..hdr_end].copy_from_slice(entry_hdr.as_bytes());
            let s_end = hdr_end + subject.len();
            frame.tail[hdr_end..s_end].copy_from_slice(subject);
            let m_end = s_end + msg_id.len();
            frame.tail[s_end..m_end].copy_from_slice(msg_id);
            let p_end = m_end + payload.len();
            frame.tail[m_end..p_end].copy_from_slice(payload);
            off = p_end;
        }
        frame
    }
}

// ── Per-entry view ─────────────────────────────────────────────────────

pub struct BatchPubEntryView<'a> {
    buf: &'a [u8],
}

impl<'a> BatchPubEntryView<'a> {
    #[inline(always)]
    pub fn header(&self) -> &'a BatchPubEntryHeader {
        BatchPubEntryHeader::ref_from_bytes(&self.buf[..BATCH_PUB_ENTRY_HEADER_SIZE])
            .expect("BatchPubEntryHeader layout")
    }

    #[inline(always)]
    pub fn subject(&self) -> &'a [u8] {
        let h = self.header();
        let s = h.subject_len.get() as usize;
        &self.buf[BATCH_PUB_ENTRY_HEADER_SIZE..BATCH_PUB_ENTRY_HEADER_SIZE + s]
    }

    /// Per-entry id used for broker-side dedup. Empty slice when
    /// `msg_id_len == 0` (legacy / non-dedup entry).
    #[inline(always)]
    pub fn msg_id(&self) -> &'a [u8] {
        let h = self.header();
        let s = h.subject_len.get() as usize;
        let m = h.msg_id_len.get() as usize;
        let start = BATCH_PUB_ENTRY_HEADER_SIZE + s;
        &self.buf[start..start + m]
    }

    #[inline(always)]
    pub fn payload(&self) -> &'a [u8] {
        let h = self.header();
        let s = h.subject_len.get() as usize;
        let m = h.msg_id_len.get() as usize;
        let p = h.payload_len.get() as usize;
        let start = BATCH_PUB_ENTRY_HEADER_SIZE + s + m;
        &self.buf[start..start + p]
    }

    #[inline(always)]
    pub fn wire_len(&self) -> usize {
        let h = self.header();
        BATCH_PUB_ENTRY_HEADER_SIZE
            + h.subject_len.get() as usize
            + h.msg_id_len.get() as usize
            + h.payload_len.get() as usize
    }
}

// ── Iterator ──────────────────────────────────────────────────────────

pub struct BatchPubIter<'a> {
    buf:       &'a [u8],
    offset:    usize,
    remaining: u32,
}

impl<'a> Iterator for BatchPubIter<'a> {
    type Item = BatchPubEntryView<'a>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let view = BatchPubEntryView { buf: &self.buf[self.offset..] };
        self.offset += view.wire_len();
        Some(view)
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.remaining as usize;
        (r, Some(r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_and_entry_sizes() {
        assert_eq!(BATCH_PUB_BODY_FIXED, 8);
        assert_eq!(BATCH_PUB_ENTRY_HEADER_SIZE, 8);
    }

    #[test]
    fn encode_then_iter_roundtrip() {
        let entries: &[(&[u8], &[u8], &[u8])] = &[
            (b"a.b", b"", b"PING"),
            (b"orders.eu.42", b"", b"hello world"),
            (b"x", b"", &[0xCC; 32]),
        ];
        let mut tail_bytes = 0usize;
        for (s, m, p) in entries {
            tail_bytes += BATCH_PUB_ENTRY_HEADER_SIZE + s.len() + m.len() + p.len();
        }

        let size = BatchPubFrame::wire_size(tail_bytes);
        let mut buf = vec![0u8; size];
        BatchPubFrame::encode_into(&mut buf, 99, 0xCAFE, 0, 0, entries);

        let frame = BatchPubFrame::ref_from_bytes(&buf).expect("layout");
        assert_eq!(frame.header.seq.get(), 99);
        assert_eq!(frame.header.action.get(), crate::action::Action::PublishBatch.as_u16());
        assert_eq!(frame.body.stream_id.get(), 0xCAFE);
        assert_eq!(frame.count(), 3);

        let collected: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = frame
            .iter()
            .map(|v| (v.subject().to_vec(), v.msg_id().to_vec(), v.payload().to_vec()))
            .collect();

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].0, b"a.b");
        assert_eq!(collected[0].2, b"PING");
        assert_eq!(collected[1].0, b"orders.eu.42");
        assert_eq!(collected[2].2, vec![0xCC; 32]);
    }

    #[test]
    fn msg_id_roundtrips_per_entry() {
        let entries: &[(&[u8], &[u8], &[u8])] = &[
            (b"orders.new", b"id-1", b"a"),
            (b"orders.new", b"",     b"b"), // legacy entry — no dedup
            (b"orders.new", b"id-2", b"c"),
        ];
        let mut tail_bytes = 0usize;
        for (s, m, p) in entries {
            tail_bytes += BATCH_PUB_ENTRY_HEADER_SIZE + s.len() + m.len() + p.len();
        }
        let size = BatchPubFrame::wire_size(tail_bytes);
        let mut buf = vec![0u8; size];
        BatchPubFrame::encode_into(&mut buf, 1, 7, 0, 0, entries);

        let frame = BatchPubFrame::ref_from_bytes(&buf).unwrap();
        let v: Vec<_> = frame.iter().collect();
        assert_eq!(v[0].msg_id(), b"id-1");
        assert_eq!(v[0].payload(), b"a");
        assert_eq!(v[1].msg_id(), b"" as &[u8]);
        assert_eq!(v[1].payload(), b"b");
        assert_eq!(v[2].msg_id(), b"id-2");
        assert_eq!(v[2].payload(), b"c");
    }

    #[test]
    fn as_bytes_is_identity_after_decode() {
        let entries: &[(&[u8], &[u8], &[u8])] = &[(b"s", b"", b"P"), (b"ss", b"", b"PP")];
        let mut tail_bytes = 0usize;
        for (s, m, p) in entries {
            tail_bytes += BATCH_PUB_ENTRY_HEADER_SIZE + s.len() + m.len() + p.len();
        }
        let size = BatchPubFrame::wire_size(tail_bytes);
        let mut buf = vec![0u8; size];
        BatchPubFrame::encode_into(&mut buf, 1, 0, 0, 0, entries);
        let snapshot = buf.clone();

        let frame = BatchPubFrame::ref_from_bytes(&buf).unwrap();
        let reemitted = frame.as_bytes();
        assert_eq!(reemitted, &snapshot[..]);
        assert_eq!(reemitted.as_ptr(), buf.as_ptr());
    }
}
