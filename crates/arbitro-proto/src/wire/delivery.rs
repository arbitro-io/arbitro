use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// 16B — Acknowledge delivery of a single message.
///
/// `sub_id` names the subscription the message was delivered on. Paired
/// with the connection the frame arrived on, it locates the binding in one
/// lookup — `(conn, sub) → binding → pending.remove(seq)` — instead of
/// walking every binding of the consumer looking for one holding `seq`.
/// It rides the slot the echoed-back `subject_hash` used to occupy, which
/// nothing ever read: the server keeps its own copy in `Pending`.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct AckAction {
    pub sequence: U64,
    pub consumer_id: U32,
    pub sub_id: U32,
}
const _: () = assert!(core::mem::size_of::<AckAction>() == 16);

/// 8B fixed — Batch ack header. Followed by N × `BatchAckEntry`.
///
/// ```text
/// [4 consumer_id][2 count][2 pad] [entry_0][entry_1]...
/// ```
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct BatchAckFixed {
    pub consumer_id: U32,
    pub count: U16,
    pub _pad: U16,
}
const _: () = assert!(core::mem::size_of::<BatchAckFixed>() == 8);

/// 16B — Per-entry payload inside a `BatchAck`.
///
/// ```text
/// [8 seq][4 sub_id][4 pad]
/// ```
/// See [`AckAction`] for why `sub_id` sits where the echoed `subject_hash`
/// used to. Entries of one batch may name different subscriptions, so the
/// id is per entry and not on `BatchAckFixed`.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct BatchAckEntry {
    pub seq: U64,
    pub sub_id: U32,
    pub _pad: U32,
}
pub const BATCH_ACK_ENTRY_SIZE: usize = core::mem::size_of::<BatchAckEntry>();
const _: () = assert!(BATCH_ACK_ENTRY_SIZE == 16);

/// 16B — Negative ack (request redelivery).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct NackAction {
    pub sequence: U64,
    pub consumer_id: U32,
    pub delay_ms: U32,
}
const _: () = assert!(core::mem::size_of::<NackAction>() == 16);

/// 16B — Server confirms a request succeeded.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct RepOkAction {
    pub ref_seq: U64,
    pub _pad: U64,
}
const _: () = assert!(core::mem::size_of::<RepOkAction>() == 16);

/// 16B — Server reports an error.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct RepErrorAction {
    pub ref_seq: U64,
    pub error_code: U16,
    pub _pad: [u8; 6],
}
const _: () = assert!(core::mem::size_of::<RepErrorAction>() == 16);

/// 4B fixed — RepBatch header. Followed by N × DeliveryEntry.
///
/// ```text
/// [2 count][2 pad] [entry_0][entry_1]...
/// ```
///
/// Each DeliveryEntry:
/// ```text
/// [8 seq][2 subj_len][payload...]
/// ```
/// Total entry wire size = 10 + subj_len + payload_len.
/// The entry payload_len is derived from msg_len in the envelope.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct RepBatchFixed {
    pub count: U16,
    pub _pad: U16,
}
pub const REP_BATCH_FIXED_SIZE: usize = core::mem::size_of::<RepBatchFixed>();
const _: () = assert!(REP_BATCH_FIXED_SIZE == 4);

/// 24B — Per-entry header inside a RepBatch.
///
/// ```text
/// [4 consumer_id][8 seq][2 subj_len][2 reply_len][4 data_len][4 subject_hash]
/// ```
/// * `data_len` = subj_len + reply_len + payload_len (total variable bytes).
/// * `reply_len` = length of the reply_to subject (0 for non-RPC messages).
///   When > 0, the data section is `[subject][reply_to][payload]`.
/// * `subject_hash` = foldhash (fixed seed) u32 of the subject bytes. Client echoes this
///   back in the ack frame so the server performs O(1) credit arithmetic
///   on ack without touching the store.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct DeliveryEntryHeader {
    pub consumer_id: U32,
    pub seq: U64,
    pub subj_len: U16,
    pub reply_len: U16,
    pub data_len: U32,
    pub sub_id: U32,
}
pub const DELIVERY_ENTRY_HEADER_SIZE: usize = core::mem::size_of::<DeliveryEntryHeader>();
const _: () = assert!(DELIVERY_ENTRY_HEADER_SIZE == 24);

// ── Lazy views ──────────────────────────────────────────────────────────────

pub struct AckView<'a> {
    buf: &'a [u8],
}

const ACK_ACTION_SIZE: usize = core::mem::size_of::<AckAction>();

impl<'a> AckView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: returns `None` when `buf` is too short
    /// for a complete `AckAction`.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < ACK_ACTION_SIZE {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    fn inner(&self) -> &AckAction {
        AckAction::ref_from_bytes(&self.buf[..ACK_ACTION_SIZE]).unwrap()
    }

    #[inline(always)]
    fn try_inner(&self) -> Option<&AckAction> {
        AckAction::ref_from_bytes(self.buf.get(..ACK_ACTION_SIZE)?).ok()
    }

    #[inline(always)]
    pub fn sequence(&self) -> u64 {
        self.inner().sequence.get()
    }

    #[inline(always)]
    pub fn try_sequence(&self) -> Option<u64> {
        Some(self.try_inner()?.sequence.get())
    }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 {
        self.inner().consumer_id.get()
    }

    #[inline(always)]
    pub fn try_consumer_id(&self) -> Option<u32> {
        Some(self.try_inner()?.consumer_id.get())
    }
}

pub struct NackView<'a> {
    buf: &'a [u8],
}

const NACK_ACTION_SIZE: usize = core::mem::size_of::<NackAction>();

impl<'a> NackView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: returns `None` when `buf` is too short
    /// for a complete `NackAction`.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < NACK_ACTION_SIZE {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    fn inner(&self) -> &NackAction {
        NackAction::ref_from_bytes(&self.buf[..NACK_ACTION_SIZE]).unwrap()
    }

    #[inline(always)]
    fn try_inner(&self) -> Option<&NackAction> {
        NackAction::ref_from_bytes(self.buf.get(..NACK_ACTION_SIZE)?).ok()
    }

    #[inline(always)]
    pub fn sequence(&self) -> u64 {
        self.inner().sequence.get()
    }

    #[inline(always)]
    pub fn try_sequence(&self) -> Option<u64> {
        Some(self.try_inner()?.sequence.get())
    }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 {
        self.inner().consumer_id.get()
    }

    #[inline(always)]
    pub fn try_consumer_id(&self) -> Option<u32> {
        Some(self.try_inner()?.consumer_id.get())
    }

    #[inline(always)]
    pub fn delay_ms(&self) -> u32 {
        self.inner().delay_ms.get()
    }

    #[inline(always)]
    pub fn try_delay_ms(&self) -> Option<u32> {
        Some(self.try_inner()?.delay_ms.get())
    }
}

pub struct RepOkView<'a> {
    buf: &'a [u8],
}

const REP_OK_ACTION_SIZE: usize = core::mem::size_of::<RepOkAction>();

impl<'a> RepOkView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: returns `None` when `buf` is too short
    /// for a complete `RepOkAction`.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < REP_OK_ACTION_SIZE {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    pub fn ref_seq(&self) -> u64 {
        RepOkAction::ref_from_bytes(&self.buf[..REP_OK_ACTION_SIZE])
            .unwrap()
            .ref_seq
            .get()
    }

    #[inline(always)]
    pub fn try_ref_seq(&self) -> Option<u64> {
        Some(
            RepOkAction::ref_from_bytes(self.buf.get(..REP_OK_ACTION_SIZE)?)
                .ok()?
                .ref_seq
                .get(),
        )
    }
}

pub struct RepErrorView<'a> {
    buf: &'a [u8],
}

const REP_ERROR_ACTION_SIZE: usize = core::mem::size_of::<RepErrorAction>();

impl<'a> RepErrorView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: returns `None` when `buf` is too short
    /// for a complete `RepErrorAction`.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < REP_ERROR_ACTION_SIZE {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    fn inner(&self) -> &RepErrorAction {
        RepErrorAction::ref_from_bytes(&self.buf[..REP_ERROR_ACTION_SIZE]).unwrap()
    }

    #[inline(always)]
    fn try_inner(&self) -> Option<&RepErrorAction> {
        RepErrorAction::ref_from_bytes(self.buf.get(..REP_ERROR_ACTION_SIZE)?).ok()
    }

    #[inline(always)]
    pub fn ref_seq(&self) -> u64 {
        self.inner().ref_seq.get()
    }

    #[inline(always)]
    pub fn try_ref_seq(&self) -> Option<u64> {
        Some(self.try_inner()?.ref_seq.get())
    }

    #[inline(always)]
    pub fn error_code(&self) -> u16 {
        self.inner().error_code.get()
    }

    #[inline(always)]
    pub fn try_error_code(&self) -> Option<u16> {
        Some(self.try_inner()?.error_code.get())
    }
}

/// View over a BatchAck frame body.
///
/// ```text
/// [4 consumer_id][2 count][2 pad][entry_0][entry_1]...
/// ```
pub struct BatchAckView<'a> {
    buf: &'a [u8],
}

const BATCH_ACK_FIXED_SIZE: usize = core::mem::size_of::<BatchAckFixed>();

impl<'a> BatchAckView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: returns `None` when `buf` is too short
    /// for a complete `BatchAckFixed` header.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < BATCH_ACK_FIXED_SIZE {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    fn fixed(&self) -> &BatchAckFixed {
        BatchAckFixed::ref_from_bytes(&self.buf[..BATCH_ACK_FIXED_SIZE]).unwrap()
    }

    #[inline(always)]
    fn try_fixed(&self) -> Option<&BatchAckFixed> {
        BatchAckFixed::ref_from_bytes(self.buf.get(..BATCH_ACK_FIXED_SIZE)?).ok()
    }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 {
        self.fixed().consumer_id.get()
    }

    #[inline(always)]
    pub fn try_consumer_id(&self) -> Option<u32> {
        Some(self.try_fixed()?.consumer_id.get())
    }

    #[inline(always)]
    pub fn count(&self) -> u16 {
        self.fixed().count.get()
    }

    #[inline(always)]
    pub fn try_count(&self) -> Option<u16> {
        Some(self.try_fixed()?.count.get())
    }

    /// Iterator over the acked entries, yielding `(seq, subject_hash)`.
    #[inline]
    pub fn entries(&self) -> BatchAckEntryIter<'a> {
        let count = self.count() as usize;
        BatchAckEntryIter {
            buf: self.buf,
            offset: BATCH_ACK_FIXED_SIZE,
            remaining: count,
        }
    }
}

pub struct BatchAckEntryIter<'a> {
    buf: &'a [u8],
    offset: usize,
    remaining: usize,
}

impl Iterator for BatchAckEntryIter<'_> {
    type Item = (u64, u32);

    /// Validates that the remaining buffer holds a complete entry before
    /// slicing. Truncated wire data yields `None` instead of panicking.
    #[inline(always)]
    fn next(&mut self) -> Option<(u64, u32)> {
        if self.remaining == 0 {
            return None;
        }
        let end = self.offset.checked_add(BATCH_ACK_ENTRY_SIZE)?;
        let slice = self.buf.get(self.offset..end)?;
        let entry = BatchAckEntry::ref_from_bytes(slice).ok()?;
        let out = (entry.seq.get(), entry.sub_id.get());
        self.remaining -= 1;
        self.offset = end;
        Some(out)
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

/// View over a RepBatch frame body for client-side parsing.
///
/// ```text
/// [2 count][2 pad][entry_0][entry_1]...
/// ```
pub struct RepBatchView<'a> {
    buf: &'a [u8],
}

impl<'a> RepBatchView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: returns `None` when `buf` is too short
    /// for a complete `RepBatchFixed` header.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < REP_BATCH_FIXED_SIZE {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    fn fixed(&self) -> &RepBatchFixed {
        RepBatchFixed::ref_from_bytes(&self.buf[..REP_BATCH_FIXED_SIZE]).unwrap()
    }

    #[inline(always)]
    fn try_fixed(&self) -> Option<&RepBatchFixed> {
        RepBatchFixed::ref_from_bytes(self.buf.get(..REP_BATCH_FIXED_SIZE)?).ok()
    }

    #[inline(always)]
    pub fn count(&self) -> u16 {
        self.fixed().count.get()
    }

    #[inline(always)]
    pub fn try_count(&self) -> Option<u16> {
        Some(self.try_fixed()?.count.get())
    }

    /// Iterator over delivered entries.
    #[inline]
    pub fn entries(&self) -> RepBatchEntryIter<'a> {
        RepBatchEntryIter {
            buf: self.buf,
            offset: REP_BATCH_FIXED_SIZE,
            remaining: self.count() as usize,
        }
    }
}

pub struct RepBatchEntryIter<'a> {
    buf: &'a [u8],
    offset: usize,
    remaining: usize,
}

pub struct RepBatchEntry<'a> {
    pub consumer_id: u32,
    pub seq: u64,
    pub subject_hash: u32,
    pub subject: &'a [u8],
    /// Reply-to subject for request/reply. Empty slice when not an RPC message.
    pub reply_to: &'a [u8],
    pub payload: &'a [u8],
}

impl<'a> Iterator for RepBatchEntryIter<'a> {
    type Item = RepBatchEntry<'a>;

    /// Validates that the remaining bytes hold a complete entry (fixed
    /// header + subject + reply_to + payload) before slicing. A
    /// truncated tail or a header whose declared lengths don't fit
    /// (including `data_len < subj_len + reply_len`) yields `None`
    /// instead of panicking on untrusted wire data.
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let rest = self.buf.get(self.offset..)?;
        if rest.len() < DELIVERY_ENTRY_HEADER_SIZE {
            self.remaining = 0;
            return None;
        }
        let header =
            DeliveryEntryHeader::ref_from_bytes(&rest[..DELIVERY_ENTRY_HEADER_SIZE]).ok()?;
        let consumer_id = header.consumer_id.get();
        let seq = header.seq.get();
        let subj_len = header.subj_len.get() as usize;
        let reply_len = header.reply_len.get() as usize;
        let data_len = header.data_len.get() as usize;
        let subject_hash = header.sub_id.get();

        let entry_total = DELIVERY_ENTRY_HEADER_SIZE.checked_add(data_len)?;
        if entry_total > rest.len() {
            self.remaining = 0;
            return None;
        }
        let head_len = subj_len.checked_add(reply_len)?;
        if head_len > data_len {
            self.remaining = 0;
            return None;
        }
        let payload_len = data_len - head_len;

        let tail = &rest[DELIVERY_ENTRY_HEADER_SIZE..entry_total];
        let subject = &tail[..subj_len];
        let reply_to = &tail[subj_len..subj_len + reply_len];
        let payload = &tail[subj_len + reply_len..subj_len + reply_len + payload_len];

        self.remaining -= 1;
        self.offset += entry_total;

        Some(RepBatchEntry {
            consumer_id,
            seq,
            subject_hash,
            subject,
            reply_to,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_view_try_new_rejects_short_buffer() {
        assert!(AckView::try_new(&[0u8; ACK_ACTION_SIZE - 1]).is_none());
        let view = AckView::try_new(&[0u8; ACK_ACTION_SIZE]).unwrap();
        assert_eq!(view.try_sequence(), Some(0));
        assert_eq!(view.try_consumer_id(), Some(0));
    }

    #[test]
    fn nack_view_try_new_rejects_short_buffer() {
        assert!(NackView::try_new(&[0u8; NACK_ACTION_SIZE - 1]).is_none());
        let view = NackView::try_new(&[0u8; NACK_ACTION_SIZE]).unwrap();
        assert_eq!(view.try_sequence(), Some(0));
        assert_eq!(view.try_consumer_id(), Some(0));
        assert_eq!(view.try_delay_ms(), Some(0));
    }

    #[test]
    fn rep_ok_view_try_new_rejects_short_buffer() {
        assert!(RepOkView::try_new(&[0u8; REP_OK_ACTION_SIZE - 1]).is_none());
        let view = RepOkView::try_new(&[0u8; REP_OK_ACTION_SIZE]).unwrap();
        assert_eq!(view.try_ref_seq(), Some(0));
    }

    #[test]
    fn rep_error_view_try_new_rejects_short_buffer() {
        assert!(RepErrorView::try_new(&[0u8; REP_ERROR_ACTION_SIZE - 1]).is_none());
        let view = RepErrorView::try_new(&[0u8; REP_ERROR_ACTION_SIZE]).unwrap();
        assert_eq!(view.try_ref_seq(), Some(0));
        assert_eq!(view.try_error_code(), Some(0));
    }

    #[test]
    fn batch_ack_view_try_new_rejects_short_buffer() {
        assert!(BatchAckView::try_new(&[0u8; BATCH_ACK_FIXED_SIZE - 1]).is_none());
        let view = BatchAckView::try_new(&[0u8; BATCH_ACK_FIXED_SIZE]).unwrap();
        assert_eq!(view.try_consumer_id(), Some(0));
        assert_eq!(view.try_count(), Some(0));
    }

    #[test]
    fn rep_batch_view_try_new_rejects_short_buffer() {
        assert!(RepBatchView::try_new(&[0u8; REP_BATCH_FIXED_SIZE - 1]).is_none());
        let view = RepBatchView::try_new(&[0u8; REP_BATCH_FIXED_SIZE]).unwrap();
        assert_eq!(view.try_count(), Some(0));
    }

    #[test]
    fn batch_ack_iter_truncated_entry_yields_none() {
        // Build a BatchAckFixed header claiming 2 entries but only provide 1.
        let fixed = BatchAckFixed {
            consumer_id: U32::new(1),
            count: U16::new(2),
            _pad: U16::new(0),
        };
        let entry = BatchAckEntry {
            seq: U64::new(42),
            sub_id: U32::new(0xABCD),
            _pad: U32::new(0),
        };
        let mut buf = Vec::new();
        buf.extend_from_slice(fixed.as_bytes());
        buf.extend_from_slice(entry.as_bytes());
        // Only 1 entry in the buffer, but count says 2.
        let view = BatchAckView::try_new(&buf).unwrap();
        let items: Vec<_> = view.entries().collect();
        // First entry should parse, second should yield None (truncated).
        assert_eq!(items.len(), 1);
        assert_eq!(items[0], (42, 0xABCD));
    }
}
