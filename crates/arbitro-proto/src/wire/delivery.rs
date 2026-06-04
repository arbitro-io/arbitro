use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// 16B — Acknowledge delivery of a single message.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct AckAction {
    pub sequence: U64,
    pub consumer_id: U32,
    pub _pad: U32,
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
/// [8 seq][4 subject_hash][4 pad]
/// ```
/// `subject_hash` is the FNV-1a u32 echoed from the `DeliveryEntryHeader`
/// the client originally received. It lets the server decrement
/// `max_subject_inflight` credits in O(1) arithmetic without any lookup.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct BatchAckEntry {
    pub seq: U64,
    pub subject_hash: U32,
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

/// 8B fixed — RepBatch header. Followed by N × DeliveryEntry.
///
/// ```text
/// [4 consumer_id][2 count][2 pad] [entry_0][entry_1]...
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
/// * `subject_hash` = FNV-1a u32 of the subject bytes. Client echoes this
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
    pub subject_hash: U32,
}
pub const DELIVERY_ENTRY_HEADER_SIZE: usize = core::mem::size_of::<DeliveryEntryHeader>();
const _: () = assert!(DELIVERY_ENTRY_HEADER_SIZE == 24);

// ── Lazy views ──────────────────────────────────────────────────────────────

pub struct AckView<'a> {
    buf: &'a [u8],
}

impl<'a> AckView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    fn inner(&self) -> &AckAction {
        AckAction::ref_from_bytes(&self.buf[..core::mem::size_of::<AckAction>()]).unwrap()
    }

    #[inline(always)]
    pub fn sequence(&self) -> u64 {
        self.inner().sequence.get()
    }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 {
        self.inner().consumer_id.get()
    }
}

pub struct NackView<'a> {
    buf: &'a [u8],
}

impl<'a> NackView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    fn inner(&self) -> &NackAction {
        NackAction::ref_from_bytes(&self.buf[..core::mem::size_of::<NackAction>()]).unwrap()
    }

    #[inline(always)]
    pub fn sequence(&self) -> u64 {
        self.inner().sequence.get()
    }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 {
        self.inner().consumer_id.get()
    }

    #[inline(always)]
    pub fn delay_ms(&self) -> u32 {
        self.inner().delay_ms.get()
    }
}

pub struct RepOkView<'a> {
    buf: &'a [u8],
}

impl<'a> RepOkView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    pub fn ref_seq(&self) -> u64 {
        RepOkAction::ref_from_bytes(&self.buf[..core::mem::size_of::<RepOkAction>()])
            .unwrap()
            .ref_seq
            .get()
    }
}

pub struct RepErrorView<'a> {
    buf: &'a [u8],
}

impl<'a> RepErrorView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    fn inner(&self) -> &RepErrorAction {
        RepErrorAction::ref_from_bytes(&self.buf[..core::mem::size_of::<RepErrorAction>()]).unwrap()
    }

    #[inline(always)]
    pub fn ref_seq(&self) -> u64 {
        self.inner().ref_seq.get()
    }

    #[inline(always)]
    pub fn error_code(&self) -> u16 {
        self.inner().error_code.get()
    }
}

/// View over a BatchAck frame body.
///
/// ```text
/// [4 consumer_id][2 count][2 pad][8 seq_0][8 seq_1]...
/// ```
pub struct BatchAckView<'a> {
    buf: &'a [u8],
}

impl<'a> BatchAckView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    fn fixed(&self) -> &BatchAckFixed {
        BatchAckFixed::ref_from_bytes(&self.buf[..core::mem::size_of::<BatchAckFixed>()]).unwrap()
    }

    #[inline(always)]
    pub fn consumer_id(&self) -> u32 {
        self.fixed().consumer_id.get()
    }

    #[inline(always)]
    pub fn count(&self) -> u16 {
        self.fixed().count.get()
    }

    /// Iterator over the acked entries, yielding `(seq, subject_hash)`.
    #[inline]
    pub fn entries(&self) -> BatchAckEntryIter<'a> {
        let count = self.count() as usize;
        let start = core::mem::size_of::<BatchAckFixed>();
        BatchAckEntryIter {
            buf: self.buf,
            offset: start,
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

    #[inline(always)]
    fn next(&mut self) -> Option<(u64, u32)> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let entry = BatchAckEntry::ref_from_bytes(
            &self.buf[self.offset..self.offset + BATCH_ACK_ENTRY_SIZE],
        )
        .unwrap();
        let out = (entry.seq.get(), entry.subject_hash.get());
        self.offset += BATCH_ACK_ENTRY_SIZE;
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
/// [4 consumer_id][2 count][2 pad][entry_0][entry_1]...
/// ```
pub struct RepBatchView<'a> {
    buf: &'a [u8],
}

impl<'a> RepBatchView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    fn fixed(&self) -> &RepBatchFixed {
        RepBatchFixed::ref_from_bytes(&self.buf[..REP_BATCH_FIXED_SIZE]).unwrap()
    }

    #[inline(always)]
    pub fn count(&self) -> u16 {
        self.fixed().count.get()
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

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;

        let header = DeliveryEntryHeader::ref_from_bytes(
            &self.buf[self.offset..self.offset + DELIVERY_ENTRY_HEADER_SIZE],
        )
        .unwrap();
        let consumer_id = header.consumer_id.get();
        let seq = header.seq.get();
        let subj_len = header.subj_len.get() as usize;
        let reply_len = header.reply_len.get() as usize;
        let data_len = header.data_len.get() as usize;
        let subject_hash = header.subject_hash.get();
        self.offset += DELIVERY_ENTRY_HEADER_SIZE;

        let subject = &self.buf[self.offset..self.offset + subj_len];
        let reply_to = &self.buf[self.offset + subj_len..self.offset + subj_len + reply_len];
        let payload_start = self.offset + subj_len + reply_len;
        let payload_len = data_len - subj_len - reply_len;
        let payload = &self.buf[payload_start..payload_start + payload_len];
        self.offset += data_len;

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
