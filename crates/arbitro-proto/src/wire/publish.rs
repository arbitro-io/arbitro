use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// 12B fixed entry header per message in a batch.
///
/// ```text
/// [4 data_len][2 subj_len][2 reply_len][1 flags][3 pad]
/// ```
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct PublishEntry {
    pub data_len: U32,
    pub subj_len: U16,
    pub reply_len: U16,
    pub flags: u8,
    pub _pad: [u8; 3],
}

pub const PUBLISH_ENTRY_SIZE: usize = core::mem::size_of::<PublishEntry>();
const _: () = assert!(PUBLISH_ENTRY_SIZE == 12);

/// Lazy view over a single publish entry within a batch body.
/// Body starts at the entry header (after count prefix has been consumed).
pub struct PublishView<'a> {
    buf: &'a [u8],
}

impl<'a> PublishView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    pub fn header(&self) -> &PublishEntry {
        PublishEntry::ref_from_bytes(&self.buf[..PUBLISH_ENTRY_SIZE]).unwrap()
    }

    #[inline(always)]
    pub fn flags(&self) -> u8 {
        self.header().flags
    }

    #[inline(always)]
    pub fn subject(&self) -> &'a [u8] {
        let sl = self.header().subj_len.get() as usize;
        &self.buf[PUBLISH_ENTRY_SIZE..PUBLISH_ENTRY_SIZE + sl]
    }

    #[inline(always)]
    pub fn reply_to(&self) -> &'a [u8] {
        let eh = self.header();
        let sl = eh.subj_len.get() as usize;
        let rl = eh.reply_len.get() as usize;
        let start = PUBLISH_ENTRY_SIZE + sl;
        &self.buf[start..start + rl]
    }

    #[inline(always)]
    pub fn payload(&self) -> &'a [u8] {
        let eh = self.header();
        let sl = eh.subj_len.get() as usize;
        let rl = eh.reply_len.get() as usize;
        let dl = eh.data_len.get() as usize;
        let start = PUBLISH_ENTRY_SIZE + sl + rl;
        &self.buf[start..start + dl]
    }

    /// Total bytes this entry occupies (header + subject + reply + data).
    #[inline(always)]
    pub fn wire_len(&self) -> usize {
        let eh = self.header();
        PUBLISH_ENTRY_SIZE
            + eh.subj_len.get() as usize
            + eh.reply_len.get() as usize
            + eh.data_len.get() as usize
    }
}

/// Zero-allocation iterator over batch entries.
/// Constructed from the body of a publish frame (after envelope).
pub struct BatchIter<'a> {
    buf: &'a [u8],
    offset: usize,
    remaining: u16,
}

impl<'a> BatchIter<'a> {
    /// Create from frame body (starts with 2-byte count).
    #[inline(always)]
    pub fn new(body: &'a [u8]) -> Self {
        let count = u16::from_le_bytes([body[0], body[1]]);
        Self { buf: body, offset: 2, remaining: count }
    }

    #[inline(always)]
    pub fn count(&self) -> u16 {
        self.remaining
    }
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = PublishView<'a>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;

        let view = PublishView::new(&self.buf[self.offset..]);
        self.offset += view.wire_len();
        Some(view)
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.remaining as usize;
        (r, Some(r))
    }
}
