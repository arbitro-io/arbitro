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

    /// Validate that `buf` holds a complete entry (header + subject +
    /// reply + payload) before constructing a view over it. Returns
    /// `None` on a truncated buffer or a header whose declared lengths
    /// don't fit — callers must not slice on an unchecked `PublishView`.
    #[inline]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < PUBLISH_ENTRY_SIZE {
            return None;
        }
        let header = PublishEntry::ref_from_bytes(&buf[..PUBLISH_ENTRY_SIZE]).ok()?;
        let sl = header.subj_len.get() as usize;
        let rl = header.reply_len.get() as usize;
        let dl = header.data_len.get() as usize;
        let total = PUBLISH_ENTRY_SIZE
            .checked_add(sl)?
            .checked_add(rl)?
            .checked_add(dl)?;
        if total > buf.len() {
            return None;
        }
        Some(Self { buf })
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
///
/// Wire layout: `[4 count][entries...]`. Count is `u32` so a single batch
/// can carry up to 4 G entries — well above any practical shard pressure.
/// (Was `u16` and silently truncated past 65535, corrupting the body.)
pub struct BatchIter<'a> {
    buf: &'a [u8],
    offset: usize,
    remaining: u32,
}

impl<'a> BatchIter<'a> {
    /// Create from frame body (starts with 4-byte count).
    ///
    /// A body shorter than the 4-byte count prefix yields an
    /// already-exhausted iterator (`count() == 0`) instead of panicking.
    #[inline(always)]
    pub fn new(body: &'a [u8]) -> Self {
        let count = match body.get(0..4) {
            Some(b) => u32::from_le_bytes([b[0], b[1], b[2], b[3]]),
            None => 0,
        };
        Self {
            buf: body,
            offset: 4,
            remaining: count,
        }
    }

    #[inline(always)]
    pub fn count(&self) -> u32 {
        self.remaining
    }
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = PublishView<'a>;

    /// Validates that the remaining bytes hold a complete entry before
    /// slicing. Untrusted wire data with lying length fields or a
    /// truncated tail yields `None` instead of panicking.
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let rest = self.buf.get(self.offset..)?;
        let view = match PublishView::try_new(rest) {
            Some(v) => v,
            None => {
                self.remaining = 0;
                return None;
            }
        };
        self.remaining -= 1;
        self.offset += view.wire_len();
        Some(view)
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.remaining as usize;
        (r, Some(r))
    }
}
