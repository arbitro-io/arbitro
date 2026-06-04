//! Batch header — frames a page-aligned group of records on disk.
//!
//! ```text
//! [BatchHeader 16B]
//!   offset 0:  magic         u32   ARBITRO_MAGIC_V2 = "ARB2"
//!   offset 4:  batch_len     u32   total batch bytes including this header + padding
//!                                  (ALWAYS multiple of PAGE_SIZE)
//!   offset 8:  content_len   u32   bytes of records (no padding)
//!   offset 12: count         u32   number of records in this batch
//! ```

use zerocopy::byteorder::little_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::v2::magic::ARBITRO_MAGIC_V2;

pub const PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct BatchHeader {
    pub magic: U32,
    pub batch_len: U32,
    pub content_len: U32,
    pub count: U32,
}
pub const BATCH_HEADER_SIZE: usize = core::mem::size_of::<BatchHeader>();
const _: () = assert!(BATCH_HEADER_SIZE == 16);

impl BatchHeader {
    #[inline(always)]
    pub fn new(content_len: u32, count: u32) -> Self {
        let batch_len = align_up_to_page(BATCH_HEADER_SIZE + content_len as usize) as u32;
        Self {
            magic: U32::new(ARBITRO_MAGIC_V2),
            batch_len: U32::new(batch_len),
            content_len: U32::new(content_len),
            count: U32::new(count),
        }
    }

    /// Validates the magic number. Call at batch boundary on read / recovery.
    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        self.magic.get() == ARBITRO_MAGIC_V2
    }

    /// Offset of the padding tail inside this batch (absolute from batch start).
    #[inline(always)]
    pub fn padding_start(&self) -> usize {
        BATCH_HEADER_SIZE + self.content_len.get() as usize
    }

    #[inline(always)]
    pub fn padding_len(&self) -> usize {
        self.batch_len.get() as usize - self.padding_start()
    }
}

#[inline(always)]
pub const fn align_up_to_page(n: usize) -> usize {
    (n + PAGE_SIZE - 1) & !(PAGE_SIZE - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_up_examples() {
        assert_eq!(align_up_to_page(0), 0);
        assert_eq!(align_up_to_page(1), 4096);
        assert_eq!(align_up_to_page(4096), 4096);
        assert_eq!(align_up_to_page(4097), 8192);
    }

    #[test]
    fn batch_header_builds_page_aligned() {
        // 16 (header) + 4000 (content) = 4016 → next page = 4096.
        let bh = BatchHeader::new(4000, 10);
        assert!(bh.is_valid());
        assert_eq!(bh.batch_len.get(), 4096);
        assert_eq!(bh.content_len.get(), 4000);
        assert_eq!(bh.count.get(), 10);
        assert_eq!(bh.padding_start(), 16 + 4000);
        assert_eq!(bh.padding_len(), 4096 - 16 - 4000);
    }

    #[test]
    fn large_batch_crosses_pages() {
        let bh = BatchHeader::new(10_000, 42);
        // 16 + 10000 = 10016 → align to 12288 (3 pages).
        assert_eq!(bh.batch_len.get(), 12288);
    }

    #[test]
    fn invalid_magic_rejected() {
        let bytes = [0u8; 16];
        let bh = BatchHeader::ref_from_bytes(&bytes).unwrap();
        assert!(!bh.is_valid());
    }
}
