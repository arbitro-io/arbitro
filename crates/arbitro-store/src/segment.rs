//! Segment management for TolerantStore.
//! Strictly follows Hardware Sympathy: O(1) segment access.

use memmap2::{Mmap, MmapMut};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

pub const MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024; // 64MB

#[derive(Debug, Clone, Copy)]
pub struct SegmentMetadata {
    pub first_seq: u64,
    pub last_seq: u64,
}

pub fn segment_path(base: &Path, first_seq: u64) -> PathBuf {
    base.join(format!("{first_seq:020}.log"))
}

pub fn create_active_segment(path: &Path) -> std::io::Result<MmapMut> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    file.set_len(MAX_SEGMENT_BYTES)?;
    unsafe { MmapMut::map_mut(&file) }
}

pub fn seal_segment(path: &Path, len: u32) -> std::io::Result<Mmap> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len as u64)?;
    let sealed_file = File::open(path)?;
    unsafe { Mmap::map(&sealed_file) }
}
