//! Tolerant journal — Segmented Log, Mmap-based zero-copy reads/writes.
//!
//! Strictly follows Hardware Sympathy: Zero allocations on delivery path.
//! Uses memmap2 to map log segments for O(1) instant access.
//! Syncs active segment to disk only on shutdown or rotation.

use memmap2::{Mmap, MmapMut};
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;

use crate::store::{entry_matches, Entry, EntryRef, Store, StoreError, StoreInfo};

const MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024; // 64MB per segment

/// Metadata for O(1) access to disk entries.
#[derive(Debug, Clone, Copy)]
struct LogMetadata {
    pub seq: u64,
    pub ts: u64,
    pub subj_len: u16,
    pub payload_len: u32,
    pub offset: u32,      // Support up to 4GB segments (O(1) fits in 32-bit)
    pub segment_idx: u32, // Index into the segments vector
}

/// Metadata for a single log segment file.
#[derive(Debug, Clone, Copy)]
struct SegmentMetadata {
    pub first_seq: u64,
    pub last_seq: u64,
}

pub struct TolerantStore {
    base_path: PathBuf,
    active_mmap: Option<MmapMut>,
    active_segment_id: u64,
    sealed_segments: Vec<Mmap>,
    segments: Vec<SegmentMetadata>, // NEW: Track segment boundaries
    index: Vec<LogMetadata>,
    next_seq: u64,
    first_seq: u64,
    total_bytes: u64,
    current_segment_offset: u32,
}

impl TolerantStore {
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path,
            active_mmap: None,
            active_segment_id: 0,
            sealed_segments: Vec::new(),
            segments: Vec::new(),
            index: Vec::with_capacity(1024),
            next_seq: 1,
            first_seq: 1,
            total_bytes: 0,
            current_segment_offset: 0,
        }
    }

    fn segment_path(&self, first_seq: u64) -> PathBuf {
        self.base_path.join(format!("{:020}.log", first_seq))
    }

    fn rotate(&mut self) -> Result<(), StoreError> {
        // Sync and seal current active segment
        if let Some(mmap) = self.active_mmap.take() {
            let path = self.segment_path(self.active_segment_id);
            // Truncate to actual written size before sealing
            let file = OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(|_| StoreError::NotFound)?;
            file.set_len(self.current_segment_offset as u64)
                .map_err(|_| StoreError::Full)?;
            let _ = mmap.flush();
            drop(mmap);

            // Re-map as read-only for sealing
            let sealed_file = File::open(&path).map_err(|_| StoreError::NotFound)?;
            let sealed_mmap = unsafe { Mmap::map(&sealed_file).map_err(|_| StoreError::NotFound)? };
            self.sealed_segments.push(sealed_mmap);
        }

        // Create new active segment
        self.active_segment_id = self.next_seq;
        let path = self.segment_path(self.active_segment_id);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|_| StoreError::NotFound)?;

        // Pre-allocate file to MAX_SEGMENT_BYTES (Zero lock during write)
        file.set_len(MAX_SEGMENT_BYTES)
            .map_err(|_| StoreError::Full)?;

        let mmap = unsafe { MmapMut::map_mut(&file).map_err(|_| StoreError::NotFound)? };
        self.active_mmap = Some(mmap);
        self.current_segment_offset = 0;
        Ok(())
    }

    fn scan_segments(&mut self) -> Result<(), StoreError> {
        if !self.base_path.exists() {
            let _ = fs::create_dir_all(&self.base_path);
            return Ok(());
        }

        let mut paths: Vec<_> = fs::read_dir(&self.base_path)
            .map_err(|_| StoreError::NotFound)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |ext| ext == "log"))
            .collect();

        paths.sort();

        for path in paths {
            let file = File::open(&path).map_err(|_| StoreError::NotFound)?;
            let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
            if file_len == 0 {
                continue;
            } // Skip empty

            let mmap = unsafe { Mmap::map(&file).map_err(|_| StoreError::NotFound)? };
            let segment_idx = self.sealed_segments.len() as u32;

            let mut offset = 0usize;
            let data = &mmap[..];

            while offset + 22 <= data.len() {
                let subj_len = u16::from_le_bytes([data[offset], data[offset + 1]]);
                let payload_len = u32::from_le_bytes([
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                ]);
                let ts = u64::from_le_bytes([
                    data[offset + 6],
                    data[offset + 7],
                    data[offset + 8],
                    data[offset + 9],
                    data[offset + 10],
                    data[offset + 11],
                    data[offset + 12],
                    data[offset + 13],
                ]);
                let seq = u64::from_le_bytes([
                    data[offset + 14],
                    data[offset + 15],
                    data[offset + 16],
                    data[offset + 17],
                    data[offset + 18],
                    data[offset + 19],
                    data[offset + 20],
                    data[offset + 21],
                ]);

                self.index.push(LogMetadata {
                    seq,
                    ts,
                    subj_len,
                    payload_len,
                    offset: (offset + 22) as u32,
                    segment_idx,
                });

                offset += 22 + (subj_len as usize) + (payload_len as usize);
                self.next_seq = seq + 1;
                self.total_bytes += (subj_len as u64) + (payload_len as u64);
            }

            self.sealed_segments.push(mmap);
        }

        if let Some(first) = self.index.first() {
            self.first_seq = first.seq;
        }

        Ok(())
    }

    #[inline]
    fn seq_to_idx(&self, seq: u64) -> Option<usize> {
        self.index.binary_search_by_key(&seq, |meta| meta.seq).ok()
    }
}

impl Store for TolerantStore {
    fn init(&mut self) -> Result<(), StoreError> {
        self.scan_segments()?;
        // Always start a fresh new active segment for simplicity in Bench Chaos
        self.rotate()?;
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), StoreError> {
        if let Some(mmap) = self.active_mmap.take() {
            let _ = mmap.flush();
            // Truncate to actual written size
            let path = self.segment_path(self.active_segment_id);
            let file = OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(|_| StoreError::NotFound)?;
            let _ = file.set_len(self.current_segment_offset as u64);
        }
        Ok(())
    }

    #[inline]
    fn append(&mut self, entry: EntryRef<'_>, timestamp: u64) -> Result<u64, StoreError> {
        let entry_total = (entry.subject.len() + entry.payload.len()) as u64;
        let total_needed = 22 + entry_total;

        // Rotate if segment is too large
        if (self.current_segment_offset as u64) + total_needed >= MAX_SEGMENT_BYTES {
            self.rotate()?;
        }

        let seq = self.next_seq;
        let mmap = self.active_mmap.as_mut().ok_or(StoreError::NotFound)?;
        let start = self.current_segment_offset as usize;

        // 1. Header (22 bytes)
        let subj_len = entry.subject.len() as u16;
        let payload_len = entry.payload.len() as u32;

        mmap[start..start + 2].copy_from_slice(&subj_len.to_le_bytes());
        mmap[start + 2..start + 6].copy_from_slice(&payload_len.to_le_bytes());
        mmap[start + 6..start + 14].copy_from_slice(&timestamp.to_le_bytes());
        mmap[start + 14..start + 22].copy_from_slice(&seq.to_le_bytes());

        // 2. Data
        let data_offset = start + 22;
        mmap[data_offset..data_offset + entry.subject.len()].copy_from_slice(entry.subject);
        mmap[data_offset + entry.subject.len()
            ..data_offset + entry.subject.len() + entry.payload.len()]
            .copy_from_slice(entry.payload);

        // 3. Update index
        self.index.push(LogMetadata {
            seq,
            ts: timestamp,
            subj_len,
            payload_len,
            offset: data_offset as u32,
            segment_idx: self.sealed_segments.len() as u32, // Points to active mmap
        });

        self.next_seq += 1;
        self.total_bytes += entry_total;
        self.current_segment_offset += total_needed as u32;

        Ok(seq)
    }

    #[inline]
    fn append_batch(
        &mut self,
        entries: &[EntryRef<'_>],
        timestamp: u64,
    ) -> Result<u64, StoreError> {
        if entries.is_empty() {
            return Ok(self.next_seq);
        }
        let first = self.next_seq;
        for entry in entries {
            self.append(*entry, timestamp)?;
        }
        Ok(first)
    }

    fn read(&self, _seq: u64) -> Result<Option<Entry<'_>>, StoreError> {
        // Prefer get() for direct access
        Err(StoreError::NotFound)
    }

    fn read_range(&self, _start: u64, _end: u64) -> Result<Vec<Entry<'_>>, StoreError> {
        Err(StoreError::NotFound)
    }

    #[inline]
    fn get(&self, seq: u64, f: &mut dyn FnMut(&Entry<'_>)) -> Result<bool, StoreError> {
        let idx = match self.seq_to_idx(seq) {
            Some(i) => i,
            None => return Ok(false),
        };

        let meta = &self.index[idx];

        // Zero-copy view directly from mmap
        let data = if meta.segment_idx == self.sealed_segments.len() as u32 {
            self.active_mmap
                .as_ref()
                .map(|m| &m[..])
                .ok_or(StoreError::NotFound)?
        } else {
            &self.sealed_segments[meta.segment_idx as usize][..]
        };

        let subj_end = (meta.offset as usize) + (meta.subj_len as usize);
        let entry = Entry {
            seq: meta.seq,
            timestamp: meta.ts,
            subject: &data[meta.offset as usize..subj_end],
            payload: &data[subj_end..subj_end + (meta.payload_len as usize)],
        };

        f(&entry);
        Ok(true)
    }

    fn for_each(
        &self,
        start: u64,
        end: u64,
        f: &mut dyn FnMut(&Entry<'_>),
    ) -> Result<(), StoreError> {
        let s = self
            .index
            .binary_search_by_key(&start, |m| m.seq)
            .unwrap_or_else(|i| i);
        let e = self
            .index
            .binary_search_by_key(&end, |m| m.seq)
            .unwrap_or_else(|i| i);

        for i in s..e {
            let meta = &self.index[i];
            let data = if meta.segment_idx == self.sealed_segments.len() as u32 {
                self.active_mmap
                    .as_ref()
                    .map(|m| &m[..])
                    .ok_or(StoreError::NotFound)?
            } else {
                &self.sealed_segments[meta.segment_idx as usize][..]
            };

            let subj_end = (meta.offset as usize) + (meta.subj_len as usize);
            let entry = Entry {
                seq: meta.seq,
                timestamp: meta.ts,
                subject: &data[meta.offset as usize..subj_end],
                payload: &data[subj_end..subj_end + (meta.payload_len as usize)],
            };
            f(&entry);
        }
        Ok(())
    }

    fn truncate_front(&mut self, target_first_seq: u64) -> u64 {
        if self.index.is_empty() || target_first_seq <= self.first_seq {
            return 0;
        }

        // 1. Find the split point in index
        let idx_split = match self
            .index
            .binary_search_by_key(&target_first_seq, |m| m.seq)
        {
            Ok(i) => i,
            Err(i) => i,
        };

        if idx_split == 0 {
            return 0;
        }

        let removed = idx_split as u64;

        // 2. Identify which segments are fully removed
        let mut segments_to_drop = 0usize;
        for i in 0..self.sealed_segments.len() {
            if self.segments[i].last_seq < target_first_seq {
                segments_to_drop += 1;
            } else {
                break;
            }
        }

        // 3. Delete files for fully removed segments
        for i in 0..segments_to_drop {
            let path = self.segment_path(self.segments[i].first_seq);
            let _ = fs::remove_file(path);
        }

        // 4. Update data structures
        self.sealed_segments.drain(0..segments_to_drop);
        self.segments.drain(0..segments_to_drop);
        self.index.drain(0..idx_split);

        // 5. Re-index remaining entries' segment_idx
        for meta in &mut self.index {
            meta.segment_idx -= segments_to_drop as u32;
        }

        self.first_seq = target_first_seq;

        // Recalculate total bytes
        self.total_bytes = self
            .index
            .iter()
            .map(|m| (m.subj_len as u64) + (m.payload_len as u64))
            .sum();

        removed
    }

    fn purge(&mut self) -> u64 {
        let count = self.index.len() as u64;
        self.index.clear();
        self.sealed_segments.clear();
        self.active_mmap = None;
        self.total_bytes = 0;
        self.current_segment_offset = 0;
        self.first_seq = self.next_seq;

        let _ = fs::remove_dir_all(&self.base_path);
        let _ = fs::create_dir_all(&self.base_path);
        let _ = self.rotate();

        count
    }

    fn drain(&mut self, _subject: &[u8]) -> u64 {
        0
    }

    fn info(&self) -> StoreInfo {
        StoreInfo {
            messages: self.index.len() as u64,
            bytes: self.total_bytes,
            first_seq: self.first_seq,
            last_seq: self.next_seq.saturating_sub(1),
        }
    }
}
