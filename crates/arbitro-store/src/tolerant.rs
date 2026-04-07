//! Tolerant journal — Segmented Log, Mmap-based zero-copy reads/writes.
//! Strictly follows Hardware Sympathy: Zero allocations on the hot path.

use memmap2::{Mmap, MmapMut};
use std::fs;
use std::path::PathBuf;

use crate::segment::{self, SegmentMetadata, MAX_SEGMENT_BYTES};
use crate::store::{Entry, EntryRef, Store, StoreError, StoreInfo};

#[derive(Debug, Clone, Copy)]
struct LogMetadata {
    pub seq: u64,
    pub ts: u64,
    pub subj_len: u16,
    pub payload_len: u32,
    pub offset: u32,
    pub segment_idx: u32,
}

pub struct TolerantStore {
    base_path: PathBuf,
    active_mmap: Option<MmapMut>,
    active_segment_id: u64,
    sealed_segments: Vec<Mmap>,
    segments: Vec<SegmentMetadata>,
    index: Vec<LogMetadata>,
    next_seq: u64,
    first_seq: u64,
    total_bytes: u64,
    current_segment_offset: u32,
}

const MAGIC: u8 = 0xAF;
const HEADER_SIZE: usize = 23;

impl TolerantStore {
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path,
            active_mmap: None,
            active_segment_id: 0,
            sealed_segments: Vec::new(),
            segments: Vec::new(),
            index: Vec::new(), // Pre-allocated in init()
            next_seq: 1,
            first_seq: 1,
            total_bytes: 0,
            current_segment_offset: 0,
        }
    }

    fn rotate(&mut self) -> Result<(), StoreError> {
        if let Some(mmap) = self.active_mmap.take() {
            let path = segment::segment_path(&self.base_path, self.active_segment_id);
            let sealed = segment::seal_segment(&path, self.current_segment_offset)
                .map_err(|_| StoreError::Full)?;
            self.sealed_segments.push(sealed);
            drop(mmap);
        }

        self.active_segment_id = self.next_seq;
        let path = segment::segment_path(&self.base_path, self.active_segment_id);
        let mmap = segment::create_active_segment(&path).map_err(|_| StoreError::NotFound)?;
        
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
            .filter_map(|e| e.ok()).map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |ext| ext == "log"))
            .collect();
        paths.sort();

        for path in paths {
            self.load_segment(&path)?;
        }
        Ok(())
    }

    fn load_segment(&mut self, path: &std::path::Path) -> Result<(), StoreError> {
        let file = fs::File::open(path).map_err(|_| StoreError::NotFound)?;
        let len = file.metadata().map(|m| m.len()).unwrap_or(0);
        if len < HEADER_SIZE as u64 { return Ok(()); }

        let mmap = unsafe { Mmap::map(&file).map_err(|_| StoreError::NotFound)? };
        let segment_idx = self.sealed_segments.len() as u32;
        let mut offset = 0usize;
        let mut first = 0;
        let mut last = 0;

        while offset + HEADER_SIZE <= mmap.len() {
            let h = &mmap[offset..offset+HEADER_SIZE];
            
            // CRASH CONSISTENCY: Stop if Magic is missing (end of valid data in pre-allocated segment)
            if h[0] != MAGIC {
                break;
            }

            let subj_len = u16::from_le_bytes([h[1], h[2]]);
            let payload_len = u32::from_le_bytes([h[3], h[4], h[5], h[6]]);
            let ts = u64::from_le_bytes([h[7], h[8], h[9], h[10], h[11], h[12], h[13], h[14]]);
            let seq = u64::from_le_bytes([h[15], h[16], h[17], h[18], h[19], h[20], h[21], h[22]]);

            if first == 0 { first = seq; }
            last = seq;
            self.index.push(LogMetadata { seq, ts, subj_len, payload_len, offset: (offset + HEADER_SIZE) as u32, segment_idx });
            
            offset += HEADER_SIZE + (subj_len as usize) + (payload_len as usize);
            self.next_seq = seq + 1;
            self.total_bytes += (subj_len as u64) + (payload_len as u64);
        }

        if first != 0 {
            self.segments.push(SegmentMetadata { first_seq: first, last_seq: last });
            self.sealed_segments.push(mmap);
        }
        Ok(())
    }
}

impl Store for TolerantStore {
    fn init(&mut self) -> Result<(), StoreError> {
        // Pre-allocate index to 1M entries for zero-alloc hot path
        // WHY: Realloc on hot path violates Hardware Sympathy.
        self.index = Vec::with_capacity(1_000_000);
        self.scan_segments()?;
        if self.active_mmap.is_none() { self.rotate()?; }
        if let Some(f) = self.index.first() { self.first_seq = f.seq; }
        Ok(())
    }

    fn append(&mut self, entry: EntryRef<'_>, timestamp: u64) -> Result<u64, StoreError> {
        let entry_total = (entry.subject.len() + entry.payload.len()) as u64;
        let total_needed = (HEADER_SIZE as u64) + entry_total;

        if (self.current_segment_offset as u64) + total_needed >= MAX_SEGMENT_BYTES {
            self.rotate()?;
        }

        let seq = self.next_seq;
        let mmap = self.active_mmap.as_mut().ok_or(StoreError::NotFound)?;
        let start = self.current_segment_offset as usize;

        let slen = entry.subject.len() as u16;
        let plen = entry.payload.len() as u32;

        mmap[start] = MAGIC;
        mmap[start + 1..start + 3].copy_from_slice(&slen.to_le_bytes());
        mmap[start + 3..start + 7].copy_from_slice(&plen.to_le_bytes());
        mmap[start + 7..start + 15].copy_from_slice(&timestamp.to_le_bytes());
        mmap[start + 15..start + 23].copy_from_slice(&seq.to_le_bytes());

        let data_off = start + HEADER_SIZE;
        mmap[data_off..data_off + entry.subject.len()].copy_from_slice(entry.subject);
        mmap[data_off + entry.subject.len()..data_off + entry.subject.len() + entry.payload.len()]
            .copy_from_slice(entry.payload);

        // WHY: Zero-allocation push (capacity guaranteed by init())
        self.index.push(LogMetadata { seq, ts: timestamp, subj_len: slen, payload_len: plen, offset: data_off as u32, segment_idx: self.sealed_segments.len() as u32 });

        self.next_seq += 1;
        self.total_bytes += entry_total;
        self.current_segment_offset += total_needed as u32;
        Ok(seq)
    }

    fn get(&self, seq: u64, f: &mut dyn FnMut(&Entry<'_>)) -> Result<bool, StoreError> {
        if seq < self.first_seq { return Ok(false); }
        let idx = (seq - self.first_seq) as usize;
        if idx >= self.index.len() { return Ok(false); }
        let m = &self.index[idx];
        let data = if m.segment_idx == self.sealed_segments.len() as u32 {
            self.active_mmap.as_ref().map(|m| &m[..]).ok_or(StoreError::NotFound)?
        } else {
            &self.sealed_segments[m.segment_idx as usize][..]
        };

        let sub_end = (m.offset as usize) + (m.subj_len as usize);
        f(&Entry { seq: m.seq, timestamp: m.ts, subject: &data[m.offset as usize..sub_end], payload: &data[sub_end..sub_end + (m.payload_len as usize)] });
        Ok(true)
    }

    fn truncate_front(&mut self, target: u64) -> u64 {
        if self.index.is_empty() || target <= self.first_seq { return 0; }
        
        let idx = (target - self.first_seq) as usize;
        let idx = idx.min(self.index.len());
        if idx == 0 { return 0; }

        let mut dropped = 0;
        while !self.segments.is_empty() && self.segments[0].last_seq < target {
            let path = segment::segment_path(&self.base_path, self.segments[0].first_seq);
            let _ = fs::remove_file(path);
            self.sealed_segments.remove(0);
            self.segments.remove(0);
            dropped += 1;
        }

        self.index.drain(0..idx);
        if dropped > 0 {
            for m in &mut self.index { m.segment_idx -= dropped as u32; }
        }
        self.first_seq = target;
        self.total_bytes = self.index.iter().map(|m| (m.subj_len as u64) + (m.payload_len as u64)).sum();
        idx as u64
    }

    fn shutdown(&mut self) -> Result<(), StoreError> {
        if let Some(mmap) = self.active_mmap.take() {
            let _ = mmap.flush();
            let path = segment::segment_path(&self.base_path, self.active_segment_id);
            let _ = segment::seal_segment(&path, self.current_segment_offset);
        }
        Ok(())
    }

    fn purge(&mut self) -> u64 {
        let count = self.index.len() as u64;
        self.index.clear();
        self.sealed_segments.clear();
        self.segments.clear();
        self.active_mmap = None;
        let _ = fs::remove_dir_all(&self.base_path);
        let _ = fs::create_dir_all(&self.base_path);
        count
    }

    fn info(&self) -> StoreInfo {
        StoreInfo { messages: self.index.len() as u64, bytes: self.total_bytes, first_seq: self.first_seq, last_seq: self.next_seq.saturating_sub(1) }
    }

    fn append_batch(&mut self, entries: &[EntryRef<'_>], ts: u64) -> Result<u64, StoreError> {
        if entries.is_empty() { return Ok(self.next_seq); }
        let first = self.next_seq;
        for e in entries { self.append(*e, ts)?; }
        Ok(first)
    }
    
    fn read(&self, _: u64) -> Result<Option<Entry<'_>>, StoreError> { Err(StoreError::NotFound) }
    fn read_range(&self, _: u64, _: u64) -> Result<Vec<Entry<'_>>, StoreError> { Err(StoreError::NotFound) }
    fn drain(&mut self, _: &[u8]) -> u64 { 0 }
    fn for_each(&self, s: u64, e: u64, f: &mut dyn FnMut(&Entry<'_>)) -> Result<(), StoreError> {
        let start = if s < self.first_seq { 0 } else { (s - self.first_seq) as usize };
        let end = if e < self.first_seq { 0 } else { (e - self.first_seq) as usize };
        let end = end.min(self.index.len());
        let start = start.min(end);
        for i in start..end { self.get(self.index[i].seq, f)?; }
        Ok(())
    }
}
