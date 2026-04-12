//! In-memory journal — Vec-backed, single-threaded.
//!
//! O(1) append, O(1) read by seq (index = seq - first_seq).
//! No limits enforcement here — the engine checks before calling.

use crate::store::{Entry, EntryRef, SeedHeader, Store, StoreError, StoreInfo, entry_matches};
use arbitro_engine_v2::catalog::fnv1a_32;
use zerocopy::byteorder::little_endian::{U32, U64};

pub struct MemoryStore {
    /// Contiguous arena for all subjects and payloads.
    data: Vec<u8>,
    /// Metadata for each entry to allow O(1) access.
    index: Vec<LogMetadata>,
    /// Contiguous seed index for zero-copy drainer access.
    seed_idx: Vec<SeedHeader>,
    next_seq: u64,
    first_seq: u64,
    total_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct LogMetadata {
    pub seq: u64,
    pub ts: u64,
    pub subj_len: u16,
    pub payload_len: u32,
    pub offset: usize,
    #[allow(dead_code)]
    pub subject_hash: u32,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            data: Vec::with_capacity(65536), // Initial 64KB
            index: Vec::with_capacity(1024),
            seed_idx: Vec::with_capacity(1024),
            next_seq: 1,
            first_seq: 1,
            total_bytes: 0,
        }
    }

    #[inline]
    fn seq_to_idx(&self, seq: u64) -> Option<usize> {
        if seq < self.first_seq {
            return None;
        }
        let est_idx = (seq - self.first_seq) as usize;
        if est_idx < self.index.len() && self.index[est_idx].seq == seq {
            Some(est_idx)
        } else {
            // Fallback for sparse indices (e.g. after subject drain)
            self.index.binary_search_by_key(&seq, |meta| meta.seq).ok()
        }
    }

    #[inline]
    fn find_lower_bound(&self, seq: u64) -> usize {
        if seq <= self.first_seq { return 0; }
        let est_idx = (seq - self.first_seq) as usize;
        if est_idx < self.index.len() && self.index[est_idx].seq == seq {
            est_idx
        } else {
            self.index.binary_search_by_key(&seq, |m| m.seq).unwrap_or_else(|i| i)
        }
    }

    #[inline]
    fn push_entry(&mut self, entry: &EntryRef<'_>, timestamp: u64) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;

        let subj_len = entry.subject.len() as u16;
        let payload_len = entry.payload.len() as u32;
        let offset = self.data.len();
        let subject_hash = fnv1a_32(entry.subject);

        // 1. Append data to arena
        self.data.extend_from_slice(entry.subject);
        self.data.extend_from_slice(entry.payload);

        // 2. Register in both indices
        self.index.push(LogMetadata {
            seq,
            ts: timestamp,
            subj_len,
            payload_len,
            offset,
            subject_hash,
        });
        self.seed_idx.push(SeedHeader {
            seq: U64::new(seq),
            subject_hash: U32::new(subject_hash),
        });

        self.total_bytes += (subj_len as u64) + (payload_len as u64);
        seq
    }

    #[inline]
    fn get_entry_view(&self, idx: usize) -> Entry<'_> {
        let meta = &self.index[idx];
        let subj_start = meta.offset;
        let payload_start = subj_start + (meta.subj_len as usize);
        let payload_end = payload_start + (meta.payload_len as usize);

        Entry {
            seq: meta.seq,
            timestamp: meta.ts,
            subject: &self.data[subj_start..payload_start],
            payload: &self.data[payload_start..payload_end],
        }
    }
}

impl Store for MemoryStore {
    #[inline]
    fn append(&mut self, entry: EntryRef<'_>, timestamp: u64) -> Result<u64, StoreError> {
        Ok(self.push_entry(&entry, timestamp))
    }

    #[inline]
    fn append_batch(&mut self, entries: &[EntryRef<'_>], timestamp: u64) -> Result<u64, StoreError> {
        if entries.is_empty() {
            return Ok(self.next_seq);
        }
        self.index.reserve(entries.len());
        self.seed_idx.reserve(entries.len());
        let first = self.next_seq;
        for entry in entries {
            self.push_entry(entry, timestamp);
        }
        Ok(first)
    }

    #[inline]
    fn read(&self, seq: u64) -> Result<Option<Entry<'_>>, StoreError> {
        Ok(self.seq_to_idx(seq).map(|idx| self.get_entry_view(idx)))
    }

    fn read_range(&self, start: u64, end: u64) -> Result<Vec<Entry<'_>>, StoreError> {
        let s = self.find_lower_bound(start);
        let e = self.find_lower_bound(end);
        let e = e.min(self.index.len());
        let s = s.min(e);
        
        let mut out = Vec::with_capacity(e - s);
        for i in s..e {
            out.push(self.get_entry_view(i));
        }
        Ok(out)
    }

    #[inline]
    fn get(&self, seq: u64, f: &mut dyn FnMut(&Entry<'_>)) -> Result<bool, StoreError> {
        match self.seq_to_idx(seq) {
            Some(idx) => {
                let entry = self.get_entry_view(idx);
                f(&entry);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn for_each(&self, start: u64, end: u64, f: &mut dyn FnMut(&Entry<'_>)) -> Result<(), StoreError> {
        let s = self.find_lower_bound(start);
        let e = self.find_lower_bound(end);
        let e = e.min(self.index.len());
        let s = s.min(e);
        
        for i in s..e {
            let entry = self.get_entry_view(i);
            f(&entry);
        }
        Ok(())
    }

    fn seed_index(&self, start: u64, end: u64) -> &[SeedHeader] {
        let s = self.find_lower_bound(start);
        let e = self.find_lower_bound(end);
        let e = e.min(self.seed_idx.len());
        let s = s.min(e);
        &self.seed_idx[s..e]
    }

    fn truncate_front(&mut self, first_seq: u64) -> u64 {
        if first_seq <= self.first_seq || self.index.is_empty() { return 0; }
        
        let idx = (first_seq - self.first_seq) as usize;
        let idx = idx.min(self.index.len());

        if idx == 0 {
            return 0;
        }

        let removed = idx as u64;
        let data_cut = self.index[idx - 1].offset + (self.index[idx - 1].subj_len as usize) + (self.index[idx - 1].payload_len as usize);

        // 1. Drain data arena (Hardware Sympathy: this is a memory move, O(N))
        self.data.drain(0..data_cut);

        // 2. Drain both indices
        self.index.drain(0..idx);
        self.seed_idx.drain(0..idx);

        // 3. Update offsets in remaining index entries
        for meta in &mut self.index {
            meta.offset -= data_cut;
        }

        self.first_seq = first_seq;
        
        // Recalculate total bytes
        self.total_bytes = self.index.iter()
            .map(|m| (m.subj_len as u64) + (m.payload_len as u64))
            .sum();

        removed
    }

    fn purge(&mut self) -> u64 {
        let count = self.index.len() as u64;
        self.data.clear();
        self.index.clear();
        self.seed_idx.clear();
        self.first_seq = self.next_seq;
        self.total_bytes = 0;
        count
    }

    fn drain(&mut self, subject: &[u8]) -> u64 {
        let mut new_data = Vec::with_capacity(self.data.len());
        let mut new_index = Vec::with_capacity(self.index.len());
        let mut new_seed = Vec::with_capacity(self.seed_idx.len());
        let mut removed = 0;
        let mut bytes = 0;

        for i in 0..self.index.len() {
            let entry = self.get_entry_view(i);
            if !entry_matches(&entry, subject) {
                let offset = new_data.len();
                new_data.extend_from_slice(entry.subject);
                new_data.extend_from_slice(entry.payload);

                let meta = self.index[i];
                new_index.push(LogMetadata {
                    offset,
                    ..meta
                });
                new_seed.push(self.seed_idx[i]);
                bytes += (meta.subj_len as u64) + (meta.payload_len as u64);
            } else {
                removed += 1;
            }
        }

        self.data = new_data;
        self.index = new_index;
        self.seed_idx = new_seed;
        self.total_bytes = bytes;

        if let Some(first) = self.index.first() {
            self.first_seq = first.seq;
        } else {
            self.first_seq = self.next_seq;
        }

        removed
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_read() {
        let mut s = MemoryStore::new();
        let seq = s.append(EntryRef { subject: b"orders.created", payload: b"{}" }, 1000).unwrap();
        assert_eq!(seq, 1);

        let e = s.read(1).unwrap().unwrap();
        assert_eq!(&*e.subject, b"orders.created");
        assert_eq!(&*e.payload, b"{}");
        assert_eq!(e.timestamp, 1000);
    }

    #[test]
    fn append_batch() {
        let mut s = MemoryStore::new();
        let entries = [
            EntryRef { subject: b"a", payload: b"1" },
            EntryRef { subject: b"b", payload: b"2" },
            EntryRef { subject: b"c", payload: b"3" },
        ];
        let first = s.append_batch(&entries, 100).unwrap();
        assert_eq!(first, 1);
        assert_eq!(s.info().messages, 3);

        let e2 = s.read(2).unwrap().unwrap();
        assert_eq!(&*e2.subject, b"b");
    }

    #[test]
    fn read_range() {
        let mut s = MemoryStore::new();
        for i in 0..5 {
            s.append(EntryRef { subject: b"x", payload: &[i] }, 0).unwrap();
        }
        let range = s.read_range(2, 5).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].seq, 2);
        assert_eq!(range[2].seq, 4);
    }

    #[test]
    fn read_not_found() {
        let s = MemoryStore::new();
        assert!(s.read(1).unwrap().is_none());
        assert!(s.read(999).unwrap().is_none());
    }

    #[test]
    fn purge() {
        let mut s = MemoryStore::new();
        for i in 0..10 {
            s.append(EntryRef { subject: b"x", payload: &[i] }, 0).unwrap();
        }
        let deleted = s.purge();
        assert_eq!(deleted, 10);
        assert_eq!(s.info().messages, 0);
        assert_eq!(s.info().first_seq, 11);

        // New appends continue from where we left off
        let seq = s.append(EntryRef { subject: b"y", payload: b"new" }, 0).unwrap();
        assert_eq!(seq, 11);
    }

    #[test]
    fn drain_by_subject() {
        let mut s = MemoryStore::new();
        s.append(EntryRef { subject: b"orders.created", payload: b"1" }, 0).unwrap();
        s.append(EntryRef { subject: b"orders.updated", payload: b"2" }, 0).unwrap();
        s.append(EntryRef { subject: b"orders.created", payload: b"3" }, 0).unwrap();
        s.append(EntryRef { subject: b"payments.done", payload: b"4" }, 0).unwrap();

        let drained = s.drain(b"orders.created");
        assert_eq!(drained, 2);
        assert_eq!(s.info().messages, 2);

        // Remaining: orders.updated (seq 2), payments.done (seq 4)
        assert!(s.read(1).unwrap().is_none());
        assert!(s.read(2).unwrap().is_some());
        assert!(s.read(3).unwrap().is_none());
        assert!(s.read(4).unwrap().is_some());
    }

    #[test]
    fn drain_with_wildcard() {
        let mut s = MemoryStore::new();
        s.append(EntryRef { subject: b"orders.created", payload: b"1" }, 0).unwrap();
        s.append(EntryRef { subject: b"orders.updated", payload: b"2" }, 0).unwrap();
        s.append(EntryRef { subject: b"payments.done", payload: b"3" }, 0).unwrap();

        let drained = s.drain(b"orders.>");
        assert_eq!(drained, 2);
        assert_eq!(s.info().messages, 1);

        let remaining = s.read(3).unwrap().unwrap();
        assert_eq!(&*remaining.subject, b"payments.done");
    }

    #[test]
    fn info_tracks_bytes() {
        let mut s = MemoryStore::new();
        s.append(EntryRef { subject: b"ab", payload: b"cd" }, 0).unwrap();
        assert_eq!(s.info().bytes, 4);
        s.append(EntryRef { subject: b"ef", payload: b"ghij" }, 0).unwrap();
        assert_eq!(s.info().bytes, 10);
    }

    #[test]
    fn empty_batch_returns_next_seq() {
        let mut s = MemoryStore::new();
        s.append(EntryRef { subject: b"x", payload: b"y" }, 0).unwrap();
        let seq = s.append_batch(&[], 0).unwrap();
        assert_eq!(seq, 2); // next would be 2
    }

    #[test]
    fn get_borrows_without_clone() {
        let mut s = MemoryStore::new();
        s.append(EntryRef { subject: b"orders.created", payload: b"{}" }, 1000).unwrap();

        let mut found = false;
        let ok = s.get(1, &mut |entry| {
            assert_eq!(&*entry.subject, b"orders.created");
            assert_eq!(&*entry.payload, b"{}");
            assert_eq!(entry.timestamp, 1000);
            found = true;
        }).unwrap();
        assert!(ok);
        assert!(found);

        // Not found
        let ok = s.get(999, &mut |_| panic!("should not be called")).unwrap();
        assert!(!ok);
    }

    #[test]
    fn for_each_borrows_without_clone() {
        let mut s = MemoryStore::new();
        for i in 0..5u8 {
            s.append(EntryRef { subject: b"x", payload: &[i] }, 0).unwrap();
        }

        let mut count = 0u32;
        let mut seqs = Vec::new();
        s.for_each(2, 5, &mut |entry| {
            seqs.push(entry.seq);
            count += 1;
        }).unwrap();
        assert_eq!(count, 3);
        assert_eq!(seqs, vec![2, 3, 4]);
    }
}
