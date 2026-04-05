//! In-memory journal — Vec-backed, single-threaded.
//!
//! O(1) append, O(1) read by seq (index = seq - first_seq).
//! No limits enforcement here — the engine checks before calling.

use crate::store::{Entry, EntryRef, Store, StoreError, StoreInfo, entry_matches};

pub struct MemoryStore {
    entries: Vec<Entry>,
    next_seq: u64,
    first_seq: u64,
    total_bytes: u64,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_seq: 1,
            first_seq: 1,
            total_bytes: 0,
        }
    }

    #[inline]
    fn seq_to_idx(&self, seq: u64) -> Option<usize> {
        if seq < self.first_seq || seq >= self.next_seq {
            return None;
        }
        Some((seq - self.first_seq) as usize)
    }

    #[inline]
    fn entry_bytes(subject: &[u8], payload: &[u8]) -> u64 {
        (subject.len() + payload.len()) as u64
    }

    #[inline]
    fn push_entry(&mut self, entry: &EntryRef<'_>, timestamp: u64) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.total_bytes += Self::entry_bytes(entry.subject, entry.payload);
        self.entries.push(Entry {
            seq,
            timestamp,
            subject: Box::from(entry.subject),
            payload: Box::from(entry.payload),
        });
        seq
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
        self.entries.reserve(entries.len());
        let first = self.next_seq;
        for entry in entries {
            self.push_entry(entry, timestamp);
        }
        Ok(first)
    }

    #[inline]
    fn read(&self, seq: u64) -> Result<Option<Entry>, StoreError> {
        Ok(self.seq_to_idx(seq).map(|idx| self.entries[idx].clone()))
    }

    fn read_range(&self, start: u64, end: u64) -> Result<Vec<Entry>, StoreError> {
        let s = self.seq_to_idx(start).unwrap_or(0);
        let e = self.seq_to_idx(end.saturating_sub(1))
            .map(|i| i + 1)
            .unwrap_or(self.entries.len());
        Ok(self.entries[s..e].to_vec())
    }

    #[inline]
    fn get(&self, seq: u64, f: &mut dyn FnMut(&Entry)) -> Result<bool, StoreError> {
        match self.seq_to_idx(seq) {
            Some(idx) => {
                f(&self.entries[idx]);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn for_each(&self, start: u64, end: u64, f: &mut dyn FnMut(&Entry)) -> Result<(), StoreError> {
        let s = self.seq_to_idx(start).unwrap_or(0);
        let e = self.seq_to_idx(end.saturating_sub(1))
            .map(|i| i + 1)
            .unwrap_or(self.entries.len());
        for entry in &self.entries[s..e] {
            f(entry);
        }
        Ok(())
    }

    fn purge(&mut self) -> u64 {
        let count = self.entries.len() as u64;
        self.entries.clear();
        self.first_seq = self.next_seq;
        self.total_bytes = 0;
        count
    }

    fn drain(&mut self, subject: &[u8]) -> u64 {
        let before = self.entries.len();
        self.entries.retain(|e| !entry_matches(e, subject));
        let removed = (before - self.entries.len()) as u64;

        // Recalculate bytes and first_seq
        self.total_bytes = self.entries.iter()
            .map(|e| (e.subject.len() + e.payload.len()) as u64)
            .sum();
        if let Some(first) = self.entries.first() {
            self.first_seq = first.seq;
        } else {
            self.first_seq = self.next_seq;
        }

        removed
    }

    fn info(&self) -> StoreInfo {
        StoreInfo {
            messages: self.entries.len() as u64,
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
