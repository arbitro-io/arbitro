//! In-memory journal — segmented, append-only, backed by **anonymous mmap**.
//!
//! Each segment is a fixed-size anonymous `MmapMut` allocated once. When the
//! active segment fills up it is sealed (frozen, read-only) and a fresh
//! segment is mmap'd for further writes. This avoids the realloc chain
//! of a single growing `Vec<u8>` AND matches the memory layout properties
//! of `TolerantStore`: page-aligned (4 KB boundaries), OS-managed prefetch,
//! and a virtual-address region separate from the process heap.
//!
//! The only difference vs `TolerantStore` is the backing storage:
//! `MemoryStore` uses `MmapMut::map_anon(..)` (pure RAM, no file, no
//! persistence) while `TolerantStore` maps a real file for durability.

use crate::store::{entry_matches, Entry, EntryRef, Store, StoreError, StoreInfo};
use arbitro_engine_v2::catalog::wire_hash_32;
use memmap2::{MmapMut, MmapOptions};

/// Default capacity per segment. Chosen as a balance between
/// per-segment overhead and per-store footprint for workloads that
/// do not use `with_capacity`.
pub const DEFAULT_SEGMENT_SIZE: usize = 16 * 1024 * 1024;

/// Minimum sensible segment size (no point going below a page).
const MIN_SEGMENT_SIZE: usize = 4 * 1024;

pub struct MemoryStore {
    /// Active segment — anonymous mmap, fixed size. Page-aligned.
    active: MmapMut,
    /// Bytes written into `active`. Writes advance this counter; the
    /// unused tail is never read.
    active_len: usize,
    /// Sealed segments, in insertion order. Held as `MmapMut` (not
    /// `Mmap`) to avoid a frozen/unfrozen split in the type system;
    /// the code treats them as read-only (only reads ever occur).
    sealed: Vec<MmapMut>,
    /// Number of used bytes in each sealed segment (for bounds-safe reads).
    sealed_lens: Vec<usize>,
    /// Global index: seq → location.
    index: Vec<LogMetadata>,
    next_seq: u64,
    first_seq: u64,
    total_bytes: u64,
    /// Fixed size of each mmap segment. Configured at construction.
    segment_size: usize,
}

#[derive(Debug, Clone, Copy)]
struct LogMetadata {
    pub seq: u64,
    pub ts: u64,
    pub subj_len: u16,
    pub payload_len: u32,
    /// Offset within the segment identified by `segment_idx`.
    pub offset: u32,
    /// Index into `sealed` (if `< sealed.len()`) or the active segment
    /// (if `== sealed.len()`).
    pub segment_idx: u32,
    #[allow(dead_code)]
    pub subject_hash: u32,
    pub stream_id: u32,
    pub flags: u8,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    /// Create a store with `DEFAULT_SEGMENT_SIZE` (16 MB) per segment
    /// and a small default index capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_SEGMENT_SIZE, 1024)
    }

    /// Construct with an explicit minimum data capacity and index capacity.
    /// The actual `segment_size` is `max(data_cap, DEFAULT_SEGMENT_SIZE)`
    /// so that for small expected workloads a single segment suffices
    /// (avoiding rotation overhead) while huge workloads get a larger
    /// backing segment.
    pub fn with_capacity(data_cap: usize, index_cap: usize) -> Self {
        let segment_size = data_cap.max(DEFAULT_SEGMENT_SIZE).max(MIN_SEGMENT_SIZE);
        Self::with_segment_size(segment_size, index_cap)
    }

    /// Explicit segment size — useful for tests or when the caller knows
    /// the optimal per-segment budget.
    pub fn with_segment_size(segment_size: usize, index_cap: usize) -> Self {
        let segment_size = segment_size.max(MIN_SEGMENT_SIZE);
        let active = alloc_anon_segment(segment_size);
        Self {
            active,
            active_len: 0,
            sealed: Vec::new(),
            sealed_lens: Vec::new(),
            index: Vec::with_capacity(index_cap),
            next_seq: 1,
            first_seq: 1,
            total_bytes: 0,
            segment_size,
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
            self.index.binary_search_by_key(&seq, |meta| meta.seq).ok()
        }
    }

    #[inline]
    fn find_lower_bound(&self, seq: u64) -> usize {
        if seq <= self.first_seq {
            return 0;
        }
        let est_idx = (seq - self.first_seq) as usize;
        if est_idx < self.index.len() && self.index[est_idx].seq == seq {
            est_idx
        } else {
            self.index
                .binary_search_by_key(&seq, |m| m.seq)
                .unwrap_or_else(|i| i)
        }
    }

    /// Seal the active segment, allocate a fresh one. Called when the
    /// next entry would exceed the active segment's capacity.
    #[inline(never)]
    fn rotate(&mut self) {
        let new_active = alloc_anon_segment(self.segment_size);
        let full_len = self.active_len;
        let full = std::mem::replace(&mut self.active, new_active);
        self.sealed.push(full);
        self.sealed_lens.push(full_len);
        self.active_len = 0;
    }

    /// Return an immutable slice into the segment identified by `segment_idx`,
    /// bounded to the number of bytes actually used in that segment.
    #[inline]
    fn segment_slice(&self, segment_idx: u32) -> &[u8] {
        let idx = segment_idx as usize;
        if idx < self.sealed.len() {
            &self.sealed[idx][..self.sealed_lens[idx]]
        } else {
            debug_assert_eq!(idx, self.sealed.len());
            &self.active[..self.active_len]
        }
    }

    #[inline]
    fn push_entry(&mut self, entry: &EntryRef<'_>, timestamp: u64) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;

        let subj_len = entry.subject.len() as u16;
        let payload_len = entry.payload.len() as u32;
        let entry_bytes = (subj_len as usize) + (payload_len as usize);
        let subject_hash = wire_hash_32(entry.subject);

        // Rotate if this entry wouldn't fit in the active segment.
        if self.active_len + entry_bytes > self.segment_size {
            self.rotate();
        }

        let segment_idx = self.sealed.len() as u32;
        let offset = self.active_len as u32;

        // Direct mmap writes: no growable Vec, no bounds check branch for
        // capacity (we already rotated if needed).
        let subj_end = self.active_len + (subj_len as usize);
        let pld_end = subj_end + (payload_len as usize);
        self.active[self.active_len..subj_end].copy_from_slice(entry.subject);
        self.active[subj_end..pld_end].copy_from_slice(entry.payload);
        self.active_len = pld_end;

        self.index.push(LogMetadata {
            seq,
            ts: timestamp,
            subj_len,
            payload_len,
            offset,
            segment_idx,
            subject_hash,
            stream_id: entry.stream_id,
            flags: entry.flags,
        });

        self.total_bytes += entry_bytes as u64;
        seq
    }

    #[inline]
    fn get_entry_view(&self, idx: usize) -> Entry<'_> {
        let meta = &self.index[idx];
        let data = self.segment_slice(meta.segment_idx);
        let subj_start = meta.offset as usize;
        let payload_start = subj_start + (meta.subj_len as usize);
        let payload_end = payload_start + (meta.payload_len as usize);

        Entry {
            seq: meta.seq,
            stream_id: meta.stream_id,
            timestamp: meta.ts,
            subject: &data[subj_start..payload_start],
            payload: &data[payload_start..payload_end],
            flags: meta.flags,
        }
    }

    /// Low-level iteration — hands the caller the **raw contiguous bytes**
    /// `[subject ++ payload]` plus scalar metadata, without constructing
    /// an `Entry<'_>` struct or splitting the byte range.
    ///
    /// Intended for benchmarks and specialized drain paths that want to
    /// skip the per-entry slice arithmetic. The callback receives:
    ///   - `seq`, `stream_id`, `timestamp`, `flags` — all from the index
    ///   - `subj_len` — first `subj_len` bytes of `bytes` are the subject
    ///   - `bytes` — the full `subject ++ payload` byte range
    ///
    /// Subject is `bytes[..subj_len]`, payload is `bytes[subj_len..]`.
    /// The caller performs the split (usually not needed at all when the
    /// hot path just wants to `writev` the bytes).
    #[inline]
    pub fn for_each_raw(
        &self,
        start: u64,
        end: u64,
        f: &mut dyn FnMut(RawEntry<'_>),
    ) -> Result<(), StoreError> {
        let s = self.find_lower_bound(start);
        let e = self.find_lower_bound(end).min(self.index.len());
        let s = s.min(e);

        for i in s..e {
            let meta = &self.index[i];
            let data = self.segment_slice(meta.segment_idx);
            let subj_start = meta.offset as usize;
            let total_len = (meta.subj_len as usize) + (meta.payload_len as usize);
            let bytes = &data[subj_start..subj_start + total_len];
            f(RawEntry {
                seq: meta.seq,
                stream_id: meta.stream_id,
                timestamp: meta.ts,
                subj_len: meta.subj_len,
                flags: meta.flags,
                bytes,
            });
        }
        Ok(())
    }
}

/// Raw-view of a stored entry — hands back the contiguous `subject ++ payload`
/// bytes along with scalar metadata. Used by `MemoryStore::for_each_raw`.
///
/// Contrast with `Entry<'a>`, which constructs two separate `&[u8]` slices
/// (one for subject, one for payload). When the caller is going to encode
/// the bytes back-to-back anyway, skipping the split saves the subtraction
/// and second slice range check per entry.
#[derive(Debug, Clone, Copy)]
pub struct RawEntry<'a> {
    pub seq: u64,
    pub stream_id: u32,
    pub timestamp: u64,
    pub subj_len: u16,
    pub flags: u8,
    /// Contiguous bytes: `subject ++ payload`.
    /// Use `.subject()` / `.payload()` for split views.
    pub bytes: &'a [u8],
}

impl<'a> RawEntry<'a> {
    /// Subject bytes — first `subj_len` bytes of `self.bytes`.
    #[inline(always)]
    pub fn subject(&self) -> &'a [u8] {
        &self.bytes[..self.subj_len as usize]
    }

    /// Payload bytes — everything after the subject.
    #[inline(always)]
    pub fn payload(&self) -> &'a [u8] {
        &self.bytes[self.subj_len as usize..]
    }

    /// Length of payload (derived from total bytes minus subject).
    #[inline(always)]
    pub fn payload_len(&self) -> usize {
        self.bytes.len() - self.subj_len as usize
    }
}

/// Zero-copy view over a stored entry's metadata — a direct reference
/// into the store's `index: Vec<EntryMeta>`. Callbacks that only read
/// fields skip the `RawEntry` field-copy construction and instead
/// dereference this view in-place.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct EntryMeta {
    pub seq: u64,
    pub ts: u64,
    pub subj_len: u16,
    pub payload_len: u32,
    pub subject_hash: u32,
    pub stream_id: u32,
    pub flags: u8,
}

/// Low-level iteration that exposes a DIRECT reference into the store's
/// metadata index, plus the contiguous bytes. No struct copy, no field
/// projection — the callback reads fields through the `&EntryMeta` ref
/// directly (compile-time field offsets, cache-hot loads).
///
/// This is the absolute fastest iteration path the store can offer.
impl MemoryStore {
    #[inline]
    pub fn for_each_view(
        &self,
        start: u64,
        end: u64,
        f: &mut dyn FnMut(&EntryMeta, &[u8]),
    ) -> Result<(), StoreError> {
        let s = self.find_lower_bound(start);
        let e = self.find_lower_bound(end).min(self.index.len());
        let s = s.min(e);

        for i in s..e {
            let meta = &self.index[i];
            let data = self.segment_slice(meta.segment_idx);
            let subj_start = meta.offset as usize;
            let total_len = (meta.subj_len as usize) + (meta.payload_len as usize);
            let bytes = &data[subj_start..subj_start + total_len];
            // Project a reference to a public EntryMeta. Since LogMetadata
            // and EntryMeta have compatible layouts for the exported fields,
            // we build a stack-local EntryMeta on the fly — cheap Copy.
            let view = EntryMeta {
                seq: meta.seq,
                ts: meta.ts,
                subj_len: meta.subj_len,
                payload_len: meta.payload_len,
                subject_hash: meta.subject_hash,
                stream_id: meta.stream_id,
                flags: meta.flags,
            };
            f(&view, bytes);
        }
        Ok(())
    }
}

impl Store for MemoryStore {
    #[inline]
    fn append(&mut self, entry: EntryRef<'_>, timestamp: u64) -> Result<u64, StoreError> {
        Ok(self.push_entry(&entry, timestamp))
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
        // Reserve in the index Vec once — avoids N reallocations as
        // entries are pushed (each push could otherwise trigger a
        // grow, copying O(index.len()) bytes).
        self.index.reserve(entries.len());
        let first = self.next_seq;
        for entry in entries {
            // `push_entry` already handles per-entry rotate, hash,
            // mmap write, and index push. Coalescing the rotate check
            // up-front would be incorrect — entries can be huge enough
            // that the batch needs mid-flight rotation, and a single
            // up-front check would either over-allocate or miss the
            // boundary. The per-entry check is one integer compare;
            // dominant cost is the byte copy, which is unavoidable.
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

    fn for_each(
        &self,
        start: u64,
        end: u64,
        f: &mut dyn FnMut(&Entry<'_>),
    ) -> Result<(), StoreError> {
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

    fn truncate_front(&mut self, first_seq: u64) -> u64 {
        if first_seq <= self.first_seq || self.index.is_empty() {
            return 0;
        }
        let cut = (first_seq - self.first_seq) as usize;
        let cut = cut.min(self.index.len());
        if cut == 0 {
            return 0;
        }

        let removed = cut as u64;

        // F24: compute dropped bytes from the cut prefix in O(cut)
        // instead of O(remaining). On 1M-entry stores this turns a
        // multi-ms stall under the publish lock into a sub-ms hit.
        let dropped_bytes: u64 = self.index[..cut]
            .iter()
            .map(|m| (m.subj_len as u64) + (m.payload_len as u64))
            .sum();

        // Determine how many whole sealed segments are fully dropped
        // (all their entries have seq < first_seq).
        let remaining_start_seg = if cut < self.index.len() {
            self.index[cut].segment_idx as usize
        } else {
            // Everything removed — all sealed + active are now stale.
            self.sealed.len() + 1
        };

        // Drop fully-stale sealed segments. Surviving entries keep the
        // same `segment_idx` values *minus* how many we dropped.
        let dropped_segments = remaining_start_seg.min(self.sealed.len());
        if dropped_segments > 0 {
            self.sealed.drain(0..dropped_segments);
            self.sealed_lens.drain(0..dropped_segments);
            // Reindex surviving entries.
            for m in &mut self.index[cut..] {
                m.segment_idx -= dropped_segments as u32;
            }
        }

        // Drain the dead index entries.
        self.index.drain(0..cut);
        self.first_seq = first_seq;

        // Incremental update — no full re-walk.
        self.total_bytes = self.total_bytes.saturating_sub(dropped_bytes);

        removed
    }

    fn purge(&mut self) -> u64 {
        let count = self.index.len() as u64;
        // Reset active segment by zeroing its length. The mmap region stays
        // resident — we just re-use it for future appends.
        self.active_len = 0;
        self.sealed.clear();
        self.sealed_lens.clear();
        self.index.clear();
        self.first_seq = self.next_seq;
        self.total_bytes = 0;
        count
    }

    fn drain(&mut self, subject: &[u8]) -> u64 {
        // Cold path: rebuild all segments + index from scratch, skipping
        // entries that match `subject`. This compacts gaps left by
        // removed entries.
        let mut new_store = MemoryStore::with_segment_size(self.segment_size, self.index.len());
        let mut removed = 0u64;

        for i in 0..self.index.len() {
            let entry = self.get_entry_view(i);
            if entry_matches(&entry, subject) {
                removed += 1;
            } else {
                let meta = &self.index[i];
                // Use push_entry to let the new store handle rotation.
                let seq_override = meta.seq;
                let ts = meta.ts;
                new_store.next_seq = seq_override;
                let _ = new_store.push_entry(
                    &EntryRef {
                        stream_id: meta.stream_id,
                        subject: entry.subject,
                        payload: entry.payload,
                        flags: meta.flags,
                        deliver_at_ms: 0,
                    },
                    ts,
                );
            }
        }

        // Preserve sequence continuity: next_seq = original next_seq.
        new_store.next_seq = self.next_seq;
        new_store.first_seq = new_store
            .index
            .first()
            .map(|m| m.seq)
            .unwrap_or(self.next_seq);

        *self = new_store;
        removed
    }

    fn tombstone_at(&mut self, seq: u64) -> bool {
        match self.seq_to_idx(seq) {
            Some(idx) => {
                let flags = &mut self.index[idx].flags;
                if *flags & crate::store::flags::TOMBSTONE != 0 {
                    return false; // already tombstoned
                }
                *flags |= crate::store::flags::TOMBSTONE;
                true
            }
            None => false,
        }
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

/// Allocate an anonymous mmap of exactly `size` bytes, zero-initialised.
/// Panics if the mapping fails — this is a constructor helper and an
/// OOM/ENOMEM at this point is unrecoverable for the caller anyway.
#[inline]
fn alloc_anon_segment(size: usize) -> MmapMut {
    MmapOptions::new()
        .len(size)
        .map_anon()
        .expect("MemoryStore: anonymous mmap failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_read() {
        let mut s = MemoryStore::new();
        let seq = s
            .append(
                EntryRef {
                    subject: b"orders.created",
                    payload: b"{}",
                    stream_id: 0,
                    flags: 0,
                    deliver_at_ms: 0,
                },
                1000,
            )
            .unwrap();
        assert_eq!(seq, 1);

        let e = s.read(1).unwrap().unwrap();
        assert_eq!(e.subject, b"orders.created");
        assert_eq!(e.payload, b"{}");
        assert_eq!(e.timestamp, 1000);
    }

    #[test]
    fn append_batch() {
        let mut s = MemoryStore::new();
        let entries = [
            EntryRef {
                subject: b"a",
                payload: b"1",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            EntryRef {
                subject: b"b",
                payload: b"2",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            EntryRef {
                subject: b"c",
                payload: b"3",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
        ];
        let first = s.append_batch(&entries, 100).unwrap();
        assert_eq!(first, 1);
        assert_eq!(s.info().messages, 3);

        let e2 = s.read(2).unwrap().unwrap();
        assert_eq!(e2.subject, b"b");
    }

    #[test]
    fn read_range() {
        let mut s = MemoryStore::new();
        for i in 0..5 {
            s.append(
                EntryRef {
                    subject: b"x",
                    payload: &[i],
                    stream_id: 0,
                    flags: 0,
                    deliver_at_ms: 0,
                },
                0,
            )
            .unwrap();
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
            s.append(
                EntryRef {
                    subject: b"x",
                    payload: &[i],
                    stream_id: 0,
                    flags: 0,
                    deliver_at_ms: 0,
                },
                0,
            )
            .unwrap();
        }
        let deleted = s.purge();
        assert_eq!(deleted, 10);
        assert_eq!(s.info().messages, 0);
        assert_eq!(s.info().first_seq, 11);

        let seq = s
            .append(
                EntryRef {
                    subject: b"y",
                    payload: b"new",
                    stream_id: 0,
                    flags: 0,
                    deliver_at_ms: 0,
                },
                0,
            )
            .unwrap();
        assert_eq!(seq, 11);
    }

    #[test]
    fn drain_by_subject() {
        let mut s = MemoryStore::new();
        s.append(
            EntryRef {
                subject: b"orders.created",
                payload: b"1",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            0,
        )
        .unwrap();
        s.append(
            EntryRef {
                subject: b"orders.updated",
                payload: b"2",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            0,
        )
        .unwrap();
        s.append(
            EntryRef {
                subject: b"orders.created",
                payload: b"3",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            0,
        )
        .unwrap();
        s.append(
            EntryRef {
                subject: b"payments.done",
                payload: b"4",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            0,
        )
        .unwrap();

        let drained = s.drain(b"orders.created");
        assert_eq!(drained, 2);
        assert_eq!(s.info().messages, 2);

        assert!(s.read(1).unwrap().is_none());
        assert!(s.read(2).unwrap().is_some());
        assert!(s.read(3).unwrap().is_none());
        assert!(s.read(4).unwrap().is_some());
    }

    #[test]
    fn drain_with_wildcard() {
        let mut s = MemoryStore::new();
        s.append(
            EntryRef {
                subject: b"orders.created",
                payload: b"1",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            0,
        )
        .unwrap();
        s.append(
            EntryRef {
                subject: b"orders.updated",
                payload: b"2",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            0,
        )
        .unwrap();
        s.append(
            EntryRef {
                subject: b"payments.done",
                payload: b"3",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            0,
        )
        .unwrap();

        let drained = s.drain(b"orders.>");
        assert_eq!(drained, 2);
        assert_eq!(s.info().messages, 1);

        let remaining = s.read(3).unwrap().unwrap();
        assert_eq!(remaining.subject, b"payments.done");
    }

    #[test]
    fn info_tracks_bytes() {
        let mut s = MemoryStore::new();
        s.append(
            EntryRef {
                subject: b"ab",
                payload: b"cd",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            0,
        )
        .unwrap();
        assert_eq!(s.info().bytes, 4);
        s.append(
            EntryRef {
                subject: b"ef",
                payload: b"ghij",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            0,
        )
        .unwrap();
        assert_eq!(s.info().bytes, 10);
    }

    #[test]
    fn empty_batch_returns_next_seq() {
        let mut s = MemoryStore::new();
        s.append(
            EntryRef {
                subject: b"x",
                payload: b"y",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            0,
        )
        .unwrap();
        let seq = s.append_batch(&[], 0).unwrap();
        assert_eq!(seq, 2);
    }

    #[test]
    fn for_each_borrows_without_clone() {
        let mut s = MemoryStore::new();
        s.append(
            EntryRef {
                subject: b"a",
                payload: b"1",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            0,
        )
        .unwrap();
        s.append(
            EntryRef {
                subject: b"b",
                payload: b"2",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            0,
        )
        .unwrap();

        let mut seen = 0;
        s.for_each(1, 3, &mut |e| {
            assert!(matches!(e.subject, b"a" | b"b"));
            seen += 1;
        })
        .unwrap();
        assert_eq!(seen, 2);
    }

    #[test]
    fn get_borrows_without_clone() {
        let mut s = MemoryStore::new();
        s.append(
            EntryRef {
                subject: b"hello",
                payload: b"world",
                stream_id: 0,
                flags: 0,
                deliver_at_ms: 0,
            },
            42,
        )
        .unwrap();

        let mut captured_ts = 0;
        let found = s
            .get(1, &mut |e| {
                captured_ts = e.timestamp;
                assert_eq!(e.subject, b"hello");
            })
            .unwrap();
        assert!(found);
        assert_eq!(captured_ts, 42);
    }

    /// Verify rotation actually happens when entries exceed segment_size.
    #[test]
    fn rotation_when_segment_full() {
        let mut s = MemoryStore::with_segment_size(MIN_SEGMENT_SIZE, 16);
        // MIN_SEGMENT_SIZE = 4096. Each entry here is 1 + 1024 = 1025 bytes,
        // so 4 entries fit in one segment, the 5th triggers rotation.
        let payload = vec![0u8; 1024];
        for _ in 0..6 {
            s.append(
                EntryRef {
                    subject: b"x",
                    payload: &payload,
                    stream_id: 0,
                    flags: 0,
                    deliver_at_ms: 0,
                },
                0,
            )
            .unwrap();
        }
        // 6 entries, 2 segments expected (4 + 2 = 6 under 4096 each).
        assert_eq!(s.info().messages, 6);
        assert!(
            !s.sealed.is_empty(),
            "at least one segment should be sealed"
        );

        // Verify we can still read across segments.
        for i in 1..=6 {
            let e = s.read(i).unwrap().expect("entry present");
            assert_eq!(e.payload.len(), 1024);
        }

        let mut seen = 0;
        s.for_each(1, 7, &mut |_| seen += 1).unwrap();
        assert_eq!(seen, 6);
    }

    #[test]
    fn tombstone_at_marks_entry() {
        let mut s = MemoryStore::new();
        for i in 0..5u8 {
            s.append(
                EntryRef {
                    subject: b"orders",
                    payload: &[i],
                    stream_id: 0,
                    flags: 0,
                    deliver_at_ms: 0,
                },
                0,
            )
            .unwrap();
        }

        // Tombstone seq 3
        assert!(s.tombstone_at(3));
        // Idempotent — second call returns false
        assert!(!s.tombstone_at(3));
        // Non-existent seq
        assert!(!s.tombstone_at(999));

        // Read still returns the entry (tombstone is metadata)
        let e = s.read(3).unwrap().unwrap();
        assert_eq!(e.flags & crate::store::flags::TOMBSTONE, crate::store::flags::TOMBSTONE);

        // Non-tombstoned entries are clean
        let e2 = s.read(2).unwrap().unwrap();
        assert_eq!(e2.flags & crate::store::flags::TOMBSTONE, 0);
    }

    #[test]
    fn tombstone_across_sealed_segments() {
        let mut s = MemoryStore::with_segment_size(MIN_SEGMENT_SIZE, 16);
        let payload = vec![0u8; 1024];
        // Append enough to trigger rotation (5 entries ~5KB > 4096)
        for _ in 0..6 {
            s.append(
                EntryRef {
                    subject: b"x",
                    payload: &payload,
                    stream_id: 0,
                    flags: 0,
                    deliver_at_ms: 0,
                },
                0,
            )
            .unwrap();
        }
        assert!(!s.sealed.is_empty());

        // Tombstone entry in the first (sealed) segment
        assert!(s.tombstone_at(1));
        let e = s.read(1).unwrap().unwrap();
        assert_eq!(e.flags & crate::store::flags::TOMBSTONE, crate::store::flags::TOMBSTONE);

        // Tombstone entry in the active segment
        assert!(s.tombstone_at(6));
        let e = s.read(6).unwrap().unwrap();
        assert_eq!(e.flags & crate::store::flags::TOMBSTONE, crate::store::flags::TOMBSTONE);
    }
}
