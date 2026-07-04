//! Tolerant journal — Segmented Log, Mmap-based zero-copy reads/writes.
//! Strictly follows Hardware Sympathy: Zero allocations on the hot path.

use memmap2::{Mmap, MmapMut};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

use crate::segment::{self, SegmentMetadata, MAX_SEGMENT_BYTES};
use crate::store::{entry_matches, Entry, EntryRef, FsyncPolicy, RetentionLimits, Store, StoreError, StoreInfo};
use arbitro_engine_v2::common::wire_hash_32;

#[derive(Debug, Clone, Copy)]
struct LogMetadata {
    pub seq: u64,
    pub ts: u64,
    pub subj_len: u16,
    pub payload_len: u32,
    pub offset: u32,
    pub segment_idx: u32,
    #[allow(dead_code)]
    pub subject_hash: u32,
    pub stream_id: u32,
    pub flags: u8,
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
    /// FEAT-2: durability policy applied after append writes.
    fsync_policy: FsyncPolicy,
    /// Appends since the last explicit flush — used by `EveryNWrites`.
    writes_since_flush: u32,
    /// FEAT-1: retention limits enforced by `append`/`append_batch`.
    limits: RetentionLimits,
    /// ROB-17: sequence numbers tombstoned in the *active* segment that
    /// have not yet been persisted to the sidecar file, plus the full
    /// tombstoned set for quick membership checks across all segments.
    tombstoned: HashSet<u64>,
}

const MAGIC: u8 = 0xAF;
/// On-disk header layout (hand-rolled little-endian, breaking change from 23 B):
/// [0]       MAGIC (1 B)
/// [1..3]    subj_len u16
/// [3..7]    payload_len u32
/// [7..15]   ts u64
/// [15..23]  seq u64
/// [23..27]  stream_id u32
/// [27]      flags u8
const HEADER_SIZE: usize = 28;
/// **B5 record CRC** — written immediately after `[subject || payload]`
/// and covers `[header bytes 0..27 || subject || payload]`. Recovery
/// re-computes and compares before adding the entry to the index; a
/// mismatch stops the scan at that record (treat the remainder of the
/// segment as truncated, same shape as a missing MAGIC byte).
/// Without this every tear leaves the recovery path open to silently
/// emit garbage `subj_len` / `payload_len` and panic the drain when
/// it tries to slice the mmap. crc32fast is the same hasher
/// `command_log.rs` uses, so the broker only depends on one CRC
/// implementation.
const RECORD_CRC_SIZE: usize = 4;

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
            fsync_policy: FsyncPolicy::None,
            writes_since_flush: 0,
            limits: RetentionLimits::UNLIMITED,
            tombstoned: HashSet::new(),
        }
    }

    /// FEAT-2: configure the fsync/msync policy applied after appends.
    pub fn set_fsync_policy(&mut self, policy: FsyncPolicy) {
        self.fsync_policy = policy;
    }

    /// FEAT-1: configure max_msgs/max_bytes/max_age retention limits.
    /// Enforced by `append`/`append_batch`, which call `truncate_front`
    /// when a limit would be exceeded.
    pub fn set_retention_limits(&mut self, limits: RetentionLimits) {
        self.limits = limits;
    }

    /// FEAT-6: sidecar path holding the serialized index for a sealed
    /// segment, so recovery can load it instead of re-scanning + CRC
    /// checking every byte.
    fn idx_path(base: &std::path::Path, segment_first_seq: u64) -> PathBuf {
        base.join(format!("{segment_first_seq:020}.idx"))
    }

    /// ROB-17: sidecar path holding tombstoned sequence numbers for a
    /// segment (one file per segment, named after the segment's first_seq).
    fn tombstone_path(base: &std::path::Path, segment_first_seq: u64) -> PathBuf {
        base.join(format!("{segment_first_seq:020}.tombstones"))
    }

    /// FEAT-6: write the just-sealed segment's index entries as a flat
    /// binary sidecar so recovery can skip the scan+CRC pass entirely.
    /// Format: repeated fixed-size records matching `LogMetadata`
    /// (all fields little-endian).
    fn write_idx_sidecar(&self, segment_first_seq: u64, entries: &[LogMetadata]) {
        let path = Self::idx_path(&self.base_path, segment_first_seq);
        let mut buf = Vec::with_capacity(entries.len() * 41);
        for m in entries {
            buf.extend_from_slice(&m.seq.to_le_bytes());
            buf.extend_from_slice(&m.ts.to_le_bytes());
            buf.extend_from_slice(&m.subj_len.to_le_bytes());
            buf.extend_from_slice(&m.payload_len.to_le_bytes());
            buf.extend_from_slice(&m.offset.to_le_bytes());
            buf.extend_from_slice(&m.subject_hash.to_le_bytes());
            buf.extend_from_slice(&m.stream_id.to_le_bytes());
            buf.push(m.flags);
        }
        if let Err(e) = fs::write(&path, &buf) {
            tracing::warn!(error = %e, path = %path.display(), "tolerant store: failed to write .idx sidecar");
        }
    }

    /// FEAT-6: load a previously written `.idx` sidecar for the segment
    /// starting at `segment_first_seq`. Returns `None` if missing or
    /// malformed (falls back to the scan path).
    fn load_idx_sidecar(path: &std::path::Path, segment_idx: u32) -> Option<Vec<LogMetadata>> {
        const REC: usize = 8 + 8 + 2 + 4 + 4 + 4 + 4 + 1;
        let bytes = fs::read(path).ok()?;
        if bytes.is_empty() || bytes.len() % REC != 0 {
            return None;
        }
        let mut out = Vec::with_capacity(bytes.len() / REC);
        for chunk in bytes.chunks_exact(REC) {
            let seq = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
            let ts = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
            let subj_len = u16::from_le_bytes(chunk[16..18].try_into().unwrap());
            let payload_len = u32::from_le_bytes(chunk[18..22].try_into().unwrap());
            let offset = u32::from_le_bytes(chunk[22..26].try_into().unwrap());
            let subject_hash = u32::from_le_bytes(chunk[26..30].try_into().unwrap());
            let stream_id = u32::from_le_bytes(chunk[30..34].try_into().unwrap());
            let flags = chunk[34];
            out.push(LogMetadata {
                seq,
                ts,
                subj_len,
                payload_len,
                offset,
                segment_idx,
                subject_hash,
                stream_id,
                flags,
            });
        }
        Some(out)
    }

    /// ROB-17: persist the full tombstoned-sequence set for a segment.
    /// Called on each `tombstone_at` — O(tombstones-in-segment) so it
    /// stays cheap for typical low tombstone rates.
    fn save_tombstones_for_segment(&self, segment_first_seq: u64, seqs: &[u64]) {
        let path = Self::tombstone_path(&self.base_path, segment_first_seq);
        let mut buf = Vec::with_capacity(seqs.len() * 8);
        for s in seqs {
            buf.extend_from_slice(&s.to_le_bytes());
        }
        if let Err(e) = fs::write(&path, &buf) {
            tracing::warn!(error = %e, path = %path.display(), "tolerant store: failed to write .tombstones sidecar");
        }
    }

    /// ROB-17: load tombstoned sequence numbers from a segment's sidecar,
    /// if present, and apply them to the in-memory index + `tombstoned` set.
    fn load_tombstones_for_segment(&mut self, segment_first_seq: u64) {
        let path = Self::tombstone_path(&self.base_path, segment_first_seq);
        let Ok(bytes) = fs::read(&path) else {
            return;
        };
        if bytes.is_empty() || bytes.len() % 8 != 0 {
            return;
        }
        for chunk in bytes.chunks_exact(8) {
            let seq = u64::from_le_bytes(chunk.try_into().unwrap());
            self.tombstoned.insert(seq);
        }
    }

    fn rotate(&mut self) -> Result<(), StoreError> {
        if let Some(mmap) = self.active_mmap.take() {
            // H18: surface msync failures instead of swallowing them.
            // A silent flush failure means the durability story for
            // the previous segment is a lie. Logging the error makes
            // it visible to operators without breaking shutdown (we
            // still try to seal and rotate).
            if let Err(e) = mmap.flush() {
                tracing::error!(
                    error = %e,
                    "tolerant store: mmap.flush failed during rotate — segment durability lost",
                );
            }
            // Drop mmap BEFORE seal_segment reopens the file (Windows file locking).
            drop(mmap);
            let path = segment::segment_path(&self.base_path, self.active_segment_id);
            let sealed = segment::seal_segment(&path, self.current_segment_offset)?;

            // ROB-10: record metadata for the segment just sealed so
            // `truncate_front` can find + delete it later.
            let sealed_idx = self.sealed_segments.len() as u32;
            let seg_entries: Vec<LogMetadata> = self
                .index
                .iter()
                .filter(|m| m.segment_idx == sealed_idx)
                .copied()
                .collect();
            let first_seq = seg_entries.first().map(|m| m.seq).unwrap_or(self.active_segment_id);
            let last_seq = seg_entries.last().map(|m| m.seq).unwrap_or(self.active_segment_id);
            self.segments.push(SegmentMetadata {
                path: path.clone(),
                first_seq,
                last_seq,
            });

            // FEAT-6: write the sidecar index for the now-sealed segment.
            self.write_idx_sidecar(first_seq, &seg_entries);

            self.sealed_segments.push(sealed);
        }

        self.active_segment_id = self.next_seq;
        let path = segment::segment_path(&self.base_path, self.active_segment_id);
        let mmap = segment::create_active_segment(&path)?;

        self.active_mmap = Some(mmap);
        self.current_segment_offset = 0;
        Ok(())
    }

    fn scan_segments(&mut self) -> Result<(), StoreError> {
        if !self.base_path.exists() {
            let _ = fs::create_dir_all(&self.base_path);
            return Ok(());
        }

        let mut paths: Vec<_> = fs::read_dir(&self.base_path)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
            .collect();
        paths.sort();

        for path in paths {
            self.load_segment(&path)?;
        }
        Ok(())
    }

    /// Parse the `first_seq` a segment file was named with, e.g.
    /// `00000000000000000001.log` → `1`. Used to look up the `.idx`
    /// and `.tombstones` sidecars before falling back to a full scan.
    fn segment_first_seq_from_path(path: &std::path::Path) -> Option<u64> {
        path.file_stem()?.to_str()?.parse().ok()
    }

    fn load_segment(&mut self, path: &std::path::Path) -> Result<(), StoreError> {
        let file = fs::File::open(path)?;
        let len = file.metadata().map(|m| m.len()).unwrap_or(0);
        if len < HEADER_SIZE as u64 {
            return Ok(());
        }

        let mmap = unsafe { Mmap::map(&file)? };
        let segment_idx = self.sealed_segments.len() as u32;

        // FEAT-6: if a sidecar `.idx` exists for this segment, trust it
        // instead of re-scanning + CRC-checking every byte. The sidecar
        // is only written on clean seal (rotate/shutdown), so its mere
        // presence implies the segment was not torn.
        if let Some(named_first_seq) = Self::segment_first_seq_from_path(path) {
            let idx_path = Self::idx_path(&self.base_path, named_first_seq);
            if let Some(entries) = Self::load_idx_sidecar(&idx_path, segment_idx) {
                if let (Some(first_m), Some(last_m)) = (entries.first(), entries.last()) {
                    let first = first_m.seq;
                    let last = last_m.seq;
                    for m in &entries {
                        self.total_bytes += (m.subj_len as u64) + (m.payload_len as u64);
                        self.next_seq = m.seq + 1;
                    }
                    self.index.extend(entries);
                    self.segments.push(SegmentMetadata {
                        path: path.to_path_buf(),
                        first_seq: first,
                        last_seq: last,
                    });
                    self.sealed_segments.push(mmap);
                    self.load_tombstones_for_segment(first);
                    return Ok(());
                }
            }
        }

        let mut offset = 0usize;
        let mut first = 0;
        let mut last = 0;

        while offset + HEADER_SIZE <= mmap.len() {
            let h = &mmap[offset..offset + HEADER_SIZE];

            // CRASH CONSISTENCY: Stop if Magic is missing (end of valid data in pre-allocated segment)
            if h[0] != MAGIC {
                break;
            }

            let subj_len = u16::from_le_bytes([h[1], h[2]]);
            let payload_len = u32::from_le_bytes([h[3], h[4], h[5], h[6]]);
            let ts = u64::from_le_bytes([h[7], h[8], h[9], h[10], h[11], h[12], h[13], h[14]]);
            let seq = u64::from_le_bytes([h[15], h[16], h[17], h[18], h[19], h[20], h[21], h[22]]);
            let stream_id = u32::from_le_bytes([h[23], h[24], h[25], h[26]]);
            let flags = h[27];

            // B5 defensive bounds check — clamp lengths against the
            // remaining segment so a tear that left garbage starting
            // with the MAGIC byte cannot drive a downstream OOB slice
            // when the (now-meaningless) subj_len / payload_len
            // exceed what's left.
            let data_off = offset + HEADER_SIZE;
            let body_end = match data_off
                .checked_add(subj_len as usize)
                .and_then(|s| s.checked_add(payload_len as usize))
            {
                Some(v) => v,
                None => break,
            };
            let crc_end = match body_end.checked_add(RECORD_CRC_SIZE) {
                Some(v) => v,
                None => break,
            };
            if crc_end > mmap.len() {
                break;
            }

            // B5: re-compute CRC32 over [header || subject || payload]
            // and stop the scan on any mismatch. Treats a corrupt tail
            // the same as a tear (lost-after-crash).
            let expected_crc = u32::from_le_bytes([
                mmap[body_end],
                mmap[body_end + 1],
                mmap[body_end + 2],
                mmap[body_end + 3],
            ]);
            let actual_crc = crc32fast::hash(&mmap[offset..body_end]);
            if expected_crc != actual_crc {
                break;
            }

            if first == 0 {
                first = seq;
            }
            last = seq;
            let subject_hash = wire_hash_32(&mmap[data_off..data_off + subj_len as usize]);
            self.index.push(LogMetadata {
                seq,
                ts,
                subj_len,
                payload_len,
                offset: data_off as u32,
                segment_idx,
                subject_hash,
                stream_id,
                flags,
            });

            offset = crc_end;
            self.next_seq = seq + 1;
            self.total_bytes += (subj_len as u64) + (payload_len as u64);
        }

        if first != 0 {
            self.segments.push(SegmentMetadata {
                path: path.to_path_buf(),
                first_seq: first,
                last_seq: last,
            });
            self.sealed_segments.push(mmap);
            self.load_tombstones_for_segment(first);
        }
        Ok(())
    }

    /// Append assuming the caller has already verified there is room
    /// in the active segment for this entry. Used by `append_batch`
    /// when the whole batch fits in one segment — we do ONE rotate
    /// check up front, then call this per entry to skip the redundant
    /// per-entry test and keep the inner loop tight.
    ///
    /// Precondition (caller MUST honour): the entry's `HEADER_SIZE +
    /// subject.len() + payload.len()` bytes fit in the active mmap
    /// starting at `current_segment_offset`. Violating this causes a
    /// mmap bounds-check panic (no silent corruption).
    fn append_no_rotate(&mut self, entry: EntryRef<'_>, timestamp: u64) -> u64 {
        let entry_total = (entry.subject.len() + entry.payload.len()) as u64;
        let total_needed = (HEADER_SIZE as u64) + entry_total + RECORD_CRC_SIZE as u64;

        let seq = self.next_seq;
        let mmap = self.active_mmap.as_mut().expect("active mmap initialised");
        let start = self.current_segment_offset as usize;

        let slen = entry.subject.len() as u16;
        let plen = entry.payload.len() as u32;

        mmap[start] = MAGIC;
        mmap[start + 1..start + 3].copy_from_slice(&slen.to_le_bytes());
        mmap[start + 3..start + 7].copy_from_slice(&plen.to_le_bytes());
        mmap[start + 7..start + 15].copy_from_slice(&timestamp.to_le_bytes());
        mmap[start + 15..start + 23].copy_from_slice(&seq.to_le_bytes());
        mmap[start + 23..start + 27].copy_from_slice(&entry.stream_id.to_le_bytes());
        mmap[start + 27] = entry.flags;

        let data_off = start + HEADER_SIZE;
        mmap[data_off..data_off + entry.subject.len()].copy_from_slice(entry.subject);
        mmap[data_off + entry.subject.len()..data_off + entry.subject.len() + entry.payload.len()]
            .copy_from_slice(entry.payload);

        // B5: CRC32 over [header || subject || payload] written
        // immediately after the payload. Recovery verifies this before
        // accepting the record.
        let crc_off = data_off + entry.subject.len() + entry.payload.len();
        let covered = &mmap[start..crc_off];
        let crc = crc32fast::hash(covered);
        mmap[crc_off..crc_off + RECORD_CRC_SIZE].copy_from_slice(&crc.to_le_bytes());

        // Zero-allocation push (capacity guaranteed by init() +
        // reserve() in `append_batch` for batched callers).
        let subject_hash = wire_hash_32(entry.subject);
        self.index.push(LogMetadata {
            seq,
            ts: timestamp,
            subj_len: slen,
            payload_len: plen,
            offset: data_off as u32,
            segment_idx: self.sealed_segments.len() as u32,
            subject_hash,
            stream_id: entry.stream_id,
            flags: entry.flags,
        });

        self.next_seq += 1;
        self.total_bytes += entry_total;
        self.current_segment_offset += total_needed as u32;

        // FEAT-2: apply the configured fsync policy after the write.
        self.writes_since_flush += 1;
        let should_flush = match self.fsync_policy {
            FsyncPolicy::None => false,
            FsyncPolicy::EveryWrite => true,
            FsyncPolicy::EveryNWrites(n) => n > 0 && self.writes_since_flush >= n,
        };
        if should_flush {
            if let Some(mmap) = self.active_mmap.as_ref() {
                if let Err(e) = mmap.flush() {
                    tracing::error!(error = %e, "tolerant store: fsync policy flush failed");
                }
            }
            self.writes_since_flush = 0;
        }

        seq
    }

    /// ROB-9: resolve `seq` to an index position. The direct
    /// `seq - first_seq` computation is only valid when the index has no
    /// gaps (e.g. from partial recovery or future compaction removing
    /// entries); verify it and fall back to binary search on mismatch.
    #[inline]
    fn seq_to_idx(&self, seq: u64) -> Option<usize> {
        if seq < self.first_seq {
            return None;
        }
        let est_idx = (seq - self.first_seq) as usize;
        if est_idx < self.index.len() && self.index[est_idx].seq == seq {
            return Some(est_idx);
        }
        self.index.binary_search_by_key(&seq, |m| m.seq).ok()
    }

    /// ROB-9: lower-bound variant for range scans (`read_range`,
    /// `for_each`) — same verify-then-fallback strategy as `seq_to_idx`
    /// but returns an insertion point instead of requiring an exact hit.
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

    /// FEAT-1: enforce retention limits by truncating the front of the
    /// log until the store is back within `max_msgs`/`max_bytes`. Called
    /// before accepting new entries in `append`/`append_batch`.
    fn enforce_limits(&mut self, incoming_msgs: u64, incoming_bytes: u64) {
        if self.limits == RetentionLimits::UNLIMITED {
            return;
        }
        if self.limits.max_msgs > 0 {
            let projected = self.index.len() as u64 + incoming_msgs;
            if projected > self.limits.max_msgs {
                let overflow = projected - self.limits.max_msgs;
                let target = self.first_seq + overflow;
                self.truncate_front(target);
            }
        }
        if self.limits.max_bytes > 0 {
            let projected = self.total_bytes + incoming_bytes;
            if projected > self.limits.max_bytes {
                // Walk forward from first_seq, dropping entries until
                // projected bytes fit under the limit.
                let mut to_drop_bytes = projected - self.limits.max_bytes;
                let mut target = self.first_seq;
                for m in &self.index {
                    if to_drop_bytes == 0 {
                        break;
                    }
                    let sz = (m.subj_len as u64) + (m.payload_len as u64);
                    to_drop_bytes = to_drop_bytes.saturating_sub(sz);
                    target = m.seq + 1;
                }
                self.truncate_front(target);
            }
        }
        if self.limits.max_age_ms > 0 {
            // Best-effort: caller-supplied timestamps are the only clock
            // this store has. Skipped here — enforced via `purge_before`
            // by callers that track wall-clock time, since `enforce_limits`
            // has no access to "now".
        }
    }

    /// FEAT-7: like `get`, but recomputes the CRC32 over the on-disk
    /// record and returns `StoreError::Io` (data corruption) on mismatch
    /// instead of silently handing back the (possibly corrupt) bytes.
    pub fn get_verified(&self, seq: u64, f: &mut dyn FnMut(&Entry<'_>)) -> Result<bool, StoreError> {
        let Some(idx) = self.seq_to_idx(seq) else {
            return Ok(false);
        };
        let m = &self.index[idx];
        let data = if m.segment_idx == self.sealed_segments.len() as u32 {
            self.active_mmap
                .as_ref()
                .map(|m| &m[..])
                .ok_or(StoreError::NotFound)?
        } else {
            &self.sealed_segments[m.segment_idx as usize][..]
        };

        let header_start = (m.offset as usize) - HEADER_SIZE;
        let sub_end = (m.offset as usize) + (m.subj_len as usize);
        let body_end = sub_end + (m.payload_len as usize);
        let crc_end = body_end + RECORD_CRC_SIZE;
        if crc_end > data.len() {
            return Err(StoreError::Io(std::io::ErrorKind::UnexpectedEof));
        }
        let expected_crc = u32::from_le_bytes(data[body_end..crc_end].try_into().unwrap());
        let actual_crc = crc32fast::hash(&data[header_start..body_end]);
        if expected_crc != actual_crc {
            return Err(StoreError::Io(std::io::ErrorKind::InvalidData));
        }

        f(&Entry {
            seq: m.seq,
            stream_id: m.stream_id,
            timestamp: m.ts,
            subject: &data[m.offset as usize..sub_end],
            payload: &data[sub_end..body_end],
            flags: m.flags,
        });
        Ok(true)
    }

    /// FEAT-8: rewrite all sealed segments, dropping tombstoned entries.
    /// Returns the number of entries reclaimed. Rebuilds the on-disk
    /// segment files and their `.idx`/`.tombstones` sidecars; the active
    /// (unsealed) segment is left untouched since it is still being
    /// written to.
    pub fn compact(&mut self) -> Result<u64, StoreError> {
        if self.segments.is_empty() {
            return Ok(0);
        }

        let active_seg_id = self.sealed_segments.len() as u32;
        let mut reclaimed = 0u64;
        let mut new_index: Vec<LogMetadata> = Vec::with_capacity(self.index.len());
        let mut new_sealed_segments: Vec<Mmap> = Vec::with_capacity(self.sealed_segments.len());
        let mut new_segments: Vec<SegmentMetadata> = Vec::with_capacity(self.segments.len());

        for (seg_idx, seg_meta) in self.segments.iter().enumerate() {
            let seg_idx = seg_idx as u32;
            let entries: Vec<&LogMetadata> = self
                .index
                .iter()
                .filter(|m| m.segment_idx == seg_idx)
                .collect();

            let tmp_path = seg_meta.path.with_extension("log.compact");
            let mmap = segment::create_active_segment(&tmp_path)?;
            let mut mmap = mmap;
            let mut write_offset = 0u32;
            let new_seg_idx = new_sealed_segments.len() as u32;
            let mut kept: Vec<LogMetadata> = Vec::with_capacity(entries.len());

            for m in &entries {
                if m.flags & crate::store::flags::TOMBSTONE != 0 {
                    reclaimed += 1;
                    continue;
                }
                let src = &self.sealed_segments[seg_idx as usize][..];
                let header_start = (m.offset as usize) - HEADER_SIZE;
                let body_end = (m.offset as usize) + (m.subj_len as usize) + (m.payload_len as usize);
                let record_end = body_end + RECORD_CRC_SIZE;
                let record = &src[header_start..record_end];

                let start = write_offset as usize;
                mmap[start..start + record.len()].copy_from_slice(record);

                kept.push(LogMetadata {
                    seq: m.seq,
                    ts: m.ts,
                    subj_len: m.subj_len,
                    payload_len: m.payload_len,
                    offset: (start + HEADER_SIZE) as u32,
                    segment_idx: new_seg_idx,
                    subject_hash: m.subject_hash,
                    stream_id: m.stream_id,
                    flags: m.flags,
                });
                write_offset += record.len() as u32;
            }

            if let Err(e) = mmap.flush() {
                tracing::warn!(error = %e, "tolerant store: compact flush failed");
            }
            drop(mmap);

            if kept.is_empty() {
                let _ = fs::remove_file(&tmp_path);
                continue;
            }

            let sealed = segment::seal_segment(&tmp_path, write_offset)?;
            // Atomically replace the original segment file with the
            // compacted one.
            if let Err(e) = fs::rename(&tmp_path, &seg_meta.path) {
                tracing::warn!(error = %e, "tolerant store: compact rename failed");
            }

            let first_seq = kept.first().map(|m| m.seq).unwrap_or(seg_meta.first_seq);
            let last_seq = kept.last().map(|m| m.seq).unwrap_or(seg_meta.last_seq);
            self.write_idx_sidecar(first_seq, &kept);

            new_segments.push(SegmentMetadata {
                path: seg_meta.path.clone(),
                first_seq,
                last_seq,
            });
            new_sealed_segments.push(sealed);
            new_index.extend(kept);
        }

        // Active (unsealed) segment entries pass through untouched.
        for m in self.index.iter().filter(|m| m.segment_idx == active_seg_id) {
            let mut m = *m;
            m.segment_idx = new_sealed_segments.len() as u32;
            new_index.push(m);
        }

        self.sealed_segments = new_sealed_segments;
        self.segments = new_segments;
        self.index = new_index;
        self.total_bytes = self
            .index
            .iter()
            .map(|m| (m.subj_len as u64) + (m.payload_len as u64))
            .sum();
        if let Some(f) = self.index.first() {
            self.first_seq = f.seq;
        }

        Ok(reclaimed)
    }
}

impl Store for TolerantStore {
    fn init(&mut self) -> Result<(), StoreError> {
        // PERF-3: start small — the previous 1M-entry pre-allocation
        // reserved ~41 MB per store up front regardless of actual usage.
        self.index = Vec::with_capacity(1024);
        self.scan_segments()?;
        if self.active_mmap.is_none() {
            self.rotate()?;
        }
        if let Some(f) = self.index.first() {
            self.first_seq = f.seq;
        }
        Ok(())
    }

    fn append(&mut self, entry: EntryRef<'_>, timestamp: u64) -> Result<u64, StoreError> {
        // ROB-11: header's subj_len field is a u16 — reject anything
        // that can't round-trip through it before touching the mmap.
        if entry.subject.len() > u16::MAX as usize {
            return Err(StoreError::InvalidSubjectLength);
        }

        let entry_total = (entry.subject.len() + entry.payload.len()) as u64;
        // +RECORD_CRC_SIZE for the trailing CRC32 (B5).
        let total_needed = (HEADER_SIZE as u64) + entry_total + RECORD_CRC_SIZE as u64;

        // CRASH-2: an entry that can never fit in a freshly rotated
        // segment must be rejected up front, not after an infinite
        // rotate loop / mmap bounds panic.
        if total_needed > MAX_SEGMENT_BYTES {
            return Err(StoreError::EntryTooLarge);
        }

        self.enforce_limits(1, entry_total);

        if (self.current_segment_offset as u64) + total_needed >= MAX_SEGMENT_BYTES {
            self.rotate()?;
        }

        Ok(self.append_no_rotate(entry, timestamp))
    }

    fn get(&self, seq: u64, f: &mut dyn FnMut(&Entry<'_>)) -> Result<bool, StoreError> {
        let Some(idx) = self.seq_to_idx(seq) else {
            return Ok(false);
        };
        let m = &self.index[idx];
        let data = if m.segment_idx == self.sealed_segments.len() as u32 {
            self.active_mmap
                .as_ref()
                .map(|m| &m[..])
                .ok_or(StoreError::NotFound)?
        } else {
            &self.sealed_segments[m.segment_idx as usize][..]
        };

        let sub_end = (m.offset as usize) + (m.subj_len as usize);
        f(&Entry {
            seq: m.seq,
            stream_id: m.stream_id,
            timestamp: m.ts,
            subject: &data[m.offset as usize..sub_end],
            payload: &data[sub_end..sub_end + (m.payload_len as usize)],
            flags: m.flags,
        });
        Ok(true)
    }

    fn truncate_front(&mut self, target: u64) -> u64 {
        // ROB-16: never target beyond what has actually been written —
        // a caller-supplied `target > next_seq` would otherwise drop
        // every existing entry and desynchronize `first_seq`.
        let target = target.min(self.next_seq);

        if self.index.is_empty() || target <= self.first_seq {
            return 0;
        }

        let idx = (target - self.first_seq) as usize;
        let idx = idx.min(self.index.len());
        if idx == 0 {
            return 0;
        }

        let mut dropped = 0;
        while !self.segments.is_empty() && self.segments[0].last_seq < target {
            let seg_first_seq = self.segments[0].first_seq;
            let path = segment::segment_path(&self.base_path, seg_first_seq);

            // ROB-15: drop the mmap handle BEFORE removing the file —
            // on Windows an open mapping holds the file open and the
            // delete silently fails (or, worse, succeeds and leaves a
            // dangling handle). Log failures instead of discarding them.
            self.sealed_segments.remove(0);
            self.segments.remove(0);

            if let Err(e) = fs::remove_file(&path) {
                tracing::warn!(error = %e, path = %path.display(), "tolerant store: failed to remove old segment during truncate_front");
            }
            let idx_path = Self::idx_path(&self.base_path, seg_first_seq);
            if idx_path.exists() {
                if let Err(e) = fs::remove_file(&idx_path) {
                    tracing::warn!(error = %e, path = %idx_path.display(), "tolerant store: failed to remove .idx sidecar during truncate_front");
                }
            }
            let tomb_path = Self::tombstone_path(&self.base_path, seg_first_seq);
            if tomb_path.exists() {
                if let Err(e) = fs::remove_file(&tomb_path) {
                    tracing::warn!(error = %e, path = %tomb_path.display(), "tolerant store: failed to remove .tombstones sidecar during truncate_front");
                }
            }
            dropped += 1;
        }

        // F25: subtract bytes from the dropped prefix in O(idx) instead
        // of re-walking the survivors. Eviction no longer pauses publish
        // for ms-scale on large stores.
        let dropped_bytes: u64 = self.index[..idx]
            .iter()
            .map(|m| (m.subj_len as u64) + (m.payload_len as u64))
            .sum();

        // Drop tombstone bookkeeping for evicted sequences.
        for m in &self.index[..idx] {
            self.tombstoned.remove(&m.seq);
        }

        self.index.drain(0..idx);
        if dropped > 0 {
            for m in &mut self.index {
                m.segment_idx -= dropped as u32;
            }
        }
        self.first_seq = target;
        self.total_bytes = self.total_bytes.saturating_sub(dropped_bytes);
        idx as u64
    }

    fn shutdown(&mut self) -> Result<(), StoreError> {
        if let Some(mmap) = self.active_mmap.take() {
            if let Err(e) = mmap.flush() {
                tracing::error!(
                    error = %e,
                    "tolerant store: mmap.flush failed during shutdown — final segment durability lost",
                );
            }
            // Drop mmap BEFORE seal_segment reopens the file.
            // On Windows, mmap holds an exclusive file handle.
            drop(mmap);
            let path = segment::segment_path(&self.base_path, self.active_segment_id);
            let _ = segment::seal_segment(&path, self.current_segment_offset)?;

            // FEAT-6: write the sidecar index for the final sealed segment
            // so a future clean restart can skip the scan+CRC pass.
            let sealed_idx = self.sealed_segments.len() as u32;
            let seg_entries: Vec<LogMetadata> = self
                .index
                .iter()
                .filter(|m| m.segment_idx == sealed_idx)
                .copied()
                .collect();
            if let Some(first) = seg_entries.first().map(|m| m.seq) {
                self.write_idx_sidecar(first, &seg_entries);
            }
        }
        Ok(())
    }

    fn purge(&mut self) -> u64 {
        let count = self.index.len() as u64;
        self.index.clear();
        self.sealed_segments.clear();
        self.segments.clear();
        self.active_mmap = None;
        self.total_bytes = 0;
        self.current_segment_offset = 0;
        self.tombstoned.clear();
        self.writes_since_flush = 0;
        let _ = fs::remove_dir_all(&self.base_path);
        let _ = fs::create_dir_all(&self.base_path);

        // CRASH-1: purge tore down the active segment along with
        // everything else — without rotating, the very next append()
        // would panic on `active_mmap.as_mut().expect(...)`.
        if let Err(e) = self.rotate() {
            tracing::error!(error = %e, "tolerant store: rotate after purge failed — store has no active segment");
        }

        count
    }

    fn info(&self) -> StoreInfo {
        StoreInfo {
            messages: self.index.len() as u64,
            bytes: self.total_bytes,
            first_seq: self.first_seq,
            last_seq: self.next_seq.saturating_sub(1),
        }
    }

    fn append_batch(&mut self, entries: &[EntryRef<'_>], ts: u64) -> Result<u64, StoreError> {
        if entries.is_empty() {
            return Ok(self.next_seq);
        }

        // ROB-11 / CRASH-2: validate every entry up front so a batch
        // never partially applies before hitting a bad entry.
        let mut total_needed: u64 = 0;
        for e in entries {
            if e.subject.len() > u16::MAX as usize {
                return Err(StoreError::InvalidSubjectLength);
            }
            let needed = (HEADER_SIZE as u64)
                + (e.subject.len() as u64)
                + (e.payload.len() as u64)
                + RECORD_CRC_SIZE as u64; // B5
            if needed > MAX_SEGMENT_BYTES {
                return Err(StoreError::EntryTooLarge);
            }
            total_needed += needed;
        }

        let incoming_bytes: u64 = entries
            .iter()
            .map(|e| (e.subject.len() as u64) + (e.payload.len() as u64))
            .sum();
        self.enforce_limits(entries.len() as u64, incoming_bytes);

        // Reserve in the index Vec once — without this, the loop below
        // pays for up to log2(N) reallocations of the index, each of
        // which copies the whole existing index into a new buffer.
        // For a 256-entry batch that's ~8 grows. This single line is
        // by far the largest win available at the store level.
        self.index.reserve(entries.len());

        let first = self.next_seq;

        // Fast path: if the entire batch fits in the active segment,
        // skip the rotate-check per entry. The check is a single
        // integer compare so the win is small in absolute terms, but
        // it also lets the compiler keep all the per-entry work in
        // tight loop without a branch that's almost never taken.
        if (self.current_segment_offset as u64) + total_needed < MAX_SEGMENT_BYTES {
            // Whole batch fits — single rotate check, no per-entry check.
            for e in entries {
                self.append_no_rotate(*e, ts);
            }
        } else {
            // Batch crosses (or could cross) a segment boundary —
            // fall back to the general path that re-checks per entry.
            // This is the same cost as the old `append_batch`; we
            // don't try to slice the batch at the boundary because
            // the `rotate` boundary depends on each entry's actual
            // size, not an upfront tally.
            for e in entries {
                let entry_total = (e.subject.len() + e.payload.len()) as u64;
                let needed = (HEADER_SIZE as u64) + entry_total + RECORD_CRC_SIZE as u64;
                if (self.current_segment_offset as u64) + needed >= MAX_SEGMENT_BYTES {
                    self.rotate()?;
                }
                self.append_no_rotate(*e, ts);
            }
        }
        Ok(first)
    }

    fn read(&self, seq: u64) -> Result<Option<Entry<'_>>, StoreError> {
        let Some(idx) = self.seq_to_idx(seq) else {
            return Ok(None);
        };
        let m = &self.index[idx];
        let data = if m.segment_idx == self.sealed_segments.len() as u32 {
            self.active_mmap
                .as_ref()
                .map(|mm| &mm[..])
                .ok_or(StoreError::NotFound)?
        } else {
            &self.sealed_segments[m.segment_idx as usize][..]
        };
        let sub_end = (m.offset as usize) + (m.subj_len as usize);
        Ok(Some(Entry {
            seq: m.seq,
            stream_id: m.stream_id,
            timestamp: m.ts,
            subject: &data[m.offset as usize..sub_end],
            payload: &data[sub_end..sub_end + (m.payload_len as usize)],
            flags: m.flags,
        }))
    }
    fn read_range(&self, start: u64, end: u64) -> Result<Vec<Entry<'_>>, StoreError> {
        let s = self.find_lower_bound(start);
        let e = self.find_lower_bound(end);
        let e = e.min(self.index.len());
        let s = s.min(e);

        let mut result = Vec::with_capacity(e - s);
        for i in s..e {
            let m = &self.index[i];
            let data = if m.segment_idx == self.sealed_segments.len() as u32 {
                self.active_mmap
                    .as_ref()
                    .map(|mm| &mm[..])
                    .ok_or(StoreError::NotFound)?
            } else {
                &self.sealed_segments[m.segment_idx as usize][..]
            };
            let sub_end = (m.offset as usize) + (m.subj_len as usize);
            result.push(Entry {
                seq: m.seq,
                stream_id: m.stream_id,
                timestamp: m.ts,
                subject: &data[m.offset as usize..sub_end],
                payload: &data[sub_end..sub_end + (m.payload_len as usize)],
                flags: m.flags,
            });
        }
        Ok(result)
    }
    fn drain(&mut self, subject: &[u8]) -> u64 {
        // ROB-18: find every live (non-tombstoned) entry whose subject
        // matches, then tombstone each one. `tombstone_at` owns the
        // CRC-fixup and `.tombstones` sidecar persistence (ROB-17), so
        // this just collects the matching seqs first (read-only pass)
        // and mutates in a second pass.
        let active_seg_id = self.sealed_segments.len() as u32;
        let mut to_tombstone = Vec::new();
        for m in &self.index {
            if m.flags & crate::store::flags::TOMBSTONE != 0 {
                continue;
            }
            let data: &[u8] = if m.segment_idx == active_seg_id {
                match self.active_mmap.as_ref() {
                    Some(mm) => &mm[..],
                    None => continue,
                }
            } else {
                match self.sealed_segments.get(m.segment_idx as usize) {
                    Some(s) => &s[..],
                    None => continue,
                }
            };
            let sub_end = (m.offset as usize) + (m.subj_len as usize);
            let subj = &data[m.offset as usize..sub_end];
            if entry_matches(
                &Entry {
                    seq: m.seq,
                    stream_id: m.stream_id,
                    timestamp: m.ts,
                    subject: subj,
                    payload: &[][..],
                    flags: m.flags,
                },
                subject,
            ) {
                to_tombstone.push(m.seq);
            }
        }

        let mut count = 0u64;
        for seq in to_tombstone {
            if self.tombstone_at(seq) {
                count += 1;
            }
        }
        count
    }

    fn tombstone_at(&mut self, seq: u64) -> bool {
        let Some(idx) = self.seq_to_idx(seq) else {
            return false;
        };
        let m = &mut self.index[idx];
        if m.flags & crate::store::flags::TOMBSTONE != 0 {
            return false; // already tombstoned
        }
        m.flags |= crate::store::flags::TOMBSTONE;

        // Persist the flag byte + recompute CRC to the mmap.
        // LogMetadata.offset = data_off = header_start + HEADER_SIZE (28).
        // Flags byte lives at header_start + 27 = data_off - 1.
        let data_off = m.offset as usize;
        let header_start = data_off - HEADER_SIZE;
        let flags_disk_offset = data_off - 1;
        let body_end = data_off + m.subj_len as usize + m.payload_len as usize;
        let segment_idx = m.segment_idx as usize;
        let active_seg_id = self.sealed_segments.len();
        if segment_idx == active_seg_id {
            if let Some(ref mut mmap) = self.active_mmap {
                // Update flags byte
                mmap[flags_disk_offset] = m.flags;
                // Recompute CRC over [header || subject || payload]
                let crc = crc32fast::hash(&mmap[header_start..body_end]);
                mmap[body_end..body_end + RECORD_CRC_SIZE]
                    .copy_from_slice(&crc.to_le_bytes());
                // Flush the modified region so it survives process exit.
                let _ = mmap.flush();
            }
        }
        // Sealed segments are read-only Mmap — the flag byte can't be
        // patched in place. ROB-17: persist the tombstoned seq to a
        // per-segment `.tombstones` sidecar instead, loaded back on init.
        self.tombstoned.insert(seq);
        let seg_first_seq = self
            .segments
            .get(segment_idx)
            .map(|s| s.first_seq)
            .unwrap_or(self.active_segment_id);
        let seqs_for_segment: Vec<u64> = self
            .index
            .iter()
            .filter(|m| m.segment_idx as usize == segment_idx && self.tombstoned.contains(&m.seq))
            .map(|m| m.seq)
            .collect();
        self.save_tombstones_for_segment(seg_first_seq, &seqs_for_segment);
        true
    }
    fn tombstone_stream(&mut self, stream_id: u32) -> u64 {
        let seqs: Vec<u64> = self
            .index
            .iter()
            .filter(|m| {
                m.stream_id == stream_id
                    && m.flags & crate::store::flags::TOMBSTONE == 0
            })
            .map(|m| m.seq)
            .collect();
        let mut count = 0u64;
        for seq in seqs {
            if self.tombstone_at(seq) {
                count += 1;
            }
        }
        count
    }

    fn for_each(&self, s: u64, e: u64, f: &mut dyn FnMut(&Entry<'_>)) -> Result<(), StoreError> {
        let start = self.find_lower_bound(s);
        let end = self.find_lower_bound(e);
        let end = end.min(self.index.len());
        let start = start.min(end);

        // Cache the active-segment id once. The `sealed_segments.len()` value
        // can't change during this read-only walk (we hold `&self`).
        let active_seg_id = self.sealed_segments.len() as u32;
        let active_slice: Option<&[u8]> = self.active_mmap.as_ref().map(|m| &m[..]);

        for i in start..end {
            let m = &self.index[i];
            let data: &[u8] = if m.segment_idx == active_seg_id {
                match active_slice {
                    Some(s) => s,
                    None => return Err(StoreError::NotFound),
                }
            } else {
                &self.sealed_segments[m.segment_idx as usize][..]
            };
            let sub_start = m.offset as usize;
            let sub_end = sub_start + (m.subj_len as usize);
            let pld_end = sub_end + (m.payload_len as usize);
            let entry = Entry {
                seq: m.seq,
                stream_id: m.stream_id,
                timestamp: m.ts,
                subject: &data[sub_start..sub_end],
                payload: &data[sub_end..pld_end],
                flags: m.flags,
            };
            f(&entry);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{EntryRef, Store};

    fn make_entry<'a>(subject: &'a [u8], payload: &'a [u8]) -> EntryRef<'a> {
        EntryRef {
            stream_id: 0,
            subject,
            payload,
            flags: 0,
            deliver_at_ms: 0,
        }
    }

    #[test]
    fn append_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = TolerantStore::new(dir.path().join("store"));
        store.init().unwrap();

        let seq = store
            .append(make_entry(b"orders.created", b"{\"id\":1}"), 1000)
            .unwrap();
        assert_eq!(seq, 1);

        let mut found = false;
        let ok = store
            .get(1, &mut |entry| {
                assert_eq!(entry.seq, 1);
                assert_eq!(entry.subject, b"orders.created");
                assert_eq!(entry.payload, b"{\"id\":1}");
                assert_eq!(entry.timestamp, 1000);
                found = true;
            })
            .unwrap();
        assert!(ok);
        assert!(found);

        // Not found returns false
        let ok = store
            .get(999, &mut |_| panic!("should not be called"))
            .unwrap();
        assert!(!ok);
    }

    #[test]
    fn append_batch_and_for_each() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = TolerantStore::new(dir.path().join("store"));
        store.init().unwrap();

        let entries = [
            make_entry(b"a", b"1"),
            make_entry(b"b", b"2"),
            make_entry(b"c", b"3"),
            make_entry(b"d", b"4"),
            make_entry(b"e", b"5"),
        ];
        let first = store.append_batch(&entries, 100).unwrap();
        assert_eq!(first, 1);
        assert_eq!(store.info().messages, 5);

        let mut count = 0u32;
        let mut seqs = Vec::new();
        store
            .for_each(1, 6, &mut |entry| {
                seqs.push(entry.seq);
                count += 1;
            })
            .unwrap();
        assert_eq!(count, 5);
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
    }

    /// Large batch — exercises the fast path that pre-checks the total
    /// size once, reserves `index` capacity, and calls
    /// `append_no_rotate` per entry. Verifies seq monotonicity,
    /// payload fidelity, and that no entries are dropped or
    /// duplicated when the optimised loop runs at scale.
    #[test]
    fn append_batch_large_fast_path() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = TolerantStore::new(dir.path().join("store"));
        store.init().unwrap();

        const N: usize = 1_000;
        // Build owned buffers so the EntryRef borrows live the whole call.
        let subjects: Vec<Vec<u8>> = (0..N)
            .map(|i| format!("subj.{i:04}").into_bytes())
            .collect();
        let payloads: Vec<Vec<u8>> = (0..N)
            .map(|i| format!("payload-{i}").into_bytes())
            .collect();
        let entries: Vec<EntryRef<'_>> = (0..N)
            .map(|i| make_entry(&subjects[i], &payloads[i]))
            .collect();

        let first = store.append_batch(&entries, 42).unwrap();
        assert_eq!(first, 1, "first seq of the batch must be 1 on fresh store");
        assert_eq!(store.info().messages, N as u64);

        // Read every entry by sequence and verify payload roundtrip.
        for i in 0..N {
            let seq = (i + 1) as u64;
            let mut got_subject = Vec::new();
            let mut got_payload = Vec::new();
            let mut got_seq = 0u64;
            let ok = store
                .get(seq, &mut |entry| {
                    got_seq = entry.seq;
                    got_subject = entry.subject.to_vec();
                    got_payload = entry.payload.to_vec();
                })
                .unwrap();
            assert!(ok, "entry seq={seq} must be present");
            assert_eq!(got_seq, seq, "stored seq must match expected");
            assert_eq!(
                &got_subject, &subjects[i],
                "subject roundtrip mismatch at i={i}"
            );
            assert_eq!(
                &got_payload, &payloads[i],
                "payload roundtrip mismatch at i={i}"
            );
        }
    }

    /// Multiple consecutive batches — proves that `index.reserve` does
    /// not corrupt state across calls (the Vec keeps its capacity
    /// hint, subsequent batches still allocate correctly), and that
    /// seq numbering continues monotonically across batch boundaries.
    #[test]
    fn append_batch_multiple_calls_preserve_monotonicity() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = TolerantStore::new(dir.path().join("store"));
        store.init().unwrap();

        // 3 batches of 50 entries each, with content distinguishable
        // by batch index so a misordering would show up.
        let mut next_expected_first = 1u64;
        for batch_idx in 0..3u32 {
            let subjects: Vec<Vec<u8>> = (0..50)
                .map(|i| format!("b{batch_idx}.{i:02}").into_bytes())
                .collect();
            let payloads: Vec<Vec<u8>> = (0..50)
                .map(|i| format!("v{batch_idx}-{i}").into_bytes())
                .collect();
            let entries: Vec<EntryRef<'_>> = (0..50)
                .map(|i| make_entry(&subjects[i], &payloads[i]))
                .collect();
            let first = store
                .append_batch(&entries, 100 + batch_idx as u64)
                .unwrap();
            assert_eq!(
                first, next_expected_first,
                "batch {batch_idx}: first seq must continue from previous batch",
            );
            next_expected_first += 50;
        }
        assert_eq!(store.info().messages, 150);

        // Sanity: walk all 150 entries in order, no gaps, no dups.
        let mut seqs = Vec::with_capacity(150);
        store
            .for_each(1, 151, &mut |entry| seqs.push(entry.seq))
            .unwrap();
        let expected: Vec<u64> = (1..=150).collect();
        assert_eq!(
            seqs, expected,
            "150 entries across 3 batches must be 1..=150 in order"
        );
    }

    /// Empty batch — must be a no-op that doesn't corrupt seq state
    /// or touch the index. Defends the fast-path's `if empty → early
    /// return` guard.
    #[test]
    fn append_batch_empty_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = TolerantStore::new(dir.path().join("store"));
        store.init().unwrap();

        // Pre-seed with one entry so the store has state to corrupt.
        store.append(make_entry(b"seed", b"before"), 1).unwrap();
        assert_eq!(store.info().messages, 1);

        // Empty batch must NOT advance next_seq or touch the index.
        let first = store.append_batch(&[], 999).unwrap();
        assert_eq!(first, 2, "empty batch returns next_seq, not 0");
        assert_eq!(
            store.info().messages,
            1,
            "empty batch must not change message count"
        );

        // Subsequent append still works and gets seq=2 (proves
        // `next_seq` wasn't perturbed).
        let seq = store.append(make_entry(b"after", b"value"), 2).unwrap();
        assert_eq!(seq, 2);
    }

    #[test]
    fn info_tracks_messages_and_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = TolerantStore::new(dir.path().join("store"));
        store.init().unwrap();

        // "ab" + "cd" = 4 bytes
        store.append(make_entry(b"ab", b"cd"), 0).unwrap();
        // "ef" + "ghij" = 6 bytes
        store.append(make_entry(b"ef", b"ghij"), 0).unwrap();
        // "x" + "y" = 2 bytes
        store.append(make_entry(b"x", b"y"), 0).unwrap();

        let info = store.info();
        assert_eq!(info.messages, 3);
        assert_eq!(info.bytes, 12); // 4 + 6 + 2
        assert_eq!(info.first_seq, 1);
        assert_eq!(info.last_seq, 3);
    }

    #[test]
    fn shutdown_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");

        // Write 3 entries and shut down
        {
            let mut store = TolerantStore::new(path.clone());
            store.init().unwrap();
            store
                .append(make_entry(b"subj.a", b"payload1"), 10)
                .unwrap();
            store
                .append(make_entry(b"subj.b", b"payload2"), 20)
                .unwrap();
            store
                .append(make_entry(b"subj.c", b"payload3"), 30)
                .unwrap();
            store.shutdown().unwrap();
        }

        // Reopen and verify recovery
        {
            let mut store = TolerantStore::new(path);
            store.init().unwrap();

            let info = store.info();
            assert_eq!(info.messages, 3);

            let mut found_subjects = Vec::new();
            store
                .for_each(1, 4, &mut |entry| {
                    found_subjects.push(entry.subject.to_vec());
                })
                .unwrap();
            assert_eq!(found_subjects.len(), 3);
            assert_eq!(found_subjects[0], b"subj.a");
            assert_eq!(found_subjects[1], b"subj.b");
            assert_eq!(found_subjects[2], b"subj.c");

            // Verify individual get works
            let mut ok = false;
            store
                .get(2, &mut |entry| {
                    assert_eq!(entry.subject, b"subj.b");
                    assert_eq!(entry.payload, b"payload2");
                    assert_eq!(entry.timestamp, 20);
                    ok = true;
                })
                .unwrap();
            assert!(ok);
        }
    }

    #[test]
    fn many_appends_info_and_for_each() {
        // Verifies correctness with many small appends (not large enough to
        // trigger 64MB rotation, but exercises the hot path thoroughly).
        let dir = tempfile::tempdir().unwrap();
        let mut store = TolerantStore::new(dir.path().join("store"));
        store.init().unwrap();

        let count = 1000u64;
        let mut expected_bytes = 0u64;
        for i in 0..count {
            let payload = format!("payload-{i}");
            store
                .append(make_entry(b"test.subject", payload.as_bytes()), i)
                .unwrap();
            expected_bytes += b"test.subject".len() as u64 + payload.len() as u64;
        }

        let info = store.info();
        assert_eq!(info.messages, count);
        assert_eq!(info.bytes, expected_bytes);
        assert_eq!(info.first_seq, 1);
        assert_eq!(info.last_seq, count);

        // Verify for_each over the full range
        let mut seen = 0u64;
        store
            .for_each(1, count + 1, &mut |entry| {
                assert_eq!(entry.subject, b"test.subject");
                seen += 1;
            })
            .unwrap();
        assert_eq!(seen, count);
    }

    #[test]
    fn purge_clears_all() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = TolerantStore::new(dir.path().join("store"));
        store.init().unwrap();

        for i in 0..5u8 {
            store.append(make_entry(b"x", &[i]), 0).unwrap();
        }
        assert_eq!(store.info().messages, 5);

        let deleted = store.purge();
        assert_eq!(deleted, 5);
        assert_eq!(store.info().messages, 0);
        assert_eq!(store.info().bytes, 0);
    }

    #[test]
    fn truncate_front() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = TolerantStore::new(dir.path().join("store"));
        store.init().unwrap();

        for i in 0..10u8 {
            store.append(make_entry(b"x", &[i]), i as u64).unwrap();
        }
        assert_eq!(store.info().messages, 10);

        // Truncate front up to seq 6 (removes seqs 1..6, keeps 6..10)
        let removed = store.truncate_front(6);
        assert_eq!(removed, 5);
        assert_eq!(store.info().messages, 5);
        assert_eq!(store.info().first_seq, 6);

        // First 5 entries should be gone
        for seq in 1..=5 {
            let ok = store
                .get(seq, &mut |_| panic!("should not be found"))
                .unwrap();
            assert!(!ok);
        }

        // Last 5 entries should be readable
        for seq in 6..=10 {
            let mut found = false;
            let ok = store
                .get(seq, &mut |entry| {
                    assert_eq!(entry.seq, seq);
                    // payload is (seq - 1) as u8
                    assert_eq!(entry.payload, &[(seq - 1) as u8]);
                    found = true;
                })
                .unwrap();
            assert!(ok);
            assert!(found);
        }
    }

    #[test]
    fn crash_recovery_truncated_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        std::fs::create_dir_all(&store_path).unwrap();

        // Manually write a segment file with 2 valid entries + garbage at the end
        let seg_path = segment::segment_path(&store_path, 1);
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&seg_path).unwrap();

            // Entry 1: subject="a", payload="bb", seq=1, ts=100
            let subj = b"a";
            let payload = b"bb";
            let mut header = [0u8; HEADER_SIZE];
            header[0] = MAGIC;
            header[1..3].copy_from_slice(&(subj.len() as u16).to_le_bytes());
            header[3..7].copy_from_slice(&(payload.len() as u32).to_le_bytes());
            header[7..15].copy_from_slice(&100u64.to_le_bytes());
            header[15..23].copy_from_slice(&1u64.to_le_bytes());
            header[23..27].copy_from_slice(&0u32.to_le_bytes());
            header[27] = 0;
            file.write_all(&header).unwrap();
            file.write_all(subj).unwrap();
            file.write_all(payload).unwrap();
            // B5: trailing CRC32 over [header || subject || payload].
            let mut h1 = crc32fast::Hasher::new();
            h1.update(&header);
            h1.update(subj);
            h1.update(payload);
            file.write_all(&h1.finalize().to_le_bytes()).unwrap();

            // Entry 2: subject="c", payload="dd", seq=2, ts=200
            let subj2 = b"c";
            let payload2 = b"dd";
            header[0] = MAGIC;
            header[1..3].copy_from_slice(&(subj2.len() as u16).to_le_bytes());
            header[3..7].copy_from_slice(&(payload2.len() as u32).to_le_bytes());
            header[7..15].copy_from_slice(&200u64.to_le_bytes());
            header[15..23].copy_from_slice(&2u64.to_le_bytes());
            header[23..27].copy_from_slice(&0u32.to_le_bytes());
            header[27] = 0;
            file.write_all(&header).unwrap();
            file.write_all(subj2).unwrap();
            file.write_all(payload2).unwrap();
            let mut h2 = crc32fast::Hasher::new();
            h2.update(&header);
            h2.update(subj2);
            h2.update(payload2);
            file.write_all(&h2.finalize().to_le_bytes()).unwrap();

            // Garbage bytes (no MAGIC prefix) simulating crash mid-write
            file.write_all(&[0x00, 0x01, 0x02, 0xFF, 0xFE]).unwrap();
        }

        // Open the store — it should recover only the 2 valid entries
        let mut store = TolerantStore::new(store_path);
        store.init().unwrap();

        let info = store.info();
        assert_eq!(info.messages, 2);

        let mut found = false;
        store
            .get(1, &mut |entry| {
                assert_eq!(entry.subject, b"a");
                assert_eq!(entry.payload, b"bb");
                assert_eq!(entry.timestamp, 100);
                found = true;
            })
            .unwrap();
        assert!(found);

        found = false;
        store
            .get(2, &mut |entry| {
                assert_eq!(entry.subject, b"c");
                assert_eq!(entry.payload, b"dd");
                assert_eq!(entry.timestamp, 200);
                found = true;
            })
            .unwrap();
        assert!(found);
    }

    /// **B5 regression**: a record whose payload bytes were silently
    /// flipped in the segment file must be rejected on recovery; the
    /// CRC32 trailer is the only thing keeping the drain from slicing
    /// the mmap with bad lengths.
    #[test]
    fn b5_corrupt_record_is_rejected_on_recovery() {
        use std::io::{Seek, SeekFrom, Write};

        let dir = tempfile::tempdir().unwrap();
        let store_path = dir.path().join("store");
        std::fs::create_dir_all(&store_path).unwrap();

        // First write two valid entries via the store API.
        {
            let mut store = TolerantStore::new(store_path.clone());
            store.init().unwrap();
            store
                .append(
                    EntryRef {
                        subject: b"good",
                        payload: b"first",
                        stream_id: 0,
                        flags: 0,
                        deliver_at_ms: 0,
                    },
                    1_000,
                )
                .unwrap();
            store
                .append(
                    EntryRef {
                        subject: b"good",
                        payload: b"second",
                        stream_id: 0,
                        flags: 0,
                        deliver_at_ms: 0,
                    },
                    2_000,
                )
                .unwrap();
            store.shutdown().unwrap();
        }

        // Find the sealed segment file and corrupt one payload byte
        // inside the SECOND record. The trailing CRC must catch it.
        let seg_path = std::fs::read_dir(&store_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "log"))
            .expect("sealed segment");
        {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&seg_path)
                .unwrap();
            // Each record = 28 hdr + 4 subj + N payload + 4 crc.
            // First record body = 5 payload → first record total = 41 B.
            // Second record body = 6 payload → starts at offset 41,
            // header 28, subject 4 ("good"), payload starts at 41+32 = 73.
            let payload_off = 41 + 28 + 4;
            file.seek(SeekFrom::Start(payload_off as u64)).unwrap();
            file.write_all(b"X").unwrap(); // flip first byte of payload
        }

        // Delete the .idx sidecar so recovery falls back to the CRC scan
        // path instead of trusting the pre-corruption index.
        for entry in std::fs::read_dir(&store_path).unwrap().flatten() {
            if entry.path().extension().is_some_and(|x| x == "idx") {
                std::fs::remove_file(entry.path()).unwrap();
            }
        }

        // Reopen: recovery must stop at the corrupt second entry and
        // load only the first one. The drain index must therefore have
        // exactly one entry; without the CRC check the drain would
        // crash or hand a partial entry to subscribers.
        let mut store = TolerantStore::new(store_path);
        store.init().unwrap();
        let info = store.info();
        assert_eq!(
            info.messages, 1,
            "CRC failure on entry 2 should drop it from the recovered set",
        );
    }

    #[test]
    fn read_range_basic() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = TolerantStore::new(dir.path().join("store"));
        store.init().unwrap();

        let entries = [
            make_entry(b"a", b"1"),
            make_entry(b"b", b"2"),
            make_entry(b"c", b"3"),
            make_entry(b"d", b"4"),
            make_entry(b"e", b"5"),
        ];
        store.append_batch(&entries, 100).unwrap();

        // Full range
        let range = store.read_range(1, 6).unwrap();
        assert_eq!(range.len(), 5);
        assert_eq!(range[0].subject, b"a");
        assert_eq!(range[0].payload, b"1");
        assert_eq!(range[0].seq, 1);
        assert_eq!(range[4].subject, b"e");
        assert_eq!(range[4].seq, 5);

        // Partial range
        let range = store.read_range(2, 4).unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].subject, b"b");
        assert_eq!(range[1].subject, b"c");

        // Empty range
        let range = store.read_range(10, 20).unwrap();
        assert!(range.is_empty());
    }

    #[test]
    fn read_range_after_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");

        {
            let mut store = TolerantStore::new(path.clone());
            store.init().unwrap();
            store.append(make_entry(b"subj.a", b"p1"), 10).unwrap();
            store.append(make_entry(b"subj.b", b"p2"), 20).unwrap();
            store.append(make_entry(b"subj.c", b"p3"), 30).unwrap();
            store.shutdown().unwrap();
        }

        {
            let mut store = TolerantStore::new(path);
            store.init().unwrap();

            let range = store.read_range(1, 4).unwrap();
            assert_eq!(range.len(), 3);
            assert_eq!(range[0].subject, b"subj.a");
            assert_eq!(range[0].payload, b"p1");
            assert_eq!(range[1].subject, b"subj.b");
            assert_eq!(range[2].subject, b"subj.c");
            assert_eq!(range[2].timestamp, 30);
        }
    }

    #[test]
    fn empty_store_init() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = TolerantStore::new(dir.path().join("empty_store"));
        store.init().unwrap();

        let info = store.info();
        assert_eq!(info.messages, 0);
        assert_eq!(info.bytes, 0);
        assert_eq!(info.first_seq, 1);

        // get on empty store returns false
        let ok = store
            .get(1, &mut |_| panic!("should not be called"))
            .unwrap();
        assert!(!ok);
    }

    #[test]
    fn tombstone_at_marks_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tombstone_test");

        {
            let mut store = TolerantStore::new(path.clone());
            store.init().unwrap();

            for i in 0..5u8 {
                store
                    .append(
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
            assert!(store.tombstone_at(3));
            // Idempotent
            assert!(!store.tombstone_at(3));
            // Non-existent
            assert!(!store.tombstone_at(999));

            // Verify flag is set on read
            let mut flags = 0u8;
            store
                .get(3, &mut |e| {
                    flags = e.flags;
                })
                .unwrap();
            assert_eq!(flags & crate::store::flags::TOMBSTONE, crate::store::flags::TOMBSTONE);
        }

        // Reopen and verify tombstone survives (active segment is mmap'd)
        {
            let mut store = TolerantStore::new(path);
            store.init().unwrap();

            let mut flags = 0u8;
            store
                .get(3, &mut |e| {
                    flags = e.flags;
                })
                .unwrap();
            assert_eq!(flags & crate::store::flags::TOMBSTONE, crate::store::flags::TOMBSTONE);

            // Non-tombstoned entry is clean
            let mut flags2 = 0xff;
            store
                .get(2, &mut |e| {
                    flags2 = e.flags;
                })
                .unwrap();
            assert_eq!(flags2 & crate::store::flags::TOMBSTONE, 0);
        }
    }
}
