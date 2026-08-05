//! File-backed Raft storage: durable HardState (JSON), durable append-only
//! log WAL, and durable snapshot file.
//!
//! # On-disk log format (`raft_log.wal`)
//!
//! An append-only sequence of length-prefixed records, little-endian:
//!
//! ```text
//! offset  size  field
//! 0       8     index (u64)
//! 8       8     term  (u64)
//! 16      4     payload_len (u32)
//! 20      4     crc (u32) — CRC-32 (IEEE, via crc32fast) over bytes
//!               (index || term || payload_len || payload), i.e. header
//!               bytes 0..20 plus the payload
//! 24      N     payload bytes
//! ```
//!
//! Every `append_entries` call writes its records and fsyncs BEFORE
//! returning `Ok` — the RaftStorage durability contract (P1-1) requires the
//! entry to be on stable storage before the leader may count the ack toward
//! a commit quorum. Recovery (`FileRaftStorage::new`) replays the file into
//! an in-RAM index; a torn tail (crash mid-write) is detected by an
//! incomplete header/payload and truncated away.
//!
//! # Record integrity (C7)
//!
//! Every record's CRC is verified during replay. The corruption policy
//! follows etcd's WAL:
//!
//! * **Tail corruption** (the CRC-failing record is the LAST record in the
//!   file, i.e. its bytes run exactly to EOF): indistinguishable from a torn
//!   append that never fully hit the platter — the record was never acked,
//!   so it is safe to drop. Recovery truncates the file back to the last
//!   good record boundary and logs loudly.
//! * **Mid-log corruption** (any complete record with a bad CRC that is NOT
//!   the last record): the record was durably written and acked, so it may
//!   be committed state — silently truncating it (and everything after it)
//!   could un-commit acknowledged entries and diverge this node. This is
//!   unrecoverable local corruption: `FileRaftStorage::new` fails loudly and
//!   the node must be repaired (restore from a peer / snapshot) by an
//!   operator.
//!
//! Caveat: a bit-flip inside the `payload_len` field of the LAST record is
//! indistinguishable from a torn tail (the claimed payload no longer fits
//! the file) and is handled by tail truncation; for any non-final record the
//! shifted framing makes the following record's CRC fail mid-log, which is
//! surfaced as fatal.
//!
//! Format migration: the CRC is mandatory. L1 wrote a zero placeholder in
//! this field; no deployment ever ran on the L1 format, so pre-C7 files are
//! NOT grandfathered — a placeholder-zero record simply fails verification
//! (unless its computed CRC happens to be zero, which is a legitimate value;
//! zero is never treated as "unchecked").
//!
//! `truncate_suffix` physically truncates the file at the byte offset of the
//! first dropped record and fsyncs. `truncate_before` (compaction) rewrites
//! the surviving suffix into a fresh file and atomically renames it over the
//! WAL. Snapshots live in their own durable file (`snapshot.bin`), written
//! via the same temp-file + fsync + atomic-rename discipline as HardState.
//!
//! # On-disk snapshot format (`snapshot.bin`, C6)
//!
//! A single versioned, checksummed blob, little-endian:
//!
//! ```text
//! offset  size  field
//! 0       4     magic ("ASNP")
//! 4       4     format version (u32) — currently 1
//! 8       8     last_included_index (u64)
//! 16      8     last_included_term (u64)
//! 24      8     payload_len (u64)
//! 32      4     crc (u32) — CRC-32 (IEEE, via crc32fast) over bytes 0..32
//!               (magic || version || index || term || payload_len) plus the
//!               payload
//! 36      N     payload bytes (exactly payload_len)
//! ```
//!
//! # Snapshot integrity policy (C6)
//!
//! The snapshot is the state machine's restore point — loading a truncated
//! or bit-rotted snapshot silently produces a divergent state machine, the
//! worst failure class Raft has. So verification is strict and the failure
//! mode is FATAL, mirroring the WAL's mid-log policy:
//!
//! * Missing file → `Ok(None)` (a node that never snapshotted).
//! * Bad magic, unknown version, truncated header/payload, `payload_len`
//!   mismatch, or CRC mismatch → `FileRaftStorage::new` **refuses to open**
//!   with a loud error. It is NOT treated as "no snapshot": after
//!   `truncate_before` the log's compacted prefix exists only in the
//!   snapshot, so pretending the snapshot is absent would replay a truncated
//!   log and diverge. The operator must repair the node (restore from a peer
//!   or delete the data dir and re-join).
//!
//! Format migration: like C7, no deployment ever ran on the unversioned L1
//! layout (16-byte meta header, no magic/CRC), so it is NOT grandfathered —
//! a legacy or foreign file fails the magic check and is surfaced as
//! corruption. The wire-level `InstallSnapshot` frame checksum is separate
//! (D5 territory); this covers the at-rest file only.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use arbitro_raft::{
    EntryPayload, HardState, LogEntry, LogIndex, PeerId, RaftError, RaftStorage, SnapshotMeta, Term,
};
use parking_lot::Mutex;

/// Fixed size of a WAL record header (index + term + payload_len + crc).
const REC_HEADER: usize = 24;

/// Byte offset of the CRC field within a record header; the CRC covers
/// everything before it (index || term || payload_len) plus the payload.
const REC_CRC_OFFSET: usize = 20;

/// CRC-32 (IEEE) over a record's covered bytes: the first 20 header bytes
/// (index || term || payload_len) followed by the payload.
fn record_crc(header_prefix: &[u8], payload: &[u8]) -> u32 {
    debug_assert_eq!(header_prefix.len(), REC_CRC_OFFSET);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(header_prefix);
    hasher.update(payload);
    hasher.finalize()
}

/// Magic prefix of `snapshot.bin` ("ASNP" = Arbitro SNaPshot).
const SNAP_MAGIC: [u8; 4] = *b"ASNP";

/// Current `snapshot.bin` format version. Bump on any layout change; a
/// reader that sees an unknown version refuses to open (never guesses).
const SNAP_VERSION: u32 = 1;

/// Fixed size of the snapshot header
/// (magic + version + index + term + payload_len + crc).
const SNAP_HEADER: usize = 36;

/// Byte offset of the CRC field within the snapshot header; the CRC covers
/// everything before it plus the payload.
const SNAP_CRC_OFFSET: usize = 32;

/// CRC-32 (IEEE) over a snapshot file's covered bytes: the first 32 header
/// bytes (magic || version || index || term || payload_len) followed by the
/// payload.
fn snapshot_crc(header_prefix: &[u8], payload: &[u8]) -> u32 {
    debug_assert_eq!(header_prefix.len(), SNAP_CRC_OFFSET);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(header_prefix);
    hasher.update(payload);
    hasher.finalize()
}

/// Sanity cap used during replay: a header claiming a payload larger than
/// this is treated as corruption (torn/garbage tail) and the file is
/// truncated at the last good record boundary. Matches the apply-loop's
/// 16 MiB per-entry ceiling with headroom.
const MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

/// Owned representation of a log entry plus its byte offset in the WAL file.
#[derive(Debug, Clone)]
struct StoredEntry {
    term: Term,
    index: LogIndex,
    payload: Vec<u8>,
    /// Byte offset of this record's header in `raft_log.wal`. Used by
    /// `truncate_suffix` to physically cut the file at the right point.
    offset: u64,
}

/// Mutable log state guarded by a single mutex: the WAL file handle, the
/// current end-of-log byte offset, and the in-RAM index (sorted by index —
/// `append_entries` enforces monotonicity, so binary search is valid).
struct LogInner {
    file: std::fs::File,
    /// Byte offset one past the last valid record (the write cursor).
    tail: u64,
    entries: Vec<StoredEntry>,
}

/// Simple JSON structure for persisting `HardState` to disk.
///
/// The upstream `HardState` / `Term` / `PeerId` types do not derive Serde
/// traits, so we use a thin DTO for file serialization.
#[derive(serde::Serialize, serde::Deserialize)]
struct HardStateDto {
    current_term: u64,
    voted_for: Option<u64>,
}

impl From<&HardState> for HardStateDto {
    fn from(hs: &HardState) -> Self {
        Self {
            current_term: hs.current_term.0,
            voted_for: hs.voted_for.map(|p| p.0),
        }
    }
}

impl From<HardStateDto> for HardState {
    fn from(dto: HardStateDto) -> Self {
        Self {
            current_term: Term(dto.current_term),
            voted_for: dto.voted_for.map(PeerId),
        }
    }
}

/// File-backed Raft storage rooted at a data directory.
///
/// * `hard_state.json` — term + vote (atomic replace + fsync).
/// * `raft_log.wal`    — append-only entry log (fsync per append).
/// * `snapshot.bin`    — latest snapshot: versioned + CRC-checked 36-byte
///   header + bytes (atomic replace + fsync). Also cached in RAM for fast
///   `load_snapshot`.
pub struct FileRaftStorage {
    hard_state_path: PathBuf,
    log_path: PathBuf,
    snapshot_path: PathBuf,
    log: Mutex<LogInner>,
    /// RAM cache of the on-disk snapshot (disk is the source of truth; the
    /// cache is populated at open and on every `save_snapshot`).
    snapshot: Mutex<Option<(SnapshotMeta, Vec<u8>)>>,
}

impl FileRaftStorage {
    /// Open (or create) the storage rooted at `data_dir`, replaying the WAL
    /// into the in-RAM index. `data_dir` must already exist.
    ///
    /// A torn tail (crash mid-append) is truncated away; everything up to
    /// the last complete record is recovered byte-identically.
    pub fn new(data_dir: &Path) -> Result<Self, RaftError> {
        let log_path = data_dir.join("raft_log.wal");
        let snapshot_path = data_dir.join("snapshot.bin");

        let inner = Self::open_and_replay(&log_path)
            .map_err(|e| RaftError::Storage(format!("open raft log: {e}")))?;

        // Make the log file's directory entry durable on first creation.
        #[cfg(unix)]
        {
            std::fs::File::open(data_dir)
                .and_then(|d| d.sync_all())
                .map_err(|e| RaftError::Storage(format!("fsync data dir: {e}")))?;
        }

        let snapshot = Self::read_snapshot_file(&snapshot_path)
            .map_err(|e| RaftError::Storage(format!("open snapshot: {e}")))?;

        Ok(Self {
            hard_state_path: data_dir.join("hard_state.json"),
            log_path,
            snapshot_path,
            log: Mutex::new(inner),
            snapshot: Mutex::new(snapshot),
        })
    }

    /// Open the WAL file and replay every complete record into RAM.
    fn open_and_replay(log_path: &Path) -> std::io::Result<LogInner> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // WAL: preserve the existing log for replay
            .open(log_path)?;

        let mut data = Vec::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut data)?;

        let mut entries: Vec<StoredEntry> = Vec::new();
        let mut off = 0usize;
        let mut good = 0usize; // offset one past the last fully-valid record

        while data.len() - off >= REC_HEADER {
            let index = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            let term = u64::from_le_bytes(data[off + 8..off + 16].try_into().unwrap());
            let len = u32::from_le_bytes(data[off + 16..off + 20].try_into().unwrap()) as usize;
            let stored_crc = u32::from_le_bytes(data[off + 20..off + 24].try_into().unwrap());

            if len > MAX_PAYLOAD_LEN {
                // Garbage header (torn write or corruption): stop here.
                break;
            }
            if data.len() - off - REC_HEADER < len {
                // Torn payload: the append never completed. Stop here.
                break;
            }

            // C7: verify the record checksum before trusting anything in it.
            let payload = &data[off + REC_HEADER..off + REC_HEADER + len];
            let computed_crc = record_crc(&data[off..off + REC_CRC_OFFSET], payload);
            if computed_crc != stored_crc {
                let record_end = off + REC_HEADER + len;
                if record_end == data.len() {
                    // Tail corruption: the CRC-failing record is the last one
                    // in the file — indistinguishable from a torn append, and
                    // the entry was never acked. Safe to drop: truncate back
                    // to the last good record boundary (below).
                    tracing::warn!(
                        offset = off,
                        index,
                        term,
                        stored_crc,
                        computed_crc,
                        "raft WAL: CRC mismatch on final record — treating as \
                         torn tail and truncating to last good boundary"
                    );
                    break;
                }
                // Mid-log corruption: a durably-written (possibly committed)
                // record is damaged. Truncating here would silently un-commit
                // acknowledged entries — refuse to open instead (etcd policy).
                tracing::error!(
                    offset = off,
                    index,
                    term,
                    stored_crc,
                    computed_crc,
                    "raft WAL: CRC mismatch on a non-final record — \
                     unrecoverable local corruption; refusing to open"
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "raft WAL corrupt: CRC mismatch mid-log at byte offset \
                         {off} (record index {index}, term {term}, stored crc \
                         {stored_crc:#010x}, computed {computed_crc:#010x}); \
                         possibly-committed entries are damaged — restore this \
                         node from a peer or snapshot"
                    ),
                ));
            }

            // The append path guarantees strictly increasing indexes on disk
            // (conflicting suffixes are physically truncated before the new
            // entries are written). A non-monotonic record therefore means
            // corruption: stop at the last good boundary.
            if entries.last().is_some_and(|e| e.index.0 >= index) {
                break;
            }

            entries.push(StoredEntry {
                term: Term(term),
                index: LogIndex(index),
                payload: data[off + REC_HEADER..off + REC_HEADER + len].to_vec(),
                offset: off as u64,
            });
            off += REC_HEADER + len;
            good = off;
        }

        if (good as u64) < file.metadata()?.len() {
            // Drop the torn/garbage tail durably so the next append starts
            // at a clean record boundary.
            file.set_len(good as u64)?;
            file.sync_all()?;
        }

        Ok(LogInner {
            file,
            tail: good as u64,
            entries,
        })
    }

    /// Encode one WAL record into `buf`. Layout documented at module level.
    fn encode_record(buf: &mut Vec<u8>, index: u64, term: u64, payload: &[u8]) {
        let start = buf.len();
        buf.extend_from_slice(&index.to_le_bytes());
        buf.extend_from_slice(&term.to_le_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        // C7: CRC-32 over (index || term || payload_len || payload), verified
        // on every replay (see "Record integrity" in the module docs).
        let crc = record_crc(&buf[start..start + REC_CRC_OFFSET], payload);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(payload);
    }

    /// Physically truncate the WAL at the first entry with `index >= from`,
    /// fsync, then drop the RAM tail. No-op if nothing is at/after `from`.
    fn truncate_suffix_locked(inner: &mut LogInner, from: LogIndex) -> Result<(), RaftError> {
        let pos = inner.entries.partition_point(|e| e.index < from);
        if pos == inner.entries.len() {
            return Ok(());
        }
        let cut = inner.entries[pos].offset;
        inner
            .file
            .set_len(cut)
            .map_err(|e| storage_write_err("truncate raft log", e))?;
        // Durability: the truncation itself must survive a crash, otherwise
        // a replaced suffix could resurrect on restart.
        inner
            .file
            .sync_all()
            .map_err(|e| storage_write_err("fsync raft log", e))?;
        inner.entries.truncate(pos);
        inner.tail = cut;
        Ok(())
    }

    /// Read and verify `snapshot.bin` if present. Layout and corruption
    /// policy are documented at module level ("On-disk snapshot format" /
    /// "Snapshot integrity policy"): a missing file is `Ok(None)`; ANY
    /// malformed file (bad magic, unknown version, truncation, length or CRC
    /// mismatch) is an error — the caller (`FileRaftStorage::new`) refuses
    /// to open, because treating a corrupt snapshot as absent would replay a
    /// truncated log into a divergent state machine.
    fn read_snapshot_file(path: &Path) -> std::io::Result<Option<(SnapshotMeta, Vec<u8>)>> {
        let corrupt = |what: String| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "snapshot.bin corrupt: {what}; refusing to open — restore \
                     this node from a peer or remove its data dir and re-join"
                ),
            )
        };

        let data = match std::fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if data.len() < SNAP_HEADER {
            return Err(corrupt(format!(
                "file is {} bytes, shorter than the {SNAP_HEADER}-byte header \
                 (truncated write or pre-versioning format)",
                data.len()
            )));
        }
        if data[0..4] != SNAP_MAGIC {
            return Err(corrupt(format!(
                "bad magic {:02x?} (expected {SNAP_MAGIC:02x?}); legacy \
                 unversioned or foreign file",
                &data[0..4]
            )));
        }
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        if version != SNAP_VERSION {
            return Err(corrupt(format!(
                "unknown format version {version} (this build reads version \
                 {SNAP_VERSION})"
            )));
        }
        let index = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let term = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let payload_len = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let stored_crc = u32::from_le_bytes(data[SNAP_CRC_OFFSET..SNAP_HEADER].try_into().unwrap());

        // The payload must be exactly payload_len bytes: shorter = truncated
        // write, longer = trailing garbage — both are corruption.
        let actual_len = (data.len() - SNAP_HEADER) as u64;
        if actual_len != payload_len {
            return Err(corrupt(format!(
                "payload is {actual_len} bytes but the header claims \
                 {payload_len} (truncated or garbled file)"
            )));
        }

        let payload = &data[SNAP_HEADER..];
        let computed_crc = snapshot_crc(&data[..SNAP_CRC_OFFSET], payload);
        if computed_crc != stored_crc {
            return Err(corrupt(format!(
                "CRC mismatch (stored {stored_crc:#010x}, computed \
                 {computed_crc:#010x}); the snapshot is bit-rotted"
            )));
        }

        let meta = SnapshotMeta {
            last_included_index: LogIndex(index),
            last_included_term: Term(term),
        };
        Ok(Some((meta, payload.to_vec())))
    }
}

/// C8: wrap an IO failure from a WRITE path (append / write / fsync /
/// rename) into a `RaftError`.
///
/// Resource-exhaustion kinds (`StorageFull` / `QuotaExceeded` /
/// `OutOfMemory` — ENOSPC/EDQUOT class) are forwarded as
/// [`RaftError::Io`] with the kind preserved, so the raft run loop
/// classifies them as `ErrorClass::Resource` and degrades to read-only
/// survival instead of dying (crate-side C8). Every other kind — and every
/// logic error — stays the stringified [`RaftError::Storage`], which is
/// Fatal: corruption or a real bug must never be masked as "disk full".
fn storage_write_err(what: &str, e: std::io::Error) -> RaftError {
    match e.kind() {
        std::io::ErrorKind::StorageFull
        | std::io::ErrorKind::QuotaExceeded
        | std::io::ErrorKind::OutOfMemory => {
            RaftError::Io(std::io::Error::new(e.kind(), format!("{what}: {e}")))
        }
        _ => RaftError::Storage(format!("{what}: {e}")),
    }
}

/// Crash-safe file write: write to a sibling temp file, fsync it, atomically
/// rename over `path`, then fsync the parent directory so the rename itself
/// is durable.
///
/// This is required by the `RaftStorage` durability contract (C-D1): the
/// value must be on stable storage BEFORE the save returns `Ok`. A plain
/// `fs::write` only reaches the OS page cache — on power loss the node would
/// "forget" its vote and could double-vote in the same term.
fn write_file_durable(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "path has no parent directory",
        )
    })?;

    // Sibling temp file in the same directory so the rename is atomic
    // (rename across filesystems is not).
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);

    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(data)?;
        // Durability point 1: file contents reach the disk.
        f.sync_all()?;
    }

    // Atomic replace: readers see either the old or the new full content,
    // never a partial write.
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }

    // Durability point 2: the directory entry (the rename) reaches the disk.
    // Without this, a crash can revert to the old file even though the data
    // blocks of the new one were synced. Directory fsync is a POSIX-ism;
    // on non-Unix platforms opening a directory fails, so it is Unix-only.
    #[cfg(unix)]
    {
        std::fs::File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir; // rename on NTFS is atomic; no portable dir-fsync exists
    }

    Ok(())
}

/// Arc-wrapped storage that can be shared between the RaftNode (which takes
/// ownership of its `S: RaftStorage`) and the apply loop (which needs to
/// read committed entries).  Delegates every method to the inner
/// `FileRaftStorage`.
pub struct SharedRaftStorage(pub std::sync::Arc<FileRaftStorage>);

impl RaftStorage for SharedRaftStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        self.0.load_hard_state()
    }
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError> {
        self.0.save_hard_state(state)
    }
    fn append_entries(&self, entries: &[LogEntry<'_>]) -> Result<(), RaftError> {
        self.0.append_entries(entries)
    }
    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        self.0.entry_at(index, payload_buf)
    }
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        self.0.last_log_position()
    }
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        self.0.truncate_suffix(from)
    }
    fn truncate_before(&self, up_to: LogIndex) -> Result<(), RaftError> {
        self.0.truncate_before(up_to)
    }
    fn save_snapshot(&self, meta: &SnapshotMeta, snapshot: &[u8]) -> Result<(), RaftError> {
        self.0.save_snapshot(meta, snapshot)
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        self.0.load_snapshot()
    }
    fn read_entries<'a>(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry<'a>>,
        payload_buf: &'a mut [u8],
    ) -> Result<usize, RaftError> {
        self.0.read_entries(from, to, out, payload_buf)
    }
}

impl RaftStorage for FileRaftStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        if !self.hard_state_path.exists() {
            return Ok(HardState::default());
        }
        let data = std::fs::read(&self.hard_state_path)
            .map_err(|e| RaftError::Storage(format!("read hard_state: {e}")))?;
        let dto: HardStateDto = serde_json::from_slice(&data)
            .map_err(|e| RaftError::Storage(format!("parse hard_state: {e}")))?;
        Ok(dto.into())
    }

    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError> {
        let dto = HardStateDto::from(state);
        let data = serde_json::to_vec(&dto)
            .map_err(|e| RaftError::Storage(format!("serialize hard_state: {e}")))?;
        // Durability contract (C-D1): the state must be on stable storage
        // before we return Ok — temp file + fsync + atomic rename + dir fsync.
        write_file_durable(&self.hard_state_path, &data)
            .map_err(|e| storage_write_err("write hard_state", e))?;
        Ok(())
    }

    fn append_entries(&self, new_entries: &[LogEntry<'_>]) -> Result<(), RaftError> {
        if new_entries.is_empty() {
            return Ok(());
        }
        // Batches must be strictly increasing so the on-disk log (and the
        // binary-searchable RAM index) stay sorted.
        for w in new_entries.windows(2) {
            if w[1].index.0 <= w[0].index.0 {
                return Err(RaftError::Storage(
                    "append_entries: batch indexes not strictly increasing".into(),
                ));
            }
        }

        let mut inner = self.log.lock();

        // Conflict overwrite: if the batch overlaps the existing tail,
        // physically drop the conflicting suffix first so the file never
        // contains two records for the same index (replay relies on this).
        let first = new_entries[0].index;
        if inner.entries.last().is_some_and(|e| e.index >= first) {
            Self::truncate_suffix_locked(&mut inner, first)?;
        }

        // Serialize the whole batch into one buffer → one write + one fsync.
        let total: usize = new_entries
            .iter()
            .map(|e| REC_HEADER + e.payload.0.len())
            .sum();
        let mut buf = Vec::with_capacity(total);
        let base = inner.tail;
        let mut staged = Vec::with_capacity(new_entries.len());
        for e in new_entries {
            let offset = base + buf.len() as u64;
            Self::encode_record(&mut buf, e.index.0, e.term.0, e.payload.0);
            staged.push(StoredEntry {
                term: e.term,
                index: e.index,
                payload: e.payload.0.to_vec(),
                offset,
            });
        }

        inner
            .file
            .seek(SeekFrom::Start(base))
            .map_err(|e| storage_write_err("seek raft log", e))?;
        inner
            .file
            .write_all(&buf)
            .map_err(|e| storage_write_err("write raft log", e))?;
        // Durability contract (P1-1): the entries must be on stable storage
        // BEFORE this returns Ok — the leader counts our ack toward the
        // commit quorum. Per-append fsync; group-commit is deferred (G2).
        inner
            .file
            .sync_all()
            .map_err(|e| storage_write_err("fsync raft log", e))?;

        inner.tail = base + buf.len() as u64;
        inner.entries.extend(staged);
        Ok(())
    }

    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        let inner = self.log.lock();
        // Entries are sorted by index (append enforces monotonicity).
        let Ok(i) = inner.entries.binary_search_by(|e| e.index.cmp(&index)) else {
            return Ok(None);
        };
        let e = &inner.entries[i];
        if payload_buf.len() < e.payload.len() {
            return Err(RaftError::Storage("payload_buf too small".into()));
        }
        payload_buf[..e.payload.len()].copy_from_slice(&e.payload);

        // SAFETY: payload_buf outlives the returned LogEntry because both
        // share the caller's lifetime 'a.
        let slice =
            unsafe { std::mem::transmute::<&[u8], &'a [u8]>(&payload_buf[..e.payload.len()]) };

        Ok(Some(LogEntry {
            term: e.term,
            index: e.index,
            payload: EntryPayload(slice),
        }))
    }

    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        Ok(self
            .log
            .lock()
            .entries
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or_default())
    }

    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        let mut inner = self.log.lock();
        Self::truncate_suffix_locked(&mut inner, from)
    }

    /// Compaction: durably drop every entry with `index < up_to` by rewriting
    /// the surviving suffix into a fresh file and atomically renaming it over
    /// the WAL (same crash-safe pattern as `write_file_durable`).
    fn truncate_before(&self, up_to: LogIndex) -> Result<(), RaftError> {
        let mut inner = self.log.lock();
        let pos = inner.entries.partition_point(|e| e.index < up_to);
        if pos == 0 {
            return Ok(()); // nothing to compact
        }

        // Re-encode the survivors with fresh offsets.
        let mut buf = Vec::new();
        let mut new_offsets = Vec::with_capacity(inner.entries.len() - pos);
        for e in &inner.entries[pos..] {
            new_offsets.push(buf.len() as u64);
            Self::encode_record(&mut buf, e.index.0, e.term.0, &e.payload);
        }

        let mut tmp = self.log_path.as_os_str().to_owned();
        tmp.push(".compact.tmp");
        let tmp = PathBuf::from(tmp);
        {
            let mut f = std::fs::File::create(&tmp)
                .map_err(|e| storage_write_err("create compact tmp", e))?;
            f.write_all(&buf)
                .map_err(|e| storage_write_err("write compact tmp", e))?;
            f.sync_all()
                .map_err(|e| storage_write_err("fsync compact tmp", e))?;
        }

        // Atomic replace. On Unix this succeeds even while the old WAL
        // handle is still open (the old inode lingers until we swap the
        // handle below). On Windows, renaming over an open file fails —
        // compaction is then skipped with an error, which is safe (the log
        // merely stays uncompacted); production runs on Linux/WSL.
        if let Err(e) = std::fs::rename(&tmp, &self.log_path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(storage_write_err("rename compacted log", e));
        }
        #[cfg(unix)]
        if let Some(dir) = self.log_path.parent() {
            std::fs::File::open(dir)
                .and_then(|d| d.sync_all())
                .map_err(|e| storage_write_err("fsync data dir", e))?;
        }

        // Swap the handle to the new file and fix up the RAM index.
        inner.file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.log_path)
            .map_err(|e| RaftError::Storage(format!("reopen compacted log: {e}")))?;
        inner.tail = buf.len() as u64;
        inner.entries.drain(..pos);
        for (e, off) in inner.entries.iter_mut().zip(new_offsets) {
            e.offset = off;
        }
        Ok(())
    }

    fn save_snapshot(&self, meta: &SnapshotMeta, snapshot: &[u8]) -> Result<(), RaftError> {
        // Versioned + checksummed header (C6, layout in the module docs) +
        // snapshot bytes, atomically replaced + fsync'd.
        let mut data = Vec::with_capacity(SNAP_HEADER + snapshot.len());
        data.extend_from_slice(&SNAP_MAGIC);
        data.extend_from_slice(&SNAP_VERSION.to_le_bytes());
        data.extend_from_slice(&meta.last_included_index.0.to_le_bytes());
        data.extend_from_slice(&meta.last_included_term.0.to_le_bytes());
        data.extend_from_slice(&(snapshot.len() as u64).to_le_bytes());
        // C6: CRC-32 over (magic || version || index || term || payload_len
        // || payload), verified on every open.
        let crc = snapshot_crc(&data[..SNAP_CRC_OFFSET], snapshot);
        data.extend_from_slice(&crc.to_le_bytes());
        data.extend_from_slice(snapshot);
        write_file_durable(&self.snapshot_path, &data)
            .map_err(|e| storage_write_err("write snapshot", e))?;
        *self.snapshot.lock() = Some((meta.clone(), snapshot.to_vec()));
        Ok(())
    }

    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(self.snapshot.lock().clone())
    }

    fn read_entries<'a>(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry<'a>>,
        payload_buf: &'a mut [u8],
    ) -> Result<usize, RaftError> {
        let inner = self.log.lock();
        let start = inner.entries.partition_point(|e| e.index < from);
        let mut offset = 0;
        for e in inner.entries[start..].iter().take_while(|e| e.index < to) {
            let len = e.payload.len();
            if offset + len > payload_buf.len() {
                // Audit #10: entries already pushed into `out` hold
                // transmuted `&'a` views into `payload_buf`. The caller's
                // recovery path resizes `payload_buf` and retries — if the
                // partial fill survived this error, those views would
                // dangle into the old allocation (invalidated-reference
                // UB, one refactor away from a use-after-free). Clear the
                // partial fill so an Err never leaks borrowed views.
                out.clear();
                return Err(RaftError::Storage("payload_buf too small".into()));
            }
            payload_buf[offset..offset + len].copy_from_slice(&e.payload);

            // SAFETY: payload_buf outlives the returned LogEntry because
            // both share the caller's lifetime 'a.
            let slice = unsafe {
                std::mem::transmute::<&[u8], &'a [u8]>(&payload_buf[offset..offset + len])
            };

            out.push(LogEntry {
                term: e.term,
                index: e.index,
                payload: EntryPayload(slice),
            });
            offset += len;
        }
        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Unique temp dir per test (no tempfile dep; cleaned up by the guard).
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static SEQ: AtomicU64 = AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "arbitro_l2_{}_{}_{}",
                tag,
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).expect("create temp dir");
            TempDir(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn entry<'a>(idx: u64, term: u64, payload: &'a [u8]) -> LogEntry<'a> {
        LogEntry {
            term: Term(term),
            index: LogIndex(idx),
            payload: EntryPayload(payload),
        }
    }

    /// Read an entry's (term, payload) via the trait, panicking on absence.
    fn must_read(storage: &FileRaftStorage, idx: u64) -> (u64, Vec<u8>) {
        let mut buf = vec![0u8; 4096];
        let e = storage
            .entry_at(LogIndex(idx), &mut buf)
            .expect("entry_at")
            .unwrap_or_else(|| panic!("entry {idx} missing"));
        (e.term.0, e.payload.0.to_vec())
    }

    #[test]
    fn load_without_file_returns_default() {
        let dir = TempDir::new("default");
        let storage = FileRaftStorage::new(&dir.0).expect("open");
        let hs = storage.load_hard_state().expect("load");
        assert_eq!(hs.current_term, Term(0));
        assert_eq!(hs.voted_for, None);
        assert_eq!(
            storage.last_log_position().expect("pos"),
            (LogIndex(0), Term(0))
        );
        assert!(storage.load_snapshot().expect("snap").is_none());
    }

    /// C-D1 round-trip: a saved vote must be recovered exactly by a fresh
    /// storage over the same path — a node must never forget it voted in a
    /// term (double-vote hazard).
    #[test]
    fn hard_state_survives_reopen_no_double_vote() {
        let dir = TempDir::new("reopen");

        let storage = FileRaftStorage::new(&dir.0).expect("open");
        let hs = HardState {
            current_term: Term(7),
            voted_for: Some(PeerId(3)),
        };
        storage.save_hard_state(&hs).expect("save");
        drop(storage);

        // Simulate restart: brand-new storage over the same data dir.
        let reopened = FileRaftStorage::new(&dir.0).expect("reopen");
        let recovered = reopened.load_hard_state().expect("load");
        assert_eq!(recovered.current_term, Term(7));
        assert_eq!(recovered.voted_for, Some(PeerId(3)));
    }

    /// Overwrite path: the latest save wins after reopen (atomic replace,
    /// not append/partial).
    #[test]
    fn hard_state_overwrite_recovers_latest() {
        let dir = TempDir::new("overwrite");

        let storage = FileRaftStorage::new(&dir.0).expect("open");
        storage
            .save_hard_state(&HardState {
                current_term: Term(1),
                voted_for: Some(PeerId(1)),
            })
            .expect("save 1");
        storage
            .save_hard_state(&HardState {
                current_term: Term(2),
                voted_for: None,
            })
            .expect("save 2");
        storage
            .save_hard_state(&HardState {
                current_term: Term(9),
                voted_for: Some(PeerId(2)),
            })
            .expect("save 3");
        drop(storage);

        let recovered = FileRaftStorage::new(&dir.0)
            .expect("reopen")
            .load_hard_state()
            .expect("load");
        assert_eq!(recovered.current_term, Term(9));
        assert_eq!(recovered.voted_for, Some(PeerId(2)));
    }

    /// The crash-safe write paths must not leave temp residue: after saving
    /// hard state, appending log entries, and saving a snapshot, the data
    /// dir contains exactly the three expected files — no `.tmp` siblings.
    #[test]
    fn writes_leave_no_tmp_residue() {
        let dir = TempDir::new("residue");

        let storage = FileRaftStorage::new(&dir.0).expect("open");
        for term in 1..=5u64 {
            storage
                .save_hard_state(&HardState {
                    current_term: Term(term),
                    voted_for: Some(PeerId(term)),
                })
                .expect("save");
        }
        storage
            .append_entries(&[entry(1, 1, b"a"), entry(2, 1, b"b"), entry(3, 1, b"c")])
            .expect("append");
        storage
            .save_snapshot(
                &SnapshotMeta {
                    last_included_index: LogIndex(2),
                    last_included_term: Term(1),
                },
                b"snap",
            )
            .expect("snapshot");
        storage.truncate_before(LogIndex(3)).expect("compact");

        let mut names: Vec<String> = std::fs::read_dir(&dir.0)
            .expect("read dir")
            .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "hard_state.json".to_string(),
                "raft_log.wal".to_string(),
                "snapshot.bin".to_string(),
            ],
            "dir = {names:?}"
        );
    }

    /// A pre-existing stale temp file (crash between write and rename) must
    /// not break recovery: load reads the target file, and the next save
    /// replaces the stale temp.
    #[test]
    fn stale_tmp_from_previous_crash_is_harmless() {
        let dir = TempDir::new("staletmp");

        // Persist a valid state, then plant a corrupt leftover temp file as
        // if a previous process died between fsync and rename.
        let storage = FileRaftStorage::new(&dir.0).expect("open");
        storage
            .save_hard_state(&HardState {
                current_term: Term(4),
                voted_for: Some(PeerId(1)),
            })
            .expect("save");
        std::fs::write(dir.0.join("hard_state.json.tmp"), b"{corrupt").expect("plant stale tmp");
        drop(storage);

        // Recovery ignores the temp file entirely.
        let reopened = FileRaftStorage::new(&dir.0).expect("reopen");
        let recovered = reopened.load_hard_state().expect("load");
        assert_eq!(recovered.current_term, Term(4));
        assert_eq!(recovered.voted_for, Some(PeerId(1)));

        // The next durable save overwrites the stale temp and leaves no residue.
        reopened
            .save_hard_state(&HardState {
                current_term: Term(5),
                voted_for: None,
            })
            .expect("save after stale tmp");
        assert!(!dir.0.join("hard_state.json.tmp").exists());
        let recovered = reopened.load_hard_state().expect("reload");
        assert_eq!(recovered.current_term, Term(5));
    }

    // ---------------------------------------------------------------------
    // L1: durable log tests
    // ---------------------------------------------------------------------

    /// THE core L1 test: a full restart (power event) must not lose a single
    /// appended entry. Append across several terms with varied payloads
    /// (including an empty one), drop the storage, reopen over the same dir,
    /// and assert every entry recovers byte-identical.
    #[test]
    fn log_survives_reopen_byte_identical() {
        let dir = TempDir::new("logreopen");

        // (index, term, payload) across three terms, varied payload sizes.
        let mut expected: Vec<(u64, u64, Vec<u8>)> = Vec::new();
        for i in 1..=50u64 {
            let term = 1 + (i - 1) / 20; // 1..=20 → t1, 21..=40 → t2, 41..=50 → t3
            let payload = if i == 25 {
                Vec::new() // empty payload must round-trip too
            } else {
                format!("entry-{i}-{}", "x".repeat((i as usize * 7) % 300)).into_bytes()
            };
            expected.push((i, term, payload));
        }

        {
            let storage = FileRaftStorage::new(&dir.0).expect("open");
            // Append in three batches (as the raft node would).
            for chunk in expected.chunks(17) {
                let batch: Vec<LogEntry<'_>> =
                    chunk.iter().map(|(i, t, p)| entry(*i, *t, p)).collect();
                storage.append_entries(&batch).expect("append");
            }
            assert_eq!(
                storage.last_log_position().expect("pos"),
                (LogIndex(50), Term(3))
            );
        } // drop = simulated process death after fsync

        let reopened = FileRaftStorage::new(&dir.0).expect("reopen");
        assert_eq!(
            reopened.last_log_position().expect("pos"),
            (LogIndex(50), Term(3)),
            "last_log_position must survive restart"
        );
        for (i, t, p) in &expected {
            let (term, payload) = must_read(&reopened, *i);
            assert_eq!(term, *t, "term mismatch at index {i}");
            assert_eq!(&payload, p, "payload mismatch at index {i}");
        }

        // Bulk read path must agree with entry_at.
        let mut out = Vec::new();
        let mut buf = vec![0u8; 64 * 1024];
        let used = reopened
            .read_entries(LogIndex(1), LogIndex(51), &mut out, &mut buf)
            .expect("read_entries");
        assert_eq!(out.len(), 50);
        assert!(used > 0);
        for (e, (i, t, p)) in out.iter().zip(expected.iter()) {
            assert_eq!(e.index.0, *i);
            assert_eq!(e.term.0, *t);
            assert_eq!(e.payload.0, &p[..]);
        }
    }

    /// Audit #10 — `read_entries` must not leak partially-filled views on
    /// the "payload_buf too small" error path. The pushed `LogEntry<'a>`
    /// views are transmuted borrows into `payload_buf`; the caller
    /// (apply_loop) resizes the buffer and retries after this error, so a
    /// surviving partial fill would dangle into the old allocation.
    #[test]
    fn read_entries_error_path_clears_partial_fill() {
        let dir = TempDir::new("readclear");
        let storage = FileRaftStorage::new(&dir.0).expect("open");
        storage
            .append_entries(&[
                entry(1, 1, b"small"),
                entry(2, 1, &[0xBB; 128]), // won't fit after entry 1
            ])
            .expect("append");

        let mut out = Vec::new();
        // Big enough for entry 1 (5 B) but not entry 2 (128 B).
        let mut buf = vec![0u8; 16];
        let err = storage
            .read_entries(LogIndex(1), LogIndex(3), &mut out, &mut buf)
            .expect_err("undersized payload_buf must error");
        assert!(matches!(err, RaftError::Storage(_)));
        assert!(
            out.is_empty(),
            "error path must clear the partial fill (no dangling views)"
        );

        // Retry with a big-enough buffer must succeed cleanly.
        let mut big = vec![0u8; 4096];
        storage
            .read_entries(LogIndex(1), LogIndex(3), &mut out, &mut big)
            .expect("retry with resized buffer");
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].payload.0, b"small");
        assert_eq!(out[1].payload.0, &[0xBB; 128][..]);
    }

    /// truncate_suffix must be durable: after dropping a suffix and
    /// restarting, only the prefix survives; a post-truncate append with a
    /// different term (leader-conflict overwrite) also survives restart.
    #[test]
    fn truncate_suffix_durable_across_reopen() {
        let dir = TempDir::new("truncsuffix");

        {
            let storage = FileRaftStorage::new(&dir.0).expect("open");
            let batch: Vec<(u64, Vec<u8>)> = (1..=10u64)
                .map(|i| (i, format!("v{i}").into_bytes()))
                .collect();
            let entries: Vec<LogEntry<'_>> = batch.iter().map(|(i, p)| entry(*i, 1, p)).collect();
            storage.append_entries(&entries).expect("append");

            storage.truncate_suffix(LogIndex(6)).expect("truncate");
            assert_eq!(
                storage.last_log_position().expect("pos"),
                (LogIndex(5), Term(1))
            );
        }

        // Restart 1: only 1..=5 survive.
        {
            let reopened = FileRaftStorage::new(&dir.0).expect("reopen");
            assert_eq!(
                reopened.last_log_position().expect("pos"),
                (LogIndex(5), Term(1))
            );
            let mut buf = vec![0u8; 128];
            assert!(reopened
                .entry_at(LogIndex(6), &mut buf)
                .expect("entry_at")
                .is_none());
            assert!(reopened
                .entry_at(LogIndex(10), &mut buf)
                .expect("entry_at")
                .is_none());
            let (t, p) = must_read(&reopened, 5);
            assert_eq!((t, p), (1, b"v5".to_vec()));

            // New leader overwrites from index 6 with term 2 (append after
            // truncate — also exercises the implicit-conflict path).
            reopened
                .append_entries(&[entry(6, 2, b"new6")])
                .expect("append after truncate");
        }

        // Restart 2: the overwrite is durable.
        let reopened = FileRaftStorage::new(&dir.0).expect("reopen 2");
        assert_eq!(
            reopened.last_log_position().expect("pos"),
            (LogIndex(6), Term(2))
        );
        let (t, p) = must_read(&reopened, 6);
        assert_eq!((t, p), (2, b"new6".to_vec()));
    }

    /// Compaction + snapshot interplay: after snapshot-at-K and
    /// truncate_before, a restart restores the snapshot AND replays the log
    /// tail — entries below the boundary are gone, the tail is intact.
    #[test]
    fn truncate_before_with_snapshot_reopen() {
        let dir = TempDir::new("compact");

        let snap_bytes = b"state-machine-through-6".to_vec();
        {
            let storage = FileRaftStorage::new(&dir.0).expect("open");
            let batch: Vec<(u64, Vec<u8>)> = (1..=10u64)
                .map(|i| (i, format!("p{i}").into_bytes()))
                .collect();
            let entries: Vec<LogEntry<'_>> = batch.iter().map(|(i, p)| entry(*i, 1, p)).collect();
            storage.append_entries(&entries).expect("append");

            storage
                .save_snapshot(
                    &SnapshotMeta {
                        last_included_index: LogIndex(6),
                        last_included_term: Term(1),
                    },
                    &snap_bytes,
                )
                .expect("snapshot");
            // Compact everything the snapshot covers (delete index < 7).
            storage.truncate_before(LogIndex(7)).expect("compact");

            // In-process: prefix gone, tail readable, tip unchanged.
            let mut buf = vec![0u8; 128];
            assert!(storage
                .entry_at(LogIndex(6), &mut buf)
                .expect("at")
                .is_none());
            assert_eq!(
                storage.last_log_position().expect("pos"),
                (LogIndex(10), Term(1))
            );
        }

        // Restart: snapshot loads, tail replays, prefix stays gone.
        let reopened = FileRaftStorage::new(&dir.0).expect("reopen");
        let (meta, bytes) = reopened
            .load_snapshot()
            .expect("load_snapshot")
            .expect("snapshot must survive restart");
        assert_eq!(meta.last_included_index, LogIndex(6));
        assert_eq!(meta.last_included_term, Term(1));
        assert_eq!(bytes, snap_bytes);

        let mut buf = vec![0u8; 128];
        for i in 1..=6u64 {
            assert!(
                reopened
                    .entry_at(LogIndex(i), &mut buf)
                    .expect("at")
                    .is_none(),
                "compacted entry {i} must not resurrect"
            );
        }
        for i in 7..=10u64 {
            let (t, p) = must_read(&reopened, i);
            assert_eq!((t, p), (1, format!("p{i}").into_bytes()));
        }
        assert_eq!(
            reopened.last_log_position().expect("pos"),
            (LogIndex(10), Term(1))
        );

        // The compacted log must keep working: append continues at the tip
        // and survives another restart.
        reopened
            .append_entries(&[entry(11, 2, b"p11")])
            .expect("append after compaction");
        drop(reopened);
        let again = FileRaftStorage::new(&dir.0).expect("reopen 2");
        assert_eq!(
            again.last_log_position().expect("pos"),
            (LogIndex(11), Term(2))
        );
    }

    /// Torn tail: a crash mid-append leaves a partial record at the end of
    /// the WAL. Recovery must keep every complete record, drop the torn
    /// bytes, and leave the file appendable.
    #[test]
    fn torn_tail_is_truncated_on_recovery() {
        let dir = TempDir::new("torntail");
        let log_path = dir.0.join("raft_log.wal");

        {
            let storage = FileRaftStorage::new(&dir.0).expect("open");
            let payloads: Vec<(u64, Vec<u8>)> = (1..=5u64)
                .map(|i| (i, format!("ok{i}").into_bytes()))
                .collect();
            let entries: Vec<LogEntry<'_>> =
                payloads.iter().map(|(i, p)| entry(*i, 1, p)).collect();
            storage.append_entries(&entries).expect("append");
        }
        let good_len = std::fs::metadata(&log_path).expect("meta").len();

        // Simulate a torn write: a full header claiming a 100-byte payload,
        // followed by only 3 payload bytes (the crash hit mid-payload).
        {
            let mut torn = Vec::new();
            torn.extend_from_slice(&6u64.to_le_bytes()); // index
            torn.extend_from_slice(&1u64.to_le_bytes()); // term
            torn.extend_from_slice(&100u32.to_le_bytes()); // payload_len
            torn.extend_from_slice(&0u32.to_le_bytes()); // bogus crc (never reached: length check fires first)
            torn.extend_from_slice(b"abc"); // only 3 of 100 bytes
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&log_path)
                .expect("open for tear");
            f.write_all(&torn).expect("write torn tail");
            f.sync_all().expect("sync torn tail");
        }

        // Recovery: complete records survive, the torn record is dropped and
        // the file is truncated back to the last good boundary.
        let reopened = FileRaftStorage::new(&dir.0).expect("reopen");
        assert_eq!(
            reopened.last_log_position().expect("pos"),
            (LogIndex(5), Term(1))
        );
        for i in 1..=5u64 {
            let (t, p) = must_read(&reopened, i);
            assert_eq!((t, p), (1, format!("ok{i}").into_bytes()));
        }
        assert_eq!(
            std::fs::metadata(&log_path).expect("meta").len(),
            good_len,
            "torn bytes must be physically truncated"
        );

        // The log stays appendable at a clean boundary and the new entry
        // survives another restart.
        reopened
            .append_entries(&[entry(6, 1, b"ok6")])
            .expect("append");
        drop(reopened);
        let again = FileRaftStorage::new(&dir.0).expect("reopen 2");
        let (t, p) = must_read(&again, 6);
        assert_eq!((t, p), (1, b"ok6".to_vec()));
    }

    // ---------------------------------------------------------------------
    // C7: record CRC integrity tests
    // ---------------------------------------------------------------------

    /// Flip one bit of `needle`'s first occurrence inside the WAL file.
    /// Panics if the needle is not found (the test payloads are unique).
    fn flip_payload_bit(log_path: &Path, needle: &[u8]) {
        let mut data = std::fs::read(log_path).expect("read wal");
        let pos = data
            .windows(needle.len())
            .position(|w| w == needle)
            .unwrap_or_else(|| panic!("payload {needle:?} not found in WAL"));
        data[pos] ^= 0x01; // single-bit rot
        std::fs::write(log_path, &data).expect("write corrupted wal");
    }

    /// Bit-rot in the LAST record's payload: indistinguishable from a torn
    /// append, so recovery must truncate back to the last good record and
    /// leave the log appendable — never load the damaged payload.
    #[test]
    fn crc_bit_flip_in_tail_record_truncates_to_last_good() {
        let dir = TempDir::new("crctail");
        let log_path = dir.0.join("raft_log.wal");

        {
            let storage = FileRaftStorage::new(&dir.0).expect("open");
            let payloads: Vec<(u64, Vec<u8>)> = (1..=5u64)
                .map(|i| (i, format!("crc{i}").into_bytes()))
                .collect();
            let entries: Vec<LogEntry<'_>> =
                payloads.iter().map(|(i, p)| entry(*i, 1, p)).collect();
            storage.append_entries(&entries).expect("append");
        }

        // Rot one bit inside the payload of record 5 (the tail record).
        flip_payload_bit(&log_path, b"crc5");

        // Recovery: 1..=4 survive byte-identical, the rotted record is
        // detected by CRC and truncated away.
        let reopened = FileRaftStorage::new(&dir.0).expect("reopen after tail rot");
        assert_eq!(
            reopened.last_log_position().expect("pos"),
            (LogIndex(4), Term(1)),
            "corrupt tail record must be dropped, not loaded"
        );
        for i in 1..=4u64 {
            let (t, p) = must_read(&reopened, i);
            assert_eq!((t, p), (1, format!("crc{i}").into_bytes()));
        }
        let mut buf = vec![0u8; 128];
        assert!(
            reopened
                .entry_at(LogIndex(5), &mut buf)
                .expect("at")
                .is_none(),
            "the bit-rotted entry must not be readable"
        );

        // The file was physically truncated at the good boundary: 4 records
        // of header + 4-byte payload each.
        assert_eq!(
            std::fs::metadata(&log_path).expect("meta").len(),
            4 * (REC_HEADER as u64 + 4),
            "corrupt tail bytes must be physically truncated"
        );

        // Appendable at the clean boundary; the new entry survives restart.
        reopened
            .append_entries(&[entry(5, 2, b"fresh5")])
            .expect("append after CRC truncation");
        drop(reopened);
        let again = FileRaftStorage::new(&dir.0).expect("reopen 2");
        let (t, p) = must_read(&again, 5);
        assert_eq!((t, p), (2, b"fresh5".to_vec()));
    }

    /// Bit-rot in a MIDDLE record (durably written, possibly committed):
    /// truncating would silently un-commit acknowledged entries, so open
    /// must FAIL loudly instead of recovering (etcd mid-log CRC policy).
    #[test]
    fn crc_bit_flip_mid_log_refuses_to_open() {
        let dir = TempDir::new("crcmid");
        let log_path = dir.0.join("raft_log.wal");

        {
            let storage = FileRaftStorage::new(&dir.0).expect("open");
            let payloads: Vec<(u64, Vec<u8>)> = (1..=5u64)
                .map(|i| (i, format!("mid{i}").into_bytes()))
                .collect();
            let entries: Vec<LogEntry<'_>> =
                payloads.iter().map(|(i, p)| entry(*i, 1, p)).collect();
            storage.append_entries(&entries).expect("append");
        }
        let len_before = std::fs::metadata(&log_path).expect("meta").len();

        // Rot one bit inside the payload of record 2 — NOT the tail.
        flip_payload_bit(&log_path, b"mid2");

        let err = FileRaftStorage::new(&dir.0)
            .err()
            .expect("mid-log CRC corruption must refuse to open, not recover");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("CRC mismatch mid-log"),
            "error must name the mid-log CRC failure, got: {msg}"
        );

        // Refusal must be non-destructive: no truncation of possibly
        // committed data happened.
        assert_eq!(
            std::fs::metadata(&log_path).expect("meta").len(),
            len_before,
            "mid-log corruption must never be silently truncated"
        );
    }

    /// Bit-rot in a header field (the index of the tail record) is also
    /// caught by the CRC — the header prefix is covered, not just the payload.
    #[test]
    fn crc_bit_flip_in_tail_header_truncates_to_last_good() {
        let dir = TempDir::new("crchdr");
        let log_path = dir.0.join("raft_log.wal");

        {
            let storage = FileRaftStorage::new(&dir.0).expect("open");
            let entries: Vec<LogEntry<'_>> = vec![
                entry(1, 1, b"hdr1"),
                entry(2, 1, b"hdr2"),
                entry(3, 1, b"hdr3"),
            ];
            storage.append_entries(&entries).expect("append");
        }

        // Corrupt the term field (bytes 8..16 of the header) of record 3.
        // Record offsets: each record is REC_HEADER + 4 payload bytes.
        {
            let mut data = std::fs::read(&log_path).expect("read wal");
            let rec3 = 2 * (REC_HEADER + 4);
            data[rec3 + 8] ^= 0x80; // flip a bit in the term
            std::fs::write(&log_path, &data).expect("write corrupted wal");
        }

        let reopened = FileRaftStorage::new(&dir.0).expect("reopen after header rot");
        assert_eq!(
            reopened.last_log_position().expect("pos"),
            (LogIndex(2), Term(1)),
            "a header-rotted tail record must be dropped by CRC verification"
        );
    }

    // ---------------------------------------------------------------------
    // C6: snapshot format version + integrity tests
    // ---------------------------------------------------------------------

    /// Save a valid snapshot into `dir` and return its bytes on disk.
    fn write_valid_snapshot(dir: &Path, index: u64, term: u64, payload: &[u8]) -> Vec<u8> {
        let storage = FileRaftStorage::new(dir).expect("open");
        storage
            .save_snapshot(
                &SnapshotMeta {
                    last_included_index: LogIndex(index),
                    last_included_term: Term(term),
                },
                payload,
            )
            .expect("save snapshot");
        drop(storage);
        std::fs::read(dir.join("snapshot.bin")).expect("read snapshot.bin")
    }

    /// Assert that opening `dir` fails and the error message contains
    /// `needle` (the corrupt-snapshot policy is refuse-to-open, never None).
    fn assert_open_fails_with(dir: &Path, needle: &str) {
        let err = FileRaftStorage::new(dir)
            .err()
            .unwrap_or_else(|| panic!("open must fail (expected error containing {needle:?})"));
        let msg = format!("{err:?}");
        assert!(
            msg.contains(needle),
            "error must contain {needle:?}, got: {msg}"
        );
    }

    /// C6 round-trip: header fields, CRC, and payload survive a restart
    /// byte-identically — including the empty-payload edge case and an
    /// overwrite (the latest snapshot wins).
    #[test]
    fn snapshot_roundtrip_survives_reopen() {
        let dir = TempDir::new("snaprt");

        let payload = b"c6-round-trip-\x00\xff-binary".to_vec();
        {
            let storage = FileRaftStorage::new(&dir.0).expect("open");
            // First an empty snapshot, then overwrite with real bytes: the
            // atomic-replace path must leave only the latest.
            storage
                .save_snapshot(
                    &SnapshotMeta {
                        last_included_index: LogIndex(1),
                        last_included_term: Term(1),
                    },
                    b"",
                )
                .expect("save empty snapshot");
            storage
                .save_snapshot(
                    &SnapshotMeta {
                        last_included_index: LogIndex(42),
                        last_included_term: Term(7),
                    },
                    &payload,
                )
                .expect("save snapshot");
        }

        let reopened = FileRaftStorage::new(&dir.0).expect("reopen");
        let (meta, bytes) = reopened
            .load_snapshot()
            .expect("load_snapshot")
            .expect("snapshot must survive restart");
        assert_eq!(meta.last_included_index, LogIndex(42));
        assert_eq!(meta.last_included_term, Term(7));
        assert_eq!(bytes, payload);

        // On-disk layout sanity: magic, version, meta, payload_len.
        let data = std::fs::read(dir.0.join("snapshot.bin")).expect("read");
        assert_eq!(data.len(), SNAP_HEADER + payload.len());
        assert_eq!(&data[0..4], &SNAP_MAGIC);
        assert_eq!(
            u32::from_le_bytes(data[4..8].try_into().unwrap()),
            SNAP_VERSION
        );
        assert_eq!(u64::from_le_bytes(data[8..16].try_into().unwrap()), 42);
        assert_eq!(u64::from_le_bytes(data[16..24].try_into().unwrap()), 7);
        assert_eq!(
            u64::from_le_bytes(data[24..32].try_into().unwrap()),
            payload.len() as u64
        );
    }

    /// Bit-rot in the snapshot PAYLOAD: the CRC catches it and open fails
    /// loudly — a corrupt snapshot must never restore the state machine, and
    /// treating it as absent would replay a truncated log.
    #[test]
    fn snapshot_payload_bit_flip_refuses_to_open() {
        let dir = TempDir::new("snaprot");
        let snap_path = dir.0.join("snapshot.bin");
        let mut data = write_valid_snapshot(&dir.0, 5, 2, b"state-machine-bytes");

        data[SNAP_HEADER + 4] ^= 0x01; // single-bit rot inside the payload
        std::fs::write(&snap_path, &data).expect("write rotted snapshot");

        assert_open_fails_with(&dir.0, "CRC mismatch");
        // Non-destructive refusal: the file is left for the operator.
        assert_eq!(std::fs::read(&snap_path).expect("read"), data);
    }

    /// Bit-rot in the snapshot META (last_included_index): the CRC covers
    /// the header too, so restoring under a shifted log boundary is caught.
    #[test]
    fn snapshot_meta_bit_flip_refuses_to_open() {
        let dir = TempDir::new("snapmeta");
        let mut data = write_valid_snapshot(&dir.0, 5, 2, b"meta-covered");

        data[8] ^= 0x80; // flip a bit in last_included_index
        std::fs::write(dir.0.join("snapshot.bin"), &data).expect("write");

        assert_open_fails_with(&dir.0, "CRC mismatch");
    }

    /// Truncated snapshot (crash/partial copy): both a mid-payload cut and a
    /// mid-header cut are detected — never loaded as a shorter-but-valid
    /// snapshot.
    #[test]
    fn snapshot_truncated_file_refuses_to_open() {
        let dir = TempDir::new("snaptrunc");
        let snap_path = dir.0.join("snapshot.bin");
        let data = write_valid_snapshot(&dir.0, 9, 3, b"will-be-truncated");

        // Cut inside the payload: header claims more bytes than exist.
        std::fs::write(&snap_path, &data[..data.len() - 5]).expect("truncate payload");
        assert_open_fails_with(&dir.0, "header claims");

        // Cut inside the header itself.
        std::fs::write(&snap_path, &data[..SNAP_HEADER - 1]).expect("truncate header");
        assert_open_fails_with(&dir.0, "shorter than");
    }

    /// Unknown format version: a well-formed file (valid magic AND valid CRC
    /// over its own bytes) from a future format must be rejected by the
    /// version check alone — this build cannot know how to parse it.
    #[test]
    fn snapshot_unknown_version_refuses_to_open() {
        let dir = TempDir::new("snapver");
        let mut data = write_valid_snapshot(&dir.0, 3, 1, b"future-format");

        // Bump the version and RECOMPUTE a valid CRC so only the version
        // check can fire (not the integrity check).
        data[4..8].copy_from_slice(&(SNAP_VERSION + 1).to_le_bytes());
        let crc = snapshot_crc(&data[..SNAP_CRC_OFFSET], &data[SNAP_HEADER..]);
        data[SNAP_CRC_OFFSET..SNAP_HEADER].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(dir.0.join("snapshot.bin"), &data).expect("write");

        assert_open_fails_with(&dir.0, "unknown format version");
    }

    /// Pre-C6 (L1) layout — bare 16-byte meta header, no magic/CRC — is NOT
    /// grandfathered (no deployment ever ran on it): the magic check rejects
    /// it as corruption instead of misparsing meta bytes as a header.
    #[test]
    fn snapshot_legacy_unversioned_format_refuses_to_open() {
        let dir = TempDir::new("snaplegacy");
        {
            // Create a valid empty storage first so only the snapshot is odd.
            FileRaftStorage::new(&dir.0).expect("open");
        }
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&6u64.to_le_bytes()); // last_included_index
        legacy.extend_from_slice(&1u64.to_le_bytes()); // last_included_term
        legacy.extend_from_slice(b"legacy-snapshot-bytes-with-padding-to-pass-length");
        std::fs::write(dir.0.join("snapshot.bin"), &legacy).expect("write legacy");

        assert_open_fails_with(&dir.0, "bad magic");
    }
}
