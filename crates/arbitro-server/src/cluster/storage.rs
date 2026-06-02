//! In-memory Raft log storage with file-backed HardState persistence.

use std::path::{Path, PathBuf};

use arbitro_raft::{
    EntryPayload, HardState, LogEntry, LogIndex, PeerId, RaftError, RaftStorage, SnapshotMeta,
    Term,
};
use parking_lot::Mutex;

/// Owned representation of a log entry (payload is not borrowed).
#[derive(Debug, Clone)]
struct StoredEntry {
    term: Term,
    index: LogIndex,
    payload: Vec<u8>,
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

/// Simple JSON structure for persisting `SnapshotMeta`.
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotMetaDto {
    last_included_index: u64,
    last_included_term: u64,
}

/// In-memory Raft storage.
///
/// The log is kept in a `Vec` behind a `parking_lot::Mutex`. `HardState` is
/// persisted as JSON to a file so that term/vote survive restarts.  Full
/// file-backed log persistence will be added later.
pub struct FileRaftStorage {
    hard_state_path: PathBuf,
    entries: Mutex<Vec<StoredEntry>>,
    snapshot: Mutex<Option<(SnapshotMeta, Vec<u8>)>>,
}

impl FileRaftStorage {
    /// Create a new storage rooted at `data_dir`.
    ///
    /// `data_dir` must already exist. The hard-state file is created lazily on
    /// the first `save_hard_state` call.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            hard_state_path: data_dir.join("hard_state.json"),
            entries: Mutex::new(Vec::new()),
            snapshot: Mutex::new(None),
        }
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
        std::fs::write(&self.hard_state_path, &data)
            .map_err(|e| RaftError::Storage(format!("write hard_state: {e}")))?;
        Ok(())
    }

    fn append_entries(&self, new_entries: &[LogEntry<'_>]) -> Result<(), RaftError> {
        let mut entries = self.entries.lock();
        for e in new_entries {
            entries.push(StoredEntry {
                term: e.term,
                index: e.index,
                payload: e.payload.0.to_vec(),
            });
        }
        Ok(())
    }

    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        let entries = self.entries.lock();
        if let Some(e) = entries.iter().rev().find(|e| e.index == index) {
            if payload_buf.len() < e.payload.len() {
                return Err(RaftError::Storage("payload_buf too small".into()));
            }
            payload_buf[..e.payload.len()].copy_from_slice(&e.payload);

            // SAFETY: payload_buf outlives the returned LogEntry because both
            // share the caller's lifetime 'a.
            let slice = unsafe {
                std::mem::transmute::<&[u8], &'a [u8]>(&payload_buf[..e.payload.len()])
            };

            Ok(Some(LogEntry {
                term: e.term,
                index: e.index,
                payload: EntryPayload(slice),
            }))
        } else {
            Ok(None)
        }
    }

    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        Ok(self
            .entries
            .lock()
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or_default())
    }

    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        self.entries.lock().retain(|e| e.index < from);
        Ok(())
    }

    fn save_snapshot(&self, meta: &SnapshotMeta, snapshot: &[u8]) -> Result<(), RaftError> {
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
        let entries = self.entries.lock();
        let mut offset = 0;
        for e in entries.iter() {
            if e.index >= from && e.index < to {
                let len = e.payload.len();
                if offset + len > payload_buf.len() {
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
        }
        Ok(offset)
    }
}
