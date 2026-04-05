//! Store trait — the contract all journal backends must follow.
//!
//! Sync only. No tokio, no async. If a backend needs async I/O,
//! the server wraps it in spawn_blocking.

use arbitro_common::subject::subject_matches;

/// A single stored message.
#[derive(Debug, Clone)]
pub struct Entry {
    pub seq: u64,
    pub timestamp: u64,
    pub subject: Box<[u8]>,
    pub payload: Box<[u8]>,
}

/// Message reference for appending — borrows data, no allocation.
pub struct EntryRef<'a> {
    pub subject: &'a [u8],
    pub payload: &'a [u8],
}

/// Store stats.
#[derive(Debug, Clone, Copy, Default)]
pub struct StoreInfo {
    pub messages: u64,
    pub bytes: u64,
    pub first_seq: u64,
    pub last_seq: u64,
}

/// Store errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    /// Stream has reached max_msgs or max_bytes.
    Full,
    /// Sequence not found.
    NotFound,
}

/// The journal contract. All backends implement this.
///
/// `&mut self` for writes, `&self` for reads.
/// Single-threaded access guaranteed by the engine (one lock per stream).
pub trait Store {
    /// Append a single message. Returns the assigned sequence.
    fn append(&mut self, entry: EntryRef<'_>, timestamp: u64) -> Result<u64, StoreError>;

    /// Append a batch of messages. Returns the first assigned sequence.
    /// All entries get consecutive sequences.
    fn append_batch(&mut self, entries: &[EntryRef<'_>], timestamp: u64) -> Result<u64, StoreError>;

    /// Read a single entry by sequence.
    fn read(&self, seq: u64) -> Result<Option<Entry>, StoreError>;

    /// Read a range [start, end) of entries.
    fn read_range(&self, start: u64, end: u64) -> Result<Vec<Entry>, StoreError>;

    /// Delete all messages. Stream survives. Returns deleted count.
    fn purge(&mut self) -> u64;

    /// Delete all messages matching a subject pattern. Returns deleted count.
    fn drain(&mut self, subject: &[u8]) -> u64;

    /// Current stats.
    fn info(&self) -> StoreInfo;
}

/// Helper: check if an entry's subject matches a pattern.
/// Used by drain implementations.
#[inline]
pub fn entry_matches(entry: &Entry, pattern: &[u8]) -> bool {
    subject_matches(pattern, &entry.subject)
}
