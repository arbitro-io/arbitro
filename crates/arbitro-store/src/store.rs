//! Store trait — the contract all journal backends must follow.
//!
//! Sync only. No tokio, no async. If a backend needs async I/O,
//! the server wraps it in spawn_blocking.

use arbitro_engine_v2::common::subject_matches;

/// Entry flag bits — stored as `flags: u8` on every Entry.
pub mod flags {
    /// Entry has been tombstoned (logically deleted, kept for drain-time skip).
    pub const TOMBSTONE: u8 = 0b0000_0001;
    /// Payload is prefixed with `[reply_len:u16 LE][reply_to bytes]`.
    /// Used by request/reply (PubWithReply). Drain extracts the prefix
    /// and passes `reply_to` through the delivery wire frame.
    pub const HAS_REPLY_TO: u8 = 0b0000_1000;
}

/// A single stored message view.
/// Borrows data from the store arena — zero allocations.
///
/// `stream_id` is embedded so the store remains stream-agnostic: the
/// shard has ONE store, one cursor, and the drain filters by
/// `engine.has_demand(stream_id)` per entry during linear walk.
#[derive(Debug, Clone, Copy)]
pub struct Entry<'a> {
    pub seq: u64,
    pub stream_id: u32,
    pub timestamp: u64,
    pub subject: &'a [u8],
    pub payload: &'a [u8],
    pub flags: u8,
}

/// Message reference for appending — borrows data, no allocation.
#[derive(Debug, Clone, Copy)]
pub struct EntryRef<'a> {
    pub stream_id: u32,
    pub subject: &'a [u8],
    pub payload: &'a [u8],
    pub flags: u8,
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
pub trait Store: Send + Sync {
    // ── Lifecycle ────────────────────────────────────────────────────

    /// Initialize the store — open files, create dirs, recover state.
    /// Called once before any reads/writes. Default: no-op.
    fn init(&mut self) -> Result<(), StoreError> { Ok(()) }

    /// Graceful shutdown — flush buffers, sync to disk, close handles.
    /// Called once on engine shutdown. Default: no-op.
    fn shutdown(&mut self) -> Result<(), StoreError> { Ok(()) }

    // ── Hot path ────────────────────────────────────────────────────

    /// Append a single message. Returns the assigned sequence.
    fn append(&mut self, entry: EntryRef<'_>, timestamp: u64) -> Result<u64, StoreError>;

    /// Append a batch of messages. Returns the first assigned sequence.
    /// All entries get consecutive sequences.
    fn append_batch(&mut self, entries: &[EntryRef<'_>], timestamp: u64) -> Result<u64, StoreError>;

    /// Read a single entry by sequence.
    /// Deprecated: users should prefer get() or for_each() for better performance.
    fn read(&self, seq: u64) -> Result<Option<Entry<'_>>, StoreError>;

    /// Read a range [start, end) of entries.
    /// Deprecated: users should prefer get() or for_each() for better performance.
    fn read_range(&self, start: u64, end: u64) -> Result<Vec<Entry<'_>>, StoreError>;

    /// Zero-alloc: calls `f` with a borrowed entry at `seq`.
    /// Returns `Ok(true)` if found, `Ok(false)` if not found.
    fn get(&self, seq: u64, f: &mut dyn FnMut(&Entry<'_>)) -> Result<bool, StoreError>;

    /// Zero-alloc: calls `f` for each entry in `[start..end)`.
    /// Borrows directly from internal storage — no cloning.
    fn for_each(&self, start: u64, end: u64, f: &mut dyn FnMut(&Entry<'_>)) -> Result<(), StoreError>;

    // ── Management ──────────────────────────────────────────────────

    /// Delete all messages. Stream survives. Returns deleted count.
    fn purge(&mut self) -> u64;

    /// Delete messages before the given sequence. Stream survives.
    /// Returns number of deleted messages.
    fn truncate_front(&mut self, first_seq: u64) -> u64;

    /// Delete all messages matching a subject pattern. Returns deleted count.
    fn drain(&mut self, subject: &[u8]) -> u64;

    /// Current stats.
    fn info(&self) -> StoreInfo;
}

/// Helper: check if an entry's subject matches a pattern.
/// Used by drain implementations.
#[inline]
pub fn entry_matches(entry: &Entry<'_>, pattern: &[u8]) -> bool {
    subject_matches(pattern, &entry.subject)
}
