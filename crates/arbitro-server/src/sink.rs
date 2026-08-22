//! Everything a caller needs from ONE stream's storage, with no mention of
//! shards, locks, or gates.
//!
//! Before this, publish did its own routing, its own locking and its own
//! signalling:
//!
//! ```ignore
//! let shared_store = server.store_for(seq_stream);
//! let first = shared_store.lock().append_batch(&entries, now_ms)?;
//! server.gate_for(seq_stream).release();
//! ```
//!
//! Three responsibilities in the transport layer, repeated at 22 call sites
//! across six files. Every one of them had to remember the gate — forgetting
//! it means the messages are stored and never delivered, with no error.
//!
//! `StreamSink` collapses them into one call. What it does NOT expose is the
//! point: the caller cannot see whether the store is behind a mutex, owned by
//! this thread, or reached some other way. That is what lets the execution
//! model change underneath — shared store today, thread-per-shard later —
//! without touching a single caller.
//!
//! **Deliberately synchronous.** An async signature would admit a variant
//! that forwards to another thread and waits for the sequence to come back,
//! and every fast caller would pay for a case that only exists when a
//! connection publishes to a stream it does not own. Keeping it sync makes
//! that shape impossible to add by accident: misplacement has to be answered
//! with a protocol error, not hidden behind a queue.

use arbitro_store::{EntryRef, StoreError, StoreInfo};

use crate::common::Gate;
use crate::shard::router::SharedStore;

/// One stream's storage, as the rest of the server sees it.
pub trait StreamSink {
    /// Append and wake whoever delivers. Returns the first assigned sequence.
    ///
    /// The wake is part of the contract, not a separate step the caller has
    /// to remember: a publish that stores without signalling is invisible
    /// until some other publish happens to wake the drain.
    fn publish(&self, entries: &[EntryRef<'_>], now_ms: u64) -> Result<u64, StoreError>;

    /// Message/byte counts and sequence bounds — for quota checks.
    fn info(&self) -> StoreInfo;
}

/// Today's model: the store is shared, so reaching it means taking a lock,
/// and the deliverer is woken through a gate.
///
/// Borrowed, not owned: it is built per call from the router's existing
/// `Arc`s and costs nothing to create.
pub struct SharedStoreSink<'a> {
    store: &'a SharedStore,
    gate: &'a Gate,
}

impl<'a> SharedStoreSink<'a> {
    #[inline]
    pub fn new(store: &'a SharedStore, gate: &'a Gate) -> Self {
        Self { store, gate }
    }
}

impl StreamSink for SharedStoreSink<'_> {
    #[inline]
    fn publish(&self, entries: &[EntryRef<'_>], now_ms: u64) -> Result<u64, StoreError> {
        // Scoped so the guard is released BEFORE the gate fires. Waking the
        // drain while still holding the lock hands it a mutex it will
        // immediately block on.
        let first = {
            let mut g = self.store.lock();
            g.append_batch(entries, now_ms)?
        };
        self.gate.release();
        Ok(first)
    }

    #[inline]
    fn info(&self) -> StoreInfo {
        self.store.lock().info()
    }
}

/// The shard's journal reached from the shard's OWN thread. No lock.
///
/// This is the fast door, and it is only constructible when the calling
/// thread actually owns the shard — see `ShardRouter::local_sink`. That is
/// the whole safety argument: there is no lock because there is no second
/// thread, and the type cannot be built where that is untrue.
///
/// A connection that arrived on this shard's listener is already here. One
/// that arrived on the bootstrap port is not, and has to take the routed
/// path instead — a channel hop, paid by the case that earned it.
pub struct LocalSink<'a> {
    shard_id: usize,
    gate: &'a Gate,
}

impl<'a> LocalSink<'a> {
    /// Caller must have established that this thread owns `shard_id`.
    #[inline]
    pub(crate) fn new(shard_id: usize, gate: &'a Gate) -> Self {
        Self { shard_id, gate }
    }
}

impl StreamSink for LocalSink<'_> {
    #[inline]
    fn publish(&self, entries: &[EntryRef<'_>], now_ms: u64) -> Result<u64, StoreError> {
        // Same ordering rule as the shared sink: the borrow ends before the
        // gate fires. Here it matters for a different reason — the drain is
        // a task on THIS thread, so releasing the gate while the journal is
        // still borrowed would let it wake into an outstanding borrow.
        // `None` here means this thread does not own the shard, which is a
        // construction error rather than a storage one — `local_sink` is
        // supposed to have refused. Surfaced as an I/O error because the
        // caller's contract is `StoreError`, and never reached in practice.
        let first = crate::shard::local::with_store(self.shard_id, |s| {
            s.append_batch(entries, now_ms)
        })
        .ok_or(StoreError::Io(std::io::ErrorKind::WouldBlock))??;
        self.gate.release();
        Ok(first)
    }

    #[inline]
    fn info(&self) -> StoreInfo {
        crate::shard::local::with_store(self.shard_id, |s| s.info()).unwrap_or_default()
    }
}
