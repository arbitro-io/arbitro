//! Pluggable delivered-set store for the client's redelivery-dedup guarantee.
//!
//! On each incoming Deliver frame the client asks the store "have I already
//! run the user handler for this message?" — a "yes" means silently re-ack, a
//! "no" means run the handler.
//!
//! # Identity: `(stream_name, consumer_name, seq)`
//!
//! The dedup key is NOT the broker's numeric `consumer_id` — that ID is
//! ephemeral and changes when a consumer is deleted and recreated. Because
//! arbitro's `seq` is stream-scoped (the same message always has the same seq
//! for every consumer of that stream, verified server-side), a durable key is
//! `(stream_name, consumer_name, seq)`. A consumer recreated under the SAME
//! name still recognizes already-processed messages; a DIFFERENT name is a
//! fresh workload.
//!
//! # Hot-path design: resolve once, bounds-gate per message
//!
//! String keys are hashed once per subscription via [`Store::slot`], returning
//! a [`SlotRef`]. The delivery hot path calls [`SlotRef::seen`] — a lock-free
//! `(min_seq, max_seq)` bounds probe — and only falls back to the exact set
//! check + persist when the bounds gate is inconclusive.
//!
//! This is the pluggable abstraction: today the durable backend is a WAL
//! ([`super::wal::Wal`]); the trait lets us swap infra without touching the
//! client hot path.

use std::sync::Arc;
use std::time::SystemTime;

/// Errors surfaced by store operations.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("ackstore: closed")]
    Closed,
    #[error("ackstore: slot id overflow")]
    SlotOverflow,
    #[error("ackstore: stream/consumer name too long")]
    NameTooLong,
    #[error("ackstore: unknown slot")]
    UnknownSlot,
    #[error("ackstore: io: {0}")]
    Io(#[from] std::io::Error),
    #[error("ackstore: corrupt record")]
    Corrupt,
}

/// The pluggable delivered-set store. Implementations MUST be `Send + Sync`.
pub trait Store: Send + Sync {
    /// Resolve `(stream, consumer)` to a durable handle, registering it on
    /// first use. Cold path — the client calls this once per subscription and
    /// caches the returned [`SlotRef`].
    fn slot(&self, stream: &str, consumer: &str) -> Result<Arc<dyn SlotRef>, StoreError>;

    /// Flush buffered writes and (if configured) fsync. The client's ack
    /// batcher calls this after a batch of confirms.
    fn sync(&self) -> Result<(), StoreError>;

    /// Rebuild in-memory state from persistent storage. Called once at
    /// construction; a no-op for the memory backend.
    fn restore(&self) -> Result<(), StoreError>;

    /// Flush and release resources. After close, all methods error.
    fn close(&self) -> Result<(), StoreError>;

    // --- admin / introspection ---

    /// Snapshot of every known slot's metadata.
    fn list_slots(&self) -> Vec<SlotInfo>;

    /// Metadata for one slot, `None` if unknown.
    fn slot_info(&self, stream: &str, consumer: &str) -> Option<SlotInfo>;

    /// Forget a slot entirely (all its seqs), persisting a tombstone.
    fn delete_slot(&self, stream: &str, consumer: &str) -> Result<(), StoreError>;

    /// Cumulative counters and live gauges.
    fn metrics(&self) -> Metrics;
}

/// A resolved handle to one `(stream, consumer)` pair. The client caches it
/// for the subscription's lifetime and uses it on the hot path.
///
/// Two usage patterns:
///
/// **A) record-at-ack (recommended, lock-free hot path):**
/// ```ignore
/// // on delivery:  if !slot.seen(seq) { run_handler(); }
/// // on ack:        slot.record(seq);          // batched write
/// // on broker-ack: slot.confirm_up_to(cursor) // driven by AckBatchResp
/// ```
///
/// **B) test-and-set at delivery (records eagerly):**
/// ```ignore
/// if slot.check_record(seq)? { run_handler() } else { re_ack() }
/// ```
pub trait SlotRef: Send + Sync {
    /// Reports whether `seq` has already been recorded (processed + acked
    /// before). Lock-free bounds probe first (returns false fast when `seq`
    /// is outside the recorded range); locks only for in-bounds seqs. This is
    /// the DELIVERY-path dedup check.
    fn seen(&self, seq: u64) -> bool;

    /// Marks `seq` as processed/acked and persists it. Idempotent. Called on
    /// ACK (batched); a following [`Store::sync`] makes it durable.
    fn record(&self, seq: u64) -> Result<(), StoreError>;

    /// The raw lock-free bounds gate: `true` = `seq` is outside the recorded
    /// `[min,max]` range (definitely not seen). Most callers use [`seen`].
    ///
    /// [`seen`]: SlotRef::seen
    fn fresh(&self, seq: u64) -> bool;

    /// Atomic test-and-set: exact lookup, and on a miss records + persists,
    /// returning `true` (new). Returns `false` for a duplicate.
    fn check_record(&self, seq: u64) -> Result<bool, StoreError>;

    /// Marks `seq` as broker-acked; removes it from the live set and appends
    /// a confirm record.
    fn confirm(&self, seq: u64) -> Result<(), StoreError>;

    /// Drops every live seq `<= cursor` in one pass. Driven by the server's
    /// ack cursor (`AckBatchResp.new_cursor` / `AckStateRep.cursor`): the
    /// broker never redelivers at or below it, so those seqs need not be
    /// remembered. Keeps the live set tiny on the normal ack path — no
    /// periodic job required. Returns the count dropped.
    fn confirm_up_to(&self, cursor: u64) -> Result<usize, StoreError>;

    /// This slot's current metadata.
    fn info(&self) -> SlotInfo;
}

/// Introspection view of one slot.
#[derive(Debug, Clone)]
pub struct SlotInfo {
    pub slot_id: u32,
    pub stream: String,
    pub consumer: String,
    /// Live seqs (recorded, not yet confirmed/expired).
    pub live: usize,
    /// Smallest live seq (0 if empty).
    pub min_seq: u64,
    /// Largest live seq (0 if empty).
    pub max_seq: u64,
    /// Timestamp (unix ms) of the oldest live seq (0 if empty).
    pub oldest_ts_ms: u64,
    pub registered_at: SystemTime,
}

/// Point-in-time snapshot of store counters.
#[derive(Debug, Clone, Default)]
pub struct Metrics {
    pub slots: usize,
    pub live_entries: u64,
    pub recorded: u64,
    pub confirmed: u64,
    pub expired: u64,
    pub registered: u64,
    pub tombstoned: u64,
    pub sync_errors: u64,
    pub file_size: i64,
    pub last_sync_ms: i64,
    pub last_snapshot_ms: i64,
}
