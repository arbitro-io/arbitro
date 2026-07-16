//! Client-side redelivery-dedup store.
//!
//! Pluggable [`Store`] trait ([`store`]) with two backends: an in-memory store
//! ([`memory::MemoryStore`]) and a durable, restart-surviving write-ahead log
//! ([`wal::Wal`]). The WAL replaces the old SQLite cold tier — same on-disk
//! format as the Go/TS/C clients (see [`format`]).
//!
//! Design mirrors the Go reference implementation (`arbitro-go/internal/
//! ackstore`): key = `(stream_name, consumer_name, seq)`, lock-free bounds
//! gate on the delivery hot path, server-cursor-driven cleanup, and log
//! compaction.

mod compact;
mod format;
pub mod memory;
pub mod store;
pub mod wal;

#[cfg(test)]
mod tests;

pub use store::{Metrics, SlotInfo, SlotRef, Store, StoreError};
pub use wal::WalConfig;
