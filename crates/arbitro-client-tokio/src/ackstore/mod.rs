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
pub mod dir;
mod format;
pub(crate) mod lock;
pub mod memory;
pub mod store;
pub mod wal;

#[cfg(test)]
mod tests;

pub use dir::{default_dir, ENV_DIR};
pub use store::{Metrics, SlotInfo, SlotRef, Store, StoreError};
pub use wal::WalConfig;
