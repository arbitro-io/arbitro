//! Runtime operations — all hot-path processing.
//!
//! Level 7 — depends on everything below.
//!
//! Each submodule handles one operation type:
//! - `publish`: dedup → match → enqueue ready
//! - `claim`: pop ready → build PendingNode → register edges
//! - `ack`: release_pending protocol (core primitive)
//! - `bind`: subscription ↔ connection edge management
//! - `drain`: connection, subscription, consumer, queue, node

pub mod ack;
pub mod publish;
pub mod claim;
pub mod bind;
pub mod drain;
pub mod execute;
pub mod seed;
