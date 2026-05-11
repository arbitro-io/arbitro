//! persistence/ — durable metadata and recovery.
//!
//! * `command_log` — append-only metadata command log (raft-compatible).
//! * `recovery` — `ReplayApplier` re-dispatches logged commands into shards.

pub mod command_log;
pub mod recovery;
