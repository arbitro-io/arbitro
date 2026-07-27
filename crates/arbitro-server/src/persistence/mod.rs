//! persistence/ — durable metadata and recovery.
//!
//! * `command_log` — append-only metadata command log (raft-compatible).
//! * `cursor_persist` — periodic `CMD_CURSOR_UPDATE` writer (consumer
//!   ack cursors survive full restarts).
//! * `recovery` — `ReplayApplier` re-dispatches logged commands into shards.

pub mod command_log;
pub mod cursor_persist;
pub mod recovery;
