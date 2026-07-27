//! Periodic consumer-cursor persistence — `CMD_CURSOR_UPDATE` records.
//!
//! `handle_ack` tracks the highest acked seq per consumer in the
//! `NameRegistry` (RAM only). This module appends the write side that was
//! missing (A7): a background sweep records a `CMD_CURSOR_UPDATE` in the
//! command log for every cursor that advanced since the last sweep, and a
//! final flush runs on graceful shutdown. Recovery already replays the
//! record (`persistence/recovery.rs`, `CMD_CURSOR_UPDATE` arm), so a
//! resubscribe after a full broker restart resumes from the persisted
//! cursor instead of replaying fully-acked history.
//!
//! Cold path by construction: the sweep runs on a background tokio task
//! (never on a shard worker thread — shard workers must not fsync inline),
//! and only changed cursors are written, so an idle broker appends nothing.

use std::collections::HashMap;

use crate::common::NameRegistry;
use crate::persistence::command_log::SharedCommandLog;

/// Tracks the last persisted cursor per consumer and appends
/// `CMD_CURSOR_UPDATE` records for cursors that advanced.
#[derive(Default)]
pub struct CursorPersister {
    last_persisted: HashMap<u32, u64>,
}

impl CursorPersister {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a `CMD_CURSOR_UPDATE` for every consumer whose cursor
    /// advanced past the last persisted value. Returns the number of
    /// records written.
    pub fn persist_changed(&mut self, names: &NameRegistry, log: &SharedCommandLog) -> u32 {
        let mut written = 0u32;
        for (consumer_id, cursor) in names.consumer_cursors_snapshot() {
            if cursor == 0 {
                continue;
            }
            let prev = self
                .last_persisted
                .get(&consumer_id.0)
                .copied()
                .unwrap_or(0);
            if cursor <= prev {
                continue;
            }
            let cmd = arbitro_proto::metadata::build_cursor_update(consumer_id.0, cursor);
            match log.record(&cmd) {
                Ok(()) => {
                    self.last_persisted.insert(consumer_id.0, cursor);
                    written += 1;
                }
                Err(e) => {
                    // Leave last_persisted untouched — the next sweep
                    // retries. Cursor loss only causes redelivery
                    // (at-least-once), never data loss.
                    tracing::warn!(
                        consumer_id = consumer_id.0,
                        cursor,
                        error = %e,
                        "cursor persist failed; will retry next sweep"
                    );
                }
            }
        }
        written
    }
}
