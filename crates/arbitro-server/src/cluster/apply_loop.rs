//! Background task that applies committed Raft log entries to the local
//! state machine.
//!
//! The loop polls `FileRaftStorage::last_log_position()` every 100 ms and
//! applies any entries newer than `last_applied`.  On followers, entries
//! appear in storage when `AppendEntries` RPCs arrive from the leader.
//! On the leader, the dispatch already executes commands locally after a
//! successful propose, so double-apply is safe because create/delete
//! operations are idempotent (returns "already exists" / "not found").

use std::sync::Arc;
use std::time::Duration;

use arbitro_raft::{LogIndex, RaftStorage, StateMachine};
use parking_lot::Mutex;
use tokio::sync::watch;

use super::state_machine::ArbitroStateMachine;
use super::storage::FileRaftStorage;

/// Continuously read new log entries from Raft storage and apply them
/// to the state machine.  Returns when the shutdown signal fires.
pub async fn apply_loop(
    storage: Arc<FileRaftStorage>,
    state_machine: Arc<Mutex<ArbitroStateMachine>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut last_applied = LogIndex(0);
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    // Generous payload buffer — most commands are small JSON.
    let mut payload_buf = vec![0u8; 64 * 1024];

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown.changed() => {
                tracing::debug!("apply_loop: shutting down");
                return;
            }
        }

        let (last_log, _term) = match storage.last_log_position() {
            Ok(pos) => pos,
            Err(e) => {
                tracing::trace!(error = ?e, "apply_loop: last_log_position failed");
                continue;
            }
        };

        if last_log <= last_applied {
            continue;
        }
        tracing::debug!(
            last_log = last_log.0,
            last_applied = last_applied.0,
            "apply_loop: new entries"
        );

        let from = LogIndex(last_applied.0 + 1);
        let to = LogIndex(last_log.0 + 1);

        let mut entries = Vec::new();
        let read_result = storage.read_entries(from, to, &mut entries, &mut payload_buf);
        if read_result.is_err() {
            // Buffer may be too small — grow and retry next tick.
            if payload_buf.len() < 1024 * 1024 {
                payload_buf.resize(payload_buf.len() * 2, 0);
            }
            continue;
        }

        let mut sm = state_machine.lock();
        for entry in &entries {
            if entry.index > last_applied {
                if let Err(e) = sm.apply(entry.payload.0) {
                    tracing::warn!(index = entry.index.0, error = ?e, "apply_loop: failed to apply entry");
                }
                last_applied = entry.index;
            }
        }
    }
}
