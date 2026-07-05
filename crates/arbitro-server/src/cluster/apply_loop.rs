//! Background task that applies committed Raft log entries to the local
//! state machine.
//!
//! The loop polls the [`CommitIndexObserver`] every 100 ms and applies
//! any entries in range `(last_applied, commit_index]` — never past the
//! Raft-committed boundary. This preserves the Committed-before-Applied
//! safety property: entries that a new leader later truncates via
//! `truncate_suffix` (uncommitted tail) never reach the state machine.
//!
//! On followers, entries appear in storage when `AppendEntries` RPCs
//! arrive from the leader; `commit_index` advances when the leader
//! signals a new safe commit boundary. On the leader, the dispatch
//! already executes commands locally after a successful propose, so
//! double-apply is safe because create/delete operations are
//! idempotent (returns "already exists" / "not found").

use std::sync::Arc;
use std::time::Duration;

use arbitro_raft::{CommitIndexObserver, LogIndex, RaftStorage, StateMachine};
use parking_lot::Mutex;
use tokio::sync::watch;

use super::state_machine::ArbitroStateMachine;
use super::storage::FileRaftStorage;

/// Continuously read committed log entries from Raft storage and apply
/// them to the state machine. Returns when the shutdown signal fires.
pub async fn apply_loop(
    storage: Arc<FileRaftStorage>,
    state_machine: Arc<Mutex<ArbitroStateMachine>>,
    commit_index: CommitIndexObserver,
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

        // Cap apply upper-bound at the Raft commit index. Also cap at
        // `last_log_position` so we never read past what storage
        // actually has (storage may be behind commit briefly on
        // followers between AppendEntries arrival and storage flush).
        let committed = commit_index.get();
        if committed <= last_applied {
            continue;
        }
        let last_stored = match storage.last_log_position() {
            Ok((idx, _)) => idx,
            Err(e) => {
                tracing::trace!(error = ?e, "apply_loop: last_log_position failed");
                continue;
            }
        };
        let apply_upper = LogIndex(committed.0.min(last_stored.0));
        if apply_upper <= last_applied {
            continue;
        }
        tracing::debug!(
            commit = committed.0,
            last_stored = last_stored.0,
            apply_upper = apply_upper.0,
            last_applied = last_applied.0,
            "apply_loop: new committed entries"
        );

        let from = LogIndex(last_applied.0 + 1);
        let to = LogIndex(apply_upper.0 + 1);

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
            if entry.index > last_applied && entry.index <= apply_upper {
                if let Err(e) = sm.apply(entry.payload.0) {
                    tracing::warn!(index = entry.index.0, error = ?e, "apply_loop: failed to apply entry");
                }
                last_applied = entry.index;
            }
        }
    }
}
