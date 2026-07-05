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

use arbitro_raft::{CommitIndexObserver, LogIndex, RaftError, RaftStorage, StateMachine};
use parking_lot::Mutex;
use tokio::sync::watch;

use super::state_machine::ArbitroStateMachine;
use super::storage::FileRaftStorage;

/// Error sentinel returned by `read_entries` when the caller's payload
/// buffer is too small to fit the requested batch. Must exactly match
/// the string produced by [`FileRaftStorage`] and [`SharedRaftStorage`]
/// in `storage.rs`; if the storage message changes, this must too.
const BUFFER_TOO_SMALL: &str = "payload_buf too small";

/// Hard cap on the apply-loop payload buffer. Chosen to match the
/// engine's per-entry frame ceiling so any valid Raft entry fits after
/// at most log2(16 MB / 64 KB) = 8 doublings from the initial 64 KB.
const PAYLOAD_BUF_MAX: usize = 16 * 1024 * 1024;

/// Interval to sleep after a non-recoverable storage error or after
/// hitting `PAYLOAD_BUF_MAX` without success — avoids busy-spinning the
/// runtime while the operator investigates.
const BACKOFF_ON_ERROR: Duration = Duration::from_secs(1);

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
        match storage.read_entries(from, to, &mut entries, &mut payload_buf) {
            Ok(_) => {}
            Err(RaftError::Storage(msg)) if msg == BUFFER_TOO_SMALL => {
                if payload_buf.len() < PAYLOAD_BUF_MAX {
                    let new_len = (payload_buf.len() * 2).min(PAYLOAD_BUF_MAX);
                    payload_buf.resize(new_len, 0);
                    // Retry with a larger buffer on the next tick — the
                    // interval already keeps us from busy-spinning.
                } else {
                    // Cap reached: a single entry apparently exceeds
                    // PAYLOAD_BUF_MAX. This is an operator-visible
                    // condition — we cannot make progress until the
                    // entry is either fixed (repair) or the cap is
                    // raised (recompile). Log loudly and back off so we
                    // don't burn the runtime.
                    tracing::error!(
                        from = from.0,
                        to = to.0,
                        buf_cap = PAYLOAD_BUF_MAX,
                        "apply_loop: payload_buf at cap and entry still too large — apply is stalled"
                    );
                    tokio::time::sleep(BACKOFF_ON_ERROR).await;
                }
                continue;
            }
            Err(e) => {
                // Any other storage error (I/O, corrupt log, etc.).
                // The previous implementation treated this as
                // "buffer too small" and doubled the buffer forever
                // — a silent CPU sink that never advanced last_applied.
                // Log the real error and back off so it becomes
                // visible in operator dashboards.
                tracing::error!(
                    error = ?e,
                    from = from.0,
                    to = to.0,
                    "apply_loop: read_entries failed"
                );
                tokio::time::sleep(BACKOFF_ON_ERROR).await;
                continue;
            }
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
