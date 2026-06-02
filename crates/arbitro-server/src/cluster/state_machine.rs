//! Raft state machine for Arbitro cluster metadata operations.

use arbitro_raft::{RaftError, StateMachine};
use serde::{Deserialize, Serialize};

/// Commands replicated through Raft for cluster-wide metadata consistency.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClusterCommand {
    CreateStream {
        name: String,
        filter: String,
        max_msgs: u64,
        max_bytes: u64,
        max_age_secs: u64,
        replicas: u8,
        journal_kind: u8,
        retention: u8,
        discard: u8,
        idempotency_window_ms: u32,
    },
    DeleteStream {
        name: String,
    },
    CreateConsumer {
        stream_name: String,
        name: String,
        group: String,
        filter: String,
        max_inflight: u16,
        ack_policy: u8,
        deliver_policy: u8,
        deliver_mode: u8,
        ack_wait_ms: u32,
        start_seq: u64,
    },
    DeleteConsumer {
        stream_name: String,
        name: String,
    },
}

/// State machine that accumulates cluster commands.
///
/// For now, `apply` deserializes and stores each command in an ordered log.
/// The actual engine application will be wired through the dispatch layer.
pub struct ArbitroStateMachine {
    applied: Vec<ClusterCommand>,
}

impl ArbitroStateMachine {
    pub fn new() -> Self {
        Self {
            applied: Vec::new(),
        }
    }
}

impl StateMachine for ArbitroStateMachine {
    fn apply(&mut self, entry: &[u8]) -> Result<(), RaftError> {
        let cmd: ClusterCommand = serde_json::from_slice(entry)
            .map_err(|e| RaftError::Storage(format!("failed to deserialize command: {e}")))?;
        self.applied.push(cmd);
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        serde_json::to_vec(&self.applied)
            .map_err(|e| RaftError::Snapshot(format!("failed to serialize snapshot: {e}")))
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), RaftError> {
        self.applied = serde_json::from_slice(snapshot)
            .map_err(|e| RaftError::Snapshot(format!("failed to deserialize snapshot: {e}")))?;
        Ok(())
    }
}
