//! Cluster subsystem — Raft-based metadata replication.
//!
//! When the `cluster` feature is enabled and `ARBITRO_CLUSTER_PEERS` is set,
//! the broker boots in Clustered mode. Metadata operations (CreateStream,
//! DeleteStream, CreateConsumer, DeleteConsumer) go through Raft for strong
//! consistency. Message publish does NOT go through Raft — it uses the local
//! shard path for maximum throughput (Kafka-style async replication).

pub mod apply_loop;
pub mod replication;
pub mod state_machine;
pub mod storage;
pub mod transport;

use std::sync::Arc;

use arbitro_raft::ClientHandle;

/// Cluster operating mode.
pub enum ClusterState {
    /// Single-node mode — all operations execute locally.
    Standalone,
    /// Multi-node mode — metadata ops go through Raft.
    Clustered {
        /// Handle to propose entries to the Raft log.
        client: Arc<ClientHandle>,
        /// This node's peer ID.
        peer_id: arbitro_raft::PeerId,
    },
}

impl ClusterState {
    /// Returns true if this node is in clustered mode.
    pub fn is_clustered(&self) -> bool {
        matches!(self, Self::Clustered { .. })
    }

    /// Get the Raft client handle (panics if Standalone).
    pub fn client(&self) -> &Arc<ClientHandle> {
        match self {
            Self::Clustered { client, .. } => client,
            Self::Standalone => panic!("not in clustered mode"),
        }
    }
}

impl std::fmt::Debug for ClusterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standalone => write!(f, "Standalone"),
            Self::Clustered { peer_id, .. } => write!(f, "Clustered(peer={})", peer_id.0),
        }
    }
}

/// Serialize a cluster command and propose it to the Raft log.
/// Returns `Ok(())` when the entry is committed by a majority.
pub async fn propose_command(
    client: &ClientHandle,
    cmd: &state_machine::ClusterCommand,
) -> Result<(), String> {
    let payload = serde_json::to_vec(cmd).map_err(|e| e.to_string())?;
    client.write(&payload).await.map_err(|e| format!("{e:?}"))?;
    Ok(())
}
