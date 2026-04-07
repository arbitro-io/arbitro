use serde::{Deserialize, Serialize};
use crate::config::{StreamConfig, ConsumerConfig};

/// Metadata commands for broker state management.
///
/// These commands are applied to the Engine to mutate the global metadata
/// registry (Streams and Consumers). This structure is designed to be
/// appended to a local Command Ledger or replicated via Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataCommand {
    /// Create a new stream with the given configuration.
    CreateStream(StreamConfig),
    /// Delete a stream by its ID.
    DeleteStream(u32),
    /// Create a new consumer with the given configuration.
    CreateConsumer(ConsumerConfig),
    /// Delete a consumer by its stream and consumer IDs.
    DeleteConsumer {
        stream_id: u32,
        consumer_id: u32,
    },
}
