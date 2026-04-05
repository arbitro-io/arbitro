//! MetadataStore trait — contract for persisting stream and consumer configs.
//!
//! Cold path only. Called on create/delete, never on publish/deliver.
//! Implementations: NoopMetadataStore (testing), FsMetadataStore (arbitro-server).

use crate::config::{StreamConfig, ConsumerConfig};

/// Snapshot of all persisted state — returned on load.
pub struct MetadataSnapshot {
    pub streams: Vec<StreamConfig>,
    pub consumers: Vec<ConsumerConfig>,
}

/// Errors from metadata persistence.
#[derive(Debug)]
pub enum MetadataError {
    Io(String),
    Corrupt(String),
}

/// Persistence contract for stream/consumer configs.
///
/// All methods are sync — if the backend needs async I/O,
/// the server wraps in spawn_blocking.
pub trait MetadataStore {
    // ── Lifecycle ────────────���──────────────────────────────────���────

    /// Load all persisted configs. Called once at startup.
    fn load(&self) -> Result<MetadataSnapshot, MetadataError>;

    /// Graceful shutdown — flush pending writes, close handles.
    /// Default: no-op.
    fn shutdown(&self) -> Result<(), MetadataError> { Ok(()) }

    // ── Operations ──────────────────────────────────────��───────────

    /// Persist a stream config. Called on CreateStream.
    fn save_stream(&self, config: &StreamConfig) -> Result<(), MetadataError>;

    /// Remove a stream config. Called on DeleteStream.
    fn delete_stream(&self, stream_id: u32) -> Result<(), MetadataError>;

    /// Persist a consumer config. Called on CreateConsumer.
    fn save_consumer(&self, config: &ConsumerConfig) -> Result<(), MetadataError>;

    /// Remove a consumer config. Called on DeleteConsumer.
    fn delete_consumer(&self, stream_id: u32, consumer_id: u32) -> Result<(), MetadataError>;
}

/// No-op implementation for testing and embedded use.
pub struct NoopMetadataStore;

impl MetadataStore for NoopMetadataStore {
    fn load(&self) -> Result<MetadataSnapshot, MetadataError> {
        Ok(MetadataSnapshot {
            streams: Vec::new(),
            consumers: Vec::new(),
        })
    }

    fn save_stream(&self, _config: &StreamConfig) -> Result<(), MetadataError> { Ok(()) }
    fn delete_stream(&self, _stream_id: u32) -> Result<(), MetadataError> { Ok(()) }
    fn save_consumer(&self, _config: &ConsumerConfig) -> Result<(), MetadataError> { Ok(()) }
    fn delete_consumer(&self, _stream_id: u32, _consumer_id: u32) -> Result<(), MetadataError> { Ok(()) }
}
