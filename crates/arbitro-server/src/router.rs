//! Server — spawn shards, route commands by stream_id.

use std::sync::Arc;

use arbitro_engine_v2::types::StreamId;
use arbitro_engine_v2::ArbitroEngine;
use tokio::sync::mpsc;

use crate::config::Config;
use crate::gate::Gate;
use crate::handle::ShardHandle;
use crate::shard::ShardWorker;
use crate::transport::ConnectionRegistry;

/// The server routes commands to the correct shard by stream_id.
/// Clone-friendly — backed by Arc.
#[derive(Clone)]
pub struct Server {
    shards: Arc<[ShardHandle]>,
}

impl Server {
    /// Spawn N shard workers on dedicated OS threads.
    pub fn spawn(config: &Config, registry: &ConnectionRegistry) -> Self {
        let shard_count = config.shard_count;
        let channel_capacity = config.channel_capacity;

        let mut handles = Vec::with_capacity(shard_count);

        for id in 0..shard_count {
            let (tx, rx) = mpsc::channel(channel_capacity);
            let engine = ArbitroEngine::new();
            let gate = Gate::new();

            let worker = ShardWorker::new(engine, rx, gate, registry.clone(), config.data_dir.clone());

            // Named thread — mandatory per concurrency.md
            let join_handle = std::thread::Builder::new()
                .name(format!("shard-{id}"))
                .spawn(move || worker.run())
                .expect("failed to spawn shard thread");

            let shard_thread = join_handle.thread().clone();
            handles.push(ShardHandle::new(id as u32, tx, shard_thread));
        }

        Self {
            shards: handles.into(),
        }
    }

    /// Route to the shard that owns this stream.
    /// Deterministic: stream_id.raw() % shard_count.
    #[inline]
    pub fn shard_for(&self, stream_id: StreamId) -> &ShardHandle {
        let idx = stream_id.raw() as usize % self.shards.len();
        &self.shards[idx]
    }

    /// Get shard by index.
    #[inline]
    pub fn shard(&self, index: usize) -> &ShardHandle {
        &self.shards[index]
    }

    /// Number of shards.
    #[inline]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Graceful shutdown — send Shutdown to all shards.
    pub fn shutdown(&self) {
        for shard in self.shards.iter() {
            shard.send_shutdown();
        }
    }
}
