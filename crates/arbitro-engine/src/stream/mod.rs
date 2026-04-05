//! StreamMap — 64-way sharded stream registry.
//!
//! Lookup by stream_id (FNV-1a u32). Each shard is its own Mutex,
//! so streams on different shards never contend.
//!
//! StreamSlot owns: config + store + drain + signal.
//! The drain lives here (not in a separate HashMap) so publish
//! can append + signal under a SINGLE shard lock (R19).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arbitro_proto::config::StreamConfig;
use arbitro_store::{MemoryStore, Store, StoreInfo};

use crate::drain::ReactiveDrain;
use crate::drain::signal::DrainSignal;
#[cfg(test)]
use crate::drain::signal::NullSignal;

const SHARD_COUNT: usize = 64;
const SHARD_MASK: u32 = (SHARD_COUNT as u32) - 1;

/// A single stream: config + journal store + drain + signal.
pub struct StreamSlot {
    pub config: StreamConfig,
    pub store: Box<dyn Store>,
    pub drain: ReactiveDrain,
    pub signal: Arc<dyn DrainSignal>,
}

impl StreamSlot {
    pub fn new(config: StreamConfig, signal: Arc<dyn DrainSignal>) -> Self {
        let stream_id = config.stream_id;
        let store: Box<dyn Store> = match config.journal_kind {
            arbitro_proto::config::JournalKind::Memory => Box::new(MemoryStore::new()),
            // Disk journals provided by arbitro-server via factory
            _ => Box::new(MemoryStore::new()),
        };
        Self {
            config,
            store,
            drain: ReactiveDrain::new(stream_id),
            signal,
        }
    }

    #[inline]
    pub fn info(&self) -> StoreInfo {
        self.store.info()
    }
}

struct Shard {
    streams: HashMap<u32, StreamSlot>,
}

impl Shard {
    fn new() -> Self {
        Self { streams: HashMap::new() }
    }
}

/// Sharded stream registry. Lock one shard at a time — no global lock.
pub struct StreamMap {
    shards: Box<[Mutex<Shard>]>,
}

impl Default for StreamMap {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamMap {
    pub fn new() -> Self {
        let shards: Vec<Mutex<Shard>> = (0..SHARD_COUNT)
            .map(|_| Mutex::new(Shard::new()))
            .collect();
        Self { shards: shards.into_boxed_slice() }
    }

    #[inline(always)]
    fn shard_idx(stream_id: u32) -> usize {
        (stream_id & SHARD_MASK) as usize
    }

    /// Insert a stream with a drain signal. Returns false if already exists.
    pub fn insert(&self, config: StreamConfig, signal: Arc<dyn DrainSignal>) -> bool {
        let id = config.stream_id;
        let mut shard = self.shards[Self::shard_idx(id)].lock().unwrap();
        if shard.streams.contains_key(&id) {
            return false;
        }
        shard.streams.insert(id, StreamSlot::new(config, signal));
        true
    }

    /// Remove a stream. Returns the config if it existed.
    pub fn remove(&self, stream_id: u32) -> Option<StreamConfig> {
        let mut shard = self.shards[Self::shard_idx(stream_id)].lock().unwrap();
        shard.streams.remove(&stream_id).map(|s| s.config)
    }

    /// Execute a closure with mutable access to a stream slot.
    /// Returns None if stream not found.
    #[inline]
    pub fn with_mut<F, R>(&self, stream_id: u32, f: F) -> Option<R>
    where
        F: FnOnce(&mut StreamSlot) -> R,
    {
        let mut shard = self.shards[Self::shard_idx(stream_id)].lock().unwrap();
        shard.streams.get_mut(&stream_id).map(f)
    }

    /// Execute a closure with read access to a stream slot.
    #[inline]
    pub fn with<F, R>(&self, stream_id: u32, f: F) -> Option<R>
    where
        F: FnOnce(&StreamSlot) -> R,
    {
        let shard = self.shards[Self::shard_idx(stream_id)].lock().unwrap();
        shard.streams.get(&stream_id).map(f)
    }

    /// Total number of streams across all shards.
    pub fn count(&self) -> usize {
        self.shards.iter()
            .map(|s| s.lock().unwrap().streams.len())
            .sum()
    }

    /// Collect all stream configs (cold path — management only).
    pub fn list_configs(&self) -> Vec<StreamConfig> {
        let mut out = Vec::new();
        for shard in self.shards.iter() {
            let s = shard.lock().unwrap();
            for slot in s.streams.values() {
                out.push(slot.config.clone());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arbitro_proto::config::StreamConfig;
    use arbitro_store::EntryRef;

    fn test_config(name: &[u8]) -> StreamConfig {
        StreamConfig::new(name).build()
    }

    fn null_signal() -> Arc<dyn DrainSignal> {
        Arc::new(NullSignal)
    }

    #[test]
    fn insert_and_lookup() {
        let map = StreamMap::new();
        let cfg = test_config(b"ORDERS");
        let id = cfg.stream_id;

        assert!(map.insert(cfg, null_signal()));
        assert!(map.with(id, |s| s.config.stream_id).is_some());
    }

    #[test]
    fn duplicate_insert_fails() {
        let map = StreamMap::new();
        let cfg1 = test_config(b"ORDERS");
        let cfg2 = test_config(b"ORDERS");

        assert!(map.insert(cfg1, null_signal()));
        assert!(!map.insert(cfg2, null_signal()));
    }

    #[test]
    fn remove_stream() {
        let map = StreamMap::new();
        let cfg = test_config(b"ORDERS");
        let id = cfg.stream_id;

        map.insert(cfg, null_signal());
        assert!(map.remove(id).is_some());
        assert!(map.with(id, |_| ()).is_none());
    }

    #[test]
    fn with_mut_appends() {
        let map = StreamMap::new();
        let cfg = test_config(b"ORDERS");
        let id = cfg.stream_id;
        map.insert(cfg, null_signal());

        let seq = map.with_mut(id, |slot| {
            slot.store.append(EntryRef { subject: b"orders.created", payload: b"{}" }, 1000)
        });
        assert_eq!(seq, Some(Ok(1)));

        let info = map.with(id, |slot| slot.info());
        assert_eq!(info.unwrap().messages, 1);
    }

    #[test]
    fn count_and_list() {
        let map = StreamMap::new();
        map.insert(test_config(b"A"), null_signal());
        map.insert(test_config(b"B"), null_signal());
        map.insert(test_config(b"C"), null_signal());

        assert_eq!(map.count(), 3);
        assert_eq!(map.list_configs().len(), 3);
    }

    #[test]
    fn different_shards_no_contention() {
        let map = StreamMap::new();
        for i in 0u32..128 {
            let name = format!("STREAM_{}", i);
            map.insert(test_config(name.as_bytes()), null_signal());
        }
        assert_eq!(map.count(), 128);
    }

    #[test]
    fn stream_slot_has_drain() {
        let map = StreamMap::new();
        let cfg = test_config(b"ORDERS");
        let id = cfg.stream_id;
        map.insert(cfg, null_signal());

        // Drain is accessible via StreamSlot
        map.with_mut(id, |slot| {
            assert_eq!(slot.drain.consumer_count(), 0);
        });
    }
}
