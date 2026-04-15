//! CatalogApi — define what exists: streams, consumers, subscriptions.
//!
//! Level 5 — depends on Level 0-4.
//!
//! The catalog manages the logical plane: creating, finding, and removing
//! streams, consumers, and subscriptions. It updates the match table,
//! graph, and edges when entities are added or removed.

pub mod match_table;

use std::collections::HashMap;
use crate::error::{EngineError, EngineResult};
use crate::types::*;
use crate::graph::GraphStore;
use crate::graph::node::*;
use crate::edge::BuiltinEdges;
use match_table::{MatchEntry, MatchTable};

// ── Config types (input to catalog operations) ───────────────────────────────

/// Configuration for creating a stream.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub id: StreamId,
    pub name: Vec<u8>,
}

/// Configuration for creating a consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub id: ConsumerId,
    pub queue_id: QueueId,
    pub stream_id: StreamId,
    pub durable: bool,
    pub ack_policy: AckPolicy,
    pub max_inflight: u32,
}

/// Configuration for creating a subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    pub id: SubscriptionId,
    pub stream_id: StreamId,
    pub consumer_id: ConsumerId,
    /// Subject filters. Empty = accept all subjects (catch-all).
    pub filters: Vec<Vec<u8>>,
}

// ── Catalog ──────────────────────────────────────────────────────────────────

/// The catalog: manages entity lifecycle and match table consistency.
pub struct Catalog {
    /// Per-stream match tables. Dense `Vec<Option<MatchTable>>` indexed by
    /// raw `stream_id`. `StreamId` is assigned monotonically and bounded
    /// (typically <100), so a Vec with direct indexing beats a HashMap on
    /// the hot path: one load + null-check vs hash + bucket walk.
    /// See `performance.md` §11 (slab/array over HashMap on hot paths).
    match_tables: Vec<Option<MatchTable>>,

    /// Map from domain IDs to slab keys.
    stream_keys: HashMap<StreamId, SlabKey, ahash::RandomState>,
    consumer_keys: HashMap<ConsumerId, SlabKey, ahash::RandomState>,
    subscription_keys: HashMap<SubscriptionId, SlabKey, ahash::RandomState>,
    queue_keys: HashMap<QueueId, SlabKey, ahash::RandomState>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            match_tables: Vec::with_capacity(16),
            stream_keys: HashMap::with_hasher(ahash::RandomState::new()),
            consumer_keys: HashMap::with_hasher(ahash::RandomState::new()),
            subscription_keys: HashMap::with_hasher(ahash::RandomState::new()),
            queue_keys: HashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    /// Grow `match_tables` so it can hold `stream_id`. Grows in chunks of
    /// 4 past the requested index to amortize resizes during startup.
    #[inline(always)]
    fn ensure_match_table_slot(&mut self, stream_id: StreamId) {
        let idx = stream_id.0 as usize;
        if idx >= self.match_tables.len() {
            self.match_tables.resize_with(idx + 4, || None);
        }
    }

    // ── Stream ───────────────────────────────────────────────────────────

    /// Create or ensure a stream exists. Idempotent.
    pub fn ensure_stream(
        &mut self,
        graph: &mut GraphStore,
        config: StreamConfig,
    ) -> EngineResult<SlabKey> {
        if let Some(&key) = self.stream_keys.get(&config.id) {
            return Ok(key);
        }

        let node = StreamNode {
            stream_id: config.id,
            name: config.name,
        };
        let key = graph.insert_stream(node);
        self.stream_keys.insert(config.id, key);
        self.ensure_match_table_slot(config.id);
        let slot = &mut self.match_tables[config.id.0 as usize];
        if slot.is_none() {
            *slot = Some(MatchTable::new());
        }
        Ok(key)
    }

    /// Remove a stream from the catalog. Returns the graph key for removal.
    pub fn remove_stream(
        &mut self,
        graph: &mut GraphStore,
        id: StreamId,
    ) -> EngineResult<()> {
        let key = self.stream_keys.remove(&id)
            .ok_or_else(EngineError::stream_not_found)?;
        if let Some(slot) = self.match_tables.get_mut(id.0 as usize) {
            *slot = None;
        }
        let _ = graph.remove_stream(key);
        Ok(())
    }

    /// Get the slab key for a stream.
    #[inline]
    pub fn stream_key(&self, id: StreamId) -> EngineResult<SlabKey> {
        self.stream_keys.get(&id).copied()
            .ok_or_else(EngineError::stream_not_found)
    }

    // ── Queue ────────────────────────────────────────────────────────────

    /// Ensure a queue exists. Created implicitly when a consumer references it.
    fn ensure_queue(
        &mut self,
        graph: &mut GraphStore,
        queue_id: QueueId,
        stream_id: StreamId,
    ) -> SlabKey {
        if let Some(&key) = self.queue_keys.get(&queue_id) {
            return key;
        }

        let node = QueueNode {
            queue_id,
            stream_id,
            paused: false,
        };
        let key = graph.insert_queue(node);
        self.queue_keys.insert(queue_id, key);
        key
    }

    /// Remove a queue from the catalog and graph.
    /// Call drain_queue() first to release pending messages and ready state.
    /// Only call when no consumers reference this queue.
    pub fn remove_queue(
        &mut self,
        graph: &mut GraphStore,
        queue_id: QueueId,
    ) -> EngineResult<()> {
        let key = self.queue_keys.remove(&queue_id)
            .ok_or_else(EngineError::queue_not_found)?;
        let _ = graph.remove_queue(key);
        Ok(())
    }

    // ── Consumer ─────────────────────────────────────────────────────────

    /// Create or ensure a consumer exists. Idempotent.
    pub fn ensure_consumer(
        &mut self,
        graph: &mut GraphStore,
        edges: &mut BuiltinEdges,
        config: ConsumerConfig,
    ) -> EngineResult<SlabKey> {
        // Verify stream exists
        let _ = self.stream_key(config.stream_id)?;

        if let Some(&key) = self.consumer_keys.get(&config.id) {
            return Ok(key);
        }

        // Ensure queue
        self.ensure_queue(graph, config.queue_id, config.stream_id);

        let node = ConsumerNode {
            consumer_id: config.id,
            queue_id: config.queue_id,
            stream_id: config.stream_id,
            durable: config.durable,
            ack_policy: config.ack_policy,
            max_inflight: config.max_inflight,
            paused: false,
        };
        let key = graph.insert_consumer(node);
        self.consumer_keys.insert(config.id, key);

        // Register in edge indexes
        edges.consumers_by_queue.insert(&config.queue_id, key);
        edges.consumers_by_stream.insert(&config.stream_id, key);

        Ok(key)
    }

    /// Remove a consumer from the catalog, graph, and edge indexes.
    /// Also removes all subscriptions owned by this consumer.
    /// Call drain_consumer() first to release pending messages and bindings.
    pub fn remove_consumer(
        &mut self,
        graph: &mut GraphStore,
        edges: &mut BuiltinEdges,
        id: ConsumerId,
    ) -> EngineResult<()> {
        let key = self.consumer_keys.remove(&id)
            .ok_or_else(EngineError::consumer_not_found)?;

        // Remove subscriptions owned by this consumer
        let sub_keys = edges.subscriptions_by_consumer.take(&id);
        for sub_key in sub_keys {
            if let Ok(sub_node) = graph.remove_subscription(sub_key) {
                self.subscription_keys.remove(&sub_node.subscription_id);
                edges.subscriptions_by_stream.remove(&sub_node.stream_id, &sub_key);
                // Clean match table entries for this subscription
                if let Some(mt) = self.match_tables
                    .get_mut(sub_node.stream_id.0 as usize)
                    .and_then(|s| s.as_mut())
                {
                    mt.remove_subscription(sub_node.subscription_id);
                }
            }
        }

        // Remove consumer from graph
        let node = graph.remove_consumer(key)?;

        // Clean up consumer edge indexes
        edges.consumers_by_queue.remove(&node.queue_id, &key);
        edges.consumers_by_stream.remove(&node.stream_id, &key);

        Ok(())
    }

    /// Get the slab key for a consumer.
    #[inline]
    pub fn consumer_key(&self, id: ConsumerId) -> EngineResult<SlabKey> {
        self.consumer_keys.get(&id).copied()
            .ok_or_else(EngineError::consumer_not_found)
    }

    // ── Subscription ─────────────────────────────────────────────────────

    /// Create or ensure a subscription exists. Updates match table.
    pub fn ensure_subscription(
        &mut self,
        graph: &mut GraphStore,
        edges: &mut BuiltinEdges,
        config: SubscriptionConfig,
    ) -> EngineResult<SlabKey> {
        // Verify parent entities
        let _ = self.stream_key(config.stream_id)?;
        let consumer_key = self.consumer_key(config.consumer_id)?;
        let consumer = graph.get_consumer(consumer_key)?;
        let queue_id = consumer.queue_id;

        if let Some(&key) = self.subscription_keys.get(&config.id) {
            return Ok(key);
        }

        let node = SubscriptionNode {
            subscription_id: config.id,
            stream_id: config.stream_id,
            consumer_id: config.consumer_id,
            filters: config.filters.clone(),
        };
        let key = graph.insert_subscription(node);
        self.subscription_keys.insert(config.id, key);

        // Register in edge indexes
        edges.subscriptions_by_consumer.insert(&config.consumer_id, key);
        edges.subscriptions_by_stream.insert(&config.stream_id, key);

        // Update match table
        let match_entry = MatchEntry {
            consumer_id: config.consumer_id,
            queue_id,
            subscription_id: config.id,
            connection_id: ConnectionId(0), // set at bind time
        };

        self.ensure_match_table_slot(config.stream_id);
        let mt = self.match_tables[config.stream_id.0 as usize]
            .get_or_insert_with(MatchTable::new);
        if config.filters.is_empty() {
            mt.add_catch_all(match_entry);
        } else {
            for filter in &config.filters {
                // If filter contains wildcards, add as pattern
                if filter.contains(&b'*') || filter.contains(&b'>') {
                    mt.add_pattern(filter.clone(), match_entry);
                } else {
                    // Exact filter — compute hash and add direct mapping
                    let hash = fnv1a_32(filter);
                    mt.add_exact(hash, match_entry);
                }
            }
        }

        Ok(key)
    }

    /// Remove a single subscription from the catalog, graph, edges, and match table.
    /// Call drain_subscription() first to release pending messages and bindings.
    pub fn remove_subscription(
        &mut self,
        graph: &mut GraphStore,
        edges: &mut BuiltinEdges,
        id: SubscriptionId,
    ) -> EngineResult<()> {
        let key = self.subscription_keys.remove(&id)
            .ok_or_else(EngineError::subscription_not_found)?;

        let node = graph.remove_subscription(key)?;

        edges.subscriptions_by_consumer.remove(&node.consumer_id, &key);
        edges.subscriptions_by_stream.remove(&node.stream_id, &key);

        if let Some(mt) = self.match_tables
            .get_mut(node.stream_id.0 as usize)
            .and_then(|s| s.as_mut())
        {
            mt.remove_subscription(id);
        }

        Ok(())
    }

    /// Get the slab key for a subscription.
    #[inline]
    pub fn subscription_key(&self, id: SubscriptionId) -> EngineResult<SlabKey> {
        self.subscription_keys.get(&id).copied()
            .ok_or_else(EngineError::subscription_not_found)
    }

    // ── Listing ──────────────────────────────────────────────────────────

    /// Return all known stream IDs. Management path.
    pub fn stream_ids(&self) -> Vec<StreamId> {
        self.stream_keys.keys().copied().collect()
    }

    /// Return all known consumer IDs. Management path.
    pub fn consumer_ids(&self) -> Vec<ConsumerId> {
        self.consumer_keys.keys().copied().collect()
    }

    // ── Match table access ───────────────────────────────────────────────

    /// Get the match table for a stream. O(1) — direct Vec index.
    #[inline]
    pub fn match_table(&self, stream_id: StreamId) -> Option<&MatchTable> {
        self.match_tables.get(stream_id.0 as usize)?.as_ref()
    }

    /// Get a mutable match table for pattern resolution. O(1) — direct Vec index.
    #[inline]
    pub fn match_table_mut(&mut self, stream_id: StreamId) -> Option<&mut MatchTable> {
        self.match_tables.get_mut(stream_id.0 as usize)?.as_mut()
    }

    /// Precompute connection_id in match entries for a subscription.
    /// Called by bind. Management path — O(subjects + catch_all).
    pub fn bind_subscription_connection(
        &mut self,
        stream_id: StreamId,
        subscription_id: SubscriptionId,
        connection_id: ConnectionId,
    ) {
        if let Some(mt) = self.match_table_mut(stream_id) {
            mt.bind_subscription(subscription_id, connection_id);
        }
    }

    /// Clear connection_id in match entries for a subscription.
    /// Called by unbind. Management path.
    pub fn unbind_subscription_connection(
        &mut self,
        stream_id: StreamId,
        subscription_id: SubscriptionId,
    ) {
        if let Some(mt) = self.match_table_mut(stream_id) {
            mt.unbind_subscription(subscription_id);
        }
    }

    /// Set max inflight per subject by pattern on a stream. Management path.
    pub fn set_max_subject_inflight(
        &mut self,
        stream_id: StreamId,
        pattern: &[u8],
        max_inflight: u32,
    ) -> EngineResult<()> {
        let _ = self.stream_key(stream_id)?;
        self.ensure_match_table_slot(stream_id);
        let mt = self.match_tables[stream_id.0 as usize]
            .get_or_insert_with(MatchTable::new);
        mt.add_max_subject_inflight(pattern, max_inflight);
        Ok(())
    }

    /// Get the max inflight for a concrete subject hash. O(1).
    #[inline]
    pub fn max_subject_inflight(&self, stream_id: StreamId, subject_hash: u32) -> Option<u32> {
        self.match_tables.get(stream_id.0 as usize)?
            .as_ref()?
            .max_subject_inflight(subject_hash)
    }

    /// Fast-path: does ANY subject on this stream have an inflight limit?
    /// Called once per claim batch; when false the hot loop skips all
    /// subject-limit HashMap lookups (~10-15 ns/msg).
    #[inline(always)]
    pub fn stream_has_subject_limits(&self, stream_id: StreamId) -> bool {
        self.match_tables
            .get(stream_id.0 as usize)
            .and_then(|s| s.as_ref())
            .map(|mt| mt.has_subject_limits())
            .unwrap_or(false)
    }
}

impl Default for Catalog {
    fn default() -> Self { Self::new() }
}

// ── FNV-1a hash (inline, branch-free) ───────────────────────────────────────

/// FNV-1a 32-bit hash. Used for subject hashing.
/// Inline, branch-free, ~0.7ns/byte for typical 15-byte subjects.
#[inline]
pub fn fnv1a_32(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (Catalog, GraphStore, BuiltinEdges) {
        let catalog = Catalog::new();
        let graph = GraphStore::new();
        let edges = BuiltinEdges::new();
        (catalog, graph, edges)
    }

    #[test]
    fn ensure_stream_idempotent() {
        let (mut cat, mut graph, _) = setup();
        let k1 = cat.ensure_stream(&mut graph, StreamConfig {
            id: StreamId(1),
            name: b"orders".to_vec(),
        }).unwrap();
        let k2 = cat.ensure_stream(&mut graph, StreamConfig {
            id: StreamId(1),
            name: b"orders".to_vec(),
        }).unwrap();
        assert_eq!(k1, k2);
    }

    #[test]
    fn ensure_consumer_requires_stream() {
        let (mut cat, mut graph, mut edges) = setup();

        let result = cat.ensure_consumer(&mut graph, &mut edges, ConsumerConfig {
            id: ConsumerId(1),
            queue_id: QueueId(10),
            stream_id: StreamId(999), // doesn't exist
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 1000,
        });
        assert!(result.is_err());
    }

    #[test]
    fn full_catalog_lifecycle() {
        let (mut cat, mut graph, mut edges) = setup();

        // Create stream
        cat.ensure_stream(&mut graph, StreamConfig {
            id: StreamId(1), name: b"messages".to_vec(),
        }).unwrap();

        // Create consumer
        cat.ensure_consumer(&mut graph, &mut edges, ConsumerConfig {
            id: ConsumerId(10),
            queue_id: QueueId(100),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 10_000,
        }).unwrap();

        // Create subscription with filters
        cat.ensure_subscription(&mut graph, &mut edges, SubscriptionConfig {
            id: SubscriptionId(20),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(10),
            filters: vec![b"message.meta.>".to_vec(), b"message.qr.>".to_vec()],
        }).unwrap();

        // Match table has pattern entries
        let mt = cat.match_table(StreamId(1)).unwrap();
        assert_eq!(mt.pattern_count(), 2);

        // Resolve a subject
        let mt = cat.match_table_mut(StreamId(1)).unwrap();
        mt.resolve_patterns(fnv1a_32(b"message.meta.123"), b"message.meta.123");
        let result = mt.lookup(fnv1a_32(b"message.meta.123"));
        assert_eq!(result.count(), 1);
    }

    #[test]
    fn catch_all_subscription() {
        let (mut cat, mut graph, mut edges) = setup();

        cat.ensure_stream(&mut graph, StreamConfig {
            id: StreamId(1), name: b"all".to_vec(),
        }).unwrap();
        cat.ensure_consumer(&mut graph, &mut edges, ConsumerConfig {
            id: ConsumerId(1), queue_id: QueueId(1), stream_id: StreamId(1),
            durable: true, ack_policy: AckPolicy::Explicit, max_inflight: 100,
        }).unwrap();
        cat.ensure_subscription(&mut graph, &mut edges, SubscriptionConfig {
            id: SubscriptionId(1), stream_id: StreamId(1), consumer_id: ConsumerId(1),
            filters: vec![], // no filters = catch-all
        }).unwrap();

        let mt = cat.match_table(StreamId(1)).unwrap();
        assert_eq!(mt.catch_all_count(), 1);
        // Any subject matches
        let result = mt.lookup(0xABCD);
        assert_eq!(result.count(), 1);
    }

    #[test]
    fn fnv1a_deterministic() {
        assert_eq!(fnv1a_32(b"orders.created"), fnv1a_32(b"orders.created"));
        assert_ne!(fnv1a_32(b"orders.created"), fnv1a_32(b"orders.updated"));
    }
}
