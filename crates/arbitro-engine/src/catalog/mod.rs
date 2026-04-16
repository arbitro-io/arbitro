//! Catalog — entity lifecycle, match tables, binding management, demand.
//!
//! Level 5 — depends on Level 0-4.
//!
//! Stores streams, consumers, subscriptions, and bindings directly — no
//! external graph or edge dependency. Bindings use 3 secondary indices
//! (`by_stream`, `by_consumer`, `by_connection`) for O(1) retire lookups.

pub mod match_table;

use std::collections::HashMap;

use crate::error::{EngineError, EngineResult};
use crate::events::DeltaEvents;
use crate::types::*;
use match_table::{MatchEntry, MatchTable};

// ── Config types (input to catalog operations) ──────────────────────────────

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

// ── Stored entities ─────────────────────────────────────────────────────────

/// Stream metadata.
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub name: Vec<u8>,
}

/// Consumer metadata — stored directly in the catalog.
#[derive(Debug, Clone)]
pub struct ConsumerInfo {
    pub stream_id: StreamId,
    pub queue_id: QueueId,
    pub max_inflight: u32,
    pub paused: bool,
    pub ack_policy: AckPolicy,
    pub durable: bool,
}

/// Subscription metadata.
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    pub stream_id: StreamId,
    pub consumer_id: ConsumerId,
    pub filters: Vec<Vec<u8>>,
}

/// Pending delivery awaiting ack. Stored inline in `Binding`.
///
/// 16 bytes — `#[repr(C)]` for zerocopy compatibility.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Pending {
    pub seq: u64,
    pub subject_hash: u32,
    pub _pad: u32,
}
const _: () = assert!(core::mem::size_of::<Pending>() == 16);

/// Active binding — subscription × connection. Per-binding pending
/// tracking replaces the legacy global `PendingNode` slab.
#[derive(Debug, Clone)]
pub struct Binding {
    pub binding_id: BindingId,
    pub stream_id: StreamId,
    pub consumer_id: ConsumerId,
    pub connection_id: ConnectionId,
    pub subscription_id: SubscriptionId,
    pub queue_id: QueueId,
    pub max_inflight: u32,
    pub paused: bool,
    pub fire_and_forget: bool,
    /// In-flight messages awaiting ack. Inline in the binding —
    /// `Vec<Pending>` for now, `SmallVec<[Pending; 4]>` once dep lands.
    pub pending: Vec<Pending>,
}

/// Recipient resolved by `resolve_recipients` — ready for dispatch.
#[derive(Debug, Clone, Copy)]
pub struct Recipient {
    pub binding_id: BindingId,
    pub consumer_id: ConsumerId,
    pub connection_id: ConnectionId,
    pub subscription_id: SubscriptionId,
    pub queue_id: QueueId,
}

// ── Catalog ─────────────────────────────────────────────────────────────────

/// The catalog: entity lifecycle, match tables, bindings, demand tracking.
pub struct Catalog {
    // Entity storage — direct HashMap, no graph indirection.
    streams: HashMap<StreamId, StreamInfo, ahash::RandomState>,
    consumers: HashMap<ConsumerId, ConsumerInfo, ahash::RandomState>,
    subscriptions: HashMap<SubscriptionId, SubscriptionInfo, ahash::RandomState>,

    // Bindings with 3 secondary indices.
    bindings: HashMap<BindingId, Binding, ahash::RandomState>,
    by_stream: HashMap<StreamId, Vec<BindingId>, ahash::RandomState>,
    by_consumer: HashMap<ConsumerId, Vec<BindingId>, ahash::RandomState>,
    by_connection: HashMap<ConnectionId, Vec<BindingId>, ahash::RandomState>,
    next_binding_id: u32,

    // Connection tracking.
    connections: HashMap<ConnectionId, NodeId, ahash::RandomState>,

    // Demand counters: streams with ≥1 active binding.
    demand: HashMap<StreamId, u32, ahash::RandomState>,

    // Per-stream match tables.
    match_tables: Vec<Option<MatchTable>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            streams: HashMap::with_hasher(ahash::RandomState::new()),
            consumers: HashMap::with_hasher(ahash::RandomState::new()),
            subscriptions: HashMap::with_hasher(ahash::RandomState::new()),
            bindings: HashMap::with_hasher(ahash::RandomState::new()),
            by_stream: HashMap::with_hasher(ahash::RandomState::new()),
            by_consumer: HashMap::with_hasher(ahash::RandomState::new()),
            by_connection: HashMap::with_hasher(ahash::RandomState::new()),
            next_binding_id: 1,
            connections: HashMap::with_hasher(ahash::RandomState::new()),
            demand: HashMap::with_hasher(ahash::RandomState::new()),
            match_tables: Vec::with_capacity(16),
        }
    }

    #[inline(always)]
    fn ensure_match_table_slot(&mut self, stream_id: StreamId) {
        let idx = stream_id.0 as usize;
        if idx >= self.match_tables.len() {
            self.match_tables.resize_with(idx + 4, || None);
        }
    }

    // ── Demand ──────────────────────────────────────────────────────────

    /// Any stream has ≥1 active binding.
    #[inline]
    pub fn has_any_demand(&self) -> bool {
        !self.demand.is_empty()
    }

    /// This stream has ≥1 active binding.
    #[inline]
    pub fn has_demand(&self, stream_id: StreamId) -> bool {
        self.demand.get(&stream_id).copied().unwrap_or(0) > 0
    }

    fn inc_demand(&mut self, stream_id: StreamId, events: &mut DeltaEvents) {
        let count = self.demand.entry(stream_id).or_insert(0);
        *count += 1;
        if *count == 1 {
            events.demand_became_available.push(stream_id);
        }
    }

    fn dec_demand(&mut self, stream_id: StreamId, events: &mut DeltaEvents) {
        if let Some(count) = self.demand.get_mut(&stream_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.demand.remove(&stream_id);
                events.demand_became_idle.push(stream_id);
            }
        }
    }

    // ── Stream ──────────────────────────────────────────────────────────

    /// Create or ensure a stream exists. Idempotent.
    pub fn ensure_stream(&mut self, config: StreamConfig) -> EngineResult<()> {
        if self.streams.contains_key(&config.id) {
            return Ok(());
        }
        self.streams
            .insert(config.id, StreamInfo { name: config.name });
        self.ensure_match_table_slot(config.id);
        let slot = &mut self.match_tables[config.id.0 as usize];
        if slot.is_none() {
            *slot = Some(MatchTable::new());
        }
        Ok(())
    }

    /// Remove a stream. Does NOT cascade bindings — caller must retire
    /// bindings first via `retire_bindings_for_stream`.
    pub fn remove_stream_entity(&mut self, id: StreamId) -> EngineResult<()> {
        if self.streams.remove(&id).is_none() {
            return Err(EngineError::stream_not_found());
        }
        if let Some(slot) = self.match_tables.get_mut(id.0 as usize) {
            *slot = None;
        }
        Ok(())
    }

    /// Stream exists?
    #[inline]
    pub fn stream_exists(&self, id: StreamId) -> bool {
        self.streams.contains_key(&id)
    }

    // ── Consumer ────────────────────────────────────────────────────────

    /// Create or ensure a consumer exists. Idempotent.
    pub fn ensure_consumer(&mut self, config: ConsumerConfig) -> EngineResult<()> {
        if !self.streams.contains_key(&config.stream_id) {
            return Err(EngineError::stream_not_found());
        }
        if self.consumers.contains_key(&config.id) {
            return Ok(());
        }
        self.consumers.insert(
            config.id,
            ConsumerInfo {
                stream_id: config.stream_id,
                queue_id: config.queue_id,
                max_inflight: config.max_inflight,
                paused: false,
                ack_policy: config.ack_policy,
                durable: config.durable,
            },
        );
        Ok(())
    }

    /// Remove a consumer entity. Does NOT cascade — caller retires
    /// bindings and subscriptions first.
    pub fn remove_consumer_entity(&mut self, id: ConsumerId) -> EngineResult<()> {
        self.consumers
            .remove(&id)
            .ok_or_else(EngineError::consumer_not_found)?;
        Ok(())
    }

    /// Get consumer info.
    #[inline]
    pub fn consumer(&self, id: ConsumerId) -> Option<&ConsumerInfo> {
        self.consumers.get(&id)
    }

    /// Is the consumer paused?
    #[inline]
    pub fn is_paused(&self, id: ConsumerId) -> bool {
        self.consumers
            .get(&id)
            .map(|c| c.paused)
            .unwrap_or(false)
    }

    /// Pause a consumer.
    pub fn pause_consumer(&mut self, id: ConsumerId) -> bool {
        if let Some(info) = self.consumers.get_mut(&id) {
            info.paused = true;
            true
        } else {
            false
        }
    }

    /// Resume a consumer.
    pub fn resume_consumer(&mut self, id: ConsumerId) -> bool {
        if let Some(info) = self.consumers.get_mut(&id) {
            info.paused = false;
            true
        } else {
            false
        }
    }

    // ── Subscription ────────────────────────────────────────────────────

    /// Create or ensure a subscription exists. Updates match table.
    pub fn ensure_subscription(&mut self, config: SubscriptionConfig) -> EngineResult<()> {
        if !self.streams.contains_key(&config.stream_id) {
            return Err(EngineError::stream_not_found());
        }
        let consumer = self
            .consumers
            .get(&config.consumer_id)
            .ok_or_else(EngineError::consumer_not_found)?;
        let queue_id = consumer.queue_id;

        if self.subscriptions.contains_key(&config.id) {
            return Ok(());
        }

        self.subscriptions.insert(
            config.id,
            SubscriptionInfo {
                stream_id: config.stream_id,
                consumer_id: config.consumer_id,
                filters: config.filters.clone(),
            },
        );

        // Update match table.
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
                if filter.contains(&b'*') || filter.contains(&b'>') {
                    mt.add_pattern(filter.clone(), match_entry);
                } else {
                    let hash = fnv1a_32(filter);
                    mt.add_exact(hash, match_entry);
                }
            }
        }

        Ok(())
    }

    /// Remove a subscription and clean match table. Does NOT cascade
    /// bindings — caller retires bindings first.
    pub fn remove_subscription_entity(&mut self, id: SubscriptionId) -> EngineResult<()> {
        let info = self
            .subscriptions
            .remove(&id)
            .ok_or_else(EngineError::subscription_not_found)?;

        if let Some(mt) = self
            .match_tables
            .get_mut(info.stream_id.0 as usize)
            .and_then(|s| s.as_mut())
        {
            mt.remove_subscription(id);
        }

        Ok(())
    }

    /// Subscription IDs owned by a consumer.
    pub fn subscriptions_for_consumer(&self, consumer_id: ConsumerId) -> Vec<SubscriptionId> {
        self.subscriptions
            .iter()
            .filter(|(_, s)| s.consumer_id == consumer_id)
            .map(|(id, _)| *id)
            .collect()
    }

    // ── Connection ──────────────────────────────────────────────────────

    /// Register a new connection.
    pub fn open_connection(&mut self, connection_id: ConnectionId, node_id: NodeId) {
        self.connections.insert(connection_id, node_id);
    }

    /// Remove connection from tracking. Does NOT cascade bindings —
    /// caller retires them first.
    pub fn remove_connection_entity(&mut self, connection_id: ConnectionId) {
        self.connections.remove(&connection_id);
    }

    // ── Binding (subscribe/unsubscribe) ─────────────────────────────────

    /// Create a binding: connect a subscription to a connection. Updates
    /// match table with the connection_id and increments demand.
    pub fn subscribe(
        &mut self,
        connection_id: ConnectionId,
        subscription_id: SubscriptionId,
        events: &mut DeltaEvents,
    ) -> EngineResult<BindingId> {
        let sub = self
            .subscriptions
            .get(&subscription_id)
            .ok_or_else(EngineError::subscription_not_found)?;
        let consumer = self
            .consumers
            .get(&sub.consumer_id)
            .ok_or_else(EngineError::consumer_not_found)?;

        let binding_id = BindingId(self.next_binding_id);
        self.next_binding_id += 1;

        let stream_id = sub.stream_id;
        let consumer_id = sub.consumer_id;

        let binding = Binding {
            binding_id,
            stream_id,
            consumer_id,
            connection_id,
            subscription_id,
            queue_id: consumer.queue_id,
            max_inflight: consumer.max_inflight,
            paused: consumer.paused,
            fire_and_forget: consumer.ack_policy == AckPolicy::None,
            pending: Vec::new(),
        };

        self.bindings.insert(binding_id, binding);
        self.by_stream
            .entry(stream_id)
            .or_default()
            .push(binding_id);
        self.by_consumer
            .entry(consumer_id)
            .or_default()
            .push(binding_id);
        self.by_connection
            .entry(connection_id)
            .or_default()
            .push(binding_id);

        // Precompute connection_id in match entries.
        self.bind_subscription_connection(stream_id, subscription_id, connection_id);

        // Increment demand.
        self.inc_demand(stream_id, events);

        Ok(binding_id)
    }

    /// Retire a single binding. Removes from all indices, cleans match
    /// table, decrements demand. Returns the binding data (including
    /// pending entries) so the caller can release inflight credits.
    ///
    /// This is the `retire_binding` primitive from the plan — shared by
    /// `delete_stream`, `delete_consumer`, `mark_connection_dead`.
    pub fn retire_binding(
        &mut self,
        binding_id: BindingId,
        events: &mut DeltaEvents,
    ) -> Option<Binding> {
        let binding = self.bindings.remove(&binding_id)?;

        // Remove from secondary indices.
        if let Some(v) = self.by_stream.get_mut(&binding.stream_id) {
            v.retain(|b| *b != binding_id);
        }
        if let Some(v) = self.by_consumer.get_mut(&binding.consumer_id) {
            v.retain(|b| *b != binding_id);
        }
        if let Some(v) = self.by_connection.get_mut(&binding.connection_id) {
            v.retain(|b| *b != binding_id);
        }

        // Unbind from match table.
        self.unbind_subscription_connection(binding.stream_id, binding.subscription_id);

        // Decrement demand.
        self.dec_demand(binding.stream_id, events);

        events.bindings_retired.push(binding_id);

        Some(binding)
    }

    // ── Binding access ──────────────────────────────────────────────────

    /// Get binding by ID.
    #[inline]
    pub fn binding(&self, id: BindingId) -> Option<&Binding> {
        self.bindings.get(&id)
    }

    /// Get mutable binding by ID.
    #[inline]
    pub fn binding_mut(&mut self, id: BindingId) -> Option<&mut Binding> {
        self.bindings.get_mut(&id)
    }

    /// Binding IDs on a stream.
    #[inline]
    pub fn bindings_for_stream(&self, stream_id: StreamId) -> &[BindingId] {
        self.by_stream
            .get(&stream_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Binding IDs for a consumer.
    #[inline]
    pub fn bindings_for_consumer(&self, consumer_id: ConsumerId) -> &[BindingId] {
        self.by_consumer
            .get(&consumer_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Binding IDs on a connection.
    #[inline]
    pub fn bindings_for_connection(&self, connection_id: ConnectionId) -> &[BindingId] {
        self.by_connection
            .get(&connection_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    // ── Match table access ──────────────────────────────────────────────

    /// Get the match table for a stream. O(1) — direct Vec index.
    #[inline]
    pub fn match_table(&self, stream_id: StreamId) -> Option<&MatchTable> {
        self.match_tables.get(stream_id.0 as usize)?.as_ref()
    }

    /// Get a mutable match table. O(1).
    #[inline]
    pub fn match_table_mut(&mut self, stream_id: StreamId) -> Option<&mut MatchTable> {
        self.match_tables.get_mut(stream_id.0 as usize)?.as_mut()
    }

    /// Precompute connection_id in match entries for a subscription.
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
    pub fn unbind_subscription_connection(
        &mut self,
        stream_id: StreamId,
        subscription_id: SubscriptionId,
    ) {
        if let Some(mt) = self.match_table_mut(stream_id) {
            mt.unbind_subscription(subscription_id);
        }
    }

    /// Set max inflight per subject by pattern on a stream.
    pub fn set_max_subject_inflight(
        &mut self,
        stream_id: StreamId,
        pattern: &[u8],
        max_inflight: u32,
    ) -> EngineResult<()> {
        if !self.streams.contains_key(&stream_id) {
            return Err(EngineError::stream_not_found());
        }
        self.ensure_match_table_slot(stream_id);
        let mt = self.match_tables[stream_id.0 as usize]
            .get_or_insert_with(MatchTable::new);
        mt.add_max_subject_inflight(pattern, max_inflight);
        Ok(())
    }

    /// Get the max inflight for a concrete subject hash. O(1).
    #[inline]
    pub fn max_subject_inflight(&self, stream_id: StreamId, subject_hash: u32) -> Option<u32> {
        self.match_tables
            .get(stream_id.0 as usize)?
            .as_ref()?
            .max_subject_inflight(subject_hash)
    }

    /// Does any subject on this stream have an inflight limit?
    #[inline(always)]
    pub fn stream_has_subject_limits(&self, stream_id: StreamId) -> bool {
        self.match_tables
            .get(stream_id.0 as usize)
            .and_then(|s| s.as_ref())
            .map(|mt| mt.has_subject_limits())
            .unwrap_or(false)
    }

    // ── Listing ─────────────────────────────────────────────────────────

    /// All stream IDs.
    pub fn stream_ids(&self) -> Vec<StreamId> {
        self.streams.keys().copied().collect()
    }

    /// All consumer IDs.
    pub fn consumer_ids(&self) -> Vec<ConsumerId> {
        self.consumers.keys().copied().collect()
    }

    /// List all streams with names.
    pub fn list_streams(&self) -> Vec<(StreamId, Vec<u8>)> {
        self.streams
            .iter()
            .map(|(id, info)| (*id, info.name.clone()))
            .collect()
    }

    /// List all consumers.
    pub fn list_consumers(&self) -> Vec<(ConsumerId, StreamId, QueueId, bool)> {
        self.consumers
            .iter()
            .map(|(id, info)| (*id, info.stream_id, info.queue_id, info.paused))
            .collect()
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
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

    #[test]
    fn ensure_stream_idempotent() {
        let mut cat = Catalog::new();
        cat.ensure_stream(StreamConfig {
            id: StreamId(1),
            name: b"orders".to_vec(),
        })
        .unwrap();
        cat.ensure_stream(StreamConfig {
            id: StreamId(1),
            name: b"orders".to_vec(),
        })
        .unwrap();
        assert!(cat.stream_exists(StreamId(1)));
    }

    #[test]
    fn ensure_consumer_requires_stream() {
        let mut cat = Catalog::new();
        let result = cat.ensure_consumer(ConsumerConfig {
            id: ConsumerId(1),
            queue_id: QueueId(10),
            stream_id: StreamId(999),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 1000,
        });
        assert!(result.is_err());
    }

    #[test]
    fn full_catalog_lifecycle() {
        let mut cat = Catalog::new();

        cat.ensure_stream(StreamConfig {
            id: StreamId(1),
            name: b"messages".to_vec(),
        })
        .unwrap();

        cat.ensure_consumer(ConsumerConfig {
            id: ConsumerId(10),
            queue_id: QueueId(100),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 10_000,
        })
        .unwrap();

        cat.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(20),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(10),
            filters: vec![b"message.meta.>".to_vec(), b"message.qr.>".to_vec()],
        })
        .unwrap();

        let mt = cat.match_table(StreamId(1)).unwrap();
        assert_eq!(mt.pattern_count(), 2);
    }

    #[test]
    fn catch_all_subscription() {
        let mut cat = Catalog::new();

        cat.ensure_stream(StreamConfig {
            id: StreamId(1),
            name: b"all".to_vec(),
        })
        .unwrap();
        cat.ensure_consumer(ConsumerConfig {
            id: ConsumerId(1),
            queue_id: QueueId(1),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 100,
        })
        .unwrap();
        cat.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(1),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(1),
            filters: vec![],
        })
        .unwrap();

        let mt = cat.match_table(StreamId(1)).unwrap();
        assert_eq!(mt.catch_all_count(), 1);
        let result = mt.lookup(0xABCD);
        assert_eq!(result.count(), 1);
    }

    #[test]
    fn subscribe_creates_binding_and_demand() {
        let mut cat = Catalog::new();
        let mut events = DeltaEvents::default();

        cat.ensure_stream(StreamConfig {
            id: StreamId(1),
            name: b"s".to_vec(),
        })
        .unwrap();
        cat.ensure_consumer(ConsumerConfig {
            id: ConsumerId(1),
            queue_id: QueueId(1),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 100,
        })
        .unwrap();
        cat.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(1),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(1),
            filters: vec![],
        })
        .unwrap();
        cat.open_connection(ConnectionId(42), NodeId(1));

        let bid = cat
            .subscribe(ConnectionId(42), SubscriptionId(1), &mut events)
            .unwrap();

        assert!(cat.has_demand(StreamId(1)));
        assert!(cat.has_any_demand());
        assert!(cat.binding(bid).is_some());
        assert_eq!(events.demand_became_available.len(), 1);
    }

    #[test]
    fn retire_binding_decrements_demand() {
        let mut cat = Catalog::new();
        let mut events = DeltaEvents::default();

        cat.ensure_stream(StreamConfig {
            id: StreamId(1),
            name: b"s".to_vec(),
        })
        .unwrap();
        cat.ensure_consumer(ConsumerConfig {
            id: ConsumerId(1),
            queue_id: QueueId(1),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 100,
        })
        .unwrap();
        cat.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(1),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(1),
            filters: vec![],
        })
        .unwrap();
        cat.open_connection(ConnectionId(42), NodeId(1));

        let bid = cat
            .subscribe(ConnectionId(42), SubscriptionId(1), &mut events)
            .unwrap();
        assert!(cat.has_demand(StreamId(1)));

        events = DeltaEvents::default();
        let retired = cat.retire_binding(bid, &mut events);
        assert!(retired.is_some());
        assert!(!cat.has_demand(StreamId(1)));
        assert_eq!(events.demand_became_idle.len(), 1);
        assert_eq!(events.bindings_retired.len(), 1);
    }

    #[test]
    fn fnv1a_deterministic() {
        assert_eq!(fnv1a_32(b"orders.created"), fnv1a_32(b"orders.created"));
        assert_ne!(fnv1a_32(b"orders.created"), fnv1a_32(b"orders.updated"));
    }
}
