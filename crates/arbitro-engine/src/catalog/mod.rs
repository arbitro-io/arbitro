//! Catalog — entity lifecycle, match tables, binding management, demand.
//!
//! Level 5 — depends on Level 0-4.
//!
//! Stores streams, consumers, subscriptions, and bindings directly — no
//! external graph or edge dependency. Bindings use 3 secondary indices
//! (`by_stream`, `by_consumer`, `by_connection`) for O(1) retire lookups.

pub mod match_table;

use std::collections::HashMap;

use crate::common::wire_hash_32;
use crate::error::{EngineError, EngineResult};
use crate::events::DeltaEvents;
use crate::types::*;
use match_table::{MatchEntry, MatchTable};

/// Upper bound on stream/consumer ids. Guards `ensure_*_slot` against a
/// caller-supplied id triggering a huge `Vec::resize_with` allocation
/// (SEC-4) — e.g. a malformed id of `u32::MAX` would otherwise attempt a
/// multi-GB allocation.
pub const MAX_ENTITY_ID: u32 = 65536;

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
    /// Ack deadline in milliseconds. 0 = no timeout (no wheel entry).
    pub ack_wait_ms: u32,
    /// Maximum nack count before a message is moved to the DLQ stream.
    /// 0 = DLQ disabled (default). Messages nacked more than `max_nack`
    /// times are published to `{stream_name}.dlq` and acked from the
    /// original stream.
    pub max_nack: u32,
    /// Subject filter declared at consumer-creation time. Empty = no
    /// filter (the consumer accepts whatever its subscriptions accept).
    ///
    /// `Box<[u8]>` and not `Vec<u8>`: it is written once at creation and
    /// never grows, so the capacity word would be dead weight.
    ///
    /// Consumed on the **management path only** — folded into the stream's
    /// match table when a subscription is created. It is never read
    /// per-message: `code-anti-patterns.md` bans runtime subject filter
    /// evaluation on the hot path, and `Binding` deliberately does not
    /// carry it.
    pub filter: Box<[u8]>,
}

/// Configuration for creating a subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    pub id: SubscriptionId,
    /// The id the CLIENT chose, unique only within its connection. Rides
    /// every delivery and comes back on every ack: the client cannot
    /// recognise `id`, which the broker assigns and never tells it.
    pub external_id: u32,
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
    /// Ack deadline in milliseconds. 0 = no timeout.
    pub ack_wait_ms: u32,
    /// Maximum nack count before DLQ. 0 = DLQ disabled.
    pub max_nack: u32,
    /// Subject filter declared at creation. Empty = no filter. See
    /// [`ConsumerConfig::filter`] for why it lives here and not on
    /// [`Binding`].
    pub filter: Box<[u8]>,
}

/// Subscription metadata.
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    pub stream_id: StreamId,
    pub consumer_id: ConsumerId,
    pub external_id: u32,
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
    /// Number of times this entry has been (re)delivered. Incremented on
    /// each nack-driven redelivery; compared against `ConsumerInfo::max_nack`.
    pub deliveries: u16,
    pub _pad: u16,
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
    /// See [`SubscriptionConfig::external_id`]. What the drain stamps on
    /// deliveries and what the ack index is keyed by.
    pub external_sub_id: u32,
    pub queue_id: QueueId,
    pub max_inflight: u32,
    pub paused: bool,
    pub fire_and_forget: bool,
    /// In-flight messages awaiting ack, keyed by seq. O(1) insert, lookup,
    /// and remove on ack/nack — replaces the old `Vec<Pending>` linear
    /// scan + parallel `HashSet<u64>` dedup index (PERF-1).
    pub pending: HashMap<u64, Pending, foldhash::fast::FixedState>,
}

impl Binding {
    /// O(1) pending lookup for wheel-tick / ack-timeout path.
    #[inline]
    pub fn is_pending(&self, seq: u64) -> bool {
        self.pending.contains_key(&seq)
    }
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
    // Entity storage — Vec<Option<..>> for dense monotonic IDs (O(1) index).
    streams: Vec<Option<StreamInfo>>,
    consumers: Vec<Option<ConsumerInfo>>,
    subscriptions: HashMap<SubscriptionId, SubscriptionInfo, foldhash::fast::FixedState>,

    // Bindings with 3 secondary indices.
    bindings: HashMap<BindingId, Binding, foldhash::fast::FixedState>,
    by_stream: HashMap<StreamId, Vec<BindingId>, foldhash::fast::FixedState>,
    by_consumer: HashMap<ConsumerId, Vec<BindingId>, foldhash::fast::FixedState>,
    by_connection: HashMap<ConnectionId, Vec<BindingId>, foldhash::fast::FixedState>,
    /// ROB-13: at most one active binding per subscription. Consulted by
    /// `subscribe` to replace a stale binding cleanly instead of leaving
    /// two live bindings racing for the same subscription's deliveries.
    by_subscription: HashMap<SubscriptionId, BindingId, foldhash::fast::FixedState>,
    /// `(connection, subscription) → binding`, for callers holding an id
    /// that arrived in a client-authored frame (acks, nacks).
    ///
    /// The connection belongs in the KEY, not in a check after the lookup:
    /// a subscription id from another connection has to MISS, not resolve
    /// to a stranger's binding and get rejected afterwards. Within one
    /// connection the id is a plain counter, so the pair is unique without
    /// the consumer.
    ///
    /// Maintained in exactly the two places `by_subscription` is — the
    /// insert in `subscribe` and the remove in `retire_binding`, which is
    /// the only path that ever drops a binding.
    by_conn_subscription: HashMap<(ConnectionId, u32), BindingId, foldhash::fast::FixedState>,

    /// `(connection, consumer) → live binding count`, for frames that name
    /// no subscription (`AckBatchReq`'s bare seqs, nacks) and can only be
    /// authorized at consumer granularity. A count, not a flag: one
    /// connection may hold several subscriptions on the same consumer, and
    /// retiring one of them must not revoke the others.
    ///
    /// Maintained alongside `by_conn_subscription`.
    by_conn_consumer: HashMap<(ConnectionId, ConsumerId), u32, foldhash::fast::FixedState>,
    next_binding_id: u32,

    // Connection tracking.
    connections: HashMap<ConnectionId, NodeId, foldhash::fast::FixedState>,

    // Demand counters: streams with ≥1 active binding.
    demand: HashMap<StreamId, u32, foldhash::fast::FixedState>,

    // Per-stream match tables.
    match_tables: Vec<Option<MatchTable>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            streams: Vec::with_capacity(16),
            consumers: Vec::with_capacity(16),
            subscriptions: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            bindings: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            by_stream: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            by_consumer: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            by_connection: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            by_subscription: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            by_conn_subscription: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            by_conn_consumer: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            next_binding_id: 1,
            connections: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            demand: HashMap::with_hasher(foldhash::fast::FixedState::default()),
            match_tables: Vec::with_capacity(16),
        }
    }

    /// SEC-4: reject ids beyond `MAX_ENTITY_ID` before resizing — an
    /// unbounded id would otherwise drive `Vec::resize_with` to attempt a
    /// huge allocation.
    #[inline(always)]
    fn ensure_stream_slot(&mut self, id: StreamId) -> EngineResult<()> {
        if id.0 > MAX_ENTITY_ID {
            return Err(EngineError::entity_id_too_large());
        }
        let idx = id.0 as usize;
        if idx >= self.streams.len() {
            self.streams.resize_with(idx + 4, || None);
        }
        Ok(())
    }

    #[inline(always)]
    fn ensure_consumer_slot(&mut self, id: ConsumerId) {
        let idx = id.0 as usize;
        if idx >= self.consumers.len() {
            self.consumers.resize_with(idx + 4, || None);
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
        self.ensure_stream_slot(config.id)?;
        if self.streams[config.id.0 as usize].is_some() {
            return Ok(());
        }
        self.streams[config.id.0 as usize] = Some(StreamInfo { name: config.name });
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
        match self.streams.get_mut(id.0 as usize).and_then(|s| s.take()) {
            Some(_) => {}
            None => return Err(EngineError::stream_not_found()),
        }
        if let Some(slot) = self.match_tables.get_mut(id.0 as usize) {
            *slot = None;
        }
        Ok(())
    }

    /// Stream exists?
    #[inline]
    pub fn stream_exists(&self, id: StreamId) -> bool {
        self.streams
            .get(id.0 as usize)
            .and_then(|s| s.as_ref())
            .is_some()
    }

    // ── Consumer ────────────────────────────────────────────────────────

    /// Create or ensure a consumer exists.
    ///
    /// GAP-3 fix: if the consumer already exists, compare the mutable
    /// config fields (`max_inflight`, `ack_policy`, `ack_wait_ms`). A
    /// mismatch returns `ConsumerConfigMismatch` so the caller knows
    /// the create was NOT idempotent — delete + recreate is required.
    /// Same-config re-creation still returns `Ok(())` (idempotent).
    pub fn ensure_consumer(&mut self, config: ConsumerConfig) -> EngineResult<bool> {
        if !self.stream_exists(config.stream_id) {
            return Err(EngineError::stream_not_found());
        }
        self.ensure_consumer_slot(config.id);
        if let Some(existing) = &self.consumers[config.id.0 as usize] {
            // Same-config re-creation is idempotent.
            if existing.max_inflight == config.max_inflight
                && existing.ack_policy == config.ack_policy
                && existing.ack_wait_ms == config.ack_wait_ms
                && existing.stream_id == config.stream_id
                && existing.queue_id == config.queue_id
                // The filter is routing configuration: re-creating a
                // consumer under a different one must NOT be treated as
                // idempotent, or the second caller would silently inherit
                // the first one's routing.
                && existing.filter == config.filter
            {
                return Ok(false); // already existed, same config
            }
            return Err(EngineError::consumer_config_mismatch());
        }
        self.consumers[config.id.0 as usize] = Some(ConsumerInfo {
            stream_id: config.stream_id,
            queue_id: config.queue_id,
            max_inflight: config.max_inflight,
            paused: false,
            ack_policy: config.ack_policy,
            durable: config.durable,
            ack_wait_ms: config.ack_wait_ms,
            max_nack: config.max_nack,
            filter: config.filter,
        });
        Ok(true) // newly created
    }

    /// Remove a consumer entity. Does NOT cascade — caller retires
    /// bindings and subscriptions first.
    pub fn remove_consumer_entity(&mut self, id: ConsumerId) -> EngineResult<()> {
        self.consumers
            .get_mut(id.0 as usize)
            .and_then(|s| s.take())
            .ok_or_else(EngineError::consumer_not_found)?;
        Ok(())
    }

    /// Get consumer info.
    #[inline]
    pub fn consumer(&self, id: ConsumerId) -> Option<&ConsumerInfo> {
        self.consumers.get(id.0 as usize).and_then(|s| s.as_ref())
    }

    /// Is the consumer paused?
    #[inline]
    pub fn is_paused(&self, id: ConsumerId) -> bool {
        self.consumers
            .get(id.0 as usize)
            .and_then(|s| s.as_ref())
            .map(|c| c.paused)
            .unwrap_or(false)
    }

    /// Pause a consumer.
    pub fn pause_consumer(&mut self, id: ConsumerId) -> bool {
        if let Some(info) = self
            .consumers
            .get_mut(id.0 as usize)
            .and_then(|s| s.as_mut())
        {
            info.paused = true;
            true
        } else {
            false
        }
    }

    /// Resume a consumer.
    pub fn resume_consumer(&mut self, id: ConsumerId) -> bool {
        if let Some(info) = self
            .consumers
            .get_mut(id.0 as usize)
            .and_then(|s| s.as_mut())
        {
            info.paused = false;
            true
        } else {
            false
        }
    }

    // ── Subscription ────────────────────────────────────────────────────

    /// Create or ensure a subscription exists. Updates match table.
    pub fn ensure_subscription(&mut self, config: SubscriptionConfig) -> EngineResult<()> {
        if !self.stream_exists(config.stream_id) {
            return Err(EngineError::stream_not_found());
        }
        let consumer = self
            .consumers
            .get(config.consumer_id.0 as usize)
            .and_then(|s| s.as_ref())
            .ok_or_else(EngineError::consumer_not_found)?;
        let queue_id = consumer.queue_id;

        if self.subscriptions.contains_key(&config.id) {
            return Ok(());
        }

        // The filters arrive already resolved: `transport::rules` inherits
        // the consumer's slice when none is declared and rejects anything
        // reaching outside it. The engine's catalog is sharded and cannot
        // see the whole picture, so admission is decided above it.
        let filters: Vec<Vec<u8>> = config.filters.clone();

        self.subscriptions.insert(
            config.id,
            SubscriptionInfo {
                stream_id: config.stream_id,
                consumer_id: config.consumer_id,
                external_id: config.external_id,
                filters: filters.clone(),
            },
        );

        // Update match table.
        // `binding_idx` is stamped by the server's `rebuild_and_swap_snapshot`
        // when it clones the match table into the drain's snapshot — see
        // `crates/arbitro-server/src/shard/worker.rs`. Engine catalog
        // itself stays server-agnostic and stores the unbound sentinel.
        let match_entry = MatchEntry {
            consumer_id: config.consumer_id,
            queue_id,
            subscription_id: config.id,
            connection_id: ConnectionId(0), // set at bind time
            binding_idx: crate::catalog::match_table::BINDING_IDX_UNBOUND,
        };

        self.ensure_match_table_slot(config.stream_id);
        let mt = self.match_tables[config.stream_id.0 as usize].get_or_insert_with(MatchTable::new);
        if filters.is_empty() {
            mt.add_catch_all(match_entry);
        } else {
            for filter in &filters {
                if filter.contains(&b'*') || filter.contains(&b'>') {
                    mt.add_pattern(filter.clone(), match_entry);
                } else {
                    let hash = wire_hash_32(filter);
                    mt.add_exact(hash, filter, match_entry);
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
    ///
    /// ROB-13: a subscription may have at most one active binding. If one
    /// already exists, it's retired cleanly (releasing its inflight and
    /// match-table state) before the new binding is created — no window
    /// where two bindings race for the same subscription's deliveries.
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
            .get(sub.consumer_id.0 as usize)
            .and_then(|s| s.as_ref())
            .ok_or_else(EngineError::consumer_not_found)?;

        let stream_id = sub.stream_id;
        let consumer_id = sub.consumer_id;
        let external_sub_id = sub.external_id;
        let queue_id = consumer.queue_id;
        let max_inflight = consumer.max_inflight;
        let paused = consumer.paused;
        let fire_and_forget = consumer.ack_policy == AckPolicy::None;

        if let Some(&old_binding_id) = self.by_subscription.get(&subscription_id) {
            self.retire_binding(old_binding_id, events);
        }

        let binding_id = BindingId(self.next_binding_id);
        self.next_binding_id += 1;

        let binding = Binding {
            binding_id,
            stream_id,
            consumer_id,
            connection_id,
            subscription_id,
            external_sub_id,
            queue_id,
            max_inflight,
            paused,
            fire_and_forget,
            pending: HashMap::with_hasher(foldhash::fast::FixedState::default()),
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
        self.by_subscription.insert(subscription_id, binding_id);
        self.by_conn_subscription
            .insert((connection_id, external_sub_id), binding_id);
        *self
            .by_conn_consumer
            .entry((connection_id, consumer_id))
            .or_insert(0) += 1;

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
        // Only clear the subscription index if it still points at this
        // binding — a replacement binding may have already overwritten it.
        if self.by_subscription.get(&binding.subscription_id) == Some(&binding_id) {
            self.by_subscription.remove(&binding.subscription_id);
        }
        // Same guard: a replacement binding on the same (conn, sub) may
        // already own the slot, and retiring the old one must not evict it.
        let conn_sub = (binding.connection_id, binding.external_sub_id);
        if self.by_conn_subscription.get(&conn_sub) == Some(&binding_id) {
            self.by_conn_subscription.remove(&conn_sub);
        }
        // Unconditional: the count tracks bindings, not slot ownership, so
        // every retire owes exactly one decrement. Drop the entry at zero
        // so the map does not grow with dead connections.
        let conn_consumer = (binding.connection_id, binding.consumer_id);
        if let Some(n) = self.by_conn_consumer.get_mut(&conn_consumer) {
            *n -= 1;
            if *n == 0 {
                self.by_conn_consumer.remove(&conn_consumer);
            }
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

    /// Live entries in the `(conn, sub)` index. Tests assert on the SIZE,
    /// not on `binding_for_subscription` returning `None` — a leaked entry
    /// pointing at a retired binding also returns `None`, so the behaviour
    /// check cannot tell a clean index from one that grows forever.
    #[cfg(test)]
    pub fn conn_subscription_index_len(&self) -> usize {
        self.by_conn_subscription.len()
    }

    /// Whether `conn` holds any live binding on `consumer`. The coarse
    /// check, for frames that carry no subscription id — one hash instead
    /// of a walk over every binding in the shard.
    #[inline]
    pub fn connection_owns_consumer(&self, conn: ConnectionId, consumer: ConsumerId) -> bool {
        self.by_conn_consumer.contains_key(&(conn, consumer))
    }

    /// Live entries in the `(conn, consumer)` index. Same reason as
    /// `conn_subscription_index_len`: assert on the size, not on behaviour.
    #[cfg(test)]
    pub fn conn_consumer_index_len(&self) -> usize {
        self.by_conn_consumer.len()
    }

    /// Just the id, for callers that need to borrow the binding mutably
    /// afterwards — the index borrow ends before `binding_mut` starts.
    #[inline]
    pub fn binding_id_for_subscription(&self, conn: ConnectionId, sub: u32) -> Option<BindingId> {
        self.by_conn_subscription.get(&(conn, sub)).copied()
    }

    /// The binding of `sub` on `conn`. One hash into the index, one into
    /// the store — no scan, constant whatever the consumer's fan-out.
    #[inline]
    pub fn binding_for_subscription(&self, conn: ConnectionId, sub: u32) -> Option<&Binding> {
        let bid = *self.by_conn_subscription.get(&(conn, sub))?;
        self.bindings.get(&bid)
    }

    /// Mutable twin — what the ack path needs to take the pending out.
    #[inline]
    pub fn binding_for_subscription_mut(&mut self, conn: ConnectionId, sub: u32) -> Option<&mut Binding> {
        let bid = *self.by_conn_subscription.get(&(conn, sub))?;
        self.bindings.get_mut(&bid)
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

    /// Clone all match tables. Used by command thread to build DrainSnapshot.
    pub fn clone_match_tables(&self) -> Vec<Option<MatchTable>> {
        self.match_tables.clone()
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
        if !self.stream_exists(stream_id) {
            return Err(EngineError::stream_not_found());
        }
        self.ensure_match_table_slot(stream_id);
        let mt = self.match_tables[stream_id.0 as usize].get_or_insert_with(MatchTable::new);
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
        self.streams
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| opt.as_ref().map(|_| StreamId(i as u32)))
            .collect()
    }

    /// All consumer IDs.
    pub fn consumer_ids(&self) -> Vec<ConsumerId> {
        self.consumers
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| opt.as_ref().map(|_| ConsumerId(i as u32)))
            .collect()
    }

    /// List all streams with names.
    pub fn list_streams(&self) -> Vec<(StreamId, Vec<u8>)> {
        self.streams
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| {
                opt.as_ref()
                    .map(|info| (StreamId(i as u32), info.name.clone()))
            })
            .collect()
    }

    /// List all consumers.
    pub fn list_consumers(&self) -> Vec<(ConsumerId, StreamId, QueueId, bool)> {
        self.consumers
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| {
                opt.as_ref().map(|info| {
                    (
                        ConsumerId(i as u32),
                        info.stream_id,
                        info.queue_id,
                        info.paused,
                    )
                })
            })
            .collect()
    }

    /// List the `ConsumerId`s of every consumer attached to `stream_id`.
    /// Used by `engine.delete_stream` to cascade-remove consumers when
    /// their owning stream is deleted — without this, consumer entities
    /// (and their NameRegistry mappings) leak past the stream's
    /// lifetime and a same-named recreate on a fresh stream silently
    /// aliases to a defunct id.
    pub fn consumers_for_stream(&self, stream_id: StreamId) -> Vec<ConsumerId> {
        self.consumers
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| {
                opt.as_ref().and_then(|info| {
                    if info.stream_id == stream_id {
                        Some(ConsumerId(i as u32))
                    } else {
                        None
                    }
                })
            })
            .collect()
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
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
            ack_wait_ms: 0,
            max_nack: 0,
            filter: Box::default(),
        });
        assert!(result.is_err());
    }

    // ── Consumer subject filter ─────────────────────────────────────────
    //
    // The filter travelled from the client and was persisted to the
    // metadata log, but was dropped before reaching the engine: it existed
    // on disk and in no live structure. These pin that it is now stored,
    // and that it participates in the same-config contract.

    /// Builds a consumer on stream 1 that differs only in `filter`.
    fn consumer_with_filter(id: u32, filter: &[u8]) -> ConsumerConfig {
        ConsumerConfig {
            id: ConsumerId(id),
            queue_id: QueueId(10),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 100,
            ack_wait_ms: 0,
            max_nack: 0,
            filter: Box::from(filter),
        }
    }

    fn catalog_with_stream() -> Catalog {
        let mut cat = Catalog::new();
        cat.ensure_stream(StreamConfig {
            id: StreamId(1),
            name: b"orders".to_vec(),
        })
        .expect("stream");
        cat
    }

    #[test]
    fn consumer_filter_is_stored_verbatim() {
        let mut cat = catalog_with_stream();
        assert!(cat
            .ensure_consumer(consumer_with_filter(1, b"orders.premium.>"))
            .expect("create"));

        assert_eq!(
            &*cat.consumer(ConsumerId(1)).expect("consumer exists").filter,
            b"orders.premium.>",
            "the filter must survive into live catalog state"
        );
    }

    #[test]
    fn absent_filter_is_stored_empty_not_wildcard() {
        // Empty means "no filter declared" — it must NOT be normalised to
        // b">" here. Whether an absent filter widens or narrows is a
        // decision for the fold into the match table, not for storage.
        let mut cat = catalog_with_stream();
        cat.ensure_consumer(consumer_with_filter(1, b""))
            .expect("create");
        assert!(cat.consumer(ConsumerId(1)).expect("exists").filter.is_empty());
    }

    #[test]
    fn recreating_with_the_same_filter_is_idempotent() {
        let mut cat = catalog_with_stream();
        assert!(cat
            .ensure_consumer(consumer_with_filter(1, b"orders.premium.>"))
            .expect("first create"));
        assert!(
            !cat.ensure_consumer(consumer_with_filter(1, b"orders.premium.>"))
                .expect("same config must be idempotent"),
            "second create must report `already existed`, not `created`"
        );
    }

    #[test]
    fn recreating_with_a_different_filter_is_a_config_mismatch() {
        let mut cat = catalog_with_stream();
        cat.ensure_consumer(consumer_with_filter(1, b"orders.premium.>"))
            .expect("first create");

        let err = cat
            .ensure_consumer(consumer_with_filter(1, b"orders.basic.>"))
            .expect_err("a different filter is a different config");
        assert_eq!(err.code(), crate::error::ErrorCode::ConsumerConfigMismatch);

        // The rejected create must not have overwritten the stored filter.
        assert_eq!(
            &*cat.consumer(ConsumerId(1)).expect("exists").filter,
            b"orders.premium.>",
            "a rejected re-create must leave the original routing intact"
        );
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
            ack_wait_ms: 0,
            max_nack: 0,
            filter: Box::default(),
        })
        .unwrap();

        cat.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(20),
            external_id: 20,
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
            ack_wait_ms: 0,
            max_nack: 0,
            filter: Box::default(),
        })
        .unwrap();
        cat.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(1),
            external_id: 1,
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
            ack_wait_ms: 0,
            max_nack: 0,
            filter: Box::default(),
        })
        .unwrap();
        cat.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(1),
            external_id: 1,
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
            ack_wait_ms: 0,
            max_nack: 0,
            filter: Box::default(),
        })
        .unwrap();
        cat.ensure_subscription(SubscriptionConfig {
            id: SubscriptionId(1),
            external_id: 1,
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
        assert_eq!(
            wire_hash_32(b"orders.created"),
            wire_hash_32(b"orders.created")
        );
        assert_ne!(
            wire_hash_32(b"orders.created"),
            wire_hash_32(b"orders.updated")
        );
    }

    /// Every path that drops a binding must also drop its `(conn, sub)`
    /// entry. `retire_binding` is the only place `bindings` shrinks, and the
    /// bulk helpers (stream / consumer / connection) all funnel through it —
    /// this pins that, so a future path that bypasses it fails here instead
    /// of leaking an entry per subscribe until the process dies.
    #[test]
    fn conn_subscription_index_is_cleared_on_every_retire_path() {
        fn setup() -> (Catalog, DeltaEvents) {
            let mut cat = Catalog::new();
            let events = DeltaEvents::default();
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
                ack_wait_ms: 0,
                max_nack: 0,
                filter: Box::default(),
            })
            .unwrap();
            cat.ensure_subscription(SubscriptionConfig {
                id: SubscriptionId(1),
                external_id: 1,
                stream_id: StreamId(1),
                consumer_id: ConsumerId(1),
                filters: vec![],
            })
            .unwrap();
            cat.open_connection(ConnectionId(42), NodeId(1));
            (cat, events)
        }

        // Direct retire.
        let (mut cat, mut ev) = setup();
        let bid = cat
            .subscribe(ConnectionId(42), SubscriptionId(1), &mut ev)
            .unwrap();
        assert_eq!(cat.conn_subscription_index_len(), 1);
        assert!(cat
            .binding_for_subscription(ConnectionId(42), 1)
            .is_some());
        cat.retire_binding(bid, &mut ev);
        assert_eq!(cat.conn_subscription_index_len(), 0, "direct retire leaked");
        assert!(cat
            .binding_for_subscription(ConnectionId(42), 1)
            .is_none());

        // Re-subscribe: the old binding is retired while the new one already
        // owns the slot. The guard must keep the replacement, not evict it.
        let (mut cat, mut ev) = setup();
        cat.subscribe(ConnectionId(42), SubscriptionId(1), &mut ev)
            .unwrap();
        let second = cat
            .subscribe(ConnectionId(42), SubscriptionId(1), &mut ev)
            .unwrap();
        assert_eq!(
            cat.conn_subscription_index_len(),
            1,
            "re-subscribe must replace, not accumulate"
        );
        assert_eq!(
            cat.binding_for_subscription(ConnectionId(42), 1)
                .map(|b| b.binding_id),
            Some(second),
            "the live binding is the replacement"
        );

        // Removing the subscription ENTITY deliberately does not cascade —
        // `remove_subscription_entity` documents that the caller retires the
        // bindings first. So the binding outlives it and the index entry
        // still points at something live; that is the contract, not a leak.
        // What must hold is that retiring afterwards still clears it.
        let (mut cat, mut ev) = setup();
        let bid = cat
            .subscribe(ConnectionId(42), SubscriptionId(1), &mut ev)
            .unwrap();
        cat.remove_subscription_entity(SubscriptionId(1)).unwrap();
        assert_eq!(
            cat.conn_subscription_index_len(),
            1,
            "the binding outlives the subscription entity by design"
        );
        cat.retire_binding(bid, &mut ev);
        assert_eq!(
            cat.conn_subscription_index_len(),
            0,
            "retiring after the entity is gone must still clear the index"
        );
    }

    /// The `(conn, consumer)` index counts bindings, so several
    /// subscriptions of one connection on one consumer must not revoke each
    /// other on retire. A flag instead of a count would drop ownership on
    /// the first unsubscribe and start rejecting the survivors' nacks.
    #[test]
    fn conn_consumer_index_survives_partial_unsubscribe() {
        let mut cat = Catalog::new();
        let mut ev = DeltaEvents::default();
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
            ack_wait_ms: 0,
            max_nack: 0,
            filter: Box::default(),
        })
        .unwrap();
        for id in [1u32, 2] {
            cat.ensure_subscription(SubscriptionConfig {
                id: SubscriptionId(id),
                external_id: id,
                stream_id: StreamId(1),
                consumer_id: ConsumerId(1),
                filters: vec![],
            })
            .unwrap();
        }
        cat.open_connection(ConnectionId(42), NodeId(1));

        let first = cat
            .subscribe(ConnectionId(42), SubscriptionId(1), &mut ev)
            .unwrap();
        let second = cat
            .subscribe(ConnectionId(42), SubscriptionId(2), &mut ev)
            .unwrap();
        assert_eq!(cat.conn_consumer_index_len(), 1, "one pair, not one per sub");
        assert!(cat.connection_owns_consumer(ConnectionId(42), ConsumerId(1)));

        cat.retire_binding(first, &mut ev);
        assert!(
            cat.connection_owns_consumer(ConnectionId(42), ConsumerId(1)),
            "one subscription left — the connection still owns the consumer"
        );

        cat.retire_binding(second, &mut ev);
        assert!(!cat.connection_owns_consumer(ConnectionId(42), ConsumerId(1)));
        assert_eq!(cat.conn_consumer_index_len(), 0, "last retire must drop the entry");

        // A stranger never owned it.
        assert!(!cat.connection_owns_consumer(ConnectionId(99), ConsumerId(1)));
    }
}
