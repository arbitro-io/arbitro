//! Bind/unbind — subscription ↔ connection edge management.
//!
//! Level 7 — depends on everything below + context.
//!
//! A binding connects a subscription to a connection (client session).
//! When a client binds, it creates a BindingNode registered in edge indexes.
//! When a client unbinds or disconnects, bindings are removed.

use crate::context::EngineContext;
use crate::types::*;
use crate::batch::{BindBatch, BindEntry};
use crate::reply::{RepOk, OperationKind};
use crate::graph::node::BindingNode;

/// Process a bind batch: create bindings between subscriptions and connections.
pub fn on_bind_batch(ctx: &mut EngineContext, batch: &BindBatch) -> RepOk<SlabKey> {
    let mut reply = RepOk::with_capacity(OperationKind::Bind, batch.entries.len());

    for entry in batch.entries {
        match create_binding(ctx, entry, batch.now) {
            Some(key) => reply.accept(key),
            None => reply.reject(),
        }
    }

    reply
}

/// Create a single binding.
fn create_binding(
    ctx: &mut EngineContext,
    entry: &BindEntry,
    now: Timestamp,
) -> Option<SlabKey> {
    // Resolve subscription to get consumer_id + stream_id
    let sub_key = ctx.catalog.subscription_key(entry.subscription_id).ok()?;
    let sub_node = ctx.graph.get_subscription(sub_key).ok()?;
    let consumer_id = sub_node.consumer_id;
    let stream_id = sub_node.stream_id;

    // Allocate binding ID
    let binding_id = BindingId(ctx.next_binding_id);
    ctx.next_binding_id += 1;

    let node = BindingNode {
        binding_id,
        connection_id: entry.connection_id,
        subscription_id: entry.subscription_id,
        consumer_id,
        created_at: now,
    };

    let key = ctx.graph.insert_binding(node);

    // Register in edge indexes
    ctx.edges.bindings_by_connection.insert(&entry.connection_id, key);
    ctx.edges.bindings_by_subscription.insert(&entry.subscription_id, key);
    ctx.edges.bindings_by_consumer.insert(&consumer_id, key);

    // Precompute connection_id in match table entries (hot path reads this)
    ctx.catalog.bind_subscription_connection(
        stream_id, entry.subscription_id, entry.connection_id,
    );

    Some(key)
}

/// Remove a single binding by slab key.
pub fn unbind(ctx: &mut EngineContext, binding_key: SlabKey) -> Option<BindingNode> {
    let node = ctx.graph.remove_binding(binding_key).ok()?;

    ctx.edges.bindings_by_connection.remove(&node.connection_id, &binding_key);
    ctx.edges.bindings_by_subscription.remove(&node.subscription_id, &binding_key);
    ctx.edges.bindings_by_consumer.remove(&node.consumer_id, &binding_key);

    // Clear precomputed connection_id in match table
    let sub_key = ctx.catalog.subscription_key(node.subscription_id).ok();
    if let Some(sk) = sub_key {
        if let Ok(sub) = ctx.graph.get_subscription(sk) {
            ctx.catalog.unbind_subscription_connection(sub.stream_id, node.subscription_id);
        }
    }

    Some(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{StreamConfig, ConsumerConfig, SubscriptionConfig};

    fn setup_with_subscription() -> EngineContext {
        let mut ctx = EngineContext::new();

        ctx.catalog.ensure_stream(&mut ctx.graph, StreamConfig {
            id: StreamId(1),
            name: b"test".to_vec(),
        }).unwrap();

        ctx.catalog.ensure_consumer(&mut ctx.graph, &mut ctx.edges, ConsumerConfig {
            id: ConsumerId(10),
            queue_id: QueueId(100),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_inflight: 1000,
        }).unwrap();

        ctx.catalog.ensure_subscription(&mut ctx.graph, &mut ctx.edges, SubscriptionConfig {
            id: SubscriptionId(20),
            stream_id: StreamId(1),
            consumer_id: ConsumerId(10),
            filters: vec![],
        }).unwrap();

        ctx
    }

    #[test]
    fn bind_creates_binding_with_edges() {
        let mut ctx = setup_with_subscription();
        let conn = ConnectionId(500);

        let entries = [BindEntry {
            connection_id: conn,
            subscription_id: SubscriptionId(20),
        }];
        let batch = BindBatch {
            entries: &entries,
            now: Timestamp::new(0),
        };

        let reply = on_bind_batch(&mut ctx, &batch);
        assert_eq!(reply.accepted, 1);

        // Edges registered
        assert_eq!(ctx.edges.bindings_by_connection.get(&conn).len(), 1);
        assert_eq!(ctx.edges.bindings_by_subscription.get(&SubscriptionId(20)).len(), 1);
        assert_eq!(ctx.edges.bindings_by_consumer.get(&ConsumerId(10)).len(), 1);
    }

    #[test]
    fn unbind_removes_all_edges() {
        let mut ctx = setup_with_subscription();
        let conn = ConnectionId(500);

        let entries = [BindEntry {
            connection_id: conn,
            subscription_id: SubscriptionId(20),
        }];
        let batch = BindBatch {
            entries: &entries,
            now: Timestamp::new(0),
        };

        let reply = on_bind_batch(&mut ctx, &batch);
        let binding_key = reply.entries[0];

        let removed = unbind(&mut ctx, binding_key);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().connection_id, conn);

        assert!(ctx.edges.bindings_by_connection.get(&conn).is_empty());
    }

    #[test]
    fn bind_invalid_subscription_rejects() {
        let mut ctx = EngineContext::new();

        let entries = [BindEntry {
            connection_id: ConnectionId(1),
            subscription_id: SubscriptionId(999), // doesn't exist
        }];
        let batch = BindBatch {
            entries: &entries,
            now: Timestamp::new(0),
        };

        let reply = on_bind_batch(&mut ctx, &batch);
        assert_eq!(reply.rejected, 1);
        assert_eq!(reply.accepted, 0);
    }
}
