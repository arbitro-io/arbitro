//! Admin operations — connection management, pause, limits.
//!
//! Level 7 — management path only.
//!
//! Admin operations are called infrequently (per-session, per-config).
//! Allocations and HashMap lookups are acceptable here.

use crate::context::EngineContext;
use crate::types::*;
use crate::batch::OpenConnectionReq;
use crate::graph::node::ConnectionNode;

/// Open a new connection. Management path.
pub fn open_connection(ctx: &mut EngineContext, req: &OpenConnectionReq) -> SlabKey {
    let node = ConnectionNode {
        connection_id: req.connection_id,
        node_id: req.node_id,
        opened_at: req.now,
    };

    let key = ctx.graph.insert_connection(node);
    ctx.edges.connections_by_node.insert(&req.node_id, key);
    ctx.connection_keys.insert(req.connection_id, key);

    key
}

/// Pause a consumer. Management path.
pub fn pause_consumer(ctx: &mut EngineContext, consumer_id: ConsumerId) -> bool {
    let key = match ctx.catalog.consumer_key(consumer_id) {
        Ok(k) => k,
        Err(_) => return false,
    };
    match ctx.graph.get_consumer_mut(key) {
        Ok(node) => {
            node.paused = true;
            true
        }
        Err(_) => false,
    }
}

/// Resume a consumer. Management path.
pub fn resume_consumer(ctx: &mut EngineContext, consumer_id: ConsumerId) -> bool {
    let key = match ctx.catalog.consumer_key(consumer_id) {
        Ok(k) => k,
        Err(_) => return false,
    };
    match ctx.graph.get_consumer_mut(key) {
        Ok(node) => {
            node.paused = false;
            true
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{StreamConfig, ConsumerConfig};

    fn setup_ctx() -> EngineContext {
        EngineContext::new()
    }

    #[test]
    fn open_connection_registers_edge() {
        let mut ctx = setup_ctx();

        let req = OpenConnectionReq {
            connection_id: ConnectionId(42),
            node_id: NodeId(1),
            now: Timestamp::new(0),
        };

        let key = open_connection(&mut ctx, &req);
        let conn = ctx.graph.get_connection(key).unwrap();
        assert_eq!(conn.connection_id, ConnectionId(42));

        // Edge registered
        let conns = ctx.edges.connections_by_node.get(&NodeId(1));
        assert_eq!(conns.len(), 1);
    }

    #[test]
    fn pause_resume_consumer() {
        let mut ctx = setup_ctx();

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

        // Pause
        assert!(pause_consumer(&mut ctx, ConsumerId(10)));
        let key = ctx.catalog.consumer_key(ConsumerId(10)).unwrap();
        assert!(ctx.graph.get_consumer(key).unwrap().paused);

        // Resume
        assert!(resume_consumer(&mut ctx, ConsumerId(10)));
        assert!(!ctx.graph.get_consumer(key).unwrap().paused);

        // Non-existent consumer
        assert!(!pause_consumer(&mut ctx, ConsumerId(999)));
    }
}
