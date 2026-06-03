//! Raft state machine for Arbitro cluster metadata operations.
//!
//! `apply()` deserializes each committed log entry into a `ClusterCommand`
//! and executes it on the local `ShardRouter` — creating or deleting
//! streams/consumers on the engine, mirroring what the v2 dispatch does
//! on the leader.  Because `StateMachine::apply` takes `&mut self` (not
//! async), we use `tokio::runtime::Handle::current().block_on()` for the
//! async shard calls.

use std::sync::Arc;

use arbitro_raft::{RaftError, StateMachine};
use serde::{Deserialize, Serialize};

use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig, wire_hash_32};
use arbitro_engine_v2::types::*;

use crate::shard::router::ShardRouter;

/// Commands replicated through Raft for cluster-wide metadata consistency.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClusterCommand {
    CreateStream {
        name: String,
        filter: String,
        max_msgs: u64,
        max_bytes: u64,
        max_age_secs: u64,
        replicas: u8,
        journal_kind: u8,
        retention: u8,
        discard: u8,
        idempotency_window_ms: u32,
    },
    DeleteStream {
        name: String,
    },
    CreateConsumer {
        stream_name: String,
        name: String,
        group: String,
        filter: String,
        max_inflight: u16,
        ack_policy: u8,
        deliver_policy: u8,
        deliver_mode: u8,
        ack_wait_ms: u32,
        start_seq: u64,
    },
    DeleteConsumer {
        stream_name: String,
        name: String,
    },
}

/// State machine that executes cluster commands on the local engine.
///
/// Holds an `Arc<ShardRouter>` so it can create/delete streams and
/// consumers when Raft entries are committed.  The `applied` log is kept
/// for snapshot/restore.
pub struct ArbitroStateMachine {
    applied: Vec<ClusterCommand>,
    router: Option<Arc<ShardRouter>>,
}

impl ArbitroStateMachine {
    /// Create a state machine without a router (used during early init
    /// before the `ShardRouter` is available).
    pub fn new() -> Self {
        Self {
            applied: Vec::new(),
            router: None,
        }
    }

    /// Wire the shard router so `apply()` actually executes commands.
    pub fn set_router(&mut self, router: Arc<ShardRouter>) {
        self.router = Some(router);
    }

    // ── Internal helpers ─────────────────────────────────────────────

    fn apply_create_stream(
        router: &ShardRouter,
        name: &str,
        _filter: &str,
        max_msgs: u64,
        max_bytes: u64,
        max_age_secs: u64,
        idempotency_window_ms: u32,
    ) {
        let name_bytes = name.as_bytes();
        let wire_stream = wire_hash_32(name_bytes);
        let (seq_stream, _created) =
            router.names().get_or_create_stream_named(wire_stream, name_bytes);

        // Sentinel checks — slot full or hash collision; nothing to do.
        use arbitro_common::name_registry::NameRegistry;
        if seq_stream.raw() == NameRegistry::STREAM_SLOT_FULL_SENTINEL
            || seq_stream.raw() == NameRegistry::STREAM_COLLISION_SENTINEL
        {
            tracing::warn!(
                name,
                "state_machine: create_stream skipped (slot full or collision)"
            );
            return;
        }

        let shard = router.shard_for(seq_stream);
        let max_age_ms = max_age_secs.saturating_mul(1_000);

        let result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(shard.create_stream(
                StreamConfig {
                    id: seq_stream,
                    name: name_bytes.to_vec(),
                },
                max_msgs,
                max_bytes,
                max_age_ms,
            ))
        });

        match result {
            Ok(true) => {
                router
                    .names()
                    .set_stream_idempotency(seq_stream, idempotency_window_ms);
                router.invalidate_list_cache();
                tracing::debug!(name, "state_machine: stream created");
            }
            Ok(false) => {
                // Already exists — idempotent, nothing to do.
                tracing::trace!(name, "state_machine: stream already exists");
            }
            Err(e) => {
                tracing::warn!(name, error = ?e, "state_machine: create_stream failed");
            }
        }
    }

    fn apply_delete_stream(router: &ShardRouter, name: &str) {
        let name_bytes = name.as_bytes();
        let wire_stream = wire_hash_32(name_bytes);
        let seq_stream = match router.names().stream_seq(wire_stream) {
            Some(s) => s,
            None => {
                tracing::trace!(name, "state_machine: delete_stream — not found");
                return;
            }
        };
        let shard = router.shard_for(seq_stream);

        // Snapshot cascaded consumers before engine removes them.
        let cascaded_consumers = router.names().consumers_for_stream(seq_stream);

        match tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(shard.delete_stream(seq_stream, true))
        }) {
            Ok(_) => {
                for cid in cascaded_consumers {
                    router.names().remove_consumer_by_id(cid);
                }
                router.names().remove_stream(wire_stream);
                router.invalidate_list_cache();
                tracing::debug!(name, "state_machine: stream deleted");
            }
            Err(e) => {
                tracing::warn!(name, error = ?e, "state_machine: delete_stream failed");
            }
        }
    }

    fn apply_create_consumer(
        router: &ShardRouter,
        stream_name: &str,
        name: &str,
        group: &str,
        _filter: &str,
        max_inflight: u16,
        ack_policy: u8,
        deliver_policy: u8,
        deliver_mode: u8,
        ack_wait_ms: u32,
        start_seq: u64,
    ) {
        // The stream_name in the command is the wire stream_id as a string
        // (set by the dispatch). Parse it back.
        let wire_stream: u32 = match stream_name.parse() {
            Ok(v) => v,
            Err(_) => {
                tracing::warn!(
                    stream_name,
                    "state_machine: create_consumer — unparseable stream_name"
                );
                return;
            }
        };
        let seq_stream = match router.names().stream_seq(wire_stream) {
            Some(s) => s,
            None => {
                tracing::warn!(
                    stream_name,
                    "state_machine: create_consumer — stream not found"
                );
                return;
            }
        };

        let name_bytes = name.as_bytes();
        let group_bytes = group.as_bytes();

        let ack_pol = match ack_policy {
            0 => AckPolicy::None,
            _ => AckPolicy::Explicit,
        };

        let effective_max_inflight: u16 = if ack_pol == AckPolicy::None {
            0
        } else {
            max_inflight
        };

        let is_fanout = deliver_mode == 0;

        let (seq_consumer, _created) =
            router.names().get_or_create_consumer(seq_stream, name_bytes);
        if seq_consumer.raw() == u32::MAX {
            tracing::warn!(name, "state_machine: consumer slot full");
            return;
        }

        let shard = router.shard_for(seq_stream);

        let queue_id = if is_fanout {
            QueueId(0)
        } else {
            router.names().get_or_create_queue(seq_stream, group_bytes)
        };
        router.names().set_consumer_queue(seq_consumer, queue_id);
        router
            .names()
            .set_consumer_stream(seq_consumer, seq_stream);
        router
            .names()
            .set_consumer_deliver_policy(seq_consumer, deliver_policy, start_seq);

        match tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(shard.create_consumer(
            ConsumerConfig {
                id: seq_consumer,
                queue_id,
                stream_id: seq_stream,
                durable: true,
                ack_policy: ack_pol,
                max_inflight: if effective_max_inflight == 0 {
                    u32::MAX
                } else {
                    effective_max_inflight as u32
                },
                ack_wait_ms,
                max_nack: 0,
            },
            Vec::new(), // no subject limits on replicated path
        ))
        }) {
            Ok(1) => {
                router.invalidate_list_cache();
                tracing::debug!(name, "state_machine: consumer created");
            }
            Ok(0) => {
                tracing::trace!(name, "state_machine: consumer already exists");
            }
            Ok(_) | Err(_) => {
                tracing::warn!(name, "state_machine: create_consumer failed or mismatch");
            }
        }
    }

    fn apply_delete_consumer(router: &ShardRouter, stream_name: &str, name: &str) {
        // `name` is actually the consumer_id as a string (set by dispatch).
        let consumer_id_raw: u32 = match name.parse() {
            Ok(v) => v,
            Err(_) => {
                tracing::warn!(
                    name,
                    "state_machine: delete_consumer — unparseable consumer id"
                );
                return;
            }
        };
        let consumer_id = ConsumerId(consumer_id_raw);

        // Determine owning shard.
        let candidate_shards: smallvec::SmallVec<[usize; 1]> = match router
            .names()
            .consumer_stream(consumer_id)
        {
            Some(stream) => {
                let idx = stream.raw() as usize % router.shard_count();
                smallvec::smallvec![idx]
            }
            None => (0..router.shard_count()).collect(),
        };

        for i in candidate_shards {
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(router.shard(i).delete_consumer(consumer_id))
            });
            if let Ok(_) = result {
                router.names().remove_consumer_by_id(consumer_id);
                router.invalidate_list_cache();
                tracing::debug!(name, "state_machine: consumer deleted");
                return;
            }
        }
        // Consumer not found on any shard — already deleted or never existed.
        let _ = stream_name; // suppress unused warning
        tracing::trace!(name, "state_machine: delete_consumer — not found on any shard");
    }
}

impl StateMachine for ArbitroStateMachine {
    fn apply(&mut self, entry: &[u8]) -> Result<(), RaftError> {
        let cmd: ClusterCommand = serde_json::from_slice(entry)
            .map_err(|e| RaftError::Storage(format!("failed to deserialize command: {e}")))?;

        if let Some(ref router) = self.router {
            match &cmd {
                ClusterCommand::CreateStream {
                    name,
                    filter,
                    max_msgs,
                    max_bytes,
                    max_age_secs,
                    idempotency_window_ms,
                    ..
                } => {
                    Self::apply_create_stream(
                        router,
                        name,
                        filter,
                        *max_msgs,
                        *max_bytes,
                        *max_age_secs,
                        *idempotency_window_ms,
                    );
                }
                ClusterCommand::DeleteStream { name } => {
                    Self::apply_delete_stream(router, name);
                }
                ClusterCommand::CreateConsumer {
                    stream_name,
                    name,
                    group,
                    filter,
                    max_inflight,
                    ack_policy,
                    deliver_policy,
                    deliver_mode,
                    ack_wait_ms,
                    start_seq,
                } => {
                    Self::apply_create_consumer(
                        router,
                        stream_name,
                        name,
                        group,
                        filter,
                        *max_inflight,
                        *ack_policy,
                        *deliver_policy,
                        *deliver_mode,
                        *ack_wait_ms,
                        *start_seq,
                    );
                }
                ClusterCommand::DeleteConsumer { stream_name, name } => {
                    Self::apply_delete_consumer(router, stream_name, name);
                }
            }
        }

        self.applied.push(cmd);
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        serde_json::to_vec(&self.applied)
            .map_err(|e| RaftError::Snapshot(format!("failed to serialize snapshot: {e}")))
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), RaftError> {
        self.applied = serde_json::from_slice(snapshot)
            .map_err(|e| RaftError::Snapshot(format!("failed to deserialize snapshot: {e}")))?;
        Ok(())
    }
}
