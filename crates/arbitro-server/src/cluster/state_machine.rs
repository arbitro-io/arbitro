//! Raft state machine for Arbitro cluster metadata operations.
//!
//! `apply()` deserializes each committed log entry into a `ClusterCommand`
//! and enqueues it on an async channel; a background worker drains that
//! channel and executes shard calls without holding the state-machine
//! lock and without `block_in_place`. That means:
//!
//! - `apply` returns in microseconds (JSON decode + channel send), so the
//!   apply-loop's `parking_lot::Mutex` is never held across an async wait.
//! - We never call `tokio::task::block_in_place`, which would panic on a
//!   current-thread runtime.
//!
//! Command execution ordering is preserved: entries are applied in Raft
//! log order, enqueued in order, and the FIFO worker processes them in
//! order.

use std::sync::Arc;

use arbitro_raft::{RaftError, StateMachine};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig};
use arbitro_engine_v2::common::wire_hash_32;
use arbitro_engine_v2::types::*;

use crate::shard::router::ShardRouter;

/// Commands replicated through Raft for cluster-wide metadata consistency.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
/// `apply()` enqueues each committed command onto an async channel; the
/// background worker returned by [`set_router`] drains the channel and
/// runs the actual shard operations. The `applied` log is retained for
/// snapshot/restore.
#[derive(Default)]
pub struct ArbitroStateMachine {
    // TODO(cluster): `applied` retains every committed metadata command
    // forever and `snapshot()` serializes the full history — unbounded RAM
    // and ever-growing snapshots on long-lived clusters (audit #10). A
    // correct bound requires real snapshot/compaction (replace the retained
    // prefix with a state snapshot and truncate the raft log accordingly);
    // a naive cap would silently corrupt `restore()`. Deferred to the
    // cluster/raft workstream — see ROBUSTNESS_AUDIT.md action #10 and the
    // arbitro-raft audit.
    applied: Vec<ClusterCommand>,
    cmd_tx: Option<mpsc::UnboundedSender<ClusterCommand>>,
}

impl ArbitroStateMachine {
    /// Create a state machine without a router (used during early init
    /// before the `ShardRouter` is available).
    pub fn new() -> Self {
        Self::default()
    }

    /// Wire the shard router. Returns the background worker future that
    /// executes committed commands; the caller must spawn it on a runtime
    /// (e.g. `tokio::spawn(worker)`).
    ///
    /// Until the worker is running, `apply()` will still enqueue commands
    /// (they are buffered in the channel) but nothing on the engine will
    /// actually change.
    pub fn set_router(
        &mut self,
        router: Arc<ShardRouter>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let (tx, rx) = mpsc::unbounded_channel();
        self.cmd_tx = Some(tx);
        Self::apply_worker(router, rx)
    }

    /// Background worker that drains the command channel and executes
    /// each command as an async shard call. Runs until the channel is
    /// closed (i.e. the state machine has been dropped and no senders
    /// remain).
    async fn apply_worker(
        router: Arc<ShardRouter>,
        mut rx: mpsc::UnboundedReceiver<ClusterCommand>,
    ) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                ClusterCommand::CreateStream {
                    name,
                    filter,
                    max_msgs,
                    max_bytes,
                    max_age_secs,
                    idempotency_window_ms,
                    ..
                } => {
                    Self::do_create_stream(
                        &router,
                        &name,
                        &filter,
                        max_msgs,
                        max_bytes,
                        max_age_secs,
                        idempotency_window_ms,
                    )
                    .await;
                }
                ClusterCommand::DeleteStream { name } => {
                    Self::do_delete_stream(&router, &name).await;
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
                    Self::do_create_consumer(
                        &router,
                        &stream_name,
                        &name,
                        &group,
                        &filter,
                        max_inflight,
                        ack_policy,
                        deliver_policy,
                        deliver_mode,
                        ack_wait_ms,
                        start_seq,
                    )
                    .await;
                }
                ClusterCommand::DeleteConsumer { stream_name, name } => {
                    Self::do_delete_consumer(&router, &stream_name, &name).await;
                }
            }
        }
    }

    // ── Internal helpers ─────────────────────────────────────────────

    async fn do_create_stream(
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
        let (seq_stream, _created) = router
            .names()
            .get_or_create_stream_named(wire_stream, name_bytes);

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

        let result = shard
            .create_stream(
                StreamConfig {
                    id: seq_stream,
                    name: name_bytes.to_vec(),
                },
                max_msgs,
                max_bytes,
                max_age_ms,
            )
            .await;

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

    async fn do_delete_stream(router: &ShardRouter, name: &str) {
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

        match shard.delete_stream(seq_stream, true).await {
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

    async fn do_create_consumer(
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

        // GROUP-1 (mirrored from `v2_create_consumer`): the group is
        // mandatory. The proposing dispatch rejects an empty group before it
        // ever reaches Raft, so this is a defensive apply-side guard — a
        // follower must never materialise the anonymous `(stream, "")` queue
        // from a malformed or legacy log entry.
        if group_bytes.is_empty() {
            tracing::warn!(
                name,
                "state_machine: create_consumer — empty group, rejected (GROUP-1)"
            );
            return;
        }

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

        let (seq_consumer, _created) = router
            .names()
            .get_or_create_consumer(seq_stream, name_bytes);
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
        let create_res = shard
            .create_consumer(
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
            )
            .await;

        let create_code = create_res.as_ref().ok().map(|r| r.code);

        // DeliverPolicy::New: same creation-tail stamping as
        // `v2_create_consumer`. The apply order is identical on every
        // replica, so the locally observed journal tail is deterministic
        // across the cluster.
        let effective_start_seq = if deliver_policy == 1 {
            let prior = router.names().consumer_deliver_policy(seq_consumer);
            match (create_code, prior) {
                (Some(0), Some((1, floor))) => floor,
                _ => create_res.as_ref().map(|r| r.journal_tail).unwrap_or(0),
            }
        } else {
            start_seq
        };

        // AUDIT-10 follow-up (same as v2_create_consumer): only mutate the
        // NameRegistry after the engine accepts the create. A rejected
        // re-create (config mismatch) must not overwrite the registry copy
        // of deliver_policy/queue/stream for the existing consumer.
        if matches!(create_code, Some(0) | Some(1)) {
            router.names().set_consumer_queue(seq_consumer, queue_id);
            router.names().set_consumer_stream(seq_consumer, seq_stream);
            router.names().set_consumer_deliver_policy(
                seq_consumer,
                deliver_policy,
                effective_start_seq,
            );
        }

        match create_code {
            Some(1) => {
                router.invalidate_list_cache();
                tracing::debug!(name, "state_machine: consumer created");
            }
            Some(0) => {
                tracing::trace!(name, "state_machine: consumer already exists");
            }
            _ => {
                tracing::warn!(name, "state_machine: create_consumer failed or mismatch");
            }
        }
    }

    async fn do_delete_consumer(router: &ShardRouter, stream_name: &str, name: &str) {
        // BUG-7: the dispatch path now sends wire-stable
        // (stream_name, consumer_name) pairs instead of the leader's
        // sequential consumer_id. Each follower resolves both to its
        // OWN local ids — that way "delete consumer X on stream Y"
        // targets the same logical consumer on every node, regardless
        // of the id it happened to be assigned locally.
        if stream_name.is_empty() || name.is_empty() {
            tracing::warn!(
                stream_name,
                name,
                "state_machine: delete_consumer — missing name(s) in cluster command"
            );
            return;
        }
        let wire_stream = wire_hash_32(stream_name.as_bytes());
        let seq_stream = match router.names().stream_seq(wire_stream) {
            Some(s) => s,
            None => {
                tracing::trace!(
                    stream_name,
                    "state_machine: delete_consumer — stream not found on this node"
                );
                return;
            }
        };
        let consumer_id = match router.names().consumer_id(seq_stream, name.as_bytes()) {
            Some(id) => id,
            None => {
                tracing::trace!(
                    stream_name,
                    name,
                    "state_machine: delete_consumer — consumer not found on this node"
                );
                return;
            }
        };
        // The owning shard is a stable function of the local stream_seq —
        // no fan-out needed now that we know the exact stream.
        let shard_idx = seq_stream.raw() as usize % router.shard_count();
        match router.shard(shard_idx).delete_consumer(consumer_id).await {
            Ok(_) => {
                router.names().remove_consumer_by_id(consumer_id);
                router.invalidate_list_cache();
                tracing::debug!(stream_name, name, "state_machine: consumer deleted");
            }
            Err(e) => {
                tracing::warn!(
                    stream_name,
                    name,
                    error = ?e,
                    "state_machine: delete_consumer shard call failed"
                );
            }
        }
    }
}

impl StateMachine for ArbitroStateMachine {
    fn apply(&mut self, entry: &[u8]) -> Result<(), RaftError> {
        let cmd: ClusterCommand = serde_json::from_slice(entry)
            .map_err(|e| RaftError::Storage(format!("failed to deserialize command: {e}")))?;

        // Enqueue for the async worker (spawned by set_router). The
        // channel is unbounded, so this is a non-blocking send. If the
        // worker has been stopped and its receiver dropped, this fails
        // silently — the command is still recorded in `applied` so
        // snapshots stay consistent.
        if let Some(ref tx) = self.cmd_tx {
            let _ = tx.send(cmd.clone());
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

#[cfg(test)]
mod tests {
    use super::*;

    /// BUG-5 regression: `apply()` must be usable from a current-thread
    /// runtime. Previously the sync path called `tokio::task::block_in_place`
    /// which panics unless the runtime is multi-thread, and it also held
    /// the parking_lot mutex across an async wait.
    ///
    /// With the enqueue-only design, `apply()` never touches any tokio
    /// primitive that requires a specific runtime flavor, so it works on
    /// `current_thread` too.
    #[tokio::test(flavor = "current_thread")]
    async fn apply_is_non_blocking_on_current_thread_runtime() {
        // No router → cmd_tx is None → apply() only decodes JSON and
        // pushes into `applied`. This is the same sync path a real
        // apply loop follows before the worker is spawned.
        let mut sm = ArbitroStateMachine::new();
        let cmd = ClusterCommand::CreateStream {
            name: "orders".to_string(),
            filter: "orders.>".to_string(),
            max_msgs: 1000,
            max_bytes: 0,
            max_age_secs: 0,
            replicas: 1,
            journal_kind: 0,
            retention: 0,
            discard: 0,
            idempotency_window_ms: 0,
        };
        let bytes = serde_json::to_vec(&cmd).unwrap();

        // If this ever regresses to block_in_place, the current-thread
        // runtime will panic with "can call blocking only when running
        // on the multi-threaded runtime".
        sm.apply(&bytes).expect("apply must succeed");

        // The command must be recorded for snapshot/restore even without
        // a router.
        assert_eq!(sm.applied.len(), 1);
    }

    /// A CreateStream JSON round-trip through apply() is captured in
    /// `applied`, so `snapshot()` reflects the applied history.
    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_captures_applied_commands() {
        let mut sm = ArbitroStateMachine::new();
        let cmd = ClusterCommand::DeleteStream {
            name: "stale".to_string(),
        };
        let bytes = serde_json::to_vec(&cmd).unwrap();
        sm.apply(&bytes).expect("apply must succeed");

        let snap = sm.snapshot().expect("snapshot must succeed");
        let mut restored = ArbitroStateMachine::new();
        restored.restore(&snap).expect("restore must succeed");
        assert_eq!(restored.applied.len(), 1);
    }
}
