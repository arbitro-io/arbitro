//! Recovery — replay metadata commands into the engine on startup.
//!
//! `ReplayApplier` implements `MetadataApplier` and re-dispatches each
//! command to the correct shard via the existing `ShardHandle` async API.
//! Since `MetadataApplier::apply` is sync, commands are buffered and
//! flushed with `flush().await` after replay completes.

use crate::shard::router::ShardRouter;
use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig};
use arbitro_engine_v2::types::*;
use arbitro_proto::metadata::{
    MetadataApplier, MetadataCommandView, CMD_CREATE_CONSUMER, CMD_CREATE_STREAM,
    CMD_CURSOR_UPDATE, CMD_DELETE_CONSUMER, CMD_DELETE_STREAM,
};
use arbitro_proto::wire::manager::CreateConsumerView;
use arbitro_proto::wire::stream::CreateStreamView;

/// A buffered command for async replay.
enum ReplayCommand {
    CreateStream {
        stream_id: StreamId,
        config: StreamConfig,
        journal_kind: u8,
        // H3: persisted retention limits — without these, recovery
        // recreates streams with unbounded retention, so a max_msgs /
        // max_bytes / max_age the operator originally configured is
        // silently lost across restart.
        max_msgs: u64,
        max_bytes: u64,
        max_age_ms: u64,
    },
    DeleteStream {
        stream_id: StreamId,
    },
    CreateConsumer {
        stream_id: StreamId,
        config: ConsumerConfig,
        max_subject_inflights: Vec<(Vec<u8>, u32)>,
    },
    DeleteConsumer {
        stream_id: StreamId,
        consumer_id: ConsumerId,
    },
    CursorUpdate {
        consumer_id: ConsumerId,
        last_acked_seq: u64,
    },
}

/// Replays metadata commands into shards.
///
/// Commands are parsed and buffered in `apply()` (sync), then dispatched
/// to shards in `flush()` (async).
pub struct ReplayApplier {
    server: ShardRouter,
    commands: Vec<ReplayCommand>,
}

impl ReplayApplier {
    pub fn new(server: ShardRouter) -> Self {
        Self {
            server,
            commands: Vec::new(),
        }
    }

    /// Dispatch all buffered commands to shards. Must be called after replay.
    ///
    /// Logs a summary: recovered streams, consumers, and messages per stream.
    pub async fn flush(&mut self) {
        let mut streams_recovered = 0u32;
        let mut consumers_recovered = 0u32;

        for cmd in self.commands.drain(..) {
            match cmd {
                ReplayCommand::CreateStream {
                    stream_id,
                    config,
                    journal_kind,
                    max_msgs,
                    max_bytes,
                    max_age_ms,
                } => {
                    let shard = self.server.shard_for(stream_id);
                    let _ = journal_kind; // no longer needed — single store per shard
                    match shard
                        .create_stream(config, max_msgs, max_bytes, max_age_ms)
                        .await
                    {
                        Ok(true) => {
                            streams_recovered += 1;
                            tracing::debug!(?stream_id, "replayed CreateStream");
                        }
                        Ok(false) => {
                            tracing::debug!(?stream_id, "CreateStream already exists (idempotent)")
                        }
                        Err(e) => {
                            tracing::error!(?stream_id, error = %e, "replay CreateStream failed")
                        }
                    }
                }
                ReplayCommand::DeleteStream { stream_id } => {
                    let shard = self.server.shard_for(stream_id);
                    match shard.delete_stream(stream_id, false).await {
                        Ok(_) => tracing::debug!(?stream_id, "replayed DeleteStream"),
                        Err(e) => {
                            tracing::error!(?stream_id, error = %e, "replay DeleteStream failed")
                        }
                    }
                }
                ReplayCommand::CreateConsumer {
                    stream_id,
                    config,
                    max_subject_inflights,
                } => {
                    let consumer_id = config.id;
                    let shard = self.server.shard_for(stream_id);
                    match shard.create_consumer(config, max_subject_inflights).await {
                        Ok(1) => {
                            consumers_recovered += 1;
                            tracing::debug!(?consumer_id, "replayed CreateConsumer");
                        }
                        Ok(0) => tracing::debug!(
                            ?consumer_id,
                            "CreateConsumer already exists (idempotent)"
                        ),
                        Ok(code) => tracing::warn!(
                            ?consumer_id,
                            code,
                            "replay CreateConsumer rejected (config mismatch?)"
                        ),
                        Err(e) => {
                            tracing::error!(?consumer_id, error = %e, "replay CreateConsumer failed")
                        }
                    }
                }
                ReplayCommand::DeleteConsumer {
                    stream_id,
                    consumer_id,
                } => {
                    let shard = self.server.shard_for(stream_id);
                    match shard.delete_consumer(consumer_id).await {
                        Ok(_) => tracing::debug!(?consumer_id, "replayed DeleteConsumer"),
                        Err(e) => {
                            tracing::error!(?consumer_id, error = %e, "replay DeleteConsumer failed")
                        }
                    }
                }
                ReplayCommand::CursorUpdate {
                    consumer_id,
                    last_acked_seq,
                } => {
                    // Restore the persisted cursor into NameRegistry.
                    self.server
                        .names()
                        .set_consumer_cursor(consumer_id, last_acked_seq);
                    tracing::debug!(?consumer_id, last_acked_seq, "replayed CursorUpdate");
                }
            }
        }

        if streams_recovered > 0 || consumers_recovered > 0 {
            // Query each shard for store message counts
            let mut total_messages = 0u64;
            for i in 0..self.server.shard_count() {
                let shard = self.server.shard(i);
                if let Ok(reply) = shard.list_streams().await {
                    for (stream_id, name) in &reply.streams {
                        if let Ok(info) = shard.store_info(StreamId(*stream_id)).await {
                            tracing::info!(
                                stream = %String::from_utf8_lossy(name),
                                stream_id = stream_id,
                                messages = info.messages,
                                bytes = info.bytes,
                                "recovered stream"
                            );
                            total_messages += info.messages;
                        }
                    }
                }
            }

            tracing::info!(
                streams = streams_recovered,
                consumers = consumers_recovered,
                total_messages = total_messages,
                "recovery complete"
            );
        }
    }
}

impl MetadataApplier for ReplayApplier {
    fn apply(&mut self, command: &[u8]) {
        let view = match MetadataCommandView::new(command) {
            Some(v) => v,
            None => return,
        };

        match view.command_type() {
            CMD_CREATE_STREAM => {
                let sv = CreateStreamView::new(view.body());
                let name = sv.name();
                // Wire stream_id is the client-side wire_hash_32(name); translate
                // through NameRegistry to a small sequential engine StreamId
                // (the engine catalog indexes match_tables by raw u32 — see
                // common::name_registry for full rationale).
                let wire_id = arbitro_engine_v2::catalog::wire_hash_32(name);
                let (stream_id, _created) = self.server.names().get_or_create_stream(wire_id);
                // Restore the per-stream idempotency window — same call
                // `v2_create_stream` makes on the live path. Without this,
                // a stream that was idempotent at write time would lose
                // its dedup window across restart.
                self.server
                    .names()
                    .set_stream_idempotency(stream_id, sv.idempotency_window_ms());
                self.commands.push(ReplayCommand::CreateStream {
                    stream_id,
                    config: StreamConfig {
                        id: stream_id,
                        name: name.to_vec(),
                    },
                    journal_kind: sv.journal_kind(),
                    // H3: extract retention from the wire body, so a
                    // restart restores what the operator configured at
                    // CreateStream time.
                    max_msgs: sv.max_msgs(),
                    max_bytes: sv.max_bytes(),
                    max_age_ms: sv.max_age_secs().saturating_mul(1_000),
                });
            }
            CMD_DELETE_STREAM => {
                let sv = arbitro_proto::wire::stream::DeleteStreamView::new(view.body());
                let name = sv.name();
                let wire_id = arbitro_engine_v2::catalog::wire_hash_32(name);
                let stream_id = match self.server.names().stream_seq(wire_id) {
                    Some(id) => id,
                    None => {
                        tracing::warn!(name = %String::from_utf8_lossy(name),
                            "replay DeleteStream for unknown stream — skipping");
                        return;
                    }
                };
                // Same cascade as `v2_delete_stream`: every consumer
                // attached to this stream must be dropped from
                // NameRegistry too, otherwise replay would leave stale
                // name → id mappings pointing at consumers the cascade
                // is about to remove from the engine catalog.
                for cid in self.server.names().consumers_for_stream(stream_id) {
                    self.server.names().remove_consumer_by_id(cid);
                }
                self.server.names().remove_stream(wire_id);
                self.commands
                    .push(ReplayCommand::DeleteStream { stream_id });
            }
            CMD_CREATE_CONSUMER => {
                let cv = CreateConsumerView::new(view.body());
                // Translate the client-supplied wire stream id.
                let wire_stream = cv.stream_id();
                let (stream_id, _) = self.server.names().get_or_create_stream(wire_stream);

                let consumer_name = cv.name();
                let (consumer_id, _) = self
                    .server
                    .names()
                    .get_or_create_consumer(stream_id, consumer_name);

                // GAP-5 (mirrored from live dispatch): Fanout consumers
                // (deliver_mode == 0) use QueueId(0) directly — the
                // drain worker skips queue-dedup for id 0, giving every
                // consumer its own copy. Queue consumers go through the
                // content-addressed allocator so members of the same
                // group share a single QueueId.
                let group = cv.group();
                let queue_id = if cv.deliver_mode() == 0 {
                    QueueId(0)
                } else {
                    self.server.names().get_or_create_queue(stream_id, group)
                };
                self.server
                    .names()
                    .set_consumer_queue(consumer_id, queue_id);
                // Record consumer→stream binding so DeleteConsumer replay can
                // route to the correct shard without a wire stream_id field.
                self.server
                    .names()
                    .set_consumer_stream(consumer_id, stream_id);

                let ack_policy = match cv.ack_policy() {
                    0 => AckPolicy::None,
                    _ => AckPolicy::Explicit,
                };

                let max_subject_inflights: Vec<(Vec<u8>, u32)> = cv
                    .subject_limits()
                    .map(|e| (e.pattern.to_vec(), e.limit))
                    .collect();

                self.commands.push(ReplayCommand::CreateConsumer {
                    stream_id,
                    config: ConsumerConfig {
                        id: consumer_id,
                        queue_id,
                        stream_id,
                        durable: true,
                        ack_policy,
                        max_inflight: if cv.max_inflight() == 0 {
                            u32::MAX
                        } else {
                            cv.max_inflight() as u32
                        },
                        ack_wait_ms: cv.ack_wait_ms(),
                        max_nack: 0,
                    },
                    max_subject_inflights,
                });
            }
            CMD_DELETE_CONSUMER => {
                let dv = arbitro_proto::wire::manager::DeleteConsumerView::new(view.body());
                // DeleteConsumer doesn't carry stream_id in the wire body.
                // Recover the correct stream by looking up the consumer→stream
                // binding that was populated during CreateConsumer replay above.
                // Journal is replayed in order, so the mapping is guaranteed to
                // be present if the consumer was created in this journal.
                let consumer_id = ConsumerId(dv.consumer_id());
                let stream_id = self
                    .server
                    .names()
                    .consumer_stream(consumer_id)
                    .unwrap_or(StreamId(0)); // fallback: consumer already absent, no-op
                                             // Mirror the wire handler's cascade (`v2_delete_consumer` in
                                             // dispatch_v2.rs): drop the wire-name → id mapping and the
                                             // reverse indexes from NameRegistry. Without this, a
                                             // pre-restart create→delete sequence replays the create into
                                             // NameRegistry but never undoes it, leaving a phantom name
                                             // mapping that aliases a future re-create on the same name.
                self.server.names().remove_consumer_by_id(consumer_id);
                self.commands.push(ReplayCommand::DeleteConsumer {
                    stream_id,
                    consumer_id,
                });
            }
            CMD_CURSOR_UPDATE => {
                // Body: [4 consumer_id LE][8 last_acked_seq LE]
                let body = view.body();
                if body.len() >= 12 {
                    let consumer_id =
                        ConsumerId(u32::from_le_bytes([body[0], body[1], body[2], body[3]]));
                    let last_acked_seq = u64::from_le_bytes([
                        body[4], body[5], body[6], body[7], body[8], body[9], body[10], body[11],
                    ]);
                    self.commands.push(ReplayCommand::CursorUpdate {
                        consumer_id,
                        last_acked_seq,
                    });
                }
            }
            _ => {
                tracing::warn!(
                    cmd_type = view.command_type(),
                    "unknown metadata command type during replay"
                );
            }
        }
    }
}

// ── Idempotency recovery ──────────────────────────────────────────────────

/// Scan all stores for records with `HAS_HEADERS` flag, extract `msg-id`
/// headers, and repopulate the per-stream `IdempotencyTracker`.
///
/// Only scans entries within each stream's `idempotency_window_ms` from
/// the current time — older entries have already expired and should not
/// block future publishes with the same id.
pub async fn rebuild_idempotency(server: &ShardRouter) {
    use arbitro_engine_v2::types::StreamId;
    use arbitro_proto::wire::msg_headers::{ExtendedPayload, HDR_MSG_ID};
    use zerocopy::FromBytes;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let mut total_recovered = 0u64;

    for shard_idx in 0..server.shard_count() {
        let shard = server.shard(shard_idx);

        // List streams on this shard.
        let streams = match shard.list_streams().await {
            Ok(r) => r.streams,
            Err(_) => continue,
        };

        for (raw_id, _name) in &streams {
            // list_streams returns (StreamId.raw(), name) — already the
            // sequential engine ID, not a wire hash.
            let stream_id = StreamId(*raw_id);

            let window_ms = server.names().stream_idempotency_window_ms(stream_id);
            if window_ms == 0 {
                continue; // stream has no dedup — skip
            }

            let cutoff_ms = now_ms.saturating_sub(window_ms as u64);

            let shared_store = server.store_for(stream_id);
            let store = shared_store.lock();
            let info = store.info();
            if info.messages == 0 {
                continue;
            }

            let shared_idemp = server.idempotency_for(stream_id);
            let tracker_arc =
                crate::shard::idempotency::idempotency_for_stream(shared_idemp, stream_id);
            let mut tracker = tracker_arc.lock();

            store.for_each(info.first_seq, info.last_seq + 1, &mut |entry| {
                // Skip entries older than the idempotency window.
                if entry.timestamp < cutoff_ms {
                    return;
                }

                // Only process entries with HAS_HEADERS flag.
                if entry.flags & arbitro_store::flags::HAS_HEADERS == 0 {
                    return;
                }

                // Parse the extended payload to extract msg-id header.
                let ext = match ExtendedPayload::ref_from_bytes(entry.payload) {
                    Ok(e) => e,
                    Err(_) => return,
                };
                let hdr = match ext.headers_block() {
                    Some(h) => h,
                    None => return,
                };
                let msg_id = match hdr.get(HDR_MSG_ID) {
                    Some(id) if !id.is_empty() => id,
                    _ => return,
                };

                // Repopulate the tracker.
                let hash = crate::transport::dispatch_v2::idempotency_hash(msg_id);
                // Remaining window = window_ms - (now - entry.timestamp).
                let elapsed = now_ms.saturating_sub(entry.timestamp);
                let remaining_ms = (window_ms as u64).saturating_sub(elapsed);
                if remaining_ms > 0 {
                    tracker.record(stream_id, hash, msg_id, remaining_ms as u32);
                    total_recovered += 1;
                }
            }).ok();

            drop(tracker);
            if total_recovered > 0 {
                server.mark_idempotency_allocated(stream_id);
            }
        }
    }

    if total_recovered > 0 {
        tracing::info!(
            count = total_recovered,
            "idempotency tracker rebuilt from journal"
        );
    }
}
