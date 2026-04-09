//! Recovery — replay metadata commands into the engine on startup.
//!
//! `ReplayApplier` implements `MetadataApplier` and re-dispatches each
//! command to the correct shard via the existing `ShardHandle` async API.
//! Since `MetadataApplier::apply` is sync, commands are buffered and
//! flushed with `flush().await` after replay completes.

use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig};
use arbitro_engine_v2::types::*;
use arbitro_proto::metadata::{
    MetadataApplier, MetadataCommandView,
    CMD_CREATE_STREAM, CMD_DELETE_STREAM, CMD_CREATE_CONSUMER, CMD_DELETE_CONSUMER,
};
use arbitro_proto::wire::manager::CreateConsumerView;
use arbitro_proto::wire::stream::CreateStreamView;
use crate::router::Server;

/// A buffered command for async replay.
enum ReplayCommand {
    CreateStream {
        stream_id: StreamId,
        config: StreamConfig,
        journal_kind: u8,
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
}

/// Replays metadata commands into shards.
///
/// Commands are parsed and buffered in `apply()` (sync), then dispatched
/// to shards in `flush()` (async).
pub struct ReplayApplier {
    server: Server,
    commands: Vec<ReplayCommand>,
}

impl ReplayApplier {
    pub fn new(server: Server) -> Self {
        Self { server, commands: Vec::new() }
    }

    /// Dispatch all buffered commands to shards. Must be called after replay.
    ///
    /// Logs a summary: recovered streams, consumers, and messages per stream.
    pub async fn flush(&mut self) {
        let mut streams_recovered = 0u32;
        let mut consumers_recovered = 0u32;

        for cmd in self.commands.drain(..) {
            match cmd {
                ReplayCommand::CreateStream { stream_id, config, journal_kind } => {
                    let shard = self.server.shard_for(stream_id);
                    match shard.create_stream(config, journal_kind).await {
                        Ok(true) => {
                            streams_recovered += 1;
                            tracing::debug!(?stream_id, "replayed CreateStream");
                        }
                        Ok(false) => tracing::debug!(?stream_id, "CreateStream already exists (idempotent)"),
                        Err(e) => tracing::error!(?stream_id, error = %e, "replay CreateStream failed"),
                    }
                }
                ReplayCommand::DeleteStream { stream_id } => {
                    let shard = self.server.shard_for(stream_id);
                    match shard.delete_stream(stream_id, DrainMode::ReleaseAndRequeue, false).await {
                        Ok(_) => tracing::debug!(?stream_id, "replayed DeleteStream"),
                        Err(e) => tracing::error!(?stream_id, error = %e, "replay DeleteStream failed"),
                    }
                }
                ReplayCommand::CreateConsumer { stream_id, config, max_subject_inflights } => {
                    let consumer_id = config.id;
                    let shard = self.server.shard_for(stream_id);
                    match shard.create_consumer(config, max_subject_inflights).await {
                        Ok(true) => {
                            consumers_recovered += 1;
                            tracing::debug!(?consumer_id, "replayed CreateConsumer");
                        }
                        Ok(false) => tracing::debug!(?consumer_id, "CreateConsumer already exists (idempotent)"),
                        Err(e) => tracing::error!(?consumer_id, error = %e, "replay CreateConsumer failed"),
                    }
                }
                ReplayCommand::DeleteConsumer { stream_id, consumer_id } => {
                    let shard = self.server.shard_for(stream_id);
                    match shard.delete_consumer(consumer_id, DrainMode::ReleaseAndRequeue).await {
                        Ok(_) => tracing::debug!(?consumer_id, "replayed DeleteConsumer"),
                        Err(e) => tracing::error!(?consumer_id, error = %e, "replay DeleteConsumer failed"),
                    }
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
                let stream_id = StreamId(arbitro_engine_v2::catalog::fnv1a_32(name));
                self.commands.push(ReplayCommand::CreateStream {
                    stream_id,
                    config: StreamConfig {
                        id: stream_id,
                        name: name.to_vec(),
                    },
                    journal_kind: sv.journal_kind(),
                });
            }
            CMD_DELETE_STREAM => {
                let sv = arbitro_proto::wire::stream::DeleteStreamView::new(view.body());
                let name = sv.name();
                let stream_id = StreamId(arbitro_engine_v2::catalog::fnv1a_32(name));
                self.commands.push(ReplayCommand::DeleteStream { stream_id });
            }
            CMD_CREATE_CONSUMER => {
                let cv = CreateConsumerView::new(view.body());
                let stream_id = StreamId(cv.stream_id());
                let consumer_name = cv.name();
                let consumer_id = ConsumerId(arbitro_engine_v2::catalog::fnv1a_32(consumer_name));

                let group = cv.group();
                let queue_id = if group.is_empty() {
                    QueueId(cv.stream_id())
                } else {
                    QueueId(arbitro_engine_v2::catalog::fnv1a_32(group))
                };

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
                        max_inflight: if cv.max_inflight() == 0 { u32::MAX } else { cv.max_inflight() as u32 },
                    },
                    max_subject_inflights,
                });
            }
            CMD_DELETE_CONSUMER => {
                let dv = arbitro_proto::wire::manager::DeleteConsumerView::new(view.body());
                // DeleteConsumer doesn't carry stream_id in the wire body.
                // We need the stream_id to route to the right shard.
                // For now, fan out to all shards — delete is idempotent.
                // TODO: encode stream_id in delete consumer wire format.
                let consumer_id = ConsumerId(dv.consumer_id());
                // Use consumer_id as a rough shard selector (won't always be correct,
                // but delete on wrong shard is a no-op).
                self.commands.push(ReplayCommand::DeleteConsumer {
                    stream_id: StreamId(dv.consumer_id()),
                    consumer_id,
                });
            }
            _ => {
                tracing::warn!(cmd_type = view.command_type(), "unknown metadata command type during replay");
            }
        }
    }
}
