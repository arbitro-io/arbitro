//! Engine — frame dispatch to handlers.
//!
//! All frames enter through `process_frame`. Hot actions (Publish, Ack)
//! are dispatched first for branch prediction.

pub mod context;
pub mod management;
pub mod publish;
pub mod reply;
pub mod subscribe;
pub mod system;

use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::ids::ConnId;
use arbitro_proto::wire::envelope::FrameView;
use arbitro_proto::metadata::MetadataCommand;
use arbitro_metadata::MetadataApplier;

use context::Context;

impl MetadataApplier for Engine {
    fn apply_command(&self, command: MetadataCommand) {
        let dummy_conn = ConnId::MAX;
        let dummy_seq = 0;

        match command {
            MetadataCommand::CreateStream(cfg) => {
                management::on_create_stream(&self.ctx, dummy_conn, dummy_seq, cfg);
            }
            MetadataCommand::DeleteStream(id) => {
                management::on_delete_stream(&self.ctx, dummy_conn, id, dummy_seq);
            }
            MetadataCommand::CreateConsumer(cfg) => {
                let stream_id = cfg.stream_id;
                subscribe::on_create_consumer(&self.ctx, dummy_conn, stream_id, dummy_seq, cfg);
            }
            MetadataCommand::DeleteConsumer { stream_id, consumer_id } => {
                subscribe::on_delete_consumer(&self.ctx, dummy_conn, stream_id, dummy_seq, consumer_id);
            }
        }
    }
}

/// The engine — owns context and scratch buffers.
pub struct Engine {
    pub ctx: Context,
    scratch: publish::PublishScratch,
}

impl Engine {
    pub fn new(ctx: Context) -> Self {
        Self {
            ctx,
            scratch: publish::PublishScratch::new(),
        }
    }

    /// Get the shared stream map. Used by the server to create drain tasks.
    pub fn streams(&self) -> &std::sync::Arc<crate::stream::StreamMap> {
        &self.ctx.streams
    }

    /// Initialize all components — transport, auth, load metadata.
    /// Called once before processing any frames.
    pub fn init(&mut self) {
        self.ctx.transport.init();
        self.ctx.auth.init();
    }

    /// Graceful shutdown — flush stores, close transport, shutdown auth.
    pub fn shutdown(&mut self) {
        // Shutdown all stream stores
        let configs = self.ctx.streams.list_configs();
        for cfg in &configs {
            self.ctx.streams.with_mut(cfg.stream_id, |slot| {
                let _ = slot.store.shutdown();
            });
        }

        self.ctx.auth.shutdown();
        self.ctx.transport.shutdown();
    }

    /// Main entry point — dispatch a raw frame.
    /// Hot actions first for branch prediction.
    #[inline]
    pub fn process_frame(&mut self, conn_id: ConnId, buf: &[u8]) {
        let frame = FrameView::new(buf);

        let action = match frame.action() {
            Some(a) => a,
            None => {
                let env_seq = frame.envelope().env_seq.get();
                reply::send_error(
                    self.ctx.transport.as_ref(), conn_id, 0, env_seq, 0,
                    ErrorCode::UnknownAction,
                );
                return;
            }
        };

        match action {
            // Hot path — publish, ack, nack, batch_ack
            Action::Publish => {
                publish::on_publish(&self.ctx, conn_id, &frame, &mut self.scratch);
            }
            Action::Ack => {
                system::on_ack(&self.ctx, conn_id, &frame);
            }
            Action::AckSync => {
                system::on_ack_sync(&self.ctx, conn_id, &frame);
            }
            Action::Nack => {
                system::on_nack(&self.ctx, conn_id, &frame);
            }
            Action::BatchAck => {
                system::on_batch_ack(&self.ctx, conn_id, &frame);
            }
            Action::BatchAckSync => {
                system::on_batch_ack_sync(&self.ctx, conn_id, &frame);
            }
            // Everything else is cold path
            _ => self.dispatch_cold(action, conn_id, &frame),
        }
    }

    /// Cold path dispatch — subscriptions, management, system.
    fn dispatch_cold(&self, action: Action, conn_id: ConnId, frame: &FrameView<'_>) {
        let stream_id = frame.stream_id();
        let env_seq = frame.envelope().env_seq.get();

        match action {
            Action::Subscribe => {
                let view = arbitro_proto::wire::subscribe::SubscribeView::new(frame.body());
                subscribe::on_subscribe(&self.ctx, conn_id, stream_id, env_seq, view.consumer_id());
            }
            Action::Unsubscribe => {
                let view = arbitro_proto::wire::subscribe::UnsubscribeView::new(frame.body());
                subscribe::on_unsubscribe(&self.ctx, conn_id, stream_id, env_seq, view.consumer_id());
            }
            Action::CreateStream => {
                let view = arbitro_proto::wire::stream::CreateStreamView::new(frame.body());
                let config = arbitro_proto::config::StreamConfig::new(view.name())
                    .max_msgs(view.max_msgs())
                    .max_bytes(view.max_bytes())
                    .max_age_secs(view.max_age_secs())
                    .replicas(view.replicas())
                    .journal_kind(
                        arbitro_proto::config::JournalKind::from_u8(view.journal_kind())
                            .unwrap_or(arbitro_proto::config::JournalKind::Memory),
                    )
                    .retention(
                        arbitro_proto::config::RetentionPolicy::from_u8(view.retention())
                            .unwrap_or(arbitro_proto::config::RetentionPolicy::Limits),
                    )
                    .build();
                management::on_create_stream(&self.ctx, conn_id, env_seq, config);
            }
            Action::DeleteStream => {
                management::on_delete_stream(&self.ctx, conn_id, stream_id, env_seq);
            }
            Action::PurgeStream => {
                management::on_purge_stream(&self.ctx, conn_id, stream_id, env_seq);
            }
            Action::DrainSubject => {
                let view = arbitro_proto::wire::stream::DrainSubjectView::new(frame.body());
                management::on_drain_subject(&self.ctx, conn_id, stream_id, env_seq, view.subject());
            }
            Action::GetStream => {
                management::on_get_stream(&self.ctx, conn_id, stream_id, env_seq);
            }
            Action::ListStreams => {
                management::on_list_streams(&self.ctx, conn_id, env_seq);
            }
            Action::CreateConsumer => {
                let view = arbitro_proto::wire::manager::CreateConsumerView::new(frame.body());
                let subject_limits: Vec<arbitro_proto::config::SubjectLimit> = view
                    .limits()
                    .map(|(pattern, limit)| arbitro_proto::config::SubjectLimit {
                        pattern: Box::from(pattern),
                        limit,
                    })
                    .collect();
                let config = arbitro_proto::config::ConsumerConfig::from_wire(
                    view.stream_id(),
                    view.name(),
                    view.subject(),
                    view.max_inflight(),
                    view.ack_policy(),
                    view.deliver_policy(),
                    view.deliver_mode(),
                    view.ack_wait_ms(),
                    view.start_seq(),
                    subject_limits.into_boxed_slice(),
                );
                subscribe::on_create_consumer(&self.ctx, conn_id, stream_id, env_seq, config);
            }
            Action::DeleteConsumer => {
                let view = arbitro_proto::wire::manager::DeleteConsumerView::new(frame.body());
                subscribe::on_delete_consumer(&self.ctx, conn_id, stream_id, env_seq, view.consumer_id());
            }
            Action::GetConsumer => {
                let view = arbitro_proto::wire::manager::GetConsumerView::new(frame.body());
                subscribe::on_get_consumer(&self.ctx, conn_id, stream_id, env_seq, view.consumer_id());
            }
            Action::ListConsumers => {
                subscribe::on_list_consumers(&self.ctx, conn_id, stream_id, env_seq);
            }
            Action::Fetch => {
                let view = arbitro_proto::wire::subscribe::FetchView::new(frame.body());
                let consumer_id = view.consumer_id();
                let max_msgs = view.max_msgs();
                // Fetch via shard lock — no global drains Mutex
                let fetched = self.ctx.streams.with_mut(stream_id, |slot| {
                    let now_ts = publish::current_timestamp();
                    slot.drain.fetch(consumer_id, max_msgs, &*slot.store, self.ctx.transport.as_ref(), now_ts, conn_id)
                }).unwrap_or(0);
                reply::send_ok(self.ctx.transport.as_ref(), conn_id, stream_id, env_seq, fetched as u64);
            }
            Action::Ping => {
                system::on_ping(&self.ctx, conn_id, frame);
            }
            Action::Connect => {
                let view = arbitro_proto::wire::system::ConnectView::new(frame.body());
                system::on_connect(&self.ctx, conn_id, view.auth_token());
            }
            Action::Disconnect => {
                system::on_disconnect(&self.ctx, conn_id);
            }
            Action::Stats => {
                system::on_stats(&self.ctx, conn_id, frame);
            }
            // Server-to-client only — never received from clients
            _ => {
                reply::send_error(
                    self.ctx.transport.as_ref(), conn_id, 0, env_seq, 0,
                    ErrorCode::UnknownAction,
                );
            }
        }
    }
}
