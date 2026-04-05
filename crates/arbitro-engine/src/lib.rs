pub mod auth;
pub mod drain;
pub mod engine;
pub mod metrics;
pub mod stream;
pub mod transport;

// Re-exports for ergonomic API
pub use engine::Engine;
pub use engine::context::Context;
pub use auth::{Auth, AllowAll};
pub use transport::{Transport, NoopTransport};
pub use metrics::{Metrics, MetricsSnapshot};

/// Builder for constructing an Engine with custom transport and auth.
pub struct EngineBuilder {
    transport: Option<Box<dyn Transport>>,
    auth: Option<Box<dyn Auth>>,
}

impl EngineBuilder {
    pub fn new() -> Self {
        Self {
            transport: None,
            auth: None,
        }
    }

    pub fn transport(mut self, t: impl Transport + 'static) -> Self {
        self.transport = Some(Box::new(t));
        self
    }

    pub fn auth(mut self, a: impl Auth + 'static) -> Self {
        self.auth = Some(Box::new(a));
        self
    }

    pub fn build(self) -> Engine {
        let transport = self.transport.unwrap_or_else(|| Box::new(NoopTransport));
        let auth = self.auth.unwrap_or_else(|| Box::new(AllowAll));
        let ctx = Context::new(transport, auth);
        Engine::new(ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
    use std::sync::Mutex;

    use arbitro_proto::action::Action;
    use arbitro_proto::config::StreamConfig;
    use arbitro_proto::ids::ConnId;
    use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
    use arbitro_proto::wire::publish::PublishEntry;
    use zerocopy::IntoBytes;
    use zerocopy::byteorder::little_endian::{U16, U32};

    /// Transport that captures sent frames for assertions.
    struct CaptureTransport {
        frames: Mutex<Vec<(ConnId, Vec<u8>)>>,
        count: AtomicU32,
    }

    impl CaptureTransport {
        fn new() -> Self {
            Self {
                frames: Mutex::new(Vec::new()),
                count: AtomicU32::new(0),
            }
        }

        fn sent_count(&self) -> u32 {
            self.count.load(Relaxed)
        }

        fn last_frame(&self) -> Option<Vec<u8>> {
            self.frames.lock().unwrap().last().map(|(_, f)| f.clone())
        }
    }

    impl Transport for CaptureTransport {
        fn send(&self, conn_id: ConnId, data: &[u8]) -> bool {
            self.frames.lock().unwrap().push((conn_id, data.to_vec()));
            self.count.fetch_add(1, Relaxed);
            true
        }
        fn close(&self, _conn_id: ConnId) {}
    }

    fn build_publish_frame(stream_id: u32, subject: &[u8], payload: &[u8]) -> Vec<u8> {
        // Entry header: data_len = payload only (not subject)
        let entry = PublishEntry {
            data_len: U32::new(payload.len() as u32),
            subj_len: U16::new(subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0; 3],
        };
        let entry_bytes = entry.as_bytes();

        // Body = 2-byte count + entry header + subject + payload
        let count: u16 = 1;
        let body_len = 2 + entry_bytes.len() + subject.len() + payload.len();

        let envelope = Envelope {
            action: U16::new(Action::Publish.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(stream_id),
            msg_len: U32::new(body_len as u32),
            env_seq: U32::new(1),
        };

        let mut frame = Vec::with_capacity(ENVELOPE_SIZE + body_len);
        frame.extend_from_slice(envelope.as_bytes());
        frame.extend_from_slice(&count.to_le_bytes()); // batch count
        frame.extend_from_slice(entry_bytes);
        frame.extend_from_slice(subject);
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn full_publish_roundtrip() {
        let transport = CaptureTransport::new();
        let mut engine = EngineBuilder::new()
            .transport(transport)
            .build();

        // Create stream
        let config = StreamConfig::new(b"ORDERS").build();
        let stream_id = config.stream_id;

        engine::management::on_create_stream(&engine.ctx, 1, 0, config);
        assert_eq!(engine.ctx.metrics.snapshot().streams, 1);

        // Publish
        let frame = build_publish_frame(stream_id, b"orders.created", b"{}");
        engine.process_frame(1, &frame);

        assert_eq!(engine.ctx.metrics.snapshot().msgs_in, 1);

        // Verify store has the entry
        let info = engine.ctx.streams.with(stream_id, |s| s.store.info());
        assert_eq!(info.unwrap().messages, 1);
    }

    #[test]
    fn publish_to_nonexistent_stream() {
        let transport = CaptureTransport::new();
        let mut engine = EngineBuilder::new()
            .transport(transport)
            .build();

        let frame = build_publish_frame(999, b"test", b"{}");
        engine.process_frame(1, &frame);

        // Should get RepError (StreamNotFound)
        assert_eq!(engine.ctx.metrics.snapshot().msgs_in, 0);
    }

    #[test]
    fn create_delete_stream() {
        let engine = EngineBuilder::new().build();

        let config = StreamConfig::new(b"ORDERS").build();
        let stream_id = config.stream_id;

        engine::management::on_create_stream(&engine.ctx, 1, 0, config);
        assert_eq!(engine.ctx.streams.count(), 1);

        engine::management::on_delete_stream(&engine.ctx, 1, stream_id, 0);
        assert_eq!(engine.ctx.streams.count(), 0);
    }

    #[test]
    fn purge_and_drain() {
        let mut engine = EngineBuilder::new().build();

        let config = StreamConfig::new(b"ORDERS").build();
        let stream_id = config.stream_id;
        engine::management::on_create_stream(&engine.ctx, 1, 0, config);

        // Publish some messages
        for _ in 0..5 {
            let frame = build_publish_frame(stream_id, b"orders.created", b"{}");
            engine.process_frame(1, &frame);
        }

        assert_eq!(
            engine.ctx.streams.with(stream_id, |s| s.store.info()).unwrap().messages,
            5
        );

        // Purge
        engine::management::on_purge_stream(&engine.ctx, 1, stream_id, 0);
        assert_eq!(
            engine.ctx.streams.with(stream_id, |s| s.store.info()).unwrap().messages,
            0
        );
    }
}
