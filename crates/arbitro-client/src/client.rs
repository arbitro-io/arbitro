//! Client — public API for interacting with the arbitro broker.

use std::sync::Arc;

use tokio::sync::watch;
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use arbitro_proto::action::Action;
use arbitro_proto::config::fnv1a_32;
use arbitro_proto::wire::manager::{CreateConsumerFixed, DeleteConsumerAction};
use arbitro_proto::wire::publish::PublishEntry;
use arbitro_proto::wire::stream::{CreateStreamFixed, DeleteStreamFixed};

use crate::conn;
use crate::consumer::Consumer;
use crate::error::ClientError;
use crate::inner::{ConnState, Inner};

/// Client for the arbitro message broker.
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    /// Connect to the broker at the given address (e.g., "127.0.0.1:4222").
    pub async fn connect(addr: &str) -> Result<Self, ClientError> {
        Self::connect_with_timeout(addr, std::time::Duration::from_secs(5)).await
    }

    /// Connect with a custom request timeout.
    pub async fn connect_with_timeout(
        addr: &str,
        request_timeout: std::time::Duration,
    ) -> Result<Self, ClientError> {
        let inner = Arc::new(Inner::new(addr.to_string(), request_timeout));

        // Spawn connection manager
        conn::spawn_connection(inner.clone());

        // Wait for first connection (with timeout)
        let mut state_rx = inner.state_tx.subscribe();
        let connected = tokio::time::timeout(request_timeout, async {
            loop {
                if *state_rx.borrow_and_update() == ConnState::Connected {
                    return true;
                }
                if state_rx.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await;

        match connected {
            Ok(true) => Ok(Self { inner }),
            _ => Err(ClientError::Timeout),
        }
    }

    // ── Stream management ────────────────────────────────────────

    /// Create a stream.
    pub async fn create_stream(
        &self,
        name: &[u8],
        max_msgs: u64,
        max_bytes: u64,
        max_age_secs: u64,
    ) -> Result<(), ClientError> {
        let fixed = CreateStreamFixed {
            name_len: U16::new(name.len() as u16),
            _pad: U16::new(0),
            max_msgs: U64::new(max_msgs),
            max_bytes: U64::new(max_bytes),
            max_age_secs: U64::new(max_age_secs),
            replicas: 1,
            journal_kind: 0, // memory
            retention: 0,    // limits
            _pad2: 0,
        };

        let mut body = Vec::with_capacity(32 + name.len());
        body.extend_from_slice(fixed.as_bytes());
        body.extend_from_slice(name);

        self.inner.request(Action::CreateStream, 0, &body).await?;
        Ok(())
    }

    /// Delete a stream by name.
    pub async fn delete_stream(&self, name: &[u8]) -> Result<(), ClientError> {
        let fixed = DeleteStreamFixed {
            name_len: U16::new(name.len() as u16),
            _pad: [0u8; 6],
        };

        let mut body = Vec::with_capacity(8 + name.len());
        body.extend_from_slice(fixed.as_bytes());
        body.extend_from_slice(name);

        let stream_id = fnv1a_32(name);
        self.inner.request(Action::DeleteStream, stream_id, &body).await?;
        Ok(())
    }

    // ── Consumer management ──────────────────────────────────────

    /// Create a consumer on a stream. Returns a Consumer handle.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_consumer(
        &self,
        stream_name: &[u8],
        consumer_name: &[u8],
        subject: &[u8],
        max_inflight: u16,
        deliver_policy: u8,
        deliver_mode: u8,
        ack_wait_ms: u32,
    ) -> Result<Consumer, ClientError> {
        let stream_id = fnv1a_32(stream_name);

        let fixed = CreateConsumerFixed {
            name_len: U16::new(consumer_name.len() as u16),
            subj_len: U16::new(subject.len() as u16),
            stream_id: U32::new(stream_id),
            max_inflight: U16::new(max_inflight),
            deliver_policy,
            deliver_mode,
            ack_wait_ms: U32::new(ack_wait_ms),
            start_seq: U64::new(0),
        };

        let mut body = Vec::with_capacity(24 + consumer_name.len() + subject.len());
        body.extend_from_slice(fixed.as_bytes());
        body.extend_from_slice(consumer_name);
        body.extend_from_slice(subject);

        // ref_seq in RepOk carries the assigned consumer_id
        let consumer_id = self.inner.request(Action::CreateConsumer, stream_id, &body).await? as u32;

        Ok(Consumer::new(self.inner.clone(), consumer_id, stream_id))
    }

    /// Delete a consumer.
    pub async fn delete_consumer(
        &self,
        stream_name: &[u8],
        consumer_id: u32,
    ) -> Result<(), ClientError> {
        let stream_id = fnv1a_32(stream_name);

        let body = DeleteConsumerAction {
            consumer_id: U32::new(consumer_id),
            _pad: U32::new(0),
        };

        self.inner.request(Action::DeleteConsumer, stream_id, body.as_bytes()).await?;
        Ok(())
    }

    // ── Publish ──────────────────────────────────────────────────

    /// Publish a single message. Returns the assigned sequence number.
    pub async fn publish(
        &self,
        stream_name: &[u8],
        subject: &[u8],
        payload: &[u8],
    ) -> Result<u64, ClientError> {
        let stream_id = fnv1a_32(stream_name);

        // Build batch body: [2 count][entry_header][subject][payload]
        let entry = PublishEntry {
            data_len: U32::new(payload.len() as u32),
            subj_len: U16::new(subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0u8; 3],
        };

        let body_len = 2 + 12 + subject.len() + payload.len();
        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&1u16.to_le_bytes()); // count = 1
        body.extend_from_slice(entry.as_bytes());
        body.extend_from_slice(subject);
        body.extend_from_slice(payload);

        self.inner.request(Action::Publish, stream_id, &body).await
    }

    /// Publish a batch of (subject, payload) pairs. Returns first sequence.
    pub async fn publish_batch(
        &self,
        stream_name: &[u8],
        entries: &[(&[u8], &[u8])],
    ) -> Result<u64, ClientError> {
        let stream_id = fnv1a_32(stream_name);

        let total_body: usize = 2 + entries.iter()
            .map(|(s, p)| 12 + s.len() + p.len())
            .sum::<usize>();

        let mut body = Vec::with_capacity(total_body);
        body.extend_from_slice(&(entries.len() as u16).to_le_bytes());

        for (subject, payload) in entries {
            let entry = PublishEntry {
                data_len: U32::new(payload.len() as u32),
                subj_len: U16::new(subject.len() as u16),
                reply_len: U16::new(0),
                flags: 0,
                _pad: [0u8; 3],
            };
            body.extend_from_slice(entry.as_bytes());
            body.extend_from_slice(subject);
            body.extend_from_slice(payload);
        }

        self.inner.request(Action::Publish, stream_id, &body).await
    }

    /// Fire-and-forget publish — no reply expected.
    pub fn publish_fire_forget(
        &self,
        stream_name: &[u8],
        subject: &[u8],
        payload: &[u8],
    ) {
        let stream_id = fnv1a_32(stream_name);

        let entry = PublishEntry {
            data_len: U32::new(payload.len() as u32),
            subj_len: U16::new(subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0u8; 3],
        };

        let body_len = 2 + 12 + subject.len() + payload.len();
        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&1u16.to_le_bytes());
        body.extend_from_slice(entry.as_bytes());
        body.extend_from_slice(subject);
        body.extend_from_slice(payload);

        self.inner.fire_and_forget(Action::Publish, stream_id, &body);
    }

    // ── Connection state ─────────────────────────────────────────

    /// Get a receiver to watch connection state changes.
    pub fn on_state_change(&self) -> watch::Receiver<ConnState> {
        self.inner.state_tx.subscribe()
    }

    /// Check if currently connected.
    pub fn is_connected(&self) -> bool {
        *self.inner.state_tx.borrow() == ConnState::Connected
    }
}
