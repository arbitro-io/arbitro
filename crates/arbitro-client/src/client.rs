//! Client — public API for interacting with the arbitro broker.

use std::sync::Arc;

use tokio::sync::watch;
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use arbitro_proto::action::Action;
use arbitro_proto::config::{ConsumerConfig, StreamConfig, wire_hash_32};
use arbitro_proto::error::ErrorCode;
use arbitro_proto::wire::manager::{CreateConsumerFixed, DeleteConsumerAction, ListStreamsAction};
use arbitro_proto::wire::publish::PublishEntry;
use arbitro_proto::wire::stream::{CreateStreamFixed, DeleteStreamFixed};

use crate::conn;
use crate::consumer::Consumer;
use crate::error::ClientError;
use crate::inner::{ConnState, Inner};

/// Client for the arbitro message broker.
#[derive(Clone)]
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

    /// Create a stream from config.
    pub async fn create_stream(&self, config: &StreamConfig) -> Result<(), ClientError> {
        let fixed = CreateStreamFixed {
            name_len: U16::new(config.name.len() as u16),
            filter_len: U16::new(config.filter.len() as u16),
            max_msgs: U64::new(config.max_msgs),
            max_bytes: U64::new(config.max_bytes),
            max_age_secs: U64::new(config.max_age_secs),
            replicas: config.replicas,
            journal_kind: config.journal_kind as u8,
            retention: config.retention as u8,
            discard: config.discard as u8,
        };

        let mut body = Vec::with_capacity(32 + config.name.len() + config.filter.len());
        body.extend_from_slice(fixed.as_bytes());
        body.extend_from_slice(&config.name);
        body.extend_from_slice(&config.filter);

        self.inner.request(Action::CreateStream, 0, &body).await?;
        Ok(())
    }

    /// Create a stream if it does not exist. Idempotent — treats
    /// `StreamAlreadyExists` as success, so the caller gets a stable
    /// "stream is ready" guarantee without hand-rolling the retry.
    ///
    /// Use the raw `create_stream` if you need a hard failure on config
    /// drift (e.g. CI bootstraps against a clean broker).
    ///
    /// See `.agent/rules/features-invariants.md` §Client API.
    pub async fn get_or_create_stream(&self, config: &StreamConfig) -> Result<(), ClientError> {
        match self.create_stream(config).await {
            Ok(()) => Ok(()),
            Err(ClientError::Broker(ErrorCode::StreamAlreadyExists)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// List all streams. Returns stream info entries.
    pub async fn list_streams(&self) -> Result<Vec<StreamInfo>, ClientError> {
        let body = ListStreamsAction {
            offset: U32::new(0),
            limit: U32::new(u32::MAX),
        };

        let raw = self.inner.request_body(Action::ListStreams, 0, body.as_bytes()).await?;
        Ok(parse_list_streams(&raw))
    }

    /// List all consumers. Returns consumer info entries.
    pub async fn list_consumers(&self) -> Result<Vec<ConsumerInfo>, ClientError> {
        let body = ListStreamsAction {
            offset: U32::new(0),
            limit: U32::new(u32::MAX),
        };

        let raw = self.inner.request_body(Action::ListConsumers, 0, body.as_bytes()).await?;
        Ok(parse_list_consumers(&raw))
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

        let stream_id = wire_hash_32(name);
        self.inner.request(Action::DeleteStream, stream_id, &body).await?;
        Ok(())
    }

    // ── Consumer management ──────────────────────────────────────

    /// Create a consumer from config. Returns a Consumer handle.
    pub async fn create_consumer(&self, config: &ConsumerConfig) -> Result<Consumer, ClientError> {
        let stream_id = config.stream_id;

        // Serialize first filter as subject (empty if no filters).
        let subject = config.filters.first().map(|f| f.as_ref()).unwrap_or(b"");

        let fixed = CreateConsumerFixed {
            name_len: U16::new(config.name.len() as u16),
            subj_len: U16::new(subject.len() as u16),
            stream_id: U32::new(stream_id),
            max_inflight: U16::new(config.max_inflight),
            ack_policy: config.ack_policy as u8,
            deliver_policy: config.deliver_policy as u8,
            deliver_mode: config.deliver_mode as u8,
            _pad: 0,
            group_len: U16::new(config.group.len() as u16),
            ack_wait_ms: U32::new(config.ack_wait_ms),
            start_seq: U64::new(config.start_seq),
        };

        let mut body = Vec::with_capacity(28 + config.name.len() + config.group.len() + subject.len());
        body.extend_from_slice(fixed.as_bytes());
        body.extend_from_slice(&config.name);
        body.extend_from_slice(&config.group);
        body.extend_from_slice(subject);

        // Variable trailer: [2 num_limits] + per limit: [4 limit][2 pattern_len][pattern]
        body.extend_from_slice(&(config.max_subject_inflights.len() as u16).to_le_bytes());
        for sl in config.max_subject_inflights.iter() {
            body.extend_from_slice(&sl.limit.to_le_bytes());
            body.extend_from_slice(&(sl.pattern.len() as u16).to_le_bytes());
            body.extend_from_slice(&sl.pattern);
        }

        let consumer_id = self.inner.request(Action::CreateConsumer, stream_id, &body).await? as u32;

        Ok(Consumer::new(self.inner.clone(), consumer_id, stream_id))
    }

    /// Create a consumer if one with that name does not exist. Idempotent —
    /// returns the new `Consumer` on success, or `None` if the consumer name
    /// was already taken (use the `ConsumerId` you received from your
    /// original `create_consumer` call).
    ///
    /// **Invariant reminder:** `ConsumerId` is keyed by name across the
    /// entire process. Two tenants sharing a name share inflight counters,
    /// pause state, and `max_subject_inflight` bookkeeping — this helper
    /// does NOT protect you from that. Always scope consumer names.
    ///
    /// See `.agent/rules/features-invariants.md` §Identity model.
    pub async fn get_or_create_consumer(
        &self,
        config: &ConsumerConfig,
    ) -> Result<Option<Consumer>, ClientError> {
        match self.create_consumer(config).await {
            Ok(consumer) => Ok(Some(consumer)),
            Err(ClientError::Broker(ErrorCode::ConsumerAlreadyExists)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Delete a consumer.
    pub async fn delete_consumer(
        &self,
        stream_name: &[u8],
        consumer_id: u32,
    ) -> Result<(), ClientError> {
        let stream_id = wire_hash_32(stream_name);

        let body = DeleteConsumerAction {
            consumer_id: U32::new(consumer_id),
            _pad: U32::new(0),
        };

        self.inner.request(Action::DeleteConsumer, stream_id, body.as_bytes()).await?;
        Ok(())
    }

    // ── Publish ──────────────────────────────────────────────────

    /// Publish a single message (fire-and-forget — no round-trip wait).
    pub async fn publish(
        &self,
        stream_name: &[u8],
        subject: &[u8],
        payload: &[u8],
    ) -> Result<(), ClientError> {
        let stream_id = wire_hash_32(stream_name);

        // Build batch body: [2 count][entry_header][subject][payload]
        let entry = PublishEntry {
            data_len: U32::new(payload.len() as u32),
            subj_len: U16::new(subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0u8; 3],
        };

        let body_len = 4 + 12 + subject.len() + payload.len();
        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&1u32.to_le_bytes()); // count = 1 (u32, see proto/wire/publish.rs)
        body.extend_from_slice(entry.as_bytes());
        body.extend_from_slice(subject);
        body.extend_from_slice(payload);

        self.inner.fire_and_forget(Action::Publish, stream_id, &body).await
    }

    /// Publish a batch of (subject, payload) pairs (fire-and-forget).
    pub async fn publish_batch(
        &self,
        stream_name: &[u8],
        entries: &[(&[u8], &[u8])],
    ) -> Result<(), ClientError> {
        let stream_id = wire_hash_32(stream_name);

        let total_body: usize = 4 + entries.iter()
            .map(|(s, p)| 12 + s.len() + p.len())
            .sum::<usize>();

        let mut body = Vec::with_capacity(total_body);
        body.extend_from_slice(&(entries.len() as u32).to_le_bytes());

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

        self.inner.fire_and_forget(Action::Publish, stream_id, &body).await
    }

    /// Publish a single message via server-side accumulation (fire-and-forget).
    /// The server batches entries and flushes with append_batch after 5ms or 1024 entries.
    pub async fn publish_accumulate(
        &self,
        stream_name: &[u8],
        subject: &[u8],
        payload: &[u8],
    ) -> Result<(), ClientError> {
        let stream_id = wire_hash_32(stream_name);

        let entry = PublishEntry {
            data_len: U32::new(payload.len() as u32),
            subj_len: U16::new(subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0u8; 3],
        };

        let body_len = 4 + 12 + subject.len() + payload.len();
        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&1u32.to_le_bytes());
        body.extend_from_slice(entry.as_bytes());
        body.extend_from_slice(subject);
        body.extend_from_slice(payload);

        self.inner.fire_and_forget(Action::PublishAccumulate, stream_id, &body).await
    }

    /// Publish a single message via server-side accumulation and wait for confirmation.
    /// Returns the assigned sequence number after the server flushes the batch.
    pub async fn publish_accumulate_sync(
        &self,
        stream_name: &[u8],
        subject: &[u8],
        payload: &[u8],
    ) -> Result<u64, ClientError> {
        let stream_id = wire_hash_32(stream_name);

        let entry = PublishEntry {
            data_len: U32::new(payload.len() as u32),
            subj_len: U16::new(subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0u8; 3],
        };

        let body_len = 4 + 12 + subject.len() + payload.len();
        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&1u32.to_le_bytes());
        body.extend_from_slice(entry.as_bytes());
        body.extend_from_slice(subject);
        body.extend_from_slice(payload);

        self.inner.request(Action::PublishAccumulate, stream_id, &body).await
    }

    /// Publish a single message and wait for server confirmation.
    /// Returns the assigned sequence number.
    pub async fn publish_sync(
        &self,
        stream_name: &[u8],
        subject: &[u8],
        payload: &[u8],
    ) -> Result<u64, ClientError> {
        let stream_id = wire_hash_32(stream_name);

        let entry = PublishEntry {
            data_len: U32::new(payload.len() as u32),
            subj_len: U16::new(subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0u8; 3],
        };

        let body_len = 4 + 12 + subject.len() + payload.len();
        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&1u32.to_le_bytes());
        body.extend_from_slice(entry.as_bytes());
        body.extend_from_slice(subject);
        body.extend_from_slice(payload);

        self.inner.request(Action::Publish, stream_id, &body).await
    }

    /// Publish a batch and wait for server confirmation.
    /// Returns the first assigned sequence number.
    pub async fn publish_batch_sync(
        &self,
        stream_name: &[u8],
        entries: &[(&[u8], &[u8])],
    ) -> Result<u64, ClientError> {
        let stream_id = wire_hash_32(stream_name);

        let total_body: usize = 4 + entries.iter()
            .map(|(s, p)| 12 + s.len() + p.len())
            .sum::<usize>();

        let mut body = Vec::with_capacity(total_body);
        body.extend_from_slice(&(entries.len() as u32).to_le_bytes());

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

/// Stream info returned by `list_streams()`.
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub stream_id: u32,
    pub name: Vec<u8>,
}

/// Parse ListStreams response body: [4B count][N × (4B stream_id, 2B name_len, name)]
fn parse_list_streams(body: &[u8]) -> Vec<StreamInfo> {
    if body.len() < 4 {
        return vec![];
    }
    let count = u32::from_le_bytes([body[0], body[1], body[2], body[3]]) as usize;
    let mut out = Vec::with_capacity(count);
    let mut pos = 4;

    for _ in 0..count {
        if pos + 6 > body.len() {
            break;
        }
        let stream_id = u32::from_le_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
        let name_len = u16::from_le_bytes([body[pos + 4], body[pos + 5]]) as usize;
        pos += 6;

        if pos + name_len > body.len() {
            break;
        }
        let name = body[pos..pos + name_len].to_vec();
        pos += name_len;

        out.push(StreamInfo { stream_id, name });
    }

    out
}

/// Consumer info returned by `list_consumers()`.
#[derive(Debug, Clone)]
pub struct ConsumerInfo {
    pub consumer_id: u32,
    pub stream_id: u32,
    pub queue_id: u32,
    pub paused: bool,
}

/// Parse ListConsumers response body: [4B count][N × (4B consumer_id, 4B stream_id, 4B queue_id, 1B paused)]
fn parse_list_consumers(body: &[u8]) -> Vec<ConsumerInfo> {
    if body.len() < 4 {
        return vec![];
    }
    let count = u32::from_le_bytes([body[0], body[1], body[2], body[3]]) as usize;
    let mut out = Vec::with_capacity(count);
    let mut pos = 4;

    for _ in 0..count {
        if pos + 13 > body.len() {
            break;
        }
        let consumer_id = u32::from_le_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
        let stream_id = u32::from_le_bytes([body[pos + 4], body[pos + 5], body[pos + 6], body[pos + 7]]);
        let queue_id = u32::from_le_bytes([body[pos + 8], body[pos + 9], body[pos + 10], body[pos + 11]]);
        let paused = body[pos + 12] != 0;
        pos += 13;

        out.push(ConsumerInfo { consumer_id, stream_id, queue_id, paused });
    }

    out
}
