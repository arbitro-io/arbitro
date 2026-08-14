//! Ergonomic builder for [`Client::create_stream`].
//!
//! `create_stream` takes ten positional arguments, three of which are `u8`
//! enums flattened to raw numbers. A call site reads
//!
//! ```ignore
//! client.create_stream(b"orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0).await?;
//! ```
//!
//! which is unreadable and unreviewable: the lone `1` is `replicas`, and the
//! zeros mean "unlimited" in three places and "the default variant" in three
//! others. Nothing in the signature distinguishes them.
//!
//! This mirrors [`crate::consumer_builder::ConsumerBuilder`]:
//!
//! - **Defaults match `arbitro_proto::config::StreamConfig::new`** — no
//!   limits, one replica, `Memory` journal, `Limits` retention, `Old`
//!   discard. Both paths start from the same place on purpose.
//! - **Typed enums**, not `u8`. [`JournalKind`], [`RetentionPolicy`] and
//!   [`DiscardPolicy`] are re-exported from the crate root.
//! - **Validated locally.** The invariants `StreamConfig` documents but
//!   never enforces — name is a non-empty identifier of at most 255 bytes,
//!   filter is required, at least one replica — are checked here. Violations
//!   surface as [`ClientError::InvalidConfig`] with no round-trip.
//! - Ends in [`Self::create`] (fails if the stream exists) or
//!   [`Self::upsert`] (creates, or succeeds if an identical one is there).
//!
//! Example:
//!
//! ```ignore
//! use arbitro_client_tokio::{DiscardPolicy, JournalKind, StreamBuilder};
//!
//! let stream_id = StreamBuilder::new(b"orders")
//!     .filter(b"orders.>")
//!     .max_msgs(1_000_000)
//!     .journal_kind(JournalKind::Tolerant)
//!     .discard(DiscardPolicy::New)
//!     .create(&client)
//!     .await?;
//! ```

use arbitro_proto::config::{DiscardPolicy, JournalKind, RetentionPolicy};

use crate::client::Client;
use crate::error::ClientError;

/// Longest stream name the broker accepts, per `StreamConfig`'s contract.
const MAX_NAME_LEN: usize = 255;

/// Fluent builder that validates invariants and ends in [`Self::create`]
/// or [`Self::upsert`].
#[derive(Debug)]
pub struct StreamBuilder<'a> {
    name: &'a [u8],
    filter: &'a [u8],
    max_msgs: u64,
    max_bytes: u64,
    max_age_secs: u64,
    replicas: u8,
    journal_kind: JournalKind,
    retention: RetentionPolicy,
    discard: DiscardPolicy,
    idempotency_window_ms: u32,
}

impl<'a> StreamBuilder<'a> {
    /// Start building a stream named `name`.
    ///
    /// Defaults mirror `arbitro_proto::config::StreamConfig::new`: no
    /// `max_msgs` / `max_bytes` / `max_age_secs` limit, one replica,
    /// [`JournalKind::Memory`], [`RetentionPolicy::Limits`],
    /// [`DiscardPolicy::Old`], and no idempotency window.
    ///
    /// The filter defaults to `b">"` — capture everything — because a
    /// stream without one captures nothing, and an empty default would
    /// silently create a dead stream.
    pub fn new(name: &'a [u8]) -> Self {
        Self {
            name,
            filter: b">",
            max_msgs: 0,
            max_bytes: 0,
            max_age_secs: 0,
            replicas: 1,
            journal_kind: JournalKind::Memory,
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
            idempotency_window_ms: 0,
        }
    }

    /// Subject pattern this stream captures. `b">"` captures everything.
    ///
    /// No two streams may have overlapping filters — the broker rejects
    /// the second one.
    pub fn filter(mut self, filter: &'a [u8]) -> Self {
        self.filter = filter;
        self
    }

    /// Keep at most this many messages. `0` (default) means unlimited.
    pub fn max_msgs(mut self, v: u64) -> Self {
        self.max_msgs = v;
        self
    }

    /// Keep at most this many bytes. `0` (default) means unlimited.
    pub fn max_bytes(mut self, v: u64) -> Self {
        self.max_bytes = v;
        self
    }

    /// Per-message TTL in seconds. `0` (default) disables expiry.
    ///
    /// Expiry is lazy: a message past its age is treated as absent on the
    /// next read and removed on the next mutating op — there is no sweeper.
    pub fn max_age_secs(mut self, v: u64) -> Self {
        self.max_age_secs = v;
        self
    }

    /// Replica count. Defaults to `1`; `0` is rejected by [`Self::create`].
    pub fn replicas(mut self, v: u8) -> Self {
        self.replicas = v;
        self
    }

    /// Where the journal lives. Defaults to [`JournalKind::Memory`].
    pub fn journal_kind(mut self, v: JournalKind) -> Self {
        self.journal_kind = v;
        self
    }

    /// When messages become eligible for removal. Defaults to
    /// [`RetentionPolicy::Limits`].
    pub fn retention(mut self, v: RetentionPolicy) -> Self {
        self.retention = v;
        self
    }

    /// What a publish does once `max_msgs` / `max_bytes` is met. Defaults
    /// to [`DiscardPolicy::Old`] (ring buffer); [`DiscardPolicy::New`]
    /// rejects the publish so the producer sees backpressure.
    pub fn discard(mut self, v: DiscardPolicy) -> Self {
        self.discard = v;
        self
    }

    /// Window in which a repeated `msg_id` is deduplicated. `0` (default)
    /// disables dedup.
    pub fn idempotency_window_ms(mut self, v: u32) -> Self {
        self.idempotency_window_ms = v;
        self
    }

    /// Create the stream. Returns its `stream_id`.
    ///
    /// Fails if a stream with this name already exists — use
    /// [`Self::upsert`] when that is acceptable.
    pub async fn create(self, client: &Client) -> Result<u32, ClientError> {
        self.validate()?;

        let resp = client
            .create_stream(
                self.name,
                self.filter,
                self.max_msgs,
                self.max_bytes,
                self.max_age_secs,
                self.replicas,
                self.journal_kind as u8,
                self.retention as u8,
                self.discard as u8,
                self.idempotency_window_ms,
            )
            .await?;

        if resp.len() < 8 {
            return Err(ClientError::InvalidConfig(
                "broker reply shorter than expected u64 stream_id".into(),
            ));
        }
        Ok(u64::from_le_bytes(resp[..8].try_into().expect("8 bytes")) as u32)
    }

    /// Create the stream, or succeed if one with this configuration is
    /// already there. Returns `true` when this call created it.
    pub async fn upsert(self, client: &Client) -> Result<bool, ClientError> {
        self.validate()?;

        client
            .upsert_stream(
                self.name,
                self.filter,
                self.max_msgs,
                self.max_bytes,
                self.max_age_secs,
                self.replicas,
                self.journal_kind as u8,
                self.retention as u8,
                self.discard as u8,
                self.idempotency_window_ms,
            )
            .await
    }

    /// Enforce the invariants `StreamConfig` documents.
    ///
    /// `StreamConfigBuilder::build` states them in prose and checks none of
    /// them — it only computes `wire_hash_32(name)`. Catching them here
    /// turns a broker round-trip (or worse, a stream that captures nothing)
    /// into a local error.
    fn validate(&self) -> Result<(), ClientError> {
        if self.name.is_empty() {
            return Err(ClientError::InvalidConfig("stream name is empty".into()));
        }
        if self.name.len() > MAX_NAME_LEN {
            return Err(ClientError::InvalidConfig(
                format!(
                    "stream name is {} bytes, max is {MAX_NAME_LEN}",
                    self.name.len()
                )
                .into(),
            ));
        }
        if let Some(bad) = self
            .name
            .iter()
            .find(|b| !(b.is_ascii_alphanumeric() || **b == b'_' || **b == b'-'))
        {
            return Err(ClientError::InvalidConfig(
                format!(
                    "stream name must match [a-zA-Z0-9_-], found byte {bad:#04x}"
                )
                .into(),
            ));
        }
        if self.filter.is_empty() {
            return Err(ClientError::InvalidConfig(
                "stream filter is empty — the stream would capture nothing; \
                 use b\">\" to capture everything"
                    .into(),
            ));
        }
        if self.replicas == 0 {
            return Err(ClientError::InvalidConfig(
                "replicas is 0 — a stream needs at least one".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Defaults must not drift from `StreamConfig::new`, which is the
    /// server-side source of truth for what "unset" means.
    #[test]
    fn defaults_match_the_proto_config() {
        let b = StreamBuilder::new(b"orders");
        let proto = arbitro_proto::config::StreamConfig::new(b"orders", b">").build();

        assert_eq!(b.max_msgs, proto.max_msgs);
        assert_eq!(b.max_bytes, proto.max_bytes);
        assert_eq!(b.max_age_secs, proto.max_age_secs);
        assert_eq!(b.replicas, proto.replicas);
        assert_eq!(b.journal_kind, proto.journal_kind);
        assert_eq!(b.retention, proto.retention);
        assert_eq!(b.discard, proto.discard);
    }

    #[test]
    fn default_filter_captures_everything() {
        assert_eq!(StreamBuilder::new(b"orders").filter, b">");
    }

    #[test]
    fn rejects_empty_name() {
        assert!(StreamBuilder::new(b"").validate().is_err());
    }

    #[test]
    fn rejects_name_over_255_bytes() {
        let long = vec![b'a'; MAX_NAME_LEN + 1];
        assert!(StreamBuilder::new(&long).validate().is_err());
        let ok = vec![b'a'; MAX_NAME_LEN];
        assert!(StreamBuilder::new(&ok).validate().is_ok());
    }

    #[test]
    fn rejects_non_identifier_name() {
        // A dot is the subject separator; allowing it in a name invites
        // confusing a stream with the pattern it captures.
        assert!(StreamBuilder::new(b"orders.eu").validate().is_err());
        assert!(StreamBuilder::new(b"orders eu").validate().is_err());
        assert!(StreamBuilder::new(b"orders_eu-2").validate().is_ok());
    }

    #[test]
    fn rejects_empty_filter() {
        assert!(StreamBuilder::new(b"orders").filter(b"").validate().is_err());
    }

    #[test]
    fn rejects_zero_replicas() {
        assert!(StreamBuilder::new(b"orders").replicas(0).validate().is_err());
    }

    #[test]
    fn enums_encode_to_the_wire_values_the_broker_expects() {
        assert_eq!(JournalKind::Memory as u8, 0);
        assert_eq!(JournalKind::Disk as u8, 1);
        assert_eq!(JournalKind::Tolerant as u8, 2);
        assert_eq!(RetentionPolicy::Limits as u8, 0);
        assert_eq!(RetentionPolicy::Interest as u8, 1);
        assert_eq!(RetentionPolicy::WorkQueue as u8, 2);
        assert_eq!(DiscardPolicy::Old as u8, 0);
        assert_eq!(DiscardPolicy::New as u8, 1);
    }
}
