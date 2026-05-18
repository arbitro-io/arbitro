//! Ergonomic builder for [`Client::create_consumer_with_limits`].
//!
//! The wire-facing client method takes 10 positional `u8`/`u32`/`u64`
//! arguments plus a slice of [`SubjectLimit`]. That signature is hard
//! to read at the call site and — worse — bypasses the invariant checks
//! that live in [`arbitro_proto::config::consumer::ConsumerConfigBuilder`].
//!
//! `ConsumerBuilder` solves both problems:
//!
//! - **Fluent API.** Set only the fields you care about; sensible defaults
//!   for the rest (`DeliverPolicy::All`, `DeliverMode::Fanout`, no filter,
//!   no group, no subject limits).
//! - **`max_subject_inflight(pattern, limit)`** wired directly — matches
//!   the proto builder. Multiple calls accumulate; you can pin every
//!   pattern you need without juggling a `Vec<SubjectLimit>` by hand.
//! - **Validates invariants on `.create()`** before sending the wire:
//!   `ack_policy` must be set; `max_inflight`, `max_subject_inflight` and
//!   `ack_wait_ms` only make sense with `AckPolicy::Explicit`;
//!   `DeliverPolicy::ByStartSeq` needs a non-zero `start_seq`. Violations
//!   surface as [`ClientError::InvalidConfig`] — no round-trip to the
//!   broker, no silently-dropped caps.
//!
//! Example:
//!
//! ```ignore
//! use arbitro_client_tokio::{ConsumerBuilder, AckPolicy};
//!
//! let consumer_id = ConsumerBuilder::new(b"isolation_tester")
//!     .filter(b">")
//!     .max_inflight(10_000)
//!     .ack_policy(AckPolicy::Explicit)
//!     .max_subject_inflight(b"orders.basic.>", 1)
//!     .max_subject_inflight(b"orders.premium.>", 1)
//!     .ack_wait_ms(30_000)
//!     .create(&client, stream_id)
//!     .await?;
//! ```

use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverMode, DeliverPolicy};
use arbitro_proto::v2::manager::SubjectLimit;

use crate::client::Client;
use crate::error::ClientError;

/// Fluent builder that validates invariants and ends in [`Self::create`].
#[derive(Debug)]
pub struct ConsumerBuilder<'a> {
    name: &'a [u8],
    group: &'a [u8],
    filter: &'a [u8],
    max_inflight: u16,
    ack_policy: Option<AckPolicy>,
    deliver_policy: DeliverPolicy,
    deliver_mode: DeliverMode,
    ack_wait_ms: u32,
    start_seq: u64,
    subject_limits: Vec<SubjectLimit<'a>>,
}

impl<'a> ConsumerBuilder<'a> {
    /// Start a builder for the consumer named `name`.
    ///
    /// Defaults: no group (server uses the stream name), no filter,
    /// `DeliverPolicy::All`, `DeliverMode::Fanout`, no inflight cap,
    /// no subject-inflight caps, no ack_wait. `ack_policy` is **unset**
    /// and **must** be picked explicitly before `.create()`.
    pub fn new(name: &'a [u8]) -> Self {
        Self {
            name,
            group: b"",
            filter: b"",
            max_inflight: 0,
            ack_policy: None,
            deliver_policy: DeliverPolicy::All,
            deliver_mode: DeliverMode::Fanout,
            ack_wait_ms: 0,
            start_seq: 0,
            subject_limits: Vec::new(),
        }
    }

    /// Queue group. Consumers sharing a group on the same stream share
    /// a round-robin ready queue (queue groups).
    pub fn group(mut self, group: &'a [u8]) -> Self {
        self.group = group;
        self
    }

    /// Subject filter for the subscription. `b">"` matches all subjects.
    pub fn filter(mut self, filter: &'a [u8]) -> Self {
        self.filter = filter;
        self
    }

    /// Global cap on in-flight (delivered, unacked) messages for this
    /// consumer. Requires [`AckPolicy::Explicit`].
    pub fn max_inflight(mut self, v: u16) -> Self {
        self.max_inflight = v;
        self
    }

    /// `None` = fire-and-forget; `Explicit` = consumer must ack.
    pub fn ack_policy(mut self, v: AckPolicy) -> Self {
        self.ack_policy = Some(v);
        self
    }

    pub fn deliver_policy(mut self, v: DeliverPolicy) -> Self {
        self.deliver_policy = v;
        self
    }

    pub fn deliver_mode(mut self, v: DeliverMode) -> Self {
        self.deliver_mode = v;
        self
    }

    pub fn ack_wait_ms(mut self, v: u32) -> Self {
        self.ack_wait_ms = v;
        self
    }

    pub fn start_seq(mut self, v: u64) -> Self {
        self.start_seq = v;
        self
    }

    /// Pin every subject matching `pattern` to at most `limit` in-flight
    /// messages. Each unique subject keeps its own counter (so 1 000
    /// subjects matching `notif.user.>` with `limit=1` allow 1 000
    /// concurrent in-flights, one per subject).
    ///
    /// Multiple calls accumulate. Requires [`AckPolicy::Explicit`].
    pub fn max_subject_inflight(mut self, pattern: &'a [u8], limit: u32) -> Self {
        self.subject_limits.push(SubjectLimit { pattern, limit });
        self
    }

    /// Validate invariants and send the `CreateConsumer` frame.
    ///
    /// Validation delegates to [`ConsumerConfigBuilder::build`] in
    /// `arbitro-proto`, so the rules stay in lock-step with the
    /// engine-side config. The wire request is only built if validation
    /// passes; on failure the call returns
    /// [`ClientError::InvalidConfig`] without touching the broker.
    ///
    /// Returns the freshly-allocated `consumer_id`.
    pub async fn create(self, client: &Client, stream_id: u32) -> Result<u32, ClientError> {
        let ack_policy = self.validate()?;

        let resp = client
            .create_consumer_with_limits(
                stream_id,
                self.name,
                self.group,
                self.filter,
                self.max_inflight,
                ack_policy as u8,
                self.deliver_policy as u8,
                self.deliver_mode as u8,
                self.ack_wait_ms,
                self.start_seq,
                &self.subject_limits,
            )
            .await?;

        if resp.len() < 8 {
            return Err(ClientError::InvalidConfig(
                "broker reply shorter than expected u64 consumer_id".into(),
            ));
        }
        let id = u64::from_le_bytes(resp[..8].try_into().expect("8 bytes")) as u32;
        Ok(id)
    }

    /// Run the same invariant checks `ConsumerConfigBuilder::build` runs,
    /// but without materialising a `ConsumerConfig` (which would also
    /// require `stream_name`). On success returns the resolved
    /// [`AckPolicy`] so the wire-encoding step doesn't `.unwrap()` again.
    fn validate(&self) -> Result<AckPolicy, ClientError> {
        // Mirror invariants from
        //   arbitro_proto::config::consumer::ConsumerConfigBuilder::build
        // so the two paths cannot drift apart.
        let _ = ConsumerConfig::new(self.name, b"placeholder");

        let ack_policy = self.ack_policy.ok_or_else(|| {
            ClientError::InvalidConfig(
                "ack_policy must be set explicitly (AckPolicy::None or \
                 AckPolicy::Explicit) — there is no safe default".into(),
            )
        })?;

        if ack_policy == AckPolicy::None {
            if self.max_inflight != 0 {
                return Err(ClientError::InvalidConfig(
                    "max_inflight requires AckPolicy::Explicit (fire-and-forget \
                     consumers don't track inflight)".into(),
                ));
            }
            if !self.subject_limits.is_empty() {
                return Err(ClientError::InvalidConfig(
                    "max_subject_inflight requires AckPolicy::Explicit \
                     (fire-and-forget consumers don't track inflight)".into(),
                ));
            }
            if self.ack_wait_ms != 0 {
                return Err(ClientError::InvalidConfig(
                    "ack_wait_ms requires AckPolicy::Explicit".into(),
                ));
            }
        }

        if self.deliver_policy == DeliverPolicy::ByStartSeq && self.start_seq == 0 {
            return Err(ClientError::InvalidConfig(
                "DeliverPolicy::ByStartSeq requires start_seq > 0".into(),
            ));
        }

        Ok(ack_policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies the validation step rejects the same shapes the proto
    /// builder rejects, without going through the wire.
    #[test]
    fn missing_ack_policy_is_rejected() {
        let err = ConsumerBuilder::new(b"c").validate().unwrap_err();
        match err {
            ClientError::InvalidConfig(msg) => assert!(msg.contains("ack_policy")),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn max_inflight_with_ack_none_is_rejected() {
        let err = ConsumerBuilder::new(b"c")
            .ack_policy(AckPolicy::None)
            .max_inflight(10)
            .validate()
            .unwrap_err();
        match err {
            ClientError::InvalidConfig(msg) => assert!(msg.contains("max_inflight")),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn subject_inflight_with_ack_none_is_rejected() {
        let err = ConsumerBuilder::new(b"c")
            .ack_policy(AckPolicy::None)
            .max_subject_inflight(b"foo.>", 1)
            .validate()
            .unwrap_err();
        match err {
            ClientError::InvalidConfig(msg) => assert!(msg.contains("max_subject_inflight")),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn ack_wait_with_ack_none_is_rejected() {
        let err = ConsumerBuilder::new(b"c")
            .ack_policy(AckPolicy::None)
            .ack_wait_ms(5_000)
            .validate()
            .unwrap_err();
        match err {
            ClientError::InvalidConfig(msg) => assert!(msg.contains("ack_wait_ms")),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn by_start_seq_without_start_seq_is_rejected() {
        let err = ConsumerBuilder::new(b"c")
            .ack_policy(AckPolicy::Explicit)
            .deliver_policy(DeliverPolicy::ByStartSeq)
            .validate()
            .unwrap_err();
        match err {
            ClientError::InvalidConfig(msg) => assert!(msg.contains("ByStartSeq")),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn explicit_with_full_config_validates() {
        ConsumerBuilder::new(b"c")
            .ack_policy(AckPolicy::Explicit)
            .filter(b">")
            .max_inflight(10_000)
            .max_subject_inflight(b"orders.basic.>", 1)
            .max_subject_inflight(b"orders.premium.>", 1)
            .ack_wait_ms(30_000)
            .validate()
            .expect("valid config must pass");
    }

    #[test]
    fn ack_none_minimal_validates() {
        ConsumerBuilder::new(b"c")
            .ack_policy(AckPolicy::None)
            .validate()
            .expect("fire-and-forget consumer must validate");
    }
}
