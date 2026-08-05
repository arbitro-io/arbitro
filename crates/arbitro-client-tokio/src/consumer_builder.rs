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
//!   for the rest (`DeliverPolicy::All`, `DeliverMode::Queue`, no filter,
//!   no subject limits). The queue group defaults to the consumer name
//!   (then the stream name, if you supplied one) — never to empty.
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
use crate::group::resolve_group;

/// Fluent builder that validates invariants and ends in [`Self::create`].
#[derive(Debug)]
pub struct ConsumerBuilder<'a> {
    name: &'a [u8],
    group: &'a [u8],
    stream_name: &'a [u8],
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
    /// Defaults: no filter, `DeliverPolicy::All`, `DeliverMode::Queue`,
    /// no inflight cap, no subject-inflight caps, no ack_wait.
    /// `ack_policy` is **unset** and **must** be picked explicitly before
    /// `.create()`.
    ///
    /// `DeliverMode::Queue` is the default in every client (TS, Go, Rust,
    /// C) so the same call means the same thing in every language. It is
    /// also the safer of the two: a caller who sets a group obviously
    /// wants a queue, and a caller who sets nothing gets work SHARED
    /// across consumers rather than silently duplicated N times. Fanout
    /// stays available — via [`Self::deliver_mode`] — but has to be asked
    /// for. A lone consumer is a queue group of one, so the default costs
    /// single-consumer setups nothing.
    ///
    /// The queue group defaults to `name` — resolved at `.create()` time,
    /// so a [`Self::group`] call in any order still wins. It is NOT left
    /// empty: the broker does not default it (the stream-name fallback in
    /// `arbitro-proto`'s `ConsumerConfigBuilder` is unreachable from the
    /// wire) and an empty group in `DeliverMode::Queue` keys one anonymous
    /// shared queue that every group-less consumer on the stream joins.
    /// Use [`Self::stream_name`] to supply the last fallback.
    pub fn new(name: &'a [u8]) -> Self {
        Self {
            name,
            group: b"",
            stream_name: b"",
            filter: b"",
            max_inflight: 0,
            ack_policy: None,
            deliver_policy: DeliverPolicy::All,
            deliver_mode: DeliverMode::Queue,
            ack_wait_ms: 0,
            start_seq: 0,
            subject_limits: Vec::new(),
        }
    }

    /// Queue group. Consumers sharing a group on the same stream share
    /// a round-robin ready queue (queue groups).
    ///
    /// Leaving this unset (or passing `b""`) defaults the group to the
    /// consumer name, then to [`Self::stream_name`].
    pub fn group(mut self, group: &'a [u8]) -> Self {
        self.group = group;
        self
    }

    /// Name of the stream this consumer will be created on — used only as
    /// the last fallback for the queue group, when neither a group nor a
    /// consumer name was given.
    ///
    /// `.create()` takes a numeric `stream_id`, so the builder cannot
    /// discover the name on its own; supply it here if you want the full
    /// group → name → stream-name chain.
    pub fn stream_name(mut self, stream_name: &'a [u8]) -> Self {
        self.stream_name = stream_name;
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

    /// How the broker fans messages out. Defaults to
    /// [`DeliverMode::Queue`] — consumers sharing a group split the work
    /// round-robin.
    ///
    /// Pass [`DeliverMode::Fanout`] to have EVERY consumer on the stream
    /// receive EVERY matching message. Note the broker treats this byte as
    /// the sole determinant and discards the group under `Fanout`, so a
    /// group + `Fanout` is not a "group of one" — it is a plain broadcast.
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
    /// The queue group is resolved here, not in [`Self::new`], so setting
    /// a group after the name (or in any other order) still wins.
    ///
    /// Returns the freshly-allocated `consumer_id`.
    pub async fn create(self, client: &Client, stream_id: u32) -> Result<u32, ClientError> {
        let (ack_policy, group) = self.validate()?;

        let resp = client
            .create_consumer_with_limits(
                stream_id,
                self.name,
                group,
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
    /// [`AckPolicy`] so the wire-encoding step doesn't `.unwrap()` again,
    /// plus the resolved queue group (group → name → stream name).
    fn validate(&self) -> Result<(AckPolicy, &'a [u8]), ClientError> {
        // Mirror invariants from
        //   arbitro_proto::config::consumer::ConsumerConfigBuilder::build
        // so the two paths cannot drift apart.
        let _ = ConsumerConfig::new(self.name, b"placeholder");

        let ack_policy = self.ack_policy.ok_or_else(|| {
            ClientError::InvalidConfig(
                "ack_policy must be set explicitly (AckPolicy::None or \
                 AckPolicy::Explicit) — there is no safe default"
                    .into(),
            )
        })?;

        if ack_policy == AckPolicy::None {
            if self.max_inflight != 0 {
                return Err(ClientError::InvalidConfig(
                    "max_inflight requires AckPolicy::Explicit (fire-and-forget \
                     consumers don't track inflight)"
                        .into(),
                ));
            }
            if !self.subject_limits.is_empty() {
                return Err(ClientError::InvalidConfig(
                    "max_subject_inflight requires AckPolicy::Explicit \
                     (fire-and-forget consumers don't track inflight)"
                        .into(),
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

        // group → name → stream_name. Never empty on the wire.
        let group = resolve_group(self.group, self.name, self.stream_name)?;

        Ok((ack_policy, group))
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

    // ── queue-group defaulting ───────────────────────────────────────

    #[test]
    fn unset_group_defaults_to_consumer_name() {
        let (_, group) = ConsumerBuilder::new(b"orders_worker")
            .ack_policy(AckPolicy::Explicit)
            .validate()
            .expect("valid");
        assert_eq!(group, b"orders_worker");
    }

    #[test]
    fn explicit_group_wins_even_when_set_last() {
        // Resolution happens at create()/validate() time, not in new(),
        // so ordering of the builder calls cannot change the outcome.
        let (_, group) = ConsumerBuilder::new(b"orders_worker")
            .ack_policy(AckPolicy::Explicit)
            .group(b"workers")
            .validate()
            .expect("valid");
        assert_eq!(group, b"workers");
    }

    #[test]
    fn empty_group_and_name_fall_back_to_stream_name() {
        let (_, group) = ConsumerBuilder::new(b"")
            .ack_policy(AckPolicy::Explicit)
            .stream_name(b"orders")
            .validate()
            .expect("valid");
        assert_eq!(group, b"orders");
    }

    #[test]
    fn nothing_to_default_from_is_rejected() {
        let err = ConsumerBuilder::new(b"")
            .ack_policy(AckPolicy::Explicit)
            .validate()
            .unwrap_err();
        match err {
            ClientError::InvalidConfig(msg) => assert!(msg.contains("queue group")),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    // ── deliver-mode defaulting ──────────────────────────────────────

    /// Cross-client parity: TS, Go, Rust and C all default to Queue, so
    /// the same call means the same thing in every language. Sharing is
    /// also the safer default — the old `Fanout` silently duplicated
    /// every message to every consumer on the stream.
    #[test]
    fn unset_deliver_mode_defaults_to_queue() {
        assert_eq!(ConsumerBuilder::new(b"c").deliver_mode, DeliverMode::Queue);
        assert_eq!(ConsumerBuilder::new(b"c").deliver_mode as u8, 1);
    }

    #[test]
    fn explicit_fanout_still_wins() {
        let b = ConsumerBuilder::new(b"c").deliver_mode(DeliverMode::Fanout);
        assert_eq!(b.deliver_mode, DeliverMode::Fanout);
        assert_eq!(b.deliver_mode as u8, 0);
    }

    /// The default must reach the WIRE: the broker never assumes a
    /// deliver_mode, so whatever byte the builder picks is the behaviour.
    #[test]
    fn defaulted_deliver_mode_reaches_the_wire_as_queue() {
        use arbitro_proto::v2::cold::{ColdBody, CreateConsumer as CreateConsumerCold};
        use arbitro_proto::v2::header::HEADER_SIZE;

        let b = ConsumerBuilder::new(b"orders_worker").ack_policy(AckPolicy::Explicit);
        let (ack_policy, group) = b.validate().expect("valid");

        let frame = crate::transport::encode::encode_create_consumer_v2(
            1,
            7,
            b.name,
            group,
            b.filter,
            b.max_inflight,
            ack_policy as u8,
            b.deliver_policy as u8,
            b.deliver_mode as u8,
            b.ack_wait_ms,
            b.start_seq,
            &b.subject_limits,
        );

        let decoded = CreateConsumerCold::decode_body(&frame[HEADER_SIZE..])
            .expect("decodes as CreateConsumer");
        assert_eq!(
            decoded.deliver_mode, 1,
            "unset deliver_mode must be sent as Queue (1), matching TS/Go/C"
        );
    }

    /// The defaulted group must reach the WIRE, not just the builder:
    /// encode the exact frame `create()` sends and decode it back.
    #[test]
    fn defaulted_group_reaches_the_wire_as_the_consumer_name() {
        use arbitro_proto::v2::cold::{ColdBody, CreateConsumer as CreateConsumerCold};
        use arbitro_proto::v2::header::HEADER_SIZE;

        let b = ConsumerBuilder::new(b"orders_worker").ack_policy(AckPolicy::Explicit);
        let (ack_policy, group) = b.validate().expect("valid");

        let frame = crate::transport::encode::encode_create_consumer_v2(
            1,
            7,
            b.name,
            group,
            b.filter,
            b.max_inflight,
            ack_policy as u8,
            b.deliver_policy as u8,
            b.deliver_mode as u8,
            b.ack_wait_ms,
            b.start_seq,
            &b.subject_limits,
        );

        let decoded =
            CreateConsumerCold::decode_body(&frame[HEADER_SIZE..]).expect("decodes as CreateConsumer");
        assert_eq!(decoded.name, b"orders_worker".to_vec());
        assert_eq!(
            decoded.group,
            b"orders_worker".to_vec(),
            "unset group must be sent as the consumer name, never empty"
        );
        assert!(!decoded.group.is_empty());
    }
}
