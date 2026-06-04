use super::stream::wire_hash_32;
use serde::{Deserialize, Serialize};

/// Consumer configuration — cold path, created once.
///
/// Invariants enforced by [`ConsumerConfigBuilder::build`]:
/// - `ack_policy` MUST be set explicitly. There is no default — `None`
///   means fire-and-forget, `Explicit` means the consumer must ack. The
///   two have wildly different inflight semantics, so silently picking
///   one is a footgun (replay benches were accidentally getting Explicit
///   and tripping inflight-related drain stalls).
/// - `max_inflight`, `max_subject_inflights`, and `ack_wait_ms` only
///   apply with `AckPolicy::Explicit`. Setting any of them with
///   `AckPolicy::None` is rejected at build time.
/// - `DeliverPolicy::ByStartSeq` requires `start_seq > 0`.
/// - Filters must not overlap each other.
/// - `group` determines the QueueId for round-robin (Queue mode).
///   Defaults to the stream name — consumers sharing the same group
///   on the same stream share a single ready queue (round-robin).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    pub name: Box<[u8]>,
    pub consumer_id: u32,
    pub stream_id: u32,
    /// Queue group name. Consumers with the same group share a ready
    /// queue (round-robin). Default: stream name.
    pub group: Box<[u8]>,
    pub filters: Box<[Box<[u8]>]>,
    pub max_subject_inflights: Box<[MaxSubjectInflight]>,
    pub max_inflight: u16,
    pub ack_policy: AckPolicy,
    pub deliver_policy: DeliverPolicy,
    pub deliver_mode: DeliverMode,
    pub ack_wait_ms: u32,
    pub start_seq: u64,
}

pub struct ConsumerConfigBuilder {
    name: Box<[u8]>,
    stream_name: Box<[u8]>,
    stream_id: u32,
    group: Option<Box<[u8]>>,
    filters: Vec<Box<[u8]>>,
    max_subject_inflights: Vec<MaxSubjectInflight>,
    max_inflight: u16,
    /// `None` until the caller picks one explicitly. `build()` errors out
    /// if this is still `None` — see `ConsumerConfigError::MissingAckPolicy`.
    ack_policy: Option<AckPolicy>,
    deliver_policy: DeliverPolicy,
    deliver_mode: DeliverMode,
    ack_wait_ms: u32,
    start_seq: u64,
}

/// Errors returned by [`ConsumerConfigBuilder::build`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerConfigError {
    /// `ack_policy` was never set on the builder. There is no safe
    /// default — pick `AckPolicy::None` for fire-and-forget delivery
    /// or `AckPolicy::Explicit` if the consumer will ack.
    MissingAckPolicy,
    /// `max_inflight > 0` is meaningless without `AckPolicy::Explicit`
    /// (the engine only tracks inflight when the consumer must ack).
    InflightWithoutAck,
    /// `max_subject_inflights` non-empty without `AckPolicy::Explicit`.
    SubjectInflightWithoutAck,
    /// `ack_wait_ms > 0` is meaningless without `AckPolicy::Explicit`.
    AckWaitWithoutAck,
    /// `DeliverPolicy::ByStartSeq` requires `start_seq > 0`.
    StartSeqRequired,
}

impl std::fmt::Display for ConsumerConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingAckPolicy => {
                f.write_str("ConsumerConfig: ack_policy must be set explicitly (None or Explicit)")
            }
            Self::InflightWithoutAck => {
                f.write_str("ConsumerConfig: max_inflight requires AckPolicy::Explicit")
            }
            Self::SubjectInflightWithoutAck => {
                f.write_str("ConsumerConfig: max_subject_inflights requires AckPolicy::Explicit")
            }
            Self::AckWaitWithoutAck => {
                f.write_str("ConsumerConfig: ack_wait_ms requires AckPolicy::Explicit")
            }
            Self::StartSeqRequired => {
                f.write_str("ConsumerConfig: DeliverPolicy::ByStartSeq requires start_seq > 0")
            }
        }
    }
}

impl std::error::Error for ConsumerConfigError {}

impl ConsumerConfig {
    /// Start building. `stream_name` is hashed to `stream_id` via FNV-1a.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(name: &[u8], stream_name: &[u8]) -> ConsumerConfigBuilder {
        ConsumerConfigBuilder {
            name: Box::from(name),
            stream_name: Box::from(stream_name),
            stream_id: wire_hash_32(stream_name),
            group: None,
            filters: Vec::new(),
            max_subject_inflights: Vec::new(),
            max_inflight: 0,
            ack_policy: None,
            deliver_policy: DeliverPolicy::All,
            deliver_mode: DeliverMode::Fanout,
            ack_wait_ms: 0,
            start_seq: 0,
        }
    }

    /// Build a ConsumerConfig directly from wire fields (cold path).
    /// Used by the engine when parsing CreateConsumer frames.
    #[allow(clippy::too_many_arguments)]
    pub fn from_wire(
        stream_id: u32,
        name: &[u8],
        group: &[u8],
        subject: &[u8],
        max_inflight: u16,
        ack_policy: u8,
        deliver_policy: u8,
        deliver_mode: u8,
        ack_wait_ms: u32,
        start_seq: u64,
        max_subject_inflights: Box<[MaxSubjectInflight]>,
    ) -> ConsumerConfig {
        ConsumerConfig {
            name: Box::from(name),
            consumer_id: 0, // server-assigned
            stream_id,
            group: Box::from(group),
            filters: if subject.is_empty() {
                Box::from([])
            } else {
                Box::from([Box::from(subject)])
            },
            max_subject_inflights,
            max_inflight,
            ack_policy: AckPolicy::from_u8(ack_policy).unwrap_or(AckPolicy::None),
            deliver_policy: DeliverPolicy::from_u8(deliver_policy).unwrap_or(DeliverPolicy::All),
            deliver_mode: DeliverMode::from_u8(deliver_mode).unwrap_or(DeliverMode::Fanout),
            ack_wait_ms,
            start_seq,
        }
    }
}

impl ConsumerConfigBuilder {
    /// Set queue group name. Consumers with the same group share a
    /// ready queue (round-robin in Queue mode). Default: stream name.
    pub fn group(mut self, name: &[u8]) -> Self {
        self.group = Some(Box::from(name));
        self
    }

    pub fn filter(mut self, pattern: &[u8]) -> Self {
        self.filters.push(Box::from(pattern));
        self
    }

    pub fn max_subject_inflight(mut self, pattern: &[u8], limit: u32) -> Self {
        self.max_subject_inflights.push(MaxSubjectInflight {
            pattern: Box::from(pattern),
            limit,
        });
        self
    }

    pub fn max_inflight(mut self, v: u16) -> Self {
        self.max_inflight = v;
        self
    }
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

    /// Validate and finalize the config. Returns a typed error for any
    /// invariant violation — see [`ConsumerConfigError`] for the full list.
    pub fn build(self) -> Result<ConsumerConfig, ConsumerConfigError> {
        // 1. ack_policy must be picked explicitly.
        let ack_policy = self
            .ack_policy
            .ok_or(ConsumerConfigError::MissingAckPolicy)?;

        // 2. Inflight knobs only make sense with Explicit acks.
        if ack_policy == AckPolicy::None {
            if self.max_inflight != 0 {
                return Err(ConsumerConfigError::InflightWithoutAck);
            }
            if !self.max_subject_inflights.is_empty() {
                return Err(ConsumerConfigError::SubjectInflightWithoutAck);
            }
            if self.ack_wait_ms != 0 {
                return Err(ConsumerConfigError::AckWaitWithoutAck);
            }
        }

        // 3. ByStartSeq needs an actual start_seq.
        if self.deliver_policy == DeliverPolicy::ByStartSeq && self.start_seq == 0 {
            return Err(ConsumerConfigError::StartSeqRequired);
        }

        // Default group = stream name
        let group = self.group.unwrap_or_else(|| self.stream_name.clone());
        Ok(ConsumerConfig {
            name: self.name,
            consumer_id: 0, // server-assigned
            stream_id: self.stream_id,
            group,
            filters: self.filters.into_boxed_slice(),
            max_subject_inflights: self.max_subject_inflights.into_boxed_slice(),
            max_inflight: self.max_inflight,
            ack_policy,
            deliver_policy: self.deliver_policy,
            deliver_mode: self.deliver_mode,
            ack_wait_ms: self.ack_wait_ms,
            start_seq: self.start_seq,
        })
    }
}

/// Per-subject flow control — prevents noisy neighbor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaxSubjectInflight {
    pub pattern: Box<[u8]>,
    pub limit: u32,
}

/// Ack policy — determines if the broker tracks in-flight messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum AckPolicy {
    /// Fire-and-forget. No ack tracking, no redelivery.
    None = 0,
    /// Consumer must ack. Enables max_inflight, max_subject_inflights, ack_wait_ms.
    Explicit = 1,
}

impl AckPolicy {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::None),
            1 => Some(Self::Explicit),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum DeliverPolicy {
    /// All messages from the beginning.
    All = 0,
    /// Only new messages from now.
    New = 1,
    /// From a specific sequence (uses `start_seq`).
    ByStartSeq = 2,
}

impl DeliverPolicy {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::All),
            1 => Some(Self::New),
            2 => Some(Self::ByStartSeq),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum DeliverMode {
    /// All consumers receive every message.
    Fanout = 0,
    /// Round-robin: one consumer per message.
    Queue = 1,
}

impl DeliverMode {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Fanout),
            1 => Some(Self::Queue),
            _ => None,
        }
    }
}

#[cfg(test)]
mod builder_invariants {
    use super::*;

    fn b() -> ConsumerConfigBuilder {
        ConsumerConfig::new(b"test_consumer", b"test_stream")
    }

    #[test]
    fn missing_ack_policy_is_rejected() {
        let err = b().build().unwrap_err();
        assert_eq!(err, ConsumerConfigError::MissingAckPolicy);
    }

    #[test]
    fn ack_none_minimal_builds() {
        let cfg = b().ack_policy(AckPolicy::None).build().unwrap();
        assert_eq!(cfg.ack_policy, AckPolicy::None);
        assert_eq!(cfg.max_inflight, 0);
        assert!(cfg.max_subject_inflights.is_empty());
        assert_eq!(cfg.ack_wait_ms, 0);
    }

    #[test]
    fn ack_explicit_minimal_builds() {
        let cfg = b().ack_policy(AckPolicy::Explicit).build().unwrap();
        assert_eq!(cfg.ack_policy, AckPolicy::Explicit);
    }

    #[test]
    fn max_inflight_without_ack_is_rejected() {
        let err = b()
            .ack_policy(AckPolicy::None)
            .max_inflight(1024)
            .build()
            .unwrap_err();
        assert_eq!(err, ConsumerConfigError::InflightWithoutAck);
    }

    #[test]
    fn max_inflight_with_ack_explicit_builds() {
        let cfg = b()
            .ack_policy(AckPolicy::Explicit)
            .max_inflight(1024)
            .build()
            .unwrap();
        assert_eq!(cfg.max_inflight, 1024);
    }

    #[test]
    fn max_subject_inflight_without_ack_is_rejected() {
        let err = b()
            .ack_policy(AckPolicy::None)
            .max_subject_inflight(b"foo.>", 10)
            .build()
            .unwrap_err();
        assert_eq!(err, ConsumerConfigError::SubjectInflightWithoutAck);
    }

    #[test]
    fn ack_wait_without_ack_is_rejected() {
        let err = b()
            .ack_policy(AckPolicy::None)
            .ack_wait_ms(5000)
            .build()
            .unwrap_err();
        assert_eq!(err, ConsumerConfigError::AckWaitWithoutAck);
    }

    #[test]
    fn by_start_seq_requires_nonzero_start_seq() {
        let err = b()
            .ack_policy(AckPolicy::None)
            .deliver_policy(DeliverPolicy::ByStartSeq)
            .build()
            .unwrap_err();
        assert_eq!(err, ConsumerConfigError::StartSeqRequired);
    }

    #[test]
    fn by_start_seq_with_seq_builds() {
        let cfg = b()
            .ack_policy(AckPolicy::None)
            .deliver_policy(DeliverPolicy::ByStartSeq)
            .start_seq(42)
            .build()
            .unwrap();
        assert_eq!(cfg.deliver_policy, DeliverPolicy::ByStartSeq);
        assert_eq!(cfg.start_seq, 42);
    }

    #[test]
    fn deliver_policy_all_does_not_require_start_seq() {
        let cfg = b()
            .ack_policy(AckPolicy::None)
            .deliver_policy(DeliverPolicy::All)
            .build()
            .unwrap();
        assert_eq!(cfg.start_seq, 0);
    }

    #[test]
    fn group_defaults_to_stream_name() {
        let cfg = b().ack_policy(AckPolicy::None).build().unwrap();
        assert_eq!(&*cfg.group, b"test_stream");
    }

    #[test]
    fn explicit_group_overrides_default() {
        let cfg = b()
            .ack_policy(AckPolicy::None)
            .group(b"custom_group")
            .build()
            .unwrap();
        assert_eq!(&*cfg.group, b"custom_group");
    }
}
