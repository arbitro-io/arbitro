use super::stream::fnv1a_32;

/// Consumer configuration — cold path, created once.
///
/// Invariants:
/// - Filters must not overlap each other.
/// - `subject_limits`, `max_inflight`, `ack_wait_ms` only apply
///   when `ack_policy` is `Explicit`.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub name: Box<[u8]>,
    pub consumer_id: u32,
    pub stream_id: u32,
    pub filters: Box<[Box<[u8]>]>,
    pub subject_limits: Box<[SubjectLimit]>,
    pub max_inflight: u16,
    pub ack_policy: AckPolicy,
    pub deliver_policy: DeliverPolicy,
    pub deliver_mode: DeliverMode,
    pub ack_wait_ms: u32,
    pub start_seq: u64,
}

pub struct ConsumerConfigBuilder {
    name: Box<[u8]>,
    stream_id: u32,
    filters: Vec<Box<[u8]>>,
    subject_limits: Vec<SubjectLimit>,
    max_inflight: u16,
    ack_policy: AckPolicy,
    deliver_policy: DeliverPolicy,
    deliver_mode: DeliverMode,
    ack_wait_ms: u32,
    start_seq: u64,
}

impl ConsumerConfig {
    /// Start building. `stream_name` is hashed to `stream_id` via FNV-1a.
    pub fn new(name: &[u8], stream_name: &[u8]) -> ConsumerConfigBuilder {
        ConsumerConfigBuilder {
            name: Box::from(name),
            stream_id: fnv1a_32(stream_name),
            filters: Vec::new(),
            subject_limits: Vec::new(),
            max_inflight: 0,
            ack_policy: AckPolicy::Explicit,
            deliver_policy: DeliverPolicy::All,
            deliver_mode: DeliverMode::Fanout,
            ack_wait_ms: 0,
            start_seq: 0,
        }
    }

    /// Build a ConsumerConfig directly from wire fields (cold path).
    /// Used by the engine when parsing CreateConsumer frames.
    pub fn from_wire(
        stream_id: u32,
        name: &[u8],
        subject: &[u8],
        max_inflight: u16,
        deliver_policy: u8,
        deliver_mode: u8,
        ack_wait_ms: u32,
        start_seq: u64,
    ) -> ConsumerConfig {
        ConsumerConfig {
            name: Box::from(name),
            consumer_id: 0, // server-assigned
            stream_id,
            filters: if subject.is_empty() {
                Box::from([])
            } else {
                Box::from([Box::from(subject)])
            },
            subject_limits: Box::from([]),
            max_inflight,
            ack_policy: if max_inflight > 0 { AckPolicy::Explicit } else { AckPolicy::None },
            deliver_policy: DeliverPolicy::from_u8(deliver_policy).unwrap_or(DeliverPolicy::All),
            deliver_mode: DeliverMode::from_u8(deliver_mode).unwrap_or(DeliverMode::Fanout),
            ack_wait_ms,
            start_seq,
        }
    }
}

impl ConsumerConfigBuilder {
    pub fn filter(mut self, pattern: &[u8]) -> Self {
        self.filters.push(Box::from(pattern));
        self
    }

    pub fn subject_limit(mut self, pattern: &[u8], limit: u32) -> Self {
        self.subject_limits.push(SubjectLimit {
            pattern: Box::from(pattern),
            limit,
        });
        self
    }

    pub fn max_inflight(mut self, v: u16) -> Self { self.max_inflight = v; self }
    pub fn ack_policy(mut self, v: AckPolicy) -> Self { self.ack_policy = v; self }
    pub fn deliver_policy(mut self, v: DeliverPolicy) -> Self { self.deliver_policy = v; self }
    pub fn deliver_mode(mut self, v: DeliverMode) -> Self { self.deliver_mode = v; self }
    pub fn ack_wait_ms(mut self, v: u32) -> Self { self.ack_wait_ms = v; self }
    pub fn start_seq(mut self, v: u64) -> Self { self.start_seq = v; self }

    pub fn build(self) -> ConsumerConfig {
        ConsumerConfig {
            name: self.name,
            consumer_id: 0, // server-assigned
            stream_id: self.stream_id,
            filters: self.filters.into_boxed_slice(),
            subject_limits: self.subject_limits.into_boxed_slice(),
            max_inflight: self.max_inflight,
            ack_policy: self.ack_policy,
            deliver_policy: self.deliver_policy,
            deliver_mode: self.deliver_mode,
            ack_wait_ms: self.ack_wait_ms,
            start_seq: self.start_seq,
        }
    }
}

/// Per-subject flow control — prevents noisy neighbor.
#[derive(Debug, Clone)]
pub struct SubjectLimit {
    pub pattern: Box<[u8]>,
    pub limit: u32,
}

/// Ack policy — determines if the broker tracks in-flight messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AckPolicy {
    /// Fire-and-forget. No ack tracking, no redelivery.
    None     = 0,
    /// Consumer must ack. Enables max_inflight, subject_limits, ack_wait_ms.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DeliverPolicy {
    /// All messages from the beginning.
    All        = 0,
    /// Only new messages from now.
    New        = 1,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DeliverMode {
    /// All consumers receive every message.
    Fanout = 0,
    /// Round-robin: one consumer per message.
    Queue  = 1,
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
