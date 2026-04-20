use serde::{Deserialize, Serialize};

/// Stream configuration — cold path, created once.
///
/// Invariants:
///   - `name` is an identifier: `[a-zA-Z0-9_-]`, max 255 bytes.
///   - `filter` is a subject pattern that defines what this stream captures.
///     No two streams may have overlapping filters.
///   - `stream_id` is always `wire_hash_32(name)`, computed server-side.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub name: Box<[u8]>,
    pub stream_id: u32,
    /// Subject pattern this stream captures. Required.
    /// Example: `"orders.>"` captures all subjects starting with `orders.`.
    pub filter: Box<[u8]>,
    pub max_msgs: u64,
    pub max_bytes: u64,
    /// Per-message TTL in seconds. Lazy expiry: validated at read time, not
    /// by a background sweeper. A message whose age exceeds `max_age_secs`
    /// is treated as not found on the next `get` / `for_each` / consumer
    /// delivery and becomes eligible for removal on the next mutating op.
    ///
    /// Policy:
    /// - `0` = disabled (infinite retention, default).
    /// - `> 0` = any positive value is valid (no minimum — there is no
    ///   sweeper tick to respect since validation is on-read).
    /// - Update: lowering the value is legitimate (ops, compliance,
    ///   rebalance). Already-stored messages that fall outside the new
    ///   window become eligible for expiry on their next access.
    /// - Interaction with `RetentionPolicy::{Interest, WorkQueue}`:
    ///   expiry must never drop a message with a pending ack — that is
    ///   an independent invariant of the retention policy.
    pub max_age_secs: u64,
    pub replicas: u8,
    pub journal_kind: JournalKind,
    pub retention: RetentionPolicy,
    /// Behavior when `max_msgs` / `max_bytes` is reached. Orthogonal to
    /// `retention`: `Old` is the default ring-buffer behavior (drop the
    /// oldest message to make room); `New` rejects the incoming publish
    /// with an error so the producer sees backpressure.
    pub discard: DiscardPolicy,
}

pub struct StreamConfigBuilder {
    name: Box<[u8]>,
    filter: Box<[u8]>,
    max_msgs: u64,
    max_bytes: u64,
    max_age_secs: u64,
    replicas: u8,
    journal_kind: JournalKind,
    retention: RetentionPolicy,
    discard: DiscardPolicy,
}

impl StreamConfig {
    /// Start building. `filter` is the subject pattern this stream captures.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(name: &[u8], filter: &[u8]) -> StreamConfigBuilder {
        StreamConfigBuilder {
            name: Box::from(name),
            filter: Box::from(filter),
            max_msgs: 0,
            max_bytes: 0,
            max_age_secs: 0,
            replicas: 1,
            journal_kind: JournalKind::Memory,
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
        }
    }
}

impl StreamConfigBuilder {
    pub fn max_msgs(mut self, v: u64) -> Self { self.max_msgs = v; self }
    pub fn max_bytes(mut self, v: u64) -> Self { self.max_bytes = v; self }
    pub fn max_age_secs(mut self, v: u64) -> Self { self.max_age_secs = v; self }
    pub fn replicas(mut self, v: u8) -> Self { self.replicas = v; self }
    pub fn journal_kind(mut self, v: JournalKind) -> Self { self.journal_kind = v; self }
    pub fn retention(mut self, v: RetentionPolicy) -> Self { self.retention = v; self }
    pub fn discard(mut self, v: DiscardPolicy) -> Self { self.discard = v; self }

    pub fn build(self) -> StreamConfig {
        let stream_id = wire_hash_32(&self.name);
        StreamConfig {
            name: self.name,
            stream_id,
            filter: self.filter,
            max_msgs: self.max_msgs,
            max_bytes: self.max_bytes,
            max_age_secs: self.max_age_secs,
            replicas: self.replicas,
            journal_kind: self.journal_kind,
            retention: self.retention,
            discard: self.discard,
        }
    }
}

/// What happens when stream limits (max_msgs, max_bytes, max_age) are reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum RetentionPolicy {
    /// Discard oldest messages to make room — ring buffer behavior. Default.
    Limits    = 0,
    /// Keep messages only while consumers with matching filters exist.
    /// Once all interested consumers ack, the message is eligible for removal.
    Interest  = 1,
    /// Messages deleted immediately after ack — work queue pattern.
    WorkQueue = 2,
}

impl RetentionPolicy {
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Limits),
            1 => Some(Self::Interest),
            2 => Some(Self::WorkQueue),
            _ => None,
        }
    }
}

/// What happens on publish when `max_msgs` or `max_bytes` is already met.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum DiscardPolicy {
    /// Drop the oldest message to make room — ring-buffer. Default.
    Old = 0,
    /// Reject the incoming publish with an error — producer backpressure.
    New = 1,
}

impl DiscardPolicy {
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Old),
            1 => Some(Self::New),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum JournalKind {
    Memory = 0,
    Disk   = 1,
    Tolerant = 2,
}

impl JournalKind {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Memory),
            1 => Some(Self::Disk),
            2 => Some(Self::Tolerant),
            _ => None,
        }
    }
}

/// Wire-hash → u32. Deterministic, uses `foldhash::fast::FixedState`
/// (constant seed → stable across processes and versions).
pub fn wire_hash_32(data: &[u8]) -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = foldhash::fast::FixedState::default().build_hasher();
    h.write(data);
    h.finish() as u32
}
