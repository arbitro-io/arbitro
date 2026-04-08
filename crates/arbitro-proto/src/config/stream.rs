use serde::{Deserialize, Serialize};

/// Stream configuration — cold path, created once.
///
/// Invariants:
///   - `name` is an identifier: `[a-zA-Z0-9_-]`, max 255 bytes.
///   - `filter` is a subject pattern that defines what this stream captures.
///     No two streams may have overlapping filters.
///   - `stream_id` is always `fnv1a_32(name)`, computed server-side.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub name: Box<[u8]>,
    pub stream_id: u32,
    /// Subject pattern this stream captures. Required.
    /// Example: `"orders.>"` captures all subjects starting with `orders.`.
    pub filter: Box<[u8]>,
    pub max_msgs: u64,
    pub max_bytes: u64,
    pub max_age_secs: u64,
    pub replicas: u8,
    pub journal_kind: JournalKind,
    pub retention: RetentionPolicy,
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

    pub fn build(self) -> StreamConfig {
        let stream_id = fnv1a_32(&self.name);
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

/// FNV-1a hash → u32. Deterministic, no-std.
pub fn fnv1a_32(data: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in data {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}
