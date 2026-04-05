/// Stream configuration — cold path, created once.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub name: Box<[u8]>,
    pub stream_id: u32,
    pub max_msgs: u64,
    pub max_bytes: u64,
    pub max_age_secs: u64,
    pub replicas: u8,
    pub journal_kind: JournalKind,
}

pub struct StreamConfigBuilder {
    name: Box<[u8]>,
    max_msgs: u64,
    max_bytes: u64,
    max_age_secs: u64,
    replicas: u8,
    journal_kind: JournalKind,
}

impl StreamConfig {
    pub fn new(name: &[u8]) -> StreamConfigBuilder {
        StreamConfigBuilder {
            name: Box::from(name),
            max_msgs: 0,
            max_bytes: 0,
            max_age_secs: 0,
            replicas: 1,
            journal_kind: JournalKind::Memory,
        }
    }
}

impl StreamConfigBuilder {
    pub fn max_msgs(mut self, v: u64) -> Self { self.max_msgs = v; self }
    pub fn max_bytes(mut self, v: u64) -> Self { self.max_bytes = v; self }
    pub fn max_age_secs(mut self, v: u64) -> Self { self.max_age_secs = v; self }
    pub fn replicas(mut self, v: u8) -> Self { self.replicas = v; self }
    pub fn journal_kind(mut self, v: JournalKind) -> Self { self.journal_kind = v; self }

    pub fn build(self) -> StreamConfig {
        let stream_id = fnv1a_32(&self.name);
        StreamConfig {
            name: self.name,
            stream_id,
            max_msgs: self.max_msgs,
            max_bytes: self.max_bytes,
            max_age_secs: self.max_age_secs,
            replicas: self.replicas,
            journal_kind: self.journal_kind,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum JournalKind {
    Memory = 0,
    Disk   = 1,
}

impl JournalKind {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Memory),
            1 => Some(Self::Disk),
            _ => None,
        }
    }
}

/// FNV-1a hash → u32. Deterministic, no-std.
pub(crate) fn fnv1a_32(data: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in data {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}
