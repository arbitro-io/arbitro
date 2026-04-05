/// Connection identifier (server-assigned).
pub type ConnId = u64;

/// Stream identifier (FNV-1a hash of stream name → u32).
pub type StreamId = u32;

/// Message sequence number within a stream.
pub type Sequence = u64;

/// Consumer identifier (server-assigned).
pub type ConsumerId = u32;
