//! Message replication — data-plane replication between cluster nodes.
//!
//! This is SEPARATE from Raft consensus. Raft replicates metadata
//! (CreateStream, CreateConsumer). This module replicates MESSAGE DATA
//! (the actual payloads stored in journals).
//!
//! ## Wire format
//!
//! Replication frames reuse the 32-byte `RaftFrameHeader` with custom
//! `kind` values. They travel over the SAME TCP connections as Raft
//! frames — no separate port needed.
//!
//! ### ReplicateEntries (leader → follower)
//! ```text
//! [RaftFrameHeader 32B]  kind = KIND_REPLICATE_ENTRIES (20)
//! [ReplicateEntriesHeader 24B]
//!   stream_id    : u32
//!   first_seq    : u64
//!   entry_count  : u32
//!   _reserved    : u32
//!   timestamp_ms : u64
//! [entries...]
//!   Each entry: [entry_len:u32][Record bytes]
//! ```
//!
//! ### ReplicateAck (follower → leader)
//! ```text
//! [RaftFrameHeader 32B]  kind = KIND_REPLICATE_ACK (21)
//! [ReplicateAckBody 16B]
//!   stream_id    : u32
//!   last_seq     : u64  ← highest seq successfully written
//!   _reserved    : u32
//! ```

use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

// ── Kind constants ────────────────────────────────────────────────────────
// Use values > 11 to avoid collision with Raft's KIND_* (1-11).

pub const KIND_REPLICATE_ENTRIES: u8 = 20;
pub const KIND_REPLICATE_ACK: u8 = 21;
pub const KIND_REPLICATE_CATCH_UP_REQ: u8 = 22;

// ── ReplicateEntries header ───────────────────────────────────────────────

/// Fixed header for a batch of replicated entries.
#[derive(
    IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub struct ReplicateEntriesHeader {
    pub stream_id: U32,
    pub first_seq: U64,
    pub entry_count: U32,
    pub _reserved: U32,
    pub timestamp_ms: U64,
}

pub const REPLICATE_ENTRIES_HEADER_SIZE: usize =
    core::mem::size_of::<ReplicateEntriesHeader>();
const _: () = assert!(REPLICATE_ENTRIES_HEADER_SIZE == 28);

// ── ReplicateAck body ─────────────────────────────────────────────────────

/// Follower acknowledges replication up to `last_seq`.
#[derive(
    IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub struct ReplicateAckBody {
    pub stream_id: U32,
    pub last_seq: U64,
    pub _reserved: U32,
}

pub const REPLICATE_ACK_BODY_SIZE: usize = core::mem::size_of::<ReplicateAckBody>();
const _: () = assert!(REPLICATE_ACK_BODY_SIZE == 16);

// ── CatchUpRequest body ───────────────────────────────────────────────────

/// Follower requests entries from `from_seq` to leader's latest.
#[derive(
    IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub struct CatchUpRequest {
    pub stream_id: U32,
    pub from_seq: U64,
    pub _reserved: U32,
}

pub const CATCH_UP_REQUEST_SIZE: usize = core::mem::size_of::<CatchUpRequest>();
const _: () = assert!(CATCH_UP_REQUEST_SIZE == 16);

// ── ISR state ─────────────────────────────────────────────────────────────

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use arbitro_engine_v2::types::StreamId;
use arbitro_raft::PeerId;

/// Tracks which followers are in-sync for each stream.
pub struct IsrTracker {
    /// Per-stream: set of in-sync peer IDs.
    isr: HashMap<StreamId, HashSet<PeerId>>,
    /// Last ack time per (stream, peer). Used for ejection.
    last_ack: HashMap<(StreamId, PeerId), Instant>,
    /// How long a follower can go without acking before ejection.
    pub lag_timeout: Duration,
}

use std::time::Duration;

impl IsrTracker {
    pub fn new(lag_timeout: Duration) -> Self {
        Self {
            isr: HashMap::new(),
            last_ack: HashMap::new(),
            lag_timeout,
        }
    }

    /// Add a peer to a stream's ISR (e.g., on successful catch-up).
    pub fn add(&mut self, stream: StreamId, peer: PeerId) {
        self.isr.entry(stream).or_default().insert(peer);
        self.last_ack.insert((stream, peer), Instant::now());
    }

    /// Record an ack from a peer for a stream.
    pub fn record_ack(&mut self, stream: StreamId, peer: PeerId) {
        self.last_ack.insert((stream, peer), Instant::now());
    }

    /// Eject peers that haven't acked within `lag_timeout`.
    pub fn tick(&mut self) -> Vec<(StreamId, PeerId)> {
        let now = Instant::now();
        let mut ejected = Vec::new();
        for (stream, peers) in &mut self.isr {
            peers.retain(|peer| {
                if let Some(last) = self.last_ack.get(&(*stream, *peer)) {
                    if now.duration_since(*last) > self.lag_timeout {
                        ejected.push((*stream, *peer));
                        return false;
                    }
                }
                true
            });
        }
        ejected
    }

    /// Get the current ISR for a stream.
    pub fn get(&self, stream: StreamId) -> Option<&HashSet<PeerId>> {
        self.isr.get(&stream)
    }

    /// Number of in-sync replicas for a stream (including leader = +1).
    pub fn isr_count(&self, stream: StreamId) -> usize {
        self.isr.get(&stream).map_or(1, |s| s.len() + 1) // +1 for leader
    }
}

// ── High watermark ────────────────────────────────────────────────────────

/// Per-stream high watermark: the highest seq confirmed by ALL ISR members.
/// Consumers should only see messages up to this seq.
pub struct HighWatermarks {
    /// stream_id → highest seq confirmed by all ISR.
    marks: HashMap<StreamId, u64>,
}

impl HighWatermarks {
    pub fn new() -> Self {
        Self {
            marks: HashMap::new(),
        }
    }

    /// Update the watermark for a stream. Only advances, never goes back.
    pub fn update(&mut self, stream: StreamId, seq: u64) {
        let entry = self.marks.entry(stream).or_insert(0);
        if seq > *entry {
            *entry = seq;
        }
    }

    /// Get the current watermark for a stream.
    pub fn get(&self, stream: StreamId) -> u64 {
        self.marks.get(&stream).copied().unwrap_or(0)
    }
}

impl Default for HighWatermarks {
    fn default() -> Self {
        Self::new()
    }
}
