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

// ── Replication batch ────────────────────────────────────────────────────

/// A batch of entries to replicate from the leader to followers.
/// Sent through an mpsc channel from shard workers to the replication loop.
pub struct ReplicationBatch {
    /// Internal stream ID (used for routing on the follower side).
    pub stream_id: u32,
    /// Sequence number of the first entry in this batch.
    pub first_seq: u64,
    /// Number of entries in the batch.
    pub entry_count: u32,
    /// Timestamp (millis since epoch) when the batch was appended.
    pub timestamp_ms: u64,
    /// Raw serialized entry bytes: each entry is `[entry_len:u32][Record bytes]`.
    pub entries_bytes: Vec<u8>,
}

// ── Frame builder ────────────────────────────────────────────────────────

/// Build a complete replication frame (RaftFrameHeader + ReplicateEntriesHeader + entries).
///
/// The RaftFrameHeader is constructed manually because the struct is
/// `pub(crate)` to `arbitro-raft`. We fill the 32-byte header inline
/// using the public constants (`RAFT_MAGIC`, `RAFT_VERSION`,
/// `RAFT_FRAME_HEADER_SIZE`).
pub fn build_replicate_entries_frame(
    batch: &ReplicationBatch,
    from_peer_id: u64,
) -> Vec<u8> {
    use arbitro_raft::{RAFT_FRAME_HEADER_SIZE, RAFT_MAGIC, RAFT_VERSION};
    use zerocopy::IntoBytes;

    let body_len = REPLICATE_ENTRIES_HEADER_SIZE + batch.entries_bytes.len();
    let total = RAFT_FRAME_HEADER_SIZE + body_len;
    let mut frame = Vec::with_capacity(total);

    // ── RaftFrameHeader (32 bytes) ────────────────────────────────────
    // [0..4]   magic    : U32 LE
    // [4]      version  : u8
    // [5]      kind     : u8  (KIND_REPLICATE_ENTRIES = 20)
    // [6..8]   flags    : U16 LE
    // [8..16]  from     : U64 LE (peer_id)
    // [16..20] body_len : U32 LE
    // [20..24] reserved : U32 LE
    // [24..32] _pad     : U64 LE
    frame.extend_from_slice(&RAFT_MAGIC.to_le_bytes());       // [0..4]
    frame.push(RAFT_VERSION);                                  // [4]
    frame.push(KIND_REPLICATE_ENTRIES);                        // [5]
    frame.extend_from_slice(&0u16.to_le_bytes());              // [6..8]
    frame.extend_from_slice(&from_peer_id.to_le_bytes());      // [8..16]
    frame.extend_from_slice(&(body_len as u32).to_le_bytes()); // [16..20]
    frame.extend_from_slice(&0u32.to_le_bytes());              // [20..24]
    frame.extend_from_slice(&0u64.to_le_bytes());              // [24..32]
    debug_assert_eq!(frame.len(), RAFT_FRAME_HEADER_SIZE);

    // ── ReplicateEntriesHeader (28 bytes) ─────────────────────────────
    let header = ReplicateEntriesHeader {
        stream_id: zerocopy::byteorder::little_endian::U32::new(batch.stream_id),
        first_seq: zerocopy::byteorder::little_endian::U64::new(batch.first_seq),
        entry_count: zerocopy::byteorder::little_endian::U32::new(batch.entry_count),
        _reserved: zerocopy::byteorder::little_endian::U32::new(0),
        timestamp_ms: zerocopy::byteorder::little_endian::U64::new(batch.timestamp_ms),
    };
    frame.extend_from_slice(header.as_bytes());

    // ── Entry payload ─────────────────────────────────────────────────
    frame.extend_from_slice(&batch.entries_bytes);
    debug_assert_eq!(frame.len(), total);

    frame
}

/// Build a ReplicateAck frame (RaftFrameHeader + ReplicateAckBody).
pub fn build_replicate_ack_frame(
    stream_id: u32,
    last_seq: u64,
    from_peer_id: u64,
) -> Vec<u8> {
    use arbitro_raft::{RAFT_FRAME_HEADER_SIZE, RAFT_MAGIC, RAFT_VERSION};
    use zerocopy::IntoBytes;

    let body_len = REPLICATE_ACK_BODY_SIZE;
    let total = RAFT_FRAME_HEADER_SIZE + body_len;
    let mut frame = Vec::with_capacity(total);

    // RaftFrameHeader
    frame.extend_from_slice(&RAFT_MAGIC.to_le_bytes());
    frame.push(RAFT_VERSION);
    frame.push(KIND_REPLICATE_ACK);
    frame.extend_from_slice(&0u16.to_le_bytes());
    frame.extend_from_slice(&from_peer_id.to_le_bytes());
    frame.extend_from_slice(&(body_len as u32).to_le_bytes());
    frame.extend_from_slice(&0u32.to_le_bytes());
    frame.extend_from_slice(&0u64.to_le_bytes());
    debug_assert_eq!(frame.len(), RAFT_FRAME_HEADER_SIZE);

    // ReplicateAckBody
    let body = ReplicateAckBody {
        stream_id: zerocopy::byteorder::little_endian::U32::new(stream_id),
        last_seq: zerocopy::byteorder::little_endian::U64::new(last_seq),
        _reserved: zerocopy::byteorder::little_endian::U32::new(0),
    };
    frame.extend_from_slice(body.as_bytes());
    debug_assert_eq!(frame.len(), total);

    frame
}

/// Extract the `kind` byte from offset 5 of a raw frame. Returns None if
/// the frame is too short to contain a RaftFrameHeader.
#[inline]
pub fn frame_kind(raw: &[u8]) -> Option<u8> {
    if raw.len() < arbitro_raft::RAFT_FRAME_HEADER_SIZE {
        return None;
    }
    Some(raw[5])
}

/// Extract the `from` (peer_id) field from a raw frame header.
#[inline]
pub fn frame_from_peer(raw: &[u8]) -> u64 {
    u64::from_le_bytes(raw[8..16].try_into().unwrap_or([0; 8]))
}

// ── Replication loop (leader side) ──────────────────────────────────────

use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Per-peer connection state for the replication sender.
struct PeerConn {
    stream: TcpStream,
}

/// Leader-side replication loop. Spawned as a tokio task in `server.rs`
/// when the cluster boots. Receives `ReplicationBatch`es from shard
/// workers and sends `ReplicateEntries` frames to all follower peers.
///
/// Each peer gets its own lazy TCP connection (separate from the Raft
/// transport's connection pool). The loop re-opens connections on error.
pub async fn replication_loop(
    mut rx: tokio::sync::mpsc::Receiver<ReplicationBatch>,
    my_peer_id: u64,
    peer_addrs: HashMap<PeerId, SocketAddr>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    // My peer_id — never send to self.
    let my_pid = PeerId(my_peer_id);

    // Lazy connection map. Re-created on error.
    let mut conns: HashMap<PeerId, PeerConn> = HashMap::new();

    loop {
        let batch = tokio::select! {
            biased;
            _ = shutdown.changed() => {
                tracing::debug!("replication loop shutting down");
                return;
            }
            batch = rx.recv() => {
                match batch {
                    Some(b) => b,
                    None => return, // channel closed
                }
            }
        };

        let frame = build_replicate_entries_frame(&batch, my_peer_id);
        let frame_bytes = bytes::Bytes::from(frame);

        // Send to all peers except self.
        for (peer_id, addr) in &peer_addrs {
            if *peer_id == my_pid {
                continue;
            }

            // Get or create connection, then send.
            let need_connect = !conns.contains_key(peer_id);
            if need_connect {
                match TcpStream::connect(addr).await {
                    Ok(stream) => {
                        let _ = stream.set_nodelay(true);
                        conns.insert(*peer_id, PeerConn { stream });
                    }
                    Err(e) => {
                        tracing::debug!(
                            peer = peer_id.0,
                            error = %e,
                            "replication: failed to connect to peer"
                        );
                        continue;
                    }
                }
            }

            if let Some(conn) = conns.get_mut(peer_id) {
                if let Err(e) = conn.stream.write_all(&frame_bytes).await {
                    tracing::debug!(
                        peer = peer_id.0,
                        error = %e,
                        "replication: write failed, dropping connection"
                    );
                    conns.remove(peer_id);
                }
            }
        }
    }
}

// ── Follower receive loop ───────────────────────────────────────────────

/// Follower-side replication handler. Spawned as a tokio task in
/// `server.rs` when the cluster boots. Receives `ReplicateEntries`
/// frames from the transport's replication channel (extracted via
/// `TcpRaftTransport::take_replication_rx`), appends entries to the
/// local store, and sends `ReplicateAck` frames back to the leader.
pub async fn follower_replication_loop(
    mut repl_rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    store_lookup: crate::shard::router::ShardRouter,
    my_peer_id: u64,
    peer_addrs: HashMap<PeerId, SocketAddr>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    use zerocopy::FromBytes;

    // Lazy connection map for sending acks back.
    let mut ack_conns: HashMap<PeerId, TcpStream> = HashMap::new();

    loop {
        let frame = tokio::select! {
            biased;
            _ = shutdown.changed() => {
                tracing::debug!("follower replication loop shutting down");
                return;
            }
            frame = repl_rx.recv() => {
                match frame {
                    Some(f) => f,
                    None => return, // channel closed
                }
            }
        };

        let Some(kind) = frame_kind(&frame) else {
            continue;
        };

        match kind {
            KIND_REPLICATE_ENTRIES => {
                let from_peer = frame_from_peer(&frame);
                let body = &frame[arbitro_raft::RAFT_FRAME_HEADER_SIZE..];

                if body.len() < REPLICATE_ENTRIES_HEADER_SIZE {
                    tracing::warn!("replication: frame body too short for header");
                    continue;
                }

                let Some(header) =
                    ReplicateEntriesHeader::ref_from_bytes(
                        &body[..REPLICATE_ENTRIES_HEADER_SIZE],
                    )
                    .ok()
                else {
                    tracing::warn!("replication: failed to parse ReplicateEntriesHeader");
                    continue;
                };

                let stream_id_raw = header.stream_id.get();
                let _first_seq = header.first_seq.get();
                let entry_count = header.entry_count.get();
                let timestamp_ms = header.timestamp_ms.get();
                let entries_data = &body[REPLICATE_ENTRIES_HEADER_SIZE..];

                // Parse entries: each is [total_len:u32][subject_len:u16][subject][payload]
                let mut refs: Vec<arbitro_store::EntryRef<'_>> = Vec::with_capacity(entry_count as usize);
                let mut offset = 0usize;
                for _ in 0..entry_count {
                    if offset + 4 > entries_data.len() {
                        break;
                    }
                    let total_len = u32::from_le_bytes(
                        entries_data[offset..offset + 4].try_into().unwrap(),
                    ) as usize;
                    offset += 4;
                    if offset + total_len > entries_data.len() {
                        break;
                    }
                    let entry_bytes = &entries_data[offset..offset + total_len];
                    offset += total_len;

                    if entry_bytes.len() < 2 {
                        continue;
                    }
                    let subj_len =
                        u16::from_le_bytes([entry_bytes[0], entry_bytes[1]]) as usize;
                    if 2 + subj_len > entry_bytes.len() {
                        continue;
                    }
                    let subject = &entry_bytes[2..2 + subj_len];
                    let payload = &entry_bytes[2 + subj_len..];

                    refs.push(arbitro_store::EntryRef {
                        stream_id: stream_id_raw,
                        subject,
                        payload,
                        flags: 0,
                        deliver_at_ms: 0,
                    });
                }

                // Append to local store.
                let stream_id = arbitro_engine_v2::types::StreamId(stream_id_raw);
                let store = store_lookup.store_for(stream_id);
                let last_seq = match store.lock().append_batch(&refs, timestamp_ms) {
                    Ok(first) => {
                        let count = refs.len() as u64;
                        let last = first + count.saturating_sub(1);
                        // Wake the drain so followers can deliver to
                        // local consumers.
                        store_lookup.gate_for(stream_id).release();
                        tracing::debug!(
                            stream_id = stream_id_raw,
                            first_seq = first,
                            count = count,
                            "replication: appended entries from leader"
                        );
                        last
                    }
                    Err(e) => {
                        tracing::warn!(
                            stream_id = stream_id_raw,
                            error = ?e,
                            "replication: failed to append entries"
                        );
                        continue;
                    }
                };

                // Send ack back to leader.
                let ack_frame = build_replicate_ack_frame(
                    stream_id_raw,
                    last_seq,
                    my_peer_id,
                );
                let leader_peer = PeerId(from_peer);
                if let Some(addr) = peer_addrs.get(&leader_peer) {
                    // Ensure we have a connection to the leader.
                    if let std::collections::hash_map::Entry::Vacant(e) =
                        ack_conns.entry(leader_peer)
                    {
                        if let Ok(stream) = TcpStream::connect(addr).await {
                            let _ = stream.set_nodelay(true);
                            e.insert(stream);
                        }
                    }
                    if let Some(conn) = ack_conns.get_mut(&leader_peer) {
                        if conn.write_all(&ack_frame).await.is_err() {
                            ack_conns.remove(&leader_peer);
                        }
                    }
                }
            }
            KIND_REPLICATE_ACK => {
                // Leader receives ack from follower — update ISR tracker.
                let body = &frame[arbitro_raft::RAFT_FRAME_HEADER_SIZE..];
                if body.len() < REPLICATE_ACK_BODY_SIZE {
                    continue;
                }
                let Some(ack) =
                    ReplicateAckBody::ref_from_bytes(&body[..REPLICATE_ACK_BODY_SIZE]).ok()
                else {
                    continue;
                };
                let from_peer = frame_from_peer(&frame);
                tracing::debug!(
                    from_peer = from_peer,
                    stream_id = ack.stream_id.get(),
                    last_seq = ack.last_seq.get(),
                    "replication: received ack from follower"
                );
                // ISR tracking is informational for v1 — quorum wait
                // will be added in a follow-up.
            }
            _ => {
                tracing::debug!(kind, "replication: ignoring unknown frame kind");
            }
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicate_entries_frame_roundtrip() {
        use zerocopy::FromBytes;

        // Build a batch with 2 entries.
        let mut entries_bytes = Vec::new();
        // Entry 1: subject="orders.new", payload="hello"
        let subj1 = b"orders.new";
        let pay1 = b"hello";
        let total1 = 2 + subj1.len() + pay1.len();
        entries_bytes.extend_from_slice(&(total1 as u32).to_le_bytes());
        entries_bytes.extend_from_slice(&(subj1.len() as u16).to_le_bytes());
        entries_bytes.extend_from_slice(subj1);
        entries_bytes.extend_from_slice(pay1);

        // Entry 2: subject="x", payload="world"
        let subj2 = b"x";
        let pay2 = b"world";
        let total2 = 2 + subj2.len() + pay2.len();
        entries_bytes.extend_from_slice(&(total2 as u32).to_le_bytes());
        entries_bytes.extend_from_slice(&(subj2.len() as u16).to_le_bytes());
        entries_bytes.extend_from_slice(subj2);
        entries_bytes.extend_from_slice(pay2);

        let batch = ReplicationBatch {
            stream_id: 42,
            first_seq: 100,
            entry_count: 2,
            timestamp_ms: 1_700_000_000_000,
            entries_bytes,
        };

        let frame = build_replicate_entries_frame(&batch, 7);

        // Verify frame header.
        assert_eq!(frame.len(), arbitro_raft::RAFT_FRAME_HEADER_SIZE + REPLICATE_ENTRIES_HEADER_SIZE + batch.entries_bytes.len());
        assert_eq!(frame[5], KIND_REPLICATE_ENTRIES);
        assert_eq!(frame_kind(&frame), Some(KIND_REPLICATE_ENTRIES));
        assert_eq!(frame_from_peer(&frame), 7);

        // Parse the ReplicateEntriesHeader from the body.
        let body = &frame[arbitro_raft::RAFT_FRAME_HEADER_SIZE..];
        let header = ReplicateEntriesHeader::ref_from_bytes(
            &body[..REPLICATE_ENTRIES_HEADER_SIZE],
        )
        .unwrap();
        assert_eq!(header.stream_id.get(), 42);
        assert_eq!(header.first_seq.get(), 100);
        assert_eq!(header.entry_count.get(), 2);
        assert_eq!(header.timestamp_ms.get(), 1_700_000_000_000);

        // Parse entries back.
        let entries_data = &body[REPLICATE_ENTRIES_HEADER_SIZE..];
        let mut offset = 0usize;
        // Entry 1
        let total_len = u32::from_le_bytes(
            entries_data[offset..offset + 4].try_into().unwrap(),
        ) as usize;
        offset += 4;
        let entry = &entries_data[offset..offset + total_len];
        offset += total_len;
        let slen = u16::from_le_bytes([entry[0], entry[1]]) as usize;
        assert_eq!(&entry[2..2 + slen], b"orders.new");
        assert_eq!(&entry[2 + slen..], b"hello");

        // Entry 2
        let total_len = u32::from_le_bytes(
            entries_data[offset..offset + 4].try_into().unwrap(),
        ) as usize;
        offset += 4;
        let entry = &entries_data[offset..offset + total_len];
        let slen = u16::from_le_bytes([entry[0], entry[1]]) as usize;
        assert_eq!(&entry[2..2 + slen], b"x");
        assert_eq!(&entry[2 + slen..], b"world");
    }

    #[test]
    fn replicate_ack_frame_roundtrip() {
        use zerocopy::FromBytes;

        let frame = build_replicate_ack_frame(42, 999, 3);

        assert_eq!(
            frame.len(),
            arbitro_raft::RAFT_FRAME_HEADER_SIZE + REPLICATE_ACK_BODY_SIZE
        );
        assert_eq!(frame_kind(&frame), Some(KIND_REPLICATE_ACK));
        assert_eq!(frame_from_peer(&frame), 3);

        let body = &frame[arbitro_raft::RAFT_FRAME_HEADER_SIZE..];
        let ack = ReplicateAckBody::ref_from_bytes(&body[..REPLICATE_ACK_BODY_SIZE]).unwrap();
        assert_eq!(ack.stream_id.get(), 42);
        assert_eq!(ack.last_seq.get(), 999);
    }

    #[test]
    fn isr_tracker_basic() {
        let mut tracker = IsrTracker::new(Duration::from_secs(10));
        let s1 = StreamId(1);
        let p1 = PeerId(10);
        let p2 = PeerId(20);

        assert_eq!(tracker.isr_count(s1), 1); // leader only

        tracker.add(s1, p1);
        tracker.add(s1, p2);
        assert_eq!(tracker.isr_count(s1), 3); // leader + 2 followers

        tracker.record_ack(s1, p1);
        tracker.record_ack(s1, p2);

        // No ejections yet.
        assert!(tracker.tick().is_empty());
    }

    #[test]
    fn high_watermarks_advance_only() {
        let mut hw = HighWatermarks::new();
        let s = StreamId(1);

        assert_eq!(hw.get(s), 0);
        hw.update(s, 10);
        assert_eq!(hw.get(s), 10);
        hw.update(s, 5); // should NOT go back
        assert_eq!(hw.get(s), 10);
        hw.update(s, 20);
        assert_eq!(hw.get(s), 20);
    }
}
