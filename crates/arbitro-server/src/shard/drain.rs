//! Reactive drain — linear walk of the shard store, delivering messages
//! to subscribed consumers. **Zero Mutex, zero engine access.**
//!
//! The drain reads `SharedCounters` (atomics) for demand/capacity/paused
//! checks and `DrainSnapshot` (ArcSwap) for bindings and match tables.
//! After successful delivery, it increments atomic inflight counters and
//! pushes notifications to the command thread via a lock-free channel.
//!
//! **Batching model**: during the walk, entries are accumulated into a
//! `HashMap<(ConnectionId, StreamId), Bucket>` local to the cycle. Every
//! recipient of an entry appends to the bucket of its connection. At the
//! end of the walk, a single flush phase iterates the buckets and emits
//! one frame per bucket. No mid-walk flush on connection change.
//!
//! lifecycle_trace stage-ids preserved from legacy drainer:
//! - 21_drainer_enter, 25_drain_loop_start, 27_store_get_loop_start,
//!   29_frame_built, 30_send_bytes_done, 33_drainer_exit_released,
//!   33_drainer_exit_locked

use std::collections::HashMap;
use std::sync::Arc;

use arbitro_engine_v2::catalog::fnv1a_32;
use arbitro_engine_v2::catalog::match_table::MatchEntry;
use arbitro_engine_v2::command::DeliveredEntry;
use arbitro_engine_v2::types::*;
use arbitro_proto::action::Action;
use arbitro_proto::wire::delivery::{
    DeliveryEntryHeader, RepBatchFixed, DELIVERY_ENTRY_HEADER_SIZE,
};
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_store::Store;
use bytes::BytesMut;
use tokio::sync::mpsc;
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use crate::common::Gate;
use crate::shard::shared::{DrainNotification, DrainSnapshot, SharedCounters};
use crate::shard::worker::ActiveBinding;

// ── Configuration ───────────────────────────────────────────────────────────

pub(in crate::shard) struct DrainConfig {
    pub max_feed: usize,
    pub max_age_ms: u64,
    pub batch_size: u16,
}

// ── Per-delivery metadata ───────────────────────────────────────────────────

/// Per-entry metadata tracked alongside the wire body.
struct PendingDelivery {
    entry: DeliveredEntry,
    binding_idx: usize,
    consumer_id: u32,
    queue_id: u32,
    fire_and_forget: bool,
}

// ── Per-connection bucket ───────────────────────────────────────────────────

/// One bucket per (connection, stream) active in the current cycle.
/// All entries sent to the same connection are accumulated here and
/// emitted as a single `RepBatch` frame at the end of the cycle.
struct Bucket {
    body: BytesMut,
    count: u16,
    first_seq: u64,
    stream_id: StreamId,
    /// Index of any binding on this connection — used to obtain the tx handle.
    tx_binding_idx: usize,
    delivered: Vec<PendingDelivery>,
}

impl Bucket {
    fn new(tx_binding_idx: usize, stream_id: StreamId, first_seq: u64) -> Self {
        let mut body = BytesMut::with_capacity(64 * 1024);
        // Envelope placeholder — patched at flush time.
        body.extend_from_slice(&[0u8; ENVELOPE_SIZE]);
        // RepBatchFixed — count patched at flush time.
        body.extend_from_slice(
            RepBatchFixed {
                count: U16::new(0),
                _pad: U16::new(0),
            }
            .as_bytes(),
        );
        Self {
            body,
            count: 0,
            first_seq,
            stream_id,
            tx_binding_idx,
            delivered: Vec::with_capacity(256),
        }
    }
}

// ── Scratch buffers ─────────────────────────────────────────────────────────

pub(in crate::shard) struct DrainScratch {
    matches: Vec<MatchEntry>,
    served_queues: Vec<QueueId>,
    dead_connections: Vec<ConnectionId>,
    /// Local pattern resolution cache. Avoids mutating shared match table.
    resolve_cache: HashMap<(u32, u32), Vec<MatchEntry>>,
    /// Local subject limit cache. (stream_id, subject_hash) → Option<max>.
    subject_limit_cache: HashMap<(u32, u32), Option<u32>>,
}

impl DrainScratch {
    pub(in crate::shard) fn new() -> Self {
        Self {
            matches: Vec::with_capacity(16),
            served_queues: Vec::with_capacity(8),
            dead_connections: Vec::with_capacity(4),
            resolve_cache: HashMap::with_capacity(64),
            subject_limit_cache: HashMap::with_capacity(64),
        }
    }
}

// ── Drain cycle (entry point) ───────────────────────────────────────────────

/// Run one drain cycle. Reads atomics + snapshot. Zero engine, zero Mutex.
pub(in crate::shard) fn drain_cycle(
    counters: &SharedCounters,
    snap: &DrainSnapshot,
    store: &dyn Store,
    gate: &Gate,
    names: &Arc<crate::common::NameRegistry>,
    cfg: &DrainConfig,
    scratch: &mut DrainScratch,
    notify_tx: &mpsc::Sender<DrainNotification>,
    now_ms: u64,
) {
    crate::lifecycle_trace!("21_drainer_enter", 0, snap.bindings.len() as u64, "shard");

    if !counters.has_any_demand() {
        gate.lock();
        crate::lifecycle_trace!("33_drainer_exit_locked", 0, 0, "shard");
        return;
    }

    let info = store.info();
    let cursor = counters.cursor();
    if info.last_seq <= cursor {
        gate.lock();
        crate::lifecycle_trace!("33_drainer_exit_locked", 0, 0, "shard");
        return;
    }

    let start = cursor + 1;
    let end = (start + cfg.max_feed as u64).min(info.last_seq + 1);
    let mut more_pending = false;
    let mut lowest_skipped: Option<u64> = None;

    scratch.dead_connections.clear();

    crate::lifecycle_trace!("25_drain_loop_start", start, end, "shard");

    // Per-cycle accumulator: one bucket per (conn, stream) that received
    // at least one entry during this walk.
    let mut batches: HashMap<(u64, u32), Bucket> = HashMap::with_capacity(8);

    // Per-cycle inflight deltas — counters are only incremented in phase 2
    // (on flush), so during the walk we must account for pending appends
    // locally to honor `max_inflight` and `max_subject_inflight` limits.
    let mut local_inflight: HashMap<u32, u32> = HashMap::new();
    let mut local_subject: HashMap<u32, u32> = HashMap::new();

    // Phase 1 — walk the store, accumulate into per-connection buckets.
    store
        .for_each(start, end, &mut |entry| {
            process_drain_entry(
                counters,
                snap,
                entry,
                scratch,
                &mut batches,
                &mut local_inflight,
                &mut local_subject,
                now_ms,
                cfg.max_age_ms,
                &mut more_pending,
                &mut lowest_skipped,
            );
        })
        .ok();

    // Phase 2 — flush every non-empty bucket. One frame per bucket.
    for ((conn_raw, _stream_raw), bucket) in batches.drain() {
        flush_bucket(
            counters,
            bucket,
            ConnectionId(conn_raw),
            &snap.bindings,
            names,
            notify_tx,
            &mut more_pending,
            &mut lowest_skipped,
            &mut scratch.dead_connections,
        );
    }

    // Cursor advances to last fully-processed entry.
    let new_cursor = lowest_skipped.map_or(end - 1, |ls| ls.saturating_sub(1));
    counters.set_cursor(new_cursor);

    if end <= info.last_seq || lowest_skipped.is_some() {
        more_pending = true;
    }

    // Notify command thread of dead connections.
    for conn_id in scratch.dead_connections.drain(..) {
        let _ = notify_tx.try_send(DrainNotification::ConnectionDead(conn_id));
    }

    if more_pending {
        gate.release();
        crate::lifecycle_trace!("33_drainer_exit_released", 0, 0, "shard");
    } else {
        gate.lock();
        crate::lifecycle_trace!("33_drainer_exit_locked", 0, 0, "shard");
    }
}

// ── Per-entry processing ────────────────────────────────────────────────────

fn process_drain_entry(
    counters: &SharedCounters,
    snap: &DrainSnapshot,
    entry: &arbitro_store::Entry<'_>,
    scratch: &mut DrainScratch,
    batches: &mut HashMap<(u64, u32), Bucket>,
    local_inflight: &mut HashMap<u32, u32>,
    local_subject: &mut HashMap<u32, u32>,
    now_ms: u64,
    max_age_ms: u64,
    more_pending: &mut bool,
    lowest_skipped: &mut Option<u64>,
) {
    let stream_id = StreamId(entry.stream_id);

    // TTL expiration.
    if max_age_ms > 0 && entry.timestamp > 0 && entry.timestamp + max_age_ms <= now_ms {
        return;
    }

    if entry.flags & arbitro_store::flags::TOMBSTONE != 0 {
        return;
    }

    // Demand check — atomic read.
    if !counters.has_demand(stream_id.raw()) {
        return;
    }

    let subject_hash = fnv1a_32(entry.subject);

    // Resolve patterns from local cache (no engine mutation needed).
    let stream_raw = stream_id.raw() as usize;
    if let Some(mt) = snap.match_tables.get(stream_raw).and_then(|o| o.as_ref()) {
        let lookup = mt.lookup(subject_hash);
        if lookup.is_empty() {
            let cache_key = (stream_id.raw(), subject_hash);
            if !scratch.resolve_cache.contains_key(&cache_key) {
                let mut resolved = Vec::new();
                mt.resolve_patterns_readonly(subject_hash, entry.subject, &mut resolved);
                scratch.resolve_cache.insert(cache_key, resolved);
            }
        }
    }

    // Subject inflight check — resolve limit and check atomic counter.
    if let Some(mt) = snap.match_tables.get(stream_raw).and_then(|o| o.as_ref()) {
        if mt.has_subject_limits() {
            let cache_key = (stream_id.raw(), subject_hash);
            let limit = scratch
                .subject_limit_cache
                .entry(cache_key)
                .or_insert_with(|| {
                    mt.resolve_subject_limit_readonly(subject_hash, entry.subject)
                });
            if let Some(max) = *limit {
                let pending = local_subject.get(&subject_hash).copied().unwrap_or(0);
                // Effective cap check: atomic + pending-in-this-cycle >= max.
                if pending >= max
                    || !counters.subject_has_room(subject_hash, max - pending)
                {
                    *more_pending = true;
                    track_skipped(lowest_skipped, entry.seq);
                    return;
                }
            }
        }
    }

    // Collect match entries into scratch.
    scratch.matches.clear();
    if let Some(mt) = snap.match_tables.get(stream_raw).and_then(|o| o.as_ref()) {
        let lookup = mt.lookup(subject_hash);
        scratch.matches.extend(lookup.iter());
        let cache_key = (stream_id.raw(), subject_hash);
        if let Some(resolved) = scratch.resolve_cache.get(&cache_key) {
            scratch.matches.extend(resolved.iter());
        }
    }

    if scratch.matches.is_empty() {
        return;
    }

    crate::lifecycle_trace!(
        "27_store_get_loop_start",
        0,
        scratch.matches.len() as u64,
        "shard"
    );

    dispatch_recipients(
        counters,
        entry,
        stream_id,
        subject_hash,
        scratch,
        batches,
        local_inflight,
        local_subject,
        &snap.bindings,
        &snap.binding_index,
        more_pending,
        lowest_skipped,
    );
}

// ── Per-recipient dispatch ──────────────────────────────────────────────────

fn dispatch_recipients(
    counters: &SharedCounters,
    entry: &arbitro_store::Entry<'_>,
    stream_id: StreamId,
    subject_hash: u32,
    scratch: &mut DrainScratch,
    batches: &mut HashMap<(u64, u32), Bucket>,
    local_inflight: &mut HashMap<u32, u32>,
    local_subject: &mut HashMap<u32, u32>,
    bindings: &[ActiveBinding],
    binding_index: &HashMap<(u32, u64), usize>,
    more_pending: &mut bool,
    lowest_skipped: &mut Option<u64>,
) {
    let DrainScratch {
        matches,
        served_queues,
        dead_connections,
        ..
    } = scratch;

    served_queues.clear();

    for i in 0..matches.len() {
        let me = matches[i];
        let consumer_id = me.consumer_id;
        let connection_id = me.connection_id;
        let queue_id = me.queue_id;

        if connection_id == ConnectionId(0) {
            continue;
        }

        // Queue dedup: one entry per queue within the match set of this entry.
        if queue_id != QueueId(0) && served_queues.contains(&queue_id) {
            continue;
        }

        if dead_connections.contains(&connection_id) {
            continue;
        }

        let binding_idx = match binding_index.get(&(consumer_id.0, connection_id.0)) {
            Some(&idx) => idx,
            None => continue,
        };
        let binding = &bindings[binding_idx];

        // Paused check — atomic read.
        if counters.is_paused(consumer_id.0) {
            *more_pending = true;
            track_skipped(lowest_skipped, entry.seq);
            continue;
        }

        // Capacity check — atomic read + pending-in-this-cycle local delta.
        if !binding.fire_and_forget {
            let pending = local_inflight.get(&consumer_id.0).copied().unwrap_or(0);
            if pending >= binding.max_inflight
                || !counters.consumer_has_capacity(
                    consumer_id.0,
                    binding.max_inflight - pending,
                )
            {
                *more_pending = true;
                track_skipped(lowest_skipped, entry.seq);
                continue;
            }
        }

        // ── Append to this connection's bucket (or create it) ────────────

        let key = (connection_id.0, stream_id.raw());
        let bucket = batches
            .entry(key)
            .or_insert_with(|| Bucket::new(binding_idx, stream_id, entry.seq));

        let subj_len = entry.subject.len();
        let data_len = subj_len + entry.payload.len();
        // Build (header + subject + payload) in a stack scratch, then emit
        // with a single `extend_from_slice`. Three separate extends cost
        // 3 capacity checks + 3 non-vectorisable memcpys. One contiguous
        // memcpy lets AVX move it in a handful of instructions.
        //
        // Falls back to 3 extends for oversized payloads (> 4 KB).
        const ENTRY_SCRATCH_SIZE: usize = 4096;
        let total = DELIVERY_ENTRY_HEADER_SIZE + data_len;
        let header = DeliveryEntryHeader {
            consumer_id: U32::new(consumer_id.0),
            seq: U64::new(entry.seq),
            subj_len: U16::new(subj_len as u16),
            data_len: U32::new(data_len as u32),
            subject_hash: U32::new(subject_hash),
        };
        if total <= ENTRY_SCRATCH_SIZE {
            let mut scratch_buf = [0u8; ENTRY_SCRATCH_SIZE];
            scratch_buf[..DELIVERY_ENTRY_HEADER_SIZE]
                .copy_from_slice(header.as_bytes());
            let subj_end = DELIVERY_ENTRY_HEADER_SIZE + subj_len;
            scratch_buf[DELIVERY_ENTRY_HEADER_SIZE..subj_end]
                .copy_from_slice(entry.subject);
            scratch_buf[subj_end..total].copy_from_slice(entry.payload);
            bucket.body.extend_from_slice(&scratch_buf[..total]);
        } else {
            bucket.body.extend_from_slice(header.as_bytes());
            bucket.body.extend_from_slice(entry.subject);
            bucket.body.extend_from_slice(entry.payload);
        }

        bucket.count += 1;
        bucket.delivered.push(PendingDelivery {
            entry: DeliveredEntry {
                seq: entry.seq,
                subject_hash,
                _pad: 0,
            },
            binding_idx,
            consumer_id: consumer_id.0,
            queue_id: queue_id.0,
            fire_and_forget: binding.fire_and_forget,
        });

        // Track pending inflight for subsequent capacity/subject checks in
        // this same cycle. Atomic counters are incremented in phase 2.
        if !binding.fire_and_forget {
            *local_inflight.entry(consumer_id.0).or_insert(0) += 1;
            *local_subject.entry(subject_hash).or_insert(0) += 1;
        }

        if queue_id != QueueId(0) {
            served_queues.push(queue_id);
        }
    }
}

// ── Bucket flush ────────────────────────────────────────────────────────────

/// Flush a single bucket: patch headers, try_send, notify command thread.
/// Consumes the bucket — called only once per bucket at end of cycle.
fn flush_bucket(
    counters: &SharedCounters,
    mut bucket: Bucket,
    connection_id: ConnectionId,
    bindings: &[ActiveBinding],
    names: &Arc<crate::common::NameRegistry>,
    notify_tx: &mpsc::Sender<DrainNotification>,
    more_pending: &mut bool,
    lowest_skipped: &mut Option<u64>,
    dead_connections: &mut Vec<ConnectionId>,
) {
    if bucket.count == 0 {
        return;
    }

    let tx_binding = &bindings[bucket.tx_binding_idx];
    let stream_id = bucket.stream_id;

    // Patch RepBatchFixed count (offset 0 in fixed header, right after envelope).
    let count_offset = ENVELOPE_SIZE;
    bucket.body[count_offset..count_offset + 2]
        .copy_from_slice(&bucket.count.to_le_bytes());

    // Patch envelope.
    let body_len = bucket.body.len() - ENVELOPE_SIZE;
    let wire_stream_id = names
        .stream_wire(stream_id)
        .unwrap_or_else(|| stream_id.raw());
    let envelope = Envelope::new(
        Action::RepBatch,
        wire_stream_id,
        body_len as u32,
        0,
    );
    bucket.body[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());

    crate::lifecycle_trace!(
        "29_frame_built",
        connection_id.0,
        bucket.count as u64,
        "shard"
    );

    let frozen = bucket.body.split().freeze();
    match tx_binding.tx.try_send(frozen) {
        Ok(()) => {
            crate::lifecycle_trace!(
                "30_send_bytes_done",
                connection_id.0,
                bucket.count as u64,
                "shard"
            );

            // Increment atomic inflight counters (per-entry metadata).
            for pd in &bucket.delivered {
                if !pd.fire_and_forget {
                    counters.inc_inflight(pd.consumer_id, pd.queue_id);
                    counters.inc_subject(pd.entry.subject_hash);
                }
            }

            // Notify command thread — group by binding_id.
            notify_delivered_grouped(notify_tx, bindings, &mut bucket.delivered);
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            *more_pending = true;
            track_skipped(lowest_skipped, bucket.first_seq);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            dead_connections.push(connection_id);
        }
    }
}

/// Group delivered entries by binding_id and send one notification per binding.
fn notify_delivered_grouped(
    notify_tx: &mpsc::Sender<DrainNotification>,
    bindings: &[ActiveBinding],
    delivered: &mut Vec<PendingDelivery>,
) {
    // Fast path: all entries belong to the same binding.
    if let Some(first) = delivered.first() {
        let first_idx = first.binding_idx;
        if delivered.iter().all(|d| d.binding_idx == first_idx) {
            let binding = &bindings[first_idx];
            let entries: Vec<DeliveredEntry> = delivered.iter().map(|d| d.entry).collect();
            let _ = notify_tx.try_send(DrainNotification::Delivered {
                binding_id: binding.binding_id,
                consumer_id: binding.consumer_id,
                queue_id: binding.queue_id,
                entries,
            });
            return;
        }
    }

    // Slow path: mixed bindings — group and send one notification each.
    delivered.sort_unstable_by_key(|d| d.binding_idx);
    let mut i = 0;
    while i < delivered.len() {
        let idx = delivered[i].binding_idx;
        let mut entries = Vec::new();
        while i < delivered.len() && delivered[i].binding_idx == idx {
            entries.push(delivered[i].entry);
            i += 1;
        }
        let binding = &bindings[idx];
        let _ = notify_tx.try_send(DrainNotification::Delivered {
            binding_id: binding.binding_id,
            consumer_id: binding.consumer_id,
            queue_id: binding.queue_id,
            entries,
        });
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

#[inline]
fn track_skipped(lowest: &mut Option<u64>, seq: u64) {
    *lowest = Some(lowest.map_or(seq, |s| s.min(seq)));
}
