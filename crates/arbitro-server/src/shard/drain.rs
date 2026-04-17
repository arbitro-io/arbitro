//! Reactive drain — linear walk of the shard store, delivering messages
//! to subscribed consumers. **Zero Mutex, zero engine access.**
//!
//! The drain reads `SharedCounters` (atomics) for demand/capacity/paused
//! checks and `DrainSnapshot` (ArcSwap) for bindings and match tables.
//! After successful delivery, it increments atomic inflight counters and
//! pushes notifications to the command thread via a lock-free channel.
//!
//! **Batching**: entries going to the same binding are accumulated into
//! multi-entry RepBatch frames (default 256 entries/frame).
//!
//! lifecycle_trace stage-ids preserved from legacy drainer:
//! - 21_drainer_enter, 25_drain_loop_start, 27_store_get_loop_start,
//!   29_frame_built, 30_send_bytes_done, 33_drainer_exit_released,
//!   33_drainer_exit_locked

use std::sync::Arc;

use arbitro_engine_v2::catalog::fnv1a_32;
use arbitro_engine_v2::catalog::match_table::MatchEntry;
use arbitro_engine_v2::command::DeliveredEntry;
use arbitro_engine_v2::types::*;
use arbitro_proto::action::Action;
use arbitro_proto::wire::delivery::{DeliveryEntryHeader, RepBatchFixed};
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

// ── Pending batch ───────────────────────────────────────────────────────────

/// Per-entry metadata tracked alongside the wire body.
struct PendingDelivery {
    entry: DeliveredEntry,
    binding_idx: usize,
    consumer_id: u32,
    queue_id: u32,
    fire_and_forget: bool,
}

struct PendingBatch {
    /// Connection index — all entries in a batch target the same connection.
    connection_id: Option<ConnectionId>,
    /// Index of any binding on this connection (for tx handle).
    tx_binding_idx: Option<usize>,
    count: u16,
    first_seq: u64,
    stream_id: StreamId,
    delivered: Vec<PendingDelivery>,
}

impl PendingBatch {
    fn new() -> Self {
        Self {
            connection_id: None,
            tx_binding_idx: None,
            count: 0,
            first_seq: 0,
            stream_id: StreamId(0),
            delivered: Vec::with_capacity(256),
        }
    }

    fn reset(&mut self) {
        self.connection_id = None;
        self.tx_binding_idx = None;
        self.count = 0;
        self.delivered.clear();
    }
}

// ── Scratch buffers ─────────────────────────────────────────────────────────

pub(in crate::shard) struct DrainScratch {
    body: BytesMut,
    matches: Vec<MatchEntry>,
    served_queues: Vec<QueueId>,
    dead_connections: Vec<ConnectionId>,
    pending: PendingBatch,
    /// Local pattern resolution cache. Avoids mutating shared match table.
    resolve_cache: std::collections::HashMap<(u32, u32), Vec<MatchEntry>>,
    /// Local subject limit cache. (stream_id, subject_hash) → Option<max>.
    subject_limit_cache: std::collections::HashMap<(u32, u32), Option<u32>>,
}

impl DrainScratch {
    pub(in crate::shard) fn new() -> Self {
        Self {
            body: BytesMut::with_capacity(64 * 1024),
            matches: Vec::with_capacity(16),
            served_queues: Vec::with_capacity(8),
            dead_connections: Vec::with_capacity(4),
            pending: PendingBatch::new(),
            resolve_cache: std::collections::HashMap::with_capacity(64),
            subject_limit_cache: std::collections::HashMap::with_capacity(64),
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
    scratch.pending.reset();

    crate::lifecycle_trace!("25_drain_loop_start", start, end, "shard");

    let mut channel_full = false;
    let batch_size = cfg.batch_size;

    store
        .for_each(start, end, &mut |entry| {
            if channel_full {
                track_skipped(&mut lowest_skipped, entry.seq);
                return;
            }
            process_drain_entry(
                counters, snap, entry, scratch, names,
                now_ms, cfg.max_age_ms, batch_size, notify_tx,
                &mut more_pending, &mut lowest_skipped,
                &mut channel_full,
            );
        })
        .ok();

    // Flush remaining partial batch.
    flush_pending_batch(
        counters,
        &mut scratch.body,
        &mut scratch.pending,
        &snap.bindings,
        names,
        notify_tx,
        &mut more_pending,
        &mut lowest_skipped,
        &mut channel_full,
        &mut scratch.dead_connections,
    );

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
    names: &Arc<crate::common::NameRegistry>,
    now_ms: u64,
    max_age_ms: u64,
    batch_size: u16,
    notify_tx: &mpsc::Sender<DrainNotification>,
    more_pending: &mut bool,
    lowest_skipped: &mut Option<u64>,
    channel_full: &mut bool,
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
        // Check if subject is already resolved in the match table.
        // If not, resolve from trie and cache locally.
        let lookup = mt.lookup(subject_hash);
        if lookup.is_empty() {
            // Check local resolve cache.
            let cache_key = (stream_id.raw(), subject_hash);
            if !scratch.resolve_cache.contains_key(&cache_key) {
                // Resolve from trie — read-only on the snapshot.
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
                if !counters.subject_has_room(subject_hash, max) {
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
        // Also add locally resolved entries.
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
        counters, entry, stream_id, subject_hash, scratch,
        &snap.bindings, &snap.binding_index, names,
        batch_size, notify_tx,
        more_pending, lowest_skipped, channel_full,
    );
}

// ── Per-recipient dispatch ──────────────────────────────────────────────────

fn dispatch_recipients(
    counters: &SharedCounters,
    entry: &arbitro_store::Entry<'_>,
    stream_id: StreamId,
    subject_hash: u32,
    scratch: &mut DrainScratch,
    bindings: &[ActiveBinding],
    binding_index: &std::collections::HashMap<(u32, u64), usize>,
    names: &Arc<crate::common::NameRegistry>,
    batch_size: u16,
    notify_tx: &mpsc::Sender<DrainNotification>,
    more_pending: &mut bool,
    lowest_skipped: &mut Option<u64>,
    channel_full: &mut bool,
) {
    let DrainScratch {
        body,
        matches,
        served_queues,
        dead_connections,
        pending,
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

        if served_queues.contains(&queue_id) {
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

        // Paused check — atomic read, immediate effect.
        if counters.is_paused(consumer_id.0) {
            *more_pending = true;
            track_skipped(lowest_skipped, entry.seq);
            continue;
        }

        // Capacity check — atomic read.
        if !binding.fire_and_forget
            && !counters.consumer_has_capacity(consumer_id.0, binding.max_inflight)
        {
            *more_pending = true;
            track_skipped(lowest_skipped, entry.seq);
            continue;
        }

        // ── Batch accumulation (grouped by connection) ────────────────

        if let Some(prev_conn) = pending.connection_id {
            if prev_conn != connection_id {
                flush_pending_batch(
                    counters, body, pending, bindings, names,
                    notify_tx, more_pending, lowest_skipped,
                    channel_full, dead_connections,
                );
                if *channel_full {
                    track_skipped(lowest_skipped, entry.seq);
                    return;
                }
            }
        }

        if pending.connection_id.is_none() {
            body.clear();
            body.extend_from_slice(&[0u8; ENVELOPE_SIZE]);
            body.extend_from_slice(
                RepBatchFixed {
                    count: U16::new(0),
                    _pad: U16::new(0),
                }
                .as_bytes(),
            );
            pending.connection_id = Some(connection_id);
            pending.tx_binding_idx = Some(binding_idx);
            pending.count = 0;
            pending.first_seq = entry.seq;
            pending.stream_id = stream_id;
            pending.delivered.clear();
        }

        let subj_len = entry.subject.len();
        let data_len = subj_len + entry.payload.len();
        body.extend_from_slice(
            DeliveryEntryHeader {
                consumer_id: U32::new(consumer_id.0),
                seq: U64::new(entry.seq),
                subj_len: U16::new(subj_len as u16),
                data_len: U32::new(data_len as u32),
                subject_hash: U32::new(subject_hash),
            }
            .as_bytes(),
        );
        body.extend_from_slice(entry.subject);
        body.extend_from_slice(entry.payload);

        pending.count += 1;
        pending.delivered.push(PendingDelivery {
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

        served_queues.push(queue_id);

        // Flush only when batch reaches max size (fire-and-forget accumulation).
        // For ack consumers, flush happens AFTER all recipients of this entry
        // are processed — see below.
        if pending.count >= batch_size {
            flush_pending_batch(
                counters, body, pending, bindings, names,
                notify_tx, more_pending, lowest_skipped,
                channel_full, dead_connections,
            );
            if *channel_full {
                return;
            }
        }
    }

    // After all recipients for this entry: flush if any ack-mode entry was added.
    // This keeps latency low for explicit-ack consumers while still grouping
    // all recipients of the same store entry into one frame.
    if pending.count > 0
        && pending.delivered.iter().any(|d| !d.fire_and_forget)
    {
        flush_pending_batch(
            counters, body, pending, bindings, names,
            notify_tx, more_pending, lowest_skipped,
            channel_full, dead_connections,
        );
    }
}

// ── Batch flush ─────────────────────────────────────────────────────────────

fn flush_pending_batch(
    counters: &SharedCounters,
    body: &mut BytesMut,
    pending: &mut PendingBatch,
    bindings: &[ActiveBinding],
    names: &Arc<crate::common::NameRegistry>,
    notify_tx: &mpsc::Sender<DrainNotification>,
    more_pending: &mut bool,
    lowest_skipped: &mut Option<u64>,
    channel_full: &mut bool,
    dead_connections: &mut Vec<ConnectionId>,
) {
    let tx_binding_idx = match pending.tx_binding_idx {
        Some(idx) => idx,
        None => return,
    };
    if pending.count == 0 {
        pending.reset();
        return;
    }

    let tx_binding = &bindings[tx_binding_idx];
    let stream_id = pending.stream_id;
    let connection_id = pending.connection_id.unwrap_or(ConnectionId(0));

    // Patch RepBatchFixed count (offset 0 in fixed header, right after envelope).
    let count_offset = ENVELOPE_SIZE;
    body[count_offset..count_offset + 2]
        .copy_from_slice(&pending.count.to_le_bytes());

    // Patch envelope.
    let body_len = body.len() - ENVELOPE_SIZE;
    let wire_stream_id = names
        .stream_wire(stream_id)
        .unwrap_or_else(|| stream_id.raw());
    let envelope = Envelope::new(
        Action::RepBatch,
        wire_stream_id,
        body_len as u32,
        0,
    );
    body[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());

    crate::lifecycle_trace!(
        "29_frame_built",
        connection_id.0,
        pending.count as u64,
        "shard"
    );

    let frozen = body.split().freeze();
    match tx_binding.tx.try_send(frozen) {
        Ok(()) => {
            crate::lifecycle_trace!(
                "30_send_bytes_done",
                connection_id.0,
                pending.count as u64,
                "shard"
            );

            // Increment atomic inflight counters (per-entry metadata).
            for pd in &pending.delivered {
                if !pd.fire_and_forget {
                    counters.inc_inflight(pd.consumer_id, pd.queue_id);
                    counters.inc_subject(pd.entry.subject_hash);
                }
            }

            // Notify command thread — group by binding_id.
            notify_delivered_grouped(
                notify_tx,
                bindings,
                &mut pending.delivered,
            );
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            *more_pending = true;
            *channel_full = true;
            track_skipped(lowest_skipped, pending.first_seq);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            dead_connections.push(connection_id);
        }
    }

    pending.reset();
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
    // Sort by binding_idx to group contiguous runs.
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
