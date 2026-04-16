//! Reactive drain — linear walk of the shard store, delivering messages
//! to subscribed consumers via the oracle engine.
//!
//! Level 7 — depends on engine, store, worker types.
//!
//! Replaces the legacy claim-based drainer. Instead of `engine.claim()` +
//! `store.get()`, we walk the store linearly and use the engine as an
//! oracle: `has_demand`, `subject_has_room`, `match_table`,
//! `consumer_has_capacity`, `is_paused`. After successful `try_send`, we
//! tell the engine via `execute(Delivered)`.
//!
//! **Batching**: entries going to the same binding are accumulated into
//! multi-entry RepBatch frames (default 256 entries/frame). This reduces
//! frames from N to N/batch_size, cutting try_send calls and TCP writes
//! proportionally. For replay (1 consumer, 500k msgs), frames drop from
//! 500k to ~2k.
//!
//! lifecycle_trace stage-ids preserved/adapted from legacy drainer:
//! - 21_drainer_enter, 25_drain_loop_start, 27_store_get_loop_start,
//!   29_frame_built, 30_send_bytes_done, 33_drainer_exit_released,
//!   33_drainer_exit_locked

use std::collections::HashMap;
use std::sync::Arc;

use arbitro_engine_v2::catalog::fnv1a_32;
use arbitro_engine_v2::catalog::match_table::MatchEntry;
use arbitro_engine_v2::command::{Command, DeliveredEntry};
use arbitro_engine_v2::types::*;
use arbitro_engine_v2::{ArbitroEngine, DeltaEvents, DropReason};
use arbitro_proto::action::Action;
use arbitro_proto::wire::delivery::{DeliveryEntryHeader, RepBatchFixed};
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_store::Store;
use bytes::BytesMut;
use tokio::sync::mpsc;
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use crate::common::Gate;
use crate::shard::worker::ActiveBinding;

// ── Configuration ───────────────────────────────────────────────────────────

/// Configuration for a drain cycle — avoids long parameter lists.
pub(in crate::shard) struct DrainConfig {
    /// Max entries to scan per cycle.
    pub max_feed: usize,
    /// Message TTL in milliseconds. 0 = no expiration.
    pub max_age_ms: u64,
    /// Max entries per RepBatch frame. Batching reduces frames from
    /// N to N/batch_size, cutting try_send and TCP write calls.
    pub batch_size: u16,
}

// ── Pending batch ───────────────────────────────────────────────────────────

/// Accumulator for a multi-entry RepBatch frame being built.
///
/// Entries going to the same binding are appended to `body` without
/// flushing. When the batch reaches `batch_size` or the binding changes
/// or the cycle ends, the frame is patched and sent via `try_send`.
struct PendingBatch {
    /// Index into the `bindings` slice. `None` = no batch in progress.
    binding_idx: Option<usize>,
    /// Number of entries accumulated so far.
    count: u16,
    /// Seq of the first entry in this batch (for cursor rewind on Full).
    first_seq: u64,
    /// Stream ID for the current batch.
    stream_id: StreamId,
    /// Entries to report to the engine after successful send.
    delivered: Vec<DeliveredEntry>,
}

impl PendingBatch {
    fn new() -> Self {
        Self {
            binding_idx: None,
            count: 0,
            first_seq: 0,
            stream_id: StreamId(0),
            delivered: Vec::with_capacity(256),
        }
    }

    fn reset(&mut self) {
        self.binding_idx = None;
        self.count = 0;
        self.delivered.clear();
    }
}

// ── Scratch buffers ─────────────────────────────────────────────────────────

/// Reusable scratch buffers for the drain hot path — pre-allocated at
/// worker init, `clear()` per cycle. Zero steady-state allocations.
pub(in crate::shard) struct DrainScratch {
    /// Assembled RepBatch body (envelope + fixed + entries).
    body: BytesMut,
    /// Match table entries for current entry.
    matches: Vec<MatchEntry>,
    /// Queue IDs served for current entry (dedup). O(k) scan where
    /// k = queue groups per entry, typically 1–3. Vec beats hash for k < 8.
    served_queues: Vec<QueueId>,
    /// Connections discovered dead during this cycle.
    dead_connections: Vec<ConnectionId>,
    /// O(1) binding lookup: `(consumer_id, connection_id)` → bindings index.
    /// Rebuilt once per cycle via `rebuild_binding_index`.
    binding_index: HashMap<(u32, u64), usize>,
    /// Multi-entry RepBatch accumulator.
    pending: PendingBatch,
}

impl DrainScratch {
    pub(in crate::shard) fn new() -> Self {
        Self {
            body: BytesMut::with_capacity(64 * 1024),
            matches: Vec::with_capacity(16),
            served_queues: Vec::with_capacity(8),
            dead_connections: Vec::with_capacity(4),
            binding_index: HashMap::with_capacity(16),
            pending: PendingBatch::new(),
        }
    }

    /// Rebuild O(1) binding index from the current bindings slice.
    /// Called once per drain cycle — management-path cost, hot-path benefit.
    fn rebuild_binding_index(&mut self, bindings: &[ActiveBinding]) {
        self.binding_index.clear();
        for (i, b) in bindings.iter().enumerate() {
            self.binding_index
                .insert((b.consumer_id.0, b.connection_id.0), i);
        }
    }
}

// ── Drain cycle (entry point) ──────────────────────────────────────────��────

/// Run one drain cycle. Returns `DeltaEvents` for the worker to process.
///
/// Called from the worker run loop when the gate is open. Walks the store
/// linearly from `cursor+1`, resolves recipients per entry via the engine
/// match table, accumulates entries into batched RepBatch frames, and sends
/// via cached `tx`. After successful send, tells the engine via
/// `execute(Delivered)`.
pub(in crate::shard) fn drain_cycle(
    engine: &mut ArbitroEngine,
    store: &dyn Store,
    cursor: &mut u64,
    rewind_cursor: &mut Option<u64>,
    bindings: &[ActiveBinding],
    gate: &Gate,
    names: &Arc<crate::common::NameRegistry>,
    cfg: &DrainConfig,
    scratch: &mut DrainScratch,
    now_ms: u64,
) -> DeltaEvents {
    crate::lifecycle_trace!("21_drainer_enter", 0, bindings.len() as u64, "shard");
    let mut delta = DeltaEvents::default();

    if !engine.has_any_demand() {
        gate.lock();
        crate::lifecycle_trace!("33_drainer_exit_locked", 0, 0, "shard");
        return delta;
    }

    let info = store.info();
    if info.last_seq <= *cursor {
        gate.lock();
        crate::lifecycle_trace!("33_drainer_exit_locked", 0, 0, "shard");
        return delta;
    }

    let start = *cursor + 1;
    let end = (start + cfg.max_feed as u64).min(info.last_seq + 1);
    let mut more_pending = false;
    let mut lowest_skipped: Option<u64> = None;

    scratch.rebuild_binding_index(bindings);
    scratch.dead_connections.clear();
    scratch.pending.reset();

    crate::lifecycle_trace!("25_drain_loop_start", start, end, "shard");

    // Channel-full flag — when any try_send returns Full, all remaining
    // entries to the same connection(s) will also fail. Skip them
    // immediately (~1ns) instead of doing full processing (~50ns each).
    let mut channel_full = false;
    let batch_size = cfg.batch_size;

    store
        .for_each(start, end, &mut |entry| {
            if channel_full {
                track_skipped(&mut lowest_skipped, entry.seq);
                return;
            }
            process_drain_entry(
                engine, entry, scratch, bindings, names,
                now_ms, cfg.max_age_ms, batch_size,
                &mut delta, &mut more_pending, &mut lowest_skipped,
                &mut channel_full,
            );
        })
        .ok();

    // Flush remaining partial batch after for_each ends.
    flush_pending_batch(
        engine,
        &mut scratch.body,
        &mut scratch.pending,
        bindings,
        names,
        &mut delta,
        &mut more_pending,
        &mut lowest_skipped,
        &mut channel_full,
        &mut scratch.dead_connections,
    );

    // Cursor advances to the last fully-processed entry. If entries
    // were skipped (channel Full, capacity, paused), the cursor stops
    // BEFORE the first skip so they are revisited on the next cycle.
    *cursor = lowest_skipped.map_or(end - 1, |ls| ls.saturating_sub(1));

    // More entries remain beyond what this cycle covered.
    if end <= info.last_seq || lowest_skipped.is_some() {
        more_pending = true;
    }

    // Rewind cursor for ack-based consumers: on ack, handle_ack
    // applies this to revisit entries that were skipped due to
    // capacity/subject limits that are now freed.
    if let Some(ls) = lowest_skipped {
        *rewind_cursor = Some(rewind_cursor.map_or(ls, |prev: u64| prev.min(ls)));
    }

    for conn_id in scratch.dead_connections.drain(..) {
        delta.merge(engine.mark_connection_dead(conn_id));
    }

    if more_pending {
        gate.release();
        crate::lifecycle_trace!("33_drainer_exit_released", 0, 0, "shard");
    } else {
        gate.lock();
        crate::lifecycle_trace!("33_drainer_exit_locked", 0, 0, "shard");
    }

    delta
}

// ── Per-entry processing ────────────────────────────────────────────────────

/// Validate and dispatch a single store entry. Called from the `for_each`
/// closure — inline checks (TTL, tombstone, demand, subject credit),
/// match table lookup, then recipient dispatch.
fn process_drain_entry(
    engine: &mut ArbitroEngine,
    entry: &arbitro_store::Entry<'_>,
    scratch: &mut DrainScratch,
    bindings: &[ActiveBinding],
    names: &Arc<crate::common::NameRegistry>,
    now_ms: u64,
    max_age_ms: u64,
    batch_size: u16,
    delta: &mut DeltaEvents,
    more_pending: &mut bool,
    lowest_skipped: &mut Option<u64>,
    channel_full: &mut bool,
) {
    let stream_id = StreamId(entry.stream_id);

    // TTL expiration (cheapest discard — no flag read).
    if max_age_ms > 0 && entry.timestamp > 0 && entry.timestamp + max_age_ms <= now_ms {
        delta.merge(engine.execute(&Command::Tombstone {
            stream_id,
            seq: entry.seq,
            reason: DropReason::Expired,
        }));
        return;
    }

    if entry.flags & arbitro_store::flags::TOMBSTONE != 0 {
        delta.merge(engine.execute(&Command::Tombstone {
            stream_id,
            seq: entry.seq,
            reason: DropReason::Tombstoned,
        }));
        return;
    }

    if !engine.has_demand(stream_id) {
        return;
    }

    let subject_hash = fnv1a_32(entry.subject);

    // Resolve patterns for new subjects (mutable access).
    if let Some(mt) = engine.ctx_mut().catalog.match_table_mut(stream_id) {
        mt.resolve_patterns(subject_hash, entry.subject);
    }

    if !engine.subject_has_room(stream_id, entry.subject, subject_hash) {
        *more_pending = true;
        track_skipped(lowest_skipped, entry.seq);
        return;
    }

    // Collect match entries into scratch (releases engine borrow).
    scratch.matches.clear();
    if let Some(mt) = engine.match_table(stream_id) {
        scratch.matches.extend(mt.lookup(subject_hash).iter());
    }

    if scratch.matches.is_empty() {
        delta.merge(engine.execute(&Command::Tombstone {
            stream_id,
            seq: entry.seq,
            reason: DropReason::NoSubscribers,
        }));
        return;
    }

    crate::lifecycle_trace!(
        "27_store_get_loop_start",
        0,
        scratch.matches.len() as u64,
        "shard"
    );

    dispatch_recipients(
        engine, entry, stream_id, subject_hash, scratch, bindings, names,
        batch_size, delta, more_pending, lowest_skipped, channel_full,
    );
}

// ── Per-recipient dispatch ──────────────────────────────────────────────────

/// Dispatch one entry to all matching recipients — queue dedup, capacity
/// checks, accumulate into batched RepBatch frame, flush when batch is
/// full or binding changes.
fn dispatch_recipients(
    engine: &mut ArbitroEngine,
    entry: &arbitro_store::Entry<'_>,
    stream_id: StreamId,
    subject_hash: u32,
    scratch: &mut DrainScratch,
    bindings: &[ActiveBinding],
    names: &Arc<crate::common::NameRegistry>,
    batch_size: u16,
    delta: &mut DeltaEvents,
    more_pending: &mut bool,
    lowest_skipped: &mut Option<u64>,
    channel_full: &mut bool,
) {
    // Destructure to allow independent field borrows inside the loop.
    let DrainScratch {
        body,
        matches,
        served_queues,
        dead_connections,
        binding_index,
        pending,
    } = scratch;

    served_queues.clear();

    for i in 0..matches.len() {
        let me = matches[i]; // Copy — releases borrow on matches.
        let consumer_id = me.consumer_id;
        let connection_id = me.connection_id;
        let queue_id = me.queue_id;

        if connection_id == ConnectionId(0) {
            continue;
        }

        // Queue dedup: O(k) where k = queue groups, typically 1–3.
        if served_queues.contains(&queue_id) {
            continue;
        }

        // O(k) where k = dead connections this cycle, typically 0.
        if dead_connections.contains(&connection_id) {
            continue;
        }

        // O(1) binding lookup via pre-built index.
        let binding_idx = match binding_index.get(&(consumer_id.0, connection_id.0)) {
            Some(&idx) => idx,
            None => continue,
        };
        let binding = &bindings[binding_idx];

        // Cached paused flag — avoids HashMap lookup per entry (~10ns→~1ns).
        if binding.paused {
            *more_pending = true;
            track_skipped(lowest_skipped, entry.seq);
            continue;
        }

        // Fire-and-forget skips capacity check — inflight is never
        // incremented (Fix 1), so the check always passes.
        if !binding.fire_and_forget
            && !engine.consumer_has_capacity(consumer_id, binding.max_inflight)
        {
            *more_pending = true;
            track_skipped(lowest_skipped, entry.seq);
            continue;
        }

        // ── Batch accumulation ─────────────────────────────────────────

        // Flush if current batch is for a different binding.
        if let Some(prev_idx) = pending.binding_idx {
            if prev_idx != binding_idx {
                flush_pending_batch(
                    engine, body, pending, bindings, names,
                    delta, more_pending, lowest_skipped,
                    channel_full, dead_connections,
                );
                if *channel_full {
                    track_skipped(lowest_skipped, entry.seq);
                    return;
                }
            }
        }

        // Start new batch if none in progress.
        if pending.binding_idx.is_none() {
            body.clear();
            // Envelope placeholder — patched on flush.
            body.extend_from_slice(&[0u8; ENVELOPE_SIZE]);
            // RepBatchFixed header — count patched on flush.
            body.extend_from_slice(
                RepBatchFixed {
                    consumer_id: U32::new(consumer_id.0),
                    count: U16::new(0), // patched on flush
                    _pad: U16::new(0),
                }
                .as_bytes(),
            );
            pending.binding_idx = Some(binding_idx);
            pending.count = 0;
            pending.first_seq = entry.seq;
            pending.stream_id = stream_id;
            pending.delivered.clear();
        }

        // Append entry to the batch body.
        let subj_len = entry.subject.len();
        let data_len = subj_len + entry.payload.len();
        body.extend_from_slice(
            DeliveryEntryHeader {
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
        pending.delivered.push(DeliveredEntry {
            seq: entry.seq,
            subject_hash,
            _pad: 0,
        });

        served_queues.push(queue_id);

        // Flush if batch is full. For tracked consumers (non-fire-and-forget),
        // flush after every entry so the engine sees the delivery immediately
        // and enforces max_inflight/capacity correctly. Only fire-and-forget
        // benefits from batching (no inflight tracking).
        let effective_limit = if binding.fire_and_forget { batch_size } else { 1 };
        if pending.count >= effective_limit {
            flush_pending_batch(
                engine, body, pending, bindings, names,
                delta, more_pending, lowest_skipped,
                channel_full, dead_connections,
            );
            if *channel_full {
                return;
            }
        }
    }
}

// ── Batch flush ─────────────────────────────────────────────────────────────

/// Patch headers, freeze, try_send, report to engine. Resets pending state.
fn flush_pending_batch(
    engine: &mut ArbitroEngine,
    body: &mut BytesMut,
    pending: &mut PendingBatch,
    bindings: &[ActiveBinding],
    names: &Arc<crate::common::NameRegistry>,
    delta: &mut DeltaEvents,
    more_pending: &mut bool,
    lowest_skipped: &mut Option<u64>,
    channel_full: &mut bool,
    dead_connections: &mut Vec<ConnectionId>,
) {
    let binding_idx = match pending.binding_idx {
        Some(idx) => idx,
        None => return,
    };
    if pending.count == 0 {
        pending.reset();
        return;
    }

    let binding = &bindings[binding_idx];
    let stream_id = pending.stream_id;

    // Patch RepBatchFixed count (at offset ENVELOPE_SIZE + 4).
    let count_offset = ENVELOPE_SIZE + 4;
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
        binding.connection_id.0,
        pending.count as u64,
        "shard"
    );

    let frozen = body.split().freeze();
    match binding.tx.try_send(frozen) {
        Ok(()) => {
            crate::lifecycle_trace!(
                "30_send_bytes_done",
                binding.connection_id.0,
                pending.count as u64,
                "shard"
            );
            delta.merge(engine.execute(&Command::Delivered {
                stream_id,
                binding_id: binding.binding_id,
                entries: &pending.delivered,
            }));
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            *more_pending = true;
            *channel_full = true;
            track_skipped(lowest_skipped, pending.first_seq);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            dead_connections.push(binding.connection_id);
        }
    }

    pending.reset();
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Track lowest skipped seq for cursor rewind.
#[inline]
fn track_skipped(lowest: &mut Option<u64>, seq: u64) {
    *lowest = Some(lowest.map_or(seq, |s| s.min(seq)));
}
