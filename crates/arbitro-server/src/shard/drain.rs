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
use arbitro_store::Store;
use tokio::sync::mpsc;

use crate::common::Gate;
use crate::shard::accumulator::Accumulator;
use crate::shard::shared::{
    find_binding_idx, find_writer, DrainNotification, DrainSnapshot, SharedCounters,
};
use crate::shard::worker::ActiveBinding;

// ── Configuration ───────────────────────────────────────────────────────────

pub(in crate::shard) struct DrainConfig {
    pub max_feed: usize,
    pub max_age_ms: u64,
    pub batch_size: u16,
}

// ── Per-cycle ack-mode delivery record ──────────────────────────────────────

/// Per-entry metadata captured for ack-mode deliveries. Lives in
/// `DrainScratch.deliveries` alongside the wire bytes held by the
/// `Accumulator`. After a frame flushes successfully, the matching
/// records bump the `SharedCounters` atomics and feed
/// `DrainNotification::Delivered` to the command thread, which owns
/// `Binding.pending` and `InFlightCounters`. Fire-and-forget never
/// pushes here — no ack will ever arrive.
#[derive(Clone, Copy)]
struct PendingNotify {
    conn: ConnectionId,
    stream: StreamId,
    binding_idx: usize,
    seq: u64,
    subject_hash: u32,
    consumer_id: u32,
    queue_id: u32,
}

// ── Scratch buffers ─────────────────────────────────────────────────────────

pub(in crate::shard) struct DrainScratch {
    matches: Vec<MatchEntry>,
    served_queues: Vec<QueueId>,
    dead_connections: Vec<ConnectionId>,
    /// Local pattern resolution cache. Avoids mutating shared match table.
    /// Sparse composite key (stream_id, subject_hash) → ahash (rule: sparse IDs).
    resolve_cache: HashMap<(u32, u32), Vec<MatchEntry>, ahash::RandomState>,
    /// Local subject limit cache. (stream_id, subject_hash) → Option<max>.
    /// Sparse composite key → ahash (rule: sparse IDs).
    subject_limit_cache: HashMap<(u32, u32), Option<u32>, ahash::RandomState>,

    /// Wire-level frame accumulator. One bucket per (conn, stream)
    /// active this cycle; each bucket emits one `RepBatch` frame at
    /// flush time. The drain owns zero frame-building bytes now —
    /// those live inside the accumulator.
    acc: Accumulator,

    /// Parallel ack-mode tracking. Populated only when a delivery is
    /// NOT fire-and-forget. Indexed into at flush time to bump atomics
    /// and generate per-binding notifications.
    deliveries: Vec<PendingNotify>,

    /// Per-cycle inflight deltas: `(consumer_id, pending)`.
    /// Vec + linear scan — N is typically 1-4 consumers per cycle, where
    /// Vec scan (~0.7-3 ns per op) beats HashMap+ahash (~1.4 ns) thanks
    /// to cache locality. Measured in `benches/local_delta.rs`.
    local_inflight: Vec<(u32, u32)>,
    /// Per-cycle subject deltas: `subject_hash -> pending`.
    /// HashMap+ahash — N can reach hundreds of unique subjects per cycle
    /// (one per distinct message subject). Vec scan cost grows linearly
    /// and crosses HashMap+ahash at N≈3-4. Measured in
    /// `benches/local_delta.rs`: at N=128, Vec = 18.7 ns/op vs
    /// HashMap+ahash = 1.4 ns/op (13x faster).
    local_subject: HashMap<u32, u32, ahash::RandomState>,
}

impl DrainScratch {
    pub(in crate::shard) fn new() -> Self {
        Self {
            matches: Vec::with_capacity(16),
            served_queues: Vec::with_capacity(8),
            dead_connections: Vec::with_capacity(4),
            resolve_cache: HashMap::with_capacity_and_hasher(
                64, ahash::RandomState::new(),
            ),
            subject_limit_cache: HashMap::with_capacity_and_hasher(
                64, ahash::RandomState::new(),
            ),
            acc: Accumulator::new(),
            deliveries: Vec::with_capacity(256),
            local_inflight: Vec::with_capacity(8),
            local_subject: HashMap::with_capacity_and_hasher(
                128, ahash::RandomState::new(),
            ),
        }
    }
}

// ── Linear-scan helpers for per-cycle deltas ────────────────────────────────

#[inline]
fn local_delta_get(list: &[(u32, u32)], key: u32) -> u32 {
    for &(k, v) in list.iter() {
        if k == key {
            return v;
        }
    }
    0
}

#[inline]
fn local_delta_inc(list: &mut Vec<(u32, u32)>, key: u32) {
    for e in list.iter_mut() {
        if e.0 == key {
            e.1 += 1;
            return;
        }
    }
    list.push((key, 1));
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
    scratch.local_inflight.clear();
    scratch.local_subject.clear();
    scratch.deliveries.clear();
    scratch.acc.clear();
    // Pattern and subject-limit caches must be flushed every cycle —
    // they hold entries resolved against the match_table snapshot, and
    // the snapshot may have changed since the last cycle (subscribe /
    // unsubscribe rebuilds it). Keeping stale entries silently drops
    // late-binding fanout subscribers during replay.
    scratch.resolve_cache.clear();
    scratch.subject_limit_cache.clear();

    crate::lifecycle_trace!("25_drain_loop_start", start, end, "shard");

    // Phase 1 — walk the store, accumulate into per-connection buckets.
    store
        .for_each(start, end, &mut |entry| {
            process_drain_entry(
                counters,
                snap,
                entry,
                scratch,
                now_ms,
                cfg.max_age_ms,
                &mut more_pending,
                &mut lowest_skipped,
            );
        })
        .ok();

    // Phase 2 — flush every accumulator bucket as one RepBatch frame.
    // Results are captured into a small Vec so Phase 3 can do ack
    // bookkeeping without borrowing scratch inside the for_each closure.
    let mut flush_results: Vec<(ConnectionId, bool)> = Vec::with_capacity(8);
    {
        let writers_by_conn = &snap.writers_by_conn;
        scratch.acc.for_each(names, |frame| {
            // O(log N) binary search — rule (performance.md dense/sparse):
            // ConnectionId is unbounded-dense, sorted Vec + binary search
            // is the canonical structure for this workload.
            let Some(writer) = find_writer(writers_by_conn, frame.connection_id.0) else {
                return false;
            };
            crate::lifecycle_trace!(
                "29_frame_built",
                frame.connection_id.0,
                frame.count as u64,
                "shard"
            );
            if std::env::var("ARBITRO_WIRE_TRACE").is_ok() {
                eprintln!(
                    "[wire] conn={} entries={} bytes={}",
                    frame.connection_id.0, frame.count, frame.bytes.len()
                );
            }
            let ok = crate::transport::registry::write_all_blocking(
                &writer.writer,
                &frame.bytes,
                &writer.runtime,
            );
            if ok {
                crate::lifecycle_trace!(
                    "30_send_bytes_done",
                    frame.connection_id.0,
                    frame.count as u64,
                    "shard"
                );
            }
            flush_results.push((frame.connection_id, ok));
            ok
        });
    }

    // Phase 3 — post-flush bookkeeping (atomics + command-thread
    // notifications). Fire-and-forget entries never hit scratch.deliveries,
    // so this loop is a no-op in the pub/sub default path.
    //
    // Build a (conn -> ok) map once; drain uses it to skip deliveries for
    // failed frames without a nested per-conn scan. Turns the previous
    // O(F x D) filter into a single O(D) pass (F = frames, D = deliveries).
    let mut flush_ok: std::collections::HashMap<ConnectionId, bool, ahash::RandomState> =
        std::collections::HashMap::with_capacity_and_hasher(
            flush_results.len(),
            ahash::RandomState::new(),
        );
    for &(conn, ok) in &flush_results {
        flush_ok.insert(conn, ok);
        if !ok {
            scratch.dead_connections.push(conn);
        }
    }
    for d in &scratch.deliveries {
        if flush_ok.get(&d.conn).copied().unwrap_or(false) {
            counters.inc_inflight(d.consumer_id, d.queue_id);
            counters.inc_subject(d.subject_hash);
        }
    }

    // Group successful deliveries by binding_id and notify the command
    // thread once per binding. Matches the old `notify_delivered_grouped`
    // semantics so the engine's Command::Delivered handler sees the same
    // shape it did before.
    if !scratch.deliveries.is_empty() {
        notify_delivered_grouped(
            notify_tx,
            &snap.bindings,
            &scratch.deliveries,
            &flush_ok,
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
    let stream_raw = stream_id.raw() as usize;

    // Single match_table lookup — reused across all three steps below.
    // Early return when no match table: all three steps would skip and
    // scratch.matches would end up empty anyway.
    let Some(mt) = snap.match_tables.get(stream_raw).and_then(|o| o.as_ref()) else {
        return;
    };
    let cache_key = (stream_id.raw(), subject_hash);
    let lookup = mt.lookup(subject_hash);

    // Step 1: pre-resolve patterns into local cache when lookup is empty.
    if lookup.is_empty() && !scratch.resolve_cache.contains_key(&cache_key) {
        let mut resolved = Vec::new();
        mt.resolve_patterns_readonly(subject_hash, entry.subject, &mut resolved);
        scratch.resolve_cache.insert(cache_key, resolved);
    }

    // Step 2: subject inflight gate — pending + atomic counter.
    if mt.has_subject_limits() {
        let limit = scratch
            .subject_limit_cache
            .entry(cache_key)
            .or_insert_with(|| {
                mt.resolve_subject_limit_readonly(subject_hash, entry.subject)
            });
        if let Some(max) = *limit {
            let pending = scratch
                .local_subject
                .get(&subject_hash)
                .copied()
                .unwrap_or(0);
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

    // Step 3: collect matches — reuse `lookup` computed above.
    scratch.matches.clear();
    scratch.matches.extend(lookup.iter());
    if let Some(resolved) = scratch.resolve_cache.get(&cache_key) {
        scratch.matches.extend(resolved.iter());
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
    bindings: &[ActiveBinding],
    binding_index: &std::collections::HashMap<(u32, u64), u32, ahash::RandomState>,
    more_pending: &mut bool,
    lowest_skipped: &mut Option<u64>,
) {
    scratch.served_queues.clear();

    // Queue fairness — rotate the iteration start offset by `entry.seq` so
    // the same binding isn't always picked first. Combined with the existing
    // `served_queues` dedup and capacity-skip fallback, this gives strict
    // round-robin for healthy workers and automatic failover when a worker
    // is saturated. Zero extra state.
    //
    // Cost: ~1 modulo per entry (~5 ns on x86 DIV). Use sub-based wrap in
    // the inner loop to avoid a second modulo per iteration.
    let n = scratch.matches.len();
    if n == 0 {
        return;
    }
    let start = (entry.seq as usize) % n;

    for i in 0..n {
        let raw = start + i;
        let idx = if raw >= n { raw - n } else { raw };
        let me = scratch.matches[idx];
        let consumer_id = me.consumer_id;
        let connection_id = me.connection_id;
        let queue_id = me.queue_id;

        if connection_id == ConnectionId(0) {
            continue;
        }

        // Queue dedup: one entry per queue within the match set of this entry.
        if queue_id != QueueId(0) && scratch.served_queues.contains(&queue_id) {
            continue;
        }

        if scratch.dead_connections.contains(&connection_id) {
            continue;
        }

        let binding_idx = match find_binding_idx(binding_index, consumer_id.0, connection_id.0)
        {
            Some(idx) => idx,
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
            let pending = local_delta_get(&scratch.local_inflight, consumer_id.0);
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

        // ── Hand off to the accumulator ───────────────────────────────────
        //
        // The accumulator is pure wire grouping: (conn, stream) → one
        // `RepBatch` frame. It does not know or care about ack state —
        // that lives in `scratch.deliveries` below, gated on
        // `!fire_and_forget`.

        let fire_and_forget = binding.fire_and_forget;
        scratch.acc.add(
            connection_id,
            stream_id,
            consumer_id,
            entry.seq,
            entry.subject,
            subject_hash,
            entry.payload,
        );

        if !fire_and_forget {
            scratch.deliveries.push(PendingNotify {
                conn: connection_id,
                stream: stream_id,
                binding_idx,
                seq: entry.seq,
                subject_hash,
                consumer_id: consumer_id.0,
                queue_id: queue_id.0,
            });
            local_delta_inc(&mut scratch.local_inflight, consumer_id.0);
            *scratch.local_subject.entry(subject_hash).or_insert(0) += 1;
        }

        if queue_id != QueueId(0) {
            scratch.served_queues.push(queue_id);
        }
    }
}

// ── Ack-mode notifications ──────────────────────────────────────────────────

/// After the accumulator flushed this cycle's frames, walk the
/// per-entry `deliveries` list, keep only the ones whose (conn, stream)
/// frame succeeded, group them by `binding_idx`, and emit one
/// `DrainNotification::Delivered` per binding. The command thread then
/// turns each of those into a `Command::Delivered` which updates
/// `Binding.pending` and `InFlightCounters` — the single source of
/// truth for ack-matching.
fn notify_delivered_grouped(
    notify_tx: &mpsc::Sender<DrainNotification>,
    bindings: &[ActiveBinding],
    deliveries: &[PendingNotify],
    flush_ok: &std::collections::HashMap<ConnectionId, bool, ahash::RandomState>,
) {
    let frame_ok = |conn: ConnectionId| -> bool {
        flush_ok.get(&conn).copied().unwrap_or(false)
    };

    // Fast path — every delivery belongs to the same binding AND all
    // frames succeeded. Pub/sub of a single consumer hits this path.
    if let Some(first) = deliveries.first() {
        let first_idx = first.binding_idx;
        if deliveries.iter().all(|d| d.binding_idx == first_idx)
            && deliveries.iter().all(|d| frame_ok(d.conn))
        {
            let binding = &bindings[first_idx];
            let entries: Vec<DeliveredEntry> = deliveries
                .iter()
                .map(|d| DeliveredEntry {
                    seq: d.seq,
                    subject_hash: d.subject_hash,
                    _pad: 0,
                })
                .collect();
            let _ = notify_tx.try_send(DrainNotification::Delivered {
                binding_id: binding.binding_id,
                consumer_id: binding.consumer_id,
                queue_id: binding.queue_id,
                entries,
            });
            return;
        }
    }

    // Slow path — mixed bindings and/or partial frame success. Sort by
    // binding_idx, scan groups, drop entries whose frame failed.
    let mut sorted: Vec<PendingNotify> = deliveries
        .iter()
        .copied()
        .filter(|d| frame_ok(d.conn))
        .collect();
    sorted.sort_unstable_by_key(|d| d.binding_idx);

    let mut i = 0;
    while i < sorted.len() {
        let idx = sorted[i].binding_idx;
        let mut entries = Vec::new();
        while i < sorted.len() && sorted[i].binding_idx == idx {
            entries.push(DeliveredEntry {
                seq: sorted[i].seq,
                subject_hash: sorted[i].subject_hash,
                _pad: 0,
            });
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
