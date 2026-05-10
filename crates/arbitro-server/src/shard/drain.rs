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

use arbitro_engine_v2::catalog::match_table::MatchEntry;
use arbitro_engine_v2::catalog::wire_hash_32;
use arbitro_engine_v2::command::DeliveredEntry;
use arbitro_engine_v2::types::*;
use arbitro_store::Store;

use crate::common::Gate;
use crate::shard::accumulator::Accumulator;
use crate::shard::shared::{find_writer, DrainNotification, DrainSnapshot, NotifyRing, SharedCounters};
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
    /// Sparse composite key (stream_id, subject_hash) → foldhash (rule: sparse IDs).
    resolve_cache: HashMap<(u32, u32), Vec<MatchEntry>, foldhash::fast::FixedState>,
    /// Local subject limit cache. (stream_id, subject_hash) → Option<max>.
    /// Sparse composite key → foldhash (rule: sparse IDs).
    subject_limit_cache: HashMap<(u32, u32), Option<u32>, foldhash::fast::FixedState>,

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
    /// Vec scan (~0.7-3 ns per op) beats HashMap+foldhash (~1.4 ns) thanks
    /// to cache locality. Measured in `benches/local_delta.rs`.
    local_inflight: Vec<(u32, u32)>,
    /// Per-cycle subject deltas: `(consumer_id, subject_hash) -> pending`.
    /// Keyed per-consumer because subject inflight counters are
    /// per-consumer (see `SharedCounters.subject`). Otherwise two
    /// consumers on the same stream publishing the same subject would
    /// collide on the local delta and under-count.
    local_subject: HashMap<(u32, u32), u32, foldhash::fast::FixedState>,
}

impl DrainScratch {
    pub(in crate::shard) fn new() -> Self {
        Self {
            matches: Vec::with_capacity(16),
            served_queues: Vec::with_capacity(8),
            dead_connections: Vec::with_capacity(4),
            resolve_cache: HashMap::with_capacity_and_hasher(
                64,
                foldhash::fast::FixedState::default(),
            ),
            subject_limit_cache: HashMap::with_capacity_and_hasher(
                64,
                foldhash::fast::FixedState::default(),
            ),
            acc: Accumulator::new(),
            deliveries: Vec::with_capacity(256),
            local_inflight: Vec::with_capacity(8),
            local_subject: HashMap::with_capacity_and_hasher(
                128,
                foldhash::fast::FixedState::default(),
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

/// Result of Phase 1 (store read). Passed from `drain_read` to `drain_deliver`
/// so the store lock can be released in between.
pub(in crate::shard) struct DrainReadResult {
    pub start: u64,
    pub end: u64,
    pub more_pending: bool,
    pub lowest_skipped: Option<u64>,
    pub last_seq: u64,
}

/// Phase 1 — read entries from the store into scratch buffers.
///
/// Holds the store reference only for the `for_each` walk. After this
/// returns, the store is no longer needed and its lock can be released.
/// Returns `None` if there is no work (caller should `gate.lock()`).
pub(in crate::shard) fn drain_read(
    counters: &SharedCounters,
    snap: &DrainSnapshot,
    store: &dyn Store,
    cfg: &DrainConfig,
    scratch: &mut DrainScratch,
    now_ms: u64,
) -> Option<DrainReadResult> {
    crate::lifecycle_trace!("21_drainer_enter", 0, snap.bindings.len() as u64, "shard");

    if !counters.has_any_demand() {
        return None;
    }

    let info = store.info();
    let cursor = counters.cursor();
    if info.last_seq <= cursor {
        return None;
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

    // Walk the store, accumulate into per-connection buckets.
    store
        .for_each(start, end, &mut |entry| {
            // Per-stream max_age_ms takes precedence over the global default;
            // falls back to global cfg if the stream has no specific limit.
            let stream_max_age = snap.stream_max_age_ms
                .get(entry.stream_id as usize)
                .copied()
                .filter(|&v| v > 0)
                .unwrap_or(cfg.max_age_ms);
            process_drain_entry(
                counters,
                snap,
                entry,
                scratch,
                now_ms,
                stream_max_age,
                &mut more_pending,
                &mut lowest_skipped,
            );
        })
        .ok();

    Some(DrainReadResult {
        start,
        end,
        more_pending,
        lowest_skipped,
        last_seq: info.last_seq,
    })
}

/// Phase 2+3 — flush accumulated frames to TCP + bookkeeping.
///
/// Does NOT need the store. The store lock should be released before
/// calling this. All entry data lives in `scratch.acc` (copied during
/// Phase 1's `for_each`).
pub(in crate::shard) fn drain_deliver(
    counters: &SharedCounters,
    snap: &DrainSnapshot,
    gate: &Gate,
    names: &Arc<crate::common::NameRegistry>,
    scratch: &mut DrainScratch,
    notify_tx: &NotifyRing,
    mut result: DrainReadResult,
) {
    // Phase 2 — flush every accumulator bucket as one RepBatch frame.
    // Results are captured into a small Vec so Phase 3 can do ack
    // bookkeeping without borrowing scratch inside the for_each closure.
    //
    // FlushOutcome distinguishes between three cases:
    //  - Ok:             frame sent successfully
    //  - Backpressured:  channel full (transient), retry next cycle
    //  - WriterGone:     writer not found (permanent), mark dead
    #[derive(Clone, Copy)]
    enum FlushOutcome { Ok, Backpressured(u64), WriterGone }

    let mut flush_results: Vec<(ConnectionId, FlushOutcome)> = Vec::with_capacity(8);
    {
        let writers_by_conn = &snap.writers_by_conn;
        scratch.acc.for_each(names, |frame| {
            // O(log N) binary search — rule (performance.md dense/sparse):
            // ConnectionId is unbounded-dense, sorted Vec + binary search
            // is the canonical structure for this workload.
            let Some(writer) = find_writer(writers_by_conn, frame.connection_id.0) else {
                // Writer not found → connection truly gone (removed from
                // registry). Mark dead so the engine retires bindings.
                flush_results.push((frame.connection_id, FlushOutcome::WriterGone));
                return false;
            };
            crate::lifecycle_trace!(
                "29_frame_built",
                frame.connection_id.0,
                frame.count as u64,
                "shard"
            );

            let ok = writer.write_tx.try_send(frame.bytes.clone()).is_ok();

            if ok {
                crate::lifecycle_trace!(
                    "30_send_bytes_done",
                    frame.connection_id.0,
                    frame.count as u64,
                    "shard"
                );
                flush_results.push((frame.connection_id, FlushOutcome::Ok));
            } else {
                // Channel full → backpressure, NOT dead. Record first_seq
                // so the cursor doesn't advance past undelivered entries.
                flush_results.push((
                    frame.connection_id,
                    FlushOutcome::Backpressured(frame.first_seq),
                ));
            }
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
    let mut flush_ok: std::collections::HashMap<ConnectionId, bool, foldhash::fast::FixedState> =
        std::collections::HashMap::with_capacity_and_hasher(
            flush_results.len(),
            foldhash::fast::FixedState::default(),
        );

    for &(conn, outcome) in &flush_results {
        match outcome {
            FlushOutcome::Ok => {
                flush_ok.insert(conn, true);
            }
            FlushOutcome::Backpressured(first_seq) => {
                flush_ok.insert(conn, false);
                // Treat as skipped — cursor won't advance past these entries.
                track_skipped(&mut result.lowest_skipped, first_seq);
                result.more_pending = true;
            }
            FlushOutcome::WriterGone => {
                flush_ok.insert(conn, false);
                scratch.dead_connections.push(conn);
            }
        }
    }
    for d in &scratch.deliveries {
        if flush_ok.get(&d.conn).copied().unwrap_or(false) {
            counters.inc_inflight(d.consumer_id, d.queue_id);
            counters.inc_subject(d.consumer_id, d.subject_hash);
        }
    }

    // Group successful deliveries by binding_id and notify the command
    // thread once per binding. Matches the old `notify_delivered_grouped`
    // semantics so the engine's Command::Delivered handler sees the same
    // shape it did before.
    if !scratch.deliveries.is_empty() {
        notify_delivered_grouped(notify_tx, &snap.bindings, &scratch.deliveries, &flush_ok);
    }

    // Cursor advances to last fully-processed entry.
    let new_cursor = result.lowest_skipped.map_or(result.end - 1, |ls| ls.saturating_sub(1));
    counters.set_cursor(new_cursor);

    if result.end <= result.last_seq || result.lowest_skipped.is_some() {
        result.more_pending = true;
    }

    // Notify command thread of truly dead connections (writer gone from
    // registry — permanent). Backpressured channels are NOT reported here;
    // they're transient and retried on the next cycle.
    for conn_id in scratch.dead_connections.drain(..) {
        let _ = notify_tx.try_send(DrainNotification::ConnectionDead(conn_id));
    }

    if result.more_pending {
        gate.release();
        crate::lifecycle_trace!("33_drainer_exit_released", 0, 0, "shard");
    } else {
        gate.lock();
        crate::lifecycle_trace!("33_drainer_exit_locked", 0, 0, "shard");
    }
}

/// Legacy single-call API — kept for callers that don't need the split.
/// Holds `store` for Phase 1 only; Phases 2+3 run after the borrow ends.
pub(in crate::shard) fn drain_cycle(
    counters: &SharedCounters,
    snap: &DrainSnapshot,
    store: &dyn Store,
    gate: &Gate,
    names: &Arc<crate::common::NameRegistry>,
    cfg: &DrainConfig,
    scratch: &mut DrainScratch,
    notify_tx: &NotifyRing,
    now_ms: u64,
) {
    match drain_read(counters, snap, store, cfg, scratch, now_ms) {
        Some(result) => drain_deliver(counters, snap, gate, names, scratch, notify_tx, result),
        None => {
            gate.lock();
            crate::lifecycle_trace!("33_drainer_exit_locked", 0, 0, "shard");
        }
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

    // TTL expiration — cheapest check, runs first.
    if max_age_ms > 0 && entry.timestamp > 0 && entry.timestamp + max_age_ms <= now_ms {
        return;
    }

    if entry.flags & arbitro_store::flags::TOMBSTONE != 0 {
        return;
    }

    // Demand check — atomic read.
    let stream_raw = stream_id.raw();
    if !counters.has_demand(stream_raw) {
        return;
    }

    let subject_hash = wire_hash_32(entry.subject);

    // Single match_table lookup — reused across all three steps below.
    // Early return when no match table: all three steps would skip and
    // scratch.matches would end up empty anyway.
    let Some(mt) = snap
        .match_tables
        .get(stream_raw as usize)
        .and_then(|o| o.as_ref())
    else {
        return;
    };
    let cache_key = (stream_raw, subject_hash);
    let lookup = mt.lookup(subject_hash);

    // Step 1: pre-resolve patterns into local cache when lookup is empty.
    if lookup.is_empty() && !scratch.resolve_cache.contains_key(&cache_key) {
        let mut resolved = Vec::new();
        mt.resolve_patterns_readonly(subject_hash, entry.subject, &mut resolved);
        scratch.resolve_cache.insert(cache_key, resolved);
    }

    // Step 2: resolve + cache subject_limit (stream-wide value — same for
    // every consumer matching this subject). The counter check using this
    // limit happens per-match in dispatch_recipients because the atomic
    // counter is keyed by (consumer_id, subject_hash) for per-consumer
    // isolation.
    let subject_limit = if mt.has_subject_limits() {
        *scratch
            .subject_limit_cache
            .entry(cache_key)
            .or_insert_with(|| mt.resolve_subject_limit_readonly(subject_hash, entry.subject))
    } else {
        None
    };

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
        subject_limit,
        scratch,
        &snap.bindings,
        more_pending,
        lowest_skipped,
    );
}

// ── Per-recipient dispatch ──────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn dispatch_recipients(
    counters: &SharedCounters,
    entry: &arbitro_store::Entry<'_>,
    stream_id: StreamId,
    subject_hash: u32,
    subject_limit: Option<u32>,
    scratch: &mut DrainScratch,
    bindings: &[ActiveBinding],
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

        // Fase C.2: binding_idx is stamped directly in MatchEntry during
        // snapshot rebuild — zero HashMap lookup on hot path. Skip
        // unbound entries (pull-model subscriptions without an active
        // connection binding yet).
        let binding_idx = me.binding_idx as usize;
        if me.binding_idx == arbitro_engine_v2::catalog::match_table::BINDING_IDX_UNBOUND
            || binding_idx >= bindings.len()
        {
            continue;
        }
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
                || !counters.consumer_has_capacity(consumer_id.0, binding.max_inflight - pending)
            {
                *more_pending = true;
                track_skipped(lowest_skipped, entry.seq);
                continue;
            }

            // Per-consumer subject inflight check — counter is keyed by
            // (consumer_id, subject_hash). Two consumers on the same
            // subject have independent budgets.
            if let Some(max) = subject_limit {
                let pending_subj = scratch
                    .local_subject
                    .get(&(consumer_id.0, subject_hash))
                    .copied()
                    .unwrap_or(0);
                if pending_subj >= max
                    || !counters.subject_has_room(consumer_id.0, subject_hash, max - pending_subj)
                {
                    *more_pending = true;
                    track_skipped(lowest_skipped, entry.seq);
                    continue;
                }
            }
        }

        // ── Hand off to the accumulator ───────────────────────────────────
        //
        // The accumulator is pure wire grouping: (conn, stream) → one
        // `RepBatch` frame. It does not know or care about ack state —
        // that lives in `scratch.deliveries` below, gated on
        // `!fire_and_forget`.

        // Extract reply_to from payload when HAS_REPLY_TO flag is set.
        // Store format: [reply_len:u16 LE][reply_to bytes][actual payload].
        let (reply_to, actual_payload): (&[u8], &[u8]) =
            if entry.flags & arbitro_store::flags::HAS_REPLY_TO != 0 && entry.payload.len() >= 2 {
                let reply_len =
                    u16::from_le_bytes([entry.payload[0], entry.payload[1]]) as usize;
                if entry.payload.len() >= 2 + reply_len {
                    (
                        &entry.payload[2..2 + reply_len],
                        &entry.payload[2 + reply_len..],
                    )
                } else {
                    (&[], entry.payload)
                }
            } else {
                (&[], entry.payload)
            };

        let fire_and_forget = binding.fire_and_forget;
        scratch.acc.add(
            connection_id,
            stream_id,
            consumer_id,
            entry.seq,
            entry.subject,
            subject_hash,
            reply_to,
            actual_payload,
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
            *scratch
                .local_subject
                .entry((consumer_id.0, subject_hash))
                .or_insert(0) += 1;
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
    notify_tx: &NotifyRing,
    bindings: &[ActiveBinding],
    deliveries: &[PendingNotify],
    flush_ok: &std::collections::HashMap<ConnectionId, bool, foldhash::fast::FixedState>,
) {
    let frame_ok = |conn: ConnectionId| -> bool { flush_ok.get(&conn).copied().unwrap_or(false) };

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
