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
use arbitro_engine_v2::command::DeliveredEntry;
use arbitro_engine_v2::common::wire_hash_32;
use arbitro_engine_v2::types::*;
use arbitro_store::Store;

use crate::common::Gate;
use crate::shard::accumulator::Accumulator;
use crate::shard::consumer_subjects::ConsumerSubjects;
use crate::shard::drain_probe::{DrainProbe, ReadVerdict};
use crate::shard::shared::{find_writer, DrainNotification, DrainSnapshot, SharedCounters};
use crate::shard::worker::{consumer_subjects_slot, consumer_subjects_slot_mut, ActiveBinding};

// ── Configuration ───────────────────────────────────────────────────────────

#[allow(dead_code)] // batch_size kept for future use
pub(in crate::shard) struct DrainConfig {
    pub max_feed: usize,
    pub max_age_ms: u64,
    pub batch_size: u16,
    /// ROB-23: evict a connection whose writer channel has been
    /// continuously full (`Backpressured` on every flush, zero `Ok`)
    /// for this many milliseconds. 0 = never evict. See
    /// `Config::drain_stall_evict_ms`.
    pub stall_evict_ms: u64,
}

// ── Per-cycle ack-mode delivery record ──────────────────────────────────────

/// Per-entry metadata captured for ack-mode deliveries. Lives in
/// `DrainScratch.deliveries` alongside the wire bytes held by the
/// `Accumulator`. After a frame flushes successfully, the matching
/// records bump the `SharedCounters` atomics and feed
/// `DrainNotification::Delivered` to the command thread, which owns
/// `Binding.pending` and `InFlightCounters`. Fire-and-forget never
/// pushes here — no ack will ever arrive.
#[allow(dead_code)] // `stream` kept for diagnostics
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

    /// F11 — persistent flush-outcome buffer reused across drain
    /// cycles. Avoids an alloc per cycle. Stores `(ConnectionId, FlushOutcome)`
    /// pairs from Phase 2; Phase 3 consumes and clears.
    flush_results: Vec<(ConnectionId, FlushOutcome)>,
    /// F12 — persistent buffer for the slow-path notify sort.
    sorted_notify: Vec<PendingNotify>,

    /// ROB-23 — per-connection stall clock, persistent ACROSS cycles
    /// (never cleared in `drain_read`). An entry `(conn, since)` means
    /// every flush to `conn` has come back `Backpressured` (writer
    /// channel full, zero progress) in every cycle since `since`. The
    /// entry is dropped the moment a frame flushes `Ok` or the conn
    /// stops producing backpressured frames (e.g. paused, retired), so
    /// the clock only measures CONTINUOUS stall. Bounded: at most one
    /// entry per connection with a currently-full writer channel.
    stalled_conns: Vec<(ConnectionId, std::time::Instant)>,
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
            flush_results: Vec::with_capacity(16),
            sorted_notify: Vec::with_capacity(256),
            stalled_conns: Vec::with_capacity(4),
        }
    }
}

/// FlushOutcome (lifted to module scope so DrainScratch can hold a Vec).
#[derive(Clone, Copy)]
pub(in crate::shard) enum FlushOutcome {
    Ok,
    Backpressured(u64),
    /// Writer permanently gone. Carries the batch's first seq — like
    /// `Backpressured` — so the cursor cannot advance past frames that
    /// were never sent (BUG1: those seqs are not in `Binding.pending`,
    /// so retirement releases nothing and no rewind would ever occur).
    WriterGone(u64),
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
#[derive(Clone, Copy)]
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
/// Non-`Fed` verdicts mean no work; the gate stays cleared by the
/// worker's top-of-cycle `lock()`.
#[allow(clippy::too_many_arguments)]
pub(in crate::shard) fn drain_read(
    counters: &SharedCounters,
    snap: &DrainSnapshot,
    store: &dyn Store,
    cfg: &DrainConfig,
    scratch: &mut DrainScratch,
    consumer_subjects: &mut Vec<Option<ConsumerSubjects>>,
    now_ms: u64,
    snap_changed: bool,
) -> ReadVerdict {
    crate::lifecycle_trace!("21_drainer_enter", 0, snap.bindings.len() as u64, "shard");

    if !counters.has_any_demand() {
        return ReadVerdict::NoDemand;
    }

    let info = store.info();
    let cursor = counters.cursor();
    if info.last_seq <= cursor {
        return ReadVerdict::UpToDate {
            last_seq: info.last_seq,
            cursor,
        };
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
    // Pattern/subject-limit caches are pure functions of the snapshot's
    // match tables — valid while the snapshot Arc is unchanged (ptr_eq
    // in the worker, ABA-safe because it holds the previous Arc). Stale
    // entries after a swap would drop late-binding fanout subscribers.
    if snap_changed {
        scratch.resolve_cache.clear();
        scratch.subject_limit_cache.clear();
    }

    crate::lifecycle_trace!("25_drain_loop_start", start, end, "shard");

    // Walk the store, accumulate into per-connection buckets.
    store
        .for_each(start, end, &mut |entry| {
            // Per-stream max_age_ms takes precedence over the global default;
            // falls back to global cfg if the stream has no specific limit.
            let stream_max_age = snap
                .stream_max_age_ms
                .get(entry.stream_id as usize)
                .copied()
                .filter(|&v| v > 0)
                .unwrap_or(cfg.max_age_ms);
            process_drain_entry(
                counters,
                snap,
                entry,
                scratch,
                consumer_subjects,
                now_ms,
                stream_max_age,
                &mut more_pending,
                &mut lowest_skipped,
            );
        })
        .ok();

    ReadVerdict::Fed(DrainReadResult {
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
///
/// `stall_evict_ms` — ROB-23 slow-consumer eviction bound (0 = disabled),
/// see `DrainConfig::stall_evict_ms`.
#[allow(clippy::too_many_arguments)]
pub(in crate::shard) fn drain_deliver<P: DrainProbe>(
    counters: &SharedCounters,
    snap: &DrainSnapshot,
    gate: &Gate,
    names: &Arc<crate::common::NameRegistry>,
    scratch: &mut DrainScratch,
    consumer_subjects: &mut Vec<Option<ConsumerSubjects>>,
    notify_tx: &mut crate::shard::shared::NotifyProducer,
    silent_drops: &crate::common::SilentDrops,
    stall_evict_ms: u64,
    mut result: DrainReadResult,
    probe: &mut P,
) {
    // Phase 2 — flush every accumulator bucket as one RepBatch frame.
    // Results are captured into a small Vec so Phase 3 can do ack
    // bookkeeping without borrowing scratch inside the for_each closure.
    //
    // FlushOutcome distinguishes between three cases:
    //  - Ok:             frame sent successfully
    //  - Backpressured:  channel full (transient), retry next cycle
    //  - WriterGone:     writer not found (permanent); carries the batch
    //                    first_seq so the cursor won't skip it, mark dead
    scratch.flush_results.clear();
    let mut flush_results = std::mem::take(&mut scratch.flush_results);
    {
        let writers_by_conn = &snap.writers_by_conn;
        // F18: one-entry cache for `find_writer`. Many frames in a
        // single cycle belong to the same connection (subscribe-heavy
        // workloads); caching the previous lookup turns the HashMap
        // hit into a pointer compare. Cleared at the start of each
        // for_each invocation (closure capture).
        let mut last_conn: u64 = u64::MAX;
        let mut last_writer: Option<&crate::shard::shared::WriterIndexEntry> = None;
        scratch.acc.for_each(names, |frame| {
            // F29: drop one Bytes::clone() per frame by transferring
            // ownership directly to try_send (consumes the Bytes).
            // `acc.for_each` already passes ownership of the inner buffer
            // via the &mut frame reference.
            // F18: lookup via cache.
            let writer = if frame.connection_id.0 == last_conn {
                last_writer
            } else {
                last_conn = frame.connection_id.0;
                last_writer = find_writer(writers_by_conn, frame.connection_id.0);
                last_writer
            };
            let Some(writer) = writer else {
                probe.flush_writer_gone(frame.connection_id, frame.first_seq, frame.count, true);
                flush_results.push((
                    frame.connection_id,
                    FlushOutcome::WriterGone(frame.first_seq),
                ));
                return false;
            };
            if writer
                .write_failed
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                probe.flush_writer_gone(frame.connection_id, frame.first_seq, frame.count, false);
                flush_results.push((
                    frame.connection_id,
                    FlushOutcome::WriterGone(frame.first_seq),
                ));
                return false;
            }
            crate::lifecycle_trace!(
                "29_frame_built",
                frame.connection_id.0,
                frame.count as u64,
                "shard"
            );

            // F29: transfer ownership of `Bytes` directly to `try_send`
            // instead of bumping its Arc refcount via `.clone()`. The
            // closure owns `frame` (passed by value); on backpressure
            // `try_send` hands the bytes back inside the Err so we can
            // safely keep `frame.first_seq` for tracking.
            let conn = frame.connection_id;
            let count = frame.count;
            let first_seq = frame.first_seq;
            let ok = writer.write_tx.try_send(frame.bytes).is_ok();

            if ok {
                probe.flush_ok(conn, first_seq, count);
                crate::lifecycle_trace!("30_send_bytes_done", conn.0, count as u64, "shard");
                flush_results.push((conn, FlushOutcome::Ok));
            } else {
                probe.flush_backpressured(conn, first_seq, count);
                flush_results.push((conn, FlushOutcome::Backpressured(first_seq)));
            }
            ok
        });
    }

    // Phase 3 — post-flush bookkeeping (atomics + command-thread
    // notifications). Fire-and-forget entries never hit scratch.deliveries,
    // so this loop is a no-op in the pub/sub default path.
    for &(conn, outcome) in &flush_results {
        match outcome {
            FlushOutcome::Ok => {}
            FlushOutcome::Backpressured(first_seq) => {
                // Treat as skipped — cursor won't advance past these entries.
                track_skipped(&mut result.lowest_skipped, first_seq);
                result.more_pending = true;
                // ROB-23 (audit #7a): a full writer channel is transient
                // ONLY for a bounded window. The reply path already ejects
                // a non-reading connection the moment its queue fills
                // (ROB-22, transport/registry.rs); mirror that policy here
                // with a grace window: once `conn` has made ZERO flush
                // progress for `stall_evict_ms`, treat it exactly like
                // `WriterGone` — queue it for retirement so the command
                // thread retires its bindings, the snapshot drops it, and
                // the shared cursor can advance past it. Healthy sibling
                // consumers on the shard stop being starved.
                //
                // No message loss: retirement never touches the store.
                // The evicted consumer's undelivered seqs stay retained
                // above its per-consumer ack floor and are replayed on
                // its next subscribe; its delivered-but-unacked pending
                // is released through the normal BUG2 retirement rewind.
                if stall_evict_ms > 0 {
                    let now = std::time::Instant::now();
                    match scratch.stalled_conns.iter().find(|e| e.0 == conn) {
                        None => scratch.stalled_conns.push((conn, now)),
                        Some(&(_, since)) => {
                            if now.duration_since(since).as_millis() as u64 >= stall_evict_ms
                                && !scratch.dead_connections.contains(&conn)
                            {
                                tracing::warn!(
                                    conn_id = conn.0,
                                    stall_ms = stall_evict_ms,
                                    "delivery writer stalled past eviction \
                                     window, retiring slow connection"
                                );
                                scratch.dead_connections.push(conn);
                            }
                        }
                    }
                }
            }
            FlushOutcome::WriterGone(first_seq) => {
                // BUG1: the batch destined to this connection was never
                // sent. Track its first seq so the cursor stops before it,
                // and retry next cycle once the conn is retired / a live
                // sibling picks it up.
                track_skipped(&mut result.lowest_skipped, first_seq);
                result.more_pending = true;
                scratch.dead_connections.push(conn);
            }
        }
    }

    // ROB-23 maintenance — the stall clock survives ONLY across strictly
    // consecutive zero-progress cycles. Drop the entry when the conn made
    // any progress this cycle (a frame flushed `Ok` — the writer drained),
    // or when it produced no backpressured frame at all (its work is done,
    // paused, or its binding was retired). A stalled conn keeps re-framing
    // every cycle while it pins the cursor, so absence == not stalling.
    if !scratch.stalled_conns.is_empty() {
        scratch.stalled_conns.retain(|&(c, _)| {
            let mut backpressured = false;
            let mut progressed = false;
            for &(fc, o) in flush_results.iter() {
                if fc == c {
                    match o {
                        FlushOutcome::Ok => progressed = true,
                        FlushOutcome::Backpressured(_) => backpressured = true,
                        FlushOutcome::WriterGone(_) => {}
                    }
                }
            }
            backpressured && !progressed
        });
    }

    record_deliveries(counters, consumer_subjects, &scratch.deliveries, &flush_results);

    // Group successful deliveries by binding_id and notify the command
    // thread once per binding (same shape Command::Delivered expects).
    if !scratch.deliveries.is_empty() {
        notify_delivered_grouped(
            notify_tx,
            &snap.bindings,
            &scratch.deliveries,
            &flush_results,
            &mut scratch.sorted_notify,
            silent_drops,
        );
    }
    // Return the persistent flush buffer for the next cycle.
    scratch.flush_results = flush_results;

    advance_cursor(counters, &result, probe);
    close_window(&mut result);
    report_dead_connections(&mut scratch.dead_connections, notify_tx, silent_drops);
    reopen_if_pending(gate, &result);
}

// ── Drain-deliver tail components ───────────────────────────────────────────

/// Bookkeeping for frames that flushed `Ok`: shared inflight, drain-owned
/// per-(consumer, subject) inflight, and delivery-memory suppression — a
/// re-walk must not re-send an in-flight seq until the command thread
/// releases it (ack absorbs into the floor; nack/timeout/retirement re-arm).
#[inline]
fn record_deliveries(
    counters: &SharedCounters,
    consumer_subjects: &mut Vec<Option<ConsumerSubjects>>,
    deliveries: &[PendingNotify],
    flush_results: &[(ConnectionId, FlushOutcome)],
) {
    for d in deliveries {
        if frame_ok_for(flush_results, d.conn) {
            counters.inc_inflight(d.consumer_id, d.queue_id);
            let cs = consumer_subjects_slot_mut(consumer_subjects, d.consumer_id);
            cs.inc(d.subject_hash);
            cs.suppress(d.seq);
        }
    }
}

/// Cursor advance — the only place the drain moves the cursor forward.
/// `lowest_skipped` pins it strictly before the first unsent seq.
#[inline]
fn advance_cursor<P: DrainProbe>(
    counters: &SharedCounters,
    result: &DrainReadResult,
    probe: &mut P,
) -> u64 {
    let new_cursor = result
        .lowest_skipped
        .map_or(result.end - 1, |ls| ls.saturating_sub(1));
    probe.cursor_advance(counters, new_cursor, result);
    counters.set_cursor(new_cursor);
    new_cursor
}

/// Final `more_pending` verdict. `end <= last_seq` (window stopped short
/// of the store tail) is the genuinely new fact; the `lowest_skipped`
/// half is redundant with the `track_skipped` sites (asserted below) and
/// kept as a belt while the tail-stall bug is open.
#[inline]
fn close_window(result: &mut DrainReadResult) {
    debug_assert!(
        result.lowest_skipped.is_none() || result.more_pending,
        "a track_skipped call site failed to set more_pending"
    );
    if result.end <= result.last_seq || result.lowest_skipped.is_some() {
        result.more_pending = true;
    }
}

/// Report permanently-dead connections (writer gone) to the command
/// thread. Backpressured conns are transient and NOT reported here.
#[inline]
fn report_dead_connections(
    dead: &mut Vec<ConnectionId>,
    notify_tx: &mut crate::shard::shared::NotifyProducer,
    silent_drops: &crate::common::SilentDrops,
) {
    for conn_id in dead.drain(..) {
        if notify_tx
            .try_send(DrainNotification::ConnectionDead(conn_id))
            .is_err()
        {
            silent_drops.inc_notify_ring();
        }
    }
}

/// INVARIANT: only ever RE-OPEN the gate here — clearing is owned by the
/// drain worker's top-of-cycle `lock()`, so a concurrent publish's
/// `release()` can never be wiped by a stale end-of-cycle clear (the
/// tail-message lost-wakeup). No path here may call `gate.lock()`.
#[inline]
fn reopen_if_pending(gate: &Gate, result: &DrainReadResult) {
    if result.more_pending {
        gate.release();
        crate::lifecycle_trace!("33_drainer_exit_released", 0, 0, "shard");
    }
}

// ── Per-entry processing ────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn process_drain_entry(
    counters: &SharedCounters,
    snap: &DrainSnapshot,
    entry: &arbitro_store::Entry<'_>,
    scratch: &mut DrainScratch,
    consumer_subjects: &mut Vec<Option<ConsumerSubjects>>,
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

    // Skip entries from previous incarnations of a recycled stream_id.
    // created_at_seq == 0 means "no filter" (backward compat).
    let stream_raw = stream_id.raw();
    if let Some(&birth_seq) = snap.stream_created_at_seq.get(stream_raw as usize) {
        if birth_seq > 0 && entry.seq < birth_seq {
            return;
        }
    }

    // Demand check — atomic read.
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
    // SEC-5: verify the literal bytes — a 32-bit hash collision misdelivers.
    let lookup = mt.lookup_verified(subject_hash, entry.subject);

    // Step 1: resolve patterns whenever the stream HAS any. Gating on
    // `lookup.is_empty()` starved every pattern subscriber as soon as one
    // catch-all existed — `catch_all` ignores the subject, so the set was
    // never empty and this branch never ran.
    if mt.pattern_count() > 0 && !scratch.resolve_cache.contains_key(&cache_key) {
        let mut resolved = Vec::new();
        mt.resolve_patterns_readonly(subject_hash, entry.subject, &mut resolved);
        scratch.resolve_cache.insert(cache_key, resolved);
    }
    // Snapshot-pin purity guard: a retained entry must equal a fresh
    // resolve against the same (ptr-identical) snapshot.
    #[cfg(debug_assertions)]
    if mt.pattern_count() > 0 {
        if let Some(cached) = scratch.resolve_cache.get(&cache_key) {
            let mut fresh = Vec::new();
            mt.resolve_patterns_readonly(subject_hash, entry.subject, &mut fresh);
            debug_assert_eq!(*cached, fresh, "resolve_cache stale under unchanged snapshot");
        }
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
    // Same snapshot-pin purity guard for the subject-limit cache.
    #[cfg(debug_assertions)]
    if mt.has_subject_limits() {
        debug_assert_eq!(
            subject_limit,
            mt.resolve_subject_limit_readonly(subject_hash, entry.subject),
            "subject_limit_cache stale under unchanged snapshot"
        );
    }

    // Step 3: collect matches — reuse `lookup` computed above.
    scratch.matches.clear();
    scratch.matches.extend(lookup.iter());
    if let Some(resolved) = scratch.resolve_cache.get(&cache_key) {
        // Dedup across the merge: one subscription can sit in both buckets
        // (literal + pattern). A duplicate delivers twice AND increments
        // inflight twice against a single ack, permanently starving the
        // consumer. `MatchEntry` equality excludes `binding_idx` by design.
        for e in resolved.iter() {
            if !scratch.matches.contains(e) {
                scratch.matches.push(*e);
            }
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
        subject_limit,
        scratch,
        consumer_subjects,
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
    consumer_subjects: &[Option<ConsumerSubjects>],
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

    // TEMP chaos-debug — per-entry skip accounting, removed after diagnosis.
    let mut dbg_recipients = 0u32;
    let mut dbg_tracked = false;
    let (mut dbg_conn0, mut dbg_qdedup, mut dbg_dead, mut dbg_unbound, mut dbg_wf) =
        (0u32, 0u32, 0u32, 0u32, 0u32);
    let mut dbg_acked_floor = 0u32;
    let mut dbg_deliver_floor = 0u32;

    for i in 0..n {
        let raw = start + i;
        let idx = if raw >= n { raw - n } else { raw };
        let me = scratch.matches[idx];
        let consumer_id = me.consumer_id;
        let connection_id = me.connection_id;
        let queue_id = me.queue_id;

        if connection_id == ConnectionId(0) {
            dbg_conn0 += 1;
            continue;
        }

        // Temporal isolation (D5/A7) + delivery memory: never send a seq
        // this consumer has already acked OR already has in flight.
        // `is_suppressed` covers the contiguous-acked floor (all seqs
        // <= floor acked, gap-free — maintained by the command thread in
        // `shard/ack_floor.rs` and mirrored via `DrainEvent::Ack`) PLUS
        // the delivered-or-acked set above it (fed by Phase 3 on every
        // successful ack-mode delivery, re-armed by explicit releases:
        // nack, ack-timeout, retirement). The set is what breaks the
        // redelivery livelock: a capacity-capped low seq pins the cursor
        // AND freezes the floor, so without delivery memory every cycle
        // re-walked the pinned window re-sending in-flight and acked
        // seqs without bound (measured: 911k deliveries for 300
        // published). Every owed seq (never delivered, nacked, timed
        // out, or released by retirement) is absent from both parts, so
        // this skip can never suppress a legitimate (re)delivery. No
        // `track_skipped`/`served_queues` side effects: the entry is not
        // owed to this consumer, so the cursor must not be pinned on it
        // and a queue sibling must not be starved of it. Steady-state
        // cost for the forward-moving cursor: one compare (seq > floor)
        // + one is_empty/contains check against drain-local state — no
        // atomics, no alloc.
        if let Some(cs) = consumer_subjects_slot(consumer_subjects, consumer_id.0) {
            if cs.is_suppressed(entry.seq) {
                dbg_acked_floor += 1;
                continue;
            }
        }

        // Queue dedup: one entry per queue within the match set of this entry.
        // F21: served_queues is typically tiny (≤8); linear scan beats
        // a HashSet at this size and reuses the existing Vec scratch.
        if queue_id != QueueId(0) {
            let mut already_served = false;
            for &q in scratch.served_queues.iter() {
                if q == queue_id {
                    already_served = true;
                    break;
                }
            }
            if already_served {
                dbg_qdedup += 1;
                continue;
            }
        }

        // F22: same shape — `dead_connections` is also small. The
        // earlier explicit linear scan via `.contains()` was already
        // O(N); keeping it as a tight loop avoids the iterator overhead.
        let mut conn_is_dead = false;
        for &dc in scratch.dead_connections.iter() {
            if dc == connection_id {
                conn_is_dead = true;
                break;
            }
        }
        if conn_is_dead {
            dbg_dead += 1;
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
            dbg_unbound += 1;
            continue;
        }
        let binding = &bindings[binding_idx];

        // Deliver floor (DeliverPolicy::New / ByStartSeq): seqs at or
        // below the binding's start position were never owed to this
        // consumer. Same no-side-effect discipline as the suppression
        // skip above — no `track_skipped`, no `served_queues` — so a
        // below-floor entry neither pins the cursor nor starves a queue
        // sibling. One u64 compare on the hot path.
        if entry.seq <= binding.deliver_floor {
            dbg_deliver_floor += 1;
            continue;
        }

        // BUG4: a binding whose writer has already failed is ineligible.
        // Skip it WITHOUT marking `served_queues` so a queue's live sibling
        // falls through and takes this entry, instead of the dead binding
        // winning the round-robin pick and the frame being dropped as
        // WriterGone at flush. One relaxed atomic load per candidate.
        if binding
            .write_failed
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            dbg_wf += 1;
            continue;
        }

        // Paused check — atomic read.
        if counters.is_paused(consumer_id.0) {
            *more_pending = true;
            track_skipped(lowest_skipped, entry.seq);
            dbg_tracked = true;
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
                dbg_tracked = true;
                continue;
            }

            // Per-consumer subject inflight check — state lives in the
            // drain-owned `ConsumerSubjects` slot (no atomics, no
            // cross-thread traffic). Two consumers on the same subject
            // have independent budgets.
            if let Some(max) = subject_limit {
                let pending_subj = scratch
                    .local_subject
                    .get(&(consumer_id.0, subject_hash))
                    .copied()
                    .unwrap_or(0);
                let committed = consumer_subjects_slot(consumer_subjects, consumer_id.0)
                    .map_or(0, |cs| cs.get(subject_hash));
                if pending_subj + committed >= max {
                    *more_pending = true;
                    track_skipped(lowest_skipped, entry.seq);
                    dbg_tracked = true;
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

        // Extract user payload from extended layout when HAS_HEADERS is set.
        // Store format: [payload_len:u32 LE][user_payload][headers...].
        // The consumer receives only the user payload — headers are broker-internal.
        let raw_payload =
            if entry.flags & arbitro_store::flags::HAS_HEADERS != 0 && entry.payload.len() >= 4 {
                let pay_len = u32::from_le_bytes([
                    entry.payload[0],
                    entry.payload[1],
                    entry.payload[2],
                    entry.payload[3],
                ]) as usize;
                if entry.payload.len() >= 4 + pay_len {
                    &entry.payload[4..4 + pay_len]
                } else {
                    entry.payload
                }
            } else {
                entry.payload
            };

        // Extract reply_to from payload when HAS_REPLY_TO flag is set.
        // Store format: [reply_len:u16 LE][reply_to bytes][actual payload].
        let (reply_to, actual_payload): (&[u8], &[u8]) =
            if entry.flags & arbitro_store::flags::HAS_REPLY_TO != 0 && raw_payload.len() >= 2 {
                let reply_len = u16::from_le_bytes([raw_payload[0], raw_payload[1]]) as usize;
                if raw_payload.len() >= 2 + reply_len {
                    (
                        &raw_payload[2..2 + reply_len],
                        &raw_payload[2 + reply_len..],
                    )
                } else {
                    (&[], raw_payload)
                }
            } else {
                (&[], raw_payload)
            };

        let fire_and_forget = binding.fire_and_forget;
        dbg_recipients += 1;
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

    // TEMP chaos-debug — an entry with matches but zero recipients and no
    // skip-track is the loss signature: the cursor will advance past it.
    if dbg_recipients == 0 && !dbg_tracked && chaos_debug() {
        eprintln!(
            "[chaos-debug] NO-RECIPIENT seq={} matches={} conn0={} qdedup={} dead={} unbound={} write_failed={} acked_floor={} deliver_floor={}",
            entry.seq, n, dbg_conn0, dbg_qdedup, dbg_dead, dbg_unbound, dbg_wf, dbg_acked_floor, dbg_deliver_floor
        );
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
#[allow(clippy::too_many_arguments)]
fn notify_delivered_grouped(
    notify_tx: &mut crate::shard::shared::NotifyProducer,
    bindings: &[ActiveBinding],
    deliveries: &[PendingNotify],
    flush_results: &[(ConnectionId, FlushOutcome)],
    sorted_buf: &mut Vec<PendingNotify>,
    silent_drops: &crate::common::SilentDrops,
) {
    // F11: replace the per-cycle HashMap<conn, bool> with a linear scan
    // over `flush_results` (typically 1–8 entries). Cache locality wins.
    let frame_ok = |conn: ConnectionId| -> bool {
        for &(c, o) in flush_results.iter() {
            if c == conn {
                return matches!(o, FlushOutcome::Ok);
            }
        }
        false
    };

    // Fast path — every delivery belongs to the same binding AND all
    // frames succeeded. Pub/sub of a single consumer hits this path.
    if let Some(first) = deliveries.first() {
        let first_idx = first.binding_idx;
        if deliveries.iter().all(|d| d.binding_idx == first_idx)
            && deliveries.iter().all(|d| frame_ok(d.conn))
        {
            let binding = &bindings[first_idx];
            // The notify ring transfers ownership across threads — the
            // entries Vec must be owned. Build it once via collect.
            let entries: Vec<DeliveredEntry> = deliveries
                .iter()
                .map(|d| DeliveredEntry {
                    seq: d.seq,
                    subject_hash: d.subject_hash,
                    _pad: 0,
                })
                .collect();
            if notify_tx
                .try_send(DrainNotification::Delivered {
                    binding_id: binding.binding_id,
                    consumer_id: binding.consumer_id,
                    queue_id: binding.queue_id,
                    entries,
                })
                .is_err()
            {
                silent_drops.inc_notify_ring();
            }
            return;
        }
    }

    // F12 + F28: slow path — mixed bindings and/or partial frame success.
    // Reuse the persistent `sorted_buf` so we don't allocate per cycle.
    // F28: counting sort on `binding_idx` (bounded
    // by bindings.len()) replaces the comparison sort — O(N + K) vs
    // O(N log N) where K = bindings.len().
    sorted_buf.clear();
    sorted_buf.extend(deliveries.iter().copied().filter(|d| frame_ok(d.conn)));

    if sorted_buf.is_empty() {
        return;
    }

    let n = sorted_buf.len();
    let k = bindings.len();
    // F28 counting sort: two-pass into bucket_starts scratch.
    // bucket_starts[i] = where bucket i begins in the placed array.
    // We no longer use entries_buf — placement is done in a local vec,
    // and per-group entries are collected directly from the placed slice.
    let mut bucket_counts: Vec<u32> = vec![0u32; k + 1];
    for d in sorted_buf.iter() {
        let idx = d.binding_idx;
        debug_assert!(idx < k, "binding_idx out of range");
        bucket_counts[idx + 1] += 1;
    }
    // Prefix sum -> bucket_starts.
    for i in 1..=k {
        bucket_counts[i] += bucket_counts[i - 1];
    }
    // Place into a scratch of PendingNotify. Allocate a small local
    // placement vec sized exactly N (single alloc/cycle in slow
    // path; the fast path above handles the steady state).
    // Sentinel value reused as default for placement scratch.
    let sentinel = sorted_buf[0];
    let mut placed: Vec<PendingNotify> = vec![sentinel; n];
    // Use bucket_counts as the moving write cursor.
    let mut cursors = bucket_counts.clone();
    for d in sorted_buf.iter() {
        let idx = d.binding_idx;
        let p = cursors[idx] as usize;
        placed[p] = *d;
        cursors[idx] += 1;
    }

    // Walk groups via bucket_starts -> bucket_starts[next].
    // Collect directly from the placed slice into an owned Vec for
    // each group — avoids the old entries_buf.clone() which was
    // duplicating every DeliveredEntry.
    for idx in 0..k {
        let start = bucket_counts[idx] as usize;
        let end = bucket_counts[idx + 1] as usize;
        if start == end {
            continue;
        }
        let entries: Vec<DeliveredEntry> = placed[start..end]
            .iter()
            .map(|p| DeliveredEntry {
                seq: p.seq,
                subject_hash: p.subject_hash,
                _pad: 0,
            })
            .collect();
        let binding = &bindings[idx];
        if notify_tx
            .try_send(DrainNotification::Delivered {
                binding_id: binding.binding_id,
                consumer_id: binding.consumer_id,
                queue_id: binding.queue_id,
                entries,
            })
            .is_err()
        {
            silent_drops.inc_notify_ring();
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

#[inline]
fn track_skipped(lowest: &mut Option<u64>, seq: u64) {
    *lowest = Some(lowest.map_or(seq, |s| s.min(seq)));
}

/// F11: flush_results is typically 1–8 entries; a linear-scan helper
/// beats HashMap on inserts and lookups at this size and removes the
/// per-cycle HashMap allocation entirely.
#[inline]
fn frame_ok_for(results: &[(ConnectionId, FlushOutcome)], conn: ConnectionId) -> bool {
    for &(c, o) in results.iter() {
        if c == conn {
            return matches!(o, FlushOutcome::Ok);
        }
    }
    false
}

// TEMP chaos-debug probe — remove after loss diagnosis.
pub(crate) fn chaos_debug() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("ARBITRO_CHAOS_DEBUG").is_ok())
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{NameRegistry, SilentDrops};
    use crate::shard::drain_probe::ProbeOff;
    use crate::shard::shared::NotifyRing;

    /// BUG1 — when a frame flushes as `WriterGone`, the cursor must NOT
    /// advance past that batch. Before the fix, `WriterGone` skipped
    /// `track_skipped`, so `new_cursor` jumped to `end - 1`, permanently
    /// losing every seq in the failed batch (they are not in
    /// `Binding.pending`, so no retirement rewind ever recovers them).
    #[tokio::test(flavor = "current_thread")]
    async fn writer_gone_leaves_cursor_before_failed_batch() {
        let counters = SharedCounters::new();
        let gate = Gate::new();
        let names = Arc::new(NameRegistry::default());
        let silent = SilentDrops::new();
        let (mut producers, _rx, _sd) = NotifyRing::new(1);
        let mut notify_tx = producers.pop().unwrap();
        let mut scratch = DrainScratch::new();
        let mut consumer_subjects: Vec<Option<ConsumerSubjects>> = Vec::new();

        // Empty snapshot → no writer for conn 5 → `find_writer` returns
        // None → the frame flushes as WriterGone.
        let snap = DrainSnapshot::empty();

        // One frame for conn 5 whose batch starts at seq 10. No entry is
        // pushed to `scratch.deliveries` (Phase 3 delivery loop is a no-op).
        scratch.acc.clear();
        scratch.acc.add(
            ConnectionId(5),
            StreamId(1),
            ConsumerId(0),
            10,
            b"subj",
            0,
            &[],
            b"payload",
        );

        let result = DrainReadResult {
            start: 1,
            end: 11, // end - 1 == 10 == the batch first_seq
            more_pending: false,
            lowest_skipped: None,
            last_seq: 10,
        };

        drain_deliver(
            &counters,
            &snap,
            &gate,
            &names,
            &mut scratch,
            &mut consumer_subjects,
            &mut notify_tx,
            &silent,
            0,
            result,
            &mut ProbeOff,
        );

        // With the fix the cursor stops at 9 (batch first_seq - 1); the
        // buggy behaviour advanced it to 10, skipping seq 10 forever.
        assert_eq!(
            counters.cursor(),
            9,
            "cursor must not advance past the undelivered WriterGone batch",
        );
        // The conn was queued for retirement.
        assert!(
            gate.is_open(),
            "more_pending must re-open the gate for retry"
        );
    }

    /// ROB-23 (audit #7a) — a connection whose writer channel stays full
    /// past `stall_evict_ms` must be queued for retirement exactly like
    /// `WriterGone`, so the shared cursor stops being pinned by a
    /// dead-reading consumer. Before the fix, `Backpressured` was retried
    /// forever and one non-reading client starved every sibling on the
    /// shard.
    #[tokio::test(flavor = "current_thread")]
    async fn backpressured_conn_evicted_after_stall_window() {
        let counters = SharedCounters::new();
        let gate = Gate::new();
        let names = Arc::new(NameRegistry::default());
        let silent = SilentDrops::new();
        let (mut producers, mut rx, _sd) = NotifyRing::new(1);
        let mut notify_tx = producers.pop().unwrap();
        let mut scratch = DrainScratch::new();
        let mut consumer_subjects: Vec<Option<ConsumerSubjects>> = Vec::new();

        // Writer for conn 7 with a FULL channel (cap 1, pre-filled) →
        // every flush comes back `Backpressured`.
        let mut snap = DrainSnapshot::empty();
        let (write_tx, _write_rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(1);
        write_tx
            .try_send(bytes::Bytes::from_static(b"plug"))
            .unwrap();
        snap.writers_by_conn.insert(
            7,
            crate::shard::shared::WriterIndexEntry {
                write_tx,
                write_failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
        );

        let run_cycle =
            |scratch: &mut DrainScratch,
             consumer_subjects: &mut Vec<Option<ConsumerSubjects>>,
             notify_tx: &mut crate::shard::shared::NotifyProducer| {
                // Re-arm the frame — a real stalled cycle re-reads the store
                // and re-accumulates the same batch every cycle.
                scratch.dead_connections.clear();
                scratch.acc.clear();
                scratch.acc.add(
                    ConnectionId(7),
                    StreamId(1),
                    ConsumerId(0),
                    10,
                    b"subj",
                    0,
                    &[],
                    b"payload",
                );
                drain_deliver(
                    &counters,
                    &snap,
                    &gate,
                    &names,
                    scratch,
                    consumer_subjects,
                    notify_tx,
                    &silent,
                    20, // stall_evict_ms
                    DrainReadResult {
                        start: 1,
                        end: 11,
                        more_pending: false,
                        lowest_skipped: None,
                        last_seq: 10,
                    },
                    &mut ProbeOff,
                );
            };

        // Cycle 1 — arms the stall clock; NOT evicted yet.
        run_cycle(&mut scratch, &mut consumer_subjects, &mut notify_tx);
        assert_eq!(
            counters.cursor(),
            9,
            "cursor pinned before the stalled batch"
        );
        assert!(
            rx.try_recv().is_none(),
            "no ConnectionDead before the stall window elapses",
        );

        // Cycle 2, past the window — evicted like WriterGone.
        std::thread::sleep(std::time::Duration::from_millis(40));
        run_cycle(&mut scratch, &mut consumer_subjects, &mut notify_tx);
        assert_eq!(counters.cursor(), 9, "cursor still pinned this cycle");
        assert!(
            matches!(
                rx.try_recv(),
                Some(DrainNotification::ConnectionDead(ConnectionId(7)))
            ),
            "stalled conn must be reported dead after the eviction window",
        );
        assert!(gate.is_open(), "more_pending must re-open the gate");
    }

    /// ROB-23 guard — a flush that makes progress (`Ok`) resets the stall
    /// clock, so a slow-but-alive consumer is never evicted.
    #[tokio::test(flavor = "current_thread")]
    async fn flush_progress_resets_stall_clock() {
        let counters = SharedCounters::new();
        let gate = Gate::new();
        let names = Arc::new(NameRegistry::default());
        let silent = SilentDrops::new();
        let (mut producers, mut rx, _sd) = NotifyRing::new(1);
        let mut notify_tx = producers.pop().unwrap();
        let mut scratch = DrainScratch::new();
        let mut consumer_subjects: Vec<Option<ConsumerSubjects>> = Vec::new();

        let mut snap = DrainSnapshot::empty();
        // Cap 1, initially FULL.
        let (write_tx, mut write_rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(1);
        write_tx
            .try_send(bytes::Bytes::from_static(b"plug"))
            .unwrap();
        snap.writers_by_conn.insert(
            7,
            crate::shard::shared::WriterIndexEntry {
                write_tx,
                write_failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
        );

        let run_cycle =
            |scratch: &mut DrainScratch,
             consumer_subjects: &mut Vec<Option<ConsumerSubjects>>,
             notify_tx: &mut crate::shard::shared::NotifyProducer| {
                scratch.dead_connections.clear();
                scratch.acc.clear();
                scratch.acc.add(
                    ConnectionId(7),
                    StreamId(1),
                    ConsumerId(0),
                    10,
                    b"subj",
                    0,
                    &[],
                    b"payload",
                );
                drain_deliver(
                    &counters,
                    &snap,
                    &gate,
                    &names,
                    scratch,
                    consumer_subjects,
                    notify_tx,
                    &silent,
                    20,
                    DrainReadResult {
                        start: 1,
                        end: 11,
                        more_pending: false,
                        lowest_skipped: None,
                        last_seq: 10,
                    },
                    &mut ProbeOff,
                );
            };

        // Cycle 1 — backpressured, clock armed.
        run_cycle(&mut scratch, &mut consumer_subjects, &mut notify_tx);
        assert_eq!(scratch.stalled_conns.len(), 1, "stall clock armed");

        // The consumer drains its channel → next flush succeeds → clock reset.
        let _ = write_rx.try_recv();
        run_cycle(&mut scratch, &mut consumer_subjects, &mut notify_tx);
        assert!(
            scratch.stalled_conns.is_empty(),
            "flush progress must reset the stall clock",
        );

        // Even long past the window, a fresh stall starts a fresh clock.
        std::thread::sleep(std::time::Duration::from_millis(40));
        run_cycle(&mut scratch, &mut consumer_subjects, &mut notify_tx);
        assert!(
            rx.try_recv().is_none(),
            "no eviction: the stall was never continuous for the window",
        );
    }
}
