//! Zero-cost drain observability, selected ONCE at spawn (`router.rs`).
//!
//! The drain loop is generic over [`DrainProbe`]. `ProbeOff` has empty
//! bodies, so its monomorph contains no flag, no load, no branch;
//! `ProbeOn` records per-cycle verdicts into a fixed ring and emits the
//! chaos-debug lines the drain hot path used to gate on `chaos_debug()`.

use crate::shard::drain::DrainReadResult;
use crate::shard::shared::SharedCounters;
use arbitro_engine_v2::types::ConnectionId;

// ── Verdict types ───────────────────────────────────────────────────────────

/// Phase-1 outcome — names WHY a cycle produced (or didn't produce) work.
#[derive(Clone, Copy)]
pub(in crate::shard) enum ReadVerdict {
    /// No stream has an active binding — nothing wants messages.
    NoDemand,
    /// The store walk already reached the tail (`last_seq <= cursor`).
    UpToDate { last_seq: u64, cursor: u64 },
    /// A window was fed into scratch.
    Fed(DrainReadResult),
}

/// End-of-cycle park decision.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::shard) enum ParkVerdict {
    /// Gate observed open on the dedicated post-sleep read — loop again.
    Continue,
    /// Gate observed closed — park on `acquire()`.
    Park,
}

/// What consuming the rewind signal did this cycle.
#[derive(Clone, Copy)]
pub(in crate::shard) enum RewindApplied {
    None,
    /// Signal consumed; cursor moved back to `to` (= signal - 1).
    Applied { to: u64 },
    /// Signal consumed but the cursor was already at/below the target.
    AlreadyBehind { signal: u64 },
}

// ── Probe trait ─────────────────────────────────────────────────────────────

/// Drain-loop observability hooks. Methods take REFERENCES to shared
/// state (`&SharedCounters`) rather than pre-read values, so the Off
/// monomorph never forces the hot path to compute an unused argument.
pub(in crate::shard) trait DrainProbe: Send + 'static {
    /// Cycle opened: gate cleared, rewind consumed, event ring drained.
    fn cycle_start(&mut self, counters: &SharedCounters, rewind: RewindApplied);
    /// Phase-1 verdict.
    fn read_verdict(&mut self, v: ReadVerdict);
    /// One frame flushed to its writer channel.
    fn flush_ok(&mut self, conn: ConnectionId, first_seq: u64, count: u16);
    /// One frame bounced off a full writer channel (transient).
    fn flush_backpressured(&mut self, conn: ConnectionId, first_seq: u64, count: u16);
    /// One frame hit a dead writer. `not_found` distinguishes "no writer
    /// in the snapshot" from "writer task reported an I/O failure".
    fn flush_writer_gone(&mut self, conn: ConnectionId, first_seq: u64, count: u16, not_found: bool);
    /// Cursor about to advance — called BEFORE `set_cursor`, so
    /// `counters.cursor()` still reads the old value.
    fn cursor_advance(&mut self, counters: &SharedCounters, new_cursor: u64, r: &DrainReadResult);
    /// End-of-cycle park decision (after the dedicated second gate read).
    fn park(&mut self, counters: &SharedCounters, v: ParkVerdict, stalled: bool);
}

// ── Off: compiles to nothing ────────────────────────────────────────────────

/// Disabled probe — every body is empty; `run::<ProbeOff>` contains
/// zero probe instructions.
pub(in crate::shard) struct ProbeOff;

impl DrainProbe for ProbeOff {
    #[inline(always)]
    fn cycle_start(&mut self, _: &SharedCounters, _: RewindApplied) {}
    #[inline(always)]
    fn read_verdict(&mut self, _: ReadVerdict) {}
    #[inline(always)]
    fn flush_ok(&mut self, _: ConnectionId, _: u64, _: u16) {}
    #[inline(always)]
    fn flush_backpressured(&mut self, _: ConnectionId, _: u64, _: u16) {}
    #[inline(always)]
    fn flush_writer_gone(&mut self, _: ConnectionId, _: u64, _: u16, _: bool) {}
    #[inline(always)]
    fn cursor_advance(&mut self, _: &SharedCounters, _: u64, _: &DrainReadResult) {}
    #[inline(always)]
    fn park(&mut self, _: &SharedCounters, _: ParkVerdict, _: bool) {}
}

// ── On: chaos-compatible prints + per-cycle ring ────────────────────────────

/// One drain cycle, as the probe saw it.
#[derive(Clone, Copy)]
pub(in crate::shard) struct CycleRecord {
    cursor_before: u64,
    /// Cursor after `advance_cursor`; for NoDemand/UpToDate cycles it
    /// stays equal to `cursor_before`.
    cursor_after: u64,
    rewind: RewindApplied,
    verdict: ReadVerdict,
    flushed_ok: u16,
    flushed_backpressured: u16,
    flushed_gone: u16,
    stalled: bool,
    parked: bool,
}

impl Default for CycleRecord {
    fn default() -> Self {
        Self {
            cursor_before: 0,
            cursor_after: 0,
            rewind: RewindApplied::None,
            verdict: ReadVerdict::NoDemand,
            flushed_ok: 0,
            flushed_backpressured: 0,
            flushed_gone: 0,
            stalled: false,
            parked: false,
        }
    }
}

const RING_LEN: usize = 512;

/// Recording probe. Emits the same lines the removed `chaos_debug()`
/// sites printed (formats preserved), keeps the last [`RING_LEN`] cycle
/// records in a ring allocated once at spawn, and dumps the ring on the
/// first park at each cursor position while demand exists — the
/// strongest drain-side proxy for "parked with a consumer still owed
/// messages" (per-consumer starvation is not decidable from drain-side
/// state alone).
pub(in crate::shard) struct ProbeOn {
    ring: Box<[CycleRecord; RING_LEN]>,
    /// Next slot to write.
    idx: usize,
    /// Records written so far, capped at `RING_LEN`.
    len: usize,
    /// Record being assembled for the current cycle.
    cur: CycleRecord,
    /// Cursor value at the last ring dump — dedups quiesce-point dumps.
    last_dump_cursor: Option<u64>,
}

impl ProbeOn {
    #[allow(clippy::new_without_default)]
    pub(in crate::shard) fn new() -> Self {
        Self {
            ring: Box::new([CycleRecord::default(); RING_LEN]),
            idx: 0,
            len: 0,
            cur: CycleRecord::default(),
            last_dump_cursor: None,
        }
    }

    fn push(&mut self, rec: CycleRecord) {
        self.ring[self.idx] = rec;
        self.idx = (self.idx + 1) % RING_LEN;
        if self.len < RING_LEN {
            self.len += 1;
        }
    }

    fn dump(&self, reason: &str) {
        eprintln!(
            "[drain-probe] DUMP ({reason}) — last {} cycles, oldest first",
            self.len
        );
        let start = (self.idx + RING_LEN - self.len) % RING_LEN;
        for i in 0..self.len {
            let r = &self.ring[(start + i) % RING_LEN];
            let verdict = match r.verdict {
                ReadVerdict::NoDemand => "no-demand".to_string(),
                ReadVerdict::UpToDate { last_seq, cursor } => {
                    format!("up-to-date(last={last_seq} cur={cursor})")
                }
                ReadVerdict::Fed(f) => format!(
                    "fed(start={} end={} last={} skip={:?} more={})",
                    f.start, f.end, f.last_seq, f.lowest_skipped, f.more_pending
                ),
            };
            let rewind = match r.rewind {
                RewindApplied::None => "-".to_string(),
                RewindApplied::Applied { to } => format!("to={to}"),
                RewindApplied::AlreadyBehind { signal } => format!("behind({signal})"),
            };
            eprintln!(
                "[drain-probe] #{i} cur {}->{} v={} rw={} flush ok/bp/gone={}/{}/{} stalled={} parked={}",
                r.cursor_before,
                r.cursor_after,
                verdict,
                rewind,
                r.flushed_ok,
                r.flushed_backpressured,
                r.flushed_gone,
                r.stalled,
                r.parked,
            );
        }
    }
}

impl DrainProbe for ProbeOn {
    fn cycle_start(&mut self, counters: &SharedCounters, rewind: RewindApplied) {
        let cursor = counters.cursor();
        self.cur = CycleRecord {
            cursor_before: cursor,
            cursor_after: cursor,
            rewind,
            ..CycleRecord::default()
        };
    }

    fn read_verdict(&mut self, v: ReadVerdict) {
        self.cur.verdict = v;
    }

    fn flush_ok(&mut self, conn: ConnectionId, first_seq: u64, count: u16) {
        self.cur.flushed_ok = self.cur.flushed_ok.saturating_add(1);
        eprintln!(
            "[FLUSH-OK] conn={} first_seq={} last_seq={} count={}",
            conn.0,
            first_seq,
            first_seq + (count as u64).saturating_sub(1),
            count
        );
    }

    fn flush_backpressured(&mut self, conn: ConnectionId, first_seq: u64, count: u16) {
        self.cur.flushed_backpressured = self.cur.flushed_backpressured.saturating_add(1);
        eprintln!(
            "[FLUSH-BACKPRESSURE] conn={} first_seq={} count={}",
            conn.0, first_seq, count
        );
    }

    fn flush_writer_gone(&mut self, conn: ConnectionId, first_seq: u64, count: u16, not_found: bool) {
        self.cur.flushed_gone = self.cur.flushed_gone.saturating_add(1);
        if not_found {
            eprintln!(
                "[FLUSH-DEAD-WRITER-NOT-FOUND] conn={} first_seq={} count={}",
                conn.0, first_seq, count
            );
        } else {
            eprintln!(
                "[FLUSH-DEAD-WRITE-FAILED] conn={} first_seq={} count={}",
                conn.0, first_seq, count
            );
        }
    }

    fn cursor_advance(&mut self, counters: &SharedCounters, new_cursor: u64, r: &DrainReadResult) {
        self.cur.cursor_after = new_cursor;
        eprintln!(
            "[chaos-debug] cursor {} -> {} (start={} end={} skipped={:?} more={})",
            counters.cursor(),
            new_cursor,
            r.start,
            r.end,
            r.lowest_skipped,
            r.more_pending
        );
    }

    fn park(&mut self, counters: &SharedCounters, v: ParkVerdict, stalled: bool) {
        self.cur.stalled = stalled;
        self.cur.parked = v == ParkVerdict::Park;
        let rec = self.cur;
        self.push(rec);
        if v != ParkVerdict::Park {
            return;
        }
        let cursor_now = counters.cursor();
        // Hard anomaly: parked while the last walk knew of entries beyond
        // the cursor. `close_window` + gate re-open make this structurally
        // unreachable — if it prints, a wake was lost.
        let fed_anomaly =
            matches!(rec.verdict, ReadVerdict::Fed(f) if f.last_seq > cursor_now);
        if fed_anomaly {
            self.dump("PARKED-WITH-TAIL-UNDELIVERED — lost wake?");
            self.last_dump_cursor = Some(cursor_now);
            return;
        }
        // Quiesce-point dump: first park at this cursor while any binding
        // exists. Fires once per stall/quiesce, giving the cycle history
        // leading into it.
        if counters.has_any_demand() && self.last_dump_cursor != Some(cursor_now) {
            self.dump("parked-with-demand");
            self.last_dump_cursor = Some(cursor_now);
        }
    }
}
