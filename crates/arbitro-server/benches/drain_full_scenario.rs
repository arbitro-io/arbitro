//! drain_full_scenario — faithful end-to-end drain bench.
//!
//! Replicates the production drain lifecycle. All variants validate the
//! SAME 9 factors and produce the SAME total emit count (asserted).
//! They differ only in how the walk / grouping / accumulation / flush
//! are organized.
//!
//! Factors validated by every variant (in this order, short-circuit):
//!   1.  has_any_demand        — shard-wide atomic gate
//!   2.  TTL                   — timestamp + max_age_ms <= now_ms
//!   3.  tombstone             — store flag
//!   4.  stream demand         — per-stream atomic counter
//!   5.  stream paused         — per-stream flag
//!   6.  match_table lookup    — exact HashMap + pattern resolve (trie-like)
//!   7.  subject_limit         — stream-wide per-subject cap
//!   8.  per-match:  owner dedup  /  capacity  /  conn alive  /  sub paused
//!   9.  write RepBatch entry  — same wire layout (seq|cons|sub|slen|tlen|subject|payload)
//!
//! Variants compared:
//!   V1  ACCUMULATOR        — prod shape: walk + match + HashMap<(conn,stream)> bucket.
//!                            Frame = one per (conn, stream).
//!   V2  NO-ACCUMULATOR     — walk + match + dense-indexed bucket array keyed by
//!                            a per-cycle assigned slot (still granular per conn,stream).
//!   V3  TWO-PASS           — Pass 1 walk -> Vec<(sid, idx)>; Pass 2 grouped match+emit.
//!   V4  STREAM-BUFFER      — walk + match + one BytesMut per stream (merges conns).
//!                            Same emit count, fewer frames (NOT equivalent semantics,
//!                            measured to quantify how much the per-conn grouping costs).
//!   V5  PER-STREAM-DRAINER — msgs pre-grouped by stream (simulates task-per-stream
//!                            architecture). Per-stream serial match+emit loop.
//!
//! Reports per variant:
//!   - ns/cycle, ns/msg, msgs/s
//!   - emit_count     (asserted equal across V1, V2, V3, V5 — V4 allowed to differ
//!                     in frame shape but NOT entry count)
//!   - frame_count    (informational)
//!
//! Run (testing.md):
//!   cargo bench --bench drain_full_scenario -p arbitro-server --no-run
//!   cp target/release/deps/drain_full_scenario-<hash> /tmp/arbitro/
//!   cd /tmp/arbitro && timeout 120 ./drain_full_scenario-<hash> --bench 2>&1 | tee /tmp/bench.log

#![allow(unused)]

use bytes::BytesMut;
use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

// ── RNG ─────────────────────────────────────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed.max(1)) }
    #[inline]
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        self.0 = x; x
    }
    #[inline] fn range(&mut self, n: u32) -> u32 { (self.next() as u32) % n.max(1) }
    #[inline] fn range_usize(&mut self, n: usize) -> usize { (self.next() as usize) % n.max(1) }
}

// ── Workload shape ──────────────────────────────────────────────────────────

const TOTAL_STREAMS:        usize = 64;
const ACTIVE_STREAMS:       usize = 16;   // fraction of streams receiving msgs
const MSGS_PER_CYCLE:       usize = 256;
const SUBS_PER_STREAM:      usize = 8;    // bindings per stream (consumers × conns)
const PATTERNS_PER_STREAM:  usize = 4;    // wildcard patterns per stream
const SUBJECT_CARDINALITY:  u32   = 10_000;
const TOTAL_CONNS:          usize = 256;
const PAYLOAD_SIZE:         usize = 128;
const INITIAL_CAP:          u16   = 32;
const MAX_AGE_MS:           u64   = 60_000;
const CYCLES:               usize = 5_000;

// ── Types ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct StoreEntry {
    seq:          u64,
    stream_id:    u16,
    subject_hash: u32,
    timestamp:    u64,
    flags:        u8,           // bit 0 = TOMBSTONE
    // Subject/payload live in shared buffers to keep the entry small
    // (matches the real store: subjects are interned, payloads in arena).
}

const FLAG_TOMBSTONE: u8 = 1 << 0;

#[derive(Clone, Copy)]
struct MatchEntry {
    consumer_id:   u32,
    sub_id:        u32,
    connection_id: u32,
    pattern_id:    u32,    // used by pattern_matches (simulates trie walk cost)
    owner:         u8,     // owner_id ∈ [0, 64) for fanout dedup
    paused:        bool,
    // capacity lives in a parallel Vec so V1/V2 can mutate it per-cycle
    // without exclusive borrow of MatchTable.
}

/// Stream metadata accessed per message (TTL, paused, subject-limit table).
struct StreamMeta {
    paused:      bool,
    max_age_ms:  u64,
    /// Subject-limit map: subject_hash -> max entries per cycle for that subject.
    /// Empty when this stream has no subject limits (common case).
    subject_limits: HashMap<u32, u16, foldhash::fast::FixedState>,
}

/// Production-shaped match table: exact hits + pattern fallback.
struct MatchTable {
    /// Fast path: concrete-subject hash -> list of matching entries.
    exact: HashMap<u32, Vec<u16>, foldhash::fast::FixedState>,   // values = entry indices
    /// Wildcard patterns; resolved when `exact` lookup is empty.
    patterns: Vec<(u32, u16)>,  // (pattern_id, entry_idx)
    /// All entries for this stream — referenced by index from `exact` and `patterns`.
    entries: Vec<MatchEntry>,
    /// Has any entry with a subject limit (gates subject-limit lookup).
    has_subject_limits: bool,
}

impl MatchTable {
    /// Mirror `MatchTable::lookup` + `resolve_patterns_readonly` from production.
    /// Fills `out` with matching entry indices. Uses scratch to avoid allocs.
    #[inline]
    fn resolve(&self, subject_hash: u32, out: &mut Vec<u16>) {
        out.clear();
        if let Some(v) = self.exact.get(&subject_hash) {
            out.extend_from_slice(v);
            return;
        }
        // Pattern fallback: walk patterns (simulates trie walk cost).
        for &(pid, idx) in &self.patterns {
            if pattern_matches(subject_hash, pid) {
                out.push(idx);
            }
        }
    }
}

/// Simulated pattern match cost: a handful of ops per check.
#[inline(always)]
fn pattern_matches(subject_hash: u32, pattern_id: u32) -> bool {
    // Cheap deterministic: bucket subjects into pattern_id % N_PATTERNS.
    // ~1/N_PATTERNS of subjects match any given pattern — realistic for
    // wildcard filters.
    (subject_hash.wrapping_mul(0x9E37_79B1) ^ pattern_id).trailing_zeros() >= 3
}

/// Dense-by-connection liveness bitmap (conn_id < TOTAL_CONNS).
struct ConnAlive { alive: Vec<bool> }

/// Shared atomics stand-in: per-stream demand + global demand gate.
struct Counters {
    has_any_demand: bool,
    stream_demand: Vec<bool>,   // TOTAL_STREAMS
}

// ── Wire layout (RepBatch entry) ────────────────────────────────────────────

#[inline(always)]
fn write_entry(buf: &mut BytesMut, seq: u64, cons: u32, sub: u32, subject: &[u8], payload: &[u8]) {
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&cons.to_le_bytes());
    buf.extend_from_slice(&sub.to_le_bytes());
    buf.extend_from_slice(&(subject.len() as u16).to_le_bytes());
    buf.extend_from_slice(&((subject.len() + payload.len()) as u32).to_le_bytes());
    buf.extend_from_slice(subject);
    buf.extend_from_slice(payload);
}

// ── Per-match validation (factors 8a–8d + subject_limit consumption) ────────
//
// This helper is the common core — every variant runs these checks per
// match. Isolated here to prove all variants validate the same factors.
#[inline(always)]
fn check_and_consume(
    m: &MatchEntry,
    caps: &mut [u16],
    entry_idx: u16,
    conns: &ConnAlive,
    seen_mask: &mut u64,
    subj_limit_remaining: &mut Option<u16>,
) -> bool {
    if m.paused { return false; }
    let bit = 1u64 << (m.owner as u64);
    if *seen_mask & bit != 0 { return false; }
    let cap = unsafe { caps.get_unchecked_mut(entry_idx as usize) };
    if *cap == 0 { return false; }
    if !unsafe { *conns.alive.get_unchecked(m.connection_id as usize) } { return false; }
    if let Some(r) = subj_limit_remaining.as_mut() {
        if *r == 0 { return false; }
        *r -= 1;
    }
    *cap -= 1;
    *seen_mask |= bit;
    true
}

// ── Shared scratch (reused across cycles) ───────────────────────────────────

struct Scratch {
    resolved:    Vec<u16>,
    caps:        Vec<Vec<u16>>,     // per stream, per entry_idx
    // V1: accumulator HashMap<(conn,stream)> -> BytesMut
    acc_bucket:  HashMap<(u32, u16), BytesMut, foldhash::fast::FixedState>,
    // V2: dense bucket pool + index map
    v2_buckets:  Vec<BytesMut>,
    v2_index:    HashMap<(u32, u16), u32, foldhash::fast::FixedState>,
    // V3: two-pass buffer
    v3_groups:   Vec<Vec<usize>>,   // stream -> msg indices
    v3_active:   Vec<u16>,
    // V4: one BytesMut per stream
    v4_out:      Vec<BytesMut>,
    v4_touched:  Vec<bool>,
    v4_active:   Vec<u16>,
}

impl Scratch {
    fn new(mts: &[MatchTable]) -> Self {
        Self {
            resolved:   Vec::with_capacity(16),
            caps:       mts.iter().map(|mt| vec![INITIAL_CAP; mt.entries.len()]).collect(),
            acc_bucket: HashMap::with_capacity_and_hasher(64, foldhash::fast::FixedState::default()),
            v2_buckets: Vec::with_capacity(64),
            v2_index:   HashMap::with_capacity_and_hasher(64, foldhash::fast::FixedState::default()),
            v3_groups:  (0..TOTAL_STREAMS).map(|_| Vec::with_capacity(32)).collect(),
            v3_active:  Vec::with_capacity(ACTIVE_STREAMS),
            v4_out:     (0..TOTAL_STREAMS).map(|_| BytesMut::with_capacity(8192)).collect(),
            v4_touched: vec![false; TOTAL_STREAMS],
            v4_active:  Vec::with_capacity(ACTIVE_STREAMS),
        }
    }

    fn reset_caps(&mut self, mts: &[MatchTable]) {
        for (caps, mt) in self.caps.iter_mut().zip(mts.iter()) {
            caps.clear();
            caps.resize(mt.entries.len(), INITIAL_CAP);
        }
    }
}

// ── Variants ────────────────────────────────────────────────────────────────
//
// Every variant takes the same inputs and returns (emit_count, frame_count).
// `process_entry` is the inlined core (factors 2–8); only the bookkeeping
// (where entries go) differs per variant.

macro_rules! preflight {
    ($e:expr, $now:expr, $stream_metas:expr, $counters:expr) => {{
        let entry = $e;
        // Factor 2: TTL
        let sm = unsafe { $stream_metas.get_unchecked(entry.stream_id as usize) };
        if sm.max_age_ms > 0 && entry.timestamp + sm.max_age_ms <= $now { continue; }
        // Factor 3: tombstone
        if entry.flags & FLAG_TOMBSTONE != 0 { continue; }
        // Factor 4: stream demand
        if !unsafe { *$counters.stream_demand.get_unchecked(entry.stream_id as usize) } { continue; }
        // Factor 5: stream paused
        if sm.paused { continue; }
        entry
    }};
}

// V1 — ACCUMULATOR (production shape)
fn run_v1(
    entries: &[StoreEntry], mts: &[MatchTable], stream_metas: &[StreamMeta],
    counters: &Counters, conns: &ConnAlive, scratch: &mut Scratch,
    subject: &[u8], payload: &[u8], now: u64,
) -> (u64, u64) {
    if !counters.has_any_demand { return (0, 0); }
    scratch.acc_bucket.clear();
    scratch.reset_caps(mts);
    let mut emit = 0u64;

    for e in entries {
        let entry = preflight!(*e, now, stream_metas, counters);
        let sid = entry.stream_id as usize;
        let mt = unsafe { mts.get_unchecked(sid) };

        // Factor 6: match_table resolve (exact + patterns).
        mt.resolve(entry.subject_hash, &mut scratch.resolved);
        if scratch.resolved.is_empty() { continue; }

        // Factor 7: subject limit.
        let mut subj_remaining = if mt.has_subject_limits {
            stream_metas[sid].subject_limits.get(&entry.subject_hash).copied()
        } else { None };

        let mut seen_mask = 0u64;
        let caps = unsafe { scratch.caps.get_unchecked_mut(sid) };
        for &ei in &scratch.resolved {
            let m = unsafe { mt.entries.get_unchecked(ei as usize) };
            if !check_and_consume(m, caps, ei, conns, &mut seen_mask, &mut subj_remaining) { continue; }
            // Accumulate into (conn, stream) bucket.
            let buf = scratch.acc_bucket.entry((m.connection_id, entry.stream_id))
                .or_insert_with(|| BytesMut::with_capacity(2048));
            write_entry(buf, entry.seq, m.consumer_id, m.sub_id, subject, payload);
            emit += 1;
        }
    }

    // Flush (count frames, touch bytes).
    let mut frames = 0u64;
    for (_k, buf) in scratch.acc_bucket.iter() {
        black_box(&buf[..]);
        frames += 1;
    }
    (emit, frames)
}

// V2 — NO-ACCUMULATOR (dense bucket pool + (conn,stream) index)
fn run_v2(
    entries: &[StoreEntry], mts: &[MatchTable], stream_metas: &[StreamMeta],
    counters: &Counters, conns: &ConnAlive, scratch: &mut Scratch,
    subject: &[u8], payload: &[u8], now: u64,
) -> (u64, u64) {
    if !counters.has_any_demand { return (0, 0); }
    // Clear reused buckets, reset index.
    for b in scratch.v2_buckets.iter_mut() { b.clear(); }
    scratch.v2_index.clear();
    let mut next_slot: u32 = 0;
    scratch.reset_caps(mts);
    let mut emit = 0u64;

    for e in entries {
        let entry = preflight!(*e, now, stream_metas, counters);
        let sid = entry.stream_id as usize;
        let mt = unsafe { mts.get_unchecked(sid) };

        mt.resolve(entry.subject_hash, &mut scratch.resolved);
        if scratch.resolved.is_empty() { continue; }

        let mut subj_remaining = if mt.has_subject_limits {
            stream_metas[sid].subject_limits.get(&entry.subject_hash).copied()
        } else { None };

        let mut seen_mask = 0u64;
        let caps = unsafe { scratch.caps.get_unchecked_mut(sid) };
        for &ei in &scratch.resolved {
            let m = unsafe { mt.entries.get_unchecked(ei as usize) };
            if !check_and_consume(m, caps, ei, conns, &mut seen_mask, &mut subj_remaining) { continue; }

            let key = (m.connection_id, entry.stream_id);
            let slot = match scratch.v2_index.get(&key) {
                Some(&s) => s,
                None => {
                    let s = next_slot;
                    next_slot += 1;
                    if scratch.v2_buckets.len() <= s as usize {
                        scratch.v2_buckets.push(BytesMut::with_capacity(2048));
                    }
                    scratch.v2_index.insert(key, s);
                    s
                }
            };
            let buf = unsafe { scratch.v2_buckets.get_unchecked_mut(slot as usize) };
            write_entry(buf, entry.seq, m.consumer_id, m.sub_id, subject, payload);
            emit += 1;
        }
    }

    let mut frames = 0u64;
    for i in 0..next_slot as usize {
        black_box(&scratch.v2_buckets[i][..]);
        frames += 1;
    }
    (emit, frames)
}

// V3 — TWO-PASS (Pass 1 bucket msg indices per stream, Pass 2 match+emit)
fn run_v3(
    entries: &[StoreEntry], mts: &[MatchTable], stream_metas: &[StreamMeta],
    counters: &Counters, conns: &ConnAlive, scratch: &mut Scratch,
    subject: &[u8], payload: &[u8], now: u64,
) -> (u64, u64) {
    if !counters.has_any_demand { return (0, 0); }
    for &sid in scratch.v3_active.iter() {
        unsafe { scratch.v3_groups.get_unchecked_mut(sid as usize).clear(); }
    }
    scratch.v3_active.clear();
    scratch.acc_bucket.clear();
    scratch.reset_caps(mts);

    // Pass 1 — preflight + group.
    for (i, e) in entries.iter().enumerate() {
        let entry = preflight!(*e, now, stream_metas, counters);
        let sid = entry.stream_id as usize;
        let g = unsafe { scratch.v3_groups.get_unchecked_mut(sid) };
        if g.is_empty() { scratch.v3_active.push(entry.stream_id); }
        g.push(i);
    }

    // Pass 2 — per active stream: match + accumulate.
    let mut emit = 0u64;
    for &sid_u16 in scratch.v3_active.iter() {
        let sid = sid_u16 as usize;
        let mt = unsafe { mts.get_unchecked(sid) };
        let grp = unsafe { scratch.v3_groups.get_unchecked(sid) };
        let caps = unsafe { scratch.caps.get_unchecked_mut(sid) };
        for &i in grp {
            let entry = unsafe { entries.get_unchecked(i) };
            mt.resolve(entry.subject_hash, &mut scratch.resolved);
            if scratch.resolved.is_empty() { continue; }
            let mut subj_remaining = if mt.has_subject_limits {
                stream_metas[sid].subject_limits.get(&entry.subject_hash).copied()
            } else { None };
            let mut seen_mask = 0u64;
            for &ei in &scratch.resolved {
                let m = unsafe { mt.entries.get_unchecked(ei as usize) };
                if !check_and_consume(m, caps, ei, conns, &mut seen_mask, &mut subj_remaining) { continue; }
                let buf = scratch.acc_bucket.entry((m.connection_id, sid_u16))
                    .or_insert_with(|| BytesMut::with_capacity(2048));
                write_entry(buf, entry.seq, m.consumer_id, m.sub_id, subject, payload);
                emit += 1;
            }
        }
    }

    let mut frames = 0u64;
    for (_k, buf) in scratch.acc_bucket.iter() { black_box(&buf[..]); frames += 1; }
    (emit, frames)
}

// V4 — STREAM-BUFFER (one BytesMut per stream; merges conns per stream)
fn run_v4(
    entries: &[StoreEntry], mts: &[MatchTable], stream_metas: &[StreamMeta],
    counters: &Counters, conns: &ConnAlive, scratch: &mut Scratch,
    subject: &[u8], payload: &[u8], now: u64,
) -> (u64, u64) {
    if !counters.has_any_demand { return (0, 0); }
    for &sid in scratch.v4_active.iter() {
        let s = sid as usize;
        unsafe {
            scratch.v4_out.get_unchecked_mut(s).clear();
            *scratch.v4_touched.get_unchecked_mut(s) = false;
        }
    }
    scratch.v4_active.clear();
    scratch.reset_caps(mts);
    let mut emit = 0u64;

    for e in entries {
        let entry = preflight!(*e, now, stream_metas, counters);
        let sid = entry.stream_id as usize;
        let mt = unsafe { mts.get_unchecked(sid) };

        mt.resolve(entry.subject_hash, &mut scratch.resolved);
        if scratch.resolved.is_empty() { continue; }

        let mut subj_remaining = if mt.has_subject_limits {
            stream_metas[sid].subject_limits.get(&entry.subject_hash).copied()
        } else { None };

        let touched = unsafe { scratch.v4_touched.get_unchecked_mut(sid) };
        if !*touched { scratch.v4_active.push(entry.stream_id); *touched = true; }
        let out = unsafe { scratch.v4_out.get_unchecked_mut(sid) };
        let caps = unsafe { scratch.caps.get_unchecked_mut(sid) };

        let mut seen_mask = 0u64;
        for &ei in &scratch.resolved {
            let m = unsafe { mt.entries.get_unchecked(ei as usize) };
            if !check_and_consume(m, caps, ei, conns, &mut seen_mask, &mut subj_remaining) { continue; }
            write_entry(out, entry.seq, m.consumer_id, m.sub_id, subject, payload);
            emit += 1;
        }
    }

    let mut frames = 0u64;
    for &sid in scratch.v4_active.iter() {
        black_box(&scratch.v4_out[sid as usize][..]);
        frames += 1;
    }
    (emit, frames)
}

// V5 — PER-STREAM-DRAINER (entries pre-grouped by stream; task-per-stream sim)
fn run_v5(
    entries_by_stream: &[Vec<StoreEntry>], mts: &[MatchTable], stream_metas: &[StreamMeta],
    counters: &Counters, conns: &ConnAlive, scratch: &mut Scratch,
    subject: &[u8], payload: &[u8], now: u64,
) -> (u64, u64) {
    if !counters.has_any_demand { return (0, 0); }
    scratch.acc_bucket.clear();
    scratch.reset_caps(mts);
    let mut emit = 0u64;

    for (sid, group) in entries_by_stream.iter().enumerate() {
        if group.is_empty() { continue; }
        // Preflight once per stream (factor 4 & 5).
        if !counters.stream_demand[sid] { continue; }
        let sm = unsafe { stream_metas.get_unchecked(sid) };
        if sm.paused { continue; }
        let mt = unsafe { mts.get_unchecked(sid) };
        let caps = unsafe { scratch.caps.get_unchecked_mut(sid) };

        for entry in group {
            // Factor 2 TTL + 3 tombstone still per-msg.
            if sm.max_age_ms > 0 && entry.timestamp + sm.max_age_ms <= now { continue; }
            if entry.flags & FLAG_TOMBSTONE != 0 { continue; }

            mt.resolve(entry.subject_hash, &mut scratch.resolved);
            if scratch.resolved.is_empty() { continue; }
            let mut subj_remaining = if mt.has_subject_limits {
                sm.subject_limits.get(&entry.subject_hash).copied()
            } else { None };

            let mut seen_mask = 0u64;
            for &ei in &scratch.resolved {
                let m = unsafe { mt.entries.get_unchecked(ei as usize) };
                if !check_and_consume(m, caps, ei, conns, &mut seen_mask, &mut subj_remaining) { continue; }
                let buf = scratch.acc_bucket.entry((m.connection_id, entry.stream_id))
                    .or_insert_with(|| BytesMut::with_capacity(2048));
                write_entry(buf, entry.seq, m.consumer_id, m.sub_id, subject, payload);
                emit += 1;
            }
        }
    }
    let mut frames = 0u64;
    for (_k, buf) in scratch.acc_bucket.iter() { black_box(&buf[..]); frames += 1; }
    (emit, frames)
}

// ── World builder ───────────────────────────────────────────────────────────

fn build_world(rng: &mut Rng) -> (Vec<MatchTable>, Vec<StreamMeta>, Counters, ConnAlive) {
    let mut mts = Vec::with_capacity(TOTAL_STREAMS);
    let mut metas = Vec::with_capacity(TOTAL_STREAMS);

    for _sid in 0..TOTAL_STREAMS {
        let mut entries = Vec::with_capacity(SUBS_PER_STREAM);
        for i in 0..SUBS_PER_STREAM {
            entries.push(MatchEntry {
                consumer_id:   rng.next() as u32,
                sub_id:        rng.next() as u32,
                connection_id: rng.range(TOTAL_CONNS as u32),
                pattern_id:    rng.range(PATTERNS_PER_STREAM as u32),
                owner:         (i as u8) & 0x3F,
                paused:        rng.range(100) < 2,   // 2% paused
            });
        }
        // Build exact map: ~30% of sub subjects get an exact entry; rest
        // fall through to pattern resolve.
        let mut exact: HashMap<u32, Vec<u16>, foldhash::fast::FixedState> =
            HashMap::with_capacity_and_hasher(32, foldhash::fast::FixedState::default());
        for i in 0..SUBS_PER_STREAM {
            if rng.range(100) < 30 {
                let h = rng.range(SUBJECT_CARDINALITY);
                exact.entry(h).or_default().push(i as u16);
            }
        }
        let mut patterns = Vec::with_capacity(PATTERNS_PER_STREAM);
        for i in 0..SUBS_PER_STREAM {
            patterns.push((entries[i].pattern_id, i as u16));
        }

        let has_subject_limits = rng.range(100) < 10;
        let mut subject_limits: HashMap<u32, u16, foldhash::fast::FixedState> =
            HashMap::with_capacity_and_hasher(8, foldhash::fast::FixedState::default());
        if has_subject_limits {
            for _ in 0..4 {
                subject_limits.insert(rng.range(SUBJECT_CARDINALITY), 16);
            }
        }

        mts.push(MatchTable { exact, patterns, entries, has_subject_limits });
        metas.push(StreamMeta {
            paused: rng.range(1000) < 5,   // 0.5% paused
            max_age_ms: MAX_AGE_MS,
            subject_limits,
        });
    }

    let counters = Counters {
        has_any_demand: true,
        stream_demand: (0..TOTAL_STREAMS).map(|_| true).collect(),
    };
    let conns = ConnAlive { alive: vec![true; TOTAL_CONNS] };
    (mts, metas, counters, conns)
}

fn build_cycle(rng: &mut Rng, base_seq: u64, now_ms: u64) -> Vec<StoreEntry> {
    // Mix streams interleaved (walk-by-seq shape); ACTIVE_STREAMS get msgs.
    let mut out = Vec::with_capacity(MSGS_PER_CYCLE);
    for i in 0..MSGS_PER_CYCLE {
        let sid = (rng.range_usize(ACTIVE_STREAMS)) as u16;
        out.push(StoreEntry {
            seq:          base_seq + i as u64,
            stream_id:    sid,
            subject_hash: rng.range(SUBJECT_CARDINALITY),
            timestamp:    now_ms.saturating_sub(rng.range(30_000) as u64),
            flags:        if rng.range(1000) < 5 { FLAG_TOMBSTONE } else { 0 },
        });
    }
    out
}

fn group_by_stream(entries: &[StoreEntry]) -> Vec<Vec<StoreEntry>> {
    let mut out: Vec<Vec<StoreEntry>> = (0..TOTAL_STREAMS).map(|_| Vec::new()).collect();
    for e in entries { out[e.stream_id as usize].push(*e); }
    out
}

// ── Runner ──────────────────────────────────────────────────────────────────

fn main() {
    let mut rng = Rng::new(0xBADC0FFEE);
    let (mts, metas, counters, conns) = build_world(&mut rng);

    let subject: Vec<u8> = b"message.meta.premium.user_1212".to_vec();
    let payload: Vec<u8> = vec![0xAB; PAYLOAD_SIZE];
    let now_ms = 1_000_000_u64;

    // Pre-build all cycles (seq interleaved) + per-stream groups for V5.
    let cycles: Vec<Vec<StoreEntry>> = (0..CYCLES)
        .map(|c| build_cycle(&mut rng, (c as u64) * MSGS_PER_CYCLE as u64, now_ms))
        .collect();
    let cycles_grouped: Vec<Vec<Vec<StoreEntry>>> =
        cycles.iter().map(|c| group_by_stream(c)).collect();

    let mut scratch = Scratch::new(&mts);

    // Warmup
    for c in cycles.iter().take(20) {
        let _ = run_v1(c, &mts, &metas, &counters, &conns, &mut scratch, &subject, &payload, now_ms);
    }

    // Measure each variant.
    macro_rules! measure {
        ($name:expr, $fn:expr) => {{
            let start = Instant::now();
            let mut emit_total = 0u64;
            let mut frame_total = 0u64;
            for c in cycles.iter() {
                let (e, f) = $fn(c);
                emit_total += e;
                frame_total += f;
            }
            let ns = start.elapsed().as_nanos() as f64 / CYCLES as f64;
            ($name, ns, emit_total, frame_total)
        }};
    }

    let r1 = measure!("V1  ACCUMULATOR    (prod shape: HashMap<(conn,stream)>)",
        |c: &Vec<StoreEntry>| run_v1(c, &mts, &metas, &counters, &conns, &mut scratch, &subject, &payload, now_ms));
    let r2 = measure!("V2  NO-ACCUMULATOR (dense bucket pool + index)         ",
        |c: &Vec<StoreEntry>| run_v2(c, &mts, &metas, &counters, &conns, &mut scratch, &subject, &payload, now_ms));
    let r3 = measure!("V3  TWO-PASS       (group-by-stream, then match+emit)  ",
        |c: &Vec<StoreEntry>| run_v3(c, &mts, &metas, &counters, &conns, &mut scratch, &subject, &payload, now_ms));
    let r4 = measure!("V4  STREAM-BUFFER  (one BytesMut per stream — fewer frames)",
        |c: &Vec<StoreEntry>| run_v4(c, &mts, &metas, &counters, &conns, &mut scratch, &subject, &payload, now_ms));

    // V5 uses the pre-grouped cycles.
    let start = Instant::now();
    let mut e5 = 0u64; let mut f5 = 0u64;
    for cg in cycles_grouped.iter() {
        let (e, f) = run_v5(cg, &mts, &metas, &counters, &conns, &mut scratch, &subject, &payload, now_ms);
        e5 += e; f5 += f;
    }
    let ns5 = start.elapsed().as_nanos() as f64 / CYCLES as f64;
    let r5 = ("V5  PER-STREAM     (pre-grouped, simulates task-per-stream) ", ns5, e5, f5);

    // ── Correctness: V1/V2/V3/V5 must emit same count (same per-(conn,stream)
    // semantics). V4 may differ in frame count but NOT in entry count.
    assert_eq!(r1.2, r2.2, "emit mismatch V1 vs V2");
    assert_eq!(r1.2, r3.2, "emit mismatch V1 vs V3");
    assert_eq!(r1.2, r4.2, "emit mismatch V1 vs V4");
    assert_eq!(r1.2, r5.2, "emit mismatch V1 vs V5");

    // ── Report ──────────────────────────────────────────────────────────
    println!();
    println!("drain_full_scenario — end-to-end bench (all variants validate same 9 factors)");
    println!("================================================================================");
    println!("streams={TOTAL_STREAMS}  active={ACTIVE_STREAMS}  msgs/cycle={MSGS_PER_CYCLE}");
    println!("subs/stream={SUBS_PER_STREAM}  patterns/stream={PATTERNS_PER_STREAM}");
    println!("subjects={SUBJECT_CARDINALITY}  conns={TOTAL_CONNS}  cycles={CYCLES}");
    println!("payload={PAYLOAD_SIZE}B  initial_cap={INITIAL_CAP}  max_age_ms={MAX_AGE_MS}");
    println!();
    println!("{:<60} | {:>12} | {:>12} | {:>10} | {:>12} | {:>10}",
             "Variant", "ns/cycle", "ns/msg", "msgs/s", "emit (total)", "frames");
    println!("{}", "-".repeat(128));
    for (name, ns, emit, frames) in [r1, r2, r3, r4, r5] {
        let per_msg = ns / MSGS_PER_CYCLE as f64;
        let mps = (MSGS_PER_CYCLE as f64 * 1e9 / ns) / 1e6;
        println!("{:<60} | {:>9.0} ns | {:>9.2} ns | {:>6.2} M/s | {:>12} | {:>10}",
                 name, ns, per_msg, mps, emit, frames);
    }
    println!();
    let base = r1.1;
    println!("Delta vs V1 (ACCUMULATOR, prod shape):");
    for (name, ns, _, _) in [r2, r3, r4, r5] {
        let delta = base / ns;
        let pct = (delta - 1.0) * 100.0;
        println!("  {:<60}  {:>6.2}×  ({:+.1}%)", name, delta, pct);
    }
    println!();
    println!("Note: V4 (STREAM-BUFFER) merges per-conn granularity into per-stream frames.");
    println!("      Same emit count but frames are fewer/bigger — NOT semantically equivalent");
    println!("      to production (which must send 1 RepBatch per (conn,stream)). Included to");
    println!("      quantify the cost of the per-conn bucket structure itself.");
    println!();
}
