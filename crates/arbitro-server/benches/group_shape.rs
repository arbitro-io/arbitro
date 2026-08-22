//! TEMPORARY. A drain cycle, end to end, in four shapes.
//!
//! Starts where `Staged::fill` hands the window over and ends where the
//! frame bytes are appended — the same span `drain_profile` calls
//! `dispatch`, which is 75% of a cycle under `AckPolicy::Explicit`.
//!
//! Everything the real cycle does per entry and per recipient is here, in
//! the same order as `drain.rs`:
//!
//!   per ENTRY   TTL, tombstone, birth_seq, has_demand, match-table lookup,
//!               pattern merge with its linear dedup
//!   per MATCH   conn!=0, suppressed, queue dedup, dead, unbound, collapse,
//!               deliver_floor, write_failed, paused, capacity (linear
//!               local-delta scan THEN the atomic), subject limit
//!   then        the HAS_HEADERS and HAS_REPLY_TO parses, and the payload
//!               copy into the frame buffer
//!
//! Shapes:
//!   flat      today, verbatim
//!   grouped   one pass builds consumer -> [entry], capacity checked ONCE
//!             per consumer, and `pending` becomes a loop local — which is
//!             what removes the per-recipient linear scan
//!   subjdedup flat, but the match set is resolved once per DISTINCT
//!             subject in the window instead of once per entry
//!   lean      grouped + subject dedup + the two payload parses hoisted out
//!             of the recipient loop (they depend only on the entry, and
//!             are redone for all 9 recipients today)
//!
//! Delete once a shape is chosen.

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

const FEED: usize = 256;
const RECIPIENTS: usize = 9;
const CONSUMERS: usize = 9;
const CYCLES: usize = 4_000;
const MAX_INFLIGHT: u32 = 4096;
/// Distinct subjects in a window. The fanout bench publishes to few; a
/// broad stream would have many. Swept in main().
const PAYLOAD: usize = 64;

type Fold = foldhash::fast::FixedState;

#[derive(Clone, Copy)]
struct Match {
    consumer: u32,
    binding: u32,
    queue: u32,
    group_idx: u32,
}

/// What `Staged` hands over: metadata plus a slice of the copied bytes.
#[derive(Clone, Copy)]
struct Entry {
    seq: u64,
    stream_id: u32,
    ts: u64,
    flags: u8,
    subject_hash: u32,
    off: u32,
    subj_len: u32,
    pay_len: u32,
}

/// `ActiveBinding`'s hot fields.
struct Binding {
    connection_id: u32,
    deliver_floor: u64,
    max_inflight: u32,
    fire_and_forget: bool,
    write_failed: bool,
    group_idx: u32,
}

struct Counters {
    consumer: Vec<u32>,
    demand: Vec<u32>,
    paused: Vec<bool>,
}

impl Counters {
    fn new(blocked: usize) -> Self {
        Self {
            consumer: (0..CONSUMERS)
                .map(|i| if i < blocked { MAX_INFLIGHT } else { 0 })
                .collect(),
            demand: vec![1; STREAMS],
            paused: vec![false; CONSUMERS],
        }
    }
    #[inline]
    fn has_demand(&self, s: u32) -> bool {
        self.demand[s as usize] > 0
    }
    #[inline]
    fn is_paused(&self, c: u32) -> bool {
        self.paused[c as usize]
    }
    #[inline]
    fn has_capacity(&self, id: u32, max: u32) -> bool {
        self.consumer[id as usize] < max
    }
}

/// Stream id space, matching `SharedCounters::SLOT_COUNT`.
const STREAMS: usize = 4096;

/// The window as `Staged` leaves it, plus the snapshot tables the drain
/// reads. `match_tables` is indexed by stream_id and holds a per-subject
/// map — the same two-level shape as `snap.match_tables[stream]` followed
/// by `mt.lookup_verified(subject_hash, ..)`.
struct Window {
    bytes: Vec<u8>,
    entries: Vec<Entry>,
    /// Indexed by stream_id. `None` = no such stream.
    match_tables: Vec<Option<HashMap<u32, Vec<Match>, Fold>>>,
    /// The three other per-stream lookups the drain does per entry.
    stream_max_age_ms: Vec<u64>,
    stream_created_at_seq: Vec<u64>,
}

fn build_window(distinct_subjects: usize, distinct_streams: usize) -> Window {
    let mut bytes = Vec::with_capacity(FEED * (16 + PAYLOAD));
    let mut entries = Vec::with_capacity(FEED);
    let mut match_tables: Vec<Option<HashMap<u32, Vec<Match>, Fold>>> =
        (0..STREAMS).map(|_| None).collect();

    for e in 0..FEED {
        // Interleaved, as entries arrive in seq order from mixed streams.
        let stream_id = (e % distinct_streams) as u32 + 1;
        // FIVE tokens. A 3-token 17-byte subject made `lookup_verified`'s
        // memcmp almost free, which is not what real subject trees look
        // like — and the memcmp is the per-entry cost that grouping cannot
        // remove, so understating it flatters every grouped shape.
        let subj = format!(
            "orders.eu-west.tenant{}.premium.created",
            e % distinct_subjects
        );
        let hash = subj
            .bytes()
            .fold(2166136261u32, |h, b| (h ^ b as u32).wrapping_mul(16777619));
        let off = bytes.len() as u32;
        bytes.extend_from_slice(subj.as_bytes());
        bytes.extend_from_slice(&vec![0xABu8; PAYLOAD]);
        entries.push(Entry {
            seq: e as u64 + 1,
            stream_id,
            ts: 1_700_000_000_000,
            flags: 0,
            subject_hash: hash,
            off,
            subj_len: subj.len() as u32,
            pay_len: PAYLOAD as u32,
        });
        match_tables[stream_id as usize]
            .get_or_insert_with(HashMap::default)
            .entry(hash)
            .or_insert_with(|| {
                (0..RECIPIENTS)
                    .map(|r| Match {
                        consumer: (r % CONSUMERS) as u32,
                        binding: r as u32,
                        queue: 0,
                        group_idx: r as u32,
                    })
                    .collect()
            });
    }
    Window {
        bytes,
        entries,
        match_tables,
        stream_max_age_ms: vec![0; STREAMS],
        stream_created_at_seq: vec![0; STREAMS],
    }
}

fn bindings() -> Vec<Binding> {
    (0..RECIPIENTS)
        .map(|r| Binding {
            connection_id: (r % 3) as u32 + 1,
            deliver_floor: 0,
            max_inflight: MAX_INFLIGHT,
            fire_and_forget: false,
            write_failed: false,
            group_idx: r as u32,
        })
        .collect()
}

// ── shared pieces, so every shape pays the same for the same work ───────────

#[inline]
fn local_get(list: &[(u32, u32)], key: u32) -> u32 {
    for &(k, v) in list.iter() {
        if k == key {
            return v;
        }
    }
    0
}

#[inline]
fn local_inc(list: &mut Vec<(u32, u32)>, key: u32) {
    for e in list.iter_mut() {
        if e.0 == key {
            e.1 += 1;
            return;
        }
    }
    list.push((key, 1));
}

/// The FOUR per-stream lookups the drain does for every entry today, plus
/// the two entry-local checks. Returns the stream's match table, or None to
/// skip. This is what grouping by stream would hoist to once per stream.
#[inline]
fn stream_gate<'a>(
    w: &'a Window,
    stream_id: u32,
    c: &Counters,
) -> Option<&'a HashMap<u32, Vec<Match>, Fold>> {
    let _max_age = w.stream_max_age_ms[stream_id as usize]; // 1
    let _birth = w.stream_created_at_seq[stream_id as usize]; // 2
    if !c.has_demand(stream_id) {
        // 3
        return None;
    }
    w.match_tables[stream_id as usize].as_ref() // 4
}

/// `mt.lookup_verified(subject_hash, subject)` — the hash finds the bucket,
/// then the LITERAL BYTES are compared, because a 32-bit hash collides and a
/// collision misdelivers (SEC-5). Modelling only the hash understated every
/// shape equally, which compresses the measured spread between them.
#[inline]
fn lookup_verified<'a>(
    mt: &'a HashMap<u32, Vec<Match>, Fold>,
    subject_hash: u32,
    subject: &[u8],
    stored: &[u8],
) -> Option<&'a Vec<Match>> {
    let set = mt.get(&subject_hash)?;
    // The memcmp the real table pays on every hit.
    if subject != stored {
        return None;
    }
    Some(set)
}

/// Entry-local checks — TTL and tombstone. Cannot be hoisted.
#[inline]
fn entry_ok(e: &Entry, now_ms: u64, max_age_ms: u64) -> bool {
    if max_age_ms > 0 && e.ts > 0 && e.ts + max_age_ms <= now_ms {
        return false;
    }
    e.flags & 0x01 == 0
}

/// The two nested payload parses, verbatim in shape.
#[inline]
fn parse_payload<'a>(bytes: &'a [u8], e: &Entry) -> (&'a [u8], &'a [u8]) {
    let start = (e.off + e.subj_len) as usize;
    let raw = &bytes[start..start + e.pay_len as usize];
    let raw = if e.flags & 0x02 != 0 && raw.len() >= 4 {
        let n = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
        if raw.len() >= 4 + n {
            &raw[4..4 + n]
        } else {
            raw
        }
    } else {
        raw
    };
    if e.flags & 0x04 != 0 && raw.len() >= 2 {
        let n = u16::from_le_bytes([raw[0], raw[1]]) as usize;
        if raw.len() >= 2 + n {
            (&raw[2..2 + n], &raw[2 + n..])
        } else {
            (&[], raw)
        }
    } else {
        (&[], raw)
    }
}

/// The frame append — a memcpy, the same bytes in every shape.
#[inline]
fn emit(frame: &mut Vec<u8>, subject: &[u8], reply: &[u8], payload: &[u8]) {
    frame.extend_from_slice(subject);
    frame.extend_from_slice(reply);
    frame.extend_from_slice(payload);
}

struct Scratch {
    matches: Vec<Match>,
    served_queues: Vec<u32>,
    fanout_stamp: Vec<u64>,
    fanout_gen: u64,
    local_inflight: Vec<(u32, u32)>,
    frame: Vec<u8>,
    groups: Vec<Vec<u32>>,
    resolved: HashMap<u32, usize, Fold>,
    /// Stream grouping. Allocated ONCE at shard start and only ever
    /// cleared — never rebuilt, never rehashed. Indexed by stream_id like
    /// `SharedCounters`, not a map: clearing a `HashMap` either drops the
    /// inner Vecs (re-allocating next cycle) or leaves dead keys that
    /// `values_mut()` walks forever as a shard sees more streams.
    /// `streams_seen` says which slots are live this cycle, so the reset is
    /// O(streams in the window), not O(SLOT_COUNT).
    streams_seen: Vec<u32>,
    by_stream: Vec<Vec<u32>>,
}

impl Scratch {
    fn new() -> Self {
        Self {
            matches: Vec::with_capacity(RECIPIENTS * 2),
            served_queues: Vec::with_capacity(4),
            fanout_stamp: vec![0; RECIPIENTS],
            fanout_gen: 0,
            local_inflight: Vec::with_capacity(CONSUMERS),
            frame: Vec::with_capacity(FEED * RECIPIENTS * (PAYLOAD + 32)),
            groups: (0..CONSUMERS).map(|_| Vec::with_capacity(FEED)).collect(),
            resolved: HashMap::default(),
            streams_seen: Vec::with_capacity(FEED),
            by_stream: (0..STREAMS).map(|_| Vec::new()).collect(),
        }
    }
}

// ── shape 1: flat — the drain as it is today ────────────────────────────────

fn run_flat(w: &Window, b: &[Binding], c: &Counters, s: &mut Scratch) -> u64 {
    let mut emitted = 0u64;
    for _ in 0..CYCLES {
        s.local_inflight.clear();
        s.frame.clear();
        for e in &w.entries {
            if !entry_ok(e, 1_700_000_100_000, 0) {
                continue;
            }
            // The four per-stream lookups, redone for EVERY entry.
            let Some(mt) = stream_gate(w, e.stream_id, c) else {
                continue;
            };
            // Per-ENTRY match resolution: copy the literals, then merge the
            // pattern set with a linear dedup.
            s.matches.clear();
            let subj = &w.bytes[e.off as usize..(e.off + e.subj_len) as usize];
            let Some(set) = lookup_verified(mt, e.subject_hash, subj, subj) else {
                continue;
            };
            s.matches.extend(set.iter().copied());
            for m in set.iter() {
                if !s.matches.iter().any(|x| x.binding == m.binding) {
                    s.matches.push(*m);
                }
            }
            s.served_queues.clear();
            s.fanout_gen += 1;
            let n = s.matches.len();
            let start = (e.seq as usize) % n;
            for i in 0..n {
                let idx = if start + i >= n { start + i - n } else { start + i };
                let m = s.matches[idx];
                let bind = &b[m.binding as usize];
                if bind.connection_id == 0 {
                    continue;
                }
                if s.served_queues.contains(&m.queue) && m.queue != 0 {
                    continue;
                }
                if bind.write_failed {
                    continue;
                }
                let collapsed = s.fanout_stamp[bind.group_idx as usize] == s.fanout_gen;
                if collapsed && bind.fire_and_forget {
                    continue;
                }
                if e.seq <= bind.deliver_floor {
                    continue;
                }
                if c.is_paused(m.consumer) {
                    continue;
                }
                if !bind.fire_and_forget {
                    let pending = local_get(&s.local_inflight, m.consumer);
                    if pending >= bind.max_inflight
                        || !c.has_capacity(m.consumer, bind.max_inflight - pending)
                    {
                        continue;
                    }
                }
                // Parses run INSIDE the recipient loop, once per recipient.
                let (reply, payload) = parse_payload(&w.bytes, e);
                let subject =
                    &w.bytes[e.off as usize..(e.off + e.subj_len) as usize];
                if !collapsed {
                    emit(&mut s.frame, subject, reply, payload);
                    emitted += 1;
                }
                local_inc(&mut s.local_inflight, m.consumer);
                s.fanout_stamp[bind.group_idx as usize] = s.fanout_gen;
            }
        }
        black_box(s.frame.len());
    }
    emitted
}

// ── shape 2: grouped — consumer is the index, capacity checked once ─────────

fn run_grouped(w: &Window, b: &[Binding], c: &Counters, s: &mut Scratch) -> u64 {
    let mut emitted = 0u64;
    for _ in 0..CYCLES {
        s.frame.clear();
        for g in s.groups.iter_mut() {
            g.clear();
        }
        // Pass 1 — admit and group. The extra pass grouping costs.
        for (ei, e) in w.entries.iter().enumerate() {
            if !entry_ok(e, 1_700_000_100_000, 0) {
                continue;
            }
            let Some(mt) = stream_gate(w, e.stream_id, c) else {
                continue;
            };
            let subj = &w.bytes[e.off as usize..(e.off + e.subj_len) as usize];
            let Some(set) = lookup_verified(mt, e.subject_hash, subj, subj) else {
                continue;
            };
            for m in set {
                s.groups[m.consumer as usize].push(ei as u32);
            }
        }
        // Pass 2 — ONE capacity check per consumer, then its whole set.
        for (cid, g) in s.groups.iter().enumerate() {
            if g.is_empty() || c.is_paused(cid as u32) {
                continue;
            }
            if !c.has_capacity(cid as u32, MAX_INFLIGHT) {
                continue; // whole group drops on one check
            }
            // `pending` is a loop local — no linear scan anywhere.
            let mut pending = 0u32;
            for &ei in g.iter() {
                if pending >= MAX_INFLIGHT {
                    break;
                }
                let e = &w.entries[ei as usize];
                let bind = &b[(ei as usize) % RECIPIENTS];
                if bind.write_failed || e.seq <= bind.deliver_floor {
                    continue;
                }
                let (reply, payload) = parse_payload(&w.bytes, e);
                let subject =
                    &w.bytes[e.off as usize..(e.off + e.subj_len) as usize];
                emit(&mut s.frame, subject, reply, payload);
                pending += 1;
                emitted += 1;
            }
        }
        black_box(s.frame.len());
    }
    emitted
}

// ── shape 3: flat, but resolve the match set once per DISTINCT subject ──────

fn run_subjdedup(w: &Window, b: &[Binding], c: &Counters, s: &mut Scratch) -> u64 {
    let mut emitted = 0u64;
    for _ in 0..CYCLES {
        s.local_inflight.clear();
        s.frame.clear();
        s.resolved.clear();
        // Group by STREAM first, so the four per-stream lookups run once
        // per stream instead of once per entry.
        for g in s.groups.iter_mut() {
            g.clear();
        }
        // Reused, not rebuilt: only the per-stream lists are cleared.
        // O(streams in this window), not O(SLOT_COUNT).
        for &sid in s.streams_seen.iter() {
            s.by_stream[sid as usize].clear();
        }
        s.streams_seen.clear();
        for (ei, e) in w.entries.iter().enumerate() {
            if !entry_ok(e, 1_700_000_100_000, 0) {
                continue;
            }
            let v = &mut s.by_stream[e.stream_id as usize];
            if v.is_empty() {
                s.streams_seen.push(e.stream_id);
            }
            v.push(ei as u32);
        }
        for si in 0..s.streams_seen.len() {
            let sid = s.streams_seen[si];
            let Some(mt) = stream_gate(w, sid, c) else {
                continue;
            };
            for gi in 0..s.by_stream[sid as usize].len() {
                let ei = s.by_stream[sid as usize][gi];
                let e = &w.entries[ei as usize];
                let subj = &w.bytes[e.off as usize..(e.off + e.subj_len) as usize];
                let Some(set) = lookup_verified(mt, e.subject_hash, subj, subj) else {
                    continue;
                };
            s.served_queues.clear();
            s.fanout_gen += 1;
            for m in set.iter() {
                let bind = &b[m.binding as usize];
                if bind.connection_id == 0 || bind.write_failed {
                    continue;
                }
                if e.seq <= bind.deliver_floor || c.is_paused(m.consumer) {
                    continue;
                }
                let pending = local_get(&s.local_inflight, m.consumer);
                if pending >= bind.max_inflight
                    || !c.has_capacity(m.consumer, bind.max_inflight - pending)
                {
                    continue;
                }
                let (reply, payload) = parse_payload(&w.bytes, e);
                let subject =
                    &w.bytes[e.off as usize..(e.off + e.subj_len) as usize];
                emit(&mut s.frame, subject, reply, payload);
                local_inc(&mut s.local_inflight, m.consumer);
                emitted += 1;
            }
            }
        }
        black_box(s.frame.len());
    }
    emitted
}

// ── shape 4: lean — grouped + subject dedup + parses hoisted ────────────────

fn run_lean(w: &Window, b: &[Binding], c: &Counters, s: &mut Scratch) -> u64 {
    let mut emitted = 0u64;
    for _ in 0..CYCLES {
        s.frame.clear();
        for g in s.groups.iter_mut() {
            g.clear();
        }
        // Full tree: stream -> subject -> consumer. The per-stream lookups
        // run once per stream, and the subject resolve once per (stream,
        // subject) — both are per-ENTRY today.
        s.resolved.clear();
        // Reused, not rebuilt: only the per-stream lists are cleared.
        // O(streams in this window), not O(SLOT_COUNT).
        for &sid in s.streams_seen.iter() {
            s.by_stream[sid as usize].clear();
        }
        s.streams_seen.clear();
        for (ei, e) in w.entries.iter().enumerate() {
            if !entry_ok(e, 1_700_000_100_000, 0) {
                continue;
            }
            let v = &mut s.by_stream[e.stream_id as usize];
            if v.is_empty() {
                s.streams_seen.push(e.stream_id);
            }
            v.push(ei as u32);
        }
        for si in 0..s.streams_seen.len() {
            let sid = s.streams_seen[si];
            let Some(mt) = stream_gate(w, sid, c) else {
                continue;
            };
            for gi in 0..s.by_stream[sid as usize].len() {
                let ei = s.by_stream[sid as usize][gi];
                let e = &w.entries[ei as usize];
                // One resolve per (stream, subject), not per entry.
                let subj = &w.bytes[e.off as usize..(e.off + e.subj_len) as usize];
                let set = match s.resolved.get(&e.subject_hash) {
                    // Already verified this subject in this cycle.
                    Some(_) => match mt.get(&e.subject_hash) {
                        Some(v) => v,
                        None => continue,
                    },
                    None => {
                        let Some(set) = lookup_verified(mt, e.subject_hash, subj, subj)
                        else {
                            continue;
                        };
                        s.resolved.insert(e.subject_hash, set.len());
                        set
                    }
                };
                for m in set.iter() {
                    s.groups[m.consumer as usize].push(ei as u32);
                }
            }
        }
        for (cid, g) in s.groups.iter().enumerate() {
            if g.is_empty() || c.is_paused(cid as u32) {
                continue;
            }
            if !c.has_capacity(cid as u32, MAX_INFLIGHT) {
                continue;
            }
            let mut pending = 0u32;
            let mut last_entry = u32::MAX;
            let mut cached: (&[u8], &[u8], &[u8]) = (&[], &[], &[]);
            for &ei in g.iter() {
                if pending >= MAX_INFLIGHT {
                    break;
                }
                let e = &w.entries[ei as usize];
                let bind = &b[(ei as usize) % RECIPIENTS];
                if bind.write_failed || e.seq <= bind.deliver_floor {
                    continue;
                }
                // The parses depend only on the ENTRY. Recomputing them per
                // recipient is the same bytes, nine times.
                if ei != last_entry {
                    let (reply, payload) = parse_payload(&w.bytes, e);
                    let subject =
                        &w.bytes[e.off as usize..(e.off + e.subj_len) as usize];
                    cached = (subject, reply, payload);
                    last_entry = ei;
                }
                emit(&mut s.frame, cached.0, cached.1, cached.2);
                pending += 1;
                emitted += 1;
            }
        }
        black_box(s.frame.len());
    }
    emitted
}

// ════════════════════════════════════════════════════════════════════════
// Section 2 — how to group a window by stream_id
//
// A window of FEED entries can hold at most FEED distinct stream ids, so a
// batch-sized structure is always enough. The question is whether that beats
// a hash map, and whether pre-allocating beats building one per cycle.
//
// Grouping by stream is the FIRST level: the drain does four lookups keyed
// by stream_id for every entry — `stream_max_age_ms`, `stream_created_at_seq`,
// `has_demand`, `match_tables` — and grouped they collapse to four per
// STREAM. `EntryLoc` already carries `stream_id`, so nothing extra has to be
// read to do it.
// ════════════════════════════════════════════════════════════════════════

/// Matches `SharedCounters::SLOT_COUNT` — the id space arrays are sized to.
const SLOT_COUNT: usize = 4096;
/// Sentinel for "no entry yet" in the head array.
const NONE: u32 = u32::MAX;

/// a) A fresh `HashMap` every cycle. Allocates.
fn group_map_fresh(streams: &[u32]) -> u64 {
    let mut sum = 0u64;
    for _ in 0..CYCLES {
        let mut m: HashMap<u32, Vec<u32>, Fold> = HashMap::default();
        for (i, &s) in streams.iter().enumerate() {
            m.entry(s).or_default().push(i as u32);
        }
        for (k, v) in m.iter() {
            sum += *k as u64 + v.len() as u64;
        }
    }
    sum
}

/// b) One `HashMap`, cleared and reused. No allocation, still hashes.
fn group_map_reused(streams: &[u32]) -> u64 {
    let mut sum = 0u64;
    let mut m: HashMap<u32, Vec<u32>, Fold> = HashMap::default();
    for _ in 0..CYCLES {
        for v in m.values_mut() {
            v.clear();
        }
        for (i, &s) in streams.iter().enumerate() {
            m.entry(s).or_default().push(i as u32);
        }
        for (k, v) in m.iter() {
            sum += *k as u64 + v.len() as u64;
        }
    }
    sum
}

/// c) Pre-allocated by ID SPACE: head array indexed by stream_id, entries
/// chained through a batch-sized `next`. O(1) insert, ~33 KB per shard.
/// A generation stamp replaces clearing.
struct Slots {
    head: Vec<u32>,
    gen: Vec<u32>,
    next: Vec<u32>,
    seen: Vec<u32>,
    cur: u32,
}

impl Slots {
    fn new() -> Self {
        Self {
            head: vec![NONE; SLOT_COUNT],
            gen: vec![0; SLOT_COUNT],
            next: vec![NONE; FEED],
            seen: Vec::with_capacity(FEED),
            cur: 0,
        }
    }
}

fn group_slots(streams: &[u32], s: &mut Slots) -> u64 {
    let mut sum = 0u64;
    for _ in 0..CYCLES {
        s.cur += 1;
        s.seen.clear();
        for (i, &sid) in streams.iter().enumerate() {
            let idx = sid as usize;
            if s.gen[idx] != s.cur {
                s.gen[idx] = s.cur;
                s.head[idx] = NONE;
                s.seen.push(sid);
            }
            s.next[i] = s.head[idx];
            s.head[idx] = i as u32;
        }
        for &sid in s.seen.iter() {
            let mut n = 0u64;
            let mut cur = s.head[sid as usize];
            while cur != NONE {
                n += 1;
                cur = s.next[cur as usize];
            }
            sum += sid as u64 + n;
        }
    }
    sum
}

/// d) Pre-allocated by BATCH SIZE: a distinct-stream list scanned linearly,
/// plus counting-sort placement. ~4 KB per shard, and the scan is over the
/// number of DISTINCT streams in the window — typically 1..4.
struct BatchGroups {
    stream_of: Vec<u32>,
    count: Vec<u32>,
    start: Vec<u32>,
    entry_idx: Vec<u32>,
}

impl BatchGroups {
    fn new() -> Self {
        Self {
            stream_of: vec![0; FEED],
            count: vec![0; FEED],
            start: vec![0; FEED],
            entry_idx: vec![0; FEED],
        }
    }
}

fn group_batch(streams: &[u32], g: &mut BatchGroups) -> u64 {
    let mut sum = 0u64;
    for _ in 0..CYCLES {
        // Pass 1 — distinct streams + counts. Linear scan over the groups
        // found so far, which is the distinct-stream count, not FEED.
        let mut n = 0usize;
        for &sid in streams.iter() {
            let mut found = usize::MAX;
            for k in 0..n {
                if g.stream_of[k] == sid {
                    found = k;
                    break;
                }
            }
            if found == usize::MAX {
                g.stream_of[n] = sid;
                g.count[n] = 1;
                n += 1;
            } else {
                g.count[found] += 1;
            }
        }
        // Prefix sums.
        let mut acc = 0u32;
        for k in 0..n {
            g.start[k] = acc;
            acc += g.count[k];
        }
        // Pass 2 — place. Same linear scan.
        let mut cursor = g.start[..n].to_vec();
        for (i, &sid) in streams.iter().enumerate() {
            for k in 0..n {
                if g.stream_of[k] == sid {
                    g.entry_idx[cursor[k] as usize] = i as u32;
                    cursor[k] += 1;
                    break;
                }
            }
        }
        for k in 0..n {
            sum += g.stream_of[k] as u64 + g.count[k] as u64;
        }
    }
    sum
}

fn section_grouping() {
    println!("\n\n╔══ Sección 2 — agrupar {FEED} entradas por stream_id ══╗\n");
    println!("  memoria fija: map=dinámica  slots={} KB  batch={} KB",
        (SLOT_COUNT * 8 + FEED * 4) / 1024,
        (FEED * 16) / 1024);
    for distinct in [1usize, 4, 16, 64, 256] {
        // Interleaved, as entries arrive in seq order from mixed streams.
        let streams: Vec<u32> = (0..FEED).map(|i| (i % distinct) as u32).collect();
        println!("\n── {distinct} stream(s) distinto(s) en la ventana ──");
        let t = |name: &str, f: &mut dyn FnMut() -> u64| {
            let t0 = Instant::now();
            let out = black_box(f());
            let ns = t0.elapsed().as_nanos() as f64;
            println!(
                "  {name:<14} {:>8.1} ms  {:>9.1} ns/ciclo  {:>6.1} ns/entrada  (chk {out})",
                ns / 1e6,
                ns / CYCLES as f64,
                ns / (CYCLES * FEED) as f64,
            );
        };
        t("map_fresh", &mut || group_map_fresh(&streams));
        t("map_reused", &mut || group_map_reused(&streams));
        let mut s = Slots::new();
        t("slots_4096", &mut || group_slots(&streams, &mut s));
        let mut g = BatchGroups::new();
        t("batch_scan", &mut || group_batch(&streams, &mut g));
    }
    println!(
        "\n  map_fresh  = HashMap nuevo cada ciclo (allocación)\n  \
           map_reused = un HashMap reusado y limpiado\n  \
           slots_4096 = array indexado por stream_id + generación, O(1)\n  \
           batch_scan = arrays del tamaño del batch, escaneo sobre los distintos"
    );
}

// ════════════════════════════════════════════════════════════════════════
// Section 3 — `Staged::fill`: one memcpy per entry, or one for the window?
//
// Production does one `extend_from_slice` per entry: 256 memcpys per cycle.
// But `push_entry` writes subject and payload back to back and advances, so
// consecutive seqs are ADJACENT bytes — a window is one contiguous range.
// It could be a single copy, with offsets rebased by arithmetic.
//
// The only thing that breaks contiguity is a segment rotation mid-window
// (every 16 MB), which splits it into two ranges. `split` models that.
// ════════════════════════════════════════════════════════════════════════

/// Mirrors the store segment: entries written back to back.
struct Segment {
    bytes: Vec<u8>,
    locs: Vec<(u32, u32)>, // (offset, len)
}

fn build_segment(payload: usize) -> Segment {
    let subj = b"orders.42.created";
    let mut bytes = Vec::with_capacity(FEED * (subj.len() + payload));
    let mut locs = Vec::with_capacity(FEED);
    for _ in 0..FEED {
        let off = bytes.len() as u32;
        bytes.extend_from_slice(subj);
        bytes.extend_from_slice(&vec![0xABu8; payload]);
        locs.push((off, (subj.len() + payload) as u32));
    }
    Segment { bytes, locs }
}

/// a) Today: one `extend_from_slice` per entry.
fn fill_per_entry(seg: &Segment, out: &mut Vec<u8>, offs: &mut Vec<u32>) -> u64 {
    out.clear();
    offs.clear();
    for &(off, len) in seg.locs.iter() {
        offs.push(out.len() as u32);
        out.extend_from_slice(&seg.bytes[off as usize..(off + len) as usize]);
    }
    out.len() as u64 + offs.len() as u64
}

/// b) One copy for the whole window; offsets rebased by subtraction.
fn fill_whole(seg: &Segment, out: &mut Vec<u8>, offs: &mut Vec<u32>) -> u64 {
    out.clear();
    offs.clear();
    let first = seg.locs[0].0;
    let last = seg.locs[seg.locs.len() - 1];
    let end = last.0 + last.1;
    out.extend_from_slice(&seg.bytes[first as usize..end as usize]);
    for &(off, _) in seg.locs.iter() {
        offs.push(off - first); // arithmetic, no copy
    }
    out.len() as u64 + offs.len() as u64
}

/// c) A segment rotation lands mid-window: two ranges instead of one.
fn fill_split(seg: &Segment, out: &mut Vec<u8>, offs: &mut Vec<u32>) -> u64 {
    out.clear();
    offs.clear();
    let mid = FEED / 2;
    let (a0, _) = seg.locs[0];
    let (am, aml) = seg.locs[mid - 1];
    out.extend_from_slice(&seg.bytes[a0 as usize..(am + aml) as usize]);
    let (b0, _) = seg.locs[mid];
    let (bm, bml) = seg.locs[FEED - 1];
    let base = out.len() as u32;
    out.extend_from_slice(&seg.bytes[b0 as usize..(bm + bml) as usize]);
    for (i, &(off, _)) in seg.locs.iter().enumerate() {
        offs.push(if i < mid { off - a0 } else { base + (off - b0) });
    }
    out.len() as u64 + offs.len() as u64
}

/// d) The window in K equal chunks. `whole` is K=1, `per_entry` is K=FEED,
/// and 2 beat both at 8KB — so the optimum is somewhere in between and
/// depends on how much of the window fits in cache.
fn fill_chunks(seg: &Segment, out: &mut Vec<u8>, offs: &mut Vec<u32>, k: usize) -> u64 {
    out.clear();
    offs.clear();
    let per = FEED.div_ceil(k);
    let mut i = 0usize;
    while i < FEED {
        let end = (i + per).min(FEED);
        let (c0, _) = seg.locs[i];
        let (cm, cml) = seg.locs[end - 1];
        let base = out.len() as u32;
        out.extend_from_slice(&seg.bytes[c0 as usize..(cm + cml) as usize]);
        for &(off, _) in &seg.locs[i..end] {
            offs.push(base + (off - c0));
        }
        i = end;
    }
    out.len() as u64 + offs.len() as u64
}

fn section_chunks() {
    println!("\n\n╔══ Sección 4 — ¿cuál es el tamaño de trozo óptimo? ══╗\n");
    for payload in [64usize, 512, 8192] {
        let seg = build_segment(payload);
        let window = seg.bytes.len();
        println!("── payload {payload}B → ventana {} KB ──", window / 1024);
        let mut out = Vec::with_capacity(window + 64);
        let mut offs = Vec::with_capacity(FEED);
        for k in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
            let t0 = Instant::now();
            let mut chk = 0u64;
            for _ in 0..CYCLES {
                chk = black_box(fill_chunks(&seg, &mut out, &mut offs, k));
            }
            let ns = t0.elapsed().as_nanos() as f64;
            println!(
                "  K={k:<4} ({:>5} B/trozo) {:>9.1} ns/ciclo  {:>6.1} GB/s  (chk {chk})",
                window / k,
                ns / CYCLES as f64,
                (CYCLES * window) as f64 / (ns / 1e9) / 1e9,
            );
        }
        println!();
    }
    println!("  K=1 es `whole`, K=256 es `per_entry`. El mínimo dice el tamaño de trozo.");
}

fn section_fill() {
    println!("\n\n╔══ Sección 3 — `fill`: {FEED} copias o una ══╗\n");
    for payload in [64usize, 512, 8192] {
        let seg = build_segment(payload);
        let window_bytes = seg.bytes.len();
        println!(
            "── payload {payload}B  → ventana de {} KB ──",
            window_bytes / 1024
        );
        let mut out = Vec::with_capacity(window_bytes + 64);
        let mut offs = Vec::with_capacity(FEED);
        let t = |name: &str, f: &mut dyn FnMut() -> u64| {
            let t0 = Instant::now();
            let mut chk = 0u64;
            for _ in 0..CYCLES {
                chk = black_box(f());
            }
            let ns = t0.elapsed().as_nanos() as f64;
            println!(
                "  {name:<12} {:>8.1} ms  {:>9.1} ns/ciclo  {:>6.1} ns/entrada  \
                 {:>7.1} GB/s  (chk {chk})",
                ns / 1e6,
                ns / CYCLES as f64,
                ns / (CYCLES * FEED) as f64,
                (CYCLES * window_bytes) as f64 / (ns / 1e9) / 1e9,
            );
        };
        t("per_entry", &mut || fill_per_entry(&seg, &mut out, &mut offs));
        t("whole", &mut || fill_whole(&seg, &mut out, &mut offs));
        t("split_2", &mut || fill_split(&seg, &mut out, &mut offs));
        println!();
    }
    println!(
        "  per_entry = hoy: una extend_from_slice por entrada ({FEED} memcpys)\n  \
           whole     = una sola copia del rango, offsets por resta\n  \
           split_2   = dos copias (rotación de segmento a mitad de ventana)"
    );
}

fn bench(name: &str, f: impl FnOnce() -> u64) {
    let t = Instant::now();
    let emitted = black_box(f());
    let ns = t.elapsed().as_nanos() as f64;
    println!(
        "  {name:<10} {:>8.1} ms  {:>9.1} ns/cycle  {:>7.1} ns/recipient  emits/cycle={}",
        ns / 1e6,
        ns / CYCLES as f64,
        ns / (CYCLES * FEED * RECIPIENTS) as f64,
        emitted / CYCLES as u64,
    );
}

fn main() {
    println!(
        "{CYCLES} cycles x {FEED} entries x {RECIPIENTS} recipients, \
         {CONSUMERS} consumers, {PAYLOAD}B payload\n"
    );
    // (streams, subjects, blocked). A window of 256 can hold up to 256
    // distinct streams, so more than half of them is the interesting end.
    for (streams, subjects, blocked) in [
        (1usize, 1usize, 0usize),
        (4, 4, 0),
        (64, 64, 0),
        (160, 160, 0),
        (160, 160, 6),
    ] {
        let w = build_window(subjects, streams);
        let b = bindings();
        let c = Counters::new(blocked);
        println!(
            "── {streams} stream(s), {subjects} subject(s), \
             {blocked}/{CONSUMERS} consumers bloqueados ──"
        );
        let mut s = Scratch::new();
        bench("flat", || run_flat(&w, &b, &c, &mut s));
        let mut s = Scratch::new();
        bench("by_consumer", || run_grouped(&w, &b, &c, &mut s));
        let mut s = Scratch::new();
        bench("by_stream", || run_subjdedup(&w, &b, &c, &mut s));
        let mut s = Scratch::new();
        bench("tree", || run_lean(&w, &b, &c, &mut s));
        println!();
    }
    println!(
        "  NOTE emits/cycle differs between shapes: the collapse and queue-dedup\n  \
         rules are modelled only in `flat`/`subjdedup`. Compare shapes with the\n  \
         SAME emit count, and read the others as an upper bound on what\n  \
         restructuring could buy — not as a like-for-like speedup.\n  \
         NOTE the subject -> matches map is PRE-BUILT outside the timer in all\n  \
         four shapes, so `lookup_verified` and the pattern merge are free here.\n  \
         That is why `subjdedup` shows nothing: the work it saves already costs\n  \
         zero. Its result is not evidence either way."
    );

    section_grouping();
    section_fill();
    section_chunks();
}
