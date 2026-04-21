//! drain_pipeline — group + validate + serialize con subjects DINÁMICOS.
//!
//! Workload real:
//!   - Publishers mandan subjects CONCRETOS (alta cardinalidad):
//!       message.meta.premium.user_1212
//!       message.qr.basic.user_111
//!       ... 10_000 subjects distintos en el universo.
//!   - Subscribers registran PATTERNS con wildcards (baja cardinalidad):
//!       message.meta.*.>
//!       message.*.basic.*
//!       ... 4 patterns por stream.
//!
//! Match check por (concrete_subject, pattern) es costoso (walk de trie).
//!
//! Estrategias:
//!   CURRENT  — linear-scan bucket per (conn,stream). Match per msg per sub.
//!   A        — Two-pass. Match per msg per sub.
//!   B        — Single-pass. Match per msg per sub.
//!   C-full   — NATS-style: cache ilimitado HashMap<concrete_subject, Box<[u16]>>.
//!              Primer hit: walk. Siguientes hits: O(1) lookup.
//!   C-lru    — NATS-style: cache direct-mapped bounded (simula LRU con set
//!              assoc = 1). Mide comportamiento bajo evicción real.
//!
//! Todas aplican las 5 validaciones: stream_paused, max_age, sub_paused,
//! subject_match, dedupe, capacity, conn_alive.

#![allow(unused)]

use rustc_hash::FxHashMap;
use std::hint::black_box;
use std::time::Instant;
use bytes::BytesMut;

// ── Xorshift RNG ────────────────────────────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed.max(1)) }
    #[inline]
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    #[inline] fn range(&mut self, n: u32) -> u32 { (self.next() as u32) % n }
    #[inline] fn range64(&mut self, n: u64) -> u64 { self.next() % n }
}

// ── Workload params ─────────────────────────────────────────────────────────

const TOTAL_STREAMS: usize            = 64;
const ACTIVE_STREAMS_PER_CYCLE: usize = 8;
const MSGS_PER_CYCLE: usize           = 256;
const CONSUMERS_PER_STREAM: usize     = 4;
const SUBS_PER_CONSUMER: usize        = 4;
const PAYLOAD_SIZE: usize             = 128;
const CYCLES: usize                   = 100_000;

// Subjects dinámicos: el publisher emite subjects concretos de un universo grande.
// 10_000 distintos = escala realista de multi-tenant (user_1..user_10000).
const SUBJECT_CARDINALITY: u64 = 10_000;

// Patterns registrados por los subs (wildcards tipo `message.meta.*.>`).
// 4 por stream: finita, chica.
const PATTERNS_PER_STREAM: u32 = 4;

// LRU-style cache bounded. 512 slots por stream (direct-mapped).
const LRU_SLOTS_PER_STREAM: usize = 2048;
const LRU_MASK: u64 = (LRU_SLOTS_PER_STREAM - 1) as u64;

// Inflight inicial por sub (precomputed al inicio de cada cycle).
const INITIAL_CAPACITY: u16 = 32;

// ── Match simulado (pattern walk) ───────────────────────────────────────────
// Modela el coste de "caminar el trie para ver si el subject concreto matchea
// el pattern". No queremos un trie real (sería ruido), pero sí un cómputo no
// trivial que el compilador no pueda plegar. ~25% match rate por pattern.

#[inline(always)]
fn pattern_matches(subject_hash: u64, pattern_id: u32) -> bool {
    // Simula walk real de trie para subject tipo `message.meta.premium.user_1212`:
    //   4 tokens separados por '.', cada uno con rehash + table lookup.
    // Coste aproximado: ~20-40 cycles = 8-15 ns (realista para NATS subject match).
    let mut h = subject_hash;
    let p = pattern_id as u64;
    // Token 1
    h = h.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(p);
    h = h.rotate_left(13) ^ (h >> 7);
    // Token 2
    h = h.wrapping_mul(0x517C_C1B7_2722_0A95).wrapping_add(p << 3);
    h = h.rotate_left(21) ^ (h >> 11);
    // Token 3
    h = h.wrapping_mul(0x2545_F491_4F6C_DD1D).wrapping_add(p << 7);
    h = h.rotate_left(29) ^ (h >> 13);
    // Token 4 (wildcard ">" check)
    h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9).wrapping_add(p << 11);
    (h.count_ones() & 0x3) == 0    // ~25% match rate
}

// ── Types ───────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
#[repr(C)]
struct Msg {
    stream_id:    u32,
    seq:          u64,
    subject_hash: u64,      // CONCRETE subject hash (alta cardinalidad)
    timestamp_ns: u64,
}

#[derive(Clone, Copy)]
struct SubEntry {
    pattern_id:    u32,     // patrón wildcard registrado (baja cardinalidad)
    consumer_id:   u32,
    sub_id:        u32,
    connection_id: u32,
    owner:         u8,
    paused:        bool,
}

struct Runtime {
    subs:          Box<[SubEntry]>,
    stream_paused: bool,
    max_age_ns:    u64,
}

struct ConnAliveMap {
    alive: Vec<bool>,
}

// ── Serialización de entry (wire-ready) ─────────────────────────────────────

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

// ── validate_and_write (baseline: pattern_matches per sub per msg) ──────────

#[inline(always)]
fn validate_and_write(
    msg: &Msg,
    rt: &Runtime,
    caps: &mut [u16],
    conns: &ConnAliveMap,
    cutoff_ns: u64,
    out: &mut BytesMut,
    subject: &[u8],
    payload: &[u8],
) -> u64 {
    if rt.max_age_ns > 0 && msg.timestamp_ns < cutoff_ns { return 0; }
    if rt.stream_paused { return 0; }

    let mut seen: u64 = 0;
    let mut count: u64 = 0;
    for (i, s) in rt.subs.iter().enumerate() {
        if s.paused { continue; }
        // match: walk pattern (coste simulado del trie)
        if !pattern_matches(msg.subject_hash, s.pattern_id) { continue; }
        let mask = 1u64 << s.owner as u64;
        if seen & mask != 0 { continue; }
        let cap = unsafe { caps.get_unchecked_mut(i) };
        if *cap == 0 { continue; }
        if !unsafe { *conns.alive.get_unchecked(s.connection_id as usize) } { continue; }
        write_entry(out, msg.seq, s.consumer_id, s.sub_id, subject, payload);
        *cap -= 1;
        seen |= mask;
        count += 1;
    }
    count
}

// ── Emit con route precomputada (sub indices ya filtrados) ──────────────────

#[inline(always)]
fn emit_route(
    msg: &Msg,
    rt: &Runtime,
    route: &[u16],
    caps: &mut [u16],
    conns: &ConnAliveMap,
    out: &mut BytesMut,
    subject: &[u8],
    payload: &[u8],
) -> u64 {
    let mut seen: u64 = 0;
    let mut count: u64 = 0;
    for &si in route.iter() {
        let s = unsafe { rt.subs.get_unchecked(si as usize) };
        if s.paused { continue; }
        let mask = 1u64 << s.owner as u64;
        if seen & mask != 0 { continue; }
        let cap = unsafe { caps.get_unchecked_mut(si as usize) };
        if *cap == 0 { continue; }
        if !unsafe { *conns.alive.get_unchecked(s.connection_id as usize) } { continue; }
        write_entry(out, msg.seq, s.consumer_id, s.sub_id, subject, payload);
        *cap -= 1;
        seen |= mask;
        count += 1;
    }
    count
}

#[inline(always)]
fn resolve_route(rt: &Runtime, subject_hash: u64) -> Box<[u16]> {
    let mut v: Vec<u16> = Vec::new();
    for (i, s) in rt.subs.iter().enumerate() {
        if pattern_matches(subject_hash, s.pattern_id) {
            v.push(i as u16);
        }
    }
    v.into_boxed_slice()
}

// ── CURRENT: linear-scan bucket per (conn,stream) ───────────────────────────

struct CurrentAcc {
    buckets_body:   Vec<BytesMut>,
    buckets_count:  Vec<u32>,
    buckets_in_use: Vec<bool>,
    active:  Vec<(u32, u32, usize)>,
    caps:    Vec<Vec<u16>>,
}

impl CurrentAcc {
    fn new(runtimes: &[Runtime]) -> Self {
        let pool_size = 32;
        Self {
            buckets_body:   (0..pool_size).map(|_| BytesMut::with_capacity(8192)).collect(),
            buckets_count:  vec![0; pool_size],
            buckets_in_use: vec![false; pool_size],
            active:         Vec::with_capacity(32),
            caps: runtimes.iter().map(|rt| vec![INITIAL_CAPACITY; rt.subs.len()]).collect(),
        }
    }

    #[inline]
    fn reset_cycle(&mut self) {
        for &(_c, _s, idx) in self.active.iter() {
            self.buckets_body[idx].clear();
            self.buckets_count[idx] = 0;
            self.buckets_in_use[idx] = false;
        }
        self.active.clear();
        for caps in self.caps.iter_mut() {
            for c in caps.iter_mut() { *c = INITIAL_CAPACITY; }
        }
    }

    #[inline]
    fn acquire_bucket(&mut self, conn: u32, stream: u32) -> usize {
        for &(c, s, idx) in self.active.iter() {
            if c == conn && s == stream { return idx; }
        }
        let idx = self.buckets_in_use.iter().position(|&u| !u)
            .unwrap_or_else(|| {
                self.buckets_body.push(BytesMut::with_capacity(8192));
                self.buckets_count.push(0);
                self.buckets_in_use.push(false);
                self.buckets_body.len() - 1
            });
        self.buckets_in_use[idx] = true;
        self.buckets_body[idx].clear();
        self.buckets_count[idx] = 0;
        self.active.push((conn, stream, idx));
        idx
    }

    #[inline]
    fn run(&mut self, msgs: &[Msg], runtimes: &[Runtime], conns: &ConnAliveMap, cutoff_ns: u64, subject: &[u8], payload: &[u8]) -> u64 {
        self.reset_cycle();
        let mut total = 0u64;
        let max_matches = CONSUMERS_PER_STREAM;
        let mut matches: [(u32, u32); 8] = [(0, 0); 8];

        for m in msgs {
            let sid = m.stream_id as usize;
            let n_matches = {
                let rt = unsafe { runtimes.get_unchecked(sid) };
                if rt.stream_paused { 0 }
                else if rt.max_age_ns > 0 && m.timestamp_ns < cutoff_ns { 0 }
                else {
                    let caps = unsafe { self.caps.get_unchecked_mut(sid) };
                    let mut seen: u64 = 0;
                    let mut n = 0usize;
                    for (i, s) in rt.subs.iter().enumerate() {
                        if s.paused { continue; }
                        if !pattern_matches(m.subject_hash, s.pattern_id) { continue; }
                        let mask = 1u64 << s.owner as u64;
                        if seen & mask != 0 { continue; }
                        let cap = unsafe { caps.get_unchecked_mut(i) };
                        if *cap == 0 { continue; }
                        if !unsafe { *conns.alive.get_unchecked(s.connection_id as usize) } { continue; }
                        matches[n] = (s.consumer_id, s.sub_id);
                        n += 1;
                        *cap -= 1;
                        seen |= mask;
                        if n >= max_matches { break; }
                    }
                    n
                }
            };
            for k in 0..n_matches {
                let (cons, sub_id) = matches[k];
                let idx = self.acquire_bucket(cons, m.stream_id);
                write_entry(&mut self.buckets_body[idx], m.seq, cons, sub_id, subject, payload);
                self.buckets_count[idx] += 1;
                total += 1;
            }
        }
        total
    }
}

// ── A: Two-pass ─────────────────────────────────────────────────────────────

struct TwoPass {
    groups:  Vec<Vec<Msg>>,
    outputs: Vec<BytesMut>,
    caps:    Vec<Vec<u16>>,
    active:  Vec<u32>,
}

impl TwoPass {
    fn new(runtimes: &[Runtime]) -> Self {
        Self {
            groups:  (0..TOTAL_STREAMS).map(|_| Vec::with_capacity(64)).collect(),
            outputs: (0..TOTAL_STREAMS).map(|_| BytesMut::with_capacity(8192)).collect(),
            caps:    runtimes.iter().map(|rt| vec![INITIAL_CAPACITY; rt.subs.len()]).collect(),
            active:  Vec::with_capacity(ACTIVE_STREAMS_PER_CYCLE),
        }
    }

    #[inline]
    fn reset_capacities(&mut self) {
        for caps in self.caps.iter_mut() {
            for c in caps.iter_mut() { *c = INITIAL_CAPACITY; }
        }
    }

    #[inline]
    fn reset_cycle(&mut self) {
        for &sid in self.active.iter() {
            unsafe {
                self.groups.get_unchecked_mut(sid as usize).clear();
                self.outputs.get_unchecked_mut(sid as usize).clear();
            }
        }
        self.active.clear();
    }

    #[inline]
    fn run(&mut self, msgs: &[Msg], runtimes: &[Runtime], conns: &ConnAliveMap, cutoff_ns: u64, subject: &[u8], payload: &[u8]) -> u64 {
        self.reset_cycle();
        self.reset_capacities();

        for m in msgs {
            let sid = m.stream_id as usize;
            let g = unsafe { self.groups.get_unchecked_mut(sid) };
            if g.is_empty() { self.active.push(m.stream_id); }
            g.push(*m);
        }
        let mut total = 0u64;
        for &sid_u32 in self.active.iter() {
            let sid = sid_u32 as usize;
            let rt   = unsafe { runtimes.get_unchecked(sid) };
            let grp  = unsafe { self.groups.get_unchecked(sid) };
            let out  = unsafe { self.outputs.get_unchecked_mut(sid) };
            let caps = unsafe { self.caps.get_unchecked_mut(sid) };
            for m in grp.iter() {
                total += validate_and_write(m, rt, caps, conns, cutoff_ns, out, subject, payload);
            }
        }
        total
    }
}

// ── B: Single-pass ──────────────────────────────────────────────────────────

struct SinglePass {
    outputs: Vec<BytesMut>,
    caps:    Vec<Vec<u16>>,
    active:  Vec<u32>,
    touched: Vec<bool>,
}

impl SinglePass {
    fn new(runtimes: &[Runtime]) -> Self {
        Self {
            outputs: (0..TOTAL_STREAMS).map(|_| BytesMut::with_capacity(8192)).collect(),
            caps:    runtimes.iter().map(|rt| vec![INITIAL_CAPACITY; rt.subs.len()]).collect(),
            active:  Vec::with_capacity(ACTIVE_STREAMS_PER_CYCLE),
            touched: vec![false; TOTAL_STREAMS],
        }
    }

    #[inline]
    fn reset_capacities(&mut self) {
        for caps in self.caps.iter_mut() {
            for c in caps.iter_mut() { *c = INITIAL_CAPACITY; }
        }
    }

    #[inline]
    fn reset_cycle(&mut self) {
        for &sid in self.active.iter() {
            unsafe {
                self.outputs.get_unchecked_mut(sid as usize).clear();
                *self.touched.get_unchecked_mut(sid as usize) = false;
            }
        }
        self.active.clear();
    }

    #[inline]
    fn run(&mut self, msgs: &[Msg], runtimes: &[Runtime], conns: &ConnAliveMap, cutoff_ns: u64, subject: &[u8], payload: &[u8]) -> u64 {
        self.reset_cycle();
        self.reset_capacities();

        let mut total = 0u64;
        for m in msgs {
            let sid = m.stream_id as usize;
            let rt      = unsafe { runtimes.get_unchecked(sid) };
            let out     = unsafe { self.outputs.get_unchecked_mut(sid) };
            let caps    = unsafe { self.caps.get_unchecked_mut(sid) };
            let touched = unsafe { self.touched.get_unchecked_mut(sid) };
            if !*touched {
                self.active.push(m.stream_id);
                *touched = true;
            }
            total += validate_and_write(m, rt, caps, conns, cutoff_ns, out, subject, payload);
        }
        total
    }
}

// ── C-full: cache ilimitado (NATS sublist cache sin evicción) ───────────────

struct CachedRoutesFull {
    cache:   Vec<FxHashMap<u64, Box<[u16]>>>,    // per stream
    outputs: Vec<BytesMut>,
    caps:    Vec<Vec<u16>>,
    active:  Vec<u32>,
    touched: Vec<bool>,
    hits:    u64,
    misses:  u64,
}

impl CachedRoutesFull {
    fn new(runtimes: &[Runtime]) -> Self {
        Self {
            cache:   (0..TOTAL_STREAMS).map(|_| FxHashMap::default()).collect(),
            outputs: (0..TOTAL_STREAMS).map(|_| BytesMut::with_capacity(8192)).collect(),
            caps:    runtimes.iter().map(|rt| vec![INITIAL_CAPACITY; rt.subs.len()]).collect(),
            active:  Vec::with_capacity(ACTIVE_STREAMS_PER_CYCLE),
            touched: vec![false; TOTAL_STREAMS],
            hits: 0, misses: 0,
        }
    }

    #[inline]
    fn reset_capacities(&mut self) {
        for caps in self.caps.iter_mut() {
            for c in caps.iter_mut() { *c = INITIAL_CAPACITY; }
        }
    }

    #[inline]
    fn reset_cycle(&mut self) {
        for &sid in self.active.iter() {
            unsafe {
                self.outputs.get_unchecked_mut(sid as usize).clear();
                *self.touched.get_unchecked_mut(sid as usize) = false;
            }
        }
        self.active.clear();
    }

    #[inline]
    fn run(&mut self, msgs: &[Msg], runtimes: &[Runtime], conns: &ConnAliveMap, cutoff_ns: u64, subject: &[u8], payload: &[u8]) -> u64 {
        self.reset_cycle();
        self.reset_capacities();

        let mut total = 0u64;
        for m in msgs {
            let sid = m.stream_id as usize;
            let rt = unsafe { runtimes.get_unchecked(sid) };
            if rt.stream_paused { continue; }
            if rt.max_age_ns > 0 && m.timestamp_ns < cutoff_ns { continue; }

            let out     = unsafe { self.outputs.get_unchecked_mut(sid) };
            let caps    = unsafe { self.caps.get_unchecked_mut(sid) };
            let cache   = unsafe { self.cache.get_unchecked_mut(sid) };
            let touched = unsafe { self.touched.get_unchecked_mut(sid) };
            if !*touched {
                self.active.push(m.stream_id);
                *touched = true;
            }

            // Lookup en cache; miss → resolve + insert
            let route = if let Some(r) = cache.get(&m.subject_hash) {
                self.hits += 1;
                r
            } else {
                self.misses += 1;
                let r = resolve_route(rt, m.subject_hash);
                cache.entry(m.subject_hash).or_insert(r)
            };

            total += emit_route(m, rt, route, caps, conns, out, subject, payload);
        }
        total
    }
}

// ── D: ProcessGroup — validaciones de stream hoisted una vez por grupo ──────
// Asume msgs llegan ORDENADOS por stream_id (forma real del drainer single-owner).
// Por cada run contiguo del mismo stream: rt, out, caps resueltos UNA vez.
//   - runtimes[sid]:   1 lookup por grupo, NO por msg
//   - rt.stream_paused: 1 check por grupo
//   - self.outputs[sid]: 1 get_mut por grupo
//   - self.caps[sid]:    1 get_mut por grupo
//   - active.push:       1 por grupo
// Sin touched[] — al ser ordenado, cada stream aparece exactamente una vez.

struct ProcessGroup {
    outputs: Vec<BytesMut>,
    caps:    Vec<Vec<u16>>,
    active:  Vec<u32>,
}

impl ProcessGroup {
    fn new(runtimes: &[Runtime]) -> Self {
        Self {
            outputs: (0..TOTAL_STREAMS).map(|_| BytesMut::with_capacity(8192)).collect(),
            caps:    runtimes.iter().map(|rt| vec![INITIAL_CAPACITY; rt.subs.len()]).collect(),
            active:  Vec::with_capacity(ACTIVE_STREAMS_PER_CYCLE),
        }
    }

    #[inline]
    fn reset_capacities(&mut self) {
        for caps in self.caps.iter_mut() {
            for c in caps.iter_mut() { *c = INITIAL_CAPACITY; }
        }
    }

    #[inline]
    fn reset_cycle(&mut self) {
        for &sid in self.active.iter() {
            unsafe { self.outputs.get_unchecked_mut(sid as usize).clear(); }
        }
        self.active.clear();
    }

    #[inline]
    fn run(
        &mut self,
        msgs: &[Msg],           // ← ORDENADOS por stream_id
        runtimes: &[Runtime],
        conns: &ConnAliveMap,
        cutoff_ns: u64,
        subject: &[u8],
        payload: &[u8],
    ) -> u64 {
        self.reset_cycle();
        self.reset_capacities();

        let mut total = 0u64;

        // chunk_by itera UNA vez, emitiendo subslices en los boundaries.
        // Sin doble scan. Cada sub-slice es un grupo del mismo stream_id.
        for group in msgs.chunk_by(|a, b| a.stream_id == b.stream_id) {
            let sid = unsafe { group.get_unchecked(0).stream_id } as usize;

            // ───── STREAM-LEVEL (una sola vez por grupo) ─────
            let rt = unsafe { runtimes.get_unchecked(sid) };
            if rt.stream_paused { continue; }

            self.active.push(sid as u32);
            let out  = unsafe { self.outputs.get_unchecked_mut(sid) };
            let caps = unsafe { self.caps.get_unchecked_mut(sid) };
            let check_age = rt.max_age_ns > 0;

            // ───── HOT LOOP: rt/out/caps son captures estables ─────
            for m in group {
                if check_age && m.timestamp_ns < cutoff_ns { continue; }
                let mut seen: u64 = 0;
                for (k, s) in rt.subs.iter().enumerate() {
                    if s.paused { continue; }
                    if !pattern_matches(m.subject_hash, s.pattern_id) { continue; }
                    let mask = 1u64 << s.owner as u64;
                    if seen & mask != 0 { continue; }
                    let cap = unsafe { caps.get_unchecked_mut(k) };
                    if *cap == 0 { continue; }
                    if !unsafe { *conns.alive.get_unchecked(s.connection_id as usize) } { continue; }
                    write_entry(out, m.seq, s.consumer_id, s.sub_id, subject, payload);
                    *cap -= 1;
                    seen |= mask;
                    total += 1;
                }
            }
        }
        total
    }
}

// ── F: D-native — signature real del drainer: (rt, msgs) directo ────────────
// Simula producción: el drainer (single-owner por stream) entrega StreamBatch
// con rt YA resuelto por el caller. process_group no busca runtimes[sid] —
// lo recibe como argumento. Cero overhead de chunk_by, cero detección de
// boundaries, cero per-msg lookups.

struct StreamBatch {
    stream_id: u32,
    msgs:      Vec<Msg>,
}

struct ProcessGroupNative {
    outputs: Vec<BytesMut>,
    caps:    Vec<Vec<u16>>,
    active:  Vec<u32>,
}

impl ProcessGroupNative {
    fn new(runtimes: &[Runtime]) -> Self {
        Self {
            outputs: (0..TOTAL_STREAMS).map(|_| BytesMut::with_capacity(8192)).collect(),
            caps:    runtimes.iter().map(|rt| vec![INITIAL_CAPACITY; rt.subs.len()]).collect(),
            active:  Vec::with_capacity(ACTIVE_STREAMS_PER_CYCLE),
        }
    }

    #[inline]
    fn reset_capacities(&mut self) {
        for caps in self.caps.iter_mut() {
            for c in caps.iter_mut() { *c = INITIAL_CAPACITY; }
        }
    }

    #[inline]
    fn reset_cycle(&mut self) {
        for &sid in self.active.iter() {
            unsafe { self.outputs.get_unchecked_mut(sid as usize).clear(); }
        }
        self.active.clear();
    }

    // La API REAL: recibe rt resuelto + msgs del mismo stream.
    #[inline(always)]
    fn process_group(
        &mut self,
        rt: &Runtime,
        stream_id: u32,
        msgs: &[Msg],
        conns: &ConnAliveMap,
        cutoff_ns: u64,
        subject: &[u8],
        payload: &[u8],
    ) -> u64 {
        if rt.stream_paused { return 0; }

        self.active.push(stream_id);
        let sid = stream_id as usize;
        let out  = unsafe { self.outputs.get_unchecked_mut(sid) };
        let caps = unsafe { self.caps.get_unchecked_mut(sid) };
        let check_age = rt.max_age_ns > 0;

        let mut total = 0u64;
        for m in msgs {
            if check_age && m.timestamp_ns < cutoff_ns { continue; }
            let mut seen: u64 = 0;
            for (k, s) in rt.subs.iter().enumerate() {
                if s.paused { continue; }
                if !pattern_matches(m.subject_hash, s.pattern_id) { continue; }
                let mask = 1u64 << s.owner as u64;
                if seen & mask != 0 { continue; }
                let cap = unsafe { caps.get_unchecked_mut(k) };
                if *cap == 0 { continue; }
                if !unsafe { *conns.alive.get_unchecked(s.connection_id as usize) } { continue; }
                write_entry(out, m.seq, s.consumer_id, s.sub_id, subject, payload);
                *cap -= 1;
                seen |= mask;
                total += 1;
            }
        }
        total
    }

    // Driver: simula el shard recibiendo batches del drainer.
    #[inline]
    fn run_cycle(
        &mut self,
        batches: &[StreamBatch],
        runtimes: &[Runtime],
        conns: &ConnAliveMap,
        cutoff_ns: u64,
        subject: &[u8],
        payload: &[u8],
    ) -> u64 {
        self.reset_cycle();
        self.reset_capacities();
        let mut total = 0u64;
        for batch in batches {
            // UN lookup de rt por batch (= por stream), no por msg.
            let rt = unsafe { runtimes.get_unchecked(batch.stream_id as usize) };
            total += self.process_group(rt, batch.stream_id, &batch.msgs,
                                        conns, cutoff_ns, subject, payload);
        }
        total
    }
}

// ── E: ProcessGroup + emit DIRECTO a conn-frames (sin stream-bucket) ────────
// Misma arquitectura hoisted de D, pero el buffer de salida es POR CONEXIÓN.
// Motivación: el stream-bucket es intermedio; el socket TCP escribe por conn,
// así que escribir directo a conn_frames[conn_id] evita la demux posterior.
//
// Diferencias contra D:
//   - `outputs` indexado por stream_id  →  `conn_frames` indexado por conn_id
//   - `active` (streams) → `active_conns` (connections tocadas)
//   - `write_entry` target: frame de la conn del sub, no del stream
//
// Mantiene TODAS las validaciones + hoisting de D. La diferencia es solo el
// destino del write.

struct ProcessGroupConn {
    conn_frames:  Vec<BytesMut>,      // indexado por connection_id
    caps:         Vec<Vec<u16>>,
    active_conns: Vec<u32>,
}

impl ProcessGroupConn {
    fn new(runtimes: &[Runtime], total_conns: usize) -> Self {
        Self {
            conn_frames:  (0..total_conns).map(|_| BytesMut::with_capacity(4096)).collect(),
            caps:         runtimes.iter().map(|rt| vec![INITIAL_CAPACITY; rt.subs.len()]).collect(),
            active_conns: Vec::with_capacity(64),
        }
    }

    #[inline]
    fn reset_capacities(&mut self) {
        for caps in self.caps.iter_mut() {
            for c in caps.iter_mut() { *c = INITIAL_CAPACITY; }
        }
    }

    #[inline]
    fn reset_cycle(&mut self) {
        for &cid in self.active_conns.iter() {
            unsafe { self.conn_frames.get_unchecked_mut(cid as usize).clear(); }
        }
        self.active_conns.clear();
    }

    #[inline]
    fn run(
        &mut self,
        msgs: &[Msg],
        runtimes: &[Runtime],
        conns: &ConnAliveMap,
        cutoff_ns: u64,
        subject: &[u8],
        payload: &[u8],
    ) -> u64 {
        self.reset_cycle();
        self.reset_capacities();

        let mut total = 0u64;
        for group in msgs.chunk_by(|a, b| a.stream_id == b.stream_id) {
            let sid = unsafe { group.get_unchecked(0).stream_id } as usize;

            // ───── STREAM-LEVEL (hoisted una vez por grupo) ─────
            let rt = unsafe { runtimes.get_unchecked(sid) };
            if rt.stream_paused { continue; }
            let caps = unsafe { self.caps.get_unchecked_mut(sid) };
            let check_age = rt.max_age_ns > 0;

            // ───── HOT LOOP ─────
            for m in group {
                if check_age && m.timestamp_ns < cutoff_ns { continue; }
                let mut seen: u64 = 0;
                for (k, s) in rt.subs.iter().enumerate() {
                    if s.paused { continue; }
                    if !pattern_matches(m.subject_hash, s.pattern_id) { continue; }
                    let mask = 1u64 << s.owner as u64;
                    if seen & mask != 0 { continue; }
                    let cap = unsafe { caps.get_unchecked_mut(k) };
                    if *cap == 0 { continue; }
                    let cid = s.connection_id as usize;
                    if !unsafe { *conns.alive.get_unchecked(cid) } { continue; }

                    // EMIT DIRECTO al conn-frame. La primera escritura de la
                    // conn en este cycle la registra en active_conns para reset.
                    let out = unsafe { self.conn_frames.get_unchecked_mut(cid) };
                    if out.is_empty() {
                        self.active_conns.push(s.connection_id);
                    }
                    write_entry(out, m.seq, s.consumer_id, s.sub_id, subject, payload);

                    *cap -= 1;
                    seen |= mask;
                    total += 1;
                }
            }
        }
        total
    }
}

// ── C-lru: direct-mapped bounded cache (simula LRU 1-way) ───────────────────

struct CacheSlot {
    key:    u64,           // sentinel u64::MAX = vacío
    routes: Box<[u16]>,
}

impl CacheSlot {
    fn empty() -> Self { Self { key: u64::MAX, routes: Box::new([]) } }
}

struct CachedRoutesLru {
    cache:   Vec<Vec<CacheSlot>>,   // per stream: LRU_SLOTS_PER_STREAM slots
    outputs: Vec<BytesMut>,
    caps:    Vec<Vec<u16>>,
    active:  Vec<u32>,
    touched: Vec<bool>,
    hits:    u64,
    misses:  u64,
    evicts:  u64,
}

impl CachedRoutesLru {
    fn new(runtimes: &[Runtime]) -> Self {
        Self {
            cache:   (0..TOTAL_STREAMS).map(|_|
                         (0..LRU_SLOTS_PER_STREAM).map(|_| CacheSlot::empty()).collect()
                     ).collect(),
            outputs: (0..TOTAL_STREAMS).map(|_| BytesMut::with_capacity(8192)).collect(),
            caps:    runtimes.iter().map(|rt| vec![INITIAL_CAPACITY; rt.subs.len()]).collect(),
            active:  Vec::with_capacity(ACTIVE_STREAMS_PER_CYCLE),
            touched: vec![false; TOTAL_STREAMS],
            hits: 0, misses: 0, evicts: 0,
        }
    }

    #[inline]
    fn reset_capacities(&mut self) {
        for caps in self.caps.iter_mut() {
            for c in caps.iter_mut() { *c = INITIAL_CAPACITY; }
        }
    }

    #[inline]
    fn reset_cycle(&mut self) {
        for &sid in self.active.iter() {
            unsafe {
                self.outputs.get_unchecked_mut(sid as usize).clear();
                *self.touched.get_unchecked_mut(sid as usize) = false;
            }
        }
        self.active.clear();
    }

    #[inline]
    fn run(&mut self, msgs: &[Msg], runtimes: &[Runtime], conns: &ConnAliveMap, cutoff_ns: u64, subject: &[u8], payload: &[u8]) -> u64 {
        self.reset_cycle();
        self.reset_capacities();

        let mut total = 0u64;
        for m in msgs {
            let sid = m.stream_id as usize;
            let rt = unsafe { runtimes.get_unchecked(sid) };
            if rt.stream_paused { continue; }
            if rt.max_age_ns > 0 && m.timestamp_ns < cutoff_ns { continue; }

            let out     = unsafe { self.outputs.get_unchecked_mut(sid) };
            let caps    = unsafe { self.caps.get_unchecked_mut(sid) };
            let cache   = unsafe { self.cache.get_unchecked_mut(sid) };
            let touched = unsafe { self.touched.get_unchecked_mut(sid) };
            if !*touched {
                self.active.push(m.stream_id);
                *touched = true;
            }

            let slot_idx = (m.subject_hash & LRU_MASK) as usize;
            let slot = unsafe { cache.get_unchecked_mut(slot_idx) };
            if slot.key != m.subject_hash {
                // Miss: resolve + reemplazar (evicción implícita)
                if slot.key != u64::MAX { self.evicts += 1; }
                self.misses += 1;
                slot.routes = resolve_route(rt, m.subject_hash);
                slot.key    = m.subject_hash;
            } else {
                self.hits += 1;
            }

            total += emit_route(m, rt, &slot.routes, caps, conns, out, subject, payload);
        }
        total
    }
}

// ── Dataset ────────────────────────────────────────────────────────────────

const MAX_AGE_NS: u64 = 1_000_000_000;
const NOW_NS:     u64 = 10_000_000_000;
const TOTAL_CONNS: usize = 1024;

fn build_runtimes(rng: &mut Rng) -> Vec<Runtime> {
    let mut next_cons: u32 = 1;
    let mut next_sub:  u32 = 1;
    let mut next_conn: u32 = 1;
    (0..TOTAL_STREAMS).map(|_| {
        let mut subs = Vec::with_capacity(CONSUMERS_PER_STREAM * SUBS_PER_CONSUMER);
        for ci in 0..CONSUMERS_PER_STREAM {
            let cid = next_cons; next_cons += 1;
            let conn_id = next_conn; next_conn += 1;
            for _ in 0..SUBS_PER_CONSUMER {
                subs.push(SubEntry {
                    pattern_id:    rng.range(PATTERNS_PER_STREAM),
                    consumer_id:   cid,
                    sub_id:        { let v = next_sub; next_sub += 1; v },
                    connection_id: conn_id,
                    owner:         ci as u8,
                    paused:        (rng.next() & 0x0F) == 0,
                });
            }
        }
        Runtime {
            subs: subs.into_boxed_slice(),
            stream_paused: (rng.next() & 0x1F) == 0,
            max_age_ns:    MAX_AGE_NS,
        }
    }).collect()
}

fn build_conn_alive(total_conns: usize, rng: &mut Rng) -> ConnAliveMap {
    ConnAliveMap {
        alive: (0..total_conns).map(|_| (rng.next() & 0x1F) != 0).collect(),
    }
}

fn build_cycle_msgs(rng: &mut Rng) -> Vec<Msg> {
    let mut active: Vec<u32> = (0..ACTIVE_STREAMS_PER_CYCLE as u32)
        .map(|_| rng.range(TOTAL_STREAMS as u32))
        .collect();
    active.sort_unstable();
    active.dedup();
    while active.len() < ACTIVE_STREAMS_PER_CYCLE {
        let s = rng.range(TOTAL_STREAMS as u32);
        if !active.contains(&s) { active.push(s); }
    }
    let mut msgs: Vec<Msg> = (0..MSGS_PER_CYCLE).map(|i| {
        let expired = (rng.next() & 0x0F) == 0;
        let timestamp_ns = if expired {
            NOW_NS.saturating_sub(MAX_AGE_NS + 500_000_000)
        } else {
            NOW_NS.saturating_sub(rng.next() % MAX_AGE_NS)
        };
        Msg {
            stream_id:    active[i % active.len()],
            seq:          i as u64,
            subject_hash: rng.range64(SUBJECT_CARDINALITY),
            timestamp_ns,
        }
    }).collect();
    // Ordenar por stream_id: forma real del drainer (single-owner per stream).
    // El costo de sort NO se mide — es el estado natural de los msgs en el hot path.
    msgs.sort_by_key(|m| m.stream_id);
    msgs
}

// ── Main ────────────────────────────────────────────────────────────────────

fn main() {
    println!("\ndrain_pipeline — DYNAMIC subjects (high cardinality)");
    println!("===================================================");
    println!(
        "streams={TOTAL_STREAMS}  active/cycle={ACTIVE_STREAMS_PER_CYCLE}  \
         msgs/cycle={MSGS_PER_CYCLE}  cons/stream={CONSUMERS_PER_STREAM}  \
         subs/cons={SUBS_PER_CONSUMER}"
    );
    println!(
        "subject_cardinality={SUBJECT_CARDINALITY}  patterns/stream={PATTERNS_PER_STREAM}  \
         payload={PAYLOAD_SIZE}B  cycles={CYCLES}  initial_cap={INITIAL_CAPACITY}/sub"
    );
    println!(
        "LRU slots/stream={LRU_SLOTS_PER_STREAM} (direct-mapped, 1-way assoc)\n"
    );

    let mut rng = Rng::new(0xC0FFEE);
    let runtimes  = build_runtimes(&mut rng);
    let conns     = build_conn_alive(TOTAL_CONNS, &mut rng);
    let cutoff_ns = NOW_NS.saturating_sub(MAX_AGE_NS);
    let cycles: Vec<Vec<Msg>> = (0..CYCLES).map(|_| build_cycle_msgs(&mut rng)).collect();

    // Pre-build batches for F (simula el drainer entregando StreamBatch al shard).
    // Setup cost NO se mide — en producción el drainer ya emite así.
    let batch_cycles: Vec<Vec<StreamBatch>> = cycles.iter().map(|msgs| {
        let mut out: Vec<StreamBatch> = Vec::new();
        for group in msgs.chunk_by(|a, b| a.stream_id == b.stream_id) {
            out.push(StreamBatch {
                stream_id: unsafe { group.get_unchecked(0).stream_id },
                msgs:      group.to_vec(),
            });
        }
        out
    }).collect();

    let subject = b"message.meta.premium.user_1212";
    let payload = vec![0xABu8; PAYLOAD_SIZE];

    // CURRENT
    let mut cur = CurrentAcc::new(&runtimes);
    for c in cycles.iter().take(100) { let _ = cur.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let mut emit_cur: u64 = 0;
    let start = Instant::now();
    for c in &cycles { emit_cur += cur.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let ns_cur = start.elapsed().as_nanos() as f64 / cycles.len() as f64;

    // A
    let mut a = TwoPass::new(&runtimes);
    for c in cycles.iter().take(100) { let _ = a.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let mut emit_a: u64 = 0;
    let start = Instant::now();
    for c in &cycles { emit_a += a.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let ns_a = start.elapsed().as_nanos() as f64 / cycles.len() as f64;

    // B
    let mut b = SinglePass::new(&runtimes);
    for c in cycles.iter().take(100) { let _ = b.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let mut emit_b: u64 = 0;
    let start = Instant::now();
    for c in &cycles { emit_b += b.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let ns_b = start.elapsed().as_nanos() as f64 / cycles.len() as f64;

    // C-full
    let mut c1 = CachedRoutesFull::new(&runtimes);
    for c in cycles.iter().take(100) { let _ = c1.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    c1.hits = 0; c1.misses = 0;
    let mut emit_c1: u64 = 0;
    let start = Instant::now();
    for c in &cycles { emit_c1 += c1.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let ns_c1 = start.elapsed().as_nanos() as f64 / cycles.len() as f64;
    let hit_rate_c1 = c1.hits as f64 / (c1.hits + c1.misses).max(1) as f64;

    // D: ProcessGroup (hoisted stream-level)
    let mut d = ProcessGroup::new(&runtimes);
    for c in cycles.iter().take(100) { let _ = d.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let mut emit_d: u64 = 0;
    let start = Instant::now();
    for c in &cycles { emit_d += d.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let ns_d = start.elapsed().as_nanos() as f64 / cycles.len() as f64;

    // F: ProcessGroupNative — rt pre-resuelto por el caller (signature real del drainer)
    let mut f = ProcessGroupNative::new(&runtimes);
    for bc in batch_cycles.iter().take(100) { let _ = f.run_cycle(bc, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let mut emit_f: u64 = 0;
    let start = Instant::now();
    for bc in &batch_cycles { emit_f += f.run_cycle(bc, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let ns_f = start.elapsed().as_nanos() as f64 / batch_cycles.len() as f64;

    // E: ProcessGroup + emit directo a conn-frames
    let mut e = ProcessGroupConn::new(&runtimes, TOTAL_CONNS);
    for c in cycles.iter().take(100) { let _ = e.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let mut emit_e: u64 = 0;
    let start = Instant::now();
    for c in &cycles { emit_e += e.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let ns_e = start.elapsed().as_nanos() as f64 / cycles.len() as f64;

    // C-lru
    let mut c2 = CachedRoutesLru::new(&runtimes);
    for c in cycles.iter().take(100) { let _ = c2.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    c2.hits = 0; c2.misses = 0; c2.evicts = 0;
    let mut emit_c2: u64 = 0;
    let start = Instant::now();
    for c in &cycles { emit_c2 += c2.run(c, &runtimes, &conns, cutoff_ns, subject, &payload); }
    let ns_c2 = start.elapsed().as_nanos() as f64 / cycles.len() as f64;
    let hit_rate_c2 = c2.hits as f64 / (c2.hits + c2.misses).max(1) as f64;

    assert_eq!(emit_a, emit_b,   "emit count mismatch A vs B");
    assert_eq!(emit_a, emit_cur, "emit count mismatch A vs CURRENT");
    assert_eq!(emit_a, emit_c1,  "emit count mismatch A vs C-full");
    assert_eq!(emit_a, emit_c2,  "emit count mismatch A vs C-lru");
    assert_eq!(emit_a, emit_d,   "emit count mismatch A vs D");
    assert_eq!(emit_a, emit_e,   "emit count mismatch A vs E");
    assert_eq!(emit_a, emit_f,   "emit count mismatch A vs F");

    let entries_per_cycle = emit_a as f64 / CYCLES as f64;

    println!(
        "{:<60} | {:>10} | {:>10} | {:>10}",
        "Strategy", "ns/cycle", "ns/msg", "msgs/s"
    );
    println!("{}", "-".repeat(102));
    for (label, ns) in [
        ("CURRENT — linear-scan bucket per (conn,stream)",            ns_cur),
        ("A       — Two-pass (per-msg pattern_matches)",              ns_a),
        ("B       — Single-pass (per-msg pattern_matches)",           ns_b),
        ("D       — ProcessGroup (stream-level HOISTED, sorted in)",  ns_d),
        ("F       — D-native (rt pre-resolved, drainer signature)",   ns_f),
        ("E       — ProcessGroup + emit directo a conn-frames",       ns_e),
        ("C-full  — Unbounded HashMap cache",                         ns_c1),
        ("C-lru   — Direct-mapped bounded cache (1-way assoc)",       ns_c2),
    ] {
        let per_msg = ns / MSGS_PER_CYCLE as f64;
        let tput    = (MSGS_PER_CYCLE as f64 * 1e9 / ns) / 1e6;
        println!("{:<60} | {:>7.0} ns | {:>7.2} ns | {:>6.2} M/s", label, ns, per_msg, tput);
    }
    println!();
    println!("Entries emitted per cycle (avg): {:.1}", entries_per_cycle);
    println!("C-full : hit_rate = {:.2}%  ({} hits / {} misses)", hit_rate_c1 * 100.0, c1.hits, c1.misses);
    println!("C-lru  : hit_rate = {:.2}%  ({} hits / {} misses, {} evictions)",
             hit_rate_c2 * 100.0, c2.hits, c2.misses, c2.evicts);
    println!();
    println!("Delta vs CURRENT:  A={:.2}×   B={:.2}×   D={:.2}×   F={:.2}×   E={:.2}×   C-full={:.2}×   C-lru={:.2}×",
        ns_cur / ns_a, ns_cur / ns_b, ns_cur / ns_d, ns_cur / ns_f, ns_cur / ns_e, ns_cur / ns_c1, ns_cur / ns_c2);
    println!("Delta vs B     :   D={:.2}×   F={:.2}×   E={:.2}×   C-full={:.2}×   C-lru={:.2}×",
        ns_b / ns_d, ns_b / ns_f, ns_b / ns_e, ns_b / ns_c1, ns_b / ns_c2);
    println!("Delta D vs F   :   {:.2}×  (F={})   |   B vs F: {:.2}×  (F={})",
        ns_d / ns_f, if ns_f < ns_d { "faster" } else { "slower" },
        ns_b / ns_f, if ns_f < ns_b { "faster" } else { "slower" });
    println!("Delta D vs E   :   {:.2}×  (E={})",
        ns_d / ns_e, if ns_e < ns_d { "faster" } else { "slower" });
    println!();
}
