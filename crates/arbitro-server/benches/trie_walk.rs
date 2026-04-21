//! trie_walk — valida la hipótesis del "mapa mental":
//!   - BASELINE: D-style, per-msg itera TODOS los subs del stream + pattern_matches.
//!   - TRIE:     per-msg tokenize (1×) + walk trie → &[BindingIdx] ya filtrados.
//!
//! El trie es precomputado en "registro" (cold path, fuera de medición).
//! El walk reemplaza N pattern_matches por O(tokens) hashmap lookups.
//! Bindings son self-contained (capacity Cell, conn_id directo, owner_mask listo).
//!
//! Clave: escalar patterns/stream para ver dónde el trie empieza a ganar.
//!        4 patterns: baseline comparable.  16/64: trie debería dominar.

#![allow(unused)]

use bytes::BytesMut;
use rustc_hash::FxHashMap;
use std::cell::Cell;
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

// ── Workload ────────────────────────────────────────────────────────────────

const TOTAL_STREAMS:            usize = 64;
const ACTIVE_STREAMS_PER_CYCLE: usize = 8;
const MSGS_PER_CYCLE:           usize = 256;
const CONSUMERS_PER_STREAM:     usize = 4;
const PAYLOAD_SIZE:             usize = 128;
const CYCLES:                   usize = 50_000;
const INITIAL_CAPACITY:         u16   = 32;
const TOTAL_CONNS:              usize = 1024;

// Subject universe: "message.{cat}.{tier}.user_{id}"
//   cat  ∈ {meta, qr, transfer, notify}                        — 4 values
//   tier ∈ {premium, basic, free}                              — 3 values
//   id   ∈ 0..10_000                                           — 10_000 values
// Total unique concrete subjects: 120_000.  Hot-path carga siempre 4 tokens.

const N_CATEGORIES: usize = 4;
const N_TIERS:      usize = 3;
const N_USERS:      u32   = 10_000;

// ── Tokens (hashes precomputados) ───────────────────────────────────────────

// Hash de un token (usamos FNV-1a para que sea estable y rápido).
#[inline(always)]
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

const CAT_NAMES:  [&[u8]; 4] = [b"meta", b"qr", b"transfer", b"notify"];
const TIER_NAMES: [&[u8]; 3] = [b"premium", b"basic", b"free"];

// ── Subject concreto ─────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct Subject {
    tokens: [u64; 4],
}

impl Subject {
    fn new(cat: usize, tier: usize, user: u32) -> Self {
        let mut user_buf = [0u8; 16];
        let user_bytes = format_user(user, &mut user_buf);
        Subject {
            tokens: [
                fnv1a(b"message"),
                fnv1a(CAT_NAMES[cat]),
                fnv1a(TIER_NAMES[tier]),
                fnv1a(user_bytes),
            ],
        }
    }
}

// Formatea "user_123" sin alloc.
fn format_user(user: u32, buf: &mut [u8; 16]) -> &[u8] {
    buf[..5].copy_from_slice(b"user_");
    let mut i = 15usize;
    let mut n = user;
    if n == 0 {
        buf[i] = b'0'; i -= 1;
    } else {
        while n > 0 {
            buf[i] = b'0' + (n % 10) as u8;
            n /= 10;
            if i == 0 { break; }
            i -= 1;
        }
    }
    let start_digits = i + 1;
    // Mover dígitos justo después de "user_"
    let n_digits = 16 - start_digits;
    for j in 0..n_digits {
        buf[5 + j] = buf[start_digits + j];
    }
    &buf[..5 + n_digits]
}

// ── Msg ──────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct Msg {
    stream_id:    u32,
    seq:          u64,
    timestamp_ns: u64,
    subject:      Subject,
}

// ── Binding (self-contained, igual para ambas estrategias) ──────────────────

struct Binding {
    pattern_id:    u32,    // solo para debug/replay
    consumer_id:   u32,
    sub_id:        u32,
    connection_id: u32,
    owner_mask:    u64,    // 1 << consumer_idx — pre-resuelto
    paused:        bool,
    capacity:      Cell<u16>,
}

// ── Pattern (para el baseline + para construir el trie) ─────────────────────

/// Patrón compilado: 4 slots, cada uno Exact/Star, más optional ">" al final.
/// Simpler que el trie completo pero suficiente para el baseline.
#[derive(Clone, Copy)]
enum Tok {
    Exact(u64),
    Star,
    Gt,      // ">": matchea desde aquí en adelante (al final)
}

struct Pattern {
    toks: [Tok; 4],
    len:  u8,       // número de slots activos antes de ">"
    has_gt: bool,
}

impl Pattern {
    #[inline(always)]
    fn matches(&self, subject: &Subject) -> bool {
        let n = self.len as usize;
        for i in 0..n {
            match self.toks[i] {
                Tok::Exact(h) => if subject.tokens[i] != h { return false; },
                Tok::Star     => { /* matchea cualquier token */ }
                Tok::Gt       => return true,
            }
        }
        // Si no hay ">", el pattern debe consumir TODOS los tokens.
        if self.has_gt { true } else { n == 4 }
    }
}

// ── Runtime stream ──────────────────────────────────────────────────────────

struct StreamRuntime {
    stream_id:     u32,
    paused:        bool,
    max_age_ns:    u64,
    // Baseline: lista plana — iteramos todos y llamamos matches() per sub per msg
    bindings:      Vec<Binding>,
    // Paralelo: patterns por binding (mismo orden que `bindings`)
    patterns:      Vec<Pattern>,
    // Trie: precomputado en registro, se walkea en hot path
    trie:          TrieNode,
}

struct ConnAliveMap { alive: Vec<bool> }

// ── Trie ────────────────────────────────────────────────────────────────────

struct TrieNode {
    /// children exactos indexados por token hash
    children:      FxHashMap<u64, Box<TrieNode>>,
    /// branch "*" — matchea cualquier token en esta posición
    wildcard_star: Option<Box<TrieNode>>,
    /// ">" bindings (si este nodo tiene patterns que terminan en ">")
    /// Matchea CUALQUIER subject que haya llegado hasta aquí.
    gt_bindings:   Vec<u16>,
    /// Bindings cuyo pattern termina EXACTAMENTE en este nodo (sin ">" ni más)
    terminal_bindings: Vec<u16>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            children:          FxHashMap::default(),
            wildcard_star:     None,
            gt_bindings:       Vec::new(),
            terminal_bindings: Vec::new(),
        }
    }

    /// Inserta binding `b_idx` con su pattern.  Cold path.
    fn insert(&mut self, pattern: &Pattern, b_idx: u16) {
        let mut node = self;
        let n = pattern.len as usize;
        for i in 0..n {
            match pattern.toks[i] {
                Tok::Exact(h) => {
                    node = node.children.entry(h).or_insert_with(|| Box::new(TrieNode::new()));
                }
                Tok::Star => {
                    if node.wildcard_star.is_none() {
                        node.wildcard_star = Some(Box::new(TrieNode::new()));
                    }
                    node = node.wildcard_star.as_mut().unwrap();
                }
                Tok::Gt => unreachable!(), // Gt solo al final, manejado por has_gt
            }
        }
        if pattern.has_gt {
            node.gt_bindings.push(b_idx);
        } else {
            node.terminal_bindings.push(b_idx);
        }
    }

    /// Walk: pobla `out` con idx de bindings candidatos. Hot path.
    /// subject siempre tiene 4 tokens.
    #[inline]
    fn walk(&self, subject: &Subject, out: &mut Vec<u16>) {
        walk_rec(self, &subject.tokens, 0, out);
    }
}

#[inline]
fn walk_rec(node: &TrieNode, tokens: &[u64; 4], depth: usize, out: &mut Vec<u16>) {
    // ">" matchea desde aquí (captura el resto)
    if !node.gt_bindings.is_empty() {
        out.extend_from_slice(&node.gt_bindings);
    }
    if depth == 4 {
        if !node.terminal_bindings.is_empty() {
            out.extend_from_slice(&node.terminal_bindings);
        }
        return;
    }
    let tok = tokens[depth];
    if let Some(child) = node.children.get(&tok) {
        walk_rec(child, tokens, depth + 1, out);
    }
    if let Some(star) = node.wildcard_star.as_deref() {
        walk_rec(star, tokens, depth + 1, out);
    }
}

// ── write_entry (wire-ready) ────────────────────────────────────────────────

#[inline(always)]
fn write_entry(buf: &mut BytesMut, seq: u64, cons: u32, sub: u32, subject_bytes: &[u8], payload: &[u8]) {
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&cons.to_le_bytes());
    buf.extend_from_slice(&sub.to_le_bytes());
    buf.extend_from_slice(&(subject_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(&((subject_bytes.len() + payload.len()) as u32).to_le_bytes());
    buf.extend_from_slice(subject_bytes);
    buf.extend_from_slice(payload);
}

// ── BASELINE: D-style, stream-hoisted, per-sub pattern.matches() ────────────

struct Baseline {
    outputs: Vec<BytesMut>,
    active:  Vec<u32>,
}

impl Baseline {
    fn new() -> Self {
        Self {
            outputs: (0..TOTAL_STREAMS).map(|_| BytesMut::with_capacity(8192)).collect(),
            active:  Vec::with_capacity(ACTIVE_STREAMS_PER_CYCLE),
        }
    }

    #[inline]
    fn reset(&mut self, runtimes: &[StreamRuntime]) {
        for &sid in self.active.iter() {
            unsafe { self.outputs.get_unchecked_mut(sid as usize).clear(); }
        }
        self.active.clear();
        for rt in runtimes {
            for b in rt.bindings.iter() { b.capacity.set(INITIAL_CAPACITY); }
        }
    }

    #[inline]
    fn run(
        &mut self,
        msgs: &[Msg],
        runtimes: &[StreamRuntime],
        conns: &ConnAliveMap,
        cutoff_ns: u64,
        subject_bytes: &[u8],
        payload: &[u8],
    ) -> u64 {
        self.reset(runtimes);
        let mut total = 0u64;

        for group in msgs.chunk_by(|a, b| a.stream_id == b.stream_id) {
            let sid = unsafe { group.get_unchecked(0).stream_id } as usize;
            let rt = unsafe { runtimes.get_unchecked(sid) };
            if rt.paused { continue; }

            self.active.push(sid as u32);
            let out = unsafe { self.outputs.get_unchecked_mut(sid) };
            let check_age = rt.max_age_ns > 0;

            for m in group {
                if check_age && m.timestamp_ns < cutoff_ns { continue; }
                let mut seen: u64 = 0;
                // ITERAR TODOS los bindings y matchear per-sub
                for (i, b) in rt.bindings.iter().enumerate() {
                    if b.paused { continue; }
                    let p = unsafe { rt.patterns.get_unchecked(i) };
                    if !p.matches(&m.subject) { continue; }
                    if seen & b.owner_mask != 0 { continue; }
                    let cap = b.capacity.get();
                    if cap == 0 { continue; }
                    if !unsafe { *conns.alive.get_unchecked(b.connection_id as usize) } { continue; }
                    write_entry(out, m.seq, b.consumer_id, b.sub_id, subject_bytes, payload);
                    b.capacity.set(cap - 1);
                    seen |= b.owner_mask;
                    total += 1;
                }
            }
        }
        total
    }
}

// ── TRIE: walk precomputado → &[BindingIdx] ya filtrados ─────────────────────

struct TrieAcc {
    outputs:    Vec<BytesMut>,
    active:     Vec<u32>,
    scratch:    Vec<u16>,      // buffer de candidatos por msg (reutilizado)
}

impl TrieAcc {
    fn new() -> Self {
        Self {
            outputs: (0..TOTAL_STREAMS).map(|_| BytesMut::with_capacity(8192)).collect(),
            active:  Vec::with_capacity(ACTIVE_STREAMS_PER_CYCLE),
            scratch: Vec::with_capacity(64),
        }
    }

    #[inline]
    fn reset(&mut self, runtimes: &[StreamRuntime]) {
        for &sid in self.active.iter() {
            unsafe { self.outputs.get_unchecked_mut(sid as usize).clear(); }
        }
        self.active.clear();
        for rt in runtimes {
            for b in rt.bindings.iter() { b.capacity.set(INITIAL_CAPACITY); }
        }
    }

    #[inline]
    fn run(
        &mut self,
        msgs: &[Msg],
        runtimes: &[StreamRuntime],
        conns: &ConnAliveMap,
        cutoff_ns: u64,
        subject_bytes: &[u8],
        payload: &[u8],
    ) -> u64 {
        self.reset(runtimes);
        let mut total = 0u64;

        for group in msgs.chunk_by(|a, b| a.stream_id == b.stream_id) {
            let sid = unsafe { group.get_unchecked(0).stream_id } as usize;
            let rt = unsafe { runtimes.get_unchecked(sid) };
            if rt.paused { continue; }

            self.active.push(sid as u32);
            let out = unsafe { self.outputs.get_unchecked_mut(sid) };
            let check_age = rt.max_age_ns > 0;

            for m in group {
                if check_age && m.timestamp_ns < cutoff_ns { continue; }

                // WALK del trie: llena scratch con idx de bindings candidatos
                self.scratch.clear();
                rt.trie.walk(&m.subject, &mut self.scratch);

                let mut seen: u64 = 0;
                for &b_idx in self.scratch.iter() {
                    let b = unsafe { rt.bindings.get_unchecked(b_idx as usize) };
                    if b.paused { continue; }
                    if seen & b.owner_mask != 0 { continue; }
                    let cap = b.capacity.get();
                    if cap == 0 { continue; }
                    if !unsafe { *conns.alive.get_unchecked(b.connection_id as usize) } { continue; }
                    write_entry(out, m.seq, b.consumer_id, b.sub_id, subject_bytes, payload);
                    b.capacity.set(cap - 1);
                    seen |= b.owner_mask;
                    total += 1;
                }
            }
        }
        total
    }
}

// ── Construcción del dataset ────────────────────────────────────────────────

fn random_pattern(rng: &mut Rng) -> Pattern {
    // Mezcla realista: ~60% con un star en alguna posición, ~20% con ">",
    // ~20% exactos. Siempre empiezan con "message" exacto.
    let kind = rng.range(10);
    let msg_hash = fnv1a(b"message");

    let cat_hash  = fnv1a(CAT_NAMES[rng.range_usize(N_CATEGORIES)]);
    let tier_hash = fnv1a(TIER_NAMES[rng.range_usize(N_TIERS)]);
    // user token: random de un user específico o *
    let mut user_buf = [0u8; 16];
    let user_hash = fnv1a(format_user(rng.range(N_USERS), &mut user_buf));

    match kind {
        0 | 1 => {
            // "message.cat.>"  (match amplio)
            Pattern {
                toks: [Tok::Exact(msg_hash), Tok::Exact(cat_hash), Tok::Star, Tok::Star],
                len:  2,
                has_gt: true,
            }
        }
        2 | 3 => {
            // "message.*.tier.*"
            Pattern {
                toks: [Tok::Exact(msg_hash), Tok::Star, Tok::Exact(tier_hash), Tok::Star],
                len:  4,
                has_gt: false,
            }
        }
        4 | 5 => {
            // "message.cat.*.*"
            Pattern {
                toks: [Tok::Exact(msg_hash), Tok::Exact(cat_hash), Tok::Star, Tok::Star],
                len:  4,
                has_gt: false,
            }
        }
        6 => {
            // "message.cat.tier.*"
            Pattern {
                toks: [Tok::Exact(msg_hash), Tok::Exact(cat_hash), Tok::Exact(tier_hash), Tok::Star],
                len:  4,
                has_gt: false,
            }
        }
        7 => {
            // "message.>"  (super amplio)
            Pattern {
                toks: [Tok::Exact(msg_hash), Tok::Star, Tok::Star, Tok::Star],
                len:  1,
                has_gt: true,
            }
        }
        _ => {
            // "message.cat.tier.user_X"  (exacto)
            Pattern {
                toks: [Tok::Exact(msg_hash), Tok::Exact(cat_hash), Tok::Exact(tier_hash), Tok::Exact(user_hash)],
                len:  4,
                has_gt: false,
            }
        }
    }
}

fn build_runtimes(patterns_per_stream: usize, rng: &mut Rng) -> Vec<StreamRuntime> {
    let mut next_cons: u32 = 1;
    let mut next_sub:  u32 = 1;
    let mut next_conn: u32 = 0;

    (0..TOTAL_STREAMS).map(|sid| {
        let mut bindings = Vec::with_capacity(patterns_per_stream);
        let mut patterns = Vec::with_capacity(patterns_per_stream);
        let mut trie = TrieNode::new();

        for i in 0..patterns_per_stream {
            let consumer_idx = (i % CONSUMERS_PER_STREAM) as u8;
            let owner_mask   = 1u64 << consumer_idx;
            let cid          = next_cons; next_cons += 1;
            // conn_id wrap-around dentro del universo de conns para evitar OOB.
            let conn_id      = next_conn % TOTAL_CONNS as u32;
            next_conn += 1;
            let pat          = random_pattern(rng);
            let b_idx        = bindings.len() as u16;

            bindings.push(Binding {
                pattern_id:    i as u32,
                consumer_id:   cid,
                sub_id:        { let v = next_sub; next_sub += 1; v },
                connection_id: conn_id,
                owner_mask,
                paused:        (rng.next() & 0x0F) == 0,
                capacity:      Cell::new(INITIAL_CAPACITY),
            });

            // Insertar en trie (cold path — fuera de medición).
            trie.insert(&pat, b_idx);
            patterns.push(pat);
        }

        StreamRuntime {
            stream_id:  sid as u32,
            paused:     (rng.next() & 0x1F) == 0,
            max_age_ns: 1_000_000_000,
            bindings,
            patterns,
            trie,
        }
    }).collect()
}

fn build_conn_alive(n: usize, rng: &mut Rng) -> ConnAliveMap {
    ConnAliveMap { alive: (0..n).map(|_| (rng.next() & 0x1F) != 0).collect() }
}

const MAX_AGE_NS: u64 = 1_000_000_000;
const NOW_NS:     u64 = 10_000_000_000;

fn build_cycle_msgs(rng: &mut Rng) -> Vec<Msg> {
    let mut active: Vec<u32> = (0..ACTIVE_STREAMS_PER_CYCLE as u32)
        .map(|_| rng.range(TOTAL_STREAMS as u32))
        .collect();
    active.sort_unstable(); active.dedup();
    while active.len() < ACTIVE_STREAMS_PER_CYCLE {
        let s = rng.range(TOTAL_STREAMS as u32);
        if !active.contains(&s) { active.push(s); }
    }
    let mut msgs: Vec<Msg> = (0..MSGS_PER_CYCLE).map(|i| {
        let expired = (rng.next() & 0x0F) == 0;
        let ts = if expired {
            NOW_NS.saturating_sub(MAX_AGE_NS + 500_000_000)
        } else {
            NOW_NS.saturating_sub(rng.next() % MAX_AGE_NS)
        };
        let cat  = rng.range_usize(N_CATEGORIES);
        let tier = rng.range_usize(N_TIERS);
        let user = rng.range(N_USERS);
        Msg {
            stream_id:    active[i % active.len()],
            seq:          i as u64,
            timestamp_ns: ts,
            subject:      Subject::new(cat, tier, user),
        }
    }).collect();
    msgs.sort_by_key(|m| m.stream_id);
    msgs
}

// ── Main ────────────────────────────────────────────────────────────────────

fn run_one(patterns_per_stream: usize) {
    let mut rng      = Rng::new(0xC0FFEE ^ patterns_per_stream as u64);
    let runtimes     = build_runtimes(patterns_per_stream, &mut rng);
    let conns        = build_conn_alive(TOTAL_CONNS, &mut rng);
    let cutoff_ns    = NOW_NS.saturating_sub(MAX_AGE_NS);
    let cycles: Vec<Vec<Msg>> = (0..CYCLES).map(|_| build_cycle_msgs(&mut rng)).collect();
    let subject_bytes = b"message.meta.premium.user_1212";
    let payload       = vec![0xABu8; PAYLOAD_SIZE];

    // Baseline
    let mut base = Baseline::new();
    for c in cycles.iter().take(100) { let _ = base.run(c, &runtimes, &conns, cutoff_ns, subject_bytes, &payload); }
    let mut emit_base: u64 = 0;
    let start = Instant::now();
    for c in &cycles { emit_base += base.run(c, &runtimes, &conns, cutoff_ns, subject_bytes, &payload); }
    let ns_base = start.elapsed().as_nanos() as f64 / cycles.len() as f64;

    // Trie
    let mut trie = TrieAcc::new();
    for c in cycles.iter().take(100) { let _ = trie.run(c, &runtimes, &conns, cutoff_ns, subject_bytes, &payload); }
    let mut emit_trie: u64 = 0;
    let start = Instant::now();
    for c in &cycles { emit_trie += trie.run(c, &runtimes, &conns, cutoff_ns, subject_bytes, &payload); }
    let ns_trie = start.elapsed().as_nanos() as f64 / cycles.len() as f64;

    assert_eq!(emit_base, emit_trie,
        "emit count mismatch @ patterns={}: base={} trie={}",
        patterns_per_stream, emit_base, emit_trie);

    let per_msg_base = ns_base / MSGS_PER_CYCLE as f64;
    let per_msg_trie = ns_trie / MSGS_PER_CYCLE as f64;
    let speedup = ns_base / ns_trie;
    let winner  = if ns_trie < ns_base { "TRIE" } else { "BASE" };

    println!(
        "patterns/stream = {:>3}  |  BASE {:>7.0} ns/cycle ({:>6.2} ns/msg)  |  \
         TRIE {:>7.0} ns/cycle ({:>6.2} ns/msg)  |  {:.2}×  ← {}  |  emit/cycle = {:.0}",
        patterns_per_stream, ns_base, per_msg_base, ns_trie, per_msg_trie,
        speedup, winner, emit_base as f64 / CYCLES as f64
    );
}

/// Diagnostic: para UN solo msg, imprime qué bindings emite baseline y qué emite trie.
fn diag_one(patterns_per_stream: usize) {
    let mut rng      = Rng::new(0xC0FFEE ^ patterns_per_stream as u64);
    let runtimes     = build_runtimes(patterns_per_stream, &mut rng);
    let conns        = build_conn_alive(TOTAL_CONNS, &mut rng);
    let cutoff_ns    = NOW_NS.saturating_sub(MAX_AGE_NS);
    let msgs         = build_cycle_msgs(&mut rng);
    let subject_bytes = b"message.meta.premium.user_1212";
    let payload       = vec![0xABu8; PAYLOAD_SIZE];

    // Para CADA msg, compara bindings que matchean.
    let mut divergences = 0usize;
    let mut total_checked = 0usize;
    for m in &msgs {
        let rt = &runtimes[m.stream_id as usize];
        if rt.paused { continue; }
        if m.timestamp_ns < cutoff_ns { continue; }
        total_checked += 1;
        let sid = m.stream_id;
        let m = *m;

        // Baseline: lista de (b_idx, matched) para este msg
        let mut base_matched: Vec<u16> = Vec::new();
        for (i, b) in rt.bindings.iter().enumerate() {
            let p = &rt.patterns[i];
            if p.matches(&m.subject) {
                base_matched.push(i as u16);
            }
        }

        // Trie: walk
        let mut trie_matched: Vec<u16> = Vec::new();
        rt.trie.walk(&m.subject, &mut trie_matched);
        trie_matched.sort_unstable();
        let mut trie_unique = trie_matched.clone();
        trie_unique.dedup();

        if base_matched != trie_unique || trie_matched.len() != trie_unique.len() {
            println!("STREAM {}: subject tokens = {:?}", sid, m.subject.tokens);
            println!("  baseline matched ({}): {:?}", base_matched.len(), base_matched);
            println!("  trie    matched ({}): {:?}  (unique: {:?})",
                trie_matched.len(), trie_matched, trie_unique);
            let dupes: Vec<u16> = trie_matched.iter().filter(|&&x| {
                trie_matched.iter().filter(|&&y| x == y).count() > 1
            }).cloned().collect();
            if !dupes.is_empty() {
                println!("  DUPLICATES in trie: {:?}", dupes);
            }
            let only_trie: Vec<u16> = trie_unique.iter().filter(|x| !base_matched.contains(x)).cloned().collect();
            let only_base: Vec<u16> = base_matched.iter().filter(|x| !trie_unique.contains(x)).cloned().collect();
            if !only_trie.is_empty() {
                println!("  ONLY in trie: {:?}", only_trie);
                for &b in &only_trie {
                    let p = &rt.patterns[b as usize];
                    println!("    binding[{}]: pattern len={} has_gt={} toks={:?}",
                        b, p.len, p.has_gt, &p.toks[..]);
                }
            }
            if !only_base.is_empty() { println!("  ONLY in base: {:?}", only_base); }
            println!();
            divergences += 1;
            if divergences >= 5 { return; }
        }
    }
    println!("checked {} usable msgs, {} divergences", total_checked, divergences);
}

/// Emit-diff: ejecuta 1 cycle bajo BOTH strategies registrando qué bindings emitió cada uno
/// y dumpea el diff por binding. Sin validaciones de cap/conn_alive para aislar.
fn emit_diff(patterns_per_stream: usize) {
    let mut rng      = Rng::new(0xC0FFEE ^ patterns_per_stream as u64);
    let runtimes     = build_runtimes(patterns_per_stream, &mut rng);
    let conns        = build_conn_alive(TOTAL_CONNS, &mut rng);
    let cutoff_ns    = NOW_NS.saturating_sub(MAX_AGE_NS);
    let msgs         = build_cycle_msgs(&mut rng);

    // Simula baseline: per-msg, per-sub matches + validaciones. Cuenta emits per b_idx.
    let mut base_emits: Vec<u64> = vec![0; TOTAL_STREAMS * patterns_per_stream];

    for group in msgs.chunk_by(|a, b| a.stream_id == b.stream_id) {
        let sid = group[0].stream_id as usize;
        let rt = &runtimes[sid];
        if rt.paused { continue; }
        // reset caps
        for b in rt.bindings.iter() { b.capacity.set(INITIAL_CAPACITY); }

        for m in group {
            if m.timestamp_ns < cutoff_ns { continue; }
            let mut seen: u64 = 0;
            for (i, b) in rt.bindings.iter().enumerate() {
                if b.paused { continue; }
                if !rt.patterns[i].matches(&m.subject) { continue; }
                if seen & b.owner_mask != 0 { continue; }
                let cap = b.capacity.get();
                if cap == 0 { continue; }
                if !conns.alive[b.connection_id as usize] { continue; }
                b.capacity.set(cap - 1);
                seen |= b.owner_mask;
                base_emits[sid * patterns_per_stream + i] += 1;
            }
        }
    }

    let mut trie_emits: Vec<u64> = vec![0; TOTAL_STREAMS * patterns_per_stream];
    let mut scratch: Vec<u16> = Vec::with_capacity(64);
    for group in msgs.chunk_by(|a, b| a.stream_id == b.stream_id) {
        let sid = group[0].stream_id as usize;
        let rt = &runtimes[sid];
        if rt.paused { continue; }
        for b in rt.bindings.iter() { b.capacity.set(INITIAL_CAPACITY); }

        for m in group {
            if m.timestamp_ns < cutoff_ns { continue; }
            scratch.clear();
            rt.trie.walk(&m.subject, &mut scratch);
            let mut seen: u64 = 0;
            for &b_idx in scratch.iter() {
                let b = &rt.bindings[b_idx as usize];
                if b.paused { continue; }
                if seen & b.owner_mask != 0 { continue; }
                let cap = b.capacity.get();
                if cap == 0 { continue; }
                if !conns.alive[b.connection_id as usize] { continue; }
                b.capacity.set(cap - 1);
                seen |= b.owner_mask;
                trie_emits[sid * patterns_per_stream + b_idx as usize] += 1;
            }
        }
    }

    let base_total: u64 = base_emits.iter().sum();
    let trie_total: u64 = trie_emits.iter().sum();
    println!("base total: {}  |  trie total: {}  |  diff: {:+}",
        base_total, trie_total, trie_total as i64 - base_total as i64);

    let mut divs = 0;
    for (i, (&b, &t)) in base_emits.iter().zip(trie_emits.iter()).enumerate() {
        if b != t {
            let sid = i / patterns_per_stream;
            let bidx = i % patterns_per_stream;
            let pat = &runtimes[sid].patterns[bidx];
            let bn = &runtimes[sid].bindings[bidx];
            println!("  sid={} bidx={} base={} trie={} diff={:+}  | pattern len={} gt={} toks={:?}  owner_mask={:b} paused={} conn={}",
                sid, bidx, b, t, t as i64 - b as i64,
                pat.len, pat.has_gt, &pat.toks[..], bn.owner_mask, bn.paused, bn.connection_id);
            divs += 1;
            if divs >= 20 { println!("... more"); return; }
        }
    }
}

impl std::fmt::Debug for Tok {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Tok::Exact(h) => write!(f, "Ex({:x})", h & 0xFFFF),
            Tok::Star     => write!(f, "*"),
            Tok::Gt       => write!(f, ">"),
        }
    }
}

fn main() {
    println!("\ntrie_walk — BASELINE (per-sub pattern) vs TRIE (precomputed walk)");
    println!("==================================================================");
    println!(
        "streams={TOTAL_STREAMS}  active/cycle={ACTIVE_STREAMS_PER_CYCLE}  \
         msgs/cycle={MSGS_PER_CYCLE}  cons/stream={CONSUMERS_PER_STREAM}  \
         payload={PAYLOAD_SIZE}B  cycles={CYCLES}"
    );
    println!(
        "subject universe = message.{{4 cat}}.{{3 tier}}.user_{{0..{}}}  =  {} concrete",
        N_USERS, N_CATEGORIES * N_TIERS * N_USERS as usize
    );
    println!();

    // DIAGNOSTIC mode at patterns=32: imprime un diff por binding para encontrar el bug.
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--diag") {
        diag_one(32);
        return;
    }
    if args.iter().any(|a| a == "--emit-diff") {
        emit_diff(32);
        return;
    }

    // Escalar patterns/stream: ahí se ve dónde el trie empieza a ganar.
    for p in [4usize, 8, 16, 32, 64] {
        run_one(p);
    }

    println!();
    println!("Nota: ambas estrategias son stream-hoisted, usan bindings self-contained");
    println!("      (capacity Cell, conn_id directo, owner_mask pre-resuelto), y aplican");
    println!("      las 5 validaciones (stream_paused, max_age, sub_paused, dedupe, cap, conn_alive).");
    println!("      La única diferencia es cómo se obtiene la lista de bindings candidatos.");
}
