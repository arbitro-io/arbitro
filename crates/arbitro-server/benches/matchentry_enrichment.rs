//! MatchEntry enrichment vs side-table — hot-path lookup of per-consumer
//! subject limits in the drain.
//!
//! The drain must decide "does this (consumer, subject) pair have room
//! in its max_subject_inflight budget?" for every match in every entry.
//! Three designs are compared:
//!
//! **V1 — CURRENT**: one `subject_limit_cache: HashMap<(stream, hash), Option<u32>>`
//!   is consulted ONCE per store entry, BEFORE matches are iterated. The
//!   limit is GLOBAL per stream+subject (not per-consumer). Multi-client
//!   scenarios share counters and collide (this is the bug we hit).
//!
//! **V2 — SIDE-TABLE per-consumer**: a `HashMap<(consumer, hash), u32>`
//!   checked PER MATCH. Each match does a hash lookup. More lookups, but
//!   correct per-consumer isolation.
//!
//! **V3 — ENRICHED MatchEntry**: the resolved limit is baked into the
//!   MatchEntry at snapshot rebuild time. Per-match check is a struct
//!   field load — zero extra lookups. Wider MatchEntry (+12 bytes).
//!
//! Workload:
//!   - N_SUBJECTS unique subject hashes
//!   - For each subject, N_MATCHES matches (N consumers fanout)
//!   - M entries to dispatch → M × N_MATCHES "match checks" total
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench matchentry_enrichment -p arbitro-server --no-run"
//!   wsl bash -lc "cp .../target/release/deps/matchentry_enrichment-* /tmp/arbitro-bench/ \
//!     && cd /tmp/arbitro-bench && ./matchentry_enrichment-* --bench"

#![allow(unused)]

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

// ── Shapes ──────────────────────────────────────────────────────────────────

/// V1/V2 — lean MatchEntry (32 bytes). Limit resolved externally.
#[derive(Clone, Copy)]
struct LeanMatchEntry {
    consumer_id: u32,
    connection_id: u64,
    queue_id: u32,
    subscription_id: u32,
    _pad: [u8; 12],
}

/// V3 — enriched MatchEntry (48 bytes). Limit baked in.
#[derive(Clone, Copy)]
struct FatMatchEntry {
    consumer_id: u32,
    connection_id: u64,
    queue_id: u32,
    subscription_id: u32,
    binding_idx: u32,
    max_inflight: u32,
    subject_limit: Option<u32>,
    flags: u8,
    _pad: [u8; 3],
}

// Sanity: sizes we advertise.
const _: () = assert!(std::mem::size_of::<LeanMatchEntry>() == 32);
// FatMatchEntry may pack differently, let it land where it lands — we'll
// print the actual size at runtime.

// ── RNG ─────────────────────────────────────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed.max(1)) }
    #[inline] fn next(&mut self) -> u32 {
        let mut x = self.0;
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        self.0 = x;
        x as u32
    }
}

// ── Data generation ─────────────────────────────────────────────────────────

/// Static workload: small working set, subjects repeat (typical pub/sub).
const N_SUBJECTS: usize = 256;   // distinct subject hashes the drain sees
const N_MATCHES: usize = 4;      // consumers per subject (fanout)
const N_ENTRIES: usize = 100_000; // entries processed

/// Dynamic workload: each entry has a FRESH subject (e.g. `vip.user_N`
/// where N grows monotonically). Simulates high-cardinality dynamic
/// subjects that the trie resolves on demand.
const N_ENTRIES_DYNAMIC: usize = 100_000;

/// Returns: list of subject hashes, for each subject its matches.
fn gen_workload() -> (Vec<u32>, Vec<Vec<u32>>) {
    let mut rng = Rng::new(0xC0FFEE);
    let subjects: Vec<u32> = (0..N_SUBJECTS).map(|_| rng.next()).collect();

    let mut all_consumers: Vec<u32> = (1..=(N_SUBJECTS * N_MATCHES) as u32).collect();
    let mut matches_per_subject: Vec<Vec<u32>> = Vec::with_capacity(N_SUBJECTS);
    for i in 0..N_SUBJECTS {
        // N_MATCHES distinct consumers per subject, rotating from the pool.
        let start = i * N_MATCHES;
        matches_per_subject.push(all_consumers[start..start + N_MATCHES].to_vec());
    }
    (subjects, matches_per_subject)
}

fn gen_entry_seqs() -> Vec<u32> {
    // Which subject each entry uses (index into subjects[]).
    let mut rng = Rng::new(0xBEEF);
    (0..N_ENTRIES).map(|_| (rng.next() as usize % N_SUBJECTS) as u32).collect()
}

// ── V1 — CURRENT: global-per-stream cache + per-entry check ────────────────
//
// The check happens ONCE per entry BEFORE matches are visited. The cache
// key is (stream_id, subject_hash) and the value is Option<u32>. After
// the check, iterate matches (no further limit check per match).

fn bench_v1_current(subjects: &[u32], matches: &[Vec<u32>], seqs: &[u32]) -> f64 {
    let stream_id: u32 = 7;
    let mut cache: HashMap<(u32, u32), Option<u32>, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(N_SUBJECTS, ahash::RandomState::new());
    // Pre-populate (simulate steady state).
    for &s in subjects {
        cache.insert((stream_id, s), Some(10));
    }

    // Counters — shared per subject_hash.
    let mut counters: HashMap<u32, u32, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(N_SUBJECTS, ahash::RandomState::new());

    // Warmup.
    for _ in 0..1_000 {
        for &sidx in seqs.iter().take(100) {
            let subject = subjects[sidx as usize];
            let _ = cache.get(&(stream_id, subject));
        }
    }

    let start = Instant::now();
    let mut total_matches_visited = 0u64;
    for &sidx in seqs {
        let subject = subjects[sidx as usize];
        // Per-entry cache lookup — 1 hash op.
        let limit = cache.get(&(stream_id, subject)).copied().flatten();
        let skip_entry = if let Some(max) = limit {
            let cur = counters.get(&subject).copied().unwrap_or(0);
            cur >= max
        } else {
            false
        };
        if skip_entry {
            continue;
        }
        // Iterate matches — no per-match limit check.
        for &cid in &matches[sidx as usize] {
            black_box(cid);
            total_matches_visited += 1;
        }
    }
    let elapsed = start.elapsed();
    black_box(total_matches_visited);
    elapsed.as_nanos() as f64 / total_matches_visited.max(1) as f64
}

// ── V2 — SIDE-TABLE per-consumer: per-match lookup ─────────────────────────
//
// Cache key is (consumer_id, subject_hash). Each match does one hash
// lookup. Correct per-consumer semantics.

fn bench_v2_side_table(subjects: &[u32], matches: &[Vec<u32>], seqs: &[u32]) -> f64 {
    let mut limits: HashMap<(u32, u32), u32, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(
            N_SUBJECTS * N_MATCHES,
            ahash::RandomState::new(),
        );
    // Pre-populate per (consumer, subject).
    for (s, m) in subjects.iter().zip(matches) {
        for &cid in m {
            limits.insert((cid, *s), 10);
        }
    }

    let mut counters: HashMap<(u32, u32), u32, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(
            N_SUBJECTS * N_MATCHES,
            ahash::RandomState::new(),
        );

    for _ in 0..1_000 {
        for &sidx in seqs.iter().take(100) {
            let subject = subjects[sidx as usize];
            for &cid in &matches[sidx as usize] {
                let _ = limits.get(&(cid, subject));
            }
        }
    }

    let start = Instant::now();
    let mut total_matches_visited = 0u64;
    for &sidx in seqs {
        let subject = subjects[sidx as usize];
        for &cid in &matches[sidx as usize] {
            // Per-match lookup — 1 hash op per match.
            let key = (cid, subject);
            let limit = limits.get(&key).copied();
            let skip = if let Some(max) = limit {
                let cur = counters.get(&key).copied().unwrap_or(0);
                cur >= max
            } else {
                false
            };
            if skip {
                continue;
            }
            black_box(cid);
            total_matches_visited += 1;
        }
    }
    let elapsed = start.elapsed();
    black_box(total_matches_visited);
    elapsed.as_nanos() as f64 / total_matches_visited.max(1) as f64
}

// ── V3 — ENRICHED MatchEntry: limit inline, zero lookups ───────────────────
//
// The MatchEntry struct carries `subject_limit: Option<u32>` — resolved at
// snapshot rebuild (cold path). Per-match check is a struct field read.

fn bench_v3_enriched(
    subjects: &[u32],
    matches: &[Vec<u32>],
    seqs: &[u32],
) -> (f64, usize) {
    // Build FatMatchEntry array per subject.
    let mut fat_matches: Vec<Vec<FatMatchEntry>> = Vec::with_capacity(N_SUBJECTS);
    for m in matches {
        let v: Vec<FatMatchEntry> = m
            .iter()
            .map(|&cid| FatMatchEntry {
                consumer_id: cid,
                connection_id: cid as u64 + 1_000,
                queue_id: cid,
                subscription_id: cid,
                binding_idx: cid,
                max_inflight: 1000,
                subject_limit: Some(10),
                flags: 0,
                _pad: [0; 3],
            })
            .collect();
        fat_matches.push(v);
    }

    // Counter still exists (subject-scoped per consumer). Same shape as V2.
    let mut counters: HashMap<(u32, u32), u32, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(
            N_SUBJECTS * N_MATCHES,
            ahash::RandomState::new(),
        );

    for _ in 0..1_000 {
        for &sidx in seqs.iter().take(100) {
            let subject = subjects[sidx as usize];
            for m in &fat_matches[sidx as usize] {
                black_box(m.subject_limit);
                let _ = counters.get(&(m.consumer_id, subject));
            }
        }
    }

    let start = Instant::now();
    let mut total_matches_visited = 0u64;
    for &sidx in seqs {
        let subject = subjects[sidx as usize];
        for m in &fat_matches[sidx as usize] {
            // Per-match — read limit from struct (no lookup).
            let skip = if let Some(max) = m.subject_limit {
                let cur = counters
                    .get(&(m.consumer_id, subject))
                    .copied()
                    .unwrap_or(0);
                cur >= max
            } else {
                false
            };
            if skip {
                continue;
            }
            black_box(m.consumer_id);
            total_matches_visited += 1;
        }
    }
    let elapsed = start.elapsed();
    black_box(total_matches_visited);
    (
        elapsed.as_nanos() as f64 / total_matches_visited.max(1) as f64,
        std::mem::size_of::<FatMatchEntry>(),
    )
}

// ── Dynamic-subject variants ────────────────────────────────────────────────
//
// Each "entry" has a FRESH subject_hash. The trie resolves on demand to the
// SAME pattern → SAME consumers → SAME subject_limit (because the pattern
// is one wildcard subscription). What changes is the COUNTER key: each hash
// is new, so each counter is fresh (0 → 1 → bounded by working set).

fn bench_v1_current_dynamic() -> f64 {
    let stream_id: u32 = 7;
    let consumers: Vec<u32> = (1..=N_MATCHES as u32).collect();

    // V1: per-entry cache keyed by (stream, hash). Every new hash is a
    // NEW cache entry — the resolve runs once, but the map grows.
    let mut cache: HashMap<(u32, u32), Option<u32>, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(N_ENTRIES_DYNAMIC, ahash::RandomState::new());
    let mut counters: HashMap<u32, u32, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(N_ENTRIES_DYNAMIC, ahash::RandomState::new());

    // Warmup a few hashes.
    for i in 0..1_000u32 {
        cache.insert((stream_id, i * 13 + 1), Some(10));
    }

    let start = Instant::now();
    let mut total_matches_visited = 0u64;
    for i in 0..N_ENTRIES_DYNAMIC {
        // Fresh hash per entry.
        let hash = (i as u32).wrapping_mul(0x9E37_79B9).wrapping_add(0xDEAD);
        // Simulate "cache miss → resolve → insert"
        let limit = *cache
            .entry((stream_id, hash))
            .or_insert(Some(10));
        let skip_entry = if let Some(max) = limit {
            let cur = counters.get(&hash).copied().unwrap_or(0);
            cur >= max
        } else {
            false
        };
        if skip_entry {
            continue;
        }
        for &cid in &consumers {
            black_box(cid);
            total_matches_visited += 1;
        }
    }
    let elapsed = start.elapsed();
    black_box(total_matches_visited);
    black_box(cache.len());
    elapsed.as_nanos() as f64 / total_matches_visited.max(1) as f64
}

fn bench_v2_side_table_dynamic() -> f64 {
    let consumers: Vec<u32> = (1..=N_MATCHES as u32).collect();
    let mut limits: HashMap<(u32, u32), u32, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(
            N_ENTRIES_DYNAMIC * N_MATCHES,
            ahash::RandomState::new(),
        );
    let mut counters: HashMap<(u32, u32), u32, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(
            N_ENTRIES_DYNAMIC * N_MATCHES,
            ahash::RandomState::new(),
        );

    let start = Instant::now();
    let mut total_matches_visited = 0u64;
    for i in 0..N_ENTRIES_DYNAMIC {
        let hash = (i as u32).wrapping_mul(0x9E37_79B9).wrapping_add(0xDEAD);
        for &cid in &consumers {
            let key = (cid, hash);
            // Entry-or-insert to simulate resolve + populate.
            let limit = *limits.entry(key).or_insert(10);
            let cur = counters.get(&key).copied().unwrap_or(0);
            if cur >= limit {
                continue;
            }
            black_box(cid);
            total_matches_visited += 1;
        }
    }
    let elapsed = start.elapsed();
    black_box(total_matches_visited);
    black_box(limits.len());
    elapsed.as_nanos() as f64 / total_matches_visited.max(1) as f64
}

fn bench_v3_enriched_dynamic() -> f64 {
    // For wildcard patterns the MatchEntry list is per-PATTERN (not per
    // concrete subject). The pattern resolves to the same 4 consumers
    // regardless of the hash. So `subject_limit` in the MatchEntry is
    // bakeable once per pattern.
    let fat_matches: Vec<FatMatchEntry> = (1..=N_MATCHES as u32)
        .map(|cid| FatMatchEntry {
            consumer_id: cid,
            connection_id: cid as u64 + 1_000,
            queue_id: cid,
            subscription_id: cid,
            binding_idx: cid,
            max_inflight: 1000,
            subject_limit: Some(10),
            flags: 0,
            _pad: [0; 3],
        })
        .collect();
    let mut counters: HashMap<(u32, u32), u32, ahash::RandomState> =
        HashMap::with_capacity_and_hasher(
            N_ENTRIES_DYNAMIC * N_MATCHES,
            ahash::RandomState::new(),
        );

    let start = Instant::now();
    let mut total_matches_visited = 0u64;
    for i in 0..N_ENTRIES_DYNAMIC {
        let hash = (i as u32).wrapping_mul(0x9E37_79B9).wrapping_add(0xDEAD);
        for m in &fat_matches {
            // Field access — no lookup.
            let skip = if let Some(max) = m.subject_limit {
                let cur = counters
                    .get(&(m.consumer_id, hash))
                    .copied()
                    .unwrap_or(0);
                cur >= max
            } else {
                false
            };
            if skip {
                continue;
            }
            black_box(m.consumer_id);
            total_matches_visited += 1;
        }
    }
    let elapsed = start.elapsed();
    black_box(total_matches_visited);
    elapsed.as_nanos() as f64 / total_matches_visited.max(1) as f64
}

fn main() {
    let fat_sz = std::mem::size_of::<FatMatchEntry>();
    let lean_sz = std::mem::size_of::<LeanMatchEntry>();

    println!();
    println!("========================================================");
    println!("       MatchEntry enrichment vs side-table bench");
    println!("========================================================");
    println!("  N_SUBJECTS={N_SUBJECTS}   N_MATCHES per subject={N_MATCHES}");
    println!("  N_ENTRIES={N_ENTRIES}   total match-checks={}", N_ENTRIES * N_MATCHES);
    println!();
    println!("  Sizes:  LeanMatchEntry = {lean_sz} B     FatMatchEntry = {fat_sz} B");
    println!();

    let (subjects, matches) = gen_workload();
    let seqs = gen_entry_seqs();

    println!("--------------------------------------------------------");
    println!("  ns per match-check (lower is better)");
    println!("--------------------------------------------------------");

    let t_v1 = bench_v1_current(&subjects, &matches, &seqs);
    println!("  V1  current (global per-stream cache, per-entry) : {t_v1:>5.2} ns/match-check");

    let t_v2 = bench_v2_side_table(&subjects, &matches, &seqs);
    println!("  V2  side-table (consumer, hash) per-match        : {t_v2:>5.2} ns/match-check");

    let (t_v3, fat_sz2) = bench_v3_enriched(&subjects, &matches, &seqs);
    println!("  V3  enriched MatchEntry (limit inline)           : {t_v3:>5.2} ns/match-check");

    println!();
    println!("--------------------------------------------------------");
    println!("  Memory footprint (per MatchEntry)");
    println!("--------------------------------------------------------");
    println!(
        "  lean (current): {lean_sz} B × N_matches = {:>6} B total",
        lean_sz * N_SUBJECTS * N_MATCHES
    );
    println!(
        "  fat (enriched): {fat_sz} B × N_matches = {:>6} B total (+{:>4} B vs lean)",
        fat_sz * N_SUBJECTS * N_MATCHES,
        (fat_sz - lean_sz) * N_SUBJECTS * N_MATCHES
    );

    println!();
    println!("--------------------------------------------------------");
    println!("  DYNAMIC subjects (fresh hash per entry, {N_ENTRIES_DYNAMIC} entries)");
    println!("--------------------------------------------------------");
    println!("  Each msg has a unique subject (e.g. `vip.user_N`). Trie");
    println!("  resolves to the same pattern, but counter/cache keys grow.");
    println!();

    let t_v1_dyn = bench_v1_current_dynamic();
    println!("  V1  current (global cache grows per fresh hash)  : {t_v1_dyn:>5.2} ns/match-check");

    let t_v2_dyn = bench_v2_side_table_dynamic();
    println!("  V2  side-table per-match (grows per fresh hash)  : {t_v2_dyn:>5.2} ns/match-check");

    let t_v3_dyn = bench_v3_enriched_dynamic();
    println!("  V3  enriched MatchEntry (counter grows only)     : {t_v3_dyn:>5.2} ns/match-check");

    println!();
    println!("--------------------------------------------------------");
    println!("  Summary");
    println!("--------------------------------------------------------");
    println!("  STATIC working set:");
    println!(
        "    V2/V1 = {:.2}x    V3/V1 = {:.2}x    V2/V3 = {:.2}x",
        t_v2 / t_v1,
        t_v3 / t_v1,
        t_v2 / t_v3
    );
    println!("  DYNAMIC (fresh hash each msg):");
    println!(
        "    V2/V1 = {:.2}x    V3/V1 = {:.2}x    V2/V3 = {:.2}x",
        t_v2_dyn / t_v1_dyn,
        t_v3_dyn / t_v1_dyn,
        t_v2_dyn / t_v3_dyn
    );
    println!();
}
