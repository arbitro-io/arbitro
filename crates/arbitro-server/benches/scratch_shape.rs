//! TEMPORARY. One prefixed map, or the split containers we have now?
//!
//! The drain's per-cycle scratch is six containers that are all really
//! key→value maps; four of them fake it with a `Vec` and a linear scan
//! because N is small. Every cycle they are cleared. The question is
//! whether ONE map with a tagged key beats six specialised ones.
//!
//! Modelled on a real cycle, from the drain profile:
//!   256 entries, 9 recipients each, a handful of consumers/connections,
//!   then everything cleared and the next cycle starts.
//!
//! Ops per cycle, per the shapes in drain.rs. NOTE the two clear rates —
//! `matches` and `served_queues` are emptied on EVERY MESSAGE, so they are
//! cleared 256x more often than the rest:
//!
//!   per MESSAGE:
//!     matches        list of recipients     (clear, fill with 9, iterate)
//!     served_queues  queue_id -> membership (clear, then checks)
//!   per CYCLE:
//!     local_inflight   consumer_id            -> counter   (inc, then get)
//!     local_subject    (consumer, subj_hash)  -> counter   (inc)
//!     acc.index        (conn, stream, fanout) -> bucket ix (get-or-insert)
//!     flush_results    conn                   -> outcome   (push, then get)
//!     dead_connections conn                   -> membership
//!
//! Shapes compared:
//!   split   — what the drain does today, each container as it is
//!   onemap  — a single HashMap<Key, u64> with a tag byte in the key
//!   onevec  — a single Vec<(Key, u64)> + linear scan, same tagged key
//!   gen     — split containers that are NEVER cleared: a generation
//!             counter invalidates stale entries, as `fanout_stamp`
//!             already does in the drain today
//!
//! Delete once the shape is chosen.

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

/// Entries the drain takes per cycle — ARBITRO_MAX_FEED_PER_CYCLE.
const FEED: usize = 256;
/// Recipients per entry in the fanout bench (3 clients x 3 consumers).
const RECIPIENTS: usize = 9;
/// Distinct consumers / connections / streams a cycle touches.
const CONSUMERS: u32 = 9;
const CONNS: u32 = 3;
const QUEUES: u32 = 3;
/// Cycles per measurement. The fanout bench ran ~8500. Kept modest so a
/// mistake in one shape costs seconds, not minutes.
const CYCLES: usize = 4_000;

/// Which logical map an entry belongs to. The "prefix" of the key.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
enum Tag {
    Inflight = 0,
    Subject = 1,
    Bucket = 2,
    Flush = 3,
    Queue = 4,
    Dead = 5,
    Match = 6,
}

/// One recipient in `scratch.matches`. Rebuilt for every message.
#[derive(Clone, Copy, PartialEq)]
struct Match {
    consumer: u32,
    binding: u32,
    queue: u32,
}

/// One tagged key covering every container's key shape. The widest is
/// `(consumer, subject_hash)`, so two u32s plus the tag is enough.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Key(Tag, u32, u32);

type Fold = foldhash::fast::FixedState;

// ── shape 1: split, as the drain does it today ──────────────────────────────

#[derive(Default)]
struct Split {
    inflight: Vec<(u32, u32)>,
    subject: HashMap<(u32, u32), u32, Fold>,
    buckets: HashMap<(u32, u32, bool), usize, Fold>,
    flush: Vec<(u32, u8)>,
    queues: Vec<u32>,
    dead: Vec<u32>,
    matches: Vec<Match>,
}

impl Split {
    /// Per-CYCLE reset — mirrors `reset_cycle` in drain.rs.
    fn clear(&mut self) {
        self.inflight.clear();
        self.subject.clear();
        self.buckets.clear();
        self.flush.clear();
        self.dead.clear();
    }

    /// Per-MESSAGE reset. 256x more frequent than `clear`.
    #[inline]
    fn clear_entry(&mut self) {
        self.matches.clear();
        self.queues.clear();
    }

    #[inline]
    fn inflight_inc(&mut self, consumer: u32) {
        for e in self.inflight.iter_mut() {
            if e.0 == consumer {
                e.1 += 1;
                return;
            }
        }
        self.inflight.push((consumer, 1));
    }

    #[inline]
    fn inflight_get(&self, consumer: u32) -> u32 {
        for &(k, v) in self.inflight.iter() {
            if k == consumer {
                return v;
            }
        }
        0
    }

    #[inline]
    fn flush_get(&self, conn: u32) -> bool {
        for &(c, o) in self.flush.iter() {
            if c == conn {
                return o == 0;
            }
        }
        false
    }
}

// ── shape 2: one HashMap, tagged key ────────────────────────────────────────

#[derive(Default)]
struct OneMap {
    m: HashMap<Key, u64, Fold>,
}

impl OneMap {
    #[inline]
    fn bump(&mut self, k: Key) {
        *self.m.entry(k).or_insert(0) += 1;
    }
    #[inline]
    fn set(&mut self, k: Key, v: u64) {
        self.m.insert(k, v);
    }
    #[inline]
    fn get(&self, k: Key) -> u64 {
        self.m.get(&k).copied().unwrap_or(0)
    }
    #[inline]
    fn has(&self, k: Key) -> bool {
        self.m.contains_key(&k)
    }
    fn clear(&mut self) {
        self.m.clear();
    }
}

// ── shape 3: one Vec, tagged key, linear scan ───────────────────────────────

#[derive(Default)]
struct OneVec {
    v: Vec<(Key, u64)>,
}

impl OneVec {
    #[inline]
    fn bump(&mut self, k: Key) {
        for e in self.v.iter_mut() {
            if e.0 == k {
                e.1 += 1;
                return;
            }
        }
        self.v.push((k, 1));
    }
    #[inline]
    fn set(&mut self, k: Key, val: u64) {
        for e in self.v.iter_mut() {
            if e.0 == k {
                e.1 = val;
                return;
            }
        }
        self.v.push((k, val));
    }
    #[inline]
    fn get(&self, k: Key) -> u64 {
        for &(kk, v) in self.v.iter() {
            if kk == k {
                return v;
            }
        }
        0
    }
    #[inline]
    fn has(&self, k: Key) -> bool {
        self.v.iter().any(|e| e.0 == k)
    }
    fn clear(&mut self) {
        self.v.clear();
    }
}

// ── shape 4: split, never cleared — a generation invalidates ────────────────

/// Same containers, but every value carries the cycle it was written in.
/// A stale generation reads as absent, so `clear()` disappears entirely.
/// This is what `fanout_stamp` already does inside the drain.
#[derive(Default)]
struct Gen {
    gen: u64,
    /// Per-MESSAGE generation — advances 256x more often than `gen`.
    /// This is exactly what `fanout_stamp`/`fanout_gen` already do in the
    /// drain today, applied to the two per-message containers.
    egen: u64,
    inflight: Vec<(u32, u32, u64)>,
    subject: HashMap<(u32, u32), (u32, u64), Fold>,
    buckets: HashMap<(u32, u32, bool), (usize, u64), Fold>,
    flush: Vec<(u32, u8, u64)>,
    queues: Vec<(u32, u64)>,
    dead: Vec<(u32, u64)>,
    /// `matches` is a LIST, not a set: it is filled then walked in order,
    /// so a generation cannot replace clearing it. What it can do is avoid
    /// re-zeroing — track how many slots are live this message instead.
    matches: Vec<Match>,
    matches_len: usize,
}

impl Gen {
    #[inline]
    fn next_cycle(&mut self) {
        self.gen += 1;
    }

    #[inline]
    fn inflight_inc(&mut self, consumer: u32) {
        let g = self.gen;
        for e in self.inflight.iter_mut() {
            if e.0 == consumer {
                // Stale entry from an older cycle restarts at 1.
                if e.2 == g {
                    e.1 += 1;
                } else {
                    e.1 = 1;
                    e.2 = g;
                }
                return;
            }
        }
        self.inflight.push((consumer, 1, g));
    }

    #[inline]
    fn inflight_get(&self, consumer: u32) -> u32 {
        for &(k, v, g) in self.inflight.iter() {
            if k == consumer && g == self.gen {
                return v;
            }
        }
        0
    }

    #[inline]
    fn flush_get(&self, conn: u32) -> bool {
        for &(c, o, g) in self.flush.iter() {
            if c == conn && g == self.gen {
                return o == 0;
            }
        }
        false
    }
}

/// Generation-set insert: find the slot by KEY ALONE and stamp it with the
/// current generation. Matching on (key, gen) instead never finds a stale
/// slot, so it pushes a duplicate every cycle and the scan degrades to
/// O(n^2) — the whole point of a generation is that the slot is reused.
#[inline]
fn claim(v: &mut Vec<(u32, u64)>, key: u32, g: u64) {
    match v.iter_mut().find(|e| e.0 == key) {
        Some(e) => e.1 = g,
        None => v.push((key, g)),
    }
}

// ── the workload ────────────────────────────────────────────────────────────

fn run_split(s: &mut Split) -> u64 {
    let mut acc = 0u64;
    for _ in 0..CYCLES {
        s.clear();
        for e in 0..FEED {
            let stream = (e as u32) % 2;
            // Per-message reset, and the match list rebuilt from scratch.
            s.clear_entry();
            for r in 0..RECIPIENTS {
                s.matches.push(Match {
                    consumer: (r as u32) % CONSUMERS,
                    binding: r as u32,
                    queue: (r as u32) % QUEUES,
                });
            }
            for i in 0..s.matches.len() {
                let m = s.matches[i];
                let (consumer, conn) = (m.consumer, m.binding % CONNS);
                let subj = (e as u32).wrapping_mul(2654435761);
                s.inflight_inc(consumer);
                *s.subject.entry((consumer, subj)).or_insert(0) += 1;
                let n = s.buckets.len();
                s.buckets.entry((conn, stream, false)).or_insert(n);
                if !s.queues.contains(&m.queue) {
                    s.queues.push(m.queue);
                }
                acc += s.inflight_get(consumer) as u64;
            }
        }
        for c in 0..CONNS {
            s.flush.push((c, 0));
        }
        for c in 0..CONNS {
            acc += s.flush_get(c) as u64;
            if !s.dead.contains(&c) {
                s.dead.push(c);
            }
        }
    }
    acc
}

fn run_onemap(s: &mut OneMap) -> u64 {
    let mut acc = 0u64;
    for _ in 0..CYCLES {
        s.clear();
        for e in 0..FEED {
            let stream = (e as u32) % 2;
            // Per-message: the match list and the queue set are re-keyed.
            // In one map that means removing every entry with those tags —
            // the price of merging containers with different lifetimes.
            s.m.retain(|k, _| k.0 != Tag::Match && k.0 != Tag::Queue);
            for r in 0..RECIPIENTS {
                s.set(Key(Tag::Match, r as u32, (r as u32) % CONSUMERS), r as u64);
            }
            for r in 0..RECIPIENTS {
                let consumer = (r as u32) % CONSUMERS;
                let conn = (r as u32) % CONNS;
                let subj = (e as u32).wrapping_mul(2654435761);
                s.bump(Key(Tag::Inflight, consumer, 0));
                s.bump(Key(Tag::Subject, consumer, subj));
                let k = Key(Tag::Bucket, conn, stream);
                if !s.has(k) {
                    let n = s.m.len() as u64;
                    s.set(k, n);
                }
                let q = (r as u32) % QUEUES;
                let qk = Key(Tag::Queue, q, 0);
                if !s.has(qk) {
                    s.set(qk, 1);
                }
                acc += s.get(Key(Tag::Inflight, consumer, 0));
            }
        }
        for c in 0..CONNS {
            s.set(Key(Tag::Flush, c, 0), 0);
        }
        for c in 0..CONNS {
            acc += (s.get(Key(Tag::Flush, c, 0)) == 0) as u64;
            let dk = Key(Tag::Dead, c, 0);
            if !s.has(dk) {
                s.set(dk, 1);
            }
        }
    }
    acc
}

fn run_onevec(s: &mut OneVec) -> u64 {
    let mut acc = 0u64;
    for _ in 0..CYCLES {
        s.clear();
        for e in 0..FEED {
            let stream = (e as u32) % 2;
            s.v.retain(|(k, _)| k.0 != Tag::Match && k.0 != Tag::Queue);
            for r in 0..RECIPIENTS {
                s.set(Key(Tag::Match, r as u32, (r as u32) % CONSUMERS), r as u64);
            }
            for r in 0..RECIPIENTS {
                let consumer = (r as u32) % CONSUMERS;
                let conn = (r as u32) % CONNS;
                let subj = (e as u32).wrapping_mul(2654435761);
                s.bump(Key(Tag::Inflight, consumer, 0));
                s.bump(Key(Tag::Subject, consumer, subj));
                let k = Key(Tag::Bucket, conn, stream);
                if !s.has(k) {
                    let n = s.v.len() as u64;
                    s.set(k, n);
                }
                let q = (r as u32) % QUEUES;
                let qk = Key(Tag::Queue, q, 0);
                if !s.has(qk) {
                    s.set(qk, 1);
                }
                acc += s.get(Key(Tag::Inflight, consumer, 0));
            }
        }
        for c in 0..CONNS {
            s.set(Key(Tag::Flush, c, 0), 0);
        }
        for c in 0..CONNS {
            acc += (s.get(Key(Tag::Flush, c, 0)) == 0) as u64;
            let dk = Key(Tag::Dead, c, 0);
            if !s.has(dk) {
                s.set(dk, 1);
            }
        }
    }
    acc
}

fn run_gen(s: &mut Gen) -> u64 {
    let mut acc = 0u64;
    for _ in 0..CYCLES {
        s.next_cycle(); // instead of clear()
        let g = s.gen;
        for e in 0..FEED {
            let stream = (e as u32) % 2;
            // Per-message: bump the entry generation instead of clearing,
            // and overwrite the match slots in place instead of push/clear.
            s.egen += 1;
            let eg = s.egen;
            s.matches_len = RECIPIENTS;
            if s.matches.len() < RECIPIENTS {
                s.matches.resize(
                    RECIPIENTS,
                    Match {
                        consumer: 0,
                        binding: 0,
                        queue: 0,
                    },
                );
            }
            for r in 0..RECIPIENTS {
                s.matches[r] = Match {
                    consumer: (r as u32) % CONSUMERS,
                    binding: r as u32,
                    queue: (r as u32) % QUEUES,
                };
            }
            for i in 0..s.matches_len {
                let m = s.matches[i];
                let (consumer, conn) = (m.consumer, m.binding % CONNS);
                let r = i;
                let subj = (e as u32).wrapping_mul(2654435761);
                s.inflight_inc(consumer);
                let ent = s.subject.entry((consumer, subj)).or_insert((0, g));
                if ent.1 == g {
                    ent.0 += 1;
                } else {
                    *ent = (1, g);
                }
                let n = s.buckets.len();
                let b = s.buckets.entry((conn, stream, false)).or_insert((n, g));
                b.1 = g;
                // Per-message set: the ENTRY generation invalidates it, so
                // it is never cleared either.
                let q = (r as u32) % QUEUES;
                claim(&mut s.queues, q, eg);
                acc += s.inflight_get(consumer) as u64;
            }
        }
        for c in 0..CONNS {
            // Reuse the slot. Pushing per cycle is what made this shape
            // O(n^2) on the first attempt: the key space is bounded, so
            // the slot must be found by KEY and its generation refreshed.
            match s.flush.iter_mut().find(|e| e.0 == c) {
                Some(e) => {
                    e.1 = 0;
                    e.2 = g;
                }
                None => s.flush.push((c, 0, g)),
            }
        }
        for c in 0..CONNS {
            acc += s.flush_get(c) as u64;
            claim(&mut s.dead, c, g);
        }
    }
    acc
}

fn bench(name: &str, f: impl FnOnce() -> u64) {
    let t = Instant::now();
    let out = black_box(f());
    let ns = t.elapsed().as_nanos() as f64;
    let ops = (CYCLES * FEED * RECIPIENTS) as f64;
    println!(
        "  {name:<8} {:>9.1} ms  {:>8.1} ns/recipient  {:>9.1} ns/cycle   (checksum {out})",
        ns / 1e6,
        ns / ops,
        ns / CYCLES as f64,
    );
}

fn main() {
    println!(
        "{CYCLES} cycles x {FEED} entries x {RECIPIENTS} recipients \
         = {} recipient-ops\n",
        CYCLES * FEED * RECIPIENTS
    );
    println!("  shape        total          per recipient       per cycle");
    println!("  {}", "-".repeat(72));

    // `gen` never frees, so it grows unbounded across cycles unless the
    // key space is bounded — it is here, exactly as in the drain.
    bench("split", || run_split(&mut Split::default()));
    bench("onemap", || run_onemap(&mut OneMap::default()));
    bench("onevec", || run_onevec(&mut OneVec::default()));
    bench("gen", || run_gen(&mut Gen::default()));

    println!(
        "\n  split  = today: six containers, six clear() per cycle\n  \
           onemap = one HashMap, tag byte in the key, one clear()\n  \
           onevec = one Vec + linear scan, same tagged key\n  \
           gen    = six containers, ZERO clear() — a generation invalidates"
    );
}
