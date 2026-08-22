//! What a publish actually pays, step by step — and what the hashing in it
//! costs.
//!
//! The mental model for a publish is "check the stream exists in a map,
//! then store it". That is right for a plain stream, but the path has two
//! more steps that only appear when the stream declares them, and they are
//! not free. This measures each piece separately so the model can be
//! checked against numbers instead of assumed.
//!
//! ## The steps, in the order `v2_publish` performs them
//!
//! 1. one catalog snapshot — a single `arc_swap` guard for everything below
//! 2. `stream_seq(wire_id)` — wire id to engine id. **A HASH lookup**, the
//!    only one on the path, because wire ids are sparse and client-chosen
//! 3. `stream_idempotency_window_ms` — a `Vec` index. Zero means skip
//! 4. dedup record, ONLY if the window is non-zero and the message carries
//!    a msg-id — takes a per-stream mutex
//! 5. `stream_quota` — a `Vec` index. Only a stream with
//!    `DiscardPolicy::New` goes further and reads store stats
//! 6. append + gate
//!
//! ## The hashed vs unhashed question
//!
//! Step 2 is the only hash. Wire ids are `u32` chosen by the client and
//! sparse, so the catalog hashes them. `indexed` shows what the same lookup
//! costs if the id were dense enough to index a `Vec` directly — the
//! difference is what hashing buys the sparse key space.
//!
//! `identity_hash` is the middle ground: keep the map, drop the hashing by
//! using the id as its own hash. Faster, and unsafe in a way worth naming —
//! a client picks these ids, so an identity hash hands it direct control of
//! bucket placement and therefore of collisions.
//!
//! The dedup tracker is MODELLED here, not the real one: it lives in
//! `arbitro-server` and this crate cannot reach it. What is modelled is its
//! shape — a mutex around a map keyed by `(stream, msg_id_hash)` — so the
//! number says what that shape costs, not what that exact type costs.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::hint::black_box;
use std::sync::Mutex;
use std::time::Instant;

use arbitro_common::name_registry::NameRegistry;
use arbitro_engine_v2::types::StreamId;

const STREAMS: u32 = 4096;
const ITERS: usize = 2_000_000;

/// Uses the value as its own hash. No mixing at all.
#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 << 8) | b as u64;
        }
    }
    fn write_u32(&mut self, v: u32) {
        self.0 = v as u64;
    }
}

fn report(name: &str, e: std::time::Duration, iters: usize) -> f64 {
    let ns = e.as_nanos() as f64 / iters as f64;
    println!("  {name:<22} {ns:>8.2} ns/op");
    ns
}

fn main() {
    println!("\n── the publish path, step by step ──\n");
    println!("  {STREAMS} streams, {ITERS} iterations\n");

    // Wire ids as a client would choose them: sparse, not 0..N.
    let wire: Vec<u32> = (0..STREAMS).map(|i| i.wrapping_mul(2_654_435_761)).collect();

    // ── Step 2, three ways ───────────────────────────────────────────
    let mut hashed: HashMap<u32, u32, foldhash::fast::FixedState> = HashMap::default();
    let mut identity: HashMap<u32, u32, BuildHasherDefault<IdentityHasher>> = HashMap::default();
    for (i, w) in wire.iter().enumerate() {
        hashed.insert(*w, i as u32);
        identity.insert(*w, i as u32);
    }
    // The unhashed alternative: a dense id indexing a Vec. Only possible
    // if the id is assigned by the broker rather than the client.
    let indexed: Vec<u32> = (0..STREAMS).collect();

    let mut acc = 0u64;

    let t = Instant::now();
    for i in 0..ITERS {
        let w = wire[i % wire.len()];
        acc += *hashed.get(black_box(&w)).unwrap_or(&0) as u64;
    }
    let h = report("seq: foldhash", t.elapsed(), ITERS);

    let t = Instant::now();
    for i in 0..ITERS {
        let w = wire[i % wire.len()];
        acc += *identity.get(black_box(&w)).unwrap_or(&0) as u64;
    }
    let id = report("seq: identity hash", t.elapsed(), ITERS);

    let t = Instant::now();
    for i in 0..ITERS {
        let d = (i % STREAMS as usize) as usize;
        acc += *indexed.get(black_box(d)).unwrap_or(&0) as u64;
    }
    let ix = report("seq: Vec index", t.elapsed(), ITERS);

    println!(
        "\n  hashing costs {:.2} ns over an index; identity saves {:.2} ns\n",
        h - ix,
        h - id
    );

    // ── Steps 3 and 5: the Vec-indexed reads, off the real registry ──
    let reg = NameRegistry::new();
    for i in 0..STREAMS {
        reg.set_stream_shard(StreamId(i), (i % 8) as u16);
    }
    let ids: Vec<StreamId> = (0..STREAMS).map(StreamId).collect();

    let t = Instant::now();
    for i in 0..ITERS {
        let s = ids[i % ids.len()];
        let cat = reg.snapshot();
        acc += cat.stream_idempotency_window_ms(s) as u64;
        acc += cat.stream_quota(s).map(|q| q.max_msgs).unwrap_or(0);
    }
    report("window+quota (guard/op)", t.elapsed(), ITERS);

    let t = Instant::now();
    for i in 0..ITERS {
        let s = ids[i % ids.len()];
        let cat = reg.snapshot();
        acc += cat.stream_seq(black_box(wire[i % wire.len()]))
            .map(|v| v.0 as u64)
            .unwrap_or(0);
        acc += cat.stream_idempotency_window_ms(s) as u64;
        acc += cat.stream_quota(s).map(|q| q.max_msgs).unwrap_or(0);
    }
    let minimal = report("PUBLISH: no dedup/quota", t.elapsed(), ITERS);

    // ── Step 4: the dedup record, modelled ───────────────────────────
    // Per-stream mutex around a map keyed by (stream, msg_id hash) — the
    // shape `IdempotencyTracker::record` has.
    const DEDUP_ITERS: usize = 500_000;
    let tracker: Mutex<HashMap<(u32, u64), u64, foldhash::fast::FixedState>> =
        Mutex::new(HashMap::default());

    let t = Instant::now();
    for i in 0..DEDUP_ITERS {
        let s = (i % STREAMS as usize) as u32;
        let msg_hash = i as u64;
        let mut g = tracker.lock().unwrap();
        // `record` returns false on a duplicate; the insert IS the check.
        g.insert(black_box((s, msg_hash)), 0);
        // Bound it so this measures steady-state cost, not growth.
        if g.len() > 100_000 {
            g.clear();
        }
    }
    let dedup = report("dedup record (modelled)", t.elapsed(), DEDUP_ITERS);

    println!(
        "\n  a stream WITH dedup pays {:.2} ns more per publish — {:.1}x the\n  \
         minimal path\n",
        dedup,
        (minimal + dedup) / minimal
    );
    println!("  (checksum {acc})\n");

    scale_sweep();
    msg_id_sweep();
}

/// What dedup costs as the window fills.
///
/// A per-stream flag is indexable — the stream id IS the offset, so the
/// previous sweep came out flat. A `msg_id` cannot be: it is a
/// client-chosen byte string, so it must be hashed, and the table holds
/// every id still inside the dedup window. That population is
/// `throughput x window`, not `stream count`, and it is the number that
/// actually grows: 1M msg/s with a 60s window is 60M live ids.
///
/// So the shapes compared here are not "hash vs index" — there is no index
/// available. They are the parts of the real cost:
///
///   - `hash only`     — hashing the id, no table at all. The floor.
///   - `lookup (miss)` — the common case: a fresh id, not a duplicate.
///   - `lookup (hit)`  — a real duplicate, which also memcmp's the stored
///     bytes, because a hash match alone is not proof of equality.
///   - `record`        — the insert that follows a miss.
///   - `+ mutex`       — the same insert through the per-stream lock the
///     tracker actually holds.
fn msg_id_sweep() {
    use std::hash::BuildHasher;

    println!("── dedup as the window fills ──\n");
    println!("  live ids     hash only   miss    hit    record   +mutex");

    const ROUNDS: usize = 500_000;
    let hasher = foldhash::fast::FixedState::default();

    // Ids shaped like a client's: a prefix plus a counter, so they share
    // leading bytes and the memcmp on a hit is not decided by byte one.
    let make_id = |i: usize| format!("order-{i:016}-evt").into_bytes();

    for &live in &[1_000usize, 100_000, 1_000_000] {
        let mut table: HashMap<u64, Vec<u8>, foldhash::fast::FixedState> = HashMap::default();
        for i in 0..live {
            let id = make_id(i);
            table.insert(hasher.hash_one(&id), id);
        }

        let probe: Vec<Vec<u8>> = (0..1024).map(|i| make_id(i * 7 % live)).collect();
        let fresh: Vec<Vec<u8>> = (0..1024).map(|i| make_id(live + i)).collect();
        let mut acc = 0u64;

        let t = Instant::now();
        for i in 0..ROUNDS {
            acc += hasher.hash_one(black_box(&fresh[i % fresh.len()]));
        }
        let h = t.elapsed().as_nanos() as f64 / ROUNDS as f64;

        let t = Instant::now();
        for i in 0..ROUNDS {
            let id = &fresh[i % fresh.len()];
            acc += table.get(&hasher.hash_one(black_box(id))).is_some() as u64;
        }
        let miss = t.elapsed().as_nanos() as f64 / ROUNDS as f64;

        let t = Instant::now();
        for i in 0..ROUNDS {
            let id = &probe[i % probe.len()];
            // A hit compares the stored bytes: equal hashes are not equal
            // ids, and treating them as such would drop a legitimate
            // publish as a duplicate.
            if let Some(stored) = table.get(&hasher.hash_one(black_box(id))) {
                acc += (stored.as_slice() == id.as_slice()) as u64;
            }
        }
        let hit = t.elapsed().as_nanos() as f64 / ROUNDS as f64;

        let mut sink: HashMap<u64, Vec<u8>, foldhash::fast::FixedState> = HashMap::default();
        let t = Instant::now();
        for i in 0..ROUNDS {
            let id = &fresh[i % fresh.len()];
            sink.insert(hasher.hash_one(black_box(id)) ^ i as u64, id.clone());
            if sink.len() > 200_000 {
                sink.clear();
            }
        }
        let rec = t.elapsed().as_nanos() as f64 / ROUNDS as f64;

        let locked: Mutex<HashMap<u64, Vec<u8>, foldhash::fast::FixedState>> =
            Mutex::new(HashMap::default());
        let t = Instant::now();
        for i in 0..ROUNDS {
            let id = &fresh[i % fresh.len()];
            let mut g = locked.lock().unwrap();
            g.insert(hasher.hash_one(black_box(id)) ^ i as u64, id.clone());
            if g.len() > 200_000 {
                g.clear();
            }
        }
        let lk = t.elapsed().as_nanos() as f64 / ROUNDS as f64;

        println!(
            "  {live:>9}   {h:>7.2}  {miss:>6.2} {hit:>6.2}  {rec:>7.2}  {lk:>7.2}"
        );
        let _ = acc;
    }
    println!();
}

/// Does an indexed per-stream flag stay cheap as the catalog grows?
///
/// "65k streams with a boolean is cheap" is right on memory — 64 KB as
/// bytes, 8 KB as a bitmap — but memory is not the cost that bites. The
/// cost is whether the read still hits cache once the array outgrows L2,
/// and that only shows up by growing it.
///
/// Three shapes at each size:
///   - `Vec<bool>`  — one byte per stream, what the catalog does today for
///     its flag-like fields
///   - bitmap       — one BIT per stream, 8x denser, one extra shift+mask
///   - `Vec<u32>`   — four bytes per stream, the shape
///     `streams_idempotency_window_ms` actually uses
///
/// Access is a strided walk, not sequential: a publish touches whichever
/// stream the message names, so a sequential scan would report the
/// prefetcher's number instead of the lookup's.
fn scale_sweep() {
    println!("── per-stream flag as the catalog grows ──\n");
    println!("  streams      Vec<bool>      bitmap     Vec<u32>       bytes");

    const ROUNDS: usize = 2_000_000;
    // Coprime with every power-of-two size below, so the walk visits every
    // slot without ever being sequential.
    const STRIDE: usize = 7919;

    for &n in &[4_096usize, 65_536, 1_048_576] {
        let flags: Vec<bool> = (0..n).map(|i| i % 3 == 0).collect();
        let words: Vec<u64> = (0..n.div_ceil(64))
            .map(|w| {
                let mut v = 0u64;
                for b in 0..64 {
                    if (w * 64 + b) % 3 == 0 {
                        v |= 1 << b;
                    }
                }
                v
            })
            .collect();
        let windows: Vec<u32> = (0..n).map(|i| (i % 7) as u32).collect();

        let mut acc = 0u64;
        let mut idx = 0usize;

        let t = Instant::now();
        for _ in 0..ROUNDS {
            idx = (idx + STRIDE) % n;
            acc += flags[black_box(idx)] as u64;
        }
        let b = t.elapsed().as_nanos() as f64 / ROUNDS as f64;

        let t = Instant::now();
        for _ in 0..ROUNDS {
            idx = (idx + STRIDE) % n;
            let i = black_box(idx);
            acc += ((words[i >> 6] >> (i & 63)) & 1) as u64;
        }
        let bm = t.elapsed().as_nanos() as f64 / ROUNDS as f64;

        let t = Instant::now();
        for _ in 0..ROUNDS {
            idx = (idx + STRIDE) % n;
            acc += windows[black_box(idx)] as u64;
        }
        let w = t.elapsed().as_nanos() as f64 / ROUNDS as f64;

        println!(
            "  {n:>9}   {b:>8.2} ns  {bm:>8.2} ns  {w:>8.2} ns   {:>6} KB",
            n * 4 / 1024
        );
        let _ = acc;
    }
    println!();
}
