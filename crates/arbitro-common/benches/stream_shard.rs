//! Cost of ONE shard resolution — `NameRegistry::stream_shard()`.
//!
//! This is what every `sink_for` / `store_for` / `gate_for` on the publish
//! path pays. The question it answers is whether hoisting the sink inside
//! the publish functions (so the quota pre-check and the append share one
//! resolution instead of taking two or three) is worth touching three
//! hot-path functions.
//!
//! Measured against the alternative it replaced — `stream_id % shard_count`
//! — so the number is read as "what recording placement costs", not as an
//! absolute nobody can calibrate.

use arbitro_common::name_registry::NameRegistry;
use arbitro_engine_v2::types::StreamId;
use std::time::Instant;

const N_STREAMS: u32 = 4096;
const N_ROUNDS: usize = 5_000_000;
const SHARDS: usize = 16;

fn main() {
    let reg = NameRegistry::new();
    for i in 0..N_STREAMS {
        reg.set_stream_shard(StreamId(i), (i as usize % SHARDS) as u16);
    }

    // Touch every stream in a rotation rather than one id repeatedly: a
    // single hot id would sit in L1 and report a cache hit, not a lookup.
    let ids: Vec<StreamId> = (0..N_STREAMS).map(StreamId).collect();

    // Warm the ArcSwap snapshot and the pages behind it.
    let mut acc = 0u64;
    for id in &ids {
        acc += reg.stream_shard(*id).unwrap_or(0) as u64;
    }

    let t = Instant::now();
    for r in 0..N_ROUNDS {
        let id = ids[r % ids.len()];
        acc += reg.stream_shard(id).unwrap_or(0) as u64;
    }
    let recorded = t.elapsed();

    let t = Instant::now();
    for r in 0..N_ROUNDS {
        let id = ids[r % ids.len()];
        acc += (id.0 as usize % SHARDS) as u64;
    }
    let modulo = t.elapsed();

    println!("\n── shard resolution, {N_ROUNDS} lookups over {N_STREAMS} streams ──");
    println!(
        "  recorded (stream_shard) : {:>8.2} ns/lookup",
        recorded.as_nanos() as f64 / N_ROUNDS as f64
    );
    println!(
        "  modulo   (stream_id % N): {:>8.2} ns/lookup",
        modulo.as_nanos() as f64 / N_ROUNDS as f64
    );
    println!(
        "  delta                   : {:>8.2} ns/lookup",
        (recorded.as_nanos() as f64 - modulo.as_nanos() as f64) / N_ROUNDS as f64
    );

    // Where does that delta go — the ArcSwap guard, or the indexing? Every
    // `stream_shard` / `stream_quota` / `stream_wire` call takes its OWN
    // guard, and the publish path makes several per frame. If the guard
    // dominates, the fix is a snapshot handle held for the frame, not
    // shaving individual call sites.
    let swap: arc_swap::ArcSwap<Vec<u16>> =
        arc_swap::ArcSwap::from_pointee((0..N_STREAMS).map(|i| (i % 16) as u16).collect());

    let t = Instant::now();
    for r in 0..N_ROUNDS {
        acc += swap.load().get(r % N_STREAMS as usize).copied().unwrap_or(0) as u64;
    }
    let per_call_guard = t.elapsed();

    let t = Instant::now();
    {
        let snap = swap.load();
        for r in 0..N_ROUNDS {
            acc += snap.get(r % N_STREAMS as usize).copied().unwrap_or(0) as u64;
        }
    }
    let one_guard = t.elapsed();

    println!("\n── where the cost lives ──");
    println!(
        "  guard per lookup        : {:>8.2} ns",
        per_call_guard.as_nanos() as f64 / N_ROUNDS as f64
    );
    println!(
        "  one guard, N indexes    : {:>8.2} ns",
        one_guard.as_nanos() as f64 / N_ROUNDS as f64
    );
    // Keep the accumulator observable so neither loop is optimized away.
    println!("  (checksum {acc})");
}
