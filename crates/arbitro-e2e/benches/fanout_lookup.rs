//! Bench: fanout subscription lookup strategies.
//!
//! Scenario: 30 subs, 3 consumers (10 subs each), all on same stream.
//! Only consumer 1 is receiving messages in this batch.
//!
//! A) **Indexed**: `HashMap<stream_id, HashMap<consumer_id, Vec<Sub>>>` — 2 lookups, iterate 10 subs × 256 entries.
//! B) **Flat scan**: `Vec<Sub>` — scan all 30, filter by stream_id + consumer_id, 256 entries.
//! C) **Indexed stream only**: `HashMap<stream_id, Vec<Sub>>` — 1 lookup, scan 30 subs filtering consumer_id, 256 entries.
//!
//! 1M batches of 256 entries.

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

const CONSUMERS: u32 = 3;
const SUBS_PER_CONSUMER: usize = 10;
const SUBS: usize = CONSUMERS as usize * SUBS_PER_CONSUMER;
const BATCH_SIZE: usize = 256;
const ITERS: usize = 1_000_000;

#[derive(Clone)]
struct Sub {
    stream_id: u32,
    consumer_id: u32,
    filter_hash: u32,
}

struct Entry {
    seq: u64,
    _subject_hash: u32,
}

fn main() {
    let stream_id: u32 = 1;
    let target_consumer: u32 = 1; // only consumer 1 receives

    // ── Setup ──────────────────────────────────────────────────────
    let mut flat: Vec<Sub> = Vec::with_capacity(SUBS);
    // A) stream → consumer → subs
    let mut deep: HashMap<u32, HashMap<u32, Vec<Sub>>> = HashMap::new();
    // C) stream → subs (all consumers mixed)
    let mut by_stream: HashMap<u32, Vec<Sub>> = HashMap::new();

    for c in 0..CONSUMERS {
        for i in 0..SUBS_PER_CONSUMER {
            let sub = Sub {
                stream_id,
                consumer_id: c,
                filter_hash: 0xBEEF + i as u32,
            };
            flat.push(sub.clone());
            deep.entry(stream_id).or_default()
                .entry(c).or_default()
                .push(sub.clone());
            by_stream.entry(stream_id).or_default().push(sub);
        }
    }

    let entries: Vec<Entry> = (0..BATCH_SIZE as u64)
        .map(|i| Entry { seq: i + 1, _subject_hash: 0xDEAD })
        .collect();

    // ── Warmup ─────────────────────────────────────────────────────
    for _ in 0..1_000 {
        // A
        let mut d = 0u64;
        if let Some(by_consumer) = deep.get(&stream_id) {
            if let Some(subs) = by_consumer.get(&target_consumer) {
                for entry in &entries {
                    for sub in subs {
                        black_box((sub.filter_hash, entry.seq));
                        d += 1;
                    }
                }
            }
        }
        black_box(d);
        // B
        let mut d = 0u64;
        for entry in &entries {
            for sub in &flat {
                if sub.stream_id == stream_id && sub.consumer_id == target_consumer {
                    black_box((sub.filter_hash, entry.seq));
                    d += 1;
                }
            }
        }
        black_box(d);
        // C
        let mut d = 0u64;
        if let Some(subs) = by_stream.get(&stream_id) {
            for entry in &entries {
                for sub in subs {
                    if sub.consumer_id == target_consumer {
                        black_box((sub.filter_hash, entry.seq));
                        d += 1;
                    }
                }
            }
        }
        black_box(d);
    }

    // ── Bench A: stream → consumer → 10 subs × 256 entries ────────
    let t0 = Instant::now();
    for _ in 0..ITERS {
        let mut d = 0u64;
        if let Some(by_consumer) = deep.get(&stream_id) {
            if let Some(subs) = by_consumer.get(&target_consumer) {
                for entry in &entries {
                    for sub in subs {
                        black_box((sub.filter_hash, entry.seq));
                        d += 1;
                    }
                }
            }
        }
        black_box(d);
    }
    let a_elapsed = t0.elapsed();

    // ── Bench B: flat scan 30 × 256, filter stream + consumer ─────
    let t0 = Instant::now();
    for _ in 0..ITERS {
        let mut d = 0u64;
        for entry in &entries {
            for sub in &flat {
                if sub.stream_id == stream_id && sub.consumer_id == target_consumer {
                    black_box((sub.filter_hash, entry.seq));
                    d += 1;
                }
            }
        }
        black_box(d);
    }
    let b_elapsed = t0.elapsed();

    // ── Bench C: stream → 30 subs × 256, filter consumer ──────────
    let t0 = Instant::now();
    for _ in 0..ITERS {
        let mut d = 0u64;
        if let Some(subs) = by_stream.get(&stream_id) {
            for entry in &entries {
                for sub in subs {
                    if sub.consumer_id == target_consumer {
                        black_box((sub.filter_hash, entry.seq));
                        d += 1;
                    }
                }
            }
        }
        black_box(d);
    }
    let c_elapsed = t0.elapsed();

    // ── Results ────────────────────────────────────────────────────
    let fmt = |label: &str, elapsed: std::time::Duration| {
        let ns = elapsed.as_nanos() as f64 / ITERS as f64;
        let per_entry = ns / BATCH_SIZE as f64;
        eprintln!("  {:<55} {:>7.1} ns/batch  ({:.2} ns/entry)", label, ns, per_entry);
    };

    eprintln!("\nFanout lookup — {SUBS} subs, {CONSUMERS} consumers × {SUBS_PER_CONSUMER} subs, batch={BATCH_SIZE}, {ITERS} iters");
    eprintln!("  Only consumer {target_consumer} receives (10 of 30 subs match)\n");
    fmt("A) indexed[stream][consumer] → 10 subs", a_elapsed);
    fmt("B) flat scan 30, filter stream+consumer", b_elapsed);
    fmt("C) indexed[stream] → 30 subs, filter consumer", c_elapsed);
    eprintln!();
    eprintln!("  B/A: {:.1}x    C/A: {:.1}x",
        b_elapsed.as_nanos() as f64 / a_elapsed.as_nanos() as f64,
        c_elapsed.as_nanos() as f64 / a_elapsed.as_nanos() as f64,
    );
    eprintln!();
}
