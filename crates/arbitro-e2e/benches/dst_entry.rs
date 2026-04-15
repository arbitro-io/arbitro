//! Batch-struct extraction bench.
//!
//! Pre-populates a real `MemoryStore` with N variable-sized messages, then
//! measures how long it takes to drain them as **batches of ≤ 256 entries**,
//! each materialized as a batch-struct ready to hand off (conceptually:
//!
//!     Batch { stream_id, consumer_ids: Vec<u32>, entries: [Entry] }
//!
//! ). Three materialization strategies are compared:
//!
//!   1. `frame_bytesmut_prod` — current production drainer path. For each
//!      batch: reusable BytesMut → envelope placeholder + RepBatchFixed +
//!      (DeliveryEntryHeader + subject + payload) per entry → split().freeze().
//!      Output is a `Bytes` ready for `tx.try_send`. Full payload memcpy.
//!
//!   2. `owned_vec_batch`    — alternative "Rust struct" path. Vec<EntryOwned>
//!      where each EntryOwned owns `subject: Vec<u8>` + `payload: Vec<u8>`.
//!      Fully Send, no lifetime ties to the store. Full payload memcpy plus
//!      per-entry Vec allocations.
//!
//!   3. `refs_vec_batch`     — zero-copy floor. Vec<EntryRefPtr> where each
//!      entry is a (seq, ts, subject_ptr, subject_len, payload_ptr,
//!      payload_len) descriptor pointing straight into the store arena. No
//!      payload copy. Represents what a Bytes/Arc-backed store could deliver.
//!
//! Payload sizes follow a realistic broker distribution (20% 64B, 30% 256B,
//! 30% 1 KB, 15% 4 KB, 5% 16 KB) generated deterministically via LCG and
//! interleaved (each message picks its own size independently).
//!
//! Run (per .agent/rules/testing.md §BENCHMARK EXECUTION):
//!   wsl bash -lc "cd /mnt/.../arbitro && cargo bench --bench dst_entry --no-run"
//!   wsl bash -lc "mkdir -p /tmp/arbitro-db && cp target/release/deps/dst_entry-* /tmp/arbitro-db/dst_entry"
//!   wsl bash -lc "cd /tmp/arbitro-db && BENCH_TOTAL=1000000 BENCH_BATCH=256 timeout 120 ./dst_entry"

use bytes::BytesMut;
use std::time::{Duration, Instant};
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use arbitro_proto::action::Action;
use arbitro_proto::wire::delivery::{DeliveryEntryHeader, RepBatchFixed};
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_store::{EntryRef, MemoryStore, Store};

// ─── Zerocopy fused arena layout ───────────────────────────────────────────
//
// For the `zerocopy_fused_view` scenario we build a parallel arena at setup
// time (OUTSIDE measurement) where each entry is laid out inline as:
//
//     [ArenaHeader (24 B)] [subject bytes] [payload bytes]
//
// Then measurement walks the arena via `ArenaHeader::ref_from_prefix` — the
// canonical zerocopy decoder pattern. No `unsafe`, no `memcpy`, full typed
// access to seq / ts / lens with a single cast.

#[repr(C)]
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy)]
struct ArenaHeader {
    seq: U64,
    ts_ms: U64,
    subj_len: U16,
    _pad: U16,
    payload_len: U32,
}
// 24 bytes
const _: () = assert!(core::mem::size_of::<ArenaHeader>() == 24);

// ─── Workload ──────────────────────────────────────────────────────────────

#[inline]
fn pick_size(state: &mut u64) -> usize {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let r = (*state >> 33) as u32 % 100;
    match r {
        0..=19 => 64,
        20..=49 => 256,
        50..=79 => 1024,
        80..=94 => 4096,
        _ => 16384,
    }
}

struct Workload {
    sizes: Vec<usize>,
    total_bytes: u64,
    dist: [usize; 5], // counts for 64/256/1K/4K/16K
}

fn gen_workload(total: usize) -> Workload {
    let mut st: u64 = 0xDEAD_BEEF_CAFE_F00D;
    let mut sizes = Vec::with_capacity(total);
    let mut dist = [0usize; 5];
    let mut total_bytes: u64 = 0;
    for _ in 0..total {
        let s = pick_size(&mut st);
        sizes.push(s);
        total_bytes += s as u64;
        let bucket = match s {
            64 => 0,
            256 => 1,
            1024 => 2,
            4096 => 3,
            _ => 4,
        };
        dist[bucket] += 1;
    }
    Workload { sizes, total_bytes, dist }
}

fn populate_store(wk: &Workload) -> MemoryStore {
    let mut store = MemoryStore::new();
    let subject: &[u8] = b"bench.dst_entry.topic";
    let big_payload = vec![0xABu8; 16384];
    for &sz in &wk.sizes {
        store
            .append(
                EntryRef {
                    subject,
                    payload: &big_payload[..sz],
                },
                1_700_000_000_000,
            )
            .unwrap();
    }
    store
}

/// Build a parallel fused arena from the store contents. Each entry is
/// laid out as `[ArenaHeader | subject | payload]`. Returns the arena and
/// a parallel `offsets` table (one u32 per entry, start offset of that
/// entry's header). This happens BEFORE measurement — it's the "what if the
/// store was zerocopy-native" hypothetical.
fn build_fused_arena(store: &MemoryStore) -> (Vec<u8>, Vec<u32>) {
    let info = store.info();
    let last_seq = info.last_seq;
    let total = last_seq as usize;
    let mut arena: Vec<u8> = Vec::with_capacity(((info.bytes as usize) + total * 24) + 4096);
    let mut offsets: Vec<u32> = Vec::with_capacity(total);
    store
        .for_each(1, last_seq + 1, &mut |e| {
            let off = arena.len() as u32;
            offsets.push(off);
            let header = ArenaHeader {
                seq: U64::new(e.seq),
                ts_ms: U64::new(e.timestamp),
                subj_len: U16::new(e.subject.len() as u16),
                _pad: U16::new(0),
                payload_len: U32::new(e.payload.len() as u32),
            };
            arena.extend_from_slice(header.as_bytes());
            arena.extend_from_slice(e.subject);
            arena.extend_from_slice(e.payload);
        })
        .ok();
    (arena, offsets)
}

// ─── Batch structs ─────────────────────────────────────────────────────────

#[allow(dead_code)]
struct EntryOwned {
    seq: u64,
    ts_ms: u64,
    subject: Vec<u8>,
    payload: Vec<u8>,
}

#[allow(dead_code)]
struct BatchOwned {
    stream_id: u32,
    consumer_ids: Vec<u32>,
    entries: Vec<EntryOwned>,
}

#[derive(Clone, Copy)]
#[allow(dead_code)]
struct EntryRefPtr {
    seq: u64,
    ts_ms: u64,
    subject_ptr: *const u8,
    subject_len: usize,
    payload_ptr: *const u8,
    payload_len: usize,
}

#[allow(dead_code)]
struct BatchRefPtr {
    stream_id: u32,
    consumer_ids: Vec<u32>,
    entries: Vec<EntryRefPtr>,
}

// ─── Scenarios ─────────────────────────────────────────────────────────────

const BATCH_SIZE: u64 = 256;

fn scene_frame_bytesmut_prod(store: &MemoryStore) -> (Duration, u64) {
    let last_seq = store.info().last_seq;
    // Match drainer.rs: single reusable scratch BytesMut, split+freeze per batch.
    let mut scratch = BytesMut::with_capacity(
        ENVELOPE_SIZE + 8 + (BATCH_SIZE as usize) * (core::mem::size_of::<DeliveryEntryHeader>() + 32 + 16384),
    );
    let mut sink: u64 = 0;

    let t0 = Instant::now();
    let mut seq: u64 = 1;
    while seq <= last_seq {
        let end = (seq + BATCH_SIZE).min(last_seq + 1);
        let count = (end - seq) as u16;

        scratch.clear();
        scratch.extend_from_slice(&[0u8; ENVELOPE_SIZE]);
        scratch.extend_from_slice(
            RepBatchFixed {
                consumer_id: U32::new(1),
                count: U16::new(count),
                _pad: U16::new(0),
            }
            .as_bytes(),
        );

        let body = &mut scratch;
        store
            .for_each(seq, end, &mut |e| {
                let subj_len = e.subject.len();
                let data_len = subj_len + e.payload.len();
                let header = DeliveryEntryHeader {
                    seq: U64::new(e.seq),
                    subj_len: U16::new(subj_len as u16),
                    data_len: U32::new(data_len as u32),
                };
                body.extend_from_slice(header.as_bytes());
                body.extend_from_slice(e.subject);
                body.extend_from_slice(e.payload);
            })
            .ok();

        let body_len = scratch.len() - ENVELOPE_SIZE;
        let envelope = Envelope::new(Action::RepBatch, 1, body_len as u32, 0);
        scratch[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());

        let frozen = scratch.split().freeze();
        sink = sink.wrapping_add(frozen.len() as u64);
        std::hint::black_box(&frozen);

        seq = end;
    }
    (t0.elapsed(), sink)
}

fn scene_owned_vec_batch(store: &MemoryStore) -> (Duration, u64) {
    let last_seq = store.info().last_seq;
    let mut sink: u64 = 0;

    let t0 = Instant::now();
    let mut seq: u64 = 1;
    while seq <= last_seq {
        let end = (seq + BATCH_SIZE).min(last_seq + 1);
        let cap = (end - seq) as usize;
        let mut entries: Vec<EntryOwned> = Vec::with_capacity(cap);
        store
            .for_each(seq, end, &mut |e| {
                entries.push(EntryOwned {
                    seq: e.seq,
                    ts_ms: e.timestamp,
                    subject: e.subject.to_vec(),
                    payload: e.payload.to_vec(),
                });
            })
            .ok();
        let batch = BatchOwned {
            stream_id: 1,
            consumer_ids: vec![1],
            entries,
        };
        for e in &batch.entries {
            sink = sink.wrapping_add(e.payload.len() as u64);
        }
        std::hint::black_box(&batch);

        seq = end;
    }
    (t0.elapsed(), sink)
}

fn scene_zerocopy_fused_view(arena: &[u8], offsets: &[u32]) -> (Duration, u64) {
    let total = offsets.len();
    let mut sink: u64 = 0;

    let t0 = Instant::now();
    let mut i: usize = 0;
    while i < total {
        let end = (i + BATCH_SIZE as usize).min(total);
        let cap = end - i;
        let mut entries: Vec<EntryRefPtr> = Vec::with_capacity(cap);
        for j in i..end {
            let off = offsets[j] as usize;
            // Single zerocopy cast: typed view over the header.
            let (header, rest) = ArenaHeader::ref_from_prefix(&arena[off..]).unwrap();
            let subj_len = header.subj_len.get() as usize;
            let payload_len = header.payload_len.get() as usize;
            let subject_ptr = rest.as_ptr();
            // SAFETY: arena[off..] contains at least subj_len + payload_len
            // trailing bytes (built by build_fused_arena above).
            let payload_ptr = unsafe { subject_ptr.add(subj_len) };
            entries.push(EntryRefPtr {
                seq: header.seq.get(),
                ts_ms: header.ts_ms.get(),
                subject_ptr,
                subject_len: subj_len,
                payload_ptr,
                payload_len,
            });
        }
        let batch = BatchRefPtr {
            stream_id: 1,
            consumer_ids: vec![1],
            entries,
        };
        for e in &batch.entries {
            sink = sink.wrapping_add(e.payload_len as u64);
        }
        std::hint::black_box(&batch);

        i = end;
    }
    (t0.elapsed(), sink)
}

fn scene_refs_vec_batch(store: &MemoryStore) -> (Duration, u64) {
    let last_seq = store.info().last_seq;
    let mut sink: u64 = 0;

    let t0 = Instant::now();
    let mut seq: u64 = 1;
    while seq <= last_seq {
        let end = (seq + BATCH_SIZE).min(last_seq + 1);
        let cap = (end - seq) as usize;
        let mut entries: Vec<EntryRefPtr> = Vec::with_capacity(cap);
        store
            .for_each(seq, end, &mut |e| {
                entries.push(EntryRefPtr {
                    seq: e.seq,
                    ts_ms: e.timestamp,
                    subject_ptr: e.subject.as_ptr(),
                    subject_len: e.subject.len(),
                    payload_ptr: e.payload.as_ptr(),
                    payload_len: e.payload.len(),
                });
            })
            .ok();
        let batch = BatchRefPtr {
            stream_id: 1,
            consumer_ids: vec![1],
            entries,
        };
        for e in &batch.entries {
            sink = sink.wrapping_add(e.payload_len as u64);
        }
        std::hint::black_box(&batch);

        seq = end;
    }
    (t0.elapsed(), sink)
}

// ─── Runner ────────────────────────────────────────────────────────────────

fn fmt_rate(msgs: usize, elapsed: Duration) -> String {
    let rate = msgs as f64 / elapsed.as_secs_f64();
    if rate >= 1_000_000.0 {
        format!("{:.2}M", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}K", rate / 1_000.0)
    } else {
        format!("{:.0}", rate)
    }
}

fn run_scene<F: FnMut(&MemoryStore) -> (Duration, u64)>(
    name: &str,
    store: &MemoryStore,
    total: usize,
    reps: usize,
    total_bytes: u64,
    mut f: F,
) {
    let mut best = Duration::MAX;
    let mut sum = Duration::ZERO;
    let mut sink_acc: u64 = 0;
    for _ in 0..reps {
        let (d, sink) = f(store);
        if d < best {
            best = d;
        }
        sum += d;
        sink_acc = sink_acc.wrapping_add(sink);
    }
    let avg = sum / reps as u32;
    let best_ns_per = best.as_nanos() as f64 / total as f64;
    let avg_ns_per = avg.as_nanos() as f64 / total as f64;
    let gbps = total_bytes as f64 / best.as_secs_f64() / 1e9;
    let msg_rate = fmt_rate(total, best);
    println!(
        "  {:<24} best {:>6.1} ns/entry   avg {:>6.1} ns/entry   {:>5.2} GB/s   {:>7} msg/s   [sink={:x}]",
        name, best_ns_per, avg_ns_per, gbps, msg_rate, sink_acc
    );
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let total = env_usize("BENCH_TOTAL", 1000);
    let batch = env_usize("BENCH_BATCH", BATCH_SIZE as usize);
    let reps = env_usize("BENCH_REPS", 10);

    // Round total down to a multiple of batch so counts are clean.
    let total = (total / batch) * batch;
    let num_batches = total / batch;

    println!("dst_entry bench — batch struct extraction from real MemoryStore");
    println!(
        "total = {}, batch_size = {}, num_batches = {}, reps = {}",
        total, batch, num_batches, reps
    );

    let wk = gen_workload(total);
    println!(
        "distribution:  64B×{}  256B×{}  1024B×{}  4096B×{}  16384B×{}",
        wk.dist[0], wk.dist[1], wk.dist[2], wk.dist[3], wk.dist[4]
    );
    println!(
        "total payload = {} KB,  avg = {} B/entry",
        wk.total_bytes / 1024,
        wk.total_bytes / total as u64
    );
    println!("{}", "=".repeat(92));

    println!("populating real MemoryStore...");
    let t_pop = Instant::now();
    let store = populate_store(&wk);
    let pop_elapsed = t_pop.elapsed();
    println!(
        "  populated {} entries in {:.2}s  ({} msg/s append)",
        total,
        pop_elapsed.as_secs_f64(),
        fmt_rate(total, pop_elapsed)
    );
    println!("{}", "-".repeat(92));

    println!("--- batch struct extraction (each batch = ≤ {} entries) ---", batch);
    run_scene("frame_bytesmut_prod", &store, total, reps, wk.total_bytes, scene_frame_bytesmut_prod);
    run_scene("owned_vec_batch",     &store, total, reps, wk.total_bytes, scene_owned_vec_batch);
    run_scene("refs_vec_batch",      &store, total, reps, wk.total_bytes, scene_refs_vec_batch);

    // Zerocopy fused-arena scenario — built once outside measurement.
    println!("\nbuilding fused zerocopy arena (outside measurement)...");
    let t_fuse = Instant::now();
    let (arena, offsets) = build_fused_arena(&store);
    let fuse_elapsed = t_fuse.elapsed();
    println!(
        "  fused arena = {} KB, offsets = {} KB, built in {:.2}s",
        arena.len() / 1024,
        (offsets.len() * 4) / 1024,
        fuse_elapsed.as_secs_f64()
    );

    // Bespoke run loop for the zerocopy scenario since it takes arena+offsets
    // instead of &MemoryStore.
    {
        let name = "zerocopy_fused_view";
        let mut best = Duration::MAX;
        let mut sum = Duration::ZERO;
        let mut sink_acc: u64 = 0;
        for _ in 0..reps {
            let (d, sink) = scene_zerocopy_fused_view(&arena, &offsets);
            if d < best { best = d; }
            sum += d;
            sink_acc = sink_acc.wrapping_add(sink);
        }
        let avg = sum / reps as u32;
        let best_ns_per = best.as_nanos() as f64 / total as f64;
        let avg_ns_per = avg.as_nanos() as f64 / total as f64;
        let gbps = wk.total_bytes as f64 / best.as_secs_f64() / 1e9;
        let msg_rate = fmt_rate(total, best);
        println!(
            "  {:<24} best {:>6.1} ns/entry   avg {:>6.1} ns/entry   {:>5.2} GB/s   {:>7} msg/s   [sink={:x}]",
            name, best_ns_per, avg_ns_per, gbps, msg_rate, sink_acc
        );
    }

    println!("\nDone.");
}
