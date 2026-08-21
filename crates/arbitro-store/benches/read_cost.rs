//! TEMPORARY. How expensive is it to get messages OUT of the memory store?
//!
//! Single-threaded, no lock, no dispatch, no fanout — just the four ways the
//! drain can obtain a window, so the read cost is isolated from everything
//! the earlier throughput runs had mixed into it.
//!
//! The four shapes mirror the server:
//!   for_each   — borrowed walk, no copy.        (`hold`, minus dispatch)
//!   fill       — walk + copy into a reused buf. (`batch`, under the lock)
//!   read_range — walk + allocate a Vec<Entry>.  (what `drain_read` calls today)
//!   get_one    — one lookup per entry.          (`single`)
//!
//! Delete once the drain shape is settled.

use std::hint::black_box;
use std::time::Instant;

use arbitro_store::{Entry, EntryRef, MemoryStore, Store};

/// Entries the drain takes per cycle — ARBITRO_MAX_FEED_PER_CYCLE.
const FEED: u64 = 256;
/// Total bytes to hold, so both payload sizes exercise the same footprint.
const TARGET_BYTES: usize = 256 << 20;

/// Same layout as the server's `StagedEntry`: offsets into one flat buffer.
#[derive(Clone, Copy)]
struct StagedEntry {
    seq: u64,
    stream_id: u32,
    timestamp: u64,
    flags: u8,
    subj_off: u32,
    subj_len: u32,
    pay_off: u32,
    pay_len: u32,
}

#[derive(Default)]
struct Staged {
    bytes: Vec<u8>,
    entries: Vec<StagedEntry>,
}

impl Staged {
    /// Byte-for-byte the server's `Staged::fill`.
    fn fill(&mut self, store: &dyn Store, start: u64, end: u64) {
        self.bytes.clear();
        self.entries.clear();
        store
            .for_each(start, end, &mut |e| {
                let subj_off = self.bytes.len() as u32;
                self.bytes.extend_from_slice(e.subject);
                let pay_off = self.bytes.len() as u32;
                self.bytes.extend_from_slice(e.payload);
                self.entries.push(StagedEntry {
                    seq: e.seq,
                    stream_id: e.stream_id,
                    timestamp: e.timestamp,
                    flags: e.flags,
                    subj_off,
                    subj_len: e.subject.len() as u32,
                    pay_off,
                    pay_len: e.payload.len() as u32,
                });
            })
            .ok();
    }

    fn as_entries(&self) -> Vec<Entry<'_>> {
        self.entries
            .iter()
            .map(|st| Entry {
                seq: st.seq,
                stream_id: st.stream_id,
                timestamp: st.timestamp,
                subject: &self.bytes[st.subj_off as usize..(st.subj_off + st.subj_len) as usize],
                payload: &self.bytes[st.pay_off as usize..(st.pay_off + st.pay_len) as usize],
                flags: st.flags,
            })
            .collect()
    }
}

/// Fill a store with `n` entries of `payload` bytes each.
fn build(n: u64, payload: usize) -> MemoryStore {
    let mut store = MemoryStore::with_capacity(TARGET_BYTES + (64 << 20), n as usize + 16);
    let subject = b"bench.read.cost";
    let body = vec![0xABu8; payload];
    for i in 0..n {
        store
            .append(
                EntryRef {
                    stream_id: 1,
                    subject,
                    payload: &body,
                    flags: 0,
                    deliver_at_ms: 0,
                },
                1_700_000_000_000 + i,
            )
            .expect("append");
    }
    store
}

/// Walk the whole store in FEED-sized windows, `f` per window, timed.
fn sweep(n: u64, mut f: impl FnMut(u64, u64)) -> f64 {
    let t = Instant::now();
    let mut start = 1u64;
    while start <= n {
        let end = (start + FEED).min(n + 1);
        f(start, end);
        start = end;
    }
    t.elapsed().as_nanos() as f64
}

fn row(name: &str, total_ns: f64, n: u64, payload: usize) {
    let per = total_ns / n as f64;
    let mbs = (n as f64 * payload as f64) / (total_ns / 1e9) / (1024.0 * 1024.0);
    println!("  {name:<12} {per:>9.1} ns/entry {mbs:>12.0} MB/s {:>10.1} ms total", total_ns / 1e6);
}

/// Every shape, against whatever backend `s` is. `label` names the backend.
fn measure(label: &str, s: &dyn Store, n: u64, payload: usize) {
    println!("\n=== {label}  payload={payload}B  entries={n}  feed={FEED} ===");

        // Warm the pages once so the first shape does not pay the fault cost.
        let _ = s.for_each(1, n + 1, &mut |e| {
            black_box(e.payload.len());
        });

        let ns = sweep(n, |start, end| {
            s.for_each(start, end, &mut |e| {
                black_box(e.seq);
                black_box(e.payload.as_ptr());
            })
            .ok();
        });
        row("for_each", ns, n, payload);

        let mut staged = Staged::default();
        let ns = sweep(n, |start, end| {
            staged.fill(s, start, end);
            black_box(staged.bytes.len());
        });
        row("fill", ns, n, payload);

        let mut staged2 = Staged::default();
        let ns = sweep(n, |start, end| {
            staged2.fill(s, start, end);
            black_box(staged2.as_entries().len());
        });
        row("fill+list", ns, n, payload);

        let ns = sweep(n, |start, end| {
            let v = s.read_range(start, end).unwrap_or_default();
            black_box(v.len());
        });
        row("read_range", ns, n, payload);

        let ns = sweep(n, |start, end| {
            for seq in start..end {
                s.get(seq, &mut |e| {
                    black_box(e.seq);
                    black_box(e.payload.as_ptr());
                })
                .ok();
            }
        });
        row("get_one", ns, n, payload);

        // The question: is grabbing the fragment cheaper than looping?
        // `index_window` is a slice — one bound lookup, no per-entry work.
        let ns = sweep(n, |start, end| {
            black_box(s.index_window(start, end).len());
        });
        row("slice", ns, n, payload);

        // Same slice, but walked once by the CONSUMER — what the drain would
        // do if extraction and dispatch were fused into a single pass.
        let ns = sweep(n, |start, end| {
            for m in s.index_window(start, end) {
                let e = m.entry(s.segment_bytes(m.segment_idx));
                black_box(e.payload.as_ptr());
                black_box(e.seq);
            }
        });
        row("slice+walk", ns, n, payload);
}

fn main() {
    for payload in [64usize, 8192] {
        let n = (TARGET_BYTES / payload) as u64;

        let store = build(n, payload);
        measure("memory", &store, n, payload);
        drop(store);

        let dir = tempfile::tempdir().expect("tempdir");
        let mut t = arbitro_store::TolerantStore::new(dir.path().to_path_buf());
        t.init().expect("init");
        let subject = b"bench.read.cost";
        let body = vec![0xABu8; payload];
        for i in 0..n {
            t.append(
                EntryRef {
                    stream_id: 1,
                    subject,
                    payload: &body,
                    flags: 0,
                    deliver_at_ms: 0,
                },
                1_700_000_000_000 + i,
            )
            .expect("append");
        }
        measure("tolerant", &t, n, payload);
    }
}
