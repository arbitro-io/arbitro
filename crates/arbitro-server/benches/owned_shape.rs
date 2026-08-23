//! What does ownership cost, and what does the lack of it cost?
//!
//! Two shapes for the same two operations. The work at the bottom is
//! IDENTICAL in both — the same `MemoryStore::append_batch`, the same
//! `HashMap` remove — so whatever separates them is ceremony, not work.
//!
//! ## publish
//!
//!   owned     the frame's bytes are viewed as `EntryRef` and handed to the
//!             store. Nothing is allocated, nothing is asked.
//!
//!   crossing  what a routed publish does today: reshape into owned entries
//!             (`Bytes::slice_ref`, so no byte copy — but a `Vec`), move
//!             them through an mpsc, and on the far side rebuild the
//!             `EntryRef` slice the store wants, after a thread-local
//!             lookup that asks whether this thread owns the shard.
//!
//! ## ack
//!
//!   owned     one pass: remove from `pending`, release the credit.
//!
//!   passes    what `handle_ack` does today: an ownership lookup, then the
//!             engine's own walk, then the contiguous-acked floor, then the
//!             DLQ counters, then the high-water seq, then the delta. Six
//!             walks of a set whose membership the first one already knew.
//!
//! Both sizes matter and they say different things. `1` is a `publish`
//! frame and a single ack — the shape where ceremony is the whole cost.
//! `256` is a batch, where the ceremony amortises and the real work shows.
//!
//! Run: `cargo bench -p arbitro-server --bench owned_shape`

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

use arbitro_store::{EntryRef, MemoryStore, Store};
use bytes::Bytes;

const ROUNDS: usize = 20_000;
const SUBJECT: &[u8] = b"bench.subject.name";
const PAYLOAD_LEN: usize = 64;

// ── The crossing shape's baggage ────────────────────────────────────────────

/// What has to be built for entries to survive a move to another owner.
#[derive(Clone)]
struct OwnedEntry {
    subject: Bytes,
    payload: Bytes,
    flags: u8,
    deliver_at_ms: u64,
}

thread_local! {
    /// The ownership question a non-owner has to ask, and the owner pays
    /// for anyway: "is this thread's store the one for shard N?"
    static LOCAL_SHARD: std::cell::Cell<usize> = const { std::cell::Cell::new(7) };
}

#[inline]
fn owns(shard_id: usize) -> bool {
    LOCAL_SHARD.with(|c| c.get() == shard_id)
}

/// One frame, laid out the way the wire delivers it: subject and payload
/// are slices INSIDE one buffer.
fn frame(batch: usize) -> (Bytes, Vec<(usize, usize, usize, usize)>) {
    let mut buf = Vec::new();
    let mut spans = Vec::with_capacity(batch);
    for _ in 0..batch {
        let s0 = buf.len();
        buf.extend_from_slice(SUBJECT);
        let s1 = buf.len();
        buf.extend(std::iter::repeat_n(0xABu8, PAYLOAD_LEN));
        let p1 = buf.len();
        spans.push((s0, s1, s1, p1));
    }
    (Bytes::from(buf), spans)
}

// ── publish ─────────────────────────────────────────────────────────────────

/// The frame is viewed, not moved. No allocation, no ownership question.
fn publish_owned(store: &mut MemoryStore, f: &Bytes, spans: &[(usize, usize, usize, usize)]) -> u64 {
    // A single entry is the shape of a `publish` frame: a stack array, no
    // collection of any kind.
    if let [(s0, s1, p0, p1)] = spans {
        let e = EntryRef {
            stream_id: 1,
            subject: &f[*s0..*s1],
            payload: &f[*p0..*p1],
            flags: 0,
            deliver_at_ms: 0,
        };
        return store.append_batch(&[e], 1).unwrap();
    }
    let mut refs: smallvec::SmallVec<[EntryRef<'_>; 16]> =
        smallvec::SmallVec::with_capacity(spans.len());
    for (s0, s1, p0, p1) in spans {
        refs.push(EntryRef {
            stream_id: 1,
            subject: &f[*s0..*s1],
            payload: &f[*p0..*p1],
            flags: 0,
            deliver_at_ms: 0,
        });
    }
    store.append_batch(&refs, 1).unwrap()
}

/// Reshape → move → ask permission → reshape back → store.
fn publish_crossing(
    store: &mut MemoryStore,
    f: &Bytes,
    spans: &[(usize, usize, usize, usize)],
    tx: &tokio::sync::mpsc::Sender<Vec<OwnedEntry>>,
    rx: &mut tokio::sync::mpsc::Receiver<Vec<OwnedEntry>>,
) -> u64 {
    // Sender side: entries must be owned to cross. `slice_ref` keeps the
    // bytes where they are — the cost here is the Vec and the refcounts.
    let owned: Vec<OwnedEntry> = spans
        .iter()
        .map(|(s0, s1, p0, p1)| OwnedEntry {
            subject: f.slice(*s0..*s1),
            payload: f.slice(*p0..*p1),
            flags: 0,
            deliver_at_ms: 0,
        })
        .collect();
    tx.try_send(owned).unwrap();

    // Receiver side: the shard worker.
    let cmd = rx.try_recv().unwrap();
    // The ownership question, per command.
    if !owns(7) {
        return 0;
    }
    let mut refs: smallvec::SmallVec<[EntryRef<'_>; 16]> =
        smallvec::SmallVec::with_capacity(cmd.len());
    for e in &cmd {
        refs.push(EntryRef {
            stream_id: 1,
            subject: &e.subject,
            payload: &e.payload,
            flags: e.flags,
            deliver_at_ms: e.deliver_at_ms,
        });
    }
    store.append_batch(&refs, 1).unwrap()
}

// ── ack ─────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct Pending {
    subject_hash: u32,
}

/// Everything one ack touches, in the shapes the broker has today.
struct AckState {
    pending: HashMap<u64, Pending, foldhash::fast::FixedState>,
    dlq: HashMap<(u32, u64), u32, foldhash::fast::FixedState>,
    conn_consumer: HashMap<(u64, u32), u32, foldhash::fast::FixedState>,
    floor: Vec<u64>,
    inflight: u32,
    cursor: u64,
    /// What the engine reports back — the set every later pass re-walks.
    matched: Vec<(u32, u32, u64)>,
}

impl AckState {
    fn new(n: u64) -> Self {
        let mut pending =
            HashMap::with_hasher(foldhash::fast::FixedState::default());
        let mut dlq = HashMap::with_hasher(foldhash::fast::FixedState::default());
        let mut conn_consumer =
            HashMap::with_hasher(foldhash::fast::FixedState::default());
        for seq in 0..n {
            pending.insert(seq, Pending { subject_hash: seq as u32 });
            dlq.insert((3u32, seq), 1u32);
        }
        conn_consumer.insert((11u64, 3u32), 1u32);
        Self {
            pending,
            dlq,
            conn_consumer,
            floor: vec![0; 8],
            inflight: n as u32,
            cursor: 0,
            matched: Vec::with_capacity(256),
        }
    }

    fn refill(&mut self, n: u64) {
        for seq in 0..n {
            self.pending.insert(seq, Pending { subject_hash: seq as u32 });
            self.dlq.insert((3u32, seq), 1u32);
        }
        self.inflight = n as u32;
    }

    /// One walk. The remove IS the release: capacity is what is in the map.
    fn ack_owned(&mut self, seqs: &[u64]) -> u32 {
        let mut matched = 0;
        for &seq in seqs {
            if let Some(p) = self.pending.remove(&seq) {
                black_box(p.subject_hash);
                self.inflight -= 1;
                if seq > self.cursor {
                    self.cursor = seq;
                }
                matched += 1;
            }
        }
        matched
    }

    /// Six walks: the ownership hash, the engine, the floor, the DLQ
    /// counters, the high-water seq, and the delta the engine just built.
    fn ack_passes(&mut self, seqs: &[u64]) -> u32 {
        // 1. may this connection release on this consumer?
        if !self.conn_consumer.contains_key(&(11u64, 3u32)) {
            return 0;
        }
        // 2. the floor, computed BEFORE the removals it depends on
        for &seq in seqs {
            if self.pending.contains_key(&seq) {
                let slot = &mut self.floor[3 % 8];
                if seq > *slot {
                    *slot = seq;
                }
            }
        }
        // 3. the engine's own walk
        self.matched.clear();
        for &seq in seqs {
            if let Some(p) = self.pending.remove(&seq) {
                self.matched.push((3, p.subject_hash, seq));
            }
        }
        // 4. DLQ counters, over the SUBMITTED set
        for &seq in seqs {
            self.dlq.remove(&(3u32, seq));
        }
        // 5. high-water, over the submitted set again
        if let Some(max) = seqs.iter().copied().max() {
            if max > self.cursor {
                self.cursor = max;
            }
        }
        // 6. the delta, re-walked to sync counters and emit events
        let matched = self.matched.len() as u32;
        for &(_cid, sh, seq) in &self.matched {
            black_box((sh, seq));
        }
        self.inflight -= matched;
        matched
    }
}

// ── harness ─────────────────────────────────────────────────────────────────

fn bench(label: &str, rounds: usize, mut f: impl FnMut()) -> f64 {
    // Warm the branch predictor and the allocator before timing.
    for _ in 0..(rounds / 10).max(1) {
        f();
    }
    let t0 = Instant::now();
    for _ in 0..rounds {
        f();
    }
    let ns = t0.elapsed().as_nanos() as f64 / rounds as f64;
    println!("  {label:<28} {ns:>10.1} ns/op");
    ns
}

fn main() {
    println!("\nowned_shape — same work at the bottom, different ceremony around it");
    println!("{ROUNDS} rounds each, 64B payload\n");

    for batch in [1usize, 256] {
        println!("[ batch = {batch} ]");
        let (f, spans) = frame(batch);

        // publish
        let mut store_a = MemoryStore::new();
        let owned_ns = bench("publish · owned", ROUNDS, || {
            black_box(publish_owned(&mut store_a, &f, &spans));
        });

        let mut store_b = MemoryStore::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<OwnedEntry>>(1024);
        let crossing_ns = bench("publish · crossing", ROUNDS, || {
            black_box(publish_crossing(&mut store_b, &f, &spans, &tx, &mut rx));
        });
        println!(
            "  {:<28} {:>10.1}x   ({:.1} ns/msg vs {:.1})",
            "→ crossing costs",
            crossing_ns / owned_ns,
            crossing_ns / batch as f64,
            owned_ns / batch as f64
        );

        // ack
        let seqs: Vec<u64> = (0..batch as u64).collect();
        let mut a = AckState::new(batch as u64);
        let one_ns = bench("ack · owned", ROUNDS, || {
            a.refill(batch as u64);
            black_box(a.ack_owned(&seqs));
        });

        let mut b = AckState::new(batch as u64);
        let six_ns = bench("ack · passes", ROUNDS, || {
            b.refill(batch as u64);
            black_box(b.ack_passes(&seqs));
        });
        println!(
            "  {:<28} {:>10.1}x   ({:.1} ns/ack vs {:.1})\n",
            "→ six passes cost",
            six_ns / one_ns,
            six_ns / batch as f64,
            one_ns / batch as f64
        );
    }
}
