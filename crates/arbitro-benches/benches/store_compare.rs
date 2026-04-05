//! Benchmark: compare 4 storage strategies.
//!
//! 1. FlatVec    — current design: one Vec<Entry> per stream
//! 2. TrieStore  — stream_id is root node, subject segments are children, entries at leaves
//! 3. RingStore  — fixed-capacity ring buffer per stream
//! 4. IndexedVec — NATS model: Vec<Entry> for order + trie index for subject lookup
//!
//! Multi-stream scenario: 5 streams, 10 subjects each, 200 msgs per stream = 1000 total.
//! Max 1000 messages per iteration (bench safety rule).

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

// ── Common types ────────────────────────────────────────────────

#[derive(Clone)]
struct Entry {
    seq: u64,
    subject: Box<[u8]>,
    payload: Box<[u8]>,
}

struct EntryRef<'a> {
    stream_id: u32,
    subject: &'a [u8],
    payload: &'a [u8],
}

// ── Test data ───────────────────────────────────────────────────

const NUM_STREAMS: u32 = 5;
const SUBJECTS_PER_STREAM: u32 = 10;
const MSGS_PER_STREAM: u32 = 200;
const N: u32 = NUM_STREAMS * MSGS_PER_STREAM; // 1000 total
const PAYLOAD_SIZE: usize = 64;

const SUBJECTS: &[&[u8]] = &[
    b"orders.created", b"orders.updated", b"orders.deleted",
    b"payments.created", b"payments.updated", b"payments.deleted",
    b"users.created", b"users.updated", b"users.deleted", b"users.login",
];

// ── 1. FlatVec — one Vec per stream ─────────────────────────────

struct FlatVecStore {
    streams: HashMap<u32, FlatStream>,
}

struct FlatStream {
    entries: Vec<Entry>,
    next_seq: u64,
}

impl FlatVecStore {
    fn new() -> Self {
        Self { streams: HashMap::with_capacity(NUM_STREAMS as usize) }
    }

    #[inline]
    fn append(&mut self, entry: EntryRef<'_>) {
        let stream = self.streams
            .entry(entry.stream_id)
            .or_insert_with(|| FlatStream {
                entries: Vec::with_capacity(MSGS_PER_STREAM as usize),
                next_seq: 1,
            });
        let seq = stream.next_seq;
        stream.next_seq += 1;
        stream.entries.push(Entry {
            seq,
            subject: Box::from(entry.subject),
            payload: Box::from(entry.payload),
        });
    }

    #[inline]
    fn drain_stream(&self, stream_id: u32, mut f: impl FnMut(&Entry)) {
        if let Some(stream) = self.streams.get(&stream_id) {
            for e in &stream.entries {
                f(e);
            }
        }
    }

    #[inline]
    fn drain_subject(&self, stream_id: u32, subject: &[u8], mut f: impl FnMut(&Entry)) {
        if let Some(stream) = self.streams.get(&stream_id) {
            for e in &stream.entries {
                if &*e.subject == subject {
                    f(e);
                }
            }
        }
    }

    #[inline]
    fn drain_subtree(&self, stream_id: u32, prefix: &[u8], mut f: impl FnMut(&Entry)) {
        if let Some(stream) = self.streams.get(&stream_id) {
            for e in &stream.entries {
                if e.subject.starts_with(prefix) {
                    f(e);
                }
            }
        }
    }
}

// ── 2. TrieStore — stream_id as root node ───────────────────────
//
// Structure:
//   streams: HashMap<stream_id, TrieNode>
//   TrieNode { children: HashMap<segment, TrieNode>, entries: Vec<Entry> }
//
// "orders.created" on stream 0 → streams[0] → "orders" → "created" → entries
//
// drain_stream(0) = streams[0].visit_all()  — O(1) lookup + iterate leaves
// drain_subject("orders.created") = walk to leaf → iterate its Vec
// drain_subtree("orders") = walk to "orders" node → DFS children

struct TrieNode {
    entries: Vec<Entry>,
    children: HashMap<Box<[u8]>, TrieNode>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            children: HashMap::new(),
        }
    }

    #[inline]
    fn get_or_create(&mut self, subject: &[u8]) -> &mut TrieNode {
        let mut node = self;
        for segment in subject.split(|&b| b == b'.') {
            node = node.children
                .entry(Box::from(segment))
                .or_insert_with(TrieNode::new);
        }
        node
    }

    #[inline]
    fn get(&self, subject: &[u8]) -> Option<&TrieNode> {
        let mut node = self;
        for segment in subject.split(|&b| b == b'.') {
            node = node.children.get(segment)?;
        }
        Some(node)
    }

    #[inline]
    fn visit_all(&self, f: &mut impl FnMut(&Entry)) {
        for e in &self.entries {
            f(e);
        }
        for child in self.children.values() {
            child.visit_all(f);
        }
    }
}

struct TrieStore {
    streams: HashMap<u32, TrieNode>,
    next_seq: u64,
}

impl TrieStore {
    fn new() -> Self {
        Self {
            streams: HashMap::with_capacity(NUM_STREAMS as usize),
            next_seq: 1,
        }
    }

    #[inline]
    fn append(&mut self, entry: EntryRef<'_>) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let root = self.streams
            .entry(entry.stream_id)
            .or_insert_with(TrieNode::new);
        let leaf = root.get_or_create(entry.subject);
        leaf.entries.push(Entry {
            seq,
            subject: Box::from(entry.subject),
            payload: Box::from(entry.payload),
        });
    }

    /// Drain stream = lookup stream node (O(1)) + visit all leaves.
    #[inline]
    fn drain_stream(&self, stream_id: u32, mut f: impl FnMut(&Entry)) {
        if let Some(root) = self.streams.get(&stream_id) {
            root.visit_all(&mut f);
        }
    }

    /// Drain exact subject = walk trie to leaf (O(depth)) + iterate Vec.
    #[inline]
    fn drain_subject(&self, stream_id: u32, subject: &[u8], mut f: impl FnMut(&Entry)) {
        if let Some(root) = self.streams.get(&stream_id) {
            if let Some(leaf) = root.get(subject) {
                for e in &leaf.entries {
                    f(e);
                }
            }
        }
    }

    /// Drain subtree = walk to prefix node + DFS its children.
    #[inline]
    fn drain_subtree(&self, stream_id: u32, prefix: &[u8], mut f: impl FnMut(&Entry)) {
        if let Some(root) = self.streams.get(&stream_id) {
            if let Some(node) = root.get(prefix) {
                node.visit_all(&mut f);
            }
        }
    }
}

// ── 3. RingStore — fixed-capacity ring per stream ───────────────

struct RingSlot {
    seq: u64,
    subject: Box<[u8]>,
    payload: Box<[u8]>,
}

struct Ring {
    slots: Vec<Option<RingSlot>>,
    capacity: usize,
    write_pos: usize,
    len: usize,
    first_seq: u64,
    next_seq: u64,
}

impl Ring {
    fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || None);
        Self { slots, capacity, write_pos: 0, len: 0, first_seq: 1, next_seq: 1 }
    }

    #[inline]
    fn append(&mut self, subject: &[u8], payload: &[u8]) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.slots[self.write_pos] = Some(RingSlot {
            seq,
            subject: Box::from(subject),
            payload: Box::from(payload),
        });
        self.write_pos = (self.write_pos + 1) % self.capacity;
        if self.len < self.capacity {
            self.len += 1;
        } else {
            self.first_seq += 1;
        }
    }

    #[inline]
    fn scan_all(&self, mut f: impl FnMut(&RingSlot)) {
        let start = (self.write_pos + self.capacity - self.len) % self.capacity;
        for i in 0..self.len {
            let idx = (start + i) % self.capacity;
            if let Some(slot) = &self.slots[idx] {
                f(slot);
            }
        }
    }

    #[inline]
    fn scan_subject(&self, subject: &[u8], mut f: impl FnMut(&RingSlot)) {
        let start = (self.write_pos + self.capacity - self.len) % self.capacity;
        for i in 0..self.len {
            let idx = (start + i) % self.capacity;
            if let Some(slot) = &self.slots[idx] {
                if &*slot.subject == subject {
                    f(slot);
                }
            }
        }
    }
}

struct RingStore {
    streams: HashMap<u32, Ring>,
}

impl RingStore {
    fn new() -> Self {
        Self { streams: HashMap::with_capacity(NUM_STREAMS as usize) }
    }

    #[inline]
    fn append(&mut self, entry: EntryRef<'_>) {
        let ring = self.streams
            .entry(entry.stream_id)
            .or_insert_with(|| Ring::new(MSGS_PER_STREAM as usize * 2));
        ring.append(entry.subject, entry.payload);
    }

    #[inline]
    fn drain_stream(&self, stream_id: u32, f: impl FnMut(&RingSlot)) {
        if let Some(ring) = self.streams.get(&stream_id) {
            ring.scan_all(f);
        }
    }

    #[inline]
    fn drain_subject(&self, stream_id: u32, subject: &[u8], f: impl FnMut(&RingSlot)) {
        if let Some(ring) = self.streams.get(&stream_id) {
            ring.scan_subject(subject, f);
        }
    }

    #[inline]
    fn drain_subtree(&self, stream_id: u32, prefix: &[u8], mut f: impl FnMut(&RingSlot)) {
        if let Some(ring) = self.streams.get(&stream_id) {
            ring.scan_all(|slot| {
                if slot.subject.starts_with(prefix) {
                    f(slot);
                }
            });
        }
    }
}

// ── 4. IndexedVec — NATS model: Vec + trie index ────────────────
//
// Store = Vec<Entry> (sequential, owns data, maintains global order)
// Index = trie of subject segments, leaves hold Vec<u32> (indices into store Vec)
//
// Append: push to Vec, walk trie to leaf, push index.
// Drain stream: iterate Vec directly (linear, cache-friendly, ordered).
// Drain subject: walk trie to leaf → get indices → read from Vec.
// Drain subtree: walk trie to prefix → DFS collect indices → sort → read from Vec.

struct IndexNode {
    indices: Vec<u32>,  // positions in the parent Vec<Entry>
    children: HashMap<Box<[u8]>, IndexNode>,
}

impl IndexNode {
    fn new() -> Self {
        Self { indices: Vec::new(), children: HashMap::new() }
    }

    #[inline]
    fn get_or_create(&mut self, subject: &[u8]) -> &mut IndexNode {
        let mut node = self;
        for segment in subject.split(|&b| b == b'.') {
            node = node.children
                .entry(Box::from(segment))
                .or_insert_with(IndexNode::new);
        }
        node
    }

    #[inline]
    fn get(&self, subject: &[u8]) -> Option<&IndexNode> {
        let mut node = self;
        for segment in subject.split(|&b| b == b'.') {
            node = node.children.get(segment)?;
        }
        Some(node)
    }

    #[inline]
    fn collect_indices(&self, out: &mut Vec<u32>) {
        out.extend_from_slice(&self.indices);
        for child in self.children.values() {
            child.collect_indices(out);
        }
    }
}

struct IndexedStream {
    entries: Vec<Entry>,
    index: IndexNode,
    next_seq: u64,
}

struct IndexedVecStore {
    streams: HashMap<u32, IndexedStream>,
}

impl IndexedVecStore {
    fn new() -> Self {
        Self { streams: HashMap::with_capacity(NUM_STREAMS as usize) }
    }

    #[inline]
    fn append(&mut self, entry: EntryRef<'_>) {
        let stream = self.streams
            .entry(entry.stream_id)
            .or_insert_with(|| IndexedStream {
                entries: Vec::with_capacity(MSGS_PER_STREAM as usize),
                index: IndexNode::new(),
                next_seq: 1,
            });
        let seq = stream.next_seq;
        stream.next_seq += 1;
        let pos = stream.entries.len() as u32;
        stream.entries.push(Entry {
            seq,
            subject: Box::from(entry.subject),
            payload: Box::from(entry.payload),
        });
        // Register in trie index
        let leaf = stream.index.get_or_create(entry.subject);
        leaf.indices.push(pos);
    }

    /// Drain stream = iterate Vec directly. Same as FlatVec. Order guaranteed.
    #[inline]
    fn drain_stream(&self, stream_id: u32, mut f: impl FnMut(&Entry)) {
        if let Some(stream) = self.streams.get(&stream_id) {
            for e in &stream.entries {
                f(e);
            }
        }
    }

    /// Drain subject = walk trie to leaf → read indices → access Vec entries.
    /// Indices within a leaf are already in append order (ASC).
    #[inline]
    fn drain_subject(&self, stream_id: u32, subject: &[u8], mut f: impl FnMut(&Entry)) {
        if let Some(stream) = self.streams.get(&stream_id) {
            if let Some(leaf) = stream.index.get(subject) {
                for &idx in &leaf.indices {
                    f(&stream.entries[idx as usize]);
                }
            }
        }
    }

    /// Drain subtree = walk trie to prefix → collect all indices → sort → access Vec.
    #[inline]
    fn drain_subtree(&self, stream_id: u32, prefix: &[u8], mut f: impl FnMut(&Entry)) {
        if let Some(stream) = self.streams.get(&stream_id) {
            if let Some(node) = stream.index.get(prefix) {
                let mut indices = Vec::new();
                node.collect_indices(&mut indices);
                indices.sort_unstable();
                for idx in indices {
                    f(&stream.entries[idx as usize]);
                }
            }
        }
    }
}

// ── Expected counts for stream 0 ───────────────────────────────
//
// 1000 msgs across 5 streams → 200 per stream.
// 10 subjects round-robin → 20 per subject per stream.
// "orders" subtree = 3 subjects (created, updated, deleted) → 60 per stream.
//
// But distribution depends on (i % NUM_STREAMS, i % SUBJECTS.len()):
//   stream_id = i % 5,  subject_idx = i % 10
//   For stream 0: i ∈ {0,5,10,15,...,995} → 200 msgs
//   subject for i=0 → idx 0 (orders.created)
//   subject for i=5 → idx 5 (payments.deleted)
//   subject for i=10 → idx 0 (orders.created)  -- repeats every lcm(5,10)=10
//   So stream 0 gets subject indices: 0,5,0,5,0,5... → only 2 distinct subjects!
//
// Fix: use independent distribution to ensure all subjects appear per stream.

fn make_entries_v2() -> Vec<(u32, Vec<u8>, Vec<u8>)> {
    let payload = vec![0xABu8; PAYLOAD_SIZE];
    let mut out = Vec::with_capacity(N as usize);
    for stream_id in 0..NUM_STREAMS {
        for i in 0..MSGS_PER_STREAM {
            let subject = SUBJECTS[(i as usize) % SUBJECTS.len()].to_vec();
            out.push((stream_id, subject, payload.clone()));
        }
    }
    out
}

const STREAM_MSGS: u32 = MSGS_PER_STREAM;           // 200
const EXACT_PER_STREAM: u32 = MSGS_PER_STREAM / SUBJECTS_PER_STREAM; // 20
// "orders" subtree: 3 subjects × 20 = 60
const SUBTREE_PER_STREAM: u32 = 3 * EXACT_PER_STREAM; // 60
const TARGET_STREAM: u32 = 0;
const EXACT_SUBJECT: &[u8] = b"orders.created";
const SUBTREE_PREFIX: &[u8] = b"orders";

// ── Smoke tests ─────────────────────────────────────────────────

fn smoke_test() {
    let data = make_entries_v2();

    // FlatVec
    let mut flat = FlatVecStore::new();
    for (sid, subj, pay) in &data {
        flat.append(EntryRef { stream_id: *sid, subject: subj, payload: pay });
    }
    let mut c = 0u32;
    flat.drain_stream(TARGET_STREAM, |_| c += 1);
    assert_eq!(c, STREAM_MSGS, "FlatVec drain_stream: got {c}");
    let mut c = 0u32;
    flat.drain_subject(TARGET_STREAM, EXACT_SUBJECT, |_| c += 1);
    assert_eq!(c, EXACT_PER_STREAM, "FlatVec drain_subject: got {c}");
    let mut c = 0u32;
    flat.drain_subtree(TARGET_STREAM, SUBTREE_PREFIX, |_| c += 1);
    assert_eq!(c, SUBTREE_PER_STREAM, "FlatVec drain_subtree: got {c}");

    // TrieStore
    let mut trie = TrieStore::new();
    for (sid, subj, pay) in &data {
        trie.append(EntryRef { stream_id: *sid, subject: subj, payload: pay });
    }
    let mut c = 0u32;
    trie.drain_stream(TARGET_STREAM, |_| c += 1);
    assert_eq!(c, STREAM_MSGS, "TrieStore drain_stream: got {c}");
    let mut c = 0u32;
    trie.drain_subject(TARGET_STREAM, EXACT_SUBJECT, |_| c += 1);
    assert_eq!(c, EXACT_PER_STREAM, "TrieStore drain_subject: got {c}");
    let mut c = 0u32;
    trie.drain_subtree(TARGET_STREAM, SUBTREE_PREFIX, |_| c += 1);
    assert_eq!(c, SUBTREE_PER_STREAM, "TrieStore drain_subtree: got {c}");

    // RingStore
    let mut ring = RingStore::new();
    for (sid, subj, pay) in &data {
        ring.append(EntryRef { stream_id: *sid, subject: subj, payload: pay });
    }
    let mut c = 0u32;
    ring.drain_stream(TARGET_STREAM, |_| c += 1);
    assert_eq!(c, STREAM_MSGS, "RingStore drain_stream: got {c}");
    let mut c = 0u32;
    ring.drain_subject(TARGET_STREAM, EXACT_SUBJECT, |_| c += 1);
    assert_eq!(c, EXACT_PER_STREAM, "RingStore drain_subject: got {c}");
    let mut c = 0u32;
    ring.drain_subtree(TARGET_STREAM, SUBTREE_PREFIX, |_| c += 1);
    assert_eq!(c, SUBTREE_PER_STREAM, "RingStore drain_subtree: got {c}");

    // IndexedVec (NATS model)
    let mut indexed = IndexedVecStore::new();
    for (sid, subj, pay) in &data {
        indexed.append(EntryRef { stream_id: *sid, subject: subj, payload: pay });
    }
    let mut c = 0u32;
    indexed.drain_stream(TARGET_STREAM, |_| c += 1);
    assert_eq!(c, STREAM_MSGS, "IndexedVec drain_stream: got {c}");
    let mut c = 0u32;
    indexed.drain_subject(TARGET_STREAM, EXACT_SUBJECT, |_| c += 1);
    assert_eq!(c, EXACT_PER_STREAM, "IndexedVec drain_subject: got {c}");
    let mut c = 0u32;
    indexed.drain_subtree(TARGET_STREAM, SUBTREE_PREFIX, |_| c += 1);
    assert_eq!(c, SUBTREE_PER_STREAM, "IndexedVec drain_subtree: got {c}");
}

// ── Benchmarks ──────────────────────────────────────────────────

fn bench_stores(c: &mut Criterion) {
    smoke_test();

    let data = make_entries_v2();

    // ── Append (all 1000 msgs across 5 streams) ────────────────

    {
        let mut group = c.benchmark_group("store_append");
        group.throughput(Throughput::Elements(N as u64));
        group.measurement_time(Duration::from_secs(5));

        group.bench_function("flat_vec", |b| {
            b.iter(|| {
                let mut store = FlatVecStore::new();
                for (sid, subj, pay) in &data {
                    store.append(EntryRef { stream_id: *sid, subject: subj, payload: pay });
                }
            });
        });

        group.bench_function("trie_store", |b| {
            b.iter(|| {
                let mut store = TrieStore::new();
                for (sid, subj, pay) in &data {
                    store.append(EntryRef { stream_id: *sid, subject: subj, payload: pay });
                }
            });
        });

        group.bench_function("ring_store", |b| {
            b.iter(|| {
                let mut store = RingStore::new();
                for (sid, subj, pay) in &data {
                    store.append(EntryRef { stream_id: *sid, subject: subj, payload: pay });
                }
            });
        });

        group.bench_function("indexed_vec", |b| {
            b.iter(|| {
                let mut store = IndexedVecStore::new();
                for (sid, subj, pay) in &data {
                    store.append(EntryRef { stream_id: *sid, subject: subj, payload: pay });
                }
            });
        });

        group.finish();
    }

    // Pre-fill all stores for read benchmarks
    let mut flat = FlatVecStore::new();
    let mut trie = TrieStore::new();
    let mut ring = RingStore::new();
    let mut indexed = IndexedVecStore::new();
    for (sid, subj, pay) in &data {
        flat.append(EntryRef { stream_id: *sid, subject: subj, payload: pay });
        trie.append(EntryRef { stream_id: *sid, subject: subj, payload: pay });
        ring.append(EntryRef { stream_id: *sid, subject: subj, payload: pay });
        indexed.append(EntryRef { stream_id: *sid, subject: subj, payload: pay });
    }

    // ── Drain stream (all 200 msgs from stream 0) ──────────────

    {
        let mut group = c.benchmark_group("store_drain_stream");
        group.throughput(Throughput::Elements(STREAM_MSGS as u64));
        group.measurement_time(Duration::from_secs(5));

        group.bench_function("flat_vec", |b| {
            b.iter(|| {
                let mut count = 0u64;
                flat.drain_stream(TARGET_STREAM, |_| count += black_box(1));
                assert_eq!(count, STREAM_MSGS as u64);
            });
        });

        group.bench_function("trie_store", |b| {
            b.iter(|| {
                let mut count = 0u64;
                trie.drain_stream(TARGET_STREAM, |_| count += black_box(1));
                assert_eq!(count, STREAM_MSGS as u64);
            });
        });

        group.bench_function("ring_store", |b| {
            b.iter(|| {
                let mut count = 0u64;
                ring.drain_stream(TARGET_STREAM, |_| count += black_box(1));
                assert_eq!(count, STREAM_MSGS as u64);
            });
        });

        group.bench_function("indexed_vec", |b| {
            b.iter(|| {
                let mut count = 0u64;
                indexed.drain_stream(TARGET_STREAM, |_| count += black_box(1));
                assert_eq!(count, STREAM_MSGS as u64);
            });
        });

        group.finish();
    }

    // ── Drain subject (exact: "orders.created" on stream 0, 20 msgs) ──

    {
        let mut group = c.benchmark_group("store_drain_subject");
        group.throughput(Throughput::Elements(EXACT_PER_STREAM as u64));
        group.measurement_time(Duration::from_secs(5));

        group.bench_function("flat_vec", |b| {
            b.iter(|| {
                let mut count = 0u64;
                flat.drain_subject(TARGET_STREAM, EXACT_SUBJECT, |_| count += black_box(1));
                assert_eq!(count, EXACT_PER_STREAM as u64);
            });
        });

        group.bench_function("trie_store", |b| {
            b.iter(|| {
                let mut count = 0u64;
                trie.drain_subject(TARGET_STREAM, EXACT_SUBJECT, |_| count += black_box(1));
                assert_eq!(count, EXACT_PER_STREAM as u64);
            });
        });

        group.bench_function("ring_store", |b| {
            b.iter(|| {
                let mut count = 0u64;
                ring.drain_subject(TARGET_STREAM, EXACT_SUBJECT, |_| count += black_box(1));
                assert_eq!(count, EXACT_PER_STREAM as u64);
            });
        });

        group.bench_function("indexed_vec", |b| {
            b.iter(|| {
                let mut count = 0u64;
                indexed.drain_subject(TARGET_STREAM, EXACT_SUBJECT, |_| count += black_box(1));
                assert_eq!(count, EXACT_PER_STREAM as u64);
            });
        });

        group.finish();
    }

    // ── Drain subtree ("orders.*" on stream 0, 60 msgs) ────────

    {
        let mut group = c.benchmark_group("store_drain_subtree");
        group.throughput(Throughput::Elements(SUBTREE_PER_STREAM as u64));
        group.measurement_time(Duration::from_secs(5));

        group.bench_function("flat_vec", |b| {
            b.iter(|| {
                let mut count = 0u64;
                flat.drain_subtree(TARGET_STREAM, SUBTREE_PREFIX, |_| count += black_box(1));
                assert_eq!(count, SUBTREE_PER_STREAM as u64);
            });
        });

        group.bench_function("trie_store", |b| {
            b.iter(|| {
                let mut count = 0u64;
                trie.drain_subtree(TARGET_STREAM, SUBTREE_PREFIX, |_| count += black_box(1));
                assert_eq!(count, SUBTREE_PER_STREAM as u64);
            });
        });

        group.bench_function("ring_store", |b| {
            b.iter(|| {
                let mut count = 0u64;
                ring.drain_subtree(TARGET_STREAM, SUBTREE_PREFIX, |_| count += black_box(1));
                assert_eq!(count, SUBTREE_PER_STREAM as u64);
            });
        });

        group.bench_function("indexed_vec", |b| {
            b.iter(|| {
                let mut count = 0u64;
                indexed.drain_subtree(TARGET_STREAM, SUBTREE_PREFIX, |_| count += black_box(1));
                assert_eq!(count, SUBTREE_PER_STREAM as u64);
            });
        });

        group.finish();
    }
}

criterion_group!(benches, bench_stores);
criterion_main!(benches);
