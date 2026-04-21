//! Topology graph — is a bidirectional graph between Connection /
//! Consumer / Stream / Subscription worth the bookkeeping?
//!
//! Scenario — a server holding 10k live connections, 100 streams
//! (shared across many conns), 10k consumers, 50k subscriptions
//! (`Subscription = (ConnId, ConsumerId, StreamId)`), average 5
//! subs per connection.
//!
//! Three operations stressed:
//!
//! 1. **Cascade drop** — a connection dies. Find every sub it owned,
//!    decrement refcounts on its consumers and streams, evict the
//!    ones whose refcount hit zero. This is what the user asked
//!    about: "cuando una conexión cae, saca subs/stream/consumer
//!    si no están replicados en otras conexiones".
//!
//! 2. **subs_by_stream** — the drain hot path. Given a stream, list
//!    every live subscription routing from it.
//!
//! 3. **Bind + unbind churn** — cold but real. A stable server sees
//!    constant subscribe/unsubscribe traffic.
//!
//! Two representations compared:
//!
//! **A — Flat (current-ish)**
//!   `Vec<Subscription>` with slot recycle, plus three secondary
//!   indexes: `HashMap<ConnId, Vec<SubId>>`, `HashMap<StreamId, …>`,
//!   `HashMap<ConsumerId, …>`, all with foldhash. Cascade drop:
//!   lookup conn index → iterate its subs → per sub look up consumer
//!   and stream in two more HashMaps.
//!
//! **B — Graph (node-owned inline lists)**
//!   `Box<[ConnNode]>`, `Box<[StreamNode]>`, `Box<[ConsumerNode]>`
//!   indexed directly by dense IDs. Each node holds an inline
//!   `Vec<SubId>`. `Subscription` stores the triple. Cascade drop:
//!   direct index → iterate inline subs → direct index into
//!   consumer/stream nodes, decrement refcount, swap_remove from
//!   their inline lists.
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench topology_graph -p arbitro-server --no-run"
//!   wsl bash -lc "
//!     mkdir -p /tmp/arbitro &&
//!     cp -a target/release/deps/topology_graph-* /tmp/arbitro/ &&
//!     cd /tmp/arbitro &&
//!     timeout 120 ./topology_graph-<hash> --bench 2>&1 | tee /tmp/bench.log
//!   "

#![allow(unused)]

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

use foldhash::fast::FixedState;

// ── RNG ─────────────────────────────────────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed.max(1)) }
    #[inline] fn next(&mut self) -> u64 {
        let mut x = self.0; x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        self.0 = x; x
    }
    #[inline] fn range(&mut self, n: u32) -> u32 { (self.next() as u32) % n }
}

// ── Workload params ─────────────────────────────────────────────────────────

const N_CONNS:     u32   = 10_000;
const N_STREAMS:   u32   = 100;
const N_CONSUMERS: u32   = 10_000;
const N_SUBS:      usize = 50_000;

// Avg degrees (derived): ~5 subs per conn, ~500 subs per stream,
// ~5 subs per consumer. Realistic for a mid-sized broker.

// Ops counts (for per-op averaging)
const DROP_OPS:      usize = 5_000;     // conn drops
const LOOKUP_OPS:    usize = 100_000;   // subs_by_stream lookups
const CHURN_OPS:     usize = 100_000;   // bind+unbind pairs

// ── Shared types ────────────────────────────────────────────────────────────

type ConnId     = u32;
type StreamId   = u32;
type ConsumerId = u32;
type SubId      = u32;

#[derive(Clone, Copy, Debug)]
struct Subscription {
    conn:     ConnId,
    consumer: ConsumerId,
    stream:   StreamId,
    alive:    bool,
}

// ── A. Flat (HashMap secondary indexes) ─────────────────────────────────────

struct Flat {
    subs: Vec<Subscription>,
    // Consumer/Stream refcounts (how many conns reference them)
    consumer_refs: HashMap<ConsumerId, u32, FixedState>,
    stream_refs:   HashMap<StreamId,   u32, FixedState>,
    // Secondary indexes
    subs_by_conn:     HashMap<ConnId,     Vec<SubId>, FixedState>,
    subs_by_stream:   HashMap<StreamId,   Vec<SubId>, FixedState>,
    subs_by_consumer: HashMap<ConsumerId, Vec<SubId>, FixedState>,
    free_slots: Vec<SubId>,
}

impl Flat {
    fn new() -> Self {
        let s = FixedState::default();
        Self {
            subs: Vec::with_capacity(N_SUBS * 2),
            consumer_refs:    HashMap::with_capacity_and_hasher(N_CONSUMERS as usize, s),
            stream_refs:      HashMap::with_capacity_and_hasher(N_STREAMS as usize, s),
            subs_by_conn:     HashMap::with_capacity_and_hasher(N_CONNS as usize, s),
            subs_by_stream:   HashMap::with_capacity_and_hasher(N_STREAMS as usize, s),
            subs_by_consumer: HashMap::with_capacity_and_hasher(N_CONSUMERS as usize, s),
            free_slots: Vec::with_capacity(N_SUBS / 4),
        }
    }

    fn bind(&mut self, conn: ConnId, cons: ConsumerId, stream: StreamId) -> SubId {
        let sub = Subscription { conn, consumer: cons, stream, alive: true };
        let id = if let Some(slot) = self.free_slots.pop() {
            self.subs[slot as usize] = sub;
            slot
        } else {
            self.subs.push(sub);
            (self.subs.len() - 1) as SubId
        };
        self.subs_by_conn.entry(conn).or_default().push(id);
        self.subs_by_stream.entry(stream).or_default().push(id);
        self.subs_by_consumer.entry(cons).or_default().push(id);
        *self.consumer_refs.entry(cons).or_insert(0) += 1;
        *self.stream_refs.entry(stream).or_insert(0) += 1;
        id
    }

    fn unbind(&mut self, id: SubId) {
        let sub = self.subs[id as usize];
        if !sub.alive { return; }
        self.subs[id as usize].alive = false;
        remove_from_vec(self.subs_by_conn.get_mut(&sub.conn).unwrap(), id);
        remove_from_vec(self.subs_by_stream.get_mut(&sub.stream).unwrap(), id);
        remove_from_vec(self.subs_by_consumer.get_mut(&sub.consumer).unwrap(), id);
        let cr = self.consumer_refs.get_mut(&sub.consumer).unwrap();
        *cr -= 1; if *cr == 0 { self.consumer_refs.remove(&sub.consumer); }
        let sr = self.stream_refs.get_mut(&sub.stream).unwrap();
        *sr -= 1; if *sr == 0 { self.stream_refs.remove(&sub.stream); }
        self.free_slots.push(id);
    }

    /// Cascade drop — the measured hot op.
    #[inline(never)]
    fn drop_connection(&mut self, conn: ConnId, evicted: &mut u32) {
        let sub_ids = match self.subs_by_conn.remove(&conn) {
            Some(v) => v, None => return,
        };
        for id in &sub_ids {
            let sub = self.subs[*id as usize];
            if !sub.alive { continue; }
            self.subs[*id as usize].alive = false;
            // Remove from stream index
            remove_from_vec(self.subs_by_stream.get_mut(&sub.stream).unwrap(), *id);
            // Remove from consumer index
            remove_from_vec(self.subs_by_consumer.get_mut(&sub.consumer).unwrap(), *id);
            // Decrement refcounts; evict on zero
            let cr = self.consumer_refs.get_mut(&sub.consumer).unwrap();
            *cr -= 1;
            if *cr == 0 {
                self.consumer_refs.remove(&sub.consumer);
                *evicted += 1;
            }
            let sr = self.stream_refs.get_mut(&sub.stream).unwrap();
            *sr -= 1;
            if *sr == 0 {
                self.stream_refs.remove(&sub.stream);
                *evicted += 1;
            }
            self.free_slots.push(*id);
        }
    }

    #[inline(never)]
    fn list_subs_by_stream(&self, stream: StreamId, sink: &mut u64) {
        if let Some(v) = self.subs_by_stream.get(&stream) {
            for id in v {
                let s = self.subs[*id as usize];
                *sink = sink.wrapping_add(s.conn as u64 ^ s.consumer as u64);
            }
        }
    }
}

#[inline]
fn remove_from_vec(v: &mut Vec<SubId>, id: SubId) {
    if let Some(pos) = v.iter().position(|&x| x == id) {
        v.swap_remove(pos);
    }
}

// ── B. Graph (node-owned inline lists) ──────────────────────────────────────

#[derive(Default)]
struct ConnNode {
    subs: Vec<SubId>,
    alive: bool,
}

#[derive(Default)]
struct StreamNode {
    subs: Vec<SubId>,
    refcount: u32,
}

#[derive(Default)]
struct ConsumerNode {
    subs: Vec<SubId>,
    refcount: u32,
}

struct Graph {
    conns:     Box<[ConnNode]>,
    streams:   Box<[StreamNode]>,
    consumers: Box<[ConsumerNode]>,
    subs:      Vec<Subscription>,
    free_slots: Vec<SubId>,
}

impl Graph {
    fn new() -> Self {
        let conns = (0..N_CONNS).map(|_| ConnNode { subs: Vec::new(), alive: true })
            .collect::<Vec<_>>().into_boxed_slice();
        let streams = (0..N_STREAMS).map(|_| StreamNode::default())
            .collect::<Vec<_>>().into_boxed_slice();
        let consumers = (0..N_CONSUMERS).map(|_| ConsumerNode::default())
            .collect::<Vec<_>>().into_boxed_slice();
        Self {
            conns, streams, consumers,
            subs: Vec::with_capacity(N_SUBS * 2),
            free_slots: Vec::with_capacity(N_SUBS / 4),
        }
    }

    fn bind(&mut self, conn: ConnId, cons: ConsumerId, stream: StreamId) -> SubId {
        let sub = Subscription { conn, consumer: cons, stream, alive: true };
        let id = if let Some(slot) = self.free_slots.pop() {
            self.subs[slot as usize] = sub;
            slot
        } else {
            self.subs.push(sub);
            (self.subs.len() - 1) as SubId
        };
        self.conns[conn as usize].subs.push(id);
        self.streams[stream as usize].subs.push(id);
        self.consumers[cons as usize].subs.push(id);
        self.streams[stream as usize].refcount += 1;
        self.consumers[cons as usize].refcount += 1;
        id
    }

    fn unbind(&mut self, id: SubId) {
        let sub = self.subs[id as usize];
        if !sub.alive { return; }
        self.subs[id as usize].alive = false;
        remove_from_vec(&mut self.conns[sub.conn as usize].subs, id);
        remove_from_vec(&mut self.streams[sub.stream as usize].subs, id);
        remove_from_vec(&mut self.consumers[sub.consumer as usize].subs, id);
        self.streams[sub.stream as usize].refcount -= 1;
        self.consumers[sub.consumer as usize].refcount -= 1;
        self.free_slots.push(id);
    }

    #[inline(never)]
    fn drop_connection(&mut self, conn: ConnId, evicted: &mut u32) {
        // Safety: direct dense index — no hash.
        // Take the Vec out so we can iterate without holding a borrow.
        let sub_ids = std::mem::take(&mut self.conns[conn as usize].subs);
        self.conns[conn as usize].alive = false;
        for id in &sub_ids {
            let sub = self.subs[*id as usize];
            if !sub.alive { continue; }
            self.subs[*id as usize].alive = false;

            let sn = &mut self.streams[sub.stream as usize];
            remove_from_vec(&mut sn.subs, *id);
            sn.refcount -= 1;
            if sn.refcount == 0 { *evicted += 1; }

            let cn = &mut self.consumers[sub.consumer as usize];
            remove_from_vec(&mut cn.subs, *id);
            cn.refcount -= 1;
            if cn.refcount == 0 { *evicted += 1; }

            self.free_slots.push(*id);
        }
    }

    #[inline(never)]
    fn list_subs_by_stream(&self, stream: StreamId, sink: &mut u64) {
        let sn = &self.streams[stream as usize];
        for id in &sn.subs {
            let s = self.subs[*id as usize];
            *sink = sink.wrapping_add(s.conn as u64 ^ s.consumer as u64);
        }
    }
}

// ── Dataset: pre-generated bind plan ────────────────────────────────────────

#[derive(Clone, Copy)]
struct BindPlan { conn: ConnId, cons: ConsumerId, stream: StreamId }

fn build_plan(rng: &mut Rng) -> Vec<BindPlan> {
    let mut plan = Vec::with_capacity(N_SUBS);
    for i in 0..N_SUBS {
        plan.push(BindPlan {
            // uniform conn, skewed stream (hot streams), consumer close to conn
            conn:   rng.range(N_CONNS),
            stream: rng.range(N_STREAMS),
            cons:   rng.range(N_CONSUMERS),
        });
    }
    plan
}

// ── Benchmarks ──────────────────────────────────────────────────────────────

fn bench_flat(plan: &[BindPlan], drop_conns: &[ConnId], lookups: &[StreamId],
              churn: &[BindPlan]) -> (f64, f64, f64, u32) {
    let mut f = Flat::new();
    for p in plan { f.bind(p.conn, p.cons, p.stream); }

    // Drop cascade
    let mut evicted = 0u32;
    let start = Instant::now();
    for &c in drop_conns { f.drop_connection(c, &mut evicted); }
    let t_drop = start.elapsed().as_nanos() as f64 / drop_conns.len() as f64;

    // Re-populate after drops so lookup has data
    for p in plan { if !f.subs.iter().any(|s| s.alive && s.conn == p.conn
                       && s.consumer == p.cons && s.stream == p.stream) {
        f.bind(p.conn, p.cons, p.stream);
    }}

    // Lookup: subs_by_stream
    let mut sink = 0u64;
    let start = Instant::now();
    for &s in lookups { f.list_subs_by_stream(s, &mut sink); }
    let t_lookup = start.elapsed().as_nanos() as f64 / lookups.len() as f64;
    black_box(sink);

    // Churn: bind+unbind round-trips. Use a fresh set of slots.
    let start = Instant::now();
    let mut temp_ids = Vec::with_capacity(churn.len());
    for p in churn { temp_ids.push(f.bind(p.conn, p.cons, p.stream)); }
    for id in &temp_ids { f.unbind(*id); }
    let t_churn = start.elapsed().as_nanos() as f64 / (churn.len() * 2) as f64;

    (t_drop, t_lookup, t_churn, evicted)
}

fn bench_graph(plan: &[BindPlan], drop_conns: &[ConnId], lookups: &[StreamId],
               churn: &[BindPlan]) -> (f64, f64, f64, u32) {
    let mut g = Graph::new();
    for p in plan { g.bind(p.conn, p.cons, p.stream); }

    let mut evicted = 0u32;
    let start = Instant::now();
    for &c in drop_conns { g.drop_connection(c, &mut evicted); }
    let t_drop = start.elapsed().as_nanos() as f64 / drop_conns.len() as f64;

    // Re-populate
    for p in plan {
        let already = g.subs.iter().any(|s| s.alive
            && s.conn == p.conn && s.consumer == p.cons && s.stream == p.stream);
        if !already { g.bind(p.conn, p.cons, p.stream); }
    }

    let mut sink = 0u64;
    let start = Instant::now();
    for &s in lookups { g.list_subs_by_stream(s, &mut sink); }
    let t_lookup = start.elapsed().as_nanos() as f64 / lookups.len() as f64;
    black_box(sink);

    let start = Instant::now();
    let mut temp_ids = Vec::with_capacity(churn.len());
    for p in churn { temp_ids.push(g.bind(p.conn, p.cons, p.stream)); }
    for id in &temp_ids { g.unbind(*id); }
    let t_churn = start.elapsed().as_nanos() as f64 / (churn.len() * 2) as f64;

    (t_drop, t_lookup, t_churn, evicted)
}

// ── Memory estimate (ballpark) ──────────────────────────────────────────────

fn estimate_flat_mem() -> usize {
    let sub_size   = std::mem::size_of::<Subscription>();
    let subs       = N_SUBS * sub_size;
    // 3 HashMap<K, Vec<u32>> indexes. ~48B per Vec header + SubIds.
    let vec_header = 24;
    let idx_conn     = N_CONNS as usize * (vec_header + 8 /*hashmap slot*/) + N_SUBS * 4;
    let idx_stream   = N_STREAMS as usize * (vec_header + 8) + N_SUBS * 4;
    let idx_consumer = N_CONSUMERS as usize * (vec_header + 8) + N_SUBS * 4;
    // Refcount HashMaps
    let ref_maps = (N_CONSUMERS as usize + N_STREAMS as usize) * 12;
    subs + idx_conn + idx_stream + idx_consumer + ref_maps
}

fn estimate_graph_mem() -> usize {
    let sub_size   = std::mem::size_of::<Subscription>();
    let subs       = N_SUBS * sub_size;
    let conn_node  = std::mem::size_of::<ConnNode>();
    let strm_node  = std::mem::size_of::<StreamNode>();
    let cons_node  = std::mem::size_of::<ConsumerNode>();
    subs
        + N_CONNS     as usize * conn_node + N_SUBS * 4
        + N_STREAMS   as usize * strm_node + N_SUBS * 4
        + N_CONSUMERS as usize * cons_node + N_SUBS * 4
}

fn main() {
    println!("\nTopology graph — is a bidirectional node graph worth it?");
    println!("========================================================");
    println!(
        "N_CONNS={N_CONNS}  N_STREAMS={N_STREAMS}  N_CONSUMERS={N_CONSUMERS}  N_SUBS={N_SUBS}"
    );
    println!(
        "DROP_OPS={DROP_OPS}  LOOKUP_OPS={LOOKUP_OPS}  CHURN_OPS={CHURN_OPS}\n"
    );

    let mut rng = Rng::new(0xC0FFEE);
    let plan = build_plan(&mut rng);
    let drop_conns: Vec<ConnId> = (0..DROP_OPS).map(|_| rng.range(N_CONNS)).collect();
    let lookups:    Vec<StreamId> = (0..LOOKUP_OPS).map(|_| rng.range(N_STREAMS)).collect();
    let churn: Vec<BindPlan> = (0..CHURN_OPS).map(|_| BindPlan {
        conn:   rng.range(N_CONNS),
        stream: rng.range(N_STREAMS),
        cons:   rng.range(N_CONSUMERS),
    }).collect();

    let (f_drop, f_look, f_churn, f_ev) =
        bench_flat(&plan, &drop_conns, &lookups, &churn);
    let (g_drop, g_look, g_churn, g_ev) =
        bench_graph(&plan, &drop_conns, &lookups, &churn);

    println!(
        "{:<38} | {:>14} | {:>14} | {:>10}",
        "Operation", "A — Flat", "B — Graph", "speedup"
    );
    println!("{}", "-".repeat(86));
    print_row("Cascade drop_connection (ns/op)",  f_drop,  g_drop);
    print_row("subs_by_stream        (ns/op)",    f_look,  g_look);
    print_row("bind+unbind churn     (ns/op)",    f_churn, g_churn);

    println!();
    println!("Evicted on cascade:  A={} nodes   B={} nodes  (should match)", f_ev, g_ev);
    println!();
    println!(
        "Memory estimate:     A ≈ {:>5.1} MB    B ≈ {:>5.1} MB",
        estimate_flat_mem() as f64 / 1024.0 / 1024.0,
        estimate_graph_mem() as f64 / 1024.0 / 1024.0,
    );
    println!();
}

fn print_row(label: &str, a: f64, b: f64) {
    println!("{:<38} | {:>11.1} ns | {:>11.1} ns | {:>8.2}×",
             label, a, b, a / b);
}
