//! Where the lifecycle's time goes: how many heap allocations each
//! catalog operation makes.
//!
//! Not a benchmark — a count. `subscribe` measures 1.07 us and
//! `unsubscribe` 1.69 us against a 41 ns delivery, and the question is
//! whether that is work or allocator traffic. A count answers it without
//! guessing: an operation that should touch three fields and allocates
//! five times has its answer here, not in a profile.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use arbitro_engine::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine::command::{Command, DeliveredEntry};
use arbitro_engine::types::*;
use arbitro_engine::ArbitroEngine;

/// Counts every allocation and the bytes behind them.
struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(l.size(), Ordering::Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new.saturating_sub(l.size()), Ordering::Relaxed);
        System.realloc(p, l, new)
    }
}

#[global_allocator]
static A: Counting = Counting;

fn snap() -> (usize, usize) {
    (ALLOCS.load(Ordering::Relaxed), BYTES.load(Ordering::Relaxed))
}

fn report(name: &str, ops: usize, before: (usize, usize)) {
    let after = snap();
    let a = after.0 - before.0;
    let b = after.1 - before.1;
    println!(
        "{name:<28}{a:>8} allocs {:>7.2}/op {b:>10} B {:>9.1} B/op",
        a as f64 / ops as f64,
        b as f64 / ops as f64
    );
}

const CONSUMERS: u32 = 20;
const SUBS: u32 = 10;
const PENDINGS: u64 = 10;

/// One stream with `CONSUMERS` consumers and `SUBS` subscriptions each,
/// every subscription on its own connection.
fn world(engine: &mut ArbitroEngine, stream: StreamId) -> Vec<BindingId> {
    engine
        .create_stream(StreamConfig {
            id: stream,
            name: b"bench".to_vec(),
        })
        .unwrap();
    let mut ids = Vec::with_capacity((CONSUMERS * SUBS) as usize);
    for c in 0..CONSUMERS {
        engine
            .create_consumer(ConsumerConfig {
                id: ConsumerId(c + 1),
                queue_id: QueueId(0),
                stream_id: stream,
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100_000,
                ack_wait_ms: 0,
                max_nack: 0,
                filter: Box::from(&b""[..]),
            })
            .unwrap();
        for s in 0..SUBS {
            let sub = SubscriptionId(c * SUBS + s + 1);
            engine
                .create_subscription(SubscriptionConfig {
                    id: sub,
                    external_id: s,
                    stream_id: stream,
                    consumer_id: ConsumerId(c + 1),
                    filters: vec![format!("bench.c{c}.s{s}").into_bytes()],
                })
                .unwrap();
            let conn = ConnectionId((c * SUBS + s) as u64 + 1);
            engine.open_connection(conn, NodeId(0));
            let (id, _) = engine.subscribe(conn, sub);
            ids.push(id.unwrap());
        }
    }
    ids
}

fn main() {
    let bindings = (CONSUMERS * SUBS) as usize;
    let stream = StreamId(1);

    println!("{CONSUMERS} consumers x {SUBS} subscriptions = {bindings} bindings\n");

    // ── Build, step by step ─────────────────────────────────────────────
    let mut engine = ArbitroEngine::new();

    let b0 = snap();
    engine
        .create_stream(StreamConfig {
            id: stream,
            name: b"bench".to_vec(),
        })
        .unwrap();
    report("create_stream", 1, b0);

    let b0 = snap();
    for c in 0..CONSUMERS {
        engine
            .create_consumer(ConsumerConfig {
                id: ConsumerId(c + 1),
                queue_id: QueueId(0),
                stream_id: stream,
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100_000,
                ack_wait_ms: 0,
                max_nack: 0,
                filter: Box::from(&b""[..]),
            })
            .unwrap();
    }
    report("create_consumer", CONSUMERS as usize, b0);

    // The filter `Vec` is the caller's, not the engine's — counted so the
    // engine's own share is not inflated by the harness.
    let b0 = snap();
    let mut filters = Vec::with_capacity(bindings);
    for c in 0..CONSUMERS {
        for s in 0..SUBS {
            filters.push(vec![format!("bench.c{c}.s{s}").into_bytes()]);
        }
    }
    report("(harness: filter vecs)", bindings, b0);

    let b0 = snap();
    for (i, f) in filters.into_iter().enumerate() {
        engine
            .create_subscription(SubscriptionConfig {
                id: SubscriptionId(i as u32 + 1),
                external_id: (i as u32) % SUBS,
                stream_id: stream,
                consumer_id: ConsumerId((i as u32) / SUBS + 1),
                filters: f,
            })
            .unwrap();
    }
    report("create_subscription", bindings, b0);

    let b0 = snap();
    for i in 0..bindings as u64 {
        engine.open_connection(ConnectionId(i + 1), NodeId(0));
    }
    report("open_connection", bindings, b0);

    let mut binding_ids = Vec::with_capacity(bindings);
    let b0 = snap();
    for i in 0..bindings as u64 {
        let (id, _ev) = engine.subscribe(ConnectionId(i + 1), SubscriptionId(i as u32 + 1));
        binding_ids.push(id.unwrap());
    }
    report("subscribe", bindings, b0);

    let entries: Vec<DeliveredEntry> = (0..PENDINGS)
        .map(|k| DeliveredEntry {
            seq: k + 1,
            subject_hash: (k as u32) | 1,
            _pad: 0,
        })
        .collect();
    let b0 = snap();
    for &binding in &binding_ids {
        let _ = engine.execute(&Command::Delivered {
            stream_id: stream,
            binding_id: binding,
            entries: &entries,
        });
    }
    report("Delivered x10", bindings, b0);

    // ── Teardown, three ways, each on its own world ─────────────────────
    println!();

    let b0 = snap();
    let _ = engine.delete_stream(stream);
    report("delete_stream (per binding)", bindings, b0);

    let mut engine = ArbitroEngine::new();
    let ids = world(&mut engine, stream);
    let b0 = snap();
    for &id in &ids {
        let _ = engine.unsubscribe(id);
    }
    report("unsubscribe", bindings, b0);

    let mut engine = ArbitroEngine::new();
    let _ = world(&mut engine, stream);
    let b0 = snap();
    for c in 0..CONSUMERS {
        let _ = engine.delete_consumer(ConsumerId(c + 1));
    }
    report("delete_consumer (per binding)", bindings, b0);

    let mut engine = ArbitroEngine::new();
    let _ = world(&mut engine, stream);
    let b0 = snap();
    for i in 0..bindings as u64 {
        let _ = engine.mark_connection_dead(ConnectionId(i + 1));
    }
    report("mark_connection_dead", bindings, b0);

    // ── The hot path, for scale ─────────────────────────────────────────
    println!();
    let mut engine = ArbitroEngine::new();
    let ids = world(&mut engine, stream);
    let binding = ids[0];
    let b0 = snap();
    for r in 0..100u64 {
        let e: Vec<DeliveredEntry> = (0..PENDINGS)
            .map(|k| DeliveredEntry {
                seq: r * PENDINGS + k + 1,
                subject_hash: (k as u32) | 1,
                _pad: 0,
            })
            .collect();
        let _ = engine.execute(&Command::Delivered {
            stream_id: stream,
            binding_id: binding,
            entries: &e,
        });
    }
    report("Delivered x10 (100 rounds)", 100, b0);
}
