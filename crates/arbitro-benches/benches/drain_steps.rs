//! Benchmark: each step of the drain pipeline in isolation.
//!
//! 1. gate_signal    — release() → wait() latency (tokio::sync::Notify)
//! 2. store_get      — single store.get(seq, callback) read
//! 3. store_for_each — batch store.for_each(start, end, callback) read
//! 4. transport_send — single send_parts() call
//! 5. deliver_cycle  — full cycle (get_next_messages + send)
//!
//! All use direct engine components, no TCP.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tokio::runtime::Runtime;

use arbitro_engine::drain::ReactiveDrain;
use arbitro_engine::transport::Transport;
use arbitro_engine::DrainSignal;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverMode};
use arbitro_proto::ids::ConnId;
use arbitro_store::{EntryRef, MemoryStore, Store};

// ── Fake transport ──────────────────────────────────────────────

struct CountTransport {
    count: AtomicU32,
}

impl CountTransport {
    fn new() -> Self { Self { count: AtomicU32::new(0) } }
    fn reset(&self) { self.count.store(0, Relaxed); }
}

impl Transport for CountTransport {
    fn send(&self, _conn_id: ConnId, _data: &[u8]) -> bool {
        self.count.fetch_add(1, Relaxed);
        true
    }
    fn close(&self, _conn_id: ConnId) {}
}

// ── Helpers ─────────────────────────────────────────────────────

fn fill_store(store: &mut MemoryStore, n: u64) {
    for i in 0..n {
        store.append(EntryRef { subject: b"bench.msg", payload: &[0u8; 64] }, 1000 + i).unwrap();
    }
}

fn ff_config(name: &[u8], id: u32) -> ConsumerConfig {
    let mut cfg = ConsumerConfig::new(name, b"TEST")
        .filter(b">")
        .ack_policy(AckPolicy::None)
        .deliver_mode(DeliverMode::Fanout)
        .build();
    cfg.consumer_id = id;
    cfg
}

fn ack_config(name: &[u8], id: u32, max_inflight: u16) -> ConsumerConfig {
    let mut cfg = ConsumerConfig::new(name, b"TEST")
        .filter(b">")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(max_inflight)
        .deliver_mode(DeliverMode::Fanout)
        .build();
    cfg.consumer_id = id;
    cfg
}

// ── Benchmarks ──────────────────────────────────────────────────

fn bench_drain_steps(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // ── 1. Gate signal latency ──────────────────────────────────
    {
        use arbitro_server::gate::Gate;

        let mut group = c.benchmark_group("gate_signal");
        group.bench_function("release_wait", |b| {
            let gate = Arc::new(Gate::new());
            b.iter(|| {
                let g = gate.clone();
                rt.block_on(async {
                    g.release();
                    g.wait().await;
                });
            });
        });
        group.finish();
    }

    // ── 2. store.get — single read ──────────────────────────────
    {
        let mut group = c.benchmark_group("store_get");

        for &n in &[100u64, 1000] {
            let mut store = MemoryStore::new();
            fill_store(&mut store, n);

            group.bench_function(format!("seq_mid_{n}"), |b| {
                let mid = n / 2;
                b.iter(|| {
                    store.get(mid, &mut |_entry| {}).unwrap();
                });
            });
        }
        group.finish();
    }

    // ── 3. store.for_each — batch read ──────────────────────────
    {
        let mut group = c.benchmark_group("store_for_each");

        for &n in &[10u64, 100, 1000] {
            let mut store = MemoryStore::new();
            fill_store(&mut store, n);

            group.throughput(Throughput::Elements(n));
            group.bench_function(format!("{n}msgs"), |b| {
                b.iter(|| {
                    let mut count = 0u64;
                    store.for_each(1, n + 1, &mut |_entry| { count += 1; }).unwrap();
                    assert_eq!(count, n);
                });
            });
        }
        group.finish();
    }

    // ── 4. transport.send_parts — single send ───────────────────
    {
        let mut group = c.benchmark_group("transport_send");
        let transport = CountTransport::new();

        let envelope = [0u8; 16];
        let subj_len = 9u16.to_le_bytes();
        let subject = b"bench.msg";
        let payload = [0u8; 64];

        group.bench_function("send_parts_64B", |b| {
            b.iter(|| {
                transport.send_parts(1, &[&envelope, &subj_len, subject, &payload]);
            });
        });
        group.finish();
    }

    // ── 5. deliver_cycle — fire-and-forget ──────────────────────
    {
        let mut group = c.benchmark_group("deliver_cycle_ff");
        let transport = CountTransport::new();

        for &n in &[10u64, 100, 1000] {
            let mut store = MemoryStore::new();
            fill_store(&mut store, n);

            group.throughput(Throughput::Elements(n));
            group.bench_function(format!("{n}msgs"), |b| {
                b.iter(|| {
                    let mut drain = ReactiveDrain::new(1);
                    drain.add_consumer(ff_config(b"c1", 1), 1);
                    drain.bind(1, 100);
                    transport.reset();

                    while drain.deliver_cycle(&store, &transport, 0) {}
                });
            });
        }
        group.finish();
    }

    // ── 6. deliver_cycle — explicit ack ─────────────────────────
    {
        let mut group = c.benchmark_group("deliver_cycle_ack");
        let transport = CountTransport::new();

        for &n in &[10u64, 100, 1000] {
            let mut store = MemoryStore::new();
            fill_store(&mut store, n);

            group.throughput(Throughput::Elements(n));
            group.bench_function(format!("{n}msgs"), |b| {
                b.iter(|| {
                    let mut drain = ReactiveDrain::new(1);
                    drain.add_consumer(ack_config(b"c1", 1, 1000), 1);
                    drain.bind(1, 100);
                    transport.reset();

                    while drain.deliver_cycle(&store, &transport, 0) {}
                });
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_drain_steps);
criterion_main!(benches);
