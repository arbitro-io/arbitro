//! Benchmark: compare serial vs broadcast-based multi-actor propagation.
//!
//! Measures the full round-trip lifecycle:
//! 1. Serial: Append -> Signal -> RepOk.
//! 2. Broadcast: Append -> Emit -> [Wait for Listener A + Wait for Listener B].
//!
//! A cycle is complete when both listeners have received the event and signaled back.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tokio::runtime::Runtime;
use tokio::sync::{broadcast, oneshot};

// ── Event Structure ─────────────────────────────────────────────

#[derive(Clone, Copy, Debug)]
struct StreamEvent {
    _seq: u64,
}

// ── Benchmarks ──────────────────────────────────────────────────

fn bench_cycle_comparison(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("lifecycle_cycle");
    group.throughput(Throughput::Elements(1));

    // ── 1. Serial Strategy (Baseline) ──
    group.bench_function("serial_sequential_all", |b| {
        let signal_count = AtomicU64::new(0);
        let ok_count = AtomicU64::new(0);
        
        b.iter(|| {
            let seq = black_box(1u64);
            // Simulated side effects
            ok_count.fetch_add(seq, Relaxed);
            signal_count.fetch_add(seq, Relaxed);
        });
    });

    // ── 2. Broadcast Strategy (Propagation Latency) ──
    group.bench_function("broadcast_round_trip", |b| {
        let (tx, _rx) = broadcast::channel::<StreamEvent>(1024);
        let mut current_seq = 0u64;

        b.iter(|| {
            current_seq += 1;
            let seq = black_box(current_seq);
            let mut rx_ok = tx.subscribe();
            let mut rx_sig = tx.subscribe();

            // Measure the time to send AND for both tasks to receive
            rt.block_on(async {
                let (done_ok_tx, done_ok_rx) = oneshot::channel();
                let (done_sig_tx, done_sig_rx) = oneshot::channel();

                // Listener A (RepOk)
                tokio::spawn(async move {
                    if let Ok(_) = rx_ok.recv().await {
                        let _ = done_ok_tx.send(());
                    }
                });

                // Listener B (Drain)
                tokio::spawn(async move {
                    if let Ok(_) = rx_sig.recv().await {
                        let _ = done_sig_tx.send(());
                    }
                });

                // Emit event
                let _ = tx.send(StreamEvent { _seq: seq });

                // Wait for BOTH receivers to finish the cycle
                let _ = tokio::join!(done_ok_rx, done_sig_rx);
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_cycle_comparison);
criterion_main!(benches);
