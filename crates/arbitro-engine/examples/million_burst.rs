//! Manual benchmark: 1M messages through full cycle in bursts of 256.
//!
//! No criterion, no framework — raw Instant::now() timing.
//!
//! Run:  cargo run --example million_burst --release

use arbitro_engine::batch::*;
use arbitro_engine::catalog::{fnv1a_32, ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine::types::*;
use arbitro_engine::*;
use std::time::Instant;

const TOTAL: usize = 1_000_000;
const BURST: usize = 256;
const BURSTS: usize = TOTAL / BURST;
const WARMUP_ROUNDS: usize = 3;
const MEASURE_ROUNDS: usize = 10;

fn setup_engine() -> ArbitroEngine {
    let mut e = ArbitroEngine::new();

    e.ensure_stream(StreamConfig {
        id: StreamId(1),
        name: b"orders".to_vec(),
    })
    .unwrap();

    e.ensure_consumer(ConsumerConfig {
        id: ConsumerId(1),
        queue_id: QueueId(1),
        stream_id: StreamId(1),
        durable: true,
        ack_policy: AckPolicy::Explicit,
        max_inflight: BURST as u32 + 1,
    })
    .unwrap();

    e.ensure_subscription(SubscriptionConfig {
        id: SubscriptionId(1),
        stream_id: StreamId(1),
        consumer_id: ConsumerId(1),
        filters: vec![],
    })
    .unwrap();

    e.open_connection(&OpenConnectionReq {
        connection_id: ConnectionId(100),
        node_id: NodeId(1),
        now: Timestamp::new(0),
    });

    e.bind(&BindBatch {
        entries: &[BindEntry {
            connection_id: ConnectionId(100),
            subscription_id: SubscriptionId(1),
        }],
        now: Timestamp::new(0),
    });

    e
}

struct BurstData {
    subjects: Vec<Vec<u8>>,
    hashes: Vec<u32>,
}

fn generate_data() -> BurstData {
    let subjects: Vec<Vec<u8>> = (0..TOTAL)
        .map(|i| format!("order.burst_{}.seq_{}", i / BURST, i % BURST).into_bytes())
        .collect();
    let hashes: Vec<u32> = subjects.iter().map(|s| fnv1a_32(s)).collect();
    BurstData { subjects, hashes }
}

fn run_cycle(data: &BurstData, payload: &[u8]) -> std::time::Duration {
    let mut engine = setup_engine();
    let mut seq_counter: u64 = 1;

    let start = Instant::now();

    for burst in 0..BURSTS {
        let base = burst * BURST;

        // ── Publish ──
        let pub_entries: Vec<PublishEntry<'_>> = (0..BURST)
            .map(|j| {
                let idx = base + j;
                PublishEntry {
                    subject_hash: data.hashes[idx],
                    subject: &data.subjects[idx],
                    payload: PayloadRef::Borrowed(payload),
                    idempotency_key: 0,
                    credits_cost: 1,
                }
            })
            .collect();

        engine.publish(&PublishBatch {
            stream_id: StreamId(1),
            entries: &pub_entries,
            now: Timestamp::new(seq_counter),
        });

        // ── Drain fanout ──
        let _drain = engine.drain_fanout();
        drop(_drain);

        // ── Claim ──
        let ack_entries: Vec<AckEntry>;
        {
            let claimed = engine.claim(
                &ClaimBatch {
                    queue_id: QueueId(1),
                    connection_id: ConnectionId(100),
                    consumer_id: ConsumerId(1),
                    max_items: BURST as u16,
                    now: Timestamp::new(seq_counter + 1),
                },
                SubscriptionId(0),
                BindingId(0),
            );
            ack_entries = claimed
                .entries()
                .iter()
                .map(|e| AckEntry { seq: e.seq })
                .collect();
        }

        // ── Ack ──
        engine.ack(&AckBatch {
            consumer_id: ConsumerId(1),
            entries: &ack_entries,
            now: Timestamp::new(seq_counter + 2),
        });

        seq_counter += 3;
    }

    let elapsed = start.elapsed();

    // Sanity check
    assert_eq!(
        engine
            .ctx()
            .inflight
            .get(arbitro_engine::inflight::InFlightScope::Consumer, 1),
        0,
        "inflight leak"
    );

    elapsed
}

fn main() {
    println!("=== million_burst: 1M msgs × burst 256 ===");
    println!(
        "  {} bursts, full cycle: publish → drain → claim → ack",
        BURSTS
    );
    println!();

    let data = generate_data();
    let payload = b"order-payload-64B-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

    // ── Warmup ──
    print!("warmup ({WARMUP_ROUNDS} rounds)...");
    for _ in 0..WARMUP_ROUNDS {
        run_cycle(&data, payload);
        print!(" .");
    }
    println!(" done");
    println!();

    // ── Measure ──
    let mut times = Vec::with_capacity(MEASURE_ROUNDS);

    for i in 0..MEASURE_ROUNDS {
        let elapsed = run_cycle(&data, payload);
        let ms = elapsed.as_secs_f64() * 1000.0;
        let throughput = TOTAL as f64 / elapsed.as_secs_f64();
        let ns_per_msg = elapsed.as_nanos() as f64 / TOTAL as f64;

        println!(
            "  round {:2}: {:8.2} ms  |  {:.2}M msg/s  |  {:.0} ns/msg",
            i + 1,
            ms,
            throughput / 1_000_000.0,
            ns_per_msg
        );
        times.push(elapsed);
    }

    // ── Stats ──
    times.sort();
    let min = times[0];
    let max = times[times.len() - 1];
    let median = times[times.len() / 2];
    let mean: std::time::Duration = times.iter().sum::<std::time::Duration>() / times.len() as u32;

    println!();
    println!("── results ({MEASURE_ROUNDS} rounds, {TOTAL} msgs each) ──");
    println!();
    println!(
        "  min:    {:8.2} ms  |  {:.2}M msg/s  |  {:.0} ns/msg",
        min.as_secs_f64() * 1000.0,
        TOTAL as f64 / min.as_secs_f64() / 1_000_000.0,
        min.as_nanos() as f64 / TOTAL as f64,
    );
    println!(
        "  median: {:8.2} ms  |  {:.2}M msg/s  |  {:.0} ns/msg",
        median.as_secs_f64() * 1000.0,
        TOTAL as f64 / median.as_secs_f64() / 1_000_000.0,
        median.as_nanos() as f64 / TOTAL as f64,
    );
    println!(
        "  mean:   {:8.2} ms  |  {:.2}M msg/s  |  {:.0} ns/msg",
        mean.as_secs_f64() * 1000.0,
        TOTAL as f64 / mean.as_secs_f64() / 1_000_000.0,
        mean.as_nanos() as f64 / TOTAL as f64,
    );
    println!(
        "  max:    {:8.2} ms  |  {:.2}M msg/s  |  {:.0} ns/msg",
        max.as_secs_f64() * 1000.0,
        TOTAL as f64 / max.as_secs_f64() / 1_000_000.0,
        max.as_nanos() as f64 / TOTAL as f64,
    );
    println!();
    println!("  burst size: {BURST}  |  payload: {} bytes", payload.len());
}
