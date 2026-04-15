//! Multi-threaded benchmark: N shards × (1M/N) messages each.
//!
//! Each shard is an independent ArbitroEngine on its own thread.
//! Measures aggregate throughput and per-shard stats.
//!
//! Run:  cargo run --example million_burst_mt --release

use arbitro_engine::batch::*;
use arbitro_engine::catalog::{fnv1a_32, ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine::types::*;
use arbitro_engine::*;
use std::time::Instant;

const TOTAL: usize = 1_000_000;
const BURST: usize = 256;

fn setup_engine(shard: u32) -> ArbitroEngine {
    let mut e = ArbitroEngine::new();

    e.ensure_stream(StreamConfig {
        id: StreamId(shard),
        name: format!("orders-{shard}").into_bytes(),
    })
    .unwrap();

    e.ensure_consumer(ConsumerConfig {
        id: ConsumerId(shard),
        queue_id: QueueId(shard),
        stream_id: StreamId(shard),
        durable: true,
        ack_policy: AckPolicy::Explicit,
        max_inflight: BURST as u32 + 1,
    })
    .unwrap();

    e.ensure_subscription(SubscriptionConfig {
        id: SubscriptionId(shard),
        stream_id: StreamId(shard),
        consumer_id: ConsumerId(shard),
        filters: vec![],
    })
    .unwrap();

    e.open_connection(&OpenConnectionReq {
        connection_id: ConnectionId(100 + shard as u64),
        node_id: NodeId(1),
        now: Timestamp::new(0),
    });

    e.bind(&BindBatch {
        entries: &[BindEntry {
            connection_id: ConnectionId(100 + shard as u64),
            subscription_id: SubscriptionId(shard),
        }],
        now: Timestamp::new(0),
    });

    e
}

struct ShardData {
    subjects: Vec<Vec<u8>>,
    hashes: Vec<u32>,
}

fn generate_shard_data(shard: u32, count: usize) -> ShardData {
    let subjects: Vec<Vec<u8>> = (0..count)
        .map(|i| format!("order.s{shard}.burst_{}.seq_{}", i / BURST, i % BURST).into_bytes())
        .collect();
    let hashes: Vec<u32> = subjects.iter().map(|s| fnv1a_32(s)).collect();
    ShardData { subjects, hashes }
}

fn run_shard(shard: u32, msg_count: usize) -> std::time::Duration {
    let data = generate_shard_data(shard, msg_count);
    let payload = b"order-payload-64B-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    let bursts = msg_count / BURST;

    let mut engine = setup_engine(shard);
    let mut seq_counter: u64 = 1;
    let conn = ConnectionId(100 + shard as u64);

    let start = Instant::now();

    for burst in 0..bursts {
        let base = burst * BURST;

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
            stream_id: StreamId(shard),
            entries: &pub_entries,
            now: Timestamp::new(seq_counter),
        });

        let _drain = engine.drain_fanout();
        drop(_drain);

        let ack_entries: Vec<AckEntry>;

        {
            let claimed = engine.claim(
                &ClaimBatch {
                    queue_id: QueueId(shard),
                    connection_id: conn,
                    consumer_id: ConsumerId(shard),
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

        engine.ack(&AckBatch {
            consumer_id: ConsumerId(shard),
            entries: &ack_entries,
            now: Timestamp::new(seq_counter + 2),
        });

        seq_counter += 3;
    }

    let elapsed = start.elapsed();

    assert_eq!(
        engine
            .ctx()
            .inflight
            .get(arbitro_engine::inflight::InFlightScope::Consumer, shard),
        0,
        "shard {shard}: inflight leak"
    );

    elapsed
}

#[tokio::main]
async fn main() {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let thread_counts: Vec<usize> = vec![1, 2, 4, 8, cpus]
        .into_iter()
        .filter(|&n| n <= cpus)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    println!("=== million_burst_mt: {TOTAL} msgs total, burst {BURST} ===");
    println!("  available cores: {cpus}");
    println!();

    // Warmup single-thread
    print!("warmup...");
    run_shard(1, TOTAL.min(256 * 100));
    println!(" done");
    println!();

    let mut baseline_throughput = 0.0_f64;

    for &threads in &thread_counts {
        let msgs_per_shard = TOTAL / threads;
        // Round down to multiple of BURST
        let msgs_per_shard = (msgs_per_shard / BURST) * BURST;
        let total_msgs = msgs_per_shard * threads;

        println!("── {threads} thread(s) × {msgs_per_shard} msgs/shard = {total_msgs} total ──");

        let wall_start = Instant::now();

        let mut handles = Vec::with_capacity(threads);
        for t in 0..threads {
            let shard = (t + 1) as u32;
            let count = msgs_per_shard;
            handles.push(tokio::task::spawn_blocking(move || {
                (shard, run_shard(shard, count))
            }));
        }

        let mut shard_times = Vec::with_capacity(threads);
        for h in handles {
            let (shard, elapsed) = h.await.unwrap();
            shard_times.push((shard, elapsed));
        }

        let wall_elapsed = wall_start.elapsed();
        let wall_ms = wall_elapsed.as_secs_f64() * 1000.0;
        let throughput = total_msgs as f64 / wall_elapsed.as_secs_f64();
        let ns_per_msg = wall_elapsed.as_nanos() as f64 / total_msgs as f64;

        if threads == 1 {
            baseline_throughput = throughput;
        }
        let scaling = if baseline_throughput > 0.0 {
            throughput / baseline_throughput
        } else {
            1.0
        };

        for (shard, elapsed) in &shard_times {
            let shard_tp = msgs_per_shard as f64 / elapsed.as_secs_f64();
            println!(
                "  shard {:2}: {:8.2} ms  |  {:.2}M msg/s",
                shard,
                elapsed.as_secs_f64() * 1000.0,
                shard_tp / 1_000_000.0
            );
        }

        println!();
        println!(
            "  TOTAL:   {:8.2} ms  |  {:.2}M msg/s  |  {:.0} ns/msg  |  {:.2}x scaling",
            wall_ms,
            throughput / 1_000_000.0,
            ns_per_msg,
            scaling
        );
        println!();
    }
}
