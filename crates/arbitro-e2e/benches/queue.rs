//! Queue balancing bench — verifies queue-group semantics end-to-end.
//!
//! ## Semantics under test
//!
//! N consumers share the **same group** on the same stream, which makes
//! them share a single `queue_id` in the broker. For every message:
//!
//!   - **Exactly one** consumer in the group receives it (queue dedup)
//!   - Across all messages, the load **distributes across consumers**
//!
//! A pure "first binding always wins" broker would pass the first rule
//! (no duplicates) but fail the second (one consumer takes 100 %). The
//! bench reports both.
//!
//! ## Shape
//!
//! - In-process `ArbitroServer`, memory journal.
//! - Manager creates the stream and **N consumers** with unique names
//!   (`worker-0`, `worker-1`, ...) but the same explicit `group`.
//! - Each consumer runs in its own tokio task: subscribe, receive, ack.
//! - Manager publishes `BENCH_QUEUE_MSGS` messages to a single subject.
//! - After publish, waits until `sum(received) >= published` or a stall
//!   deadline; then asserts no-loss, no-duplicates, and reports fairness.
//!
//! ## Env vars
//!
//!   BENCH_QUEUE_MSGS       default 10_000
//!   BENCH_QUEUE_CONSUMERS  default 2
//!   BENCH_QUEUE_FAIRNESS   default 0.25  (per-consumer min/avg ratio)
//!
//! ## Hygiene
//!
//! Before run: prunes `/tmp/arbitro-queue-*` from prior invocations.
//! After run: drops the data dir via a `Drop` guard (panic-safe).
//!
//! ## Run
//!
//! ```bash
//! wsl bash -lc "cd /mnt/.../arbitro && \
//!   cargo bench --bench queue --no-run 2>&1"
//! wsl bash -lc "cp .../target/release/deps/queue-* /tmp/arbitro-bench/ && \
//!   cd /tmp/arbitro-bench && timeout 60 ./queue-* --bench"
//! ```

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverPolicy, StreamConfig};
use arbitro_server::{ArbitroServer, Config};

const DEFAULT_MSGS: u64 = 10_000;
const DEFAULT_CONSUMERS: u64 = 2;
const PAYLOAD_SIZE: usize = 64;
const STREAM: &[u8] = b"queue_bench";
const GROUP: &[u8] = b"queue_group";
const SUBJECT: &[u8] = b"queue.work";

fn env_u64(var: &str, fallback: u64) -> u64 {
    std::env::var(var).ok().and_then(|s| s.parse().ok()).unwrap_or(fallback)
}

fn env_f64(var: &str, fallback: f64) -> f64 {
    std::env::var(var).ok().and_then(|s| s.parse().ok()).unwrap_or(fallback)
}

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn prune_stale_tmp() {
    let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else {
        return;
    };
    for ent in entries.flatten() {
        let path = ent.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with("arbitro-queue-") {
            let _ = std::fs::remove_dir_all(&path);
        }
    }
}

fn make_data_dir() -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!("arbitro-queue-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tmp data dir");
    dir
}

struct DataDirCleanup(PathBuf);
impl Drop for DataDirCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn spawn_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(64)
        .shard_count(1)
        .write_buffer_cap(4 * 1024 * 1024);
    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect_with_timeout(addr, Duration::from_secs(5))
        .await
        .expect("client connects")
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let msgs = env_u64("BENCH_QUEUE_MSGS", DEFAULT_MSGS);
    let n_consumers = env_u64("BENCH_QUEUE_CONSUMERS", DEFAULT_CONSUMERS);
    let fairness_min_ratio = env_f64("BENCH_QUEUE_FAIRNESS", 0.25);

    // Hygiene — even though we use a memory journal, if tolerant variants
    // get added later, keep the cleanup pattern.
    prune_stale_tmp();
    let data_dir = make_data_dir();
    let _cleanup_guard = DataDirCleanup(data_dir.clone());

    println!();
    println!("========================================================");
    println!("                   Queue balancing bench");
    println!("========================================================");
    println!(
        "  msgs={msgs}   consumers={n_consumers}   payload={PAYLOAD_SIZE}B   journal=Memory"
    );
    println!("  fairness threshold (min/avg): {fairness_min_ratio}");
    println!();

    let addr = spawn_server().await;

    // ── Manager: create stream and N consumers sharing the same group ──
    let manager = connect(&addr).await;
    manager
        .create_stream(&StreamConfig::new(STREAM, b">").build())
        .await
        .expect("create stream");

    println!("  Manager: created stream \"{}\"", String::from_utf8_lossy(STREAM));

    // Create each consumer with a UNIQUE name + SHARED group — this is
    // exactly the queue-semantics setup (different identities, same queue).
    for i in 0..n_consumers {
        let name = format!("worker-{i}");
        let cfg = ConsumerConfig::new(name.as_bytes(), STREAM)
            .group(GROUP)
            .ack_policy(AckPolicy::Explicit)
            .max_inflight(u16::MAX)
            .deliver_policy(DeliverPolicy::All)
            .build()
            .unwrap();
        manager.create_consumer(&cfg).await.expect("create consumer");
    }
    println!("  Manager: created {n_consumers} consumers in group \"{}\"", String::from_utf8_lossy(GROUP));

    // ── Spawn one subscriber task per consumer ─────────────────────────
    //
    // Each task has its own TCP connection and receives only the messages
    // the broker routed to its consumer_id. Tasks push each received seq
    // into a shared `HashSet` (for no-duplicate verification) and bump a
    // per-consumer counter (for fairness reporting).
    let seen_seqs: Arc<std::sync::Mutex<HashSet<u64>>> =
        Arc::new(std::sync::Mutex::new(HashSet::with_capacity(msgs as usize)));
    let per_consumer: Vec<Arc<AtomicU64>> =
        (0..n_consumers).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let mut worker_handles = Vec::new();
    for i in 0..n_consumers {
        let name = format!("worker-{i}");
        let addr = addr.clone();
        let counter = Arc::clone(&per_consumer[i as usize]);
        let seen_seqs = Arc::clone(&seen_seqs);

        worker_handles.push(tokio::spawn(async move {
            let client = connect(&addr).await;
            // The consumer already exists — subscribe using its name+stream.
            let cfg = ConsumerConfig::new(name.as_bytes(), STREAM)
                .group(GROUP)
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(u16::MAX)
                .deliver_policy(DeliverPolicy::All)
                .build()
                .unwrap();
            let consumer = client.create_consumer(&cfg).await.unwrap();
            let mut sub = consumer.subscribe(None).await.unwrap();

            loop {
                match tokio::time::timeout(Duration::from_millis(500), sub.next()).await {
                    Ok(Some(msg)) => {
                        let seq = msg.seq;
                        msg.ack();
                        seen_seqs.lock().unwrap().insert(seq);
                        counter.fetch_add(1, Relaxed);
                    }
                    Ok(None) => break,
                    Err(_) => {
                        // Idle timeout — keep waiting until outer stall cutoff.
                    }
                }
            }
        }));
    }

    // Give subscribers a moment to register before publishing.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Publish ────────────────────────────────────────────────────────
    let pub_start = Instant::now();
    let payload = vec![0xABu8; PAYLOAD_SIZE];
    let entries: Vec<(&[u8], &[u8])> =
        (0..msgs).map(|_| (SUBJECT, payload.as_slice())).collect();
    manager.publish_batch(STREAM, &entries).await.expect("publish_batch");
    let pub_elapsed = pub_start.elapsed();
    println!(
        "  published {msgs} msgs in {:.2?} ({:.0} msg/s)",
        pub_elapsed,
        msgs as f64 / pub_elapsed.as_secs_f64()
    );

    // ── Drain phase ────────────────────────────────────────────────────
    //
    // Wait for the subscribers to receive everything, with a stall cutoff
    // (3 s of no forward progress across all workers).
    let drain_start = Instant::now();
    let stall_budget = Duration::from_secs(3);
    let overall_deadline = drain_start + Duration::from_secs(30);
    let mut last_total = 0u64;
    let mut stall_start = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let total: u64 = per_consumer.iter().map(|a| a.load(Relaxed)).sum();
        if total >= msgs {
            break;
        }
        if Instant::now() >= overall_deadline {
            println!("  WARN drain hard-deadline hit (30 s)");
            break;
        }
        if total != last_total {
            last_total = total;
            stall_start = Instant::now();
        } else if Instant::now().duration_since(stall_start) > stall_budget {
            println!("  WARN drain stalled — no progress in {stall_budget:?}");
            break;
        }
    }
    let drain_elapsed = drain_start.elapsed();

    // Give the subscribers a moment to note they're done, then drop.
    drop(worker_handles);

    // ── Report ─────────────────────────────────────────────────────────
    let per_consumer_counts: Vec<u64> =
        per_consumer.iter().map(|a| a.load(Relaxed)).collect();
    let total_received: u64 = per_consumer_counts.iter().sum();
    let unique = seen_seqs.lock().unwrap().len() as u64;
    let duplicates = total_received.saturating_sub(unique);

    let avg = total_received as f64 / n_consumers as f64;
    let min = *per_consumer_counts.iter().min().unwrap_or(&0);
    let max = *per_consumer_counts.iter().max().unwrap_or(&0);
    let min_over_avg = if avg > 0.0 { min as f64 / avg } else { 0.0 };

    println!();
    println!("  drain elapsed: {drain_elapsed:.2?}");
    println!();
    println!("--------------------------------------------------------");
    println!("  Distribution");
    println!("--------------------------------------------------------");
    for (i, count) in per_consumer_counts.iter().enumerate() {
        let pct = if total_received > 0 {
            *count as f64 / total_received as f64 * 100.0
        } else {
            0.0
        };
        println!("  worker-{i:<2} received: {count:>7}  ({pct:>5.1} %)");
    }
    println!();
    println!("  total received : {total_received}");
    println!("  unique seqs    : {unique}");
    println!("  duplicates     : {duplicates}");
    println!("  min / max      : {min} / {max}");
    println!("  min/avg ratio  : {min_over_avg:.3}  (1.0 = perfectly fair)");
    println!();

    // ── Assertions ─────────────────────────────────────────────────────
    assert_eq!(
        total_received, msgs,
        "queue total {} != published {} — message loss or over-delivery",
        total_received, msgs,
    );
    assert_eq!(
        unique, msgs,
        "queue unique {} != published {} — duplicate deliveries within the group",
        unique, msgs,
    );
    assert_eq!(duplicates, 0, "duplicate deliveries detected");

    if min_over_avg < fairness_min_ratio {
        println!(
            "  FAIRNESS FAIL: min/avg = {min_over_avg:.3} < {fairness_min_ratio} — distribution too skewed"
        );
        println!("  (queue grouping is correct, but one consumer is starving)");
        // Fail the bench so CI catches it.
        std::process::exit(1);
    }

    println!("  RESULT: OK — correct grouping + fair distribution");
    println!();
}
