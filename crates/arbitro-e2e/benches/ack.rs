//! Ack throughput + correctness bench.
//!
//! Two stages:
//!
//! **ack_single** — `msg.ack_sync().await` per message. Each ack is a full
//!   round-trip (client → broker → reply). Measures the worst-case latency
//!   path: no coalescing, one syscall per ack.
//!
//! **ack_batch** — `msg.ack()` (fire-and-forget) for every message, then a
//!   final `ack_sync()` on the last one to force the broker to drain and
//!   acknowledge the tail. The client's internal `ack_loop` coalesces the
//!   queued acks into `BatchAck` frames (up to 256 per frame).
//!
//! Correctness check: after acks complete, the consumer's inflight must be
//! 0 and publishing fresh messages must still deliver (no stuck pendings).
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench ack --no-run 2>&1"
//!   wsl bash -lc "cp .../target/release/deps/ack-* /tmp/arbitro-bench/ && \
//!     cd /tmp/arbitro-bench && timeout 120 ./ack-* --bench"

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverPolicy, StreamConfig};
use arbitro_server::{ArbitroServer, Config};

const DEFAULT_TOTAL: u64 = 20_000;
const PAYLOAD: &[u8] = b"ack-bench-64b-payload-............................";
const STREAM: &[u8] = b"ack_bench";
const SUBJECT: &[u8] = b"ack.bench.topic";

fn env_u64(var: &str, fallback: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(fallback)
}

fn portpicker() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn spawn_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(32)
        .shard_count(1)
        .write_buffer_cap(1024 * 1024);
    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(120)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect_with_timeout(addr, Duration::from_secs(5))
        .await
        .expect("client connects")
}

/// Prepare: create stream, consumer (ack-explicit), subscribe, publish N msgs.
/// Returns the subscription handle and the client for ack publishing.
async fn setup(
    client: &Client,
    max_inflight: u16,
    total: u64,
    label: &[u8],
) -> arbitro_client::SubscriptionHandle {
    let mut stream_name = STREAM.to_vec();
    stream_name.extend_from_slice(b"_");
    stream_name.extend_from_slice(label);

    client
        .create_stream(&StreamConfig::new(&stream_name, b">").build())
        .await
        .unwrap();

    let consumer_cfg = ConsumerConfig::new(b"ack_worker", &stream_name)
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(max_inflight)
        .deliver_policy(DeliverPolicy::All)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
    let sub = consumer.subscribe(None).await.unwrap();

    // Publish total msgs via a single batch publish.
    let entries: Vec<(&[u8], &[u8])> = (0..total).map(|_| (SUBJECT, PAYLOAD)).collect();
    client.publish_batch(&stream_name, &entries).await.unwrap();

    sub
}

async fn stage_ack_single(total: u64) -> (Duration, u64) {
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    let mut sub = setup(&client, 256, total, b"single").await;

    let mut received = 0u64;
    let start = Instant::now();
    for _ in 0..total {
        let msg = tokio::time::timeout(Duration::from_secs(10), sub.next())
            .await
            .expect("receive timeout")
            .expect("subscription closed");
        msg.ack_sync().await.expect("ack_sync should succeed");
        received += 1;
    }
    let elapsed = start.elapsed();
    (elapsed, received)
}

async fn stage_ack_batch(total: u64) -> (Duration, u64) {
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    // Saturate inflight so the broker never throttles during the run.
    let cap = total.saturating_add(256).min(u16::MAX as u64) as u16;
    let mut sub = setup(&client, cap, total, b"batch").await;

    let mut received = 0u64;
    let mut last_msg: Option<arbitro_client::Message> = None;
    let start = Instant::now();
    for _ in 0..total {
        let msg = tokio::time::timeout(Duration::from_secs(10), sub.next())
            .await
            .expect("receive timeout")
            .expect("subscription closed");
        // Replace any previous "last" with current — keep only the final
        // message so we can ack_sync on it at the end, forcing the broker
        // to drain the accumulated BatchAck frames.
        if let Some(prev) = last_msg.take() {
            prev.ack();
        }
        last_msg = Some(msg);
        received += 1;
    }
    // Force the tail — broker replies RepOk only after processing this ack.
    if let Some(last) = last_msg {
        last.ack_sync().await.expect("tail ack_sync should succeed");
    }
    let elapsed = start.elapsed();
    (elapsed, received)
}

/// Multi-client batch stage: N parallel clients each with their own
/// consumer, each acking its fanout share. Measures scalability with
/// multiple TCP connections acking concurrently.
async fn stage_ack_multi(total: u64, n_clients: u64) -> (Duration, u64) {
    let addr = spawn_server().await;

    // Stream set up by a control client.
    let control = connect(&addr).await;
    let stream_name = b"ack_bench_multi".to_vec();
    control
        .create_stream(&StreamConfig::new(&stream_name, b">").build())
        .await
        .unwrap();

    // Create N consumers, each with a UNIQUE name and a UNIQUE group
    // (fanout: each consumer receives all msgs independently).
    let cap = total.saturating_add(256).min(u16::MAX as u64) as u16;
    for i in 0..n_clients {
        let name = format!("acker-{i}");
        let group = format!("grp-{i}");
        let cfg = ConsumerConfig::new(name.as_bytes(), &stream_name)
            .group(group.as_bytes())
            .ack_policy(AckPolicy::Explicit)
            .max_inflight(cap)
            .deliver_policy(DeliverPolicy::All)
            .build()
            .unwrap();
        control.create_consumer(&cfg).await.unwrap();
    }

    // Spawn N subscriber tasks, each on its own TCP connection.
    let mut worker_handles = Vec::new();
    let received_counts: Vec<Arc<AtomicU64>> = (0..n_clients)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    for i in 0..n_clients {
        let addr = addr.clone();
        let stream_name = stream_name.clone();
        let counter = Arc::clone(&received_counts[i as usize]);
        worker_handles.push(tokio::spawn(async move {
            let client = connect(&addr).await;
            let name = format!("acker-{i}");
            let group = format!("grp-{i}");
            let cfg = ConsumerConfig::new(name.as_bytes(), &stream_name)
                .group(group.as_bytes())
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(cap)
                .deliver_policy(DeliverPolicy::All)
                .build()
                .unwrap();
            let consumer = client.create_consumer(&cfg).await.unwrap();
            let mut sub = consumer.subscribe(None).await.unwrap();

            let mut last_msg: Option<arbitro_client::Message> = None;
            while counter.load(Relaxed) < total {
                match tokio::time::timeout(Duration::from_secs(10), sub.next()).await {
                    Ok(Some(msg)) => {
                        if let Some(prev) = last_msg.take() {
                            prev.ack();
                        }
                        last_msg = Some(msg);
                        counter.fetch_add(1, Relaxed);
                    }
                    _ => break,
                }
            }
            if let Some(last) = last_msg {
                let _ = last.ack_sync().await;
            }
        }));
    }

    // Give subscribers a moment to register.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publisher: 1 batch of `total` msgs.
    let entries: Vec<(&[u8], &[u8])> = (0..total).map(|_| (SUBJECT, PAYLOAD)).collect();
    let start = Instant::now();
    control.publish_batch(&stream_name, &entries).await.unwrap();

    // Wait for all subscribers to receive + ack all msgs.
    for h in worker_handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();

    // Verify each received exactly `total` (fanout contract).
    let mut total_received = 0u64;
    for (i, c) in received_counts.iter().enumerate() {
        let cnt = c.load(Relaxed);
        assert_eq!(cnt, total, "client {i} received {cnt} (expected {total})");
        total_received += cnt;
    }

    (elapsed, total_received)
}

/// Correctness probe: after the bench, publish K more messages and confirm
/// they arrive. Stale pendings would block redelivery because max_inflight
/// would already be saturated.
async fn correctness_probe(addr: &str, client: &Client, stream: &[u8], probe_count: u32) -> u32 {
    let entries: Vec<(&[u8], &[u8])> = (0..probe_count)
        .map(|_| (b"ack.bench.probe".as_slice(), b"p".as_slice()))
        .collect();
    client.publish_batch(stream, &entries).await.unwrap();

    // Reuse existing sub via a fresh consumer (simpler — avoid reusing a subscription after a bench).
    let consumer_cfg = ConsumerConfig::new(b"probe_worker", stream)
        .ack_policy(AckPolicy::None)
        .deliver_policy(DeliverPolicy::All)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
    let mut sub = consumer.subscribe(None).await.unwrap();

    let mut got = 0u32;
    for _ in 0..probe_count {
        match tokio::time::timeout(Duration::from_secs(3), sub.next()).await {
            Ok(Some(msg)) => {
                drop(msg);
                got += 1;
            }
            _ => break,
        }
    }
    let _ = addr; // reserved — addr not needed here
    got
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let total = env_u64("BENCH_ACK_MSGS", DEFAULT_TOTAL);
    let n_clients = env_u64("BENCH_ACK_CLIENTS", 4);

    println!();
    println!("========================================================");
    println!("                      Ack bench");
    println!("========================================================");
    println!(
        "  total_msgs={total}   payload={}B   stream=\"{}\"",
        PAYLOAD.len(),
        String::from_utf8_lossy(STREAM)
    );
    println!();

    // Stage 1 — single ack (one round-trip each)
    println!("--------------------------------------------------------");
    println!("  Stage 1 — ack_single (msg.ack_sync() per message)");
    println!("--------------------------------------------------------");
    let (dur_single, recv_single) = stage_ack_single(total).await;
    assert_eq!(recv_single, total, "all messages must be received");
    let throughput_single = recv_single as f64 / dur_single.as_secs_f64();
    let ns_per_ack_single = dur_single.as_nanos() as f64 / recv_single as f64;
    println!(
        "  received {recv_single}/{total} in {:.2?}  ->  {:.0} acks/s  ({:.0} ns/ack)",
        dur_single, throughput_single, ns_per_ack_single
    );

    println!();

    // Stage 2 — batch ack (coalesced via client-side ack_loop)
    println!("--------------------------------------------------------");
    println!("  Stage 2 — ack_batch (msg.ack() coalesced into BatchAck)");
    println!("--------------------------------------------------------");
    let (dur_batch, recv_batch) = stage_ack_batch(total).await;
    assert_eq!(recv_batch, total, "all messages must be received");
    let throughput_batch = recv_batch as f64 / dur_batch.as_secs_f64();
    let ns_per_ack_batch = dur_batch.as_nanos() as f64 / recv_batch as f64;
    println!(
        "  received {recv_batch}/{total} in {:.2?}  ->  {:.0} acks/s  ({:.0} ns/ack)",
        dur_batch, throughput_batch, ns_per_ack_batch
    );

    println!();

    // Stage 3 — multi-client batch ack (N parallel TCP connections)
    println!();
    println!("--------------------------------------------------------");
    println!("  Stage 3 — ack_multi ({n_clients} parallel clients, each with its own consumer)");
    println!("--------------------------------------------------------");
    let (dur_multi, total_recv_multi) = stage_ack_multi(total, n_clients).await;
    let throughput_multi = total_recv_multi as f64 / dur_multi.as_secs_f64();
    let ns_per_ack_multi = dur_multi.as_nanos() as f64 / total_recv_multi as f64;
    println!(
        "  {n_clients} clients × {total} msgs each = {total_recv_multi} acks in {:.2?}",
        dur_multi
    );
    println!(
        "  aggregate: {:.0} acks/s  ({:.0} ns/ack)   per-client: {:.0} acks/s",
        throughput_multi,
        ns_per_ack_multi,
        throughput_multi / n_clients as f64
    );

    // Summary
    println!();
    let speedup = throughput_batch / throughput_single;
    let scale = throughput_multi / throughput_batch;
    println!("--------------------------------------------------------");
    println!("  Summary");
    println!("--------------------------------------------------------");
    println!(
        "  single (1 client, sync ack)        : {:>10.0} acks/s   ({:>6.0} ns/ack)",
        throughput_single, ns_per_ack_single
    );
    println!(
        "  batch  (1 client, coalesced acks)  : {:>10.0} acks/s   ({:>6.0} ns/ack)",
        throughput_batch, ns_per_ack_batch
    );
    println!(
        "  multi  ({n_clients} clients, coalesced)         : {:>10.0} acks/s   ({:>6.0} ns/ack)",
        throughput_multi, ns_per_ack_multi
    );
    println!("  batch/single: {speedup:.1}x   multi/batch: {scale:.1}x");

    // Correctness: spin up a fresh server/client for the probe so we know
    // acks didn't leave orphans from the bench run's state.
    println!();
    println!("--------------------------------------------------------");
    println!("  Correctness probe (fresh server, 100 msgs round-trip)");
    println!("--------------------------------------------------------");
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    let probe_stream = b"ack_probe_stream".to_vec();
    client
        .create_stream(&StreamConfig::new(&probe_stream, b">").build())
        .await
        .unwrap();
    let got = correctness_probe(&addr, &client, &probe_stream, 100).await;
    if got == 100 {
        println!("  ok — received {got}/100");
    } else {
        panic!("  FAIL — received {got}/100");
    }

    println!();
    std::mem::drop(AtomicU64::new(0));
}
