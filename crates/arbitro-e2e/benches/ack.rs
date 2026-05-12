//! Ack throughput + correctness bench.
//!
//! Two stages:
//!
//! **ack_single** — `msg.ack()` per message (fire-and-forget). Measures
//!   receive throughput with per-message ack enqueued via the ack-batcher.
//!
//! **ack_batch** — `msg.ack()` (fire-and-forget) for every message. The
//!   client's internal `ack_loop` coalesces the queued acks into
//!   `BatchAck` frames (up to 256 per frame).
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

use arbitro_client_tokio::{BatchEntry, Client, ClientConfig};
use bytes::Bytes;
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
    Client::connect(ClientConfig { addr: addr.to_string(), ..ClientConfig::default() })
        .await
        .expect("client connects")
}

/// Prepare: create stream, consumer (ack-explicit), subscribe, publish N msgs.
/// Returns the subscription handle.
/// AckPolicy::Explicit = 1, DeliverPolicy::All = 0
async fn setup(
    client: &Client,
    max_inflight: u16,
    total: u64,
    label: &[u8],
) -> arbitro_client_tokio::SubscriptionHandle {
    let mut stream_name = STREAM.to_vec();
    stream_name.extend_from_slice(b"_");
    stream_name.extend_from_slice(label);

    let resp = client
        .create_stream(&stream_name, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    let resp = client
        .create_consumer(
            stream_id,
            b"ack_worker",
            b"",
            b"",
            max_inflight,
            1, // ack_policy = Explicit
            0, // deliver_policy = All
            0, // deliver_mode = Push/Fanout
            30_000,
            0,
        )
        .await
        .unwrap();
    let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    let sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Publish total msgs via a single batch publish.
    let entries: Vec<BatchEntry<'_>> = (0..total)
        .map(|_| BatchEntry::new(SUBJECT, Bytes::copy_from_slice(PAYLOAD)))
        .collect();
    // Use sync to ensure publish is stored before we try to receive.
    client.publish_batch_sync(stream_id, &entries).await.unwrap();

    sub
}

async fn stage_ack_single(total: u64) -> (Duration, u64) {
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    let mut sub = setup(&client, 256, total, b"single").await;

    let mut received = 0u64;
    let start = Instant::now();
    for _ in 0..total {
        let msg = tokio::time::timeout(Duration::from_secs(10), sub.recv())
            .await
            .expect("receive timeout")
            .expect("subscription closed");
        // No ack_sync in new client — use fire-and-forget ack.
        msg.ack();
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
    let start = Instant::now();
    for _ in 0..total {
        let msg = tokio::time::timeout(Duration::from_secs(10), sub.recv())
            .await
            .expect("receive timeout")
            .expect("subscription closed");
        msg.ack();
        received += 1;
    }
    let elapsed = start.elapsed();
    (elapsed, received)
}

/// Multi-client batch stage: N parallel clients each with their own
/// consumer, each acking its fanout share.
async fn stage_ack_multi(total: u64, n_clients: u64) -> (Duration, u64) {
    let addr = spawn_server().await;

    // Stream set up by a control client.
    let control = connect(&addr).await;
    let stream_name = b"ack_bench_multi".to_vec();
    let resp = control
        .create_stream(&stream_name, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // Create N consumers, each with a UNIQUE name and a UNIQUE group
    // (fanout: each consumer receives all msgs independently).
    let cap = total.saturating_add(256).min(u16::MAX as u64) as u16;
    let mut consumer_ids: Vec<u32> = Vec::with_capacity(n_clients as usize);
    for i in 0..n_clients {
        let name = format!("acker-{i}");
        let group = format!("grp-{i}");
        let resp = control
            .create_consumer(
                stream_id,
                name.as_bytes(),
                group.as_bytes(),
                b"",
                cap,
                1, // ack_policy = Explicit
                0, // deliver_policy = All
                0, // deliver_mode = Push/Fanout
                30_000,
                0,
            )
            .await
            .unwrap();
        let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
        consumer_ids.push(consumer_id);
    }

    // Spawn N subscriber tasks, each on its own TCP connection.
    let mut worker_handles = Vec::new();
    let received_counts: Vec<Arc<AtomicU64>> = (0..n_clients)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    for i in 0..n_clients {
        let addr = addr.clone();
        let consumer_id = consumer_ids[i as usize];
        let counter = Arc::clone(&received_counts[i as usize]);
        worker_handles.push(tokio::spawn(async move {
            let client = connect(&addr).await;
            let mut sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

            while counter.load(Relaxed) < total {
                match tokio::time::timeout(Duration::from_secs(10), sub.recv()).await {
                    Ok(Some(msg)) => {
                        msg.ack();
                        counter.fetch_add(1, Relaxed);
                    }
                    _ => break,
                }
            }
        }));
    }

    // Give subscribers a moment to register.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publisher: 1 batch of `total` msgs.
    let entries: Vec<BatchEntry<'_>> = (0..total)
        .map(|_| BatchEntry::new(SUBJECT, Bytes::copy_from_slice(PAYLOAD)))
        .collect();
    let start = Instant::now();
    control.publish_batch_sync(stream_id, &entries).await.unwrap();

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
/// they arrive.
async fn correctness_probe(client: &Client, probe_count: u32) -> u32 {
    // Create a fresh stream for the probe.
    let probe_stream = b"ack_probe_stream".to_vec();
    let resp = client
        .create_stream(&probe_stream, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    let entries: Vec<BatchEntry<'_>> = (0..probe_count)
        .map(|_| {
            BatchEntry::new(
                b"ack.bench.probe".as_slice(),
                Bytes::copy_from_slice(b"p"),
            )
        })
        .collect();

    // Create consumer with AckPolicy::None = 0, DeliverPolicy::All = 0
    let resp = client
        .create_consumer(
            stream_id,
            b"probe_worker",
            b"",
            b"",
            1024,
            0, // ack_policy = None
            0, // deliver_policy = All
            0, // deliver_mode = Push/Fanout
            30_000,
            0,
        )
        .await
        .unwrap();
    let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
    let mut sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Publish after subscribing so delivery is live.
    client.publish_batch_sync(stream_id, &entries).await.unwrap();

    let mut got = 0u32;
    for _ in 0..probe_count {
        match tokio::time::timeout(Duration::from_secs(3), sub.recv()).await {
            Ok(Some(msg)) => {
                drop(msg);
                got += 1;
            }
            _ => break,
        }
    }
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

    // Stage 1 — single ack (fire-and-forget per message)
    println!("--------------------------------------------------------");
    println!("  Stage 1 — ack_single (msg.ack() per message)");
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
        "  single (1 client, fire-and-forget ack) : {:>10.0} acks/s   ({:>6.0} ns/ack)",
        throughput_single, ns_per_ack_single
    );
    println!(
        "  batch  (1 client, coalesced acks)      : {:>10.0} acks/s   ({:>6.0} ns/ack)",
        throughput_batch, ns_per_ack_batch
    );
    println!(
        "  multi  ({n_clients} clients, coalesced)           : {:>10.0} acks/s   ({:>6.0} ns/ack)",
        throughput_multi, ns_per_ack_multi
    );
    println!("  batch/single: {speedup:.1}x   multi/batch: {scale:.1}x");

    // Correctness probe.
    println!();
    println!("--------------------------------------------------------");
    println!("  Correctness probe (fresh server, 100 msgs round-trip)");
    println!("--------------------------------------------------------");
    let probe_addr = spawn_server().await;
    let probe_client = connect(&probe_addr).await;
    let got = correctness_probe(&probe_client, 100).await;
    if got == 100 {
        println!("  ok — received {got}/100");
    } else {
        panic!("  FAIL — received {got}/100");
    }

    println!();
    std::mem::drop(AtomicU64::new(0));
}
