//! Does releasing an ack without the channel actually pay, in the real
//! broker?
//!
//! `benches/ack.rs` cannot answer this. It dials the bootstrap port, so its
//! connections are not pinned to any shard, `local::owns` is false, and
//! every ack takes the routed path no matter what the seam offers. Running
//! it after wiring the direct path produced noise in both directions —
//! which is exactly what a bench that never exercises the change should
//! produce.
//!
//! This one steers. The broker runs with per-shard listeners, the client
//! asks for the topology, and each stream is acked over the connection
//! belonging to the shard that owns it. Only then is `CommandPath::Local`
//! reachable.
//!
//! ## Arms
//!
//! - `bootstrap` — one connection to the fixed port, every stream. What
//!   `ack.rs` measures, and what a client that does not steer gets.
//! - `steered`   — one connection per shard port, each stream acked over
//!   its owner. The ack is released by a direct call on the shard's own
//!   thread.
//!
//! The difference between the two IS the value of removing the channel from
//! the ack path. Everything else about the two arms is identical: same
//! server settings, same streams, same message count.
//!
//! Under the 25k-message ceiling this project holds benches to.

use std::time::{Duration, Instant};

use arbitro_client_tokio::{BatchEntry, Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;
use tokio::runtime::Runtime;

const SHARDS: usize = 8;
const STREAMS: usize = 16;
/// 16 x 1_200 = 19_200 acks per run.
const PER_STREAM: usize = 1_200;
const REPS: usize = 3;

#[derive(Clone, Copy, PartialEq)]
enum Arm {
    Bootstrap,
    Steered,
}

async fn start() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let config = Config::default()
        .listen_addr(addr.clone())
        .shard_count(SHARDS)
        .shard_listeners(true)
        .max_connections(500)
        .write_buffer_cap(65536);
    let mut server = ArbitroServer::new(config);
    server.set_listener(listener);
    let h = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (addr, h)
}

async fn connect(addr: &str) -> Client {
    for _ in 0..200 {
        if let Ok(c) = Client::connect(ClientConfig {
            addr: addr.to_string(),
            ..ClientConfig::default()
        })
        .await
        {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("client must connect to {addr}");
}

/// Returns acks per second.
async fn run_once(arm: Arm) -> f64 {
    let (addr, server_task) = start().await;
    let boot = connect(&addr).await;
    let topo = boot.shard_topology().await.expect("topology");

    let mut per_shard: Vec<Client> = Vec::new();
    if arm == Arm::Steered {
        for (_, port) in &topo {
            assert_ne!(*port, 0, "steered arm needs real per-shard ports");
            per_shard.push(connect(&format!("127.0.0.1:{port}")).await);
        }
    }

    // Create every stream and consumer over the SAME connection that will
    // ack them, so the consumer's binding belongs to that connection —
    // the ack index is keyed by (connection, sub_id), and acking from a
    // different connection would miss it entirely.
    let mut work = Vec::new();
    for i in 0..STREAMS {
        let name = format!("ackaff_{i}");
        let filter = format!("ackaff{i}.>");
        // Create over bootstrap to learn the id, then bind on the owner.
        let resp = boot
            .create_stream(name.as_bytes(), filter.as_bytes(), 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .expect("stream");
        let wire = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

        let owner: &Client = match arm {
            // `wire % SHARDS` mirrors the broker's own placement rule for a
            // freshly created stream. Getting this wrong would silently turn
            // the steered arm into the bootstrap arm plus a hop.
            Arm::Steered => &per_shard[wire as usize % per_shard.len()],
            Arm::Bootstrap => &boot,
        };

        let cname = format!("ackaff_w{i}");
        let resp = owner
            .create_consumer(
                wire,
                cname.as_bytes(),
                cname.as_bytes(),
                b"",
                4096,
                1, // AckPolicy::Explicit — acks are the point
                0, // DeliverPolicy::All
                0, // Push
                30_000,
                0,
            )
            .await
            .expect("consumer");
        let consumer = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
        let sub = owner.subscribe(wire, consumer, b"").await.expect("subscribe");
        work.push((i, wire, sub));
    }

    // Pre-load: publishing is not being measured, so it happens before the
    // clock starts and over whichever connection owns the stream.
    let payload = Bytes::from_static(b"0123456789abcdef");
    for (i, wire, _) in &work {
        let subject = format!("ackaff{i}.evt");
        let entries: Vec<BatchEntry<'_>> = (0..PER_STREAM)
            .map(|_| BatchEntry::new(subject.as_bytes(), payload.clone()))
            .collect();
        let owner: &Client = match arm {
            Arm::Steered => &per_shard[*wire as usize % per_shard.len()],
            Arm::Bootstrap => &boot,
        };
        owner.publish_batch_wait(*wire, &entries).await.expect("publish");
    }

    // Timed: receive and ack everything.
    let t = Instant::now();
    let mut acked = 0usize;
    for (_, _, sub) in work.iter_mut() {
        for _ in 0..PER_STREAM {
            match tokio::time::timeout(Duration::from_secs(10), sub.recv()).await {
                Ok(Some(msg)) => {
                    msg.ack();
                    acked += 1;
                }
                _ => break,
            }
        }
    }
    let elapsed = t.elapsed();
    assert_eq!(
        acked,
        STREAMS * PER_STREAM,
        "only {acked} acked — the arms are not comparable if one lost messages"
    );

    boot.close();
    for c in per_shard {
        c.close();
    }
    server_task.abort();

    acked as f64 / elapsed.as_secs_f64()
}

fn main() {
    let rt = Runtime::new().unwrap();
    println!(
        "\nack affinity — {SHARDS} shards, {STREAMS} streams, {} acks/run, {REPS} reps\n",
        STREAMS * PER_STREAM
    );
    println!("  arm          median acks/s     runs");

    let mut medians = Vec::new();
    for (label, arm) in [("bootstrap", Arm::Bootstrap), ("steered", Arm::Steered)] {
        let mut runs: Vec<f64> = Vec::new();
        for _ in 0..REPS {
            runs.push(rt.block_on(run_once(arm)));
            std::thread::sleep(Duration::from_millis(400));
        }
        let mut sorted = runs.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = sorted[sorted.len() / 2];
        medians.push(median);
        let all: Vec<String> = runs.iter().map(|r| format!("{:.0}", r)).collect();
        println!("  {label:<12} {median:>12.0}     [{}]", all.join(", "));
    }
    println!(
        "\n  steered / bootstrap: {:.2}x\n",
        medians[1] / medians[0]
    );
}
