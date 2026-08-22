//! Does share-nothing pay in the REAL broker, and how much is left under
//! the store's lock?
//!
//! `arbitro-experiment/shardbench` already answered the question on a
//! synthetic data path: share-nothing beats the shared-mutex model by 4% at
//! 4 shards, 22% at 8 and 28% at 16, while routing publishes through a
//! channel is 30% WORSE than the mutex at every size. This bench asks
//! whether the real broker reproduces it, because the synthetic path does
//! an append and an ack, and arbitro does subject matching, fanout, a drain
//! cycle and an ack round trip on top.
//!
//! ## The three arms
//!
//! - **shared** — today's model. One listener, connections land on whatever
//!   tokio worker the pool picks, every shard's drain and command worker
//!   share that pool. The store's `Mutex` is genuinely contended.
//!
//! - **pinned** — per-shard listeners AND per-shard runtimes, but the client
//!   still dials the bootstrap port. The shards' own tasks stop competing
//!   with each other; the publish still arrives from a pool thread. This arm
//!   isolates how much comes from separating the shards alone.
//!
//! - **steered** — the same server, but the client asks for the topology and
//!   opens one connection per shard, publishing each stream through its
//!   owner's port. Now the publish runs ON the thread that owns the store.
//!   This is `dedicated` in the synthetic bench.
//!
//! `steered` minus `pinned` is the value of client steering. `steered`
//! against `shared` is the value of the whole thing. Whatever remains under
//! an uncontended lock is what deleting the `Mutex` could still buy — and
//! that is the number this exists to produce, rather than assert.
//!
//! Message counts stay under the 25k ceiling this project holds benches to.

use std::time::{Duration, Instant};

use arbitro_client_tokio::{BatchEntry, Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;
use tokio::runtime::Runtime;

const SHARDS: usize = 8;
const STREAMS: usize = 16;
/// 16 streams x 1_200 = 19_200 messages per run, under the 25k cap.
const PER_STREAM: usize = 1_200;
const REPS: usize = 3;

#[derive(Clone, Copy, PartialEq)]
enum Arm {
    Shared,
    Pinned,
    Steered,
}

impl Arm {
    fn label(self) -> &'static str {
        match self {
            Arm::Shared => "shared  ",
            Arm::Pinned => "pinned  ",
            Arm::Steered => "steered ",
        }
    }
}

async fn start(arm: Arm) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let private = arm != Arm::Shared;
    let config = Config::default()
        .listen_addr(addr.clone())
        .shard_count(SHARDS)
        .shard_runtimes(private)
        .shard_listeners(private)
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

/// One measured run. Returns messages published per second.
///
/// Publishing is `publish_batch_wait` — a real round trip per batch, not
/// enqueue-only. What is being compared is where the append RUNS, so the
/// reply has to be waited for or the arms would differ only in how fast
/// they fill a queue.
async fn run_once(arm: Arm) -> f64 {
    let (addr, server_task) = start(arm).await;
    let boot = connect(&addr).await;

    // Topology first: `steered` needs the ports before it creates anything,
    // and asking on every arm keeps the setup identical.
    let topo = boot.shard_topology().await.expect("topology");

    // One connection per shard for the steered arm; everyone else works
    // through the bootstrap connection.
    let mut per_shard: Vec<Client> = Vec::new();
    if arm == Arm::Steered {
        for (_, port) in &topo {
            assert_ne!(*port, 0, "steered arm needs real per-shard ports");
            per_shard.push(connect(&format!("127.0.0.1:{port}")).await);
        }
    }

    // Create the streams on the bootstrap connection regardless — stream
    // creation is not what is being measured, and doing it identically
    // everywhere keeps the catalog state the same across arms.
    let mut streams = Vec::new();
    for i in 0..STREAMS {
        let name = format!("aff_{i}");
        let filter = format!("aff{i}.>");
        let resp = boot
            .create_stream(name.as_bytes(), filter.as_bytes(), 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .expect("stream");
        let wire = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
        streams.push((i, wire));
    }

    let subject: Vec<String> = (0..STREAMS).map(|i| format!("aff{i}.evt")).collect();
    let payload = Bytes::from_static(b"0123456789abcdef");

    // Warm: one batch per stream, so segment allocation and the first
    // catalog snapshot are not inside the timed region.
    for (i, wire) in &streams {
        let e = vec![BatchEntry::new(subject[*i].as_bytes(), payload.clone()); 32];
        let c = pick(arm, &boot, &per_shard, *wire);
        c.publish_batch_wait(*wire, &e).await.expect("warm");
    }

    let t = Instant::now();
    for (i, wire) in &streams {
        let entries: Vec<BatchEntry<'_>> = (0..PER_STREAM)
            .map(|_| BatchEntry::new(subject[*i].as_bytes(), payload.clone()))
            .collect();
        let c = pick(arm, &boot, &per_shard, *wire);
        c.publish_batch_wait(*wire, &entries).await.expect("publish");
    }
    let elapsed = t.elapsed();

    boot.close();
    for c in per_shard {
        c.close();
    }
    server_task.abort();

    (STREAMS * PER_STREAM) as f64 / elapsed.as_secs_f64()
}

/// Which connection publishes this stream.
///
/// The steered arm mirrors the broker's own placement rule — `stream % N`
/// for a stream the broker placed by the same rule. Getting this wrong
/// would silently turn the steered arm into the shared arm plus an extra
/// hop, so it is worth stating: these streams are created fresh on a
/// pinned shard count, so the modulo IS their placement.
fn pick<'a>(arm: Arm, boot: &'a Client, per_shard: &'a [Client], wire: u32) -> &'a Client {
    match arm {
        Arm::Steered => &per_shard[wire as usize % per_shard.len()],
        _ => boot,
    }
}

fn main() {
    let rt = Runtime::new().unwrap();
    println!(
        "\nshard affinity — {SHARDS} shards, {STREAMS} streams, \
         {PER_STREAM} msgs/stream ({} total), {REPS} reps\n",
        STREAMS * PER_STREAM
    );
    println!("  arm       median msg/s     runs");

    for arm in [Arm::Shared, Arm::Pinned, Arm::Steered] {
        let mut runs: Vec<f64> = Vec::new();
        for _ in 0..REPS {
            runs.push(rt.block_on(run_once(arm)));
            // Let the aborted server's sockets and threads go before the
            // next run, so an arm is not measured against its own leftovers.
            std::thread::sleep(Duration::from_millis(400));
        }
        let mut sorted = runs.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = sorted[sorted.len() / 2];
        let all: Vec<String> = runs.iter().map(|r| format!("{:.0}", r)).collect();
        println!(
            "  {}  {:>12.0}     [{}]",
            arm.label(),
            median,
            all.join(", ")
        );
    }
    println!();
}
