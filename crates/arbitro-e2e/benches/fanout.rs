//! Fanout bench — two sections.
//!
//! ## Section 1 — throughput matrix
//!
//! Scenarios × publish modes:
//!   - pub/sub : subscribe first (DeliverPolicy::New), publish after →
//!               live drain with all N consumers in the snapshot.
//!   - replay  : publish first, then one consumer joins with
//!               DeliverPolicy::All → drain walks the backlog. Single
//!               consumer because the shard rewinds the global cursor on
//!               every subscribe, so concurrent replay subscribers would
//!               just produce duplicate deliveries.
//!   - single  : one `publish()` per message.
//!   - batch   : `publish_batch(BATCH_SIZE)`.
//!
//! Reports per-consumer and aggregate msg/s.
//!
//! ## Section 2 — distribution check
//!
//! 4 fanout subscriptions on the same stream with different filters,
//! `DIST_TOTAL` (300k) messages spread across 3 subject shapes (100k each)
//! chosen so the expected per-filter counts are unambiguous given
//! arbitro's wildcard semantics (`*` = exactly one token, `>` = one or
//! more tokens, must be last):
//!
//!   sub  filter                       expected
//!   ─────────────────────────────────────────
//!   c0   None  (catch-all `>`)        300_000
//!   c1   message.*.vip                200_000
//!   c2   message.client.vip.>         100_000
//!   c3   ignore.me                          0
//!
//! Subjects published (100k each):
//!   - message.client.vip.alert  (4 tokens) → catch-all + client.vip.>
//!   - message.acme.vip          (3 tokens) → catch-all + *.vip
//!   - message.client.vip        (3 tokens) → catch-all + *.vip
//!
//! Reports per-subscription got vs expected and exits non-zero on mismatch.
//!
//! Rule: compile from /mnt, run from /tmp/arbitro, timeout 120, tee log.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_client::Client;
use arbitro_proto::config::{
    AckPolicy, ConsumerConfig, DeliverMode, DeliverPolicy, StreamConfig,
};
use arbitro_server::{ArbitroServer, Config};

const N_CONSUMERS_PUBSUB: usize = 4;
const N_CONSUMERS_REPLAY: usize = 1;
const MSGS: u64 = 100_000;
const BATCH_SIZE: usize = 256;
const PAYLOAD: &[u8] = &[0u8; 64];
const SUBJECT: &[u8] = b"bench.topic";
const STREAM: &[u8] = b"fanout_bench";

// ── Distribution stage parameters ────────────────────────────────────────

const DIST_PER_SUBJECT: u64 = 100_000;
const DIST_STREAM: &[u8] = b"fanout_dist";
const DIST_SUBJECTS: &[&[u8]] = &[
    b"message.client.vip.alert", // 4 tokens → catch-all + client.vip.>
    b"message.acme.vip",         // 3 tokens → catch-all + *.vip
    b"message.client.vip",       // 3 tokens → catch-all + *.vip
];

struct DistSub {
    label: &'static str,
    filter: Option<&'static [u8]>,
    expected: u64,
}

const DIST_SUBS: &[DistSub] = &[
    DistSub { label: "catch-all  (>)",       filter: None,                          expected: 3 * DIST_PER_SUBJECT },
    DistSub { label: "message.*.vip",        filter: Some(b"message.*.vip"),        expected: 2 * DIST_PER_SUBJECT },
    DistSub { label: "message.client.vip.>", filter: Some(b"message.client.vip.>"), expected: 1 * DIST_PER_SUBJECT },
    DistSub { label: "ignore.me",            filter: Some(b"ignore.me"),            expected: 0 },
];

fn portpicker() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn spawn_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let cfg = Config::default()
        .listen_addr(addr.clone())
        .max_connections(64)
        .write_buffer_cap(1024 * 1024);
    let server = ArbitroServer::new(cfg);
    tokio::spawn(async move { let _ = server.run().await; });
    tokio::time::sleep(Duration::from_millis(150)).await;
    addr
}

struct ConsumerStats {
    name: String,
    count: Arc<AtomicU64>,
    first_msg_at: Arc<tokio::sync::Mutex<Option<Instant>>>,
    last_msg_at: Arc<tokio::sync::Mutex<Option<Instant>>>,
}

/// Create `N_CONSUMERS` consumers on the given stream, each on its own
/// `Client` (separate TCP connection). Registration runs in parallel so
/// the shard's cursor rewinds (one per subscribe under
/// `DeliverPolicy::All`) overlap instead of serializing into N full
/// walks — otherwise each existing consumer would receive N − 1 worth
/// of duplicates before the last subscriber catches up.
async fn spawn_consumers(
    addr: &str,
    stream: &[u8],
    policy: DeliverPolicy,
    n: usize,
) -> (
    Vec<ConsumerStats>,
    Vec<arbitro_client::subscription::CallbackHandle>,
    Vec<Client>,
) {
    // Connect all clients first (serial — cheap, TCP accept is fast).
    let mut clients = Vec::with_capacity(n);
    for _ in 0..n {
        clients.push(Client::connect(addr).await.unwrap());
    }

    let mut subscribe_futures = Vec::with_capacity(n);
    for (i, client) in clients.iter().enumerate() {
        let name = format!("c{i}");
        let stream_v = stream.to_vec();
        let client = client.clone();
        subscribe_futures.push(async move {
            let ccfg = ConsumerConfig::new(name.as_bytes(), &stream_v)
                .deliver_mode(DeliverMode::Fanout)
                .deliver_policy(policy)
                .ack_policy(AckPolicy::None)
                .build()
                .unwrap();
            let consumer = client.create_consumer(&ccfg).await.unwrap();

            let count = Arc::new(AtomicU64::new(0));
            let first = Arc::new(tokio::sync::Mutex::new(None::<Instant>));
            let last = Arc::new(tokio::sync::Mutex::new(None::<Instant>));

            let cc = count.clone();
            let cf = first.clone();
            let cl = last.clone();
            let handle = consumer
                .subscribe_callback(None, move |_msg| {
                    let now = Instant::now();
                    let was = cc.fetch_add(1, Relaxed);
                    if was == 0 {
                        if let Ok(mut g) = cf.try_lock() { *g = Some(now); }
                    }
                    if let Ok(mut g) = cl.try_lock() { *g = Some(now); }
                })
                .await
                .unwrap();

            (
                ConsumerStats { name, count, first_msg_at: first, last_msg_at: last },
                handle,
            )
        });
    }

    let results = futures::future::join_all(subscribe_futures).await;
    let mut stats = Vec::with_capacity(n);
    let mut handles = Vec::with_capacity(n);
    for (s, h) in results {
        stats.push(s);
        handles.push(h);
    }

    (stats, handles, clients)
}

/// Wait until every consumer has received `target` messages, or `timeout`
/// expires. Returns the wall-clock duration measured from `start`.
async fn wait_for_all(stats: &[ConsumerStats], target: u64, start: Instant, timeout: Duration) -> Duration {
    loop {
        let all_done = stats.iter().all(|s| s.count.load(Relaxed) >= target);
        if all_done { return start.elapsed(); }
        if start.elapsed() > timeout { return start.elapsed(); }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn publish_single(client: &Client, stream: &[u8], n: u64) {
    for _ in 0..n {
        client.publish(stream, SUBJECT, PAYLOAD).await.unwrap();
    }
}

async fn publish_batched(client: &Client, stream: &[u8], n: u64) {
    let mut remaining = n as usize;
    while remaining > 0 {
        let size = remaining.min(BATCH_SIZE);
        let entries: Vec<(&[u8], &[u8])> = (0..size).map(|_| (SUBJECT, PAYLOAD)).collect();
        client.publish_batch(stream, &entries).await.unwrap();
        remaining -= size;
    }
}

#[derive(Clone, Copy)]
enum Mode { PubSub, Replay }

#[derive(Clone, Copy)]
enum Pub { Single, Batch }

async fn run_stage(label: &str, mode: Mode, pub_mode: Pub) {
    let addr = spawn_server().await;

    // Setup: stream.
    let setup = Client::connect(&addr).await.unwrap();
    setup
        .create_stream(&StreamConfig::new(STREAM, b">").build())
        .await
        .unwrap();

    let n_consumers = match mode {
        Mode::PubSub => N_CONSUMERS_PUBSUB,
        Mode::Replay => N_CONSUMERS_REPLAY,
    };

    let (stats, _handles, _clients) = match mode {
        Mode::PubSub => {
            // Subscribe first, then publish.
            let s = spawn_consumers(&addr, STREAM, DeliverPolicy::New, n_consumers).await;
            tokio::time::sleep(Duration::from_millis(200)).await; // settle bindings
            s
        }
        Mode::Replay => {
            // Publish first, then subscribe.
            match pub_mode {
                Pub::Single => publish_single(&setup, STREAM, MSGS).await,
                Pub::Batch => publish_batched(&setup, STREAM, MSGS).await,
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            spawn_consumers(&addr, STREAM, DeliverPolicy::All, n_consumers).await
        }
    };

    let start = Instant::now();
    if let Mode::PubSub = mode {
        match pub_mode {
            Pub::Single => publish_single(&setup, STREAM, MSGS).await,
            Pub::Batch => publish_batched(&setup, STREAM, MSGS).await,
        }
    }
    let publish_done_at = start.elapsed();

    let elapsed = wait_for_all(&stats, MSGS, start, Duration::from_secs(60)).await;

    // Per-consumer summary.
    let mut total_delivered = 0u64;
    println!("\n  [{label}] publish took {:.2?}   delivery window {:.2?}", publish_done_at, elapsed);
    for s in &stats {
        let got = s.count.load(Relaxed);
        total_delivered += got;
        let first = *s.first_msg_at.lock().await;
        let last = *s.last_msg_at.lock().await;
        let per = match (first, last) {
            (Some(f), Some(l)) => {
                let dur = l.duration_since(f).as_secs_f64().max(1e-9);
                got as f64 / dur
            }
            _ => 0.0,
        };
        println!(
            "    {:<4} recv {:>7}/{} in [{:>6.1}ms..{:>6.1}ms]   {:>10.0} msg/s",
            s.name,
            got,
            MSGS,
            first.map(|t| t.duration_since(start).as_secs_f64() * 1000.0).unwrap_or(0.0),
            last.map(|t| t.duration_since(start).as_secs_f64() * 1000.0).unwrap_or(0.0),
            per
        );
    }
    let agg = total_delivered as f64 / elapsed.as_secs_f64();
    println!(
        "    ----\n    total {} × {} consumers = {} msgs   aggregate {:.0} msg/s   per-consumer avg {:.0} msg/s",
        MSGS, n_consumers, total_delivered, agg, agg / n_consumers as f64
    );
}

// ── Distribution stage ───────────────────────────────────────────────────

async fn run_distribution() {
    let addr = spawn_server().await;
    let setup = Client::connect(&addr).await.unwrap();
    setup
        .create_stream(&StreamConfig::new(DIST_STREAM, b">").build())
        .await
        .unwrap();

    let mut counts: Vec<Arc<AtomicU64>> = Vec::with_capacity(DIST_SUBS.len());
    let mut _handles = Vec::with_capacity(DIST_SUBS.len());
    let mut _clients = Vec::with_capacity(DIST_SUBS.len());

    for (i, s) in DIST_SUBS.iter().enumerate() {
        let client = Client::connect(&addr).await.unwrap();
        let cname = format!("c{i}");
        let ccfg = ConsumerConfig::new(cname.as_bytes(), DIST_STREAM)
            .deliver_mode(DeliverMode::Fanout)
            .deliver_policy(DeliverPolicy::New)
            .ack_policy(AckPolicy::None)
            .build()
            .unwrap();
        let consumer = client.create_consumer(&ccfg).await.unwrap();

        let count = Arc::new(AtomicU64::new(0));
        let cc = count.clone();
        let h = consumer
            .subscribe_callback(s.filter, move |_msg| {
                cc.fetch_add(1, Relaxed);
            })
            .await
            .unwrap();

        counts.push(count);
        _handles.push(h);
        _clients.push(client);
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    let total: u64 = DIST_SUBJECTS.len() as u64 * DIST_PER_SUBJECT;
    let pub_start = Instant::now();
    for subject in DIST_SUBJECTS {
        let mut remaining = DIST_PER_SUBJECT as usize;
        while remaining > 0 {
            let size = remaining.min(BATCH_SIZE);
            let entries: Vec<(&[u8], &[u8])> =
                (0..size).map(|_| (*subject, PAYLOAD)).collect();
            setup.publish_batch(DIST_STREAM, &entries).await.unwrap();
            remaining -= size;
        }
    }
    let pub_dur = pub_start.elapsed();

    // Wait for deliveries to converge.
    let wait_start = Instant::now();
    let timeout = Duration::from_secs(30);
    loop {
        let all_ready = DIST_SUBS
            .iter()
            .enumerate()
            .all(|(i, s)| counts[i].load(Relaxed) >= s.expected);
        if all_ready { break; }
        if wait_start.elapsed() > timeout { break; }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let delivery_dur = wait_start.elapsed();

    let total_delivered: u64 = counts.iter().map(|c| c.load(Relaxed)).sum();

    println!("\n  ── Subjects published (each {} msgs) ──", DIST_PER_SUBJECT);
    for s in DIST_SUBJECTS {
        println!("       {}", std::str::from_utf8(s).unwrap_or("?"));
    }
    println!(
        "    publish {:.2?}   delivery window {:.2?}   total published {}   total delivered {}",
        pub_dur, delivery_dur, total, total_delivered
    );
    println!();
    println!("    {:<26} {:>10}  {:>10}   status", "filter", "expected", "got");
    println!("    ──────────────────────────────────────────────────────────");
    let mut failures = 0;
    for (i, s) in DIST_SUBS.iter().enumerate() {
        let got = counts[i].load(Relaxed);
        let status = if got == s.expected {
            "OK".to_string()
        } else {
            failures += 1;
            format!("FAIL  (Δ {:+})", got as i64 - s.expected as i64)
        };
        println!("    {:<26} {:>10}  {:>10}   {}", s.label, s.expected, got, status);
    }

    if failures > 0 {
        eprintln!("\n  {failures} subscription(s) did not match expected counts");
        std::process::exit(1);
    }
}

#[tokio::main]
async fn main() {
    println!("\n========================================================");
    println!("                    Fanout bench");
    println!("========================================================");
    println!(
        "  payload={}B  batch_size={}",
        PAYLOAD.len(), BATCH_SIZE
    );

    println!("\n--------------------------------------------------------");
    println!("  Section 1 — throughput matrix (pub/sub × replay)");
    println!("--------------------------------------------------------");
    println!(
        "  pub/sub consumers={}   replay consumers={}   msgs={}   subject=\"{}\"   stream=\"{}\"",
        N_CONSUMERS_PUBSUB, N_CONSUMERS_REPLAY, MSGS,
        std::str::from_utf8(SUBJECT).unwrap_or("?"),
        std::str::from_utf8(STREAM).unwrap_or("?"),
    );

    run_stage("pub/sub × single", Mode::PubSub, Pub::Single).await;
    run_stage("pub/sub × batch",  Mode::PubSub, Pub::Batch).await;
    run_stage("replay × single",  Mode::Replay, Pub::Single).await;
    run_stage("replay × batch",   Mode::Replay, Pub::Batch).await;

    println!("\n--------------------------------------------------------");
    println!("  Section 2 — distribution check (subject filters)");
    println!("--------------------------------------------------------");
    println!(
        "  consumers={}   per-subject msgs={}   total msgs={}   stream=\"{}\"",
        DIST_SUBS.len(), DIST_PER_SUBJECT,
        DIST_SUBJECTS.len() as u64 * DIST_PER_SUBJECT,
        std::str::from_utf8(DIST_STREAM).unwrap_or("?"),
    );
    run_distribution().await;

    println!("\n========================================================\n");
}
