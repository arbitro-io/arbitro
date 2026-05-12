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
//!               every subscribe.
//!   - single  : one `publish()` per message.
//!   - batch   : `publish_batch(BATCH_SIZE)`.
//!
//! Reports per-consumer and aggregate msg/s.
//!
//! ## Section 2 — distribution check
//!
//! 4 fanout subscriptions on the same stream with different filters,
//! `DIST_TOTAL` (300k) messages spread across 3 subject shapes (100k each).
//!
//! Rule: compile from /mnt, run from /tmp/arbitro, timeout 120, tee log.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_client_tokio::{BatchEntry, Client, ClientConfig, SubscriptionHandle};
use bytes::Bytes;
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
    filter: &'static [u8],
    expected: u64,
}

const DIST_SUBS: &[DistSub] = &[
    DistSub { label: "catch-all  (>)",       filter: b"",                           expected: 3 * DIST_PER_SUBJECT },
    DistSub { label: "message.*.vip",        filter: b"message.*.vip",              expected: 2 * DIST_PER_SUBJECT },
    DistSub { label: "message.client.vip.>", filter: b"message.client.vip.>",      expected: 1 * DIST_PER_SUBJECT },
    DistSub { label: "ignore.me",            filter: b"ignore.me",                 expected: 0 },
];

// ── Single-connection multi-subscription stage ──────────────────────────

const SCMS_N_CONSUMERS: usize = 4;
const SCMS_MSGS: u64 = 50_000;
const SCMS_STREAM: &[u8] = b"fanout_scms";
const SCMS_SUBJECT: &[u8] = b"scms.topic";

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

async fn connect(addr: &str) -> Client {
    Client::connect(ClientConfig { addr: addr.to_string(), ..ClientConfig::default() })
        .await
        .unwrap()
}

struct ConsumerStats {
    name: String,
    count: Arc<AtomicU64>,
    first_msg_at: Arc<tokio::sync::Mutex<Option<Instant>>>,
    last_msg_at: Arc<tokio::sync::Mutex<Option<Instant>>>,
}

/// Create `n` consumers on the given stream. Each consumer runs a background
/// tokio task that loops `sub.recv()` and increments atomic counters.
/// Returns stats vec, join handles (keep alive), and subscription handles (keep alive).
///
/// AckPolicy::None=0, DeliverPolicy::New=1, DeliverPolicy::All=0
/// DeliverMode::Push/Fanout=0
async fn spawn_consumers(
    addr: &str,
    stream_id: u32,
    deliver_policy: u8, // 0=All, 1=New
    n: usize,
) -> (
    Vec<ConsumerStats>,
    Vec<tokio::task::JoinHandle<()>>,
    Vec<SubscriptionHandle>,
    Vec<Client>,
) {
    let mut clients = Vec::with_capacity(n);
    for _ in 0..n {
        clients.push(connect(addr).await);
    }

    let mut stats = Vec::with_capacity(n);
    let mut join_handles = Vec::with_capacity(n);
    let sub_handles: Vec<_> = Vec::with_capacity(n);

    for (i, client) in clients.iter().enumerate() {
        let name = format!("c{i}");
        let group_name = format!("fanout_g{i}");
        let resp = client
            .create_consumer(
                stream_id,
                name.as_bytes(),
                group_name.as_bytes(),
                b"",
                u16::MAX,
                0,              // ack_policy = None
                deliver_policy, // deliver_policy
                0,              // deliver_mode = Push/Fanout
                30_000,
                0,
            )
            .await
            .unwrap();
        let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

        let sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

        let count = Arc::new(AtomicU64::new(0));
        let first = Arc::new(tokio::sync::Mutex::new(None::<Instant>));
        let last = Arc::new(tokio::sync::Mutex::new(None::<Instant>));

        let cc = count.clone();
        let cf = first.clone();
        let cl = last.clone();

        // Spawn a task that drains messages via recv().
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        // We don't need the sender — the sub handle lives in sub_handles.
        drop(tx);

        // Move sub into the task and drain it.
        let mut sub_owned = sub;
        let handle = tokio::spawn(async move {
            while let Some(_msg) = sub_owned.recv().await {
                let now = Instant::now();
                let was = cc.fetch_add(1, Relaxed);
                if was == 0 {
                    if let Ok(mut g) = cf.try_lock() { *g = Some(now); }
                }
                if let Ok(mut g) = cl.try_lock() { *g = Some(now); }
            }
            // rx kept alive so the task doesn't exit early
            drop(rx);
        });

        stats.push(ConsumerStats { name, count, first_msg_at: first, last_msg_at: last });
        join_handles.push(handle);
        // sub_handles now empty — sub was moved into task
    }

    (stats, join_handles, sub_handles, clients)
}

/// Wait until every consumer has received `target` messages, or `timeout` expires.
async fn wait_for_all(stats: &[ConsumerStats], target: u64, start: Instant, timeout: Duration) -> Duration {
    loop {
        let all_done = stats.iter().all(|s| s.count.load(Relaxed) >= target);
        if all_done { return start.elapsed(); }
        if start.elapsed() > timeout { return start.elapsed(); }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn publish_single(client: &Client, stream_id: u32, n: u64) {
    for _ in 0..n {
        loop {
            match client.publish(stream_id, SUBJECT, Bytes::copy_from_slice(PAYLOAD)) {
                Ok(()) => break,
                Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => panic!("publish: {e:?}"),
            }
        }
    }
}

async fn publish_batched(client: &Client, stream_id: u32, n: u64) {
    let mut remaining = n as usize;
    while remaining > 0 {
        let size = remaining.min(BATCH_SIZE);
        let entries: Vec<BatchEntry<'_>> = (0..size)
            .map(|_| BatchEntry::new(SUBJECT, Bytes::copy_from_slice(PAYLOAD)))
            .collect();
        loop {
            match client.publish_batch(stream_id, &entries) {
                Ok(()) => break,
                Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => panic!("publish_batch: {e:?}"),
            }
        }
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
    let setup = connect(&addr).await;
    let resp = setup
        .create_stream(STREAM, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    let n_consumers = match mode {
        Mode::PubSub => N_CONSUMERS_PUBSUB,
        Mode::Replay => N_CONSUMERS_REPLAY,
    };

    let deliver_policy = match mode {
        Mode::PubSub => 1u8, // New
        Mode::Replay => 0u8, // All
    };

    let (stats, _handles, _subs, _clients) = match mode {
        Mode::PubSub => {
            // Subscribe first, then publish.
            let s = spawn_consumers(&addr, stream_id, deliver_policy, n_consumers).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            s
        }
        Mode::Replay => {
            // Publish first, then subscribe.
            match pub_mode {
                Pub::Single => publish_single(&setup, stream_id, MSGS).await,
                Pub::Batch => publish_batched(&setup, stream_id, MSGS).await,
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            spawn_consumers(&addr, stream_id, deliver_policy, n_consumers).await
        }
    };

    let start = Instant::now();
    if let Mode::PubSub = mode {
        match pub_mode {
            Pub::Single => publish_single(&setup, stream_id, MSGS).await,
            Pub::Batch => publish_batched(&setup, stream_id, MSGS).await,
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
    let setup = connect(&addr).await;
    let resp = setup
        .create_stream(DIST_STREAM, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    let mut counts: Vec<Arc<AtomicU64>> = Vec::with_capacity(DIST_SUBS.len());
    let mut _handles = Vec::with_capacity(DIST_SUBS.len());
    let mut _clients = Vec::with_capacity(DIST_SUBS.len());

    for (i, s) in DIST_SUBS.iter().enumerate() {
        let client = connect(&addr).await;
        let cname = format!("c{i}");
        let group = format!("dist_g{i}");
        // DeliverPolicy::New = 1
        let resp = client
            .create_consumer(
                stream_id,
                cname.as_bytes(),
                group.as_bytes(),
                b"",
                u16::MAX,
                0, // ack_policy = None
                1, // deliver_policy = New
                0, // deliver_mode = Push/Fanout
                30_000,
                0,
            )
            .await
            .unwrap();
        let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

        // Subscribe with per-consumer filter.
        let sub = client
            .subscribe(stream_id, consumer_id, s.filter)
            .await
            .unwrap();

        let count = Arc::new(AtomicU64::new(0));
        let cc = count.clone();

        let handle = tokio::spawn(async move {
            let mut sub_owned = sub;
            while let Some(_msg) = sub_owned.recv().await {
                cc.fetch_add(1, Relaxed);
            }
        });

        counts.push(count);
        _handles.push(handle);
        _clients.push(client);
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    let total: u64 = DIST_SUBJECTS.len() as u64 * DIST_PER_SUBJECT;
    eprintln!("[wire-marker] BEGIN distribution");
    let pub_start = Instant::now();
    for subject in DIST_SUBJECTS {
        let mut remaining = DIST_PER_SUBJECT as usize;
        while remaining > 0 {
            let size = remaining.min(BATCH_SIZE);
            let entries: Vec<BatchEntry<'_>> = (0..size)
                .map(|_| BatchEntry::new(*subject, Bytes::copy_from_slice(PAYLOAD)))
                .collect();
            loop {
                match setup.publish_batch(stream_id, &entries) {
                    Ok(()) => break,
                    Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                        tokio::task::yield_now().await;
                    }
                    Err(e) => panic!("publish_batch dist: {e:?}"),
                }
            }
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

// ── Single-connection multi-sub stage ────────────────────────────────────

async fn run_single_conn_multi_sub() {
    let addr = spawn_server().await;
    let setup = connect(&addr).await;
    let resp = setup
        .create_stream(SCMS_STREAM, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // ONE TCP connection for all N consumers.
    let sub_client = connect(&addr).await;

    let mut counts: Vec<Arc<AtomicU64>> = Vec::with_capacity(SCMS_N_CONSUMERS);
    let mut _handles = Vec::with_capacity(SCMS_N_CONSUMERS);

    for i in 0..SCMS_N_CONSUMERS {
        let cname = format!("scms_c{i}");
        let group = format!("scms_g{i}");
        // DeliverPolicy::New = 1
        let resp = sub_client
            .create_consumer(
                stream_id,
                cname.as_bytes(),
                group.as_bytes(),
                b"",
                u16::MAX,
                0, // ack_policy = None
                1, // deliver_policy = New
                0, // deliver_mode = Push/Fanout
                30_000,
                0,
            )
            .await
            .unwrap();
        let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

        let sub = sub_client
            .subscribe(stream_id, consumer_id, b"")
            .await
            .unwrap();

        let count = Arc::new(AtomicU64::new(0));
        let cc = count.clone();

        let handle = tokio::spawn(async move {
            let mut sub_owned = sub;
            while let Some(_msg) = sub_owned.recv().await {
                cc.fetch_add(1, Relaxed);
            }
        });

        counts.push(count);
        _handles.push(handle);
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    eprintln!("[wire-marker] BEGIN single_conn_multi_sub");

    let pub_start = Instant::now();
    let mut remaining = SCMS_MSGS as usize;
    while remaining > 0 {
        let size = remaining.min(BATCH_SIZE);
        let entries: Vec<BatchEntry<'_>> = (0..size)
            .map(|_| BatchEntry::new(SCMS_SUBJECT, Bytes::copy_from_slice(PAYLOAD)))
            .collect();
        loop {
            match setup.publish_batch(stream_id, &entries) {
                Ok(()) => break,
                Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => panic!("publish_batch scms: {e:?}"),
            }
        }
        remaining -= size;
    }
    let pub_dur = pub_start.elapsed();

    let wait_start = Instant::now();
    let timeout = Duration::from_secs(30);
    loop {
        let all_ready = counts.iter().all(|c| c.load(Relaxed) >= SCMS_MSGS);
        if all_ready { break; }
        if wait_start.elapsed() > timeout { break; }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let delivery_dur = wait_start.elapsed();

    let total_callbacks: u64 = counts.iter().map(|c| c.load(Relaxed)).sum();
    let wire_today = SCMS_MSGS * SCMS_N_CONSUMERS as u64;
    let wire_collapsed = SCMS_MSGS;
    let reduction = SCMS_N_CONSUMERS as u64;

    println!("\n  ── Shape: 1 TCP connection, {} fanout consumers, {} msgs ──",
             SCMS_N_CONSUMERS, SCMS_MSGS);
    println!("    publish {:.2?}   delivery window {:.2?}", pub_dur, delivery_dur);
    for (i, c) in counts.iter().enumerate() {
        println!("    c{i} recv {}/{}", c.load(Relaxed), SCMS_MSGS);
    }
    println!();
    println!("    total client callbacks (delivered):     {}", total_callbacks);
    println!("    wire entries emitted today (no collapse): {}  (msgs × consumers)", wire_today);
    println!("    wire entries WITH broadcast collapse:     {}  (msgs × 1 conn)", wire_collapsed);
    println!("    potential reduction:                      {}×", reduction);
    println!();
    println!("    Run with ARBITRO_WIRE_TRACE=1 to verify the server-side");
    println!("    entry count against these numbers.");
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
    println!("  Section 2 — single connection, many subscriptions");
    println!("--------------------------------------------------------");
    println!(
        "  Measures broadcast-collapse potential: today the server emits");
    println!(
        "  1 wire entry per (msg × consumer); with collapse it would emit");
    println!(
        "  1 wire entry per (msg × connection).");
    run_single_conn_multi_sub().await;

    println!("\n--------------------------------------------------------");
    println!("  Section 3 — distribution check (subject filters)");
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
