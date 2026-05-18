//! Subject-limit isolation bench — measures latency / throughput when
//! `max_subject_inflight` is actually configured.
//!
//! Every stage in this file builds its consumer with the
//! [`ConsumerBuilder`] and pins per-subject caps via
//! [`ConsumerBuilder::max_subject_inflight`] so the numbers reflect the
//! cost of enforcing the cap, not the cost of a bare consumer with no
//! limits. The previous version of this bench used the flat
//! `Client::create_consumer(...)` helper (no `SubjectLimit`s) and was
//! silently testing the wrong thing.
//!
//! ## Semantics
//!
//! `max_subject_inflight(pattern, N)` sets a per-*subject* cap whose
//! value `N` comes from the pattern. Each unique subject that matches
//! the pattern keeps its own atomic counter — 100 distinct
//! `orders.basic.user_{i}` subjects with `("orders.basic.>", 1)` give
//! 100 independent 1/1 counters.
//!
//! ## Stages
//!
//!   1. **baseline** — single subject, no contention. Pure publish→deliver
//!      latency with the cap configured (but never saturated).
//!   2. **isolated** — 100 basic subjects each pinned at 1/1 (unacked
//!      backlog); fresh premium subject every iter. Premium has its own
//!      `(orders.premium.>, 1)` counter, so the basic pin must not bleed.
//!   3. **multi-client isolated** — 4 parallel clients, each pinning
//!      100 basic subjects on its own stream.
//!   4. **dynamic subjects throughput** — N unique subjects under
//!      `(notif.user.>, 1)` exercises HashMap insert+remove on every
//!      ack-driven dec → key removal.
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench limits --no-run 2>&1"
//!   wsl bash -lc "cp .../target/release/deps/limits-* /tmp/arbitro/ && \
//!     cd /tmp/arbitro && timeout 120 ./limits-* --bench"

use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_client_tokio::{
    AckPolicy, BatchEntry, Client, ClientConfig, ConsumerBuilder, SubscriptionHandle,
};
use bytes::Bytes;
use arbitro_server::{ArbitroServer, Config};

const DEFAULT_ITERS: u64 = 1_000;
const BASIC_BACKLOG: u32 = 100;
const PAYLOAD_SIZE: usize = 64;
const STREAM: &[u8] = b"limits_e2e";
/// Default users for Stage 4 (dynamic subjects throughput).
const DEFAULT_DYNAMIC_USERS: u64 = 10_000;

/// Subject patterns used across all stages.
const PAT_BASIC: &[u8] = b"orders.basic.>";
const PAT_PREMIUM: &[u8] = b"orders.premium.>";
const PAT_DYNAMIC: &[u8] = b"notif.user.>";

/// Per-subject caps actually configured on the consumers. Basic is
/// pinned at 1 because that's the cheapest way to force every basic
/// subject into the "1/1 saturated" state for the isolation tests.
///
/// Premium uses a HIGHER cap so the bench can prove the limit is
/// observable: Stage 0 publishes `PAT_PREMIUM_CAP + 10` messages to a
/// single premium subject and asserts the server delivers **exactly**
/// `PAT_PREMIUM_CAP` (the 10 extra must stay held). With `cap = 1` the
/// test was a tautology — every VIP iteration used a fresh subject
/// whose counter started at `0/1`, so we never saw the cap engage.
const PAT_BASIC_CAP: u32 = 1;
const PAT_PREMIUM_CAP: u32 = 100;
const PAT_DYNAMIC_CAP: u32 = 1;

fn env_u64(var: &str, fallback: u64) -> u64 {
    std::env::var(var).ok().and_then(|s| s.parse().ok()).unwrap_or(fallback)
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

async fn create_stream(client: &Client, name: &[u8]) -> u32 {
    let resp = client
        .create_stream(name, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream");
    u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ── VIP latency measurement ─────────────────────────────────────────

/// Run `iters` VIP publish + deliver rounds, measuring each. The
/// consumer is created with `(orders.premium.>, 1)` so each fresh
/// `orders.premium.vip_{i}` subject has its own 0/1 counter; the ack
/// at the end of the iteration drops the counter back to 0 (and
/// removes the entry from the per-consumer HashMap).
async fn measure_vip_latency(
    client: Client,
    stream_id: u32,
    mut sub: SubscriptionHandle,
    payload: Vec<u8>,
    iters: u64,
) -> (Vec<Duration>, Client, SubscriptionHandle) {
    let mut latencies = Vec::with_capacity(iters as usize);
    for i in 0..iters {
        let subj = format!("orders.premium.vip_{i}");
        let start = Instant::now();
        loop {
            match client.publish(stream_id, subj.as_bytes(), Bytes::copy_from_slice(&payload)) {
                Ok(()) => break,
                Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => panic!("vip publish: {e:?}"),
            }
        }
        let vip_msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("VIP delivery timeout")
            .expect("subscription closed");
        latencies.push(start.elapsed());
        vip_msg.ack();
    }
    (latencies, client, sub)
}

// ── Stage 0 — proof that the cap is enforced ──────────────────────

/// Functional check that doubles as the bench's "trust marker": if the
/// server is honouring `max_subject_inflight(PAT_PREMIUM, PAT_PREMIUM_CAP)`,
/// then publishing `PAT_PREMIUM_CAP + 10` messages to ONE single premium
/// subject without acking must deliver **exactly** `PAT_PREMIUM_CAP`
/// messages and stall on the next one. If the broker delivers any of
/// the extra 10 inside the timeout window, the cap is not being
/// enforced and the whole bench is a lie — we panic loudly so the
/// failure is impossible to miss.
///
/// Returns (delivered_within_cap, extras_seen_after_cap, elapsed).
async fn stage0_cap_enforced() -> (u32, u32, Duration) {
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    let stream_id = create_stream(&client, b"limits_cap_enforced").await;

    let consumer_id = ConsumerBuilder::new(b"cap_enforced")
        .filter(b">")
        .max_inflight(10_000)
        .ack_policy(AckPolicy::Explicit)
        .ack_wait_ms(30_000)
        .max_subject_inflight(PAT_PREMIUM, PAT_PREMIUM_CAP)
        .create(&client, stream_id)
        .await
        .expect("cap_enforced consumer");
    let mut sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // All messages go to ONE subject so they share the same counter.
    let one_subject = b"orders.premium.singleton";
    let payload = vec![0u8; PAYLOAD_SIZE];
    let total = PAT_PREMIUM_CAP + 10;
    let entries: Vec<BatchEntry<'_>> = (0..total)
        .map(|_| BatchEntry::new(one_subject, Bytes::copy_from_slice(payload.as_slice())))
        .collect();
    client.publish_batch_sync(stream_id, &entries).await.unwrap();

    // Receive up to PAT_PREMIUM_CAP — must succeed inside a short budget.
    let start = Instant::now();
    let mut delivered = 0u32;
    while delivered < PAT_PREMIUM_CAP {
        let msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("premium cap delivery timeout — cap is suspiciously slow")
            .expect("subscription closed before cap reached");
        // Do NOT ack — we want the counter to STAY at PAT_PREMIUM_CAP.
        let _ = msg;
        delivered += 1;
    }
    let elapsed = start.elapsed();

    // Now wait for an extra message. If the server respects the cap,
    // none should arrive (the next 10 are all blocked behind the same
    // saturated subject counter). Use a tight 250ms budget — that's
    // ~10× the steady-state delivery latency we measure elsewhere.
    let mut extras = 0u32;
    let extra_window = Duration::from_millis(250);
    let extras_start = Instant::now();
    while extras_start.elapsed() < extra_window {
        match tokio::time::timeout(Duration::from_millis(50), sub.recv()).await {
            Ok(Some(_)) => extras += 1,
            _ => {}
        }
    }

    assert_eq!(
        extras, 0,
        "cap_enforcement FAILED — published {total} msgs to one subject \
         with cap={PAT_PREMIUM_CAP}, expected exactly {PAT_PREMIUM_CAP} to \
         deliver and the next {} to be held. Got {} extras inside a \
         250 ms window. The server is NOT enforcing max_subject_inflight.",
        total - PAT_PREMIUM_CAP,
        extras,
    );

    (delivered, extras, elapsed)
}

// ── Stage 2b — burst isolation under per-subject backlog ──────────

/// Stage 2 with teeth: 100 basic subjects pinned at 1/1 AND a burst of
/// `PAT_PREMIUM_CAP + 50` messages to a SINGLE premium subject (so the
/// premium counter saturates at `PAT_PREMIUM_CAP`). Asserts:
///   - exactly `PAT_PREMIUM_CAP` premium messages deliver
///   - none of the 50 extras leak through inside the timeout
///   - the basic backlog does not unblock anything (it never gets acked)
///
/// This is the strongest functional proof that isolation is real.
async fn stage2b_burst_isolation() -> (u32, u32, Duration) {
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    let stream_id = create_stream(&client, b"limits_burst_isolation").await;

    let consumer_id = ConsumerBuilder::new(b"burst_isolation")
        .filter(b">")
        .max_inflight(10_000)
        .ack_policy(AckPolicy::Explicit)
        .ack_wait_ms(30_000)
        .max_subject_inflight(PAT_BASIC, PAT_BASIC_CAP)
        .max_subject_inflight(PAT_PREMIUM, PAT_PREMIUM_CAP)
        .create(&client, stream_id)
        .await
        .expect("burst_isolation consumer");
    let mut sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
    let payload = vec![0u8; PAYLOAD_SIZE];

    // Pin 100 basic subjects at 1/1 — drain without ack.
    let basic_subjects: Vec<String> =
        (0..BASIC_BACKLOG).map(|i| format!("orders.basic.user_{i}")).collect();
    let basic_entries: Vec<BatchEntry<'_>> = basic_subjects
        .iter()
        .map(|s| BatchEntry::new(s.as_bytes(), Bytes::copy_from_slice(payload.as_slice())))
        .collect();
    client.publish_batch_sync(stream_id, &basic_entries).await.unwrap();
    let mut got = 0u32;
    while got < BASIC_BACKLOG {
        let _msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("basic backlog timeout")
            .expect("subscription closed");
        got += 1;
    }

    // Burst of PAT_PREMIUM_CAP + 50 to ONE premium subject.
    let one_subject = b"orders.premium.singleton";
    let total = PAT_PREMIUM_CAP + 50;
    let entries: Vec<BatchEntry<'_>> = (0..total)
        .map(|_| BatchEntry::new(one_subject, Bytes::copy_from_slice(payload.as_slice())))
        .collect();
    let start = Instant::now();
    client.publish_batch_sync(stream_id, &entries).await.unwrap();

    let mut delivered = 0u32;
    while delivered < PAT_PREMIUM_CAP {
        let msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("burst delivery timeout — basic backlog is contaminating premium")
            .expect("subscription closed before cap reached");
        let _ = msg;
        delivered += 1;
    }
    let elapsed = start.elapsed();

    // Wait for any of the 50 extras.
    let mut extras = 0u32;
    let extra_window = Duration::from_millis(250);
    let extras_start = Instant::now();
    while extras_start.elapsed() < extra_window {
        match tokio::time::timeout(Duration::from_millis(50), sub.recv()).await {
            Ok(Some(_)) => extras += 1,
            _ => {}
        }
    }
    assert_eq!(
        extras, 0,
        "burst_isolation FAILED — under 100 basic pinned at 1/1 and a \
         burst of {total} premium to one subject (cap={PAT_PREMIUM_CAP}), \
         expected exactly {PAT_PREMIUM_CAP} premium to deliver. Got {} \
         extras — premium counter is either not being enforced or basic \
         backlog is leaking into premium isolation.",
        extras,
    );

    (delivered, extras, elapsed)
}

// ── Stage 1 — baseline ─────────────────────────────────────────────

/// Consumer with `(orders.premium.>, 1)` set but never under pressure
/// (only one VIP in flight at a time, acked immediately). Measures
/// the steady-state cost of having the cap configured.
async fn baseline_latency(iters: u64) -> Vec<Duration> {
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    let stream_id = create_stream(&client, STREAM).await;

    let consumer_id = ConsumerBuilder::new(b"baseline")
        .filter(b">")
        .max_inflight(10_000)
        .ack_policy(AckPolicy::Explicit)
        .ack_wait_ms(30_000)
        .max_subject_inflight(PAT_PREMIUM, PAT_PREMIUM_CAP)
        .create(&client, stream_id)
        .await
        .expect("baseline consumer");

    let sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
    let payload = vec![0u8; PAYLOAD_SIZE];

    let (latencies, _client, _sub) =
        measure_vip_latency(client, stream_id, sub, payload, iters).await;
    latencies
}

// ── Stage 2 — isolated under per-subject backlog ───────────────────

/// Consumer with caps on BOTH families: `(orders.basic.>, 1)` and
/// `(orders.premium.>, 1)`. We then pin every basic subject by
/// publishing one msg per subject and NOT acking — each basic counter
/// is now stuck at 1/1.
///
/// Premium subjects keep their own (orders.premium.>, 1) counters,
/// which are independent of the basic counters. So VIP must keep
/// delivering with the same latency as Stage 1.
async fn isolated_latency(iters: u64) -> Vec<Duration> {
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    let stream_id = create_stream(&client, STREAM).await;

    let consumer_id = ConsumerBuilder::new(b"isolation_tester")
        .filter(b">")
        .max_inflight(10_000)
        .ack_policy(AckPolicy::Explicit)
        .ack_wait_ms(30_000)
        .max_subject_inflight(PAT_BASIC, PAT_BASIC_CAP)
        .max_subject_inflight(PAT_PREMIUM, PAT_PREMIUM_CAP)
        .create(&client, stream_id)
        .await
        .expect("isolated consumer");
    let mut sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
    let payload = vec![0u8; PAYLOAD_SIZE];

    // Pin 100 unique basic subjects at 1/1.
    let basic_subjects: Vec<String> =
        (0..BASIC_BACKLOG).map(|i| format!("orders.basic.user_{i}")).collect();
    let basic_entries: Vec<BatchEntry<'_>> = basic_subjects
        .iter()
        .map(|s| BatchEntry::new(s.as_bytes(), Bytes::copy_from_slice(payload.as_slice())))
        .collect();
    client.publish_batch_sync(stream_id, &basic_entries).await.unwrap();

    let mut got = 0u32;
    while got < BASIC_BACKLOG {
        let _msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("basic backlog timeout")
            .expect("subscription closed");
        got += 1;
    }
    // Do NOT ack — basic subjects stay pinned at 1/1 for the whole
    // measurement window.

    let (latencies, _c, _s) =
        measure_vip_latency(client, stream_id, sub, payload, iters).await;
    latencies
}

// ── Stage 3 — multi-client isolated ────────────────────────────────

/// Same as Stage 2 but with N parallel clients on N independent streams.
async fn multi_client_isolated_latency(
    iters: u64,
    n_clients: u64,
) -> Vec<Vec<Duration>> {
    let addr = spawn_server().await;

    let mut futs = Vec::new();
    for i in 0..n_clients {
        let addr = addr.clone();
        futs.push(async move {
            let client = connect(&addr).await;
            let stream_name = format!("limits_stream_c{i}");
            let stream_id = create_stream(&client, stream_name.as_bytes()).await;

            let name = format!("isolation_tester_c{i}");
            let consumer_id = ConsumerBuilder::new(name.as_bytes())
                .filter(b">")
                .max_inflight(10_000)
                .ack_policy(AckPolicy::Explicit)
                .ack_wait_ms(30_000)
                .max_subject_inflight(PAT_BASIC, 1)
                .max_subject_inflight(PAT_PREMIUM, 1)
                .create(&client, stream_id)
                .await
                .expect("multi-client consumer");
            let mut sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
            let payload = vec![0u8; PAYLOAD_SIZE];

            let basic_subjects: Vec<String> =
                (0..BASIC_BACKLOG).map(|j| format!("orders.basic.user_{j}")).collect();
            let basic_entries: Vec<BatchEntry<'_>> = basic_subjects
                .iter()
                .map(|s| BatchEntry::new(s.as_bytes(), Bytes::copy_from_slice(payload.as_slice())))
                .collect();
            client.publish_batch_sync(stream_id, &basic_entries).await.unwrap();

            let mut got = 0u32;
            while got < BASIC_BACKLOG {
                let _msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
                    .await
                    .expect("basic backlog timeout")
                    .expect("subscription closed");
                got += 1;
            }

            let (latencies, _c, _s) =
                measure_vip_latency(client, stream_id, sub, payload, iters).await;
            latencies
        });
    }

    futures::future::join_all(futs).await
}

// ── Reporting ──────────────────────────────────────────────────────

fn report(label: &str, latencies: &[Duration]) {
    let mut sorted = latencies.to_vec();
    sorted.sort();
    let sum: Duration = sorted.iter().sum();
    let avg = sum / sorted.len() as u32;
    let p50 = percentile(&sorted, 0.50);
    let p90 = percentile(&sorted, 0.90);
    let p99 = percentile(&sorted, 0.99);
    let min = sorted.first().copied().unwrap_or(Duration::ZERO);
    let max = sorted.last().copied().unwrap_or(Duration::ZERO);
    println!(
        "  {label:<36} | n={:<5} | avg={:>8.2?} | p50={:>8.2?} | p90={:>8.2?} | p99={:>8.2?} | min={:>8.2?} | max={:>8.2?}",
        sorted.len(),
        avg,
        p50,
        p90,
        p99,
        min,
        max
    );
}

// ── Stage 4 — high-cardinality dynamic subjects ────────────────────

/// One consumer with `(notif.user.>, 1)`. Publish N msgs to N distinct
/// `notif.user.{i}` subjects, then drain+ack all. Each delivery hits
/// the per-(consumer, subject_hash) counter inc; each ack drives
/// dec→0→remove. The HashMap touches the maximum number of unique
/// keys possible.
async fn dynamic_subjects_throughput(n_users: u64) -> (Duration, u64) {
    let addr = spawn_server().await;
    let client = connect(&addr).await;

    let stream_name: &[u8] = b"dynamic_subjects";
    let stream_id = create_stream(&client, stream_name).await;

    let consumer_id = ConsumerBuilder::new(b"dyn_consumer")
        .filter(b">")
        .max_inflight(60_000)
        .ack_policy(AckPolicy::Explicit)
        .ack_wait_ms(30_000)
        .max_subject_inflight(PAT_DYNAMIC, PAT_DYNAMIC_CAP)
        .create(&client, stream_id)
        .await
        .expect("dynamic consumer");
    let mut sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
    let payload = vec![0u8; PAYLOAD_SIZE];

    let subjects: Vec<String> =
        (0..n_users).map(|i| format!("notif.user.{i}")).collect();
    let entries: Vec<BatchEntry<'_>> = subjects
        .iter()
        .map(|s| BatchEntry::new(s.as_bytes(), Bytes::copy_from_slice(payload.as_slice())))
        .collect();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let start = Instant::now();
    client.publish_batch_sync(stream_id, &entries).await.unwrap();

    let mut got = 0u64;
    while got < n_users {
        match tokio::time::timeout(Duration::from_secs(30), sub.recv()).await {
            Ok(Some(msg)) => {
                msg.ack();
                got += 1;
            }
            _ => break,
        }
    }
    let elapsed = start.elapsed();
    (elapsed, got)
}

// ── Main ───────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let iters = env_u64("BENCH_LIMITS_ITERS", DEFAULT_ITERS);
    let n_clients = env_u64("BENCH_LIMITS_CLIENTS", 4);
    let dynamic_users = env_u64("BENCH_LIMITS_DYNAMIC_USERS", DEFAULT_DYNAMIC_USERS);

    println!();
    println!("========================================================");
    println!("                  Subject limits bench");
    println!("========================================================");
    println!("  iters={iters}   payload={PAYLOAD_SIZE}B");
    println!("  All stages use max_subject_inflight via ConsumerBuilder.");
    println!(
        "  Caps: basic={PAT_BASIC_CAP}, premium={PAT_PREMIUM_CAP}, dynamic={PAT_DYNAMIC_CAP}"
    );
    println!("  Stage 2/3 hold {BASIC_BACKLOG} basic subjects at 1/1.");
    println!();

    // ── Stage 0 — cap enforcement check (panics on failure) ─────────
    println!("--------------------------------------------------------");
    println!(
        "  Stage 0 — cap_enforcement (publish {} to ONE subject, expect exactly {})",
        PAT_PREMIUM_CAP + 10,
        PAT_PREMIUM_CAP,
    );
    println!("--------------------------------------------------------");
    let (delivered_s0, extras_s0, elapsed_s0) = stage0_cap_enforced().await;
    println!(
        "  delivered={delivered_s0}  extras={extras_s0}  elapsed={:.2?}  → cap IS enforced",
        elapsed_s0
    );

    // ── Stage 2b — burst isolation under basic backlog ──────────────
    println!();
    println!("--------------------------------------------------------");
    println!(
        "  Stage 2b — burst_isolation (100 basic pinned + {} premium burst to ONE subject)",
        PAT_PREMIUM_CAP + 50,
    );
    println!("--------------------------------------------------------");
    let (delivered_s2b, extras_s2b, elapsed_s2b) = stage2b_burst_isolation().await;
    println!(
        "  delivered={delivered_s2b}  extras={extras_s2b}  elapsed={:.2?}  → premium isolated from basic",
        elapsed_s2b
    );
    println!();

    // Stage 1 — baseline
    println!("--------------------------------------------------------");
    println!("  Stage 1 — baseline (orders.premium.> capped at 1, never saturated)");
    println!("--------------------------------------------------------");
    let base = baseline_latency(iters).await;
    report("baseline VIP publish -> deliver", &base);

    // Stage 2 — isolated under per-subject backlog
    println!();
    println!("--------------------------------------------------------");
    println!("  Stage 2 — isolated (100 basic subjects pinned at 1/1)");
    println!("--------------------------------------------------------");
    let iso = isolated_latency(iters).await;
    report("VIP under basic load", &iso);

    let avg_base: Duration =
        base.iter().sum::<Duration>() / base.len() as u32;
    let avg_iso: Duration =
        iso.iter().sum::<Duration>() / iso.len() as u32;
    let ratio = avg_iso.as_secs_f64() / avg_base.as_secs_f64();

    // Stage 3 — multi-client isolated
    println!();
    println!("--------------------------------------------------------");
    println!(
        "  Stage 3 — multi-client isolated ({n_clients} parallel clients, each pinning 100 basic)"
    );
    println!("--------------------------------------------------------");
    let per_client = multi_client_isolated_latency(iters, n_clients).await;
    for (i, lats) in per_client.iter().enumerate() {
        let label = format!("client {i} VIP under load");
        report(&label, lats);
    }

    let mut all: Vec<Duration> = Vec::with_capacity((iters * n_clients) as usize);
    for lats in &per_client {
        all.extend_from_slice(lats);
    }
    let avg_multi: Duration = all.iter().sum::<Duration>() / all.len() as u32;
    let ratio_multi = avg_multi.as_secs_f64() / avg_base.as_secs_f64();

    println!();
    println!("--------------------------------------------------------");
    println!("  Summary");
    println!("--------------------------------------------------------");
    println!("  baseline (1 client, no backlog)        avg : {:>9.2?}", avg_base);
    println!("  isolated (1 client, 100 basic at 1/1)  avg : {:>9.2?}", avg_iso);
    println!("  multi    ({n_clients} clients, 100 basic each)   avg : {:>9.2?}", avg_multi);
    println!("  ratios (vs baseline):  isolated={ratio:.2}x   multi={ratio_multi:.2}x");
    println!("  (closer to 1.0 = better isolation)");
    println!();

    // Stage 4 — dynamic subjects throughput
    println!("--------------------------------------------------------");
    println!(
        "  Stage 4 — dynamic subjects throughput ({dynamic_users} unique users)"
    );
    println!("  Pattern: notif.user.<id> with max_subject_inflight(notif.user.>, 1)");
    println!("  Exercises: HashMap insert+remove on every msg lifecycle");
    println!("--------------------------------------------------------");
    let (elapsed, delivered) = dynamic_subjects_throughput(dynamic_users).await;
    let msgs_per_sec = delivered as f64 / elapsed.as_secs_f64();
    let ns_per_msg = elapsed.as_nanos() as f64 / delivered as f64;
    println!(
        "  {dynamic_users} users | delivered={delivered} | elapsed={:.2?} | {msgs_per_sec:>10.0} msg/s | {ns_per_msg:>7.0} ns/msg",
        elapsed
    );
    println!();

    // Stage 4b — 1k for comparison
    let small_n = 1_000u64;
    let (elapsed_s, delivered_s) = dynamic_subjects_throughput(small_n).await;
    let msgs_per_sec_s = delivered_s as f64 / elapsed_s.as_secs_f64();
    let ns_per_msg_s = elapsed_s.as_nanos() as f64 / delivered_s as f64;
    println!(
        "  {small_n} users  | delivered={delivered_s} | elapsed={:.2?} | {msgs_per_sec_s:>10.0} msg/s | {ns_per_msg_s:>7.0} ns/msg",
        elapsed_s
    );
    println!();

    let _ = (ratio, ratio_multi, Arc::new(()));
}
