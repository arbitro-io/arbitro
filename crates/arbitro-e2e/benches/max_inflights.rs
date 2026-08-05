//! Max-inflight bench — proves BOTH backpressure limits hold, and
//! measures what enforcing them costs.
//!
//! Arbitro has two independent in-flight caps and this bench covers both:
//!
//!   * **`max_inflight(N)`** — per **CONSUMER**. At most `N` messages may be
//!     outstanding (delivered, unacked) for that consumer in total, whatever
//!     subjects they are on.
//!   * **`max_subject_inflight(pattern, N)`** — per **SUBJECT**. Every unique
//!     subject matching `pattern` keeps its OWN counter, so 100 distinct
//!     `orders.basic.user_{i}` subjects under `("orders.basic.>", 1)` give
//!     100 independent 1/1 counters.
//!
//! The two are easy to conflate, so each proof stage neutralises the other
//! limit: the consumer-cap stage puts every message on a DISTINCT subject
//! (no subject counter can ever reach its cap), and the subject-cap stages
//! set `max_inflight` far above anything they publish.
//!
//! Every stage builds its consumer through [`ConsumerBuilder`] rather than
//! the flat `Client::create_consumer(...)` helper — the latter configures no
//! `SubjectLimit`s at all, so an earlier version of this bench was silently
//! measuring a consumer with no limits.
//!
//! ## Stages
//!
//!   0. **contract proofs** — these panic rather than report a number.
//!      * `Stage 00 consumer_inflight` — `max_inflight` is respected:
//!        exactly `N` deliver, the rest are held, acking one releases
//!        exactly one.
//!      * `Stage 0 cap_enforcement` — same three assertions for
//!        `max_subject_inflight`, on one saturated subject.
//!      * `Stage 2b burst_isolation` — the subject cap still holds while a
//!        100-subject basic backlog is pinned at 1/1.
//!   1. **baseline** — single subject, no contention. Pure publish→deliver
//!      latency with the cap configured (but never saturated).
//!   2. **isolated** — 100 basic subjects each pinned at 1/1 (unacked
//!      backlog); fresh premium subject every iter. Premium has its own
//!      `(orders.premium.>, 1)` counter, so the basic pin must not bleed.
//!   3. **shared consumer** — N connections bound to ONE `consumer_id` on
//!      ONE stream, so they genuinely share its cursor, pending set and
//!      subject counters. (Before, each "parallel client" got its own
//!      stream and consumer, sharing nothing and contending for nothing.)
//!   4. **dynamic subjects throughput** — N unique subjects under
//!      `(notif.user.>, 1)` exercises HashMap insert+remove on every
//!      ack-driven dec → key removal.
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench max_inflights --no-run 2>&1"
//!   wsl bash -lc "cp .../target/release/deps/max_inflights-* /tmp/arbitro/ && \
//!     cd /tmp/arbitro && timeout 120 ./max_inflights-* --bench"

use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_client_tokio::{
    AckPolicy, BatchEntry, Client, ClientConfig, ConsumerBuilder, DeliverMode, SubscriptionHandle,
};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;

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
    Client::connect(ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    })
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

// ── Cap-contract helpers ───────────────────────────────────────────

/// How long to wait before concluding that a message is genuinely being
/// held. Proving absence is only ever "absent so far", so this is
/// deliberately generous — ~4000× the steady-state delivery latency the
/// other stages measure.
const HOLD_WINDOW: Duration = Duration::from_secs(1);

/// What a delivery must look like for it to count.
///
/// The per-SUBJECT cap saturates one subject, so every delivery must be that
/// exact subject. The per-CONSUMER cap is deliberately exercised across many
/// distinct subjects (so no subject cap can interfere), so there only the
/// family can be checked.
#[derive(Clone, Copy)]
enum Expect<'a> {
    Exact(&'a [u8]),
    Prefix(&'a [u8]),
}

impl Expect<'_> {
    fn matches(&self, subject: &[u8]) -> bool {
        match self {
            Expect::Exact(s) => subject == *s,
            Expect::Prefix(p) => subject.starts_with(p),
        }
    }
    fn describe(&self) -> String {
        match self {
            Expect::Exact(s) => format!("exactly {}", String::from_utf8_lossy(s)),
            Expect::Prefix(p) => format!("anything under {}", String::from_utf8_lossy(p)),
        }
    }
}

/// Receive exactly `want` messages and **verify every one matches `expect`**.
///
/// The returned messages are NOT acked — the caller owns them so the limit
/// stays saturated. Counting without checking the subject was the original
/// hole here: a redelivery, or a message from another subject family, was
/// counted as if it were the capped one, so the number proved nothing.
async fn drain_expecting(
    sub: &mut SubscriptionHandle,
    want: u32,
    expect: Expect<'_>,
    label: &str,
) -> Vec<arbitro_client_tokio::Message> {
    let mut held = Vec::with_capacity(want as usize);
    while (held.len() as u32) < want {
        let msg = match tokio::time::timeout(Duration::from_secs(5), sub.recv()).await {
            Ok(Some(m)) => m,
            Ok(None) => panic!(
                "{label}: subscription closed after {} of {want}",
                held.len()
            ),
            Err(_) => panic!(
                "{label}: only {} of {want} arrived before the timeout — the limit is \
                 holding back messages it should have released",
                held.len()
            ),
        };
        assert!(
            expect.matches(msg.subject()),
            "{label}: expected every delivery to be {}, got {}. The count is therefore \
             not a count of capped messages and the assertion below would be \
             measuring the wrong thing.",
            expect.describe(),
            String::from_utf8_lossy(msg.subject()),
        );
        held.push(msg);
    }
    held
}

/// Fail if anything at all arrives inside `HOLD_WINDOW`.
async fn assert_nothing_arrives(sub: &mut SubscriptionHandle, label: &str) {
    if let Ok(Some(msg)) = tokio::time::timeout(HOLD_WINDOW, sub.recv()).await {
        panic!(
            "{label}: a message arrived on {} while the limit was already saturated \
             — the limit is NOT being enforced",
            String::from_utf8_lossy(msg.subject()),
        );
    }
}

/// The other half of the contract: acking one held message must release
/// **exactly one** more, on the same subject.
///
/// Without this the bench only proved the subject can block. A broker that
/// blocked and never unblocked again would have passed — and that is the
/// failure mode that actually hurts, because it is indistinguishable from a
/// stuck consumer.
async fn assert_ack_releases_exactly_one(
    sub: &mut SubscriptionHandle,
    held: &mut Vec<arbitro_client_tokio::Message>,
    expect: Expect<'_>,
    label: &str,
) {
    held.pop().expect("nothing held to ack").ack();

    let released = match tokio::time::timeout(Duration::from_secs(5), sub.recv()).await {
        Ok(Some(m)) => m,
        _ => panic!(
            "{label}: acking one message released NOTHING. The limit blocks but never \
             unblocks — a consumer that hits it once is stuck forever."
        ),
    };
    assert!(
        expect.matches(released.subject()),
        "{label}: the released message is {}, expected {}",
        String::from_utf8_lossy(released.subject()),
        expect.describe(),
    );
    held.push(released);

    // Freeing ONE slot must not release TWO.
    assert_nothing_arrives(sub, &format!("{label} (after releasing one slot)")).await;
}

// ── Stage 00 — proof that the CONSUMER cap is enforced ────────────

/// Consumer-level cap under test, small enough that an off-by-N is obvious.
const CONSUMER_INFLIGHT_CAP: u16 = 10;
/// Published well past it so an unenforced cap overshoots visibly.
const CONSUMER_INFLIGHT_PUBLISHED: u32 = 40;

/// The sibling of Stage 0, one level up: `max_inflight` bounds how many
/// messages a CONSUMER may hold unacked in total, regardless of subject.
///
/// Every other stage in this file sets `max_inflight` to 10 000 or 60 000 —
/// values chosen so the consumer cap never engages and the per-subject cap is
/// the only thing under observation. That left the consumer cap completely
/// unverified: it was configured everywhere and exercised nowhere.
///
/// Each message goes to its OWN subject (`orders.premium.slot_{i}`) so every
/// per-subject counter sits at 0/1 and cannot be what blocks delivery. The
/// only limit in play is the consumer's.
async fn stage00_consumer_inflight_enforced() -> (u32, Duration) {
    let addr = spawn_server().await;
    let client = connect(&addr).await;
    let stream_id = create_stream(&client, b"limits_consumer_inflight").await;

    let consumer_id = ConsumerBuilder::new(b"consumer_inflight")
        .filter(b">")
        .max_inflight(CONSUMER_INFLIGHT_CAP)
        .ack_policy(AckPolicy::Explicit)
        .ack_wait_ms(30_000)
        // Per-subject cap of 1, but every message is on a DIFFERENT subject,
        // so this never blocks — it only guards against the two limits being
        // silently conflated.
        .max_subject_inflight(PAT_PREMIUM, 1)
        .create(&client, stream_id)
        .await
        .expect("consumer_inflight consumer");
    let mut sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    let payload = vec![0u8; PAYLOAD_SIZE];
    let subjects: Vec<String> = (0..CONSUMER_INFLIGHT_PUBLISHED)
        .map(|i| format!("orders.premium.slot_{i}"))
        .collect();
    let entries: Vec<BatchEntry<'_>> = subjects
        .iter()
        .map(|s| BatchEntry::new(s.as_bytes(), Bytes::copy_from_slice(payload.as_slice())))
        .collect();

    let start = Instant::now();
    client
        .publish_batch_wait(stream_id, &entries)
        .await
        .unwrap();

    let expect = Expect::Prefix(b"orders.premium.slot_");

    // 1. Exactly `cap` deliver.
    let mut held = drain_expecting(
        &mut sub,
        CONSUMER_INFLIGHT_CAP as u32,
        expect,
        "consumer_inflight",
    )
    .await;
    let elapsed = start.elapsed();

    // 2. The rest are held — the consumer is full.
    assert_nothing_arrives(&mut sub, "consumer_inflight (consumer at max_inflight)").await;

    // 3. Acking one frees exactly one slot.
    assert_ack_releases_exactly_one(&mut sub, &mut held, expect, "consumer_inflight").await;

    (held.len() as u32, elapsed)
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
    client
        .publish_batch_wait(stream_id, &entries)
        .await
        .unwrap();

    // 1. Exactly `cap` deliver — and every one on the capped subject.
    let start = Instant::now();
    let mut held = drain_expecting(
        &mut sub,
        PAT_PREMIUM_CAP,
        Expect::Exact(one_subject),
        "cap_enforcement",
    )
    .await;
    let elapsed = start.elapsed();

    // 2. The remaining 10 must be HELD, not delivered.
    assert_nothing_arrives(&mut sub, "cap_enforcement (subject at cap)").await;

    // 3. Acking one must release exactly one — the cap unblocks.
    assert_ack_releases_exactly_one(
        &mut sub,
        &mut held,
        Expect::Exact(one_subject),
        "cap_enforcement",
    )
    .await;

    let delivered = held.len() as u32;
    (delivered, 0, elapsed)
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
    let basic_subjects: Vec<String> = (0..BASIC_BACKLOG)
        .map(|i| format!("orders.basic.user_{i}"))
        .collect();
    let basic_entries: Vec<BatchEntry<'_>> = basic_subjects
        .iter()
        .map(|s| BatchEntry::new(s.as_bytes(), Bytes::copy_from_slice(payload.as_slice())))
        .collect();
    client
        .publish_batch_wait(stream_id, &basic_entries)
        .await
        .unwrap();
    // Every one of these must be a BASIC subject — if a premium slipped in
    // here the pin below would be measuring the wrong family.
    let mut got = 0u32;
    let mut _pinned = Vec::with_capacity(BASIC_BACKLOG as usize);
    while got < BASIC_BACKLOG {
        let msg = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("basic backlog timeout")
            .expect("subscription closed");
        assert!(
            msg.subject().starts_with(b"orders.basic."),
            "burst_isolation: the pinning phase received {}, which is not a basic \
             subject — the 100 pins are not where the test thinks they are",
            String::from_utf8_lossy(msg.subject()),
        );
        _pinned.push(msg); // held unacked: the basic subjects stay at 1/1
        got += 1;
    }

    // Burst of PAT_PREMIUM_CAP + 50 to ONE premium subject.
    let one_subject = b"orders.premium.singleton";
    let total = PAT_PREMIUM_CAP + 50;
    let entries: Vec<BatchEntry<'_>> = (0..total)
        .map(|_| BatchEntry::new(one_subject, Bytes::copy_from_slice(payload.as_slice())))
        .collect();
    let start = Instant::now();
    client
        .publish_batch_wait(stream_id, &entries)
        .await
        .unwrap();

    // 1. Exactly `cap` premium deliver, all on the burst subject. A basic
    //    redelivery arriving here would now fail loudly instead of being
    //    counted as premium.
    let mut held = drain_expecting(
        &mut sub,
        PAT_PREMIUM_CAP,
        Expect::Exact(one_subject),
        "burst_isolation",
    )
    .await;
    let elapsed = start.elapsed();

    // 2. The other 50 stay held.
    assert_nothing_arrives(&mut sub, "burst_isolation (premium at cap)").await;

    // 3. And the premium cap still releases while basic stays pinned.
    assert_ack_releases_exactly_one(
        &mut sub,
        &mut held,
        Expect::Exact(one_subject),
        "burst_isolation",
    )
    .await;

    let delivered = held.len() as u32;
    (delivered, 0, elapsed)
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
    let basic_subjects: Vec<String> = (0..BASIC_BACKLOG)
        .map(|i| format!("orders.basic.user_{i}"))
        .collect();
    let basic_entries: Vec<BatchEntry<'_>> = basic_subjects
        .iter()
        .map(|s| BatchEntry::new(s.as_bytes(), Bytes::copy_from_slice(payload.as_slice())))
        .collect();
    client
        .publish_batch_wait(stream_id, &basic_entries)
        .await
        .unwrap();

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

    let (latencies, _c, _s) = measure_vip_latency(client, stream_id, sub, payload, iters).await;
    latencies
}

// ── Stage 3 — multi-client isolated ────────────────────────────────

/// Receive from whichever of `subs` produces first. Returns `None` on timeout.
///
/// Needed because the connections below share ONE consumer: a message goes to
/// exactly one of them and the caller cannot know which, so waiting on a fixed
/// subscription would deadlock.
async fn recv_any(
    subs: &mut [SubscriptionHandle],
    budget: Duration,
) -> Option<arbitro_client_tokio::Message> {
    let futs: Vec<_> = subs.iter_mut().map(|s| Box::pin(s.recv())).collect();
    match tokio::time::timeout(budget, futures::future::select_all(futs)).await {
        Ok((msg, _idx, _rest)) => msg,
        Err(_) => None,
    }
}

/// N connections **sharing ONE stream and ONE consumer**.
///
/// This is what "multi-client" has to mean for a per-subject cap to be under
/// test at all. The counter is per-CONSUMER (`consumer_subjects`, keyed by
/// `ConsumerId`), so the previous shape here — a private
/// `limits_stream_c{i}` + `isolation_tester_c{i}` for every client — shared
/// no counter, no cursor and no ack floor between the "parallel clients". It
/// measured N unrelated workloads on one shard, not isolation.
///
/// Here the 100 basic subjects are pinned once, on the one consumer, and all
/// N connections compete for its VIP traffic.
async fn shared_consumer_latency(iters: u64, n_conns: u64) -> Vec<Duration> {
    let addr = spawn_server().await;
    let admin = connect(&addr).await;
    let stream_id = create_stream(&admin, b"limits_shared").await;

    let consumer_id = ConsumerBuilder::new(b"shared_isolation_tester")
        // Queue group so the connections are competing members of one
        // consumer rather than independent readers.
        .group(b"shared_isolation_tester")
        .deliver_mode(DeliverMode::Queue)
        .filter(b">")
        .max_inflight(10_000)
        .ack_policy(AckPolicy::Explicit)
        .ack_wait_ms(30_000)
        .max_subject_inflight(PAT_BASIC, 1)
        .max_subject_inflight(PAT_PREMIUM, 1)
        .create(&admin, stream_id)
        .await
        .expect("shared consumer");

    let mut conns = Vec::with_capacity(n_conns as usize);
    let mut subs = Vec::with_capacity(n_conns as usize);
    for _ in 0..n_conns {
        let c = connect(&addr).await;
        let s = c
            .subscribe(stream_id, consumer_id, b"")
            .await
            .expect("subscribe to the shared consumer");
        conns.push(c);
        subs.push(s);
    }

    // Pin the basic subjects ONCE — they belong to the one consumer.
    let payload = vec![0u8; PAYLOAD_SIZE];
    let basic_subjects: Vec<String> = (0..BASIC_BACKLOG)
        .map(|j| format!("orders.basic.user_{j}"))
        .collect();
    let basic_entries: Vec<BatchEntry<'_>> = basic_subjects
        .iter()
        .map(|s| BatchEntry::new(s.as_bytes(), Bytes::copy_from_slice(payload.as_slice())))
        .collect();
    admin
        .publish_batch_wait(stream_id, &basic_entries)
        .await
        .unwrap();

    let mut got = 0u32;
    while got < BASIC_BACKLOG {
        recv_any(&mut subs, Duration::from_secs(5))
            .await
            .expect("basic backlog timeout");
        got += 1;
        // Never acked — the subjects stay pinned at 1/1.
    }

    let mut latencies = Vec::with_capacity(iters as usize);
    for i in 0..iters {
        let subj = format!("orders.premium.vip_{i}");
        let start = Instant::now();
        loop {
            match admin.publish(stream_id, subj.as_bytes(), Bytes::copy_from_slice(&payload)) {
                Ok(()) => break,
                Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => panic!("vip publish: {e:?}"),
            }
        }
        let msg = match recv_any(&mut subs, Duration::from_secs(5)).await {
            Some(m) => m,
            None => panic!("VIP delivery timeout at iteration {i} (subject {subj})"),
        };
        latencies.push(start.elapsed());
        msg.ack();
    }

    latencies
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

    let subjects: Vec<String> = (0..n_users).map(|i| format!("notif.user.{i}")).collect();
    let entries: Vec<BatchEntry<'_>> = subjects
        .iter()
        .map(|s| BatchEntry::new(s.as_bytes(), Bytes::copy_from_slice(payload.as_slice())))
        .collect();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let start = Instant::now();
    client
        .publish_batch_wait(stream_id, &entries)
        .await
        .unwrap();

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
    println!("                   max-inflight bench");
    println!("========================================================");
    println!("  iters={iters}   payload={PAYLOAD_SIZE}B");
    println!("  Stage 00 proves max_inflight (per CONSUMER).");
    println!("  Stage 0/2b prove max_subject_inflight (per SUBJECT).");
    println!("  Caps: basic={PAT_BASIC_CAP}, premium={PAT_PREMIUM_CAP}, dynamic={PAT_DYNAMIC_CAP}");
    println!("  Stage 2/3 hold {BASIC_BACKLOG} basic subjects at 1/1.");
    println!();

    // ── Stage 00 — consumer max_inflight check (panics on failure) ──
    println!("--------------------------------------------------------");
    println!(
        "  Stage 00 — consumer_inflight (publish {} on {} DISTINCT subjects, \
         max_inflight={}, expect exactly {})",
        CONSUMER_INFLIGHT_PUBLISHED,
        CONSUMER_INFLIGHT_PUBLISHED,
        CONSUMER_INFLIGHT_CAP,
        CONSUMER_INFLIGHT_CAP,
    );
    println!("--------------------------------------------------------");
    let (delivered_s00, elapsed_s00) = stage00_consumer_inflight_enforced().await;
    println!(
        "  delivered={delivered_s00}  elapsed={:.2?}  → max_inflight IS enforced",
        elapsed_s00
    );
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

    let avg_base: Duration = base.iter().sum::<Duration>() / base.len() as u32;
    let avg_iso: Duration = iso.iter().sum::<Duration>() / iso.len() as u32;
    let ratio = avg_iso.as_secs_f64() / avg_base.as_secs_f64();

    // Stage 3 — one consumer, many connections
    println!();
    println!("--------------------------------------------------------");
    println!("  Stage 3 — shared consumer ({n_clients} connections on ONE stream + ONE consumer)");
    println!("--------------------------------------------------------");
    let all = shared_consumer_latency(iters, n_clients).await;
    report("VIP across shared-consumer members", &all);

    let avg_multi: Duration = all.iter().sum::<Duration>() / all.len() as u32;
    let ratio_multi = avg_multi.as_secs_f64() / avg_base.as_secs_f64();

    println!();
    println!("--------------------------------------------------------");
    println!("  Summary");
    println!("--------------------------------------------------------");
    println!(
        "  baseline (1 client, no backlog)        avg : {:>9.2?}",
        avg_base
    );
    println!(
        "  isolated (1 client, 100 basic at 1/1)  avg : {:>9.2?}",
        avg_iso
    );
    println!(
        "  shared   ({n_clients} conns on ONE consumer)      avg : {:>9.2?}",
        avg_multi
    );
    println!("  ratios (vs baseline):  isolated={ratio:.2}x   shared={ratio_multi:.2}x");
    println!("  (closer to 1.0 = better isolation)");
    println!();

    // Stage 4 — dynamic subjects throughput
    println!("--------------------------------------------------------");
    println!("  Stage 4 — dynamic subjects throughput ({dynamic_users} unique users)");
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
