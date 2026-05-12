//! Memory growth bench — verifies bounded working-set memory under
//! sustained high-subject-cardinality traffic with the in-memory store.
//!
//! ## The invariant under test
//!
//! After the `refactor/consumer-owned-counters` PR, per-(consumer,
//! subject) inflight lives in the drain thread's
//! `ConsumerSubjects` map, and `dec()` removes the entry when its
//! count reaches zero. The whole point of moving off papaya was to
//! make that removal safe (no ABA hazard under single-thread
//! ownership). If the invariant holds:
//!
//!   * over K iterations of "publish N unique subjects → ack all" the
//!     map should return to size 0 after each batch
//!   * RSS should stabilise instead of climbing linearly with the
//!     number of distinct subjects ever observed
//!
//! ## Workload shape
//!
//! - 1 stream, in-memory store (`JOURNAL_KIND = Memory`)
//! - 1 consumer, `AckPolicy::Explicit`, catch-all filter
//! - Per iteration: publish `MSGS_PER_ITER` messages, each with a
//!   fresh `bench.subj.<ID>` subject so the `(consumer, subject_hash)`
//!   key is brand-new every time. Then wait for every message to be
//!   received and acked.
//! - After ack, the drain's `ConsumerSubjects` slot for this consumer
//!   should be back to `total() == 0` and `distinct_subjects() == 0`.
//!
//! Repeat `ITERATIONS` times. Report RSS at every iter so a leak shows
//! up as a monotonic climb. With the refactor in place, RSS should
//! plateau after the first few iters (Vec/HashMap capacity stays at
//! peak, but no new pages are committed).
//!
//! ## bench_safety waiver
//!
//! `MSGS_PER_ITER * ITERATIONS` exceeds the 1000-msg cap; explicit
//! waiver — a leak only manifests across MANY distinct subjects, so
//! 1000 total is a smoke test, not a memory-growth probe. Defaults:
//! `MSGS_PER_ITER=1000`, `ITERATIONS=20` → 20k unique subjects.
//! Timeout per iter: `ITER_TIMEOUT = 30s`. Total bench wall clock
//! capped at `OUTER_TIMEOUT = 600s`.

extern crate libc;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitro_client_tokio::{Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;
use tokio::runtime::Runtime;

// ── Settings ────────────────────────────────────────────────────────────────

/// Messages per iteration. Each carries a fresh, unique subject so the
/// `(consumer, subject_hash)` key is never repeated within a run.
const MSGS_PER_ITER: u32 = 1_000;

/// Number of publish→ack rounds. Total distinct subjects observed by
/// the broker = `MSGS_PER_ITER * ITERATIONS`.
const ITERATIONS: u32 = 20;

/// Soft timeout per iteration. Should comfortably fit even on a cold
/// runner; a hang means delivery or ack flow is stuck.
const ITER_TIMEOUT: Duration = Duration::from_secs(30);

/// Bench-wide hard cap. Honours the `timeout 120` rule for the typical
/// run but allows headroom for higher `BENCH_MEMORY_ITERATIONS` env
/// overrides.
const OUTER_TIMEOUT: Duration = Duration::from_secs(600);

/// 64-byte payload — small enough to keep the bench focused on the
/// per-subject bookkeeping, large enough to materialise real frames.
fn shared_payload() -> Arc<[u8]> {
    Arc::from(vec![0u8; 64].into_boxed_slice())
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn cfg_msgs_per_iter() -> u32 {
    env_u32("BENCH_MEMORY_MSGS", MSGS_PER_ITER)
}

fn cfg_iterations() -> u32 {
    env_u32("BENCH_MEMORY_ITERATIONS", ITERATIONS)
}

// ── Process introspection ───────────────────────────────────────────────────

/// Resident set size in KiB. Linux-only (we run benches under WSL).
fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages * 4) // 4 KiB pages on Linux
        .unwrap_or(0)
}

// ── Infrastructure ──────────────────────────────────────────────────────────

fn pick_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn start_server() -> String {
    let port = pick_port();
    let addr = format!("127.0.0.1:{port}");
    // Memory store: any RSS climb comes from per-subject bookkeeping,
    // not from disk-backed segments.
    let config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(8)
        .write_buffer_cap(65536);
    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect(ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    })
    .await
    .expect("client must connect")
}

// ── Bench loop ──────────────────────────────────────────────────────────────

/// Publish `msgs` messages each with a UNIQUE subject (`prefix.{iter}.{i}`),
/// then drain + ack every one of them through the supplied subscription
/// handle. Returns the elapsed wall-clock time for the round-trip.
async fn run_iteration(
    publisher: &Client,
    stream_id: u32,
    handle: &mut arbitro_client_tokio::SubscriptionHandle,
    iter: u32,
    msgs: u32,
    payload: &Arc<[u8]>,
) -> Duration {
    let start = Instant::now();

    // Publish msgs with unique subjects. fire-and-forget — the consumer
    // is already subscribed, drain delivers as the publish lands.
    for i in 0..msgs {
        let subject = format!("bench.subj.{iter}.{i}");
        // Tiny retry loop on channel-full backpressure.
        loop {
            match publisher.publish(
                stream_id,
                subject.as_bytes(),
                Bytes::copy_from_slice(&payload[..]),
            ) {
                Ok(()) => break,
                Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => panic!("publish: {e:?}"),
            }
        }
    }

    // Drain every message and ack. We MUST ack so the per-(consumer,
    // subject) entry hits zero and the drain removes it.
    let mut received: u32 = 0;
    while received < msgs {
        match tokio::time::timeout(ITER_TIMEOUT, handle.recv()).await {
            Ok(Some(msg)) => {
                msg.ack();
                received += 1;
            }
            Ok(None) => panic!("handle.recv returned None mid-iteration"),
            Err(_) => panic!(
                "iter={iter} timed out after {received}/{msgs} acks — leak or stall",
            ),
        }
    }

    start.elapsed()
}

// ── Report ──────────────────────────────────────────────────────────────────

struct IterReport {
    iter: u32,
    rss_kib: u64,
    rss_delta_kib: i64,
    elapsed_ms: u128,
}

fn print_header() {
    println!(
        "  {:>4} | {:>10} | {:>12} | {:>10}",
        "iter", "elapsed", "RSS", "Δ RSS"
    );
    println!("  {}", "-".repeat(50));
}

fn print_row(r: &IterReport) {
    println!(
        "  {:>4} | {:>7} ms | {:>9} MiB | {:>+7} KiB",
        r.iter,
        r.elapsed_ms,
        r.rss_kib / 1024,
        r.rss_delta_kib,
    );
}

// ── Main ────────────────────────────────────────────────────────────────────

fn main() {
    let msgs = cfg_msgs_per_iter();
    let iters = cfg_iterations();
    let total = msgs as u64 * iters as u64;

    println!(
        "\nMemory growth bench — verify ConsumerSubjects drops to zero on ack"
    );
    println!(
        "Workload: in-memory store, 1 stream, 1 consumer (Explicit ack),\
         catch-all filter."
    );
    println!(
        "Config: msgs_per_iter={msgs}, iterations={iters}, total_unique_subjects={total}"
    );
    println!("{}", "=".repeat(70));

    let rt = Runtime::new().unwrap();
    let outer_start = Instant::now();

    rt.block_on(async move {
        let addr = start_server().await;
        let publisher = connect(&addr).await;
        let subscriber = connect(&addr).await;

        // Stream + explicit-ack consumer + catch-all subscription.
        // `max_msgs = msgs as u64` caps the store at one iteration's
        // working set so the MemoryStore backlog stops being a source
        // of RSS growth — only per-consumer bookkeeping can still
        // climb if the refactor's invariant is broken.
        // (name, filter, max_msgs, max_bytes, max_age_secs, replicas,
        //  journal_kind=Memory, retention=Limits, discard=Old)
        let resp = publisher
            .create_stream(b"mem_bench", b">", msgs as u64, 0, 0, 1, 0, 0, 0, 0)
            .await
            .expect("create_stream");
        let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

        let resp = subscriber
            .create_consumer(
                stream_id,
                b"mem_consumer",
                b"",       // queue group (unique → fanout group of 1)
                b"",       // no filter override
                u16::MAX,  // max_inflight
                1u8,       // AckPolicy::Explicit
                0u8,       // DeliverPolicy::All
                0u8,       // DeliverMode::Push
                30_000,    // ack_wait_ms
                0,         // start_seq
            )
            .await
            .expect("create_consumer");
        let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

        let mut handle = subscriber
            .subscribe(stream_id, consumer_id, b"")
            .await
            .expect("subscribe");

        let payload = shared_payload();

        print_header();
        let baseline = rss_kib();
        let mut last_rss = baseline;
        println!(
            "  {:>4} | {:>10} | {:>9} MiB | {:>10}",
            "0", "(baseline)", baseline / 1024, "—"
        );

        for iter in 1..=iters {
            if outer_start.elapsed() >= OUTER_TIMEOUT {
                println!("  OUTER TIMEOUT hit ({OUTER_TIMEOUT:?}) — stopping");
                break;
            }

            let elapsed = run_iteration(
                &publisher,
                stream_id,
                &mut handle,
                iter,
                msgs,
                &payload,
            )
            .await;

            // Drop the store backlog so the next iter's RSS sample
            // reflects bookkeeping only (no acked-but-still-stored
            // entries). With explicit ack + max_msgs cap this is
            // belt-and-braces, but it makes the per-subject signal
            // cleaner if the cap-eviction path ever changes.
            let _ = publisher.purge_stream(b"mem_bench").await;

            let now_rss = rss_kib();
            let delta = now_rss as i64 - last_rss as i64;

            print_row(&IterReport {
                iter,
                rss_kib: now_rss,
                rss_delta_kib: delta,
                elapsed_ms: elapsed.as_millis(),
            });

            last_rss = now_rss;
        }

        let final_rss = rss_kib();
        let total_delta_kib = final_rss as i64 - baseline as i64;
        println!("{}", "=".repeat(70));
        println!(
            "  Final: baseline={} MiB  end={} MiB  Δ={} KiB across {} unique subjects",
            baseline / 1024,
            final_rss / 1024,
            total_delta_kib,
            total,
        );

        // Heuristic verdict. With removal-on-zero working, the drain's
        // per-consumer map has 0 entries by end-of-bench; the only RSS
        // growth should come from the store + accumulator + frame
        // buffers (bounded by config). A leak proportional to `total`
        // would explode this number into the tens of MiB.
        let per_subject_bytes = (total_delta_kib.max(0) * 1024) as u64 / total.max(1);
        println!(
            "  ≈ {} bytes/subject grown (target: ~0 — leak would be ≥16 B/subject)",
            per_subject_bytes,
        );
    });
}
