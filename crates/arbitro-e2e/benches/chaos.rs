//! Chaos bench — sustained concurrent load on a tolerant (disk-backed) stream,
//! verifying zero message loss end-to-end.
//!
//! ## Shape
//!
//! - **Stream**: `JournalKind::Tolerant` under `/tmp/arbitro-chaos-<pid>`.
//! - **Producers**: `BENCH_CHAOS_PRODUCERS` (default 4) tokio tasks, each
//!   publishing to a unique subject (`prod.{id}.evt`) at a target rate of
//!   `BENCH_CHAOS_RATE` (default 1000 msgs/s **per producer**).
//!   Rate-limited with a simple sleep loop so the consumer always keeps up.
//! - **Consumer**: single `AckPolicy::Explicit`, `DeliverPolicy::All`,
//!   `max_inflight = 65_535`. Client's ack_loop coalesces acks into
//!   BatchAck frames automatically.
//! - **Duration**: `BENCH_CHAOS_SECS` (default 10). Producers stop after
//!   the timer; consumer keeps draining until the published seq range is
//!   fully covered or a stall window elapses.
//!
//! ## Loss verification
//!
//! Every message the server stores gets a unique monotonic `seq` from the
//! journal. The consumer records which seqs it has received in a
//! `HashSet<u64>`. At the end we assert:
//!
//!   - `received_count == published` (counts match — no loss)
//!   - `received_unique == published` (no duplicates)
//!   - `seq range is 1..=published` (no gaps)
//!
//! ## Cleanup
//!
//! Startup kills any lingering bench processes AND wipes
//! `/tmp/arbitro-chaos-*` dirs from previous runs. On exit the bench
//! removes its own data dir (even on panic, via best-effort cleanup).
//!
//! ## Run
//!
//! ```bash
//! wsl bash -lc "cd /mnt/.../arbitro && \
//!   cargo bench --bench chaos --no-run 2>&1"
//! wsl bash -lc "cp .../target/release/deps/chaos-* /tmp/arbitro-bench/ && \
//!   cd /tmp/arbitro-bench && timeout 60 ./chaos-* --bench"
//! ```

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverPolicy, JournalKind, StreamConfig};
use arbitro_server::{ArbitroServer, Config};

const DEFAULT_SECS: u64 = 10;
const DEFAULT_PRODUCERS: u64 = 4;
const DEFAULT_CONSUMERS: u64 = 1;
/// Per-producer target rate (msgs/second).
const DEFAULT_RATE: u64 = 1_000;
const BATCH_SIZE: usize = 32;
const PAYLOAD_SIZE: usize = 64;
const STREAM: &[u8] = b"chaos_stream";

fn env_u64(var: &str, fallback: u64) -> u64 {
    std::env::var(var).ok().and_then(|s| s.parse().ok()).unwrap_or(fallback)
}

// ── Metrics helpers ─────────────────────────────────────────────────────────

/// Resident set size (KB) of the current process. Linux only.
fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages * 4)
        .unwrap_or(0)
}

/// Total CPU time (user + kernel) consumed by the process in nanoseconds.
#[cfg(target_os = "linux")]
fn cpu_time_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe {
        libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts);
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

#[cfg(not(target_os = "linux"))]
fn cpu_time_ns() -> u64 {
    0
}

fn portpicker() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Best-effort wipe of any stale `/tmp/arbitro-chaos-*` dirs from previous runs.
fn prune_stale_tmp() {
    let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else {
        return;
    };
    for ent in entries.flatten() {
        let path = ent.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with("arbitro-chaos-") {
            let _ = std::fs::remove_dir_all(&path);
        }
    }
}

fn make_data_dir() -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!("arbitro-chaos-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tmp data dir");
    dir
}

/// Cleanup guard — removes the data dir when dropped (even on panic).
struct DataDirCleanup(PathBuf);
impl Drop for DataDirCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn spawn_server(data_dir: &Path) -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(64)
        .shard_count(1)
        .write_buffer_cap(4 * 1024 * 1024)
        .data_dir(data_dir.to_string_lossy().into_owned());
    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect_with_timeout(addr, Duration::from_secs(5))
        .await
        .expect("client connects")
}

async fn producer_task(
    id: u64,
    addr: String,
    rate_per_sec: u64,
    stop: Arc<AtomicBool>,
    published: Arc<AtomicU64>,
) {
    let client = connect(&addr).await;
    let subject = format!("prod.{id}.evt");
    let payload = vec![0xABu8; PAYLOAD_SIZE];

    // One batch of BATCH_SIZE msgs every `batch_period` seconds gives
    // BATCH_SIZE / batch_period = rate_per_sec effective throughput.
    let batch_period = Duration::from_nanos(
        (BATCH_SIZE as u64 * 1_000_000_000 / rate_per_sec.max(1)).max(1),
    );

    let mut next_tick = Instant::now();
    while !stop.load(Relaxed) {
        let entries: Vec<(&[u8], &[u8])> = (0..BATCH_SIZE)
            .map(|_| (subject.as_bytes(), payload.as_slice()))
            .collect();
        if client.publish_batch(STREAM, &entries).await.is_err() {
            break;
        }
        published.fetch_add(BATCH_SIZE as u64, Relaxed);

        next_tick += batch_period;
        let now = Instant::now();
        if next_tick > now {
            tokio::time::sleep(next_tick - now).await;
        } else {
            // We fell behind; resync to avoid burst-catch-up.
            next_tick = now;
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let secs = env_u64("BENCH_CHAOS_SECS", DEFAULT_SECS);
    let n_producers = env_u64("BENCH_CHAOS_PRODUCERS", DEFAULT_PRODUCERS);
    let n_consumers = env_u64("BENCH_CHAOS_CONSUMERS", DEFAULT_CONSUMERS);
    let rate = env_u64("BENCH_CHAOS_RATE", DEFAULT_RATE);

    // ── Pre-run cleanup ─────────────────────────────────────────────────
    prune_stale_tmp();

    let data_dir = make_data_dir();
    let _cleanup_guard = DataDirCleanup(data_dir.clone());

    let total_target_rate = rate * n_producers;
    let expected_total = total_target_rate * secs;

    println!();
    println!("========================================================");
    println!("                      Chaos bench");
    println!("========================================================");
    println!("  duration={secs}s   producers={n_producers}   consumers={n_consumers}   rate={rate} msg/s/producer");
    println!("  total target: {total_target_rate} msg/s  ~  {expected_total} msgs");
    println!("  batch={BATCH_SIZE}   payload={PAYLOAD_SIZE}B");
    println!("  journal=Tolerant   data_dir={}", data_dir.display());
    println!();

    let addr = spawn_server(&data_dir).await;

    // ── Stream + consumer setup ─────────────────────────────────────────
    let control_client = connect(&addr).await;
    let stream_cfg = StreamConfig::new(STREAM, b">")
        .journal_kind(JournalKind::Tolerant)
        .build();
    control_client.create_stream(&stream_cfg).await.expect("create stream");

    // Create N consumers, each with a UNIQUE name + UNIQUE group so they
    // form independent fanout streams — every msg must reach every consumer.
    // BENCH_CHAOS_CONSUMERS=1 keeps the original single-consumer behaviour.
    for i in 0..n_consumers {
        let name = format!("chaos-{i}");
        let group = format!("chaos-grp-{i}");
        let cfg = ConsumerConfig::new(name.as_bytes(), STREAM)
            .group(group.as_bytes())
            .filter(b">")
            .ack_policy(AckPolicy::Explicit)
            .max_inflight(u16::MAX)
            .deliver_policy(DeliverPolicy::All)
            .build()
            .unwrap();
        control_client.create_consumer(&cfg).await.unwrap();
    }

    // ── Consumer tasks — N parallel TCP connections drain concurrently ───
    // Each consumer has its own HashSet of received seqs (to verify
    // per-consumer no-loss) and its own counter. Under fanout semantics
    // each consumer must receive every msg published.
    //
    // Subscribers start BEFORE producers so every consumer's subscribe
    // RPC has landed on the server by the time the first publish happens.
    // Otherwise early msgs race the subscribe and — despite
    // DeliverPolicy::All — risk being filed before the binding is live.
    let consumer_stop = Arc::new(AtomicBool::new(false));
    let per_consumer_seqs: Vec<Arc<std::sync::Mutex<HashSet<u64>>>> = (0..n_consumers)
        .map(|_| {
            Arc::new(std::sync::Mutex::new(HashSet::with_capacity(
                expected_total as usize,
            )))
        })
        .collect();
    let per_consumer_count: Vec<Arc<AtomicU64>> =
        (0..n_consumers).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let mut recv_tasks = Vec::new();
    for i in 0..n_consumers {
        let addr = addr.clone();
        let consumer_stop = Arc::clone(&consumer_stop);
        let seqs = Arc::clone(&per_consumer_seqs[i as usize]);
        let counter = Arc::clone(&per_consumer_count[i as usize]);

        recv_tasks.push(tokio::spawn(async move {
            // Each consumer gets its own Client = its own TCP connection.
            let client = connect(&addr).await;
            let name = format!("chaos-{i}");
            let group = format!("chaos-grp-{i}");
            let cfg = ConsumerConfig::new(name.as_bytes(), STREAM)
                .group(group.as_bytes())
                .filter(b">")
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
                        seqs.lock().unwrap().insert(seq);
                        counter.fetch_add(1, Relaxed);
                    }
                    Ok(None) => break,
                    Err(_) => {
                        if consumer_stop.load(Relaxed) {
                            break;
                        }
                    }
                }
            }
        }));
    }

    // Give every subscriber a moment to complete `subscribe()` on the
    // server before producers start. 200 ms is plenty for loopback; if
    // this races on a slower box, raise BENCH_CHAOS_SETTLE_MS.
    let settle_ms = env_u64("BENCH_CHAOS_SETTLE_MS", 200);
    tokio::time::sleep(Duration::from_millis(settle_ms)).await;

    // ── Kick off producers (after subscribers are ready) ───────────────
    let stop = Arc::new(AtomicBool::new(false));
    let published = Arc::new(AtomicU64::new(0));

    let producer_handles: Vec<_> = (0..n_producers)
        .map(|id| {
            let addr = addr.clone();
            let stop = Arc::clone(&stop);
            let published = Arc::clone(&published);
            tokio::spawn(producer_task(id, addr, rate, stop, published))
        })
        .collect();

    // ── Baseline metrics BEFORE the run ────────────────────────────────
    let rss_start_kb = rss_kb();
    let cpu_start_ns = cpu_time_ns();

    // Peak RSS tracker — sampled every 100ms by a side task.
    let peak_rss = Arc::new(AtomicU64::new(rss_start_kb));
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let sampler_task = {
        let peak_rss = Arc::clone(&peak_rss);
        let sampler_stop = Arc::clone(&sampler_stop);
        tokio::spawn(async move {
            while !sampler_stop.load(Relaxed) {
                let cur = rss_kb();
                peak_rss.fetch_max(cur, Relaxed);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };

    // ── Timer: let the chaos run; print progress every second ─────────
    let run_start = Instant::now();
    for elapsed in 1..=secs {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let pub_total = published.load(Relaxed);
        // Aggregate received across all consumers (for the progress line).
        let recv_total: u64 = per_consumer_count
            .iter()
            .map(|c| c.load(Relaxed))
            .sum();
        let min_recv = per_consumer_count
            .iter()
            .map(|c| c.load(Relaxed))
            .min()
            .unwrap_or(0);
        let rss_now_mb = rss_kb() / 1024;
        println!(
            "  [t={elapsed:>2}s] published={pub_total:>6} received={recv_total:>6} min_per_consumer={min_recv:>5} rss={rss_now_mb:>4} MB",
        );
    }

    // ── Stop producers and wait for them to finish publishing ──────────
    stop.store(true, Relaxed);
    for h in producer_handles {
        let _ = h.await;
    }
    let pub_total_final = published.load(Relaxed);
    let pub_elapsed = run_start.elapsed();
    println!();
    println!(
        "  producers stopped: {pub_total_final} msgs in {:.2?} ({:.0} msg/s)",
        pub_elapsed,
        pub_total_final as f64 / pub_elapsed.as_secs_f64()
    );

    // ── Wait for every consumer to catch the tail ─────────────────────
    // Each consumer must independently have received all published msgs
    // (fanout semantics). We're done when the MIN of per-consumer counts
    // reaches pub_total_final.
    let target_seq = pub_total_final;
    let drain_start = Instant::now();
    let drain_deadline = drain_start + Duration::from_secs(15);
    let total_so_far = |counts: &[Arc<AtomicU64>]| -> u64 {
        counts.iter().map(|c| c.load(Relaxed)).sum()
    };
    let min_so_far = |counts: &[Arc<AtomicU64>]| -> u64 {
        counts.iter().map(|c| c.load(Relaxed)).min().unwrap_or(0)
    };
    let mut last_recv = total_so_far(&per_consumer_count);
    let mut stall_start = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let per_min = min_so_far(&per_consumer_count);
        if per_min >= target_seq {
            break;
        }
        if Instant::now() >= drain_deadline {
            println!(
                "  WARN drain_deadline hit — min per-consumer {per_min}/{target_seq}"
            );
            break;
        }
        let recv = total_so_far(&per_consumer_count);
        if recv != last_recv {
            last_recv = recv;
            stall_start = Instant::now();
        } else if Instant::now().duration_since(stall_start) > Duration::from_secs(3) {
            println!(
                "  WARN drain stalled — min per-consumer {per_min}/{target_seq} (no progress 3s)"
            );
            break;
        }
    }

    consumer_stop.store(true, Relaxed);
    for h in recv_tasks {
        let _ = h.await;
    }
    sampler_stop.store(true, Relaxed);
    let _ = sampler_task.await;

    // ── Resource usage ──────────────────────────────────────────────────
    let rss_end_kb = rss_kb();
    let peak_rss_kb = peak_rss.load(Relaxed);
    let cpu_end_ns = cpu_time_ns();

    let drain_elapsed = drain_start.elapsed();
    let total_elapsed = run_start.elapsed();

    println!("  drain tail: {drain_elapsed:.2?}");
    println!();

    // ── Loss verification (per-consumer) ──────────────────────────────
    println!("--------------------------------------------------------");
    println!("  Loss check  (per-consumer fanout integrity)");
    println!("--------------------------------------------------------");
    println!("  published : {pub_total_final}");
    println!();

    let mut any_loss = false;
    let mut total_recv_all = 0u64;
    let mut all_gaps: Vec<Vec<u64>> = Vec::new();
    for i in 0..n_consumers {
        let seqs = per_consumer_seqs[i as usize].lock().unwrap();
        let count = per_consumer_count[i as usize].load(Relaxed);
        let unique = seqs.len() as u64;
        let duplicates = count.saturating_sub(unique);
        let max_seq = seqs.iter().copied().max().unwrap_or(0);
        let gaps: Vec<u64> = if max_seq > 0 {
            (1..=pub_total_final)
                .filter(|s| !seqs.contains(s))
                .take(5)
                .collect()
        } else {
            (1..=pub_total_final.min(5)).collect()
        };
        drop(seqs);

        let ok = count == pub_total_final && unique == pub_total_final && gaps.is_empty();
        if !ok {
            any_loss = true;
        }
        total_recv_all += count;
        all_gaps.push(gaps.clone());

        let status = if ok { "OK" } else { "LOSS" };
        println!(
            "  consumer {i:<2}: recv={count:>6} unique={unique:>6} dup={duplicates} max_seq={max_seq} [{status}]"
        );
        if !gaps.is_empty() {
            println!("             missing (first 5): {gaps:?}");
        }
    }
    println!();
    println!("  aggregate received : {total_recv_all}  (expected {})", pub_total_final * n_consumers);

    println!("--------------------------------------------------------");
    println!("  Summary");
    println!("--------------------------------------------------------");
    println!("  runtime            : {total_elapsed:.2?}");
    println!(
        "  publish rate       : {:.0} msg/s",
        pub_total_final as f64 / pub_elapsed.as_secs_f64()
    );
    println!(
        "  end-to-end rate    : {:.0} msg/s  (aggregate across {n_consumers} consumers)",
        total_recv_all as f64 / total_elapsed.as_secs_f64()
    );

    // Resource usage summary.
    let rss_start_mb = rss_start_kb as f64 / 1024.0;
    let rss_end_mb = rss_end_kb as f64 / 1024.0;
    let rss_peak_mb = peak_rss_kb as f64 / 1024.0;
    let rss_delta_mb = rss_end_mb - rss_start_mb;
    let cpu_used_ns = cpu_end_ns.saturating_sub(cpu_start_ns);
    let cpu_used_secs = cpu_used_ns as f64 / 1_000_000_000.0;
    let wall_secs = total_elapsed.as_secs_f64();
    let cpu_pct = (cpu_used_secs / wall_secs) * 100.0;

    println!(
        "  RSS start          : {rss_start_mb:>6.1} MB   end: {rss_end_mb:>6.1} MB   peak: {rss_peak_mb:>6.1} MB   Δ: {rss_delta_mb:+.1} MB"
    );
    println!(
        "  CPU used           : {cpu_used_secs:>6.2} s   ({cpu_pct:>5.1}% of wall)   per msg: {:>5.0} ns",
        cpu_used_ns as f64 / total_recv_all.max(1) as f64
    );

    // ── Assertions ─────────────────────────────────────────────────────
    assert!(pub_total_final > 0, "no messages were published");
    assert!(!any_loss, "per-consumer loss detected: {all_gaps:?}");
    assert_eq!(
        total_recv_all,
        pub_total_final * n_consumers,
        "aggregate received ({total_recv_all}) != published * consumers ({})",
        pub_total_final * n_consumers,
    );

    println!();
    println!("  RESULT: OK — no loss per consumer, no duplicates, no gaps");
    println!();

    // DataDirCleanup drops here and removes the dir.
}
