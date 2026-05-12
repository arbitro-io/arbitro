//! Chaos bench — correctness under real fault injection.
//!
//! ## Disasters simulated (inline in the main ticker loop)
//!
//! | t  | Event |
//! |----|-------|
//! | 4s | Server kill |
//! | 5s | Server restart on a new port; producers reconnect |
//! | 8s | Consumer force-disconnect + reconnect |
//! | 12s| Server kill |
//! | 13s| Server restart on a new port; producers reconnect |
//!
//! ## Why inline (not spawned tasks)?
//!
//! Management methods (`create_stream`, `create_consumer`, etc.) are
//! `async fn(&self)` — their futures borrow `&Client` across `.await`.
//! Since `Client: !Sync`, `&Client: !Send`, so those futures are `!Send`.
//! `tokio::spawn` requires `Future: Send`.  Only `publish_sync` and
//! `subscribe` are designed as `fn(&self) -> impl Future + Send`.
//!
//! Running chaos logic inline in `main` (which uses `block_on`, no `Send`
//! requirement) sidesteps the issue entirely.
//!
//! ## Loss invariant
//!
//! `acked_seqs ⊆ received_seqs` — every server-confirmed message eventually
//! delivered. Duplicates (redelivery after reconnect) are expected and handled
//! by the `HashSet` deduplication.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use arbitro_client_tokio::{Client, ClientConfig, ReconnectPolicy};
use bytes::Bytes;
use arbitro_server::{ArbitroServer, Config};
use tokio::sync::watch;

// ── Tunables ──────────────────────────────────────────────────────────────────

const RUN_SECS:        u64 = 18;
const N_PRODUCERS:     u64 = 4;
const RATE:            u64 = 150;   // target publish_sync msg/s per producer
const JOURNAL_DISK:    u8  = 1;
const STREAM:          &[u8] = b"chaos";
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

// ── Helpers ───────────────────────────────────────────────────────────────────

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn prune_stale() {
    let Ok(rd) = std::fs::read_dir(std::env::temp_dir()) else { return };
    for e in rd.flatten() {
        let p = e.path();
        if p.file_name().and_then(|n| n.to_str())
            .map(|n| n.starts_with("arbitro-chaos-")).unwrap_or(false)
        {
            let _ = std::fs::remove_dir_all(&p);
        }
    }
}

fn make_data_dir() -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("arbitro-chaos-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
}

fn make_cfg(addr: &str) -> ClientConfig {
    ClientConfig {
        addr: addr.to_string(),
        reconnect: ReconnectPolicy {
            base:         Duration::from_millis(100),
            cap:          Duration::from_millis(1_000),
            max_attempts: None,
        },
        ..ClientConfig::default()
    }
}

/// Spawn a server and wait until it actually accepts TCP connections.
async fn spawn_server(addr: &str, data_dir: &Path) -> watch::Sender<bool> {
    let (tx, rx) = watch::channel(false);
    let cfg = Config::default()
        .listen_addr(addr)
        .max_connections(64)
        .shard_count(1)
        .data_dir(data_dir.to_string_lossy().into_owned());
    tokio::spawn(async move { let _ = ArbitroServer::new(cfg).run_with_shutdown(rx).await; });
    // Poll until the port is accepting connections.
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if std::net::TcpStream::connect(addr).is_ok() { break; }
    }
    tx
}

/// Connect, retrying until success.
async fn connect_retry(addr: &str) -> Client {
    loop {
        match Client::connect(make_cfg(addr)).await {
            Ok(c)  => return c,
            Err(_) => tokio::time::sleep(Duration::from_millis(150)).await,
        }
    }
}

/// Create (or re-confirm) the chaos stream on the current server.
/// Returns (stream_id, consumer_id).
async fn ensure_stream_and_consumer(addr: &str) -> (u32, u32) {
    let c = connect_retry(addr).await;
    let resp = c.create_stream(STREAM, b">", 0, 0, 0, 1, JOURNAL_DISK, 0, 0, 0)
        .await.expect("create_stream");
    let sid = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    let resp = c.create_consumer(sid, b"chaos-c", b"chaos-grp", b"",
                                 u16::MAX, 1u8, 0u8, 0u8, 30_000u32, 0u64)
        .await.expect("create_consumer");
    let cid = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
    (sid, cid)
}

#[cfg(target_os = "linux")]
fn rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/statm").ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|p| p as f64 * 4.0 / 1024.0)
        .unwrap_or(0.0)
}
#[cfg(not(target_os = "linux"))]
fn rss_mb() -> f64 { 0.0 }

// ── Producer ──────────────────────────────────────────────────────────────────
//
// Only calls `publish_sync` (impl Future + Send) and `connect_retry` (free fn).
// NO management async fn(&self) calls — those require Client: Sync.

async fn producer(
    id:       u64,
    cur_addr: Arc<RwLock<String>>,
    cur_sid:  Arc<AtomicU32>,
    stop:     Arc<AtomicBool>,
    acked:    Arc<std::sync::Mutex<HashSet<u64>>>,
    ok_cnt:   Arc<AtomicU64>,
    err_cnt:  Arc<AtomicU64>,
) {
    let subj     = format!("prod.{id}");
    let payload  = vec![id as u8; 32];
    let interval = Duration::from_nanos(1_000_000_000 / RATE.max(1));

    // Option<Client> lets us drop the client eagerly when the server goes down.
    let mut client: Option<Client> = None;
    let mut last_addr = String::new();
    let mut tick      = Instant::now();

    while !stop.load(Relaxed) {
        let sid = cur_sid.load(Relaxed);

        // ── Server is down: drop client, wait ────────────────────────────
        if sid == 0 {
            client = None; // release connection to dead server
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        // ── Addr changed or no client yet: reconnect immediately ─────────
        let cur = cur_addr.read().unwrap().clone();
        if client.is_none() || cur != last_addr {
            last_addr = cur.clone();
            client    = Some(connect_retry(&cur).await);
            tick      = Instant::now();
        }

        // publish_sync(&self) -> impl Future + 'static + Send — borrow ends
        // after the call; future is self-contained (no &Client held across .await).
        let fut = client.as_ref().unwrap()
            .publish_sync(sid, subj.as_bytes(), Bytes::copy_from_slice(&payload));
        let result = tokio::time::timeout(PUBLISH_TIMEOUT, fut).await;

        match result {
            Ok(Ok(b)) => {
                let seq = u64::from_le_bytes(b[..8].try_into().unwrap());
                acked.lock().unwrap().insert(seq);
                ok_cnt.fetch_add(1, Relaxed);
            }
            _ => {
                err_cnt.fetch_add(1, Relaxed);
                // Drop client; next iteration will reconnect to cur_addr.
                client = None;
                tokio::time::sleep(Duration::from_millis(200)).await;
                tick = Instant::now();
                continue;
            }
        }

        tick += interval;
        let now = Instant::now();
        if tick > now { tokio::time::sleep(tick - now).await; }
        else          { tick = now; }
    }
}

// ── Consumer ──────────────────────────────────────────────────────────────────
//
// Only calls `subscribe` (impl Future + Send) and `connect_retry` (free fn).

async fn consumer_loop(
    cur_addr:        Arc<RwLock<String>>,
    cur_sid:         Arc<AtomicU32>,
    cur_cid:         Arc<AtomicU32>,
    stop:            Arc<AtomicBool>,
    force_reconnect: Arc<AtomicBool>,
    recv_seqs:       Arc<std::sync::Mutex<HashSet<u64>>>,
    recv_total:      Arc<AtomicU64>,
    reconnect_cnt:   Arc<AtomicU64>,
) {
    while !stop.load(Relaxed) {
        let sid = cur_sid.load(Relaxed);
        let cid = cur_cid.load(Relaxed);
        if sid == 0 || cid == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let addr   = cur_addr.read().unwrap().clone();
        let client = connect_retry(&addr).await;

        // subscribe returns impl Future + Send — safe in spawn.
        let sub = client.subscribe(sid, cid, b"").await;
        let mut sub = match sub {
            Ok(s)  => s,
            Err(_) => { tokio::time::sleep(Duration::from_millis(200)).await; continue; }
        };

        loop {
            match tokio::time::timeout(Duration::from_millis(400), sub.recv()).await {
                Ok(Some(msg)) => {
                    recv_seqs.lock().unwrap().insert(msg.seq);
                    recv_total.fetch_add(1, Relaxed);
                    msg.ack();
                    if force_reconnect.swap(false, Relaxed) {
                        reconnect_cnt.fetch_add(1, Relaxed);
                        break;
                    }
                }
                Ok(None) => {
                    reconnect_cnt.fetch_add(1, Relaxed);
                    break;
                }
                Err(_timeout) => {
                    if stop.load(Relaxed) { return; }
                    if force_reconnect.swap(false, Relaxed) {
                        reconnect_cnt.fetch_add(1, Relaxed);
                        break;
                    }
                    // addr changed? reconnect.
                    let new_addr = cur_addr.read().unwrap().clone();
                    if new_addr != addr {
                        reconnect_cnt.fetch_add(1, Relaxed);
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ── Main (chaos events inline) ────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    prune_stale();
    let data_dir = make_data_dir();
    let _cleanup = Cleanup(data_dir.clone());

    println!();
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║                    Chaos bench                           ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("  producers={N_PRODUCERS}   rate≈{RATE} msg/s each   run={RUN_SECS}s");
    println!("  journal=Disk");
    println!("  chaos events:");
    println!("    t= 4s — server kill");
    println!("    t= 5s — server restart (new port)");
    println!("    t= 8s — consumer force-disconnect + reconnect");
    println!("    t=12s — server kill");
    println!("    t=13s — server restart (new port)");
    println!("  publish_sync timeout={PUBLISH_TIMEOUT:?}  producer reconnects after 3 errors");
    println!();

    // ── Initial server ─────────────────────────────────────────────────────
    let addr0 = format!("127.0.0.1:{}", portpicker());
    let mut srv_tx = spawn_server(&addr0, &data_dir).await;
    println!("[chaos] server up on {addr0}");

    let (sid0, cid0) = ensure_stream_and_consumer(&addr0).await;
    println!("[chaos] stream_id={sid0}  consumer_id={cid0}");
    println!();

    // ── Shared state ───────────────────────────────────────────────────────
    let cur_addr  = Arc::new(RwLock::new(addr0.clone()));
    let cur_sid   = Arc::new(AtomicU32::new(sid0));
    let cur_cid   = Arc::new(AtomicU32::new(cid0));

    let prod_stop       = Arc::new(AtomicBool::new(false));
    let cons_stop       = Arc::new(AtomicBool::new(false));
    let force_reconnect = Arc::new(AtomicBool::new(false));

    let acked_seqs  = Arc::new(std::sync::Mutex::new(HashSet::<u64>::new()));
    let ok_cnt      = Arc::new(AtomicU64::new(0));
    let err_cnt     = Arc::new(AtomicU64::new(0));
    let recv_seqs   = Arc::new(std::sync::Mutex::new(HashSet::<u64>::new()));
    let recv_total  = Arc::new(AtomicU64::new(0));
    let reconnect_cnt = Arc::new(AtomicU64::new(0));

    // ── Consumer task ──────────────────────────────────────────────────────
    tokio::spawn(consumer_loop(
        Arc::clone(&cur_addr), Arc::clone(&cur_sid), Arc::clone(&cur_cid),
        Arc::clone(&cons_stop), Arc::clone(&force_reconnect),
        Arc::clone(&recv_seqs), Arc::clone(&recv_total), Arc::clone(&reconnect_cnt),
    ));
    tokio::time::sleep(Duration::from_millis(400)).await;

    // ── Producer tasks ─────────────────────────────────────────────────────
    let prod_handles: Vec<_> = (0..N_PRODUCERS).map(|id| {
        tokio::spawn(producer(
            id,
            Arc::clone(&cur_addr), Arc::clone(&cur_sid),
            Arc::clone(&prod_stop),
            Arc::clone(&acked_seqs), Arc::clone(&ok_cnt), Arc::clone(&err_cnt),
        ))
    }).collect();

    // ── Main ticker + inline chaos events ─────────────────────────────────
    let run_start = Instant::now();

    for t in 1u64..=RUN_SECS {
        tokio::time::sleep(Duration::from_secs(1)).await;

        // ── Chaos: kill server at t=4 ──────────────────────────────────
        if t == 4 {
            println!("\n  [chaos] ⚡ t=4s: killing server ...");
            let _ = srv_tx.send(true);
            // cur_sid = 0 so producers pause immediately
            cur_sid.store(0, Relaxed);
            cur_cid.store(0, Relaxed);
        }

        // ── Chaos: restart server at t=5 ───────────────────────────────
        if t == 5 {
            let new_addr = format!("127.0.0.1:{}", portpicker());
            println!("  [chaos] ↩  t=5s: restarting on {new_addr} ...");
            srv_tx = spawn_server(&new_addr, &data_dir).await;
            let (s, c) = ensure_stream_and_consumer(&new_addr).await;
            *cur_addr.write().unwrap() = new_addr.clone();
            cur_sid.store(s, Relaxed);
            cur_cid.store(c, Relaxed);
            println!("  [chaos] ✓  t=5s: server up  sid={s} cid={c}  (consumer_reconnects: {})",
                reconnect_cnt.load(Relaxed));
        }

        // ── Chaos: consumer force-disconnect at t=8 ────────────────────
        if t == 8 {
            println!("\n  [chaos] ⚡ t=8s: force-disconnecting consumer ...");
            force_reconnect.store(true, Relaxed);
            // consumer_loop will detect on next timeout (~400ms)
        }

        // ── Chaos: kill server at t=12 ─────────────────────────────────
        if t == 12 {
            println!("\n  [chaos] ⚡ t=12s: killing server ...");
            let _ = srv_tx.send(true);
            cur_sid.store(0, Relaxed);
            cur_cid.store(0, Relaxed);
        }

        // ── Chaos: restart server at t=13 ──────────────────────────────
        if t == 13 {
            let new_addr = format!("127.0.0.1:{}", portpicker());
            println!("  [chaos] ↩  t=13s: restarting on {new_addr} ...");
            srv_tx = spawn_server(&new_addr, &data_dir).await;
            let (s, c) = ensure_stream_and_consumer(&new_addr).await;
            *cur_addr.write().unwrap() = new_addr.clone();
            cur_sid.store(s, Relaxed);
            cur_cid.store(c, Relaxed);
            println!("  [chaos] ✓  t=13s: server up  sid={s} cid={c}  (consumer_reconnects: {})",
                reconnect_cnt.load(Relaxed));
        }

        let pub_n  = ok_cnt.load(Relaxed);
        let err_n  = err_cnt.load(Relaxed);
        let recv_n = recv_total.load(Relaxed);
        let uniq_n = recv_seqs.lock().unwrap().len();
        let rc_n   = reconnect_cnt.load(Relaxed);
        println!(
            "  [t={t:>2}s] published={pub_n:>5}  received={recv_n:>5} (uniq={uniq_n:>5})  \
             errors={err_n:>4}  consumer_reconnects={rc_n}  rss={:.1}MB",
            rss_mb()
        );
    }
    drop(srv_tx);

    // ── Stop producers ─────────────────────────────────────────────────────
    prod_stop.store(true, Relaxed);
    for h in prod_handles { let _ = h.await; }
    let pub_total = ok_cnt.load(Relaxed);
    let err_total = err_cnt.load(Relaxed);
    println!();
    println!("  producers stopped: {pub_total} acked  {err_total} errors  elapsed={:.2?}",
        run_start.elapsed());

    // ── Drain consumer ─────────────────────────────────────────────────────
    println!("  draining consumer (target={pub_total} unique seqs) ...");
    let drain_start    = Instant::now();
    let drain_deadline = drain_start + Duration::from_secs(30);
    let mut last_uniq  = recv_seqs.lock().unwrap().len();
    let mut stall_at   = Instant::now();

    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let uniq = recv_seqs.lock().unwrap().len();

        if uniq >= pub_total as usize {
            println!("  drain complete in {:.2?}", drain_start.elapsed());
            break;
        }
        if Instant::now() >= drain_deadline {
            println!("  WARN drain deadline — unique={uniq}/{pub_total}");
            break;
        }
        if uniq != last_uniq {
            last_uniq = uniq;
            stall_at  = Instant::now();
        } else if Instant::now().duration_since(stall_at) > Duration::from_secs(8) {
            println!("  WARN drain stalled 8s — unique={uniq}/{pub_total}");
            break;
        }
    }

    cons_stop.store(true, Relaxed);
    tokio::time::sleep(Duration::from_millis(600)).await;

    // ── Results ────────────────────────────────────────────────────────────
    let acked    = acked_seqs.lock().unwrap().clone();
    let recvd    = recv_seqs.lock().unwrap().clone();
    let recv_tot = recv_total.load(Relaxed);
    let uniq_tot = recvd.len() as u64;
    let dups     = recv_tot.saturating_sub(uniq_tot);
    let rc_tot   = reconnect_cnt.load(Relaxed);

    let missing_seqs: Vec<u64> = acked.difference(&recvd).copied().take(20).collect();
    let missing_cnt  = acked.difference(&recvd).count();

    println!();
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║                      Results                             ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("  published (acked)     : {pub_total}");
    println!("  errors (transient)    : {err_total}  (during server-down windows)");
    println!("  received (total)      : {recv_tot}");
    println!("  received (unique)     : {uniq_tot}");
    println!("  duplicates            : {dups}  (redelivery — expected)");
    println!("  consumer reconnects   : {rc_tot}  (expected ≥3: 2 kills + 1 force)");
    println!("  total elapsed         : {:.2?}", run_start.elapsed());
    println!();

    if missing_cnt == 0 {
        println!("  LOSS CHECK : ✓  PASS — all {pub_total} acked seqs received");
    } else {
        println!("  LOSS CHECK : ✗  FAIL — {missing_cnt} seqs missing");
        println!("               first 20: {missing_seqs:?}");
    }
    println!();

    assert!(pub_total > 0, "no messages published — check server restart logic");
    assert!(rc_tot >= 3, "expected ≥3 consumer reconnects (2 kills + 1 force), got {rc_tot}");
    assert_eq!(
        missing_cnt, 0,
        "LOSS: {missing_cnt} acked seqs never received.\nFirst missing: {missing_seqs:?}"
    );

    println!("  RESULT: OK — {pub_total} msgs, zero loss, {rc_tot} consumer reconnects survived");
    println!();
}
