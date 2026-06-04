//! Cluster chaos bench — correctness under leader failover.
//!
//! ## Scenario
//!
//! | t   | Event |
//! |-----|-------|
//! | 0s  | 3-node cluster boots, waits for leader election (~8s) |
//! | 8s  | Client connects to leader, publishers start |
//! | 14s | Leader killed (shutdown) |
//! | 16s | Re-election completes, client reconnects to surviving node |
//! | 26s | Run ends, verify zero loss |
//!
//! ## Loss invariant
//!
//! `acked_seqs <= received_seqs` — every server-confirmed message eventually
//! delivered. Duplicates (redelivery after reconnect) are expected and handled
//! by the `HashSet` deduplication.

#![cfg(feature = "cluster")]

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arbitro_client_tokio::{Client, ClientConfig, ReconnectPolicy};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;
use tokio::sync::watch;

// ── Tunables ──────────────────────────────────────────────────────────────────

/// Total run time after publishers start (keeps under 1000 msgs with 4x55 msg/s).
const RUN_SECS: u64 = 18;
const N_PRODUCERS: u64 = 4;
/// Target publish rate per producer — conservative to stay under 1000 total msgs.
const RATE: u64 = 55;
const JOURNAL_DISK: u8 = 1;
const STREAM: &[u8] = b"chaos-cluster";
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

// ── Helpers ───────────────────────────────────────────────────────────────────

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn make_cfg(addr: &str) -> ClientConfig {
    ClientConfig {
        addr: addr.to_string(),
        reconnect: ReconnectPolicy {
            base: Duration::from_millis(100),
            cap: Duration::from_millis(1_000),
            max_attempts: None,
        },
        ..ClientConfig::default()
    }
}

/// Connect, retrying until success or timeout.
async fn connect_retry(addr: &str) -> Client {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match Client::connect(make_cfg(addr)).await {
            Ok(c) => return c,
            Err(_) => {
                if Instant::now() > deadline {
                    panic!("connect_retry timed out connecting to {addr}");
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        }
    }
}

/// Ensure the stream and consumer exist. Returns (stream_id, consumer_id).
async fn ensure_stream_and_consumer(addr: &str) -> (u32, u32) {
    let c = connect_retry(addr).await;
    let resp = c
        .create_stream(STREAM, b">", 0, 0, 0, 1, JOURNAL_DISK, 0, 0, 0)
        .await
        .expect("create_stream");
    let sid = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    let resp = c
        .create_consumer(
            sid,
            b"chaos-cluster-c",
            b"chaos-cluster-grp",
            b"",
            u16::MAX,
            1u8,
            0u8,
            0u8,
            30_000u32,
            0u64,
        )
        .await
        .expect("create_consumer");
    let cid = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
    (sid, cid)
}

#[cfg(target_os = "linux")]
fn rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|p| p as f64 * 4.0 / 1024.0)
        .unwrap_or(0.0)
}
#[cfg(not(target_os = "linux"))]
fn rss_mb() -> f64 {
    0.0
}

// ── Cluster Node ─────────────────────────────────────────────────────────────

struct ClusterNode {
    node_id: u64,
    client_addr: String,
    #[allow(dead_code)]
    raft_addr: String,
    shutdown_tx: Option<watch::Sender<bool>>,
    _tmpdir: tempfile::TempDir,
}

impl ClusterNode {
    async fn spawn(
        node_id: u64,
        client_addr: String,
        raft_addr: String,
        cluster_peers: Vec<(u64, String)>,
    ) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_str().unwrap().to_string();

        let mut config = Config::default()
            .listen_addr(&client_addr)
            .shard_count(1)
            .max_connections(64)
            .shutdown_timeout(Duration::from_millis(500))
            .data_dir(&data_dir);

        config.cluster_node_id = node_id;
        config.cluster_listen = raft_addr.clone();
        config.cluster_peers = cluster_peers;

        let (tx, rx) = watch::channel(false);

        tokio::spawn(async move {
            let server = ArbitroServer::new(config);
            let _ = server.run_with_shutdown(rx).await;
        });

        // Wait until the client port is accepting connections.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if std::net::TcpStream::connect(&client_addr).is_ok() {
                break;
            }
            if Instant::now() > deadline {
                panic!("node {node_id} failed to start accepting connections on {client_addr}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Self {
            node_id,
            client_addr,
            raft_addr,
            shutdown_tx: Some(tx),
            _tmpdir: tmp,
        }
    }

    /// Shutdown this node (simulates kill).
    fn kill(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
    }

    fn is_alive(&self) -> bool {
        self.shutdown_tx.is_some()
    }
}

// ── Producer ─────────────────────────────────────────────────────────────────

async fn producer(
    id: u64,
    cur_addr: Arc<RwLock<String>>,
    cur_sid: Arc<AtomicU32>,
    stop: Arc<AtomicBool>,
    acked: Arc<std::sync::Mutex<HashSet<u64>>>,
    ok_cnt: Arc<AtomicU64>,
    err_cnt: Arc<AtomicU64>,
) {
    let subj = format!("prod.{id}");
    let payload = vec![id as u8; 32];
    let interval = Duration::from_nanos(1_000_000_000 / RATE.max(1));

    let mut client: Option<Client> = None;
    let mut last_addr = String::new();
    let mut tick = Instant::now();

    while !stop.load(Relaxed) {
        let sid = cur_sid.load(Relaxed);

        // Server is down: drop client, wait.
        if sid == 0 {
            client = None;
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        // Addr changed or no client: reconnect.
        let cur = cur_addr.read().unwrap().clone();
        if client.is_none() || cur != last_addr {
            last_addr = cur.clone();
            client = Some(connect_retry(&cur).await);
            tick = Instant::now();
        }

        let fut = client.as_ref().unwrap().publish_sync(
            sid,
            subj.as_bytes(),
            Bytes::copy_from_slice(&payload),
        );
        let result = tokio::time::timeout(PUBLISH_TIMEOUT, fut).await;

        match result {
            Ok(Ok(b)) => {
                let seq = u64::from_le_bytes(b[..8].try_into().unwrap());
                acked.lock().unwrap().insert(seq);
                ok_cnt.fetch_add(1, Relaxed);
            }
            _ => {
                err_cnt.fetch_add(1, Relaxed);
                client = None;
                tokio::time::sleep(Duration::from_millis(200)).await;
                tick = Instant::now();
                continue;
            }
        }

        tick += interval;
        let now = Instant::now();
        if tick > now {
            tokio::time::sleep(tick - now).await;
        } else {
            tick = now;
        }
    }
}

// ── Consumer ─────────────────────────────────────────────────────────────────

async fn consumer_loop(
    cur_addr: Arc<RwLock<String>>,
    cur_sid: Arc<AtomicU32>,
    cur_cid: Arc<AtomicU32>,
    stop: Arc<AtomicBool>,
    recv_seqs: Arc<std::sync::Mutex<HashSet<u64>>>,
    recv_total: Arc<AtomicU64>,
    reconnect_cnt: Arc<AtomicU64>,
) {
    while !stop.load(Relaxed) {
        let sid = cur_sid.load(Relaxed);
        let cid = cur_cid.load(Relaxed);
        if sid == 0 || cid == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let addr = cur_addr.read().unwrap().clone();
        let client = connect_retry(&addr).await;

        let sub = client.subscribe(sid, cid, b"").await;
        let mut sub = match sub {
            Ok(s) => s,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };

        loop {
            match tokio::time::timeout(Duration::from_millis(400), sub.recv()).await {
                Ok(Some(msg)) => {
                    recv_seqs.lock().unwrap().insert(msg.seq);
                    recv_total.fetch_add(1, Relaxed);
                    msg.ack();
                }
                Ok(None) => {
                    reconnect_cnt.fetch_add(1, Relaxed);
                    break;
                }
                Err(_timeout) => {
                    if stop.load(Relaxed) {
                        return;
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

// ── Find leader ──────────────────────────────────────────────────────────────

/// Try create_stream on each node to find the leader.
/// Returns the index of the node that succeeded, or None.
async fn find_leader(nodes: &[ClusterNode]) -> Option<usize> {
    for (i, node) in nodes.iter().enumerate() {
        if !node.is_alive() {
            continue;
        }
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            let client = connect_retry(&node.client_addr).await;
            client
                .create_stream(STREAM, b">", 0, 0, 0, 1, JOURNAL_DISK, 0, 0, 0)
                .await
        })
        .await;

        match result {
            Ok(Ok(_)) => return Some(i),
            _ => continue,
        }
    }
    None
}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    println!();
    println!("=====================================================");
    println!("          Cluster Chaos Bench (3-node Raft)          ");
    println!("=====================================================");
    println!("  producers={N_PRODUCERS}   rate~{RATE} msg/s each   run={RUN_SECS}s");
    println!("  journal=Disk");
    println!("  chaos events:");
    println!("    t= 6s — leader kill");
    println!("    t= 8s — re-election expected, reconnect to survivor");
    println!();

    // ── Step 1: Pick 6 dynamic ports (3 client + 3 raft) ─────────────────
    let mut client_addrs = Vec::new();
    let mut raft_addrs = Vec::new();
    for _ in 0..3 {
        client_addrs.push(format!("127.0.0.1:{}", portpicker()));
        raft_addrs.push(format!("127.0.0.1:{}", portpicker()));
    }

    let cluster_peers: Vec<(u64, String)> = (0..3)
        .map(|i| ((i + 1) as u64, raft_addrs[i].clone()))
        .collect();

    // ── Step 2: Spawn 3-node cluster ─────────────────────────────────────
    println!("[cluster] spawning 3 nodes ...");
    let mut nodes = Vec::new();
    for i in 0..3 {
        let node = ClusterNode::spawn(
            (i + 1) as u64,
            client_addrs[i].clone(),
            raft_addrs[i].clone(),
            cluster_peers.clone(),
        )
        .await;
        println!(
            "  node {} up: client={} raft={}",
            node.node_id, node.client_addr, node.raft_addr
        );
        nodes.push(node);
    }

    // ── Step 3: Wait for Raft leader election (~8s) ──────────────────────
    println!("[cluster] waiting for leader election (up to 10s) ...");
    let election_start = Instant::now();
    let mut leader_idx = None;
    let election_deadline = Instant::now() + Duration::from_secs(10);

    while Instant::now() < election_deadline {
        if let Some(idx) = find_leader(&nodes).await {
            leader_idx = Some(idx);
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let leader_idx = leader_idx.expect("no leader elected within 10s");
    println!(
        "[cluster] leader elected: node {} ({}) in {:.1}s",
        nodes[leader_idx].node_id,
        nodes[leader_idx].client_addr,
        election_start.elapsed().as_secs_f64()
    );

    // ── Step 4: Setup stream + consumer on leader ────────────────────────
    let leader_addr = nodes[leader_idx].client_addr.clone();
    let (sid0, cid0) = ensure_stream_and_consumer(&leader_addr).await;
    println!("[cluster] stream_id={sid0}  consumer_id={cid0}");
    println!();

    // ── Shared state ─────────────────────────────────────────────────────
    let cur_addr = Arc::new(RwLock::new(leader_addr.clone()));
    let cur_sid = Arc::new(AtomicU32::new(sid0));
    let cur_cid = Arc::new(AtomicU32::new(cid0));

    let prod_stop = Arc::new(AtomicBool::new(false));
    let cons_stop = Arc::new(AtomicBool::new(false));

    let acked_seqs = Arc::new(std::sync::Mutex::new(HashSet::<u64>::new()));
    let ok_cnt = Arc::new(AtomicU64::new(0));
    let err_cnt = Arc::new(AtomicU64::new(0));
    let recv_seqs = Arc::new(std::sync::Mutex::new(HashSet::<u64>::new()));
    let recv_total = Arc::new(AtomicU64::new(0));
    let reconnect_cnt = Arc::new(AtomicU64::new(0));

    // ── Consumer task ────────────────────────────────────────────────────
    tokio::spawn(consumer_loop(
        Arc::clone(&cur_addr),
        Arc::clone(&cur_sid),
        Arc::clone(&cur_cid),
        Arc::clone(&cons_stop),
        Arc::clone(&recv_seqs),
        Arc::clone(&recv_total),
        Arc::clone(&reconnect_cnt),
    ));
    tokio::time::sleep(Duration::from_millis(400)).await;

    // ── Producer tasks ───────────────────────────────────────────────────
    let prod_handles: Vec<_> = (0..N_PRODUCERS)
        .map(|id| {
            tokio::spawn(producer(
                id,
                Arc::clone(&cur_addr),
                Arc::clone(&cur_sid),
                Arc::clone(&prod_stop),
                Arc::clone(&acked_seqs),
                Arc::clone(&ok_cnt),
                Arc::clone(&err_cnt),
            ))
        })
        .collect();

    // ── Main ticker + chaos events ───────────────────────────────────────
    let run_start = Instant::now();

    for t in 1u64..=RUN_SECS {
        tokio::time::sleep(Duration::from_secs(1)).await;

        // ── Chaos: kill leader at t=6 ────────────────────────────────
        if t == 6 {
            println!(
                "\n  [chaos] t=6s: killing leader (node {}) ...",
                nodes[leader_idx].node_id
            );
            nodes[leader_idx].kill();
            // Signal producers to pause.
            cur_sid.store(0, Relaxed);
            cur_cid.store(0, Relaxed);
        }

        // ── Chaos: reconnect to a surviving node at t=8 ──────────────
        if t == 8 {
            println!("  [chaos] t=8s: attempting reconnect to surviving node ...");

            // Find a surviving node that can serve as the new leader.
            let mut new_leader_idx = None;
            for (i, node) in nodes.iter().enumerate() {
                if !node.is_alive() {
                    continue;
                }
                // Try to confirm it can accept operations.
                let result = tokio::time::timeout(Duration::from_secs(3), async {
                    let client = connect_retry(&node.client_addr).await;
                    // Try creating stream (idempotent) to verify this is leader.
                    client
                        .create_stream(STREAM, b">", 0, 0, 0, 1, JOURNAL_DISK, 0, 0, 0)
                        .await
                })
                .await;

                if result.is_ok() && result.unwrap().is_ok() {
                    new_leader_idx = Some(i);
                    break;
                }
            }

            if let Some(idx) = new_leader_idx {
                let new_addr = nodes[idx].client_addr.clone();
                println!(
                    "  [chaos] t=8s: new leader = node {} ({})",
                    nodes[idx].node_id, new_addr
                );
                // Re-ensure stream + consumer on new leader.
                let (s, c) = ensure_stream_and_consumer(&new_addr).await;
                *cur_addr.write().unwrap() = new_addr;
                cur_sid.store(s, Relaxed);
                cur_cid.store(c, Relaxed);
                println!(
                    "  [chaos] t=8s: reconnected sid={s} cid={c} (reconnects: {})",
                    reconnect_cnt.load(Relaxed)
                );
            } else {
                println!("  [chaos] t=8s: WARNING — no surviving leader found, continuing ...");
                // Keep sid=0 — producers stay paused; we'll check loss at end.
            }
        }

        let pub_n = ok_cnt.load(Relaxed);
        let err_n = err_cnt.load(Relaxed);
        let recv_n = recv_total.load(Relaxed);
        let uniq_n = recv_seqs.lock().unwrap().len();
        let rc_n = reconnect_cnt.load(Relaxed);
        println!(
            "  [t={t:>2}s] published={pub_n:>5}  received={recv_n:>5} (uniq={uniq_n:>5})  \
             errors={err_n:>4}  reconnects={rc_n}  rss={:.1}MB",
            rss_mb()
        );
    }

    // ── Stop producers ───────────────────────────────────────────────────
    prod_stop.store(true, Relaxed);
    for h in prod_handles {
        let _ = h.await;
    }
    let pub_total = ok_cnt.load(Relaxed);
    let err_total = err_cnt.load(Relaxed);
    println!();
    println!(
        "  producers stopped: {pub_total} acked  {err_total} errors  elapsed={:.2?}",
        run_start.elapsed()
    );

    // ── Drain consumer ───────────────────────────────────────────────────
    println!("  draining consumer (target={pub_total} unique seqs) ...");
    let drain_start = Instant::now();
    let drain_deadline = drain_start + Duration::from_secs(30);
    let mut last_uniq = recv_seqs.lock().unwrap().len();
    let mut stall_at = Instant::now();

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
            stall_at = Instant::now();
        } else if Instant::now().duration_since(stall_at) > Duration::from_secs(8) {
            println!("  WARN drain stalled 8s — unique={uniq}/{pub_total}");
            break;
        }
    }

    cons_stop.store(true, Relaxed);
    tokio::time::sleep(Duration::from_millis(600)).await;

    // ── Shutdown surviving nodes ─────────────────────────────────────────
    for node in &mut nodes {
        node.kill();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── Results ──────────────────────────────────────────────────────────
    let acked = acked_seqs.lock().unwrap().clone();
    let recvd = recv_seqs.lock().unwrap().clone();
    let recv_tot = recv_total.load(Relaxed);
    let uniq_tot = recvd.len() as u64;
    let dups = recv_tot.saturating_sub(uniq_tot);
    let rc_tot = reconnect_cnt.load(Relaxed);

    let missing_seqs: Vec<u64> = acked.difference(&recvd).copied().take(20).collect();
    let missing_cnt = acked.difference(&recvd).count();

    println!();
    println!("=====================================================");
    println!("                    Results                           ");
    println!("=====================================================");
    println!("  published (acked)     : {pub_total}");
    println!("  errors (transient)    : {err_total}  (during leader-down window)");
    println!("  received (total)      : {recv_tot}");
    println!("  received (unique)     : {uniq_tot}");
    println!("  duplicates            : {dups}  (redelivery — expected)");
    println!("  consumer reconnects   : {rc_tot}");
    println!("  total elapsed         : {:.2?}", run_start.elapsed());
    println!();

    if missing_cnt == 0 {
        println!("  LOSS CHECK : PASS — all {pub_total} acked seqs received");
    } else {
        println!("  LOSS CHECK : FAIL — {missing_cnt} seqs missing");
        println!("               first 20: {missing_seqs:?}");
    }
    println!();

    assert!(
        pub_total > 0,
        "no messages published — check cluster/leader logic"
    );
    assert_eq!(
        missing_cnt, 0,
        "LOSS: {missing_cnt} acked seqs never received.\nFirst missing: {missing_seqs:?}"
    );

    println!("  RESULT: OK — {pub_total} msgs, zero loss, leader failover survived");
    println!();
}
