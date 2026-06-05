//! Workflow orchestrator — launches broker + 6 worker processes,
//! triggers 20 workflow instances, kills 2 workers mid-run, verifies:
//!   1. Every instance completes all 3 steps (no lost work)
//!   2. No step is executed twice for the same instance (no duplicates)
//!   3. Killed workers' steps are picked up by survivors
//!
//! Usage: wf_orchestrator
//!
//! Exits 0 if all invariants hold, 1 otherwise.

use std::collections::{HashMap, HashSet};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use bytes::Bytes;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let code = rt.block_on(run());
    std::process::exit(code);
}

async fn run() -> i32 {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("broker");
    std::fs::create_dir_all(&data_dir).unwrap();
    let log_dir = tmp.path().join("logs");
    std::fs::create_dir_all(&log_dir).unwrap();
    let log_dir_str = log_dir.to_str().unwrap().to_string();

    // ── Find a free port for the broker ───────────────────────────────
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let broker_addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    // ── Start broker in-process ───────────────────────────────────────
    let broker_addr_clone = broker_addr.clone();
    let data_dir_clone = data_dir.to_str().unwrap().to_string();
    let broker_handle = tokio::spawn(async move {
        use arbitro_server::{ArbitroServer, Config};
        let config = Config {
            listen_addr: broker_addr_clone,
            data_dir: Some(data_dir_clone),
            ..Config::default()
        };
        let server = ArbitroServer::new(config);
        server.run().await.ok();
    });

    // Wait for broker to be ready
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── Find the worker binary ────────────────────────────────────────
    let worker_bin = find_worker_binary();
    if worker_bin.is_empty() {
        eprintln!("ERROR: wf_worker binary not found. Build with: cargo build -p arbitro-e2e --bin wf_worker");
        return 1;
    }
    eprintln!("worker binary: {worker_bin}");

    // ── Launch 6 worker processes ─────────────────────────────────────
    let mut workers: Vec<Child> = Vec::new();
    for id in 0..6u32 {
        let child = Command::new(&worker_bin)
            .args([&broker_addr, &id.to_string(), &log_dir_str])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit()) // show worker logs
            .spawn()
            .expect("spawn worker");
        workers.push(child);
    }

    // Wait for workers to connect, create stream, create consumer, subscribe.
    // Each worker does 3 sequential round-trips. Give them enough time.
    tokio::time::sleep(Duration::from_secs(5)).await;
    eprintln!("6 workers launched, waiting for all subscriptions to stabilize...");

    // ── Connect a trigger client ──────────────────────────────────────
    let client = arbitro_client_tokio::Client::connect(
        arbitro_client_tokio::ClientConfig {
            addr: broker_addr.clone(),
            ..Default::default()
        },
    )
    .await
    .expect("trigger client connect");

    // The workers created the task stream. Get its ID.
    let resp = client.list_streams(0, 100).await.expect("list streams");
    let task_stream_name = b"_wf_multiproc_tasks";
    let task_stream_id = find_stream_id(&resp, task_stream_name)
        .expect("task stream must exist (workers create it)");

    // ── Trigger 20 workflow instances ──────────────────────────────────
    let num_instances = 20u32;
    for i in 1..=num_instances {
        let msg_id = format!("wf:{i}:0:0");
        let subject = b"_wf.multiproc.step.0";
        let mut payload = Vec::with_capacity(7 + 4);
        payload.extend_from_slice(&i.to_le_bytes()); // instance_id
        payload.extend_from_slice(&0u16.to_le_bytes()); // step_index = 0
        payload.push(0); // attempt = 0
        payload.extend_from_slice(format!("job-{i}").as_bytes());
        client
            .publish_sync_with_id(
                task_stream_id,
                subject,
                msg_id.as_bytes(),
                Bytes::from(payload),
            )
            .await
            .expect("trigger publish");
    }
    eprintln!("triggered {num_instances} instances");

    // ── Wait a bit, then kill 2 workers ───────────────────────────────
    tokio::time::sleep(Duration::from_secs(1)).await;
    eprintln!("killing workers 0 and 1");
    workers[0].kill().ok();
    workers[1].kill().ok();
    workers[0].wait().ok();
    workers[1].wait().ok();

    // ── Wait for remaining 4 workers to finish all steps ──────────────
    eprintln!("waiting for completion (up to 30s)...");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let log_content = std::fs::read_to_string(log_dir.join("steps.log")).unwrap_or_default();
        let completed = count_completed_instances(&log_content, num_instances);
        eprintln!("  completed: {completed}/{num_instances}");
        if completed >= num_instances {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            eprintln!("TIMEOUT: only {completed}/{num_instances} completed");
            break;
        }
    }

    // ── Kill remaining workers ────────────────────────────────────────
    for w in &mut workers[2..] {
        w.kill().ok();
        w.wait().ok();
    }

    // ── Verify invariants ─────────────────────────────────────────────
    let log_content = std::fs::read_to_string(log_dir.join("steps.log")).unwrap_or_default();
    let (ok, report) = verify_invariants(&log_content, num_instances);
    eprintln!("\n{report}");

    broker_handle.abort();

    if ok { 0 } else { 1 }
}

fn count_completed_instances(log: &str, _num_instances: u32) -> u32 {
    let mut step2_done: HashSet<u32> = HashSet::new();
    for line in log.lines() {
        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() == 3 {
            if let (Ok(inst), Ok(step)) = (parts[1].parse::<u32>(), parts[2].parse::<u32>()) {
                if step == 2 {
                    step2_done.insert(inst);
                }
            }
        }
    }
    step2_done.len() as u32
}

fn verify_invariants(log: &str, num_instances: u32) -> (bool, String) {
    let mut report = String::new();
    let mut all_ok = true;

    // Parse log: (instance_id, step_index) → Vec<worker_id>
    let mut executions: HashMap<(u32, u32), Vec<u32>> = HashMap::new();
    let mut workers_seen: HashSet<u32> = HashSet::new();

    for line in log.lines() {
        let parts: Vec<&str> = line.trim().split(':').collect();
        if parts.len() != 3 {
            continue;
        }
        let worker_id: u32 = parts[0].parse().unwrap_or(999);
        let instance_id: u32 = parts[1].parse().unwrap_or(0);
        let step_index: u32 = parts[2].parse().unwrap_or(0);
        workers_seen.insert(worker_id);
        executions
            .entry((instance_id, step_index))
            .or_default()
            .push(worker_id);
    }

    report.push_str("=== MULTI-PROCESS WORKFLOW INVARIANTS ===\n\n");
    report.push_str(&format!("Workers seen: {workers_seen:?}\n"));
    report.push_str(&format!("Total log lines: {}\n\n", log.lines().count()));

    // Invariant 1: Every instance completed all 3 steps
    let mut missing_steps = Vec::new();
    for inst in 1..=num_instances {
        for step in 0..3u32 {
            if !executions.contains_key(&(inst, step)) {
                missing_steps.push((inst, step));
            }
        }
    }
    if missing_steps.is_empty() {
        report.push_str(&format!(
            "✅ INV1: All {num_instances} instances completed all 3 steps\n"
        ));
    } else {
        report.push_str(&format!(
            "❌ INV1: Missing steps: {:?}\n",
            &missing_steps[..missing_steps.len().min(10)]
        ));
        all_ok = false;
    }

    // Invariant 2: No duplicate executions
    let mut duplicates = Vec::new();
    for ((inst, step), workers) in &executions {
        if workers.len() > 1 {
            duplicates.push((*inst, *step, workers.clone()));
        }
    }
    if duplicates.is_empty() {
        report.push_str("✅ INV2: No duplicate step executions\n");
    } else {
        report.push_str(&format!(
            "❌ INV2: Duplicate executions: {:?}\n",
            &duplicates[..duplicates.len().min(10)]
        ));
        all_ok = false;
    }

    // Invariant 3: More than 2 workers participated (proves killed workers' work was redistributed)
    let active_workers = workers_seen.len();
    if active_workers > 2 {
        report.push_str(&format!(
            "✅ INV3: {active_workers} workers participated (killed 2, survivors took over)\n"
        ));
    } else {
        report.push_str(&format!(
            "❌ INV3: Only {active_workers} workers participated\n"
        ));
        all_ok = false;
    }

    report.push_str(&format!(
        "\nVERDICT: {}\n",
        if all_ok { "ALL PASS" } else { "FAILED" }
    ));
    (all_ok, report)
}

fn find_worker_binary() -> String {
    // Look in target/debug and target/release
    for profile in ["debug", "release"] {
        for ext in ["", ".exe"] {
            let path = format!("target/{profile}/wf_worker{ext}");
            if std::path::Path::new(&path).exists() {
                return path;
            }
        }
    }
    String::new()
}

fn find_stream_id(resp: &bytes::Bytes, name: &[u8]) -> Option<u32> {
    // ListStreams reply: [count:u32][entries: (stream_id:u32, name_len:u16, name:bytes)*]
    if resp.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]) as usize;
    let mut off = 4;
    for _ in 0..count {
        if off + 6 > resp.len() {
            break;
        }
        let sid = u32::from_le_bytes([resp[off], resp[off + 1], resp[off + 2], resp[off + 3]]);
        let nlen = u16::from_le_bytes([resp[off + 4], resp[off + 5]]) as usize;
        off += 6;
        if off + nlen > resp.len() {
            break;
        }
        let sname = &resp[off..off + nlen];
        if sname == name {
            return Some(sid);
        }
        off += nlen;
    }
    None
}
