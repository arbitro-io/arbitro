//! Workflow worker process — connects to broker, registers workflow,
//! processes steps, logs completions to a shared file.
//!
//! Usage: wf_worker <broker_addr> <worker_id> <log_dir>
//!
//! Each step appends a line to `<log_dir>/steps.log`:
//!   <worker_id>:<instance_id>:<step_index>
//!
//! The worker runs until killed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arbitro_client_tokio::workflow::StepResult;
use arbitro_client_tokio::{Client, ClientConfig};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: wf_worker <broker_addr> <worker_id> <log_dir>");
        std::process::exit(1);
    }
    let addr = &args[1];
    let worker_id: u32 = args[2].parse().unwrap();
    let log_dir = &args[3];

    let log_path = format!("{log_dir}/steps.log");

    // Connect to broker with retries
    let client = loop {
        match Client::connect(ClientConfig {
            addr: addr.to_string(),
            ..ClientConfig::default()
        })
        .await
        {
            Ok(c) => break c,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };

    let running = Arc::new(AtomicBool::new(true));
    let log_path_arc = Arc::new(log_path);

    // Register a 3-step workflow
    eprintln!("worker {worker_id} connected, registering workflow...");
    let wf = client
        .workflow(b"multiproc")
        .trigger(b"jobs.start")
        .ack_wait_ms(3_000) // 3s timeout — if worker dies, redelivers
        .max_inflight(5)
        .max_retries(3)
        .step(b"step-0", {
            let log = Arc::clone(&log_path_arc);
            let wid = worker_id;
            move |ctx| {
                let log = Arc::clone(&log);
                async move {
                    // Simulate work
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    // Log step execution
                    let line = format!("{wid}:{}:0\n", ctx.instance_id);
                    append_log(&log, &line);
                    let mut new_ctx = ctx.context.clone();
                    new_ctx.extend_from_slice(b"|s0");
                    Ok(StepResult { context: new_ctx })
                }
            }
        })
        .step(b"step-1", {
            let log = Arc::clone(&log_path_arc);
            let wid = worker_id;
            move |ctx| {
                let log = Arc::clone(&log);
                async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let line = format!("{wid}:{}:1\n", ctx.instance_id);
                    append_log(&log, &line);
                    let mut new_ctx = ctx.context.clone();
                    new_ctx.extend_from_slice(b"|s1");
                    Ok(StepResult { context: new_ctx })
                }
            }
        })
        .step(b"step-2", {
            let log = Arc::clone(&log_path_arc);
            let wid = worker_id;
            move |ctx| {
                let log = Arc::clone(&log);
                async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let line = format!("{wid}:{}:2\n", ctx.instance_id);
                    append_log(&log, &line);
                    Ok(StepResult {
                        context: ctx.context,
                    })
                }
            }
        })
        .start()
        .await
        .expect("workflow start failed");

    eprintln!("worker {worker_id} ready");

    // Run until killed
    tokio::signal::ctrl_c().await.ok();
    running.store(false, Ordering::Relaxed);
    wf.stop();
}

fn append_log(path: &str, line: &str) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open log");
    f.write_all(line.as_bytes()).expect("write log");
}
