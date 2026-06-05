mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_client_tokio::workflow::{StepContext, StepResult};
use arbitro_client_tokio::ClientError;
use arbitro_proto::error::ErrorCode;
use bytes::Bytes;

#[tokio::test(flavor = "multi_thread")]
async fn workflow_3_steps_via_streams() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let completed = Arc::new(AtomicBool::new(false));
    let completed_flag = completed.clone();

    let handle = client
        .workflow(b"e2e-pipeline")
        .trigger(b"pipeline.start")
        .step(b"validate", |ctx: StepContext| async move {
            let mut out = ctx.context.clone();
            out.extend_from_slice(b"|validated");
            Ok(StepResult { context: out })
        })
        .step(b"process", |ctx: StepContext| async move {
            let mut out = ctx.context.clone();
            out.extend_from_slice(b"|processed");
            Ok(StepResult { context: out })
        })
        .step(b"complete", move |ctx: StepContext| {
            let flag = completed_flag.clone();
            async move {
                let mut out = ctx.context.clone();
                out.extend_from_slice(b"|completed");
                flag.store(true, Ordering::Release);
                Ok(StepResult { context: out })
            }
        })
        .start()
        .await
        .expect("workflow start");

    // Trigger a workflow instance.
    handle
        .trigger(&client, b"initial")
        .await
        .expect("trigger workflow");

    // Wait for the last step to complete (timeout after 10s).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !completed.load(Ordering::Acquire) {
        if tokio::time::Instant::now() >= deadline {
            panic!("workflow did not complete within 10 seconds");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        completed.load(Ordering::Acquire),
        "all 3 steps must have executed"
    );

    handle.stop();
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 1: Step retry on nack
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn workflow_step_retry_on_nack() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let attempts = Arc::new(AtomicU32::new(0));
    let completed = Arc::new(AtomicBool::new(false));

    let attempts_clone = attempts.clone();
    let completed_flag = completed.clone();

    let handle = client
        .workflow(b"retry-test")
        .trigger(b"retry.start")
        .ack_wait_ms(2000)
        .step(b"maybe-fail", move |_ctx: StepContext| {
            let att = attempts_clone.clone();
            let flag = completed_flag.clone();
            async move {
                let n = att.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 {
                    // First attempt: fail => nack => redelivery
                    Err("transient failure".to_string())
                } else {
                    // Second attempt: succeed
                    flag.store(true, Ordering::Release);
                    Ok(StepResult {
                        context: b"done".to_vec(),
                    })
                }
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle
        .trigger(&client, b"initial")
        .await
        .expect("trigger workflow");

    tokio::time::timeout(Duration::from_secs(10), async {
        while !completed.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("workflow did not complete within 10 seconds");

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "step must have been attempted exactly twice (1 fail + 1 success)"
    );

    handle.stop();
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 2: Two concurrent workflow instances
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn workflow_two_concurrent_instances() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let completed_a = Arc::new(AtomicBool::new(false));
    let completed_b = Arc::new(AtomicBool::new(false));

    let flag_a = completed_a.clone();
    let flag_b = completed_b.clone();

    let handle = client
        .workflow(b"concurrent")
        .trigger(b"concurrent.start")
        .step(b"enrich", |ctx: StepContext| async move {
            let mut out = ctx.context.clone();
            out.extend_from_slice(b"|enriched");
            Ok(StepResult { context: out })
        })
        .step(b"finalize", move |ctx: StepContext| {
            let fa = flag_a.clone();
            let fb = flag_b.clone();
            async move {
                let mut out = ctx.context.clone();
                out.extend_from_slice(b"|finalized");
                if ctx.context.starts_with(b"instance-A") {
                    fa.store(true, Ordering::Release);
                } else if ctx.context.starts_with(b"instance-B") {
                    fb.store(true, Ordering::Release);
                }
                Ok(StepResult { context: out })
            }
        })
        .start()
        .await
        .expect("workflow start");

    // Trigger two instances with different contexts.
    handle
        .trigger(&client, b"instance-A")
        .await
        .expect("trigger A");
    handle
        .trigger(&client, b"instance-B")
        .await
        .expect("trigger B");

    tokio::time::timeout(Duration::from_secs(10), async {
        while !completed_a.load(Ordering::Acquire) || !completed_b.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("both instances did not complete within 10 seconds");

    assert!(
        completed_a.load(Ordering::Acquire),
        "instance A must complete"
    );
    assert!(
        completed_b.load(Ordering::Acquire),
        "instance B must complete"
    );

    handle.stop();
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 3: Idempotent — no duplicate steps
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn workflow_idempotent_no_duplicate_steps() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let step0_count = Arc::new(AtomicU32::new(0));
    let completed = Arc::new(AtomicBool::new(false));

    let count_clone = step0_count.clone();
    let completed_flag = completed.clone();

    let handle = client
        .workflow(b"idem-test")
        .trigger(b"idem.start")
        .step(b"counted", move |ctx: StepContext| {
            let cnt = count_clone.clone();
            async move {
                cnt.fetch_add(1, Ordering::SeqCst);
                Ok(StepResult {
                    context: ctx.context.clone(),
                })
            }
        })
        .step(b"finish", move |ctx: StepContext| {
            let flag = completed_flag.clone();
            async move {
                flag.store(true, Ordering::Release);
                Ok(StepResult {
                    context: ctx.context.clone(),
                })
            }
        })
        .start()
        .await
        .expect("workflow start");

    let instance_id = handle
        .trigger(&client, b"payload")
        .await
        .expect("trigger workflow");

    // Wait for step 1 (finish) to complete.
    tokio::time::timeout(Duration::from_secs(10), async {
        while !completed.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("workflow did not complete within 10 seconds");

    // Try to re-publish step 0 with the same msg_id.
    let task_stream_id = handle.task_stream_id();
    let msg_id = format!("wf:{instance_id}:0:0");
    let subject = b"_wf.idem-test.step.0";

    // Encode: [instance_id:4 LE][step_index:2 LE][attempt:1][context...]
    let mut payload = Vec::new();
    payload.extend_from_slice(&instance_id.to_le_bytes());
    payload.extend_from_slice(&0u16.to_le_bytes());
    payload.push(0u8);
    payload.extend_from_slice(b"payload");

    let result = client
        .publish_sync_with_id(
            task_stream_id,
            subject,
            msg_id.as_bytes(),
            Bytes::from(payload),
        )
        .await;

    assert!(
        matches!(
            result,
            Err(ClientError::Broker {
                code: ErrorCode::IdempotencyDuplicate
            })
        ),
        "duplicate publish must be rejected, got {:?}",
        result,
    );

    assert_eq!(
        step0_count.load(Ordering::SeqCst),
        1,
        "step 0 handler must have run exactly once"
    );

    handle.stop();
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 4: Worker disconnect redelivers to a new worker
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn workflow_worker_disconnect_redelivers() {
    let mut server = TestServerBuilder::new().spawn().await;

    // Client 1: registers the workflow and triggers it, then disconnects.
    let client1 = server.connect().await;

    let handle1 = client1
        .workflow(b"failover")
        .trigger(b"failover.start")
        .ack_wait_ms(1000)
        .step(b"slow-step", |_ctx: StepContext| async move {
            // Simulate slow work — this won't finish because client1 closes.
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(StepResult {
                context: b"never".to_vec(),
            })
        })
        .start()
        .await
        .expect("workflow start on client1");

    handle1
        .trigger(&client1, b"work")
        .await
        .expect("trigger workflow");

    // Give the consumer a moment to receive the message.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Close client1 — the in-flight message should NOT be acked.
    handle1.stop();
    client1.close();

    // Wait for the ack_wait timeout to expire so the server redelivers.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Client 2: connect, register the same workflow, pick up the redelivered task.
    let client2 = server.connect().await;
    let completed = Arc::new(AtomicBool::new(false));
    let completed_flag = completed.clone();

    let handle2 = client2
        .workflow(b"failover")
        .trigger(b"failover.start")
        .ack_wait_ms(1000)
        .step(b"slow-step", move |ctx: StepContext| {
            let flag = completed_flag.clone();
            async move {
                flag.store(true, Ordering::Release);
                Ok(StepResult {
                    context: ctx.context.clone(),
                })
            }
        })
        .start()
        .await
        .expect("workflow start on client2");

    tokio::time::timeout(Duration::from_secs(10), async {
        while !completed.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("workflow did not complete on client2 within 10 seconds");

    assert!(
        completed.load(Ordering::Acquire),
        "client2 must have processed the redelivered message"
    );

    handle2.stop();
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 5: Step timeout redelivers (ack_wait expiry)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn workflow_step_timeout_redelivers() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let attempts = Arc::new(AtomicU32::new(0));
    let completed = Arc::new(AtomicBool::new(false));

    let attempts_clone = attempts.clone();
    let completed_flag = completed.clone();

    let handle = client
        .workflow(b"timeout-test")
        .trigger(b"timeout.start")
        .ack_wait_ms(1000) // 1 second timeout
        .step(b"maybe-slow", move |_ctx: StepContext| {
            let att = attempts_clone.clone();
            let flag = completed_flag.clone();
            async move {
                let n = att.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 {
                    // First attempt: sleep longer than ack_wait (5s > 1s).
                    // The broker will auto-nack after 1s and redeliver.
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    // Even if we return Ok here, the message was already
                    // redelivered — the ack will be a no-op or ignored.
                    Ok(StepResult {
                        context: b"late".to_vec(),
                    })
                } else {
                    // Subsequent attempt: succeed immediately.
                    flag.store(true, Ordering::Release);
                    Ok(StepResult {
                        context: b"done".to_vec(),
                    })
                }
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle
        .trigger(&client, b"payload")
        .await
        .expect("trigger workflow");

    tokio::time::timeout(Duration::from_secs(10), async {
        while !completed.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("workflow did not complete within 10 seconds");

    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "step must have been attempted at least twice (first timed out, redelivered), got {}",
        attempts.load(Ordering::SeqCst),
    );

    handle.stop();
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 6: Workflow survives broker restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn workflow_survives_broker_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    // Reserve a fixed address so the second boot can bind the same port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let step0_started = Arc::new(AtomicBool::new(false));
    let step1_completed = Arc::new(AtomicBool::new(false));

    // ── First boot ───────────────────────────────────────────────────────
    {
        let mut server = TestServerBuilder::new()
            .data_dir(dir_str)
            .spawn_on(&addr)
            .await;
        let client = server.connect().await;

        // Create a user-facing stream (to prove metadata survives).
        client
            .create_stream(b"user_data", b">", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .expect("create user_data stream");

        let s0_flag = step0_started.clone();
        let s1_flag = step1_completed.clone();

        let handle = client
            .workflow(b"survive-restart")
            .trigger(b"survive.start")
            .ack_wait_ms(5000)
            .step(b"first", move |ctx: StepContext| {
                let flag = s0_flag.clone();
                async move {
                    flag.store(true, Ordering::Release);
                    let mut out = ctx.context.clone();
                    out.extend_from_slice(b"|step0");
                    Ok(StepResult { context: out })
                }
            })
            .step(b"second", move |ctx: StepContext| {
                let flag = s1_flag.clone();
                async move {
                    flag.store(true, Ordering::Release);
                    let mut out = ctx.context.clone();
                    out.extend_from_slice(b"|step1");
                    Ok(StepResult { context: out })
                }
            })
            .start()
            .await
            .expect("workflow start (boot 1)");

        // Trigger an instance and wait for both steps to complete.
        handle
            .trigger(&client, b"boot1")
            .await
            .expect("trigger workflow");

        tokio::time::timeout(Duration::from_secs(10), async {
            while !step1_completed.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("first-boot workflow did not complete within 10 seconds");

        assert!(
            step0_started.load(Ordering::Acquire),
            "step 0 must have run on first boot"
        );
        assert!(
            step1_completed.load(Ordering::Acquire),
            "step 1 must have completed on first boot"
        );

        handle.stop();
        client.close();
        server.shutdown().await;
    }

    // ── Second boot (same data_dir) ──────────────────────────────────────
    // The _wf_* streams and consumer should be restored from metadata.
    {
        let mut server = TestServerBuilder::new()
            .data_dir(dir_str)
            .spawn_on(&addr)
            .await;
        let client = server.connect().await;

        // Verify the user_data stream survived.
        let resp = client.list_streams(0, 1000).await.unwrap();
        let names = TestServer::stream_names(&resp);
        assert!(
            names.iter().any(|n| n == b"user_data"),
            "user_data stream must survive restart"
        );

        let completed2 = Arc::new(AtomicBool::new(false));
        let completed2_flag = completed2.clone();

        // Re-register the same workflow on the new client.
        let handle2 = client
            .workflow(b"survive-restart")
            .trigger(b"survive.start")
            .ack_wait_ms(5000)
            .step(b"first", |ctx: StepContext| async move {
                let mut out = ctx.context.clone();
                out.extend_from_slice(b"|step0");
                Ok(StepResult { context: out })
            })
            .step(b"second", move |ctx: StepContext| {
                let flag = completed2_flag.clone();
                async move {
                    flag.store(true, Ordering::Release);
                    let mut out = ctx.context.clone();
                    out.extend_from_slice(b"|step1");
                    Ok(StepResult { context: out })
                }
            })
            .start()
            .await
            .expect("workflow start (boot 2)");

        // Trigger a NEW instance on the restarted server.
        handle2
            .trigger(&client, b"boot2")
            .await
            .expect("trigger workflow (boot 2)");

        tokio::time::timeout(Duration::from_secs(10), async {
            while !completed2.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("second-boot workflow did not complete within 10 seconds");

        assert!(
            completed2.load(Ordering::Acquire),
            "new workflow instance must complete on restarted broker"
        );

        handle2.stop();
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Test 7: 6 workers distribute workflow instances via consumer group
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn workflow_6_workers_distribute_in_process() {
    let mut server = TestServerBuilder::new().spawn().await;

    // Shared log: (worker_id, instance_id, step_index)
    let log: Arc<Mutex<Vec<(u32, u32, u16)>>> = Arc::new(Mutex::new(Vec::new()));
    let entry_count = Arc::new(AtomicU32::new(0));

    // Connect 6 separate clients, each registering the same workflow.
    let mut handles = Vec::new();
    let mut clients = Vec::new();

    for worker_id in 0u32..6 {
        let client = server.connect().await;

        let log0 = log.clone();
        let log1 = log.clone();
        let log2 = log.clone();
        let cnt0 = entry_count.clone();
        let cnt1 = entry_count.clone();
        let cnt2 = entry_count.clone();

        let handle = client
            .workflow(b"distrib")
            .trigger(b"jobs.>")
            .ack_wait_ms(5000)
            .step(b"step-0", move |ctx: StepContext| {
                let l = log0.clone();
                let c = cnt0.clone();
                async move {
                    l.lock().unwrap().push((worker_id, ctx.instance_id, 0));
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(StepResult {
                        context: ctx.context.clone(),
                    })
                }
            })
            .step(b"step-1", move |ctx: StepContext| {
                let l = log1.clone();
                let c = cnt1.clone();
                async move {
                    l.lock().unwrap().push((worker_id, ctx.instance_id, 1));
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(StepResult {
                        context: ctx.context.clone(),
                    })
                }
            })
            .step(b"step-2", move |ctx: StepContext| {
                let l = log2.clone();
                let c = cnt2.clone();
                async move {
                    l.lock().unwrap().push((worker_id, ctx.instance_id, 2));
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(StepResult {
                        context: ctx.context.clone(),
                    })
                }
            })
            .start()
            .await
            .expect("workflow start");

        handles.push(handle);
        clients.push(client);
    }

    // Use the first client's handle to trigger 12 instances.
    for i in 0u32..12 {
        let payload = format!("job-{i}");
        handles[0]
            .trigger(&clients[0], payload.as_bytes())
            .await
            .unwrap_or_else(|e| panic!("trigger instance {i} failed: {e:?}"));
    }

    // Wait for all 36 log entries (12 instances × 3 steps).
    tokio::time::timeout(Duration::from_secs(15), async {
        while entry_count.load(Ordering::SeqCst) < 36 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("not all 36 step executions completed within 15 seconds");

    let entries = log.lock().unwrap().clone();

    // Assert: no duplicate (instance_id, step_index) pairs.
    let mut seen_pairs: HashSet<(u32, u16)> = HashSet::new();
    for &(_, inst, step) in &entries {
        assert!(
            seen_pairs.insert((inst, step)),
            "duplicate execution: instance_id={inst}, step_index={step}"
        );
    }

    // Assert: every instance completed all 3 steps.
    let mut instance_steps: std::collections::HashMap<u32, HashSet<u16>> =
        std::collections::HashMap::new();
    for &(_, inst, step) in &entries {
        instance_steps.entry(inst).or_default().insert(step);
    }
    assert_eq!(
        instance_steps.len(),
        12,
        "expected 12 distinct instances, got {}",
        instance_steps.len()
    );
    for (inst, steps) in &instance_steps {
        assert_eq!(
            steps.len(),
            3,
            "instance {inst} has {} steps instead of 3: {:?}",
            steps.len(),
            steps
        );
    }

    // Assert: multiple workers participated.
    let unique_workers: HashSet<u32> = entries.iter().map(|&(w, _, _)| w).collect();
    assert!(
        unique_workers.len() > 1,
        "expected more than 1 worker to participate, but only {:?} did",
        unique_workers
    );

    for h in &handles {
        h.stop();
    }
    server.shutdown().await;
}
