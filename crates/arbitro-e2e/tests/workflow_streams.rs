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

    // Encode: [id_len:2 LE][instance_id:id_len][step_index:2 LE][attempt:1][context...]
    let mut payload = Vec::new();
    let id_bytes = instance_id.as_bytes();
    payload.extend_from_slice(&(id_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(id_bytes);
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
    let log: Arc<Mutex<Vec<(u32, String, u16)>>> = Arc::new(Mutex::new(Vec::new()));
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
    let mut seen_pairs: HashSet<(String, u16)> = HashSet::new();
    for (_, inst, step) in &entries {
        assert!(
            seen_pairs.insert((inst.clone(), *step)),
            "duplicate execution: instance_id={inst}, step_index={step}"
        );
    }

    // Assert: every instance completed all 3 steps.
    let mut instance_steps: std::collections::HashMap<String, HashSet<u16>> =
        std::collections::HashMap::new();
    for (_, inst, step) in &entries {
        instance_steps.entry(inst.clone()).or_default().insert(*step);
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
    let unique_workers: HashSet<u32> = entries.iter().map(|(w, _, _)| *w).collect();
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

// ── T1: trigger_with_id — explicit ID arrives in handler ──────────────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_trigger_with_id_explicit() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let received_id: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let received_id_clone = received_id.clone();
    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();

    let handle = client
        .workflow(b"twid-explicit")
        .trigger(b"twid.>")
        .step(b"check-id", move |ctx: StepContext| {
            let r = received_id_clone.clone();
            let d = done_flag.clone();
            async move {
                *r.lock().unwrap() = Some(ctx.instance_id.clone());
                d.store(true, Ordering::Release);
                Ok(StepResult { context: vec![] })
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle
        .trigger_with_id(&client, "ord_42", b"payload")
        .await
        .expect("trigger_with_id");

    tokio::time::timeout(Duration::from_secs(5), async {
        while !done.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("step did not complete");

    let id = received_id.lock().unwrap().clone().unwrap();
    assert_eq!(id, "ord_42", "handler must receive the explicit instance_id");

    handle.stop();
    server.shutdown().await;
}

// ── T2: trigger_with_id — two IDs run independently ──────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_trigger_with_id_two_instances() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let ids: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let ids_clone = ids.clone();
    let count = Arc::new(AtomicU32::new(0));
    let count_clone = count.clone();

    let handle = client
        .workflow(b"twid-two")
        .trigger(b"twid2.>")
        .step(b"collect", move |ctx: StepContext| {
            let i = ids_clone.clone();
            let c = count_clone.clone();
            async move {
                i.lock().unwrap().push(ctx.instance_id.clone());
                c.fetch_add(1, Ordering::SeqCst);
                Ok(StepResult { context: vec![] })
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle.trigger_with_id(&client, "alpha", b"a").await.unwrap();
    handle.trigger_with_id(&client, "beta", b"b").await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while count.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("both instances did not complete");

    let mut collected = ids.lock().unwrap().clone();
    collected.sort();
    assert_eq!(collected, vec!["alpha", "beta"]);

    handle.stop();
    server.shutdown().await;
}

// ── T3: trigger() returns auto-generated String ID ───────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_trigger_returns_string_id() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let received_id: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let received_id_clone = received_id.clone();
    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();

    let handle = client
        .workflow(b"twid-auto")
        .trigger(b"twid3.>")
        .step(b"capture", move |ctx: StepContext| {
            let r = received_id_clone.clone();
            let d = done_flag.clone();
            async move {
                *r.lock().unwrap() = Some(ctx.instance_id.clone());
                d.store(true, Ordering::Release);
                Ok(StepResult { context: vec![] })
            }
        })
        .start()
        .await
        .expect("workflow start");

    let returned_id: String = handle
        .trigger(&client, b"data")
        .await
        .expect("trigger");

    tokio::time::timeout(Duration::from_secs(5), async {
        while !done.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("step did not complete");

    let handler_id = received_id.lock().unwrap().clone().unwrap();
    assert_eq!(
        returned_id, handler_id,
        "trigger() return value must match ctx.instance_id in handler"
    );
    // Auto-generated IDs are numeric strings (from AtomicU32).
    assert!(
        returned_id.parse::<u32>().is_ok(),
        "auto-generated ID must be a numeric string, got: {returned_id}"
    );

    handle.stop();
    server.shutdown().await;
}

// ── T4: trigger_with_id — duplicate is idempotent (dedup by msg_id) ──────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_trigger_with_id_idempotent() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let exec_count = Arc::new(AtomicU32::new(0));
    let exec_clone = exec_count.clone();

    let handle = client
        .workflow(b"twid-idem")
        .trigger(b"twid4.>")
        .step(b"count", move |ctx: StepContext| {
            let c = exec_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(StepResult { context: ctx.context })
            }
        })
        .start()
        .await
        .expect("workflow start");

    // First trigger succeeds.
    handle.trigger_with_id(&client, "dedup_1", b"x").await.unwrap();

    // Second trigger with same ID — dedup rejects at broker.
    let dup = handle.trigger_with_id(&client, "dedup_1", b"x").await;
    assert!(
        matches!(
            dup,
            Err(ClientError::Broker {
                code: ErrorCode::IdempotencyDuplicate
            })
        ),
        "duplicate trigger_with_id must be rejected, got {:?}",
        dup,
    );

    // Wait for the single execution to complete.
    tokio::time::timeout(Duration::from_secs(5), async {
        while exec_count.load(Ordering::SeqCst) < 1 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("step did not execute");

    // Give a brief window for any duplicate to sneak through.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        exec_count.load(Ordering::SeqCst),
        1,
        "step must execute exactly once despite duplicate trigger"
    );

    handle.stop();
    server.shutdown().await;
}

// ── T5: source — publish to external stream triggers workflow ─────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_source_basic() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    // Create external source stream.
    let src_resp = client
        .create_stream(b"src-payments", b"payments.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create source stream");
    let src_stream_id = u64::from_le_bytes(src_resp[..8].try_into().unwrap()) as u32;

    let received_ctx: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let received_clone = received_ctx.clone();
    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();

    let handle = client
        .workflow(b"src-basic")
        .trigger(b"src-basic.>")
        .source(b"src-payments", b"payments.>")
        .step(b"process", move |ctx: StepContext| {
            let r = received_clone.clone();
            let d = done_flag.clone();
            async move {
                *r.lock().unwrap() = Some(ctx.context.clone());
                d.store(true, Ordering::Release);
                Ok(StepResult { context: ctx.context })
            }
        })
        .start()
        .await
        .expect("workflow start");

    // Publish to the source stream (NOT the workflow stream).
    client
        .publish_sync(src_stream_id, b"payments.completed", Bytes::from_static(b"order_99"))
        .await
        .expect("publish to source");

    tokio::time::timeout(Duration::from_secs(5), async {
        while !done.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("workflow did not trigger from source within 5s");

    let ctx = received_ctx.lock().unwrap().clone().unwrap();
    assert_eq!(ctx, b"order_99", "source payload must become workflow context");

    handle.stop();
    server.shutdown().await;
}

// ── T6: two sources — both feed into the same workflow ───────────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_source_multiple() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    // Create two external streams.
    let resp_a = client
        .create_stream(b"src-orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create orders stream");
    let sid_a = u64::from_le_bytes(resp_a[..8].try_into().unwrap()) as u32;

    let resp_b = client
        .create_stream(b"src-refunds", b"refunds.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create refunds stream");
    let sid_b = u64::from_le_bytes(resp_b[..8].try_into().unwrap()) as u32;

    let payloads: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let payloads_clone = payloads.clone();
    let count = Arc::new(AtomicU32::new(0));
    let count_clone = count.clone();

    let handle = client
        .workflow(b"src-multi")
        .trigger(b"src-multi.>")
        .source(b"src-orders", b"orders.>")
        .source(b"src-refunds", b"refunds.>")
        .step(b"collect", move |ctx: StepContext| {
            let p = payloads_clone.clone();
            let c = count_clone.clone();
            async move {
                p.lock().unwrap().push(ctx.context.clone());
                c.fetch_add(1, Ordering::SeqCst);
                Ok(StepResult { context: ctx.context })
            }
        })
        .start()
        .await
        .expect("workflow start");

    client.publish_sync(sid_a, b"orders.new", Bytes::from_static(b"from_orders")).await.unwrap();
    client.publish_sync(sid_b, b"refunds.new", Bytes::from_static(b"from_refunds")).await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while count.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("both sources did not trigger workflow");

    let mut collected: Vec<Vec<u8>> = payloads.lock().unwrap().clone();
    collected.sort();
    assert_eq!(
        collected,
        vec![b"from_orders".to_vec(), b"from_refunds".to_vec()],
    );

    handle.stop();
    server.shutdown().await;
}

// ── T7: source + manual trigger coexist ──────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_source_plus_trigger() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"src-events", b"events.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create events stream");
    let src_sid = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    let payloads: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let payloads_clone = payloads.clone();
    let count = Arc::new(AtomicU32::new(0));
    let count_clone = count.clone();

    let handle = client
        .workflow(b"src-plus-trig")
        .trigger(b"src-plus-trig.>")
        .source(b"src-events", b"events.>")
        .step(b"gather", move |ctx: StepContext| {
            let p = payloads_clone.clone();
            let c = count_clone.clone();
            async move {
                p.lock().unwrap().push(ctx.context.clone());
                c.fetch_add(1, Ordering::SeqCst);
                Ok(StepResult { context: ctx.context })
            }
        })
        .start()
        .await
        .expect("workflow start");

    // Manual trigger.
    handle.trigger(&client, b"manual").await.unwrap();
    // Source trigger.
    client.publish_sync(src_sid, b"events.click", Bytes::from_static(b"source")).await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while count.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("both triggers did not complete");

    let mut collected: Vec<Vec<u8>> = payloads.lock().unwrap().clone();
    collected.sort();
    assert_eq!(
        collected,
        vec![b"manual".to_vec(), b"source".to_vec()],
    );

    handle.stop();
    server.shutdown().await;
}

// ── T9: suspend step — basic resume ─────────────────────────────────

use arbitro_client_tokio::workflow::{StepOutcome, ResumeContext, TimeoutContext};

#[tokio::test(flavor = "multi_thread")]
async fn workflow_suspend_basic_resume() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();
    let resume_event_received: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let resume_event_clone = resume_event_received.clone();

    let handle = client
        .workflow(b"suspend-basic")
        .trigger(b"suspend-basic.>")
        .suspend_step(
            b"wait-approval",
            5000,
            |_ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: b"my-state".to_vec(),
                    timeout_ms: 0,
                })
            },
            move |rctx: ResumeContext| {
                let ev = resume_event_clone.clone();
                let d = done_flag.clone();
                async move {
                    *ev.lock().unwrap() = Some(rctx.event.clone());
                    d.store(true, Ordering::Release);
                    Ok(StepResult { context: rctx.event })
                }
            },
        )
        .start()
        .await
        .expect("workflow start");

    handle
        .trigger_with_id(&client, "susp_1", b"initial")
        .await
        .expect("trigger");

    tokio::time::sleep(Duration::from_millis(500)).await;

    handle
        .resume(&client, "susp_1", b"approved")
        .await
        .expect("resume");

    tokio::time::timeout(Duration::from_secs(5), async {
        while !done.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("on_resume did not complete");

    let ev = resume_event_received.lock().unwrap().clone().unwrap();
    assert_eq!(ev, b"approved", "resume event must match");

    handle.stop();
    server.shutdown().await;
}

// ── T10: suspend step — timeout fires ───────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_suspend_timeout() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let timed_out = Arc::new(AtomicBool::new(false));
    let timed_out_flag = timed_out.clone();
    let timeout_state: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let timeout_state_clone = timeout_state.clone();

    let handle = client
        .workflow(b"suspend-timeout")
        .trigger(b"suspend-timeout.>")
        .suspend_step(
            b"wait-payment",
            0,
            |_ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: b"pending-payment".to_vec(),
                    timeout_ms: 500,
                })
            },
            |_rctx: ResumeContext| async move {
                panic!("on_resume should not be called");
            },
        )
        .on_timeout(move |tctx: TimeoutContext| {
            let flag = timed_out_flag.clone();
            let st = timeout_state_clone.clone();
            async move {
                *st.lock().unwrap() = Some(tctx.state.clone());
                flag.store(true, Ordering::Release);
                Ok(StepResult { context: b"timed-out".to_vec() })
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle
        .trigger_with_id(&client, "susp_timeout_1", b"ctx")
        .await
        .expect("trigger");

    tokio::time::timeout(Duration::from_secs(10), async {
        while !timed_out.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("on_timeout did not fire");

    let st = timeout_state.lock().unwrap().clone().unwrap();
    assert_eq!(st, b"pending-payment", "timeout state must match suspended state");

    handle.stop();
    server.shutdown().await;
}

// ── T11: suspend — resume before timeout (timeout becomes no-op) ────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_suspend_resume_before_timeout() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resumed = Arc::new(AtomicBool::new(false));
    let resumed_flag = resumed.clone();
    let timed_out = Arc::new(AtomicBool::new(false));
    let timed_out_flag = timed_out.clone();

    let handle = client
        .workflow(b"susp-race")
        .trigger(b"susp-race.>")
        .suspend_step(
            b"wait",
            0,
            |_ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: b"s".to_vec(),
                    timeout_ms: 2000,
                })
            },
            move |_rctx: ResumeContext| {
                let f = resumed_flag.clone();
                async move {
                    f.store(true, Ordering::Release);
                    Ok(StepResult { context: vec![] })
                }
            },
        )
        .on_timeout(move |_tctx: TimeoutContext| {
            let f = timed_out_flag.clone();
            async move {
                f.store(true, Ordering::Release);
                Ok(StepResult { context: vec![] })
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle.trigger_with_id(&client, "race_1", b"").await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    handle.resume(&client, "race_1", b"event").await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while !resumed.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("on_resume did not fire");

    // Wait a bit more to see if timeout fires (it shouldn't).
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        !timed_out.load(Ordering::Acquire),
        "timeout should NOT fire after successful resume"
    );

    handle.stop();
    server.shutdown().await;
}

// ── T12: suspend step returns Done immediately ──────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_suspend_done_immediate() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();

    let handle = client
        .workflow(b"susp-done")
        .trigger(b"susp-done.>")
        .suspend_step(
            b"maybe-wait",
            5000,
            |ctx: StepContext| async move {
                Ok(StepOutcome::Done(StepResult {
                    context: ctx.context,
                }))
            },
            |_rctx: ResumeContext| async move {
                panic!("on_resume should not be called when Done");
            },
        )
        .step(b"finish", move |_ctx: StepContext| {
            let f = done_flag.clone();
            async move {
                f.store(true, Ordering::Release);
                Ok(StepResult { context: vec![] })
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle.trigger(&client, b"data").await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while !done.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("workflow did not complete through Done path");

    handle.stop();
    server.shutdown().await;
}

// ── T13: normal → suspend → resume → normal (full chain) ───────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_suspend_full_chain() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let final_ctx: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let final_ctx_clone = final_ctx.clone();
    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();

    let handle = client
        .workflow(b"susp-chain")
        .trigger(b"susp-chain.>")
        .step(b"prepare", |ctx: StepContext| async move {
            let mut out = ctx.context.clone();
            out.extend_from_slice(b"|prepared");
            Ok(StepResult { context: out })
        })
        .suspend_step(
            b"await-approval",
            10000,
            |ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: ctx.context.clone(),
                    timeout_ms: 0,
                })
            },
            |rctx: ResumeContext| async move {
                let mut out = rctx.state.clone();
                out.extend_from_slice(b"|");
                out.extend_from_slice(&rctx.event);
                Ok(StepResult { context: out })
            },
        )
        .step(b"finalize", move |ctx: StepContext| {
            let fc = final_ctx_clone.clone();
            let d = done_flag.clone();
            async move {
                let mut out = ctx.context.clone();
                out.extend_from_slice(b"|finalized");
                *fc.lock().unwrap() = Some(out.clone());
                d.store(true, Ordering::Release);
                Ok(StepResult { context: out })
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle.trigger_with_id(&client, "chain_1", b"init").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    handle.resume(&client, "chain_1", b"approved").await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while !done.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("full chain did not complete");

    let ctx = final_ctx.lock().unwrap().clone().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&ctx),
        "init|prepared|approved|finalized",
    );

    handle.stop();
    server.shutdown().await;
}

// ── T14: cancel suspended instance — timeout becomes no-op ────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_cancel_removes_suspended() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let timed_out = Arc::new(AtomicBool::new(false));
    let timed_out_flag = timed_out.clone();
    let resumed = Arc::new(AtomicBool::new(false));
    let resumed_flag = resumed.clone();

    let handle = client
        .workflow(b"cancel-basic")
        .trigger(b"cancel-basic.>")
        .suspend_step(
            b"wait",
            2000, // 2s timeout
            |_ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: b"parked".to_vec(),
                    timeout_ms: 0,
                })
            },
            move |_rctx: ResumeContext| {
                let f = resumed_flag.clone();
                async move {
                    f.store(true, Ordering::Release);
                    Ok(StepResult { context: vec![] })
                }
            },
        )
        .on_timeout(move |_tctx: TimeoutContext| {
            let f = timed_out_flag.clone();
            async move {
                f.store(true, Ordering::Release);
                Ok(StepResult { context: vec![] })
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle.trigger_with_id(&client, "cancel_1", b"").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cancel while suspended.
    handle.cancel(&client, "cancel_1").await.unwrap();

    // Wait past the original timeout window.
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(
        !timed_out.load(Ordering::Acquire),
        "timeout should NOT fire after cancel"
    );
    assert!(
        !resumed.load(Ordering::Acquire),
        "resume should NOT fire after cancel"
    );

    handle.stop();
    server.shutdown().await;
}

// ── T15: cancel non-existent instance — idempotent no-op ─────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_cancel_nonexistent_noop() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let handle = client
        .workflow(b"cancel-noop")
        .trigger(b"cancel-noop.>")
        .step(b"only", |ctx: StepContext| async move {
            Ok(StepResult { context: ctx.context })
        })
        .start()
        .await
        .expect("workflow start");

    // Cancel an instance that was never created — should not panic or error.
    handle.cancel(&client, "ghost_42").await.unwrap();

    handle.stop();
    server.shutdown().await;
}

// ── T16: cancel after resume — double cancel is no-op ─────────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_cancel_after_resume_noop() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();

    let handle = client
        .workflow(b"cancel-post")
        .trigger(b"cancel-post.>")
        .suspend_step(
            b"wait",
            0,
            |_ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: b"s".to_vec(),
                    timeout_ms: 0,
                })
            },
            move |_rctx: ResumeContext| {
                let f = done_flag.clone();
                async move {
                    f.store(true, Ordering::Release);
                    Ok(StepResult { context: vec![] })
                }
            },
        )
        .start()
        .await
        .expect("workflow start");

    handle.trigger_with_id(&client, "post_1", b"").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Resume first.
    handle.resume(&client, "post_1", b"ev").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !done.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("resume did not complete");

    // Cancel after resume — should be no-op, no crash.
    handle.cancel(&client, "post_1").await.unwrap();

    handle.stop();
    server.shutdown().await;
}

// ── T17: cancel then resume — resume is no-op (entry gone) ───────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_cancel_then_resume_noop() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resumed = Arc::new(AtomicBool::new(false));
    let resumed_flag = resumed.clone();

    let handle = client
        .workflow(b"cancel-then-r")
        .trigger(b"cancel-then-r.>")
        .suspend_step(
            b"wait",
            0,
            |_ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: b"s".to_vec(),
                    timeout_ms: 0,
                })
            },
            move |_rctx: ResumeContext| {
                let f = resumed_flag.clone();
                async move {
                    f.store(true, Ordering::Release);
                    Ok(StepResult { context: vec![] })
                }
            },
        )
        .start()
        .await
        .expect("workflow start");

    handle.trigger_with_id(&client, "ctr_1", b"").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cancel first.
    handle.cancel(&client, "ctr_1").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Resume after cancel — entry gone, should be acked as no-op.
    handle.resume(&client, "ctr_1", b"event").await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(
        !resumed.load(Ordering::Acquire),
        "on_resume should NOT fire after cancel"
    );

    handle.stop();
    server.shutdown().await;
}

// ── T18: cancel running (non-suspended) instance — best-effort ───

#[tokio::test(flavor = "multi_thread")]
async fn workflow_cancel_running_instance() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();

    let handle = client
        .workflow(b"cancel-run")
        .trigger(b"cancel-run.>")
        .step(b"slow", |_ctx: StepContext| async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(StepResult { context: b"done".to_vec() })
        })
        .step(b"finish", move |_ctx: StepContext| {
            let f = done_flag.clone();
            async move {
                f.store(true, Ordering::Release);
                Ok(StepResult { context: vec![] })
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle.trigger_with_id(&client, "run_1", b"").await.unwrap();
    // Give it just enough time to start running step "slow" but not finish.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Cancel while the step is running (not suspended). Instance is
    // not in the suspended map, so cancel is a no-op — the step
    // continues to completion. This tests that cancel doesn't crash.
    handle.cancel(&client, "run_1").await.unwrap();

    // The workflow should still complete because cancel only removes
    // from the suspended registry.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !done.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("workflow should still complete after cancel on running step");

    handle.stop();
    server.shutdown().await;
}

// ══════════════════════════════════════════════════════════════════════
// Fase 4 — E2E integration tests (cross-feature)
// ══════════════════════════════════════════════════════════════════════

// ── T19: trigger → normal → suspend → resume → normal (pipeline) ──

#[tokio::test(flavor = "multi_thread")]
async fn workflow_e2e_source_suspend_resume() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let final_ctx: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let final_ctx_clone = final_ctx.clone();
    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();

    let handle = client
        .workflow(b"e2e-ssr")
        .trigger(b"e2e-ssr.>")
        .step(b"validate", |ctx: StepContext| async move {
            let mut out = ctx.context.clone();
            out.extend_from_slice(b"|validated");
            Ok(StepResult { context: out })
        })
        .suspend_step(
            b"await-approval",
            10000,
            |ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: ctx.context.clone(),
                    timeout_ms: 0,
                })
            },
            |rctx: ResumeContext| async move {
                let mut out = rctx.state.clone();
                out.extend_from_slice(b"|");
                out.extend_from_slice(&rctx.event);
                Ok(StepResult { context: out })
            },
        )
        .step(b"ship", move |ctx: StepContext| {
            let fc = final_ctx_clone.clone();
            let d = done_flag.clone();
            async move {
                let mut out = ctx.context.clone();
                out.extend_from_slice(b"|shipped");
                *fc.lock().unwrap() = Some(out.clone());
                d.store(true, Ordering::Release);
                Ok(StepResult { context: out })
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle
        .trigger_with_id(&client, "order_42", b"order-data")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    handle
        .resume(&client, "order_42", b"approved")
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while !done.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("e2e source→suspend→resume did not complete");

    let ctx = final_ctx.lock().unwrap().clone().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&ctx),
        "order-data|validated|approved|shipped",
    );

    handle.stop();
    server.shutdown().await;
}

// ── T20: multi-instance suspend + selective cancel ────────────────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_e2e_multi_suspend_selective_cancel() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let results: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let results_clone = results.clone();

    let handle = client
        .workflow(b"e2e-multi")
        .trigger(b"e2e-multi.>")
        .suspend_step(
            b"wait",
            0,
            |ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: ctx.context.clone(),
                    timeout_ms: 0,
                })
            },
            move |rctx: ResumeContext| {
                let r = results_clone.clone();
                async move {
                    let label = format!(
                        "{}+{}",
                        String::from_utf8_lossy(&rctx.state),
                        String::from_utf8_lossy(&rctx.event),
                    );
                    r.lock().unwrap().push(label);
                    Ok(StepResult { context: vec![] })
                }
            },
        )
        .start()
        .await
        .expect("workflow start");

    // Trigger 3 instances.
    for i in 1..=3u8 {
        let id = format!("inst_{i}");
        let ctx = format!("data_{i}");
        handle
            .trigger_with_id(&client, &id, ctx.as_bytes())
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cancel instance 2.
    handle.cancel(&client, "inst_2").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Resume instances 1 and 3.
    handle.resume(&client, "inst_1", b"resumed_1").await.unwrap();
    handle.resume(&client, "inst_3", b"resumed_3").await.unwrap();

    // Resume instance 2 — should be no-op (already cancelled).
    handle.resume(&client, "inst_2", b"resumed_2").await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if results.lock().unwrap().len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("two resumed instances did not complete");

    // Give a moment for the (incorrectly) resumed inst_2 to fire.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut got = results.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, vec!["data_1+resumed_1", "data_3+resumed_3"]);

    handle.stop();
    server.shutdown().await;
}

// ── T21: suspend timeout fires, on_timeout handler continues ──────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_e2e_suspend_timeout_continues() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let final_ctx: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let final_ctx_clone = final_ctx.clone();
    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();

    let handle = client
        .workflow(b"e2e-tcomp")
        .trigger(b"e2e-tcomp.>")
        .step(b"prepare", |ctx: StepContext| async move {
            let mut out = ctx.context.clone();
            out.extend_from_slice(b"|prepared");
            Ok(StepResult { context: out })
        })
        .suspend_step(
            b"await",
            500, // 500ms timeout
            |ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: ctx.context.clone(),
                    timeout_ms: 0,
                })
            },
            |_rctx: ResumeContext| async move {
                panic!("on_resume should not fire — testing timeout path");
            },
        )
        .on_timeout(|tctx: TimeoutContext| async move {
            let mut out = tctx.state.clone();
            out.extend_from_slice(b"|timed-out");
            Ok(StepResult { context: out })
        })
        .step(b"finalize", move |ctx: StepContext| {
            let fc = final_ctx_clone.clone();
            let d = done_flag.clone();
            async move {
                let mut out = ctx.context.clone();
                out.extend_from_slice(b"|finalized");
                *fc.lock().unwrap() = Some(out.clone());
                d.store(true, Ordering::Release);
                Ok(StepResult { context: out })
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle
        .trigger_with_id(&client, "tcomp_1", b"init")
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while !done.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("timeout path did not complete");

    let ctx = final_ctx.lock().unwrap().clone().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&ctx),
        "init|prepared|timed-out|finalized",
    );

    handle.stop();
    server.shutdown().await;
}

// ── T22: cancel then resume — resume is no-op, workflow dead ──────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_e2e_cancel_blocks_resume() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resumed = Arc::new(AtomicBool::new(false));
    let resumed_flag = resumed.clone();
    let finished = Arc::new(AtomicBool::new(false));
    let finished_flag = finished.clone();

    let handle = client
        .workflow(b"e2e-cb")
        .trigger(b"e2e-cb.>")
        .suspend_step(
            b"wait",
            0,
            |ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: ctx.context.clone(),
                    timeout_ms: 0,
                })
            },
            move |_rctx: ResumeContext| {
                let f = resumed_flag.clone();
                async move {
                    f.store(true, Ordering::Release);
                    Ok(StepResult { context: vec![] })
                }
            },
        )
        .step(b"after", move |_ctx: StepContext| {
            let f = finished_flag.clone();
            async move {
                f.store(true, Ordering::Release);
                Ok(StepResult { context: vec![] })
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle
        .trigger_with_id(&client, "cb_1", b"data")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cancel first, then try to resume.
    handle.cancel(&client, "cb_1").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    handle.resume(&client, "cb_1", b"event").await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(!resumed.load(Ordering::Acquire), "on_resume must not fire after cancel");
    assert!(!finished.load(Ordering::Acquire), "next step must not fire after cancel");

    handle.stop();
    server.shutdown().await;
}

// ── T23: full lifecycle — 2 suspend points, context pipeline ──────

#[tokio::test(flavor = "multi_thread")]
async fn workflow_e2e_full_lifecycle() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let final_ctx: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let final_ctx_clone = final_ctx.clone();
    let done = Arc::new(AtomicBool::new(false));
    let done_flag = done.clone();

    let handle = client
        .workflow(b"e2e-full")
        .trigger(b"e2e-full.>")
        .step(b"enrich", |ctx: StepContext| async move {
            let mut out = ctx.context.clone();
            out.extend_from_slice(b"|enriched");
            Ok(StepResult { context: out })
        })
        .suspend_step(
            b"await-approval",
            10000,
            |ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: ctx.context.clone(),
                    timeout_ms: 0,
                })
            },
            |rctx: ResumeContext| async move {
                let mut out = rctx.state.clone();
                out.extend_from_slice(b"|");
                out.extend_from_slice(&rctx.event);
                Ok(StepResult { context: out })
            },
        )
        .step(b"transform", |ctx: StepContext| async move {
            let mut out = ctx.context.clone();
            out.extend_from_slice(b"|transformed");
            Ok(StepResult { context: out })
        })
        .suspend_step(
            b"await-payment",
            10000,
            |ctx: StepContext| async move {
                Ok(StepOutcome::Suspend {
                    state: ctx.context.clone(),
                    timeout_ms: 0,
                })
            },
            |rctx: ResumeContext| async move {
                let mut out = rctx.state.clone();
                out.extend_from_slice(b"|");
                out.extend_from_slice(&rctx.event);
                Ok(StepResult { context: out })
            },
        )
        .step(b"finalize", move |ctx: StepContext| {
            let fc = final_ctx_clone.clone();
            let d = done_flag.clone();
            async move {
                let mut out = ctx.context.clone();
                out.extend_from_slice(b"|done");
                *fc.lock().unwrap() = Some(out.clone());
                d.store(true, Ordering::Release);
                Ok(StepResult { context: out })
            }
        })
        .start()
        .await
        .expect("workflow start");

    handle
        .trigger_with_id(&client, "full_1", b"order-123")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // First resume (approval)
    handle.resume(&client, "full_1", b"approved").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Second resume (payment)
    handle.resume(&client, "full_1", b"paid").await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while !done.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("full lifecycle did not complete");

    let ctx = final_ctx.lock().unwrap().clone().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&ctx),
        "order-123|enriched|approved|transformed|paid|done",
    );

    handle.stop();
    server.shutdown().await;
}

// ══════════════════════════════════════════════════════════════════════
// Distribution tests — multiple workers with suspend/cancel step types
// ══════════════════════════════════════════════════════════════════════
//
// NOTE: The suspended registry is per-worker (in-memory). Resume and
// cancel messages are routed through the shared consumer group, so
// they may land on a different worker than the one that holds the
// entry. True distributed suspend/resume requires a shared registry
// (future work). These tests exercise the StepKind::Suspend code path
// in distributed mode using StepOutcome::Done (immediate — no actual
// suspension), which IS safe across workers.

// ── T24: 4 workers, 12 instances, normal→suspend(Done)→normal ─────
//    The suspend step always returns Done immediately, so no entry is
//    stored. Verifies the full StepKind::Suspend dispatch path works
//    across multiple workers without actual suspension.

#[tokio::test(flavor = "multi_thread")]
async fn workflow_distrib_suspend_done() {
    let mut server = TestServerBuilder::new().spawn().await;

    // Shared log: (worker_id, instance_id, step_index)
    let log: Arc<Mutex<Vec<(u32, String, u16)>>> = Arc::new(Mutex::new(Vec::new()));
    let entry_count = Arc::new(AtomicU32::new(0));

    let mut handles = Vec::new();
    let mut clients = Vec::new();

    for worker_id in 0u32..4 {
        let client = server.connect().await;

        let log0 = log.clone();
        let log1 = log.clone();
        let log2 = log.clone();
        let cnt0 = entry_count.clone();
        let cnt1 = entry_count.clone();
        let cnt2 = entry_count.clone();

        let handle = client
            .workflow(b"distrib-sd")
            .trigger(b"distrib-sd.>")
            .ack_wait_ms(5000)
            .step(b"prep", move |ctx: StepContext| {
                let l = log0.clone();
                let c = cnt0.clone();
                async move {
                    l.lock().unwrap().push((worker_id, ctx.instance_id.clone(), 0));
                    c.fetch_add(1, Ordering::SeqCst);
                    let mut out = ctx.context.clone();
                    out.extend_from_slice(b"|prepared");
                    Ok(StepResult { context: out })
                }
            })
            // Suspend step that returns Done — exercises StepKind::Suspend
            // code path but doesn't actually suspend.
            .suspend_step(
                b"check",
                5000,
                move |ctx: StepContext| {
                    let l = log1.clone();
                    let c = cnt1.clone();
                    async move {
                        l.lock().unwrap().push((worker_id, ctx.instance_id.clone(), 1));
                        c.fetch_add(1, Ordering::SeqCst);
                        let mut out = ctx.context.clone();
                        out.extend_from_slice(b"|checked");
                        Ok(StepOutcome::Done(StepResult { context: out }))
                    }
                },
                |_rctx: ResumeContext| async move {
                    panic!("on_resume should not be called — step returns Done");
                },
            )
            .step(b"finalize", move |ctx: StepContext| {
                let l = log2.clone();
                let c = cnt2.clone();
                async move {
                    l.lock().unwrap().push((worker_id, ctx.instance_id.clone(), 2));
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(StepResult { context: ctx.context })
                }
            })
            .start()
            .await
            .expect("workflow start");

        handles.push(handle);
        clients.push(client);
    }

    // Trigger 12 instances.
    for i in 0u32..12 {
        let id = format!("dsd_{i}");
        handles[0]
            .trigger_with_id(&clients[0], &id, format!("d{i}").as_bytes())
            .await
            .unwrap();
    }

    // Wait for 36 log entries (12 × 3 steps).
    tokio::time::timeout(Duration::from_secs(15), async {
        while entry_count.load(Ordering::SeqCst) < 36 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("not all 36 step executions completed");

    let entries = log.lock().unwrap().clone();

    // No duplicate (instance_id, step_index) pairs.
    let mut seen: HashSet<(String, u16)> = HashSet::new();
    for (_, inst, step) in &entries {
        assert!(
            seen.insert((inst.clone(), *step)),
            "duplicate: instance={inst}, step={step}"
        );
    }

    // Every instance completed all 3 steps.
    let mut instance_steps: std::collections::HashMap<String, HashSet<u16>> =
        std::collections::HashMap::new();
    for (_, inst, step) in &entries {
        instance_steps.entry(inst.clone()).or_default().insert(*step);
    }
    assert_eq!(instance_steps.len(), 12);
    for (inst, steps) in &instance_steps {
        assert_eq!(steps.len(), 3, "instance {inst} missing steps: {steps:?}");
    }

    // Multiple workers participated.
    let unique_workers: HashSet<u32> = entries.iter().map(|(w, _, _)| *w).collect();
    assert!(
        unique_workers.len() > 1,
        "expected >1 worker, got {:?}",
        unique_workers,
    );

    for h in &handles { h.stop(); }
    server.shutdown().await;
}

// ── T25: 4 workers, normal + suspend(Done) + compensation ─────────
//    (existing — unchanged)
// ── T26: 4 workers, actual suspend → cross-worker resume via state stream ─
//    Each instance suspends on step 1.  Resume events are round-robined to
//    whichever worker the consumer group picks — likely different from the one
//    that parked.  The state stream ensures every worker has the full suspended
//    map, so the resume handler finds the entry and advances.
// ── T27: 4 workers, suspend → cross-worker cancel via state stream ───
//    (added below after T25)
//    Verifies: StepKind::Suspend with Done distributes correctly and
//    the context pipeline is intact across workers.

#[tokio::test(flavor = "multi_thread")]
async fn workflow_distrib_suspend_done_context_pipeline() {
    let mut server = TestServerBuilder::new().spawn().await;

    let results: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let done_count = Arc::new(AtomicU32::new(0));

    let mut handles = Vec::new();
    let mut clients = Vec::new();

    for _worker_id in 0u32..4 {
        let client = server.connect().await;

        let r = results.clone();
        let c = done_count.clone();

        let handle = client
            .workflow(b"distrib-ctx")
            .trigger(b"distrib-ctx.>")
            .ack_wait_ms(5000)
            .step(b"enrich", |ctx: StepContext| async move {
                let mut out = ctx.context.clone();
                out.extend_from_slice(b"|enriched");
                Ok(StepResult { context: out })
            })
            .suspend_step(
                b"gate",
                5000,
                |ctx: StepContext| async move {
                    // Conditional: if context starts with "vip", suspend;
                    // otherwise pass through immediately.
                    // For this test we always pass through (Done).
                    let mut out = ctx.context.clone();
                    out.extend_from_slice(b"|gated");
                    Ok(StepOutcome::Done(StepResult { context: out }))
                },
                |_rctx: ResumeContext| async move {
                    panic!("should not resume — all return Done");
                },
            )
            .step(b"finish", move |ctx: StepContext| {
                let r2 = r.clone();
                let c2 = c.clone();
                async move {
                    let mut out = ctx.context.clone();
                    out.extend_from_slice(b"|finished");
                    r2.lock().unwrap().push((
                        ctx.instance_id.clone(),
                        String::from_utf8_lossy(&out).into_owned(),
                    ));
                    c2.fetch_add(1, Ordering::SeqCst);
                    Ok(StepResult { context: out })
                }
            })
            .start()
            .await
            .expect("workflow start");

        handles.push(handle);
        clients.push(client);
    }

    // Trigger 8 instances with distinct payloads.
    for i in 0u32..8 {
        let id = format!("dctx_{i}");
        let payload = format!("p{i}");
        handles[0]
            .trigger_with_id(&clients[0], &id, payload.as_bytes())
            .await
            .unwrap();
    }

    // Wait for all 8 to finish.
    tokio::time::timeout(Duration::from_secs(15), async {
        while done_count.load(Ordering::SeqCst) < 8 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("not all 8 instances completed");

    let got = results.lock().unwrap().clone();
    assert_eq!(got.len(), 8);

    // Each instance's context should follow the pipeline.
    for i in 0u32..8 {
        let id = format!("dctx_{i}");
        let expected = format!("p{i}|enriched|gated|finished");
        let entry = got.iter().find(|(iid, _)| *iid == id);
        assert!(entry.is_some(), "instance {id} not found in results");
        assert_eq!(entry.unwrap().1, expected, "context mismatch for {id}");
    }

    for h in &handles { h.stop(); }
    server.shutdown().await;
}

// ── T26: 4 workers, actual suspend → cross-worker resume via state stream ─

#[tokio::test(flavor = "multi_thread")]
async fn workflow_distrib_suspend_resume() {
    use arbitro_client_tokio::workflow::{StepOutcome, ResumeContext};

    let mut server = TestServerBuilder::new().spawn().await;

    let finished: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let done_count = Arc::new(AtomicU32::new(0));

    let mut handles = Vec::new();
    let mut clients = Vec::new();

    for _worker_id in 0u32..4 {
        let client = server.connect().await;
        let fin = finished.clone();
        let cnt = done_count.clone();

        let handle = client
            .workflow(b"distrib-sr")
            .trigger(b"distrib-sr.>")
            .ack_wait_ms(5000)
            .step(b"prep", |ctx: StepContext| async move {
                let mut out = ctx.context.clone();
                out.extend_from_slice(b"|prepared");
                Ok(StepResult { context: out })
            })
            .suspend_step(
                b"wait",
                10_000,
                |ctx: StepContext| async move {
                    Ok(StepOutcome::Suspend {
                        state: ctx.context.clone(),
                        timeout_ms: 0,
                    })
                },
                |rctx: ResumeContext| async move {
                    let mut out = rctx.state.clone();
                    out.extend_from_slice(b"|resumed:");
                    out.extend_from_slice(&rctx.event);
                    Ok(StepResult { context: out })
                },
            )
            .step(b"finalize", move |ctx: StepContext| {
                let f = fin.clone();
                let c = cnt.clone();
                async move {
                    let mut out = ctx.context.clone();
                    out.extend_from_slice(b"|done");
                    f.lock().unwrap().push((
                        ctx.instance_id.clone(),
                        String::from_utf8_lossy(&out).into_owned(),
                    ));
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(StepResult { context: out })
                }
            })
            .start()
            .await
            .expect("workflow start");

        handles.push(handle);
        clients.push(client);
    }

    let count = 8u32;

    for i in 0..count {
        let id = format!("dsr_{i}");
        handles[0]
            .trigger_with_id(&clients[0], &id, format!("p{i}").as_bytes())
            .await
            .unwrap();
    }

    // Wait for state stream fanout to propagate park events.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Resume — consumer group delivers to random workers.
    for i in 0..count {
        let id = format!("dsr_{i}");
        handles[0]
            .resume(&clients[0], &id, format!("ev{i}").as_bytes())
            .await
            .unwrap();
    }

    tokio::time::timeout(Duration::from_secs(15), async {
        while done_count.load(Ordering::SeqCst) < count {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("not all instances completed after cross-worker resume");

    let got = finished.lock().unwrap().clone();
    assert_eq!(got.len(), count as usize);

    for i in 0..count {
        let id = format!("dsr_{i}");
        let expected = format!("p{i}|prepared|resumed:ev{i}|done");
        let entry = got.iter().find(|(iid, _)| *iid == id);
        assert!(entry.is_some(), "instance {id} not found in results");
        assert_eq!(entry.unwrap().1, expected, "context mismatch for {id}");
    }

    for h in &handles { h.stop(); }
    server.shutdown().await;
}

// ── T27: 4 workers, suspend → cross-worker cancel via state stream ───

#[tokio::test(flavor = "multi_thread")]
async fn workflow_distrib_suspend_cancel() {
    use arbitro_client_tokio::workflow::{StepOutcome, ResumeContext};

    let mut server = TestServerBuilder::new().spawn().await;

    let resumed = Arc::new(AtomicU32::new(0));

    let mut handles = Vec::new();
    let mut clients = Vec::new();

    for _worker_id in 0u32..4 {
        let client = server.connect().await;
        let r = resumed.clone();

        let handle = client
            .workflow(b"distrib-sc")
            .trigger(b"distrib-sc.>")
            .ack_wait_ms(5000)
            .step(b"prep", |ctx: StepContext| async move {
                Ok(StepResult { context: ctx.context })
            })
            .suspend_step(
                b"wait",
                30_000,
                |ctx: StepContext| async move {
                    Ok(StepOutcome::Suspend {
                        state: ctx.context.clone(),
                        timeout_ms: 0,
                    })
                },
                move |_rctx: ResumeContext| {
                    let r2 = r.clone();
                    async move {
                        r2.fetch_add(1, Ordering::SeqCst);
                        Ok(StepResult { context: Vec::new() })
                    }
                },
            )
            .step(b"never", |ctx: StepContext| async move {
                Ok(StepResult { context: ctx.context })
            })
            .start()
            .await
            .expect("workflow start");

        handles.push(handle);
        clients.push(client);
    }

    let count = 8u32;

    for i in 0..count {
        let id = format!("dsc_{i}");
        handles[0]
            .trigger_with_id(&clients[0], &id, format!("c{i}").as_bytes())
            .await
            .unwrap();
    }

    // Wait for suspend + state stream propagation.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cancel all — round-robined across workers.
    for i in 0..count {
        let id = format!("dsc_{i}");
        handles[0]
            .cancel(&clients[0], &id)
            .await
            .unwrap();
    }

    // Wait for cancel processing + state stream remove propagation.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Resume all — should be no-ops (instances already cancelled).
    for i in 0..count {
        let id = format!("dsc_{i}");
        handles[0]
            .resume(&clients[0], &id, b"late")
            .await
            .unwrap();
    }

    // Verify no resumes fired.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        resumed.load(Ordering::SeqCst), 0,
        "resume should not fire after cancel"
    );

    for h in &handles { h.stop(); }
    server.shutdown().await;
}
