mod test_helper;
use test_helper::TestServerBuilder;

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
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
