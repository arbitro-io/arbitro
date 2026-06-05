mod test_helper;
use test_helper::TestServerBuilder;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arbitro_client_tokio::workflow::{StepContext, StepResult};

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
