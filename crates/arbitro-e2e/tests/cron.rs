mod test_helper;
use test_helper::TestServerBuilder;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arbitro_client_tokio::Client;

// ══════════════════════════════════════════════════════════════════════════════
// 1. Basic cron — register, receive fire, verify
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn cron_basic_fire() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let fire_count = Arc::new(AtomicU64::new(0));
    let fc = fire_count.clone();

    let cron = client
        .cron(b"test-basic")
        .every(b"* * * * * *") // every second
        .run(move |_ctx| {
            let fc = fc.clone();
            async move {
                fc.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await
        .expect("create cron");

    // Wait 3 seconds — should fire at least 2 times
    tokio::time::sleep(Duration::from_secs(3)).await;

    let fires = fire_count.load(Ordering::Relaxed);
    assert!(fires >= 2, "expected ≥2 fires, got {fires}");

    cron.stop().await.unwrap();
    server.shutdown().await;
}

// ══════════════════════════════════════════════════════════════════════════════
// 2. Queue semantics — 10 workers, only 1 receives each fire
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn cron_10_workers_single_delivery() {
    let mut server = TestServerBuilder::new().spawn().await;

    // 10 workers all register the same cron name
    let total_fires = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    let mut clients = Vec::new();

    for _i in 0..10 {
        let client = server.connect().await;
        let tf = total_fires.clone();

        let cron = client
            .cron(b"shared-job")
            .every(b"* * * * * *") // every second
            .run(move |_ctx| {
                let tf = tf.clone();
                async move {
                    tf.fetch_add(1, Ordering::Relaxed);
                }
            })
            .await
            .expect("create cron");

        handles.push(cron);
        clients.push(client);
    }

    // Wait 5 seconds — cron should fire ~5 times
    tokio::time::sleep(Duration::from_secs(5)).await;

    let fires = total_fires.load(Ordering::Relaxed);

    // Key invariant: each fire goes to exactly ONE worker.
    // With 5 seconds and 1 fire/sec, we expect ~5 total fires across
    // all 10 workers combined (NOT 50 — that would mean all 10 got each fire).
    assert!(
        fires >= 3 && fires <= 7,
        "expected 3-7 total fires (one worker per fire), got {fires}. \
         If {fires} ≈ 50, the broker is delivering to ALL workers (fanout bug)."
    );

    for h in &handles {
        h.stop().await.unwrap();
    }
    server.shutdown().await;
}
