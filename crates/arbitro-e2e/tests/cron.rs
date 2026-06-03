mod test_helper;
use test_helper::TestServerBuilder;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arbitro_client_tokio::{ClientConfig, ReconnectPolicy};

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

    // Wait 4 seconds — should fire at least 2 times (1s cron, 4s window)
    tokio::time::sleep(Duration::from_secs(4)).await;

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

// ══════════════════════════════════════════════════════════════════════════════
// 3. Reconnect — server dies, restarts on same port, crons resume firing
// ══════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn cron_survives_server_restart() {
    // Bind a port we'll reuse across restarts.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    // Phase 1: start server, register cron, verify it fires.
    let mut server = TestServerBuilder::new().spawn_on(&addr).await;

    let fire_count = Arc::new(AtomicU64::new(0));
    let fc = fire_count.clone();

    let client = arbitro_client_tokio::Client::connect(ClientConfig {
        addr: addr.clone(),
        reconnect: ReconnectPolicy {
            base: Duration::from_millis(100),
            cap: Duration::from_millis(500),
            max_attempts: Some(30),
        },
        ..ClientConfig::default()
    })
    .await
    .expect("initial connect");

    let _cron = client
        .cron(b"reconnect-test")
        .every(b"*/2 * * * * *")
        .run(move |_ctx| {
            let fc = fc.clone();
            async move {
                fc.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await
        .expect("create cron");

    // Let it fire at least once (every 2s, wait 5s for margin).
    tokio::time::sleep(Duration::from_secs(5)).await;
    let before_restart = fire_count.load(Ordering::Relaxed);
    assert!(before_restart >= 1, "expected ≥1 fires before restart, got {before_restart}");

    // Phase 2: kill server.
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Phase 3: restart server on same port.
    let mut server2 = TestServerBuilder::new().spawn_on(&addr).await;

    // Wait for client to reconnect + re-register cron + fires to resume.
    // Reconnect + handshake + CreateCron round-trip can take 1-2s.
    // Cron fires every 2s, so 8s gives 3-4 chances.
    tokio::time::sleep(Duration::from_secs(8)).await;

    let after_restart = fire_count.load(Ordering::Relaxed);
    let new_fires = after_restart - before_restart;
    assert!(
        new_fires >= 1,
        "expected ≥1 new fires after server restart, got {new_fires} \
         (total: {after_restart}, before: {before_restart}). \
         Cron did not resume after reconnect."
    );

    server2.shutdown().await;
}
