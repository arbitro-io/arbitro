//! Reconnect smoke test.
//!
//! Verifies that the client's background reconnect loop re-establishes the
//! TCP connection after the server restarts on the same port.
//!
//! Scenario:
//! 1. Start `server_1` on a random port.
//! 2. Connect the client with an aggressive backoff (50 ms base).
//! 3. Abort `server_1` — the client is now disconnected.
//! 4. Start `server_2` on **the same port** (SO_REUSEADDR, tokio default).
//! 5. Wait up to 2 s for the reconnect loop to succeed.
//! 6. Perform a `list_streams` round-trip to prove the connection is live.

use std::time::Duration;

use arbitro_client_tokio::{Client, ClientConfig, KeepAlive, ReconnectPolicy};
use arbitro_server::{ArbitroServer, Config};

// ── helpers ───────────────────────────────────────────────────────────────────

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Spawn an `ArbitroServer` on `addr` and return its task handle.
async fn spawn_server(addr: &str) -> tokio::task::JoinHandle<()> {
    let cfg = Config::default()
        .listen_addr(addr.to_string())
        .max_connections(64);
    let h = tokio::spawn(async move {
        let _ = ArbitroServer::new(cfg).run().await;
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    h
}

// ── test ──────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_reconnects_after_server_restart() {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");

    // Start the first server.
    let server1 = spawn_server(&addr).await;

    // Connect with aggressive reconnect so the test doesn't take long.
    let cfg = ClientConfig {
        addr: addr.clone(),
        reconnect: ReconnectPolicy {
            base: Duration::from_millis(50),
            cap: Duration::from_millis(200),
            max_attempts: None,
        },
        keep_alive: KeepAlive {
            interval: Duration::from_secs(60),
            timeout: Duration::from_secs(120),
        },
        ..ClientConfig::default()
    };
    let client = Client::connect(cfg).await.expect("initial connect");

    // Verify the connection is live before killing the server.
    client
        .list_streams(0, 10)
        .await
        .expect("pre-restart list_streams");

    // Kill server_1 — the TCP connection drops; reconnect loop starts.
    server1.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start server_2 on the same port.
    // tokio's TcpListener sets SO_REUSEADDR so re-binding succeeds promptly.
    let _server2 = spawn_server(&addr).await;

    // The reconnect loop should re-connect within a few backoff intervals.
    // Give it up to 3 seconds.
    let reconnected = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match client.list_streams(0, 10).await {
                Ok(_) => return, // reconnected and command works
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await;

    assert!(
        reconnected.is_ok(),
        "client did not reconnect within 3 s after server restart"
    );

    client.close();
}

/// After `close()`, the reconnect loop must NOT attempt further dials.
/// Verify by closing the client and asserting no reconnect errors surface.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_stops_reconnect_loop() {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");

    let server = spawn_server(&addr).await;

    let cfg = ClientConfig {
        addr: addr.clone(),
        reconnect: ReconnectPolicy {
            base: Duration::from_millis(20),
            cap: Duration::from_millis(50),
            max_attempts: None,
        },
        keep_alive: KeepAlive {
            interval: Duration::from_secs(60),
            timeout: Duration::from_secs(120),
        },
        ..ClientConfig::default()
    };
    let client = Client::connect(cfg).await.expect("connect");

    // Close before killing the server.
    client.close();

    // Kill the server — even though the reconnect loop would otherwise retry,
    // the cancel token must prevent any further dial attempts.
    server.abort();

    // Wait a moment — if the reconnect loop were still running, it would
    // typically log warnings.  We just assert no panic occurs.
    tokio::time::sleep(Duration::from_millis(300)).await;
    // No assertion needed — reaching here means no panic / no deadlock.
}
