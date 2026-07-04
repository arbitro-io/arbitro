//! idle_timeout tests.
//!
//! The server's keepalive sweep (`keepalive_loop` in
//! `arbitro-server/src/server.rs`) runs every `keepalive_interval` and
//! evicts any connection whose last activity is older than `idle_timeout`.
//! Eviction drops the connection's write sender, which closes the socket
//! on the client side.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::{ClientConfig, ReconnectPolicy};
use std::time::Duration;

const IDLE_TIMEOUT: Duration = Duration::from_secs(2);
// Sweep frequently relative to idle_timeout so eviction happens close to
// the deadline instead of lagging a full extra interval behind it.
const KEEPALIVE_INTERVAL: Duration = Duration::from_millis(300);

/// Client config with auto-reconnect disabled, so a server-initiated
/// disconnect is observable as an error on the next request rather than
/// being silently healed by a background reconnect.
fn no_reconnect_config(addr: &str) -> ClientConfig {
    ClientConfig {
        addr: addr.to_string(),
        reconnect: ReconnectPolicy {
            max_attempts: Some(0),
            ..Default::default()
        },
        ..ClientConfig::default()
    }
}


#[tokio::test(flavor = "multi_thread")]
async fn idle_client_disconnected_active_client_stays_connected() {
    let mut server = TestServerBuilder::new()
        .idle_timeout(IDLE_TIMEOUT)
        .keepalive_interval(KEEPALIVE_INTERVAL)
        .spawn()
        .await;

    let idle_client = server.connect_with_config(no_reconnect_config(&server.addr)).await;
    let active_client = server.connect_with_config(no_reconnect_config(&server.addr)).await;

    // Touch both once at t=0 via a real request (registry.touch runs on
    // every dispatched frame), then let the idle client go completely
    // silent while the active client keeps sending well within the
    // idle window.
    idle_client.list_streams(0, 10).await.unwrap();
    active_client.list_streams(0, 10).await.unwrap();

    let deadline = tokio::time::Instant::now() + IDLE_TIMEOUT + Duration::from_secs(3);
    let mut active_pings = 0u32;
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        active_client
            .list_streams(0, 10)
            .await
            .expect("active client must stay connected while it keeps sending requests");
        active_pings += 1;
    }
    assert!(active_pings > 0, "test did not exercise the active client");

    // The idle client has now been silent for well over IDLE_TIMEOUT —
    // the next request must fail because the server evicted it.
    // On some platforms (WSL) the TCP close isn't propagated promptly,
    // so a timeout is also valid evidence of eviction.
    let idle_result = tokio::time::timeout(
        Duration::from_secs(3),
        idle_client.list_streams(0, 10),
    )
    .await;
    match idle_result {
        Err(_timeout) => {}       // no response — server evicted (WSL)
        Ok(Err(_)) => {}          // error — server evicted (native)
        Ok(Ok(_)) => panic!(
            "idle client should have been disconnected after {:?} of inactivity",
            IDLE_TIMEOUT
        ),
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn active_client_survives_past_idle_timeout_with_steady_traffic() {
    let mut server = TestServerBuilder::new()
        .idle_timeout(IDLE_TIMEOUT)
        .keepalive_interval(KEEPALIVE_INTERVAL)
        .spawn()
        .await;

    let client = server.connect_with_config(no_reconnect_config(&server.addr)).await;

    client
        .create_stream(b"steady", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();

    // Send a request every 400ms for longer than 2x idle_timeout. If the
    // idle sweep were incorrectly evicting active connections, one of
    // these would fail.
    let rounds = 12; // 12 * 400ms = 4.8s, > 2x IDLE_TIMEOUT
    for i in 0..rounds {
        tokio::time::sleep(Duration::from_millis(400)).await;
        client
            .list_streams(0, 10)
            .await
            .unwrap_or_else(|e| panic!("round {i} failed, active client was wrongly evicted: {e:?}"));
    }

    let resp = client.list_streams(0, 1000).await.unwrap();
    assert_eq!(TestServer::stream_count(&resp), 1);

    server.shutdown().await;
}
