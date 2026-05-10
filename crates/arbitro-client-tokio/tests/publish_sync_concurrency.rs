//! Concurrent `publish_sync` regression gate — MERGE GATE.
//!
//! The legacy `arbitro-client` deterministically timed out when two or more
//! cloned handles called `publish_sync` concurrently, because its single OS
//! thread + ack-loop architecture serialized all round-trips.
//!
//! This test proves `arbitro-client-tokio` handles conn=1/2/4/8 with zero
//! timeouts or `ChannelClosed` errors.  It must **always pass** before a
//! merge — any regression is a blocker.

use std::time::Duration;

use bytes::Bytes;
use arbitro_client_tokio::{Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};

// ── helpers ───────────────────────────────────────────────────────────────────

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

async fn start_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let cfg = Config::default()
        .listen_addr(addr.clone())
        .max_connections(128);
    tokio::spawn(async move { let _ = ArbitroServer::new(cfg).run().await; });
    tokio::time::sleep(Duration::from_millis(80)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    let cfg = ClientConfig { addr: addr.to_string(), ..ClientConfig::default() };
    Client::connect(cfg).await.expect("connect")
}

// ── MERGE GATE ────────────────────────────────────────────────────────────────

/// `conn_count` concurrent clones each call `publish_sync` 1 000 times.
/// Total round-trips = conn_count × 1 000.  All must complete within 15 s.
async fn run_concurrent_publish_sync(conn_count: usize, stream_id: u32, root: &Client) {
    const PER_CONN: u32 = 1_000;
    const BUDGET: Duration = Duration::from_secs(15);

    // Clone `conn_count` handles.  Max pool = 14, so conn_count ≤ 8 is safe
    // (root = 1 handle + 8 clones = 9 total, well within 15 max).
    let clients: Vec<Client> = (0..conn_count).map(|_| root.clone()).collect();

    let mut handles = Vec::with_capacity(conn_count);
    for c in clients {
        let sid = stream_id;
        let h = tokio::spawn(async move {
            for i in 0u32..PER_CONN {
                c.publish_sync(
                    sid,
                    b"conc.sync",
                    Bytes::from(i.to_le_bytes().to_vec()),
                )
                .await
                .unwrap_or_else(|e| panic!("publish_sync failed (conn_count={conn_count}, i={i}): {e}"));
            }
        });
        handles.push(h);
    }

    // Enforce a hard budget — any hang is a regression.
    let result = tokio::time::timeout(BUDGET, async {
        for h in handles {
            h.await.expect("spawned task panicked");
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "concurrent publish_sync timed out at conn_count={conn_count}  \
         (regression: old client failed at conn=2/4/8)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_publish_sync_no_timeout_conn_1_2_4_8() {
    let addr   = start_server().await;
    let root   = connect(&addr).await;

    let resp = root
        .create_stream(b"conc-sync-gate", b">", 0, 0, 0, 1, 0, 0, 0)
        .await
        .expect("create_stream");
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    for conn_count in [1usize, 2, 4, 8] {
        run_concurrent_publish_sync(conn_count, stream_id, &root).await;
    }

    root.delete_stream(b"conc-sync-gate").await.ok();
    root.close();
}
