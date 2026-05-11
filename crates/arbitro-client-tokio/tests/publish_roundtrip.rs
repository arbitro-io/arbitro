//! Fire-and-forget + sync publish smoke test.
//!
//! Verifies that all publish variants complete without errors against a
//! real `ArbitroServer`.  No subscription / delivery path exercised here.

use std::time::Duration;

use bytes::Bytes;
use arbitro_client_tokio::{Client, ClientConfig, BatchEntry};
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
        .max_connections(64);
    tokio::spawn(async move { let _ = ArbitroServer::new(cfg).run().await; });
    tokio::time::sleep(Duration::from_millis(80)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    let cfg = ClientConfig { addr: addr.to_string(), ..ClientConfig::default() };
    Client::connect(cfg).await.expect("connect")
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_single_and_batch_no_errors() {
    let addr   = start_server().await;
    let client = connect(&addr).await;

    // Create a stream so the server accepts publishes.
    let resp = client
        .create_stream(b"pub-test", b">", 0, 0, 0, 1, 0, 0, 0)
        .await
        .expect("create_stream");
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // Fire-and-forget single publish (1000 frames — all async, lock-free).
    for i in 0u32..1000 {
        client
            .publish(stream_id, b"test.subject", Bytes::from(i.to_le_bytes().to_vec()))
            .expect("publish");
    }

    // Batch publish (100 entries).
    let entries: Vec<BatchEntry<'_>> = (0u32..100)
        .map(|i| BatchEntry::new(b"test.batch", Bytes::from(i.to_le_bytes().to_vec())))
        .collect();
    client
        .publish_batch(stream_id, &entries)
        .expect("publish_batch");

    // Sync publish — waits for broker RepOk.
    let _resp = client
        .publish_sync(stream_id, b"test.sync", Bytes::from_static(b"payload"))
        .await
        .expect("publish_sync");

    client.delete_stream(b"pub-test").await.expect("delete_stream");
    client.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publish_sync_no_timeout() {
    let addr   = start_server().await;
    let client = connect(&addr).await;

    let resp = client
        .create_stream(b"conc-test", b">", 0, 0, 0, 1, 0, 0, 0)
        .await
        .expect("create_stream");
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // 4 concurrent publish_sync goroutines — each sends 100 messages.
    let mut handles = Vec::new();
    for _ in 0..4 {
        let c = client.clone();
        let h = tokio::spawn(async move {
            for i in 0u32..100 {
                c.publish_sync(
                    stream_id,
                    b"conc.subject",
                    Bytes::from(i.to_le_bytes().to_vec()),
                )
                .await
                .expect("publish_sync in concurrent task");
            }
        });
        handles.push(h);
    }
    for h in handles {
        h.await.expect("task panicked");
    }

    client.delete_stream(b"conc-test").await.ok();
    client.close();
}
