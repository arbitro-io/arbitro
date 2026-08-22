//! Fire-and-forget + sync publish smoke test.
//!
//! Verifies that all publish variants complete without errors against a
//! real `ArbitroServer`.  No subscription / delivery path exercised here.


use arbitro_client_tokio::{BatchEntry, Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Bind the socket HERE and hand it to the server, so there is nothing to
/// wait for: the port is listening before this returns, and it is never
/// released, so a sibling test cannot take it.
///
/// The previous version picked a port by binding, DROPPING the listener, and
/// returning the number — then slept 80ms hoping the server had bound it.
/// Both halves were races: under the full suite the machine is loaded, 80ms
/// was not enough, and the client hit ConnectionRefused; the freed port could
/// also be claimed by a sibling test in the same binary.
async fn start_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local_addr").to_string();
    let cfg = Config::default()
        .listen_addr(addr.clone())
        .max_connections(64);
    let mut server = ArbitroServer::new(cfg);
    server.set_listener(listener);
    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            panic!("test server exited: {e}");
        }
    });
    addr
}

async fn connect(addr: &str) -> Client {
    let cfg = ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    };
    Client::connect(cfg).await.expect("connect")
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_single_and_batch_no_errors() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    // Create a stream so the server accepts publishes.
    let resp = client
        .create_stream(b"pub-test", b"test.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream");
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // Fire-and-forget single publish (1000 frames — all async, lock-free).
    for i in 0u32..1000 {
        client
            .publish(
                stream_id,
                b"test.subject",
                Bytes::from(i.to_le_bytes().to_vec()),
            )
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
        .publish_wait(stream_id, b"test.sync", Bytes::from_static(b"payload"))
        .await
        .expect("publish_wait");

    client
        .delete_stream(b"pub-test")
        .await
        .expect("delete_stream");
    client.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publish_wait_no_timeout() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    let resp = client
        .create_stream(b"conc-test", b"conc.subject", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream");
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // 4 concurrent publish_wait goroutines — each sends 100 messages.
    let mut handles = Vec::new();
    for _ in 0..4 {
        let c = client.clone();
        let h = tokio::spawn(async move {
            for i in 0u32..100 {
                c.publish_wait(
                    stream_id,
                    b"conc.subject",
                    Bytes::from(i.to_le_bytes().to_vec()),
                )
                .await
                .expect("publish_wait in concurrent task");
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
