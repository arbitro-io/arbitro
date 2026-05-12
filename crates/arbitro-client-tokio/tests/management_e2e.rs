//! End-to-end management smoke test for the v2 ingress path.
//!
//! Spins up a real `ArbitroServer` on a random port, connects via the
//! pure-tokio `Client`, and exercises every CRUD method round-trip:
//! `create_stream`, `get_stream`, `list_streams`, `create_consumer`,
//! `list_consumers`, `delete_consumer`, `delete_stream`.
//!
//! The server is **v1-by-default**; this test proves the v2 HELLO switch
//! plus `dispatch_v2.rs` answers correctly to v2 frames.

use std::time::Duration;

use arbitro_client_tokio::{Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};

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
    let server = ArbitroServer::new(cfg);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    // Give the listener a moment to bind.
    tokio::time::sleep(Duration::from_millis(80)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    let cfg = ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    };
    Client::connect(cfg).await.expect("client connect")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_management_crud_roundtrip() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    // ── create_stream ───────────────────────────────────────────────────
    let resp = client
        .create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream");
    // RepOk body: first 8 bytes = ref_seq (the wire stream id, u64 LE).
    assert!(resp.len() >= 8, "RepOk body should carry ref_seq");
    let stream_wire_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
    assert_ne!(stream_wire_id, 0, "wire stream id should be non-zero");

    // ── get_stream ──────────────────────────────────────────────────────
    client
        .get_stream(b"orders")
        .await
        .expect("get_stream");

    // ── list_streams ────────────────────────────────────────────────────
    let body = client.list_streams(0, 100).await.expect("list_streams");
    assert!(body.len() >= 4, "list_streams body must contain count");
    let count = u32::from_le_bytes(body[..4].try_into().unwrap());
    assert!(count >= 1, "at least the freshly-created stream");

    // ── create_consumer ─────────────────────────────────────────────────
    let resp = client
        .create_consumer(
            stream_wire_id,
            b"worker",
            b"",
            b"",
            16,    // max_inflight
            0,     // ack_policy = None
            0,     // deliver_policy
            0,     // deliver_mode
            30_000,
            0,
        )
        .await
        .expect("create_consumer");
    assert!(resp.len() >= 8);
    let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
    assert_ne!(consumer_id, 0);

    // ── list_consumers ──────────────────────────────────────────────────
    let body = client.list_consumers(stream_wire_id, 0, 100).await.expect("list_consumers");
    assert!(body.len() >= 4);
    let count = u32::from_le_bytes(body[..4].try_into().unwrap());
    assert!(count >= 1);

    // ── delete_consumer ─────────────────────────────────────────────────
    client.delete_consumer(consumer_id).await.expect("delete_consumer");

    // ── delete_stream ───────────────────────────────────────────────────
    client.delete_stream(b"orders").await.expect("delete_stream");

    client.close();
}
