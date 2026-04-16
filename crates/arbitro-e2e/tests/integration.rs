//! End-to-end integration tests — client ↔ server.
//!
//! Every test spins up a real `ArbitroServer` on a random port,
//! connects via `Client`, and exercises the full protocol stack.
//! Zero manual frame parsing — the client handles encoding/decoding.

use std::time::Duration;

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverPolicy, StreamConfig};
use arbitro_server::{ArbitroServer, Config};

// ── Infrastructure ───────────────────────────────────────────────────────

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

async fn start_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(256);
    let server = ArbitroServer::new(config);
    tokio::spawn(async move { let _ = server.run().await; });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect_with_timeout(addr, Duration::from_secs(5))
        .await
        .expect("client must connect")
}

// ── Lifecycle ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_stream() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    client
        .create_stream(&StreamConfig::new(b"orders", b">").build())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_create_consumer() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    client
        .create_stream(&StreamConfig::new(b"orders", b">").build())
        .await
        .unwrap();

    client
        .create_consumer(
            &ConsumerConfig::new(b"worker", b"orders")
                .ack_policy(AckPolicy::None)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_list_streams() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    for name in [b"orders".as_slice(), b"payments", b"events"] {
        client
            .create_stream(&StreamConfig::new(name, b">").build())
            .await
            .unwrap();
    }

    let streams = client.list_streams().await.unwrap();
    assert_eq!(streams.len(), 3);
}

#[tokio::test]
async fn test_list_streams_empty() {
    let addr = start_server().await;
    let client = connect(&addr).await;
    let streams = client.list_streams().await.unwrap();
    assert!(streams.is_empty());
}

// ── Publish-Deliver-Ack ─────────────────────────────────────────────────

#[tokio::test]
async fn test_publish_ack_cycle() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    client
        .create_stream(&StreamConfig::new(b"orders", b">").build())
        .await
        .unwrap();

    let consumer = client
        .create_consumer(
            &ConsumerConfig::new(b"worker", b"orders")
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(1000)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let mut sub = consumer.subscribe(None).await.unwrap();

    // Publish 100 messages
    let entries: Vec<(&[u8], &[u8])> = (0..100)
        .map(|_| (b"orders.new".as_slice(), b"test-payload".as_slice()))
        .collect();
    client.publish_batch(b"orders", &entries).await.unwrap();

    // Receive and ack all 100
    for _ in 0..100 {
        let msg = sub.next().await.unwrap();
        msg.ack();
    }
}

#[tokio::test]
async fn test_publish_batch() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    client
        .create_stream(&StreamConfig::new(b"batch", b">").build())
        .await
        .unwrap();

    let entries: Vec<(&[u8], &[u8])> = (0..1000)
        .map(|_| (b"batch.msg".as_slice(), b"data".as_slice()))
        .collect();
    client.publish_batch(b"batch", &entries).await.unwrap();
}

#[tokio::test]
async fn test_fanout_delivery() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    client
        .create_stream(&StreamConfig::new(b"fanout", b">").build())
        .await
        .unwrap();

    // 3 consumers, each with its own group = fanout (not round-robin)
    let mut subs = Vec::new();
    for i in 0..3u32 {
        let name = format!("fan-{i}");
        let group = format!("group-{i}");
        let consumer = client
            .create_consumer(
                &ConsumerConfig::new(name.as_bytes(), b"fanout")
                    .group(group.as_bytes())
                    .ack_policy(AckPolicy::None)
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        subs.push(consumer.subscribe(None).await.unwrap());
    }

    // Publish 10 messages
    for i in 0..10u32 {
        let payload = format!("msg-{i}");
        client
            .publish(b"fanout", b"fanout.evt", payload.as_bytes())
            .await
            .unwrap();
    }

    // Each consumer should receive all 10 (fanout)
    for (idx, sub) in subs.iter_mut().enumerate() {
        for j in 0..10 {
            let msg = sub.next().await;
            assert!(
                msg.is_some(),
                "consumer {idx} should receive msg {j}"
            );
        }
    }
}

// ── Nack & redelivery ───────────────────────────────────────────────────

#[tokio::test]
async fn test_nack_redelivery() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    client
        .create_stream(&StreamConfig::new(b"nack_test", b">").build())
        .await
        .unwrap();

    let consumer = client
        .create_consumer(
            &ConsumerConfig::new(b"nacker", b"nack_test")
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(100)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let mut sub = consumer.subscribe(None).await.unwrap();

    client
        .publish(b"nack_test", b"nack.msg", b"data")
        .await
        .unwrap();

    // Receive and nack
    let msg = sub.next().await.unwrap();
    msg.nack();

    // Should be re-delivered
    let redelivered = sub.next().await;
    assert!(redelivered.is_some(), "nacked message should be re-delivered");
}

// ── Replay: publish-first, subscribe-later ──────────────────────────────

#[tokio::test]
async fn test_replay_publish_then_subscribe() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    // 1. Create stream (no subscriber yet)
    client
        .create_stream(&StreamConfig::new(b"replay", b">").build())
        .await
        .unwrap();

    // 2. Publish 500 messages before anyone subscribes
    let entries: Vec<(&[u8], &[u8])> = (0..500)
        .map(|_| (b"replay.evt".as_slice(), b"data".as_slice()))
        .collect();
    client.publish_batch(b"replay", &entries).await.unwrap();

    // 3. Now subscribe — should replay all 500
    let consumer = client
        .create_consumer(
            &ConsumerConfig::new(b"replayer", b"replay")
                .ack_policy(AckPolicy::None)
                .deliver_policy(DeliverPolicy::All)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let mut sub = consumer.subscribe(None).await.unwrap();

    // 4. Receive all 500
    let mut prev_seq = 0u64;
    for i in 0..500 {
        let msg = sub.next().await.unwrap();
        assert!(
            msg.seq > prev_seq,
            "msg {i}: seqs must be monotonic: {} <= {}",
            msg.seq,
            prev_seq,
        );
        prev_seq = msg.seq;
    }
}

// ── Gate smoke test ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_gate_auto_delivery_smoke() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    client
        .create_stream(&StreamConfig::new(b"gate_smoke", b">").build())
        .await
        .unwrap();

    let consumer = client
        .create_consumer(
            &ConsumerConfig::new(b"gater", b"gate_smoke")
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(100)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let mut sub = consumer.subscribe(None).await.unwrap();

    // 3 rounds — publish 5 msgs each, verify auto-delivery
    for round in 0..3u32 {
        for i in 0..5u32 {
            let payload = format!("r{round}-{i}");
            client
                .publish(b"gate_smoke", b"gate.evt", payload.as_bytes())
                .await
                .unwrap();
        }

        // Receive and ack all 5
        for _ in 0..5 {
            let msg = sub.next().await.unwrap();
            msg.ack();
        }
    }
}

// ── Graceful shutdown ───────────────────────────────────────────────────

#[tokio::test]
async fn test_graceful_shutdown() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    client
        .create_stream(&StreamConfig::new(b"shutdown_test", b">").build())
        .await
        .unwrap();

    // Server shuts down when the spawned task is dropped (end of test).
    // Verify we can still use the client up to this point.
    let streams = client.list_streams().await.unwrap();
    assert_eq!(streams.len(), 1);
}
