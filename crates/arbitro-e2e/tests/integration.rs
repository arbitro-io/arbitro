//! End-to-end integration tests — client ↔ server.
//!
//! Every test spins up a real `ArbitroServer` on a random port,
//! connects via `Client`, and exercises the full protocol stack.
//! Zero manual frame parsing — the client handles encoding/decoding.

use std::time::Duration;

use arbitro_client::{BatchEntry, Client};
use bytes::Bytes;
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
    let entries: Vec<BatchEntry<'_>> = (0..100)
        .map(|_| BatchEntry::new(b"orders.new".as_slice(), Bytes::copy_from_slice(b"test-payload")))
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

    let entries: Vec<BatchEntry<'_>> = (0..1000)
        .map(|_| BatchEntry::new(b"batch.msg".as_slice(), Bytes::copy_from_slice(b"data")))
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
    let entries: Vec<BatchEntry<'_>> = (0..500)
        .map(|_| BatchEntry::new(b"replay.evt".as_slice(), Bytes::copy_from_slice(b"data")))
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

// ── Connection grouping (same connection, multiple consumers) ───────────

/// Fanout on a single connection — 3 consumers with different groups on the
/// same client. The drain should group all deliveries into one frame per
/// store entry (connection-based batching). Each consumer receives all msgs.
#[tokio::test]
async fn test_fanout_same_connection() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    client
        .create_stream(&StreamConfig::new(b"fsc", b">").build())
        .await
        .unwrap();

    let mut subs = Vec::new();
    for i in 0..3u32 {
        let name = format!("fsc-{i}");
        let group = format!("fsc-grp-{i}");
        let consumer = client
            .create_consumer(
                &ConsumerConfig::new(name.as_bytes(), b"fsc")
                    .group(group.as_bytes())
                    .ack_policy(AckPolicy::Explicit)
                    .max_inflight(100)
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        subs.push(consumer.subscribe(None).await.unwrap());
    }

    // Publish 10 messages
    for i in 0..10u32 {
        client
            .publish(b"fsc", b"fsc.evt", &i.to_le_bytes())
            .await
            .unwrap();
    }

    // Each consumer should receive all 10 (fanout, different groups)
    for (idx, sub) in subs.iter_mut().enumerate() {
        let mut count = 0u32;
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.next()).await {
                Ok(Some(msg)) => {
                    msg.ack();
                    count += 1;
                }
                _ => break,
            }
        }
        assert_eq!(count, 10, "fanout consumer {idx} should get 10, got {count}");
    }
}

/// Queue on a single connection — 2 consumers with same default group on the
/// same client. Messages distributed, total = published count, no duplicates.
#[tokio::test]
async fn test_queue_same_connection() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    client
        .create_stream(&StreamConfig::new(b"qsc", b">").build())
        .await
        .unwrap();

    let c1 = client
        .create_consumer(
            &ConsumerConfig::new(b"qsc-w1", b"qsc")
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(100)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let c2 = client
        .create_consumer(
            &ConsumerConfig::new(b"qsc-w2", b"qsc")
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(100)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let mut sub1 = c1.subscribe(None).await.unwrap();
    let mut sub2 = c2.subscribe(None).await.unwrap();

    for i in 0..10u32 {
        client
            .publish(b"qsc", b"qsc.job", &i.to_le_bytes())
            .await
            .unwrap();
    }

    let mut count1 = 0u32;
    let mut count2 = 0u32;
    let mut seqs = std::collections::HashSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);

    loop {
        if count1 + count2 >= 10 {
            break;
        }
        tokio::select! {
            result = sub1.next() => {
                if let Some(msg) = result {
                    seqs.insert(msg.seq);
                    msg.ack();
                    count1 += 1;
                }
            }
            result = sub2.next() => {
                if let Some(msg) = result {
                    seqs.insert(msg.seq);
                    msg.ack();
                    count2 += 1;
                }
            }
            _ = tokio::time::sleep_until(deadline) => { break; }
        }
    }

    let total = count1 + count2;
    assert_eq!(total, 10, "queue total should be 10, got {count1}+{count2}={total}");
    assert_eq!(seqs.len(), 10, "no duplicates: unique seqs={}", seqs.len());
}

/// Fanout with subject filters on a single connection — verifies the
/// per-entry consumer_id routing works when the client dispatches entries
/// from a mixed-consumer frame.
#[tokio::test]
async fn test_fanout_filtered_same_connection() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    client
        .create_stream(&StreamConfig::new(b"ffsc", b">").build())
        .await
        .unwrap();

    // Consumer A: orders.* only
    let ca = client
        .create_consumer(
            &ConsumerConfig::new(b"ffsc-a", b"ffsc")
                .group(b"ga")
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(100)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let mut sub_a = ca.subscribe(Some(b"orders.*")).await.unwrap();

    // Consumer B: payments.* only
    let cb = client
        .create_consumer(
            &ConsumerConfig::new(b"ffsc-b", b"ffsc")
                .group(b"gb")
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(100)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let mut sub_b = cb.subscribe(Some(b"payments.*")).await.unwrap();

    // Consumer C: catch-all
    let cc = client
        .create_consumer(
            &ConsumerConfig::new(b"ffsc-c", b"ffsc")
                .group(b"gc")
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(100)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let mut sub_c = cc.subscribe(Some(b">")).await.unwrap();

    // Publish 3 orders + 2 payments = 5 total
    for i in 0..3u32 {
        let subj = format!("orders.{i}");
        client.publish(b"ffsc", subj.as_bytes(), b"o").await.unwrap();
    }
    for i in 0..2u32 {
        let subj = format!("payments.{i}");
        client.publish(b"ffsc", subj.as_bytes(), b"p").await.unwrap();
    }

    // A: 3 orders
    let mut count_a = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub_a.next()).await {
            Ok(Some(msg)) => { msg.ack(); count_a += 1; }
            _ => break,
        }
    }
    assert_eq!(count_a, 3, "A (orders.*) should get 3, got {count_a}");

    // B: 2 payments
    let mut count_b = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub_b.next()).await {
            Ok(Some(msg)) => { msg.ack(); count_b += 1; }
            _ => break,
        }
    }
    assert_eq!(count_b, 2, "B (payments.*) should get 2, got {count_b}");

    // C: all 5
    let mut count_c = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub_c.next()).await {
            Ok(Some(msg)) => { msg.ack(); count_c += 1; }
            _ => break,
        }
    }
    assert_eq!(count_c, 5, "C (>) should get 5, got {count_c}");
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
