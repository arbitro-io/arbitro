//! End-to-end invariant tests — real TCP client ↔ server.
//!
//! Each test starts a server on a random port, connects a client,
//! and verifies correctness through the public client API only.

use std::time::Duration;

use arbitro_client::Client;
use arbitro_proto::config::AckPolicy;
use arbitro_proto::config::{ConsumerConfig, StreamConfig};
use arbitro_server::{ArbitroServer, Config};
use tokio::sync::watch;

/// Start a server on a random port, return (shutdown_tx, addr).
async fn start_server() -> (watch::Sender<bool>, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener); // free the port for the server

    let (tx, rx) = watch::channel(false);
    let config = Config::default()
        .listen_addr(&addr)
        .shard_count(2)
        .channel_capacity(1024);

    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run_with_shutdown(rx).await;
    });

    // Give server a moment to bind
    tokio::time::sleep(Duration::from_millis(50)).await;

    (tx, addr)
}

/// Connect a client to the given address.
async fn connect(addr: &str) -> Client {
    Client::connect_with_timeout(addr, Duration::from_secs(2))
        .await
        .expect("client should connect")
}

// ═══════════════════════════════════════════════════════════════════════════
// Stream CRUD
// ═══════════════════════════════════════════════════════════════════════════

/// 1. Create stream → appears in list_streams.
#[tokio::test]
async fn stream_create_then_list() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let config = StreamConfig::new(b"orders", b">").build();
    client.create_stream(&config).await.unwrap();

    let streams = client.list_streams().await.unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].name, b"orders");
}

/// 2. Create duplicate stream → idempotent (no error).
#[tokio::test]
async fn stream_create_idempotent() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let config = StreamConfig::new(b"orders", b">").build();
    client.create_stream(&config).await.unwrap();
    // Second create should not fail
    client.create_stream(&config).await.unwrap();

    let streams = client.list_streams().await.unwrap();
    assert_eq!(streams.len(), 1);
}

/// 3. Delete stream → disappears from list_streams.
#[tokio::test]
async fn stream_delete_then_list() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let config = StreamConfig::new(b"events", b">").build();
    client.create_stream(&config).await.unwrap();
    assert_eq!(client.list_streams().await.unwrap().len(), 1);

    client.delete_stream(b"events").await.unwrap();
    assert_eq!(client.list_streams().await.unwrap().len(), 0);
}

/// 4. Publish to non-existent stream → error.
#[tokio::test]
async fn publish_to_missing_stream_errors() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let result = client.publish_sync(b"ghost", b"ghost_event", b"data").await;
    assert!(
        result.is_err(),
        "publish to non-existent stream should fail"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Consumer CRUD
// ═══════════════════════════════════════════════════════════════════════════

/// 5. Create consumer → returns valid ID.
#[tokio::test]
async fn consumer_create_returns_id() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"orders", b">").build();
    client.create_stream(&stream).await.unwrap();

    let consumer_cfg = ConsumerConfig::new(b"worker", b"orders")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
    assert!(consumer.id() > 0, "consumer ID should be non-zero");
}

/// 6. Delete consumer → clean removal.
#[tokio::test]
async fn consumer_delete() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"orders", b">").build();
    client.create_stream(&stream).await.unwrap();

    let consumer_cfg = ConsumerConfig::new(b"worker", b"orders")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
    let cid = consumer.id();

    consumer.delete().await.unwrap();

    // Deleting again should error (or be no-op)
    let result = client.delete_consumer(b"orders", cid).await;
    // Either error or idempotent — just shouldn't panic
    let _ = result;
}

// ═══════════════════════════════════════════════════════════════════════════
// Publish + Deliver
// ═══════════════════════════════════════════════════════════════════════════

/// 7. Publish single → subscriber receives correct subject + payload.
#[tokio::test]
async fn publish_single_delivers_correctly() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"chat", b">").build();
    client.create_stream(&stream).await.unwrap();

    let consumer_cfg = ConsumerConfig::new(b"reader", b"chat")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
    let mut sub = consumer.subscribe(None).await.unwrap();

    client
        .publish(b"chat", b"chat_hello", b"world")
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("should receive within timeout")
        .expect("subscription open");

    assert_eq!(&*msg.subject, b"chat_hello");
    assert_eq!(&msg.payload[..], b"world");
    msg.ack();
}

/// 8. Publish batch → all messages delivered.
#[tokio::test]
async fn publish_batch_delivers_all() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"logs", b">").build();
    client.create_stream(&stream).await.unwrap();

    let consumer_cfg = ConsumerConfig::new(b"sink", b"logs")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
    let mut sub = consumer.subscribe(None).await.unwrap();

    let entries: Vec<(&[u8], &[u8])> = (0..50).map(|_| (&b"logs_line"[..], &b"data"[..])).collect();
    client.publish_batch(b"logs", &entries).await.unwrap();

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

    assert_eq!(count, 50, "all 50 messages should be delivered");
}

/// 9. Publish returns monotonic sequences.
#[tokio::test]
async fn publish_sequences_monotonic() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"counter", b">").build();
    client.create_stream(&stream).await.unwrap();

    let seq1 = client
        .publish_sync(b"counter", b"counter_inc", b"1")
        .await
        .unwrap();

    let seq2 = client
        .publish_sync(b"counter", b"counter_inc", b"2")
        .await
        .unwrap();

    let seq3 = client
        .publish_sync(b"counter", b"counter_inc", b"3")
        .await
        .unwrap();

    assert!(seq2 > seq1, "seq2 ({seq2}) > seq1 ({seq1})");
    assert!(seq3 > seq2, "seq3 ({seq3}) > seq2 ({seq2})");
}

/// publish-before-subscribe replay: publish N msgs, then create consumer with
/// DeliverPolicy::All and subscribe — should receive all N historical messages.
#[tokio::test]
async fn replay_deliver_all_historical() {
    use arbitro_proto::config::DeliverPolicy;

    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"history", b">").build();
    client.create_stream(&stream).await.unwrap();

    // Publish 100 messages BEFORE any consumer exists.
    let entries: Vec<(&[u8], &[u8])> = (0..100).map(|_| (&b"history.evt"[..], &b"data"[..])).collect();
    client.publish_batch(b"history", &entries).await.unwrap();

    // Small delay so the shard drain loop processes publish_pending_to_engine.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Create consumer with DeliverPolicy::All — should replay from seq=1.
    let consumer = client
        .create_consumer(
            &ConsumerConfig::new(b"replayer", b"history")
                .deliver_policy(DeliverPolicy::All)
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let mut sub = consumer.subscribe(None).await.unwrap();

    let mut count = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub.next()).await {
            Ok(Some(_)) => count += 1,
            _ => break,
        }
    }

    assert_eq!(count, 100, "replay should deliver all 100 historical messages, got {count}");
}

// ═══════════════════════════════════════════════════════════════════════════
// Ack / Nack
// ═══════════════════════════════════════════════════════════════════════════

/// 10. Ack → message not redelivered.
#[tokio::test]
async fn ack_prevents_redelivery() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"acktest", b">").build();
    client.create_stream(&stream).await.unwrap();

    let consumer_cfg = ConsumerConfig::new(b"acker", b"acktest")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
    let mut sub = consumer.subscribe(None).await.unwrap();

    client
        .publish(b"acktest", b"acktest_msg", b"data")
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .unwrap()
        .unwrap();
    msg.ack();

    // No more messages should arrive
    let extra = tokio::time::timeout(Duration::from_millis(200), sub.next()).await;
    assert!(extra.is_err(), "after ack, no redelivery should happen");
}

/// 11. Nack → message redelivered.
#[tokio::test]
async fn nack_causes_redelivery() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"nacktest", b">").build();
    client.create_stream(&stream).await.unwrap();

    let consumer_cfg = ConsumerConfig::new(b"nacker", b"nacktest")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
    let mut sub = consumer.subscribe(None).await.unwrap();

    client
        .publish(b"nacktest", b"nacktest_msg", b"data")
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .unwrap()
        .unwrap();
    msg.nack();

    // Should get redelivered
    let redelivered = tokio::time::timeout(Duration::from_secs(2), sub.next()).await;
    assert!(
        redelivered.is_ok(),
        "after nack, message should be redelivered"
    );
    if let Ok(Some(msg)) = redelivered {
        assert_eq!(&msg.payload[..], b"data");
        msg.ack();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Ordering
// ═══════════════════════════════════════════════════════════════════════════

/// 12. Messages arrive in sequence order.
#[tokio::test]
async fn delivery_preserves_order() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"ordered", b">").build();
    client.create_stream(&stream).await.unwrap();

    let consumer_cfg = ConsumerConfig::new(b"reader", b"ordered")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
    let mut sub = consumer.subscribe(None).await.unwrap();

    for i in 0..20u32 {
        let payload = i.to_le_bytes();
        client
            .publish(b"ordered", b"ordered_seq", &payload)
            .await
            .unwrap();
    }

    let mut prev_seq = 0u64;
    for _ in 0..20 {
        let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
            .await
            .unwrap()
            .unwrap();
        assert!(
            msg.seq > prev_seq,
            "seq {} should be > prev {}",
            msg.seq,
            prev_seq
        );
        prev_seq = msg.seq;
        msg.ack();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Fan-out & Queue groups (overlapping consumers)
// ═══════════════════════════════════════════════════════════════════════════

/// 13. Fan-out — two consumers with DIFFERENT groups both receive every message.
#[tokio::test]
async fn fanout_two_consumers_each_receive_all() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"events", b">").build();
    client.create_stream(&stream).await.unwrap();

    // Two consumers with DIFFERENT groups → separate queues → fan-out
    let c1_cfg = ConsumerConfig::new(b"svc_a", b"events")
        .group(b"group_a")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();

    let c2_cfg = ConsumerConfig::new(b"svc_b", b"events")
        .group(b"group_b")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();

    let c1 = client.create_consumer(&c1_cfg).await.unwrap();
    let c2 = client.create_consumer(&c2_cfg).await.unwrap();

    let mut sub1 = c1.subscribe(None).await.unwrap();
    let mut sub2 = c2.subscribe(None).await.unwrap();

    // Publish 5 messages
    for i in 0..5u32 {
        client
            .publish(b"events", b"events_tick", &i.to_le_bytes())
            .await
            .unwrap();
    }

    // Both subscribers should receive all 5
    for (name, sub) in [("sub1", &mut sub1), ("sub2", &mut sub2)] {
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
        assert_eq!(count, 5, "{name} should receive all 5 messages");
    }
}

/// 14. Queue group — two consumers with the SAME group share messages (each delivered once).
#[tokio::test]
async fn queue_group_distributes_messages() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"tasks", b">").build();
    client.create_stream(&stream).await.unwrap();

    // Two consumers with the SAME default group → same queue → round-robin
    let c1_cfg = ConsumerConfig::new(b"worker1", b"tasks")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();
    let c2_cfg = ConsumerConfig::new(b"worker2", b"tasks")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();

    let c1 = client.create_consumer(&c1_cfg).await.unwrap();
    let c2 = client.create_consumer(&c2_cfg).await.unwrap();

    let mut sub1 = c1.subscribe(None).await.unwrap();
    let mut sub2 = c2.subscribe(None).await.unwrap();

    // Publish 10 messages
    for i in 0..10u32 {
        client
            .publish(b"tasks", b"tasks_job", &i.to_le_bytes())
            .await
            .unwrap();
    }

    // Drain both subscribers
    let mut count1 = 0u32;
    let mut count2 = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub1.next()).await {
            Ok(Some(msg)) => {
                msg.ack();
                count1 += 1;
            }
            _ => break,
        }
    }
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub2.next()).await {
            Ok(Some(msg)) => {
                msg.ack();
                count2 += 1;
            }
            _ => break,
        }
    }

    let total = count1 + count2;
    assert_eq!(
        total, 10,
        "total delivered should be 10, got {count1}+{count2}={total}"
    );
}

/// 14e. Fanout with subject filters — only matching subscriptions receive.
///
/// 3 consumers on separate connections, different groups (fanout), but each
/// subscribes to a different subject pattern. Publishing to `orders.new`
/// should only deliver to the consumer whose filter matches.
#[tokio::test]
async fn fanout_with_subject_filters() {
    let (_tx, addr) = start_server().await;

    let setup = connect(&addr).await;
    setup
        .create_stream(&StreamConfig::new(b"filt", b">").build())
        .await
        .unwrap();

    // Consumer A: subscribes to `orders.*`
    let cli_a = connect(&addr).await;
    let ca = cli_a
        .create_consumer(
            &ConsumerConfig::new(b"fa", b"filt")
                .group(b"ga")
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let mut sub_a = ca.subscribe(Some(b"orders.*")).await.unwrap();

    // Consumer B: subscribes to `payments.*`
    let cli_b = connect(&addr).await;
    let cb = cli_b
        .create_consumer(
            &ConsumerConfig::new(b"fb", b"filt")
                .group(b"gb")
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let mut sub_b = cb.subscribe(Some(b"payments.*")).await.unwrap();

    // Consumer C: subscribes to `>` (catch-all)
    let cli_c = connect(&addr).await;
    let cc = cli_c
        .create_consumer(
            &ConsumerConfig::new(b"fc", b"filt")
                .group(b"gc")
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let mut sub_c = cc.subscribe(Some(b">")).await.unwrap();

    // Publish: 3 orders, 2 payments
    let publisher = connect(&addr).await;
    for i in 0..3u32 {
        let subj = format!("orders.{i}");
        publisher
            .publish(b"filt", subj.as_bytes(), b"order-data")
            .await
            .unwrap();
    }
    for i in 0..2u32 {
        let subj = format!("payments.{i}");
        publisher
            .publish(b"filt", subj.as_bytes(), b"pay-data")
            .await
            .unwrap();
    }

    // Consumer A: should get 3 (orders.* only)
    let mut count_a = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub_a.next()).await {
            Ok(Some(msg)) => {
                assert!(
                    msg.subject.starts_with(b"orders."),
                    "A got unexpected subject: {:?}",
                    String::from_utf8_lossy(&msg.subject)
                );
                msg.ack();
                count_a += 1;
            }
            _ => break,
        }
    }
    assert_eq!(count_a, 3, "consumer A (orders.*) should get 3, got {count_a}");

    // Consumer B: should get 2 (payments.* only)
    let mut count_b = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub_b.next()).await {
            Ok(Some(msg)) => {
                assert!(
                    msg.subject.starts_with(b"payments."),
                    "B got unexpected subject: {:?}",
                    String::from_utf8_lossy(&msg.subject)
                );
                msg.ack();
                count_b += 1;
            }
            _ => break,
        }
    }
    assert_eq!(count_b, 2, "consumer B (payments.*) should get 2, got {count_b}");

    // Consumer C: should get all 5 (catch-all)
    let mut count_c = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub_c.next()).await {
            Ok(Some(msg)) => {
                msg.ack();
                count_c += 1;
            }
            _ => break,
        }
    }
    assert_eq!(count_c, 5, "consumer C (>) should get 5, got {count_c}");
}

/// 14f. Queue with subject filters — dedup only among matching bindings.
///
/// 2 consumers same group (queue mode). Consumer A subscribes to `orders.*`,
/// consumer B subscribes to `payments.*`. Publishing `orders.1` should ONLY
/// go to A (B's filter doesn't match, so no queue dedup conflict).
/// Publishing `payments.1` should ONLY go to B.
#[tokio::test]
async fn queue_with_subject_filters_no_false_dedup() {
    let (_tx, addr) = start_server().await;

    let setup = connect(&addr).await;
    setup
        .create_stream(&StreamConfig::new(b"qfilt", b">").build())
        .await
        .unwrap();

    // Same default group (queue mode) but different subject filters
    let cli_a = connect(&addr).await;
    let ca = cli_a
        .create_consumer(
            &ConsumerConfig::new(b"qfa", b"qfilt")
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let mut sub_a = ca.subscribe(Some(b"orders.*")).await.unwrap();

    let cli_b = connect(&addr).await;
    let cb = cli_b
        .create_consumer(
            &ConsumerConfig::new(b"qfb", b"qfilt")
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let mut sub_b = cb.subscribe(Some(b"payments.*")).await.unwrap();

    let publisher = connect(&addr).await;

    // Publish 3 orders + 2 payments
    for i in 0..3u32 {
        let subj = format!("orders.{i}");
        publisher
            .publish(b"qfilt", subj.as_bytes(), b"o")
            .await
            .unwrap();
    }
    for i in 0..2u32 {
        let subj = format!("payments.{i}");
        publisher
            .publish(b"qfilt", subj.as_bytes(), b"p")
            .await
            .unwrap();
    }

    // A should get all 3 orders (only filter that matches)
    let mut count_a = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub_a.next()).await {
            Ok(Some(msg)) => {
                assert!(
                    msg.subject.starts_with(b"orders."),
                    "A got unexpected: {:?}",
                    String::from_utf8_lossy(&msg.subject)
                );
                msg.ack();
                count_a += 1;
            }
            _ => break,
        }
    }
    assert_eq!(count_a, 3, "A (orders.*) should get 3, got {count_a}");

    // B should get all 2 payments (only filter that matches)
    let mut count_b = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub_b.next()).await {
            Ok(Some(msg)) => {
                assert!(
                    msg.subject.starts_with(b"payments."),
                    "B got unexpected: {:?}",
                    String::from_utf8_lossy(&msg.subject)
                );
                msg.ack();
                count_b += 1;
            }
            _ => break,
        }
    }
    assert_eq!(count_b, 2, "B (payments.*) should get 2, got {count_b}");
}

/// 14g. Queue with overlapping filters — same group, both match.
///
/// 2 consumers same group. A subscribes to `events.*`, B subscribes to `>`.
/// Publishing `events.x` matches BOTH, but queue dedup ensures only ONE
/// receives each message. Total = published count, no duplicates.
#[tokio::test]
async fn queue_overlapping_filters_no_duplicates() {
    let (_tx, addr) = start_server().await;

    let setup = connect(&addr).await;
    setup
        .create_stream(&StreamConfig::new(b"qovlp", b">").build())
        .await
        .unwrap();

    let cli_a = connect(&addr).await;
    let ca = cli_a
        .create_consumer(
            &ConsumerConfig::new(b"qoa", b"qovlp")
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let mut sub_a = ca.subscribe(Some(b"events.*")).await.unwrap();

    let cli_b = connect(&addr).await;
    let cb = cli_b
        .create_consumer(
            &ConsumerConfig::new(b"qob", b"qovlp")
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let mut sub_b = cb.subscribe(Some(b">")).await.unwrap();

    let publisher = connect(&addr).await;
    for i in 0..10u32 {
        let subj = format!("events.{i}");
        publisher
            .publish(b"qovlp", subj.as_bytes(), &i.to_le_bytes())
            .await
            .unwrap();
    }

    // Drain both concurrently
    let mut count_a = 0u32;
    let mut count_b = 0u32;
    let mut seqs = std::collections::HashSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);

    loop {
        if count_a + count_b >= 10 {
            break;
        }
        tokio::select! {
            result = sub_a.next() => {
                if let Some(msg) = result {
                    seqs.insert(msg.seq);
                    msg.ack();
                    count_a += 1;
                }
            }
            result = sub_b.next() => {
                if let Some(msg) = result {
                    seqs.insert(msg.seq);
                    msg.ack();
                    count_b += 1;
                }
            }
            _ = tokio::time::sleep_until(deadline) => { break; }
        }
    }

    let total = count_a + count_b;
    assert_eq!(total, 10, "total should be 10, got {count_a}+{count_b}={total}");
    assert_eq!(seqs.len(), 10, "all seqs unique (no duplicates), got {}", seqs.len());
}

/// 15. Consumers on different streams — publishing to one doesn't deliver to the other.
#[tokio::test]
async fn consumers_on_different_streams_isolated() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let s1 = StreamConfig::new(b"logs", b">").build();
    let s2 = StreamConfig::new(b"metrics", b">").build();
    client.create_stream(&s1).await.unwrap();
    client.create_stream(&s2).await.unwrap();

    // Unique consumer names (consumer_id = fnv1a of name, must be distinct)
    let c1 = ConsumerConfig::new(b"log_sink", b"logs")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();
    let c2 = ConsumerConfig::new(b"metric_sink", b"metrics")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();

    let consumer1 = client.create_consumer(&c1).await.unwrap();
    let consumer2 = client.create_consumer(&c2).await.unwrap();

    let mut sub1 = consumer1.subscribe(None).await.unwrap();
    let mut sub2 = consumer2.subscribe(None).await.unwrap();

    // Publish to logs only
    client
        .publish(b"logs", b"logs_line", b"hello")
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub1.next())
        .await
        .expect("sub1 should receive")
        .expect("channel open");
    assert_eq!(&msg.payload[..], b"hello");
    msg.ack();

    // metrics subscriber should not receive anything
    let leaked = tokio::time::timeout(Duration::from_millis(300), sub2.next()).await;
    assert!(
        leaked.is_err(),
        "metrics sub should not receive logs messages"
    );
}

/// 14b. Queue group — multiple clients (separate connections), same group.
///
/// Each consumer connects from its own TCP connection. 10 messages published,
/// total delivered across both must be exactly 10 (no duplicates).
#[tokio::test]
async fn queue_group_multi_client() {
    let (_tx, addr) = start_server().await;

    let stream = StreamConfig::new(b"qtasks", b">").build();
    let setup = connect(&addr).await;
    setup.create_stream(&stream).await.unwrap();

    // Two separate connections, each with its own consumer, same default group
    let client1 = connect(&addr).await;
    let client2 = connect(&addr).await;

    let c1 = client1
        .create_consumer(
            &ConsumerConfig::new(b"qw1", b"qtasks")
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let c2 = client2
        .create_consumer(
            &ConsumerConfig::new(b"qw2", b"qtasks")
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let mut sub1 = c1.subscribe(None).await.unwrap();
    let mut sub2 = c2.subscribe(None).await.unwrap();

    // Publish from a third connection
    let publisher = connect(&addr).await;
    for i in 0..10u32 {
        publisher
            .publish(b"qtasks", b"qtasks_job", &i.to_le_bytes())
            .await
            .unwrap();
    }

    // Drain both — collect concurrently with a shared counter
    let mut count1 = 0u32;
    let mut count2 = 0u32;
    let mut seqs = std::collections::HashSet::new();

    // Use select to drain both at once for fairness
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
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
        if count1 + count2 >= 10 {
            break;
        }
    }

    let total = count1 + count2;
    assert_eq!(total, 10, "queue total should be 10, got {count1}+{count2}={total}");
    assert_eq!(seqs.len(), 10, "all 10 seqs must be unique (no duplicates)");
    assert!(count1 > 0 || count2 > 0, "at least one worker got messages");
}

/// 14c. Fanout — multiple clients (separate connections), different groups.
///
/// Each consumer on its own TCP connection with a unique group.
/// 5 messages published → each consumer receives all 5.
#[tokio::test]
async fn fanout_multi_client() {
    let (_tx, addr) = start_server().await;

    let stream = StreamConfig::new(b"fevents", b">").build();
    let setup = connect(&addr).await;
    setup.create_stream(&stream).await.unwrap();

    // 3 separate connections, each with its own consumer and unique group
    let mut subs = Vec::new();
    for i in 0..3u32 {
        let cli = connect(&addr).await;
        let name = format!("fc{i}");
        let group = format!("fgrp{i}");
        let consumer = cli
            .create_consumer(
                &ConsumerConfig::new(name.as_bytes(), b"fevents")
                    .group(group.as_bytes())
                    .ack_policy(AckPolicy::Explicit)
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
        subs.push((cli, consumer.subscribe(None).await.unwrap()));
    }

    // Publish from yet another connection
    let publisher = connect(&addr).await;
    for i in 0..5u32 {
        publisher
            .publish(b"fevents", b"fevents_tick", &i.to_le_bytes())
            .await
            .unwrap();
    }

    // Each consumer should receive all 5
    for (idx, (_cli, sub)) in subs.iter_mut().enumerate() {
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
        assert_eq!(count, 5, "fanout consumer {idx} should receive all 5, got {count}");
    }
}

/// 14d. Queue group — 3 consumers on 3 connections, 100 messages.
///
/// Verifies no duplicates and full coverage at scale.
#[tokio::test]
async fn queue_group_three_clients_100_msgs() {
    let (_tx, addr) = start_server().await;

    let setup = connect(&addr).await;
    setup
        .create_stream(&StreamConfig::new(b"q3", b">").build())
        .await
        .unwrap();

    // 3 consumers on separate connections, same default group
    let cli0 = connect(&addr).await;
    let cli1 = connect(&addr).await;
    let cli2 = connect(&addr).await;

    let c0 = cli0
        .create_consumer(
            &ConsumerConfig::new(b"q3w0", b"q3")
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let c1 = cli1
        .create_consumer(
            &ConsumerConfig::new(b"q3w1", b"q3")
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let c2 = cli2
        .create_consumer(
            &ConsumerConfig::new(b"q3w2", b"q3")
                .ack_policy(AckPolicy::Explicit)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let mut sub0 = c0.subscribe(None).await.unwrap();
    let mut sub1 = c1.subscribe(None).await.unwrap();
    let mut sub2 = c2.subscribe(None).await.unwrap();

    // Publish 100 messages
    let publisher = connect(&addr).await;
    let entries: Vec<(&[u8], &[u8])> = (0..100)
        .map(|_| (&b"q3_job"[..], &b"work"[..]))
        .collect();
    publisher.publish_batch(b"q3", &entries).await.unwrap();

    // Drain all 3 concurrently
    let mut counts = [0u32; 3];
    let mut all_seqs = std::collections::HashSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    loop {
        let total: u32 = counts.iter().sum();
        if total >= 100 {
            break;
        }

        tokio::select! {
            result = sub0.next() => {
                if let Some(msg) = result {
                    all_seqs.insert(msg.seq);
                    msg.ack();
                    counts[0] += 1;
                }
            }
            result = sub1.next() => {
                if let Some(msg) = result {
                    all_seqs.insert(msg.seq);
                    msg.ack();
                    counts[1] += 1;
                }
            }
            result = sub2.next() => {
                if let Some(msg) = result {
                    all_seqs.insert(msg.seq);
                    msg.ack();
                    counts[2] += 1;
                }
            }
            _ = tokio::time::sleep_until(deadline) => { break; }
        }
    }

    let total: u32 = counts.iter().sum();
    assert_eq!(
        total, 100,
        "queue total should be 100, got {counts:?} = {total}"
    );
    assert_eq!(
        all_seqs.len(),
        100,
        "all 100 seqs must be unique (no duplicates), got {}",
        all_seqs.len()
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Isolation
// ═══════════════════════════════════════════════════════════════════════════

/// 16. Streams are independent — publish to one doesn't leak to another.
#[tokio::test]
async fn streams_are_isolated() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_a = StreamConfig::new(b"alpha", b">").build();
    let stream_b = StreamConfig::new(b"beta", b">").build();
    client.create_stream(&stream_a).await.unwrap();
    client.create_stream(&stream_b).await.unwrap();

    let consumer_b_cfg = ConsumerConfig::new(b"beta_reader", b"beta")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();
    let consumer_b = client.create_consumer(&consumer_b_cfg).await.unwrap();
    let mut sub_b = consumer_b.subscribe(None).await.unwrap();

    // Publish to stream alpha only
    client
        .publish(b"alpha", b"alpha_event", b"data")
        .await
        .unwrap();

    // Stream beta subscriber should receive nothing
    let leaked = tokio::time::timeout(Duration::from_millis(300), sub_b.next()).await;
    assert!(leaked.is_err(), "messages in alpha must not leak to beta");
}

// ═══════════════════════════════════════════════════════════════════════════
// AckSync
// ═══════════════════════════════════════════════════════════════════════════

/// 17. ack_sync waits for broker confirmation and returns Ok.
#[tokio::test]
async fn ack_sync_returns_ok() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"acksync", b">").build();
    client.create_stream(&stream).await.unwrap();

    let consumer_cfg = ConsumerConfig::new(b"syncer", b"acksync")
        .ack_policy(AckPolicy::Explicit)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
    let mut sub = consumer.subscribe(None).await.unwrap();

    client
        .publish(b"acksync", b"acksync_ev", b"payload")
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg.payload[..], b"payload");

    // ack_sync must return Ok — broker confirmed
    msg.ack_sync().await.expect("ack_sync should succeed");

    // No redelivery after confirmed ack
    let extra = tokio::time::timeout(Duration::from_millis(300), sub.next()).await;
    assert!(
        extra.is_err(),
        "after ack_sync, no redelivery should happen"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Inflight limits
// ═══════════════════════════════════════════════════════════════════════════

/// 18. max_inflight caps the number of unacked messages delivered.
///
/// With max_inflight=2 and 5 published messages, only 2 should arrive
/// before we ack. After acking one, a third arrives.
#[tokio::test]
async fn max_inflight_caps_delivery() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"inf_stream", b">").build();
    client.create_stream(&stream).await.unwrap();

    let consumer_cfg = ConsumerConfig::new(b"inf_consumer", b"inf_stream")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(2)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
    let mut sub = consumer.subscribe(None).await.unwrap();

    // Publish 5 messages
    for i in 0..5u8 {
        client
            .publish(b"inf_stream", b"inf_subj", &[i])
            .await
            .unwrap();
    }

    // Should receive exactly 2 (the inflight cap)
    let m1 = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .unwrap()
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .unwrap()
        .unwrap();

    // Third should NOT arrive — we're at the cap
    let blocked = tokio::time::timeout(Duration::from_millis(300), sub.next()).await;
    assert!(
        blocked.is_err(),
        "3rd message should be blocked by max_inflight=2"
    );

    // Ack one → frees a slot → third arrives
    m1.ack_sync().await.unwrap();

    let m3 = tokio::time::timeout(Duration::from_secs(2), sub.next()).await;
    assert!(m3.is_ok(), "after ack, 3rd message should arrive");

    // Cleanup
    m2.ack();
    if let Ok(Some(msg)) = m3 {
        msg.ack();
    }
}

/// 19. Multiple max_subject_inflight patterns with different limits.
///
/// Three tiers:
///   - `message.premium.>` → limit 3
///   - `message.freemium.>` → limit 1
///   - `other.*` → no limit (uncapped)
///
/// Publish 5× premium, 3× freemium, 3× other.
/// Initial delivery: 3 premium + 1 freemium + 3 other = 7.
/// After acking 1 premium → 4th premium arrives.
/// After acking 1 freemium → 2nd freemium arrives.
#[tokio::test]
async fn max_subject_inflight_multiple_patterns() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream = StreamConfig::new(b"msi", b">").build();
    client.create_stream(&stream).await.unwrap();

    let consumer_cfg = ConsumerConfig::new(b"msi_c", b"msi")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(100)
        .max_subject_inflight(b"message.premium.>", 3)
        .max_subject_inflight(b"message.freemium.>", 1)
        .build()
        .unwrap();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
    let mut sub = consumer.subscribe(None).await.unwrap();

    // Single batch publish — all 11 land in one shard command, one drain sees all.
    let entries: Vec<(&[u8], &[u8])> = vec![
        (b"other.x", b"O0"),
        (b"other.x", b"O1"),
        (b"other.x", b"O2"),
        (b"message.freemium.events", b"F0"),
        (b"message.freemium.events", b"F1"),
        (b"message.freemium.events", b"F2"),
        (b"message.premium.orders", b"P0"),
        (b"message.premium.orders", b"P1"),
        (b"message.premium.orders", b"P2"),
        (b"message.premium.orders", b"P3"),
        (b"message.premium.orders", b"P4"),
    ];
    client.publish_batch(b"msi", &entries).await.unwrap();

    // Let all publishes and drain cycles settle
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Collect initial burst
    let mut received = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(500), sub.next()).await {
            Ok(Some(msg)) => received.push(msg),
            _ => break,
        }
    }

    let premium_count = received
        .iter()
        .filter(|m| m.subject.starts_with(b"message.premium."))
        .count();
    let freemium_count = received
        .iter()
        .filter(|m| m.subject.starts_with(b"message.freemium."))
        .count();
    let other_count = received
        .iter()
        .filter(|m| m.subject.starts_with(b"other."))
        .count();

    assert_eq!(
        premium_count, 3,
        "premium should be capped at 3, got {premium_count}"
    );
    assert_eq!(
        freemium_count, 1,
        "freemium should be capped at 1, got {freemium_count}"
    );
    assert_eq!(
        other_count, 3,
        "other has no cap, all 3 should arrive, got {other_count}"
    );
    assert_eq!(
        received.len(),
        7,
        "total initial should be 7, got {}",
        received.len()
    );

    // ── Ack one premium → 4th premium should arrive ──
    let first_premium = received
        .iter()
        .find(|m| m.subject.starts_with(b"message.premium."))
        .unwrap();
    first_premium.ack_sync().await.unwrap();

    let next = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("4th premium should arrive after ack");
    let msg = next.unwrap();
    assert!(
        msg.subject.starts_with(b"message.premium."),
        "expected premium, got {:?}",
        String::from_utf8_lossy(&msg.subject)
    );

    // ── Ack one freemium → 2nd freemium should arrive ──
    let first_freemium = received
        .iter()
        .find(|m| m.subject.starts_with(b"message.freemium."))
        .unwrap();
    first_freemium.ack_sync().await.unwrap();

    let next = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("2nd freemium should arrive after ack");
    let msg = next.unwrap();
    assert!(
        msg.subject.starts_with(b"message.freemium."),
        "expected freemium, got {:?}",
        String::from_utf8_lossy(&msg.subject)
    );

    // Cleanup
    for msg in &received {
        msg.ack();
    }
}
