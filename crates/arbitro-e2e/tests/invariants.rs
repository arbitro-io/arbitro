mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use std::time::Duration;
use bytes::Bytes;
use arbitro_client_tokio::{BatchEntry, SubjectLimit};


// ═══════════════════════════════════════════════════════════════════════════
// Stream CRUD
// ═══════════════════════════════════════════════════════════════════════════

/// 1. Create stream → appears in list_streams.
#[tokio::test(flavor = "multi_thread")]
async fn stream_create_then_list() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    client.create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();

    let resp = client.list_streams(0, 1000).await.unwrap();
    assert_eq!(TestServer::stream_count(&resp), 1);
    let names = TestServer::stream_names(&resp);
    assert_eq!(names[0], b"orders");
    server.shutdown().await;
}

/// 2. Create duplicate stream → idempotent (no error).
#[tokio::test(flavor = "multi_thread")]
async fn stream_create_idempotent() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    client.create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    // Second create should not fail
    client.create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();

    let resp = client.list_streams(0, 1000).await.unwrap();
    assert_eq!(TestServer::stream_count(&resp), 1);
    server.shutdown().await;
}

/// 3. Delete stream → disappears from list_streams.
#[tokio::test(flavor = "multi_thread")]
async fn create_and_list_streams() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    client.create_stream(b"events", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();

    let resp = client.list_streams(0, 1000).await.unwrap();
    assert_eq!(TestServer::stream_count(&resp), 1);

    client.delete_stream(b"events").await.unwrap();
    let resp = client.list_streams(0, 1000).await.unwrap();
    assert_eq!(TestServer::stream_count(&resp), 0);
    server.shutdown().await;
}

/// 4. Publish to non-existent stream → error.
#[tokio::test(flavor = "multi_thread")]
async fn publish_to_missing_stream_errors() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    // Use a non-existent stream_id (e.g., u32::MAX)
    let result = client
        .publish_sync(u32::MAX, b"ghost_event", Bytes::copy_from_slice(b"data"))
        .await;
    assert!(
        result.is_err(),
        "publish to non-existent stream should fail"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Consumer CRUD
// ═══════════════════════════════════════════════════════════════════════════

/// 5. Create consumer → returns valid ID.
#[tokio::test(flavor = "multi_thread")]
async fn create_consumer_and_delete() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id, b"worker", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);

    client.delete_consumer(consumer_id).await.unwrap();

    // Deleting again should be fine (returning success or specific error, but not crashing)
    let _ = client.delete_consumer(consumer_id).await;
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Publish + Deliver
// ═══════════════════════════════════════════════════════════════════════════

/// 7. Publish single → subscriber receives correct subject + payload.
#[tokio::test(flavor = "multi_thread")]
async fn publish_single_delivers_correctly() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"chat", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id, b"reader", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    client
        .publish_sync(stream_id, b"chat_hello", Bytes::copy_from_slice(b"world"))
        .await
        .expect("publish");

    let msg = tokio::time::timeout(Duration::from_secs(2), handle.recv())
        .await
        .expect("should receive within timeout")
        .expect("subscription open");

    assert_eq!(msg.subject(), b"chat_hello");
    assert_eq!(&msg.payload()[..], b"world");
    msg.ack();
    server.shutdown().await;
}

/// 8. Publish batch → all messages delivered.
#[tokio::test(flavor = "multi_thread")]
async fn publish_batch_delivers_all() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"logs", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id, b"sink", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    let entries: Vec<BatchEntry<'_>> = (0..50)
        .map(|_| BatchEntry::new(&b"logs_line"[..], Bytes::copy_from_slice(b"data")))
        .collect();
    client.publish_batch_sync(stream_id, &entries).await.expect("publish_batch");

    let mut count = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), handle.recv()).await {
            Ok(Some(msg)) => {
                msg.ack();
                count += 1;
            }
            _ => break,
        }
    }

    assert_eq!(count, 50, "all 50 messages should be delivered");
    server.shutdown().await;
}

/// 9. Publish returns monotonic sequences.
#[tokio::test(flavor = "multi_thread")]
async fn publish_sequences_monotonic() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"counter", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp1 = client
        .publish_sync(stream_id, b"counter_inc", Bytes::copy_from_slice(b"1"))
        .await
        .unwrap();
    let seq1 = u64::from_le_bytes(resp1[..8].try_into().unwrap());

    let resp2 = client
        .publish_sync(stream_id, b"counter_inc", Bytes::copy_from_slice(b"2"))
        .await
        .unwrap();
    let seq2 = u64::from_le_bytes(resp2[..8].try_into().unwrap());

    let resp3 = client
        .publish_sync(stream_id, b"counter_inc", Bytes::copy_from_slice(b"3"))
        .await
        .unwrap();
    let seq3 = u64::from_le_bytes(resp3[..8].try_into().unwrap());

    assert!(seq2 > seq1, "seq2 ({seq2}) > seq1 ({seq1})");
    assert!(seq3 > seq2, "seq3 ({seq3}) > seq2 ({seq2})");
    server.shutdown().await;
}

/// publish-before-subscribe replay: publish N msgs, then create consumer with
/// deliver_policy=All (0) and subscribe — should receive all N historical messages.
#[tokio::test(flavor = "multi_thread")]
async fn replay_deliver_all_historical() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"history", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // Publish 100 messages BEFORE any consumer exists.
    let entries: Vec<BatchEntry<'_>> = (0..100)
        .map(|_| BatchEntry::new(&b"history.evt"[..], Bytes::copy_from_slice(b"data")))
        .collect();
    client.publish_batch_sync(stream_id, &entries).await.expect("publish_batch");

    // Create consumer with deliver_policy=0 (All) — should replay from seq=1.
    let resp = client
        .create_consumer(stream_id, b"replayer", b"", b"", 200u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);

    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    let mut count = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), handle.recv()).await {
            Ok(Some(_)) => count += 1,
            _ => break,
        }
    }

    assert_eq!(count, 100, "replay should deliver all 100 historical messages, got {count}");
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Ack / Nack
// ═══════════════════════════════════════════════════════════════════════════

/// 10. Ack → message not redelivered.
#[tokio::test(flavor = "multi_thread")]
async fn ack_prevents_redelivery() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"acktest", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id, b"acker", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    client
        .publish_sync(stream_id, b"acktest_msg", Bytes::copy_from_slice(b"data"))
        .await
        .expect("publish");

    let msg = tokio::time::timeout(Duration::from_secs(2), handle.recv())
        .await
        .unwrap()
        .unwrap();
    msg.ack();

    // No more messages should arrive
    let extra = tokio::time::timeout(Duration::from_millis(200), handle.recv()).await;
    assert!(extra.is_err(), "after ack, no redelivery should happen");
    server.shutdown().await;
}

/// 11. Nack → message redelivered.
#[tokio::test(flavor = "multi_thread")]
async fn nack_causes_redelivery() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"nacktest", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id, b"nacker", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    client
        .publish_sync(stream_id, b"nacktest_msg", Bytes::copy_from_slice(b"data"))
        .await
        .expect("publish");

    let msg = tokio::time::timeout(Duration::from_secs(2), handle.recv())
        .await
        .unwrap()
        .unwrap();
    msg.nack();

    // Should get redelivered
    let redelivered = tokio::time::timeout(Duration::from_secs(2), handle.recv()).await;
    assert!(
        redelivered.is_ok(),
        "after nack, message should be redelivered"
    );
    if let Ok(Some(msg)) = redelivered {
        assert_eq!(&msg.payload()[..], b"data");
        msg.ack();
    }
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Ordering
// ═══════════════════════════════════════════════════════════════════════════

/// 12. Messages arrive in sequence order.
#[tokio::test(flavor = "multi_thread")]
async fn delivery_preserves_order() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"ordered", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id, b"reader", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    for i in 0..20u32 {
        let payload = i.to_le_bytes();
        client
            .publish_sync(stream_id, b"ordered_seq", Bytes::copy_from_slice(&payload))
            .await
            .expect("publish");
    }

    let mut prev_seq = 0u64;
    for _ in 0..20 {
        let msg = tokio::time::timeout(Duration::from_secs(2), handle.recv())
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
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Fan-out & Queue groups (overlapping consumers)
// ═══════════════════════════════════════════════════════════════════════════

/// 13. Fan-out — two consumers with DIFFERENT groups both receive every message.
#[tokio::test(flavor = "multi_thread")]
async fn fanout_two_consumers_each_receive_all() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"events", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // Two consumers with DIFFERENT groups → separate queues → fan-out
    let resp = client
        .create_consumer(stream_id, b"svc_a", b"group_a", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid1 = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id, b"svc_b", b"group_b", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid2 = TestServer::parse_id(&resp);

    let mut handle1 = client.subscribe(stream_id, cid1, b"").await.unwrap();
    let mut handle2 = client.subscribe(stream_id, cid2, b"").await.unwrap();

    // Publish 5 messages
    for i in 0..5u32 {
        client
            .publish_sync(stream_id, b"events_tick", Bytes::copy_from_slice(&i.to_le_bytes()))
            .await
            .expect("publish");
    }

    // Both subscribers should receive all 5
    for (name, handle) in [("sub1", &mut handle1), ("sub2", &mut handle2)] {
        let mut count = 0u32;
        loop {
            match tokio::time::timeout(Duration::from_secs(2), handle.recv()).await {
                Ok(Some(msg)) => {
                    msg.ack();
                    count += 1;
                }
                _ => break,
            }
        }
        assert_eq!(count, 5, "{name} should receive all 5 messages");
    }
    server.shutdown().await;
}

/// 14. Queue group — two consumers with the SAME group share messages (each delivered once).
#[tokio::test(flavor = "multi_thread")]
async fn queue_group_distributes_messages() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"tasks", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // Two consumers with the SAME group + deliver_mode=1 (Queue) → round-robin
    let resp = client
        .create_consumer(stream_id, b"worker1", b"workers", b"", 100u16, 1u8, 0u8, 1u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid1 = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id, b"worker2", b"workers", b"", 100u16, 1u8, 0u8, 1u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid2 = TestServer::parse_id(&resp);

    let mut handle1 = client.subscribe(stream_id, cid1, b"").await.unwrap();
    let mut handle2 = client.subscribe(stream_id, cid2, b"").await.unwrap();

    // Publish 10 messages
    for i in 0..10u32 {
        client
            .publish_sync(stream_id, b"tasks_job", Bytes::copy_from_slice(&i.to_le_bytes()))
            .await
            .expect("publish");
    }

    // Drain both subscribers
    let mut count1 = 0u32;
    let mut count2 = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), handle1.recv()).await {
            Ok(Some(msg)) => {
                msg.ack();
                count1 += 1;
            }
            _ => break,
        }
    }
    loop {
        match tokio::time::timeout(Duration::from_secs(2), handle2.recv()).await {
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
    server.shutdown().await;
}

/// 14e. Fanout with subject filters — only matching subscriptions receive.
///
/// 3 consumers on separate connections, different groups (fanout), but each
/// subscribes to a different subject pattern. Publishing to `orders.new`
/// should only deliver to the consumer whose filter matches.
#[tokio::test(flavor = "multi_thread")]
async fn fanout_with_subject_filters() {
    let mut server = TestServerBuilder::new().spawn().await;

    let setup = server.connect().await;
    let resp = setup
        .create_stream(b"filt", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // Consumer A: subscribes to `orders.*`
    let cli_a = server.connect().await;
    let resp = cli_a
        .create_consumer(stream_id, b"fa", b"ga", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid_a = TestServer::parse_id(&resp);
    let mut handle_a = cli_a.subscribe(stream_id, cid_a, b"orders.*").await.unwrap();

    // Consumer B: subscribes to `payments.*`
    let cli_b = server.connect().await;
    let resp = cli_b
        .create_consumer(stream_id, b"fb", b"gb", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid_b = TestServer::parse_id(&resp);
    let mut handle_b = cli_b.subscribe(stream_id, cid_b, b"payments.*").await.unwrap();

    // Consumer C: subscribes to `>` (catch-all)
    let cli_c = server.connect().await;
    let resp = cli_c
        .create_consumer(stream_id, b"fc", b"gc", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid_c = TestServer::parse_id(&resp);
    let mut handle_c = cli_c.subscribe(stream_id, cid_c, b">").await.unwrap();

    // Publish: 3 orders, 2 payments
    let publisher = server.connect().await;
    for i in 0..3u32 {
        let subj = format!("orders.{i}");
        publisher
            .publish_sync(stream_id, subj.as_bytes(), Bytes::copy_from_slice(b"order-data"))
            .await
            .expect("publish");
    }
    for i in 0..2u32 {
        let subj = format!("payments.{i}");
        publisher
            .publish_sync(stream_id, subj.as_bytes(), Bytes::copy_from_slice(b"pay-data"))
            .await
            .expect("publish");
    }

    // Consumer A: should get 3 (orders.* only)
    let mut count_a = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), handle_a.recv()).await {
            Ok(Some(msg)) => {
                assert!(
                    msg.subject().starts_with(b"orders."),
                    "A got unexpected subject: {:?}",
                    String::from_utf8_lossy(msg.subject())
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
        match tokio::time::timeout(Duration::from_secs(2), handle_b.recv()).await {
            Ok(Some(msg)) => {
                assert!(
                    msg.subject().starts_with(b"payments."),
                    "B got unexpected subject: {:?}",
                    String::from_utf8_lossy(msg.subject())
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
        match tokio::time::timeout(Duration::from_secs(2), handle_c.recv()).await {
            Ok(Some(msg)) => {
                msg.ack();
                count_c += 1;
            }
            _ => break,
        }
    }
    assert_eq!(count_c, 5, "consumer C (>) should get 5, got {count_c}");
    server.shutdown().await;
}

/// 14f. Queue with subject filters — dedup only among matching bindings.
///
/// 2 consumers same group (queue mode). Consumer A subscribes to `orders.*`,
/// consumer B subscribes to `payments.*`. Publishing `orders.1` should ONLY
/// go to A (B's filter doesn't match, so no queue dedup conflict).
/// Publishing `payments.1` should ONLY go to B.
#[tokio::test(flavor = "multi_thread")]
async fn queue_with_subject_filters_no_false_dedup() {
    let mut server = TestServerBuilder::new().spawn().await;

    let setup = server.connect().await;
    let resp = setup
        .create_stream(b"qfilt", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // Same default group (queue mode) but different subject filters
    let cli_a = server.connect().await;
    let resp = cli_a
        .create_consumer(stream_id, b"qfa", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid_a = TestServer::parse_id(&resp);
    let mut handle_a = cli_a.subscribe(stream_id, cid_a, b"orders.*").await.unwrap();

    let cli_b = server.connect().await;
    let resp = cli_b
        .create_consumer(stream_id, b"qfb", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid_b = TestServer::parse_id(&resp);
    let mut handle_b = cli_b.subscribe(stream_id, cid_b, b"payments.*").await.unwrap();

    let publisher = server.connect().await;

    // Publish 3 orders + 2 payments
    for i in 0..3u32 {
        let subj = format!("orders.{i}");
        publisher
            .publish_sync(stream_id, subj.as_bytes(), Bytes::copy_from_slice(b"o"))
            .await
            .expect("publish");
    }
    for i in 0..2u32 {
        let subj = format!("payments.{i}");
        publisher
            .publish_sync(stream_id, subj.as_bytes(), Bytes::copy_from_slice(b"p"))
            .await
            .expect("publish");
    }

    // A should get all 3 orders (only filter that matches)
    let mut count_a = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), handle_a.recv()).await {
            Ok(Some(msg)) => {
                assert!(
                    msg.subject().starts_with(b"orders."),
                    "A got unexpected: {:?}",
                    String::from_utf8_lossy(msg.subject())
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
        match tokio::time::timeout(Duration::from_secs(2), handle_b.recv()).await {
            Ok(Some(msg)) => {
                assert!(
                    msg.subject().starts_with(b"payments."),
                    "B got unexpected: {:?}",
                    String::from_utf8_lossy(msg.subject())
                );
                msg.ack();
                count_b += 1;
            }
            _ => break,
        }
    }
    assert_eq!(count_b, 2, "B (payments.*) should get 2, got {count_b}");
    server.shutdown().await;
}

/// 14g. Queue with overlapping filters — same group, both match.
///
/// 2 consumers same group. A subscribes to `events.*`, B subscribes to `>`.
/// Publishing `events.x` matches BOTH, but queue dedup ensures only ONE
/// receives each message. Total = published count, no duplicates.
#[tokio::test(flavor = "multi_thread")]
async fn queue_overlapping_filters_no_duplicates() {
    let mut server = TestServerBuilder::new().spawn().await;

    let setup = server.connect().await;
    let resp = setup
        .create_stream(b"qovlp", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let cli_a = server.connect().await;
    let resp = cli_a
        .create_consumer(stream_id, b"qoa", b"qovlp-group", b"", 100u16, 1u8, 0u8, 1u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid_a = TestServer::parse_id(&resp);
    let mut handle_a = cli_a.subscribe(stream_id, cid_a, b"events.*").await.unwrap();

    let cli_b = server.connect().await;
    let resp = cli_b
        .create_consumer(stream_id, b"qob", b"qovlp-group", b"", 100u16, 1u8, 0u8, 1u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid_b = TestServer::parse_id(&resp);
    let mut handle_b = cli_b.subscribe(stream_id, cid_b, b">").await.unwrap();

    let publisher = server.connect().await;
    for i in 0..10u32 {
        let subj = format!("events.{i}");
        publisher
            .publish_sync(stream_id, subj.as_bytes(), Bytes::copy_from_slice(&i.to_le_bytes()))
            .await
            .expect("publish");
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
            result = handle_a.recv() => {
                if let Some(msg) = result {
                    seqs.insert(msg.seq);
                    msg.ack();
                    count_a += 1;
                }
            }
            result = handle_b.recv() => {
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
    server.shutdown().await;
}

/// 15. Consumers on different streams — publishing to one doesn't deliver to the other.
#[tokio::test(flavor = "multi_thread")]
async fn consumers_on_different_streams_isolated() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp1 = client.create_stream(b"logs", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id1 = TestServer::parse_id(&resp1);
    let resp2 = client.create_stream(b"metrics", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id2 = TestServer::parse_id(&resp2);

    let resp = client
        .create_consumer(stream_id1, b"log_sink", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid1 = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id2, b"metric_sink", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid2 = TestServer::parse_id(&resp);

    let mut handle1 = client.subscribe(stream_id1, cid1, b"").await.unwrap();
    let mut handle2 = client.subscribe(stream_id2, cid2, b"").await.unwrap();

    // Publish to logs only
    client
        .publish_sync(stream_id1, b"logs_line", Bytes::copy_from_slice(b"hello"))
        .await
        .expect("publish");

    let msg = tokio::time::timeout(Duration::from_secs(2), handle1.recv())
        .await
        .expect("handle1 should receive")
        .expect("channel open");
    assert_eq!(&msg.payload()[..], b"hello");
    msg.ack();

    // metrics subscriber should not receive anything
    let leaked = tokio::time::timeout(Duration::from_millis(300), handle2.recv()).await;
    assert!(
        leaked.is_err(),
        "metrics sub should not receive logs messages"
    );
    server.shutdown().await;
}

/// 14b. Queue group — multiple clients (separate connections), same group.
///
/// Each consumer connects from its own TCP connection. 10 messages published,
/// total delivered across both must be exactly 10 (no duplicates).
#[tokio::test(flavor = "multi_thread")]
async fn queue_group_multi_client() {
    let mut server = TestServerBuilder::new().spawn().await;

    let setup = server.connect().await;
    let resp = setup
        .create_stream(b"qtasks", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // Two separate connections, each with its own consumer, same queue group
    let client1 = server.connect().await;
    let client2 = server.connect().await;

    let resp = client1
        .create_consumer(stream_id, b"qw1", b"qtasks-group", b"", 100u16, 1u8, 0u8, 1u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid1 = TestServer::parse_id(&resp);

    let resp = client2
        .create_consumer(stream_id, b"qw2", b"qtasks-group", b"", 100u16, 1u8, 0u8, 1u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid2 = TestServer::parse_id(&resp);

    let mut handle1 = client1.subscribe(stream_id, cid1, b"").await.unwrap();
    let mut handle2 = client2.subscribe(stream_id, cid2, b"").await.unwrap();

    // Publish from a third connection
    let publisher = server.connect().await;
    for i in 0..10u32 {
        publisher
            .publish_sync(stream_id, b"qtasks_job", Bytes::copy_from_slice(&i.to_le_bytes()))
            .await
            .expect("publish");
    }

    // Drain both — collect concurrently with a shared counter
    let mut count1 = 0u32;
    let mut count2 = 0u32;
    let mut seqs = std::collections::HashSet::new();

    // Use select to drain both at once for fairness
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        tokio::select! {
            result = handle1.recv() => {
                if let Some(msg) = result {
                    seqs.insert(msg.seq);
                    msg.ack();
                    count1 += 1;
                }
            }
            result = handle2.recv() => {
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
    server.shutdown().await;
}

/// 14c. Fanout — multiple clients (separate connections), different groups.
///
/// Each consumer on its own TCP connection with a unique group.
/// 5 messages published → each consumer receives all 5.
#[tokio::test(flavor = "multi_thread")]
async fn fanout_multi_client() {
    let mut server = TestServerBuilder::new().spawn().await;

    let setup = server.connect().await;
    let resp = setup
        .create_stream(b"fevents", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // 3 separate connections, each with its own consumer and unique group
    let mut handles = Vec::new();
    for i in 0..3u32 {
        let cli = server.connect().await;
        let name = format!("fc{i}");
        let group = format!("fgrp{i}");
        let resp = cli
            .create_consumer(stream_id, name.as_bytes(), group.as_bytes(), b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
            .await
            .unwrap();
        let consumer_id = TestServer::parse_id(&resp);
        let sub = cli.subscribe(stream_id, consumer_id, b"").await.unwrap();
        handles.push((cli, sub));
    }

    // Publish from yet another connection
    let publisher = server.connect().await;
    for i in 0..5u32 {
        publisher
            .publish_sync(stream_id, b"fevents_tick", Bytes::copy_from_slice(&i.to_le_bytes()))
            .await
            .expect("publish");
    }

    // Each consumer should receive all 5
    for (idx, (_cli, handle)) in handles.iter_mut().enumerate() {
        let mut count = 0u32;
        loop {
            match tokio::time::timeout(Duration::from_secs(2), handle.recv()).await {
                Ok(Some(msg)) => {
                    msg.ack();
                    count += 1;
                }
                _ => break,
            }
        }
        assert_eq!(count, 5, "fanout consumer {idx} should receive all 5, got {count}");
    }
    server.shutdown().await;
}

/// 14d. Queue group — 3 consumers on 3 connections, 100 messages.
///
/// Verifies no duplicates and full coverage at scale.
#[tokio::test(flavor = "multi_thread")]
async fn queue_group_three_clients_100_msgs() {
    let mut server = TestServerBuilder::new().spawn().await;

    let setup = server.connect().await;
    let resp = setup
        .create_stream(b"q3", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // 3 consumers on separate connections, same queue group
    let cli0 = server.connect().await;
    let cli1 = server.connect().await;
    let cli2 = server.connect().await;

    let resp = cli0
        .create_consumer(stream_id, b"q3w0", b"q3-group", b"", 100u16, 1u8, 0u8, 1u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid0 = TestServer::parse_id(&resp);

    let resp = cli1
        .create_consumer(stream_id, b"q3w1", b"q3-group", b"", 100u16, 1u8, 0u8, 1u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid1 = TestServer::parse_id(&resp);

    let resp = cli2
        .create_consumer(stream_id, b"q3w2", b"q3-group", b"", 100u16, 1u8, 0u8, 1u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid2 = TestServer::parse_id(&resp);

    let mut handle0 = cli0.subscribe(stream_id, cid0, b"").await.unwrap();
    let mut handle1 = cli1.subscribe(stream_id, cid1, b"").await.unwrap();
    let mut handle2 = cli2.subscribe(stream_id, cid2, b"").await.unwrap();

    // Publish 100 messages
    let publisher = server.connect().await;
    let entries: Vec<BatchEntry<'_>> = (0..100)
        .map(|_| BatchEntry::new(&b"q3_job"[..], Bytes::copy_from_slice(b"work")))
        .collect();
    publisher.publish_batch_sync(stream_id, &entries).await.expect("publish_batch");

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
            result = handle0.recv() => {
                if let Some(msg) = result {
                    all_seqs.insert(msg.seq);
                    msg.ack();
                    counts[0] += 1;
                }
            }
            result = handle1.recv() => {
                if let Some(msg) = result {
                    all_seqs.insert(msg.seq);
                    msg.ack();
                    counts[1] += 1;
                }
            }
            result = handle2.recv() => {
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
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Isolation
// ═══════════════════════════════════════════════════════════════════════════

/// 16. Streams are independent — publish to one doesn't leak to another.
#[tokio::test(flavor = "multi_thread")]
async fn streams_are_isolated() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp_a = client.create_stream(b"alpha", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id_a = TestServer::parse_id(&resp_a);
    let resp_b = client.create_stream(b"beta", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id_b = TestServer::parse_id(&resp_b);

    let resp = client
        .create_consumer(stream_id_b, b"beta_reader", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let cid_b = TestServer::parse_id(&resp);
    let mut handle_b = client.subscribe(stream_id_b, cid_b, b"").await.unwrap();

    // Publish to stream alpha only
    client
        .publish_sync(stream_id_a, b"alpha_event", Bytes::copy_from_slice(b"data"))
        .await
        .expect("publish");

    // Stream beta subscriber should receive nothing
    let leaked = tokio::time::timeout(Duration::from_millis(300), handle_b.recv()).await;
    assert!(leaked.is_err(), "messages in alpha must not leak to beta");
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// AckSync (now ack() + sleep)
// ═══════════════════════════════════════════════════════════════════════════

/// 17. ack waits for broker confirmation and returns Ok.
#[tokio::test(flavor = "multi_thread")]
async fn ack_sync_returns_ok() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"acksync", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id, b"syncer", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    client
        .publish_sync(stream_id, b"acksync_ev", Bytes::copy_from_slice(b"payload"))
        .await
        .expect("publish");

    let msg = tokio::time::timeout(Duration::from_secs(2), handle.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg.payload()[..], b"payload");

    // ack (fire-and-forget) + sleep to ensure ack is processed before checking redelivery
    msg.ack();
    tokio::time::sleep(Duration::from_millis(30)).await;

    // No redelivery after confirmed ack
    let extra = tokio::time::timeout(Duration::from_millis(300), handle.recv()).await;
    assert!(
        extra.is_err(),
        "after ack, no redelivery should happen"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Inflight limits
// ═══════════════════════════════════════════════════════════════════════════

/// 18. max_inflight caps the number of unacked messages delivered.
///
/// With max_inflight=2 and 5 published messages, only 2 should arrive
/// before we ack. After acking one, a third arrives.
#[tokio::test(flavor = "multi_thread")]
async fn max_inflight_caps_delivery() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"inf_stream", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id, b"inf_consumer", b"", b"", 2u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Publish 5 messages
    for i in 0..5u8 {
        client
            .publish_sync(stream_id, b"inf_subj", Bytes::copy_from_slice(&[i]))
            .await
            .expect("publish");
    }

    // Should receive exactly 2 (the inflight cap)
    let m1 = tokio::time::timeout(Duration::from_secs(2), handle.recv())
        .await
        .unwrap()
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_secs(2), handle.recv())
        .await
        .unwrap()
        .unwrap();

    // Third should NOT arrive — we're at the cap
    let blocked = tokio::time::timeout(Duration::from_millis(300), handle.recv()).await;
    assert!(
        blocked.is_err(),
        "3rd message should be blocked by max_inflight=2"
    );

    // Ack one → frees a slot → third arrives
    m1.ack();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let m3 = tokio::time::timeout(Duration::from_secs(2), handle.recv()).await;
    assert!(m3.is_ok(), "after ack, 3rd message should arrive");

    // Cleanup
    m2.ack();
    if let Ok(Some(msg)) = m3 {
        msg.ack();
    }
    server.shutdown().await;
}

/// 19. Multiple max_subject_inflight patterns with different limits.
///
/// Exercises the wire path end-to-end: client packs subject limits in
/// the `CreateConsumer` trailer, server parses them and calls
/// `engine.set_max_subject_inflight` per pattern. Two patterns are
/// configured with different caps; a third subject has no cap and
/// flows freely.
#[tokio::test(flavor = "multi_thread")]
async fn max_subject_inflight_multiple_patterns() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"msi", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // Per-subject inflight caps: premium 3, freemium 1. `other.*` has no cap.
    let limits = [
        SubjectLimit { pattern: b"message.premium.>",  limit: 3 },
        SubjectLimit { pattern: b"message.freemium.>", limit: 1 },
    ];
    let resp = client
        .create_consumer_with_limits(
            stream_id, b"msi_c", b"", b"",
            100u16, 1u8, 0u8, 0u8, 0u32, 0u64,
            &limits,
        )
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Single batch publish — all 11 land in one shard command, one drain sees all.
    let entries: Vec<BatchEntry<'_>> = vec![
        BatchEntry::new(b"other.x", Bytes::copy_from_slice(b"O0")),
        BatchEntry::new(b"other.x", Bytes::copy_from_slice(b"O1")),
        BatchEntry::new(b"other.x", Bytes::copy_from_slice(b"O2")),
        BatchEntry::new(b"message.freemium.events", Bytes::copy_from_slice(b"F0")),
        BatchEntry::new(b"message.freemium.events", Bytes::copy_from_slice(b"F1")),
        BatchEntry::new(b"message.freemium.events", Bytes::copy_from_slice(b"F2")),
        BatchEntry::new(b"message.premium.orders", Bytes::copy_from_slice(b"P0")),
        BatchEntry::new(b"message.premium.orders", Bytes::copy_from_slice(b"P1")),
        BatchEntry::new(b"message.premium.orders", Bytes::copy_from_slice(b"P2")),
        BatchEntry::new(b"message.premium.orders", Bytes::copy_from_slice(b"P3")),
        BatchEntry::new(b"message.premium.orders", Bytes::copy_from_slice(b"P4")),
    ];
    client.publish_batch_sync(stream_id, &entries).await.expect("publish_batch");

    // Let all publishes and drain cycles settle
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Collect initial burst
    let mut received = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(500), handle.recv()).await {
            Ok(Some(msg)) => received.push(msg),
            _ => break,
        }
    }

    let premium_count = received
        .iter()
        .filter(|m| m.subject().starts_with(b"message.premium."))
        .count();
    let freemium_count = received
        .iter()
        .filter(|m| m.subject().starts_with(b"message.freemium."))
        .count();
    let other_count = received
        .iter()
        .filter(|m| m.subject().starts_with(b"other."))
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

    // Cleanup
    for msg in received {
        msg.ack();
    }
    server.shutdown().await;
}
