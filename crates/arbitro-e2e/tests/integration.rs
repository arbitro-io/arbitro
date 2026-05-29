mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::BatchEntry;
use bytes::Bytes;
use std::time::Duration;

// ── Lifecycle ─────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_create_stream() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    client
        .create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_consumer() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let resp = client
        .create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let sid = TestServer::parse_id(&resp);
    client
        .create_consumer(sid, b"worker", b"", b"", u16::MAX, 0, 0, 0, 0, 0)
        .await
        .unwrap();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_streams() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    for name in [b"orders".as_slice(), b"payments", b"events"] {
        client
            .create_stream(name, b">", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
    }
    let resp = client.list_streams(0, 1000).await.unwrap();
    assert_eq!(TestServer::stream_count(&resp), 3);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_streams_empty() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let resp = client.list_streams(0, 1000).await.unwrap();
    assert_eq!(TestServer::stream_count(&resp), 0);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_publish_ack_cycle() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let sid = TestServer::parse_id(&resp);
    let resp = client
        .create_consumer(sid, b"worker", b"", b"", 1000, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let cid = TestServer::parse_id(&resp);
    let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

    let entries: Vec<BatchEntry<'_>> = (0..100)
        .map(|_| BatchEntry::new(b"orders.new", Bytes::copy_from_slice(b"test-payload")))
        .collect();
    client.publish_batch_sync(sid, &entries).await.expect("publish_batch");

    for _ in 0..100 {
        let msg = sub.recv().await.unwrap();
        msg.ack();
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_publish_batch() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let resp = client
        .create_stream(b"batch", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let sid = TestServer::parse_id(&resp);
    let entries: Vec<BatchEntry<'_>> = (0..1000)
        .map(|_| BatchEntry::new(b"batch.msg", Bytes::copy_from_slice(b"data")))
        .collect();
    client.publish_batch_sync(sid, &entries).await.expect("publish_batch");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fanout_delivery() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let resp = client
        .create_stream(b"fanout", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let sid = TestServer::parse_id(&resp);

    let mut subs = Vec::new();
    for i in 0..3u32 {
        let name = format!("fan-{i}");
        let group = format!("group-{i}");
        let resp = client
            .create_consumer(
                sid,
                name.as_bytes(),
                group.as_bytes(),
                b"",
                u16::MAX,
                0,
                0,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        let cid = TestServer::parse_id(&resp);
        subs.push(client.subscribe(sid, cid, b"").await.unwrap());
    }

    for i in 0..10u32 {
        let payload = format!("msg-{i}");
        client
            .publish_sync(
                sid,
                b"fanout.evt",
                Bytes::copy_from_slice(payload.as_bytes()),
            )
            .await
            .expect("publish");
    }

    for (idx, sub) in subs.iter_mut().enumerate() {
        for j in 0..10 {
            assert!(
                sub.recv().await.is_some(),
                "consumer {idx} should receive msg {j}"
            );
        }
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nack_redelivery() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let resp = client
        .create_stream(b"nack_test", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let sid = TestServer::parse_id(&resp);
    let resp = client
        .create_consumer(sid, b"nacker", b"", b"", 100, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let cid = TestServer::parse_id(&resp);
    let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

    client
        .publish_sync(sid, b"nack.msg", Bytes::copy_from_slice(b"data"))
        .await
        .expect("publish");
    let msg = sub.recv().await.unwrap();
    msg.nack();
    assert!(
        sub.recv().await.is_some(),
        "nacked message should be re-delivered"
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replay_publish_then_subscribe() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let resp = client
        .create_stream(b"replay", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let sid = TestServer::parse_id(&resp);

    let entries: Vec<BatchEntry<'_>> = (0..500)
        .map(|_| BatchEntry::new(b"replay.evt", Bytes::copy_from_slice(b"data")))
        .collect();
    client.publish_batch_sync(sid, &entries).await.expect("publish_batch");

    let resp = client
        .create_consumer(sid, b"replayer", b"", b"", u16::MAX, 0, 0, 0, 0, 0)
        .await
        .unwrap();
    let cid = TestServer::parse_id(&resp);
    let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

    let mut prev_seq = 0u64;
    for i in 0..500 {
        let msg = sub.recv().await.unwrap();
        assert!(
            msg.seq > prev_seq,
            "msg {i}: seqs must be monotonic: {} <= {}",
            msg.seq,
            prev_seq
        );
        prev_seq = msg.seq;
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_gate_auto_delivery_smoke() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let resp = client
        .create_stream(b"gate_smoke", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let sid = TestServer::parse_id(&resp);
    let resp = client
        .create_consumer(sid, b"gater", b"", b"", 100, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let cid = TestServer::parse_id(&resp);
    let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

    for round in 0..3u32 {
        for i in 0..5u32 {
            let payload = format!("r{round}-{i}");
            client
                .publish_sync(sid, b"gate.evt", Bytes::copy_from_slice(payload.as_bytes()))
                .await
                .expect("publish");
        }
        for _ in 0..5 {
            let msg = sub.recv().await.unwrap();
            msg.ack();
        }
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fanout_same_connection() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let resp = client
        .create_stream(b"fsc", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let sid = TestServer::parse_id(&resp);

    let mut subs = Vec::new();
    for i in 0..3u32 {
        let name = format!("fsc-{i}");
        let group = format!("fsc-grp-{i}");
        let resp = client
            .create_consumer(
                sid,
                name.as_bytes(),
                group.as_bytes(),
                b"",
                100,
                1,
                0,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        let cid = TestServer::parse_id(&resp);
        subs.push(client.subscribe(sid, cid, b"").await.unwrap());
    }

    for i in 0..10u32 {
        client
            .publish_sync(sid, b"fsc.evt", Bytes::copy_from_slice(&i.to_le_bytes()))
            .await
            .expect("publish");
    }

    for (idx, sub) in subs.iter_mut().enumerate() {
        let mut count = 0u32;
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
                Ok(Some(msg)) => {
                    msg.ack();
                    count += 1;
                }
                _ => break,
            }
        }
        assert_eq!(
            count, 10,
            "fanout consumer {idx} should get 10, got {count}"
        );
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_queue_same_connection() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let resp = client
        .create_stream(b"qsc", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let sid = TestServer::parse_id(&resp);

    // deliver_mode=1 (Queue) + shared group → round-robin.
    let resp = client
        .create_consumer(sid, b"qsc-w1", b"qsc-group", b"", 100, 1, 0, 1, 0, 0)
        .await
        .unwrap();
    let cid1 = TestServer::parse_id(&resp);
    let resp = client
        .create_consumer(sid, b"qsc-w2", b"qsc-group", b"", 100, 1, 0, 1, 0, 0)
        .await
        .unwrap();
    let cid2 = TestServer::parse_id(&resp);

    let mut sub1 = client.subscribe(sid, cid1, b"").await.unwrap();
    let mut sub2 = client.subscribe(sid, cid2, b"").await.unwrap();

    for i in 0..10u32 {
        client
            .publish_sync(sid, b"qsc.job", Bytes::copy_from_slice(&i.to_le_bytes()))
            .await
            .expect("publish");
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
            r = sub1.recv() => { if let Some(m) = r { seqs.insert(m.seq); m.ack(); count1 += 1; } }
            r = sub2.recv() => { if let Some(m) = r { seqs.insert(m.seq); m.ack(); count2 += 1; } }
            _ = tokio::time::sleep_until(deadline) => { break; }
        }
    }
    let total = count1 + count2;
    assert_eq!(
        total, 10,
        "queue total should be 10, got {count1}+{count2}={total}"
    );
    assert_eq!(seqs.len(), 10, "no duplicates: unique seqs={}", seqs.len());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fanout_filtered_same_connection() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let resp = client
        .create_stream(b"ffsc", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let sid = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(sid, b"ffsc-a", b"ga", b"", 100, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let cid_a = TestServer::parse_id(&resp);
    let mut sub_a = client.subscribe(sid, cid_a, b"orders.*").await.unwrap();

    let resp = client
        .create_consumer(sid, b"ffsc-b", b"gb", b"", 100, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let cid_b = TestServer::parse_id(&resp);
    let mut sub_b = client.subscribe(sid, cid_b, b"payments.*").await.unwrap();

    let resp = client
        .create_consumer(sid, b"ffsc-c", b"gc", b"", 100, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let cid_c = TestServer::parse_id(&resp);
    let mut sub_c = client.subscribe(sid, cid_c, b">").await.unwrap();

    for i in 0..3u32 {
        let subj = format!("orders.{i}");
        client
            .publish_sync(sid, subj.as_bytes(), Bytes::copy_from_slice(b"o"))
            .await
            .expect("publish");
    }
    for i in 0..2u32 {
        let subj = format!("payments.{i}");
        client
            .publish_sync(sid, subj.as_bytes(), Bytes::copy_from_slice(b"p"))
            .await
            .expect("publish");
    }

    let mut ca = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub_a.recv()).await {
            Ok(Some(msg)) => {
                msg.ack();
                ca += 1;
            }
            _ => break,
        }
    }
    assert_eq!(ca, 3, "A (orders.*) should get 3, got {ca}");

    let mut cb = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub_b.recv()).await {
            Ok(Some(msg)) => {
                msg.ack();
                cb += 1;
            }
            _ => break,
        }
    }
    assert_eq!(cb, 2, "B (payments.*) should get 2, got {cb}");

    let mut cc = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub_c.recv()).await {
            Ok(Some(msg)) => {
                msg.ack();
                cc += 1;
            }
            _ => break,
        }
    }
    assert_eq!(cc, 5, "C (>) should get 5, got {cc}");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_publish_with_reply_delivers_reply_to() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let resp = client
        .create_stream(b"rpc", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let sid = TestServer::parse_id(&resp);
    let resp = client
        .create_consumer(sid, b"rpc-worker", b"", b"", 100, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let cid = TestServer::parse_id(&resp);
    let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

    // Publish with reply_to
    let reply_subject = b"_INBOX.abc123";
    client
        .publish_with_reply(
            sid,
            b"rpc.request",
            reply_subject,
            Bytes::from_static(b"hello"),
        )
        .await
        .unwrap();

    // Consumer should receive the message with reply_to set
    let msg = tokio::time::timeout(Duration::from_secs(3), sub.recv())
        .await
        .expect("timeout")
        .expect("message");
    assert_eq!(msg.subject(), b"rpc.request");
    assert_eq!(msg.reply_to(), reply_subject.as_slice());
    assert_eq!(&msg.payload()[..], b"hello");
    assert!(msg.has_reply_to());
    msg.ack();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_publish_without_reply_has_empty_reply_to() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let resp = client
        .create_stream(b"norpc", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let sid = TestServer::parse_id(&resp);
    let resp = client
        .create_consumer(sid, b"norpc-worker", b"", b"", 100, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let cid = TestServer::parse_id(&resp);
    let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

    // Normal publish (no reply)
    client
        .publish_sync(sid, b"norpc.msg", Bytes::from_static(b"data"))
        .await
        .expect("publish");

    let msg = tokio::time::timeout(Duration::from_secs(3), sub.recv())
        .await
        .expect("timeout")
        .expect("message");
    assert_eq!(msg.subject(), b"norpc.msg");
    assert!(msg.reply_to().is_empty());
    assert!(!msg.has_reply_to());
    assert_eq!(&msg.payload()[..], b"data");
    msg.ack();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_graceful_shutdown() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    client
        .create_stream(b"shutdown_test", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let resp = client.list_streams(0, 1000).await.unwrap();
    assert_eq!(TestServer::stream_count(&resp), 1);
    server.shutdown().await;
}
