mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use std::time::Duration;
use bytes::Bytes;

// ═══════════════════════════════════════════════════════════════════════════
// 1. Stream metadata survives restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn stream_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        client.create_stream(b"orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
        assert_eq!(TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()), 1);
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(TestServer::stream_count(&resp), 1, "stream should survive restart");
        let names = TestServer::stream_names(&resp);
        assert_eq!(names[0], b"orders");
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Multiple streams survive restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn multiple_streams_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        client.create_stream(b"orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
        client.create_stream(b"events", b"events.>", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(TestServer::stream_count(&resp), 2, "both streams should survive restart");
        let names = TestServer::stream_names(&resp);
        assert!(names.iter().any(|n| n == b"orders"), "orders missing");
        assert!(names.iter().any(|n| n == b"events"), "events missing");
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Deleted stream stays deleted after restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn deleted_stream_stays_deleted_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        client.create_stream(b"temp", b"temp.>", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
        client.delete_stream(b"temp").await.unwrap();
        assert_eq!(TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()), 0);
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        assert_eq!(
            TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()),
            0,
            "deleted stream should not reappear"
        );
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. No data_dir — no persistence
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn no_data_dir_works_without_persistence() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    client.create_stream(b"ephemeral", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    assert_eq!(TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()), 1);
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Command log file is created on disk
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn command_log_file_is_created() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let log_path = dir.path().join("metadata.log");

    let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
    let client = server.connect().await;
    client.create_stream(b"logged", b"logged.>", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    server.shutdown().await;

    assert!(log_path.exists(), "metadata.log should be created");
    assert!(std::fs::metadata(&log_path).unwrap().len() > 0, "metadata.log should be non-empty");
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Consumer survives restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn consumer_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.create_stream(b"orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
        let sid = TestServer::parse_id(&resp);
        client.create_consumer(sid, b"worker1", b"", b"", u16::MAX, 1, 0, 0, 0, 0).await.unwrap();
        assert_eq!(TestServer::consumer_count(&client.list_consumers(0, 0, 1000).await.unwrap()), 1);
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        assert_eq!(TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()), 1, "stream should survive");
        assert_eq!(TestServer::consumer_count(&client.list_consumers(0, 0, 1000).await.unwrap()), 1, "consumer should survive");
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Messages survive restart (disk store, journal_kind=1)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn messages_survive_restart_with_disk_store() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.create_stream(b"durable", b"durable.>", 0, 0, 0, 1, 1, 0, 0, 0).await.unwrap();
        let sid = TestServer::parse_id(&resp);
        for i in 0u32..10 {
            let payload = format!("msg-{i}");
            client.publish_sync(sid, b"durable.events", Bytes::copy_from_slice(payload.as_bytes())).await.expect("publish");
        }
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        assert_eq!(TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()), 1);

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = TestServer::find_stream_id(&resp, b"durable").expect("durable stream not found");

        let resp = client.create_consumer(sid, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0).await.unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }
        assert_eq!(received.len(), 10, "all 10 messages should survive restart, got {}", received.len());

        for (i, msg) in received.iter().enumerate() {
            let expected = format!("msg-{i}");
            assert_eq!(&msg.payload()[..], expected.as_bytes(), "message {i} payload mismatch");
        }
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Multiple restart cycles
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn multiple_restart_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    { // Cycle 1: create stream A
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        client.create_stream(b"alpha", b"alpha.>", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
        server.shutdown().await;
    }

    { // Cycle 2: create stream B (A should still exist)
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        assert_eq!(TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()), 1, "alpha should survive first restart");
        client.create_stream(b"beta", b"beta.>", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
        server.shutdown().await;
    }

    { // Cycle 3: verify both A and B exist
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(TestServer::stream_count(&resp), 2, "both alpha and beta should survive");
        let names = TestServer::stream_names(&resp);
        assert!(names.iter().any(|n| n == b"alpha"), "alpha missing");
        assert!(names.iter().any(|n| n == b"beta"), "beta missing");
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Idempotent create — same stream twice, still one after restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_create_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        client.create_stream(b"unique", b"unique.>", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
        let _ = client.create_stream(b"unique", b"unique.>", 0, 0, 0, 1, 0, 0, 0, 0).await;
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        assert_eq!(
            TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()),
            1,
            "duplicate create should not produce two streams after restart"
        );
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. Deleted disk stream data does not leak into recreated stream
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn deleted_disk_stream_data_does_not_leak() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.create_stream(b"recycled", b"recycled.>", 0, 0, 0, 1, 1, 0, 0, 0).await.unwrap();
        let sid = TestServer::parse_id(&resp);

        for i in 0u32..5 {
            let payload = format!("old-{i}");
            client.publish_sync(sid, b"recycled.data", Bytes::copy_from_slice(payload.as_bytes())).await.expect("publish");
        }

        client.delete_stream(b"recycled").await.unwrap();
        let resp = client.create_stream(b"recycled", b"recycled.>", 0, 0, 0, 1, 1, 0, 0, 0).await.unwrap();
        let sid2 = TestServer::parse_id(&resp);

        for i in 0u32..2 {
            let payload = format!("new-{i}");
            client.publish_sync(sid2, b"recycled.data", Bytes::copy_from_slice(payload.as_bytes())).await.expect("publish");
        }
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        assert_eq!(TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()), 1);

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = TestServer::find_stream_id(&resp, b"recycled").expect("recycled stream not found");
        let resp = client.create_consumer(sid, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0).await.unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }
        assert_eq!(received.len(), 2, "only 2 new messages should exist, got {}", received.len());
        assert_eq!(&received[0].payload()[..], b"new-0");
        assert_eq!(&received[1].payload()[..], b"new-1");
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. Consumer created before shutdown receives messages after restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn consumer_and_messages_survive_together() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.create_stream(b"durable", b"durable.>", 0, 0, 0, 1, 1, 0, 0, 0).await.unwrap();
        let sid = TestServer::parse_id(&resp);
        client.create_consumer(sid, b"worker", b"", b"", u16::MAX, 0, 0, 0, 0, 0).await.unwrap();
        for i in 0u32..5 {
            let payload = format!("event-{i}");
            client.publish_sync(sid, b"durable.data", Bytes::copy_from_slice(payload.as_bytes())).await.expect("publish");
        }
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        assert_eq!(TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()), 1);
        assert_eq!(TestServer::consumer_count(&client.list_consumers(0, 0, 1000).await.unwrap()), 1);

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = TestServer::find_stream_id(&resp, b"durable").expect("durable stream not found");
        let resp = client.create_consumer(sid, b"worker", b"", b"", u16::MAX, 0, 0, 0, 0, 0).await.unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }
        assert_eq!(received.len(), 5, "all 5 messages should survive restart, got {}", received.len());
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. Publish after restart continues correctly
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn publish_after_restart_continues() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.create_stream(b"seq", b"seq.>", 0, 0, 0, 1, 1, 0, 0, 0).await.unwrap();
        let sid = TestServer::parse_id(&resp);
        for i in 0u32..3 {
            let payload = format!("before-{i}");
            client.publish_sync(sid, b"seq.data", Bytes::copy_from_slice(payload.as_bytes())).await.expect("publish");
        }
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = TestServer::find_stream_id(&resp, b"seq").expect("seq stream not found");

        let resp = client.create_consumer(sid, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0).await.unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        for i in 0u32..3 {
            let payload = format!("after-{i}");
            client.publish_sync(sid, b"seq.data", Bytes::copy_from_slice(payload.as_bytes())).await.expect("publish");
        }

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }
        assert_eq!(received.len(), 6, "should receive 6 messages (3 before + 3 after restart), got {}", received.len());
        assert_eq!(&received[0].payload()[..], b"before-0");
        assert_eq!(&received[2].payload()[..], b"before-2");
        assert_eq!(&received[3].payload()[..], b"after-0");
        assert_eq!(&received[5].payload()[..], b"after-2");
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. Messages survive multiple restart cycles (disk store)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn messages_survive_multiple_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.create_stream(b"multi", b"multi.>", 0, 0, 0, 1, 1, 0, 0, 0).await.unwrap();
        let sid = TestServer::parse_id(&resp);
        for i in 0u32..3 {
            let payload = format!("c1-{i}");
            client.publish_sync(sid, b"multi.data", Bytes::copy_from_slice(payload.as_bytes())).await.expect("publish");
        }
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = TestServer::find_stream_id(&resp, b"multi").expect("multi stream not found");
        for i in 0u32..3 {
            let payload = format!("c2-{i}");
            client.publish_sync(sid, b"multi.data", Bytes::copy_from_slice(payload.as_bytes())).await.expect("publish");
        }
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = TestServer::find_stream_id(&resp, b"multi").expect("multi stream not found");
        let resp = client.create_consumer(sid, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0).await.unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }
        assert_eq!(received.len(), 6, "should receive all 6 messages across 2 cycles, got {}", received.len());
        assert_eq!(&received[0].payload()[..], b"c1-0");
        assert_eq!(&received[3].payload()[..], b"c2-0");
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Catalog-state invariants across restart.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn consumer_survives_restart_with_same_id() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    let pre_consumer_id: u32;
    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.create_stream(b"orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
        let stream_id = TestServer::parse_id(&resp);
        let resp = client
            .create_consumer(stream_id, b"worker", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
            .await
            .unwrap();
        pre_consumer_id = TestServer::parse_id(&resp);
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;

        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(TestServer::stream_count(&resp), 1, "stream must survive restart");
        let stream_id = TestServer::find_stream_id(&resp, b"orders").expect("orders stream not found");

        let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
        assert_eq!(
            TestServer::consumer_count(&resp),
            1,
            "consumer must survive restart and appear in list_consumers"
        );

        let resp = client
            .get_consumer(stream_id, b"worker")
            .await
            .expect("GetConsumer must succeed for a recovered consumer");
        let post_consumer_id = TestServer::parse_id(&resp);
        assert_eq!(
            post_consumer_id, pre_consumer_id,
            "recovered consumer must keep its original id"
        );
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn deleted_consumer_stays_deleted_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
        let stream_id = TestServer::parse_id(&resp);
        let resp = client
            .create_consumer(stream_id, b"worker", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
            .await
            .unwrap();
        let consumer_id = TestServer::parse_id(&resp);
        client.delete_consumer(consumer_id).await.unwrap();
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        let stream_id = TestServer::find_stream_id(&resp, b"orders").expect("orders stream not found");

        let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
        assert_eq!(
            TestServer::consumer_count(&resp),
            0,
            "consumer deleted pre-restart must remain deleted"
        );
        server.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn post_restart_create_does_not_collide_with_recovered_ids() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    let pre_ids: Vec<u32>;
    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
        let stream_id = TestServer::parse_id(&resp);

        let mut ids = Vec::new();
        for n in 0..5u32 {
            let name = format!("worker-{n}");
            let resp = client
                .create_consumer(stream_id, name.as_bytes(), b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
                .await
                .unwrap();
            ids.push(TestServer::parse_id(&resp));
        }
        pre_ids = ids;
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        let stream_id = TestServer::find_stream_id(&resp, b"orders").expect("orders stream not found");

        let resp = client
            .create_consumer(stream_id, b"worker-new", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
            .await
            .unwrap();
        let new_id = TestServer::parse_id(&resp);

        assert!(
            !pre_ids.contains(&new_id),
            "id allocator after recovery must advance past the highest recovered id"
        );
        server.shutdown().await;
    }
}
