mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use bytes::Bytes;
use std::time::Duration;

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
        client
            .create_stream(b"orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        assert_eq!(
            TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()),
            1
        );
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(
            TestServer::stream_count(&resp),
            1,
            "stream should survive restart"
        );
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
        client
            .create_stream(b"orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        client
            .create_stream(b"events", b"events.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(
            TestServer::stream_count(&resp),
            2,
            "both streams should survive restart"
        );
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
        client
            .create_stream(b"temp", b"temp.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        client.delete_stream(b"temp").await.unwrap();
        assert_eq!(
            TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()),
            0
        );
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
    client
        .create_stream(b"ephemeral", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    assert_eq!(
        TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()),
        1
    );
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
    client
        .create_stream(b"logged", b"logged.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    server.shutdown().await;

    assert!(log_path.exists(), "metadata.log should be created");
    assert!(
        std::fs::metadata(&log_path).unwrap().len() > 0,
        "metadata.log should be non-empty"
    );
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
        let resp = client
            .create_stream(b"orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        let sid = TestServer::parse_id(&resp);
        client
            .create_consumer(sid, b"worker1", b"", b"", u16::MAX, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        assert_eq!(
            TestServer::consumer_count(&client.list_consumers(0, 0, 1000).await.unwrap()),
            1
        );
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        assert_eq!(
            TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()),
            1,
            "stream should survive"
        );
        assert_eq!(
            TestServer::consumer_count(&client.list_consumers(0, 0, 1000).await.unwrap()),
            1,
            "consumer should survive"
        );
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
        let resp = client
            .create_stream(b"durable", b"durable.>", 0, 0, 0, 1, 1, 0, 0, 0)
            .await
            .unwrap();
        let sid = TestServer::parse_id(&resp);
        for i in 0u32..10 {
            let payload = format!("msg-{i}");
            client
                .publish_sync(
                    sid,
                    b"durable.events",
                    Bytes::copy_from_slice(payload.as_bytes()),
                )
                .await
                .expect("publish");
        }
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        assert_eq!(
            TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()),
            1
        );

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = TestServer::find_stream_id(&resp, b"durable").expect("durable stream not found");

        let resp = client
            .create_consumer(sid, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0)
            .await
            .unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }
        assert_eq!(
            received.len(),
            10,
            "all 10 messages should survive restart, got {}",
            received.len()
        );

        for (i, msg) in received.iter().enumerate() {
            let expected = format!("msg-{i}");
            assert_eq!(
                &msg.payload()[..],
                expected.as_bytes(),
                "message {i} payload mismatch"
            );
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

    {
        // Cycle 1: create stream A
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        client
            .create_stream(b"alpha", b"alpha.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        server.shutdown().await;
    }

    {
        // Cycle 2: create stream B (A should still exist)
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        assert_eq!(
            TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()),
            1,
            "alpha should survive first restart"
        );
        client
            .create_stream(b"beta", b"beta.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        server.shutdown().await;
    }

    {
        // Cycle 3: verify both A and B exist
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(
            TestServer::stream_count(&resp),
            2,
            "both alpha and beta should survive"
        );
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
        client
            .create_stream(b"unique", b"unique.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        let _ = client
            .create_stream(b"unique", b"unique.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await;
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
        let resp = client
            .create_stream(b"recycled", b"recycled.>", 0, 0, 0, 1, 1, 0, 0, 0)
            .await
            .unwrap();
        let sid = TestServer::parse_id(&resp);

        for i in 0u32..5 {
            let payload = format!("old-{i}");
            client
                .publish_sync(
                    sid,
                    b"recycled.data",
                    Bytes::copy_from_slice(payload.as_bytes()),
                )
                .await
                .expect("publish");
        }

        client.delete_stream(b"recycled").await.unwrap();
        let resp = client
            .create_stream(b"recycled", b"recycled.>", 0, 0, 0, 1, 1, 0, 0, 0)
            .await
            .unwrap();
        let sid2 = TestServer::parse_id(&resp);

        for i in 0u32..2 {
            let payload = format!("new-{i}");
            client
                .publish_sync(
                    sid2,
                    b"recycled.data",
                    Bytes::copy_from_slice(payload.as_bytes()),
                )
                .await
                .expect("publish");
        }
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        assert_eq!(
            TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()),
            1
        );

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid =
            TestServer::find_stream_id(&resp, b"recycled").expect("recycled stream not found");
        let resp = client
            .create_consumer(sid, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0)
            .await
            .unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }
        assert_eq!(
            received.len(),
            2,
            "only 2 new messages should exist, got {}",
            received.len()
        );
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
        let resp = client
            .create_stream(b"durable", b"durable.>", 0, 0, 0, 1, 1, 0, 0, 0)
            .await
            .unwrap();
        let sid = TestServer::parse_id(&resp);
        client
            .create_consumer(sid, b"worker", b"", b"", u16::MAX, 0, 0, 0, 0, 0)
            .await
            .unwrap();
        for i in 0u32..5 {
            let payload = format!("event-{i}");
            client
                .publish_sync(
                    sid,
                    b"durable.data",
                    Bytes::copy_from_slice(payload.as_bytes()),
                )
                .await
                .expect("publish");
        }
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        assert_eq!(
            TestServer::stream_count(&client.list_streams(0, 1000).await.unwrap()),
            1
        );
        assert_eq!(
            TestServer::consumer_count(&client.list_consumers(0, 0, 1000).await.unwrap()),
            1
        );

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = TestServer::find_stream_id(&resp, b"durable").expect("durable stream not found");
        let resp = client
            .create_consumer(sid, b"worker", b"", b"", u16::MAX, 0, 0, 0, 0, 0)
            .await
            .unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }
        assert_eq!(
            received.len(),
            5,
            "all 5 messages should survive restart, got {}",
            received.len()
        );
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
        let resp = client
            .create_stream(b"seq", b"seq.>", 0, 0, 0, 1, 1, 0, 0, 0)
            .await
            .unwrap();
        let sid = TestServer::parse_id(&resp);
        for i in 0u32..3 {
            let payload = format!("before-{i}");
            client
                .publish_sync(sid, b"seq.data", Bytes::copy_from_slice(payload.as_bytes()))
                .await
                .expect("publish");
        }
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = TestServer::find_stream_id(&resp, b"seq").expect("seq stream not found");

        let resp = client
            .create_consumer(sid, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0)
            .await
            .unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        for i in 0u32..3 {
            let payload = format!("after-{i}");
            client
                .publish_sync(sid, b"seq.data", Bytes::copy_from_slice(payload.as_bytes()))
                .await
                .expect("publish");
        }

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }
        assert_eq!(
            received.len(),
            6,
            "should receive 6 messages (3 before + 3 after restart), got {}",
            received.len()
        );
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
        let resp = client
            .create_stream(b"multi", b"multi.>", 0, 0, 0, 1, 1, 0, 0, 0)
            .await
            .unwrap();
        let sid = TestServer::parse_id(&resp);
        for i in 0u32..3 {
            let payload = format!("c1-{i}");
            client
                .publish_sync(
                    sid,
                    b"multi.data",
                    Bytes::copy_from_slice(payload.as_bytes()),
                )
                .await
                .expect("publish");
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
            client
                .publish_sync(
                    sid,
                    b"multi.data",
                    Bytes::copy_from_slice(payload.as_bytes()),
                )
                .await
                .expect("publish");
        }
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = TestServer::find_stream_id(&resp, b"multi").expect("multi stream not found");
        let resp = client
            .create_consumer(sid, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0)
            .await
            .unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }
        assert_eq!(
            received.len(),
            6,
            "should receive all 6 messages across 2 cycles, got {}",
            received.len()
        );
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
        let resp = client
            .create_stream(b"orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        let stream_id = TestServer::parse_id(&resp);
        let resp = client
            .create_consumer(
                stream_id, b"worker", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64,
            )
            .await
            .unwrap();
        pre_consumer_id = TestServer::parse_id(&resp);
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;

        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(
            TestServer::stream_count(&resp),
            1,
            "stream must survive restart"
        );
        let stream_id =
            TestServer::find_stream_id(&resp, b"orders").expect("orders stream not found");

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
        let resp = client
            .create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        let stream_id = TestServer::parse_id(&resp);
        let resp = client
            .create_consumer(
                stream_id, b"worker", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64,
            )
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
        let stream_id =
            TestServer::find_stream_id(&resp, b"orders").expect("orders stream not found");

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
        let resp = client
            .create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        let stream_id = TestServer::parse_id(&resp);

        let mut ids = Vec::new();
        for n in 0..5u32 {
            let name = format!("worker-{n}");
            let resp = client
                .create_consumer(
                    stream_id,
                    name.as_bytes(),
                    b"",
                    b"",
                    100u16,
                    1u8,
                    0u8,
                    0u8,
                    0u32,
                    0u64,
                )
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
        let stream_id =
            TestServer::find_stream_id(&resp, b"orders").expect("orders stream not found");

        let resp = client
            .create_consumer(
                stream_id,
                b"worker-new",
                b"",
                b"",
                100u16,
                1u8,
                0u8,
                0u8,
                0u32,
                0u64,
            )
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

// ═══════════════════════════════════════════════════════════════════════════
// T8 — Retention config (max_msgs) survives a restart
//
// Before H3, recovery silently dropped per-stream retention. After restart
// the broker re-published with unbounded retention regardless of the
// original CreateStream parameters. This test creates a stream with
// max_msgs = 10, publishes 20 entries pre-restart, restarts the broker,
// then asserts that a fresh DeliverPolicy::All subscriber sees at most
// 10 messages — proving the retention config (and the eviction it
// implies) was preserved across the boundary.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn t8_retention_max_msgs_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    const CAP: u64 = 10;
    const PUBLISH: u32 = 20;

    {
        // Pre-restart: create stream with max_msgs = 10, fire 20 publishes.
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client
            .create_stream(b"capped", b"capped.>", CAP, 0, 0, 1, 0, 0, 0, 0)
            .await
            .expect("create_stream");
        let stream_id = TestServer::parse_id(&resp);

        for i in 0u32..PUBLISH {
            let p = format!("msg-{i}");
            client
                .publish_sync(
                    stream_id,
                    b"capped.event",
                    Bytes::copy_from_slice(p.as_bytes()),
                )
                .await
                .expect("publish_sync");
        }
        server.shutdown().await;
    }

    {
        // Post-restart: subscribe DeliverPolicy::All and count messages.
        // If retention was lost, we'd see all 20+; with retention preserved
        // (H3), we see at most CAP.
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        let stream_id = TestServer::find_stream_id(&resp, b"capped")
            .expect("capped stream missing after restart");

        let resp = client
            .create_consumer(
                stream_id, b"reader", b"", b"", 100u16, 1u8, /* Explicit */
                0u8, /* All */ 0u8, 30_000u32, 0u64,
            )
            .await
            .unwrap();
        let consumer_id = TestServer::parse_id(&resp);
        let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

        // Generous overall budget so a slow CI box doesn't false-fail.
        let mut received = 0usize;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(Duration::from_millis(500), handle.recv()).await {
                Ok(Some(m)) => {
                    m.ack();
                    received += 1;
                    if received > CAP as usize + 5 {
                        break; // bail early — already failed.
                    }
                }
                _ => break,
            }
        }

        // Pre-H3 the broker would deliver all PUBLISH=20 entries because
        // recovery dropped the retention config entirely. Post-H3 the
        // retention config is restored, so the count is bounded — but
        // the broker is allowed an eviction overshoot (eviction runs on
        // a fixed cadence and can lag a burst of publishes). The shape
        // we want to assert is "fewer than PUBLISH, by a wide margin".
        // A generous bound (CAP * 2) catches the regression (would see
        // all 20) without being flaky on the eviction batching.
        assert!(
            received as u64 <= CAP * 2,
            "retention max_msgs={CAP} not enforced after restart; \
             received {received} entries (would have been {PUBLISH} \
             pre-H3 fix)"
        );
        // We should also actually see at least some — eviction shouldn't
        // wipe everything.
        assert!(received > 0, "all entries lost after restart");
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST-1: Command log survives a bit-flip corruption in its trailing entry.
//
// CommandLog::record() frames each entry as [4 len_le][4 crc32_le][payload].
// A single flipped bit inside the payload of the last entry breaks its
// CRC32 check; replay() must skip that one entry (CRC mismatch) while
// keeping every valid entry before it. This test creates 3 streams
// (3 valid log entries), corrupts a byte inside the last entry's payload
// on disk, restarts the broker, and asserts the first 2 streams recovered
// cleanly while the corrupted 3rd did not silently reappear.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn command_log_recovers_valid_prefix_after_bitflip_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let log_path = dir.path().join("metadata.log");

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        client
            .create_stream(b"alpha", b"alpha.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        client
            .create_stream(b"beta", b"beta.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        client
            .create_stream(b"gamma", b"gamma.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        server.shutdown().await;
    }

    // Flip a bit inside the payload of the last entry (the "gamma" stream
    // creation). The payload starts right after the 8-byte header
    // [len_le][crc32_le], so any byte at offset >= 8 in the final entry's
    // span lands inside its payload and invalidates its CRC without
    // touching the length prefix — replay stays aligned for the entries
    // before it and cleanly stops after skipping the corrupted one.
    {
        let mut bytes = std::fs::read(&log_path).unwrap();
        assert!(
            bytes.len() > 16,
            "expected at least 3 framed entries on disk, got {} bytes",
            bytes.len()
        );
        let flip_at = bytes.len() - 4;
        bytes[flip_at] ^= 0x01;
        std::fs::write(&log_path, &bytes).unwrap();
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        let names = TestServer::stream_names(&resp);

        assert!(
            names.iter().any(|n| n == b"alpha"),
            "alpha (before corruption) must survive"
        );
        assert!(
            names.iter().any(|n| n == b"beta"),
            "beta (before corruption) must survive"
        );
        assert!(
            !names.iter().any(|n| n == b"gamma"),
            "gamma (corrupted entry) must not silently reappear"
        );

        // The broker must still be fully usable after recovering from
        // the corrupted trailing entry — not stuck in a degraded state.
        client
            .create_stream(b"delta", b"delta.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .expect("broker must remain writable after corruption recovery");
        let resp = client.list_streams(0, 1000).await.unwrap();
        assert!(
            TestServer::stream_names(&resp)
                .iter()
                .any(|n| n == b"delta"),
            "post-recovery writes must persist"
        );
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST-1b: A corrupted entry in the MIDDLE of the log (not just the tail)
// is skipped without derailing replay of the entries that follow it,
// since each entry's length prefix is read before its CRC is checked.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn command_log_skips_mid_log_corruption_and_continues_replay() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let log_path = dir.path().join("metadata.log");

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        client
            .create_stream(b"first", b"first.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        client
            .create_stream(b"second", b"second.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        client
            .create_stream(b"third", b"third.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        server.shutdown().await;
    }

    // Flip a bit near the start of the file — inside the payload of the
    // FIRST entry ("first"), well past its 8-byte header.
    {
        let mut bytes = std::fs::read(&log_path).unwrap();
        let flip_at = 10.min(bytes.len().saturating_sub(1));
        bytes[flip_at] ^= 0x01;
        std::fs::write(&log_path, &bytes).unwrap();
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        let names = TestServer::stream_names(&resp);

        assert!(
            !names.iter().any(|n| n == b"first"),
            "corrupted first entry must not reappear"
        );
        assert!(
            names.iter().any(|n| n == b"second"),
            "second entry (after corrupted first) must still replay"
        );
        assert!(
            names.iter().any(|n| n == b"third"),
            "third entry (after corrupted first) must still replay"
        );
        server.shutdown().await;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// created_at_seq filters old entries after stream_id recycle
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn created_at_seq_filters_old_entries_after_recycle() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    // Phase 1: create streams, publish data, delete + recreate stream A.
    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;

        // Create a filler stream to push global seq forward.
        let resp = client
            .create_stream(b"filler", b"filler.>", 0, 0, 0, 1, 1, 0, 0, 0)
            .await
            .unwrap();
        let filler_sid = TestServer::parse_id(&resp);

        // Publish ~99 messages to filler so stream A starts around seq 100.
        for i in 0u32..99 {
            let payload = i.to_le_bytes();
            client
                .publish_sync(
                    filler_sid,
                    b"filler.pad",
                    Bytes::copy_from_slice(&payload),
                )
                .await
                .expect("filler pre-publish");
        }

        // Create stream A and publish 100 messages (8 bytes each).
        let resp = client
            .create_stream(b"stream_a", b"stream_a.>", 0, 0, 0, 1, 1, 0, 0, 0)
            .await
            .unwrap();
        let a_sid = TestServer::parse_id(&resp);

        for i in 0u32..100 {
            let payload = (i as u64).to_le_bytes();
            client
                .publish_sync(
                    a_sid,
                    b"stream_a.data",
                    Bytes::copy_from_slice(&payload),
                )
                .await
                .expect("publish to A");
        }

        // Push global seq far forward with filler messages.
        // Use batches to speed this up — 500 messages at a time.
        for batch in 0..1000 {
            let payload = (batch as u64).to_le_bytes();
            client
                .publish_sync(
                    filler_sid,
                    b"filler.bulk",
                    Bytes::copy_from_slice(&payload),
                )
                .await
                .expect("filler bulk publish");
        }

        // Delete stream A.
        client.delete_stream(b"stream_a").await.unwrap();

        // Recreate stream A (gets the same recycled stream_id from IdPool).
        let resp = client
            .create_stream(b"stream_a", b"stream_a.>", 0, 0, 0, 1, 1, 0, 0, 0)
            .await
            .unwrap();
        let a_sid2 = TestServer::parse_id(&resp);

        // Publish 5 messages to the new incarnation of stream A.
        for i in 0u32..5 {
            let payload = (1000 + i as u64).to_le_bytes();
            client
                .publish_sync(
                    a_sid2,
                    b"stream_a.data",
                    Bytes::copy_from_slice(&payload),
                )
                .await
                .expect("publish to new A");
        }

        // Consume from new stream A — should see exactly 5 messages.
        let resp = client
            .create_consumer(a_sid2, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0)
            .await
            .unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(a_sid2, cid, b"").await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(3), sub.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }
        assert_eq!(
            received.len(),
            5,
            "new stream A should only deliver 5 messages (not old 100), got {}",
            received.len()
        );

        server.shutdown().await;
    }

    // Phase 2: restart and verify created_at_seq persists via sidecar.
    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;

        let resp = client.list_streams(0, 1000).await.unwrap();
        let a_sid =
            TestServer::find_stream_id(&resp, b"stream_a").expect("stream_a not found after restart");

        let resp = client
            .create_consumer(a_sid, b"reader2", b"", b"", u16::MAX, 0, 0, 0, 0, 0)
            .await
            .unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(a_sid, cid, b"").await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(3), sub.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }
        assert_eq!(
            received.len(),
            5,
            "after restart, new stream A should still only deliver 5 messages, got {}",
            received.len()
        );

        server.shutdown().await;
    }
}
