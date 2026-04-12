//! Persistence tests — verify metadata and store data survive server restart.
//!
//! Each test starts a server with a temp data_dir, performs operations,
//! shuts down, starts a new server on the same data_dir, and verifies
//! state was restored from the command log and disk stores.

use std::time::Duration;

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, JournalKind, StreamConfig};
use arbitro_server::command_log::{CommandLog, SharedCommandLog};
use arbitro_server::{ArbitroServer, Config};
use tokio::sync::watch;

/// Start a server with persistence on the given data_dir.
async fn start_server_with_dir(data_dir: &str) -> (watch::Sender<bool>, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let (tx, rx) = watch::channel(false);
    let config = Config::default()
        .listen_addr(&addr)
        .shard_count(2)
        .channel_capacity(1024)
        .data_dir(data_dir);

    let mut server = ArbitroServer::new(config);

    let path = std::path::Path::new(data_dir).join("metadata.log");
    let log = CommandLog::open(path).unwrap();
    server.set_command_log(SharedCommandLog::new(log));

    tokio::spawn(async move {
        let _ = server.run_with_shutdown(rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    (tx, addr)
}

/// Connect a client to the given address.
async fn connect(addr: &str) -> Client {
    Client::connect_with_timeout(addr, Duration::from_secs(2))
        .await
        .expect("client should connect")
}

/// Graceful shutdown: signal + wait for shard threads to flush stores.
async fn shutdown(tx: watch::Sender<bool>) {
    let _ = tx.send(true);
    tokio::time::sleep(Duration::from_millis(500)).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Stream metadata survives restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn stream_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let stream = StreamConfig::new(b"orders", b"orders.>").build();
        client.create_stream(&stream).await.unwrap();

        let streams = client.list_streams().await.unwrap();
        assert_eq!(streams.len(), 1);

        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let streams = client.list_streams().await.unwrap();
        assert_eq!(streams.len(), 1, "stream should survive restart");
        assert_eq!(streams[0].name, b"orders");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Multiple streams survive restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn multiple_streams_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        client.create_stream(&StreamConfig::new(b"orders", b"orders.>").build()).await.unwrap();
        client.create_stream(&StreamConfig::new(b"events", b"events.>").build()).await.unwrap();

        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let streams = client.list_streams().await.unwrap();
        assert_eq!(streams.len(), 2, "both streams should survive restart");

        let names: Vec<&[u8]> = streams.iter().map(|s| s.name.as_slice()).collect();
        assert!(names.contains(&b"orders".as_slice()));
        assert!(names.contains(&b"events".as_slice()));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Deleted stream stays deleted after restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn deleted_stream_stays_deleted_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        client.create_stream(&StreamConfig::new(b"temp", b"temp.>").build()).await.unwrap();
        client.delete_stream(b"temp").await.unwrap();

        let streams = client.list_streams().await.unwrap();
        assert_eq!(streams.len(), 0);

        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let streams = client.list_streams().await.unwrap();
        assert_eq!(streams.len(), 0, "deleted stream should not reappear after restart");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. No data_dir — no persistence
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn no_data_dir_works_without_persistence() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let (tx, rx) = watch::channel(false);
    let config = Config::default()
        .listen_addr(&addr)
        .shard_count(2)
        .channel_capacity(1024);

    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run_with_shutdown(rx).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = connect(&addr).await;
    client.create_stream(&StreamConfig::new(b"ephemeral", b">").build()).await.unwrap();

    let streams = client.list_streams().await.unwrap();
    assert_eq!(streams.len(), 1);

    let _ = tx.send(true);
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Command log file is created on disk
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn command_log_file_is_created() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let log_path = dir.path().join("metadata.log");

    let (tx, addr) = start_server_with_dir(dir_str).await;
    let client = connect(&addr).await;

    client.create_stream(&StreamConfig::new(b"logged", b"logged.>").build()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(log_path.exists(), "metadata.log should be created");
    let meta = std::fs::metadata(&log_path).unwrap();
    assert!(meta.len() > 0, "metadata.log should be non-empty");

    shutdown(tx).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Consumer survives restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn consumer_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        client.create_stream(&StreamConfig::new(b"orders", b"orders.>").build()).await.unwrap();
        let c = ConsumerConfig::new(b"worker1", b"orders")
            .ack_policy(AckPolicy::Explicit)
            .build()
            .unwrap();
        client.create_consumer(&c).await.unwrap();

        let consumers = client.list_consumers().await.unwrap();
        assert_eq!(consumers.len(), 1, "consumer should exist before restart");

        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let streams = client.list_streams().await.unwrap();
        assert_eq!(streams.len(), 1, "stream should survive restart");

        let consumers = client.list_consumers().await.unwrap();
        assert_eq!(consumers.len(), 1, "consumer should survive restart");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Stream + consumer + messages survive restart (disk store)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn messages_survive_restart_with_disk_store() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    // Phase 1: create stream (journal_kind=1 → TolerantStore), publish messages
    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        // Disk store (TolerantStore) for message persistence
        let stream = StreamConfig::new(b"durable", b"durable.>")
            .journal_kind(JournalKind::Disk)
            .build();
        client.create_stream(&stream).await.unwrap();

        // Publish 10 messages
        for i in 0u32..10 {
            let payload = format!("msg-{i}");
            client.publish(b"durable", b"durable.events", payload.as_bytes()).await.unwrap();
        }

        // Let writes settle
        tokio::time::sleep(Duration::from_millis(100)).await;

        shutdown(tx).await;
    }

    // Phase 2: restart, create consumer, verify messages arrive
    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let streams = client.list_streams().await.unwrap();
        assert_eq!(streams.len(), 1, "durable stream should survive restart");

        let c = ConsumerConfig::new(b"reader", b"durable")
            .ack_policy(AckPolicy::None)
            .build()
            .unwrap();
        let consumer = client.create_consumer(&c).await.unwrap();
        let mut sub = consumer.subscribe(None).await.unwrap();

        // Collect messages
        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.next()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }

        assert_eq!(received.len(), 10, "all 10 messages should survive restart, got {}", received.len());

        // Verify order
        for (i, msg) in received.iter().enumerate() {
            let expected = format!("msg-{i}");
            assert_eq!(msg.payload.as_ref(), expected.as_bytes(),
                "message {} payload mismatch", i);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Multiple restart cycles
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn multiple_restart_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    // Cycle 1: create stream A
    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        client.create_stream(&StreamConfig::new(b"alpha", b"alpha.>").build()).await.unwrap();
        shutdown(tx).await;
    }

    // Cycle 2: create stream B (A should still exist)
    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let streams = client.list_streams().await.unwrap();
        assert_eq!(streams.len(), 1, "alpha should survive first restart");

        client.create_stream(&StreamConfig::new(b"beta", b"beta.>").build()).await.unwrap();
        shutdown(tx).await;
    }

    // Cycle 3: verify both A and B exist
    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let streams = client.list_streams().await.unwrap();
        assert_eq!(streams.len(), 2, "both alpha and beta should survive two restarts");

        let names: Vec<&[u8]> = streams.iter().map(|s| s.name.as_slice()).collect();
        assert!(names.contains(&b"alpha".as_slice()));
        assert!(names.contains(&b"beta".as_slice()));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Idempotent create — same stream twice, still one after restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn idempotent_create_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        client.create_stream(&StreamConfig::new(b"unique", b"unique.>").build()).await.unwrap();
        // Second create should be idempotent (StreamAlreadyExists)
        let _ = client.create_stream(&StreamConfig::new(b"unique", b"unique.>").build()).await;

        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let streams = client.list_streams().await.unwrap();
        assert_eq!(streams.len(), 1, "duplicate create should not produce two streams after restart");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. Deleted consumer stays deleted after restart
//     NOTE: Skipped — engine.drain_consumer() only drains pending/bindings,
//     does not remove from catalog. Requires engine-level remove_consumer().
// ═══════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════
// 10b. Deleted disk stream data does not leak into recreated stream
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn deleted_disk_stream_data_does_not_leak() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    // Phase 1: create disk stream, publish 5 old messages, delete, recreate,
    // publish 2 new messages
    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let stream = StreamConfig::new(b"recycled", b"recycled.>")
            .journal_kind(JournalKind::Disk)
            .build();
        client.create_stream(&stream).await.unwrap();

        // Publish 5 "old" messages
        for i in 0u32..5 {
            let payload = format!("old-{i}");
            client.publish(b"recycled", b"recycled.data", payload.as_bytes()).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Delete the stream (should wipe store data)
        client.delete_stream(b"recycled").await.unwrap();

        // Recreate same stream
        client.create_stream(&stream).await.unwrap();

        // Publish 2 "new" messages
        for i in 0u32..2 {
            let payload = format!("new-{i}");
            client.publish(b"recycled", b"recycled.data", payload.as_bytes()).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        shutdown(tx).await;
    }

    // Phase 2: restart, verify only the 2 new messages exist
    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let streams = client.list_streams().await.unwrap();
        assert_eq!(streams.len(), 1, "recreated stream should survive restart");

        let c = ConsumerConfig::new(b"reader", b"recycled")
            .ack_policy(AckPolicy::None)
            .build()
            .unwrap();
        let consumer = client.create_consumer(&c).await.unwrap();
        let mut sub = consumer.subscribe(None).await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.next()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }

        assert_eq!(received.len(), 2,
            "only 2 new messages should exist, old data must not leak; got {}", received.len());

        let payloads: Vec<String> = received.iter()
            .map(|m| String::from_utf8_lossy(m.payload.as_ref()).to_string())
            .collect();
        assert_eq!(payloads[0], "new-0");
        assert_eq!(payloads[1], "new-1");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. Consumer created before shutdown receives messages after restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn consumer_and_messages_survive_together() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    // Phase 1: create stream + consumer + publish
    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let stream = StreamConfig::new(b"durable", b"durable.>")
            .journal_kind(JournalKind::Disk)
            .build();
        client.create_stream(&stream).await.unwrap();

        let c = ConsumerConfig::new(b"worker", b"durable")
            .ack_policy(AckPolicy::None)
            .build()
            .unwrap();
        client.create_consumer(&c).await.unwrap();

        for i in 0u32..5 {
            let payload = format!("event-{i}");
            client.publish(b"durable", b"durable.data", payload.as_bytes()).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown(tx).await;
    }

    // Phase 2: restart — both consumer and messages should exist
    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let streams = client.list_streams().await.unwrap();
        assert_eq!(streams.len(), 1, "stream should survive");

        let consumers = client.list_consumers().await.unwrap();
        assert_eq!(consumers.len(), 1, "consumer should survive");

        // Subscribe with the recovered consumer
        let c = ConsumerConfig::new(b"worker", b"durable")
            .ack_policy(AckPolicy::None)
            .build()
            .unwrap();
        let consumer = client.create_consumer(&c).await.unwrap();
        let mut sub = consumer.subscribe(None).await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.next()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }

        assert_eq!(received.len(), 5, "all 5 messages should survive restart, got {}", received.len());
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. Publish after restart continues correctly
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn publish_after_restart_continues() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    // Phase 1: create stream, publish 3 messages
    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let stream = StreamConfig::new(b"seq", b"seq.>")
            .journal_kind(JournalKind::Disk)
            .build();
        client.create_stream(&stream).await.unwrap();

        for i in 0u32..3 {
            let payload = format!("before-{i}");
            client.publish(b"seq", b"seq.data", payload.as_bytes()).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown(tx).await;
    }

    // Phase 2: restart, subscribe (seeds from store), then publish 3 more
    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        // Subscribe first — triggers seed_from_store (engine seqs align with store seqs)
        let c = ConsumerConfig::new(b"reader", b"seq")
            .ack_policy(AckPolicy::None)
            .build()
            .unwrap();
        let consumer = client.create_consumer(&c).await.unwrap();
        let mut sub = consumer.subscribe(None).await.unwrap();

        // Small delay so seeded messages arrive before new publishes
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Publish 3 more after subscribe
        for i in 0u32..3 {
            let payload = format!("after-{i}");
            client.publish(b"seq", b"seq.data", payload.as_bytes()).await.unwrap();
        }

        // Collect all messages
        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.next()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }

        assert_eq!(received.len(), 6, "should receive all 6 messages (3 before + 3 after restart), got {}", received.len());

        // Verify order: before-0, before-1, before-2, after-0, after-1, after-2
        let payloads: Vec<String> = received.iter()
            .map(|m| String::from_utf8_lossy(m.payload.as_ref()).to_string())
            .collect();
        assert_eq!(payloads[0], "before-0");
        assert_eq!(payloads[1], "before-1");
        assert_eq!(payloads[2], "before-2");
        assert_eq!(payloads[3], "after-0");
        assert_eq!(payloads[4], "after-1");
        assert_eq!(payloads[5], "after-2");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. Messages survive multiple restart cycles (disk store)
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn messages_survive_multiple_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    // Cycle 1: create stream + publish 3
    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let stream = StreamConfig::new(b"multi", b"multi.>")
            .journal_kind(JournalKind::Disk)
            .build();
        client.create_stream(&stream).await.unwrap();

        for i in 0u32..3 {
            let payload = format!("c1-{i}");
            client.publish(b"multi", b"multi.data", payload.as_bytes()).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown(tx).await;
    }

    // Cycle 2: publish 3 more
    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        for i in 0u32..3 {
            let payload = format!("c2-{i}");
            client.publish(b"multi", b"multi.data", payload.as_bytes()).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown(tx).await;
    }

    // Cycle 3: verify all 6 arrive
    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let c = ConsumerConfig::new(b"reader", b"multi")
            .ack_policy(AckPolicy::None)
            .build()
            .unwrap();
        let consumer = client.create_consumer(&c).await.unwrap();
        let mut sub = consumer.subscribe(None).await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.next()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }

        assert_eq!(received.len(), 6, "should receive all 6 messages across 2 cycles, got {}", received.len());

        let payloads: Vec<String> = received.iter()
            .map(|m| String::from_utf8_lossy(m.payload.as_ref()).to_string())
            .collect();
        assert_eq!(payloads[0], "c1-0");
        assert_eq!(payloads[3], "c2-0");
    }
}
