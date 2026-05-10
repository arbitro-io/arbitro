//! Persistence tests — verify metadata and store data survive server restart.

use std::time::Duration;

use arbitro_client_tokio::{Client, ClientConfig};
use bytes::Bytes;
use arbitro_server::command_log::{CommandLog, SharedCommandLog};
use arbitro_server::{ArbitroServer, Config};
use tokio::sync::watch;

// ── Helpers ───────────────────────────────────────────────────────────────────

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

    tokio::spawn(async move { let _ = server.run_with_shutdown(rx).await; });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (tx, addr)
}

async fn connect(addr: &str) -> Client {
    Client::connect(ClientConfig { addr: addr.to_string(), ..ClientConfig::default() })
        .await.expect("client should connect")
}

async fn shutdown(tx: watch::Sender<bool>) {
    let _ = tx.send(true);
    tokio::time::sleep(Duration::from_millis(500)).await;
}

fn parse_id(resp: &Bytes) -> u32 {
    u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32
}

fn stream_count(resp: &Bytes) -> usize {
    u32::from_le_bytes(resp[..4].try_into().unwrap()) as usize
}

fn stream_names(resp: &Bytes) -> Vec<Vec<u8>> {
    let count = u32::from_le_bytes(resp[..4].try_into().unwrap()) as usize;
    let mut names = Vec::with_capacity(count);
    let mut pos = 4usize;
    for _ in 0..count {
        pos += 4; // wire_id
        let name_len = u16::from_le_bytes(resp[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        names.push(resp[pos..pos + name_len].to_vec());
        pos += name_len;
    }
    names
}

fn consumer_count(resp: &Bytes) -> usize {
    u32::from_le_bytes(resp[..4].try_into().unwrap()) as usize
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Stream metadata survives restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn stream_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        client.create_stream(b"orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0).await.unwrap();
        assert_eq!(stream_count(&client.list_streams(0, 1000).await.unwrap()), 1);
        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(stream_count(&resp), 1, "stream should survive restart");
        let names = stream_names(&resp);
        assert_eq!(names[0], b"orders");
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
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        client.create_stream(b"orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0).await.unwrap();
        client.create_stream(b"events", b"events.>", 0, 0, 0, 1, 0, 0, 0).await.unwrap();
        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(stream_count(&resp), 2, "both streams should survive restart");
        let names = stream_names(&resp);
        assert!(names.iter().any(|n| n == b"orders"), "orders missing");
        assert!(names.iter().any(|n| n == b"events"), "events missing");
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
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        client.create_stream(b"temp", b"temp.>", 0, 0, 0, 1, 0, 0, 0).await.unwrap();
        client.delete_stream(b"temp").await.unwrap();
        assert_eq!(stream_count(&client.list_streams(0, 1000).await.unwrap()), 0);
        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        assert_eq!(
            stream_count(&client.list_streams(0, 1000).await.unwrap()),
            0,
            "deleted stream should not reappear"
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. No data_dir — no persistence
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn no_data_dir_works_without_persistence() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let (tx, rx) = watch::channel(false);
    let config = Config::default().listen_addr(&addr).shard_count(2).channel_capacity(1024);
    let server = ArbitroServer::new(config);
    tokio::spawn(async move { let _ = server.run_with_shutdown(rx).await; });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = connect(&addr).await;
    client.create_stream(b"ephemeral", b">", 0, 0, 0, 1, 0, 0, 0).await.unwrap();
    assert_eq!(stream_count(&client.list_streams(0, 1000).await.unwrap()), 1);
    let _ = tx.send(true);
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Command log file is created on disk
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn command_log_file_is_created() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let log_path = dir.path().join("metadata.log");

    let (tx, addr) = start_server_with_dir(dir_str).await;
    let client = connect(&addr).await;
    client.create_stream(b"logged", b"logged.>", 0, 0, 0, 1, 0, 0, 0).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(log_path.exists(), "metadata.log should be created");
    assert!(std::fs::metadata(&log_path).unwrap().len() > 0, "metadata.log should be non-empty");
    shutdown(tx).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Consumer survives restart
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn consumer_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        let resp = client.create_stream(b"orders", b"orders.>", 0, 0, 0, 1, 0, 0, 0).await.unwrap();
        let sid = parse_id(&resp);
        client.create_consumer(sid, b"worker1", b"", b"", u16::MAX, 1, 0, 0, 0, 0).await.unwrap();
        assert_eq!(consumer_count(&client.list_consumers(0, 0, 1000).await.unwrap()), 1);
        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        assert_eq!(stream_count(&client.list_streams(0, 1000).await.unwrap()), 1, "stream should survive");
        assert_eq!(consumer_count(&client.list_consumers(0, 0, 1000).await.unwrap()), 1, "consumer should survive");
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
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        let resp = client.create_stream(b"durable", b"durable.>", 0, 0, 0, 1, 1, 0, 0).await.unwrap();
        let sid = parse_id(&resp);
        for i in 0u32..10 {
            let payload = format!("msg-{i}");
            client.publish(sid, b"durable.events", Bytes::copy_from_slice(payload.as_bytes())).expect("publish");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        assert_eq!(stream_count(&client.list_streams(0, 1000).await.unwrap()), 1);

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = {
            let mut pos = 4usize;
            let wire_id = u32::from_le_bytes(resp[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let name_len = u16::from_le_bytes(resp[pos..pos + 2].try_into().unwrap()) as usize;
            let _ = name_len;
            wire_id
        };

        let resp = client.create_consumer(sid, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0).await.unwrap();
        let cid = parse_id(&resp);
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
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        client.create_stream(b"alpha", b"alpha.>", 0, 0, 0, 1, 0, 0, 0).await.unwrap();
        shutdown(tx).await;
    }

    { // Cycle 2: create stream B (A should still exist)
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        assert_eq!(stream_count(&client.list_streams(0, 1000).await.unwrap()), 1, "alpha should survive first restart");
        client.create_stream(b"beta", b"beta.>", 0, 0, 0, 1, 0, 0, 0).await.unwrap();
        shutdown(tx).await;
    }

    { // Cycle 3: verify both A and B exist
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(stream_count(&resp), 2, "both alpha and beta should survive");
        let names = stream_names(&resp);
        assert!(names.iter().any(|n| n == b"alpha"), "alpha missing");
        assert!(names.iter().any(|n| n == b"beta"), "beta missing");
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
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        client.create_stream(b"unique", b"unique.>", 0, 0, 0, 1, 0, 0, 0).await.unwrap();
        let _ = client.create_stream(b"unique", b"unique.>", 0, 0, 0, 1, 0, 0, 0).await;
        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        assert_eq!(
            stream_count(&client.list_streams(0, 1000).await.unwrap()),
            1,
            "duplicate create should not produce two streams after restart"
        );
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
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        let resp = client.create_stream(b"recycled", b"recycled.>", 0, 0, 0, 1, 1, 0, 0).await.unwrap();
        let sid = parse_id(&resp);

        for i in 0u32..5 {
            let payload = format!("old-{i}");
            client.publish(sid, b"recycled.data", Bytes::copy_from_slice(payload.as_bytes())).expect("publish");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        client.delete_stream(b"recycled").await.unwrap();
        let resp = client.create_stream(b"recycled", b"recycled.>", 0, 0, 0, 1, 1, 0, 0).await.unwrap();
        let sid2 = parse_id(&resp);

        for i in 0u32..2 {
            let payload = format!("new-{i}");
            client.publish(sid2, b"recycled.data", Bytes::copy_from_slice(payload.as_bytes())).expect("publish");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        assert_eq!(stream_count(&client.list_streams(0, 1000).await.unwrap()), 1);

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = u32::from_le_bytes(resp[4..8].try_into().unwrap());
        let resp = client.create_consumer(sid, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0).await.unwrap();
        let cid = parse_id(&resp);
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
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        let resp = client.create_stream(b"durable", b"durable.>", 0, 0, 0, 1, 1, 0, 0).await.unwrap();
        let sid = parse_id(&resp);
        client.create_consumer(sid, b"worker", b"", b"", u16::MAX, 0, 0, 0, 0, 0).await.unwrap();
        for i in 0u32..5 {
            let payload = format!("event-{i}");
            client.publish(sid, b"durable.data", Bytes::copy_from_slice(payload.as_bytes())).expect("publish");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        assert_eq!(stream_count(&client.list_streams(0, 1000).await.unwrap()), 1);
        assert_eq!(consumer_count(&client.list_consumers(0, 0, 1000).await.unwrap()), 1);

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = u32::from_le_bytes(resp[4..8].try_into().unwrap());
        let resp = client.create_consumer(sid, b"worker", b"", b"", u16::MAX, 0, 0, 0, 0, 0).await.unwrap();
        let cid = parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
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

#[tokio::test(flavor = "multi_thread")]
async fn publish_after_restart_continues() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        let resp = client.create_stream(b"seq", b"seq.>", 0, 0, 0, 1, 1, 0, 0).await.unwrap();
        let sid = parse_id(&resp);
        for i in 0u32..3 {
            let payload = format!("before-{i}");
            client.publish(sid, b"seq.data", Bytes::copy_from_slice(payload.as_bytes())).expect("publish");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = u32::from_le_bytes(resp[4..8].try_into().unwrap());

        let resp = client.create_consumer(sid, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0).await.unwrap();
        let cid = parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        for i in 0u32..3 {
            let payload = format!("after-{i}");
            client.publish(sid, b"seq.data", Bytes::copy_from_slice(payload.as_bytes())).expect("publish");
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
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        let resp = client.create_stream(b"multi", b"multi.>", 0, 0, 0, 1, 1, 0, 0).await.unwrap();
        let sid = parse_id(&resp);
        for i in 0u32..3 {
            let payload = format!("c1-{i}");
            client.publish(sid, b"multi.data", Bytes::copy_from_slice(payload.as_bytes())).expect("publish");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown(tx).await;
    }

    {
        let (tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = u32::from_le_bytes(resp[4..8].try_into().unwrap());
        for i in 0u32..3 {
            let payload = format!("c2-{i}");
            client.publish(sid, b"multi.data", Bytes::copy_from_slice(payload.as_bytes())).expect("publish");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown(tx).await;
    }

    {
        let (_tx, addr) = start_server_with_dir(dir_str).await;
        let client = connect(&addr).await;
        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = u32::from_le_bytes(resp[4..8].try_into().unwrap());
        let resp = client.create_consumer(sid, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0).await.unwrap();
        let cid = parse_id(&resp);
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
    }
}
