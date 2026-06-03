mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use std::time::{Duration, Instant};
use bytes::Bytes;

// ===================================================================
// Item 14: Publish with 2s delay, verify message arrives after 2s
// ===================================================================

#[tokio::test(flavor = "multi_thread")]
async fn delayed_publish_arrives_after_delay() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
    let client = server.connect().await;

    // Create stream + consumer.
    let resp = client
        .create_stream(b"delayed_test", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id, b"delayed_worker", b"", b"", 10, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);

    let mut handle = client
        .subscribe(stream_id, consumer_id, b"")
        .await
        .unwrap();

    // Publish with 2s delay.
    let delay_ms = 2000u64;
    let publish_time = Instant::now();

    client
        .publish_delayed(stream_id, b"delayed_test.evt", Bytes::from_static(b"hello-delayed"), delay_ms)
        .await
        .expect("publish_delayed should succeed");

    // The message should NOT arrive before the delay.
    let early_result = tokio::time::timeout(
        Duration::from_millis(1500),
        handle.recv(),
    ).await;
    assert!(
        early_result.is_err(),
        "message should NOT arrive before the 2s delay"
    );

    // The message SHOULD arrive after the delay (give it some slack).
    let msg = tokio::time::timeout(
        Duration::from_secs(4),
        handle.recv(),
    )
    .await
    .expect("message should arrive after the delay")
    .expect("channel should be open");

    let elapsed = publish_time.elapsed();
    assert!(
        elapsed >= Duration::from_millis(1800),
        "message arrived too early: {:?} (expected >= 2s)",
        elapsed
    );
    assert_eq!(&msg.payload()[..], b"hello-delayed");

    msg.ack();
    server.shutdown().await;
}

// ===================================================================
// Item 15: Broker restart mid-delay, message still delivers
// ===================================================================

#[tokio::test(flavor = "multi_thread")]
async fn delayed_publish_survives_broker_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    // Phase 1: Start server, publish a delayed message, then shut down
    // before the message matures.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    {
        let mut server = TestServerBuilder::new()
            .data_dir(dir_str)
            .spawn_on(&addr)
            .await;
        let client = server.connect().await;

        // Create stream.
        let resp = client
            .create_stream(b"delayed_restart", b">", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        let stream_id = TestServer::parse_id(&resp);

        // Publish with 4s delay — we'll restart before maturation.
        client
            .publish_delayed(
                stream_id,
                b"delayed_restart.evt",
                Bytes::from_static(b"survive-restart"),
                4000,
            )
            .await
            .expect("publish_delayed should succeed");

        // Wait 1s then shut down (message should NOT have matured yet).
        tokio::time::sleep(Duration::from_secs(1)).await;
        client.close();
        server.shutdown().await;
    }

    // Phase 2: Restart the server on the same address/data_dir.
    // The delayed journal recovery should catch up the matured entry
    // (or re-schedule it if still pending).
    // Wait a bit so that by restart time, the 4s delay has passed.
    tokio::time::sleep(Duration::from_secs(4)).await;

    {
        let mut server = TestServerBuilder::new()
            .data_dir(dir_str)
            .spawn_on(&addr)
            .await;
        let client = server.connect().await;

        // Re-resolve the stream (metadata survived via command log).
        let resp = client.list_streams(0, 1000).await.unwrap();
        let stream_id = TestServer::find_stream_id(&resp, b"delayed_restart")
            .expect("stream should survive restart");

        // Create a fresh consumer + subscribe.
        let resp = client
            .create_consumer(stream_id, b"delayed_restart_c", b"", b"", 10, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        let consumer_id = TestServer::parse_id(&resp);

        let mut handle = client
            .subscribe(stream_id, consumer_id, b"")
            .await
            .unwrap();

        // The matured delayed message should have been caught up on restart
        // and is now in the main store, so it should be delivered.
        let msg = tokio::time::timeout(Duration::from_secs(5), handle.recv())
            .await
            .expect("delayed message should arrive after restart")
            .expect("channel should be open");

        assert_eq!(&msg.payload()[..], b"survive-restart");
        msg.ack();

        server.shutdown().await;
    }
}
