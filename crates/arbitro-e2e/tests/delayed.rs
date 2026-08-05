mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use bytes::Bytes;
use std::time::{Duration, Instant};

use arbitro_client_tokio::ClientError;
use arbitro_proto::error::ErrorCode;

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

    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Publish with 2s delay.
    let delay_ms = 2000u64;
    let publish_time = Instant::now();

    client
        .publish_delayed(
            stream_id,
            b"delayed_test.evt",
            Bytes::from_static(b"hello-delayed"),
            delay_ms,
        )
        .await
        .expect("publish_delayed should succeed");

    // The message should NOT arrive before the delay.
    let early_result = tokio::time::timeout(Duration::from_millis(1500), handle.recv()).await;
    assert!(
        early_result.is_err(),
        "message should NOT arrive before the 2s delay"
    );

    // The message SHOULD arrive after the delay (give it some slack).
    let msg = tokio::time::timeout(Duration::from_secs(4), handle.recv())
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
    // before the message matures. `spawn()` binds `:0` and hands the live
    // listener to the server (no drop-and-rebind TOCTOU); the restart
    // reuses the address it reported.
    let addr;
    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        addr = server.addr.clone();
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

        let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

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

// ===================================================================
// Audit #9: the delayed path must enforce the same idempotency window
// and DiscardPolicy::New quota pre-check as the immediate publish path.
// ===================================================================

/// A duplicate msg-id DELAYED publish must be deduped at publish time —
/// exactly one copy matures and is delivered, and the id is recorded in
/// the stream's dedup window (an immediate publish with the same id is
/// rejected too). Pre-fix, the delayed path skipped the window entirely:
/// both copies matured and were delivered.
///
/// The reference client's `publish_delayed` never sends a msg_id, so the
/// PUB-DELAYED frame is built raw in-test (what a foreign client sends).
#[tokio::test(flavor = "multi_thread")]
async fn delayed_publish_duplicate_msg_id_is_deduped() {
    use arbitro_proto::v2::ingress::pub_delayed_frame::PubDelayedFrame;
    use arbitro_proto::v2::magic::ARBITRO_MAGIC_V2;
    use tokio::io::AsyncWriteExt;

    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
    let client = server.connect().await;

    let stream_id = TestServer::parse_id(
        &client
            .create_stream(
                b"delay_dedup",
                b">",
                0,
                0,
                0,
                1,
                0,
                0,
                0,
                /*window_ms*/ 60_000,
            )
            .await
            .unwrap(),
    );
    let consumer_id = TestServer::parse_id(
        &client
            .create_consumer(stream_id, b"dd_c", b"", b"", 100, 1, 0, 0, 5000, 0)
            .await
            .unwrap(),
    );
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Raw socket: HELLO + the same delayed frame TWICE (same msg_id).
    let subject: &[u8] = b"delay_dedup.evt";
    let msg_id: &[u8] = b"delayed-dup-1";
    let payload: &[u8] = b"only-once";
    let delay_ms = 800u64;
    let size = PubDelayedFrame::wire_size(subject.len(), msg_id.len(), payload.len());

    let mut sock = tokio::net::TcpStream::connect(&server.addr)
        .await
        .expect("raw connect");
    let mut hello = Vec::with_capacity(8);
    hello.extend_from_slice(&ARBITRO_MAGIC_V2.to_le_bytes());
    hello.extend_from_slice(&[0u8; 4]);
    sock.write_all(&hello).await.expect("write HELLO");
    for seq in 1..=2u64 {
        let mut frame = vec![0u8; size];
        PubDelayedFrame::encode_into(
            &mut frame, seq, stream_id, 0, 0, subject, msg_id, payload, delay_ms,
        );
        sock.write_all(&frame).await.expect("write delayed frame");
    }
    sock.flush().await.expect("flush");

    // The id must be in the dedup window immediately (recorded at
    // publish time, not maturation) — an immediate publish with the
    // same id is rejected.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let err = client
        .publish_wait_with_id(
            stream_id,
            subject,
            msg_id,
            Bytes::from_static(b"immediate-dup"),
        )
        .await
        .expect_err("delayed publish must record its msg-id in the dedup window");
    assert!(
        matches!(
            err,
            ClientError::Broker {
                code: ErrorCode::IdempotencyDuplicate
            }
        ),
        "expected IdempotencyDuplicate, got {err:?}"
    );

    // Exactly ONE copy matures and is delivered.
    let msg = tokio::time::timeout(Duration::from_secs(5), handle.recv())
        .await
        .expect("the single delayed copy must mature and deliver")
        .expect("subscription open");
    assert_eq!(
        &msg.payload()[..],
        payload,
        "delivered payload must be the clean user payload (msg-id metadata stripped)"
    );
    msg.ack();

    let second = tokio::time::timeout(Duration::from_secs(2), handle.recv()).await;
    assert!(
        second.is_err(),
        "the duplicate delayed publish must NOT produce a second delivery"
    );

    drop(sock);
    server.shutdown().await;
}

/// A DELAYED publish to a full DiscardPolicy::New (discard=1) stream must
/// be rejected with StreamFull at publish time, exactly like the immediate
/// path. Pre-fix, the delayed path skipped the quota pre-check and the
/// message matured into the full stream.
///
/// Semantics note: the quota is evaluated against the store's occupancy at
/// PUBLISH time (mirror of the immediate path). Maturation itself does not
/// re-check — that gap is documented in ROBUSTNESS_AUDIT.md.
#[tokio::test(flavor = "multi_thread")]
async fn delayed_publish_respects_discard_new_quota() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
    let client = server.connect().await;

    // discard=1 (DiscardPolicy::New), max_msgs=2.
    let stream_id = TestServer::parse_id(
        &client
            .create_stream(
                b"delay_quota",
                b">",
                /*max_msgs*/ 2,
                0,
                0,
                1,
                0,
                0,
                /*discard*/ 1,
                0,
            )
            .await
            .unwrap(),
    );

    // Fill the stream to its quota with immediate publishes.
    for i in 0..2u8 {
        client
            .publish_wait(stream_id, b"delay_quota.k", Bytes::from(vec![i]))
            .await
            .expect("filling publish under quota must succeed");
    }

    // Over-quota DELAYED publish must be rejected at publish time.
    let err = client
        .publish_delayed(
            stream_id,
            b"delay_quota.k",
            Bytes::from_static(b"over-quota-delayed"),
            500,
        )
        .await
        .expect_err("delayed publish into a full discard=1 stream must be rejected");
    assert!(
        matches!(
            err,
            ClientError::Broker {
                code: ErrorCode::StreamFull
            }
        ),
        "expected StreamFull, got {err:?}"
    );

    // The delay==0 fast path of PublishDelayed must enforce it too.
    let err = client
        .publish_delayed(
            stream_id,
            b"delay_quota.k",
            Bytes::from_static(b"over-quota-immediate"),
            0,
        )
        .await
        .expect_err("delay=0 publish into a full discard=1 stream must be rejected");
    assert!(
        matches!(
            err,
            ClientError::Broker {
                code: ErrorCode::StreamFull
            }
        ),
        "expected StreamFull on the delay=0 path, got {err:?}"
    );

    server.shutdown().await;
}
