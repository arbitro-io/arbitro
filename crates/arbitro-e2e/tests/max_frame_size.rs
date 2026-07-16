//! max_frame_size boundary tests.
//!
//! The server checks `msg_len > max_frame_size` on the raw v2 wire frame
//! (see `arbitro-server/src/server.rs`), where `msg_len` is the PUB frame
//! body: `PUB_BODY_FIXED(8) + subject_len + msg_id_len + payload_len`.
//! A publish whose wire `msg_len` is exactly `max_frame_size` must be
//! accepted; one byte over must drop the connection.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::ClientConfig;
use arbitro_proto::v2::ingress::pub_frame::PUB_BODY_FIXED;
use bytes::Bytes;
use std::time::Duration;

const SUBJECT: &[u8] = b"limits.frame";

/// Compute the payload length that makes a `publish_wait(stream_id, SUBJECT, payload)`
/// frame's wire `msg_len` equal exactly `target_msg_len`.
fn payload_len_for_msg_len(target_msg_len: usize) -> usize {
    target_msg_len - PUB_BODY_FIXED - SUBJECT.len()
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST-2a: Payload at exactly max_frame_size succeeds.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn publish_at_exact_max_frame_size_succeeds() {
    const MAX_FRAME_SIZE: usize = 64 * 1024; // small cap, fast to allocate

    let mut server = TestServerBuilder::new()
        .max_frame_size(MAX_FRAME_SIZE)
        .spawn()
        .await;
    let client = server.connect().await;

    let stream_id = {
        let resp = client
            .create_stream(b"exact_limit", b">", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        TestServer::parse_id(&resp)
    };

    let payload_len = payload_len_for_msg_len(MAX_FRAME_SIZE);
    let payload = vec![0xABu8; payload_len];

    let resp = client
        .publish_wait(stream_id, SUBJECT, Bytes::from(payload))
        .await
        .expect("publish at exactly max_frame_size must succeed");
    let _ = TestServer::parse_id(&resp);

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST-2b: Payload one byte over max_frame_size is rejected — connection
// is dropped by the server (frame never dispatched, no partial write).
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn publish_one_byte_over_max_frame_size_disconnects() {
    const MAX_FRAME_SIZE: usize = 64 * 1024;

    let mut server = TestServerBuilder::new()
        .max_frame_size(MAX_FRAME_SIZE)
        .spawn()
        .await;

    // Disable auto-reconnect so the disconnect is directly observable
    // instead of being silently papered over by a background retry.
    let client = server
        .connect_with_config(ClientConfig {
            reconnect: arbitro_client_tokio::ReconnectPolicy {
                max_attempts: Some(0),
                ..Default::default()
            },
            ..ClientConfig::default()
        })
        .await;

    let stream_id = {
        let resp = client
            .create_stream(b"over_limit", b">", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        TestServer::parse_id(&resp)
    };

    let payload_len = payload_len_for_msg_len(MAX_FRAME_SIZE + 1);
    let payload = vec![0xCDu8; payload_len];

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        client.publish_wait(stream_id, SUBJECT, Bytes::from(payload)),
    )
    .await
    .expect("server must not hang — it should drop the connection promptly");

    assert!(
        result.is_err(),
        "publish one byte over max_frame_size must not succeed"
    );

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST-2c: Multi-MB payload round-trips (well under the configured cap).
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn multi_mb_payload_round_trips() {
    const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
    const PAYLOAD_SIZE: usize = 4 * 1024 * 1024;

    let mut server = TestServerBuilder::new()
        .max_frame_size(MAX_FRAME_SIZE)
        .spawn()
        .await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"big_payload", b">", 0, 0, 0, 1, 1, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let consumer_resp = client
        .create_consumer(stream_id, b"reader", b"", b"", u16::MAX, 0, 0, 0, 0, 0)
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&consumer_resp);
    let mut sub = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Deterministic, non-zero pattern so a partial/garbled round-trip is
    // detectable and not just a length check.
    let payload: Vec<u8> = (0..PAYLOAD_SIZE).map(|i| (i % 251) as u8).collect();

    client
        .publish_wait(stream_id, b"big.data", Bytes::copy_from_slice(&payload))
        .await
        .expect("multi-MB publish must succeed");

    let msg = tokio::time::timeout(Duration::from_secs(10), sub.recv())
        .await
        .expect("delivery timeout")
        .expect("subscription closed");

    assert_eq!(
        msg.payload().len(),
        PAYLOAD_SIZE,
        "delivered payload length mismatch"
    );
    assert_eq!(
        &msg.payload()[..],
        payload.as_slice(),
        "delivered payload content mismatch"
    );
    msg.ack();

    server.shutdown().await;
}

/// Sanity check for the arithmetic helper itself — protects the other
/// tests in this file from a silent off-by-one in `payload_len_for_msg_len`.
#[test]
fn payload_len_helper_matches_wire_size() {
    use arbitro_proto::v2::ingress::pub_frame::PubFrame;

    let target = 64 * 1024usize;
    let payload_len = payload_len_for_msg_len(target);
    let msg_len = PubFrame::wire_size(SUBJECT.len(), 0, payload_len)
        - arbitro_proto::v2::header::HEADER_SIZE;
    assert_eq!(msg_len, target);
}
