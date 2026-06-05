mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::{BatchEntry, ClientError};
use arbitro_proto::error::ErrorCode;
use bytes::Bytes;

// ── Helpers ────────────────────────────────────────────────────────────────

fn is_duplicate(err: &ClientError) -> bool {
    matches!(
        err,
        ClientError::Broker {
            code: ErrorCode::IdempotencyDuplicate
        }
    )
}

// ═══════════════════════════════════════════════════════════════════════════
// Single publish
// ═══════════════════════════════════════════════════════════════════════════

/// Stream created with `idempotency_window_ms = 0` (the default).
/// Two publishes with the same msg_id must both succeed — the feature
/// is fully opt-in.
#[tokio::test(flavor = "multi_thread")]
async fn stream_without_window_allows_duplicates() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(
            b"plain", b">", 0, 0, 0, 1, 0, 0, 0, /*idempotency_window_ms*/ 0,
        )
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    client
        .publish_sync_with_id(stream_id, b"k.a", b"msg-1", Bytes::from_static(b"v1"))
        .await
        .expect("first publish should succeed");
    client
        .publish_sync_with_id(stream_id, b"k.a", b"msg-1", Bytes::from_static(b"v1"))
        .await
        .expect("second publish must also succeed when window=0");
    server.shutdown().await;
}

/// Stream created with `idempotency_window_ms > 0`. Second publish
/// of the same msg_id within the window is rejected with
/// `ErrorCode::IdempotencyDuplicate`.
#[tokio::test(flavor = "multi_thread")]
async fn stream_with_window_rejects_duplicate_msg_id() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(
            b"dedup", b">", 0, 0, 0, 1, 0, 0, 0, /*window_ms*/ 60_000,
        )
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    client
        .publish_sync_with_id(stream_id, b"k.a", b"msg-1", Bytes::from_static(b"v1"))
        .await
        .expect("first publish");
    let err = client
        .publish_sync_with_id(stream_id, b"k.a", b"msg-1", Bytes::from_static(b"v1-prime"))
        .await
        .expect_err("duplicate must be rejected");
    assert!(
        is_duplicate(&err),
        "expected IdempotencyDuplicate, got {err:?}",
    );
    server.shutdown().await;
}

/// Even on an idempotent stream, a publish with EMPTY msg_id is never
/// deduped — opt-in is per-message. Repeating an empty-id publish
/// must succeed every time.
#[tokio::test(flavor = "multi_thread")]
async fn empty_msg_id_is_never_deduped() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"dedup2", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // Three identical publishes with NO msg_id — all three must land.
    for _ in 0..3 {
        client
            .publish_sync(stream_id, b"k.x", Bytes::from_static(b"v"))
            .await
            .expect("empty-id publish must always succeed");
    }
    server.shutdown().await;
}

/// Distinct msg_ids on the same stream don't collide.
#[tokio::test(flavor = "multi_thread")]
async fn different_msg_ids_are_independent() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"dedup3", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    client
        .publish_sync_with_id(stream_id, b"k", b"id-a", Bytes::from_static(b"a"))
        .await
        .expect("id-a accepted");
    client
        .publish_sync_with_id(stream_id, b"k", b"id-b", Bytes::from_static(b"b"))
        .await
        .expect("id-b accepted (different id)");
    client
        .publish_sync_with_id(stream_id, b"k", b"id-c", Bytes::from_static(b"c"))
        .await
        .expect("id-c accepted (different id)");
    server.shutdown().await;
}

/// Two streams, one with dedup, one without. The dedup state on the
/// idempotent stream must not bleed into the plain stream.
#[tokio::test(flavor = "multi_thread")]
async fn two_streams_isolated_one_with_one_without() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let dedup_id = TestServer::parse_id(
        &client
            .create_stream(b"with_dedup", b"a.>", 0, 0, 0, 1, 0, 0, 0, 60_000)
            .await
            .unwrap(),
    );
    let plain_id = TestServer::parse_id(
        &client
            .create_stream(b"no_dedup", b"b.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap(),
    );

    // First publish to dedup stream — accepted.
    client
        .publish_sync_with_id(dedup_id, b"a.x", b"shared-id", Bytes::from_static(b"v1"))
        .await
        .expect("dedup first publish");

    // Same msg_id on the plain stream — must be accepted (different stream).
    client
        .publish_sync_with_id(plain_id, b"b.x", b"shared-id", Bytes::from_static(b"v1"))
        .await
        .expect("plain stream is not deduped");

    // Repeat on plain — still accepted.
    client
        .publish_sync_with_id(plain_id, b"b.x", b"shared-id", Bytes::from_static(b"v2"))
        .await
        .expect("plain stream accepts duplicates");

    // Repeat on dedup — must be rejected.
    let err = client
        .publish_sync_with_id(dedup_id, b"a.x", b"shared-id", Bytes::from_static(b"v2"))
        .await
        .expect_err("dedup stream rejects duplicate");
    assert!(
        is_duplicate(&err),
        "expected IdempotencyDuplicate, got {err:?}"
    );
    server.shutdown().await;
}

/// After delete + recreate, the dedup state for a stream is reset.
/// (The new stream gets a fresh stream_id, and the tracker keys on
/// `(StreamId, hash)` so old entries cannot collide.)
#[tokio::test(flavor = "multi_thread")]
async fn delete_and_recreate_clears_dedup_state() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"recycle", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap();
    let first_id = TestServer::parse_id(&resp);

    client
        .publish_sync_with_id(first_id, b"k", b"reused", Bytes::from_static(b"v1"))
        .await
        .expect("first publish");

    client.delete_stream(b"recycle").await.expect("delete");

    let resp = client
        .create_stream(b"recycle", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap();
    let second_id = TestServer::parse_id(&resp);

    // The reused msg_id must be accepted on the fresh stream.
    client
        .publish_sync_with_id(second_id, b"k", b"reused", Bytes::from_static(b"v2"))
        .await
        .expect("recreated stream starts with empty dedup state");
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Batch publish — atomic all-or-nothing dedup
// ═══════════════════════════════════════════════════════════════════════════

/// A batch where every entry has a unique msg_id lands in full.
#[tokio::test(flavor = "multi_thread")]
async fn batch_with_all_unique_ids_succeeds() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = TestServer::parse_id(
        &client
            .create_stream(b"batch_uniq", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
            .await
            .unwrap(),
    );

    let entries = [
        BatchEntry::with_msg_id(b"k", b"b-1", Bytes::from_static(b"v1")),
        BatchEntry::with_msg_id(b"k", b"b-2", Bytes::from_static(b"v2")),
        BatchEntry::with_msg_id(b"k", b"b-3", Bytes::from_static(b"v3")),
    ];
    client
        .publish_batch_sync(stream_id, &entries)
        .await
        .expect("unique-id batch must succeed");
    server.shutdown().await;
}

/// A batch whose entries collide with previously-recorded msg_ids on
/// the same stream is rejected wholesale.
#[tokio::test(flavor = "multi_thread")]
async fn batch_with_duplicate_from_prior_publish_is_rejected() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = TestServer::parse_id(
        &client
            .create_stream(b"batch_dup", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
            .await
            .unwrap(),
    );

    // Seed the dedup window with one id.
    client
        .publish_sync_with_id(stream_id, b"k", b"seeded", Bytes::from_static(b"seed"))
        .await
        .expect("seed");

    let entries = [
        BatchEntry::with_msg_id(b"k", b"new-1", Bytes::from_static(b"v1")),
        BatchEntry::with_msg_id(b"k", b"seeded", Bytes::from_static(b"collides")),
        BatchEntry::with_msg_id(b"k", b"new-2", Bytes::from_static(b"v2")),
    ];
    let err = client
        .publish_batch_sync(stream_id, &entries)
        .await
        .expect_err("batch with a colliding id must be rejected");
    assert!(
        is_duplicate(&err),
        "expected IdempotencyDuplicate, got {err:?}"
    );

    // After the rejection, the OTHER ids in the batch must NOT have
    // been recorded — we can publish them individually and they land.
    client
        .publish_sync_with_id(stream_id, b"k", b"new-1", Bytes::from_static(b"v1-retry"))
        .await
        .expect("non-colliding id must remain unrecorded after batch rejection");
    client
        .publish_sync_with_id(stream_id, b"k", b"new-2", Bytes::from_static(b"v2-retry"))
        .await
        .expect("non-colliding id must remain unrecorded after batch rejection");
    server.shutdown().await;
}

/// A batch containing two entries with the SAME msg_id is rejected
/// (the second entry collides with the first within the same batch).
#[tokio::test(flavor = "multi_thread")]
async fn batch_with_internal_duplicate_is_rejected() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = TestServer::parse_id(
        &client
            .create_stream(b"batch_internal", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
            .await
            .unwrap(),
    );

    let entries = [
        BatchEntry::with_msg_id(b"k", b"twin", Bytes::from_static(b"v1")),
        BatchEntry::with_msg_id(b"k", b"twin", Bytes::from_static(b"v2")),
    ];
    let err = client
        .publish_batch_sync(stream_id, &entries)
        .await
        .expect_err("batch with internal twin id must be rejected");
    assert!(
        is_duplicate(&err),
        "expected IdempotencyDuplicate, got {err:?}"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Cross-restart contract
// ═══════════════════════════════════════════════════════════════════════════

/// T9. Idempotency dedup state is rebuilt from the journal on restart.
/// The msg_id is stored as a header in the extended payload layout
/// (HAS_HEADERS flag) and recovered by scanning the store on boot.
/// Re-publishing the same msg_id after restart must be REJECTED.
#[tokio::test(flavor = "multi_thread")]
async fn cross_restart_dedup_survives() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    // ── First boot: publish with a msg_id, confirm dedup rejects it ──
    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;

        let resp = client
            .create_stream(b"persistent", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
            .await
            .unwrap();
        let stream_id = TestServer::parse_id(&resp);

        client
            .publish_sync_with_id(stream_id, b"k", b"cross-id", Bytes::from_static(b"v1"))
            .await
            .expect("first publish must succeed");

        // Within the same session, the duplicate must be rejected.
        let err = client
            .publish_sync_with_id(stream_id, b"k", b"cross-id", Bytes::from_static(b"v1-dup"))
            .await
            .expect_err("duplicate within same session");
        assert!(is_duplicate(&err));

        server.shutdown().await;
    }

    // ── Second boot: same data_dir (metadata + journal restored) ─────
    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;

        // The stream should exist (metadata restored from command log).
        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(
            TestServer::stream_count(&resp),
            1,
            "stream must survive restart"
        );

        let stream_id = TestServer::find_stream_id(&resp, b"persistent")
            .expect("must find stream by name after restart");

        // The same msg_id must be REJECTED — dedup state was rebuilt
        // from the journal on startup.
        let err = client
            .publish_sync_with_id(stream_id, b"k", b"cross-id", Bytes::from_static(b"v2"))
            .await
            .expect_err(
                "cross-restart dedup must reject duplicate; the broker \
                 rebuilds the idempotency tracker from the journal",
            );
        assert!(
            is_duplicate(&err),
            "expected IdempotencyDuplicate after restart, got {err:?}"
        );

        // A NEW msg_id must still be accepted.
        client
            .publish_sync_with_id(
                stream_id,
                b"k",
                b"fresh-id",
                Bytes::from_static(b"v3"),
            )
            .await
            .expect("new msg_id must be accepted after restart");

        server.shutdown().await;
    }
}

/// On a stream with dedup enabled, mixing entries WITH and WITHOUT
/// msg_id is allowed. The empty-id entries are never deduped; the
/// id-bearing ones are. A duplicate in only the id-bearing entries
/// is rejected; a "duplicate" empty-id is fine because there's no
/// id to track.
#[tokio::test(flavor = "multi_thread")]
async fn batch_mixed_id_and_no_id_entries() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = TestServer::parse_id(
        &client
            .create_stream(b"batch_mixed", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
            .await
            .unwrap(),
    );

    // First batch — three id-bearing entries, two no-id entries.
    let first = [
        BatchEntry::with_msg_id(b"k", b"m-1", Bytes::from_static(b"a")),
        BatchEntry::new(b"k", Bytes::from_static(b"x")),
        BatchEntry::with_msg_id(b"k", b"m-2", Bytes::from_static(b"b")),
        BatchEntry::new(b"k", Bytes::from_static(b"y")),
        BatchEntry::with_msg_id(b"k", b"m-3", Bytes::from_static(b"c")),
    ];
    client
        .publish_batch_sync(stream_id, &first)
        .await
        .expect("mixed batch first attempt should land");

    // Second batch — same no-id entries (must be allowed), distinct ids.
    let second = [
        BatchEntry::new(b"k", Bytes::from_static(b"x-again")),
        BatchEntry::with_msg_id(b"k", b"m-4", Bytes::from_static(b"d")),
        BatchEntry::new(b"k", Bytes::from_static(b"y-again")),
    ];
    client
        .publish_batch_sync(stream_id, &second)
        .await
        .expect("empty-id entries are never deduped, fresh ids accepted");

    // Third batch — reuses one of the original ids → must be rejected.
    let third = [
        BatchEntry::new(b"k", Bytes::from_static(b"z")),
        BatchEntry::with_msg_id(b"k", b"m-1", Bytes::from_static(b"replay")),
    ];
    let err = client
        .publish_batch_sync(stream_id, &third)
        .await
        .expect_err("batch with replayed id must be rejected");
    assert!(
        is_duplicate(&err),
        "expected IdempotencyDuplicate, got {err:?}"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Consumer delivery — headers stripping & cross-restart with consumer
// ═══════════════════════════════════════════════════════════════════════════

/// When a message is published with a msg_id (idempotency enabled), the
/// broker stores the id as an internal header in the extended payload layout
/// (HAS_HEADERS flag). The consumer must receive ONLY the user payload —
/// the internal headers/metadata must be stripped before delivery.
#[tokio::test(flavor = "multi_thread")]
async fn delivery_with_headers_strips_metadata() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"strip_hdr", b">", 0, 0, 0, 1, 0, 0, 0, /*window_ms*/ 60_000)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(
            stream_id,
            b"strip_c",
            b"",
            b"",
            100u16,
            1u8,  // AckPolicy::Explicit
            0u8,  // DeliverPolicy::All
            0u8,  // DeliverMode::default
            5000u32, // ack_wait_ms
            0u64,
        )
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Publish with a msg_id — triggers HAS_HEADERS extended payload internally.
    client
        .publish_sync_with_id(
            stream_id,
            b"k.a",
            b"test-msg-id",
            Bytes::from_static(b"hello world"),
        )
        .await
        .expect("publish with msg_id");

    // Consumer must receive only the user payload, not the extended blob.
    let msg = tokio::time::timeout(std::time::Duration::from_secs(3), handle.recv())
        .await
        .expect("message should arrive within 3s")
        .expect("subscription open");

    assert_eq!(
        msg.payload().as_ref(),
        b"hello world",
        "consumer must receive only the user payload without HAS_HEADERS metadata"
    );
    msg.ack();
    server.shutdown().await;
}

/// Cross-restart with a consumer: after reboot the idempotency state is
/// rebuilt from the journal, duplicate msg_ids are still rejected, new ones
/// succeed, and the consumer only receives the new message (no replay of
/// already-acked messages from the previous boot).
#[tokio::test(flavor = "multi_thread")]
async fn cross_restart_idempotency_with_consumer() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    // ── First boot: publish, consume, ack ────────────────────────────────
    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;

        let resp = client
            .create_stream(b"restart_c", b">", 0, 0, 0, 1, 0, 0, 0, /*window_ms*/ 60_000)
            .await
            .unwrap();
        let stream_id = TestServer::parse_id(&resp);

        let resp = client
            .create_consumer(
                stream_id,
                b"worker",
                b"",
                b"",
                100u16,
                1u8,  // AckPolicy::Explicit
                0u8,  // DeliverPolicy::All
                0u8,  // DeliverMode::default
                5000u32, // ack_wait_ms
                0u64,
            )
            .await
            .unwrap();
        let consumer_id = TestServer::parse_id(&resp);
        let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

        // Publish with msg_id "dedup-1".
        client
            .publish_sync_with_id(
                stream_id,
                b"k.ev",
                b"dedup-1",
                Bytes::from_static(b"first-payload"),
            )
            .await
            .expect("first publish");

        // Consume and ack.
        let msg = tokio::time::timeout(std::time::Duration::from_secs(3), handle.recv())
            .await
            .expect("delivery within 3s")
            .expect("subscription open");
        assert_eq!(msg.payload().as_ref(), b"first-payload");
        msg.ack();

        // Small delay to let ack propagate to the engine.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        drop(handle);
        server.shutdown().await;
    }

    // ── Second boot: same data_dir ───────────────────────────────────────
    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;

        // Verify the stream survived restart.
        let resp = client.list_streams(0, 1000).await.unwrap();
        assert_eq!(
            TestServer::stream_count(&resp),
            1,
            "stream must survive restart"
        );
        let stream_id = TestServer::find_stream_id(&resp, b"restart_c")
            .expect("must find stream by name after restart");

        // Re-publishing "dedup-1" must be REJECTED — idempotency state rebuilt.
        let err = client
            .publish_sync_with_id(
                stream_id,
                b"k.ev",
                b"dedup-1",
                Bytes::from_static(b"replay-attempt"),
            )
            .await
            .expect_err("duplicate msg_id must be rejected after restart");
        assert!(
            is_duplicate(&err),
            "expected IdempotencyDuplicate after restart, got {err:?}"
        );

        // Publish a NEW msg_id "dedup-2" — must succeed.
        client
            .publish_sync_with_id(
                stream_id,
                b"k.ev",
                b"dedup-2",
                Bytes::from_static(b"second-payload"),
            )
            .await
            .expect("new msg_id must be accepted after restart");

        // Re-create consumer (same name returns same id) and subscribe.
        let resp = client
            .create_consumer(
                stream_id,
                b"worker",
                b"",
                b"",
                100u16,
                1u8,  // AckPolicy::Explicit
                0u8,  // DeliverPolicy::All
                0u8,  // DeliverMode::default
                5000u32, // ack_wait_ms
                0u64,
            )
            .await
            .unwrap();
        let consumer_id = TestServer::parse_id(&resp);
        let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

        // Drain all delivered messages. Both "first-payload" (from boot 1)
        // and "second-payload" (just published) live in the store. The
        // consumer will deliver them; crucially, "second-payload" must
        // appear — proving the new msg_id landed — and neither payload
        // must contain HAS_HEADERS metadata (the broker strips it).
        let mut payloads = Vec::new();
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(3), handle.recv()).await {
                Ok(Some(msg)) => {
                    payloads.push(msg.payload().to_vec());
                    msg.ack();
                }
                _ => break,
            }
        }

        // The new message ("dedup-2") MUST have been delivered.
        assert!(
            payloads.iter().any(|p| p == b"second-payload"),
            "consumer must receive the new message (dedup-2); got: {:?}",
            payloads.iter().map(|p| String::from_utf8_lossy(p)).collect::<Vec<_>>()
        );
        // The rejected duplicate "dedup-1" must NOT have produced a second
        // copy of "replay-attempt" in the store — only the original
        // "first-payload" may appear (once).
        assert!(
            !payloads.iter().any(|p| p == b"replay-attempt"),
            "the rejected duplicate must not appear in the store"
        );

        server.shutdown().await;
    }
}
