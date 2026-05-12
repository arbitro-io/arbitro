//! End-to-end idempotency invariants.
//!
//! Pins the per-stream dedup behaviour from the user's API surface:
//!  - Streams created with `idempotency_window_ms = 0` accept every
//!    publish; identical `msg_id` causes no rejection.
//!  - Streams created with `idempotency_window_ms > 0` reject the
//!    second publish of the same `msg_id` with
//!    `ErrorCode::IdempotencyDuplicate`.
//!  - Two streams are independent: enabling dedup on one does not
//!    affect the other.
//!  - Batch publishes honour the same contract atomically — a batch
//!    that contains a duplicate is rejected wholesale.
//!  - Entries with empty `msg_id` are NEVER deduped, even on a stream
//!    that has the feature enabled (per-message opt-in).
//!
//! These tests use the real TCP client + server, so they also catch
//! wire-shape regressions in `PubFrame` / `BatchPubFrame`.

use std::time::Duration;

use arbitro_client_tokio::{BatchEntry, Client, ClientConfig, ClientError};
use arbitro_proto::error::ErrorCode;
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;
use tokio::sync::watch;

// ── Helpers ────────────────────────────────────────────────────────────────

fn parse_id(resp: &Bytes) -> u32 {
    u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32
}

async fn start_server() -> (watch::Sender<bool>, String) {
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
    (tx, addr)
}

async fn connect(addr: &str) -> Client {
    Client::connect(ClientConfig { addr: addr.to_string(), ..ClientConfig::default() })
        .await
        .expect("client should connect")
}

fn is_duplicate(err: &ClientError) -> bool {
    matches!(err, ClientError::Broker { code: ErrorCode::IdempotencyDuplicate })
}

// ═══════════════════════════════════════════════════════════════════════════
// Single publish
// ═══════════════════════════════════════════════════════════════════════════

/// Stream created with `idempotency_window_ms = 0` (the default).
/// Two publishes with the same msg_id must both succeed — the feature
/// is fully opt-in.
#[tokio::test(flavor = "multi_thread")]
async fn stream_without_window_allows_duplicates() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let resp = client
        .create_stream(b"plain", b">", 0, 0, 0, 1, 0, 0, 0, /*idempotency_window_ms*/ 0)
        .await
        .unwrap();
    let stream_id = parse_id(&resp);

    client
        .publish_sync_with_id(stream_id, b"k.a", b"msg-1", Bytes::from_static(b"v1"))
        .await
        .expect("first publish should succeed");
    client
        .publish_sync_with_id(stream_id, b"k.a", b"msg-1", Bytes::from_static(b"v1"))
        .await
        .expect("second publish must also succeed when window=0");
}

/// Stream created with `idempotency_window_ms > 0`. Second publish
/// of the same msg_id within the window is rejected with
/// `ErrorCode::IdempotencyDuplicate`.
#[tokio::test(flavor = "multi_thread")]
async fn stream_with_window_rejects_duplicate_msg_id() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let resp = client
        .create_stream(b"dedup", b">", 0, 0, 0, 1, 0, 0, 0, /*window_ms*/ 60_000)
        .await
        .unwrap();
    let stream_id = parse_id(&resp);

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
}

/// Even on an idempotent stream, a publish with EMPTY msg_id is never
/// deduped — opt-in is per-message. Repeating an empty-id publish
/// must succeed every time.
#[tokio::test(flavor = "multi_thread")]
async fn empty_msg_id_is_never_deduped() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let resp = client
        .create_stream(b"dedup2", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap();
    let stream_id = parse_id(&resp);

    // Three identical publishes with NO msg_id — all three must land.
    for _ in 0..3 {
        client
            .publish_sync(stream_id, b"k.x", Bytes::from_static(b"v"))
            .await
            .expect("empty-id publish must always succeed");
    }
}

/// Distinct msg_ids on the same stream don't collide.
#[tokio::test(flavor = "multi_thread")]
async fn different_msg_ids_are_independent() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let resp = client
        .create_stream(b"dedup3", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap();
    let stream_id = parse_id(&resp);

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
}

/// Two streams, one with dedup, one without. The dedup state on the
/// idempotent stream must not bleed into the plain stream.
#[tokio::test(flavor = "multi_thread")]
async fn two_streams_isolated_one_with_one_without() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let dedup_id = parse_id(&client
        .create_stream(b"with_dedup", b"a.>", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap());
    let plain_id = parse_id(&client
        .create_stream(b"no_dedup", b"b.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap());

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
    assert!(is_duplicate(&err), "expected IdempotencyDuplicate, got {err:?}");
}

/// After delete + recreate, the dedup state for a stream is reset.
/// (The new stream gets a fresh stream_id, and the tracker keys on
/// `(StreamId, hash)` so old entries cannot collide.)
#[tokio::test(flavor = "multi_thread")]
async fn delete_and_recreate_clears_dedup_state() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let resp = client
        .create_stream(b"recycle", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap();
    let first_id = parse_id(&resp);

    client
        .publish_sync_with_id(first_id, b"k", b"reused", Bytes::from_static(b"v1"))
        .await
        .expect("first publish");

    client.delete_stream(b"recycle").await.expect("delete");

    let resp = client
        .create_stream(b"recycle", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap();
    let second_id = parse_id(&resp);

    // The reused msg_id must be accepted on the fresh stream.
    client
        .publish_sync_with_id(second_id, b"k", b"reused", Bytes::from_static(b"v2"))
        .await
        .expect("recreated stream starts with empty dedup state");
}

// ═══════════════════════════════════════════════════════════════════════════
// Batch publish — atomic all-or-nothing dedup
// ═══════════════════════════════════════════════════════════════════════════

/// A batch where every entry has a unique msg_id lands in full.
#[tokio::test(flavor = "multi_thread")]
async fn batch_with_all_unique_ids_succeeds() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id = parse_id(&client
        .create_stream(b"batch_uniq", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap());

    let entries = [
        BatchEntry::with_msg_id(b"k", b"b-1", Bytes::from_static(b"v1")),
        BatchEntry::with_msg_id(b"k", b"b-2", Bytes::from_static(b"v2")),
        BatchEntry::with_msg_id(b"k", b"b-3", Bytes::from_static(b"v3")),
    ];
    client
        .publish_batch_sync(stream_id, &entries)
        .await
        .expect("unique-id batch must succeed");
}

/// A batch whose entries collide with previously-recorded msg_ids on
/// the same stream is rejected wholesale.
#[tokio::test(flavor = "multi_thread")]
async fn batch_with_duplicate_from_prior_publish_is_rejected() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id = parse_id(&client
        .create_stream(b"batch_dup", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap());

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
    assert!(is_duplicate(&err), "expected IdempotencyDuplicate, got {err:?}");

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
}

/// A batch containing two entries with the SAME msg_id is rejected
/// (the second entry collides with the first within the same batch).
#[tokio::test(flavor = "multi_thread")]
async fn batch_with_internal_duplicate_is_rejected() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id = parse_id(&client
        .create_stream(b"batch_internal", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap());

    let entries = [
        BatchEntry::with_msg_id(b"k", b"twin", Bytes::from_static(b"v1")),
        BatchEntry::with_msg_id(b"k", b"twin", Bytes::from_static(b"v2")),
    ];
    let err = client
        .publish_batch_sync(stream_id, &entries)
        .await
        .expect_err("batch with internal twin id must be rejected");
    assert!(is_duplicate(&err), "expected IdempotencyDuplicate, got {err:?}");
}

/// On a stream with dedup enabled, mixing entries WITH and WITHOUT
/// msg_id is allowed. The empty-id entries are never deduped; the
/// id-bearing ones are. A duplicate in only the id-bearing entries
/// is rejected; a "duplicate" empty-id is fine because there's no
/// id to track.
#[tokio::test(flavor = "multi_thread")]
async fn batch_mixed_id_and_no_id_entries() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id = parse_id(&client
        .create_stream(b"batch_mixed", b">", 0, 0, 0, 1, 0, 0, 0, 60_000)
        .await
        .unwrap());

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
    assert!(is_duplicate(&err), "expected IdempotencyDuplicate, got {err:?}");
}
