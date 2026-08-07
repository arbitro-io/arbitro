//! Reproduction for the "temporal isolation" gap flagged by the robustness
//! audit (Part B): the shard has ONE shared drain cursor and no per-consumer
//! delivery floor, so a late-joining consumer with DeliverPolicy::All rewinds
//! the shard cursor and re-delivers already-acked messages to EVERY sibling
//! consumer on the shard — not just to the late joiner.
//!
//! Two tests:
//!   1. `distinct_names_each_get_own_copy` — the WORKING invariant (both
//!      subscribed BEFORE publishing): distinct durable names = independent
//!      queues, each receives its own full copy. This must PASS.
//!   2. `late_join_all_does_not_redeliver_to_acked_sibling` — the GAP: A acks
//!      everything, then sibling B late-joins with DeliverPolicy::All; A must
//!      NOT receive any already-acked message again. If the gap is real, this
//!      assertion FAILS.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::Client;
use bytes::Bytes;
use std::time::Duration;

async fn create_stream(client: &Client, name: &[u8], filter: &[u8]) -> u32 {
    let resp = client
        .create_stream(name, filter, 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream");
    TestServer::parse_id(&resp)
}

#[allow(clippy::too_many_arguments)]
async fn create_consumer(
    client: &Client,
    stream_id: u32,
    name: &[u8],
    group: &[u8],
    max_inflight: u16,
    deliver_policy: u8,
) -> u32 {
    let resp = client
        .create_consumer(
            stream_id,
            name,
            group,
            b"",
            max_inflight,
            1u8,            // ack_policy = explicit
            deliver_policy, // 0=All, 1=New, 2=ByStartSeq
            0u8,            // push
            0u32,           // ack_wait_ms
            0u64,           // start_seq
        )
        .await
        .expect("create_consumer");
    TestServer::parse_id(&resp)
}

async fn drain_acking(
    handle: &mut arbitro_client_tokio::SubscriptionHandle,
    want: usize,
    budget: Duration,
) -> usize {
    let deadline = tokio::time::Instant::now() + budget;
    let mut got = 0usize;
    while got < want {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, handle.recv()).await {
            Ok(Some(msg)) => {
                msg.ack();
                got += 1;
            }
            _ => break,
        }
    }
    got
}

/// WORKING invariant: two distinct durable names, both subscribed before
/// publishing, each get their own full copy.
#[tokio::test(flavor = "multi_thread")]
async fn distinct_names_each_get_own_copy() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"events", b">").await;

    let cid_a = create_consumer(&client, stream_id, b"svc_a", b"group_a", 100, 1).await;
    let cid_b = create_consumer(&client, stream_id, b"svc_b", b"group_b", 100, 1).await;
    let mut ha = client.subscribe(stream_id, cid_a, b"").await.unwrap();
    let mut hb = client.subscribe(stream_id, cid_b, b"").await.unwrap();

    for i in 0..5u32 {
        client
            .publish_wait(
                stream_id,
                b"events_tick",
                Bytes::copy_from_slice(&i.to_le_bytes()),
            )
            .await
            .expect("publish");
    }

    let ga = drain_acking(&mut ha, 5, Duration::from_secs(3)).await;
    let gb = drain_acking(&mut hb, 5, Duration::from_secs(3)).await;
    assert_eq!(ga, 5, "A must receive its own full copy");
    assert_eq!(gb, 5, "B must receive its own full copy");
    server.shutdown().await;
}

/// THE GAP (now closed): after A acks everything, sibling B late-joins with
/// DeliverPolicy::All. A must NOT be re-delivered its already-acked messages.
///
/// Fixed by the per-consumer contiguous-acked floor
/// (`arbitro-server/src/shard/ack_floor.rs` + the floor check in
/// `dispatch_recipients`): B's rewind still replays the store, but the drain
/// skips every seq at or below A's floor. See ROBUSTNESS_AUDIT.md Part B
/// (D5 / A7 temporal isolation).
#[tokio::test(flavor = "multi_thread")]
async fn late_join_all_does_not_redeliver_to_acked_sibling() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"events", b">").await;

    // A: DeliverPolicy::New, subscribe, consume + ack all 5.
    let cid_a = create_consumer(&client, stream_id, b"svc_a", b"group_a", 100, 1).await;
    let mut ha = client.subscribe(stream_id, cid_a, b"").await.unwrap();

    for i in 0..5u32 {
        client
            .publish_wait(
                stream_id,
                b"events_tick",
                Bytes::copy_from_slice(&i.to_le_bytes()),
            )
            .await
            .expect("publish");
    }
    let ga = drain_acking(&mut ha, 5, Duration::from_secs(3)).await;
    assert_eq!(ga, 5, "A must receive its 5 before B joins");

    // Let A's acks land.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // B: DeliverPolicy::All — late join. Legitimately replays the 5 for itself.
    let cid_b = create_consumer(&client, stream_id, b"svc_b", b"group_b", 100, 0).await;
    let mut hb = client.subscribe(stream_id, cid_b, b"").await.unwrap();
    let gb = drain_acking(&mut hb, 5, Duration::from_secs(3)).await;
    assert_eq!(gb, 5, "B (DeliverPolicy::All) must replay all 5 for itself");

    // THE INVARIANT UNDER TEST: A already acked everything and nothing new was
    // published, so A must receive ZERO further messages.
    let mut redelivered = 0usize;
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(600), ha.recv()).await {
        msg.ack();
        redelivered += 1;
    }
    assert_eq!(
        redelivered, 0,
        "TEMPORAL-ISOLATION GAP: consumer A was re-delivered {redelivered} already-acked \
         message(s) after sibling B late-joined with DeliverPolicy::All (shard cursor rewound)"
    );
    server.shutdown().await;
}

/// Sibling variant: a `DeliverPolicy::ByStartSeq` late join replays for the
/// joiner only — the already-acked sibling must stay silent.
#[tokio::test(flavor = "multi_thread")]
async fn late_join_by_start_seq_does_not_redeliver_to_acked_sibling() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"events", b">").await;

    // A: DeliverPolicy::New, consume + ack all 5.
    let cid_a = create_consumer(&client, stream_id, b"svc_a", b"group_a", 100, 1).await;
    let mut ha = client.subscribe(stream_id, cid_a, b"").await.unwrap();
    for i in 0..5u32 {
        client
            .publish_wait(
                stream_id,
                b"events_tick",
                Bytes::copy_from_slice(&i.to_le_bytes()),
            )
            .await
            .expect("publish");
    }
    assert_eq!(drain_acking(&mut ha, 5, Duration::from_secs(3)).await, 5);
    tokio::time::sleep(Duration::from_millis(400)).await;

    // B: DeliverPolicy::ByStartSeq(start_seq = 2) — replays 2..=5 for itself.
    let resp = client
        .create_consumer(
            stream_id, b"svc_b", b"group_b", b"", 100, 1u8, 2u8, 0u8, 0u32, 2u64,
        )
        .await
        .expect("create_consumer");
    let cid_b = TestServer::parse_id(&resp);
    let mut hb = client.subscribe(stream_id, cid_b, b"").await.unwrap();
    let gb = drain_acking(&mut hb, 4, Duration::from_secs(3)).await;
    assert_eq!(gb, 4, "B (ByStartSeq=2) must replay seqs 2..=5 for itself");

    // A already acked everything — it must receive ZERO further messages.
    let mut redelivered = 0usize;
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(600), ha.recv()).await {
        msg.ack();
        redelivered += 1;
    }
    assert_eq!(
        redelivered, 0,
        "consumer A was re-delivered {redelivered} already-acked message(s) after sibling B \
         late-joined with DeliverPolicy::ByStartSeq"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// DeliverPolicy::New over an UNDRAINED backlog.
//
// The existing New coverage (catalog_invariants.rs
// `rejected_reconsumer_does_not_corrupt_registry`) has a sibling drain and ack
// the history first, so the shard cursor is already past the backlog and
// "leave the cursor where it is" happens to be correct. Nothing exercises New
// when the backlog is still sitting there unread — which is the ordinary way
// a monitoring or tail-follow consumer joins.
//
// Found from the other side: the C and Go suites both assert this and both see
// the full history. Adding it here puts the case in the reference client's own
// harness rather than leaving it as "the C client disagrees".
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn deliver_new_skips_an_undrained_backlog() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"new_undrained", b">").await;

    // Publish history with NO consumer attached — nothing advances the cursor.
    for i in 0..10u8 {
        client
            .publish_wait(stream_id, b"new_undrained.h", Bytes::from(vec![b'h', i]))
            .await
            .expect("history publish");
    }

    let late_id = create_consumer(&client, stream_id, b"late", b"late", 100u16, 1u8).await;
    let mut late = client.subscribe(stream_id, late_id, b"").await.unwrap();

    // Drain for a bounded window rather than asserting immediately: the claim
    // is that nothing arrives, and checking too early would pass by not having
    // waited.
    let mut got: Vec<Vec<u8>> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(250), late.recv()).await {
            Ok(Some(msg)) => {
                got.push(msg.payload().to_vec());
                msg.ack();
            }
            Ok(None) => break,
            Err(_) => {}
        }
    }

    assert!(
        got.is_empty(),
        "DeliverPolicy::New replayed {} message(s) of an undrained backlog; \
         a New consumer must start at the tail regardless of whether anyone \
         consumed the history: {:?}",
        got.len(),
        got.iter().map(|p| String::from_utf8_lossy(p).to_string()).collect::<Vec<_>>()
    );
    server.shutdown().await;
}
