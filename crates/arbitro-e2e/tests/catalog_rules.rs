//! Catalog containment rules — stream ⊃ consumer ⊃ subscription.
//!
//! Every subject a consumer can see must lie inside its stream's filter,
//! and every subject a subscription can see must lie inside its consumer's.
//! Siblings never overlap: two streams cannot claim the same slice, and a
//! consumer cannot nest under another consumer on the same stream.
//!
//! Only B1 is enforced today (93c08b0). The rest are red on purpose: each
//! one names a rule the broker accepts silently, which is the failure mode
//! worth pinning — a consumer that exists, reports success, and quietly
//! receives the wrong traffic.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::Client;
use bytes::Bytes;
use std::time::Duration;

// ── Helpers ──────────────────────────────────────────────────────────────

async fn make_stream(client: &Client, name: &[u8], filter: &[u8]) -> u32 {
    let resp = client
        .create_stream(name, filter, 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream must succeed");
    TestServer::parse_id(&resp)
}

async fn make_consumer(client: &Client, stream_id: u32, name: &[u8], filter: &[u8]) -> u32 {
    let resp = client
        .create_consumer(
            stream_id, name, name, filter, 100u16, 1u8, 0u8, 0u8, 30_000u32, 0u64,
        )
        .await
        .expect("create_consumer must succeed");
    TestServer::parse_id(&resp)
}

/// Every subject a subscription hands over before it goes quiet.
async fn drain_subjects(sub: &mut arbitro_client_tokio::SubscriptionHandle) -> Vec<String> {
    let mut out = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(400), sub.recv()).await {
        out.push(String::from_utf8_lossy(msg.subject()).into_owned());
        msg.ack();
    }
    out
}

// ═════════════════════════════════════════════════════════════════════════
// STREAM
// ═════════════════════════════════════════════════════════════════════════

/// S1 — a name identifies one stream, so a second create under the same
/// name with a DIFFERENT filter must not be waved through.
///
/// `handle_create_stream` returns `ok` for both new and existing, so the
/// `StreamAlreadyExists` arm in `v2_create_stream` is unreachable: the
/// second create replies RepOk and the new filter is dropped in silence.
#[tokio::test(flavor = "multi_thread")]
async fn recreating_a_stream_with_a_different_filter_is_rejected() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    make_stream(&client, b"orders", b"orders.>").await;
    let second = client
        .create_stream(b"orders", b"payments.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await;

    server.shutdown().await;
    assert!(
        second.is_err(),
        "a second `orders` declaring `payments.>` was accepted — the filter \
         it asked for is not the filter it got, and nothing said so"
    );
}

/// S2 — a stream owns a slice of the subject space. An empty filter claims
/// nothing, so the stream can never be reasoned about.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_must_declare_a_subject_filter() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let created = client
        .create_stream(b"nofilter", b"", 0, 0, 0, 1, 0, 0, 0, 0)
        .await;

    server.shutdown().await;
    assert!(
        created.is_err(),
        "a stream with an empty subject filter was created"
    );
}

/// S2 — `>` claims the whole space, which overlaps every other stream by
/// construction and makes S3 unsatisfiable for any second stream.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_filter_may_not_be_global() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let created = client
        .create_stream(b"global", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await;

    server.shutdown().await;
    assert!(
        created.is_err(),
        "a stream claiming `>` was created — it owns every subject, so no \
         second stream can ever satisfy the no-overlap rule"
    );
}

/// S3 — two streams claiming the same slice makes "which stream owns this
/// subject" undecidable. `subjects_overlap()` already answers this and has
/// 24 assertions behind it; nothing calls it.
#[tokio::test(flavor = "multi_thread")]
async fn two_streams_may_not_claim_the_same_filter() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    make_stream(&client, b"orders_a", b"orders.>").await;
    let second = client
        .create_stream(b"orders_b", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await;

    server.shutdown().await;
    assert!(
        second.is_err(),
        "two streams both claiming `orders.>` were created"
    );
}

/// S3 — overlap, not just equality. `orders.premium.>` sits inside
/// `orders.>`, so both claim `orders.premium.1`.
#[tokio::test(flavor = "multi_thread")]
async fn two_streams_may_not_claim_overlapping_filters() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    make_stream(&client, b"wide", b"orders.>").await;
    let nested = client
        .create_stream(b"nested", b"orders.premium.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await;

    server.shutdown().await;
    assert!(
        nested.is_err(),
        "`orders.premium.>` was accepted alongside `orders.>` — both own \
         `orders.premium.1` and neither is authoritative"
    );
}

// ═════════════════════════════════════════════════════════════════════════
// CONSUMER
//
// C1 (no two consumers with the same name) is already covered by
// `catalog_invariants::two_consumers_with_the_same_name`.
// ═════════════════════════════════════════════════════════════════════════

/// C2 — consumers on a stream are siblings, not a hierarchy. A consumer
/// whose filter sits inside another's makes delivery ambiguous: the same
/// subject belongs to two independently-acking readers.
#[tokio::test(flavor = "multi_thread")]
async fn a_consumer_may_not_nest_under_another() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = make_stream(&client, b"nesting", b"orders.>").await;

    make_consumer(&client, stream_id, b"wide", b"orders.>").await;
    let nested = client
        .create_consumer(
            stream_id,
            b"nested",
            b"nested",
            b"orders.premium.>",
            100u16,
            1u8,
            0u8,
            0u8,
            30_000u32,
            0u64,
        )
        .await;

    server.shutdown().await;
    assert!(
        nested.is_err(),
        "`orders.premium.>` was accepted under a sibling on `orders.>`"
    );
}

/// C3 — a consumer cannot reach outside the stream that holds it. A filter
/// that leaves the stream's slice can only ever match nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_consumer_filter_must_stay_inside_its_stream() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = make_stream(&client, b"scoped", b"orders.>").await;

    let outside = client
        .create_consumer(
            stream_id,
            b"stray",
            b"stray",
            b"telemetry.cpu.>",
            100u16,
            1u8,
            0u8,
            0u8,
            30_000u32,
            0u64,
        )
        .await;

    server.shutdown().await;
    assert!(
        outside.is_err(),
        "a consumer on `telemetry.cpu.>` was created inside a stream that \
         only owns `orders.>` — it can never match anything"
    );
}

/// C4 — a consumer that declares no filter takes its stream's, the same
/// way a subscription takes its consumer's (93c08b0).
///
/// Publishing is addressed by stream_id, not by subject match, so a
/// producer can put an out-of-slice subject into the stream. Inheritance is
/// what keeps that from reaching a consumer that never asked for it.
#[tokio::test(flavor = "multi_thread")]
async fn a_consumer_without_a_filter_inherits_the_stream() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = make_stream(&client, b"inherit", b"orders.>").await;
    let consumer = make_consumer(&client, stream_id, b"reader", b"").await;

    let cc = server.connect().await;
    let mut sub = cc
        .subscribe(stream_id, consumer, b"")
        .await
        .expect("subscribe");

    for subj in [&b"orders.new.1"[..], &b"telemetry.cpu.1"[..]] {
        client
            .publish_wait(stream_id, subj, Bytes::from("x"))
            .await
            .expect("publish");
    }

    let got = drain_subjects(&mut sub).await;
    server.shutdown().await;

    assert_eq!(
        got,
        vec!["orders.new.1".to_string()],
        "a filterless consumer on a stream scoped to `orders.>` should have \
         inherited that scope; it received {got:?}"
    );
}

// ═════════════════════════════════════════════════════════════════════════
// SUBSCRIPTION
//
// B1 (subscription under its consumer) is enforced and covered by
// `catalog_invariants::nested_consumer_filters_are_independent`.
// ═════════════════════════════════════════════════════════════════════════

/// B2 — a subscription cannot reach outside its stream either. With C3 in
/// place this is implied by transitivity, but it is pinned separately so a
/// regression in C3 shows up as two failures, not one.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_filter_must_stay_inside_its_stream() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = make_stream(&client, b"substream", b"orders.>").await;
    let consumer = make_consumer(&client, stream_id, b"reader", b"").await;

    let cc = server.connect().await;
    let outside = cc.subscribe(stream_id, consumer, b"telemetry.cpu.>").await;

    server.shutdown().await;
    assert!(
        outside.is_err(),
        "a subscription on `telemetry.cpu.>` was accepted on a stream that \
         only owns `orders.>`"
    );
}

/// B3 — subscribe checks BOTH ancestors, not just the nearest one. Here the
/// filter is legal against the consumer and illegal against the stream, so
/// a check that only consults the consumer lets it through.
#[tokio::test(flavor = "multi_thread")]
async fn subscribe_validates_against_stream_and_consumer_together() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = make_stream(&client, b"bothcheck", b"orders.>").await;

    // Consumer declares nothing, so the consumer-side check cannot reject
    // anything on its own — only the stream can.
    let consumer = make_consumer(&client, stream_id, b"reader", b"").await;

    let cc = server.connect().await;
    let outside = cc.subscribe(stream_id, consumer, b"payments.>").await;

    server.shutdown().await;
    assert!(
        outside.is_err(),
        "`payments.>` passed because the consumer declared no filter — the \
         stream's `orders.>` was never consulted"
    );
}

// ── The verdict must reach the client intact ─────────────────────────────
//
// Every rule above pins only THAT a creation was refused. The code carrying
// the refusal went unchecked, and it was wrong: a stream refused for
// claiming `>` came back as StreamAlreadyExists, sending whoever hit it
// hunting a duplicate that did not exist; a subscription refused for
// reaching outside its consumer came back as InvalidLength.
//
// These pin the code itself, so the mapping cannot collapse again.

async fn rejection_code(
    result: Result<Bytes, arbitro_client_tokio::ClientError>,
) -> u16 {
    use arbitro_client_tokio::ClientError;
    match result {
        Err(ClientError::Broker { code }) => code.as_u16(),
        other => panic!("expected a broker rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn a_global_stream_filter_is_named_as_a_filter_problem() {
    use arbitro_proto::error::ErrorCode;
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let code = rejection_code(
        client
            .create_stream(b"global_code", b">", 0, 0, 0, 1, 0, 0, 0, 0)
            .await,
    )
    .await;

    server.shutdown().await;
    assert_eq!(
        code,
        ErrorCode::InvalidStreamFilter.as_u16(),
        "a stream refused for claiming `>` must say so — StreamAlreadyExists \
         sends the caller hunting a duplicate that does not exist"
    );
}

#[tokio::test]
async fn an_overlapping_stream_filter_is_named_as_a_conflict() {
    use arbitro_proto::error::ErrorCode;
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    make_stream(&client, b"orders_a", b"orders.>").await;
    let code = rejection_code(
        client
            .create_stream(b"orders_b", b"orders.premium.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await,
    )
    .await;

    server.shutdown().await;
    assert_eq!(
        code,
        ErrorCode::StreamFilterConflict.as_u16(),
        "the filter is well-formed; what fails is that a peer already owns \
         subjects inside it — a different problem from an unusable filter"
    );
}

#[tokio::test]
async fn a_consumer_reaching_outside_its_stream_is_named_as_a_filter_problem() {
    use arbitro_proto::error::ErrorCode;
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let sid = make_stream(&client, b"orders_c", b"orders.>").await;
    let code = rejection_code(
        client
            .create_consumer(
                sid, b"wide", b"wide", b"payments.>", 100u16, 1u8, 0u8, 0u8, 30_000u32, 0u64,
            )
            .await,
    )
    .await;

    server.shutdown().await;
    assert_eq!(
        code,
        ErrorCode::InvalidConsumerFilter.as_u16(),
        "the filter is what was refused, so the code must point at the \
         filter and not at the rest of the config"
    );
}
