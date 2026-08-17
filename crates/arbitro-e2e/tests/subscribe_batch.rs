//! `SubscribeBatch` — N subscriptions in ONE round-trip.
//!
//! The sibling fan-out itself is pinned by `one_consumer_many_filters`; what
//! is new here is that opening the siblings in a single frame reaches the
//! same place as opening them one at a time.
//!
//! The failure shape matters as much as the happy path. A filter outside the
//! consumer's slice must come back naming ITS index — that is the whole
//! point of a per-entry verdict — while its legal peers stay open.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::Client;
use bytes::Bytes;
use std::time::Duration;

async fn make_stream(client: &Client, name: &[u8], filter: &[u8]) -> u32 {
    let resp = client
        .create_stream(name, filter, 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream must succeed");
    TestServer::parse_id(&resp)
}

/// Fanout consumer — `deliver_mode = 0`. Every narrowing is
/// subscription-side, and each sibling is owed its own copy; queue mode
/// (`1`) would split the fixture between them instead.
async fn make_consumer(client: &Client, stream_id: u32, name: &[u8], filter: &[u8]) -> u32 {
    let resp = client
        .create_consumer(
            stream_id, name, name, filter, 100u16, 1u8, 0u8, 0u8, 30_000u32, 0u64,
        )
        .await
        .expect("create_consumer must succeed");
    TestServer::parse_id(&resp)
}

async fn drain_within(
    sub: &mut arbitro_client_tokio::SubscriptionHandle,
    quiet_ms: u64,
) -> Vec<String> {
    let mut out = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(quiet_ms), sub.recv()).await
    {
        out.push(String::from_utf8_lossy(msg.subject()).into_owned());
        msg.ack();
    }
    out
}

async fn drain_subjects(sub: &mut arbitro_client_tokio::SubscriptionHandle) -> Vec<String> {
    drain_within(sub, 400).await
}

/// Three filtered siblings opened in one frame split exactly as three
/// separate subscribes would — distinct ids, and each seeing only its own.
#[tokio::test(flavor = "multi_thread")]
async fn one_frame_opens_every_filtered_sibling() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = make_stream(&client, b"orders", b"orders.>").await;
    let consumer_id = make_consumer(&client, stream_id, b"triage", b"orders.>").await;

    let outcome = client
        .subscribe_batch(
            stream_id,
            consumer_id,
            vec![
                b"orders.new.*".to_vec(),
                b"orders.paid.*".to_vec(),
                // Empty — inherits the consumer's filter, as a single
                // subscribe does.
                Vec::new(),
            ],
        )
        .await
        .expect("a batch of legal filters must be accepted");

    assert!(
        outcome.rejected.is_empty(),
        "legal filters were refused: {:?}",
        outcome.rejected
    );
    assert_eq!(outcome.accepted.len(), 3, "one handle per entry, in order");

    for subject in [
        &b"orders.new.1"[..],
        b"orders.new.2",
        b"orders.paid.1",
        b"orders.audit.trail",
    ] {
        client
            .publish_wait(stream_id, subject, Bytes::from_static(b"x"))
            .await
            .expect("publish");
    }

    let mut subs = outcome.accepted;
    let all = drain_subjects(&mut subs[2]).await;
    let paid = drain_subjects(&mut subs[1]).await;
    let new = drain_subjects(&mut subs[0]).await;
    server.shutdown().await;

    assert_eq!(new.len(), 2, "orders.new.* saw {new:?}");
    assert_eq!(paid.len(), 1, "orders.paid.* saw {paid:?}");
    assert_eq!(all.len(), 4, "the inherited catch-all saw {all:?}");
    assert!(
        new.iter().all(|s| s.starts_with("orders.new.")),
        "a subject outside the filter arrived: {new:?}"
    );
}

/// A filter that escapes the consumer's slice is refused ALONE, by index,
/// and its peers stay open. Rolling the whole batch back would cost more
/// than it buys, and would hide which entry was actually wrong.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_entry_names_its_index_and_spares_its_peers() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = make_stream(&client, b"orders", b"orders.>").await;
    // Deliberately narrow: an `orders.paid.*` sibling escapes `orders.new.>`.
    let consumer_id = make_consumer(&client, stream_id, b"narrow", b"orders.new.>").await;

    let outcome = client
        .subscribe_batch(
            stream_id,
            consumer_id,
            vec![
                b"orders.new.a".to_vec(),
                b"orders.paid.*".to_vec(),
                b"orders.new.b".to_vec(),
            ],
        )
        .await
        .expect("a per-entry refusal must not fail the whole call");

    server.shutdown().await;

    assert_eq!(
        outcome.rejected.len(),
        1,
        "expected exactly one refusal, got {:?}",
        outcome.rejected
    );
    assert_eq!(
        outcome.rejected[0].index, 1,
        "the refusal named the wrong entry"
    );
    assert_eq!(
        outcome.rejected[0].code,
        arbitro_proto::error::ErrorCode::InvalidSubscriptionFilter.as_u16(),
        "a filter outside the consumer must be refused as such"
    );
    assert_eq!(
        outcome.accepted.len(),
        2,
        "a single bad entry took the whole batch down"
    );
}

/// A hundred at once — the scenario the batch exists for. Three entries can
/// collide by luck; a hundred cannot.
#[tokio::test(flavor = "multi_thread")]
async fn a_hundred_subscriptions_in_one_frame_keep_their_own_ids() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = make_stream(&client, b"tenants", b"tenants.>").await;
    let consumer_id = make_consumer(&client, stream_id, b"fanout", b"tenants.>").await;

    let filters: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("tenants.t{i}.>").into_bytes())
        .collect();
    let outcome = client
        .subscribe_batch(stream_id, consumer_id, filters)
        .await
        .expect("a hundred legal filters must be accepted");

    assert!(
        outcome.rejected.is_empty(),
        "legal filters were refused: {:?}",
        outcome.rejected
    );
    assert_eq!(outcome.accepted.len(), 100);

    for i in 0..100 {
        client
            .publish_wait(
                stream_id,
                format!("tenants.t{i}.evt").as_bytes(),
                Bytes::from_static(b"x"),
            )
            .await
            .expect("publish");
    }

    let mut subs = outcome.accepted;
    let mut wrong = Vec::new();
    for (i, sub) in subs.iter_mut().enumerate() {
        // Short quiet window on purpose: each message is already buffered in
        // its channel by now, and draining 100 subscriptions at 400 ms apiece
        // would outlast the consumer's 30 s ack_wait — the tail would be
        // redelivered before this loop reached it, and read as a duplicate.
        let got = drain_within(sub, 120).await;
        if got.len() != 1 || !got[0].starts_with(&format!("tenants.t{i}.")) {
            wrong.push((i, got));
        }
    }
    server.shutdown().await;

    assert!(
        wrong.is_empty(),
        "subscriptions that did not see exactly their own message: {:?}",
        &wrong[..wrong.len().min(10)]
    );
}
