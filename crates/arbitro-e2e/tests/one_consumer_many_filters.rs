//! One consumer, several filtered subscriptions — same connection and across
//! connections. Plus: may two consumers share a filter?
//!
//! The binding table is keyed by subscription id, and `encode_sub_v2` sends a
//! literal `0` (`consume/mod.rs:86`), so every subscription to the same
//! consumer collapses onto the same key. `service.rs:485` records what that
//! cost once: each new instance's `subscribe` retired the previous instance's
//! binding, and only the last one received anything. The fix there was
//! per-instance consumer NAMES — it never made one consumer hold several
//! bindings.
//!
//! `subject_limit_shared_consumer` looks like a counter-example: N connections,
//! one consumer, and it passes. But all N pass `b""`, so "only the last one
//! receives" and "they share the load" are indistinguishable from its
//! assertions. Give each subscription a DIFFERENT filter and the two stop
//! looking alike.
//!
//! None of these are written to pass. They are written to say what the broker
//! actually does.

mod test_helper;
use test_helper::TestServerBuilder;

use arbitro_client_tokio::{
    AckPolicy, Client, ConsumerBuilder, DeliverMode, StreamBuilder, SubscriptionHandle,
};
use bytes::Bytes;
use std::time::Duration;

const ORDERS: usize = 3;
const PAYMENTS: usize = 2;
const AUDIT: usize = 1;
const TOTAL: usize = ORDERS + PAYMENTS + AUDIT;

#[tokio::test]
async fn filtered_subs_on_one_consumer_across_connections() {
    let mut server = TestServerBuilder::new().spawn().await;
    let admin = server.connect().await;

    let (stream_id, consumer_id) = setup(&admin, b"triage_a", b"worker_a").await;

    // Three CONNECTIONS, one consumer, three filters.
    let c1 = server.connect().await;
    let mut orders = c1
        .subscribe(stream_id, consumer_id, b"orders.*")
        .await
        .expect("subscribe orders");
    let c2 = server.connect().await;
    let mut pay = c2
        .subscribe(stream_id, consumer_id, b"payments.*")
        .await
        .expect("subscribe payments");
    let c3 = server.connect().await;
    let mut all = c3
        .subscribe(stream_id, consumer_id, b"*.>")
        .await
        .expect("subscribe all");

    publish_fixture(&admin, stream_id).await;

    let got_orders = drain(&mut orders).await;
    let got_pay = drain(&mut pay).await;
    let got_all = drain(&mut all).await;

    report("across connections", &got_orders, &got_pay, &got_all);

    // Clean up BEFORE asserting. A failing assert panics, and a panic skips
    // everything after it — so cleanup placed below the assert only runs when
    // it is not needed.
    admin
        .delete_stream(b"triage_a")
        .await
        .expect("delete stream");
    server.shutdown().await;

    assert_outcome("across connections", &got_orders, &got_pay, &got_all);
}

#[tokio::test]
async fn filtered_subs_on_one_consumer_same_connection() {
    let mut server = TestServerBuilder::new().spawn().await;
    let admin = server.connect().await;

    let (stream_id, consumer_id) = setup(&admin, b"triage_b", b"worker_b").await;

    // ONE connection, one consumer, three filters. If the takeover is keyed by
    // subscription id alone, this behaves exactly like the case above — the
    // connection is not part of the key.
    let c = server.connect().await;
    let mut orders = c
        .subscribe(stream_id, consumer_id, b"orders.*")
        .await
        .expect("subscribe orders");
    let mut pay = c
        .subscribe(stream_id, consumer_id, b"payments.*")
        .await
        .expect("subscribe payments");
    let mut all = c
        .subscribe(stream_id, consumer_id, b"*.>")
        .await
        .expect("subscribe all");

    publish_fixture(&admin, stream_id).await;

    let got_orders = drain(&mut orders).await;
    let got_pay = drain(&mut pay).await;
    let got_all = drain(&mut all).await;

    report("same connection", &got_orders, &got_pay, &got_all);

    admin
        .delete_stream(b"triage_b")
        .await
        .expect("delete stream");
    server.shutdown().await;

    assert_outcome("same connection", &got_orders, &got_pay, &got_all);
}

/// The same arrangement, four times over: FOUR connections, each holding the
/// same three filtered subscriptions on the SAME consumer.
///
/// `filtered_subs_on_one_consumer_same_connection` proves one connection's
/// worth. This proves the two halves compose — that collapsing per
/// `(connection, consumer)` really is per connection, and that four clients
/// reading one consumer neither steal from nor duplicate each other.
///
/// Under fanout every connection is owed the full fixture, split across its
/// own three subscriptions. So the matrix is four identical rows, and the
/// wire carries one copy per message per connection — 4 × 6 = 24 entries for
/// 6 published messages, versus 4 × 11 = 44 before the collapse.
///
/// Two ways this can fail, and they mean opposite things:
///   * a row that is `[0, 0, 6]` — that connection's client merged its three
///     handles into one. Client-side, same gap the single-connection test pins.
///   * rows that disagree with each other — the broker is treating connections
///     differently, which would be a real distribution bug.
#[tokio::test]
async fn filtered_subs_on_four_connections() {
    let mut server = TestServerBuilder::new().spawn().await;
    let admin = server.connect().await;

    let (stream_id, consumer_id) = setup(&admin, b"triage_quad", b"worker_quad").await;

    const CONNS: usize = 4;
    let mut conns = Vec::with_capacity(CONNS);
    let mut subs = Vec::with_capacity(CONNS);
    for _ in 0..CONNS {
        let c = server.connect().await;
        // Same three filters on every connection — nothing distinguishes them
        // but the socket, which is exactly the variable under test.
        let orders = c
            .subscribe(stream_id, consumer_id, b"orders.*")
            .await
            .expect("subscribe orders");
        let pay = c
            .subscribe(stream_id, consumer_id, b"payments.*")
            .await
            .expect("subscribe payments");
        let all = c
            .subscribe(stream_id, consumer_id, b"*.>")
            .await
            .expect("subscribe all");
        conns.push(c);
        subs.push((orders, pay, all));
    }

    publish_fixture(&admin, stream_id).await;

    let mut rows = Vec::with_capacity(CONNS);
    for (i, (orders, pay, all)) in subs.iter_mut().enumerate() {
        let got_orders = drain(orders).await;
        let got_pay = drain(pay).await;
        let got_all = drain(all).await;
        report(&format!("conn {i}"), &got_orders, &got_pay, &got_all);
        rows.push((got_orders, got_pay, got_all));
    }

    admin
        .delete_stream(b"triage_quad")
        .await
        .expect("delete stream");
    server.shutdown().await;

    let shape: Vec<(usize, usize, usize)> = rows
        .iter()
        .map(|(o, p, a)| (o.len(), p.len(), a.len()))
        .collect();
    println!("[four conns] per connection (orders, payments, all): {shape:?}");

    // Deliveries counted at the handles, not copies on the wire. The broker
    // sends each connection ONE copy per message and the client is expected
    // to hand it to every subscription whose filter accepts it, so the two
    // numbers differ on purpose: 6 copies in, 11 deliveries out. A shortfall
    // here is the client failing to fan out — the `all` column below shows
    // whether the copies arrived at all.
    for (i, (o, p, a)) in rows.iter().enumerate() {
        let total = o.len() + p.len() + a.len();
        assert_eq!(
            total,
            TOTAL + ORDERS + PAYMENTS,
            "[four conns] connection {i} made {total} deliveries, expected {} \
             ({ORDERS} orders + {PAYMENTS} payments + {TOTAL} catch-all): {shape:?}",
            TOTAL + ORDERS + PAYMENTS
        );
    }

    // The four rows must be identical. A row that differs means the broker
    // singled out a connection — a distribution bug, not a client one.
    assert!(
        shape.windows(2).all(|w| w[0] == w[1]),
        "[four conns] connections disagree: {shape:?} — under fanout every \
         connection is owed the same fixture"
    );

    // And each row must show the split, not one handle holding everything.
    for (i, (o, p, _)) in rows.iter().enumerate() {
        assert!(
            !o.is_empty() && !p.is_empty(),
            "[four conns] connection {i} funnelled its three subscriptions \
             into one handle: {shape:?}"
        );
    }
}

/// Two consumers, same stream, SAME filter, different names.
///
/// Streams may not have overlapping filters — `StreamConfig` says so. Nothing
/// says the same about consumers, and the natural reading is that they are
/// independent readers of one log. This pins whether the broker agrees.
#[tokio::test]
async fn two_consumers_may_share_one_filter() {
    let mut server = TestServerBuilder::new().spawn().await;
    let admin = server.connect().await;

    let stream_id = StreamBuilder::new(b"shared_filter")
        .filter(b"*.>")
        .create(&admin)
        .await
        .expect("stream");

    let first = ConsumerBuilder::new(b"reader_one")
        .filter(b"orders.>")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(100)
        .ack_wait_ms(30_000)
        .create(&admin, stream_id)
        .await
        .expect("first consumer with orders.> filter");

    let second = ConsumerBuilder::new(b"reader_two")
        .filter(b"orders.>")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(100)
        .ack_wait_ms(30_000)
        .create(&admin, stream_id)
        .await;

    match second {
        Ok(second_id) => {
            assert_ne!(
                first, second_id,
                "two consumers sharing a filter collapsed onto one id"
            );
            println!(
                "[same filter] ALLOWED — reader_one={first}, reader_two={second_id}"
            );

            // Independent readers: each should see every matching message.
            let c1 = server.connect().await;
            let mut s1 = c1.subscribe(stream_id, first, b"").await.expect("sub 1");
            let c2 = server.connect().await;
            let mut s2 = c2.subscribe(stream_id, second_id, b"").await.expect("sub 2");

            for i in 0..3u32 {
                admin
                    .publish_wait(
                        stream_id,
                        format!("orders.{i}").as_bytes(),
                        Bytes::from("o"),
                    )
                    .await
                    .expect("publish");
            }

            let got1 = drain(&mut s1).await;
            let got2 = drain(&mut s2).await;
            println!("[same filter] reader_one saw {}, reader_two saw {}", got1.len(), got2.len());

            admin
                .delete_stream(b"shared_filter")
                .await
                .expect("delete stream");

            assert_eq!(
                got1.len(),
                3,
                "reader_one missed messages: {got1:?} — two consumers on one \
                 filter are not independent"
            );
            assert_eq!(
                got2.len(),
                3,
                "reader_two missed messages: {got2:?} — the second consumer \
                 is starved by the first"
            );
        }
        Err(e) => {
            panic!(
                "[same filter] REJECTED — the broker refused a second consumer \
                 with filter `orders.>`: {e:?}. If that is intentional it \
                 belongs in the docs; today only STREAM filters are documented \
                 as non-overlapping."
            );
        }
    }

    server.shutdown().await;
}

/// Same NAME, twice, on the same stream.
///
/// `create_consumer` is documented to error when an existing consumer has a
/// different config (task "Stale Config"), and consumer ids are namespaced by
/// `(stream_id, name)` (task "Shared Consumer Names"). Both imply a repeat of
/// the same name resolves to the SAME consumer rather than a second one — but
/// nothing pins which, nor what a conflicting filter does.
#[tokio::test]
async fn two_consumers_with_the_same_name() {
    let mut server = TestServerBuilder::new().spawn().await;
    let admin = server.connect().await;

    let stream_id = StreamBuilder::new(b"same_name")
        .filter(b"*.>")
        .create(&admin)
        .await
        .expect("stream");

    let first = ConsumerBuilder::new(b"duplicate")
        .filter(b"orders.>")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(100)
        .ack_wait_ms(30_000)
        .create(&admin, stream_id)
        .await
        .expect("first consumer");
    println!("[same name] first  -> {first}");

    // Identical config. If names are keys, this is an idempotent re-create.
    let same_config = ConsumerBuilder::new(b"duplicate")
        .filter(b"orders.>")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(100)
        .ack_wait_ms(30_000)
        .create(&admin, stream_id)
        .await;
    match &same_config {
        Ok(id) => println!("[same name] identical config -> ALLOWED, id={id}"),
        Err(e) => println!("[same name] identical config -> REJECTED: {e:?}"),
    }

    // Different filter under the same name. This is the one that matters: if
    // it is accepted AND returns the first id, the second call's config is
    // silently discarded and the caller believes a filter it never got.
    let other_config = ConsumerBuilder::new(b"duplicate")
        .filter(b"telemetry.>")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(999)
        .ack_wait_ms(30_000)
        .create(&admin, stream_id)
        .await;
    match &other_config {
        Ok(id) => println!("[same name] different config -> ALLOWED, id={id}"),
        Err(e) => println!("[same name] different config -> REJECTED: {e:?}"),
    }

    admin
        .delete_stream(b"same_name")
        .await
        .expect("delete stream");
    server.shutdown().await;

    // Re-creating with the SAME config must not mint a second consumer.
    if let Ok(id) = same_config {
        assert_eq!(
            id, first,
            "the same name with the same config produced a SECOND consumer \
             ({id} != {first}) — consumer ids are supposed to be namespaced by \
             (stream_id, name)"
        );
    }

    // A conflicting config must be refused, not silently ignored.
    if let Ok(id) = other_config {
        panic!(
            "the same name with a DIFFERENT filter was accepted and returned \
             id={id} (first={first}). The caller asked for `telemetry.>` and \
             max_inflight=999 and got neither — a silent no-op is worse than \
             an error, because the config the caller believes in never took \
             effect."
        );
    }
}


async fn setup(admin: &Client, stream: &[u8], consumer: &[u8]) -> (u32, u32) {
    let stream_id = StreamBuilder::new(stream)
        .filter(b"*.>")
        .create(admin)
        .await
        .expect("stream");

    // ONE consumer, wide open. Every narrowing below is subscription-side.
    // Fanout: each subscription sees everything its own filter accepts,
    // instead of the three splitting one copy between them.
    let consumer_id = ConsumerBuilder::new(consumer)
        .ack_policy(AckPolicy::Explicit)
        .deliver_mode(DeliverMode::Fanout)
        .max_inflight(1_000)
        .ack_wait_ms(30_000)
        .create(admin, stream_id)
        .await
        .expect("consumer");

    (stream_id, consumer_id)
}

async fn publish_fixture(admin: &Client, stream_id: u32) {
    for i in 0..ORDERS {
        admin
            .publish_wait(
                stream_id,
                format!("orders.{i}").as_bytes(),
                Bytes::from("o"),
            )
            .await
            .expect("publish order");
    }
    for i in 0..PAYMENTS {
        admin
            .publish_wait(
                stream_id,
                format!("payments.{i}").as_bytes(),
                Bytes::from("p"),
            )
            .await
            .expect("publish payment");
    }
    // Matches the consumer's `>` and only the widest subscription. If a narrow
    // subscription is handed this one, it is stranded.
    admin
        .publish_wait(stream_id, b"audit.trail", Bytes::from("x"))
        .await
        .expect("publish audit");
}

async fn drain(sub: &mut SubscriptionHandle) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(400), sub.recv()).await {
        out.push(msg.subject().to_vec());
        msg.ack();
    }
    out
}

/// Printed before the asserts so a failure shows the shape, not just a count.
fn report(case: &str, orders: &[Vec<u8>], pay: &[Vec<u8>], all: &[Vec<u8>]) {
    let show = |v: &[Vec<u8>]| {
        v.iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>()
    };
    println!("[{case}] orders.*   -> {:?}", show(orders));
    println!("[{case}] payments.* -> {:?}", show(pay));
    println!("[{case}] >          -> {:?}", show(all));
}

fn assert_outcome(case: &str, orders: &[Vec<u8>], pay: &[Vec<u8>], all: &[Vec<u8>]) {
    // 1. Nobody may receive what its own filter excludes. Holds regardless of
    //    how the broker splits the work.
    assert!(
        orders.iter().all(|s| s.starts_with(b"orders.")),
        "[{case}] orders subscription got foreign subjects: {orders:?}"
    );
    assert!(
        pay.iter().all(|s| s.starts_with(b"payments.")),
        "[{case}] payments subscription got foreign subjects: {pay:?}"
    );

    // 2. Fanout: every subscription sees each message its own filter
    //    accepts. Nothing is split, so each count is exact and independent.
    assert_eq!(
        orders.len(),
        ORDERS,
        "[{case}] orders.* saw {} of {ORDERS} — fanout gives each \
         subscription its own copy, so a short count is a lost message",
        orders.len()
    );
    assert_eq!(
        pay.len(),
        PAYMENTS,
        "[{case}] payments.* saw {} of {PAYMENTS}",
        pay.len()
    );
    assert_eq!(
        all.len(),
        TOTAL,
        "[{case}] the catch-all saw {} of {TOTAL}",
        all.len()
    );
}
