//! Several consumers sharing one queue group. The sibling of
//! `one_consumer_many_filters`: there, one consumer held several
//! subscriptions and the question was whether they stayed separate. Here the
//! consumers are separate by construction and the question is the opposite —
//! does the broker actually SPREAD the work across them.
//!
//! Queue mode promises each seq goes to exactly one member. That alone is
//! satisfied by handing everything to one worker and leaving the rest idle,
//! which is the failure these tests are written to catch. `drain.rs:745`
//! rotates the candidate start by `entry.seq`, so an even spread is the
//! claim; and `drain.rs:877` skips a member with no capacity WITHOUT marking
//! the queue served, so a saturated worker should hand over rather than
//! block the queue.
//!
//! Each consumer gets its OWN connection. Deliveries carry `consumer_id` and
//! nothing finer, so co-locating them would only re-test the client demux
//! limit that `one_consumer_many_filters` already pins.

mod test_helper;
use test_helper::TestServerBuilder;

use arbitro_client_tokio::{AckPolicy, Client, ConsumerBuilder, StreamBuilder, SubscriptionHandle};
use bytes::Bytes;
use std::time::Duration;

const MEMBERS: usize = 3;
const MESSAGES: usize = 15;

/// Stand-in for handler work, held before each ack.
///
/// Honest about what it buys: the publishes all complete before any drain
/// starts, so this does not change what the broker decided — the spread is
/// already fixed by then. What it does buy is a consumer that acks slowly
/// rather than instantly, which is how a real worker behaves and which keeps
/// messages inflight long enough that an ack-accounting bug has room to show.
const WORK_MS: u64 = 50;

/// Three workers, one queue, `MESSAGES` jobs. Every worker must get
/// some. An even split is the intent, but the assertion only demands that
/// nobody starves — the exact ratio is the scheduler's business.
#[tokio::test]
async fn queue_spreads_across_its_members() {
    let mut server = TestServerBuilder::new().spawn().await;
    let admin = server.connect().await;

    let stream_id = StreamBuilder::new(b"queue_spread")
        .filter(b"jobs.>")
        .create(&admin)
        .await
        .expect("stream");

    // One connection per worker, each its own consumer, all in one group.
    let mut conns = Vec::new();
    let mut subs = Vec::new();
    for i in 0..MEMBERS {
        let c = server.connect().await;
        let consumer_id = ConsumerBuilder::new(format!("worker_{i}").as_bytes())
            .group(b"crew")
            .filter(b"jobs.>")
            .ack_policy(AckPolicy::Explicit)
            .max_inflight(100)
            .ack_wait_ms(30_000)
            .create(&admin, stream_id)
            .await
            .expect("consumer");
        let sub = c
            .subscribe(stream_id, consumer_id, b"jobs.>")
            .await
            .expect("subscribe");
        conns.push(c);
        subs.push(sub);
    }

    for i in 0..MESSAGES {
        admin
            .publish_wait(stream_id, format!("jobs.{i}").as_bytes(), Bytes::from("j"))
            .await
            .expect("publish");
    }

    let mut counts = Vec::new();
    for sub in subs.iter_mut() {
        counts.push(drain(sub).await.len());
    }
    let total: usize = counts.iter().sum();
    println!("[spread] per worker: {counts:?}  total={total}");

    admin
        .delete_stream(b"queue_spread")
        .await
        .expect("delete stream");
    server.shutdown().await;

    assert_eq!(
        total, MESSAGES,
        "[spread] queue mode must deliver each seq exactly once: got {total} \
         of {MESSAGES} across {counts:?}"
    );
    assert!(
        counts.iter().all(|&c| c > 0),
        "[spread] a worker got nothing while others were free — the queue \
         handed the work to a subset instead of spreading it: {counts:?}"
    );
}

/// One worker saturates (max_inflight=1, never acks) while a second is idle.
/// The saturated one may keep its single message; everything else belongs to
/// the worker that can take it. If the queue stalls instead, one slow
/// consumer can hold up every other member.
#[tokio::test]
async fn a_saturated_member_does_not_hold_the_queue() {
    let mut server = TestServerBuilder::new().spawn().await;
    let admin = server.connect().await;

    let stream_id = StreamBuilder::new(b"queue_saturate")
        .filter(b"tasks.>")
        .create(&admin)
        .await
        .expect("stream");

    // Cap of 1 and no acks: after its first message this one is full.
    let stuck_conn = server.connect().await;
    let stuck_id = ConsumerBuilder::new(b"stuck")
        .group(b"pair")
        .filter(b"tasks.>")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(1)
        .ack_wait_ms(30_000)
        .create(&admin, stream_id)
        .await
        .expect("stuck consumer");
    let mut stuck = stuck_conn
        .subscribe(stream_id, stuck_id, b"tasks.>")
        .await
        .expect("subscribe stuck");

    let healthy_conn = server.connect().await;
    let healthy_id = ConsumerBuilder::new(b"healthy")
        .group(b"pair")
        .filter(b"tasks.>")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(100)
        .ack_wait_ms(30_000)
        .create(&admin, stream_id)
        .await
        .expect("healthy consumer");
    let mut healthy = healthy_conn
        .subscribe(stream_id, healthy_id, b"tasks.>")
        .await
        .expect("subscribe healthy");

    for i in 0..MESSAGES {
        admin
            .publish_wait(stream_id, format!("tasks.{i}").as_bytes(), Bytes::from("t"))
            .await
            .expect("publish");
    }

    // Deliberately does NOT ack — that is what keeps it saturated.
    let stuck_got = drain_without_ack(&mut stuck).await.len();
    let healthy_got = drain(&mut healthy).await.len();
    println!("[saturate] stuck={stuck_got}  healthy={healthy_got}");

    admin
        .delete_stream(b"queue_saturate")
        .await
        .expect("delete stream");
    server.shutdown().await;

    assert!(
        stuck_got <= 1,
        "[saturate] max_inflight=1 was exceeded: the stuck worker holds \
         {stuck_got} unacked messages"
    );
    assert_eq!(
        stuck_got + healthy_got,
        MESSAGES,
        "[saturate] {} messages stranded — a saturated member blocked the \
         queue instead of yielding to its idle sibling (stuck={stuck_got}, \
         healthy={healthy_got})",
        MESSAGES - (stuck_got + healthy_got)
    );
}

/// The same question one level down: THREE SUBSCRIPTIONS on ONE consumer,
/// all with the identical filter. Nothing about the subjects forces a split,
/// so whatever spread appears is the scheduler's doing alone.
///
/// The queue is keyed by `QueueId`, and a subscription inherits its
/// consumer's queue (`catalog/mod.rs:422`). Three subscriptions of one
/// consumer therefore land in ONE queue slot, and `served_queues` allows a
/// single delivery per seq across all of them. Whether the rotation then
/// picks a different one each time — or hands everything to the first and
/// leaves the other two idle — is what this measures.
///
/// Three CONNECTIONS, deliberately. On one connection the client routes by
/// `consumer_id` alone and would funnel all three into the last handle, so
/// the result would say nothing about the broker. That variant stays
/// unmeasurable until the delivery frame carries the subscription id.
#[tokio::test]
async fn queue_spreads_across_subscriptions_of_one_consumer() {
    let mut server = TestServerBuilder::new().spawn().await;
    let admin = server.connect().await;

    let stream_id = StreamBuilder::new(b"sub_spread")
        .filter(b"work.>")
        .create(&admin)
        .await
        .expect("stream");

    // ONE consumer. Every subscription below hangs off this same id.
    let consumer_id = ConsumerBuilder::new(b"single_worker")
        .filter(b"work.>")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(100)
        .ack_wait_ms(30_000)
        .create(&admin, stream_id)
        .await
        .expect("consumer");

    let mut conns = Vec::new();
    let mut subs = Vec::new();
    for _ in 0..MEMBERS {
        let c = server.connect().await;
        // Identical filter — no subject-driven split to confound the result.
        let sub = c
            .subscribe(stream_id, consumer_id, b"work.>")
            .await
            .expect("subscribe");
        conns.push(c);
        subs.push(sub);
    }

    for i in 0..MESSAGES {
        admin
            .publish_wait(stream_id, format!("work.{i}").as_bytes(), Bytes::from("w"))
            .await
            .expect("publish");
    }

    let mut counts = Vec::new();
    for sub in subs.iter_mut() {
        counts.push(drain(sub).await.len());
    }
    let total: usize = counts.iter().sum();
    println!("[sub spread] per subscription: {counts:?}  total={total}");

    admin
        .delete_stream(b"sub_spread")
        .await
        .expect("delete stream");
    server.shutdown().await;

    assert_eq!(
        total, MESSAGES,
        "[sub spread] each seq must be delivered exactly once across the \
         consumer's subscriptions: got {total} of {MESSAGES} across {counts:?}"
    );
    assert!(
        counts.iter().all(|&c| c > 0),
        "[sub spread] a subscription received nothing while the others \
         worked — the queue treats the consumer as ONE slot and serves a \
         single subscription instead of rotating between them: {counts:?}"
    );
}

/// The variant a real caller would write: ONE stream, ONE consumer, ONE
/// client, three subscriptions. Same shape as
/// `queue_spreads_across_subscriptions_of_one_consumer`, minus the three
/// connections that test needs to stay measurable.
///
/// That difference is the whole point. This test alone cannot say WHERE the
/// work stopped being spread — "broker sent all 15 to one binding" and
/// "broker spread them and the client merged them" produce the identical
/// `[0, 0, 15]` at this vantage point. Instrumenting the drain's pick
/// settled it: on a single connection it rotates across all three
/// subscriptions (`sub=1,2,3` in turn, `n=3`, one `conn`), so the merge is
/// the client's. `Deliver` carries `consumer_id` and nothing finer, so
/// `consume/demux.rs:78` resolves one channel for all three and the last
/// `register` wins.
#[tokio::test]
async fn one_client_three_subscriptions_on_one_consumer() {
    let mut server = TestServerBuilder::new().spawn().await;
    let admin = server.connect().await;

    let stream_id = StreamBuilder::new(b"one_client")
        .filter(b"unit.>")
        .create(&admin)
        .await
        .expect("stream");

    let consumer_id = ConsumerBuilder::new(b"solo")
        .filter(b"unit.>")
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(100)
        .ack_wait_ms(30_000)
        .create(&admin, stream_id)
        .await
        .expect("consumer");

    // ONE client for all three.
    let c = server.connect().await;
    let mut subs = Vec::new();
    for _ in 0..MEMBERS {
        subs.push(
            c.subscribe(stream_id, consumer_id, b"unit.>")
                .await
                .expect("subscribe"),
        );
    }

    for i in 0..MESSAGES {
        admin
            .publish_wait(stream_id, format!("unit.{i}").as_bytes(), Bytes::from("u"))
            .await
            .expect("publish");
    }

    let mut counts = Vec::new();
    for sub in subs.iter_mut() {
        counts.push(drain(sub).await.len());
    }
    let total: usize = counts.iter().sum();
    println!("[one client] per subscription: {counts:?}  total={total}");

    admin
        .delete_stream(b"one_client")
        .await
        .expect("delete stream");
    server.shutdown().await;

    assert_eq!(
        total, MESSAGES,
        "[one client] {} of {MESSAGES} never reached the process at all — \
         this is delivery loss, not just misrouting: {counts:?}",
        MESSAGES - total
    );
    assert!(
        counts.iter().all(|&c| c > 0),
        "[one client] the broker spread the work but the client funnelled it \
         into one handle: deliveries carry only `consumer_id`, so all three \
         subscriptions resolve to the same channel and the last `register` \
         wins. The same arrangement across three connections spreads evenly \
         — see `queue_spreads_across_subscriptions_of_one_consumer`. \
         Got {counts:?}"
    );
}

async fn drain(sub: &mut SubscriptionHandle) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(400), sub.recv()).await {
        out.push(msg.subject().to_vec());
        // Stand-in for handler work: the message stays inflight while it runs.
        tokio::time::sleep(Duration::from_millis(WORK_MS)).await;
        msg.ack();
    }
    out
}

/// Collects without acking, so the consumer stays at its inflight cap.
async fn drain_without_ack(sub: &mut SubscriptionHandle) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(400), sub.recv()).await {
        out.push(msg.subject().to_vec());
    }
    out
}

/// Silences the unused-import warning when only one test body uses `Client`.
#[allow(dead_code)]
fn _assert_client_type(_: &Client) {}
