//! Regression tests for the robustness audit of `master@f73393e`.
//!
//! Each test states the behaviour the broker MUST have and fails while the
//! finding it names is open. They are expected to be red until fixed.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;

const ACK_NONE: u8 = 0;
const ACK_EXPLICIT: u8 = 1;
const DELIVER_ALL: u8 = 0;
const MODE_FANOUT: u8 = 0;

async fn stream(client: &arbitro_client_tokio::Client, name: &[u8], journal_kind: u8) -> u32 {
    let mut filter = name.to_vec();
    filter.extend_from_slice(b".>");
    let resp = client
        .create_stream(name, &filter, 0, 0, 0, 1, journal_kind, 0, 0, 0)
        .await
        .expect("create stream");
    TestServer::parse_id(&resp)
}

async fn consumer(
    client: &arbitro_client_tokio::Client,
    stream_id: u32,
    name: &[u8],
    ack_policy: u8,
    max_inflight: u16,
) -> u32 {
    let ack_wait_ms = if ack_policy == ACK_EXPLICIT { 30_000 } else { 0 };
    let resp = client
        .create_consumer(
            stream_id,
            name,
            name,
            b"",
            max_inflight,
            ack_policy,
            DELIVER_ALL,
            MODE_FANOUT,
            ack_wait_ms,
            0,
        )
        .await
        .expect("create consumer");
    TestServer::parse_id(&resp)
}

async fn publish(client: &arbitro_client_tokio::Client, stream_id: u32, subject: &[u8], n: u64) {
    for i in 0..n {
        client
            .publish_wait(stream_id, subject, Bytes::copy_from_slice(&i.to_le_bytes()))
            .await
            .expect("publish");
    }
}

/// AUDIT R-04. Pausing sets a per-consumer flag in the drain's counters that
/// nothing clears on delete. Consumer ids are recycled, so the next consumer
/// to get the id inherits the pause and never receives anything.
#[tokio::test(flavor = "multi_thread")]
async fn a_recycled_consumer_id_does_not_inherit_a_pause() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let sid = stream(&client, b"pz", 0).await;

    let old = consumer(&client, sid, b"old", ACK_NONE, u16::MAX).await;
    client.pause_consumer(old).await.expect("pause");
    client.delete_consumer(old).await.expect("delete");

    let new = consumer(&client, sid, b"new", ACK_NONE, u16::MAX).await;
    assert_eq!(new, old, "precondition: the id was recycled");

    let mut handle = client.subscribe(sid, new, b"").await.expect("subscribe");
    publish(&client, sid, b"pz.a", 1).await;

    let got = tokio::time::timeout(Duration::from_secs(3), handle.recv()).await;
    assert!(
        matches!(got, Ok(Some(_))),
        "consumer {new} never received: it inherited the deleted consumer's pause"
    );
    server.shutdown().await;
}

/// AUDIT R-01. A fire-and-forget consumer has no delivery memory. When an
/// ack-mode sibling on the same shard fills its window, the cursor stays
/// pinned and every re-walk sends the fire-and-forget consumer the same
/// messages again.
#[tokio::test(flavor = "multi_thread")]
async fn fire_and_forget_is_not_redelivered_while_a_sibling_is_capped() {
    const N: u64 = 50;
    let mut server = TestServerBuilder::new().shard_count(1).spawn().await;
    let client = server.connect().await;
    let sid = stream(&client, b"ff", 0).await;

    // Takes one message, never acks: pins the cursor right after it.
    let capped = consumer(&client, sid, b"capped", ACK_EXPLICIT, 1).await;
    let free = consumer(&client, sid, b"free", ACK_NONE, u16::MAX).await;
    let _capped_handle = client.subscribe(sid, capped, b"").await.expect("subscribe");
    let mut free_handle = client.subscribe(sid, free, b"").await.expect("subscribe");

    publish(&client, sid, b"ff.a", N).await;

    let mut seen = HashSet::new();
    let mut total = 0u64;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
    while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, free_handle.recv()).await {
        total += 1;
        seen.insert(msg.seq);
    }

    assert_eq!(seen.len() as u64, N, "every message must arrive once");
    assert_eq!(
        total, N,
        "fire-and-forget got {total} deliveries for {N} messages: {} duplicates",
        total - N
    );
    server.shutdown().await;
}

/// AUDIT R-05. The persisted cursor is the highest acked seq, not the
/// contiguous ack floor. Ack 1 and 3, leave 2 unacked, restart: the consumer
/// resumes after 3 and seq 2 is never delivered again.
#[tokio::test(flavor = "multi_thread")]
async fn an_unacked_gap_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    let (sid, cid) = {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let sid = stream(&client, b"gap", 1).await;
        let cid = consumer(&client, sid, b"worker", ACK_EXPLICIT, 3).await;
        publish(&client, sid, b"gap.a", 3).await;

        let mut handle = client.subscribe(sid, cid, b"").await.expect("subscribe");
        for _ in 0..3 {
            let msg = tokio::time::timeout(Duration::from_secs(3), handle.recv())
                .await
                .expect("delivery")
                .expect("open");
            if msg.seq != 2 {
                msg.ack();
            }
        }
        // Let the client's ack batcher reach the broker before it stops.
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(handle);
        server.shutdown().await;
        (sid, cid)
    };

    let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
    let client = server.connect().await;
    let mut handle = client.subscribe(sid, cid, b"").await.expect("resubscribe");

    let mut redelivered = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, handle.recv()).await {
        redelivered.push(msg.seq);
    }
    assert!(
        redelivered.contains(&2),
        "seq 2 was never acked, yet after restart the consumer got {redelivered:?}"
    );
    server.shutdown().await;
}

/// Control for `an_unacked_gap_survives_a_restart`: with NOTHING acked the
/// same flow must redeliver all three. If this is red too, the gap test's
/// failure is not about the cursor.
#[tokio::test(flavor = "multi_thread")]
async fn control_unacked_messages_return_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    let (sid, cid) = {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let sid = stream(&client, b"ctl", 1).await;
        let cid = consumer(&client, sid, b"worker", ACK_EXPLICIT, 3).await;
        publish(&client, sid, b"ctl.a", 3).await;
        let mut handle = client.subscribe(sid, cid, b"").await.expect("subscribe");
        for _ in 0..3 {
            tokio::time::timeout(Duration::from_secs(3), handle.recv())
                .await
                .expect("delivery")
                .expect("open");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(handle);
        server.shutdown().await;
        (sid, cid)
    };

    let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
    let client = server.connect().await;
    let mut handle = client.subscribe(sid, cid, b"").await.expect("resubscribe");
    let mut redelivered = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, handle.recv()).await {
        redelivered.push(msg.seq);
    }
    assert_eq!(redelivered, vec![1, 2, 3], "nothing acked, all three must return");
    server.shutdown().await;
}

/// AUDIT security P1-1 / client contract. `SubscriptionHandle`'s own docs
/// promise that dropping one subscription leaves its siblings on the same
/// consumer running. The broker's Unsubscribe retires every binding of the
/// consumer instead — including the ones held by other connections.
#[tokio::test(flavor = "multi_thread")]
async fn dropping_one_subscription_leaves_other_connections_subscribed() {
    let mut server = TestServerBuilder::new().spawn().await;
    let a = server.connect().await;
    let b = server.connect().await;
    let sid = stream(&a, b"us", 0).await;
    let cid = consumer(&a, sid, b"shared", ACK_NONE, u16::MAX).await;

    let handle_a = a.subscribe(sid, cid, b"").await.expect("subscribe a");
    let mut handle_b = b.subscribe(sid, cid, b"").await.expect("subscribe b");

    drop(handle_a);
    // The read loop handles a connection's frames in order, so this round
    // trip lands after the Unsubscribe that the drop just sent.
    a.get_stream(b"us").await.expect("round trip");

    publish(&a, sid, b"us.a", 1).await;
    let got = tokio::time::timeout(Duration::from_secs(3), handle_b.recv()).await;
    assert!(
        matches!(got, Ok(Some(_))),
        "connection B lost its subscription when connection A dropped its own"
    );
    server.shutdown().await;
}

/// AUDIT Egr F1. A consumer at its `max_inflight` pins the drain cursor; the
/// drain then slept a "50 µs" timer that tokio rounds up to ≥ 1 ms, and the
/// ack that freed the slot could not wake it. With `max_inflight = 1` every
/// delivery after the first paid that timer: 100 messages took ≥ 99 ms.
/// Refilled by the ack's own gate release, they take a few ack round trips.
#[tokio::test(flavor = "multi_thread")]
async fn a_capped_consumer_is_refilled_by_its_ack_not_a_timer() {
    const MESSAGES: u64 = 100;
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let sid = stream(&client, b"cap", 0).await;
    publish(&client, sid, b"cap.a", MESSAGES).await;
    let cid = consumer(&client, sid, b"capped", ACK_EXPLICIT, 1).await;

    let mut handle = client.subscribe(sid, cid, b"").await.expect("subscribe");
    let started = std::time::Instant::now();
    for _ in 0..MESSAGES {
        let msg = tokio::time::timeout(Duration::from_secs(5), handle.recv())
            .await
            .expect("delivery stalled 5s")
            .expect("subscription closed");
        msg.ack();
    }
    let elapsed = started.elapsed();
    // The timer floor is ≥ 99 ms; 70 ms leaves the ack path ~3x headroom.
    assert!(
        elapsed < Duration::from_millis(70),
        "{MESSAGES} capped deliveries took {elapsed:?}: each refill waited on a timer, not the ack"
    );
    server.shutdown().await;
}
