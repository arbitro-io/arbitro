//! Reproductions for the P0/P1 findings of the arbitro-server robustness
//! audit (`crates/arbitro-server/ROBUSTNESS_AUDIT.md`):
//!
//! * F1 — blind inflight decrement on ack: a double-ack drives the shared
//!   consumer/queue inflight counter below zero, wrapping to ~u32::MAX and
//!   permanently wedging the consumer (audit 2.2 P0-2, invariant A4).
//! * F2 — phantom inflight on redelivery: the drain increments the shared
//!   inflight per delivery with no dedup while the engine skips already
//!   pending seqs (ROB-12), so a cursor rewind permanently leaks capacity
//!   (audit 2.2 P0-1, invariant A3).
//! * F3 — consumer ack cursors are never written to the command log, so a
//!   full broker restart replays fully-acked history (audit 2.4 P1,
//!   invariant A7).

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use bytes::Bytes;
use std::collections::HashSet;
use std::time::Duration;

// ═══════════════════════════════════════════════════════════════════════════
// F1 — double ack must not wedge the consumer (A4)
// ═══════════════════════════════════════════════════════════════════════════

/// Deliver one message, ack the SAME delivery several times (an ordinary
/// client retry), then publish a probe message. If the shared inflight
/// counter underflows (u32 wrap), `consumer_has_capacity` is false forever
/// and the probe is never delivered.
#[tokio::test(flavor = "multi_thread")]
async fn double_ack_does_not_wedge_consumer() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"dack", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(
            stream_id, b"worker", b"worker", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64,
        )
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    client
        .publish_wait(stream_id, b"dack_m", Bytes::from_static(b"one"))
        .await
        .expect("publish");

    let msg = tokio::time::timeout(Duration::from_secs(2), handle.recv())
        .await
        .expect("first delivery should arrive")
        .expect("subscription open");
    assert_eq!(&msg.payload()[..], b"one");

    // Ack the same delivery three times. The engine matches the seq once;
    // every extra ack must be a no-op on the shared counters.
    msg.ack();
    msg.ack();
    msg.ack();

    // Let the fire-and-forget acks reach the shard.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The consumer must still receive new messages.
    client
        .publish_wait(stream_id, b"dack_m", Bytes::from_static(b"two"))
        .await
        .expect("probe publish");

    let probe = tokio::time::timeout(Duration::from_secs(2), handle.recv())
        .await
        .expect("consumer wedged after double-ack: probe message never delivered")
        .expect("subscription open");
    assert_eq!(&probe.payload()[..], b"two");
    probe.ack();

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// F2 — redelivery of still-pending seqs must not leak inflight capacity (A3)
// ═══════════════════════════════════════════════════════════════════════════

/// With max_inflight = 8: deliver 6, ack the two lowest, nack the third.
/// The nack rewinds the shard cursor, and the drain re-walks the window —
/// redelivering seqs that are STILL pending (unacked tail). The drain
/// increments the shared inflight for each redelivery, but the engine's
/// ROB-12 dedup skips re-adding them, so acking everything once leaves a
/// permanent phantom inflight and the consumer loses capacity forever.
///
/// After settling (every seq acked exactly once, engine pending empty),
/// a burst of 8 unacked deliveries must fit in max_inflight = 8.
#[tokio::test(flavor = "multi_thread")]
async fn redelivery_does_not_leak_inflight_capacity() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"phantom", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(
            stream_id, b"worker", b"worker", b"", 8u16, 1u8, 0u8, 0u8, 0u32, 0u64,
        )
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    for i in 0..6u32 {
        let payload = format!("m{i}");
        client
            .publish_wait(
                stream_id,
                b"phantom_m",
                Bytes::copy_from_slice(payload.as_bytes()),
            )
            .await
            .expect("publish");
    }

    let mut initial = Vec::new();
    for _ in 0..6 {
        let msg = tokio::time::timeout(Duration::from_secs(2), handle.recv())
            .await
            .expect("initial delivery should arrive")
            .expect("subscription open");
        initial.push(msg);
    }
    initial.sort_by_key(|m| m.seq);

    // Ack the two lowest seqs, nack the third. The rewind re-walks the
    // still-pending tail (the three highest seqs).
    let mut acked: HashSet<u64> = HashSet::new();
    initial[0].ack();
    acked.insert(initial[0].seq);
    initial[1].ack();
    acked.insert(initial[1].seq);
    initial[2].nack();

    // Absorb the redelivery wave, acking every seq exactly once (dups are
    // dropped without a second ack so this test stays independent of F1).
    loop {
        match tokio::time::timeout(Duration::from_millis(700), handle.recv()).await {
            Ok(Some(msg)) => {
                if acked.insert(msg.seq) {
                    msg.ack();
                }
            }
            _ => break,
        }
    }
    // Anything never redelivered is acked from its original handle.
    for msg in &initial {
        if acked.insert(msg.seq) {
            msg.ack();
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Engine pending is now empty — the full max_inflight budget must be
    // available. Publish 8 and receive all 8 WITHOUT acking.
    for i in 0..8u32 {
        let payload = format!("probe{i}");
        client
            .publish_wait(
                stream_id,
                b"phantom_m",
                Bytes::copy_from_slice(payload.as_bytes()),
            )
            .await
            .expect("probe publish");
    }

    let mut probes = Vec::new();
    for _ in 0..8 {
        match tokio::time::timeout(Duration::from_secs(2), handle.recv()).await {
            Ok(Some(msg)) => probes.push(msg),
            _ => break,
        }
    }
    assert_eq!(
        probes.len(),
        8,
        "phantom inflight consumed capacity: only {}/8 unacked deliveries fit \
         in max_inflight=8 after a nack-driven redelivery",
        probes.len()
    );
    for msg in &probes {
        msg.ack();
    }

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// F3 — acked cursor survives a full broker restart (A7)
// ═══════════════════════════════════════════════════════════════════════════

/// Consume + ack the whole stream, restart the broker on the same data dir,
/// resubscribe (DeliverPolicy::All): ZERO redeliveries must occur — the
/// consumer cursor must be persisted in the command log and replayed.
/// A new message published after the restart must still be delivered.
#[tokio::test(flavor = "multi_thread")]
async fn acked_cursor_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let resp = client
            .create_stream(b"curdur", b"cur.>", 0, 0, 0, 1, 1, 0, 0, 0)
            .await
            .unwrap();
        let sid = TestServer::parse_id(&resp);
        let resp = client
            .create_consumer(
                sid, b"reader", b"reader", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64,
            )
            .await
            .unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        for i in 0..5u32 {
            let payload = format!("msg-{i}");
            client
                .publish_wait(sid, b"cur.evt", Bytes::copy_from_slice(payload.as_bytes()))
                .await
                .expect("publish");
        }
        for _ in 0..5 {
            let msg = tokio::time::timeout(Duration::from_secs(2), sub.recv())
                .await
                .expect("delivery should arrive")
                .expect("subscription open");
            msg.ack();
        }
        // Let the fire-and-forget acks reach the broker before shutdown.
        tokio::time::sleep(Duration::from_millis(500)).await;
        server.shutdown().await;
    }

    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;

        let resp = client.list_streams(0, 1000).await.unwrap();
        let sid = TestServer::find_stream_id(&resp, b"curdur").expect("stream survived restart");

        // Same durable name — resolves to the replayed consumer.
        let resp = client
            .create_consumer(
                sid, b"reader", b"reader", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64,
            )
            .await
            .unwrap();
        let cid = TestServer::parse_id(&resp);
        let mut sub = client.subscribe(sid, cid, b"").await.unwrap();

        // Everything was acked before the restart — nothing may be replayed.
        let redelivered = tokio::time::timeout(Duration::from_millis(1500), sub.recv()).await;
        assert!(
            redelivered.is_err(),
            "fully-acked history was replayed after restart: consumer cursor \
             was not persisted"
        );

        // Liveness: a message published after the restart still flows.
        client
            .publish_wait(sid, b"cur.evt", Bytes::from_static(b"after-restart"))
            .await
            .expect("post-restart publish");
        let msg = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("post-restart message should be delivered")
            .expect("subscription open");
        assert_eq!(&msg.payload()[..], b"after-restart");
        msg.ack();

        server.shutdown().await;
    }
}
