//! `Client::queue_subscribe` — the one-call durable work queue.
//!
//! `queue_subscribe(stream, queue, filter)` ties the durable consumer name to
//! the queue group name, so N workers calling it with the same queue name all
//! land on the SAME durable consumer and the broker load-balances between
//! them. A different queue name is a separate, independent durable queue.
//!
//! These tests prove the three properties that matter:
//!   1. `load_balances_across_workers` — same queue name from 3 connections:
//!      every message delivered exactly once, spread across workers.
//!   2. `distinct_queue_names_are_independent` — a different queue name is a
//!      NEW queue with its own cursor: it gets its own full copy.
//!   3. `queue_survives_broker_restart` — the queue is durable: after a
//!      restart, re-joining the same queue name resumes instead of replaying
//!      already-acked messages, and still delivers new ones.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::Client;
use bytes::Bytes;
use std::time::Duration;

async fn create_stream(client: &Client, name: &[u8]) -> u32 {
    let resp = client
        .create_stream(name, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream");
    TestServer::parse_id(&resp)
}

/// Drain up to `want` messages, acking each. Returns the payloads received.
async fn drain_acking(
    handle: &mut arbitro_client_tokio::SubscriptionHandle,
    want: usize,
    budget: Duration,
) -> Vec<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + budget;
    let mut got = Vec::new();
    while got.len() < want {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, handle.recv()).await {
            Ok(Some(msg)) => {
                got.push(msg.payload().to_vec());
                msg.ack();
            }
            _ => break,
        }
    }
    got
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Same queue name from N connections → competing consumers.
//    Each message is delivered exactly ONCE across the whole queue.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn queue_subscribe_load_balances_across_workers() {
    let mut server = TestServerBuilder::new().spawn().await;

    let setup = server.connect().await;
    let stream_id = create_stream(&setup, b"qs_lb").await;

    // Three workers, three separate connections, SAME queue name. The first
    // call creates the durable consumer; the other two join it idempotently.
    let w1 = server.connect().await;
    let w2 = server.connect().await;
    let w3 = server.connect().await;

    let mut s1 = w1
        .queue_subscribe(stream_id, b"workers", b"")
        .await
        .expect("worker 1 must join the queue");
    let mut s2 = w2
        .queue_subscribe(stream_id, b"workers", b"")
        .await
        .expect("worker 2 must join the same queue (idempotent create)");
    let mut s3 = w3
        .queue_subscribe(stream_id, b"workers", b"")
        .await
        .expect("worker 3 must join the same queue (idempotent create)");

    const TOTAL: usize = 30;
    for i in 0..TOTAL {
        setup
            .publish_wait(stream_id, b"qs_lb.job", Bytes::from(vec![i as u8]))
            .await
            .expect("publish");
    }

    // Drain all three concurrently — a worker that gets nothing must not
    // block the others, so each drain has its own budget and may return
    // fewer than TOTAL.
    let budget = Duration::from_secs(3);
    let (g1, g2, g3) = tokio::join!(
        drain_acking(&mut s1, TOTAL, budget),
        drain_acking(&mut s2, TOTAL, budget),
        drain_acking(&mut s3, TOTAL, budget),
    );

    let total = g1.len() + g2.len() + g3.len();
    assert_eq!(
        total, TOTAL,
        "queue must deliver every message exactly once across the group; \
         got {}+{}+{}={total} (a duplicate means queue dedup broke, a \
         shortfall means messages were dropped)",
        g1.len(),
        g2.len(),
        g3.len()
    );

    // No payload may appear twice anywhere in the group.
    let mut all: Vec<Vec<u8>> = Vec::with_capacity(total);
    all.extend(g1.iter().cloned());
    all.extend(g2.iter().cloned());
    all.extend(g3.iter().cloned());
    let unique: std::collections::HashSet<_> = all.iter().collect();
    assert_eq!(
        unique.len(),
        TOTAL,
        "no message may be delivered to more than one queue member"
    );

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. A DIFFERENT queue name is a separate durable queue — its own cursor,
//    its own full copy of the stream. This is the fan-out axis.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn distinct_queue_names_are_independent_queues() {
    let mut server = TestServerBuilder::new().spawn().await;

    let setup = server.connect().await;
    let stream_id = create_stream(&setup, b"qs_indep").await;

    let a = server.connect().await;
    let b = server.connect().await;

    // Two DIFFERENT queue names on the same stream. Neither create may be
    // rejected: because queue_subscribe keeps name == group, the two never
    // collide on config.
    let mut sa = a
        .queue_subscribe(stream_id, b"billing", b"")
        .await
        .expect("queue 'billing' must be created");
    let mut sb = b
        .queue_subscribe(stream_id, b"audit", b"")
        .await
        .expect("queue 'audit' must be a separate, independent queue");

    const N: usize = 5;
    for i in 0..N {
        setup
            .publish_wait(stream_id, b"qs_indep.evt", Bytes::from(vec![i as u8]))
            .await
            .expect("publish");
    }

    let budget = Duration::from_secs(3);
    let (ga, gb) = tokio::join!(
        drain_acking(&mut sa, N, budget),
        drain_acking(&mut sb, N, budget),
    );

    assert_eq!(
        ga.len(),
        N,
        "queue 'billing' must receive its own full copy of all {N} messages"
    );
    assert_eq!(
        gb.len(),
        N,
        "queue 'audit' must receive its own full copy of all {N} messages — \
         a different queue name is an INDEPENDENT durable queue, not a \
         member of the same load-balancing group"
    );

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Durability: the queue survives a broker restart. Re-joining the same
//    queue name resumes from the persisted cursor — it does NOT replay the
//    messages that were already acked before the restart.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn queue_survives_broker_restart() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    // ── Session 1: join the queue, consume + ack 4 messages. ──────────────
    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;
        let stream_id = create_stream(&client, b"qs_durable").await;

        let mut sub = client
            .queue_subscribe(stream_id, b"workers", b"")
            .await
            .expect("queue join must succeed");

        for i in 0..4u8 {
            client
                .publish_wait(stream_id, b"qs_durable.job", Bytes::from(vec![b'a', i]))
                .await
                .expect("publish");
        }

        let got = drain_acking(&mut sub, 4, Duration::from_secs(3)).await;
        assert_eq!(got.len(), 4, "session 1 must consume and ack all 4");

        // Let the acks and the cursor sweep land before shutting down.
        tokio::time::sleep(Duration::from_millis(400)).await;
        server.shutdown().await;
    }

    // ── Session 2: same data_dir, same queue name. ────────────────────────
    {
        let mut server = TestServerBuilder::new().data_dir(dir_str).spawn().await;
        let client = server.connect().await;

        // The stream survived the restart, so this resolves the existing one.
        let stream_id = create_stream(&client, b"qs_durable").await;

        // Re-join the SAME queue: the durable consumer was persisted, so this
        // is an idempotent re-create, not a fresh queue.
        let mut sub = client
            .queue_subscribe(stream_id, b"workers", b"")
            .await
            .expect("re-joining the durable queue after restart must succeed");

        // Publish 2 NEW messages.
        for i in 0..2u8 {
            client
                .publish_wait(stream_id, b"qs_durable.job", Bytes::from(vec![b'n', i]))
                .await
                .expect("publish");
        }

        // Ask for more than we expect: if the queue wrongly replayed the
        // pre-restart backlog we would see the 'a' payloads too.
        let got = drain_acking(&mut sub, 6, Duration::from_secs(2)).await;

        assert!(
            got.iter().all(|p| p.first() == Some(&b'n')),
            "the durable queue must NOT replay messages that were acked \
             before the restart; got payloads {got:?}"
        );
        assert_eq!(
            got.len(),
            2,
            "the durable queue must deliver exactly the 2 messages published \
             after the restart"
        );

        server.shutdown().await;
    }
}
