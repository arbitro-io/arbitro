//! Does `max_subject_inflight` hold when SEVERAL connections share ONE
//! consumer?
//!
//! The per-subject counter is per-CONSUMER (`consumer_subjects`, indexed by
//! `ConsumerId`), so the cap only means anything if it is enforced across
//! every connection bound to that consumer. Nothing tested that until now:
//! the `limits` bench gives each client its own stream and its own consumer
//! (`limits_stream_c{i}` / `isolation_tester_c{i}`), so its four "parallel
//! clients" share no counter at all and cannot contend for a cap.
//!
//! Here every connection subscribes to the SAME `consumer_id` on the SAME
//! stream — the only setup where the shared counter is actually exercised.
//! Both delivery modes are covered because the contract differs:
//!
//!   * Queue  — each message goes to exactly ONE member, so the cap bounds
//!     the total number of messages delivered.
//!   * Fanout — every member gets a copy, so the cap bounds the number of
//!     DISTINCT seqs released; copies of the same seq are not extra
//!     in-flight work against that subject.
//!
//! Both assert the same underlying invariant: the broker must never let more
//! than `CAP` messages be outstanding on one subject for one consumer, no
//! matter how the consumer's connections are arranged.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::{AckPolicy, Client, ConsumerBuilder, DeliverMode, SubscriptionHandle};
use bytes::Bytes;
use std::collections::HashSet;
use std::time::Duration;

const STREAM: &[u8] = b"subj_cap_shared";
/// Small enough that an off-by-N is unmistakable in the failure message.
const CAP: u32 = 10;
/// Published to ONE subject — well past the cap, so an unenforced cap shows
/// up as a large overshoot rather than a borderline one.
const PUBLISHED: u32 = 30;
const N_CONNS: usize = 3;
/// One subject for everything: all of it shares a single counter.
const SUBJECT: &[u8] = b"orders.premium.singleton";

async fn create_stream(client: &Client) -> u32 {
    let resp = client
        .create_stream(STREAM, b"orders.premium.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream");
    TestServer::parse_id(&resp)
}

/// Drain every subscription until they all go quiet. Returns
/// `(total_messages, distinct_seqs)` — deliberately NOT acking, so the cap
/// stays engaged for the whole window.
async fn collect_without_acking(
    subs: &mut [SubscriptionHandle],
    quiet_for: Duration,
) -> (usize, usize) {
    let mut total = 0usize;
    let mut seqs: HashSet<Vec<u8>> = HashSet::new();

    // Round-robin the subscriptions until a full sweep yields nothing.
    loop {
        let mut got_any = false;
        for sub in subs.iter_mut() {
            while let Ok(Some(msg)) = tokio::time::timeout(quiet_for, sub.recv()).await {
                total += 1;
                seqs.insert(msg.data().to_vec());
                got_any = true;
                // Do NOT ack — an ack would free the slot and let the next
                // message through, which is the behaviour under test.
            }
        }
        if !got_any {
            break;
        }
    }
    (total, seqs.len())
}

async fn run(mode: DeliverMode, group: &[u8]) -> (usize, usize) {
    let mut server = TestServerBuilder::new().spawn().await;

    let admin = server.connect().await;
    let stream_id = create_stream(&admin).await;

    // ONE consumer. Every connection below binds to this exact id, so they
    // all share its cursor, its pending set and its subject counters.
    let consumer_id = ConsumerBuilder::new(b"shared_cap_tester")
        .group(group)
        .filter(b">")
        .max_inflight(10_000)
        .ack_policy(AckPolicy::Explicit)
        .deliver_mode(mode)
        .ack_wait_ms(60_000)
        .max_subject_inflight(b"orders.premium.>", CAP)
        .create(&admin, stream_id)
        .await
        .expect("shared consumer");

    let mut clients = Vec::with_capacity(N_CONNS);
    let mut subs = Vec::with_capacity(N_CONNS);
    for _ in 0..N_CONNS {
        let c = server.connect().await;
        let s = c
            .subscribe(stream_id, consumer_id, b"")
            .await
            .expect("subscribe to the shared consumer");
        clients.push(c);
        subs.push(s);
    }

    for i in 0..PUBLISHED {
        admin
            .publish_wait(stream_id, SUBJECT, Bytes::from(format!("m-{i}")))
            .await
            .expect("publish");
    }

    let out = collect_without_acking(&mut subs, Duration::from_millis(400)).await;

    drop(subs);
    drop(clients);
    // Tears the broker down with it — nothing survives the test.
    server.shutdown().await;
    out
}

// ═══════════════════════════════════════════════════════════════════════════
// Queue: one message → one member. The cap bounds the message count.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn subject_cap_holds_across_connections_queue_mode() {
    let (total, distinct) = run(DeliverMode::Queue, b"shared_cap_tester").await;

    eprintln!(
        "QUEUE  — {N_CONNS} connections, ONE consumer, cap={CAP}, published={PUBLISHED} \
         → delivered total={total} distinct={distinct}"
    );

    assert!(
        total <= CAP as usize,
        "the per-subject cap is {CAP} but {total} messages were delivered across \
         {N_CONNS} connections sharing ONE consumer. The counter is per-consumer, \
         so spreading the consumer over more connections must not raise its cap — \
         otherwise the limit is per-connection in practice and a caller can defeat \
         it just by opening more sockets."
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Fanout: every member gets a copy. The cap bounds the DISTINCT seqs.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn subject_cap_holds_across_connections_fanout_mode() {
    let (total, distinct) = run(DeliverMode::Fanout, b"").await;

    eprintln!(
        "FANOUT — {N_CONNS} connections, ONE consumer, cap={CAP}, published={PUBLISHED} \
         → delivered total={total} distinct={distinct}"
    );

    assert!(
        distinct <= CAP as usize,
        "the per-subject cap is {CAP} but {distinct} DISTINCT messages were released \
         across {N_CONNS} fanout connections on ONE consumer (total deliveries \
         {total}). Copies of one message are expected in fanout; releasing more \
         distinct messages than the cap is not."
    );
}
