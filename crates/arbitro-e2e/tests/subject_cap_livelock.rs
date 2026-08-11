//! Redelivery livelock: a per-subject cap that blocks a LOW seq pins the
//! shard cursor, and the frozen contiguous ack-floor cannot cover the
//! acked seqs ABOVE the pin — so every drain cycle re-walks the pinned
//! window and re-delivers messages the consumer already acked.
//!
//! Measured on master before the fix: 911,294 deliveries for 300
//! published messages. This test reproduces the same mechanism with a
//! bounded-deliveries assertion:
//!
//!   1. Publish CAP hot messages — delivered, held UNACKED (cap full,
//!      floor frozen at 0).
//!   2. Publish one more hot message — capped, skipped, cursor pinned
//!      just below it (`lowest_skipped`).
//!   3. Publish ~300 cold messages — delivered and acked, but every seq
//!      is above the frozen floor, so nothing shields them from the
//!      re-walk: each drain cycle re-delivers the whole cold range.
//!
//! The invariant under test: total deliveries stay bounded (each
//! published message delivered at most ~once while the cap is engaged),
//! and the capped hot message is still delivered — not lost — once the
//! held hot messages are acked and the cap frees.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::{AckPolicy, Client, ConsumerBuilder, DeliverMode};
use bytes::Bytes;
use std::collections::HashSet;
use std::time::Duration;

const STREAM: &[u8] = b"cap_livelock";
/// Per-subject cap on the hot subject.
const CAP: u32 = 2;
/// Cold messages published after the capped hot one.
const COLD: usize = 297;
/// Total published = CAP held hot + 1 capped hot + COLD cold.
const PUBLISHED: usize = CAP as usize + 1 + COLD;
/// Livelock detector: far beyond any legitimate delivery count, small
/// enough that the failing run bails out quickly instead of streaming
/// hundreds of thousands of frames.
const BLOWUP_BAIL: usize = PUBLISHED * 20;

async fn create_stream(client: &Client) -> u32 {
    let resp = client
        .create_stream(STREAM, b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream");
    TestServer::parse_id(&resp)
}

#[tokio::test(flavor = "multi_thread")]
async fn capped_low_seq_does_not_livelock_redelivery() {
    let mut server = TestServerBuilder::new().spawn().await;
    let admin = server.connect().await;
    let stream_id = create_stream(&admin).await;

    let consumer_id = ConsumerBuilder::new(b"livelock_tester")
        .group(b"")
        .filter(b">")
        .max_inflight(10_000)
        .ack_policy(AckPolicy::Explicit)
        .deliver_mode(DeliverMode::Fanout)
        // Long ack_wait so ack-timeout redelivery can never explain (or
        // legitimately produce) extra deliveries inside the test window.
        .ack_wait_ms(90_000)
        .max_subject_inflight(b"hot.>", CAP)
        .create(&admin, stream_id)
        .await
        .expect("create consumer");

    let client = server.connect().await;
    let mut sub = client
        .subscribe(stream_id, consumer_id, b"")
        .await
        .expect("subscribe");

    // CAP hot messages: delivered and held unacked → the cap fills and the
    // consumer's contiguous ack-floor freezes at 0.
    for i in 0..CAP {
        admin
            .publish_wait(stream_id, b"hot.msg", Bytes::from(format!("hot-{i}")))
            .await
            .expect("publish hot");
    }
    // One more hot message: blocked by the cap → skipped → cursor pinned
    // at the seq just below it.
    admin
        .publish_wait(stream_id, b"hot.msg", Bytes::from("hot-capped"))
        .await
        .expect("publish capped hot");
    // Cold messages: all above the pin, all acked on receipt. With the
    // frozen floor nothing marks them as done, so a buggy drain
    // re-delivers them on every cycle.
    for i in 0..COLD {
        admin
            .publish_wait(stream_id, b"cold.msg", Bytes::from(format!("cold-{i}")))
            .await
            .expect("publish cold");
    }

    // Receive: ack every cold message, hold the hot ones unacked. Count
    // every delivery (redeliveries included).
    let mut held_hot = Vec::new();
    let mut total = 0usize;
    let mut distinct: HashSet<Vec<u8>> = HashSet::new();
    loop {
        match tokio::time::timeout(Duration::from_millis(700), sub.recv()).await {
            Ok(Some(msg)) => {
                total += 1;
                distinct.insert(msg.data().to_vec());
                if msg.subject().starts_with(b"hot.") {
                    held_hot.push(msg); // keep unacked — cap stays engaged
                } else {
                    msg.ack();
                }
                if total > BLOWUP_BAIL {
                    break; // livelock confirmed — no point streaming more
                }
            }
            _ => break, // quiet — deliveries settled
        }
    }

    eprintln!(
        "published={PUBLISHED} cap={CAP} → deliveries={total} distinct={} held_hot={}",
        distinct.len(),
        held_hot.len()
    );

    assert!(
        total <= PUBLISHED * 2,
        "REDELIVERY LIVELOCK: {total} deliveries for {PUBLISHED} published messages \
         (bound: {}). A capped low seq pinned the drain cursor and the frozen \
         ack-floor let every cycle re-deliver the already-acked range above it.",
        PUBLISHED * 2
    );

    // While the cap is engaged, exactly CAP hot messages may be delivered
    // and the capped one must NOT have slipped through.
    assert_eq!(
        held_hot.len(),
        CAP as usize,
        "cap must hold exactly {CAP} hot messages while unacked"
    );

    // No-loss follow-through: ack the held hot messages → the cap frees →
    // the capped hot message must still arrive.
    for msg in held_hot.drain(..) {
        msg.ack();
    }
    let mut got_capped = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(500), sub.recv()).await {
            let is_capped = msg.data() == b"hot-capped".as_slice();
            msg.ack();
            if is_capped {
                got_capped = true;
                break;
            }
        }
    }
    assert!(
        got_capped,
        "the capped hot message must be delivered once the cap frees — it was lost"
    );

    drop(sub);
    server.shutdown().await;
}
