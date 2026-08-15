//! Drain-event ring overflow must not starve released messages.
//!
//! The drain's redelivery-suppression set (`ConsumerSubjects::suppressed`)
//! is fed on delivery by the drain itself and RELEASED by the command
//! thread via `DrainEvent::Ack { op: Released }` over a fixed-capacity
//! (2048) SPSC ring. A binding retirement (connection death) releases
//! EVERY pending entry of the binding in a single delta, pushed into the
//! ring before `gate.release()` — so with more than 2048 unacked
//! in-flight messages the ring overflows deterministically while the
//! drain is parked.
//!
//! Before the fix the overflow was silently dropped: the excess seqs
//! stayed suppressed forever (the contiguous ack-floor cannot rise past
//! an unacked seq, and resubscribe replays from the consumer cursor,
//! above them) — permanent starvation of `N - 2048` messages. The fix
//! retains overflowed events on a command-side retry queue
//! (`pending_drain_acks`, mirroring the H11 `pending_consumer_remove`
//! pattern) and re-signals the cursor rewind when they finally land.
//!
//! This test: deliver > 2048 messages unacked, kill the connection
//! abruptly (bulk retirement → guaranteed ring overflow), resubscribe
//! the same consumer, and require EVERY message to be redelivered.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::{AckPolicy, Client, ConsumerBuilder, DeliverMode};
use bytes::Bytes;
use std::collections::HashSet;
use std::time::Duration;

const STREAM: &[u8] = b"ring_overflow";
/// Must exceed `DRAIN_EVENT_CAP` (2048, `arbitro-server/src/shard/
/// drain_events.rs`) so the bulk retirement overflows the ring. 2600
/// leaves 552 events that MUST survive the overflow.
const N: usize = 2600;

async fn create_stream(client: &Client) -> u32 {
    let resp = client
        .create_stream(STREAM, b"ev.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream");
    TestServer::parse_id(&resp)
}

async fn drain_payloads(
    sub: &mut arbitro_client_tokio::SubscriptionHandle,
    expected: usize,
    total_budget: Duration,
    ack: bool,
) -> (usize, HashSet<Vec<u8>>) {
    let deadline = tokio::time::Instant::now() + total_budget;
    let mut total = 0usize;
    let mut distinct: HashSet<Vec<u8>> = HashSet::new();
    while distinct.len() < expected {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, sub.recv()).await {
            Ok(Some(msg)) => {
                total += 1;
                distinct.insert(msg.data().to_vec());
                if ack {
                    msg.ack();
                }
                // Held unacked otherwise — msg dropped without ack.
            }
            _ => break,
        }
    }
    (total, distinct)
}

#[tokio::test(flavor = "multi_thread")]
async fn ring_overflow_on_connection_death_does_not_starve_messages() {
    let mut server = TestServerBuilder::new().shard_count(1).spawn().await;
    let admin = server.connect().await;
    let stream_id = create_stream(&admin).await;

    // ack_wait far beyond the test window so ack-timeout auto-nack can
    // never drip-release the pendings in small batches — the ONLY release
    // path exercised here is the bulk retirement on connection death.
    let consumer_id = ConsumerBuilder::new(b"overflow_tester")
        .group(b"")
        .max_inflight(10_000)
        .ack_policy(AckPolicy::Explicit)
        .deliver_mode(DeliverMode::Fanout)
        .ack_wait_ms(90_000)
        .create(&admin, stream_id)
        .await
        .expect("create consumer");

    // Publish N messages (fire-and-forget + final wait as an ordering
    // fence — same connection, frames processed in order).
    for i in 0..N - 1 {
        admin
            .publish(stream_id, b"ev.msg", Bytes::from(format!("m-{i}")))
            .expect("publish");
    }
    admin
        .publish_wait(stream_id, b"ev.msg", Bytes::from(format!("m-{}", N - 1)))
        .await
        .expect("publish fence");

    // First subscriber: receive everything, ack NOTHING.
    let victim = server.connect().await;
    let mut sub = victim
        .subscribe(stream_id, consumer_id, b"")
        .await
        .expect("subscribe");
    let (_, first) = drain_payloads(&mut sub, N, Duration::from_secs(30), false).await;
    assert_eq!(
        first.len(),
        N,
        "precondition: all {N} messages must be delivered to the first subscriber"
    );

    // Let the drain go idle (parked) so the retirement delta hits an
    // empty, un-drained ring — deterministic overflow of N - 2048 events.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Abrupt connection death with N unacked in-flight: the server
    // retires the binding and releases all N pendings in one delta.
    drop(sub);
    drop(victim);

    // Give the server time to notice the dead connection and process the
    // retirement (this is where, pre-fix, 552 Released events died).
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Resubscribe the SAME consumer on a fresh connection. Every message
    // is unacked, so every message is owed again. The generous budget
    // covers the command-loop retry cadence for the queued events.
    let rescuer = server.connect().await;
    let mut sub2 = rescuer
        .subscribe(stream_id, consumer_id, b"")
        .await
        .expect("resubscribe");
    let (_, redelivered) = drain_payloads(&mut sub2, N, Duration::from_secs(30), true).await;

    let missing = N - redelivered.len();
    assert_eq!(
        redelivered.len(),
        N,
        "RING-OVERFLOW STARVATION: {missing} of {N} unacked messages were never \
         redelivered after connection death + resubscribe. Their `Released` \
         drain events overflowed the 2048-slot ring and were dropped, leaving \
         the seqs permanently suppressed for this consumer."
    );

    drop(sub2);
    server.shutdown().await;
}
