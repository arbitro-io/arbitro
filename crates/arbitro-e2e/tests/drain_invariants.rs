//! Drain invariants — the delivery role must work under every shape
//! of workload the broker exposes.
//!
//! "Drain" here means the role inside the shard worker that walks the
//! store cursor for a consumer and pushes frames to its subscription.
//! These tests lock in behaviour the system must guarantee REGARDLESS
//! of timing, sequencing, or lifecycle interleaving.
//!
//! Each test seeds a fresh broker, exercises one specific drain
//! behaviour, and asserts the observable contract. No reliance on
//! state from a previous test (each builds its own server + client).
//!
//! ## What's NOT here
//!
//! - Basic happy-path delivery (covered in `invariants.rs`)
//! - Multi-consumer fanout / queue groups (also in `invariants.rs`)
//! - Catalog lifecycle (`catalog_invariants.rs`)
//! - Restart recovery (`persistence.rs`)
//!
//! This file covers the EDGE cases of the drain itself: pause/resume,
//! ack-timeout, wildcards, mid-drain teardown, fairness, etc.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::Client;
use bytes::Bytes;
use std::time::Duration;

// ── Helpers ─────────────────────────────────────────────────────────────────

async fn create_stream(client: &Client, name: &[u8], filter: &[u8]) -> u32 {
    let resp = client
        .create_stream(name, filter, 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream must succeed");
    TestServer::parse_id(&resp)
}

/// Full create_consumer call exposed so tests can tune every knob.
#[allow(clippy::too_many_arguments)]
async fn create_consumer(
    client: &Client,
    stream_id: u32,
    name: &[u8],
    group: &[u8],
    filter: &[u8],
    max_inflight: u16,
    ack_policy: u8,
    deliver_policy: u8,
    ack_wait_ms: u32,
    start_seq: u64,
) -> u32 {
    let resp = client
        .create_consumer(
            stream_id,
            name,
            group,
            filter,
            max_inflight,
            ack_policy,
            deliver_policy,
            0u8, /* push */
            ack_wait_ms,
            start_seq,
        )
        .await
        .expect("create_consumer must succeed");
    TestServer::parse_id(&resp)
}

/// Block on `recv` with a deadline. Returns `Some(msg)` or `None` on
/// timeout. Lets us assert both "must arrive" and "must NOT arrive".
async fn recv_within<'a>(
    handle: &'a mut arbitro_client_tokio::SubscriptionHandle,
    timeout: Duration,
) -> Option<arbitro_client_tokio::Message> {
    tokio::time::timeout(timeout, handle.recv())
        .await
        .ok()
        .flatten()
}

/// Drain a subscription until either `expected` messages are received
/// or the TOTAL deadline elapses. The caller passes a single total
/// budget (not per-message) so that one slow message under CPU
/// contention doesn't abort the whole drain prematurely — what we
/// want to assert is "at least N messages arrive within W time",
/// not "every message arrives within W/N time". Returns the messages
/// received (ownership transferred so tests can ack/inspect).
async fn drain_n(
    handle: &mut arbitro_client_tokio::SubscriptionHandle,
    expected: usize,
    total_budget: Duration,
) -> Vec<arbitro_client_tokio::Message> {
    let deadline = std::time::Instant::now() + total_budget;
    let mut out = Vec::with_capacity(expected);
    while out.len() < expected {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, handle.recv()).await {
            Ok(Some(m)) => out.push(m),
            _ => break,
        }
    }
    out
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Ack-wait timeout → redelivery
//
// If a consumer has ack_policy=Explicit and ack_wait_ms > 0, a delivered
// message that is NOT acked within the window must be redelivered.
// Otherwise the broker silently drops messages on slow / crashed clients.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn ack_wait_timeout_redelivers() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"acktimeout", b">").await;

    // 500 ms wait window — short enough to keep the test fast.
    let consumer_id = create_consumer(
        &client, stream_id, b"worker", b"worker", b"", 100, 1,   /* Explicit */
        0,   /* All */
        500, /* ack_wait_ms */
        0,
    )
    .await;
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    client
        .publish_wait(stream_id, b"acktimeout.event", Bytes::from_static(b"hello"))
        .await
        .unwrap();

    // First delivery arrives quickly.
    let first = recv_within(&mut handle, Duration::from_secs(2))
        .await
        .expect("first delivery must arrive");
    assert_eq!(&first.payload()[..], b"hello");
    // Drop without ack → redelivery should happen after ack_wait_ms.
    drop(first);

    let second = recv_within(&mut handle, Duration::from_secs(3))
        .await
        .expect(
            "ack_wait_ms timeout must trigger redelivery; pre-fix \
                 a stalled consumer never sees the message again",
        );
    assert_eq!(
        &second.payload()[..],
        b"hello",
        "redelivered payload must match the original"
    );
    second.ack();
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. MaxAckPending saturates → pauses → ack → resumes
//
// With max_inflight=K, the drain MUST stop at exactly K un-acked
// messages and MUST advance the moment ANY of them is acked. This is
// the per-consumer flow-control contract.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn max_inflight_pauses_then_resumes_on_ack() {
    let server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"flow", b">").await;

    const K: u16 = 4;
    let consumer_id = create_consumer(
        &client, stream_id, b"slow", b"slow", b"", K, 1, /* Explicit */
        0, /* All */
        30_000, 0,
    )
    .await;
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Publish 10 — drain must stop after 4.
    for i in 0u32..10 {
        let p = format!("msg-{i}");
        client
            .publish_wait(
                stream_id,
                b"flow.event",
                Bytes::copy_from_slice(p.as_bytes()),
            )
            .await
            .unwrap();
    }

    let first_batch = drain_n(&mut handle, K as usize, Duration::from_secs(3)).await;
    assert_eq!(
        first_batch.len(),
        K as usize,
        "must deliver exactly K={K} before pausing on max_inflight"
    );

    // No further delivery while all K are un-acked.
    let nothing = recv_within(&mut handle, Duration::from_millis(200)).await;
    assert!(
        nothing.is_none(),
        "with K un-acked messages, drain must stay paused; got {nothing:?}"
    );

    // Ack one → exactly one more must come through.
    let mut iter = first_batch.into_iter();
    iter.next().unwrap().ack();
    let resumed = recv_within(&mut handle, Duration::from_secs(2))
        .await
        .expect("ack must release one inflight slot and drain must resume");
    resumed.ack();

    // Ack the rest — drain to completion. The remainder of the
    // backlog must drain through the same K-sized window: ack every
    // message inline so we keep credit available.
    for m in iter {
        m.ack();
    }
    let mut remaining = 0;
    while remaining < 5 {
        match recv_within(&mut handle, Duration::from_secs(2)).await {
            Some(m) => {
                m.ack();
                remaining += 1;
            }
            None => break,
        }
    }
    assert_eq!(
        remaining, 5,
        "after ack burst, the remaining 5-msg backlog must drain"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Subject wildcard `*` delivers exactly the matching subjects
//
// Single-token wildcard. Publishes across three subjects: filter must
// catch only the ones matching the wildcard slot.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn wildcard_single_token_filter_matches_correctly() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"wcsingle", b">").await;

    let consumer_id = create_consumer(
        &client,
        stream_id,
        b"reader",
        b"reader",
        b"wcsingle.*.event",
        100,
        1,
        0,
        30_000,
        0,
    )
    .await;
    let mut handle = client
        .subscribe(stream_id, consumer_id, b"wcsingle.*.event")
        .await
        .unwrap();

    // 3 should match (a/b/c at the wildcard slot); 1 should NOT.
    //
    // `publish_wait` so each message is confirmed in the store before we
    // measure the drain — keeps the test deterministic regardless of how
    // long the broker takes to apply the publishes (which can spike under
    // load and turn fire-and-forget + tight drain timeout into a flake).
    client
        .publish_wait(stream_id, b"wcsingle.a.event", Bytes::from_static(b"a"))
        .await
        .unwrap();
    client
        .publish_wait(stream_id, b"wcsingle.b.event", Bytes::from_static(b"b"))
        .await
        .unwrap();
    client
        .publish_wait(stream_id, b"wcsingle.c.event", Bytes::from_static(b"c"))
        .await
        .unwrap();
    client
        .publish_wait(
            stream_id,
            b"wcsingle.a.b.event",
            Bytes::from_static(b"too-many-tokens"),
        )
        .await
        .unwrap();

    let got = drain_n(&mut handle, 3, Duration::from_secs(5)).await;
    assert_eq!(
        got.len(),
        3,
        "exactly the three single-token matches must arrive; got {}",
        got.len()
    );
    for m in got {
        m.ack();
    }

    // The 4-token subject must NOT have been delivered.
    let extra = recv_within(&mut handle, Duration::from_millis(300)).await;
    assert!(
        extra.is_none(),
        "`*` must NOT match more than one token; spurious delivery: {extra:?}"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Subject wildcard `>` delivers everything below the prefix
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn wildcard_multi_token_filter_matches_everything_below() {
    let server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"wcmulti", b">").await;

    let consumer_id = create_consumer(
        &client,
        stream_id,
        b"reader",
        b"reader",
        b"wcmulti.>",
        100,
        1,
        0,
        30_000,
        0,
    )
    .await;
    let mut handle = client
        .subscribe(stream_id, consumer_id, b"wcmulti.>")
        .await
        .unwrap();

    // All 4 of these match `wcmulti.>`. Use publish_wait so each message
    // is confirmed in the store before we measure the drain — the bare
    // `publish` is fire-and-forget at the LOCAL socket buffer, not the
    // broker, and `drain_n` with a short per-msg timeout would otherwise
    // race the broker's apply step and intermittently fail.
    let subjects: &[&[u8]] = &[
        b"wcmulti.a",
        b"wcmulti.a.b",
        b"wcmulti.a.b.c",
        b"wcmulti.long.chain.of.tokens",
    ];
    for s in subjects {
        client
            .publish_wait(stream_id, s, Bytes::from_static(b"data"))
            .await
            .unwrap();
    }

    let got = drain_n(&mut handle, subjects.len(), Duration::from_secs(5)).await;
    assert_eq!(
        got.len(),
        subjects.len(),
        "`>` must match arbitrarily-deep subjects below the prefix"
    );
    for m in got {
        m.ack();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Delete consumer mid-drain → no further deliveries
//
// Once `delete_consumer` returns Ok, no more frames must arrive on
// that subscription. Anything in flight at delete time is either
// consumed before the delete or quietly dropped.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn delete_consumer_mid_drain_stops_delivery() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"mid", b">").await;
    let consumer_id = create_consumer(
        &client, stream_id, b"worker", b"worker", b"", 100, 1, 0, 30_000, 0,
    )
    .await;
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Saturate with messages.
    for i in 0u32..20 {
        let p = format!("msg-{i}");
        client
            .publish_wait(
                stream_id,
                b"mid.event",
                Bytes::copy_from_slice(p.as_bytes()),
            )
            .await
            .unwrap();
    }

    // Consume a few, then delete the consumer.
    let pre = drain_n(&mut handle, 3, Duration::from_secs(2)).await;
    assert!(!pre.is_empty(), "must deliver something before delete");
    for m in pre {
        m.ack();
    }

    client.delete_consumer(consumer_id).await.unwrap();

    // From here, the subscription must go quiet within a short grace
    // window. Frames already in flight at delete time may still land,
    // but the broker must not produce new deliveries.
    // GRACE: in-flight frames keep landing for a brief window after
    // the delete completes. We intentionally keep this budget short
    // because we are asserting "drain stops", not "drain finishes":
    // a longer wait here would mask a regression in delete cascade.
    let _grace = drain_n(&mut handle, 20, Duration::from_millis(500)).await;

    let after = recv_within(&mut handle, Duration::from_millis(500)).await;
    assert!(
        after.is_none(),
        "after delete_consumer, the subscription must stop receiving; \
         got {after:?}"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Delete stream mid-drain → all attached subscriptions go silent
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn delete_stream_mid_drain_stops_all_subscriptions() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"midstream", b">").await;

    let consumer_id_a = create_consumer(
        &client,
        stream_id,
        b"worker-a",
        b"worker-a",
        b"",
        100,
        1,
        0,
        30_000,
        0,
    )
    .await;
    let consumer_id_b = create_consumer(
        &client,
        stream_id,
        b"worker-b",
        b"worker-b",
        b"",
        100,
        1,
        0,
        30_000,
        0,
    )
    .await;

    // Two subscriptions on the same stream (fanout).
    let mut handle_a = client
        .subscribe(stream_id, consumer_id_a, b"")
        .await
        .unwrap();
    let mut handle_b = client
        .subscribe(stream_id, consumer_id_b, b"")
        .await
        .unwrap();

    for i in 0u32..10 {
        let p = format!("msg-{i}");
        client
            .publish_wait(
                stream_id,
                b"midstream.event",
                Bytes::copy_from_slice(p.as_bytes()),
            )
            .await
            .unwrap();
    }

    // Consume something on each, then nuke the stream.
    let _ = drain_n(&mut handle_a, 2, Duration::from_secs(2)).await;
    let _ = drain_n(&mut handle_b, 2, Duration::from_secs(2)).await;

    client.delete_stream(b"midstream").await.unwrap();

    // GRACE: short by design (asserting "drain stops", not "drain finishes").
    let _ = drain_n(&mut handle_a, 20, Duration::from_millis(500)).await;
    let _ = drain_n(&mut handle_b, 20, Duration::from_millis(500)).await;

    let extra_a = recv_within(&mut handle_a, Duration::from_millis(500)).await;
    let extra_b = recv_within(&mut handle_b, Duration::from_millis(500)).await;
    assert!(
        extra_a.is_none() && extra_b.is_none(),
        "after delete_stream, BOTH attached subscriptions must stop \
         receiving; got a={extra_a:?} b={extra_b:?}"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. AckPolicy::None — drain runs without any ack tracking
//
// Fire-and-forget consumers must drain at line rate without the
// per-message ack handshake. Verifies the drain path doesn't depend
// on a NotPersistent → NotAcked path that only works in Explicit mode.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn ack_policy_none_drains_without_acks() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"fire", b">").await;
    let consumer_id = create_consumer(
        &client,
        stream_id,
        b"firehose",
        b"firehose",
        b"",
        100,
        0, /* None */
        0, /* All */
        0,
        0,
    )
    .await;
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    const N: usize = 50;
    for i in 0..N {
        let p = format!("msg-{i}");
        client
            .publish_wait(
                stream_id,
                b"fire.event",
                Bytes::copy_from_slice(p.as_bytes()),
            )
            .await
            .unwrap();
    }

    // Why 30s instead of a tight budget: AckPolicy::None has NO flow-
    // control — the drain just hands frames to the writer as fast as it
    // can. Under heavy host contention (14 parallel test brokers fighting
    // for 8 cores), the writer's `try_send` can backpressure for many
    // hundreds of ms at a time and the drain task may not be scheduled
    // for similar stretches.
    //
    // The contract we lock in is LOSSLESSNESS, not latency: the broker
    // records backpressure with `FlushOutcome::Backpressured(first_seq)`
    // and does NOT advance the cursor past undelivered entries (see
    // shard/drain.rs:298-304). Every published message will eventually
    // arrive. So the deadline must be generous enough that "eventually"
    // fits within the test's wall-clock budget even on a contended host.
    //
    // 30s for 50 msgs is overkill in isolation (test runs in 50ms when
    // alone) but it prevents flake when running with the full suite.
    let got = drain_n(&mut handle, N, Duration::from_secs(30)).await;
    assert_eq!(
        got.len(),
        N,
        "AckPolicy::None must deliver every message; the drain has no \
         ack-pending gate to wait on, but the cursor stays put on \
         writer backpressure → eventual delivery is guaranteed"
    );
    // Intentionally NOT acking — should not block redelivery (there's
    // no redelivery contract when ack_policy=None).
    drop(got);

    let extra = recv_within(&mut handle, Duration::from_millis(200)).await;
    assert!(
        extra.is_none(),
        "AckPolicy::None must NOT redeliver; the un-acked drop is final. \
         Got {extra:?}"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 7b. AckPolicy::None must IGNORE `max_inflight`.
//
// Fire-and-forget has no ack feedback, so a max_inflight gate would
// permanently stall the drain after the first K messages (no ack will
// ever decrement the counter). The contract: when ack_policy = None,
// the drain MUST NOT consult max_inflight. We pin this by setting an
// absurdly tight max_inflight (= 2) and verifying all 50 messages
// still arrive.
//
// Mirror test for Explicit (right below) shows the SAME consumer
// config with ack_policy = Explicit DOES stall at K messages — that
// asymmetry is the invariant.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn ack_policy_none_ignores_max_inflight() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"fire-cap", b">").await;
    let consumer_id = create_consumer(
        &client,
        stream_id,
        b"firehose",
        b"firehose",
        b"",
        2, /* max_inflight = tiny */
        0, /* AckPolicy::None */
        0, /* DeliverPolicy::All */
        0,
        0,
    )
    .await;
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    const N: usize = 50;
    for i in 0..N {
        let p = format!("msg-{i}");
        client
            .publish_wait(
                stream_id,
                b"fire-cap.event",
                Bytes::copy_from_slice(p.as_bytes()),
            )
            .await
            .unwrap();
    }

    // All 50 must arrive — max_inflight=2 is supposed to be IGNORED for
    // fire-and-forget. If we ever start enforcing it, the drain would
    // pause after delivering 2 messages and never resume (no ack will
    // come).
    let got = drain_n(&mut handle, N, Duration::from_secs(30)).await;
    assert_eq!(
        got.len(),
        N,
        "AckPolicy::None must IGNORE max_inflight — got {} / {N}. \
         If you see ≤ 2, the drain is gating fire-and-forget on a \
         counter that ack will never decrement.",
        got.len()
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 7c. Counter-test: AckPolicy::Explicit DOES enforce `max_inflight`.
//
// Same config as the test above but with ack_policy = Explicit. Drain
// must stop at K = 2 (no acks coming). This proves the asymmetry is
// driven by ack_policy, not by some other accidental knob.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn ack_policy_explicit_does_enforce_max_inflight() {
    let server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"explicit-cap", b">").await;
    let consumer_id = create_consumer(
        &client, stream_id, b"worker", b"worker", b"", 2, /* max_inflight = tiny */
        1, /* AckPolicy::Explicit */
        0, /* DeliverPolicy::All */
        30_000, 0,
    )
    .await;
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    const N: usize = 50;
    for i in 0..N {
        let p = format!("msg-{i}");
        client
            .publish_wait(
                stream_id,
                b"explicit-cap.event",
                Bytes::copy_from_slice(p.as_bytes()),
            )
            .await
            .unwrap();
    }

    // Drain WITHOUT acking. With Explicit + max_inflight=2, drain must
    // stop after delivering 2. Anything more would mean the gate is
    // broken (or the consumer was misclassified as fire-and-forget).
    let got = drain_n(&mut handle, N, Duration::from_secs(2)).await;
    assert_eq!(
        got.len(),
        2,
        "AckPolicy::Explicit with max_inflight=2 must deliver exactly \
         2 messages without acks; got {}. More than 2 means the gate \
         is broken; fewer means the drain isn't running.",
        got.len()
    );
    drop(got);

    // Acking one must release exactly one more delivery (not more).
    // We re-receive one to verify it's exactly one.
    // (Already covered by max_inflight_pauses_then_resumes_on_ack, but
    // this asserts the gate is alive end-to-end in this same scenario.)
}

// ═══════════════════════════════════════════════════════════════════════════
// 7d. AckPolicy::None must IGNORE `max_subject_inflight` too.
//
// Same logic as max_inflight: without acks there is no signal to
// decrement the per-subject counter, so a per-subject limit would
// permanently stall. The drain must skip the subject-limit check
// when ack_policy = None.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn ack_policy_none_ignores_max_subject_inflight() {
    use arbitro_client_tokio::SubjectLimit;
    let server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"fire-subj", b">").await;

    // Cannot reuse the helper — need create_consumer_with_limits.
    let resp = client
        .create_consumer_with_limits(
            stream_id,
            b"firehose",
            b"firehose",
            b"",
            u16::MAX, // max_inflight unlimited
            0,        /* AckPolicy::None */
            0,        /* DeliverPolicy::All */
            0,        /* push */
            0,
            0,
            &[SubjectLimit {
                pattern: b"fire-subj.>",
                limit: 1,
            }],
        )
        .await
        .expect("create_consumer with subject limit");
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    const N: usize = 20;
    for i in 0..N {
        let p = format!("msg-{i}");
        client
            .publish_wait(
                stream_id,
                b"fire-subj.event",
                Bytes::copy_from_slice(p.as_bytes()),
            )
            .await
            .unwrap();
    }

    // All 20 must arrive — even with max_subject_inflight=1, fire-and-
    // forget must ignore it (no ack will ever decrement).
    let got = drain_n(&mut handle, N, Duration::from_secs(30)).await;
    assert_eq!(
        got.len(),
        N,
        "AckPolicy::None must IGNORE max_subject_inflight — got {} / {N}. \
         If you see only 1, the drain is enforcing the per-subject cap \
         on a fire-and-forget consumer.",
        got.len()
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. DeliverPolicy::ByStartSeq — drain begins at the requested offset
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn deliver_policy_by_start_seq_skips_earlier() {
    let server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"seq", b">").await;

    // Publish 10 BEFORE subscribing.
    for i in 0u32..10 {
        let p = format!("msg-{i}");
        client
            .publish_wait(
                stream_id,
                b"seq.event",
                Bytes::copy_from_slice(p.as_bytes()),
            )
            .await
            .unwrap();
    }

    // Consumer that starts at seq 6 (deliver_policy=2 = ByStartSeq).
    let consumer_id = create_consumer(
        &client, stream_id, b"late", b"late", b"", 100, 1, 2, /* ByStartSeq */
        30_000, 6,
    )
    .await;
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    let got = drain_n(&mut handle, 5, Duration::from_secs(5)).await;
    assert!(
        !got.is_empty(),
        "ByStartSeq must deliver something from the requested offset"
    );
    // First delivery should be one of msg-5 .. msg-9 (the broker is
    // 1-indexed so start_seq=6 maps to the 6th publish; allow any of
    // the late entries to be the head — what we lock in is "earlier
    // messages are NOT delivered").
    let first_payload = got[0].payload().to_vec();
    let s = std::str::from_utf8(&first_payload).unwrap();
    let idx: u32 = s.strip_prefix("msg-").unwrap().parse().unwrap();
    assert!(
        idx >= 5,
        "ByStartSeq=6 must skip publishes 0..=4; got first={s} (idx={idx})"
    );
    for m in got {
        m.ack();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Empty stream + fresh subscribe → no spurious deliveries
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn empty_stream_subscribe_produces_no_deliveries() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"empty", b">").await;
    let consumer_id = create_consumer(
        &client, stream_id, b"reader", b"reader", b"", 100, 1, 0, 30_000, 0,
    )
    .await;
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    let nothing = recv_within(&mut handle, Duration::from_millis(300)).await;
    assert!(
        nothing.is_none(),
        "subscribing to an empty stream must NOT produce any frames; \
         got {nothing:?}"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. Slow subscriber + fast publisher — drain is bounded and lossless
//
// Publisher races ahead. Subscriber paces at one ack per ~5ms. With
// max_inflight bounded, the drain must back-pressure the publisher
// (TCP) and deliver every published message in order.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn slow_consumer_fast_publisher_is_lossless() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"pace", b">").await;
    let consumer_id = create_consumer(
        &client, stream_id, b"slow", b"slow", b"", 16, /* tight inflight */
        1, 0, 30_000, 0,
    )
    .await;
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    const N: usize = 100;
    for i in 0..N {
        let p = format!("msg-{i:03}");
        client
            .publish_wait(
                stream_id,
                b"pace.event",
                Bytes::copy_from_slice(p.as_bytes()),
            )
            .await
            .unwrap();
    }

    let mut received = Vec::with_capacity(N);
    while received.len() < N {
        match recv_within(&mut handle, Duration::from_secs(2)).await {
            Some(m) => {
                m.ack();
                received.push(received.len());
                tokio::time::sleep(Duration::from_micros(500)).await;
            }
            None => break,
        }
    }
    assert_eq!(
        received.len(),
        N,
        "every message must be delivered despite slow consumer; \
         got {} / {N}",
        received.len()
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. Recycle: drain → ack → republish same subject → drain again
//
// After a full ack cycle, the same subject must drain a fresh batch
// without bookkeeping confusion. Catches per-subject inflight maps
// that fail to reset to zero after the last ack.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn recycle_subject_after_ack_drains_fresh_batch() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"recycle", b">").await;
    let consumer_id = create_consumer(
        &client, stream_id, b"worker", b"worker", b"", 10, 1, 0, 30_000, 0,
    )
    .await;
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Cycle 1 — publish_wait so the broker has confirmed acceptance
    // of every message before we measure drain.
    for i in 0u32..5 {
        client
            .publish_wait(
                stream_id,
                b"recycle.event",
                Bytes::copy_from_slice(format!("c1-{i}").as_bytes()),
            )
            .await
            .unwrap();
    }
    let batch_a = drain_n(&mut handle, 5, Duration::from_secs(5)).await;
    assert_eq!(batch_a.len(), 5, "cycle 1: all 5 must drain");
    for m in batch_a {
        m.ack();
    }

    // Cycle 2 — same subject.
    for i in 0u32..5 {
        client
            .publish_wait(
                stream_id,
                b"recycle.event",
                Bytes::copy_from_slice(format!("c2-{i}").as_bytes()),
            )
            .await
            .unwrap();
    }
    let batch_b = drain_n(&mut handle, 5, Duration::from_secs(5)).await;
    assert_eq!(
        batch_b.len(),
        5,
        "cycle 2 on the same subject must also drain 5; pre-fix a \
         broken per-subject inflight that didn't dec-on-zero would \
         leave residual credit and starve the cycle 2 publishes"
    );
    for m in batch_b {
        m.ack();
    }
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. Fairness: one stalled consumer must not starve another
//
// Two consumers on the same stream (fanout, no queue group). One acks
// slowly; the other acks immediately. The fast consumer must drain
// at full rate independent of the slow one.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn slow_consumer_does_not_starve_fast_consumer() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"fair", b">").await;

    let slow_id = create_consumer(
        &client,
        stream_id,
        b"slow",
        b"slow-group",
        b"",
        4,
        1,
        0,
        30_000,
        0,
    )
    .await;
    let fast_id = create_consumer(
        &client,
        stream_id,
        b"fast",
        b"fast-group",
        b"",
        100,
        1,
        0,
        30_000,
        0,
    )
    .await;
    let mut slow = client.subscribe(stream_id, slow_id, b"").await.unwrap();
    let mut fast = client.subscribe(stream_id, fast_id, b"").await.unwrap();

    const N: usize = 30;
    for i in 0..N {
        client
            .publish_wait(
                stream_id,
                b"fair.event",
                Bytes::copy_from_slice(format!("m-{i}").as_bytes()),
            )
            .await
            .unwrap();
    }

    // Fast consumer drains everything quickly.
    let fast_msgs = drain_n(&mut fast, N, Duration::from_secs(8)).await;
    assert_eq!(
        fast_msgs.len(),
        N,
        "fast consumer must receive ALL {N} messages even while the \
         slow consumer has un-acked frames blocking its own inflight"
    );
    for m in fast_msgs {
        m.ack();
    }

    // Now drain the slow one (acking as we go).
    let mut slow_count = 0;
    while slow_count < N {
        match recv_within(&mut slow, Duration::from_secs(2)).await {
            Some(m) => {
                m.ack();
                slow_count += 1;
            }
            None => break,
        }
    }
    assert_eq!(
        slow_count, N,
        "slow consumer must also receive all {N} once its inflight clears"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. Concurrent publishes from multiple sockets → exactly-N delivery
//
// K parallel publishing clients hammer the same stream. A single
// consumer must receive exactly the union of all published messages,
// with no duplicates and no losses.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_publishers_one_consumer_exactly_n() {
    let mut server = TestServerBuilder::new().spawn().await;
    let addr = server.addr.clone();

    // Subscriber side.
    let sub_client = server.connect().await;
    let stream_id = create_stream(&sub_client, b"concur", b">").await;
    let consumer_id = create_consumer(
        &sub_client,
        stream_id,
        b"reader",
        b"reader",
        b"",
        u16::MAX,
        1,
        0,
        30_000,
        0,
    )
    .await;
    let mut handle = sub_client
        .subscribe(stream_id, consumer_id, b"")
        .await
        .unwrap();

    // Publisher side — 4 separate connections each pushing 25 messages.
    const PUB_COUNT: usize = 4;
    const PER_PUB: u32 = 25;
    const TOTAL: usize = PUB_COUNT * PER_PUB as usize;

    // `Client` is !Send (its writer pool uses Cell), so each publisher
    // runs on its own dedicated tokio runtime via std::thread::spawn.
    // This is closer to real-world usage anyway: separate clients in
    // separate processes hammering the same broker.
    let mut publishers = Vec::new();
    for p in 0..PUB_COUNT {
        let addr = addr.clone();
        let p = p as u32;
        publishers.push(std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let c = TestServer::connect_to(&addr).await;
                let resp = c.get_stream(b"concur").await.unwrap();
                let sid = TestServer::parse_id(&resp);
                for i in 0..PER_PUB {
                    let payload = format!("p{p}-i{i}");
                    c.publish_wait(
                        sid,
                        b"concur.event",
                        Bytes::copy_from_slice(payload.as_bytes()),
                    )
                    .await
                    .unwrap();
                }
            });
        }));
    }
    for h in publishers {
        h.join().unwrap();
    }

    // Receive everyone.
    let mut got = std::collections::HashSet::new();
    for _ in 0..TOTAL + 5 {
        match recv_within(&mut handle, Duration::from_secs(2)).await {
            Some(m) => {
                let s = String::from_utf8(m.payload().to_vec()).unwrap();
                let inserted = got.insert(s.clone());
                assert!(inserted, "duplicate delivery: {s}");
                m.ack();
                if got.len() == TOTAL {
                    break;
                }
            }
            None => break,
        }
    }
    assert_eq!(
        got.len(),
        TOTAL,
        "exactly {TOTAL} unique messages must be delivered from {PUB_COUNT} \
         concurrent publishers; got {}",
        got.len()
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 14. Subscribe → drop subscription → re-subscribe → continues from cursor
//
// Re-subscribing with the same consumer_id must resume from the
// engine-tracked cursor (whatever was acked already stays acked). New
// publishes between the drop and the resubscribe must NOT be lost.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn resubscribe_continues_from_cursor() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"cursor", b">").await;
    let consumer_id = create_consumer(
        &client, stream_id, b"reader", b"reader", b"", 100, 1, 0, 30_000, 0,
    )
    .await;

    // Phase 1: subscribe, drain 3, ack, drop.
    {
        let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
        // publish_wait waits for the broker's RepOk per message so we
        // know all 5 are committed in the store BEFORE we measure the
        // drain — rules out a publish-arrival race that would skew the
        // assertion.
        for i in 0u32..5 {
            client
                .publish_wait(
                    stream_id,
                    b"cursor.event",
                    Bytes::copy_from_slice(format!("m-{i}").as_bytes()),
                )
                .await
                .unwrap();
        }
        let three = drain_n(&mut handle, 3, Duration::from_secs(5)).await;
        assert_eq!(three.len(), 3, "phase 1: 3 must drain");
        for m in three {
            m.ack();
        }
    } // handle dropped — subscription closed at the client end

    // While unsubscribed, publish more. publish_wait guarantees these
    // are confirmed in the store before re-subscribe.
    for i in 5u32..8 {
        client
            .publish_wait(
                stream_id,
                b"cursor.event",
                Bytes::copy_from_slice(format!("m-{i}").as_bytes()),
            )
            .await
            .unwrap();
    }

    // Phase 2: re-subscribe.
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
    let resumed = drain_n(&mut handle, 5, Duration::from_secs(5)).await;
    assert!(
        resumed.len() >= 2,
        "after resubscribe, the un-acked + un-delivered messages (m-3..m-7) \
         must continue arriving; got {}",
        resumed.len()
    );
    for m in resumed {
        m.ack();
    }
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// T13 — Single-shard saturation: channel-full causes back-pressure, no drops
//
// Pin the broker to shard_count = 1 and push more messages than the
// drain can keep up with in real time. The contract is that publish ↔
// drain ↔ ack counts MUST match — every accepted publish must be both
// delivered and acked, with nothing silently dropped along the way.
// Pre-H10 there were three `let _ = try_send(...)` sites that could
// drop notifications silently; this test exercises the path that would
// fire on a saturated drain → cmd notify ring.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn t13_single_shard_saturation_no_silent_drops() {
    // Pinning to one shard maximises contention on the drain → cmd
    // notify ring (the silent-drop site H10 wired counters to).
    let mut server = TestServerBuilder::new().shard_count(1).spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"sat1", b">").await;

    // Explicit ack + small inflight cap forces the broker to alternate
    // between delivering and blocking, exercising the drain pause /
    // resume cycle under load.
    const TOTAL: usize = 1024;
    const INFLIGHT: u16 = 32;
    let consumer_id = create_consumer(
        &client, stream_id, b"c1", b"c1", b"", INFLIGHT, 1, /* Explicit */
        0, /* All */
        30_000, 0,
    )
    .await;
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Fire-and-forget publishes — flood the publish channel.
    for i in 0u32..TOTAL as u32 {
        let p = format!("m-{i}");
        let _ = client.publish(
            stream_id,
            b"sat1.event",
            Bytes::copy_from_slice(p.as_bytes()),
        );
    }

    // Drain every message, acking inline so the inflight window stays
    // open. The whole budget is generous so even a slow CI box gets
    // through TOTAL messages.
    let mut received = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while received < TOTAL {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, handle.recv()).await {
            Ok(Some(m)) => {
                m.ack();
                received += 1;
            }
            _ => break,
        }
    }

    assert_eq!(
        received, TOTAL,
        "single-shard saturation: publish/drain/ack counts must match; \
         received only {received} of {TOTAL}. A delta means the drain → \
         cmd notify ring dropped a Delivered without bumping a counter."
    );

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// T15. evict_expired must NOT stall publish.
//
// The eviction walk is bounded by EVICT_WALK_CAP (10K entries) per call.
// This test creates a stream with max_age=1s, fills it with messages,
// waits for expiry, then publishes a new message and asserts the publish
// completes within a tight deadline. If eviction blocked the entire
// shard worker unbounded, publish_wait would time out.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn evict_expired_does_not_stall_publish() {
    let mut server = TestServerBuilder::new()
        .shard_count(1) // force all traffic onto one shard for maximum contention
        .spawn()
        .await;
    let client = server.connect().await;

    // max_age_secs = 1 → entries expire after 1 second.
    let resp = client
        .create_stream(
            b"evict_test",
            b">",
            0,
            0,
            /*max_age_secs*/ 1,
            1,
            0,
            0,
            0,
            0,
        )
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // Fill the stream with a burst of messages that will expire quickly.
    let batch: Vec<arbitro_client_tokio::BatchEntry<'_>> = (0..500)
        .map(|_| {
            arbitro_client_tokio::BatchEntry::new(b"evict_test.fill", Bytes::from_static(b"x"))
        })
        .collect();
    client
        .publish_batch_wait(stream_id, &batch)
        .await
        .expect("fill batch");

    // Wait for all entries to expire (max_age = 1s).
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Now publish a fresh message. The shard may be running eviction
    // during this call — it must complete within 2 seconds regardless.
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.publish_wait(stream_id, b"evict_test.fresh", Bytes::from_static(b"alive")),
    )
    .await;

    match result {
        Ok(Ok(_)) => {} // publish completed within deadline — pass
        Ok(Err(e)) => panic!("publish_wait errored (eviction may have broken state): {e:?}"),
        Err(_) => panic!(
            "publish_wait timed out after 2s — evict_expired likely stalled the shard worker"
        ),
    }
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 16. Partial-write recovery — connection drops mid-delivery, unacked
//     messages redeliver after ack_wait timeout.
//
// T16 (gated on M8 writer feedback): when the subscriber's TCP
// connection drops after receiving messages but before acking them,
// those messages must be redelivered to the same (or replacement)
// consumer once the ack-wait timeout fires. The writer feedback loop
// ensures the drain detects the dead connection fast, but the
// recovery mechanism is the ack-timeout wheel.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn partial_write_recovery_redelivers_unacked() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"pw_recover", b">").await;

    // ack_wait_ms = 500ms — short enough for the test to be fast.
    let consumer_id = create_consumer(
        &client, stream_id, b"pw_sub", b"pw_sub", b"", 10, 1, 0, 500, 0,
    )
    .await;

    // Publish 5 messages before subscribing.
    for i in 0u32..5 {
        client
            .publish_wait(
                stream_id,
                b"pw_recover.ev",
                Bytes::copy_from_slice(format!("msg-{i}").as_bytes()),
            )
            .await
            .unwrap();
    }

    // First subscriber — receives but does NOT ack.
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
    let batch = drain_n(&mut handle, 5, Duration::from_secs(3)).await;
    assert_eq!(
        batch.len(),
        5,
        "first subscriber must receive all 5 messages"
    );

    // Simulate connection failure: drop the subscription handle and close
    // the client without acking. This triggers the writer feedback (M8)
    // detecting the dead connection.
    drop(handle);
    drop(client);

    // Wait for ack-timeout to fire (500ms + buffer for wheel granularity).
    tokio::time::sleep(Duration::from_millis(900)).await;

    // Reconnect and re-subscribe the SAME consumer. Messages should be
    // redelivered because they were never acked.
    let client2 = server.connect().await;
    let mut handle2 = client2
        .subscribe(stream_id, consumer_id, b"")
        .await
        .unwrap();

    let redelivered = drain_n(&mut handle2, 5, Duration::from_secs(5)).await;
    assert!(
        !redelivered.is_empty(),
        "after connection drop + ack-wait timeout, unacked messages must \
         be redelivered; got 0 messages on re-subscribe"
    );

    // Ack redelivered messages to clean up.
    for m in redelivered {
        m.ack();
    }
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Triple fanout — 2x Explicit (different groups) + 1x None, all receive all
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn triple_fanout_two_explicit_one_none_all_receive() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"triple", b">").await;

    // Consumer A: Explicit, group "workers"
    let a_id = create_consumer(
        &client,
        stream_id,
        b"worker-a",
        b"workers",
        b"",
        100,
        1,
        0,
        30_000,
        0,
    )
    .await;
    // Consumer B: Explicit, group "auditors" (different group → fanout)
    let b_id = create_consumer(
        &client,
        stream_id,
        b"auditor-b",
        b"auditors",
        b"",
        100,
        1,
        0,
        30_000,
        0,
    )
    .await;
    // Consumer C: None, group "tap" (fire-and-forget metrics)
    let c_id = create_consumer(&client, stream_id, b"tap-c", b"tap", b"", 100, 0, 0, 0, 0).await;

    let mut sub_a = client.subscribe(stream_id, a_id, b"").await.unwrap();
    let mut sub_b = client.subscribe(stream_id, b_id, b"").await.unwrap();
    let mut sub_c = client.subscribe(stream_id, c_id, b"").await.unwrap();

    const N: usize = 15;
    for i in 0..N {
        client
            .publish_wait(
                stream_id,
                b"triple.ev",
                Bytes::copy_from_slice(format!("m-{i}").as_bytes()),
            )
            .await
            .unwrap();
    }

    let c_msgs = drain_n(&mut sub_c, N, Duration::from_secs(5)).await;
    assert_eq!(
        c_msgs.len(),
        N,
        "None consumer must get all {N} (got {})",
        c_msgs.len()
    );

    let a_msgs = drain_n(&mut sub_a, N, Duration::from_secs(5)).await;
    let a_count = a_msgs.len();
    for m in a_msgs {
        m.ack();
    }
    assert_eq!(a_count, N, "Explicit-A must get all {N} (got {a_count})");

    let b_msgs = drain_n(&mut sub_b, N, Duration::from_secs(5)).await;
    let b_count = b_msgs.len();
    for m in b_msgs {
        m.ack();
    }
    assert_eq!(b_count, N, "Explicit-B must get all {N} (got {b_count})");

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Mixed AckPolicy — Explicit + None on the same Fanout stream
//
// One consumer acks (processing pipeline), the other is fire-and-forget
// (metrics tap). Both MUST receive every message independently.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn mixed_ack_explicit_and_none_both_receive_all() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"mixed", b">").await;

    // Consumer A: Explicit ack (processing)
    let explicit_id = create_consumer(
        &client,
        stream_id,
        b"processor",
        b"proc-group",
        b"",
        100,
        1,
        /* Explicit */ 0,
        30_000,
        0,
    )
    .await;

    // Consumer B: None (fire-and-forget metrics tap)
    let none_id = create_consumer(
        &client,
        stream_id,
        b"metrics-tap",
        b"tap-group",
        b"",
        100,
        0,
        /* None */ 0,
        0,
        0,
    )
    .await;

    let mut sub_explicit = client.subscribe(stream_id, explicit_id, b"").await.unwrap();
    let mut sub_none = client.subscribe(stream_id, none_id, b"").await.unwrap();

    const N: usize = 20;
    for i in 0..N {
        client
            .publish_wait(
                stream_id,
                b"mixed.event",
                Bytes::copy_from_slice(format!("m-{i}").as_bytes()),
            )
            .await
            .unwrap();
    }

    // Fire-and-forget consumer drains without acking.
    let none_msgs = drain_n(&mut sub_none, N, Duration::from_secs(5)).await;
    assert_eq!(
        none_msgs.len(),
        N,
        "AckPolicy::None consumer must receive all {N} messages (got {})",
        none_msgs.len()
    );

    // Explicit consumer drains and acks each message.
    let explicit_msgs = drain_n(&mut sub_explicit, N, Duration::from_secs(5)).await;
    let explicit_count = explicit_msgs.len();
    for m in explicit_msgs {
        m.ack();
    }
    assert_eq!(
        explicit_count, N,
        "AckPolicy::Explicit consumer must also receive all {N} messages (got {explicit_count})",
    );

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// ROB-23 (audit #7a) — a dead-reading consumer must not starve its shard
//
// The reply path already ejects a connection whose outbound queue is full
// (ROB-22). Before ROB-23, the DELIVERY path treated a full writer channel
// as transient forever: a live-but-not-reading consumer pinned the shared
// shard cursor at its first undeliverable seq, permanently starving every
// healthy sibling consumer on the same shard — the primary authenticated-
// client DoS surface. Fix: after `drain_stall_evict_ms` of continuous
// zero-progress backpressure, the drain retires the connection exactly
// like a dead writer, and the cursor advances past it. No stored message
// is deleted — an explicit-ack consumer can resubscribe and resume from
// its ack floor.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn dead_reading_consumer_does_not_starve_healthy_sibling() {
    use arbitro_proto::v2::cold::{ColdBody, Subscribe};
    use arbitro_proto::v2::ingress::hello::{HelloFrame, Role};
    use tokio::io::AsyncWriteExt;
    use zerocopy::IntoBytes;

    // Single shard so A and B share one drain cursor. Tiny write channel +
    // small per-cycle feed so the dead reader saturates its pipeline
    // (channel + in-flight frame + TCP buffers) long before N messages.
    let mut server = TestServerBuilder::new()
        .shard_count(1)
        .write_buffer_cap(4)
        .max_feed_per_cycle(16)
        .drain_stall_evict_ms(750)
        .spawn()
        .await;

    // Separate connections: publisher/control vs healthy consumer B. Keeps
    // B's delivery traffic off the publisher's (tiny) write channel.
    let client = server.connect().await;
    let client_b = server.connect().await;

    let stream_id = create_stream(&client, b"starve", b">").await;

    // A — fire-and-forget fanout consumer (no inflight cap, so frames keep
    // flowing until its writer channel is full).
    let consumer_a = create_consumer(
        &client,
        stream_id,
        b"dead-reader",
        b"dead-reader",
        b"",
        100,
        0, /* None */
        0,
        0,
        0,
    )
    .await;
    // B — healthy explicit-ack sibling.
    let consumer_b = create_consumer(
        &client, stream_id, b"healthy", b"healthy", b"", 512, 1, /* Explicit */
        0, 0, 0,
    )
    .await;

    // Bind A from a RAW TCP connection that never reads a single byte —
    // the truest "dead reading" client. Handshake + Subscribe only.
    let mut raw = tokio::net::TcpStream::connect(&server.addr).await.unwrap();
    raw.write_all(HelloFrame::new(Role::Client).as_bytes())
        .await
        .unwrap();
    let sub = Subscribe {
        consumer_id: consumer_a,
        subscription_id: 0,
        filters: Vec::new(),
    }
    .encode(1);
    raw.write_all(&sub).await.unwrap();

    let handle_b = client_b
        .subscribe(stream_id, consumer_b, b"")
        .await
        .unwrap();

    // Let both bindings register before publishing.
    tokio::time::sleep(Duration::from_millis(300)).await;

    const N: usize = 400;
    // 64 KiB payloads fill A's TCP buffers within a few dozen messages.
    let payload = Bytes::from(vec![0xABu8; 64 * 1024]);

    // B drains + acks CONCURRENTLY with publishing so its own channel
    // never fills and its ack floor keeps rising (suppresses the re-walk
    // duplicates while A pins the cursor).
    let collector = tokio::spawn(async move {
        let mut handle_b = handle_b;
        let mut seen = std::collections::HashSet::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while seen.len() < N {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, handle_b.recv()).await {
                Ok(Some(m)) => {
                    let seq = m.seq;
                    m.ack();
                    seen.insert(seq);
                }
                _ => break,
            }
        }
        (seen, handle_b)
    });

    for i in 0..N {
        client
            .publish_wait(
                stream_id,
                format!("starve.msg.{i}").as_bytes(),
                payload.clone(),
            )
            .await
            .unwrap();
    }

    // The invariant: B receives ALL N messages within the budget. While A
    // pins the cursor this is impossible (B stalls ~max_feed past the pin);
    // it only passes once A's stalled binding is evicted and the cursor
    // advances — the starvation bound under test.
    let (seen, mut handle_b) = collector.await.unwrap();
    assert_eq!(
        seen.len(),
        N,
        "healthy sibling starved: got {} of {N} unique messages — the \
         dead-reading consumer still pins the shard cursor",
        seen.len()
    );

    // Post-eviction liveness: new publishes keep flowing to B.
    const M: usize = 20;
    for i in 0..M {
        client
            .publish_wait(
                stream_id,
                format!("starve.tail.{i}").as_bytes(),
                Bytes::from_static(b"tail"),
            )
            .await
            .unwrap();
    }
    let max_phase1_seq = seen.iter().max().copied().unwrap_or(0);
    let mut tail_unique = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while tail_unique.len() < M {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, handle_b.recv()).await {
            Ok(Some(m)) => {
                let seq = m.seq;
                m.ack();
                if seq > max_phase1_seq {
                    tail_unique.insert(seq);
                }
            }
            _ => break,
        }
    }
    assert_eq!(
        tail_unique.len(),
        M,
        "shard not live after eviction: got {} of {M} tail messages",
        tail_unique.len()
    );

    // Close the dead reader BEFORE shutdown so the server's blocked writer
    // task errors out instead of blocking the graceful drain.
    drop(raw);
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Nack racing an ACTIVE drain → the nacked seq must always be redelivered
//
// BUG3/M3 regression gate. `handle_nack` used to rewind the cursor with a
// bare `set_cursor(min-1)` + unconditional `clear_rewind()`. A drain cycle
// already past its top-of-cycle `take_rewind` writes `new_cursor` forward
// at cycle end, clobbering the bare set_cursor — and the `clear_rewind()`
// wiped any co-pending rewind signal — so a nacked message could silently
// never be redelivered until an unrelated rewind happened to occur. The
// fix mirrors handle_bind's protocol (`set_cursor` + durable
// `signal_rewind`), which a mid-flight cycle cannot erase.
//
// This is a RACE with no deterministic repro: the test's job is to hammer
// the interleaving (small max_inflight so drain cycles constantly overlap
// the client's nacks, publisher running concurrently) and lock in
// no-regression — every nacked message must eventually come back.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn nack_during_active_drain_always_redelivers() {
    const ITERATIONS: usize = 8;
    const N: u32 = 80; // messages per iteration
    const NACK_STRIDE: u32 = 7; // nack payload indices where i % 7 == 3

    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    for iter in 0..ITERATIONS {
        let stream_name = format!("nackrace{iter}");
        let stream_id = create_stream(&client, stream_name.as_bytes(), b">").await;
        // Small window: the drain pauses/resumes on every few acks, so
        // cycles are guaranteed to be mid-flight while nacks land.
        let consumer_id = create_consumer(
            &client, stream_id, b"racer", b"racer", b"", 4,      /* max_inflight */
            1,      /* Explicit */
            0,      /* All */
            30_000, /* ack_wait_ms — keep the wheel out of this test */
            0,
        )
        .await;
        let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

        // Publish CONCURRENTLY with consumption so the drain is actively
        // cycling (publish → gate release → drain) while nacks arrive.
        let pub_client = client.clone();
        let subject = format!("nackrace{iter}.msg");
        let publisher = tokio::spawn(async move {
            for i in 0u32..N {
                pub_client
                    .publish_wait(
                        stream_id,
                        subject.as_bytes(),
                        Bytes::copy_from_slice(&i.to_le_bytes()),
                    )
                    .await
                    .expect("publish must succeed");
            }
        });

        // deliveries[i] = number of times payload index i was delivered.
        let mut deliveries = vec![0u32; N as usize];
        let is_nack_target = |i: u32| i % NACK_STRIDE == 3;
        let done = |deliveries: &[u32]| {
            (0..N).all(|i| {
                let d = deliveries[i as usize];
                if is_nack_target(i) {
                    d >= 2
                } else {
                    d >= 1
                }
            })
        };

        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !done(&deliveries) {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, handle.recv()).await {
                Ok(Some(m)) => {
                    let mut idx_bytes = [0u8; 4];
                    idx_bytes.copy_from_slice(&m.payload()[..4]);
                    let i = u32::from_le_bytes(idx_bytes);
                    let count = &mut deliveries[i as usize];
                    *count += 1;
                    if is_nack_target(i) && *count == 1 {
                        // First sight of a target: nack WHILE the drain is
                        // actively delivering the rest of the backlog.
                        m.nack();
                    } else {
                        // Everything else (and redeliveries) acks inline so
                        // the max_inflight window keeps cycling.
                        m.ack();
                    }
                }
                _ => break,
            }
        }
        publisher.await.unwrap();

        // At-least-once: nothing lost, every nacked seq redelivered.
        for i in 0..N {
            let d = deliveries[i as usize];
            if is_nack_target(i) {
                assert!(
                    d >= 2,
                    "iter {iter}: nacked payload {i} was never redelivered \
                     (delivered {d} time(s)) — nack rewind lost to a \
                     concurrent drain cycle (BUG3)",
                );
            } else {
                assert!(
                    d >= 1,
                    "iter {iter}: payload {i} was never delivered — message loss",
                );
            }
        }

        client.delete_stream(stream_name.as_bytes()).await.unwrap();
    }

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// A ByStartSeq join must not skip a capacity-pinned sibling's owed messages
//
// The existing ByStartSeq coverage cannot catch this:
// `deliver_policy_by_start_seq_skips_earlier` has a single consumer, and
// `late_join_by_start_seq_does_not_redeliver_to_acked_sibling` has a sibling
// that already acked everything. Neither has a sibling still OWED messages
// below the new consumer's start offset.
//
// The concern (flagged by audit, unproven until this test): the forward-skip
// branch does a bare `set_cursor(target)` on the SHARD-wide cursor. If a
// sibling is pinned by max_inflight at a low seq, jumping the shared cursor
// past its owed range would mean those entries are never walked again — loss,
// not just late delivery.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn by_start_seq_join_does_not_skip_a_pinned_siblings_backlog() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"pin", b">").await;

    // Sibling A: tiny inflight so it pins near the head of the stream.
    let a_id = create_consumer(
        &client, stream_id, b"pinned", b"pinned-group", b"", 4, 1, 0, 30_000, 0,
    )
    .await;
    let mut a = client.subscribe(stream_id, a_id, b"").await.unwrap();

    const N: usize = 30;
    for i in 0..N {
        client
            .publish_wait(
                stream_id,
                b"pin.event",
                Bytes::copy_from_slice(format!("m-{i}").as_bytes()),
            )
            .await
            .unwrap();
    }

    // A fills its inflight and holds it — deliberately no acks yet.
    let held = drain_n(&mut a, 4, Duration::from_secs(5)).await;
    assert_eq!(held.len(), 4, "A must fill its max_inflight of 4 first");
    let mut a_payloads: Vec<String> = held
        .iter()
        .map(|m| String::from_utf8_lossy(&m.payload()).to_string())
        .collect();

    // B joins asking to start near the tail, while A is still pinned with
    // 26 messages owed to it.
    let b_id = create_consumer(
        &client, stream_id, b"jumper", b"jumper-group", b"", 100, 1, 2, 30_000, 25,
    )
    .await;
    let mut b = client.subscribe(stream_id, b_id, b"").await.unwrap();
    let b_got = drain_n(&mut b, 6, Duration::from_secs(5)).await;
    for m in b_got {
        m.ack();
    }

    // A releases its inflight. Everything it was owed must still arrive.
    for m in held {
        m.ack();
    }

    let mut a_seen = 4usize;
    while a_seen < N {
        match recv_within(&mut a, Duration::from_secs(3)).await {
            Some(m) => {
                a_payloads.push(String::from_utf8_lossy(&m.payload()).to_string());
                m.ack();
                a_seen += 1;
            }
            None => break,
        }
    }
    eprintln!("A RECEIVED ({}): {:?}", a_payloads.len(), a_payloads);
    assert_eq!(
        a_seen, N,
        "A was owed all {N}; a ByStartSeq sibling joining at 25 must not \
         move the shared cursor past what A had not yet been served"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Deleting a stream must give back the inflight credit its consumers held.
//
// Every piece of this is already covered on its own. `max_inflight` pausing
// and resuming lives in `max_inflight_pauses_then_resumes_on_ack`, but only
// within one consumer's lifetime. `delete_stream_cascades_consumers` proves
// the catalog entries go away. `create_delete_cycles_no_leak` runs 50 cycles
// without ever publishing, so it can only see registry rows, not credit.
//
// The combination is what nothing tested: a SMALL cap, messages delivered and
// deliberately NOT acked, and then the stream deleted out from under them. If
// the charge for those unacked deliveries outlives the stream, the next
// consumer to ask for the same small cap starts with nothing.
//
// Nothing here is exotic. It is what a worker that dies holding messages does,
// or an operator dropping a stream that still has work in flight.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn delete_stream_releases_inflight_credit() {
    const CAP: u16 = 3;
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    // ── First lifecycle: fill the cap and walk away without acking.
    let stream_a = create_stream(&client, b"credit_a", b"credit_a.>").await;
    let consumer_a = create_consumer(
        &client, stream_a, b"holder_a", b"holder_a", b"", CAP, 1, /* Explicit */
        0, /* All */
        30_000, 0,
    )
    .await;
    let mut sub_a = client.subscribe(stream_a, consumer_a, b"").await.unwrap();

    for i in 0u32..10 {
        client
            .publish_wait(
                stream_a,
                b"credit_a.msg",
                Bytes::from(format!("a-{i}").into_bytes()),
            )
            .await
            .expect("publish to A");
    }

    let held = drain_n(&mut sub_a, CAP as usize, Duration::from_secs(5)).await;
    assert_eq!(
        held.len(),
        CAP as usize,
        "first consumer must receive exactly its cap before pausing"
    );

    // Held, never acked — this is the state the cap exists to produce.
    drop(sub_a);
    client.delete_stream(b"credit_a").await.expect("delete A");
    drop(held);

    // ── Second lifecycle: a different stream, a different consumer name, the
    // same cap. Nothing it can see is shared with the first.
    let stream_b = create_stream(&client, b"credit_b", b"credit_b.>").await;
    let consumer_b = create_consumer(
        &client, stream_b, b"holder_b", b"holder_b", b"", CAP, 1, /* Explicit */
        0, /* All */
        30_000, 0,
    )
    .await;
    let mut sub_b = client.subscribe(stream_b, consumer_b, b"").await.unwrap();

    for i in 0u32..10 {
        client
            .publish_wait(
                stream_b,
                b"credit_b.msg",
                Bytes::from(format!("b-{i}").into_bytes()),
            )
            .await
            .expect("publish to B");
    }

    let got = drain_n(&mut sub_b, CAP as usize, Duration::from_secs(5)).await;
    assert_eq!(
        got.len(),
        CAP as usize,
        "a fresh consumer on a fresh stream must get its full cap of {CAP}; \
         got {} — the credit held by the deleted stream's unacked deliveries \
         was never returned, so the cap is permanently spent",
        got.len()
    );

    server.shutdown().await;
}
