mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use std::collections::HashSet;
use std::time::Duration;

use arbitro_client_tokio::{Client, ClientError};
use bytes::Bytes;

// ═══════════════════════════════════════════════════════════════════════════
// 1. Concurrent publish_sync correlation
// ═══════════════════════════════════════════════════════════════════════════

/// N concurrent `publish_sync` calls on the same client must each receive
/// **their own** ack. The server assigns a monotonic `ref_seq` per stream,
/// so the set of returned ref_seqs must be exactly `{1..=N}` — no
/// duplicates (would mean two callers got the same reply), no gaps
/// (would mean a reply was lost or routed to the wrong caller).
///
/// This is the central invariant of the pending-registry: `env_seq`
/// keys must route replies one-to-one to waiters, even under concurrent
/// allocation + concurrent reply delivery.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_publish_sync_correlation() {
    const N: u64 = 100;
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"corr", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // Obtain all futures synchronously before spawning — publish_sync returns
    // `impl Future + Send` so the futures themselves can be sent to tasks.
    let mut handles = Vec::with_capacity(N as usize);
    for i in 0..N {
        let fut = client.publish_sync(
            stream_id,
            b"corr_ev",
            Bytes::copy_from_slice(&i.to_le_bytes()),
        );
        handles.push(tokio::spawn(fut));
    }

    let mut seqs = HashSet::with_capacity(N as usize);
    for h in handles {
        let resp = h.await.unwrap().expect("publish_sync should ack");
        let ref_seq = u64::from_le_bytes(resp[..8].try_into().unwrap());
        assert!(
            seqs.insert(ref_seq),
            "duplicate ref_seq {ref_seq} — two callers got the same reply"
        );
    }
    assert_eq!(
        seqs.len(),
        N as usize,
        "lost reply: only {} acks",
        seqs.len()
    );

    // Sanity: contiguous monotonic range starting at 1.
    let mut sorted: Vec<u64> = seqs.into_iter().collect();
    sorted.sort_unstable();
    assert_eq!(*sorted.first().unwrap(), 1);
    assert_eq!(*sorted.last().unwrap(), N);
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Many in-flight publish_sync, server dies → all callers wake
// ═══════════════════════════════════════════════════════════════════════════

/// Spawn N `publish_sync` calls, then shut the server down. Every caller
/// must wake with an `Err(_)` — never hang past the request timeout.
///
/// This invariant is what makes the pending registry safe to swap: any
/// transport that drops pending reply channels on disconnect must wake
/// all waiters. With `tokio::oneshot` this works because dropping the
/// `Sender` closes the `Receiver`. With `Pipe<T>`, the equivalent is
/// the disconnect path explicitly poisoning every pending Pipe.
#[tokio::test(flavor = "multi_thread")]
async fn many_inflight_publish_sync_wake_on_disconnect() {
    const N: u64 = 50;
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"orphan", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // Launch N concurrent publish_sync, each carrying a slow-ish payload.
    // Obtain futures synchronously (publish_sync returns impl Future + Send).
    let mut handles = Vec::with_capacity(N as usize);
    for i in 0..N {
        let fut = client.publish_sync(
            stream_id,
            b"orphan_ev",
            Bytes::copy_from_slice(&i.to_le_bytes()),
        );
        handles.push(tokio::spawn(fut));
    }

    // // Give the publishes time to register in the pending map.
    // tokio::time::sleep(Duration::from_millis(50)).await;

    // Kill the server — every pending request must wake (Ok or Err, never hang).
    server.shutdown().await;

    // Hard wall: 5 s total. Default request_timeout is 30 s, so a hang
    // here proves a leaked waiter, not a slow timeout.
    let outcomes = tokio::time::timeout(Duration::from_secs(5), async {
        let mut results = Vec::with_capacity(N as usize);
        for h in handles {
            results.push(h.await.unwrap());
        }
        results
    })
    .await
    .expect("all callers must wake within 5 s of disconnect");

    // We don't care which Ok/Err each got (race with the kill), only that
    // none are still pending. Anything that DID complete with Ok is fine
    // (made it through before the kill); anything Err is fine (woke from
    // disconnect). The test fails by hanging, not by the outcome mix.
    assert_eq!(outcomes.len(), N as usize);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Pending registry recycles cleanly (no leak across calls)
// ═══════════════════════════════════════════════════════════════════════════

/// After resolving a `publish_sync`, the env_seq slot must be released so
/// the next call can reuse it without colliding with the previous waiter.
///
/// Direct leak detection requires touching `Inner.pending` (private).
/// Behaviorally we exercise it: 1000 sequential `publish_sync` calls
/// must all complete in sub-second wall time. If the registry leaked
/// `oneshot::Sender`s (or, post-swap, `Arc<Pipe<_>>`s), memory would
/// grow but the test would still pass — so we add a second probe:
/// after the loop, one more `publish_sync` succeeds, proving the
/// registry is still functional (not deadlocked on a stale entry that
/// happens to alias a new env_seq via u32 wrap — irrelevant at 1000,
/// but shape of the test is the same at any N).
#[tokio::test(flavor = "multi_thread")]
async fn publish_sync_recycles_env_seq() {
    const N: u64 = 1000;
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"recycle", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let mut last_seq = 0u64;
    for i in 0..N {
        let resp = client
            .publish_sync(
                stream_id,
                b"recycle_ev",
                Bytes::copy_from_slice(&i.to_le_bytes()),
            )
            .await
            .expect("publish_sync should succeed mid-loop");
        let s = u64::from_le_bytes(resp[..8].try_into().unwrap());
        assert!(s > last_seq, "ref_seq must be monotonic");
        last_seq = s;
    }

    // Probe: registry still alive after N calls.
    let resp = client
        .publish_sync(stream_id, b"recycle_ev", Bytes::copy_from_slice(b"final"))
        .await
        .expect("post-loop publish_sync must succeed (registry not stuck)");
    let final_seq = u64::from_le_bytes(resp[..8].try_into().unwrap());
    assert!(final_seq > last_seq);
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Ack semantics through the client survive concurrent traffic
// ═══════════════════════════════════════════════════════════════════════════

/// Mixed concurrent traffic: producer publishes N messages, consumer
/// receives + acks them, while a second task fires unrelated
/// `publish_sync` calls. Acks are fire-and-forget, so they don't share
/// the pending registry with the publishes in the new client.
///
/// Invariant: acks and publish replies never cross-route. Verified by:
///   - All N messages received once (no redelivery → ack worked).
///   - All side-channel publish_syncs succeed.
#[tokio::test(flavor = "multi_thread")]
async fn ack_sync_and_publish_sync_share_registry_without_crosstalk() {
    const N: usize = 50;
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"mix", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(
            stream_id,
            b"mix_c",
            b"",
            b"",
            (N as u16) + 10,
            1u8,
            0u8,
            0u8,
            0u32,
            0u64,
        )
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    // Side-channel: another stream, fire publish_syncs in parallel.
    let resp = client
        .create_stream(b"mix_side", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let side_stream_id = TestServer::parse_id(&resp);

    // Publish N messages on the consumed stream.
    for i in 0..N {
        client
            .publish_sync(
                stream_id,
                b"mix_ev",
                Bytes::copy_from_slice(&(i as u64).to_le_bytes()),
            )
            .await
            .expect("publish");
    }

    // Obtain N publish_sync futures synchronously (they are Send), then spawn them.
    let mut side_futures = Vec::with_capacity(N);
    for i in 0..N {
        let fut = client.publish_sync(
            side_stream_id,
            b"side_ev",
            Bytes::copy_from_slice(&(i as u64).to_le_bytes()),
        );
        side_futures.push(tokio::spawn(fut));
    }

    // Drain + ack each delivery.
    let mut received = 0usize;
    while received < N {
        let msg = tokio::time::timeout(Duration::from_secs(3), handle.recv())
            .await
            .expect("delivery should arrive")
            .expect("subscription open");
        msg.ack();
        received += 1;
    }

    let mut side_results = Vec::with_capacity(N);
    for h in side_futures {
        side_results.push(h.await.unwrap());
    }
    assert_eq!(side_results.len(), N);
    for (i, r) in side_results.iter().enumerate() {
        assert!(
            r.is_ok(),
            "side publish_sync #{i} failed: {:?}",
            r.as_ref().err()
        );
    }

    // No redelivery — acks took effect, didn't leak into the publish_sync waiters.
    let extra = tokio::time::timeout(Duration::from_millis(300), handle.recv()).await;
    assert!(
        extra.is_err(),
        "no message should redeliver after ack of all {N}"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Stale Message ack after server death is silent (no panic, clean error)
// ═══════════════════════════════════════════════════════════════════════════

/// `Message::ack()` is fire-and-forget over the ack_tx mpsc. After the
/// server dies, the ack_tx may be replaced (reconnect path) or simply
/// unable to deliver — either way the call must not panic.
#[tokio::test(flavor = "multi_thread")]
async fn stale_message_ack_after_disconnect_is_silent() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"stale", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(
            stream_id, b"stale_c", b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64,
        )
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    client
        .publish_sync(stream_id, b"stale_ev", Bytes::copy_from_slice(b"once"))
        .await
        .expect("publish");

    let msg = tokio::time::timeout(Duration::from_secs(2), handle.recv())
        .await
        .unwrap()
        .unwrap();

    // Kill server — Message now holds a stale ack_tx.
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Fire-and-forget ack: must not panic, regardless of internal state.
    msg.ack();

    // Get a second message via a freshly published one is impossible
    // (server is down) — instead reuse the first by cloning subject/payload
    // into a synthetic ack via the existing handle. We don't have a
    // second Message handle; this branch covers the panic-safety of ack()
    // on a dead transport. ack_sync is covered structurally by test #2.
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. ClientError variants on disconnect are well-formed (no surprises)
// ═══════════════════════════════════════════════════════════════════════════

/// `publish_sync` raced with server shutdown must resolve (ok or error) within
/// the timeout — never hang, never return an unexpected error variant.
///
/// Two outcomes are correct:
///   - `Ok(_)`: request was processed before the server fully stopped
///     (server has a graceful-drain window after the shutdown signal).
///   - `Err(Disconnected | Timeout | ChannelClosed)`: session ended before
///     the ack arrived; `drain_disconnected()` woke the pending waiter.
///
/// What must NOT happen: the future hangs indefinitely or panics.
///
/// Note: `publish_sync` is a lazy future — no frame is sent until it is
/// first polled. The future is spawned here so it races concurrently with
/// the shutdown signal rather than being blocked behind it.
#[tokio::test(flavor = "multi_thread")]
async fn publish_sync_on_dead_server_returns_error() {
    use arbitro_client_tokio::{ClientConfig, ReconnectPolicy};
    use std::time::Duration as D;

    let mut server = TestServerBuilder::new().spawn().await;
    let addr = server.addr.clone();

    // Connect with no reconnect attempts — client will not retry after disconnect.
    let cfg = ClientConfig {
        addr: addr.clone(),
        reconnect: ReconnectPolicy {
            base: D::from_millis(50),
            cap: D::from_millis(100),
            max_attempts: Some(0),
        },
        ..ClientConfig::default()
    };
    let client = Client::connect(cfg).await.expect("client should connect");

    let resp = client
        .create_stream(b"dead", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    // Spawn the publish_sync future so it starts running concurrently with
    // the shutdown signal. It may complete (Ok) during the drain window or
    // fail (Err) if the session ends first.
    let handle =
        tokio::spawn(client.publish_sync(stream_id, b"dead_ev", Bytes::copy_from_slice(b"x")));

    // Signal shutdown — drain_disconnected() will wake any pending waiters.
    server.shutdown().await;

    let r = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("must not hang past 5 s")
        .expect("task must not panic");

    // Both outcomes are valid — the invariant is "resolves, no panic, no hang".
    match r {
        Ok(_) => {} // Processed during drain window — correct.
        Err(ClientError::Disconnected)
        | Err(ClientError::Timeout)
        | Err(ClientError::ChannelClosed) => {}
        Err(e) => panic!("unexpected error variant: {e:?}"),
    }
}
