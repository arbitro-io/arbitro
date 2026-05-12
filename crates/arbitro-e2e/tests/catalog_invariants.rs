//! Catalog invariants — regression + structural tests.
//!
//! These tests pin down the contract for the broker's STREAM + CONSUMER
//! catalog separately from the message journal. Every test starts with
//! a fresh in-memory broker so any failure is reproducible without
//! restart.
//!
//! ## Why this file exists
//!
//! In 2026-05 we shipped a fix for `DeleteConsumer` (Action 0x0502).
//! The handler used to call `engine.delete_consumer()` (which removed
//! the entity from the engine's catalog HashMap) but FORGOT to also
//! call `NameRegistry::remove_consumer_by_id()`. The wire-name → id
//! mapping survived, so the next `GetConsumer` request returned `Ok`
//! for a consumer the engine had already deleted, and the same
//! consumer name allocated to a fresh client run silently aliased the
//! ghost id — breaking the next run's subscription.
//!
//! The fix is at:
//!   - `arbitro-common/src/name_registry.rs`: `remove_consumer_by_id`
//!   - `arbitro-server/src/transport/dispatch_v2.rs`: cascade call
//!
//! Every test below either re-checks the original bug (regression) or
//! generalises it into an invariant the system must honour going
//! forward.

use std::time::Duration;

use arbitro_client_tokio::{Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;
use tokio::sync::watch;

// ── Helpers (kept inline so tests are self-contained) ───────────────────────

fn parse_id(resp: &Bytes) -> u32 {
    u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32
}

fn consumer_count(resp: &Bytes) -> usize {
    u32::from_le_bytes(resp[..4].try_into().unwrap()) as usize
}

async fn start_server() -> (watch::Sender<bool>, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let (tx, rx) = watch::channel(false);
    let config = Config::default()
        .listen_addr(&addr)
        .shard_count(2)
        .channel_capacity(1024);

    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run_with_shutdown(rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    (tx, addr)
}

async fn connect(addr: &str) -> Client {
    Client::connect(ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    })
    .await
    .expect("client should connect")
}

async fn create_stream(client: &Client, name: &[u8]) -> u32 {
    // (name, filter, max_msgs, max_bytes, max_age, replicas,
    //  journal_kind=Memory, retention=Limits, discard=Old)
    let resp = client
        .create_stream(name, b">", 0, 0, 0, 1, 0, 0, 0)
        .await
        .expect("create_stream must succeed");
    parse_id(&resp)
}

async fn create_consumer(client: &Client, stream_id: u32, name: &[u8]) -> u32 {
    // (stream_id, name, group, filter, max_inflight, ack_policy=Explicit,
    //  deliver_policy=All, deliver_mode=Push, ack_wait_ms, start_seq)
    let resp = client
        .create_consumer(stream_id, name, b"", b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64)
        .await
        .expect("create_consumer must succeed");
    parse_id(&resp)
}

/// Returns `true` if `GetConsumer` returns `Ok`, `false` if it returns
/// `Err`. Used to assert that a deleted consumer is unreachable.
async fn consumer_exists(client: &Client, stream_id: u32, name: &[u8]) -> bool {
    client.get_consumer(stream_id, name).await.is_ok()
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Regression: DeleteConsumer must remove the wire-name → id mapping.
//
// Pre-fix: GetConsumer returned Ok after DeleteConsumer succeeded.
// Post-fix: GetConsumer returns Err.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn delete_consumer_then_get_returns_not_found() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id = create_stream(&client, b"orders").await;
    let consumer_id = create_consumer(&client, stream_id, b"worker").await;

    // Sanity: it exists before delete.
    assert!(
        consumer_exists(&client, stream_id, b"worker").await,
        "consumer must exist right after create"
    );

    client
        .delete_consumer(consumer_id)
        .await
        .expect("delete_consumer must return Ok");

    // The invariant — pre-fix this assertion FAILED.
    assert!(
        !consumer_exists(&client, stream_id, b"worker").await,
        "GetConsumer must return Err after DeleteConsumer succeeds; \
         pre-fix the wire-name -> id mapping survived in NameRegistry \
         and this returned Ok for a phantom consumer"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. `ListConsumers` must drop the deleted entry as well.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn delete_consumer_excluded_from_list() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id = create_stream(&client, b"orders").await;
    let consumer_id = create_consumer(&client, stream_id, b"worker").await;

    let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
    assert_eq!(
        consumer_count(&resp),
        1,
        "list_consumers must include the freshly-created consumer"
    );

    client.delete_consumer(consumer_id).await.unwrap();

    let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
    assert_eq!(
        consumer_count(&resp),
        0,
        "list_consumers must drop the deleted consumer; otherwise the \
         engine catalog and the wire-facing view disagree"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. The hang-on-second-run scenario, codified.
//
// Re-creating a consumer with the SAME NAME after deleting it must
// produce a fully functional consumer. Pre-fix the second create
// either failed silently or aliased the stale id and the subscription
// received zero deliveries.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn delete_then_recreate_same_name_is_functional() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id = create_stream(&client, b"orders").await;

    let id_a = create_consumer(&client, stream_id, b"worker").await;
    client.delete_consumer(id_a).await.unwrap();

    // Same name, after delete. Must succeed AND yield a fresh id.
    let id_b = create_consumer(&client, stream_id, b"worker").await;
    assert_ne!(
        id_a, id_b,
        "re-created consumer must have a fresh id; reusing the deleted \
         id would silently alias subscriptions to a half-cleaned entity"
    );

    // The freshly-recreated consumer must be reachable through GetConsumer.
    assert!(
        consumer_exists(&client, stream_id, b"worker").await,
        "GetConsumer must succeed for the re-created consumer"
    );

    // ... and through ListConsumers.
    let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
    assert_eq!(
        consumer_count(&resp),
        1,
        "exactly one consumer with the recycled name must be listed"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 3b. DeleteStream must cascade-clean the consumer NAMESPACE too.
//
// If a stream is deleted, every consumer attached to it must lose its
// NameRegistry mapping (wire-name → ConsumerId + reverse indexes). A
// subsequent CreateConsumer with the same name on a freshly-recreated
// stream MUST allocate a new id; reusing the old id silently aliases
// the new consumer to an engine catalog slot that no longer exists.
//
// Pre-fix: engine.delete_stream removed the stream entity but did NOT
// cascade-delete the consumer entities, so NameRegistry retained the
// old name → id mapping. Subsequent CreateConsumer with the same name
// returned the old id, which referenced a non-existent stream → silent
// breakage on subscribe.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn delete_stream_resets_consumer_namespace() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id_a = create_stream(&client, b"events").await;
    let consumer_id_a = create_consumer(&client, stream_id_a, b"worker").await;

    // Sanity: both exist.
    assert!(consumer_exists(&client, stream_id_a, b"worker").await);

    // Delete the stream (NOT the consumer first — we want cascade).
    client.delete_stream(b"events").await.unwrap();

    // Recreate the stream + consumer with SAME names.
    let stream_id_b = create_stream(&client, b"events").await;
    let consumer_id_b = create_consumer(&client, stream_id_b, b"worker").await;

    // The streams may collapse to the same wire-id hash (deterministic
    // foldhash) — that's fine and expected. The CONSUMER id must be
    // fresh: reusing the old id would silently alias the new consumer
    // to a no-longer-existent catalog slot.
    assert_ne!(
        consumer_id_a, consumer_id_b,
        "after delete_stream → create_stream → create_consumer (same \
         name), the consumer MUST get a fresh id; reusing {consumer_id_a} \
         means NameRegistry kept a stale mapping pointing at a catalog \
         slot that delete_stream removed (or should have removed)"
    );

    // And the re-created consumer must actually work end-to-end:
    // subscribe + publish + receive.
    let mut handle = client.subscribe(stream_id_b, consumer_id_b, b"").await.unwrap();
    client
        .publish_sync(stream_id_b, b"events.x", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), handle.recv())
        .await
        .expect("re-created consumer after delete_stream must deliver")
        .expect("subscription must yield a message");
    assert_eq!(&msg.payload()[..], b"hello");
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. DeleteStream MUST cascade-delete consumers attached to it.
//
// Pre-existing behaviour (already correct, locked in here so a future
// refactor that breaks the cascade fails this test).
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn delete_stream_cascades_consumers() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id = create_stream(&client, b"events").await;
    for name in [&b"worker-a"[..], b"worker-b", b"worker-c"] {
        create_consumer(&client, stream_id, name).await;
    }

    let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
    assert_eq!(consumer_count(&resp), 3);

    client.delete_stream(b"events").await.unwrap();

    // Recreate the stream with the same name. The cascade must have
    // cleared its three consumers, so the list under the new stream id
    // must be empty.
    let stream_id_2 = create_stream(&client, b"events").await;
    let resp = client.list_consumers(stream_id_2, 0, 1000).await.unwrap();
    assert_eq!(
        consumer_count(&resp),
        0,
        "DeleteStream must cascade-delete its consumers; otherwise \
         GetConsumer / ListConsumers leak stale catalog entries across \
         stream lifecycles"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. No NameRegistry leak under create→delete pressure.
//
// Repeatedly create and delete a consumer with the SAME NAME. Every
// post-delete check must show zero consumers and a non-aliasing fresh
// id. This catches the class of bugs where DeleteConsumer forgets to
// purge a reverse index (e.g. `consumer_queue`, `consumer_stream`,
// `consumer_deliver`) and the map silently grows.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn create_delete_cycles_no_leak() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id = create_stream(&client, b"orders").await;

    const CYCLES: usize = 50;
    let mut ids = Vec::with_capacity(CYCLES);

    for i in 0..CYCLES {
        let id = create_consumer(&client, stream_id, b"worker").await;
        ids.push(id);

        // After each create, exactly one consumer should be listed.
        let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
        assert_eq!(
            consumer_count(&resp),
            1,
            "iter {i}: exactly one consumer must be listed mid-cycle"
        );

        client.delete_consumer(id).await.unwrap();

        // After each delete, none.
        let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
        assert_eq!(
            consumer_count(&resp),
            0,
            "iter {i}: list_consumers must be empty after delete; \
             pre-fix this stayed at 1 across all cycles"
        );
    }

    // Every cycle should yield a unique id (proves no slot reuse — the
    // current `next_consumer` allocator never recycles; if that ever
    // changes via IdPool wiring, update this assertion accordingly).
    let unique: std::collections::HashSet<_> = ids.iter().copied().collect();
    assert_eq!(
        unique.len(),
        CYCLES,
        "every create must yield a distinct ConsumerId; collision would \
         mean two different lifecycles share state"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. DeleteConsumer is idempotent — the consumer stays gone.
//
// The engine treats `delete_consumer` as idempotent (a second call is a
// no-op, like `DELETE` in S3 / kubectl). That's a deliberate design
// choice; what we MUST guarantee is that the FIRST delete completes
// the removal and a SECOND delete leaves nothing behind either way.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn delete_consumer_is_idempotent() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id = create_stream(&client, b"orders").await;
    let consumer_id = create_consumer(&client, stream_id, b"worker").await;

    client.delete_consumer(consumer_id).await.unwrap();
    assert!(
        !consumer_exists(&client, stream_id, b"worker").await,
        "consumer must be gone after first delete"
    );

    // Second delete: the broker MAY return Ok (idempotent) or Err
    // (strict). Either is acceptable, but state must be unchanged.
    let _ = client.delete_consumer(consumer_id).await;
    assert!(
        !consumer_exists(&client, stream_id, b"worker").await,
        "consumer must remain gone after the redundant second delete"
    );

    let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
    assert_eq!(
        consumer_count(&resp),
        0,
        "list_consumers must remain at 0 even after a redundant delete"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Distinct consumer names → distinct ids (no aliasing).
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn distinct_names_have_distinct_ids() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id = create_stream(&client, b"orders").await;
    let mut ids = Vec::new();
    for n in 0..20u32 {
        let name = format!("worker-{n}");
        let id = create_consumer(&client, stream_id, name.as_bytes()).await;
        ids.push(id);
    }

    let unique: std::collections::HashSet<_> = ids.iter().copied().collect();
    assert_eq!(
        unique.len(),
        ids.len(),
        "distinct consumer names must allocate distinct ids; \
         id aliasing would silently mis-route deliveries"
    );

    let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
    assert_eq!(consumer_count(&resp), 20);
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. End-to-end: delete + recreate must yield a working publish/deliver.
//
// Combines the regression with the actual data path. Pre-fix the
// second subscription received zero messages because the stale
// consumer name pointed at a retired binding.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn delete_recreate_subscription_delivers() {
    let (_tx, addr) = start_server().await;
    let client = connect(&addr).await;

    let stream_id = create_stream(&client, b"orders").await;

    // First lifecycle.
    let id_a = create_consumer(&client, stream_id, b"worker").await;
    let mut sub_a = client.subscribe(stream_id, id_a, b"").await.unwrap();
    client
        .publish_sync(stream_id, b"orders.first", Bytes::from_static(b"first"))
        .await
        .unwrap();
    let msg_a = tokio::time::timeout(Duration::from_secs(2), sub_a.recv())
        .await
        .expect("first lifecycle must deliver");
    drop(msg_a);
    drop(sub_a);
    client.delete_consumer(id_a).await.unwrap();

    // Second lifecycle — same name, same stream.
    let id_b = create_consumer(&client, stream_id, b"worker").await;
    let mut sub_b = client.subscribe(stream_id, id_b, b"").await.unwrap();
    client
        .publish_sync(stream_id, b"orders.second", Bytes::from_static(b"second"))
        .await
        .unwrap();
    let msg_b = tokio::time::timeout(Duration::from_secs(2), sub_b.recv())
        .await
        .expect(
            "re-created consumer with same name must receive deliveries; \
             pre-fix the broker held a phantom binding and this timed out",
        );
    assert!(msg_b.is_some(), "second subscription must produce a message");
}
