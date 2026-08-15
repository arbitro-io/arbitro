mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::Client;
use bytes::Bytes;
use std::time::Duration;

// ── Helpers ──────────────────────────────────────────────────────────────────

async fn create_stream(client: &Client, name: &[u8], filter: &[u8]) -> u32 {
    let resp = client
        .create_stream(name, filter, 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream must succeed");
    TestServer::parse_id(&resp)
}

async fn create_consumer(client: &Client, stream_id: u32, name: &[u8]) -> u32 {
    let resp = client
        .create_consumer(
            stream_id, name, name, b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64,
        )
        .await
        .expect("create_consumer must succeed");
    TestServer::parse_id(&resp)
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
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"orders", b"evt.>").await;
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
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. `ListConsumers` must drop the deleted entry as well.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn delete_consumer_excluded_from_list() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"orders", b"evt.>").await;
    let consumer_id = create_consumer(&client, stream_id, b"worker").await;

    let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
    assert_eq!(
        TestServer::consumer_count(&resp),
        1,
        "list_consumers must include the freshly-created consumer"
    );

    client.delete_consumer(consumer_id).await.unwrap();

    let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
    assert_eq!(
        TestServer::consumer_count(&resp),
        0,
        "list_consumers must drop the deleted consumer; otherwise the \
         engine catalog and the wire-facing view disagree"
    );
    server.shutdown().await;
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
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"orders", b"evt.>").await;

    let id_a = create_consumer(&client, stream_id, b"worker").await;
    client.delete_consumer(id_a).await.unwrap();

    // Same name, after delete. Must succeed.
    // With IdPool slot recycling (ROB-29), the raw id MAY be reused —
    // generation tags prevent aliasing. The important invariant is that
    // the re-created consumer is reachable and functional.
    let id_b = create_consumer(&client, stream_id, b"worker").await;
    let _ = id_b;

    // The freshly-recreated consumer must be reachable through GetConsumer.
    assert!(
        consumer_exists(&client, stream_id, b"worker").await,
        "GetConsumer must succeed for the re-created consumer"
    );

    // ... and through ListConsumers.
    let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
    assert_eq!(
        TestServer::consumer_count(&resp),
        1,
        "exactly one consumer with the recycled name must be listed"
    );
    server.shutdown().await;
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
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id_a = create_stream(&client, b"events", b"events.x").await;
    let consumer_id_a = create_consumer(&client, stream_id_a, b"worker").await;

    // Sanity: both exist.
    assert!(consumer_exists(&client, stream_id_a, b"worker").await);

    // Delete the stream (NOT the consumer first — we want cascade).
    client.delete_stream(b"events").await.unwrap();

    // Recreate the stream + consumer with SAME names.
    let stream_id_b = create_stream(&client, b"events", b"events.x").await;
    let consumer_id_b = create_consumer(&client, stream_id_b, b"worker").await;

    // The streams may collapse to the same wire-id hash (deterministic
    // foldhash) — that's fine and expected. The CONSUMER id must be
    // fresh: reusing the old id would silently alias the new consumer
    // to a no-longer-existent catalog slot.
    // With IdPool slot recycling (ROB-29), the raw consumer id MAY be
    // reused after cascade-delete. The important invariant is that the
    // re-created consumer on the new stream actually works end-to-end.
    let _ = (consumer_id_a, consumer_id_b);

    // And the re-created consumer must actually work end-to-end:
    // subscribe + publish + receive.
    let mut handle = client
        .subscribe(stream_id_b, consumer_id_b, b"")
        .await
        .unwrap();
    client
        .publish_wait(stream_id_b, b"events.x", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), handle.recv())
        .await
        .expect("re-created consumer after delete_stream must deliver")
        .expect("subscription must yield a message");
    assert_eq!(&msg.payload()[..], b"hello");
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. DeleteStream MUST cascade-delete consumers attached to it.
//
// Pre-existing behaviour (already correct, locked in here so a future
// refactor that breaks the cascade fails this test).
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn delete_stream_cascades_consumers() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"events", b"evt.>").await;
    for name in [&b"worker-a"[..], b"worker-b", b"worker-c"] {
        create_consumer(&client, stream_id, name).await;
    }

    let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
    assert_eq!(TestServer::consumer_count(&resp), 3);

    client.delete_stream(b"events").await.unwrap();

    // Recreate the stream with the same name. The cascade must have
    // cleared its three consumers, so the list under the new stream id
    // must be empty.
    let stream_id_2 = create_stream(&client, b"events", b"evt.>").await;
    let resp = client.list_consumers(stream_id_2, 0, 1000).await.unwrap();
    assert_eq!(
        TestServer::consumer_count(&resp),
        0,
        "DeleteStream must cascade-delete its consumers; otherwise \
         GetConsumer / ListConsumers leak stale catalog entries across \
         stream lifecycles"
    );
    server.shutdown().await;
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
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"orders", b"evt.>").await;

    const CYCLES: usize = 50;
    let mut ids = Vec::with_capacity(CYCLES);

    for i in 0..CYCLES {
        let id = create_consumer(&client, stream_id, b"worker").await;
        ids.push(id);

        // After each create, exactly one consumer should be listed.
        let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
        assert_eq!(
            TestServer::consumer_count(&resp),
            1,
            "iter {i}: exactly one consumer must be listed mid-cycle"
        );

        client.delete_consumer(id).await.unwrap();

        // After each delete, none.
        let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
        assert_eq!(
            TestServer::consumer_count(&resp),
            0,
            "iter {i}: list_consumers must be empty after delete; \
             pre-fix this stayed at 1 across all cycles"
        );
    }

    // With IdPool slot recycling (ROB-29), the same raw id IS reused
    // after delete — that's expected. The per-cycle count assertions
    // above already prove that create/delete cycles don't leak.
    server.shutdown().await;
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
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"orders", b"evt.>").await;
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
        TestServer::consumer_count(&resp),
        0,
        "list_consumers must remain at 0 even after a redundant delete"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Distinct consumer names → distinct ids (no aliasing).
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn distinct_names_have_distinct_ids() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"orders", b"evt.>").await;
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
    assert_eq!(TestServer::consumer_count(&resp), 20);
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. End-to-end: delete + recreate must yield a working publish/deliver.
//
// Combines the regression with the actual data path. Pre-fix the
// second subscription received zero messages because the stale
// consumer name pointed at a retired binding.
// ═══════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════
// T4. Creating 4097 consumers must NOT panic — the 4097th must surface
// as an error, not a thread abort that takes down the harness. Pre-B1
// the NameRegistry's MAX_SLOT_COUNT (=4096) was reached via debug_assert
// and panicked the shard worker.
// ═══════════════════════════════════════════════════════════════════════════
#[tokio::test(flavor = "multi_thread")]
async fn create_4097_consumers_does_not_panic() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"capacity-stream", b"evt.>").await;

    // First 4096 should succeed.
    let mut ok = 0u32;
    for i in 0..4096u32 {
        let name = format!("c-{i}");
        let resp = client
            .create_consumer(
                stream_id,
                name.as_bytes(),
                name.as_bytes(),
                b"",
                100u16,
                1u8,
                0u8,
                0u8,
                0u32,
                0u64,
            )
            .await;
        if resp.is_ok() {
            ok += 1;
        } else {
            // Acceptable: some IDs may collide via foldhash in the
            // wire-name namespace; the test is about NOT panicking.
            break;
        }
    }
    // 4097th: must either error or be Ok with a graceful path — must NOT
    // bring the harness down by panicking.
    let resp = client
        .create_consumer(
            stream_id,
            b"c-overflow",
            b"c-overflow",
            b"",
            100u16,
            1u8,
            0u8,
            0u8,
            0u32,
            0u64,
        )
        .await;
    // The point of the test: we got here without the server panicking.
    // The reply itself may be Ok (foldhash collision freed a slot) or
    // Err (capacity); both are fine.
    let _ = resp;
    assert!(ok > 0, "at least some consumers should have been created");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_recreate_subscription_delivers() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"orders", b"orders.>").await;

    // First lifecycle.
    let id_a = create_consumer(&client, stream_id, b"worker").await;
    let mut sub_a = client.subscribe(stream_id, id_a, b"").await.unwrap();
    client
        .publish_wait(stream_id, b"orders.first", Bytes::from_static(b"first"))
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
        .publish_wait(stream_id, b"orders.second", Bytes::from_static(b"second"))
        .await
        .unwrap();
    let msg_b = tokio::time::timeout(Duration::from_secs(2), sub_b.recv())
        .await
        .expect(
            "re-created consumer with same name must receive deliveries; \
             pre-fix the broker held a phantom binding and this timed out",
        );
    assert!(
        msg_b.is_some(),
        "second subscription must produce a message"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// T6. Malformed CreateConsumer (invalid name) must NOT leak a consumer slot.
//
// The dispatch validation rejects names with invalid characters before
// allocating a slot in NameRegistry. This test verifies that after
// multiple rejected requests, valid consumer creation still works and
// the namespace isn't polluted with ghost entries.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn malformed_create_consumer_does_not_leak_slot() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"orders", b"evt.>").await;

    // Attempt several invalid consumer names. Each should fail without
    // allocating a slot in the internal registry.
    let invalid_names: &[&[u8]] = &[
        b"",           // empty
        b"has space",  // space char
        b"dot.name",   // dot char
        b"slash/name", // slash char
    ];

    for &bad_name in invalid_names {
        let result = client
            .create_consumer(
                stream_id, bad_name, bad_name, b"", 100u16, 1u8, 0u8, 0u8, 0u32, 0u64,
            )
            .await;
        assert!(
            result.is_err(),
            "create_consumer with invalid name {:?} must fail",
            bad_name
        );
    }

    // After all the rejected attempts, listing consumers must show zero.
    let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
    assert_eq!(
        TestServer::consumer_count(&resp),
        0,
        "malformed CreateConsumer must not leak phantom entries into \
         the consumer list"
    );

    // And creating a VALID consumer must succeed — proving the slot
    // pool wasn't corrupted by the rejected attempts.
    let valid_id = create_consumer(&client, stream_id, b"worker").await;
    assert!(valid_id > 0, "valid consumer id must be allocated");

    let resp = client.list_consumers(stream_id, 0, 1000).await.unwrap();
    assert_eq!(
        TestServer::consumer_count(&resp),
        1,
        "exactly one valid consumer must exist after the malformed attempts"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// F1 (audit Part B): stale-config rejection — re-creating an existing
// durable consumer with a DIFFERENT config must be rejected with
// InvalidConsumerConfig, and the ORIGINAL config must stay in force.
// The wire path existed (Ok(2) → InvalidConsumerConfig) but had zero e2e
// coverage.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn create_consumer_config_mismatch_rejected() {
    use arbitro_client_tokio::ClientError;
    use arbitro_proto::error::ErrorCode;

    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"f1_cfg", b"f1_cfg.k").await;

    // Original: max_inflight = 10, AckPolicy::Explicit.
    let consumer_id = TestServer::parse_id(
        &client
            .create_consumer(
                stream_id,
                b"f1_worker",
                b"f1_worker",
                b"",
                10u16,
                1u8,
                0u8,
                0u8,
                30_000u32,
                0u64,
            )
            .await
            .expect("original create must succeed"),
    );

    // Re-create the SAME name with max_inflight = 20 → must be rejected,
    // not silently merged or updated.
    let err = client
        .create_consumer(
            stream_id,
            b"f1_worker",
            b"f1_worker",
            b"",
            20u16,
            1u8,
            0u8,
            0u8,
            30_000u32,
            0u64,
        )
        .await
        .expect_err("re-create with different max_inflight must be rejected");
    assert!(
        matches!(
            err,
            ClientError::Broker {
                code: ErrorCode::InvalidConsumerConfig
            }
        ),
        "expected InvalidConsumerConfig, got {err:?}"
    );

    // Idempotent re-create with the ORIGINAL config still works and
    // returns the same consumer id — the registration was not disturbed.
    let same_id = TestServer::parse_id(
        &client
            .create_consumer(
                stream_id,
                b"f1_worker",
                b"f1_worker",
                b"",
                10u16,
                1u8,
                0u8,
                0u8,
                30_000u32,
                0u64,
            )
            .await
            .expect("idempotent re-create with the original config must succeed"),
    );
    assert_eq!(same_id, consumer_id, "same durable name must keep its id");

    // Behavioral proof the ORIGINAL max_inflight=10 is in force (not the
    // rejected 20): publish 15, ack nothing → exactly 10 are delivered.
    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();
    for i in 0..15u8 {
        client
            .publish_wait(stream_id, b"f1_cfg.k", Bytes::from(vec![i]))
            .await
            .expect("publish");
    }
    let mut delivered = 0usize;
    while let Ok(Some(_msg)) = tokio::time::timeout(Duration::from_secs(2), handle.recv()).await {
        delivered += 1;
        // Do NOT ack — pin the inflight window at the configured cap.
        if delivered > 10 {
            break;
        }
    }
    assert_eq!(
        delivered, 10,
        "exactly max_inflight=10 unacked deliveries must arrive — the \
         rejected max_inflight=20 config must NOT be in force"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// F1b (audit #10 follow-up): a REJECTED re-create must not corrupt the
// NameRegistry either. Pre-fix, `v2_create_consumer` wrote deliver_policy /
// queue / stream into the NameRegistry BEFORE the engine's config-mismatch
// check — so a rejected re-create with deliver_policy=All silently flipped
// the registry copy of a DeliverPolicy::New consumer, and the next
// subscribe replayed the full history despite the create having errored.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn rejected_reconsumer_does_not_corrupt_registry() {
    use arbitro_client_tokio::ClientError;
    use arbitro_proto::error::ErrorCode;

    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let stream_id = create_stream(&client, b"reg_guard", b"reg_guard.>").await;

    // A sibling consumer drains 5 history messages first, so the stream
    // has advanced past the backlog before the victim joins (same
    // late-join setup as temporal_isolation_repro.rs).
    let sib_id = TestServer::parse_id(
        &client
            .create_consumer(
                stream_id, b"sibling", b"sibling", b"", 100u16, 1u8, 1u8, 0u8, 0u32, 0u64,
            )
            .await
            .expect("sibling create must succeed"),
    );
    let mut sib = client.subscribe(stream_id, sib_id, b"").await.unwrap();
    for i in 0..5u8 {
        client
            .publish_wait(stream_id, b"reg_guard.hist", Bytes::from(vec![b'h', i]))
            .await
            .expect("history publish");
    }
    for _ in 0..5 {
        let msg = tokio::time::timeout(Duration::from_secs(2), sib.recv())
            .await
            .expect("sibling must drain the history")
            .expect("sibling subscription must yield");
        msg.ack();
    }

    // Victim: DeliverPolicy::New (1) — a late joiner that must NOT replay.
    let victim_id = TestServer::parse_id(
        &client
            .create_consumer(
                stream_id, b"victim", b"victim", b"", 100u16, 1u8, 1u8, 0u8, 0u32, 0u64,
            )
            .await
            .expect("victim create must succeed"),
    );

    // Rejected re-create: deliver_policy=All (0) AND max_inflight=50 (the
    // engine-visible mismatch). Must be rejected with InvalidConsumerConfig.
    let err = client
        .create_consumer(
            stream_id, b"victim", b"victim", b"", 50u16, 1u8, 0u8, 0u8, 0u32, 0u64,
        )
        .await
        .expect_err("re-create with different config must be rejected");
    assert!(
        matches!(
            err,
            ClientError::Broker {
                code: ErrorCode::InvalidConsumerConfig
            }
        ),
        "expected InvalidConsumerConfig, got {err:?}"
    );

    // Subscribe the victim AFTER the rejected re-create, then publish 3
    // new messages. With deliver_policy=New intact it must see ONLY the
    // 3 new ones. Pre-fix the registry already held the rejected All
    // policy, so the subscribe rewound and replayed the 5 history
    // messages too.
    let mut sub = client.subscribe(stream_id, victim_id, b"").await.unwrap();
    for i in 0..3u8 {
        client
            .publish_wait(stream_id, b"reg_guard.live", Bytes::from(vec![b'n', i]))
            .await
            .expect("live publish");
    }
    let mut got: Vec<Vec<u8>> = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(2), sub.recv()).await {
        got.push(msg.payload().to_vec());
        msg.ack();
        if got.len() > 3 {
            break; // already corrupt — no need to drain the full replay
        }
    }
    assert!(
        got.iter().all(|p| p.first() == Some(&b'n')),
        "victim (DeliverPolicy::New) must not replay history after a \
         REJECTED re-create with DeliverPolicy::All; got payloads {got:?}"
    );
    assert_eq!(
        got.len(),
        3,
        "victim must receive exactly the 3 post-subscribe messages; a \
         rejected re-create must leave the registry deliver_policy intact"
    );
    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// GROUP-1: an EMPTY group must be rejected by the BROKER at CreateConsumer.
//
// Pre-fix the broker accepted it and, in Queue mode, ran the group through
// `get_or_create_queue(stream_id, b"")` — allocating a real queue keyed
// `(stream_id, "")`, a single anonymous queue silently shared by every
// no-group queue consumer on that stream, so unrelated workers round-robined
// each other's messages.
//
// Every client now fills the group in (group, else consumer name, else
// stream name), so a CreateConsumer arriving without one is a client bug and
// must fail loudly. Because the Rust client defaults the group before it
// encodes the frame, this test talks RAW WIRE — it is the only way to put a
// genuinely empty group in front of the dispatcher and prove the SERVER-side
// guard, not the client-side one.
// ═══════════════════════════════════════════════════════════════════════════

/// Send one cold-path frame on a raw socket and return `(action, body)` of
/// the reply. Frames a HELLO first, so each call is a fresh connection that
/// bypasses the client's group defaulting entirely.
async fn raw_roundtrip(addr: &str, frame: Bytes) -> (u16, Vec<u8>) {
    use arbitro_proto::v2::magic::ARBITRO_MAGIC_V2;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut sock = tokio::net::TcpStream::connect(addr)
        .await
        .expect("raw connect");

    let mut hello = Vec::with_capacity(8);
    hello.extend_from_slice(&ARBITRO_MAGIC_V2.to_le_bytes());
    hello.extend_from_slice(&[0u8; 4]);
    sock.write_all(&hello).await.expect("write HELLO");
    sock.write_all(&frame).await.expect("write frame");

    let mut header = [0u8; 16];
    tokio::time::timeout(Duration::from_secs(5), sock.read_exact(&mut header))
        .await
        .expect("reply must arrive")
        .expect("reply header");
    let action = u16::from_le_bytes(header[0..2].try_into().unwrap());
    let msg_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let mut body = vec![0u8; msg_len];
    if msg_len > 0 {
        tokio::time::timeout(Duration::from_secs(5), sock.read_exact(&mut body))
            .await
            .expect("reply body must arrive")
            .expect("reply body");
    }
    (action, body)
}

fn create_consumer_frame(stream_id: u32, name: &[u8], group: &[u8], deliver_mode: u8) -> Bytes {
    use arbitro_proto::v2::cold::{ColdBody, CreateConsumer};
    CreateConsumer {
        stream_id,
        name: name.to_vec(),
        group: group.to_vec(),
        subject: Vec::new(),
        max_inflight: 100,
        ack_policy: 1,
        deliver_policy: 0,
        deliver_mode,
        ack_wait_ms: 30_000,
        start_seq: 0,
        subject_limits: Vec::new(),
        max_nack: None,
    }
    .encode(1)
}

#[tokio::test(flavor = "multi_thread")]
async fn create_consumer_empty_group_rejected_by_broker() {
    use arbitro_proto::action::Action;
    use arbitro_proto::error::ErrorCode;

    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"group_guard", b"evt.>").await;

    // Queue mode (deliver_mode = 1) — the case that used to land every
    // group-less consumer in the shared anonymous `(stream, "")` queue.
    let (action, body) = raw_roundtrip(
        &server.addr,
        create_consumer_frame(stream_id, b"no_group_q", b"", 1),
    )
    .await;
    assert_eq!(
        action,
        Action::RepError.as_u16(),
        "empty group in Queue mode must come back as RepError"
    );
    assert_eq!(
        u16::from_le_bytes(body[8..10].try_into().unwrap()),
        ErrorCode::InvalidConsumerConfig.as_u16(),
        "expected InvalidConsumerConfig"
    );

    // Fanout mode (deliver_mode = 0) — same rule. The group is mandatory
    // regardless of delivery mode, so a client cannot dodge the guard by
    // leaving the mode at its default.
    let (action, body) = raw_roundtrip(
        &server.addr,
        create_consumer_frame(stream_id, b"no_group_f", b"", 0),
    )
    .await;
    assert_eq!(
        action,
        Action::RepError.as_u16(),
        "empty group in Fanout mode must come back as RepError"
    );
    assert_eq!(
        u16::from_le_bytes(body[8..10].try_into().unwrap()),
        ErrorCode::InvalidConsumerConfig.as_u16(),
        "expected InvalidConsumerConfig"
    );

    // Neither rejected create may have left anything behind — no name
    // registration, no anonymous queue.
    assert!(
        !consumer_exists(&client, stream_id, b"no_group_q").await,
        "a rejected create must not register the consumer"
    );
    assert!(
        !consumer_exists(&client, stream_id, b"no_group_f").await,
        "a rejected create must not register the consumer"
    );

    // The byte-identical request WITH a group succeeds on the same raw
    // path — the rejection is about the empty group and nothing else.
    let (action, _) = raw_roundtrip(
        &server.addr,
        create_consumer_frame(stream_id, b"with_group", b"workers", 1),
    )
    .await;
    assert_ne!(
        action,
        Action::RepError.as_u16(),
        "the same frame with a non-empty group must be accepted"
    );
    assert!(
        consumer_exists(&client, stream_id, b"with_group").await,
        "the accepted create must be registered"
    );

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// PurgeStream and DrainSubject must be scoped to the named stream.
//
// `PurgeStreamCmd` and `DrainSubjectCmd` both carry a `stream_id`, but
// `handle_purge_stream` / `handle_drain_subject` call `store.purge()` /
// `store.drain()` on the SHARD store and never read that id. A shard holds
// every stream routed to it, so purging one stream wipes its neighbours'
// messages and returns a shard-wide count.
//
// `shard_count(1)` forces both streams onto the same shard so the collision
// is deterministic rather than hash-dependent.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn purge_stream_does_not_touch_a_shard_neighbour() {
    let mut server = TestServerBuilder::new().shard_count(1).spawn().await;
    let client = server.connect().await;

    let victim = create_stream(&client, b"purge_victim", b"purge_victim.>").await;
    let bystander = create_stream(&client, b"purge_bystander", b"purge_bystander.>").await;

    for i in 0..10u8 {
        client
            .publish_wait(victim, b"purge_victim.a", Bytes::from(vec![i]))
            .await
            .expect("publish to victim");
    }
    for i in 0..7u8 {
        client
            .publish_wait(bystander, b"purge_bystander.a", Bytes::from(vec![100 + i]))
            .await
            .expect("publish to bystander");
    }

    let resp = client
        .purge_stream(b"purge_victim")
        .await
        .expect("purge must succeed");
    let purged = u64::from_le_bytes(resp[..8].try_into().unwrap());
    assert_eq!(
        purged, 10,
        "purge must count only the named stream's messages, not the whole shard"
    );

    // The bystander's 10..=16 payloads must still be deliverable.
    let consumer = create_consumer(&client, bystander, b"survivor").await;
    let mut sub = client
        .subscribe(bystander, consumer, b"")
        .await
        .expect("subscribe to the bystander");

    let mut got = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while got.len() < 7 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, sub.recv()).await {
            Ok(Some(msg)) => {
                got.push(msg.payload().to_vec());
                msg.ack();
            }
            _ => break,
        }
    }
    assert_eq!(
        got.len(),
        7,
        "purging a sibling stream must not destroy this stream's messages; \
         got {} of 7",
        got.len()
    );

    // …and the purge must actually purge. Counting 10 while still
    // delivering them would satisfy every assertion above.
    let victim_consumer = create_consumer(&client, victim, b"after_purge").await;
    let mut victim_sub = client
        .subscribe(victim, victim_consumer, b"")
        .await
        .expect("subscribe to the purged stream");
    let leaked = tokio::time::timeout(Duration::from_millis(500), victim_sub.recv()).await;
    assert!(
        matches!(leaked, Err(_) | Ok(None)),
        "a purged stream must deliver nothing; got {:?}",
        leaked.ok().flatten().map(|m| m.payload().to_vec())
    );

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// DrainSubject is the same defect class as PurgeStream: the command carried
// a stream_id that `handle_drain_subject` threw away, because `Store::drain`
// only took a subject and swept the shard. Two streams sharing a shard and
// using the same subject names — the common case, not a contrived one — meant
// draining either destroyed the other's matching messages.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn drain_subject_does_not_touch_a_shard_neighbour() {
    let mut server = TestServerBuilder::new().shard_count(1).spawn().await;
    let client = server.connect().await;

    let victim = create_stream(&client, b"drain_victim", b"shared.victim").await;
    let bystander = create_stream(&client, b"drain_bystander", b"shared.bystander").await;

    // Both sit under `shared.` on ONE shard, so a drain that walks the shard
    // instead of the named stream would take the neighbour with it.
    for i in 0..6u8 {
        client
            .publish_wait(victim, b"shared.victim", Bytes::from(vec![i]))
            .await
            .expect("publish to victim");
    }
    for i in 0..4u8 {
        client
            .publish_wait(bystander, b"shared.bystander", Bytes::from(vec![100 + i]))
            .await
            .expect("publish to bystander");
    }

    let resp = client
        .drain_subject(b"drain_victim", b"shared.>")
        .await
        .expect("drain must succeed");
    let drained = u64::from_le_bytes(resp[..8].try_into().unwrap());
    assert_eq!(
        drained, 6,
        "drain must count only the named stream's matches, not the shard's"
    );

    // The bystander's 4 messages must still be deliverable.
    let consumer = create_consumer(&client, bystander, b"drain_survivor").await;
    let mut sub = client
        .subscribe(bystander, consumer, b"")
        .await
        .expect("subscribe to the bystander");

    let mut got = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while got.len() < 4 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, sub.recv()).await {
            Ok(Some(msg)) => {
                got.push(msg.payload().to_vec());
                msg.ack();
            }
            _ => break,
        }
    }
    assert_eq!(
        got.len(),
        4,
        "draining a sibling stream must not destroy this stream's messages; \
         got {} of 4",
        got.len()
    );

    // …and the drain must actually drain.
    let victim_consumer = create_consumer(&client, victim, b"after_drain").await;
    let mut victim_sub = client
        .subscribe(victim, victim_consumer, b"")
        .await
        .expect("subscribe to the drained stream");
    let leaked = tokio::time::timeout(Duration::from_millis(500), victim_sub.recv()).await;
    assert!(
        matches!(leaked, Err(_) | Ok(None)),
        "a drained stream must deliver nothing; got {:?}",
        leaked.ok().flatten().map(|m| m.payload().to_vec())
    );

    server.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Nested consumer filters: `orders.premium.>` alongside `orders.>`.
//
// Streams may not have overlapping filters — `StreamConfig` says so and the
// broker enforces it. Consumers have no such documented rule, and the natural
// reading is that they are independent readers of one log: a wide consumer
// and a narrow one nested inside it should both work, each seeing its own
// slice. That is what hierarchical routing needs.
//
// Two things are being pinned, and they are separable:
//
//   1. Creation is allowed. If the broker refused, hierarchical routing would
//      be impossible by construction — worth knowing loudly.
//   2. The narrow consumer only receives what its filter matches. This is the
//      part that fails today: the consumer-side filter is stored and even
//      compared when detecting a config conflict, but `Binding` carries no
//      filter field and the drain never consults one. `one_consumer_many_
//      filters.rs` proves the strong form (a consumer on `telemetry.cpu.>`
//      receives `orders.*`); this pins the subtler nested case, where the
//      wrong behaviour is easy to mistake for correct.
// ═══════════════════════════════════════════════════════════════════════════

/// Create a consumer with an explicit subject filter. The module's own
/// `create_consumer` helper hardcodes `b""` — as do all 157 `create_consumer`
/// calls across the e2e suite, which is why nothing ever exercised this.
async fn create_filtered_consumer(
    client: &Client,
    stream_id: u32,
    name: &[u8],
    filter: &[u8],
) -> u32 {
    let resp = client
        .create_consumer(
            stream_id, name, name, filter, 100u16, 1u8, 0u8, 0u8, 30_000u32, 0u64,
        )
        .await
        .expect("create_consumer with a subject filter must succeed");
    TestServer::parse_id(&resp)
}

/// The same cut, but over the BACKLOG: everything is published before either
/// consumer subscribes.
///
/// [`sibling_consumer_filters_are_independent`] only exercises live drain —
/// it subscribes first and publishes after. Backlog replay is a different
/// walk through the store, so a filter honoured on one path says nothing
/// about the other.
#[tokio::test]
async fn sibling_consumer_filters_cut_the_backlog_too() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"backlog_filters", b"orders.>").await;

    // PUBLISH FIRST — nobody is subscribed yet.
    for subj in [
        &b"orders.basic.1"[..],
        &b"orders.premium.1"[..],
        &b"orders.premium.2"[..],
    ] {
        client
            .publish_wait(stream_id, subj, Bytes::from("o"))
            .await
            .expect("publish");
    }

    let basic = create_filtered_consumer(&client, stream_id, b"basic_bl", b"orders.basic.>").await;
    let premium =
        create_filtered_consumer(&client, stream_id, b"premium_bl", b"orders.premium.>").await;

    let cb = server.connect().await;
    let mut sub_basic = cb
        .subscribe(stream_id, basic, b"")
        .await
        .expect("subscribe basic");
    let cp = server.connect().await;
    let mut sub_premium = cp
        .subscribe(stream_id, premium, b"")
        .await
        .expect("subscribe premium");

    let got_basic = drain_subjects(&mut sub_basic).await;
    let got_premium = drain_subjects(&mut sub_premium).await;

    client
        .delete_stream(b"backlog_filters")
        .await
        .expect("delete stream");
    server.shutdown().await;

    let foreign: Vec<String> = got_premium
        .iter()
        .filter(|s| !s.starts_with(b"orders.premium."))
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect();
    assert!(
        foreign.is_empty(),
        "replaying the backlog handed the `orders.premium.>` consumer {foreign:?}"
    );
    assert_eq!(
        got_premium.len(),
        2,
        "the premium consumer must replay exactly its 2, saw {}: {got_premium:?}",
        got_premium.len()
    );
    assert_eq!(
        got_basic.len(),
        1,
        "the basic consumer must replay exactly its 1, saw {}: {got_basic:?}",
        got_basic.len()
    );
}

#[tokio::test]
async fn sibling_consumer_filters_are_independent() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"nested_filters", b"orders.>").await;

    let wide = create_filtered_consumer(&client, stream_id, b"basic_only", b"orders.basic.>").await;
    let nested =
        create_filtered_consumer(&client, stream_id, b"premium_only", b"orders.premium.>").await;
    assert_ne!(
        wide, nested,
        "the two sibling filters collapsed onto one consumer id"
    );

    let cw = server.connect().await;
    let mut sub_wide = cw
        .subscribe(stream_id, wide, b"")
        .await
        .expect("subscribe wide");
    let cn = server.connect().await;
    let mut sub_nested = cn
        .subscribe(stream_id, nested, b"")
        .await
        .expect("subscribe nested");

    // One outside the nested filter, two inside it.
    for subj in [
        &b"orders.basic.1"[..],
        &b"orders.premium.1"[..],
        &b"orders.premium.2"[..],
    ] {
        client
            .publish_wait(stream_id, subj, Bytes::from("o"))
            .await
            .expect("publish");
    }

    let got_wide = drain_subjects(&mut sub_wide).await;
    let got_nested = drain_subjects(&mut sub_nested).await;

    client
        .delete_stream(b"nested_filters")
        .await
        .expect("delete stream");
    server.shutdown().await;

    // WHAT arrived, before HOW MUCH — a count mismatch reads as "someone stole
    // from me" when the defect is "I was handed traffic I never asked for".
    let foreign: Vec<String> = got_nested
        .iter()
        .filter(|s| !s.starts_with(b"orders.premium."))
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect();
    assert!(
        foreign.is_empty(),
        "the consumer filtered on `orders.premium.>` was delivered {} subject(s) \
         outside its filter: {foreign:?} — the consumer-side filter is not \
         consulted at delivery time",
        foreign.len()
    );

    assert_eq!(
        got_nested.len(),
        2,
        "the nested consumer must see exactly the 2 premium messages, saw {}: \
         {got_nested:?}",
        got_nested.len()
    );

    // Neither sibling starves the other: both are independent readers.
    assert_eq!(
        got_wide.len(),
        1,
        "the consumer on `orders.basic.>` must see its 1 message, saw {}: {got_wide:?}",
        got_wide.len()
    );
}

/// Collect every subject a subscription hands over before it goes quiet.
async fn drain_subjects(sub: &mut arbitro_client_tokio::SubscriptionHandle) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(400), sub.recv()).await {
        out.push(msg.subject().to_vec());
        msg.ack();
    }
    out
}

/// Control for [`sibling_consumer_filters_are_independent`]: same two sibling
/// consumers, but the filter is handed to `subscribe` as well.
///
/// Nesting is deliberately absent. Two consumers may not nest under one
/// another, so a nested pair cannot be built here at all — that shape belongs
/// to two subscriptions on ONE consumer.
#[tokio::test]
async fn sibling_filters_applied_at_subscribe_do_cut() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;
    let stream_id = create_stream(&client, b"nested_at_sub", b"orders.>").await;

    let wide = create_filtered_consumer(&client, stream_id, b"basic_only_s", b"orders.basic.>").await;
    let nested =
        create_filtered_consumer(&client, stream_id, b"premium_only_s", b"orders.premium.>").await;

    // The only difference from the other test: the pattern goes here too.
    let cw = server.connect().await;
    let mut sub_wide = cw
        .subscribe(stream_id, wide, b"orders.basic.>")
        .await
        .expect("subscribe wide");
    let cn = server.connect().await;
    let mut sub_nested = cn
        .subscribe(stream_id, nested, b"orders.premium.>")
        .await
        .expect("subscribe nested");

    for subj in [
        &b"orders.basic.1"[..],
        &b"orders.premium.1"[..],
        &b"orders.premium.2"[..],
    ] {
        client
            .publish_wait(stream_id, subj, Bytes::from("o"))
            .await
            .expect("publish");
    }

    let got_wide = drain_subjects(&mut sub_wide).await;
    let got_nested = drain_subjects(&mut sub_nested).await;
    println!(
        "[at-subscribe] wide={:?} nested={:?}",
        got_wide
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>(),
        got_nested
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>()
    );

    client
        .delete_stream(b"nested_at_sub")
        .await
        .expect("delete stream");
    server.shutdown().await;

    let foreign: Vec<String> = got_nested
        .iter()
        .filter(|s| !s.starts_with(b"orders.premium."))
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect();
    assert!(
        foreign.is_empty(),
        "even with the pattern at subscribe, the premium consumer got {foreign:?}"
    );
}
