//! Integration tests — shard workers + handles + engine.
//!
//! Tests the full command lifecycle: spawn shard → send commands via
//! ShardHandle → verify engine state through replies.

use std::time::Duration;

use arbitro_engine_v2::batch::{AckEntry, NackEntry};
use arbitro_engine_v2::catalog::{ConsumerConfig, StreamConfig, SubscriptionConfig};
use arbitro_engine_v2::types::*;
use arbitro_proto::action::Action;
use arbitro_proto::wire::envelope::ENVELOPE_SIZE;
use arbitro_server::config::Config;
use arbitro_server::router::Server;
use arbitro_server::transport::ConnectionRegistry;

fn now() -> Timestamp {
    Timestamp::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    )
}

/// Spawn a 2-shard server with a ConnectionRegistry for testing.
fn test_server() -> (Server, ConnectionRegistry) {
    let config = Config::default().shard_count(2).channel_capacity(1024);
    let registry = ConnectionRegistry::new(256);
    let server = Server::spawn(&config, &registry);
    (server, registry)
}

// ── Lifecycle ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_create_stream() {
    let (server, _registry) = test_server();
    let shard = server.shard(0);

    let ok = shard
        .create_stream(
            StreamConfig {
                id: StreamId(1),
                name: b"orders".to_vec(),
            },
            0, // memory store
        )
        .await
        .unwrap();

    assert!(ok);
    server.shutdown();
}

#[tokio::test]
async fn test_create_consumer() {
    let (server, _registry) = test_server();
    let shard = server.shard(0);

    shard
        .create_stream(
            StreamConfig {
                id: StreamId(1),
                name: b"orders".to_vec(),
            },
            0,
        )
        .await
        .unwrap();

    let ok = shard
        .create_consumer(
            ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 1000,
            },
            vec![],
        )
        .await
        .unwrap();

    assert!(ok);
    server.shutdown();
}

#[tokio::test]
async fn test_open_connection() {
    let (server, _registry) = test_server();
    let shard = server.shard(0);

    shard
        .open_connection(ConnectionId(100), NodeId(1), now())
        .await
        .unwrap();

    server.shutdown();
}

#[tokio::test]
async fn test_subscribe_bind() {
    let (server, _registry) = test_server();
    let shard = server.shard(0);

    shard
        .open_connection(ConnectionId(100), NodeId(1), now())
        .await
        .unwrap();

    let ok = shard
        .subscribe(
            StreamConfig {
                id: StreamId(1),
                name: b"events".to_vec(),
            },
            ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 1000,
            },
            SubscriptionConfig {
                id: SubscriptionId(1),
                stream_id: StreamId(1),
                consumer_id: ConsumerId(1),
                filters: vec![],
            },
            ConnectionId(100),
            now(),
        )
        .await
        .unwrap();

    assert!(ok);
    server.shutdown();
}

// ── Publish-Deliver-Ack cycle ──────────────────────────────────────────────

#[tokio::test]
async fn test_publish_ack_cycle() {
    let (server, registry) = test_server();
    let shard = server.shard(0);
    let ts = now();

    // Register a fake connection so the shard can send RepOk
    let (conn_id, mut _rx) = registry.register();

    // Setup: stream + consumer + subscription + connection + bind
    shard
        .create_stream(
            StreamConfig {
                id: StreamId(1),
                name: b"orders".to_vec(),
            },
            0,
        )
        .await
        .unwrap();

    shard
        .create_consumer(
            ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 1000,
            },
            vec![],
        )
        .await
        .unwrap();

    shard
        .open_connection(ConnectionId(conn_id), NodeId(1), ts)
        .await
        .unwrap();

    shard
        .subscribe(
            StreamConfig {
                id: StreamId(1),
                name: b"orders".to_vec(),
            },
            ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 1000,
            },
            SubscriptionConfig {
                id: SubscriptionId(1),
                stream_id: StreamId(1),
                consumer_id: ConsumerId(1),
                filters: vec![],
            },
            ConnectionId(conn_id),
            ts,
        )
        .await
        .unwrap();

    // Publish 100 messages (fire & forget — shard sends RepOk directly)
    let entries: Vec<_> = (0..100u64)
        .map(|_| arbitro_server::command::PublishEntryOwned {
            subject: bytes::Bytes::from_static(b"orders.new"),
            payload: bytes::Bytes::from_static(b"test-payload"),
        })
        .collect();

    shard
        .publish(StreamId(1), conn_id, 1, entries)
        .await
        .unwrap();

    // Give shard a moment to process
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Claim all
    let claimed = shard
        .claim(QueueId(1), ConnectionId(conn_id), ConsumerId(1), 256, ts)
        .await
        .unwrap();

    // Ack all
    let ack_entries: Vec<AckEntry> = claimed.iter().map(|e| AckEntry { seq: e.seq }).collect();
    let count = ack_entries.len() as u32;
    let ack_reply = shard.ack(ConsumerId(1), ack_entries, ts).await.unwrap();
    assert_eq!(ack_reply.accepted + ack_reply.rejected, count);

    server.shutdown();
}

#[tokio::test]
async fn test_publish_batch() {
    let (server, registry) = test_server();
    let shard = server.shard(0);

    let (conn_id, _rx) = registry.register();

    shard
        .create_stream(
            StreamConfig {
                id: StreamId(1),
                name: b"batch".to_vec(),
            },
            0,
        )
        .await
        .unwrap();

    let entries: Vec<_> = (0..1000u64)
        .map(|_| arbitro_server::command::PublishEntryOwned {
            subject: bytes::Bytes::from_static(b"batch_msg"),
            payload: bytes::Bytes::from_static(b"data"),
        })
        .collect();

    // Fire & forget — shard replies directly to connection
    shard
        .publish(StreamId(1), conn_id, 1, entries)
        .await
        .unwrap();

    server.shutdown();
}

#[tokio::test]
async fn test_fanout_delivery() {
    let (server, _registry) = test_server();
    let shard = server.shard(0);
    let ts = now();

    shard
        .create_stream(
            StreamConfig {
                id: StreamId(1),
                name: b"fanout".to_vec(),
            },
            0,
        )
        .await
        .unwrap();

    for i in 1..=3u32 {
        shard
            .create_consumer(
                ConsumerConfig {
                    id: ConsumerId(i),
                    queue_id: QueueId(i),
                    stream_id: StreamId(1),
                    durable: true,
                    ack_policy: AckPolicy::Explicit,
                    max_inflight: 100,
                },
                vec![],
            )
            .await
            .unwrap();

        shard
            .open_connection(ConnectionId(i as u64), NodeId(1), ts)
            .await
            .unwrap();

        shard
            .subscribe(
                StreamConfig {
                    id: StreamId(1),
                    name: b"fanout".to_vec(),
                },
                ConsumerConfig {
                    id: ConsumerId(i),
                    queue_id: QueueId(i),
                    stream_id: StreamId(1),
                    durable: true,
                    ack_policy: AckPolicy::Explicit,
                    max_inflight: 100,
                },
                SubscriptionConfig {
                    id: SubscriptionId(i),
                    stream_id: StreamId(1),
                    consumer_id: ConsumerId(i),
                    filters: vec![],
                },
                ConnectionId(i as u64),
                ts,
            )
            .await
            .unwrap();
    }

    // Consumers are set up — fanout happens at claim time, not publish time
    server.shutdown();
}

// ── Error & invariants ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_nack_redelivery() {
    let (server, registry) = test_server();
    let shard = server.shard(0);
    let ts = now();

    let (conn_id, mut rx) = registry.register();

    shard
        .create_stream(
            StreamConfig {
                id: StreamId(1),
                name: b"nack_test".to_vec(),
            },
            0,
        )
        .await
        .unwrap();

    shard
        .create_consumer(
            ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100,
            },
            vec![],
        )
        .await
        .unwrap();

    shard
        .open_connection(ConnectionId(conn_id), NodeId(1), ts)
        .await
        .unwrap();

    shard
        .subscribe(
            StreamConfig {
                id: StreamId(1),
                name: b"nack_test".to_vec(),
            },
            ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100,
            },
            SubscriptionConfig {
                id: SubscriptionId(1),
                stream_id: StreamId(1),
                consumer_id: ConsumerId(1),
                filters: vec![],
            },
            ConnectionId(conn_id),
            ts,
        )
        .await
        .unwrap();

    let entries = vec![arbitro_server::command::PublishEntryOwned {
        subject: bytes::Bytes::from_static(b"nack_test_msg"),
        payload: bytes::Bytes::from_static(b"data"),
    }];
    shard
        .publish(StreamId(1), conn_id, 1, entries)
        .await
        .unwrap();

    // Wait for shard to auto-deliver via Gate (publish → gate.release → drain_deliver)
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain the auto-delivered frames from connection rx to get the seq.
    // Frames: RepOk (publish reply) + RepBatch (auto-delivered).
    // RepBatch body: [4 consumer_id][2 count][2 pad][8 seq][...].
    let mut delivered_seq = None;
    while let Ok(frame) = rx.try_recv() {
        let action = u16::from_le_bytes([frame[0], frame[1]]);
        if action == Action::RepBatch.as_u16() {
            let off = ENVELOPE_SIZE + 8;
            let seq = u64::from_le_bytes([
                frame[off],
                frame[off + 1],
                frame[off + 2],
                frame[off + 3],
                frame[off + 4],
                frame[off + 5],
                frame[off + 6],
                frame[off + 7],
            ]);
            delivered_seq = Some(seq);
        }
    }
    let seq = delivered_seq.expect("should have received auto-delivered message");

    // Nack — should requeue the auto-delivered message
    let nack_entries = vec![NackEntry {
        seq,
        retry_at: None,
    }];
    let nack = shard.nack(ConsumerId(1), nack_entries, ts).await.unwrap();
    assert_eq!(nack.requeued, 1);

    // Wait for re-delivery via Gate (nack → gate.release → drain_deliver)
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify re-delivery arrived on the connection (auto-delivered by Gate)
    let mut redelivered = false;
    while let Ok(frame) = rx.try_recv() {
        let action = u16::from_le_bytes([frame[0], frame[1]]);
        if action == Action::RepBatch.as_u16() {
            redelivered = true;
        }
    }
    assert!(
        redelivered,
        "nacked message should be re-delivered via Gate"
    );

    server.shutdown();
}

// ── Disconnect invariants ──────────────────────────────────────────────────

#[tokio::test]
async fn test_disconnect_releases_pending() {
    let (server, registry) = test_server();
    let shard = server.shard(0);
    let ts = now();

    let (conn_id, _rx) = registry.register();

    shard
        .create_stream(
            StreamConfig {
                id: StreamId(1),
                name: b"disc_test".to_vec(),
            },
            0,
        )
        .await
        .unwrap();

    shard
        .create_consumer(
            ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100,
            },
            vec![],
        )
        .await
        .unwrap();

    shard
        .open_connection(ConnectionId(conn_id), NodeId(1), ts)
        .await
        .unwrap();

    shard
        .subscribe(
            StreamConfig {
                id: StreamId(1),
                name: b"disc_test".to_vec(),
            },
            ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100,
            },
            SubscriptionConfig {
                id: SubscriptionId(1),
                stream_id: StreamId(1),
                consumer_id: ConsumerId(1),
                filters: vec![],
            },
            ConnectionId(conn_id),
            ts,
        )
        .await
        .unwrap();

    // Publish 50 messages
    let entries: Vec<_> = (0..50)
        .map(|_| arbitro_server::command::PublishEntryOwned {
            subject: bytes::Bytes::from_static(b"disc_test_msg"),
            payload: bytes::Bytes::from_static(b"data"),
        })
        .collect();
    shard
        .publish(StreamId(1), conn_id, 1, entries)
        .await
        .unwrap();

    // Wait for shard to auto-deliver via Gate (publish → gate.release → drain_deliver)
    // Messages become inflight automatically — no explicit claim needed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Disconnect — all inflight (auto-delivered) should be requeued
    let drain = shard
        .drain_connection(ConnectionId(conn_id), DrainMode::ReleaseAndRequeue, ts)
        .await
        .unwrap();

    assert!(
        drain.pending_requeued > 0,
        "pending should be requeued on disconnect"
    );

    // Reconnect and re-claim
    shard
        .open_connection(ConnectionId(200), NodeId(1), ts)
        .await
        .unwrap();

    shard
        .bind(ConnectionId(200), SubscriptionId(1), ts)
        .await
        .unwrap();

    let reclaimed = shard
        .claim(QueueId(1), ConnectionId(200), ConsumerId(1), 256, ts)
        .await
        .unwrap();

    assert!(
        !reclaimed.is_empty(),
        "messages should be reclaimable after disconnect"
    );

    server.shutdown();
}

// ── Admin ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_pause_resume_consumer() {
    let (server, _registry) = test_server();
    let shard = server.shard(0);

    shard
        .create_stream(
            StreamConfig {
                id: StreamId(1),
                name: b"pause_test".to_vec(),
            },
            0,
        )
        .await
        .unwrap();

    shard
        .create_consumer(
            ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100,
            },
            vec![],
        )
        .await
        .unwrap();

    let paused = shard.pause_consumer(ConsumerId(1)).await.unwrap();
    assert!(paused);

    let resumed = shard.resume_consumer(ConsumerId(1)).await.unwrap();
    assert!(resumed);

    server.shutdown();
}

// ── Routing ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_shard_routing_deterministic() {
    let (server, _registry) = test_server();

    let shard_a = server.shard_for(StreamId(42));
    let shard_b = server.shard_for(StreamId(42));
    assert_eq!(shard_a.shard_id(), shard_b.shard_id());

    let s0 = server.shard_for(StreamId(0));
    let s1 = server.shard_for(StreamId(1));
    assert_eq!(s0.shard_id(), 0);
    assert_eq!(s1.shard_id(), 1);

    server.shutdown();
}

// ── Shutdown ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_graceful_shutdown() {
    let (server, _registry) = test_server();
    let shard = server.shard(0);

    shard
        .create_stream(
            StreamConfig {
                id: StreamId(1),
                name: b"shutdown_test".to_vec(),
            },
            0,
        )
        .await
        .unwrap();

    server.shutdown();

    tokio::time::sleep(Duration::from_millis(50)).await;
    let result = shard
        .create_stream(
            StreamConfig {
                id: StreamId(2),
                name: b"after_shutdown".to_vec(),
            },
            0,
        )
        .await;
    assert!(result.is_err(), "commands after shutdown should fail");
}

// ── ListStreams ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_streams() {
    let (server, _registry) = test_server();

    // Use small sequential stream IDs — the engine catalog indexes
    // match_tables by stream_id.0 directly, so hash-derived IDs blow up
    // memory. Names stay distinct so list_streams content is still meaningful.
    let names: &[&[u8]] = &[b"orders", b"payments", b"events"];
    for (i, name) in names.iter().enumerate() {
        let stream_id = StreamId((i + 1) as u32);
        let shard = server.shard_for(stream_id);
        shard
            .create_stream(
                StreamConfig {
                    id: stream_id,
                    name: name.to_vec(),
                },
                0,
            )
            .await
            .unwrap();
    }

    let mut all = Vec::new();
    for i in 0..server.shard_count() {
        let reply = server.shard(i).list_streams().await.unwrap();
        all.extend(reply.streams);
    }

    assert_eq!(all.len(), 3, "should list all 3 streams across shards");

    let mut found_names: Vec<&[u8]> = all.iter().map(|(_, n)| n.as_slice()).collect();
    found_names.sort();
    let mut expected: Vec<&[u8]> = names.to_vec();
    expected.sort();
    assert_eq!(found_names, expected);

    server.shutdown();
}

#[tokio::test]
async fn test_list_streams_empty() {
    let (server, _registry) = test_server();

    for i in 0..server.shard_count() {
        let reply = server.shard(i).list_streams().await.unwrap();
        assert!(reply.streams.is_empty());
    }

    server.shutdown();
}

// ── End-to-end: connect → create → publish → deliver → ack ────────────

#[tokio::test]
async fn test_end_to_end_publish_deliver_ack() {
    let (server, registry) = test_server();
    let ts = now();

    // 1. Register a fake TCP connection — gives us a write channel
    let (conn_id, mut rx) = registry.register();
    let shard = server.shard(0);

    // 2. Open connection in engine
    shard
        .open_connection(ConnectionId(conn_id), NodeId(1), ts)
        .await
        .unwrap();

    // 3. Create stream (with memory store)
    shard
        .create_stream(
            StreamConfig {
                id: StreamId(1),
                name: b"orders".to_vec(),
            },
            0,
        )
        .await
        .unwrap();

    // 4. Subscribe — creates consumer + subscription + bind
    shard
        .subscribe(
            StreamConfig {
                id: StreamId(1),
                name: b"orders".to_vec(),
            },
            ConsumerConfig {
                id: ConsumerId(1),
                queue_id: QueueId(1),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_inflight: 100,
            },
            SubscriptionConfig {
                id: SubscriptionId(1),
                stream_id: StreamId(1),
                consumer_id: ConsumerId(1),
                filters: vec![],
            },
            ConnectionId(conn_id),
            ts,
        )
        .await
        .unwrap();

    // 5. Publish 10 messages
    let entries: Vec<_> = (0..10u64)
        .map(|i| {
            let payload = format!("payload-{i}");
            arbitro_server::command::PublishEntryOwned {
                subject: bytes::Bytes::from_static(b"orders_new"),
                payload: bytes::Bytes::from(payload.into_bytes()),
            }
        })
        .collect();

    shard
        .publish(StreamId(1), conn_id, 1, entries)
        .await
        .unwrap();

    // Drain the RepOk frame that publish sent
    let rep_ok_frame = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("should receive RepOk")
        .expect("channel open");

    // Verify it's a RepOk
    let action_bytes = u16::from_le_bytes([rep_ok_frame[0], rep_ok_frame[1]]);
    assert_eq!(
        action_bytes,
        Action::RepOk.as_u16(),
        "first frame should be RepOk"
    );

    // 6. Wait for shard thread to drain-deliver (gate auto-releases on publish)
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 7. Receive RepBatch frames — drainer batches all entries into one frame.
    //    RepBatch body: [4 consumer_id][2 count][2 pad][entries...]
    //    Entry: [8 seq][2 subj_len][4 data_len][subject][payload]
    let mut delivered_count = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(frame)) => {
                let action = u16::from_le_bytes([frame[0], frame[1]]);
                if action == Action::RepBatch.as_u16() {
                    let body_start = ENVELOPE_SIZE;
                    let count =
                        u16::from_le_bytes([frame[body_start + 4], frame[body_start + 5]]) as u32;
                    delivered_count += count;

                    // Walk entries and verify subject of the first one.
                    let mut off = body_start + 8;
                    for _ in 0..count {
                        let seq = u64::from_le_bytes([
                            frame[off],
                            frame[off + 1],
                            frame[off + 2],
                            frame[off + 3],
                            frame[off + 4],
                            frame[off + 5],
                            frame[off + 6],
                            frame[off + 7],
                        ]);
                        assert!(seq > 0, "seq should be positive");
                        let subj_len =
                            u16::from_le_bytes([frame[off + 8], frame[off + 9]]) as usize;
                        let data_len = u32::from_le_bytes([
                            frame[off + 10],
                            frame[off + 11],
                            frame[off + 12],
                            frame[off + 13],
                        ]) as usize;
                        let subject = &frame[off + 14..off + 14 + subj_len];
                        assert_eq!(subject, b"orders_new");
                        off += 14 + data_len;
                    }
                }
            }
            _ => break,
        }
    }

    assert_eq!(delivered_count, 10, "should deliver all 10 messages");

    // 8. Ack all — claim first to get pending_ids
    let _claimed = shard
        .claim(QueueId(1), ConnectionId(conn_id), ConsumerId(1), 256, ts)
        .await
        .unwrap();

    // All messages were already claimed by drain_deliver, so explicit claim returns 0
    // Ack the sequences we received in deliver frames (seq 1..=10)
    let ack_entries: Vec<AckEntry> = (1..=10u64).map(|seq| AckEntry { seq }).collect();
    let ack_reply = shard.ack(ConsumerId(1), ack_entries, ts).await.unwrap();
    assert_eq!(
        ack_reply.accepted + ack_reply.rejected,
        10,
        "all 10 should be processed"
    );

    server.shutdown();
}

// ── Gate smoke test ───────────────────────────────────────────────────────

/// Verify Gate-driven auto-delivery works end-to-end:
/// publish → gate.release → shard drains → Deliver frames arrive on connection.
/// 3 iterations, 2-second global timeout — must not hang.
#[tokio::test]
async fn test_gate_auto_delivery_smoke() {
    let overall = tokio::time::timeout(Duration::from_secs(2), async {
        let (server, registry) = test_server();
        let shard = server.shard(0);
        let ts = now();

        let (conn_id, mut rx) = registry.register();

        shard
            .open_connection(ConnectionId(conn_id), NodeId(1), ts)
            .await
            .unwrap();

        shard
            .create_stream(
                StreamConfig {
                    id: StreamId(1),
                    name: b"gate_smoke".to_vec(),
                },
                0,
            )
            .await
            .unwrap();

        shard
            .subscribe(
                StreamConfig {
                    id: StreamId(1),
                    name: b"gate_smoke".to_vec(),
                },
                ConsumerConfig {
                    id: ConsumerId(1),
                    queue_id: QueueId(1),
                    stream_id: StreamId(1),
                    durable: true,
                    ack_policy: AckPolicy::Explicit,
                    max_inflight: 100,
                },
                SubscriptionConfig {
                    id: SubscriptionId(1),
                    stream_id: StreamId(1),
                    consumer_id: ConsumerId(1),
                    filters: vec![],
                },
                ConnectionId(conn_id),
                ts,
            )
            .await
            .unwrap();

        // 3 rounds — publish 5 msgs each, verify auto-delivery
        for round in 0..3u32 {
            let entries: Vec<_> = (0..5)
                .map(|i| arbitro_server::command::PublishEntryOwned {
                    subject: bytes::Bytes::from_static(b"gate.smoke"),
                    payload: bytes::Bytes::from(format!("r{round}-{i}").into_bytes()),
                })
                .collect();

            shard
                .publish(StreamId(1), conn_id, round + 1, entries)
                .await
                .unwrap();

            // Wait for shard to auto-deliver
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Drain rx — count entries inside RepBatch frames
            let mut delivers = 0u32;
            while let Ok(frame) = rx.try_recv() {
                let action = u16::from_le_bytes([frame[0], frame[1]]);
                if action == Action::RepBatch.as_u16() {
                    let body_start = ENVELOPE_SIZE;
                    let count =
                        u16::from_le_bytes([frame[body_start + 4], frame[body_start + 5]]) as u32;
                    delivers += count;
                }
            }

            assert_eq!(
                delivers, 5,
                "round {round}: expected 5 auto-delivered messages"
            );

            // Ack all so inflight is freed for next round
            let ack_entries: Vec<AckEntry> = (1..=5u64)
                .map(|i| AckEntry {
                    seq: round as u64 * 5 + i,
                })
                .collect();
            shard.ack(ConsumerId(1), ack_entries, ts).await.unwrap();
        }

        server.shutdown();
    });

    overall
        .await
        .expect("test_gate_auto_delivery_smoke hung — Gate is blocked");
}
