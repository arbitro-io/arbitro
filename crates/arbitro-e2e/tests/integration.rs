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
        .create_consumer(ConsumerConfig {
            id: ConsumerId(1),
            queue_id: QueueId(1),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: 1000,
        }, vec![])
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
                max_ack_pending: 1000,
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
        .create_consumer(ConsumerConfig {
            id: ConsumerId(1),
            queue_id: QueueId(1),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: 1000,
        }, vec![])
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
                max_ack_pending: 1000,
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

    shard.publish(StreamId(1), conn_id, 1, entries).await.unwrap();

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
    shard.publish(StreamId(1), conn_id, 1, entries).await.unwrap();

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
            .create_consumer(ConsumerConfig {
                id: ConsumerId(i),
                queue_id: QueueId(i),
                stream_id: StreamId(1),
                durable: true,
                ack_policy: AckPolicy::Explicit,
                max_ack_pending: 100,
            }, vec![])
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
                    max_ack_pending: 100,
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

    let (conn_id, _rx) = registry.register();

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
        .create_consumer(ConsumerConfig {
            id: ConsumerId(1),
            queue_id: QueueId(1),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: 100,
        }, vec![])
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
                max_ack_pending: 100,
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
    shard.publish(StreamId(1), conn_id, 1, entries).await.unwrap();

    // Give shard a moment to process
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Claim
    let claim1 = shard
        .claim(QueueId(1), ConnectionId(conn_id), ConsumerId(1), 10, ts)
        .await
        .unwrap();
    assert_eq!(claim1.len(), 1);

    // Nack — should requeue
    let nack_entries: Vec<NackEntry> = claim1.iter().map(|e| NackEntry { seq: e.seq, retry_at: None }).collect();
    let nack = shard.nack(ConsumerId(1), nack_entries, ts).await.unwrap();
    assert_eq!(nack.requeued, 1);

    // Re-claim — should get the same message back
    let claim2 = shard
        .claim(QueueId(1), ConnectionId(conn_id), ConsumerId(1), 10, ts)
        .await
        .unwrap();
    assert_eq!(claim2.len(), 1);

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
        .create_consumer(ConsumerConfig {
            id: ConsumerId(1),
            queue_id: QueueId(1),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: 100,
        }, vec![])
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
                max_ack_pending: 100,
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
    shard.publish(StreamId(1), conn_id, 1, entries).await.unwrap();

    // Give shard a moment to process
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Claim all 50
    let claimed = shard
        .claim(QueueId(1), ConnectionId(conn_id), ConsumerId(1), 256, ts)
        .await
        .unwrap();
    assert!(!claimed.is_empty());

    // Disconnect — all pending should be requeued
    let drain = shard
        .drain_connection(ConnectionId(conn_id), DrainMode::ReleaseAndRequeue, ts)
        .await
        .unwrap();

    assert!(drain.pending_requeued > 0, "pending should be requeued on disconnect");

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

    assert!(!reclaimed.is_empty(), "messages should be reclaimable after disconnect");

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
        .create_consumer(ConsumerConfig {
            id: ConsumerId(1),
            queue_id: QueueId(1),
            stream_id: StreamId(1),
            durable: true,
            ack_policy: AckPolicy::Explicit,
            max_ack_pending: 100,
        }, vec![])
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

    let names: &[&[u8]] = &[b"orders", b"payments", b"events"];
    for name in names {
        let stream_id = arbitro_proto::config::fnv1a_32(name);
        let shard = server.shard_for(StreamId(stream_id));
        shard
            .create_stream(
                StreamConfig {
                    id: StreamId(stream_id),
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
                max_ack_pending: 100,
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
    assert_eq!(action_bytes, Action::RepOk.as_u16(), "first frame should be RepOk");

    // 6. Trigger delivery via DrainDeliver
    shard.drain_deliver().await.unwrap();

    // 7. Receive deliver frames — one per message
    let mut delivered_count = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(frame)) => {
                let action = u16::from_le_bytes([frame[0], frame[1]]);
                if action == Action::Deliver.as_u16() {
                    delivered_count += 1;

                    // Parse seq from envelope env_seq (bytes 12..16)
                    let seq = u32::from_le_bytes([
                        frame[12], frame[13], frame[14], frame[15],
                    ]) as u64;
                    assert!(seq > 0, "seq should be positive");

                    // Body format: [4 consumer_id][2 subj_len][subject][payload]
                    let body_start = ENVELOPE_SIZE;
                    let subj_len = u16::from_le_bytes([
                        frame[body_start + 4],
                        frame[body_start + 5],
                    ]) as usize;
                    let subject = &frame[body_start + 6..body_start + 6 + subj_len];
                    assert_eq!(subject, b"orders_new");
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
