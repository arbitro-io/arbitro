//! End-to-end integration tests — server + client.

use std::sync::Arc;
use std::time::Duration;

use arbitro_client::{Client, ConnState};
use arbitro_proto::config::{AckPolicy, ConsumerConfig, StreamConfig};
use arbitro_server::{ArbitroServer, Config, TokioTransport};

/// Start a server on a random port, return the address.
async fn start_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");

    let config = Config {
        listen_addr: addr.clone(),
        max_connections: 100,
        write_buffer_cap: 1024,
        idle_timeout: Duration::from_secs(60),
        keepalive_interval: Duration::from_secs(10),
        shutdown_timeout: Duration::from_secs(2),
    };

    let transport = Arc::new(TokioTransport::new(config.write_buffer_cap));
    let server = ArbitroServer::new(config, transport);

    tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Wait for server to bind
    tokio::time::sleep(Duration::from_millis(50)).await;

    addr
}

/// Pick a random available port.
fn portpicker() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn client_connects_to_server() {
    let addr = start_server().await;
    let client = Client::connect_with_timeout(&addr, Duration::from_secs(3)).await;
    assert!(client.is_ok(), "client should connect: {:?}", client.err());
    assert!(client.unwrap().is_connected());
}

#[tokio::test]
async fn create_stream_and_publish() {
    let addr = start_server().await;
    let client = Client::connect_with_timeout(&addr, Duration::from_secs(3))
        .await
        .expect("connect");

    // Create a stream
    let cfg = StreamConfig::new(b"orders").max_msgs(100_000).build();
    let result = client.create_stream(&cfg).await;
    assert!(result.is_ok(), "create_stream failed: {:?}", result.err());

    // Publish a message
    let seq = client
        .publish(b"orders", b"orders.new", b"hello world")
        .await;
    assert!(seq.is_ok(), "publish failed: {:?}", seq.err());
    assert!(seq.unwrap() >= 1, "sequence should be >= 1");
}

#[tokio::test]
async fn publish_batch() {
    let addr = start_server().await;
    let client = Client::connect_with_timeout(&addr, Duration::from_secs(3))
        .await
        .expect("connect");

    let cfg = StreamConfig::new(b"events").max_msgs(100_000).build();
    client.create_stream(&cfg).await.unwrap();

    let entries: Vec<(&[u8], &[u8])> = vec![
        (b"events.a", b"payload1"),
        (b"events.b", b"payload2"),
        (b"events.c", b"payload3"),
    ];

    let seq = client.publish_batch(b"events", &entries).await;
    assert!(seq.is_ok(), "publish_batch failed: {:?}", seq.err());
}

#[tokio::test]
async fn publish_to_nonexistent_stream_fails() {
    let addr = start_server().await;
    let client = Client::connect_with_timeout(&addr, Duration::from_secs(3))
        .await
        .expect("connect");

    let result = client
        .publish(b"no_such_stream", b"test.subj", b"data")
        .await;
    assert!(result.is_err(), "publish to nonexistent stream should fail");
}

#[tokio::test]
async fn publish_burst_does_not_block() {
    let addr = start_server().await;
    let client = Client::connect_with_timeout(&addr, Duration::from_secs(3))
        .await
        .expect("connect");

    let cfg = StreamConfig::new(b"fast").max_msgs(100_000).build();
    client.create_stream(&cfg).await.unwrap();

    // Rapid publish should not block
    for i in 0..100u32 {
        client.publish(b"fast", b"fast.msg", &i.to_le_bytes()).await.unwrap();
    }
}

#[tokio::test]
async fn publish_then_subscribe_receives() {
    let addr = start_server().await;
    let client = Client::connect_with_timeout(&addr, Duration::from_secs(3))
        .await
        .expect("connect");

    // Create stream
    let stream_cfg = StreamConfig::new(b"sub_test").build();
    client.create_stream(&stream_cfg).await.unwrap();

    // Create consumer (fire-and-forget)
    let consumer_cfg = ConsumerConfig::new(b"my_consumer", b"sub_test")
        .filter(b">")
        .ack_policy(AckPolicy::None)
        .build();
    let consumer = client.create_consumer(&consumer_cfg).await.expect("create consumer");

    // Publish 4 messages BEFORE subscribing
    for i in 0u32..4 {
        client.publish(b"sub_test", b"sub_test.msg", &i.to_le_bytes()).await.unwrap();
    }

    // Now subscribe — should receive backlog
    let mut sub = consumer.subscribe(None).await.expect("subscribe");

    let mut received = 0u32;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), sub.next()).await {
            Ok(Some(msg)) => {
                eprintln!("  received msg seq={} subject={:?} payload_len={}", msg.seq, std::str::from_utf8(&msg.subject), msg.payload.len());
                received += 1;
                if received >= 4 { break; }
            }
            Ok(None) => {
                panic!("subscription closed after {received} msgs");
            }
            Err(_) => {
                panic!("timeout after {received}/4 msgs");
            }
        }
    }
    assert_eq!(received, 4);
}

#[tokio::test]
async fn connection_state_changes() {
    let addr = start_server().await;
    let client = Client::connect_with_timeout(&addr, Duration::from_secs(3))
        .await
        .expect("connect");

    let mut state_rx = client.on_state_change();
    assert_eq!(*state_rx.borrow_and_update(), ConnState::Connected);
}
