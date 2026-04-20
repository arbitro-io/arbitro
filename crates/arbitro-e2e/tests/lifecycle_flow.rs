//! End-to-end lifecycle trace dump.
//!
//! Run with:
//!   cargo test -p arbitro-e2e --test lifecycle_flow \
//!     --features arbitro-server/lifecycle_trace -- --nocapture
//!
//! When compiled WITHOUT the feature, the trace collection is a no-op and
//! the test prints only the summary (0 events). With the feature on, every
//! `lifecycle_trace!` call-site fires and we dump the full flow.

use std::time::Duration;

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, StreamConfig};
use arbitro_server::{ArbitroServer, Config};

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

async fn start_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(32);
    let server = ArbitroServer::new(config);
    tokio::spawn(async move { let _ = server.run().await; });
    tokio::time::sleep(Duration::from_millis(80)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect_with_timeout(addr, Duration::from_secs(5))
        .await
        .expect("client must connect")
}

#[tokio::test]
async fn trace_publish_subscribe_ack_flow() {
    // Enable tracing BEFORE any server work.
    arbitro_server::lifecycle_trace::enable();

    let addr = start_server().await;
    let client = connect(&addr).await;

    client
        .create_stream(&StreamConfig::new(b"trace_stream", b">").build())
        .await
        .unwrap();

    let consumer = client
        .create_consumer(
            &ConsumerConfig::new(b"trace_worker", b"trace_stream")
                .ack_policy(AckPolicy::Explicit)
                .max_inflight(10)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let mut sub = consumer.subscribe(None).await.unwrap();

    // Publish 3 messages to see the flow with multiple entries
    for i in 0..3u32 {
        client
            .publish(b"trace_stream", b"trace_stream.evt", &i.to_le_bytes())
            .await
            .unwrap();
    }

    // Receive and ack all 3
    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
            .await
            .expect("message within timeout")
            .expect("channel open");
        msg.ack_sync().await.ok();
    }

    // Allow final tracepoints to settle (ack dispatch is async on the command thread)
    tokio::time::sleep(Duration::from_millis(80)).await;

    arbitro_server::lifecycle_trace::disable();

    // Dump events
    let events = arbitro_server::lifecycle_trace::take();
    println!("\n===== LIFECYCLE TRACE ({} events) =====", events.len());
    if events.is_empty() {
        println!("(no events — build with --features arbitro-server/lifecycle_trace)");
    } else {
        let t0 = events[0].at;
        let mut prev = t0;
        for (i, e) in events.iter().enumerate() {
            let from_start = e.at.duration_since(t0);
            let from_prev = e.at.duration_since(prev);
            println!(
                "[{i:>3}] +{:>9}µs (Δ{:>7}µs) {:<30} conn={:>3} seq={:>4} thread={}",
                from_start.as_micros(),
                from_prev.as_micros(),
                e.label,
                e.conn_id,
                e.seq,
                e.thread,
            );
            prev = e.at;
        }
    }
    println!("=======================================\n");
}
