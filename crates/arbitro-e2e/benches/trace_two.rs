//! trace_two — minimal 2-message bench for tracing with println!() probes.
//!
//! Sends exactly 2 messages under AckPolicy::None (fanout) and then exactly
//! 2 messages under AckPolicy::Explicit (ack). Not a throughput measurement —
//! the purpose is to exercise the smallest possible path so prints in
//! `shard/worker.rs` / `shard/drain.rs` are readable line-by-line.
//!
//! Run (testing.md):
//!   cargo bench --bench trace_two -p arbitro-e2e --no-run
//!   cp target/release/deps/trace_two-<hash> /tmp/arbitro/
//!   cd /tmp/arbitro && timeout 120 ./trace_two-<hash> --bench 2>&1 | tee /tmp/bench.log

use std::time::Duration;

use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, DeliverPolicy, StreamConfig};
use arbitro_server::{lifecycle_trace, ArbitroServer, Config};
use tokio::runtime::Runtime;

const PAYLOAD: &[u8] = b"hello";

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

async fn spawn_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(8)
        .shard_count(1)
        .write_buffer_cap(64 * 1024);
    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(120)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    Client::connect_with_timeout(addr, Duration::from_secs(5))
        .await
        .expect("client connects")
}

async fn scenario_fanout(addr: &str) {
    println!("\n─── [FANOUT] 2 msgs, AckPolicy::None ───────────────────────");
    let client = connect(addr).await;
    let stream = b"trace_fanout".to_vec();

    client
        .create_stream(&StreamConfig::new(&stream, b">").build())
        .await
        .expect("create_stream");

    let cfg = ConsumerConfig::new(b"trace_fanout_c", &stream)
        .ack_policy(AckPolicy::None)
        .deliver_policy(DeliverPolicy::All)
        .build()
        .expect("consumer cfg");
    let consumer = client.create_consumer(&cfg).await.expect("create_consumer");
    let mut sub = consumer.subscribe(None).await.expect("subscribe");

    println!("[FANOUT] publishing msg #1");
    client
        .publish(&stream, b"trace.fanout", PAYLOAD)
        .await
        .expect("publish #1");
    println!("[FANOUT] publishing msg #2");
    client
        .publish(&stream, b"trace.fanout", PAYLOAD)
        .await
        .expect("publish #2");

    for i in 1..=2 {
        let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .expect("recv timeout")
            .expect("subscription closed");
        println!(
            "[FANOUT] received msg #{i} seq={} subject={:?}",
            msg.seq,
            std::str::from_utf8(&msg.subject).unwrap_or("?"),
        );
    }
}

async fn scenario_ack(addr: &str) {
    println!("\n─── [ACK] 2 msgs, AckPolicy::Explicit ──────────────────────");
    let client = connect(addr).await;
    let stream = b"trace_ack".to_vec();

    client
        .create_stream(&StreamConfig::new(&stream, b">").build())
        .await
        .expect("create_stream");

    let cfg = ConsumerConfig::new(b"trace_ack_c", &stream)
        .ack_policy(AckPolicy::Explicit)
        .max_inflight(8)
        .deliver_policy(DeliverPolicy::All)
        .build()
        .expect("consumer cfg");
    let consumer = client.create_consumer(&cfg).await.expect("create_consumer");
    let mut sub = consumer.subscribe(None).await.expect("subscribe");

    println!("[ACK] publishing msg #1");
    client
        .publish(&stream, b"trace.ack", PAYLOAD)
        .await
        .expect("publish #1");
    println!("[ACK] publishing msg #2");
    client
        .publish(&stream, b"trace.ack", PAYLOAD)
        .await
        .expect("publish #2");

    for i in 1..=2 {
        let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .expect("recv timeout")
            .expect("subscription closed");
        println!(
            "[ACK] received msg #{i} seq={} subject={:?}",
            msg.seq,
            std::str::from_utf8(&msg.subject).unwrap_or("?"),
        );
        println!("[ACK] ack_sync msg #{i}");
        msg.ack_sync().await.expect("ack_sync");
    }
}

fn main() {
    let rt = Runtime::new().expect("tokio runtime");
    let t0 = std::time::Instant::now();
    rt.block_on(async {
        let addr = spawn_server().await;
        println!("[server] listening on {addr}");

        // No-op when built WITHOUT --features lifecycle_trace.
        // When the feature is on, events are pushed to an internal Vec and
        // we dump them at the end.
        lifecycle_trace::enable();

        scenario_fanout(&addr).await;
        scenario_ack(&addr).await;

        lifecycle_trace::disable();

        let events = lifecycle_trace::take();
        println!(
            "\n[lifecycle_trace] events captured = {} (0 means feature disabled)",
            events.len()
        );
        #[cfg(feature = "lifecycle_trace")]
        {
            if !events.is_empty() {
                let t_base = events[0].at;
                for e in &events {
                    let us = e.at.duration_since(t_base).as_nanos() as f64 / 1000.0;
                    println!(
                        "  +{:>10.3} µs  {:<40}  conn={} seq={} thread={}",
                        us, e.label, e.conn_id, e.seq, e.thread,
                    );
                }
            }
        }
        #[cfg(not(feature = "lifecycle_trace"))]
        let _ = events;

        println!("\n[done]");
    });
    let total = t0.elapsed();
    println!("[total wall-clock] {:?}", total);
}
