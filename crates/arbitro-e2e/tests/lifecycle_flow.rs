mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use std::time::Duration;
use bytes::Bytes;


#[tokio::test(flavor = "multi_thread")]
async fn trace_publish_subscribe_ack_flow() {
    arbitro_server::lifecycle_trace::enable();

    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client.create_stream(b"trace_stream", b">", 0, 0, 0, 1, 0, 0, 0, 0).await.unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client.create_consumer(stream_id, b"trace_worker", b"", b"", 10, 1, 0, 0, 0, 0)
        .await.unwrap();
    let consumer_id = TestServer::parse_id(&resp);

    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    for i in 0..3u32 {
        client.publish_sync(stream_id, b"trace_stream.evt", Bytes::copy_from_slice(&i.to_le_bytes()))
            .await
            .expect("publish");
    }

    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(2), handle.recv())
            .await.expect("msg timeout").expect("channel open");
        msg.ack();
    }

    server.shutdown().await;
    arbitro_server::lifecycle_trace::disable();

    let events = arbitro_server::lifecycle_trace::take();
    println!("\n===== LIFECYCLE TRACE ({} events) =====", events.len());
    if events.is_empty() {
        println!("(no events — build with --features arbitro-server/lifecycle_trace)");
    } else {
        let t0 = events[0].at;
        let mut prev = t0;
        for (i, e) in events.iter().enumerate() {
            let from_start = e.at.duration_since(t0);
            let from_prev  = e.at.duration_since(prev);
            println!(
                "[{i:>3}] +{:>9}µs (Δ{:>7}µs) {:<30} conn={:>3} seq={:>4} thread={}",
                from_start.as_micros(), from_prev.as_micros(),
                e.label, e.conn_id, e.seq, e.thread,
            );
            prev = e.at;
        }
    }
    println!("=======================================\n");
}
