//! Delete a stream and recreate it under the SAME name, then subscribe live
//! and publish a burst — four times over, against ONE broker.
//!
//! The TypeScript and Go clients both see 1000, 0, 1000, 0 doing this. Every
//! other e2e test here spawns its own broker per test, so none of them can
//! observe what a recreated name inherits from the name it replaced.

mod test_helper;
use test_helper::TestServerBuilder;

use arbitro_client_tokio::{AckPolicy, ConsumerBuilder, DeliverPolicy, StreamBuilder};
use bytes::Bytes;
use std::time::Duration;

const MSGS: usize = 1000;

#[tokio::test(flavor = "multi_thread")]
async fn recreated_stream_still_delivers() {
    let mut server = TestServerBuilder::new().spawn().await;
    let admin = server.connect().await;

    let mut got_per_attempt = Vec::new();

    for attempt in 1..=2 {
        // Record only the attempt that fails, so the ring holds its moments
        // and not the healthy round's noise.
        if attempt == 2 {
            arbitro_server::lifecycle_trace::enable();
        }
        let _ = admin.delete_stream(b"repro_recreate").await;

        let stream_id = StreamBuilder::new(b"repro_recreate")
            .filter(b"repro_recreate.>")
            .create(&admin)
            .await
            .expect("create stream");

        let consumer_id = ConsumerBuilder::new(b"repro_recreate_workers")
            .stream_name(b"repro_recreate")
            .filter(b"repro_recreate.>")
            .ack_policy(AckPolicy::None)
            .deliver_policy(DeliverPolicy::New)
            .create(&admin, stream_id)
            .await
            .expect("create consumer");

        let sub_client = server.connect().await;
        let mut sub = sub_client
            .subscribe(stream_id, consumer_id, b"")
            .await
            .expect("subscribe");

        let payload = Bytes::from(vec![0x42u8; 128]);
        for _ in 0..MSGS - 1 {
            let _ = admin.publish(stream_id, b"repro_recreate.event", payload.clone());
        }
        admin
            .publish_wait(stream_id, b"repro_recreate.event", payload.clone())
            .await
            .expect("publish");

        let mut got = 0usize;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while got < MSGS {
            match tokio::time::timeout_at(deadline, sub.recv()).await {
                Ok(Some(_)) => got += 1,
                _ => break,
            }
        }
        println!("attempt {attempt}: got={got}/{MSGS}");
        got_per_attempt.push(got);
    }

    server.shutdown().await;
    arbitro_server::lifecycle_trace::disable();

    let events = arbitro_server::lifecycle_trace::take();
    println!("\n===== LIFECYCLE ({} events) =====", events.len());
    if events.is_empty() {
        println!("(build with --features lifecycle_trace)");
    } else {
        // Collapse runs of the same label: the stalled drain repeats the same
        // handful thousands of times, and the shape is what matters.
        let mut prev = String::new();
        let mut run = 0usize;
        for e in events.iter() {
            let key = format!("{}  conn={} seq={}", e.label, e.conn_id, e.seq);
            if key == prev {
                run += 1;
                continue;
            }
            if run > 0 {
                println!("  {prev}  ×{run}");
            }
            prev = key;
            run = 1;
        }
        if run > 0 {
            println!("  {prev}  ×{run}");
        }
    }

    assert!(
        got_per_attempt.iter().all(|&n| n == MSGS),
        "a recreated stream stopped delivering: {got_per_attempt:?}"
    );
}
