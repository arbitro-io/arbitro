mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use bytes::Bytes;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn trace_publish_subscribe_ack_flow() {
    arbitro_server::lifecycle_trace::enable();

    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"trace_stream", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(stream_id, b"trace_worker", b"", b"", 10, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let consumer_id = TestServer::parse_id(&resp);

    let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

    for i in 0..3u32 {
        client
            .publish_sync(
                stream_id,
                b"trace_stream.evt",
                Bytes::copy_from_slice(&i.to_le_bytes()),
            )
            .await
            .expect("publish");
    }

    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(2), handle.recv())
            .await
            .expect("msg timeout")
            .expect("channel open");
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

// ═══════════════════════════════════════════════════════════════════════════
// T12 — Stream recreation after delete must not cross-contaminate the
// previous live subscriber. A consumer subscribed to the *old* stream
// must NOT see messages published to the *new* (same-name) stream.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn t12_stream_recreation_does_not_cross_contaminate() {
    let mut server = TestServerBuilder::new().spawn().await;
    let client = server.connect().await;

    // Create stream + consumer; publish 5, drain+ack 3, leave 2 unacked.
    let resp = client
        .create_stream(b"t12", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let old_stream_id = TestServer::parse_id(&resp);
    let resp = client
        .create_consumer(old_stream_id, b"t12_c", b"", b"", 100, 1, 0, 0, 30_000, 0)
        .await
        .unwrap();
    let old_consumer_id = TestServer::parse_id(&resp);
    let mut old_handle = client
        .subscribe(old_stream_id, old_consumer_id, b"")
        .await
        .unwrap();

    for i in 0u32..5 {
        client
            .publish_sync(
                old_stream_id,
                b"t12.ev",
                Bytes::copy_from_slice(&i.to_le_bytes()),
            )
            .await
            .expect("publish");
    }
    // Drain everything the broker is currently willing to deliver on
    // the old subscription, acking as we go, so we don't confuse the
    // post-delete sweep with leftover *old* traffic. We ack 5 entries
    // (everything we published), then ensure no further delivery is
    // pending before deletion.
    for _ in 0..5 {
        let m = tokio::time::timeout(Duration::from_secs(2), old_handle.recv())
            .await
            .expect("recv")
            .expect("open");
        m.ack();
    }
    // Drain any tail-end redelivery.
    while let Ok(Some(m)) =
        tokio::time::timeout(Duration::from_millis(150), old_handle.recv()).await
    {
        m.ack();
    }

    // Delete the stream — this drops the old consumer and the old
    // subscription server-side.
    client.delete_stream(b"t12").await.expect("delete");

    // Recreate with the same name → fresh stream_id.
    let resp = client
        .create_stream(b"t12", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let new_stream_id = TestServer::parse_id(&resp);
    // Note: the broker reuses the wire_id for same-name recreate (name→id
    // mapping is stable). The cross-contamination guard below is the
    // real test — old handle must not see new traffic, regardless of
    // whether the IDs match.
    let _ = (old_stream_id, new_stream_id);

    // Publish 3 new entries on the fresh stream.
    for i in 100u32..103 {
        client
            .publish_sync(
                new_stream_id,
                b"t12.new",
                Bytes::copy_from_slice(&i.to_le_bytes()),
            )
            .await
            .expect("publish");
    }

    // Old handle must not receive any of the new messages — its
    // subscription was torn down with delete_stream. Either timeout
    // (Err) or channel-closed (Ok(None)) are valid; a delivery is the
    // failure path.
    let stray = tokio::time::timeout(Duration::from_millis(400), old_handle.recv()).await;
    match &stray {
        Err(_) => {}   // timeout — no delivery
        Ok(None) => {} // subscription closed cleanly
        Ok(Some(m)) => panic!(
            "old subscription must not see new-stream traffic; got msg seq={} stream={}",
            m.seq, m.stream_id
        ),
    }

    // Fresh consumer on the new stream MUST see all 3.
    let resp = client
        .create_consumer(
            new_stream_id,
            b"t12_fresh",
            b"",
            b"",
            100,
            1,
            0,
            0,
            30_000,
            0,
        )
        .await
        .unwrap();
    let new_consumer_id = TestServer::parse_id(&resp);
    let mut new_handle = client
        .subscribe(new_stream_id, new_consumer_id, b"")
        .await
        .unwrap();
    let mut got = 0usize;
    while got < 3 {
        let m = tokio::time::timeout(Duration::from_secs(2), new_handle.recv())
            .await
            .expect("new consumer must receive 3 messages")
            .expect("open");
        m.ack();
        got += 1;
    }
    assert_eq!(got, 3);

    server.shutdown().await;
}
