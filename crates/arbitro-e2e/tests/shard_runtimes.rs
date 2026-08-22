//! A shard can own its runtime, and the broker still works.
//!
//! With `shard_runtimes` on, each shard's drain and command worker run on
//! one dedicated OS thread instead of the shared tokio pool. That removes
//! the store contention BETWEEN those two — they can no longer be polled
//! simultaneously — which is where the measured share-nothing gain comes
//! from (`arbitro-experiment/shardbench`: +4% at 4 shards, +22% at 8, +28%
//! at 16 over the shared-mutex model).
//!
//! The risk this file exists to catch is not throughput, it is liveness.
//! A `current_thread` runtime has no work-stealing: a task that blocks the
//! thread stalls every other task on that shard, and a shard whose drain
//! never yields simply stops delivering — with no error anywhere, which is
//! the same silent shape as the routing bugs this branch has been chasing.
//! So these tests assert DELIVERY, under fanout and under load, not that
//! the server starts.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use bytes::Bytes;
use std::time::Duration;

const SHARDS: usize = 4;
/// Well under the 25k ceiling this project holds benches to, and enough to
/// span many drain cycles at the default 256-entry feed window.
const N: usize = 4_000;

/// End to end on a broker where every shard owns its thread.
#[tokio::test(flavor = "multi_thread")]
async fn a_broker_on_private_shard_runtimes_still_delivers() {
    let mut server = TestServerBuilder::new()
        .shard_count(SHARDS)
        .shard_runtimes(true)
        .spawn()
        .await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"rt_stream", b"*.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("stream");
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(
            stream_id,
            b"rt_worker",
            b"rt_worker",
            b"",
            1024,
            1, // AckPolicy::Explicit
            0, // DeliverPolicy::All
            0, // Push
            30_000,
            0,
        )
        .await
        .expect("consumer");
    let consumer_id = TestServer::parse_id(&resp);
    let mut sub = client
        .subscribe(stream_id, consumer_id, b"")
        .await
        .expect("subscribe");

    let entries: Vec<arbitro_client_tokio::BatchEntry<'_>> = (0..N)
        .map(|_| arbitro_client_tokio::BatchEntry::new(b"orders.created", Bytes::from_static(b"rt")))
        .collect();
    client
        .publish_batch_wait(stream_id, &entries)
        .await
        .expect("publish");

    let mut got = 0usize;
    for _ in 0..N {
        match tokio::time::timeout(Duration::from_secs(10), sub.recv()).await {
            Ok(Some(msg)) => {
                msg.ack();
                got += 1;
            }
            _ => break,
        }
    }
    assert_eq!(
        got, N,
        "{got}/{N} delivered — a shard's drain stalled on its own runtime, \
         which has no work-stealing to rescue it"
    );

    server.shutdown().await;
}

/// Several streams at once, so more than one shard is live simultaneously.
///
/// One shard working proves the runtime starts; the failure worth catching
/// is a shard that never gets polled at all, and with a single stream the
/// other three shards are idle and indistinguishable from broken.
#[tokio::test(flavor = "multi_thread")]
async fn every_shard_makes_progress_not_just_the_first() {
    let mut server = TestServerBuilder::new()
        .shard_count(SHARDS)
        .shard_runtimes(true)
        .spawn()
        .await;
    let client = server.connect().await;

    // 8 streams over 4 shards — the modulo puts at least two on each.
    const STREAMS: usize = 8;
    const PER_STREAM: usize = 500;
    let mut subs = Vec::new();

    for i in 0..STREAMS {
        let name = format!("rt_multi_{i}");
        let filter = format!("rtmulti{i}.>");
        let resp = client
            .create_stream(name.as_bytes(), filter.as_bytes(), 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .expect("stream");
        let stream_id = TestServer::parse_id(&resp);

        let cname = format!("rt_multi_w{i}");
        let resp = client
            .create_consumer(
                stream_id,
                cname.as_bytes(),
                cname.as_bytes(),
                b"",
                1024,
                1,
                0,
                0,
                30_000,
                0,
            )
            .await
            .expect("consumer");
        let consumer_id = TestServer::parse_id(&resp);
        let sub = client
            .subscribe(stream_id, consumer_id, b"")
            .await
            .expect("subscribe");
        // Keep `i`: the subject must match the stream's OWN filter slice,
        // and the wire id is not it.
        subs.push((i, stream_id, sub));
    }

    for (i, stream_id, _) in &subs {
        let subject = format!("rtmulti{i}.evt");
        let entries: Vec<arbitro_client_tokio::BatchEntry<'_>> = (0..PER_STREAM)
            .map(|_| {
                arbitro_client_tokio::BatchEntry::new(subject.as_bytes(), Bytes::from_static(b"m"))
            })
            .collect();
        client
            .publish_batch_wait(*stream_id, &entries)
            .await
            .expect("publish");
    }

    for (_, stream_id, sub) in subs.iter_mut() {
        let mut got = 0usize;
        for _ in 0..PER_STREAM {
            match tokio::time::timeout(Duration::from_secs(10), sub.recv()).await {
                Ok(Some(msg)) => {
                    msg.ack();
                    got += 1;
                }
                _ => break,
            }
        }
        assert_eq!(
            got, PER_STREAM,
            "stream {stream_id}: {got}/{PER_STREAM} — its shard is not making progress"
        );
    }

    server.shutdown().await;
}

/// Shutdown must finish. Each shard's runtime is a live OS thread parked on
/// a notify; if teardown does not release them the process keeps threads
/// (and their stores) alive, which in a test binary shows up as a suite
/// that never exits rather than as a failure.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_releases_the_shard_threads() {
    for _ in 0..3 {
        let mut server = TestServerBuilder::new()
            .shard_count(SHARDS)
            .shard_runtimes(true)
            .spawn()
            .await;
        let client = server.connect().await;
        client
            .create_stream(b"rt_cycle", b"*.>", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .expect("stream");
        client.close();
        // The assertion is that this returns at all — a leaked runtime
        // thread holding the store would hang here or on the next boot.
        tokio::time::timeout(Duration::from_secs(15), server.shutdown())
            .await
            .expect("shutdown hung with private shard runtimes");
    }
}
