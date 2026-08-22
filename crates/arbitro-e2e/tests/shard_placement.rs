//! Every stream must come out of `CreateStream` with its shard RECORDED.
//!
//! Placement used to be computed at five separate call sites — `store_for`,
//! `gate_for`, `idempotency_for`, `mark_idempotency_allocated`, `shard_for`
//! — each doing its own `stream_id % len`. Changing some and not others is
//! the dangerous shape: publish appends to one shard's store while the gate
//! wakes another shard's drain, so messages are stored and never delivered,
//! with no error in any log. They all go through one decision now, and that
//! decision prefers the recorded value.
//!
//! ## What this does NOT test, and why
//!
//! An earlier version of this file moved a live stream to another shard and
//! asserted delivery still worked. It does not, and the reason is worth
//! recording: **a shard owns its engine catalog, not just its store.**
//! Rewriting the placement of an existing stream points routing at a shard
//! that has never heard of it — the next `create_consumer` lands somewhere
//! the stream does not exist and the connection drops.
//!
//! So recording the placement makes the shard count able to GROW (existing
//! streams stay put, new shards take new streams). It does not make an
//! existing stream movable. That needs migration: freeze, copy the segments,
//! move the catalog entry, then rewrite the field.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_engine_v2::types::StreamId;
use bytes::Bytes;
use std::time::Duration;

const SHARDS: usize = 4;

/// A stream that falls back to the modulo is a stream whose placement was
/// never written down — which silently reintroduces the coupling the
/// recording exists to remove.
#[tokio::test(flavor = "multi_thread")]
async fn create_stream_records_its_placement() {
    let mut server = TestServerBuilder::new().shard_count(SHARDS).spawn().await;
    let client = server.connect().await;

    for i in 0..8u32 {
        let name = format!("placed_{i}");
        // Each stream owns its own slice: admission rejects two streams
        // claiming the same subject space.
        let filter = format!("placed{i}.>");
        let resp = client
            .create_stream(name.as_bytes(), filter.as_bytes(), 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .expect("stream");
        // `parse_id` yields the WIRE id; the catalog is keyed by the small
        // sequential engine id.
        let wire_id = TestServer::parse_id(&resp);
        let seq = server
            .names()
            .stream_seq(wire_id)
            .unwrap_or_else(|| panic!("stream {name} has no engine id"));

        let placed = server.names().stream_shard(seq);
        assert!(
            placed.is_some(),
            "stream {name} (wire {wire_id}, seq {}) came out unplaced — \
             CreateStream did not record a shard",
            seq.raw()
        );
        let placed = placed.unwrap();
        assert!(
            (placed as usize) < SHARDS,
            "stream {name} placed on shard {placed} with only {SHARDS} shards"
        );
        // While the shard count is pinned, the recorded value and the modulo
        // must agree — a disagreement here means the live path and the
        // routing path picked differently, which is the split-brain the
        // single decision point exists to prevent.
        assert_eq!(
            placed as usize,
            seq.raw() as usize % SHARDS,
            "stream {name}: recorded shard {placed} disagrees with the \
             placement rule — the write and the wake can land apart"
        );
    }

    server.shutdown().await;
}

/// End to end on a placed stream: publish, deliver, ack. If any routing path
/// still computed its own index, the append and the gate would go to
/// different shards and this times out rather than failing loudly.
#[tokio::test(flavor = "multi_thread")]
async fn placed_stream_delivers_end_to_end() {
    let mut server = TestServerBuilder::new().shard_count(SHARDS).spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"placed_delivery", b"*.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("stream");
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(
            stream_id,
            b"placed_worker",
            b"placed_worker",
            b"",
            256,
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

    const N: usize = 200;
    let entries: Vec<arbitro_client_tokio::BatchEntry<'_>> = (0..N)
        .map(|_| {
            arbitro_client_tokio::BatchEntry::new(
                b"orders.created",
                Bytes::from_static(b"placed"),
            )
        })
        .collect();
    client
        .publish_batch_wait(stream_id, &entries)
        .await
        .expect("publish");

    let mut got = 0usize;
    for _ in 0..N {
        match tokio::time::timeout(Duration::from_secs(5), sub.recv()).await {
            Ok(Some(msg)) => {
                msg.ack();
                got += 1;
            }
            _ => break,
        }
    }
    assert_eq!(got, N, "{got}/{N} delivered on a placed stream");

    server.shutdown().await;
}

/// Placement must survive the routing helpers being asked repeatedly — the
/// recorded value is read on every publish, so a stale or moving answer
/// would show up as inconsistent routing over time.
#[tokio::test(flavor = "multi_thread")]
async fn placement_is_stable_across_reads() {
    let mut server = TestServerBuilder::new().shard_count(SHARDS).spawn().await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"placed_stable", b"*.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("stream");
    let seq = server
        .names()
        .stream_seq(TestServer::parse_id(&resp))
        .expect("engine id");

    let first = server.names().stream_shard(seq).expect("placed");
    for _ in 0..1000 {
        assert_eq!(
            server.names().stream_shard(seq),
            Some(first),
            "placement changed between reads"
        );
    }
    let _ = StreamId::new(0); // keep the import honest

    server.shutdown().await;
}
