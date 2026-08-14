//! Invariant: a consumer receives its ENTIRE backlog, whatever its sibling
//! consumers on the same shard were doing when it subscribed.
//!
//! The drain cursor is shared per shard, not per stream. One walk feeds every
//! stream that shard owns, and `process_drain_entry` skips entries belonging
//! to a stream with no demand. Whether that skip is allowed to move the shared
//! cursor is the whole question: if it is, entries of a not-yet-subscribed
//! stream are stepped over and only a later rewind can recover them. These
//! tests pin the observable contract — every message published before a
//! consumer existed still reaches it — so a regression shows up as a short
//! count instead of a hung benchmark.
//!
//! Every scenario runs on `shard_count(1)` so all streams provably share one
//! cursor. With the default shard count the streams hash across shards and the
//! interaction under test may not be exercised at all.
//!
//! A stalled drain is observed as a per-message receive timeout, never as a
//! hang: the count that did arrive is the diagnostic.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arbitro_client_tokio::{BatchEntry, Client};
use bytes::Bytes;

const STREAMS: usize = 4;
const BATCH: usize = 256;
const PAYLOAD: &[u8] = &[0u8; 64];

/// Idle tolerance per message. Generous, because it only ever elapses when
/// the drain has genuinely stopped feeding this consumer.
const RECV_IDLE: Duration = Duration::from_secs(5);

/// Backlog per stream. The shared-cursor race widens with the length of the
/// walk, so the reproducing size is much larger than a CI-friendly default —
/// override to reproduce, see `heavy_backlog_is_fully_delivered`.
fn cfg_msgs() -> u32 {
    std::env::var("ARBITRO_TEST_BACKLOG_MSGS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000)
}

/// Shard count. Defaults to 1 so the streams provably share one drain cursor;
/// overridable because whether the stall needs one shard or several is an open
/// question — the throughput bench that stalls runs on the default count.
fn cfg_shards() -> usize {
    std::env::var("ARBITRO_TEST_SHARDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

/// Publish `total` messages, then wait for the tail batch so every entry is
/// provably in the store before any consumer exists.
async fn prefill(client: &Client, stream_id: u32, subject: &[u8], total: u32) {
    let payload = Bytes::from_static(PAYLOAD);
    let entries: Vec<BatchEntry<'_>> = (0..BATCH)
        .map(|_| BatchEntry {
            subject,
            msg_id: &[],
            payload: payload.clone(),
        })
        .collect();

    let total = total as usize;
    let batches = total.div_ceil(BATCH);
    for b in 0..batches {
        let size = BATCH.min(total - b * BATCH);
        let slice = &entries[..size];
        // `ChannelClosed` is this client's "outbound queue is full" signal,
        // not a dead connection — it is transient on BOTH publish paths, so
        // both retry. Only the tail batch is awaited: its reply proves every
        // earlier entry of this stream is already in the store.
        if b + 1 == batches {
            loop {
                match client.publish_batch_wait(stream_id, slice).await {
                    Ok(_) => break,
                    Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                        tokio::task::yield_now().await
                    }
                    Err(e) => panic!("prefill tail batch: {e:?}"),
                }
            }
        } else {
            loop {
                match client.publish_batch(stream_id, slice) {
                    Ok(()) => break,
                    Err(arbitro_client_tokio::ClientError::ChannelClosed) => {
                        tokio::task::yield_now().await
                    }
                    Err(e) => panic!("prefill: {e:?}"),
                }
            }
        }
    }
}

/// Knobs where the CI-friendly invariant runs differ from an exact replica of
/// the throughput bench's replay setup. The bench stalls and these tests do
/// not, so the difference lives in here somewhere: start from `bench_parity`
/// and subtract until the stall stops reproducing — that names the cause.
#[derive(Clone, Copy)]
struct Setup {
    shards: usize,
    write_buffer_cap: Option<usize>,
    max_connections: Option<u32>,
    /// `true` funnels every stream's prefill through ONE connection, as the
    /// bench does; `false` gives each publisher its own. This changes how the
    /// streams' sequences interleave in the store.
    shared_publisher: bool,
}

impl Setup {
    fn invariant() -> Self {
        Self {
            shards: cfg_shards(),
            write_buffer_cap: None,
            max_connections: None,
            shared_publisher: false,
        }
    }

    /// Mirrors `arbitro-e2e/benches/throughput.rs` replay mode.
    fn bench_parity() -> Self {
        Self {
            shards: std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4),
            write_buffer_cap: Some(65536),
            max_connections: Some(500),
            shared_publisher: true,
        }
    }
}

struct Fixture {
    /// Ownership guard only — keeps the broker alive for the whole test.
    _server: TestServer,
    readers: Vec<Client>,
    stream_ids: Vec<u32>,
    consumer_ids: Vec<u32>,
    msgs: u32,
}

/// `STREAMS` streams on ONE shard, each prefilled, each with a consumer
/// created but NOT yet subscribed — no stream has demand when this returns.
async fn fixture(tag: &str, cfg: Setup) -> Fixture {
    let msgs = cfg_msgs();
    let mut builder = TestServerBuilder::new().shard_count(cfg.shards);
    if let Some(cap) = cfg.write_buffer_cap {
        builder = builder.write_buffer_cap(cap);
    }
    if let Some(max) = cfg.max_connections {
        builder = builder.max_connections(max);
    }
    let server = builder.spawn().await;
    let setup = server.connect().await;

    let mut stream_ids = Vec::with_capacity(STREAMS);
    for i in 0..STREAMS {
        let name = format!("{tag}_s{i}");
        let resp = setup
            .create_stream(name.as_bytes(), format!("{name}.>").as_bytes(), 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .expect("create stream");
        stream_ids.push(TestServer::parse_id(&resp));
    }

    // Prefill every stream CONCURRENTLY so their sequences interleave in the
    // shared store. Filling them one after another would leave a contiguous
    // block per stream, and a walk would cross each block while that stream
    // is either wholly subscribed or wholly idle — which never exercises the
    // per-window "this stream has no demand, skip it" decision the shared
    // cursor is exposed to.
    // One connection per publisher: sharing a single client would make the
    // four tasks contend for one outbound queue, which throttles the
    // interleaving this fixture exists to produce.
    {
        let mut js = tokio::task::JoinSet::new();
        for (i, &stream_id) in stream_ids.iter().enumerate() {
            let c = if cfg.shared_publisher {
                setup.clone()
            } else {
                server.connect().await
            };
            let subject = format!("{tag}_s{i}.msg");
            js.spawn(async move {
                prefill(&c, stream_id, subject.as_bytes(), msgs).await;
            });
        }
        while let Some(res) = js.join_next().await {
            res.expect("prefill task");
        }
    }

    // Consumers exist but stay unsubscribed: creating one is a cold-path
    // registry write, only `subscribe` raises demand for its stream.
    let mut consumer_ids = Vec::with_capacity(STREAMS);
    for (i, &stream_id) in stream_ids.iter().enumerate() {
        let name = format!("{tag}_c{i}");
        let resp = setup
            .create_consumer(
                stream_id,
                name.as_bytes(),
                name.as_bytes(),
                b"",
                u16::MAX,
                0, // ack_policy = None
                0, // deliver_policy = All
                0, // deliver_mode = Push
                30_000,
                0,
            )
            .await
            .expect("create consumer");
        consumer_ids.push(TestServer::parse_id(&resp));
    }

    let mut readers = Vec::with_capacity(STREAMS);
    for _ in 0..STREAMS {
        readers.push(server.connect().await);
    }

    Fixture {
        _server: server,
        readers,
        stream_ids,
        consumer_ids,
        msgs,
    }
}

/// Subscribe and consume until `expected` arrive or the drain goes quiet for
/// `RECV_IDLE`. Returns what actually arrived.
async fn drain(
    client: Client,
    stream_id: u32,
    consumer_id: u32,
    expected: u32,
    progress: Arc<AtomicU32>,
) -> u32 {
    let mut handle = client
        .subscribe(stream_id, consumer_id, b"")
        .await
        .expect("subscribe");
    let mut count = 0u32;
    while count < expected {
        match tokio::time::timeout(RECV_IDLE, handle.recv()).await {
            Ok(Some(_)) => {
                count += 1;
                progress.store(count, Ordering::Relaxed);
            }
            // Timeout, or the subscription ended early. Either way the
            // backlog stopped flowing; the caller asserts on the count.
            _ => break,
        }
    }
    count
}

fn assert_all_complete(got: &[u32], expected: u32, what: &str) {
    let short: Vec<String> = got
        .iter()
        .enumerate()
        .filter(|(_, &c)| c != expected)
        .map(|(i, &c)| format!("consumer {i}: {c}/{expected} (missing {})", expected - c))
        .collect();
    assert!(
        short.is_empty(),
        "{what}: {} of {STREAMS} consumers did not receive their full backlog — {}",
        short.len(),
        short.join(", ")
    );
}

/// All consumers subscribe at once. Every stream gains demand before the walk
/// gets far, so no stream should ever be walked past while it looks idle.
#[tokio::test(flavor = "multi_thread")]
async fn backlog_is_complete_when_all_consumers_subscribe_together() {
    let fx = fixture("together", Setup::invariant()).await;
    let mut js = tokio::task::JoinSet::new();
    for i in 0..STREAMS {
        let client = fx.readers[i].clone();
        let (sid, cid, msgs) = (fx.stream_ids[i], fx.consumer_ids[i], fx.msgs);
        js.spawn(async move { (i, drain(client, sid, cid, msgs, Arc::new(AtomicU32::new(0))).await) });
    }

    let mut got = vec![0u32; STREAMS];
    while let Some(res) = js.join_next().await {
        let (i, count) = res.expect("drain task");
        got[i] = count;
    }
    assert_all_complete(&got, fx.msgs, "simultaneous subscribe");
    drop(fx);
}

/// The siblings join WHILE the first consumer's walk is already in flight.
/// This is the interleaving the shared cursor is most exposed to: the walk is
/// mid-store, three of four streams still have zero demand, and their entries
/// are being skipped as the cursor moves.
#[tokio::test(flavor = "multi_thread")]
async fn backlog_is_complete_when_siblings_join_mid_walk() {
    let fx = fixture("midwalk", Setup::invariant()).await;
    let lead = Arc::new(AtomicU32::new(0));

    let mut js = tokio::task::JoinSet::new();
    {
        let client = fx.readers[0].clone();
        let (sid, cid, msgs, p) = (fx.stream_ids[0], fx.consumer_ids[0], fx.msgs, lead.clone());
        js.spawn(async move { (0usize, drain(client, sid, cid, msgs, p).await) });
    }

    // Wait for real progress rather than a fixed sleep: once the lead
    // consumer has messages, the drain is provably walking the store.
    let warmup = (fx.msgs / 10).max(1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while lead.load(Ordering::Relaxed) < warmup && tokio::time::Instant::now() < deadline {
        tokio::task::yield_now().await;
    }
    assert!(
        lead.load(Ordering::Relaxed) > 0,
        "lead consumer never received anything — the walk never started"
    );

    for i in 1..STREAMS {
        let client = fx.readers[i].clone();
        let (sid, cid, msgs) = (fx.stream_ids[i], fx.consumer_ids[i], fx.msgs);
        js.spawn(async move { (i, drain(client, sid, cid, msgs, Arc::new(AtomicU32::new(0))).await) });
    }

    let mut got = vec![0u32; STREAMS];
    while let Some(res) = js.join_next().await {
        let (i, count) = res.expect("drain task");
        got[i] = count;
    }
    assert_all_complete(&got, fx.msgs, "siblings joined mid-walk");
    drop(fx);
}

/// Strictly sequential late joiners: each consumer subscribes only after the
/// previous one has drained everything, so the cursor has already reached the
/// end of the store — every join must rewind it back.
#[tokio::test(flavor = "multi_thread")]
async fn backlog_is_complete_for_sequential_late_joiners() {
    let fx = fixture("sequential", Setup::invariant()).await;
    let mut got = vec![0u32; STREAMS];
    for i in 0..STREAMS {
        got[i] = drain(
            fx.readers[i].clone(),
            fx.stream_ids[i],
            fx.consumer_ids[i],
            fx.msgs,
            Arc::new(AtomicU32::new(0)),
        )
        .await;
    }
    assert_all_complete(&got, fx.msgs, "sequential late joiners");
    drop(fx);
}

/// Replica of the `4conn_4stream/2000000` throughput bench, which DOES stall:
/// its server config, its single shared prefill connection, and all consumers
/// subscribing at once with no synchronisation.
///
/// The scenarios above are green at the same message count, so if this one
/// fails, the cause is one of the knobs in `Setup::bench_parity` — drop them
/// one at a time until it goes green again and the last one removed is it.
///
///   ARBITRO_TEST_BACKLOG_MSGS=500000 cargo test -p arbitro-e2e \
///     --test backlog_delivery_invariants -- --ignored --nocapture
#[tokio::test(flavor = "multi_thread")]
#[ignore = "mirrors the throughput bench; needs ARBITRO_TEST_BACKLOG_MSGS=500000"]
async fn bench_parity_backlog_is_fully_delivered() {
    let fx = fixture("parity", Setup::bench_parity()).await;

    // No warm-up handshake: the bench spawns all four straight into a JoinSet
    // and lets them race, which is a less orderly interleaving than waiting
    // for the lead consumer to report progress.
    let mut js = tokio::task::JoinSet::new();
    for i in 0..STREAMS {
        let client = fx.readers[i].clone();
        let (sid, cid, msgs) = (fx.stream_ids[i], fx.consumer_ids[i], fx.msgs);
        js.spawn(async move { (i, drain(client, sid, cid, msgs, Arc::new(AtomicU32::new(0))).await) });
    }

    let mut got = vec![0u32; STREAMS];
    while let Some(res) = js.join_next().await {
        let (i, count) = res.expect("drain task");
        got[i] = count;
    }
    for (i, c) in got.iter().enumerate() {
        println!("consumer {i}: {c}/{}", fx.msgs);
    }
    assert_all_complete(&got, fx.msgs, "heavy backlog");
    drop(fx);
}
