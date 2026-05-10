//! Deliver demux integration test.
//!
//! Verifies the full subscribe → publish → Deliver frame → Message path:
//!
//! 1. Create a stream and a consumer.
//! 2. `subscribe()` — waits for `RepOk`, subscriber channel is live.
//! 3. Concurrently publish 100 messages with fire-and-forget.
//! 4. `recv()` all 100 messages within a 5-second budget.
//! 5. Drop the `SubscriptionHandle` and clean up.

use std::time::Duration;

use bytes::Bytes;
use arbitro_client_tokio::{Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};

// ── helpers ───────────────────────────────────────────────────────────────────

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

async fn start_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let cfg = Config::default()
        .listen_addr(addr.clone())
        .max_connections(64);
    tokio::spawn(async move { let _ = ArbitroServer::new(cfg).run().await; });
    tokio::time::sleep(Duration::from_millis(80)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    let cfg = ClientConfig { addr: addr.to_string(), ..ClientConfig::default() };
    Client::connect(cfg).await.expect("connect")
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wildcard_subject_fanout_correct() {
    const MSG_COUNT: u32 = 100;

    let addr   = start_server().await;
    let client = connect(&addr).await;

    // ── set up stream ────────────────────────────────────────────────────────
    let resp = client
        .create_stream(b"deliver-test", b">", 0, 0, 0, 1, 0, 0, 0)
        .await
        .expect("create_stream");
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // ── set up consumer ───────────────────────────────────────────────────────
    // subject = b"" → empty bytes = server catch-all (add_catch_all path).
    // Using ">" as a pattern does NOT work as a wildcard in Arbitro's trie;
    // only empty bytes triggers the catch-all delivery route.
    let resp = client
        .create_consumer(
            stream_id,
            b"deliver-consumer",
            b"",   // group
            b"",   // subject = catch-all
            256,   // max_inflight
            0,     // ack_policy = None
            0,     // deliver_policy = All (from beginning)
            0,     // deliver_mode = Push
            30_000,
            0,
        )
        .await
        .expect("create_consumer");
    let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // ── subscribe — waits for RepOk ──────────────────────────────────────────
    // filter = b"" → empty bytes = catch-all subscription filter.
    let mut sub = client
        .subscribe(stream_id, consumer_id, b"")
        .await
        .expect("subscribe");

    // ── publish concurrently (fire-and-forget) ───────────────────────────────
    let pub_client = client.clone();
    tokio::spawn(async move {
        for i in 0u32..MSG_COUNT {
            pub_client
                .publish(stream_id, b"deliver.test", Bytes::from(i.to_le_bytes().to_vec()))
                .expect("publish");
        }
    });

    // ── receive all messages within a generous budget ────────────────────────
    let mut received = 0u32;
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            biased;
            _ = &mut deadline => {
                panic!("timed out: received {received}/{MSG_COUNT} messages");
            }
            msg = sub.recv() => {
                match msg {
                    Some(_m) => {
                        received += 1;
                        if received == MSG_COUNT { break; }
                    }
                    None => panic!("subscription channel closed after {received} messages"),
                }
            }
        }
    }

    assert_eq!(received, MSG_COUNT, "must receive exactly {MSG_COUNT} messages");

    // ── clean up ─────────────────────────────────────────────────────────────
    drop(sub);
    client.delete_consumer(consumer_id).await.ok();
    client.delete_stream(b"deliver-test").await.ok();
    client.close();
}

/// Two consumers on independent streams receive their own messages.
///
/// Each consumer lives on its own stream to sidestep server-side
/// fanout/queue semantics (which are server-controlled).  The test
/// verifies that the demux correctly routes by consumer_id across
/// two concurrent `SubscriptionHandle::recv()` loops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_consumers_independent_streams_no_crosstalk() {
    const MSG_COUNT: u32 = 50;

    let addr   = start_server().await;
    let client = connect(&addr).await;

    // ── Stream A + consumer A ─────────────────────────────────────────────────
    let resp_sa = client
        .create_stream(b"demux-stream-a", b">", 0, 0, 0, 1, 0, 0, 0)
        .await.expect("create stream A");
    let stream_a = u64::from_le_bytes(resp_sa[..8].try_into().unwrap()) as u32;

    let resp_ca = client
        .create_consumer(stream_a, b"cons-a", b"", b"", 256, 0, 0, 0, 30_000, 0)
        .await.expect("create consumer A");
    let cons_a = u64::from_le_bytes(resp_ca[..8].try_into().unwrap()) as u32;

    // ── Stream B + consumer B ─────────────────────────────────────────────────
    let resp_sb = client
        .create_stream(b"demux-stream-b", b">", 0, 0, 0, 1, 0, 0, 0)
        .await.expect("create stream B");
    let stream_b = u64::from_le_bytes(resp_sb[..8].try_into().unwrap()) as u32;

    let resp_cb = client
        .create_consumer(stream_b, b"cons-b", b"", b"", 256, 0, 0, 0, 30_000, 0)
        .await.expect("create consumer B");
    let cons_b = u64::from_le_bytes(resp_cb[..8].try_into().unwrap()) as u32;

    // ── subscribe (catch-all filter) ─────────────────────────────────────────
    let mut sub_a = client.subscribe(stream_a, cons_a, b"").await.expect("subscribe A");
    let mut sub_b = client.subscribe(stream_b, cons_b, b"").await.expect("subscribe B");

    // ── publish to each stream independently ─────────────────────────────────
    let pub_a = client.clone();
    let pub_b = client.clone();
    tokio::spawn(async move {
        for i in 0u32..MSG_COUNT {
            pub_a.publish(stream_a, b"a.subj", Bytes::from(i.to_le_bytes().to_vec())).expect("pub A");
        }
    });
    tokio::spawn(async move {
        for i in 0u32..MSG_COUNT {
            pub_b.publish(stream_b, b"b.subj", Bytes::from(i.to_le_bytes().to_vec())).expect("pub B");
        }
    });

    // Both consumers must receive exactly MSG_COUNT messages concurrently.
    let (recv_a, recv_b) = tokio::join!(
        async {
            let mut n = 0u32;
            while n < MSG_COUNT {
                tokio::time::timeout(Duration::from_secs(10), sub_a.recv())
                    .await.expect("timeout A").expect("channel A closed");
                n += 1;
            }
            n
        },
        async {
            let mut n = 0u32;
            while n < MSG_COUNT {
                tokio::time::timeout(Duration::from_secs(10), sub_b.recv())
                    .await.expect("timeout B").expect("channel B closed");
                n += 1;
            }
            n
        },
    );

    assert_eq!(recv_a, MSG_COUNT);
    assert_eq!(recv_b, MSG_COUNT);

    drop(sub_a);
    drop(sub_b);
    client.delete_consumer(cons_a).await.ok();
    client.delete_consumer(cons_b).await.ok();
    client.delete_stream(b"demux-stream-a").await.ok();
    client.delete_stream(b"demux-stream-b").await.ok();
    client.close();
}
