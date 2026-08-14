//! A consumer that subscribes to a stream which ALREADY holds messages must
//! receive them.
//!
//! The TypeScript suite reproduces a broker defect here: with the message
//! published first, roughly two rounds in five deliver nothing at all, while
//! the same rounds with the subscribe first deliver every time. This file
//! ports that repro so the failure can be chased in-process instead of
//! through a container.
//!
//! The TS probe cycles subscribe → close on a SINGLE client, which is what
//! its own header calls out, so these rounds share one connection too.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use bytes::Bytes;
use std::time::Duration;

const ROUNDS: usize = 10;
const RECV_TIMEOUT: Duration = Duration::from_secs(3);

struct Tally {
    publish_first_ok: usize,
    subscribe_first_ok: usize,
}

async fn run_rounds(server: &TestServer, teardown: bool) -> Tally {
    // One client for every round — the subscribe/close cycling happens on a
    // single connection, as in the TS probe.
    let client = server.connect().await;
    let mut tally = Tally {
        publish_first_ok: 0,
        subscribe_first_ok: 0,
    };

    for i in 0..ROUNDS {
        let publish_first = i % 2 == 0;
        let stream = format!("bl_{i}");
        let subject = format!("{stream}.a");

        let resp = client
            .create_stream(stream.as_bytes(), format!("{stream}.>").as_bytes(), 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap();
        let stream_id = TestServer::parse_id(&resp);

        if publish_first {
            client
                .publish_wait(stream_id, subject.as_bytes(), Bytes::from_static(b"hello"))
                .await
                .expect("publish");
        }

        let resp = client
            .create_consumer(
                stream_id,
                stream.as_bytes(),
                stream.as_bytes(),
                b"",
                10,
                1,
                0,
                0,
                0,
                0,
            )
            .await
            .unwrap();
        let consumer_id = TestServer::parse_id(&resp);
        let mut handle = client.subscribe(stream_id, consumer_id, b"").await.unwrap();

        if !publish_first {
            client
                .publish_wait(stream_id, subject.as_bytes(), Bytes::from_static(b"hello"))
                .await
                .expect("publish");
        }

        let got = matches!(
            tokio::time::timeout(RECV_TIMEOUT, handle.recv()).await,
            Ok(Some(_))
        );

        let label = if publish_first {
            "publish-then-subscribe"
        } else {
            "subscribe-then-publish"
        };
        println!(
            "round {i}: {label} consumerId={consumer_id} received={}",
            got as u8
        );
        if got {
            if publish_first {
                tally.publish_first_ok += 1;
            } else {
                tally.subscribe_first_ok += 1;
            }
        }

        // Closing the subscription is the cycle the TS probe exercises.
        drop(handle);
        if teardown {
            let _ = client.delete_consumer(consumer_id).await;
            let _ = client.delete_stream(stream.as_bytes()).await;
        }
    }
    tally
}

fn assert_all_delivered(tally: &Tally, what: &str) {
    let half = ROUNDS / 2;
    println!("publish-then-subscribe: {}/{half}", tally.publish_first_ok);
    println!("subscribe-then-publish: {}/{half}", tally.subscribe_first_ok);
    assert_eq!(
        tally.subscribe_first_ok, half,
        "{what}: the live push path lost messages, which is supposed to be solid"
    );
    assert_eq!(
        tally.publish_first_ok, half,
        "{what}: a consumer subscribing to an existing backlog got nothing in {} of {half} rounds",
        half - tally.publish_first_ok
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn backlog_survives_subscribe_close_cycling() {
    let server = TestServerBuilder::new().spawn().await;
    let tally = run_rounds(&server, false).await;
    assert_all_delivered(&tally, "no teardown");
}

/// Same rounds, but each deletes its consumer and stream, handing the next
/// round a recycled consumer id — what the TS probe does by default.
#[tokio::test(flavor = "multi_thread")]
async fn backlog_survives_recycled_consumer_ids() {
    let server = TestServerBuilder::new().spawn().await;
    let tally = run_rounds(&server, true).await;
    assert_all_delivered(&tally, "with teardown");
}

/// The container the TS suite talks to runs with a data directory, so the
/// store is on disk rather than purely in memory. Covers that difference.
#[tokio::test(flavor = "multi_thread")]
async fn backlog_survives_with_a_persistent_store() {
    let dir = std::env::temp_dir().join(format!("arbitro_backlog_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let server = TestServerBuilder::new()
        .data_dir(dir.to_str().unwrap())
        .spawn()
        .await;
    let tally = run_rounds(&server, true).await;
    let _ = std::fs::remove_dir_all(&dir);
    assert_all_delivered(&tally, "persistent store");
}
