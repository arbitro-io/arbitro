//! End-to-end proof of the durable WAL ackstore: a message processed + acked
//! before a client restart is NOT re-processed after reconnecting against the
//! SAME on-disk WAL, even though the broker redelivers it.
//!
//! Session 1 subscribes with dedup, drains + acks every message (recording each
//! seq into the WAL), then closes (final fsync). Session 2 reopens the SAME WAL
//! directory: the broker redelivers the messages, but the persistent dedup slot
//! recognizes them and the handler must not run again — the redeliveries are
//! silently re-acked and counted as `dedup_hits`.

use std::time::Duration;

use arbitro_client_tokio::ackstore::wal::Wal;
use arbitro_client_tokio::ackstore::WalConfig;
use arbitro_client_tokio::{Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;

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
    tokio::spawn(async move {
        let _ = ArbitroServer::new(cfg).run().await;
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    addr
}

async fn connect_with_wal(addr: &str, wal_dir: &std::path::Path) -> Client {
    let cfg = ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    };
    let mut wcfg = WalConfig::new(wal_dir);
    wcfg.fsync = true; // durable on sync()/close()
    let wal = Wal::open(wcfg).expect("open wal");
    Client::connect_with_ackstore(cfg, wal)
        .await
        .expect("connect with ackstore")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_dedup_survives_restart() {
    const TOTAL: u32 = 100;
    const STREAM: &str = "ackstore-persist";
    const CONSUMER: &str = "worker";

    let addr = start_server().await;
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");

    // ── session 1: process + ack 100 messages, record acks durably, close ──
    let c1 = connect_with_wal(&addr, &wal_dir).await;

    let resp = c1
        .create_stream(STREAM.as_bytes(), b">", 100_000, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream");
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // AckPolicy::None (0) + DeliverPolicy::All (0): every fresh subscription
    // re-reads the stream from the beginning, giving us deterministic
    // redelivery in session 2 without depending on ack-confirmation timing.
    let resp = c1
        .create_consumer(
            stream_id,
            CONSUMER.as_bytes(),
            b"",  // group
            b"",  // subject = catch-all
            1000, // max_inflight
            0,    // ack_policy = None
            0,    // deliver_policy = All
            0,    // deliver_mode = Push
            30_000,
            0,
        )
        .await
        .expect("create_consumer");
    let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // Publish BEFORE subscribing so all messages are on the stream.
    for i in 0u32..TOTAL {
        c1.publish_wait(
            stream_id,
            b"ackstore-persist.job",
            Bytes::from(i.to_le_bytes().to_vec()),
        )
        .await
        .expect("publish_wait");
    }

    let mut sub1 = c1
        .subscribe_dedup(STREAM, CONSUMER, stream_id, consumer_id, b"")
        .await
        .expect("subscribe_dedup 1");

    let mut processed = 0u32;
    let deadline = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(deadline);
    while processed < TOTAL {
        tokio::select! {
            biased;
            _ = &mut deadline => panic!("session1 timeout at {processed}/{TOTAL}"),
            msg = sub1.recv() => {
                let m = msg.expect("channel 1 closed");
                m.ack(); // records seq into the WAL + acks broker (no-op for AckNone)
                processed += 1;
            }
        }
    }
    assert_eq!(processed, TOTAL, "session 1 must process all messages");

    // Let the ack-batcher's periodic WAL sync run, flush the WAL.
    tokio::time::sleep(Duration::from_millis(250)).await;
    drop(sub1);

    // Delete the consumer. This is the exact scenario the durable dedup key
    // `(stream_name, consumer_name, seq)` is designed for: a consumer's
    // numeric id is ephemeral, but the WORK it represents is not. A worker
    // recreated under the SAME name must not redo already-completed jobs.
    c1.delete_consumer(consumer_id)
        .await
        .expect("delete consumer");
    c1.close(); // final WAL sync + fsync + close
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ── session 2: reopen the SAME WAL, recreate the consumer fresh. The
    // broker redelivers every message from the start (new consumer, deliver
    // policy All); the restored dedup slot must recognize them all. ──
    let c2 = connect_with_wal(&addr, &wal_dir).await;

    // Fresh consumer, SAME durable name → new consumer_id, same WAL slot.
    // deliver_policy = All (0) so the broker replays from seq 1.
    let resp = c2
        .create_consumer(
            stream_id,
            CONSUMER.as_bytes(),
            b"",
            b"",
            1000,
            0, // ack_policy = None
            0, // deliver_policy = All
            0,
            30_000,
            0,
        )
        .await
        .expect("re-create_consumer");
    let consumer_id2 = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    let mut sub2 = c2
        .subscribe_dedup(STREAM, CONSUMER, stream_id, consumer_id2, b"")
        .await
        .expect("subscribe_dedup 2");

    // Drain whatever the broker redelivers for a bounded window. Any message
    // that reaches the handler here is a dedup MISS.
    let mut reprocessed = 0u32;
    let drain = tokio::time::sleep(Duration::from_secs(3));
    tokio::pin!(drain);
    loop {
        tokio::select! {
            biased;
            _ = &mut drain => break,
            msg = sub2.recv() => match msg {
                Some(m) => { reprocessed += 1; m.ack(); }
                None => break,
            },
        }
    }

    let dedup_hits = c2.metrics().snapshot().dedup_hits;
    eprintln!("session2: reprocessed={reprocessed} dedup_hits={dedup_hits}");

    assert_eq!(
        reprocessed, 0,
        "persistent dedup failed: {reprocessed} messages reprocessed after restart"
    );
    // The redelivered messages must have been caught by the durable dedup slot.
    assert_eq!(
        dedup_hits as u32, TOTAL,
        "expected all {TOTAL} redeliveries to be deduped, got {dedup_hits}"
    );

    drop(sub2);
    c2.close();
}
