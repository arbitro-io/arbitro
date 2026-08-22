//! End-to-end proof of the on-connect ackstore purge: WAL entries recorded by
//! a session that DIED before broker confirmation (its `AckBatchResp` never
//! arrived) are purged against the server's consumer cursor when a new session
//! (re)connects — entries at or below the cursor are dropped, entries above it
//! are untouched.
//!
//! Session 1 subscribes with dedup and acks every message: each seq is
//! recorded into the WAL but nothing ever confirms it (normal acks are
//! fire-and-forget — only `AckBatchResp`/`AckStateRep` purge, and an idle
//! consumer never sees either). The client closes with all entries still live,
//! simulating the dead-session gap. An extra entry ABOVE the server cursor is
//! planted directly in the WAL (an ack the server never received). Session 2
//! reopens the SAME WAL and subscribes: the subscribe-time `AckStateReq` asks
//! the broker for its cursor and the `AckStateRep` handler drops everything at
//! or below it — and ONLY that.

use std::sync::Arc;
use std::time::Duration;

use arbitro_client_tokio::ackstore::wal::Wal;
use arbitro_client_tokio::ackstore::{Store, WalConfig};
use arbitro_client_tokio::{Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;

/// Bind the socket HERE and hand it to the server, so there is nothing to
/// wait for: the port is listening before this returns, and it is never
/// released, so a sibling test cannot take it.
///
/// The previous version picked a port by binding, DROPPING the listener, and
/// returning the number — then slept 80ms hoping the server had bound it.
/// Both halves were races: under the full suite the machine is loaded, 80ms
/// was not enough, and the client hit ConnectionRefused; the freed port could
/// also be claimed by a sibling test in the same binary.
async fn start_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local_addr").to_string();
    let cfg = Config::default()
        .listen_addr(addr.clone())
        .max_connections(64);
    let mut server = ArbitroServer::new(cfg);
    server.set_listener(listener);
    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            panic!("test server exited: {e}");
        }
    });
    addr
}

fn open_wal(wal_dir: &std::path::Path) -> Arc<Wal> {
    let mut wcfg = WalConfig::new(wal_dir);
    wcfg.fsync = true;
    Wal::open(wcfg).expect("open wal")
}

async fn connect_with(addr: &str, wal: Arc<Wal>) -> Client {
    let cfg = ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    };
    Client::connect_with_ackstore(cfg, wal)
        .await
        .expect("connect with ackstore")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_purges_ackstore_against_server_cursor() {
    const TOTAL: u32 = 10;
    const STREAM: &str = "ackstore-connect-purge";
    const CONSUMER: &str = "worker";
    /// Planted entry ABOVE the server cursor — an ack the server never saw.
    /// It must survive the purge.
    const ORPHAN_SEQ: u64 = 10_000;

    let addr = start_server().await;
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");

    // ── session 1: process + ack, then die without broker confirmation ──
    let c1 = connect_with(&addr, open_wal(&wal_dir)).await;

    let resp = c1
        .create_stream(STREAM.as_bytes(), b"ackstore-connect-purge.job", 100_000, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream");
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // Durable explicit-ack consumer: acks advance the server's consumer
    // cursor, and the consumer (with its cursor) outlives the session.
    let resp = c1
        .create_consumer(
            stream_id,
            CONSUMER.as_bytes(),
            CONSUMER.as_bytes(), // group = consumer name (empty group rejected)
            b"",                 // subject = catch-all
            1000,                // max_inflight
            1,                   // ack_policy = Explicit
            0,                   // deliver_policy = All
            0,                   // deliver_mode = Push
            30_000,
            0,
        )
        .await
        .expect("create_consumer");
    let consumer_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    for i in 0u32..TOTAL {
        c1.publish_wait(
            stream_id,
            b"ackstore-connect-purge.job",
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
                m.ack(); // records seq into the WAL + acks the broker
                processed += 1;
            }
        }
    }

    // Let the ack-batcher flush the acks to the broker and the periodic WAL
    // sync persist the recorded seqs, then close. NOTE: nothing confirms the
    // WAL entries — every one of the TOTAL seqs is still live.
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(sub1);
    c1.close();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ── plant an orphan entry above the server cursor ──
    // Simulates an ack recorded locally that never reached the broker: it is
    // NOT covered by the server's cursor and must survive the purge.
    {
        let wal = open_wal(&wal_dir);
        wal.restore().expect("restore");
        let slot = wal.slot(STREAM, CONSUMER).expect("slot");
        slot.record(ORPHAN_SEQ).expect("record orphan");
        wal.sync().expect("sync");
        wal.close().expect("close");
    }

    // Sanity: the WAL now holds TOTAL live entries + the orphan.
    {
        let wal = open_wal(&wal_dir);
        wal.restore().expect("restore");
        let info = wal.slot_info(STREAM, CONSUMER).expect("slot info");
        assert_eq!(
            info.live,
            TOTAL as usize + 1,
            "precondition: all recorded entries must still be live (unconfirmed)"
        );
        wal.close().expect("close");
    }

    // ── session 2: reconnect against the same WAL — the purge must fire ──
    let wal2 = open_wal(&wal_dir);
    let c2 = connect_with(&addr, Arc::clone(&wal2)).await;

    // Same durable consumer (still exists server-side, cursor = TOTAL).
    let mut _sub2 = c2
        .subscribe_dedup(STREAM, CONSUMER, stream_id, consumer_id, b"")
        .await
        .expect("subscribe_dedup 2");

    // The subscribe-time AckStateReq → AckStateRep round-trip is async;
    // poll the store until the purge lands (or time out).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let final_info = loop {
        let info = wal2.slot_info(STREAM, CONSUMER).expect("slot info 2");
        if info.live == 1 {
            break info;
        }
        if std::time::Instant::now() > deadline {
            break info;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(
        final_info.live, 1,
        "entries at or below the server cursor ({TOTAL}) must be purged on \
         connect; got {} live entries (min={}, max={})",
        final_info.live, final_info.min_seq, final_info.max_seq
    );
    assert_eq!(
        final_info.min_seq, ORPHAN_SEQ,
        "the entry above the server cursor must be untouched"
    );

    c2.close();
}
