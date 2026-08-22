//! Shutdown smoke test.
//!
//! Verifies that `client.close()` promptly cancels all background tasks
//! within 500 ms, even when concurrent publisher tasks are running.

use std::time::Duration;

use arbitro_client_tokio::{Client, ClientConfig, ClientError};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;

// ── helpers ───────────────────────────────────────────────────────────────────

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

async fn connect(addr: &str) -> Client {
    let cfg = ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    };
    Client::connect(cfg).await.expect("connect")
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Spawn 4 publisher loops, call `close()`, verify all loops exit within 500 ms.
///
/// Each loop calls `publish()` + `yield_now()` in a cycle.  Once the writer
/// task is cancelled and the MPSC consumer is dropped, `publish()` returns
/// `Err(ChannelClosed)` and the loop exits.  If any loop hangs, the test
/// fails via timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_client_cancels_all_tasks_under_500ms() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    let resp = client
        .create_stream(b"shutdown-test", b"shutdown.subj", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("create_stream");
    let stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // Spawn 4 publisher tasks that loop until they see ChannelClosed.
    let mut handles = Vec::new();
    for _ in 0..4 {
        let c = client.clone();
        let h = tokio::spawn(async move {
            let mut i = 0u32;
            loop {
                match c.publish(
                    stream_id,
                    b"shutdown.subj",
                    Bytes::from(i.to_le_bytes().to_vec()),
                ) {
                    Ok(()) => {}
                    Err(ClientError::ChannelClosed) => break, // cancel propagated
                    Err(e) => panic!("unexpected publish error: {e}"),
                }
                i = i.wrapping_add(1);
                // Yield to the scheduler so the cancel token can be observed.
                tokio::task::yield_now().await;
            }
        });
        handles.push(h);
    }

    // Let the publisher tasks run for a moment.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let start = std::time::Instant::now();
    client.close(); // cancel token fires → writer task exits → consumer dropped

    // All publisher tasks must exit within 500 ms.
    let join_all = async {
        for h in handles {
            h.await.expect("publisher task panicked");
        }
    };
    tokio::time::timeout(Duration::from_millis(500), join_all)
        .await
        .expect("tasks did not exit within 500 ms after close()");

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "close() propagation took {elapsed:?}, expected < 500 ms"
    );
}

/// Calling `close()` multiple times must be idempotent — no panic, no deadlock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_is_idempotent() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    client.close();
    client.close(); // idempotent
    client.close(); // again — must not panic
}
