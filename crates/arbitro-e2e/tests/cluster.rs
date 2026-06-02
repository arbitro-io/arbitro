//! Cluster integration tests.
//!
//! These tests verify that the server boots correctly with the `cluster`
//! feature enabled and that basic operations still work. The Raft propose
//! path is not wired into dispatch yet, so metadata operations still go
//! through the local shard path.

#![cfg(feature = "cluster")]

mod test_helper;
use test_helper::TestServer;

use std::time::Duration;

/// Verify that a server with cluster config boots without panicking and
/// that basic client operations (create stream, list streams) still work
/// with the cluster feature compiled in.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_server_boots_and_serves() {
    // Pick dynamic ports for both the client listener and the Raft listener.
    let client_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client_addr = client_listener.local_addr().unwrap().to_string();
    drop(client_listener);

    let raft_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let raft_addr = raft_listener.local_addr().unwrap();
    drop(raft_listener);

    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_str().unwrap().to_string();

    let (tx, rx) = tokio::sync::watch::channel(false);

    // Build a Config with cluster fields set — single-node cluster (node 1,
    // peers = [self]). This exercises the full Raft boot path without
    // requiring multiple processes.
    let mut config = arbitro_server::Config::default()
        .listen_addr(&client_addr)
        .shard_count(2)
        .shutdown_timeout(Duration::from_millis(50))
        .data_dir(&data_dir);
    config.cluster_node_id = 1;
    config.cluster_listen = raft_addr.to_string();
    config.cluster_peers = vec![(1, raft_addr.to_string())];

    let server = arbitro_server::ArbitroServer::new(config);
    let handle = tokio::spawn(async move {
        let _ = server.run_with_shutdown(rx).await;
    });

    // Give the server + Raft node time to start.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connect a client and perform basic operations.
    let client = TestServer::connect_to(&client_addr).await;

    // Create a stream — goes through local shard path (Raft propose not wired yet).
    let resp = client
        .create_stream(b"orders", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .unwrap();
    let _stream_id = TestServer::parse_id(&resp);

    // List streams — should see the one we just created.
    let resp = client.list_streams(0, 1000).await.unwrap();
    assert_eq!(TestServer::stream_count(&resp), 1, "expected 1 stream after create");

    // Shutdown.
    let _ = tx.send(true);
    handle.await.expect("server task panicked");
}
