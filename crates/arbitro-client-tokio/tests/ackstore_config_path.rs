//! The ackstore storage path is reachable from the normal client
//! configuration surface.
//!
//! `ClientConfig::ack_store` carries a `WalConfig`, so a caller declares *where
//! the dedup WAL lives* in the same struct it declares the broker address, and
//! plain `Client::connect` opens it. These tests pin that contract plus the
//! failure modes an operator can actually hit: an unusable directory, and two
//! clients aimed at one directory.


use arbitro_client_tokio::ackstore::WalConfig;
use arbitro_client_tokio::{Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Bind the socket HERE and hand it to the server, so there is nothing to
/// wait for: the port is listening before this returns, and it is never
/// released, so a sibling test cannot take it.
///
/// The previous version picked a port by binding, DROPPING the listener, and
/// returning the number — then slept 80ms hoping the server had bound it.
/// Both halves were races. Under the full suite the machine is loaded, 80ms
/// was not enough, and these tests failed with ConnectionRefused; the freed
/// port could also be claimed by a sibling test in the same binary. Same fix
/// as `service_e2e.rs`.
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

fn cfg_with_store(addr: &str, wal: Option<WalConfig>) -> ClientConfig {
    ClientConfig {
        addr: addr.to_string(),
        ack_store: wal,
        ..ClientConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_opens_the_wal_at_the_configured_dir() {
    let addr = start_server().await;
    let tmp = tempfile::tempdir().unwrap();
    // Nested + non-existent on purpose: the client must create the tree.
    let dir = tmp.path().join("var").join("lib").join("ackstore");

    let client = Client::connect(cfg_with_store(&addr, Some(WalConfig::new(&dir))))
        .await
        .expect("connect with configured ackstore dir");

    assert!(
        dir.join("ackstore.log").is_file(),
        "the WAL must materialize at the configured path, not somewhere derived"
    );
    client.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_config_opens_no_store_at_all() {
    let addr = start_server().await;
    let cwd_before: Vec<_> = std::fs::read_dir(".")
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.file_name()))
        .collect();

    let client = Client::connect(cfg_with_store(&addr, None))
        .await
        .expect("connect");
    client.close();

    // No ack_store configured => no files invented anywhere, least of all in
    // the working directory.
    let cwd_after: Vec<_> = std::fs::read_dir(".")
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.file_name()))
        .collect();
    assert_eq!(cwd_before.len(), cwd_after.len(), "cwd must be untouched");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_fails_fast_when_the_configured_dir_is_a_file() {
    let addr = start_server().await;
    let tmp = tempfile::tempdir().unwrap();
    let file = tmp.path().join("store");
    std::fs::write(&file, b"i am not a directory").unwrap();

    let err = Client::connect(cfg_with_store(&addr, Some(WalConfig::new(&file))))
        .await
        .expect_err("a file cannot be a store directory");
    let msg = err.to_string();
    assert!(
        msg.contains("ackstore") && msg.contains("not a directory"),
        "error must name the problem, got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_connection_releases_the_store_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("store");
    // A port that was just released: the store opens, then the dial is refused.
    let bad = format!("127.0.0.1:{}", portpicker());
    let mut cfg = cfg_with_store(&bad, Some(WalConfig::new(&dir)));
    cfg.reconnect.max_attempts = Some(1);
    Client::connect(cfg).await.expect_err("dial must fail");

    // The directory must be free again — otherwise a retry loop would turn one
    // transient network failure into a permanent `Locked`.
    let addr = start_server().await;
    let client = Client::connect(cfg_with_store(&addr, Some(WalConfig::new(&dir))))
        .await
        .expect("directory must be released after a failed connect");
    client.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_clients_on_one_dir_is_refused_not_silently_shared() {
    let addr = start_server().await;
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("shared");

    let first = Client::connect(cfg_with_store(&addr, Some(WalConfig::new(&dir))))
        .await
        .expect("first client takes the directory");

    let err = Client::connect(cfg_with_store(&addr, Some(WalConfig::new(&dir))))
        .await
        .expect_err("second client must be refused, not allowed to interleave writes");
    let msg = err.to_string();
    assert!(
        msg.contains("already open by another process"),
        "error must explain the single-writer rule, got: {msg}"
    );

    // Once the first client releases it, the directory is reusable.
    first.close();
    let second = Client::connect(cfg_with_store(&addr, Some(WalConfig::new(&dir))))
        .await
        .expect("directory is free after close");
    second.close();
}
