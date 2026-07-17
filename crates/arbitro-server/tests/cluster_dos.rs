//! Cluster transport DoS-hardening tests (D3).
//!
//! Covers the inbound resource limits of `TcpRaftTransport`:
//! * a forged oversize `body_len` is a fatal protocol error — the connection
//!   is closed and counted BEFORE any body byte is buffered, and the source
//!   IP is jailed at accept for the cooldown;
//! * legitimate large frames (max-size Raft consensus frame, a multi-MiB
//!   replication batch) still pass under the default cap;
//! * a TCP connect flood beyond `max_inbound_connections` is shed;
//! * a connection exceeding the optional frame-rate quota is dropped.
//!
//! Run with: `cargo test -p arbitro-server --features cluster`

#![cfg(feature = "cluster")]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use arbitro_raft::{RaftTransport, RAFT_FRAME_HEADER_SIZE};
use arbitro_server::cluster::security::ClusterSecurityConfig;
use arbitro_server::cluster::transport::{ClusterTransportLimits, TcpRaftTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Build a wire frame header (32 bytes) followed by `body` bytes.
/// Layout mirrors `RaftFrameHeader`: magic[0..4] version[4] kind[5]
/// flags[6..8] from[8..16] body_len[16..20] reserved[20..24] group_id[24..32].
fn frame(kind: u8, from: u64, body_len: u32, body: &[u8]) -> Vec<u8> {
    let mut f = vec![0u8; RAFT_FRAME_HEADER_SIZE];
    f[0..4].copy_from_slice(&0x5241_4654u32.to_le_bytes()); // RAFT_MAGIC
    f[4] = 0x01; // version
    f[5] = kind;
    f[8..16].copy_from_slice(&from.to_le_bytes());
    f[16..20].copy_from_slice(&body_len.to_le_bytes());
    f.extend_from_slice(body);
    f
}

fn any_local() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

async fn transport_with(limits: ClusterTransportLimits) -> TcpRaftTransport {
    TcpRaftTransport::new_with_limits(
        any_local(),
        HashMap::new(),
        ClusterSecurityConfig::plaintext(),
        limits,
    )
    .await
    .expect("bind transport")
}

/// Poll `probe` until it returns true or `ms` elapse.
async fn wait_for(ms: u64, mut probe: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
    while tokio::time::Instant::now() < deadline {
        if probe() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    probe()
}

/// (a) An oversize `body_len` is rejected as a fatal protocol error: the
/// connection is closed and counted without the forged length ever driving
/// an allocation, and the offender's IP is jailed at accept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversize_body_len_closes_connection_and_jails_source() {
    let receiver = transport_with(ClusterTransportLimits {
        max_frame_body_bytes: 1024 * 1024, // 1 MiB cap for the test
        jail_cooldown_ms: 60_000,          // long, deterministic
        ..ClusterTransportLimits::default()
    })
    .await;
    let addr = receiver.local_addr();

    let mut raw = tokio::net::TcpStream::connect(addr).await.expect("connect");
    // Claim a ~4 GiB body. We never send a single body byte — the header
    // alone must kill the connection.
    raw.write_all(&frame(3, 1, 0xFFFF_FF00, &[])).await.unwrap();

    assert!(
        wait_for(2_000, || receiver.oversize_frames_rejected() == 1).await,
        "oversize frame must be counted exactly once"
    );
    // The server closed its end: our read observes EOF (no data was ever
    // forwarded to the consensus channel either).
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), raw.read(&mut buf))
        .await
        .expect("server must close the connection")
        .unwrap_or(0);
    assert_eq!(n, 0, "connection must be closed after an oversize frame");

    // Reconnect from the same (jailed) IP: the accept loop drops us.
    let mut retry = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let _ = retry.write_all(&frame(3, 1, 0, &[])).await;
    assert!(
        wait_for(2_000, || receiver.jailed_accepts_rejected() >= 1).await,
        "reconnect from a jailed IP must be refused at accept"
    );
    assert!(
        receiver
            .recv_frame_timeout(Duration::from_millis(200), &mut vec![0u8; 4096])
            .await
            .expect("recv")
            .is_none(),
        "no frame from the jailed source may reach the Raft channel"
    );
}

/// (c) Legitimate large frames pass under the DEFAULT cap: a max-size Raft
/// consensus frame (64 KiB total — the crate's `MAX_FRAME_SIZE`) and a
/// multi-MiB data-plane replication batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legit_large_frames_pass_default_cap() {
    let receiver = transport_with(ClusterTransportLimits::default()).await;
    let repl_rx = receiver.take_replication_rx().expect("replication rx");
    let addr = receiver.local_addr();

    let mut raw = tokio::net::TcpStream::connect(addr).await.expect("connect");

    // Max legitimate Raft consensus frame: 64 KiB TOTAL (the peer's
    // inbound_buf size), i.e. body = 64 KiB - 32.
    let consensus_body = vec![0xABu8; 64 * 1024 - RAFT_FRAME_HEADER_SIZE];
    raw.write_all(&frame(3, 1, consensus_body.len() as u32, &consensus_body))
        .await
        .unwrap();
    let mut out = vec![0u8; 64 * 1024];
    let n = receiver
        .recv_frame_timeout(Duration::from_secs(2), &mut out)
        .await
        .expect("recv")
        .expect("max-size consensus frame must be delivered");
    assert_eq!(n, 64 * 1024);

    // Large-but-legitimate replication batch (kind 20): 8 MiB body.
    let repl_body = vec![0xCDu8; 8 * 1024 * 1024];
    raw.write_all(&frame(20, 1, repl_body.len() as u32, &repl_body))
        .await
        .unwrap();
    let mut repl_rx = repl_rx;
    let got = tokio::time::timeout(Duration::from_secs(5), repl_rx.recv())
        .await
        .expect("8 MiB replication frame must be delivered")
        .expect("replication channel open");
    assert_eq!(got.len(), RAFT_FRAME_HEADER_SIZE + repl_body.len());

    assert_eq!(receiver.oversize_frames_rejected(), 0);
    assert_eq!(receiver.rate_limited_disconnects(), 0);
}

/// A TCP connect flood beyond `max_inbound_connections` is shed at accept —
/// held connections keep their permit, the overflow is dropped and counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_cap_sheds_connect_flood() {
    let receiver = transport_with(ClusterTransportLimits {
        max_inbound_connections: 2,
        jail_cooldown_ms: 0, // isolate the cap from the jail
        ..ClusterTransportLimits::default()
    })
    .await;
    let addr = receiver.local_addr();

    // Keep 4 sockets alive; only 2 permits exist.
    let mut conns = Vec::new();
    for _ in 0..4 {
        conns.push(tokio::net::TcpStream::connect(addr).await.expect("connect"));
    }
    assert!(
        wait_for(2_000, || receiver.connections_rejected_over_cap() == 2).await,
        "exactly the two over-cap connections must be refused, got {}",
        receiver.connections_rejected_over_cap()
    );
}

/// A connection exceeding the (opt-in) per-connection frame-rate quota is
/// dropped and counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frame_rate_quota_disconnects_flooder() {
    let receiver = transport_with(ClusterTransportLimits {
        max_frames_per_conn_per_sec: 10,
        jail_cooldown_ms: 0,
        ..ClusterTransportLimits::default()
    })
    .await;
    let addr = receiver.local_addr();

    let mut raw = tokio::net::TcpStream::connect(addr).await.expect("connect");
    // Blast 50 empty-body frames in one burst — far beyond 10/sec.
    let mut burst = Vec::new();
    for _ in 0..50 {
        burst.extend_from_slice(&frame(3, 1, 0, &[]));
    }
    raw.write_all(&burst).await.unwrap();

    assert!(
        wait_for(2_000, || receiver.rate_limited_disconnects() == 1).await,
        "the flooding connection must be dropped once"
    );
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), raw.read(&mut buf))
        .await
        .expect("server must close the connection")
        .unwrap_or(0);
    assert_eq!(n, 0, "connection must be closed after the quota breach");
}
