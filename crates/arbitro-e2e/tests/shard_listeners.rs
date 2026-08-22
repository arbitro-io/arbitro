//! Per-shard listeners, and the topology frame that advertises them.
//!
//! The broker can open one extra socket per shard on an OS-assigned port,
//! on top of the fixed bootstrap address. `Action::ShardTopology` reports
//! the map. Three properties matter and each has a test here:
//!
//! 1. **The extra ports are real listeners**, not just numbers in a reply.
//!    A port that answers the topology frame but refuses a connection is
//!    worse than no feature at all — the client would dial it, fail, and
//!    have no way to tell a misconfigured broker from a network fault.
//! 2. **The single-listener deployment answers too**, reporting port 0.
//!    The client's "0 means keep using the address you dialed" rule only
//!    works if the frame is always answered; an `Unimplemented` here would
//!    force every client to special-case broker versions.
//! 3. **The reply is behind authentication.** The port map describes the
//!    deployment, so an unauthenticated socket must not be able to read
//!    it.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use bytes::Bytes;
use std::time::Duration;

const SHARDS: usize = 4;

/// Every shard gets its own port, the ports are distinct, and each one
/// actually accepts a connection that works end to end.
#[tokio::test(flavor = "multi_thread")]
async fn each_shard_listens_on_its_own_usable_port() {
    let mut server = TestServerBuilder::new()
        .shard_count(SHARDS)
        .shard_listeners(true)
        .spawn()
        .await;
    let client = server.connect().await;

    let topo = client.shard_topology().await.expect("topology");
    assert_eq!(topo.len(), SHARDS, "one entry per shard");

    let mut ports = Vec::new();
    for (shard, port) in &topo {
        assert_ne!(
            *port, 0,
            "shard {shard} reported no port while per-shard listeners are on"
        );
        ports.push(*port);
    }
    let unique: std::collections::HashSet<_> = ports.iter().collect();
    assert_eq!(
        unique.len(),
        SHARDS,
        "two shards share a port: {ports:?} — the OS cannot have assigned \
         the same one twice, so the list is built wrong"
    );

    // A port in the reply that does not accept is the failure mode worth
    // catching: the reply is generated from the same vector the sockets
    // were bound into, so an off-by-one or a partial bind shows up here
    // and nowhere else.
    for (shard, port) in &topo {
        let addr = format!("127.0.0.1:{port}");
        let direct = TestServer::connect_to(&addr).await;
        // A working connection, not just a completed handshake: create a
        // stream and read it back.
        let name = format!("via_shard_{shard}");
        let filter = format!("viashard{shard}.>");
        direct
            .create_stream(name.as_bytes(), filter.as_bytes(), 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .unwrap_or_else(|e| panic!("shard {shard} port {port} is not usable: {e}"));
        direct.close();
    }

    server.shutdown().await;
}

/// A connection must remember which shard's listener accepted it.
///
/// Without this the extra ports are indistinguishable doors to the same
/// path: the feeder hands the accept loop a socket and the shard is lost,
/// so nothing downstream can tell that a client dialed the shard it wanted.
/// Recording it is what makes the port mean something; it is a prerequisite
/// for any affinity, not affinity itself.
#[tokio::test(flavor = "multi_thread")]
async fn a_connection_remembers_the_listener_that_accepted_it() {
    let mut server = TestServerBuilder::new()
        .shard_count(SHARDS)
        .shard_listeners(true)
        .spawn()
        .await;

    // Bootstrap port: no shard of its own.
    let boot = server.connect().await;
    boot.create_stream(b"boot_probe", b"bootprobe.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("stream");
    let boot_shards: Vec<Option<u16>> = server.registry().all_listener_shards();
    assert_eq!(
        boot_shards,
        vec![None],
        "the bootstrap connection must record no shard, got {boot_shards:?}"
    );

    let topo = boot.shard_topology().await.expect("topology");
    boot.close();
    // Let the close land so the next assertions see only the new connection.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Each shard's own port: the connection records THAT shard.
    for (shard, port) in &topo {
        let direct = TestServer::connect_to(&format!("127.0.0.1:{port}")).await;
        let name = format!("probe_{shard}");
        let filter = format!("probe{shard}.>");
        direct
            .create_stream(name.as_bytes(), filter.as_bytes(), 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .expect("stream");

        let seen = server.registry().all_listener_shards();
        assert!(
            seen.contains(&Some(*shard)),
            "a connection accepted on shard {shard}'s port (port {port}) \
             recorded {seen:?} — the shard was lost between the listener \
             and the session"
        );
        direct.close();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    server.shutdown().await;
}

/// The bootstrap port keeps working while the per-shard ports exist. The
/// feature is additive; a client that never asks for the topology must not
/// notice it is on.
#[tokio::test(flavor = "multi_thread")]
async fn the_bootstrap_port_still_serves_everything() {
    let mut server = TestServerBuilder::new()
        .shard_count(SHARDS)
        .shard_listeners(true)
        .spawn()
        .await;
    let client = server.connect().await;

    let resp = client
        .create_stream(b"boot_stream", b"*.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("stream");
    let stream_id = TestServer::parse_id(&resp);

    let resp = client
        .create_consumer(
            stream_id,
            b"boot_worker",
            b"boot_worker",
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
            arbitro_client_tokio::BatchEntry::new(b"orders.created", Bytes::from_static(b"boot"))
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
    assert_eq!(got, N, "{got}/{N} delivered through the bootstrap port");

    server.shutdown().await;
}

/// With the feature off, the frame is still answered — with port 0 for
/// every shard. The client reads that as "no port of my own"; an error or
/// a dropped connection here would make the call unusable against any
/// broker that has not enabled the feature.
#[tokio::test(flavor = "multi_thread")]
async fn a_single_listener_broker_reports_zero_not_an_error() {
    let mut server = TestServerBuilder::new().shard_count(SHARDS).spawn().await;
    let client = server.connect().await;

    let topo = client.shard_topology().await.expect("topology");
    assert_eq!(topo.len(), SHARDS, "one entry per shard");
    for (shard, port) in &topo {
        assert_eq!(
            *port, 0,
            "shard {shard} reported port {port} with per-shard listeners off"
        );
    }

    server.shutdown().await;
}

/// Topology is deployment information. A broker that requires a token must
/// not answer this frame to a connection that has not presented one.
///
/// Note what this does NOT assert: that `connect` fails. It does not —
/// the TCP connection and the Hello both succeed, and the broker only
/// refuses once a frame arrives that is not Auth. Asserting on connect
/// would pass for the wrong reason on a broker that accepted the request
/// and answered it. The assertion is on the request itself.
#[tokio::test(flavor = "multi_thread")]
async fn topology_is_refused_without_a_token() {
    let mut server = TestServerBuilder::new()
        .shard_count(SHARDS)
        .shard_listeners(true)
        .auth_token("s3cret")
        .spawn()
        .await;

    let tokenless = arbitro_client_tokio::Client::connect(arbitro_client_tokio::ClientConfig {
        addr: server.addr.clone(),
        reconnect: arbitro_client_tokio::ReconnectPolicy {
            max_attempts: Some(1),
            ..Default::default()
        },
        ..Default::default()
    })
    .await
    .expect("the socket opens; the refusal comes at the first frame");

    let refused = tokenless.shard_topology().await;
    assert!(
        refused.is_err(),
        "an unauthenticated connection read the port map: {refused:?} — \
         the broker's topology is discoverable by anyone who can open a socket"
    );
    tokenless.close();

    // The same broker answers a client that does authenticate.
    let ok = arbitro_client_tokio::Client::connect(arbitro_client_tokio::ClientConfig {
        addr: server.addr.clone(),
        auth_token: Some("s3cret".to_string()),
        ..Default::default()
    })
    .await
    .expect("authenticated connect");
    let topo = ok.shard_topology().await.expect("topology");
    assert_eq!(topo.len(), SHARDS);
    assert!(
        topo.iter().all(|(_, p)| *p != 0),
        "authenticated client must get the real ports"
    );
    ok.close();

    server.shutdown().await;
}
