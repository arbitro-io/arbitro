//! Cluster transport security tests (D1 mTLS + D2 peer identity binding).
//!
//! Run with: `cargo test -p arbitro-server --features cluster-tls`
//! (the plaintext strict_from test also runs with just `--features cluster`).

#![cfg(feature = "cluster")]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use arbitro_raft::{PeerId, RaftTransport, RAFT_FRAME_HEADER_SIZE};
use arbitro_server::cluster::security::ClusterSecurityConfig;
use arbitro_server::cluster::transport::TcpRaftTransport;

/// Build a minimal, wire-valid Raft frame with an empty body and the given
/// claimed sender id. Layout mirrors `RaftFrameHeader`:
/// magic[0..4] version[4] kind[5] flags[6..8] from[8..16] body_len[16..20]
/// reserved[20..24] group_id[24..32].
fn frame_from(from: u64) -> Vec<u8> {
    let mut f = vec![0u8; RAFT_FRAME_HEADER_SIZE];
    f[0..4].copy_from_slice(&0x5241_4654u32.to_le_bytes()); // RAFT_MAGIC
    f[4] = 0x01; // version
    f[5] = 1; // kind = RequestVote (any consensus kind < 20)
    f[8..16].copy_from_slice(&from.to_le_bytes());
    // body_len = 0, reserved = 0, group_id = 0
    f
}

fn any_local() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

async fn recv_with_timeout(t: &TcpRaftTransport, ms: u64) -> Option<Vec<u8>> {
    let mut buf = vec![0u8; 64 * 1024];
    match t
        .recv_frame_timeout(Duration::from_millis(ms), &mut buf)
        .await
    {
        Ok(Some(n)) => Some(buf[..n].to_vec()),
        Ok(None) => None,
        Err(e) => panic!("recv error: {e:?}"),
    }
}

// ── D2 plaintext: address-based `from` binding ───────────────────────────────

/// strict_from mode: a frame claiming an id that is not configured for the
/// remote IP is dropped and counted; a legitimate id goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plaintext_strict_from_drops_unknown_id() {
    let receiver = TcpRaftTransport::new_with_security(
        any_local(),
        // Peer 2 is configured at a 127.0.0.1 address, so connections from
        // 127.0.0.1 may claim id 2 — and nothing else.
        HashMap::from([(PeerId(2), "127.0.0.1:1".parse().unwrap())]),
        ClusterSecurityConfig {
            #[cfg(feature = "cluster-tls")]
            tls: None,
            strict_from: true,
        },
    )
    .await
    .expect("bind receiver");
    let addr = receiver.local_addr();

    use tokio::io::AsyncWriteExt;
    let mut raw = tokio::net::TcpStream::connect(addr).await.expect("connect");

    // Spoof: claim id 99 (not a configured peer at all).
    raw.write_all(&frame_from(99)).await.unwrap();
    assert!(
        recv_with_timeout(&receiver, 500).await.is_none(),
        "spoofed frame (from=99) must be dropped"
    );

    // Legitimate: claim id 2 (configured for 127.0.0.1).
    raw.write_all(&frame_from(2)).await.unwrap();
    let got = recv_with_timeout(&receiver, 2000)
        .await
        .expect("legitimate frame (from=2) must be delivered");
    assert_eq!(u64::from_le_bytes(got[8..16].try_into().unwrap()), 2);

    assert_eq!(
        receiver.spoofed_frames_rejected(),
        1,
        "exactly the spoofed frame must be counted"
    );
}

/// Default (no security config): behavior is unchanged — any `from` passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plaintext_legacy_mode_unchanged() {
    let receiver = TcpRaftTransport::new(any_local(), HashMap::new())
        .await
        .expect("bind receiver");
    let addr = receiver.local_addr();

    use tokio::io::AsyncWriteExt;
    let mut raw = tokio::net::TcpStream::connect(addr).await.expect("connect");
    raw.write_all(&frame_from(42)).await.unwrap();
    assert!(
        recv_with_timeout(&receiver, 2000).await.is_some(),
        "legacy plaintext path must not filter frames"
    );
    assert_eq!(receiver.spoofed_frames_rejected(), 0);
}

// ── D1 + D2 over mTLS ────────────────────────────────────────────────────────

#[cfg(feature = "cluster-tls")]
mod mtls {
    use super::*;
    use std::path::Path;

    /// Mint a CA and per-node certs (SAN `peer-<id>`), write PEMs into `dir`,
    /// and return `ClusterSecurityConfig`s for the requested node ids.
    struct TestPki {
        ca_pem: String,
        ca: rcgen::Certificate,
        ca_key: rcgen::KeyPair,
    }

    impl TestPki {
        fn new(name: &str) -> Self {
            let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
            params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, name);
            let ca_key = rcgen::KeyPair::generate().unwrap();
            let ca = params.self_signed(&ca_key).unwrap();
            Self {
                ca_pem: ca.pem(),
                ca,
                ca_key,
            }
        }

        /// Write CA + a leaf cert for identity `peer-<id>` into `dir`,
        /// returning a TLS-enabled security config pointing at them.
        fn node_config(&self, dir: &Path, id: u64) -> ClusterSecurityConfig {
            let key = rcgen::KeyPair::generate().unwrap();
            let params = rcgen::CertificateParams::new(vec![format!("peer-{id}")]).unwrap();
            let cert = params.signed_by(&key, &self.ca, &self.ca_key).unwrap();

            let cert_path = dir.join(format!("node{id}.crt"));
            let key_path = dir.join(format!("node{id}.key"));
            let ca_path = dir.join("ca.crt");
            std::fs::write(&cert_path, cert.pem()).unwrap();
            std::fs::write(&key_path, key.serialize_pem()).unwrap();
            std::fs::write(&ca_path, &self.ca_pem).unwrap();

            ClusterSecurityConfig {
                tls: Some(arbitro_server::cluster::security::ClusterTlsConfig {
                    cert_path: cert_path.to_string_lossy().into_owned(),
                    key_path: key_path.to_string_lossy().into_owned(),
                    ca_path: ca_path.to_string_lossy().into_owned(),
                    peer_map: HashMap::new(),
                }),
                strict_from: false,
            }
        }
    }

    /// D1: two nodes with certs from the same cluster CA exchange a frame
    /// over mutual TLS, end to end through the production transport.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mtls_roundtrip_between_two_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let pki = TestPki::new("arbitro-test-ca");

        // Node 2: receiver. Its peer table lists node 1 (address unused for
        // receiving).
        let node2 = TcpRaftTransport::new_with_security(
            any_local(),
            HashMap::from([(PeerId(1), "127.0.0.1:1".parse().unwrap())]),
            pki.node_config(dir.path(), 2),
        )
        .await
        .expect("bind node2");
        let node2_addr = node2.local_addr();

        // Node 1: sender, dials node 2 with TLS (server name `peer-2`).
        let node1 = TcpRaftTransport::new_with_security(
            any_local(),
            HashMap::from([(PeerId(2), node2_addr)]),
            pki.node_config(dir.path(), 1),
        )
        .await
        .expect("bind node1");

        node1
            .send_frame_owned(PeerId(2), bytes::Bytes::from(frame_from(1)))
            .await
            .expect("TLS send must succeed");

        let got = recv_with_timeout(&node2, 5000)
            .await
            .expect("frame must arrive over mTLS");
        assert_eq!(u64::from_le_bytes(got[8..16].try_into().unwrap()), 1);
        assert_eq!(node2.spoofed_frames_rejected(), 0);
    }

    /// D1: a client with a certificate from a DIFFERENT CA (and a raw
    /// plaintext client) are both refused — nothing reaches the Raft channel.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mtls_rejects_untrusted_and_plaintext_clients() {
        let dir = tempfile::tempdir().unwrap();
        let pki = TestPki::new("arbitro-test-ca");

        let node2 = TcpRaftTransport::new_with_security(
            any_local(),
            HashMap::from([(PeerId(1), "127.0.0.1:1".parse().unwrap())]),
            pki.node_config(dir.path(), 2),
        )
        .await
        .expect("bind node2");
        let node2_addr = node2.local_addr();

        // Rogue node: valid-looking cert for `peer-1`, but from a rogue CA.
        let rogue_dir = tempfile::tempdir().unwrap();
        let rogue_pki = TestPki::new("rogue-ca");
        let mut rogue_cfg = rogue_pki.node_config(rogue_dir.path(), 1);
        // The rogue trusts the real CA as its root (so its client-side
        // verification of node2 could even pass) but presents a rogue cert.
        if let Some(tls) = &mut rogue_cfg.tls {
            let real_ca_path = rogue_dir.path().join("real-ca.crt");
            std::fs::write(&real_ca_path, &pki.ca_pem).unwrap();
            tls.ca_path = real_ca_path.to_string_lossy().into_owned();
        }
        let rogue = TcpRaftTransport::new_with_security(
            any_local(),
            HashMap::from([(PeerId(2), node2_addr)]),
            rogue_cfg,
        )
        .await
        .expect("bind rogue");

        // The send may fail at handshake or appear to succeed locally (TLS
        // alerts can race the write); the invariant is that node2 never
        // delivers a frame from it.
        let _ = rogue
            .send_frame_owned(PeerId(2), bytes::Bytes::from(frame_from(1)))
            .await;

        // Raw plaintext client against the TLS listener.
        use tokio::io::AsyncWriteExt;
        if let Ok(mut raw) = tokio::net::TcpStream::connect(node2_addr).await {
            let _ = raw.write_all(&frame_from(1)).await;
        }

        assert!(
            recv_with_timeout(&node2, 1000).await.is_none(),
            "unauthenticated clients must not get frames into the Raft channel"
        );
    }

    /// D2 core: a connection authenticated as peer 2 sends a frame claiming
    /// `from = 3` — it is dropped and counted, never delivered. The same
    /// connection claiming its true id goes through.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mtls_spoofed_from_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let pki = TestPki::new("arbitro-test-ca");

        // Node 1: receiver; peers 2 and 3 are both members.
        let node1 = TcpRaftTransport::new_with_security(
            any_local(),
            HashMap::from([
                (PeerId(2), "127.0.0.1:1".parse().unwrap()),
                (PeerId(3), "127.0.0.1:2".parse().unwrap()),
            ]),
            pki.node_config(dir.path(), 1),
        )
        .await
        .expect("bind node1");
        let node1_addr = node1.local_addr();

        // Node 2: authenticated with a legitimate `peer-2` cert.
        let node2 = TcpRaftTransport::new_with_security(
            any_local(),
            HashMap::from([(PeerId(1), node1_addr)]),
            pki.node_config(dir.path(), 2),
        )
        .await
        .expect("bind node2");

        // Spoof: node 2's connection claims to be member 3.
        node2
            .send_frame_owned(PeerId(1), bytes::Bytes::from(frame_from(3)))
            .await
            .expect("TLS send succeeds; the drop happens on the receiver");
        assert!(
            recv_with_timeout(&node1, 500).await.is_none(),
            "frame claiming from=3 on a connection authenticated as peer 2 must be dropped"
        );
        assert_eq!(
            node1.spoofed_frames_rejected(),
            1,
            "spoofed frame must be counted"
        );

        // Honest frame on the same connection is delivered.
        node2
            .send_frame_owned(PeerId(1), bytes::Bytes::from(frame_from(2)))
            .await
            .expect("send");
        let got = recv_with_timeout(&node1, 5000)
            .await
            .expect("honest frame (from=2) must be delivered");
        assert_eq!(u64::from_le_bytes(got[8..16].try_into().unwrap()), 2);
        assert_eq!(node1.spoofed_frames_rejected(), 1);
    }
}
