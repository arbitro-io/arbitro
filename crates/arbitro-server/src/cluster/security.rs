//! Cluster transport security (D1 mTLS + D2 peer identity binding).
//!
//! # Design
//!
//! The Raft wire header carries a *claimed* sender id (`from: U64`) that the
//! Raft node trusts for vote grants and ack crediting. The transport is the
//! only layer that can make that claim trustworthy, so security lives here:
//!
//! * **D1 (mTLS)** — when `ARBITRO_CLUSTER_TLS_CERT`, `_KEY` and `_CA` are
//!   all set (and the `cluster-tls` feature is compiled in), every inter-node
//!   connection runs over rustls with **mutual** verification: each node
//!   presents the same cert for both client and server roles, and both sides
//!   verify the peer's chain against the cluster CA. Connections that fail
//!   verification are refused.
//! * **D2 (identity binding)** — on accept, the connection is bound to an
//!   authenticated `PeerId`:
//!   - TLS path: derived from the peer certificate's SAN (DNS/URI) or CN,
//!     mapped to a `PeerId` via `ARBITRO_CLUSTER_TLS_PEER_MAP`
//!     (`identity=id,identity=id,...`) or the default `peer-<id>` naming
//!     convention.
//!   - Plaintext path (opt-in via `ARBITRO_CLUSTER_STRICT_FROM=1`): bound to
//!     the set of `PeerId`s whose configured address shares the remote IP.
//!     This is weaker than mTLS (no crypto, and peers co-hosted on one IP are
//!     mutually indistinguishable) but stops off-cluster hosts from injecting
//!     frames with a member's id.
//!
//! Any received frame whose header `from` disagrees with the bound identity
//! is dropped and counted (see `TcpRaftTransport::spoofed_frames_rejected`).
//!
//! # Certificate rotation
//!
//! TLS material is (re)loaded from disk **per connection**: the server-side
//! acceptor config is rebuilt for each accepted connection and the client-side
//! connector config for each outbound dial. Replacing the PEM files on disk
//! therefore takes effect on the next (re)connect — no process restart is
//! required, but existing established connections keep their old session until
//! they naturally reconnect. This "reload on reconnect" story is deliberate:
//! Raft reconnects are rare, so the per-connection PEM parse cost is
//! negligible, and it keeps rotation free of file-watcher machinery. The
//! cluster port should be firewalled to cluster hosts regardless; per-accept
//! config building is not a hardened public-endpoint pattern.

#[cfg(feature = "cluster-tls")]
use std::collections::HashMap;

/// Security configuration for the cluster (Raft) transport.
///
/// Built from environment variables via [`ClusterSecurityConfig::from_env`].
/// The default value ([`ClusterSecurityConfig::plaintext`]) reproduces the
/// historical behavior exactly: plaintext TCP, no identity enforcement.
#[derive(Debug, Clone, Default)]
pub struct ClusterSecurityConfig {
    /// mTLS material (paths + identity map). `None` = plaintext.
    #[cfg(feature = "cluster-tls")]
    pub tls: Option<ClusterTlsConfig>,
    /// Plaintext-only: enforce that a frame's `from` id belongs to a peer
    /// whose configured address matches the connection's remote IP.
    /// Ignored on TLS connections (those always enforce the exact cert-bound
    /// identity).
    pub strict_from: bool,
}

/// Paths to PEM files + cert-identity mapping for cluster mTLS.
#[cfg(feature = "cluster-tls")]
#[derive(Debug, Clone)]
pub struct ClusterTlsConfig {
    /// This node's certificate chain (PEM). Presented in both the client and
    /// the server role.
    pub cert_path: String,
    /// This node's private key (PEM).
    pub key_path: String,
    /// Cluster CA bundle (PEM). Peer certs must chain to it.
    pub ca_path: String,
    /// Explicit cert-identity → PeerId map (`identity=id,...`). When an
    /// identity is not in the map, the `peer-<id>` convention applies.
    pub peer_map: HashMap<String, u64>,
}

impl ClusterSecurityConfig {
    /// Plaintext, no enforcement — the historical default. Used by tests and
    /// as the fallback when no security env vars are present.
    pub fn plaintext() -> Self {
        Self::default()
    }

    /// Build from environment variables.
    ///
    /// * `ARBITRO_CLUSTER_TLS_CERT` / `_KEY` / `_CA` — all three present
    ///   enables mTLS (requires the `cluster-tls` feature; setting them on a
    ///   binary compiled without it is a hard error so the operator is never
    ///   silently downgraded to plaintext).
    /// * `ARBITRO_CLUSTER_TLS_PEER_MAP` — optional `identity=id,...` map.
    /// * `ARBITRO_CLUSTER_STRICT_FROM=1` — plaintext address-based `from`
    ///   enforcement.
    pub fn from_env() -> Result<Self, String> {
        let cert = std::env::var("ARBITRO_CLUSTER_TLS_CERT").ok();
        let key = std::env::var("ARBITRO_CLUSTER_TLS_KEY").ok();
        let ca = std::env::var("ARBITRO_CLUSTER_TLS_CA").ok();
        let strict_from = matches!(
            std::env::var("ARBITRO_CLUSTER_STRICT_FROM").as_deref(),
            Ok("1") | Ok("true")
        );

        match (cert, key, ca) {
            (None, None, None) => Ok(Self {
                #[cfg(feature = "cluster-tls")]
                tls: None,
                strict_from,
            }),
            (Some(_cert), Some(_key), Some(_ca)) => {
                #[cfg(feature = "cluster-tls")]
                {
                    Ok(Self {
                        tls: Some(ClusterTlsConfig {
                            cert_path: _cert,
                            key_path: _key,
                            ca_path: _ca,
                            peer_map: parse_peer_map(
                                &std::env::var("ARBITRO_CLUSTER_TLS_PEER_MAP")
                                    .unwrap_or_default(),
                            )?,
                        }),
                        strict_from,
                    })
                }
                #[cfg(not(feature = "cluster-tls"))]
                {
                    Err(
                        "ARBITRO_CLUSTER_TLS_* is set but this binary was built without \
                         the `cluster-tls` feature; refusing to fall back to plaintext"
                            .into(),
                    )
                }
            }
            _ => Err(
                "ARBITRO_CLUSTER_TLS_CERT, _KEY and _CA must be set together \
                 (all three, or none)"
                    .into(),
            ),
        }
    }
}

/// Parse `identity=id,identity=id,...` (empty string → empty map).
#[cfg(feature = "cluster-tls")]
fn parse_peer_map(raw: &str) -> Result<HashMap<String, u64>, String> {
    let mut map = HashMap::new();
    for pair in raw.split(',').filter(|p| !p.trim().is_empty()) {
        let (ident, id) = pair
            .split_once('=')
            .ok_or_else(|| format!("ARBITRO_CLUSTER_TLS_PEER_MAP: bad entry {pair:?}"))?;
        let id: u64 = id
            .trim()
            .parse()
            .map_err(|e| format!("ARBITRO_CLUSTER_TLS_PEER_MAP: bad id in {pair:?}: {e}"))?;
        map.insert(ident.trim().to_string(), id);
    }
    Ok(map)
}

/// The cert identity a given peer is expected to present, used as the TLS
/// server name when dialing that peer (so webpki also enforces the binding on
/// the *outbound* direction). Reverse of the accept-side identity → id lookup.
#[cfg(feature = "cluster-tls")]
pub(crate) fn identity_for_peer(cfg: &ClusterTlsConfig, id: u64) -> String {
    for (ident, mapped) in &cfg.peer_map {
        if *mapped == id {
            return ident.clone();
        }
    }
    format!("peer-{id}")
}

/// Map a cert identity string to a PeerId: explicit map first, then the
/// `peer-<id>` convention.
#[cfg(feature = "cluster-tls")]
fn peer_id_for_identity(cfg: &ClusterTlsConfig, ident: &str) -> Option<u64> {
    if let Some(id) = cfg.peer_map.get(ident) {
        return Some(*id);
    }
    ident.strip_prefix("peer-")?.parse().ok()
}

// ── TLS plumbing (cluster-tls only) ──────────────────────────────────────────

#[cfg(feature = "cluster-tls")]
pub(crate) mod tls {
    use super::{peer_id_for_identity, ClusterTlsConfig};
    use std::io::BufReader;
    use std::sync::Arc;
    use tokio_rustls::rustls;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
        let file = std::fs::File::open(path).map_err(|e| format!("open cert {path}: {e}"))?;
        rustls_pemfile::certs(&mut BufReader::new(file))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("parse cert PEM {path}: {e}"))
    }

    fn load_key(path: &str) -> Result<PrivateKeyDer<'static>, String> {
        let file = std::fs::File::open(path).map_err(|e| format!("open key {path}: {e}"))?;
        rustls_pemfile::private_key(&mut BufReader::new(file))
            .map_err(|e| format!("parse key PEM {path}: {e}"))?
            .ok_or_else(|| format!("no private key in {path}"))
    }

    fn load_roots(path: &str) -> Result<rustls::RootCertStore, String> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in load_certs(path)? {
            roots
                .add(cert)
                .map_err(|e| format!("bad CA cert in {path}: {e}"))?;
        }
        if roots.is_empty() {
            return Err(format!("no CA certificates found in {path}"));
        }
        Ok(roots)
    }

    /// Build a server-side acceptor that REQUIRES a client certificate
    /// chaining to the cluster CA (mutual TLS). Reads the PEM files fresh —
    /// see the module docs for the rotation story.
    pub(crate) fn build_acceptor(cfg: &ClusterTlsConfig) -> Result<TlsAcceptor, String> {
        let roots = load_roots(&cfg.ca_path)?;
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| format!("client-cert verifier: {e}"))?;
        let config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(load_certs(&cfg.cert_path)?, load_key(&cfg.key_path)?)
            .map_err(|e| format!("server TLS config: {e}"))?;
        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    /// Build a client-side connector that presents this node's certificate
    /// and verifies the peer against the cluster CA. Reads the PEM files
    /// fresh — see the module docs for the rotation story.
    pub(crate) fn build_connector(cfg: &ClusterTlsConfig) -> Result<TlsConnector, String> {
        let roots = load_roots(&cfg.ca_path)?;
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(load_certs(&cfg.cert_path)?, load_key(&cfg.key_path)?)
            .map_err(|e| format!("client TLS config: {e}"))?;
        Ok(TlsConnector::from(Arc::new(config)))
    }

    /// Derive the authenticated PeerId from a verified peer certificate:
    /// try every SAN DNS name, SAN URI, and finally the subject CN, mapping
    /// each candidate through the config map / `peer-<id>` convention.
    ///
    /// The cert has already been chain-verified against the cluster CA by
    /// rustls before this runs; this only extracts the identity.
    pub(crate) fn peer_id_from_cert(
        cfg: &ClusterTlsConfig,
        cert_der: &[u8],
    ) -> Result<u64, String> {
        use x509_parser::prelude::*;

        let (_, cert) = X509Certificate::from_der(cert_der)
            .map_err(|e| format!("parse peer cert: {e}"))?;

        let mut candidates: Vec<String> = Vec::new();
        if let Ok(Some(san)) = cert.subject_alternative_name() {
            for name in &san.value.general_names {
                match name {
                    GeneralName::DNSName(d) => candidates.push((*d).to_string()),
                    GeneralName::URI(u) => candidates.push((*u).to_string()),
                    _ => {}
                }
            }
        }
        if let Some(cn) = cert
            .subject()
            .iter_common_name()
            .next()
            .and_then(|cn| cn.as_str().ok())
        {
            candidates.push(cn.to_string());
        }

        for cand in &candidates {
            if let Some(id) = peer_id_for_identity(cfg, cand) {
                return Ok(id);
            }
        }
        Err(format!(
            "peer cert has no identity mapping to a PeerId (candidates: {candidates:?})"
        ))
    }
}
