//! TLS support via `tokio-rustls`.
//!
//! Builds a `TlsAcceptor` from PEM cert + key files specified via config.
//! Used by the accept loop to upgrade TCP connections to TLS before framing.

#[cfg(feature = "tls")]
use std::fs::File;
#[cfg(feature = "tls")]
use std::io::BufReader;
#[cfg(feature = "tls")]
use std::sync::Arc;

#[cfg(feature = "tls")]
use tokio_rustls::TlsAcceptor;

/// Build a `TlsAcceptor` from cert and key PEM files.
///
/// Returns `None` if paths are not configured. Panics on invalid cert/key
/// (fail-fast at startup, not at runtime per-connection).
#[cfg(feature = "tls")]
pub fn build_acceptor(cert_path: &str, key_path: &str) -> TlsAcceptor {
    use tokio_rustls::rustls::{self, pki_types::PrivateKeyDer};

    let cert_file = File::open(cert_path)
        .unwrap_or_else(|e| panic!("failed to open TLS cert {cert_path}: {e}"));
    let key_file = File::open(key_path)
        .unwrap_or_else(|e| panic!("failed to open TLS key {key_path}: {e}"));

    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to parse TLS certificate PEM");

    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .expect("failed to read TLS private key PEM")
        .expect("no private key found in PEM file");

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("failed to build TLS ServerConfig");

    TlsAcceptor::from(Arc::new(config))
}
