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

/// **M13**: TLS acceptor build errors. Surfaces every failure mode
/// the previous `.expect()` / `panic!` chain used to abort on.
#[cfg(feature = "tls")]
#[derive(Debug, thiserror::Error)]
pub enum TlsBuildError {
    #[error("failed to open TLS cert {0}: {1}")]
    CertOpen(String, std::io::Error),
    #[error("failed to open TLS key {0}: {1}")]
    KeyOpen(String, std::io::Error),
    #[error("failed to parse TLS certificate PEM: {0}")]
    CertParse(std::io::Error),
    #[error("failed to read TLS private key PEM: {0}")]
    KeyRead(std::io::Error),
    #[error("no private key found in PEM file {0}")]
    KeyMissing(String),
    #[error("failed to build TLS ServerConfig: {0}")]
    ConfigBuild(tokio_rustls::rustls::Error),
}

/// Build a `TlsAcceptor` from cert and key PEM files.
///
/// **M13**: returns a typed error instead of panicking. Caller (main)
/// logs and exits cleanly so an operator typo in `ARBITRO_TLS_CERT`
/// shows a clear message instead of a panic stack trace.
#[cfg(feature = "tls")]
pub fn build_acceptor(cert_path: &str, key_path: &str) -> Result<TlsAcceptor, TlsBuildError> {
    use tokio_rustls::rustls;

    let cert_file = File::open(cert_path)
        .map_err(|e| TlsBuildError::CertOpen(cert_path.into(), e))?;
    let key_file = File::open(key_path)
        .map_err(|e| TlsBuildError::KeyOpen(key_path.into(), e))?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(TlsBuildError::CertParse)?;

    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .map_err(TlsBuildError::KeyRead)?
        .ok_or_else(|| TlsBuildError::KeyMissing(key_path.into()))?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(TlsBuildError::ConfigBuild)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}
