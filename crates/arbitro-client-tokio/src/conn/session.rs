//! Per-session lifecycle: dial, handshake, replay subscriptions, spawn
//! writer + reader + heartbeat, run until any task drops, drain pending.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use arbitro_kit::route::MpscAsyncConsumer;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use arbitro_proto::v2::ingress::hello::Role;

use crate::conn::heartbeat::heartbeat_task;
use crate::conn::reconnect::Backoff;
use crate::error::ClientError;
use crate::state::Inner;
use crate::transport::encode::encode_hello_v2;
use crate::transport::frame::{WriteFrame, WRITE_QUEUE_CAP};
use crate::transport::reader::reader_task;
use crate::transport::writer::writer_task;

/// Connect a raw `TcpStream` and optionally wrap it with TLS.
///
/// Returns `(read_half, write_half)` as boxed trait objects so the
/// caller doesn't need separate generic paths for plain / TLS.
async fn dial(
    inner: &Inner,
) -> Result<
    (
        Box<dyn AsyncRead + Unpin + Send>,
        Box<dyn AsyncWrite + Unpin + Send>,
    ),
    ClientError,
> {
    let tcp = TcpStream::connect(&inner.cfg.addr).await?;

    #[cfg(feature = "tls")]
    {
        if let Some(ref tls_cfg) = inner.cfg.tls {
            use tokio_rustls::TlsConnector;

            let mut root_store = rustls::RootCertStore::empty();
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let mut config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            if tls_cfg.danger_accept_invalid_certs {
                config
                    .dangerous()
                    .set_certificate_verifier(Arc::new(danger::NoCertVerifier));
            }

            let connector = TlsConnector::from(Arc::new(config));
            let domain = rustls::pki_types::ServerName::try_from(tls_cfg.server_name.clone())
                .map_err(|_| ClientError::Tls("invalid server name".into()))?;

            let tls_stream = connector
                .connect(domain, tcp)
                .await
                .map_err(|e| ClientError::Tls(e.to_string()))?;

            let (r, w) = tokio::io::split(tls_stream);
            return Ok((Box::new(r), Box::new(w)));
        }
    }

    // Plain TCP — split into owned halves.
    let (r, w) = tcp.into_split();
    Ok((Box::new(r), Box::new(w)))
}

/// Spawn the background connection loop.
///
/// Establishes the first TCP connection + handshake before returning so
/// callers get a fast failure on bad addresses.  All subsequent reconnects
/// happen silently in the background task.
pub(crate) async fn spawn_connection(
    consumer: MpscAsyncConsumer<WriteFrame, WRITE_QUEUE_CAP>,
    inner: Arc<Inner>,
) -> Result<(), ClientError> {
    // Initial connection — fast failure path.
    let (r, mut w) = dial(&inner).await?;
    write_handshake(&mut w).await?;
    // Replay any subscriptions (none on first connect — future-proofs reconnect).
    replay_subscriptions(&inner);
    // Re-register any active cron jobs after reconnect.
    crate::cron::replay_crons(&inner);
    // Re-register any active workflows after reconnect.
    crate::workflow::replay_workflows(&inner);

    let cancel = inner.cancel.clone();
    tokio::spawn(async move {
        let mut consumer = consumer;
        let mut wh: Option<Box<dyn AsyncWrite + Unpin + Send>> = Some(w);
        let mut rh: Option<Box<dyn AsyncRead + Unpin + Send>> = Some(r);
        let mut back = Backoff::new(&inner.cfg.reconnect);

        loop {
            let session_cancel = cancel.child_token();

            let res = if let (Some(w), Some(r)) = (wh.take(), rh.take()) {
                run_session(
                    &mut consumer,
                    w,
                    r,
                    Arc::clone(&inner),
                    session_cancel.clone(),
                )
                .await
            } else {
                Err(ClientError::Disconnected)
            };

            inner.pending.drain_disconnected();

            if cancel.is_cancelled() {
                debug!("connection cancelled");
                return;
            }

            warn!(error = ?res, "session ended, will reconnect");

            // Back-off loop — keep retrying until we get a new connection.
            loop {
                let Some(delay) = back.next() else {
                    debug!("reconnect attempts exhausted");
                    return;
                };
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
                match dial(&inner).await {
                    Ok((r, mut w)) => {
                        if let Err(e) = write_handshake(&mut w).await {
                            warn!(?e, "handshake write failed");
                            continue;
                        }
                        // Replay subscriptions + crons + workflows before the new session starts.
                        replay_subscriptions(&inner);
                        crate::cron::replay_crons(&inner);
                        crate::workflow::replay_workflows(&inner);
                        rh = Some(r);
                        wh = Some(w);
                        back.reset();
                        break;
                    }
                    Err(e) => {
                        warn!(?e, "reconnect dial failed");
                    }
                }
            }
        }
    });

    Ok(())
}

/// Write the v2 Hello handshake frame.
async fn write_handshake<W: AsyncWrite + Unpin + ?Sized>(w: &mut W) -> Result<(), ClientError> {
    let hello = encode_hello_v2(Role::Client);
    w.write_all(&hello).await?;
    Ok(())
}

/// Enqueue all stored `sub_body` frames via the admin producer.
///
/// Called after a successful handshake so the broker re-registers all
/// active consumers.  Fire-and-forget — writer picks them up.
fn replay_subscriptions(inner: &Inner) {
    let sub_bodies = inner.subscriptions.all_sub_bodies();
    if sub_bodies.is_empty() {
        return;
    }
    // Reset the heartbeat timestamp so we don't time-out during replay.
    inner.last_pong_ns.store(Inner::now_ns(), Ordering::Relaxed);

    let admin = inner.admin_producer.lock().unwrap();
    for sub_body in sub_bodies {
        let _ = admin.try_send(WriteFrame::Mono(sub_body));
    }
}

/// Run writer + reader + heartbeat concurrently under a child token.
/// Returns when the first of the three finishes (error or clean exit).
async fn run_session<W, R>(
    consumer: &mut MpscAsyncConsumer<WriteFrame, WRITE_QUEUE_CAP>,
    w: W,
    r: R,
    inner: Arc<Inner>,
    cancel: CancellationToken,
) -> Result<(), ClientError>
where
    W: AsyncWrite + Unpin + Send + 'static,
    R: AsyncRead + Unpin + Send + 'static,
{
    let cfg_ka = inner.cfg.keep_alive.clone();

    let inner_r = Arc::clone(&inner);
    let inner_hb = Arc::clone(&inner);
    let cancel_r = cancel.clone();
    let cancel_hb = cancel.clone();

    let reader_h = tokio::spawn(reader_task(r, inner_r, cancel_r));

    tokio::select! {
        r = writer_task(consumer, w, cancel.clone()) => {
            cancel.cancel();
            r
        }
        r = reader_h => {
            cancel.cancel();
            match r {
                Ok(v) => v,
                Err(_) => Err(ClientError::Disconnected),
            }
        }
        r = heartbeat_task(inner_hb, cfg_ka, cancel_hb) => {
            cancel.cancel();
            r
        }
    }
}

// ── TLS: danger_accept_invalid_certs verifier ─────────────────────────
#[cfg(feature = "tls")]
mod danger {
    /// Certificate verifier that accepts **any** server certificate.
    ///
    /// Only enabled when `TlsConfig::danger_accept_invalid_certs` is `true`
    /// — intended for development / testing with self-signed certs.
    #[derive(Debug)]
    pub(super) struct NoCertVerifier;

    impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls::pki_types::CertificateDer<'_>,
            _intermediates: &[rustls::pki_types::CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp_response: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }
}
