//! Per-session lifecycle: dial, handshake, replay subscriptions, spawn
//! writer + reader, run until either drops, drain pending.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use arbitro_proto::v2::ingress::hello::Role;

use crate::conn::reconnect::Backoff;
use crate::error::ClientError;
use crate::state::Inner;
use crate::transport::encode::{encode_ack_state_req_v2, encode_auth_v2, encode_hello_v2};
use crate::transport::frame::{WriteConsumer, WriteFrame, WriteLease};
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
    consumer: WriteConsumer,
    inner: Arc<Inner>,
    mut session_replay_lease: WriteLease,
) -> Result<(), ClientError> {
    // Initial connection — fast failure path.
    let (r, mut w) = dial(&inner).await?;
    write_handshake(&mut w, inner.cfg.auth_token.as_deref()).await?;
    // Replay any subscriptions (none on first connect — future-proofs reconnect).
    replay_subscriptions(&inner, &mut session_replay_lease);
    send_ack_state_reqs(&inner, &mut session_replay_lease);
    // Re-register any active cron jobs after reconnect.
    crate::cron::replay_crons(&inner);

    let cancel = inner.cancel.clone();
    tokio::spawn(async move {
        let mut consumer = consumer;
        let mut wh: Option<Box<dyn AsyncWrite + Unpin + Send>> = Some(w);
        let mut rh: Option<Box<dyn AsyncRead + Unpin + Send>> = Some(r);
        let mut back = Backoff::new(&inner.cfg.reconnect);

        loop {
            let session_cancel = cancel.child_token();
            *inner.session_cancel.lock().unwrap() = Some(session_cancel.clone());

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

            *inner.session_cancel.lock().unwrap() = None;
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
                        if let Err(e) =
                            write_handshake(&mut w, inner.cfg.auth_token.as_deref()).await
                        {
                            warn!(?e, "handshake write failed");
                            continue;
                        }
                        // Replay subscriptions + crons before the new session starts.
                        replay_subscriptions(&inner, &mut session_replay_lease);
                        send_ack_state_reqs(&inner, &mut session_replay_lease);
                        crate::cron::replay_crons(&inner);
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

/// Write the v2 handshake: Hello, then the Auth frame when a token is set.
///
/// This is the **only** place either frame is written, and it runs on the
/// first dial *and* on every redial — so a reconnect cannot silently drop
/// authentication. That single choke point is the whole reason the token is
/// not sent from `connect()`.
///
/// Both frames go out in one `write_all`. The broker's handshake deadline
/// covers Hello + Auth together, and pipelining them means the client never
/// waits in between.
///
/// Nothing is awaited here: the broker answers only on *failure* (an error
/// frame, then close). Success is silent, so waiting for a reply would hang
/// forever. The reader classifies `AuthRequired`/`AuthFailed` and shuts the
/// session down.
async fn write_handshake<W: AsyncWrite + Unpin + ?Sized>(
    w: &mut W,
    auth_token: Option<&str>,
) -> Result<(), ClientError> {
    let hello = encode_hello_v2(Role::Client);
    match auth_token {
        None => w.write_all(&hello).await?,
        Some(token) => {
            let auth = encode_auth_v2(token.as_bytes());
            let mut buf = Vec::with_capacity(hello.len() + auth.len());
            buf.extend_from_slice(&hello);
            buf.extend_from_slice(&auth);
            w.write_all(&buf).await?;
        }
    }
    Ok(())
}

/// Enqueue all stored `sub_body` frames via the dedicated session-replay lease.
///
/// Called after a successful handshake so the broker re-registers all
/// active consumers.  Fire-and-forget — writer picks them up.
///
/// Unconditionally resets `last_pong_ns` on every session start (not just
/// when there are subscriptions to replay) — heartbeat now runs for the
/// whole Client lifetime rather than per-session, so this is the only
/// place left that gives it a fresh timeout window after a reconnect.
fn replay_subscriptions(inner: &Inner, lease: &mut WriteLease) {
    inner.last_pong_ns.store(Inner::now_ns(), Ordering::Relaxed);
    for sub_body in inner.subscriptions.replay_frames() {
        let _ = lease.try_send(WriteFrame::Mono(sub_body));
    }
}

/// Ack-reliability: request the broker's cursor/retention state for every
/// consumer with outstanding deferred acks, AND for every consumer that
/// carries a durable ackstore slot. Pipelined, fire-and-forget — the
/// `AckStateRep` handler in `transport::reader` drives the follow-up
/// `AckBatchFrame` flush and the WAL `confirm_up_to` purge.
///
/// The slotted sweep exists for the reconnect-purge guarantee: WAL entries
/// recorded by a session that died before its `AckBatchResp` arrived are
/// never confirmed by the normal flow, so on every (re)connect each slotted
/// consumer asks the broker for its authoritative cursor once and drops
/// everything at or below it. Cold path — one 24 B frame per consumer per
/// (re)connect, nothing added to the delivery/ack hot path.
fn send_ack_state_reqs(inner: &Inner, lease: &mut WriteLease) {
    let consumers = inner.ackrel.active_consumers();
    let mut sent: std::collections::HashSet<u32> =
        consumers.iter().map(|&(cid, _)| cid).collect();
    for (consumer_id, generation) in consumers {
        let seq = inner.seq_alloc.next();
        let _ = lease.try_send(WriteFrame::Mono(encode_ack_state_req_v2(
            seq,
            consumer_id,
            generation,
        )));
    }
    // Durable-dedup consumers with no outstanding deferred acks still need
    // the cursor snapshot (idle consumers never see an AckBatchResp).
    for consumer_id in inner.subscriptions.dedup_consumers() {
        if !sent.insert(consumer_id) {
            continue; // already covered by the deferred-ack sweep above
        }
        let seq = inner.seq_alloc.next();
        let generation = inner.ackrel.generation_of(consumer_id);
        let _ = lease.try_send(WriteFrame::Mono(encode_ack_state_req_v2(
            seq,
            consumer_id,
            generation,
        )));
    }
}

/// Run writer + reader concurrently under a child token.
/// Returns when the first of the two finishes (error or clean exit).
///
/// After the select resolves, if the parent cancel (`inner.cancel`) was
/// the trigger, we send a Disconnect frame so the server releases
/// bindings immediately (the select may drop the writer future before
/// it could flush the frame, so we do it here where `w` is still owned).
async fn run_session<W, R>(
    consumer: &mut WriteConsumer,
    mut w: W,
    r: R,
    inner: Arc<Inner>,
    cancel: CancellationToken,
) -> Result<(), ClientError>
where
    W: AsyncWrite + Unpin + Send + 'static,
    R: AsyncRead + Unpin + Send + 'static,
{
    let inner_r = Arc::clone(&inner);
    let cancel_r = cancel.clone();

    let reader_h = tokio::spawn(reader_task(r, inner_r, cancel_r));

    let result = tokio::select! {
        r = writer_task(consumer, &mut w, cancel.clone()) => {
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
    };

    // If the client initiated close, send Disconnect so the server
    // retires bindings immediately rather than waiting for keepalive.
    if inner.cancel.is_cancelled() {
        let _ = w
            .write_all(&crate::transport::writer::disconnect_frame())
            .await;
    }
    let _ = w.shutdown().await;

    result
}

#[cfg(test)]
mod tests {
    use crate::state::subscriptions::Registration;
    use super::*;
    use crate::config::ClientConfig;
    use crate::state::{pending::Pending, seq::SeqAllocator, subscriptions::Subscriptions};
    use crate::transport::frame::{WritePool, WRITE_QUEUE_CAP};
    use arbitro_proto::action::Action;
    use arbitro_proto::v2::header::HEADER_SIZE;
    use std::sync::atomic::AtomicU64;

    /// The (re)connect sweep must send one `AckStateReq` per consumer that
    /// carries a durable ackstore slot, even when it has no outstanding
    /// deferred acks — that request is what drives the reconnect WAL purge.
    /// Consumers without a slot and without deferred acks get nothing.
    #[test]
    fn send_ack_state_reqs_covers_slotted_consumers() {
        let (pool, mut consumer) = WritePool::new(8, WRITE_QUEUE_CAP);
        let (ack_tx, _ack_rx) = tokio::sync::mpsc::channel(16);
        let (nack_tx, _nack_rx) = tokio::sync::mpsc::channel(16);
        let inner = Inner {
            cfg: ClientConfig::default(),
            pool: Arc::clone(&pool),
            pending: Arc::new(Pending::new()),
            seq_alloc: SeqAllocator::new(),
            sub_id_alloc: crate::state::seq::SubIdAllocator::new(),
            cancel: CancellationToken::new(),
            subscriptions: Arc::new(Subscriptions::new()),
            ack_tx,
            nack_tx,
            last_pong_ns: AtomicU64::new(0),
            metrics: Arc::new(crate::metrics::ClientMetrics::new()),
            cron_state: crate::cron::CronState::new(),
            session_cancel: std::sync::Mutex::new(None),
            ackrel: Arc::new(crate::ackrel::AckRelay::new()),
            ack_store: None,
        };

        // Consumer 7: durable dedup slot, no deferred acks.
        let store: Arc<dyn crate::ackstore::Store> =
            Arc::new(crate::ackstore::memory::MemoryStore::new(0));
        let slot = store.slot("s", "c").unwrap();
        let _rx7 = inner
            .subscriptions
            .register(Registration {
                consumer_id: 7,
                stream_id: 1,
                fanout: false,
                dedup: Some(slot),
                sub_id: 7,
                filter: b"",
                frame: bytes::Bytes::new(),
            });
        // Consumer 8: plain subscription, no slot, no deferred acks.
        let _rx8 = inner.subscriptions.register(Registration {
            consumer_id: 8,
            stream_id: 1,
            fanout: false,
            dedup: None,
            sub_id: 8,
            filter: b"",
            frame: bytes::Bytes::new(),
        });

        let mut lease = pool.acquire().expect("fresh pool has slots");
        send_ack_state_reqs(&inner, &mut lease);

        let mut reqs: Vec<u32> = Vec::new();
        while let Some(frame) = consumer.try_recv() {
            let bytes = match frame {
                WriteFrame::Mono(b) => b,
                other => panic!("unexpected frame variant: {other:?}"),
            };
            let action = u16::from_le_bytes([bytes[0], bytes[1]]);
            assert_eq!(action, Action::AckStateReq.as_u16());
            let cid = u32::from_le_bytes(
                bytes[HEADER_SIZE..HEADER_SIZE + 4].try_into().unwrap(),
            );
            reqs.push(cid);
        }
        assert_eq!(reqs, vec![7], "exactly the slotted consumer is queried");
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
