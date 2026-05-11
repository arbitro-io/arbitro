//! Per-session lifecycle: dial, handshake, replay subscriptions, spawn
//! writer + reader + heartbeat, run until any task drops, drain pending.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use arbitro_kit::route::MpscAsyncConsumer;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use arbitro_proto::v2::ingress::hello::{cap, Role};

use crate::conn::heartbeat::heartbeat_task;
use crate::conn::reconnect::Backoff;
use crate::error::ClientError;
use crate::state::Inner;
use crate::transport::encode::encode_hello_v2;
use crate::transport::frame::{WriteFrame, WRITE_QUEUE_CAP};
use crate::transport::reader::reader_task;
use crate::transport::writer::writer_task;

/// Spawn the background connection loop.
///
/// Establishes the first TCP connection + handshake before returning so
/// callers get a fast failure on bad addresses.  All subsequent reconnects
/// happen silently in the background task.
pub(crate) async fn spawn_connection(
    consumer: MpscAsyncConsumer<WriteFrame, WRITE_QUEUE_CAP>,
    inner:    Arc<Inner>,
) -> Result<(), ClientError> {
    // Initial connection — fast failure path.
    let first = TcpStream::connect(&inner.cfg.addr).await?;
    let (read_h, mut write_h) = first.into_split();
    write_handshake(&mut write_h).await?;
    // Replay any subscriptions (none on first connect — future-proofs reconnect).
    replay_subscriptions(&inner);

    let cancel = inner.cancel.clone();
    tokio::spawn(async move {
        let mut consumer = consumer;
        let mut wh = Some(write_h);
        let mut rh = Some(read_h);
        let mut back = Backoff::new(&inner.cfg.reconnect);

        loop {
            let session_cancel = cancel.child_token();

            let res = if let (Some(w), Some(r)) = (wh.take(), rh.take()) {
                run_session(
                    &mut consumer,
                    w, r,
                    Arc::clone(&inner),
                    session_cancel.clone(),
                ).await
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
                match TcpStream::connect(&inner.cfg.addr).await {
                    Ok(s) => {
                        let (r, mut w) = s.into_split();
                        if let Err(e) = write_handshake(&mut w).await {
                            warn!(?e, "handshake write failed");
                            continue;
                        }
                        // Replay subscriptions before the new session starts.
                        replay_subscriptions(&inner);
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
async fn write_handshake(
    w: &mut tokio::net::tcp::OwnedWriteHalf,
) -> Result<(), ClientError> {
    let hello = encode_hello_v2(Role::Client, cap::REPLY | cap::BATCH_HEADERS);
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
async fn run_session(
    consumer: &mut MpscAsyncConsumer<WriteFrame, WRITE_QUEUE_CAP>,
    w:        tokio::net::tcp::OwnedWriteHalf,
    r:        tokio::net::tcp::OwnedReadHalf,
    inner:    Arc<Inner>,
    cancel:   CancellationToken,
) -> Result<(), ClientError> {
    let cfg_ka = inner.cfg.keep_alive.clone();

    let inner_r  = Arc::clone(&inner);
    let inner_hb = Arc::clone(&inner);
    let cancel_r  = cancel.clone();
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
