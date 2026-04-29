//! Per-session lifecycle: dial, handshake, spawn writer + reader, run
//! until any task drops, drain pending with `Disconnected`, return.
//!
//! `connection_loop` (top-level) loops on `dial_and_run`, applying the
//! decorrelated-jitter backoff on failure. Cancellation via the
//! shared `CancellationToken` (set on `Client::drop` / `close`).

use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use arbitro_proto::v2::ingress::hello::{cap, Role};

use crate::config::ClientConfig;
use crate::conn::reconnect::Backoff;
use crate::error::ClientError;
use crate::state::pending::Pending;
use crate::transport::encode::encode_hello_v2;
use crate::transport::frame::WriteFrame;
use crate::transport::reader::reader_task;
use crate::transport::writer::writer_task;

/// Channel handle handed to `Client` so it can enqueue outbound frames.
#[derive(Debug, Clone)]
pub(crate) struct WriteTx(pub(crate) mpsc::Sender<WriteFrame>);

impl WriteTx {
    /// Enqueue a frame; awaits if the writer queue is full (back-pressure).
    pub async fn send(&self, f: WriteFrame) -> Result<(), ClientError> {
        self.0.send(f).await.map_err(|_| ClientError::ChannelClosed)
    }
}

/// Spawn a connection that auto-reconnects until cancelled.
///
/// Returns the `WriteTx` once the *first* dial succeeds (so the caller
/// can start publishing immediately); subsequent reconnects are silent.
pub(crate) async fn spawn_connection(
    cfg: ClientConfig,
    pending: Arc<Pending>,
    cancel: CancellationToken,
) -> Result<WriteTx, ClientError> {
    // The writer queue lives across reconnects: we replace the
    // OwnedWriteHalf inside the writer task, but the channel handle the
    // public API holds stays valid for the lifetime of the Client.
    let (tx, mut rx) = mpsc::channel::<WriteFrame>(cfg.write_queue_capacity);

    // Initial connect must succeed (synchronously) so we can return a
    // ready writer to the caller. HELLO is sent inside `run_session`
    // (so every reconnect emits it on the fresh socket).
    let first = TcpStream::connect(&cfg.addr).await?;
    let (read_h, mut write_h) = first.into_split();
    write_handshake(&mut write_h).await?;

    // First session driver — owns the initial socket halves.
    let pending_w = Arc::clone(&pending);
    let cancel_w = cancel.clone();
    let cfg_w = cfg.clone();

    tokio::spawn(async move {
        // Run the first session until it returns, then loop on reconnect.
        let mut wh = Some(write_h);
        let mut rh = Some(read_h);
        let mut back = Backoff::new(&cfg_w.reconnect);

        loop {
            // ── run the current session ─────────────────────────────
            let session_cancel = cancel_w.child_token();
            let pending_r = Arc::clone(&pending_w);

            // Take ownership of write/read halves for this iteration.
            let wh_now = wh.take();
            let rh_now = rh.take();

            let res = if let (Some(w), Some(r)) = (wh_now, rh_now) {
                run_session(&mut rx, w, r, pending_r, session_cancel.clone()).await
            } else {
                Err(ClientError::Disconnected)
            };

            // Wake every pending request — the socket is gone.
            pending_w.drain_disconnected();

            // Top-level cancel? exit loop, the writer queue closes when
            // the public-side senders drop.
            if cancel_w.is_cancelled() {
                debug!("connection cancelled");
                return;
            }

            warn!(error = ?res, "session ended, will reconnect");

            // ── reconnect with backoff ──────────────────────────────
            loop {
                let Some(delay) = back.next() else {
                    debug!("reconnect attempts exhausted");
                    return;
                };
                tokio::select! {
                    _ = cancel_w.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
                match TcpStream::connect(&cfg_w.addr).await {
                    Ok(s) => {
                        let (r, mut w) = s.into_split();
                        if let Err(e) = write_handshake(&mut w).await {
                            warn!(?e, "handshake write failed");
                            continue;
                        }
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

    Ok(WriteTx(tx))
}

/// Write the v2 HELLO handshake. Must be called before any other frame
/// on a fresh socket.
async fn write_handshake(w: &mut tokio::net::tcp::OwnedWriteHalf) -> Result<(), ClientError> {
    let hello = encode_hello_v2(Role::Client, cap::REPLY | cap::BATCH_HEADERS);
    w.write_all(&hello).await?;
    Ok(())
}

/// Drive one session: spawn writer (consumes the queue) and reader
/// (decodes replies). Returns when *either* task finishes.
async fn run_session(
    rx: &mut mpsc::Receiver<WriteFrame>,
    w: tokio::net::tcp::OwnedWriteHalf,
    r: tokio::net::tcp::OwnedReadHalf,
    pending: Arc<Pending>,
    cancel: CancellationToken,
) -> Result<(), ClientError> {
    // We need a fresh per-session channel to give writer ownership of
    // the queue receiver. We drain `rx` (the long-lived receiver) into
    // a per-session forwarder, so the writer can `recv_many` from a
    // local channel and we can detect socket close by observing the
    // forwarder task exit.
    let (fwd_tx, fwd_rx) = mpsc::channel::<WriteFrame>(1024);

    let cancel_w = cancel.clone();
    let writer_h = tokio::spawn(writer_task(fwd_rx, w, cancel_w));

    let cancel_r = cancel.clone();
    let reader_h = tokio::spawn(reader_task(r, pending, cancel_r));

    // Forwarder: copies from the long-lived `rx` to the per-session
    // `fwd_tx`. Exits when the public senders all drop or the session
    // cancels.
    let cancel_f = cancel.clone();
    let fwd = async move {
        loop {
            tokio::select! {
                biased;
                _ = cancel_f.cancelled() => return Ok::<(), ClientError>(()),
                msg = rx.recv() => {
                    let Some(f) = msg else { return Ok(()) };
                    if fwd_tx.send(f).await.is_err() {
                        // Writer task gone — bail so caller reconnects.
                        return Err(ClientError::ChannelClosed);
                    }
                }
            }
        }
    };

    // Whichever task finishes first triggers the cancel for the others.
    let result: Result<(), ClientError> = tokio::select! {
        r = writer_h => match r { Ok(v) => v, Err(_) => Err(ClientError::ChannelClosed) },
        r = reader_h => match r { Ok(v) => v, Err(_) => Err(ClientError::Disconnected) },
        r = fwd      => r,
    };

    cancel.cancel();
    let _ = result;
    Ok(())
}
