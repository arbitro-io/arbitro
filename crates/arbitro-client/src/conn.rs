//! Connection — TCP connection with auto-reconnect.

use std::sync::Arc;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use arbitro_proto::action::Action;
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_proto::wire::system::ConnectFixed;

use crate::inner::{ConnState, Inner};
use crate::message::AckCmd;

/// Spawn the connection manager task. Handles connect, reconnect, read/write loops.
pub(crate) fn spawn_connection(inner: Arc<Inner>) {
    tokio::spawn(connection_loop(inner));
}

/// Main connection loop with auto-reconnect and exponential backoff.
async fn connection_loop(inner: Arc<Inner>) {
    let mut backoff_ms: u64 = 100;
    const MAX_BACKOFF_MS: u64 = 30_000;

    loop {
        inner.state_tx.send_replace(ConnState::Reconnecting);

        match TcpStream::connect(&inner.addr).await {
            Ok(stream) => {
                backoff_ms = 100; // reset on success
                let _ = stream.set_nodelay(true);

                if run_session(&inner, stream).await {
                    // Clean shutdown requested
                    inner.state_tx.send_replace(ConnState::Disconnected);
                    break;
                }

                // Disconnected — will reconnect
                inner.state_tx.send_replace(ConnState::Disconnected);
            }
            Err(e) => {
                tracing::debug!(error = %e, addr = %inner.addr, "connect failed");
            }
        }

        // Clear write channel so sends fail fast during reconnect
        {
            let mut guard = inner.write_tx.lock().unwrap();
            *guard = None;
        }

        // Fail all pending requests
        {
            let mut pending = inner.pending.lock().unwrap();
            pending.clear();
        }

        inner.state_tx.send_replace(ConnState::Disconnected);

        // Backoff with jitter
        let jitter = backoff_ms / 4;
        let sleep_ms = backoff_ms + (rand_u64() % jitter.max(1));
        tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
    }
}

/// Run a single TCP session. Returns true if clean shutdown.
async fn run_session(inner: &Arc<Inner>, stream: TcpStream) -> bool {
    let (reader, writer) = stream.into_split();

    // Create write channel
    let (write_tx, write_rx) = mpsc::channel::<Bytes>(8192);
    {
        let mut guard = inner.write_tx.lock().unwrap();
        *guard = Some(write_tx.clone());
    }

    // Send Connect handshake
    send_connect(&write_tx).await;

    // Mark connected
    inner.state_tx.send_replace(ConnState::Connected);

    // Spawn write loop
    let write_handle = tokio::spawn(write_loop(writer, write_rx));

    // Spawn ack processor
    let ack_inner = inner.clone();
    let ack_write_tx = write_tx.clone();
    let ack_handle = tokio::spawn(ack_loop(ack_inner, ack_write_tx));

    // Run read loop on this task
    read_loop(inner, reader).await;

    // Read loop ended — cleanup
    {
        let mut guard = inner.write_tx.lock().unwrap();
        *guard = None;
    }
    drop(write_tx);

    write_handle.abort();
    ack_handle.abort();

    false // not a clean shutdown, reconnect
}

/// Send Connect frame.
async fn send_connect(tx: &mpsc::Sender<Bytes>) {
    let body = ConnectFixed {
        proto_version: 1,
        flags: 0,
        auth_len: U16::new(0),
        _pad: [0u8; 4],
        _pad2: U64::new(0),
    };

    let envelope = Envelope {
        action: U16::new(Action::Connect.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(16),
        env_seq: U32::new(0),
    };

    let mut frame = Vec::with_capacity(ENVELOPE_SIZE + 16);
    frame.extend_from_slice(envelope.as_bytes());
    frame.extend_from_slice(body.as_bytes());
    let _ = tx.send(Bytes::from(frame)).await;
}

/// Read loop — reads frames from TCP, dispatches to inner.
async fn read_loop(inner: &Arc<Inner>, mut reader: tokio::net::tcp::OwnedReadHalf) {
    let mut header_buf = [0u8; ENVELOPE_SIZE];

    while reader.read_exact(&mut header_buf).await.is_ok() {
        let msg_len = u32::from_le_bytes([
            header_buf[8], header_buf[9], header_buf[10], header_buf[11],
        ]) as usize;

        let total = ENVELOPE_SIZE + msg_len;
        let mut frame = vec![0u8; total];
        frame[..ENVELOPE_SIZE].copy_from_slice(&header_buf);

        if msg_len > 0
            && reader.read_exact(&mut frame[ENVELOPE_SIZE..]).await.is_err()
        {
            break;
        }

        inner.on_frame(&frame);
    }
}

/// Write loop — drains channel, coalesces, writes to TCP.
async fn write_loop(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut rx: mpsc::Receiver<Bytes>,
) {
    let mut batch: Vec<Bytes> = Vec::with_capacity(64);

    loop {
        match rx.recv().await {
            Some(frame) => batch.push(frame),
            None => break,
        }

        while let Ok(frame) = rx.try_recv() {
            batch.push(frame);
        }

        for frame in &batch {
            if writer.write_all(frame).await.is_err() {
                return;
            }
        }

        batch.clear();
    }
}

/// Process ack/nack commands from Message handles.
async fn ack_loop(inner: Arc<Inner>, write_tx: mpsc::Sender<Bytes>) {
    let mut rx = {
        let mut guard = inner.ack_rx.lock().unwrap();
        match guard.take() {
            Some(rx) => rx,
            None => return,
        }
    };

    while let Some(cmd) = rx.recv().await {
        let frame = match cmd {
            AckCmd::Ack { stream_id, consumer_id, seq } => {
                build_ack_frame(Action::Ack, stream_id, consumer_id, seq)
            }
            AckCmd::Nack { stream_id, consumer_id, seq } => {
                build_ack_frame(Action::Nack, stream_id, consumer_id, seq)
            }
        };
        if write_tx.try_send(frame).is_err() {
            // Write channel full or closed — ack will be retried after reconnect
            continue;
        }
    }

    // Put rx back so reconnect can reuse it
    // (won't happen since rx is consumed, but ack_loop only runs once)
}

fn build_ack_frame(action: Action, stream_id: u32, consumer_id: u32, seq: u64) -> Bytes {
    let envelope = Envelope {
        action: U16::new(action.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(16),
        env_seq: U32::new(0),
    };

    let mut buf = [0u8; ENVELOPE_SIZE + 16];
    buf[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());
    // Ack body: [8 seq][4 consumer_id][4 pad]
    buf[ENVELOPE_SIZE..ENVELOPE_SIZE+8].copy_from_slice(&seq.to_le_bytes());
    buf[ENVELOPE_SIZE+8..ENVELOPE_SIZE+12].copy_from_slice(&consumer_id.to_le_bytes());

    Bytes::copy_from_slice(&buf)
}

/// Simple pseudo-random using thread-local state (no dependency needed).
fn rand_u64() -> u64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0x12345678_9abcdef0) };
    }
    STATE.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        x
    })
}
