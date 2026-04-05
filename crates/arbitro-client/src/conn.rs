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

/// Max sequences per BatchAck frame.
const ACK_BATCH_MAX: usize = 256;

/// Process ack/nack commands from Message handles.
///
/// Accumulates acks: recv first, drain with try_recv, group by
/// (stream_id, consumer_id), send one BatchAck frame per group.
/// Nacks go individually (rare, latency-sensitive).
async fn ack_loop(inner: Arc<Inner>, write_tx: mpsc::Sender<Bytes>) {
    let mut rx = {
        let mut guard = inner.ack_rx.lock().unwrap();
        match guard.take() {
            Some(rx) => rx,
            None => return,
        }
    };

    // Scratch buffers — reused across iterations, capacity grows monotonically.
    let mut pending_acks: Vec<(u32, u32, u64)> = Vec::with_capacity(ACK_BATCH_MAX);
    let mut batch_buf: Vec<u8> = Vec::with_capacity(4096);

    while let Some(cmd) = rx.recv().await {
        // Process first command
        process_ack_cmd(cmd, &mut pending_acks, &write_tx);

        // Drain all available — coalesce into batch
        while let Ok(cmd) = rx.try_recv() {
            process_ack_cmd(cmd, &mut pending_acks, &write_tx);
        }

        // Flush accumulated acks as BatchAck frames
        if !pending_acks.is_empty() {
            flush_batch_acks(&pending_acks, &write_tx, &mut batch_buf);
            pending_acks.clear();
        }
    }
}

/// Route a single AckCmd: acks go to pending batch, nacks send immediately.
#[inline]
fn process_ack_cmd(
    cmd: AckCmd,
    pending_acks: &mut Vec<(u32, u32, u64)>,
    write_tx: &mpsc::Sender<Bytes>,
) {
    match cmd {
        AckCmd::Ack { stream_id, consumer_id, seq } => {
            pending_acks.push((stream_id, consumer_id, seq));
        }
        AckCmd::Nack { stream_id, consumer_id, seq } => {
            let frame = build_ack_frame(Action::Nack, stream_id, consumer_id, seq);
            let _ = write_tx.try_send(frame);
        }
    }
}

/// Group pending acks by (stream_id, consumer_id) and send BatchAck frames.
fn flush_batch_acks(
    pending: &[(u32, u32, u64)],
    write_tx: &mpsc::Sender<Bytes>,
    buf: &mut Vec<u8>,
) {
    // Sort by (stream_id, consumer_id) so we can group in one pass.
    // For the common case (single consumer), this is a no-op.
    let mut sorted: Vec<(u32, u32, u64)> = pending.to_vec();
    sorted.sort_unstable_by_key(|&(sid, cid, _)| (sid, cid));

    let mut i = 0;
    while i < sorted.len() {
        let (stream_id, consumer_id, _) = sorted[i];

        // Find the end of this group
        let mut j = i;
        while j < sorted.len() && sorted[j].0 == stream_id && sorted[j].1 == consumer_id {
            j += 1;
        }

        // Send in chunks of ACK_BATCH_MAX
        let group = &sorted[i..j];
        for chunk in group.chunks(ACK_BATCH_MAX) {
            build_batch_ack_frame(stream_id, consumer_id, chunk, buf);
            let _ = write_tx.try_send(Bytes::copy_from_slice(buf));
        }

        i = j;
    }
}

/// Build a single Ack or Nack frame (32B). Used for nacks.
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

/// Build a BatchAck frame into `buf`.
///
/// Wire: [16B envelope][8B BatchAckFixed][N × 8B seq]
fn build_batch_ack_frame(
    stream_id: u32,
    consumer_id: u32,
    seqs: &[(u32, u32, u64)], // (stream_id, consumer_id, seq) — all same group
    buf: &mut Vec<u8>,
) {
    let count = seqs.len() as u16;
    let body_len = 8 + (seqs.len() * 8); // BatchAckFixed + N × u64

    buf.clear();

    // Envelope
    let envelope = Envelope {
        action: U16::new(Action::BatchAck.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(body_len as u32),
        env_seq: U32::new(0),
    };
    buf.extend_from_slice(envelope.as_bytes());

    // BatchAckFixed: [4 consumer_id][2 count][2 pad]
    buf.extend_from_slice(&consumer_id.to_le_bytes());
    buf.extend_from_slice(&count.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes());

    // Sequences
    for &(_, _, seq) in seqs {
        buf.extend_from_slice(&seq.to_le_bytes());
    }
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
