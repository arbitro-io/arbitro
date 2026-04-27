//! Connection — TCP connection with auto-reconnect.
//!
//! ## Thread topology
//!
//! - `connection_loop` (tokio task): owns reconnect/backoff state machine.
//! - `read_loop`       (tokio task): async, reads frames from `OwnedReadHalf`.
//! - `write_loop`      (OS thread):  drains `kit::Mpsc<Bytes>`, writes to
//!                                    `std::net::TcpStream` with `write_vectored`.
//! - `ack_loop`        (OS thread):  drains `kit::Mpsc<AckCmd>`, builds
//!                                    `BatchAck` frames, enqueues into write ring.
//!
//! `kit::MpscConsumer::recv()` parks the OS thread — incompatible with the
//! tokio runtime, so write_loop and ack_loop run on dedicated OS threads.
//! The TCP socket is split via `into_std()` + `try_clone()`: read half goes
//! back into tokio (`TcpStream::from_std`), write half stays std.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use arbitro_kit::route::{Mpsc, MpscConsumer};

use arbitro_proto::action::Action;
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_proto::wire::system::ConnectFixed;

use crate::inner::{
    ConnState, Inner, WriteProducer, ACK_RING_CAP, WRITE_RING_CAP,
};
use crate::message::AckCmd;

/// Spawn the connection manager task.
pub(crate) fn spawn_connection(inner: Arc<Inner>) {
    tokio::spawn(connection_loop(inner));
}

async fn connection_loop(inner: Arc<Inner>) {
    let mut backoff_ms: u64 = 100;
    const MAX_BACKOFF_MS: u64 = 30_000;

    loop {
        inner.state_tx.send_replace(ConnState::Reconnecting);

        match TcpStream::connect(&inner.addr).await {
            Ok(stream) => {
                backoff_ms = 100;
                let _ = stream.set_nodelay(true);

                if run_session(&inner, stream).await {
                    inner.state_tx.send_replace(ConnState::Disconnected);
                    break;
                }

                inner.state_tx.send_replace(ConnState::Disconnected);
            }
            Err(e) => {
                tracing::debug!(error = %e, addr = %inner.addr, "connect failed");
            }
        }

        inner.clear_write_producer();
        inner.drain_pending();

        inner.state_tx.send_replace(ConnState::Disconnected);

        let jitter = backoff_ms / 4;
        let sleep_ms = backoff_ms + (rand_u64() % jitter.max(1));
        tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
    }
}

/// Run a single TCP session. Returns true if clean shutdown.
async fn run_session(inner: &Arc<Inner>, stream: TcpStream) -> bool {
    // Convert tokio stream → std stream so we can hand the write half to a
    // sync OS thread. The read half goes back to tokio for the async loop.
    let std_stream = match stream.into_std() {
        Ok(s) => s,
        Err(_) => return false,
    };
    let std_write = match std_stream.try_clone() {
        Ok(c) => c,
        Err(_) => return false,
    };
    // Re-attach the read side to tokio.
    if std_stream.set_nonblocking(true).is_err() {
        return false;
    }
    let read_half = match TcpStream::from_std(std_stream) {
        Ok(s) => s,
        Err(_) => return false,
    };

    // Build write channel: M=1 producer (Mutex-shared), 1 consumer on write thread.
    let (mut write_producers, write_consumer, write_shutdown) =
        Mpsc::<Bytes, WRITE_RING_CAP>::new(1);
    let write_producer: WriteProducer =
        Arc::new(Mutex::new(write_producers.pop().unwrap()));
    inner.install_write_producer(write_producer.clone());

    // Build ack channel: M=1 producer (Mutex-shared, lives in Inner.ack_tx),
    // 1 consumer on ack thread.
    let (ack_consumer, ack_shutdown) = inner.new_ack_channel();

    // Spawn write OS thread BEFORE first frame so handshake doesn't race.
    let write_thread = std::thread::Builder::new()
        .name("arbitro-client-write".into())
        .spawn(move || write_loop(std_write, write_consumer))
        .expect("spawn write thread");

    // Send Connect handshake (synchronous: pushes to write ring).
    send_connect(inner);

    // Re-subscribe on every session.
    resubscribe_all(inner);

    inner.state_tx.send_replace(ConnState::Connected);

    // Spawn ack OS thread. Needs a clone of the write producer to enqueue
    // BatchAck frames built from drained AckCmds.
    let ack_write_producer = write_producer.clone();
    let ack_thread = std::thread::Builder::new()
        .name("arbitro-client-ack".into())
        .spawn(move || ack_loop(ack_consumer, ack_write_producer))
        .expect("spawn ack thread");

    // Run read loop on this tokio task.
    read_loop(inner, read_half).await;

    // Read loop ended — tear the session down cleanly.
    inner.clear_write_producer();
    write_shutdown.signal();
    ack_shutdown.signal();

    // Join OS threads (best-effort: their consumers wake on shutdown).
    let _ = write_thread.join();
    let _ = ack_thread.join();

    false
}

// ── Connect / Resubscribe ───────────────────────────────────────────────

/// Push the Connect frame onto the write ring. Sync; the write OS thread
/// drains it.
fn send_connect(inner: &Arc<Inner>) {
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
    inner.send_frame(Bytes::from(frame));
}

/// Re-send Subscribe frames for all active subscriptions.
fn resubscribe_all(inner: &Arc<Inner>) {
    let frames: Vec<Bytes> = {
        let subs = inner.subscriptions.read().unwrap();
        let mut out = Vec::new();
        for by_consumer in subs.values() {
            for entries in by_consumer.values() {
                for &(_, ref sub) in entries {
                    let body = &sub.subscribe_body;
                    let envelope = Envelope {
                        action: U16::new(Action::Subscribe.as_u16()),
                        flags: 0,
                        _rsv: 0,
                        stream_id: U32::new(sub.stream_id),
                        msg_len: U32::new(body.len() as u32),
                        env_seq: U32::new(0),
                    };
                    let mut frame = Vec::with_capacity(ENVELOPE_SIZE + body.len());
                    frame.extend_from_slice(envelope.as_bytes());
                    frame.extend_from_slice(body);
                    out.push(Bytes::from(frame));
                }
            }
        }
        out
    };

    for frame in frames {
        inner.send_frame(frame);
    }
}

// ── Read loop (async) ───────────────────────────────────────────────────

async fn read_loop(inner: &Arc<Inner>, mut reader: TcpStream) {
    use bytes::BytesMut;

    let mut buf = BytesMut::with_capacity(64 * 1024);

    loop {
        while buf.len() < ENVELOPE_SIZE {
            match reader.read_buf(&mut buf).await {
                Ok(0) => return,
                Ok(_) => {}
                Err(_) => return,
            }
        }

        let msg_len = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;
        let total = ENVELOPE_SIZE + msg_len;

        while buf.len() < total {
            match reader.read_buf(&mut buf).await {
                Ok(0) => return,
                Ok(_) => {}
                Err(_) => return,
            }
        }

        let frame = buf.split_to(total);
        inner.on_frame(&frame);
    }
}

// ── Write loop (OS thread) ──────────────────────────────────────────────

/// Drain the write ring, batch-coalesce, write_vectored. Runs on a
/// dedicated OS thread (sync). Returns when the consumer observes shutdown.
fn write_loop(mut writer: std::net::TcpStream, consumer: MpscConsumer<Bytes, WRITE_RING_CAP>) {
    use std::io::Write;

    consumer.bind();

    let _ = writer.set_nonblocking(false);

    let mut batch: Vec<Bytes> = Vec::with_capacity(64);

    loop {
        // Block for the first frame.
        match consumer.recv() {
            Ok(frame) => batch.push(frame),
            Err(_shutdown) => return,
        }

        // Drain everything currently available — coalesce.
        while let Some(frame) = consumer.try_recv() {
            batch.push(frame);
        }

        let result = if batch.len() == 1 {
            writer.write_all(&batch[0])
        } else {
            write_all_vectored(&mut writer, &batch)
        };

        if result.is_err() {
            return;
        }
        batch.clear();
    }
}

/// Sync write_vectored with partial-write handling.
fn write_all_vectored(
    writer: &mut std::net::TcpStream,
    frames: &[Bytes],
) -> std::io::Result<()> {
    use std::io::{IoSlice, Write};

    let mut slices: Vec<IoSlice<'_>> = frames.iter().map(|f| IoSlice::new(f)).collect();
    let total: usize = frames.iter().map(|f| f.len()).sum();
    let mut written = 0usize;

    while written < total {
        let n = writer.write_vectored(&slices)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write_vectored returned 0",
            ));
        }
        written += n;

        let mut skip = n;
        while !slices.is_empty() && skip >= slices[0].len() {
            skip -= slices[0].len();
            slices.remove(0);
        }
        if skip > 0 && !slices.is_empty() {
            let remaining_idx = frames.len() - slices.len();
            writer.write_all(&frames[remaining_idx][skip..])?;
            for frame in &frames[remaining_idx + 1..] {
                writer.write_all(frame)?;
            }
            return Ok(());
        }
    }
    Ok(())
}

// ── Ack loop (OS thread) ────────────────────────────────────────────────

const ACK_BATCH_MAX: usize = 256;

fn ack_loop(consumer: MpscConsumer<AckCmd, ACK_RING_CAP>, write_producer: WriteProducer) {
    consumer.bind();

    let mut pending_acks: Vec<(u32, u32, u64)> = Vec::with_capacity(ACK_BATCH_MAX);
    let mut batch_buf: Vec<u8> = Vec::with_capacity(4096);

    loop {
        // recv_batch: blocks until at least one cmd arrives, then drains
        // every cmd available in this pass.
        let result = consumer.recv_batch(|cmd| {
            process_ack_cmd(cmd, &mut pending_acks, &write_producer);
        });

        match result {
            Ok(_) => {}
            Err(_shutdown) => return,
        }

        if !pending_acks.is_empty() {
            flush_batch_acks(&pending_acks, &write_producer, &mut batch_buf);
            pending_acks.clear();
        }
    }
}

#[inline]
fn process_ack_cmd(
    cmd: AckCmd,
    pending_acks: &mut Vec<(u32, u32, u64)>,
    write_producer: &WriteProducer,
) {
    match cmd {
        AckCmd::Ack { stream_id, consumer_id, seq } => {
            pending_acks.push((stream_id, consumer_id, seq));
        }
        AckCmd::Nack { stream_id, consumer_id, seq } => {
            let frame = build_ack_frame(Action::Nack, stream_id, consumer_id, seq);
            let _ = write_producer.lock().unwrap().try_send(frame);
        }
    }
}

fn flush_batch_acks(
    pending: &[(u32, u32, u64)],
    write_producer: &WriteProducer,
    buf: &mut Vec<u8>,
) {
    let mut sorted: Vec<(u32, u32, u64)> = pending.to_vec();
    sorted.sort_unstable_by_key(|&(sid, cid, _)| (sid, cid));

    let mut i = 0;
    while i < sorted.len() {
        let (stream_id, consumer_id, _) = sorted[i];

        let mut j = i;
        while j < sorted.len() && sorted[j].0 == stream_id && sorted[j].1 == consumer_id {
            j += 1;
        }

        let group = &sorted[i..j];
        for chunk in group.chunks(ACK_BATCH_MAX) {
            build_batch_ack_frame(stream_id, consumer_id, chunk, buf);
            let _ = write_producer
                .lock()
                .unwrap()
                .try_send(Bytes::from(buf.clone()));
        }

        i = j;
    }
}

fn build_ack_frame(action: Action, stream_id: u32, consumer_id: u32, seq: u64) -> Bytes {
    use bytes::BytesMut;

    let envelope = Envelope {
        action: U16::new(action.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(16),
        env_seq: U32::new(0),
    };

    let mut buf = BytesMut::with_capacity(ENVELOPE_SIZE + 16);
    buf.extend_from_slice(envelope.as_bytes());
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&consumer_id.to_le_bytes());
    buf.extend_from_slice(&[0u8; 4]);
    buf.freeze()
}

fn build_batch_ack_frame(
    stream_id: u32,
    consumer_id: u32,
    seqs: &[(u32, u32, u64)],
    buf: &mut Vec<u8>,
) {
    let count = seqs.len() as u16;
    let body_len = 8 + (seqs.len() * 16);

    buf.clear();

    let envelope = Envelope {
        action: U16::new(Action::BatchAck.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(body_len as u32),
        env_seq: U32::new(0),
    };
    buf.extend_from_slice(envelope.as_bytes());

    buf.extend_from_slice(&consumer_id.to_le_bytes());
    buf.extend_from_slice(&count.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes());

    for &(_, _, seq) in seqs {
        buf.extend_from_slice(&seq.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
    }
}

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
