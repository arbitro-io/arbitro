//! ArbitroServer — TCP accept loop, per-connection I/O, keepalive, shutdown.

use std::sync::Arc;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use arbitro_engine::{Engine, EngineBuilder, Transport};
use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_proto::wire::delivery::RepErrorAction;

use crate::config::Config;
use crate::drain_task;
use crate::gate::Gate;
use crate::transport::TokioTransport;

/// The running server.
pub struct ArbitroServer {
    config: Config,
    engine: Engine,
    transport: Arc<TokioTransport>,
}

impl ArbitroServer {
    pub fn new(config: Config, transport: Arc<TokioTransport>, metadata: Option<Arc<arbitro_metadata::MetadataLog>>) -> Self {
        let streams_for_factory = Arc::new(std::sync::Mutex::new(None::<std::sync::Arc<arbitro_engine::stream::StreamMap>>));
        let transport_for_factory: Arc<dyn Transport> = transport.clone();
        let streams_clone = streams_for_factory.clone();

        let signal_factory: arbitro_engine::SignalFactory = Box::new(move |stream_id| {
            let gate = Arc::new(Gate::new());
            let streams = streams_clone.lock().unwrap().clone()
                .expect("streams must be set before creating streams");
            drain_task::spawn_drain_task(stream_id, gate.clone(), streams, transport_for_factory.clone());
            gate
        });

        let mut engine_builder = EngineBuilder::new()
            .transport(transport.clone())
            .signal_factory(signal_factory);

        if let Some(m) = metadata {
            engine_builder = engine_builder.metadata(m);
        }

        let engine = engine_builder.build();

        // Set the streams Arc so the factory can use it
        *streams_for_factory.lock().unwrap() = Some(engine.streams().clone());

        Self { config, engine, transport }
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Run the server — blocks until shutdown.
    pub async fn run(mut self) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        tracing::info!(addr = %self.config.listen_addr, "listening");

        // Shutdown signal
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // Keepalive + idle timeout task
        let keepalive_transport = self.transport.clone();
        let idle_timeout = self.config.idle_timeout;
        let keepalive_interval = self.config.keepalive_interval;
        let keepalive_handle = tokio::spawn(async move {
            keepalive_loop(keepalive_transport, idle_timeout, keepalive_interval).await;
        });

        // Accept loop
        let accept_transport = self.transport.clone();
        let max_connections = self.config.max_connections;
        // Engine is !Send (scratch buffers), so we process frames on the current task
        // via a channel from read loops.
        let (frame_tx, mut frame_rx) = mpsc::channel::<(u64, Vec<u8>)>(65536);

        // Accept task
        let mut shutdown_accept = shutdown_rx.clone();
        let accept_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, addr)) => {
                                let conn_count = accept_transport.active_count();
                                if conn_count >= max_connections as usize {
                                    tracing::warn!(%addr, "max connections reached, rejecting");
                                    // Send RepError and close
                                    let _ = reject_connection(stream).await;
                                    continue;
                                }

                                let (conn_id, rx) = accept_transport.register();
                                tracing::debug!(conn_id, %addr, "accepted");

                                let _ = stream.set_nodelay(true);
                                let (reader, writer) = stream.into_split();

                                // Spawn write loop
                                tokio::spawn(write_loop(conn_id, writer, rx));

                                // Spawn read loop
                                let tx = frame_tx.clone();
                                let t = accept_transport.clone();
                                tokio::spawn(read_loop(conn_id, reader, tx, t));
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "accept failed");
                            }
                        }
                    }
                    _ = shutdown_accept.changed() => {
                        tracing::info!("accept loop stopping");
                        break;
                    }
                }
            }
        });

        // Frame processing loop — single-threaded engine processing
        let mut shutdown_process = shutdown_rx.clone();
        loop {
            tokio::select! {
                frame = frame_rx.recv() => {
                    match frame {
                        Some((conn_id, buf)) => {
                            self.engine.process_frame(conn_id, &buf);
                        }
                        None => break,
                    }
                }
                _ = shutdown_process.changed() => {
                    tracing::info!("frame processing stopping");
                    break;
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("ctrl+c received, initiating shutdown");
                    let _ = shutdown_tx.send(true);
                    break;
                }
            }
        }

        // Graceful shutdown
        tracing::info!("shutting down...");

        // 1. Stop accepting
        let _ = shutdown_tx.send(true);
        accept_handle.abort();
        keepalive_handle.abort();

        // 2. Send ServerShuttingDown to all connections
        self.transport.drain_all();
        let all_conns = self.transport.all_conn_ids();
        for conn_id in &all_conns {
            send_shutdown_frame(&self.transport, *conn_id);
        }

        // 3. Flush engine stores
        self.engine.shutdown();

        // 4. Wait for write loops to drain (with timeout)
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // 5. Force close remaining
        for conn_id in &all_conns {
            self.transport.remove(*conn_id);
        }

        // Drain remaining frames in channel
        while frame_rx.try_recv().is_ok() {}

        tracing::info!("shutdown complete");
        Ok(())
    }
}

/// Read loop — reads frames from TCP, sends to engine via channel.
async fn read_loop(
    conn_id: u64,
    mut reader: tokio::net::tcp::OwnedReadHalf,
    tx: mpsc::Sender<(u64, Vec<u8>)>,
    transport: Arc<TokioTransport>,
) {
    let mut header_buf = [0u8; ENVELOPE_SIZE];

    loop {
        // Read envelope (16 bytes)
        match reader.read_exact(&mut header_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                tracing::debug!(conn_id, "client disconnected (EOF)");
                break;
            }
            Err(e) => {
                tracing::debug!(conn_id, error = %e, "read error");
                break;
            }
        }

        // Parse msg_len from envelope (bytes 8..12, little-endian u32)
        let msg_len = u32::from_le_bytes([
            header_buf[8], header_buf[9], header_buf[10], header_buf[11],
        ]) as usize;

        // Read body
        let total = ENVELOPE_SIZE + msg_len;
        let mut frame = vec![0u8; total];
        frame[..ENVELOPE_SIZE].copy_from_slice(&header_buf);

        if msg_len > 0 {
            if let Err(e) = reader.read_exact(&mut frame[ENVELOPE_SIZE..]).await {
                tracing::debug!(conn_id, error = %e, "read body error");
                break;
            }
        }

        // Update activity
        transport.touch(conn_id);

        // Send to engine for processing
        if tx.send((conn_id, frame)).await.is_err() {
            break; // Engine shut down
        }
    }

    // Send disconnect to engine
    let disconnect = build_disconnect_frame(conn_id);
    let _ = tx.send((conn_id, disconnect)).await;
    transport.remove(conn_id);
}

/// Write loop — drains channel, coalesces frames, writes with write_vectored.
async fn write_loop(
    _conn_id: u64,
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut rx: mpsc::Receiver<Bytes>,
) {
    let mut batch: Vec<Bytes> = Vec::with_capacity(64);

    loop {
        // Wait for first frame
        match rx.recv().await {
            Some(frame) => batch.push(frame),
            None => break, // Sender dropped — connection closing
        }

        // Coalesce: drain all ready frames without blocking
        while let Ok(frame) = rx.try_recv() {
            batch.push(frame);
        }

        // Single frame: write_all (no IoSlice overhead)
        // Multiple frames: write_vectored for single syscall
        let failed = if batch.len() == 1 {
            writer.write_all(&batch[0]).await.is_err()
        } else {
            write_all_vectored(&mut writer, &batch).await.is_err()
        };

        if failed {
            break;
        }

        batch.clear();
    }
}

/// Write all frames via write_vectored. Handles partial writes.
async fn write_all_vectored(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    frames: &[Bytes],
) -> std::io::Result<()> {
    use std::io::IoSlice;

    let mut slices: Vec<IoSlice<'_>> = frames.iter().map(|f| IoSlice::new(f)).collect();
    let total: usize = frames.iter().map(|f| f.len()).sum();
    let mut written = 0usize;

    while written < total {
        let n = writer.write_vectored(&slices).await?;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "write_vectored returned 0"));
        }
        written += n;

        // Advance past consumed slices
        let mut skip = n;
        while !slices.is_empty() && skip >= slices[0].len() {
            skip -= slices[0].len();
            slices.remove(0);
        }
        if skip > 0 && !slices.is_empty() {
            // Partial write in the middle of a slice — fall back to write_all for remainder
            let remaining_frame_idx = frames.len() - slices.len();
            writer.write_all(&frames[remaining_frame_idx][skip..]).await?;
            for frame in &frames[remaining_frame_idx + 1..] {
                writer.write_all(frame).await?;
            }
            return Ok(());
        }
    }
    Ok(())
}

/// Build a Disconnect frame to notify the engine of client departure.
fn build_disconnect_frame(_conn_id: u64) -> Vec<u8> {
    let envelope = Envelope {
        action: U16::new(Action::Disconnect.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(0),
        env_seq: U32::new(0),
    };

    envelope.as_bytes().to_vec()
}

/// Send a ServerShuttingDown error to a connection.
fn send_shutdown_frame(transport: &TokioTransport, conn_id: u64) {
    use arbitro_engine::Transport as _;

    let mut buf = [0u8; 32];
    let envelope = Envelope {
        action: U16::new(Action::RepError.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(16),
        env_seq: U32::new(0),
    };
    buf[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());

    let body = RepErrorAction {
        ref_seq: U64::new(0),
        error_code: U16::new(ErrorCode::ServerShuttingDown.as_u16()),
        _pad: [0u8; 6],
    };
    buf[ENVELOPE_SIZE..].copy_from_slice(body.as_bytes());

    transport.send(conn_id, &buf);
}

/// Reject a connection when at max capacity — send RepError and close.
async fn reject_connection(mut stream: tokio::net::TcpStream) -> std::io::Result<()> {
    let mut buf = [0u8; 32];
    let envelope = Envelope {
        action: U16::new(Action::RepError.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(16),
        env_seq: U32::new(0),
    };
    buf[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());

    let body = RepErrorAction {
        ref_seq: U64::new(0),
        error_code: U16::new(ErrorCode::InternalError.as_u16()),
        _pad: [0u8; 6],
    };
    buf[ENVELOPE_SIZE..].copy_from_slice(body.as_bytes());

    stream.write_all(&buf).await?;
    stream.shutdown().await?;
    Ok(())
}

/// Background task: keepalive pings + idle timeout.
async fn keepalive_loop(
    transport: Arc<TokioTransport>,
    idle_timeout: std::time::Duration,
    keepalive_interval: std::time::Duration,
) {
    let mut interval = tokio::time::interval(keepalive_interval);

    loop {
        interval.tick().await;

        // Close idle connections
        let idle = transport.idle_connections(idle_timeout);
        for conn_id in idle {
            tracing::info!(conn_id, "idle timeout, closing");
            transport.remove(conn_id);
        }

        // Send Ping to connections approaching idle
        let need_ping = transport.connections_needing_ping(keepalive_interval);
        for conn_id in need_ping {
            send_ping(&transport, conn_id);
        }
    }
}

/// Send a Ping frame to a connection.
fn send_ping(transport: &TokioTransport, conn_id: u64) {
    use arbitro_engine::Transport as _;

    let envelope = Envelope {
        action: U16::new(Action::Ping.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(0),
        env_seq: U32::new(0),
    };
    let mut buf = [0u8; ENVELOPE_SIZE];
    buf.copy_from_slice(envelope.as_bytes());
    transport.send(conn_id, &buf);
}
