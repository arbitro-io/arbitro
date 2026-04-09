//! ArbitroServer — TCP accept loop, per-connection I/O, keepalive, shutdown.

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::IntoBytes;

use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::wire::delivery::RepErrorAction;
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};

use arbitro_proto::lifecycle::LifeCycle;

use crate::command_log::SharedCommandLog;
use crate::config::Config;
use crate::dispatch;
use crate::drain_task;
use crate::router::Server;
use crate::transport::ConnectionRegistry;

/// The running server — owns the shard router, connection registry, and lifecycle services.
pub struct ArbitroServer {
    config: Config,
    server: Server,
    registry: ConnectionRegistry,
    services: Vec<Box<dyn LifeCycle>>,
    command_log: Option<SharedCommandLog>,
}

impl ArbitroServer {
    pub fn new(config: Config) -> Self {
        let registry = ConnectionRegistry::new(config.write_buffer_cap);
        let server = Server::spawn(&config, &registry);

        let mut services: Vec<Box<dyn LifeCycle>> = Vec::new();
        services.push(Box::new(registry.clone()));

        Self {
            config,
            server,
            registry,
            services,
            command_log: None,
        }
    }

    /// Register a lifecycle service. Called before `run()`.
    pub fn register(&mut self, service: Box<dyn LifeCycle>) {
        self.services.push(service);
    }

    /// Set the shared command log for metadata persistence.
    /// Also registers it as a lifecycle service.
    pub fn set_command_log(&mut self, log: SharedCommandLog) {
        self.services.push(Box::new(log.clone()));
        self.command_log = Some(log);
    }

    /// Access the server configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Access the shard router.
    pub fn server(&self) -> &Server {
        &self.server
    }

    /// Access the connection registry.
    pub fn registry(&self) -> &ConnectionRegistry {
        &self.registry
    }

    /// Run the server — blocks until shutdown.
    pub async fn run(self) -> std::io::Result<()> {
        let (_tx, rx) = watch::channel(false);
        self.run_with_shutdown(rx).await
    }

    /// Run the server with an external shutdown signal.
    pub async fn run_with_shutdown(
        mut self,
        mut _stop_rx: watch::Receiver<bool>,
    ) -> std::io::Result<()> {
        // ── LifeCycle: on_init ──────────────────────────────────────────
        for service in &mut self.services {
            service.on_init();
        }

        // ── Replay command log → re-create streams/consumers ───────────
        if let Some(ref log) = self.command_log {
            let server = self.server.clone();
            let mut applier = crate::recovery::ReplayApplier::new(server);
            match log.replay(&mut applier) {
                Ok(n) if n > 0 => tracing::info!(count = n, "metadata replay complete"),
                Ok(_) => {}
                Err(e) => tracing::error!(error = %e, "metadata replay failed"),
            }
            // Flush any pending async commands from replay
            applier.flush().await;
        }

        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        tracing::info!(addr = %self.config.listen_addr, "listening");

        // Internal shutdown signal
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Spawn drain tasks — one per shard
        let mut drain_handles = Vec::with_capacity(self.server.shard_count());
        for i in 0..self.server.shard_count() {
            let shard = self.server.shard(i).clone();
            let gate = self.server.gate(i).clone();
            let handle = drain_task::spawn_drain_task(shard, gate, shutdown_rx.clone());
            drain_handles.push(handle);
        }

        // Keepalive + idle timeout task
        let keepalive_registry = self.registry.clone();
        let idle_timeout = self.config.idle_timeout;
        let keepalive_interval = self.config.keepalive_interval;
        let keepalive_handle = tokio::spawn(async move {
            keepalive_loop(keepalive_registry, idle_timeout, keepalive_interval).await;
        });

        // Frame channel — read loops send parsed frames here
        let (frame_tx, mut frame_rx) = mpsc::channel::<(u64, Bytes)>(65536);

        // Accept task
        let accept_registry = self.registry.clone();
        let max_connections = self.config.max_connections;
        let mut shutdown_accept = shutdown_rx.clone();
        let accept_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, addr)) => {
                                if accept_registry.active_count() >= max_connections as usize {
                                    tracing::warn!(%addr, "max connections reached, rejecting");
                                    let _ = reject_connection(stream).await;
                                    continue;
                                }

                                let (conn_id, rx) = accept_registry.register();
                                tracing::debug!(conn_id, %addr, "accepted");

                                let _ = stream.set_nodelay(true);
                                let (reader, writer) = stream.into_split();

                                tokio::spawn(write_loop(conn_id, writer, rx));

                                let tx = frame_tx.clone();
                                let reg = accept_registry.clone();
                                tokio::spawn(read_loop(conn_id, reader, tx, reg));
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

        // Bridge external shutdown → internal shutdown
        let bridge_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            let _ = _stop_rx.changed().await;
            let _ = bridge_tx.send(true);
        });

        // Frame processing loop
        let mut shutdown_process = shutdown_rx.clone();
        loop {
            tokio::select! {
                frame = frame_rx.recv() => {
                    match frame {
                        Some((conn_id, frame)) => {
                            dispatch::dispatch_frame(
                                conn_id, frame, &self.server, &self.registry,
                                self.command_log.as_ref(),
                            ).await;
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

        let _ = shutdown_tx.send(true);
        accept_handle.abort();
        keepalive_handle.abort();

        // Send ServerShuttingDown to all connections
        let all_conns = self.registry.all_conn_ids();
        for conn_id in &all_conns {
            send_shutdown_frame(&self.registry, *conn_id);
        }

        // Shutdown shard workers
        self.server.shutdown();

        // Wait briefly for write loops to drain
        tokio::time::sleep(self.config.shutdown_timeout).await;

        // Force close remaining
        for conn_id in &all_conns {
            self.registry.remove(*conn_id);
        }

        // Abort drain tasks
        for handle in drain_handles {
            handle.abort();
        }

        while frame_rx.try_recv().is_ok() {}

        // ── LifeCycle: on_shutdown ──────────────────────────────────────
        for service in &mut self.services {
            service.on_shutdown();
        }

        tracing::info!("shutdown complete");
        Ok(())
    }
}

/// Read loop — reads frames from TCP, sends to processing channel.
async fn read_loop(
    conn_id: u64,
    mut reader: tokio::net::tcp::OwnedReadHalf,
    tx: mpsc::Sender<(u64, Bytes)>,
    registry: ConnectionRegistry,
) {
    let mut header_buf = [0u8; ENVELOPE_SIZE];

    loop {
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
        let msg_len =
            u32::from_le_bytes([header_buf[8], header_buf[9], header_buf[10], header_buf[11]])
                as usize;

        let total = ENVELOPE_SIZE + msg_len;
        let mut buf = BytesMut::with_capacity(total);
        buf.extend_from_slice(&header_buf);

        if msg_len > 0 {
            buf.resize(total, 0);
            if let Err(e) = reader.read_exact(&mut buf[ENVELOPE_SIZE..]).await {
                tracing::debug!(conn_id, error = %e, "read body error");
                break;
            }
        }

        registry.touch(conn_id);

        if tx.send((conn_id, buf.freeze())).await.is_err() {
            break;
        }
    }

    // Build and send disconnect frame
    let disconnect = Bytes::copy_from_slice(build_disconnect_frame().as_bytes());
    let _ = tx.send((conn_id, disconnect)).await;
    registry.remove(conn_id);
}

/// Write loop — drains channel, coalesces frames, writes with write_vectored.
async fn write_loop(
    _conn_id: u64,
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut rx: mpsc::Receiver<Bytes>,
) {
    let mut batch: Vec<Bytes> = Vec::with_capacity(64);

    loop {
        match rx.recv().await {
            Some(frame) => batch.push(frame),
            None => break,
        }

        // Coalesce: drain all ready frames without blocking
        while let Ok(frame) = rx.try_recv() {
            batch.push(frame);
        }

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
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write_vectored returned 0",
            ));
        }
        written += n;

        // Advance past consumed slices
        let mut skip = n;
        while !slices.is_empty() && skip >= slices[0].len() {
            skip -= slices[0].len();
            slices.remove(0);
        }
        if skip > 0 && !slices.is_empty() {
            let remaining_frame_idx = frames.len() - slices.len();
            writer
                .write_all(&frames[remaining_frame_idx][skip..])
                .await?;
            for frame in &frames[remaining_frame_idx + 1..] {
                writer.write_all(frame).await?;
            }
            return Ok(());
        }
    }
    Ok(())
}

fn build_disconnect_frame() -> Envelope {
    Envelope {
        action: U16::new(Action::Disconnect.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(0),
        env_seq: U32::new(0),
    }
}

fn send_shutdown_frame(registry: &ConnectionRegistry, conn_id: u64) {
    let envelope = Envelope {
        action: U16::new(Action::RepError.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(16),
        env_seq: U32::new(0),
    };
    let body = RepErrorAction {
        ref_seq: U64::new(0),
        error_code: U16::new(ErrorCode::ServerShuttingDown.as_u16()),
        _pad: [0u8; 6],
    };
    registry.send_parts(conn_id, &[envelope.as_bytes(), body.as_bytes()]);
}

async fn reject_connection(mut stream: tokio::net::TcpStream) -> std::io::Result<()> {
    let envelope = Envelope {
        action: U16::new(Action::RepError.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(16),
        env_seq: U32::new(0),
    };
    let body = RepErrorAction {
        ref_seq: U64::new(0),
        error_code: U16::new(ErrorCode::InternalError.as_u16()),
        _pad: [0u8; 6],
    };
    stream.write_all(envelope.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn keepalive_loop(
    registry: ConnectionRegistry,
    idle_timeout: std::time::Duration,
    keepalive_interval: std::time::Duration,
) {
    let mut interval = tokio::time::interval(keepalive_interval);

    loop {
        interval.tick().await;

        let idle = registry.idle_connections(idle_timeout);
        for conn_id in idle {
            tracing::info!(conn_id, "idle timeout, closing");
            registry.remove(conn_id);
        }

        let need_ping = registry.connections_needing_ping(keepalive_interval);
        for conn_id in need_ping {
            send_ping(&registry, conn_id);
        }
    }
}

fn send_ping(registry: &ConnectionRegistry, conn_id: u64) {
    let envelope = Envelope {
        action: U16::new(Action::Ping.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(0),
        env_seq: U32::new(0),
    };
    registry.send_parts(conn_id, &[envelope.as_bytes()]);
}
