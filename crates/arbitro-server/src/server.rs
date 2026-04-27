//! ArbitroServer — TCP accept loop, per-connection I/O, keepalive, shutdown.

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use zerocopy::byteorder::little_endian::{U16, U64};
use zerocopy::IntoBytes;

use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::wire::delivery::RepErrorAction;
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};

use arbitro_proto::lifecycle::LifeCycle;

use crate::config::Config;
use crate::persistence::command_log::SharedCommandLog;
use crate::shard::router::ShardRouter;
use crate::transport::dispatch;
use crate::transport::ConnectionRegistry;

/// The running server — owns the shard router, connection registry, and lifecycle services.
pub struct ArbitroServer {
    config: Config,
    server: ShardRouter,
    registry: ConnectionRegistry,
    services: Vec<Box<dyn LifeCycle>>,
    command_log: Option<SharedCommandLog>,
}

impl ArbitroServer {
    pub fn new(config: Config) -> Self {
        let registry = ConnectionRegistry::new(config.write_buffer_cap);
        let server = ShardRouter::spawn(&config, &registry);

        let services: Vec<Box<dyn LifeCycle>> = vec![Box::new(registry.clone())];

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
    pub fn server(&self) -> &ShardRouter {
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
            let mut applier = crate::persistence::recovery::ReplayApplier::new(server);
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

        // Keepalive + idle timeout task
        let keepalive_registry = self.registry.clone();
        let idle_timeout = self.config.idle_timeout;
        let keepalive_interval = self.config.keepalive_interval;
        let keepalive_handle = tokio::spawn(async move {
            keepalive_loop(keepalive_registry, idle_timeout, keepalive_interval).await;
        });

        // Per-connection ingress: each read_loop dispatches frames directly.
        // No central mpsc — TCP guarantees order per connection, and each
        // connection already runs in its own task, so we get free parallelism
        // on the dispatch path. Server state (shard router, registry, command
        // log) is cloneable Arc-backed and shared across read tasks.

        // Accept task
        let accept_registry = self.registry.clone();
        let max_connections = self.config.max_connections;
        let mut shutdown_accept = shutdown_rx.clone();
        let accept_server = self.server.clone();
        let accept_log = self.command_log.clone();
        let accept_shutdown = shutdown_rx.clone();
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

                                let _ = stream.set_nodelay(true);
                                let (reader, writer) = stream.into_split();
                                let conn_id = accept_registry.register(std::sync::Arc::new(writer));
                                tracing::debug!(conn_id, %addr, "accepted");

                                let reg = accept_registry.clone();
                                let srv = accept_server.clone();
                                let log = accept_log.clone();
                                let sd = accept_shutdown.clone();
                                tokio::spawn(async move {
                                    read_loop(conn_id, reader, srv, reg, log, sd).await;
                                });
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

        // Wait for shutdown signal — all real work happens in per-connection
        // read tasks now.
        let mut shutdown_process = shutdown_rx.clone();
        tokio::select! {
            _ = shutdown_process.changed() => {
                tracing::info!("shutdown signal received");
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("ctrl+c received, initiating shutdown");
                let _ = shutdown_tx.send(true);
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

        // ── LifeCycle: on_shutdown ──────────────────────────────────────
        for service in &mut self.services {
            service.on_shutdown();
        }

        tracing::info!("shutdown complete");
        Ok(())
    }
}

/// Read loop — reads frames from TCP and dispatches them in-line.
///
/// One task per connection. TCP guarantees frame order within a connection,
/// and dispatch is safe to call concurrently across connections (shard
/// router + registry are Arc-backed). No central mpsc bottleneck.
async fn read_loop(
    conn_id: u64,
    mut reader: tokio::net::tcp::OwnedReadHalf,
    server: crate::shard::router::ShardRouter,
    registry: ConnectionRegistry,
    command_log: Option<SharedCommandLog>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut header_buf = [0u8; ENVELOPE_SIZE];

    loop {
        // Race read against shutdown — once shutdown fires we stop dispatching
        // immediately so callers that race shutdown with publish_sync see the
        // expected disconnect/timeout error contract.
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                tracing::debug!(conn_id, "read loop stopping (shutdown)");
                break;
            }
            r = reader.read_exact(&mut header_buf) => {
                match r {
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
            }
        }
        crate::lifecycle_trace!("01_tcp_read_header", conn_id, 0, "transport_read");

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
        crate::lifecycle_trace!("02_tcp_read_body", conn_id, 0, "transport_read");

        registry.touch(conn_id);

        let frame = buf.freeze();
        crate::lifecycle_trace!("04_dispatch_enter", conn_id, 0, "read_loop");
        dispatch::dispatch_frame(
            conn_id,
            frame,
            &server,
            &registry,
            command_log.as_ref(),
        )
        .await;
        crate::lifecycle_trace!("19_dispatch_returned", conn_id, 0, "read_loop");
    }

    // Synthesize a Disconnect frame so all the engine bookkeeping
    // (drain_connection across shards) runs once on EOF/error.
    let disconnect = Bytes::copy_from_slice(build_disconnect_frame().as_bytes());
    dispatch::dispatch_frame(
        conn_id,
        disconnect,
        &server,
        &registry,
        command_log.as_ref(),
    )
    .await;
    registry.remove(conn_id);
}

fn build_disconnect_frame() -> Envelope {
    Envelope::new(Action::Disconnect, 0, 0, 0)
}

fn send_shutdown_frame(registry: &ConnectionRegistry, conn_id: u64) {
    let envelope = Envelope::new(Action::RepError, 0, 16, 0);
    let body = RepErrorAction {
        ref_seq: U64::new(0),
        error_code: U16::new(ErrorCode::ServerShuttingDown.as_u16()),
        _pad: [0u8; 6],
    };
    registry.send_parts(conn_id, &[envelope.as_bytes(), body.as_bytes()]);
}

async fn reject_connection(mut stream: tokio::net::TcpStream) -> std::io::Result<()> {
    let envelope = Envelope::new(Action::RepError, 0, 16, 0);
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
    let envelope = Envelope::new(Action::Ping, 0, 0, 0);
    registry.send_parts(conn_id, &[envelope.as_bytes()]);
}
