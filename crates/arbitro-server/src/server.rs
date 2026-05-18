//! ArbitroServer — TCP accept loop, per-connection I/O, keepalive, shutdown.
//!
//! The server speaks **v2 only**. Every connection MUST start with a
//! `HelloFrame` (`ARBITRO_MAGIC_V2` + 4 trailing bytes); anything else is
//! closed immediately. After HELLO, the connection is a stream of
//! `Header`-prefixed v2 frames dispatched by `dispatch_v2`.

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::watch;
use zerocopy::IntoBytes;

use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_engine_v2::types::ConnectionId;
use arbitro_proto::v2::egress::rep_frame::RepErrFrame;
use arbitro_proto::v2::header::{Header, HEADER_SIZE as HEADER_SIZE_V2};
use arbitro_proto::v2::ingress::hello::{HelloFrame, HELLO_FRAME_SIZE};
use arbitro_proto::v2::magic::ARBITRO_MAGIC_V2;

const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024; // 64 MB

use arbitro_proto::lifecycle::LifeCycle;

use crate::config::Config;
use crate::persistence::command_log::SharedCommandLog;
use crate::shard::router::ShardRouter;
use crate::transport::registry::{BoxedReader, BoxedWriter};
use crate::transport::ConnectionRegistry;
use crate::transport::dispatch_v2;

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
        // M1: persist shard_count on first boot, refuse to start if it
        // diverges from the marker on subsequent boots. Mismatched
        // shard_count would silently misroute every stream (hashing is
        // `stream_id % shard_count`) so we fail hard instead.
        if let Some(dir) = config.data_dir.as_deref() {
            check_or_persist_shard_count(dir, config.shard_count);
        }
        let mut registry = ConnectionRegistry::new(config.write_buffer_cap);
        let server = ShardRouter::spawn(&config, &registry);
        // F8: hook the registry up to the shared millisecond clock so
        // `touch()` / sweeps don't need per-call `SystemTime::now()`.
        registry.set_clock(server.clock());
        // H10: wire the shared SilentDrops so `enqueue()` can bump the
        // conn-write counter and the metrics loop can read deltas.
        registry.set_silent_drops(server.silent_drops());

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
    /// Also registers it as a lifecycle service and wires it into the shard
    /// router so v2 dispatch records metadata mutations (create/delete
    /// stream/consumer) on the cold path.
    pub fn set_command_log(&mut self, log: SharedCommandLog) {
        self.server.set_command_log(log.clone());
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
        // The replay applier still understands v1 metadata records. v2
        // dispatch doesn't record new ones, so on a fresh data dir this
        // is a no-op; on an existing data dir it replays the v1 history.
        if let Some(ref log) = self.command_log {
            let server = self.server.clone();
            let mut applier = crate::persistence::recovery::ReplayApplier::new(server);
            match log.replay(&mut applier) {
                Ok(n) if n > 0 => tracing::info!(count = n, "metadata replay complete"),
                Ok(_) => {}
                Err(e) => tracing::error!(error = %e, "metadata replay failed"),
            }
            applier.flush().await;
        }

        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        tracing::info!(addr = %self.config.listen_addr, "listening");

        // ── Startup state snapshot ─────────────────────────────────────
        // Always log post-replay state so operators see clean restarts
        // ("0 streams loaded") and not-so-clean ones ("12 streams loaded,
        // 4128 messages restored") with the same shape.
        log_startup_state(&self.server).await;

        // ── TLS acceptor (optional) ────────────────────────────────────
        #[cfg(feature = "tls")]
        let tls_acceptor = match self.config.tls_cert.as_ref() {
            Some(cert) => {
                let key = match self.config.tls_key.as_ref() {
                    Some(k) => k,
                    None => {
                        tracing::error!(
                            "ARBITRO_TLS_KEY required when ARBITRO_TLS_CERT is set"
                        );
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "ARBITRO_TLS_KEY required when ARBITRO_TLS_CERT is set",
                        ));
                    }
                };
                match crate::transport::tls::build_acceptor(cert, key) {
                    Ok(a) => {
                        tracing::info!("TLS enabled");
                        Some(a)
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "TLS setup failed");
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            e.to_string(),
                        ));
                    }
                }
            }
            None => None,
        };

        // Internal shutdown signal
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Keepalive + idle timeout task
        let keepalive_registry = self.registry.clone();
        let idle_timeout = self.config.idle_timeout;
        let keepalive_interval = self.config.keepalive_interval;
        let keepalive_handle = tokio::spawn(async move {
            keepalive_loop(keepalive_registry, idle_timeout, keepalive_interval).await;
        });

        // Periodic metrics task — env-configurable (ARBITRO_METRICS_INTERVAL).
        // Set the interval to 0 to disable entirely.
        let metrics_interval = self.config.metrics_interval;
        let metrics_handle = if metrics_interval.is_zero() {
            None
        } else {
            let metrics_server = self.server.clone();
            let metrics_registry = self.registry.clone();
            let metrics_shutdown = shutdown_rx.clone();
            Some(tokio::spawn(async move {
                metrics_loop(metrics_server, metrics_registry, metrics_interval, metrics_shutdown)
                    .await;
            }))
        };

        // H14: minimal HTTP /health endpoint. Enabled when
        // ARBITRO_HEALTH_LISTEN is set (e.g. "0.0.0.0:9090"); off by
        // default. Replies "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"
        // if the shard router reports at least one live shard.
        if let Ok(addr) = std::env::var("ARBITRO_HEALTH_LISTEN") {
            if !addr.is_empty() {
                let health_server = self.server.clone();
                let health_shutdown = shutdown_rx.clone();
                tokio::spawn(async move {
                    run_healthcheck(addr, health_server, health_shutdown).await;
                });
            }
        }

        // Per-connection ingress: each read_loop dispatches frames directly.
        // No central mpsc — TCP guarantees order per connection, and each
        // connection already runs in its own task, so we get free parallelism
        // on the dispatch path. Server state (shard router, registry) is
        // cloneable Arc-backed and shared across read tasks.

        // Accept task
        let accept_registry = self.registry.clone();
        let max_connections = self.config.max_connections;
        let mut shutdown_accept = shutdown_rx.clone();
        let accept_server = self.server.clone();
        let accept_shutdown = shutdown_rx.clone();

        #[cfg(feature = "tls")]
        let tls_acceptor_shared = tls_acceptor.map(std::sync::Arc::new);

        let auth_token_shared: Option<Arc<str>> = self.config.auth_token
            .as_deref()
            .map(Arc::from);

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

                                // Split into reader/writer — boxed for TLS/plain uniformity.
                                let (reader, writer): (BoxedReader, BoxedWriter);

                                #[cfg(feature = "tls")]
                                {
                                    if let Some(ref acceptor) = tls_acceptor_shared {
                                        match acceptor.accept(stream).await {
                                            Ok(tls_stream) => {
                                                let (r, w) = tokio::io::split(tls_stream);
                                                reader = Box::new(r);
                                                writer = Box::new(w);
                                            }
                                            Err(e) => {
                                                tracing::debug!(%addr, error = %e, "TLS handshake failed");
                                                continue;
                                            }
                                        }
                                    } else {
                                        let (r, w) = stream.into_split();
                                        reader = Box::new(r);
                                        writer = Box::new(w);
                                    }
                                }

                                #[cfg(not(feature = "tls"))]
                                {
                                    let (r, w) = stream.into_split();
                                    reader = Box::new(r);
                                    writer = Box::new(w);
                                }

                                let conn_id = accept_registry.register(writer);
                                tracing::debug!(conn_id, %addr, "accepted");

                                let reg = accept_registry.clone();
                                let srv = accept_server.clone();
                                let sd = accept_shutdown.clone();
                                let auth = auth_token_shared.clone();
                                tokio::spawn(async move {
                                    read_loop(conn_id, reader, srv, reg, sd, auth).await;
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

        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate())
                .expect("failed to register SIGTERM handler");

            // L15: SIGUSR1 dumps a diagnostic JSON snapshot to
            // /tmp/arbitro-dump-{pid}.json. Listens forever (until process
            // exit) so the operator can request multiple dumps per
            // process. Wrapped in #[cfg(unix)]; non-unix is a no-op.
            if let Ok(mut sigusr1) = signal(SignalKind::user_defined1()) {
                let server_dump = self.server.clone();
                let registry_dump = self.registry.clone();
                tokio::spawn(async move {
                    while sigusr1.recv().await.is_some() {
                        let pid = std::process::id();
                        let path = format!("/tmp/arbitro-dump-{}.json", pid);
                        let dump = build_diagnostic_dump(&server_dump, &registry_dump).await;
                        match std::fs::write(&path, &dump) {
                            Ok(()) => tracing::info!(path = %path, "SIGUSR1 diagnostic dump written"),
                            Err(e) => tracing::warn!(path = %path, error = ?e, "SIGUSR1 dump write failed"),
                        }
                    }
                });
            }

            tokio::select! {
                _ = shutdown_process.changed() => {
                    tracing::info!("shutdown signal received");
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("SIGINT received, initiating shutdown");
                    let _ = shutdown_tx.send(true);
                }
                _ = sigterm.recv() => {
                    tracing::info!("SIGTERM received, initiating shutdown");
                    let _ = shutdown_tx.send(true);
                }
            }
        }

        #[cfg(not(unix))]
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
        if let Some(h) = metrics_handle { h.abort(); }

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
///
/// ## Protocol state machine
///
/// 1. Wait until at least 4 bytes have arrived; read the magic.
/// 2. If magic ≠ `ARBITRO_MAGIC_V2`, log and close — v1 clients are no
///    longer supported.
/// 3. Wait until the full 8-byte `HelloFrame` is buffered, validate, consume.
/// 4. Drain `Header`-prefixed v2 frames from the accumulator forever.
async fn read_loop(
    conn_id: u64,
    mut reader: BoxedReader,
    server: ShardRouter,
    registry: ConnectionRegistry,
    mut shutdown: watch::Receiver<bool>,
    auth_token: Option<Arc<str>>,
) {
    use tokio::io::AsyncReadExt;

    const INITIAL_CAP: usize = 64 * 1024;
    let mut acc = BytesMut::with_capacity(INITIAL_CAP);

    // Per-connection HELLO state. Connection is closed if the first 4
    // bytes are not the v2 magic.
    let mut hello_done: bool = false;
    let mut auth_done: bool = auth_token.is_none(); // skip auth if no token configured

    'outer: loop {
        // ---- Mandatory v2 handshake ---------------------------------------
        if !hello_done
            && acc.len() >= 4 {
                let m = u32::from_le_bytes([acc[0], acc[1], acc[2], acc[3]]);
                if m != ARBITRO_MAGIC_V2 {
                    tracing::warn!(conn_id, magic = format!("{m:#010x}"), "non-v2 client, closing");
                    break 'outer;
                }
                if acc.len() >= HELLO_FRAME_SIZE {
                    let _ = HelloFrame::parse(&acc[..HELLO_FRAME_SIZE]); // validates
                    let _ = acc.split_to(HELLO_FRAME_SIZE);
                    hello_done = true;
                    tracing::debug!(conn_id, "v2 HELLO accepted");
                }
            }

        // ---- Auth check (first frame after Hello must be Auth) ------------
        if hello_done && !auth_done
            && acc.len() >= HEADER_SIZE_V2 {
                let msg_len = u32::from_le_bytes([
                    acc[4], acc[5], acc[6], acc[7],
                ]) as usize;
                let total = HEADER_SIZE_V2 + msg_len;
                if acc.len() >= total {
                    let action_raw = u16::from_le_bytes([acc[0], acc[1]]);
                    if action_raw != Action::Auth.as_u16() {
                        // H2: surface the real reason (AuthRequired) instead
                        // of pretending the server is shutting down. The
                        // client needs to distinguish "send a token" from
                        // "stop trying, the broker is down".
                        tracing::warn!(conn_id, "auth required but first frame is not Auth, closing");
                        send_error_frame(&registry, conn_id, ErrorCode::AuthRequired);
                        break 'outer;
                    }
                    // Token is the body (after 16-byte header)
                    let token_bytes = &acc[HEADER_SIZE_V2..total];
                    let expected = auth_token.as_ref().unwrap();
                    // M14: constant-time comparison so a network
                    // observer can't recover the token byte-by-byte
                    // via timing of `!=`. We keep the early
                    // length-mismatch reject (constant against a known
                    // expected length is fine — the attacker already
                    // knows it from a single failed attempt).
                    let token_ok = {
                        let e = expected.as_bytes();
                        if token_bytes.len() != e.len() {
                            false
                        } else {
                            let mut diff: u8 = 0;
                            for (a, b) in token_bytes.iter().zip(e.iter()) {
                                diff |= a ^ b;
                            }
                            diff == 0
                        }
                    };
                    if !token_ok {
                        // H2: a wrong token is AuthFailed, not a server
                        // shutdown signal. Mis-coding this confuses
                        // bootstrap loops and credential-rotation logic.
                        tracing::warn!(conn_id, "auth failed: invalid token");
                        send_error_frame(&registry, conn_id, ErrorCode::AuthFailed);
                        break 'outer;
                    }
                    let _ = acc.split_to(total);
                    auth_done = true;
                    tracing::debug!(conn_id, "auth accepted");
                }
            }

        // ---- Drain whole v2 frames already in the accumulator -------------
        if hello_done && auth_done {
            loop {
                if acc.len() < HEADER_SIZE_V2 {
                    break;
                }
                // v2 Header: msg_len at bytes 4..8 LE u32.
                let msg_len = u32::from_le_bytes([
                    acc[4], acc[5], acc[6], acc[7],
                ]) as usize;
                
                if msg_len > MAX_FRAME_SIZE {
                    tracing::warn!(conn_id, msg_len, "frame exceeds MAX_FRAME_SIZE, dropping connection");
                    break 'outer;
                }
                
                let total = HEADER_SIZE_V2 + msg_len;
                if acc.len() < total {
                    break;
                }
                let frame = acc.split_to(total).freeze();
                registry.touch(conn_id);
                if dispatch_v2::dispatch_frame_v2(conn_id, frame, &server, &registry).await.is_err() {
                    tracing::warn!(conn_id, "malformed frame, dropping connection");
                    break 'outer;
                }
            }
        }

        // ---- Slow path: read more bytes from the socket -------------------
        if acc.capacity() - acc.len() < HEADER_SIZE_V2 {
            acc.reserve(INITIAL_CAP);
        }

        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                tracing::debug!(conn_id, "read loop stopping (shutdown)");
                break 'outer;
            }
            r = reader.read_buf(&mut acc) => {
                match r {
                    Ok(0) => {
                        tracing::debug!(conn_id, "client disconnected (EOF)");
                        break 'outer;
                    }
                    Ok(_n) => { /* loop and try to drain */ }
                    Err(e) => {
                        tracing::debug!(conn_id, error = %e, "read error");
                        break 'outer;
                    }
                }
            }
        }
    }

    // EOF / shutdown / error: drain the engine bookkeeping for this conn,
    // then drop the connection from the registry. No frame is synthesized.
    for i in 0..server.shard_count() {
        let _ = server.shard(i).drain_connection(ConnectionId(conn_id)).await;
    }
    registry.remove(conn_id);
}

fn send_shutdown_frame(registry: &ConnectionRegistry, conn_id: u64) {
    let frame = RepErrFrame::new(0, 0, ErrorCode::ServerShuttingDown.as_u16());
    registry.send_parts(conn_id, &[frame.as_bytes()]);
}

/// Send a single RepError frame with the given error code. Used by the
/// auth path to surface the real reason a connection is being closed
/// (AuthRequired / AuthFailed) instead of misrepresenting it as a
/// server-side shutdown. H2.
fn send_error_frame(registry: &ConnectionRegistry, conn_id: u64, code: ErrorCode) {
    let frame = RepErrFrame::new(0, 0, code.as_u16());
    registry.send_parts(conn_id, &[frame.as_bytes()]);
}

/// M1: per-data-dir marker recording the `shard_count` the broker
/// originally booted with. If the file is absent, write it; if it
/// disagrees with the current config, log an error and abort the
/// process. Routing depends on `stream_id % shard_count` so a silent
/// change here would deliver every existing stream to the wrong shard.
fn check_or_persist_shard_count(data_dir: &str, shard_count: usize) {
    let marker_path = std::path::Path::new(data_dir).join("shards.toml");
    if let Err(e) = std::fs::create_dir_all(data_dir) {
        tracing::error!(error = %e, dir = %data_dir, "M1: failed to create data dir");
        std::process::exit(2);
    }
    match std::fs::read_to_string(&marker_path) {
        Ok(contents) => {
            // Format: `shard_count = N`. Tolerant of extra whitespace and
            // unrelated keys so a future v2 can add fields without
            // breaking older binaries.
            let parsed = contents
                .lines()
                .filter_map(|l| {
                    let (k, v) = l.split_once('=')?;
                    if k.trim() == "shard_count" {
                        v.trim().parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .next();
            match parsed {
                Some(stored) if stored == shard_count => {
                    tracing::info!(stored, "M1: shard_count marker matches");
                }
                Some(stored) => {
                    tracing::error!(
                        stored,
                        configured = shard_count,
                        path = %marker_path.display(),
                        "M1: shard_count mismatch between data dir and config — refusing to start",
                    );
                    std::process::exit(2);
                }
                None => {
                    tracing::warn!(
                        path = %marker_path.display(),
                        "M1: shard_count marker present but unparseable, rewriting",
                    );
                    let _ = std::fs::write(&marker_path, format!("shard_count = {shard_count}\n"));
                }
            }
        }
        Err(_) => {
            // First boot for this data dir — write the marker.
            if let Err(e) = std::fs::write(
                &marker_path,
                format!("shard_count = {shard_count}\n"),
            ) {
                tracing::error!(error = %e, path = %marker_path.display(), "M1: failed to write shard_count marker");
                std::process::exit(2);
            }
            tracing::info!(shard_count, "M1: shard_count marker created");
        }
    }
}

async fn reject_connection(mut stream: tokio::net::TcpStream) -> std::io::Result<()> {
    let frame = RepErrFrame::new(0, 0, ErrorCode::InternalError.as_u16());
    stream.write_all(frame.as_bytes()).await?;
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

/// Send a v2 Ping (16-byte header, empty body). Clients that never reply
/// are eventually evicted by the idle-timeout sweep.
fn send_ping(registry: &ConnectionRegistry, conn_id: u64) {
    let header = Header::new(Action::Ping.as_u16(), 0, 0);
    registry.send_parts(conn_id, &[header.as_bytes()]);
}

/// Walk every shard, enumerate streams and consumers, and log a single
/// summary line so operators see broker state at startup — whether the
/// server came up empty or recovered an existing dataset.
async fn log_startup_state(server: &ShardRouter) {
    let mut total_streams = 0usize;
    let mut total_consumers = 0usize;
    let mut total_messages = 0u64;
    let mut total_bytes = 0u64;

    for i in 0..server.shard_count() {
        let shard = server.shard(i);
        if let Ok(reply) = shard.list_streams().await {
            for (stream_id, name) in &reply.streams {
                total_streams += 1;
                if let Ok(info) = shard.store_info(arbitro_engine_v2::types::StreamId(*stream_id)).await {
                    total_messages += info.messages;
                    total_bytes += info.bytes;
                    tracing::info!(
                        stream = %String::from_utf8_lossy(name),
                        stream_id = stream_id,
                        messages = info.messages,
                        bytes = info.bytes,
                        "stream ready",
                    );
                }
            }
        }
        if let Ok(reply) = shard.list_consumers().await {
            total_consumers += reply.consumers.len();
        }
    }

    tracing::info!(
        streams = total_streams,
        consumers = total_consumers,
        messages = total_messages,
        bytes = total_bytes,
        "broker state ready",
    );
}

/// H14: tiny HTTP healthcheck server.
///
/// Accepts one connection at a time, reads up to the end of the request
/// line + headers (`\r\n\r\n`), and replies 200 OK if the shard router
/// has at least one live shard. No HTTP parser dependency — we never
/// inspect method/path beyond confirming the request terminates.
async fn run_healthcheck(
    addr: String,
    server: ShardRouter,
    mut shutdown: watch::Receiver<bool>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(target = "healthcheck", addr = %addr, error = %e, "bind failed");
            return;
        }
    };
    tracing::info!(target = "healthcheck", addr = %addr, "healthcheck listening on /health");

    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            res = listener.accept() => {
                let (mut sock, _peer) = match res {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let healthy = server.shard_count() > 0;
                tokio::spawn(async move {
                    // Read until \r\n\r\n or 1 KiB, whichever first.
                    let mut buf = [0u8; 1024];
                    let mut total = 0usize;
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        async {
                            while total < buf.len() {
                                let n = match sock.read(&mut buf[total..]).await {
                                    Ok(0) | Err(_) => return,
                                    Ok(n) => n,
                                };
                                total += n;
                                if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                    return;
                                }
                            }
                        },
                    ).await;
                    let body: &[u8] = if healthy { b"OK" } else { b"NO" };
                    let status = if healthy { "200 OK" } else { "503 Service Unavailable" };
                    let resp = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.write_all(body).await;
                    let _ = sock.shutdown().await;
                });
            }
        }
    }
}

/// Periodic metrics task. Sums `MetricsSnapshot` across all shards every
/// `interval` and emits one `tracing::info!` event with deltas vs. the
/// previous tick (so logs show consumption *rate*, not running totals).
async fn metrics_loop(
    server: ShardRouter,
    registry: ConnectionRegistry,
    interval: std::time::Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    use arbitro_engine_v2::MetricsSnapshot;
    let mut ticker = tokio::time::interval(interval);
    // Skip the first immediate tick — first emission is one interval in.
    ticker.tick().await;
    let mut prev = MetricsSnapshot::default();
    let silent_drops = server.silent_drops();
    let mut prev_drops = silent_drops.snapshot();

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown.changed() => break,
        }

        // Aggregate across all shards.
        let mut acc = MetricsSnapshot::default();
        for i in 0..server.shard_count() {
            if let Ok(snap) = server.shard(i).metrics().await {
                acc.publish_entries_accepted   += snap.publish_entries_accepted;
                acc.publish_duplicates_skipped += snap.publish_duplicates_skipped;
                acc.publish_no_match           += snap.publish_no_match;
                acc.publish_queues_pushed      += snap.publish_queues_pushed;
                acc.publish_fanout_notified    += snap.publish_fanout_notified;
                // L11: catch up the previously-unaggregated fields so
                // dashboards see the full set of engine counters.
                acc.claim_batches              += snap.claim_batches;
                acc.claim_entries_delivered    += snap.claim_entries_delivered;
                acc.claim_skipped_consumer_paused += snap.claim_skipped_consumer_paused;
                acc.claim_skipped_max_inflight += snap.claim_skipped_max_inflight;
                acc.claim_skipped_subject_limit += snap.claim_skipped_subject_limit;
                acc.claim_skipped_credit_conn  += snap.claim_skipped_credit_conn;
                acc.claim_skipped_credit_subject += snap.claim_skipped_credit_subject;
                acc.claim_empty_pop            += snap.claim_empty_pop;
                acc.ack_accepted               += snap.ack_accepted;
                acc.ack_not_found              += snap.ack_not_found;
                acc.nack_accepted              += snap.nack_accepted;
                acc.seed_entries               += snap.seed_entries;
                acc.seed_queues_pushed         += snap.seed_queues_pushed;
                acc.seed_no_match              += snap.seed_no_match;
                acc.drain_pending_removed      += snap.drain_pending_removed;
                acc.drain_connections          += snap.drain_connections;
                acc.drain_consumers            += snap.drain_consumers;
            }
        }

        // Counts of active entities and current saturation gauges.
        // `ack_pending` is the broker's headline saturation indicator —
        // sum of in-flight (delivered, unacked) messages across every
        // consumer. Operators watch this for backpressure formation.
        let mut streams        = 0usize;
        let mut consumers      = 0usize;
        let mut consumers_paused = 0usize;
        let mut ack_pending    = 0u64;
        let mut max_consumer_ack_pending = 0u32;
        let mut stream_messages = 0u64;
        let mut stream_bytes    = 0u64;
        for i in 0..server.shard_count() {
            let shard = server.shard(i);
            if let Ok(r) = shard.list_streams().await {
                streams += r.streams.len();
                for (sid, _) in &r.streams {
                    if let Ok(info) = shard.store_info(
                        arbitro_engine_v2::types::StreamId(*sid),
                    ).await {
                        stream_messages += info.messages;
                        stream_bytes    += info.bytes;
                    }
                }
            }
            if let Ok(states) = shard.consumer_states().await {
                consumers += states.len();
                for s in &states {
                    if s.paused { consumers_paused += 1; }
                    ack_pending += s.ack_pending as u64;
                    if s.ack_pending > max_consumer_ack_pending {
                        max_consumer_ack_pending = s.ack_pending;
                    }
                }
            }
        }
        let connections = registry.active_count();

        // H10: per-tick silent-drop deltas.
        let drops_now = silent_drops.snapshot();
        let drop_conn_write = drops_now.conn_write.saturating_sub(prev_drops.conn_write);
        let drop_notify_ring = drops_now.notify_ring.saturating_sub(prev_drops.notify_ring);
        let drop_drain_evt = drops_now.drain_evt.saturating_sub(prev_drops.drain_evt);
        prev_drops = drops_now;

        tracing::info!(
            interval_s    = interval.as_secs(),
            // ── Gauges (current state) ─────────────────────────────────
            connections      = connections,
            streams          = streams,
            consumers        = consumers,
            consumers_paused = consumers_paused,
            ack_pending      = ack_pending,            // total in-flight unacked
            max_ack_pending  = max_consumer_ack_pending, // worst-loaded consumer
            stream_messages  = stream_messages,
            stream_bytes     = stream_bytes,
            // ── Deltas this tick (per-interval rate) ───────────────────
            // L7: saturating_sub so a counter that fell below the previous
            // snapshot (shard restart, recovery rebuild) emits 0 instead of
            // wrapping into a 2^63 spike in the dashboard.
            published     = acc.publish_entries_accepted.saturating_sub(prev.publish_entries_accepted),
            delivered     = acc.claim_entries_delivered.saturating_sub(prev.claim_entries_delivered),
            acked         = acc.ack_accepted.saturating_sub(prev.ack_accepted),
            nacked        = acc.nack_accepted.saturating_sub(prev.nack_accepted),
            pub_no_match  = acc.publish_no_match.saturating_sub(prev.publish_no_match),
            held_inflight = acc.claim_skipped_max_inflight.saturating_sub(prev.claim_skipped_max_inflight),
            held_subject  = acc.claim_skipped_subject_limit.saturating_sub(prev.claim_skipped_subject_limit),
            // H10: silent drops at the conn-write / drain-event / notify-ring sites.
            drop_conn_write = drop_conn_write,
            drop_notify_ring = drop_notify_ring,
            drop_drain_evt = drop_drain_evt,
            "metrics",
        );

        prev = acc;
    }
}

/// L15: build a JSON diagnostic snapshot — used by the SIGUSR1 handler.
///
/// Writes a flat object with the broker's headline gauges, the silent-drop
/// counters, and an array of per-stream `messages`/`bytes`. Kept tiny and
/// dependency-free (hand-built JSON) so the dump itself is observable on
/// the most-loaded broker, and so dumping doesn't pull serde_json into the
/// server crate's dep graph.
#[cfg(unix)]
async fn build_diagnostic_dump(
    server: &ShardRouter,
    registry: &ConnectionRegistry,
) -> String {
    use arbitro_engine_v2::MetricsSnapshot;
    let mut acc = MetricsSnapshot::default();
    for i in 0..server.shard_count() {
        if let Ok(snap) = server.shard(i).metrics().await {
            acc.publish_entries_accepted += snap.publish_entries_accepted;
            acc.claim_entries_delivered += snap.claim_entries_delivered;
            acc.ack_accepted += snap.ack_accepted;
            acc.nack_accepted += snap.nack_accepted;
        }
    }
    let drops = server.silent_drops().snapshot();
    let mut streams = 0usize;
    let mut consumers = 0usize;
    let mut total_msgs = 0u64;
    let mut total_bytes = 0u64;
    let mut per_stream = String::new();
    for i in 0..server.shard_count() {
        let shard = server.shard(i);
        if let Ok(r) = shard.list_streams().await {
            streams += r.streams.len();
            for (sid, _) in &r.streams {
                if let Ok(info) = shard
                    .store_info(arbitro_engine_v2::types::StreamId(*sid))
                    .await
                {
                    total_msgs += info.messages;
                    total_bytes += info.bytes;
                    if !per_stream.is_empty() {
                        per_stream.push(',');
                    }
                    per_stream.push_str(&format!(
                        "{{\"stream_id\":{},\"messages\":{},\"bytes\":{}}}",
                        sid, info.messages, info.bytes
                    ));
                }
            }
        }
        if let Ok(states) = shard.consumer_states().await {
            consumers += states.len();
        }
    }
    let conns = registry.active_count();
    format!(
        "{{\"pid\":{pid},\"ts_ms\":{ts},\"connections\":{conns},\
         \"streams\":{streams},\"consumers\":{consumers},\
         \"stream_messages\":{tm},\"stream_bytes\":{tb},\
         \"publish_entries_accepted\":{pub_in},\
         \"claim_entries_delivered\":{dl},\
         \"ack_accepted\":{ak},\"nack_accepted\":{nk},\
         \"silent_drops\":{{\"conn_write\":{cw},\"notify_ring\":{nr},\"drain_evt\":{de}}},\
         \"per_stream\":[{ps}]}}",
        pid = std::process::id(),
        ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        conns = conns,
        streams = streams,
        consumers = consumers,
        tm = total_msgs,
        tb = total_bytes,
        pub_in = acc.publish_entries_accepted,
        dl = acc.claim_entries_delivered,
        ak = acc.ack_accepted,
        nk = acc.nack_accepted,
        cw = drops.conn_write,
        nr = drops.notify_ring,
        de = drops.drain_evt,
        ps = per_stream,
    )
}
