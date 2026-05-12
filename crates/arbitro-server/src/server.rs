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
        let tls_acceptor = self.config.tls_cert.as_ref().map(|cert| {
            let key = self.config.tls_key.as_ref()
                .expect("ARBITRO_TLS_KEY required when ARBITRO_TLS_CERT is set");
            let acceptor = crate::transport::tls::build_acceptor(cert, key);
            tracing::info!("TLS enabled");
            acceptor
        });

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
            .map(|s| Arc::from(s));

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
        if !hello_done {
            if acc.len() >= 4 {
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
        }

        // ---- Auth check (first frame after Hello must be Auth) ------------
        if hello_done && !auth_done {
            if acc.len() >= HEADER_SIZE_V2 {
                let msg_len = u32::from_le_bytes([
                    acc[4], acc[5], acc[6], acc[7],
                ]) as usize;
                let total = HEADER_SIZE_V2 + msg_len;
                if acc.len() >= total {
                    let action_raw = u16::from_le_bytes([acc[0], acc[1]]);
                    if action_raw != Action::Auth.as_u16() {
                        tracing::warn!(conn_id, "auth required but first frame is not Auth, closing");
                        send_shutdown_frame(&registry, conn_id);
                        break 'outer;
                    }
                    // Token is the body (after 16-byte header)
                    let token_bytes = &acc[HEADER_SIZE_V2..total];
                    let expected = auth_token.as_ref().unwrap();
                    if token_bytes != expected.as_bytes() {
                        tracing::warn!(conn_id, "auth failed: invalid token");
                        send_shutdown_frame(&registry, conn_id);
                        break 'outer;
                    }
                    let _ = acc.split_to(total);
                    auth_done = true;
                    tracing::debug!(conn_id, "auth accepted");
                }
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
                acc.claim_entries_delivered    += snap.claim_entries_delivered;
                acc.claim_skipped_max_inflight += snap.claim_skipped_max_inflight;
                acc.claim_skipped_subject_limit += snap.claim_skipped_subject_limit;
                acc.ack_accepted               += snap.ack_accepted;
                acc.ack_not_found              += snap.ack_not_found;
                acc.nack_accepted              += snap.nack_accepted;
                acc.drain_pending_removed      += snap.drain_pending_removed;
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
            published     = acc.publish_entries_accepted   - prev.publish_entries_accepted,
            delivered     = acc.claim_entries_delivered    - prev.claim_entries_delivered,
            acked         = acc.ack_accepted               - prev.ack_accepted,
            nacked        = acc.nack_accepted              - prev.nack_accepted,
            pub_no_match  = acc.publish_no_match           - prev.publish_no_match,
            held_inflight = acc.claim_skipped_max_inflight - prev.claim_skipped_max_inflight,
            held_subject  = acc.claim_skipped_subject_limit - prev.claim_skipped_subject_limit,
            "metrics",
        );

        prev = acc;
    }
}
