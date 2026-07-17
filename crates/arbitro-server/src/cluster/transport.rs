//! TCP-based Raft transport using the arbitro-raft wire format.
//!
//! Frames use the 32-byte `RaftFrameHeader` defined in arbitro-raft's
//! `protocol::codec::wire`. The `body_len` field at offset 16 gives the
//! body size; total frame = 32 + body_len.
//!
//! `send_vectored` uses true `write_vectored` with `IoSlice` to avoid
//! copying payload buffers — matching the zero-copy design of the bench
//! transport in `arbitro-raft/benches/tcp_raft_bench.rs`.
//!
//! # Security (D1 mTLS + D2 identity binding)
//!
//! The transport fulfills the `RaftTransport` security contract (see the
//! trait rustdoc in arbitro-raft): with a [`ClusterSecurityConfig`] carrying
//! TLS material, every connection runs over mutually-verified rustls and is
//! bound at accept time to the `PeerId` derived from the peer certificate's
//! SAN/CN. Without TLS, an opt-in plaintext mode binds connections to the
//! set of peer ids configured for the remote IP. In both modes, any frame
//! whose header `from` disagrees with the connection's bound identity is
//! dropped before reaching the Raft node and counted in
//! [`TcpRaftTransport::spoofed_frames_rejected`]. The default (no TLS
//! material, `strict_from = false`) is the original plaintext path,
//! unchanged.

use std::collections::HashMap;
use std::io::IoSlice;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arbitro_raft::{PeerId, RaftError, RaftTransport, RAFT_FRAME_HEADER_SIZE};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use super::security::ClusterSecurityConfig;

/// D3 resource-exhaustion limits for the cluster transport's INBOUND side.
///
/// Defaults are sized so that no legitimate traffic ever trips them:
///
/// * `max_frame_body_bytes` (64 MiB): Raft consensus frames are capped by the
///   crate at `arbitro_raft::MAX_FRAME_SIZE` (64 KiB total) — anything larger
///   could not even fit the receiver's `inbound_buf`. Data-plane replication
///   frames (kind >= 20) carry one client publish batch, whose size is
///   bounded by the client-plane frame cap (`ARBITRO_MAX_FRAME_SIZE`,
///   default 64 MiB, hard max 256 MiB). 64 MiB therefore admits every frame
///   a default-configured peer can produce, with three orders of magnitude
///   of headroom over consensus traffic, while shrinking the damage of a
///   forged `body_len` from 4 GiB-per-frame to the configured cap. Raise it
///   in lockstep with `ARBITRO_MAX_FRAME_SIZE` if you raise that.
/// * `max_inbound_connections` (256): a healthy cluster peer opens a small,
///   fixed number of connections to this node (raft + replication + acks —
///   single digits per peer, and clusters are single-digit peers). 256 is
///   far above any legitimate fan-in yet stops a TCP connect flood from
///   exhausting fds/task memory.
/// * `max_frames_per_conn_per_sec` (0 = unlimited): legitimate replication
///   is deliberately unthrottled by default — pipelined append/ack streams
///   can legitimately run at line rate. Operators who want a hard ceiling
///   can set one; a connection exceeding it is dropped and its source IP
///   jailed for `jail_cooldown_ms`.
/// * `jail_cooldown_ms` (2000): once a connection commits a fatal protocol
///   offense (oversize frame, rate-cap breach) its source IP is refused at
///   accept for this long, so an attacker cannot reconnect-spin. `0`
///   disables the accept-jail (offending connections are still dropped).
///   NOTE: the jail key is the source IP — co-located nodes sharing one IP
///   (e.g. an all-loopback dev cluster) share jail fate for the cooldown;
///   legitimate nodes never send oversize frames, so this only matters when
///   a hostile process shares the host.
#[derive(Clone, Copy, Debug)]
pub struct ClusterTransportLimits {
    /// Hard cap on a frame's wire `body_len` before any body byte is
    /// buffered. Oversize = fatal protocol error: connection closed,
    /// counted, source IP jailed.
    pub max_frame_body_bytes: usize,
    /// Max concurrent inbound connections; accepts beyond the cap are
    /// dropped immediately (counted).
    pub max_inbound_connections: usize,
    /// Per-connection inbound frame-rate quota (frames per second);
    /// 0 disables the quota.
    pub max_frames_per_conn_per_sec: u64,
    /// How long an offending source IP stays refused at accept; 0 disables
    /// the accept-jail.
    pub jail_cooldown_ms: u64,
}

impl Default for ClusterTransportLimits {
    fn default() -> Self {
        Self {
            max_frame_body_bytes: 64 * 1024 * 1024,
            max_inbound_connections: 256,
            max_frames_per_conn_per_sec: 0,
            jail_cooldown_ms: 2_000,
        }
    }
}

impl ClusterTransportLimits {
    /// Read the limits from the environment (defaults above when unset):
    /// `ARBITRO_CLUSTER_MAX_FRAME_SIZE`, `ARBITRO_CLUSTER_MAX_INBOUND_CONNS`,
    /// `ARBITRO_CLUSTER_MAX_FRAMES_PER_SEC`, `ARBITRO_CLUSTER_JAIL_COOLDOWN_MS`.
    pub fn from_env() -> Self {
        fn parse<T: std::str::FromStr>(key: &str, default: T) -> T {
            match std::env::var(key) {
                Ok(raw) => match raw.parse() {
                    Ok(v) => v,
                    Err(_) => {
                        tracing::warn!(var = key, value = %raw, "unparseable env var, using default");
                        default
                    }
                },
                Err(_) => default,
            }
        }
        let d = Self::default();
        Self {
            max_frame_body_bytes: parse(
                "ARBITRO_CLUSTER_MAX_FRAME_SIZE",
                d.max_frame_body_bytes,
            ),
            max_inbound_connections: parse(
                "ARBITRO_CLUSTER_MAX_INBOUND_CONNS",
                d.max_inbound_connections,
            ),
            max_frames_per_conn_per_sec: parse(
                "ARBITRO_CLUSTER_MAX_FRAMES_PER_SEC",
                d.max_frames_per_conn_per_sec,
            ),
            jail_cooldown_ms: parse("ARBITRO_CLUSTER_JAIL_COOLDOWN_MS", d.jail_cooldown_ms),
        }
    }
}

/// Shared inbound-abuse state (D3): limits, shed counters, the connection
/// cap semaphore, and the per-IP accept jail. One per transport, shared by
/// the accept loop and every inbound connection task.
struct InboundGuard {
    limits: ClusterTransportLimits,
    /// Frames rejected (connection closed) because `body_len` exceeded
    /// `limits.max_frame_body_bytes` — checked BEFORE buffering any body
    /// byte, so a forged 4 GiB `body_len` never drives an allocation.
    oversize_rejected: AtomicU64,
    /// Connections dropped for exceeding `max_frames_per_conn_per_sec`.
    rate_limited_disconnects: AtomicU64,
    /// Accepts dropped because `max_inbound_connections` were already open.
    conns_rejected_over_cap: AtomicU64,
    /// Accepts dropped because the source IP was jailed.
    jailed_accepts_rejected: AtomicU64,
    /// One permit per open inbound connection.
    conn_slots: Arc<tokio::sync::Semaphore>,
    /// Source IP -> jailed-until deadline. Entries are pruned lazily on the
    /// accept path; the map is bounded by the number of distinct offending
    /// IPs inside one cooldown window.
    jail: parking_lot::Mutex<HashMap<IpAddr, Instant>>,
}

impl InboundGuard {
    fn new(limits: ClusterTransportLimits) -> Self {
        Self {
            limits,
            oversize_rejected: AtomicU64::new(0),
            rate_limited_disconnects: AtomicU64::new(0),
            conns_rejected_over_cap: AtomicU64::new(0),
            jailed_accepts_rejected: AtomicU64::new(0),
            conn_slots: Arc::new(tokio::sync::Semaphore::new(
                limits.max_inbound_connections,
            )),
            jail: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// True when `ip` is currently jailed (expired entries are pruned here).
    fn ip_jailed(&self, ip: IpAddr) -> bool {
        let mut jail = self.jail.lock();
        match jail.get(&ip) {
            Some(until) if Instant::now() < *until => true,
            Some(_) => {
                jail.remove(&ip);
                false
            }
            None => false,
        }
    }

    /// Jail `ip` for the configured cooldown (no-op when the cooldown is 0).
    fn jail_ip(&self, ip: IpAddr) {
        if self.limits.jail_cooldown_ms == 0 {
            return;
        }
        let until = Instant::now() + Duration::from_millis(self.limits.jail_cooldown_ms);
        self.jail.lock().insert(ip, until);
    }
}

/// Offset of `body_len: U32` inside `RaftFrameHeader`.
const BODY_LEN_OFFSET: usize = 16;

/// Offset of `from: U64` inside `RaftFrameHeader`.
const FROM_OFFSET: usize = 8;

/// Minimum kind value for data-plane replication frames. Frames with
/// `kind >= REPLICATION_KIND_MIN` are routed to the dedicated replication
/// channel instead of the Raft consensus channel.
const REPLICATION_KIND_MIN: u8 = 20;

/// Offset of the `kind` byte inside `RaftFrameHeader`.
const KIND_OFFSET: usize = 5;

/// An outbound connection to a peer: plain TCP, or client-side TLS when the
/// transport was built with TLS material.
enum PeerStream {
    Plain(TcpStream),
    #[cfg(feature = "cluster-tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncWrite for PeerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            PeerStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "cluster-tls")]
            PeerStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            PeerStream::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            #[cfg(feature = "cluster-tls")]
            PeerStream::Tls(s) => Pin::new(s.as_mut()).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            PeerStream::Plain(s) => s.is_write_vectored(),
            #[cfg(feature = "cluster-tls")]
            PeerStream::Tls(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            PeerStream::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "cluster-tls")]
            PeerStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            PeerStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "cluster-tls")]
            PeerStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// The set of sender ids a connection is allowed to claim in the frame
/// header's `from` field (D2 identity binding).
#[derive(Clone)]
enum AllowedFrom {
    /// Legacy plaintext mode — no enforcement.
    Any,
    /// TLS: the single id derived from the verified peer certificate.
    #[cfg_attr(not(feature = "cluster-tls"), allow(dead_code))]
    Exact(u64),
    /// Plaintext strict mode: ids whose configured peer address shares the
    /// connection's remote IP. Empty set = unknown host, all frames dropped.
    AnyOf(Arc<std::collections::HashSet<u64>>),
}

impl AllowedFrom {
    fn permits(&self, from: u64) -> bool {
        match self {
            AllowedFrom::Any => true,
            AllowedFrom::Exact(id) => *id == from,
            AllowedFrom::AnyOf(set) => set.contains(&from),
        }
    }
}

pub struct TcpRaftTransport {
    peers: Arc<HashMap<PeerId, SocketAddr>>,
    connections: Arc<Mutex<HashMap<PeerId, Arc<tokio::sync::Mutex<PeerStream>>>>>,
    incoming_rx: Mutex<tokio::sync::mpsc::Receiver<Vec<u8>>>,
    _incoming_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    /// Replication frames (kind >= 20) arrive here. Raft consensus frames
    /// (kind 1-11) go to `incoming_rx`; replication frames are split off
    /// in `read_raft_frames` so the Raft node never sees them.
    ///
    /// `Option` so `take_replication_rx()` can extract it before the
    /// transport is moved into `RaftNode::new()`.
    repl_incoming_rx: parking_lot::Mutex<Option<tokio::sync::mpsc::Receiver<Vec<u8>>>>,
    _repl_incoming_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    /// Security posture (TLS material + plaintext strictness). Shared with
    /// the accept loop.
    security: Arc<ClusterSecurityConfig>,
    /// Count of frames dropped because their header `from` did not match the
    /// connection's authenticated identity (D2).
    spoof_rejected: Arc<AtomicU64>,
    /// Inbound-abuse limits + counters + connection cap + accept jail (D3).
    guard: Arc<InboundGuard>,
    /// Actual bound listen address (useful when binding to port 0 in tests).
    local_addr: SocketAddr,
}

impl TcpRaftTransport {
    /// Plaintext transport with no identity enforcement — the historical
    /// behavior, byte-for-byte. Prefer [`Self::new_with_security`] in
    /// production.
    pub async fn new(
        bind_addr: SocketAddr,
        peers: HashMap<PeerId, SocketAddr>,
    ) -> Result<Self, RaftError> {
        Self::new_with_security(bind_addr, peers, ClusterSecurityConfig::plaintext()).await
    }

    /// Create a transport with an explicit security posture.
    ///
    /// * TLS material present (`cluster-tls` feature): all connections run
    ///   mutual TLS; inbound connections are bound to the PeerId derived from
    ///   the client certificate and outbound dials verify the peer cert
    ///   against the expected identity (D1 + D2).
    /// * `strict_from` (plaintext): inbound connections are bound to the peer
    ///   ids configured for the remote IP (D2, address-based).
    ///
    /// Certificate rotation: TLS configs are rebuilt from the PEM paths on
    /// every accept/dial, so rotated files take effect on reconnect. See
    /// `cluster::security` module docs.
    pub async fn new_with_security(
        bind_addr: SocketAddr,
        peers: HashMap<PeerId, SocketAddr>,
        security: ClusterSecurityConfig,
    ) -> Result<Self, RaftError> {
        Self::new_with_limits(bind_addr, peers, security, ClusterTransportLimits::default())
            .await
    }

    /// Create a transport with an explicit security posture AND explicit
    /// inbound resource limits (D3). [`Self::new_with_security`] delegates
    /// here with [`ClusterTransportLimits::default`]; the server boot path
    /// passes [`ClusterTransportLimits::from_env`].
    pub async fn new_with_limits(
        bind_addr: SocketAddr,
        peers: HashMap<PeerId, SocketAddr>,
        security: ClusterSecurityConfig,
        limits: ClusterTransportLimits,
    ) -> Result<Self, RaftError> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .map_err(|e| RaftError::Transport(format!("bind {bind_addr}: {e}")))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| RaftError::Transport(format!("local_addr: {e}")))?;

        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);
        let (repl_tx, repl_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);

        let peers = Arc::new(peers);
        let security = Arc::new(security);
        let spoof_rejected = Arc::new(AtomicU64::new(0));
        let guard = Arc::new(InboundGuard::new(limits));

        let accept_tx = tx.clone();
        let accept_repl_tx = repl_tx.clone();
        let accept_security = security.clone();
        let accept_peers = peers.clone();
        let accept_spoof = spoof_rejected.clone();
        let accept_guard = guard.clone();
        tokio::spawn(async move {
            Self::accept_loop(
                listener,
                accept_tx,
                accept_repl_tx,
                accept_security,
                accept_peers,
                accept_spoof,
                accept_guard,
            )
            .await;
        });

        Ok(Self {
            peers,
            connections: Arc::new(Mutex::new(HashMap::new())),
            incoming_rx: Mutex::new(rx),
            _incoming_tx: tx,
            repl_incoming_rx: parking_lot::Mutex::new(Some(repl_rx)),
            _repl_incoming_tx: repl_tx,
            security,
            spoof_rejected,
            guard,
            local_addr,
        })
    }

    /// Actual bound listen address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Frames dropped because the wire `from` id did not match the
    /// connection's authenticated identity (D2 spoof rejection counter).
    pub fn spoofed_frames_rejected(&self) -> u64 {
        self.spoof_rejected.load(Ordering::Relaxed)
    }

    /// Connections closed because a frame's `body_len` exceeded
    /// `ClusterTransportLimits::max_frame_body_bytes` (D3). The check runs
    /// before any body byte is buffered, so the forged length never drives
    /// an allocation.
    pub fn oversize_frames_rejected(&self) -> u64 {
        self.guard.oversize_rejected.load(Ordering::Relaxed)
    }

    /// Connections dropped for exceeding the per-connection inbound
    /// frame-rate quota (D3; 0 unless a quota is configured).
    pub fn rate_limited_disconnects(&self) -> u64 {
        self.guard.rate_limited_disconnects.load(Ordering::Relaxed)
    }

    /// Inbound accepts dropped because the concurrent-connection cap was
    /// reached (D3).
    pub fn connections_rejected_over_cap(&self) -> u64 {
        self.guard.conns_rejected_over_cap.load(Ordering::Relaxed)
    }

    /// Inbound accepts dropped because the source IP was jailed after a
    /// fatal protocol offense (D3).
    pub fn jailed_accepts_rejected(&self) -> u64 {
        self.guard.jailed_accepts_rejected.load(Ordering::Relaxed)
    }

    /// Take the replication receiver out of the transport. Must be called
    /// BEFORE the transport is moved into `RaftNode::new()`. Returns the
    /// receiver end of the replication channel; the caller (typically
    /// `server.rs`) passes it to the `follower_replication_loop`.
    ///
    /// Returns `None` if already taken.
    pub fn take_replication_rx(
        &self,
    ) -> Option<tokio::sync::mpsc::Receiver<Vec<u8>>> {
        self.repl_incoming_rx.lock().take()
    }

    async fn accept_loop(
        listener: TcpListener,
        tx: tokio::sync::mpsc::Sender<Vec<u8>>,
        repl_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
        security: Arc<ClusterSecurityConfig>,
        peers: Arc<HashMap<PeerId, SocketAddr>>,
        spoof: Arc<AtomicU64>,
        guard: Arc<InboundGuard>,
    ) {
        loop {
            let Ok((stream, remote)) = listener.accept().await else {
                break;
            };
            // D3 accept-jail: a source that recently committed a fatal
            // protocol offense (oversize frame, rate breach) is refused
            // outright — no task, no TLS handshake, no buffer.
            if guard.ip_jailed(remote.ip()) {
                guard.jailed_accepts_rejected.fetch_add(1, Ordering::Relaxed);
                continue; // dropping `stream` closes the socket
            }
            // D3 connection cap: one permit per open inbound connection;
            // a connect flood beyond the cap is shed here instead of
            // exhausting fds / spawned tasks.
            let Ok(permit) = guard.conn_slots.clone().try_acquire_owned() else {
                guard
                    .conns_rejected_over_cap
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    %remote,
                    cap = guard.limits.max_inbound_connections,
                    "cluster transport: inbound connection cap reached, refusing connection"
                );
                continue;
            };
            let frame_tx = tx.clone();
            let frame_repl_tx = repl_tx.clone();
            let conn_security = security.clone();
            let conn_peers = peers.clone();
            let conn_spoof = spoof.clone();
            let conn_guard = guard.clone();
            tokio::spawn(async move {
                // Hold the connection-cap permit for the connection's life.
                let _permit = permit;
                Self::handle_inbound(
                    stream,
                    remote,
                    frame_tx,
                    frame_repl_tx,
                    conn_security,
                    conn_peers,
                    conn_spoof,
                    conn_guard,
                )
                .await;
            });
        }
    }

    /// Authenticate an inbound connection (D1) and bind it to an allowed
    /// `from` identity (D2), then pump its frames.
    #[allow(clippy::too_many_arguments)]
    async fn handle_inbound(
        stream: TcpStream,
        remote: SocketAddr,
        tx: tokio::sync::mpsc::Sender<Vec<u8>>,
        repl_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
        security: Arc<ClusterSecurityConfig>,
        peers: Arc<HashMap<PeerId, SocketAddr>>,
        spoof: Arc<AtomicU64>,
        guard: Arc<InboundGuard>,
    ) {
        #[cfg(feature = "cluster-tls")]
        if let Some(tls_cfg) = &security.tls {
            // Rebuilt per accept so rotated PEM files take effect on
            // reconnect (see cluster::security module docs).
            let acceptor = match super::security::tls::build_acceptor(tls_cfg) {
                Ok(a) => a,
                Err(e) => {
                    tracing::error!(error = %e, "cluster TLS: cannot build acceptor");
                    return;
                }
            };
            let tls_stream = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(%remote, error = %e, "cluster TLS: handshake failed, refusing connection");
                    return;
                }
            };
            // Mutual auth: the verifier already required a CA-chained client
            // cert; now bind the connection to the identity it certifies.
            let peer_id = {
                let leaf = tls_stream
                    .get_ref()
                    .1
                    .peer_certificates()
                    .and_then(|c| c.first().cloned());
                match leaf {
                    Some(cert) => {
                        match super::security::tls::peer_id_from_cert(tls_cfg, cert.as_ref()) {
                            Ok(id) => id,
                            Err(e) => {
                                tracing::warn!(%remote, error = %e, "cluster TLS: cannot derive PeerId from client cert, refusing");
                                return;
                            }
                        }
                    }
                    None => {
                        tracing::warn!(%remote, "cluster TLS: no client certificate, refusing");
                        return;
                    }
                }
            };
            if !peers.contains_key(&PeerId(peer_id)) {
                tracing::warn!(%remote, peer_id, "cluster TLS: authenticated id is not a configured peer, refusing");
                return;
            }
            tracing::debug!(%remote, peer_id, "cluster TLS: inbound connection authenticated");
            Self::read_raft_frames(
                tls_stream,
                remote,
                AllowedFrom::Exact(peer_id),
                tx,
                repl_tx,
                spoof,
                guard,
            )
            .await;
            return;
        }

        // Plaintext path. `strict_from` binds the connection to the peer ids
        // configured for the remote IP; otherwise legacy behavior (no check).
        let allowed = if security.strict_from {
            let set: std::collections::HashSet<u64> = peers
                .iter()
                .filter(|(_, addr)| addr.ip() == remote.ip())
                .map(|(id, _)| id.0)
                .collect();
            if set.is_empty() {
                tracing::warn!(%remote, "cluster strict_from: connection from unknown host, all frames will be dropped");
            }
            AllowedFrom::AnyOf(Arc::new(set))
        } else {
            AllowedFrom::Any
        };
        Self::read_raft_frames(stream, remote, allowed, tx, repl_tx, spoof, guard).await;
    }

    /// Read Raft-framed messages from a stream.
    ///
    /// Protocol: [32-byte RaftFrameHeader][body of body_len bytes]
    /// body_len is at offset 16 in the header (U32 LE).
    ///
    /// Frames with `kind >= REPLICATION_KIND_MIN` (20) are routed to the
    /// replication channel (`repl_tx`); all others go to the Raft
    /// consensus channel (`tx`).
    ///
    /// D2: frames whose header `from` (offset 8, U64 LE) is not permitted by
    /// `allowed` are dropped and counted — they never reach the Raft node.
    ///
    /// D3: the wire `body_len` is validated against
    /// `ClusterTransportLimits::max_frame_body_bytes` BEFORE any body byte is
    /// buffered — a forged multi-GiB length is a fatal protocol error
    /// (connection closed + counted + source IP jailed), never an
    /// allocation. An optional per-connection frame-rate quota drops
    /// connections that exceed it.
    #[allow(clippy::too_many_arguments)]
    async fn read_raft_frames<S: AsyncRead + Unpin>(
        mut stream: S,
        remote: SocketAddr,
        allowed: AllowedFrom,
        tx: tokio::sync::mpsc::Sender<Vec<u8>>,
        repl_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
        spoof: Arc<AtomicU64>,
        guard: Arc<InboundGuard>,
    ) {
        let mut buf = Vec::with_capacity(65536);
        // Per-connection frame-rate window (D3; active only when a quota is
        // configured).
        let mut rate_window_start = Instant::now();
        let mut frames_in_window: u64 = 0;
        loop {
            // Ensure we have at least the 32-byte header.
            while buf.len() < RAFT_FRAME_HEADER_SIZE {
                let mut tmp = [0u8; 4096];
                let n = match stream.read(&mut tmp).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                buf.extend_from_slice(&tmp[..n]);
            }

            // Extract body_len from offset 16.
            let body_len = u32::from_le_bytes(
                buf[BODY_LEN_OFFSET..BODY_LEN_OFFSET + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;

            // D3: oversize `body_len` is a fatal protocol error on this
            // connection. Checked before the body-read loop below, so the
            // attacker-controlled length never grows `buf`.
            if body_len > guard.limits.max_frame_body_bytes {
                guard.oversize_rejected.fetch_add(1, Ordering::Relaxed);
                guard.jail_ip(remote.ip());
                tracing::warn!(
                    %remote,
                    body_len,
                    max = guard.limits.max_frame_body_bytes,
                    "cluster transport: oversize frame body_len, closing connection"
                );
                return;
            }

            // D3: per-connection inbound frame-rate quota (0 = unlimited).
            let quota = guard.limits.max_frames_per_conn_per_sec;
            if quota > 0 {
                if rate_window_start.elapsed() >= Duration::from_secs(1) {
                    rate_window_start = Instant::now();
                    frames_in_window = 0;
                }
                frames_in_window += 1;
                if frames_in_window > quota {
                    guard
                        .rate_limited_disconnects
                        .fetch_add(1, Ordering::Relaxed);
                    guard.jail_ip(remote.ip());
                    tracing::warn!(
                        %remote,
                        quota,
                        "cluster transport: inbound frame-rate quota exceeded, closing connection"
                    );
                    return;
                }
            }

            let total = RAFT_FRAME_HEADER_SIZE + body_len;

            // Read until we have the full frame.
            while buf.len() < total {
                let mut tmp = [0u8; 4096];
                let n = match stream.read(&mut tmp).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                buf.extend_from_slice(&tmp[..n]);
            }

            // Extract frame and route by kind.
            let frame = buf[..total].to_vec();
            buf.drain(..total);

            // D2: the wire `from` must match the connection's authenticated
            // identity. A mismatch is a spoof attempt (or a misconfigured
            // node); the frame is dropped so it can never be credited to
            // another member (vote grants, append acks, snapshot offers).
            let from = u64::from_le_bytes(
                frame[FROM_OFFSET..FROM_OFFSET + 8].try_into().unwrap(),
            );
            if !allowed.permits(from) {
                spoof.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    claimed_from = from,
                    "dropping raft frame: wire `from` does not match the connection's authenticated identity"
                );
                continue;
            }

            let kind = frame.get(KIND_OFFSET).copied().unwrap_or(0);
            let target_tx = if kind >= REPLICATION_KIND_MIN {
                &repl_tx
            } else {
                &tx
            };
            if target_tx.send(frame).await.is_err() {
                return;
            }
        }
    }

    /// Get (or establish) the outbound connection to `peer`.
    ///
    /// With TLS material, the dial performs a mutual handshake: this node
    /// presents its own cert and webpki verifies the peer's cert against the
    /// cluster CA *and* against the identity expected for `peer` (used as the
    /// TLS server name), so the outbound direction is identity-bound too.
    async fn get_or_connect(
        connections: &Mutex<HashMap<PeerId, Arc<tokio::sync::Mutex<PeerStream>>>>,
        peers: &HashMap<PeerId, SocketAddr>,
        security: &ClusterSecurityConfig,
        peer: PeerId,
    ) -> Result<Arc<tokio::sync::Mutex<PeerStream>>, RaftError> {
        let addr = *peers
            .get(&peer)
            .ok_or_else(|| RaftError::Transport(format!("unknown peer {peer:?}")))?;

        let mut conns = connections.lock().await;
        if let Some(s) = conns.get(&peer) {
            return Ok(s.clone());
        }

        let tcp = TcpStream::connect(addr)
            .await
            .map_err(|e| RaftError::Transport(e.to_string()))?;

        #[cfg(feature = "cluster-tls")]
        let stream = if let Some(tls_cfg) = &security.tls {
            // Rebuilt per dial so rotated PEM files take effect on reconnect.
            let connector = super::security::tls::build_connector(tls_cfg)
                .map_err(RaftError::Transport)?;
            let ident = super::security::identity_for_peer(tls_cfg, peer.0);
            let server_name =
                tokio_rustls::rustls::pki_types::ServerName::try_from(ident.clone())
                    .map_err(|e| {
                        RaftError::Transport(format!("bad peer identity {ident:?}: {e}"))
                    })?;
            let tls = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| RaftError::Transport(format!("TLS connect to {ident}: {e}")))?;
            PeerStream::Tls(Box::new(tls))
        } else {
            PeerStream::Plain(tcp)
        };
        #[cfg(not(feature = "cluster-tls"))]
        let stream = {
            let _ = security; // strict_from only affects the inbound side
            PeerStream::Plain(tcp)
        };

        let s = Arc::new(tokio::sync::Mutex::new(stream));
        conns.insert(peer, s.clone());
        Ok(s)
    }
}

impl RaftTransport for TcpRaftTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let connections = self.connections.clone();
        let peers = self.peers.clone();
        let security = self.security.clone();

        // SAFETY: RaftNode guarantees the slices live until this future completes.
        let slices_static =
            unsafe { std::mem::transmute::<&[&[u8]], &'static [&'static [u8]]>(slices) };

        async move {
            let stream =
                Self::get_or_connect(&connections, &peers, &security, peer).await?;

            let mut s = stream.lock().await;

            // True vectored write — one writev syscall for all iovecs.
            let mut io_bufs: Vec<IoSlice<'_>> =
                slices_static.iter().map(|s| IoSlice::new(s)).collect();
            let mut bufs: &mut [IoSlice<'_>] = &mut io_bufs;
            while !bufs.is_empty() {
                let n = s.write_vectored(bufs).await.map_err(|e| {
                    // Don't hold the lock while removing — just flag for cleanup.
                    RaftError::Transport(format!("write to peer {}: {e}", peer.0))
                })?;
                if n == 0 {
                    return Err(RaftError::Transport("write_vectored returned 0".into()));
                }
                IoSlice::advance_slices(&mut bufs, n);
            }
            // No-op on plain TCP; on TLS this drains rustls' internal buffer
            // to the socket so the frame is actually on the wire.
            s.flush()
                .await
                .map_err(|e| RaftError::Transport(format!("flush to peer {}: {e}", peer.0)))?;
            Ok(())
        }
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let connections = self.connections.clone();
        let peers = self.peers.clone();
        let security = self.security.clone();
        async move {
            let stream =
                Self::get_or_connect(&connections, &peers, &security, peer).await?;

            let mut s = stream.lock().await;
            s.write_all(&frame)
                .await
                .map_err(|e| RaftError::Transport(format!("write to peer {}: {e}", peer.0)))?;
            s.flush()
                .await
                .map_err(|e| RaftError::Transport(format!("flush to peer {}: {e}", peer.0)))?;
            Ok(())
        }
    }

    async fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> Result<usize, RaftError> {
        let mut rx = self.incoming_rx.lock().await;
        let frame = rx
            .recv()
            .await
            .ok_or_else(|| RaftError::Transport("incoming channel closed".into()))?;
        let len = frame.len();
        if out.len() < len {
            return Err(RaftError::Transport(format!(
                "recv buffer too small: need {len}, have {}",
                out.len()
            )));
        }
        out[..len].copy_from_slice(&frame);
        Ok(len)
    }

    async fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> Result<Option<usize>, RaftError> {
        let mut rx = self.incoming_rx.lock().await;
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(frame)) => {
                let len = frame.len();
                if out.len() < len {
                    return Err(RaftError::Transport(format!(
                        "recv buffer too small: need {len}, have {}",
                        out.len()
                    )));
                }
                out[..len].copy_from_slice(&frame);
                Ok(Some(len))
            }
            Ok(None) => Err(RaftError::Transport("incoming channel closed".into())),
            Err(_) => Ok(None),
        }
    }
}
