//! TCP-based Raft transport using the arbitro-raft wire format.
//!
//! Frames use the 32-byte `RaftFrameHeader` defined in arbitro-raft's
//! `protocol::codec::wire`. The `body_len` field at offset 16 gives the
//! body size; total frame = 32 + body_len.
//!
//! `send_vectored` uses true `write_vectored` with `IoSlice` to avoid
//! copying payload buffers — matching the zero-copy design of the bench
//! transport in `arbitro-raft/benches/tcp_raft_bench.rs`.

use std::collections::HashMap;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arbitro_raft::{PeerId, RaftError, RaftTransport, RAFT_FRAME_HEADER_SIZE};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// Offset of `body_len: U32` inside `RaftFrameHeader`.
const BODY_LEN_OFFSET: usize = 16;

/// Minimum kind value for data-plane replication frames. Frames with
/// `kind >= REPLICATION_KIND_MIN` are routed to the dedicated replication
/// channel instead of the Raft consensus channel.
const REPLICATION_KIND_MIN: u8 = 20;

/// Offset of the `kind` byte inside `RaftFrameHeader`.
const KIND_OFFSET: usize = 5;

pub struct TcpRaftTransport {
    peers: HashMap<PeerId, SocketAddr>,
    connections: Arc<Mutex<HashMap<PeerId, Arc<tokio::sync::Mutex<TcpStream>>>>>,
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
}

impl TcpRaftTransport {
    pub async fn new(
        bind_addr: SocketAddr,
        peers: HashMap<PeerId, SocketAddr>,
    ) -> Result<Self, RaftError> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .map_err(|e| RaftError::Transport(format!("bind {bind_addr}: {e}")))?;

        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);
        let (repl_tx, repl_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);

        let accept_tx = tx.clone();
        let accept_repl_tx = repl_tx.clone();
        tokio::spawn(async move {
            Self::accept_loop(listener, accept_tx, accept_repl_tx).await;
        });

        Ok(Self {
            peers,
            connections: Arc::new(Mutex::new(HashMap::new())),
            incoming_rx: Mutex::new(rx),
            _incoming_tx: tx,
            repl_incoming_rx: parking_lot::Mutex::new(Some(repl_rx)),
            _repl_incoming_tx: repl_tx,
        })
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
    ) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let frame_tx = tx.clone();
            let frame_repl_tx = repl_tx.clone();
            tokio::spawn(async move {
                Self::read_raft_frames(stream, frame_tx, frame_repl_tx).await;
            });
        }
    }

    /// Read Raft-framed messages from a TCP stream.
    ///
    /// Protocol: [32-byte RaftFrameHeader][body of body_len bytes]
    /// body_len is at offset 16 in the header (U32 LE).
    ///
    /// Frames with `kind >= REPLICATION_KIND_MIN` (20) are routed to the
    /// replication channel (`repl_tx`); all others go to the Raft
    /// consensus channel (`tx`).
    async fn read_raft_frames(
        mut stream: TcpStream,
        tx: tokio::sync::mpsc::Sender<Vec<u8>>,
        repl_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) {
        let mut buf = Vec::with_capacity(65536);
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
}

impl RaftTransport for TcpRaftTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let connections = self.connections.clone();
        let peers = self.peers.clone();

        // SAFETY: RaftNode guarantees the slices live until this future completes.
        let slices_static =
            unsafe { std::mem::transmute::<&[&[u8]], &'static [&'static [u8]]>(slices) };

        async move {
            let addr = *peers
                .get(&peer)
                .ok_or_else(|| RaftError::Transport(format!("unknown peer {peer:?}")))?;

            let stream = {
                let mut conns = connections.lock().await;
                if let Some(s) = conns.get(&peer) {
                    s.clone()
                } else {
                    let s = TcpStream::connect(addr)
                        .await
                        .map_err(|e| RaftError::Transport(e.to_string()))?;
                    let s = Arc::new(tokio::sync::Mutex::new(s));
                    conns.insert(peer, s.clone());
                    s
                }
            };

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
        async move {
            let addr = *peers
                .get(&peer)
                .ok_or_else(|| RaftError::Transport(format!("unknown peer {peer:?}")))?;

            let stream = {
                let mut conns = connections.lock().await;
                if let Some(s) = conns.get(&peer) {
                    s.clone()
                } else {
                    let s = TcpStream::connect(addr)
                        .await
                        .map_err(|e| RaftError::Transport(e.to_string()))?;
                    let s = Arc::new(tokio::sync::Mutex::new(s));
                    conns.insert(peer, s.clone());
                    s
                }
            };

            let mut s = stream.lock().await;
            s.write_all(&frame)
                .await
                .map_err(|e| RaftError::Transport(format!("write to peer {}: {e}", peer.0)))?;
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
