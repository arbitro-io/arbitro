//! TCP-based Raft transport with length-prefixed framing.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arbitro_raft::{PeerId, RaftError, RaftTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// TCP transport for Raft inter-node communication.
///
/// Frames are length-prefixed: 4 bytes little-endian length followed by the
/// payload. Connections to peers are established lazily and cached for reuse.
/// Incoming frames from the listener task are funnelled through an MPSC channel
/// so that `recv_frame` / `recv_frame_timeout` are cheap polling operations.
pub struct TcpRaftTransport {
    peers: HashMap<PeerId, SocketAddr>,
    connections: Arc<Mutex<HashMap<PeerId, TcpStream>>>,
    incoming_rx: Mutex<tokio::sync::mpsc::Receiver<Vec<u8>>>,
    /// Kept alive so the accept-loop senders are not orphaned.
    _incoming_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl TcpRaftTransport {
    /// Create a new TCP transport.
    ///
    /// `bind_addr` is the local address to listen on for incoming Raft frames.
    /// `peers` maps each peer's ID to its TCP address.
    ///
    /// The listener task is spawned immediately and runs for the lifetime of
    /// the transport.
    pub async fn new(
        bind_addr: SocketAddr,
        peers: HashMap<PeerId, SocketAddr>,
    ) -> Result<Self, RaftError> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .map_err(|e| RaftError::Transport(format!("bind {bind_addr}: {e}")))?;

        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);

        let accept_tx = tx.clone();
        tokio::spawn(async move {
            Self::accept_loop(listener, accept_tx).await;
        });

        Ok(Self {
            peers,
            connections: Arc::new(Mutex::new(HashMap::new())),
            incoming_rx: Mutex::new(rx),
            _incoming_tx: tx,
        })
    }

    /// Background task: accept connections and read length-prefixed frames.
    async fn accept_loop(listener: TcpListener, tx: tokio::sync::mpsc::Sender<Vec<u8>>) {
        loop {
            let Ok((stream, _addr)) = listener.accept().await else {
                break;
            };
            let frame_tx = tx.clone();
            tokio::spawn(async move {
                Self::read_frames(stream, frame_tx).await;
            });
        }
    }

    /// Read length-prefixed frames from a single TCP connection.
    async fn read_frames(mut stream: TcpStream, tx: tokio::sync::mpsc::Sender<Vec<u8>>) {
        loop {
            // Read 4-byte LE length prefix.
            let len = match stream.read_u32_le().await {
                Ok(n) => n as usize,
                Err(_) => break,
            };
            if len == 0 {
                continue;
            }

            let mut buf = vec![0u8; len];
            if stream.read_exact(&mut buf).await.is_err() {
                break;
            }

            if tx.send(buf).await.is_err() {
                break;
            }
        }
    }

    /// Get or create a cached TCP connection to a peer.
    async fn get_connection(&self, peer: PeerId) -> Result<(), RaftError> {
        let mut conns = self.connections.lock().await;
        if conns.contains_key(&peer) {
            return Ok(());
        }
        let addr = self
            .peers
            .get(&peer)
            .ok_or_else(|| RaftError::PeerUnknown(peer))?;
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| RaftError::Transport(format!("connect to peer {}: {e}", peer.0)))?;
        conns.insert(peer, stream);
        Ok(())
    }

    /// Write a length-prefixed frame to a peer, reconnecting on failure.
    async fn write_frame(&self, peer: PeerId, data: &[u8]) -> Result<(), RaftError> {
        // Ensure connection exists.
        self.get_connection(peer).await?;

        let mut conns = self.connections.lock().await;
        let stream = conns
            .get_mut(&peer)
            .ok_or_else(|| RaftError::PeerUnknown(peer))?;

        let len = data.len() as u32;
        let result = async {
            stream.write_all(&len.to_le_bytes()).await?;
            stream.write_all(data).await?;
            stream.flush().await?;
            Ok::<(), std::io::Error>(())
        }
        .await;

        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                // Drop the broken connection so the next call reconnects.
                conns.remove(&peer);
                Err(RaftError::Transport(format!(
                    "write to peer {}: {e}",
                    peer.0
                )))
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
        // Concatenate slices into a single buffer and send as one frame.
        let mut frame = Vec::new();
        for s in slices {
            frame.extend_from_slice(s);
        }
        async move { self.write_frame(peer, &frame).await }
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move { self.write_frame(peer, &frame).await }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        async move {
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
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        async move {
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
}
