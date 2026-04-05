//! Inner — shared state for connection, request correlation, subscriber demux.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync::Mutex;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};
use zerocopy::{FromBytes, IntoBytes};
use zerocopy::byteorder::little_endian::{U16, U32};

use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::wire::delivery::{RepErrorAction, RepOkAction, RepBatchView};
use arbitro_proto::wire::envelope::{Envelope, FrameView, ENVELOPE_SIZE};

use crate::error::ClientError;
use crate::message::{AckCmd, Message};
use crate::subscription::Subscription;

/// Connection state broadcast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnState {
    Connected,
    Disconnected,
    Reconnecting,
}

/// Result of a request to the broker.
pub(crate) enum RequestResult {
    Ok(u64),
    Error(ErrorCode),
}

/// Shared state between the connection task and the client API.
pub(crate) struct Inner {
    /// Write channel — enqueues frames for the write loop.
    pub(crate) write_tx: Mutex<Option<mpsc::Sender<Bytes>>>,

    /// Connection state broadcast.
    pub(crate) state_tx: watch::Sender<ConnState>,

    /// Pending request-reply correlation: env_seq → oneshot.
    pub(crate) pending: Mutex<HashMap<u32, oneshot::Sender<RequestResult>>>,

    /// Monotonic sequence for env_seq correlation.
    pub(crate) next_seq: AtomicU32,

    /// Active subscriptions for local demux.
    pub(crate) subscriptions: Mutex<HashMap<u64, Subscription>>,

    /// Channel for ack/nack commands from Message handles.
    pub(crate) ack_tx: mpsc::Sender<AckCmd>,
    pub(crate) ack_rx: Mutex<Option<mpsc::Receiver<AckCmd>>>,

    /// Request timeout.
    pub(crate) request_timeout: std::time::Duration,

    /// Server address for reconnect.
    pub(crate) addr: String,
}

impl Inner {
    pub(crate) fn new(addr: String, request_timeout: std::time::Duration) -> Self {
        let (state_tx, _) = watch::channel(ConnState::Disconnected);
        let (ack_tx, ack_rx) = mpsc::channel(4096);

        Self {
            write_tx: Mutex::new(None),
            state_tx,
            pending: Mutex::new(HashMap::new()),
            next_seq: AtomicU32::new(1),
            subscriptions: Mutex::new(HashMap::new()),
            ack_tx,
            ack_rx: Mutex::new(Some(ack_rx)),
            request_timeout,
            addr,
        }
    }

    /// Allocate next env_seq.
    pub(crate) fn alloc_seq(&self) -> u32 {
        self.next_seq.fetch_add(1, Relaxed)
    }

    /// Send a raw frame to the write loop. Returns false if disconnected.
    pub(crate) fn send_frame(&self, frame: Bytes) -> bool {
        let guard = self.write_tx.lock().unwrap();
        if let Some(tx) = guard.as_ref() {
            tx.try_send(frame).is_ok()
        } else {
            false
        }
    }

    /// Send a request and wait for a reply (with timeout).
    pub(crate) async fn request(
        &self,
        action: Action,
        stream_id: u32,
        body: &[u8],
    ) -> Result<u64, ClientError> {
        let seq = self.alloc_seq();
        let (tx, rx) = oneshot::channel();

        // Register pending
        {
            let mut pending = self.pending.lock().unwrap();
            pending.insert(seq, tx);
        }

        // Build frame
        let envelope = Envelope {
            action: U16::new(action.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(stream_id),
            msg_len: U32::new(body.len() as u32),
            env_seq: U32::new(seq),
        };

        let mut frame = Vec::with_capacity(ENVELOPE_SIZE + body.len());
        frame.extend_from_slice(envelope.as_bytes());
        frame.extend_from_slice(body);

        if !self.send_frame(Bytes::from(frame)) {
            // Cleanup pending
            let mut pending = self.pending.lock().unwrap();
            pending.remove(&seq);
            return Err(ClientError::Disconnected);
        }

        // Wait for response with timeout
        match tokio::time::timeout(self.request_timeout, rx).await {
            Ok(Ok(RequestResult::Ok(ref_seq))) => Ok(ref_seq),
            Ok(Ok(RequestResult::Error(code))) => Err(ClientError::Broker(code)),
            Ok(Err(_)) => Err(ClientError::Disconnected), // oneshot dropped
            Err(_) => {
                // Timeout — cleanup pending
                let mut pending = self.pending.lock().unwrap();
                pending.remove(&seq);
                Err(ClientError::Timeout)
            }
        }
    }

    /// Send a frame with no reply expected (internal use only, e.g. Pong).
    fn send_no_reply(
        &self,
        action: Action,
        stream_id: u32,
        body: &[u8],
    ) {
        let envelope = Envelope {
            action: U16::new(action.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(stream_id),
            msg_len: U32::new(body.len() as u32),
            env_seq: U32::new(0),
        };

        let mut frame = Vec::with_capacity(ENVELOPE_SIZE + body.len());
        frame.extend_from_slice(envelope.as_bytes());
        frame.extend_from_slice(body);
        self.send_frame(Bytes::from(frame));
    }

    /// Process an incoming frame from the server.
    pub(crate) fn on_frame(&self, buf: &[u8]) {
        if buf.len() < ENVELOPE_SIZE {
            return;
        }

        let frame = FrameView::new(buf);
        let env = frame.envelope();
        let action = frame.action();
        let body = frame.body();

        match action {
            Some(Action::RepOk) => self.on_rep_ok(env.env_seq.get(), body),
            Some(Action::RepError) => self.on_rep_error(env.env_seq.get(), body),
            Some(Action::Deliver) => self.on_deliver(env.stream_id.get(), env.env_seq.get(), body),
            Some(Action::RepBatch) => self.on_rep_batch(env.stream_id.get(), body),
            Some(Action::Ping) => self.on_ping(body),
            Some(Action::Connected) => { /* handled in connect handshake */ }
            _ => {
                tracing::debug!(action = env.action.get(), "unknown server frame");
            }
        }
    }

    fn on_rep_ok(&self, env_seq: u32, body: &[u8]) {
        if body.len() < 16 {
            return;
        }
        let view = RepOkAction::ref_from_bytes(&body[..16]);
        let ref_seq = match view {
            Ok(v) => v.ref_seq.get(),
            Err(_) => 0,
        };

        let mut pending = self.pending.lock().unwrap();
        if let Some(tx) = pending.remove(&env_seq) {
            let _ = tx.send(RequestResult::Ok(ref_seq));
        }
    }

    fn on_rep_error(&self, env_seq: u32, body: &[u8]) {
        if body.len() < 16 {
            return;
        }
        let view = RepErrorAction::ref_from_bytes(&body[..16]);
        let code = match view {
            Ok(v) => ErrorCode::from_u16(v.error_code.get()).unwrap_or(ErrorCode::InternalError),
            Err(_) => ErrorCode::InternalError,
        };

        let mut pending = self.pending.lock().unwrap();
        if let Some(tx) = pending.remove(&env_seq) {
            let _ = tx.send(RequestResult::Error(code));
        }
    }

    /// Deliver frame: demux to local subscribers.
    /// Server body format: [2 subj_len][subject][payload]
    /// Sequence comes from envelope env_seq (u32).
    fn on_deliver(&self, stream_id: u32, env_seq: u32, body: &[u8]) {
        let subs = self.subscriptions.lock().unwrap();

        if body.len() < 2 {
            return;
        }

        let seq = env_seq as u64;
        let subj_len = u16::from_le_bytes([body[0], body[1]]) as usize;

        if 2 + subj_len > body.len() {
            return;
        }
        let subject = &body[2..2 + subj_len];
        let payload = &body[2 + subj_len..];

        for sub in subs.values() {
            if sub.stream_id == stream_id && sub.matches(subject) {
                let msg = Message {
                    seq,
                    subject: Box::from(subject),
                    payload: Bytes::copy_from_slice(payload),
                    consumer_id: sub.consumer_id,
                    stream_id,
                    ack_tx: self.ack_tx.clone(),
                };
                let _ = sub.tx.try_send(msg);
            }
        }
    }

    /// RepBatch frame: batch of delivered entries for a consumer.
    /// Body format: [8B RepBatchFixed][N × (14B entry_header + subject + payload)]
    fn on_rep_batch(&self, stream_id: u32, body: &[u8]) {
        let view = RepBatchView::new(body);
        let consumer_id = view.consumer_id();
        let subs = self.subscriptions.lock().unwrap();

        for entry in view.entries() {
            for sub in subs.values() {
                if sub.stream_id == stream_id
                    && sub.consumer_id == consumer_id
                    && sub.matches(entry.subject)
                {
                    let msg = Message {
                        seq: entry.seq,
                        subject: Box::from(entry.subject),
                        payload: Bytes::copy_from_slice(entry.payload),
                        consumer_id,
                        stream_id,
                        ack_tx: self.ack_tx.clone(),
                    };
                    let _ = sub.tx.try_send(msg);
                }
            }
        }
    }

    fn on_ping(&self, body: &[u8]) {
        // Reply with Pong
        let mut pong_body = [0u8; 8];
        if body.len() >= 8 {
            pong_body.copy_from_slice(&body[..8]);
        }
        self.send_no_reply(Action::Pong, 0, &pong_body);
    }
}
