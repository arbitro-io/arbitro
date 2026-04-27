//! Inner — shared state for connection, request correlation, subscriber demux.
//!
//! ## Channel topology (kit-native, zero `tokio::sync::*`)
//!
//! - `write_tx`     : `kit::Mpsc<Bytes, 256>` — M:1 fan-in to write_loop (OS thread).
//! - `ack_tx`       : `kit::Mpsc<AckCmd, 256>` — M:1 fan-in to ack_loop (OS thread).
//! - `pending`      : `kit::OneShot<RequestResult>` — per-request reply slot.
//! - `state_tx`     : `tokio::sync::watch` — kept (pub/sub many subscribers, async API).
//!
//! `MpscProducer` is `Send + !Sync`. Many concurrent call sites push frames →
//! we wrap the producer in `Mutex<MpscProducer>` (single producer slot, M=1).
//! The Mutex serialises pushes; kit's lock-free `try_send` (Relaxed + Release,
//! no LOCK-prefixed RMW) still beats `tokio::mpsc::Sender::try_send`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use tokio::sync::watch;
use zerocopy::{FromBytes, IntoBytes};
use zerocopy::byteorder::little_endian::{U16, U32};

use arbitro_kit::route::{
    Mpsc, MpscConsumer, MpscProducer, MpscShutdown, OneShot, OneShotSender,
};

use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::wire::delivery::{RepBatchView, RepErrorAction, RepOkAction};
use arbitro_proto::wire::envelope::{Envelope, FrameView, ENVELOPE_SIZE};
use arbitro_proto::wire::publish::PublishEntry;
use arbitro_engine_v2::common::SubjectTrie;

use crate::error::ClientError;
use crate::message::{AckCmd, AckProducer, Message};
use crate::subscription::Subscription;

/// Ring capacity for write_tx and ack_tx.
///
/// 4096 publish frames is ~256 KB worst-case (Bytes is 32 B, PubSingle is
/// 2× Bytes + tag ≈ 72 B). Sized to absorb fire-and-forget bursts without
/// forcing producers into backpressure parking on every other publish.
pub(crate) const WRITE_RING_CAP: usize = 4096;
pub(crate) const ACK_RING_CAP: usize = 4096;

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
    /// Variable-length reply: the raw body bytes (for ListStreams etc).
    OkBody(Bytes),
    Error(ErrorCode),
}

/// Write-ring item. Two variants so the publish hot path can ship the
/// user payload as a separate iovec (zero-copy via `write_vectored`),
/// while every other frame stays in a single contiguous buffer.
pub(crate) enum WriteFrame {
    /// Single contiguous frame (envelope + body in one alloc).
    Mono(Bytes),
    /// Two-part frame for single-entry publish.
    /// `header` = `[envelope][count=1][PublishEntry][subject]`
    /// `payload` = the user's bytes (never copied past the API boundary).
    PubSingle { header: Bytes, payload: Bytes },
}


/// Producer handle for the write channel. Wrapped in Arc<Mutex<…>> so it can
/// be cloned across send call sites and reset on reconnect without taking
/// down the live Inner struct.
pub(crate) type WriteProducer = Arc<Mutex<MpscProducer<WriteFrame, WRITE_RING_CAP>>>;

/// Shared state between the connection task and the client API.
pub(crate) struct Inner {
    /// Producer half of the write channel. `None` while disconnected.
    /// Replaced on each reconnect session.
    pub(crate) write_tx: RwLock<Option<WriteProducer>>,

    /// Connection state broadcast.
    pub(crate) state_tx: watch::Sender<ConnState>,

    /// Pending request-reply correlation: env_seq → kit OneShot sender.
    /// Each in-flight `request*` allocates one OneShot pair; the Sender
    /// lives here until the reply arrives or the request is cleaned up
    /// (timeout / disconnect). Dropping the Sender wakes the parked
    /// receiver with `Err(Closed)`.
    pub(crate) pending: Mutex<HashMap<u32, OneShotSender<RequestResult>>>,

    /// Monotonic sequence for env_seq correlation.
    pub(crate) next_seq: AtomicU32,

    /// Active subscriptions for local demux.
    pub(crate) subscriptions: RwLock<HashMap<u32, HashMap<u32, Vec<(u64, Subscription)>>>>,

    /// Current ack producer — replaced on each reconnect session.
    /// Messages hold a clone (`Arc<Mutex<MpscProducer>>`); after reconnect
    /// the old clone goes stale (try_send may succeed into a doomed ring,
    /// or fail — both are correct: stale ACKs are discarded).
    pub(crate) ack_tx: RwLock<AckProducer>,

    /// Precomputed Trie for O(M) fanout distribution.
    pub(crate) subject_trie: RwLock<SubjectTrie>,

    /// Monotonic subscription ID — shared across all Consumer objects.
    pub(crate) next_sub_id: AtomicU64,

    /// Request timeout.
    pub(crate) request_timeout: std::time::Duration,

    /// Server address for reconnect.
    pub(crate) addr: String,
}

impl Inner {
    pub(crate) fn new(addr: String, request_timeout: std::time::Duration) -> Self {
        let (state_tx, _) = watch::channel(ConnState::Disconnected);

        // Initial sentinel ack producer — replaced on first session via new_ack_channel.
        // We need a valid Arc<Mutex<MpscProducer>> to satisfy the type before the
        // first session brings up a real channel.
        let (mut producers, _consumer, _shutdown) =
            Mpsc::<AckCmd, ACK_RING_CAP>::new(1);
        let ack_tx = Arc::new(Mutex::new(producers.pop().unwrap()));

        Self {
            write_tx: RwLock::new(None),
            state_tx,
            pending: Mutex::new(HashMap::new()),
            next_seq: AtomicU32::new(1),
            subscriptions: RwLock::new(HashMap::new()),
            subject_trie: RwLock::new(SubjectTrie::new()),
            ack_tx: RwLock::new(ack_tx),
            next_sub_id: AtomicU64::new(1),
            request_timeout,
            addr,
        }
    }

    /// Update the SubjectTrie from the current subscriptions (Cold Path).
    fn rebuild_trie(&self) {
        let subs = self.subscriptions.read().unwrap();
        let mut trie = self.subject_trie.write().unwrap();
        trie.clear();
        for by_consumer in subs.values() {
            for entries in by_consumer.values() {
                for &(id, ref sub) in entries {
                    if let Some(ref filter) = sub.filter {
                        trie.insert(filter, id as u32);
                    } else {
                        trie.insert(b">", id as u32);
                    }
                }
            }
        }
    }

    /// Build a fresh ack channel for a new session. Returns the consumer +
    /// shutdown handle for the ack_loop thread; installs the new producer
    /// into `self.ack_tx`. Old `Message` clones hold the previous producer
    /// (correctly stale).
    pub(crate) fn new_ack_channel(
        &self,
    ) -> (
        MpscConsumer<AckCmd, ACK_RING_CAP>,
        MpscShutdown<AckCmd, ACK_RING_CAP>,
    ) {
        let (mut producers, consumer, shutdown) =
            Mpsc::<AckCmd, ACK_RING_CAP>::new(1);
        let new_tx = Arc::new(Mutex::new(producers.pop().unwrap()));
        *self.ack_tx.write().unwrap() = new_tx;
        (consumer, shutdown)
    }

    /// Install a new write channel producer. Called by `run_session` after
    /// `Mpsc::new(1)` has spun up the consumer/shutdown handles for the
    /// dedicated write OS thread.
    pub(crate) fn install_write_producer(&self, producer: WriteProducer) {
        *self.write_tx.write().unwrap() = Some(producer);
    }

    /// Clear the write producer (on disconnect). Subsequent `send_frame`
    /// returns `false` until the next reconnect installs a new producer.
    pub(crate) fn clear_write_producer(&self) {
        *self.write_tx.write().unwrap() = None;
    }

    /// Allocate next env_seq.
    pub(crate) fn alloc_seq(&self) -> u32 {
        self.next_seq.fetch_add(1, Relaxed)
    }

    /// Non-blocking enqueue. Returns `false` if disconnected OR the ring
    /// is full. Used by code paths that explicitly handle backpressure
    /// or by best-effort send sites (Pong, internal bookkeeping).
    pub(crate) fn send_frame(&self, frame: WriteFrame) -> bool {
        let guard = self.write_tx.read().unwrap();
        match guard.as_ref() {
            Some(producer) => producer.lock().unwrap().try_send(frame).is_ok(),
            None => false,
        }
    }

    /// Convenience: enqueue a `Bytes` as a single contiguous frame.
    #[inline]
    pub(crate) fn send_mono(&self, frame: Bytes) -> bool {
        self.send_frame(WriteFrame::Mono(frame))
    }

    /// Snapshot the current write producer, if any.
    fn current_producer(&self) -> Option<WriteProducer> {
        self.write_tx.read().unwrap().clone()
    }

    /// Backpressure-aware enqueue used by `publish` / `request` paths.
    ///
    /// Layered policy:
    /// 1. `try_send` — succeeds in the steady state (ring has room).
    /// 2. On `Full`, burn a short window of cooperative `yield_now`
    ///    turns so the writer task has a chance to drain without
    ///    paying spawn_blocking overhead (~50 µs/publish, kills
    ///    throughput).
    /// 3. If the ring stays full beyond that, return
    ///    `Err(ClientError::Backpressure)`. The frame was NOT enqueued;
    ///    the caller decides retry / drop / circuit-break. Sync
    ///    wrappers translate this into a bounded retry loop.
    ///
    /// Returns `Err(ClientError::Disconnected)` only when the producer
    /// handle is actually gone (clear_write_producer happened) — never
    /// on transient backpressure.
    pub(crate) async fn enqueue(&self, mut frame: WriteFrame) -> Result<(), ClientError> {
        const FAST_YIELDS: usize = 64;

        for _ in 0..FAST_YIELDS {
            let producer = match self.current_producer() {
                Some(p) => p,
                None => return Err(ClientError::Disconnected),
            };
            match producer.lock().unwrap().try_send(frame) {
                Ok(()) => return Ok(()),
                Err(returned) => frame = returned,
            }
            tokio::task::yield_now().await;
        }

        // Ring saturated past the absorption window. Surface backpressure
        // to the caller — do NOT spawn_blocking (~50 µs/publish), do NOT
        // disconnect (the connection is fine, the network is just slow).
        Err(ClientError::Backpressure)
    }

    /// Build the header for a single-entry publish frame in one alloc.
    /// Layout: `[envelope 16][count=1, 4][PublishEntry 12][subject N]`.
    /// The user payload is NOT included — it travels separately as a
    /// second iovec to the write loop, never copied past the API boundary.
    #[inline]
    fn build_publish_single_header(
        seq: u32,
        action: Action,
        stream_id: u32,
        subject: &[u8],
        payload_len: usize,
    ) -> Bytes {
        let body_len: u32 = (4 + 12 + subject.len() + payload_len) as u32;
        let total = ENVELOPE_SIZE + 4 + 12 + subject.len();
        let mut buf = Vec::with_capacity(total);

        let env = Envelope {
            action: U16::new(action.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(stream_id),
            msg_len: U32::new(body_len),
            env_seq: U32::new(seq),
        };
        buf.extend_from_slice(env.as_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        let entry = PublishEntry {
            data_len: U32::new(payload_len as u32),
            subj_len: U16::new(subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0u8; 3],
        };
        buf.extend_from_slice(entry.as_bytes());
        buf.extend_from_slice(subject);
        debug_assert_eq!(buf.len(), total);
        Bytes::from(buf)
    }

    /// Fire-and-forget single-entry publish — zero-copy of `payload`.
    /// `payload` is shipped to the write loop as a separate iovec and
    /// goes to the kernel via `write_vectored` without ever being
    /// copied in userspace past this point.
    pub(crate) async fn fire_and_forget_publish_single(
        &self,
        action: Action,
        stream_id: u32,
        subject: &[u8],
        payload: Bytes,
    ) -> Result<(), ClientError> {
        let seq = self.alloc_seq();
        let header = Self::build_publish_single_header(
            seq, action, stream_id, subject, payload.len(),
        );
        self.enqueue(WriteFrame::PubSingle { header, payload }).await
    }

    /// Sync single-entry publish — same zero-copy path as
    /// `fire_and_forget_publish_single`, with reply correlation.
    pub(crate) async fn request_publish_single(
        &self,
        action: Action,
        stream_id: u32,
        subject: &[u8],
        payload: Bytes,
    ) -> Result<u64, ClientError> {
        let seq = self.alloc_seq();
        let (tx, rx) = OneShot::<RequestResult>::new();
        self.pending.lock().unwrap().insert(seq, tx);

        let header = Self::build_publish_single_header(
            seq, action, stream_id, subject, payload.len(),
        );

        if let Err(e) = self.enqueue(WriteFrame::PubSingle { header, payload }).await {
            self.pending.lock().unwrap().remove(&seq);
            return Err(e);
        }

        let timeout_dur = self.request_timeout;
        let handle = tokio::task::spawn_blocking(move || {
            rx.bind();
            rx.recv()
        });

        match tokio::time::timeout(timeout_dur, handle).await {
            Ok(Ok(Ok(RequestResult::Ok(v)))) => Ok(v),
            Ok(Ok(Ok(RequestResult::OkBody(_)))) => Ok(0),
            Ok(Ok(Ok(RequestResult::Error(code)))) => Err(ClientError::Broker(code)),
            Ok(Ok(Err(_closed))) => Err(ClientError::Disconnected),
            Ok(Err(_join)) => Err(ClientError::Disconnected),
            Err(_elapsed) => {
                self.pending.lock().unwrap().remove(&seq);
                Err(ClientError::Timeout)
            }
        }
    }

    /// Send a request and wait for a fixed-size reply (with timeout).
    pub(crate) async fn request(
        &self,
        action: Action,
        stream_id: u32,
        body: &[u8],
    ) -> Result<u64, ClientError> {
        match self.do_request(action, stream_id, body).await? {
            RequestResult::Ok(v) => Ok(v),
            RequestResult::OkBody(_) => Ok(0),
            RequestResult::Error(code) => Err(ClientError::Broker(code)),
        }
    }

    /// Send a request and wait for a variable-length body reply.
    pub(crate) async fn request_body(
        &self,
        action: Action,
        stream_id: u32,
        body: &[u8],
    ) -> Result<Bytes, ClientError> {
        match self.do_request(action, stream_id, body).await? {
            RequestResult::OkBody(data) => Ok(data),
            RequestResult::Ok(_) => Ok(Bytes::new()),
            RequestResult::Error(code) => Err(ClientError::Broker(code)),
        }
    }

    /// Internal: build the frame, register the OneShot Sender in `pending`,
    /// enqueue, then await the reply via spawn_blocking + tokio timeout.
    /// On timeout we drop the Sender (by removing from `pending`), which
    /// wakes the parked receiver in spawn_blocking with `Closed`.
    async fn do_request(
        &self,
        action: Action,
        stream_id: u32,
        body: &[u8],
    ) -> Result<RequestResult, ClientError> {
        let seq = self.alloc_seq();
        let (tx, rx) = OneShot::<RequestResult>::new();

        self.pending.lock().unwrap().insert(seq, tx);

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

        if let Err(e) = self.enqueue(WriteFrame::Mono(Bytes::from(frame))).await {
            self.pending.lock().unwrap().remove(&seq);
            return Err(e);
        }

        let timeout_dur = self.request_timeout;
        let handle = tokio::task::spawn_blocking(move || {
            rx.bind();
            rx.recv()
        });

        match tokio::time::timeout(timeout_dur, handle).await {
            Ok(Ok(Ok(result))) => Ok(result),
            Ok(Ok(Err(_closed))) => Err(ClientError::Disconnected),
            Ok(Err(_join)) => Err(ClientError::Disconnected),
            Err(_elapsed) => {
                // Drop the Sender: dropping wakes the parked spawn_blocking
                // task with Err(Closed); it cleans itself up.
                self.pending.lock().unwrap().remove(&seq);
                Err(ClientError::Timeout)
            }
        }
    }

    /// Fire-and-forget: enqueue a frame with no reply correlation.
    /// Synchronous body — the `async` signature is preserved for API
    /// compatibility with prior code paths but contains no `.await`.
    pub(crate) async fn fire_and_forget(
        &self,
        action: Action,
        stream_id: u32,
        body: &[u8],
    ) -> Result<(), ClientError> {
        let seq = self.alloc_seq();
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

        self.enqueue(WriteFrame::Mono(Bytes::from(frame))).await
    }

    /// Send a frame with no reply expected (internal use, e.g. Pong).
    fn send_no_reply(&self, action: Action, stream_id: u32, body: &[u8]) {
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
        self.send_mono(Bytes::from(frame));
    }

    /// Process an incoming frame from the server.
    pub(crate) fn on_frame(self: &Arc<Self>, buf: &[u8]) {
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
            Some(Action::FanoutBatch) => self.on_fanout_batch(env.stream_id.get(), body),
            Some(Action::ListStreams) => self.on_list_streams(env.env_seq.get(), body),
            Some(Action::ListConsumers) => self.on_list_consumers(env.env_seq.get(), body),
            Some(Action::Ping) => self.on_ping(body),
            Some(Action::Connected) => { /* handshake */ }
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

        let tx_opt = self.pending.lock().unwrap().remove(&env_seq);
        if let Some(tx) = tx_opt {
            tx.send(RequestResult::Ok(ref_seq));
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

        let tx_opt = self.pending.lock().unwrap().remove(&env_seq);
        if let Some(tx) = tx_opt {
            tx.send(RequestResult::Error(code));
        }
    }

    fn on_deliver(self: &Arc<Self>, stream_id: u32, env_seq: u32, body: &[u8]) {
        let subs = self.subscriptions.read().unwrap();

        if body.len() < 6 {
            return;
        }

        let seq = env_seq as u64;
        let consumer_id = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
        let subj_len = u16::from_le_bytes([body[4], body[5]]) as usize;

        if 6 + subj_len > body.len() {
            return;
        }
        let subject = &body[6..6 + subj_len];
        let payload = &body[6 + subj_len..];

        let ack_tx = self.ack_tx.read().unwrap().clone();
        if let Some(by_consumer) = subs.get(&stream_id) {
            if let Some(entries) = by_consumer.get(&consumer_id) {
                if let Some(&(_, ref sub)) = entries.first() {
                    let msg = Message {
                        seq,
                        subject: Box::from(subject),
                        payload: Bytes::copy_from_slice(payload),
                        consumer_id,
                        stream_id,
                        ack_tx,
                        inner: Arc::clone(self),
                    };
                    let _ = sub.tx.try_send(msg);
                }
            }
        }
    }

    fn on_rep_batch(self: &Arc<Self>, stream_id: u32, body: &[u8]) {
        let view = RepBatchView::new(body);
        let subs = self.subscriptions.read().unwrap();
        let ack_tx = self.ack_tx.read().unwrap().clone();

        let by_consumer = match subs.get(&stream_id) {
            Some(m) => m,
            None => return,
        };

        for entry in view.entries() {
            let targets = match by_consumer.get(&entry.consumer_id) {
                Some(t) => t,
                None => continue,
            };
            for &(_, ref sub) in targets {
                if sub.matches(entry.subject) {
                    let msg = Message {
                        seq: entry.seq,
                        subject: Box::from(entry.subject),
                        payload: Bytes::copy_from_slice(entry.payload),
                        consumer_id: entry.consumer_id,
                        stream_id,
                        ack_tx: ack_tx.clone(),
                        inner: Arc::clone(self),
                    };
                    let _ = sub.tx.try_send(msg);
                }
            }
        }
    }

    fn on_list_streams(&self, env_seq: u32, body: &[u8]) {
        let tx_opt = self.pending.lock().unwrap().remove(&env_seq);
        if let Some(tx) = tx_opt {
            tx.send(RequestResult::OkBody(Bytes::copy_from_slice(body)));
        }
    }

    fn on_list_consumers(&self, env_seq: u32, body: &[u8]) {
        let tx_opt = self.pending.lock().unwrap().remove(&env_seq);
        if let Some(tx) = tx_opt {
            tx.send(RequestResult::OkBody(Bytes::copy_from_slice(body)));
        }
    }

    fn on_ping(&self, body: &[u8]) {
        let mut pong_body = [0u8; 8];
        if body.len() >= 8 {
            pong_body.copy_from_slice(&body[..8]);
        }
        self.send_no_reply(Action::Pong, 0, &pong_body);
    }

    fn on_fanout_batch(self: &Arc<Self>, stream_id: u32, body: &[u8]) {
        let view = RepBatchView::new(body);
        let subs = self.subscriptions.read().unwrap();
        let by_consumer = match subs.get(&stream_id) {
            Some(m) => m,
            None => return,
        };
        let trie = self.subject_trie.read().unwrap();
        let ack_tx = self.ack_tx.read().unwrap().clone();

        for entry in view.entries() {
            trie.find_matches(entry.subject, |sub_id| {
                let sub_id_64 = sub_id as u64;
                for entries in by_consumer.values() {
                    for &(id, ref sub) in entries {
                        if id == sub_id_64 {
                            let msg = Message {
                                seq: entry.seq,
                                subject: Box::from(entry.subject),
                                payload: Bytes::copy_from_slice(entry.payload),
                                consumer_id: sub.consumer_id,
                                stream_id,
                                ack_tx: ack_tx.clone(),
                                inner: Arc::clone(self),
                            };
                            let _ = sub.tx.try_send(msg);
                            return;
                        }
                    }
                }
            });
        }
    }

    pub(crate) fn add_subscription(&self, id: u64, sub: Subscription) {
        {
            let mut subs = self.subscriptions.write().unwrap();
            subs.entry(sub.stream_id)
                .or_default()
                .entry(sub.consumer_id)
                .or_default()
                .push((id, sub));
        }
        self.rebuild_trie();
    }

    pub(crate) fn remove_subscription(&self, id: u64) {
        {
            let mut subs = self.subscriptions.write().unwrap();
            'outer: for by_consumer in subs.values_mut() {
                for entries in by_consumer.values_mut() {
                    if let Some(pos) = entries.iter().position(|&(sid, _)| sid == id) {
                        entries.swap_remove(pos);
                        break 'outer;
                    }
                }
            }
        }
        self.rebuild_trie();
    }

    /// Drain all `pending` OneShot senders by dropping them. Receivers wake
    /// with `Closed`; callers translate to `ClientError::Disconnected`.
    /// Called by `connection_loop` on disconnect / shutdown.
    pub(crate) fn drain_pending(&self) {
        self.pending.lock().unwrap().clear();
    }
}
