//! Service-to-service RPC: `client.service("name")` builder, `svc.request()`,
//! and `svc.handle()` for registering handlers that auto-reply.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::client::Client;
use crate::consume::message::{encode_reply_to, Message};
use crate::error::ClientError;

/// Prefix for all service streams.
const SVC_PREFIX: &[u8] = b"_svc.";
/// Separator before the reply correlation segment.
const REPLY_INFIX: &[u8] = b"._r.";

// ── ReplyMux ─────────────────────────────────────────────────────────────────

/// Routes incoming reply messages to the correct waiting request() future
/// by matching the correlation ID in the subject suffix.
struct ReplyMux {
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Bytes>>>>,
    next_corr: AtomicU64,
}

impl ReplyMux {
    fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_corr: AtomicU64::new(1),
        }
    }

    fn next_correlation(&self) -> u64 {
        self.next_corr.fetch_add(1, Ordering::Relaxed)
    }

    fn register(&self, corr_id: u64) -> oneshot::Receiver<Bytes> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(corr_id, tx);
        rx
    }

    fn complete(&self, corr_id: u64, payload: Bytes) {
        if let Some(tx) = self.pending.lock().unwrap().remove(&corr_id) {
            let _ = tx.send(payload);
        }
    }

    fn cancel(&self, corr_id: u64) {
        self.pending.lock().unwrap().remove(&corr_id);
    }
}

// ── Service ──────────────────────────────────────────────────────────────────

/// Type-erased handler: takes a Message and returns a pinned future.
type BoxHandler = Arc<
    dyn Fn(Message) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// A named service that can handle incoming RPC requests and make outgoing
/// requests to other services.
///
/// Created via [`Client::service`]. Internally manages a stream, consumer,
/// and reply correlation mux.
pub struct Service {
    name: String,
    stream_id: u32,
    #[allow(dead_code)]
    consumer_id: u32,
    client: Client,
    reply_mux: Arc<ReplyMux>,
    /// Cache of resolved service name → stream_id.
    stream_cache: Arc<Mutex<HashMap<String, u32>>>,
    /// Registered handlers: subject prefix → handler fn.
    handlers: Arc<Mutex<Vec<(Vec<u8>, BoxHandler)>>>,
    cancel: CancellationToken,
}

impl std::fmt::Debug for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Service")
            .field("name", &self.name)
            .field("stream_id", &self.stream_id)
            .finish()
    }
}

impl Service {
    /// The service name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The stream_id backing this service.
    pub fn stream_id(&self) -> u32 {
        self.stream_id
    }

    /// Send a request to another service and wait for the reply.
    ///
    /// `target` is the service name (e.g., "payments").
    /// `method` is the subject suffix (e.g., "charge").
    /// Returns the reply payload or a timeout/error.
    pub async fn request(
        &self,
        target: &str,
        method: &[u8],
        payload: Bytes,
        timeout_ms: u64,
    ) -> Result<Bytes, ClientError> {
        let target_stream_id = self.resolve_stream(target).await?;

        let corr_id = self.reply_mux.next_correlation();
        let rx = self.reply_mux.register(corr_id);

        // Build subject: _svc.<target>.<method>
        let mut subject = Vec::with_capacity(SVC_PREFIX.len() + target.len() + 1 + method.len());
        subject.extend_from_slice(SVC_PREFIX);
        subject.extend_from_slice(target.as_bytes());
        subject.push(b'.');
        subject.extend_from_slice(method);

        // Build reply_to: encoded(self.stream_id, _svc.<self.name>._r.<corr_id>)
        let corr_str = corr_id.to_string();
        let mut reply_subject =
            Vec::with_capacity(SVC_PREFIX.len() + self.name.len() + REPLY_INFIX.len() + corr_str.len());
        reply_subject.extend_from_slice(SVC_PREFIX);
        reply_subject.extend_from_slice(self.name.as_bytes());
        reply_subject.extend_from_slice(REPLY_INFIX);
        reply_subject.extend_from_slice(corr_str.as_bytes());

        let reply_to = encode_reply_to(self.stream_id, &reply_subject);

        // Publish with reply_to
        self.client
            .publish_with_reply(target_stream_id, &subject, &reply_to, payload)
            .await?;

        // Wait for reply with timeout
        match timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(_)) => {
                // Sender was dropped (service crashed or mux shut down)
                Err(ClientError::Timeout)
            }
            Err(_) => {
                self.reply_mux.cancel(corr_id);
                Err(ClientError::Timeout)
            }
        }
    }

    /// Fire-and-forget send to another service (no reply expected).
    pub async fn send(
        &self,
        target: &str,
        method: &[u8],
        payload: Bytes,
    ) -> Result<(), ClientError> {
        let target_stream_id = self.resolve_stream(target).await?;

        let mut subject = Vec::with_capacity(SVC_PREFIX.len() + target.len() + 1 + method.len());
        subject.extend_from_slice(SVC_PREFIX);
        subject.extend_from_slice(target.as_bytes());
        subject.push(b'.');
        subject.extend_from_slice(method);

        self.client.publish(target_stream_id, &subject, payload)
    }

    /// Register a handler for incoming requests matching `method`.
    ///
    /// The handler receives a [`Message`] and should call `msg.reply(payload)`
    /// to send the response back to the requester.
    ///
    /// Returns a [`ServiceHandle`] that can be used to stop the handler.
    pub fn handle<F, Fut>(&self, method: &[u8], handler: F) -> ServiceHandle
    where
        F: Fn(Message) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let cancel = self.cancel.child_token();

        // Build the subject prefix this handler matches:
        // _svc.<name>.<method>
        let mut match_prefix =
            Vec::with_capacity(SVC_PREFIX.len() + self.name.len() + 1 + method.len());
        match_prefix.extend_from_slice(SVC_PREFIX);
        match_prefix.extend_from_slice(self.name.as_bytes());
        match_prefix.push(b'.');
        match_prefix.extend_from_slice(method);

        let boxed: BoxHandler = Arc::new(move |msg| Box::pin(handler(msg)));
        self.handlers.lock().unwrap().push((match_prefix, boxed));

        ServiceHandle { cancel }
    }

    /// Resolve a service name to its stream_id, creating the stream if needed.
    async fn resolve_stream(&self, target: &str) -> Result<u32, ClientError> {
        // Check cache first
        if let Some(&id) = self.stream_cache.lock().unwrap().get(target) {
            return Ok(id);
        }

        // Build stream name: _svc-<target> (dashes, not dots — validate_name constraint)
        let stream_name = format!("_svc-{}", target);
        let stream_name = stream_name.as_bytes();

        // Get stream info — this will fail if the stream doesn't exist
        let resp = self.client.get_stream(&stream_name).await?;

        // Parse stream_id from the response (JSON: {"stream_id": N, ...})
        let stream_id = parse_stream_id_from_response(&resp)?;

        self.stream_cache
            .lock()
            .unwrap()
            .insert(target.to_string(), stream_id);
        Ok(stream_id)
    }

    /// Stop all handlers and clean up.
    pub fn close(&self) {
        self.cancel.cancel();
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Handle to a registered service handler. Dropping it stops the handler.
#[derive(Debug)]
pub struct ServiceHandle {
    cancel: CancellationToken,
}

impl ServiceHandle {
    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

impl Drop for ServiceHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

// ── ServiceBuilder ───────────────────────────────────────────────────────────

/// Builder for creating a Service.
#[derive(Debug)]
pub struct ServiceBuilder {
    client: Client,
    name: String,
    max_inflight: u32,
}

impl ServiceBuilder {
    pub(crate) fn new(client: Client, name: &str) -> Self {
        Self {
            client,
            name: name.to_string(),
            max_inflight: 1024,
        }
    }

    /// Set the max inflight messages for the service consumer.
    pub fn max_inflight(mut self, n: u32) -> Self {
        self.max_inflight = n;
        self
    }

    /// Build the service: creates stream + consumer, starts dispatch loop.
    pub async fn build(self) -> Result<Service, ClientError> {
        // Names use dashes (validate_name allows [a-zA-Z0-9_-], no dots).
        // Subjects/filters use dots (validate_subject allows dots).
        let stream_name_str = format!("_svc-{}", self.name);
        let stream_name = stream_name_str.as_bytes();

        // Build filter: _svc.<name>.>
        let mut filter = Vec::with_capacity(SVC_PREFIX.len() + self.name.len() + 2);
        filter.extend_from_slice(SVC_PREFIX);
        filter.extend_from_slice(self.name.as_bytes());
        filter.extend_from_slice(b".>");

        // Create stream (idempotent — server returns existing if already created)
        let resp = self.client.create_stream(
            stream_name,
            &filter,
            0,    // max_msgs (unlimited)
            0,    // max_bytes (unlimited)
            3600, // max_age_secs (1 hour default for RPC)
            1,    // replicas
            0,    // journal_kind (tolerant)
            0,    // retention (limits)
            0,    // discard (old)
            0,    // idempotency_window_ms (disabled for RPC streams)
        ).await?;

        let stream_id = parse_stream_id_from_response(&resp)?;

        // Create consumer with group for load balancing
        let consumer_name = format!("_svc-{}-worker", self.name);
        let consumer_resp = self.client.create_consumer(
            stream_id,
            consumer_name.as_bytes(),
            consumer_name.as_bytes(), // group = same as name (load balanced)
            &filter,
            self.max_inflight as u16,
            1, // ack_policy: explicit
            0, // deliver_policy: all
            0, // deliver_mode: push
            30_000, // ack_wait_ms
            0,      // start_seq
        ).await?;

        let consumer_id = parse_consumer_id_from_response(&consumer_resp)?;

        // Subscribe
        let mut sub = self.client.subscribe(stream_id, consumer_id, &filter).await?;

        let cancel = CancellationToken::new();
        let reply_mux = Arc::new(ReplyMux::new());
        let reply_mux_clone = Arc::clone(&reply_mux);
        let name_clone = self.name.clone();
        let cancel_clone = cancel.clone();
        let handlers: Arc<Mutex<Vec<(Vec<u8>, BoxHandler)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let handlers_clone = Arc::clone(&handlers);

        // Spawn dispatch loop
        tokio::spawn(async move {
            let reply_prefix = format!("_svc.{}._r.", name_clone);
            let reply_prefix_bytes = reply_prefix.as_bytes();

            loop {
                tokio::select! {
                    biased;
                    _ = cancel_clone.cancelled() => break,
                    msg = sub.recv() => {
                        let Some(msg) = msg else { break };

                        let subject = msg.subject().to_vec();

                        // Check if this is a reply (correlation routing)
                        if let Some(corr_start) = find_subsequence(&subject, reply_prefix_bytes) {
                            let corr_bytes = &subject[corr_start + reply_prefix_bytes.len()..];
                            if let Ok(corr_str) = std::str::from_utf8(corr_bytes) {
                                if let Ok(corr_id) = corr_str.parse::<u64>() {
                                    reply_mux_clone.complete(corr_id, msg.payload());
                                    msg.ack();
                                    continue;
                                }
                            }
                        }

                        // Route to registered handler
                        let handler = {
                            let locked = handlers_clone.lock().unwrap();
                            locked.iter()
                                .find(|(prefix, _)| subject.starts_with(prefix))
                                .map(|(_, h)| Arc::clone(h))
                        };

                        if let Some(h) = handler {
                            tokio::spawn(h(msg));
                        } else {
                            msg.nack();
                        }
                    }
                }
            }
        });

        let mut stream_cache = HashMap::new();
        stream_cache.insert(self.name.clone(), stream_id);

        Ok(Service {
            name: self.name,
            stream_id,
            consumer_id,
            client: self.client,
            reply_mux,
            stream_cache: Arc::new(Mutex::new(stream_cache)),
            handlers,
            cancel,
        })
    }
}

// ── Client extension ─────────────────────────────────────────────────────────

impl Client {
    /// Create a service builder. Call `.build().await` to finalize.
    ///
    /// A `Service` provides both request and handler capabilities:
    /// - `svc.request("target", method, payload, timeout)` — RPC call
    /// - `svc.handle(method, handler)` — register a request handler
    /// - `svc.send("target", method, payload)` — fire-and-forget
    pub fn service(&self, name: &str) -> ServiceBuilder {
        ServiceBuilder::new(self.clone(), name)
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

/// Parse a stream_id or consumer_id from a broker RepOk response.
/// The first 8 bytes are a u64 LE representing the assigned ID.
fn parse_id_from_response(resp: &[u8]) -> Result<u32, ClientError> {
    if resp.len() < 8 {
        return Err(ClientError::InvalidConfig(
            "response too short to contain id".into(),
        ));
    }
    let id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
    Ok(id)
}

#[inline]
fn parse_stream_id_from_response(resp: &[u8]) -> Result<u32, ClientError> {
    parse_id_from_response(resp)
}

#[inline]
fn parse_consumer_id_from_response(resp: &[u8]) -> Result<u32, ClientError> {
    parse_id_from_response(resp)
}
