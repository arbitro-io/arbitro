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
use crate::consume::message::encode_reply_to;
use crate::error::ClientError;

/// Error returned by a service handler.
///
/// Any error type that implements `std::error::Error + Send + Sync` can be
/// converted with `?`. The dispatcher nacks the incoming message and does
/// not send a reply.
pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;

/// Result returned by a service handler.
///
/// - `Ok(bytes)` — dispatcher publishes `bytes` to the requester (if a
///   reply address is present) and acks the message. An empty `Vec` acks
///   without replying.
/// - `Err(_)` — dispatcher nacks the message for redelivery.
pub type HandlerResult = Result<Vec<u8>, HandlerError>;

/// Incoming service request.
///
/// A read-only view over the delivered message. Ack/nack/reply are managed
/// by the framework based on the handler's returned [`HandlerResult`], so
/// this type intentionally does not expose them.
#[derive(Debug)]
pub struct Request {
    subject: Box<[u8]>,
    payload: Bytes,
    has_reply: bool,
    seq: u64,
    consumer_id: u32,
}

impl Request {
    /// Full subject the message was published to (e.g., `_svc.orders.charge`).
    #[inline]
    pub fn subject(&self) -> &[u8] {
        &self.subject
    }

    /// Method segment after the service prefix (e.g., `charge`).
    ///
    /// Returns `None` if the subject is malformed. Uses the prefix
    /// `_svc.<service-name>.m.` to locate the split (methods live under
    /// `.m.` to keep them separable from replies).
    pub fn method(&self, service_name: &str) -> Option<&[u8]> {
        let prefix_len = SVC_PREFIX.len() + service_name.len() + METHOD_INFIX.len();
        (self.subject.len() > prefix_len).then(|| &self.subject[prefix_len..])
    }

    /// Payload bytes (zero-copy `Bytes` handle).
    #[inline]
    pub fn payload(&self) -> Bytes {
        self.payload.clone()
    }

    /// Payload bytes as a slice.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.payload
    }

    /// `true` if the request has a reply address — the requester is
    /// waiting for a response. `false` for fire-and-forget sends.
    #[inline]
    pub fn has_reply(&self) -> bool {
        self.has_reply
    }

    /// Delivery sequence number assigned by the broker.
    #[inline]
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Consumer that received this request.
    #[inline]
    pub fn consumer_id(&self) -> u32 {
        self.consumer_id
    }
}

/// Prefix for all service streams.
const SVC_PREFIX: &[u8] = b"_svc.";
/// Separator marking the method segment: `_svc.<name>.m.<method>`.
///
/// The `.m.` insert exists so the worker consumer can filter
/// `_svc.<name>.m.>` and match ONLY method calls, never replies (which
/// live under `_svc.<name>._r.<instance_id>.<corr_id>`). Without this
/// separation the worker's queue group would load-balance replies away
/// from the instance that issued the original request. See BUG-2.
const METHOD_INFIX: &[u8] = b".m.";
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

/// Type-erased handler: takes a Request and returns a pinned future
/// resolving to a HandlerResult.
type BoxHandler = Arc<
    dyn Fn(Request) -> std::pin::Pin<Box<dyn std::future::Future<Output = HandlerResult> + Send>>
        + Send
        + Sync,
>;

type HandlerRegistry = Arc<Mutex<Vec<(Vec<u8>, BoxHandler)>>>;

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
    /// Process-unique random id embedded in every reply subject issued
    /// by `request()` and matched by the private reply consumer's
    /// filter, so replies always land on the originating instance
    /// even when N sibling instances share the queue-grouped worker
    /// consumer. See BUG-2.
    instance_id: u64,
    client: Client,
    reply_mux: Arc<ReplyMux>,
    /// Cache of resolved service name → stream_id.
    stream_cache: Arc<Mutex<HashMap<String, u32>>>,
    /// Registered handlers: subject prefix → handler fn.
    handlers: HandlerRegistry,
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

        // Build subject: _svc.<target>.m.<method>
        let mut subject =
            Vec::with_capacity(SVC_PREFIX.len() + target.len() + METHOD_INFIX.len() + method.len());
        subject.extend_from_slice(SVC_PREFIX);
        subject.extend_from_slice(target.as_bytes());
        subject.extend_from_slice(METHOD_INFIX);
        subject.extend_from_slice(method);

        // Build reply_to: encoded(self.stream_id,
        // _svc.<self.name>._r.<instance_id>.<corr_id>). The instance_id
        // is what routes the reply to the private per-instance reply
        // consumer instead of the queue-grouped worker consumer.
        let instance_str = self.instance_id.to_string();
        let corr_str = corr_id.to_string();
        let mut reply_subject = Vec::with_capacity(
            SVC_PREFIX.len()
                + self.name.len()
                + REPLY_INFIX.len()
                + instance_str.len()
                + 1
                + corr_str.len(),
        );
        reply_subject.extend_from_slice(SVC_PREFIX);
        reply_subject.extend_from_slice(self.name.as_bytes());
        reply_subject.extend_from_slice(REPLY_INFIX);
        reply_subject.extend_from_slice(instance_str.as_bytes());
        reply_subject.push(b'.');
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

        let mut subject =
            Vec::with_capacity(SVC_PREFIX.len() + target.len() + METHOD_INFIX.len() + method.len());
        subject.extend_from_slice(SVC_PREFIX);
        subject.extend_from_slice(target.as_bytes());
        subject.extend_from_slice(METHOD_INFIX);
        subject.extend_from_slice(method);

        self.client.publish(target_stream_id, &subject, payload)
    }

    /// Register a handler for incoming requests matching `method`.
    ///
    /// The handler receives a [`Request`] and returns a [`HandlerResult`]:
    ///
    /// - `Ok(bytes)` — the dispatcher publishes `bytes` back to the requester
    ///   (if the request carried a reply address) and acks the delivery.
    ///   Return `Ok(vec![])` to ack without replying.
    /// - `Err(_)` — the dispatcher nacks the delivery for redelivery.
    ///
    /// The handler never touches ack/nack/reply itself; the framework
    /// guarantees exactly one ack or nack per invocation.
    ///
    /// Returns a [`ServiceHandle`] that can be used to stop the handler.
    pub fn handle<F, Fut>(&self, method: &[u8], handler: F) -> ServiceHandle
    where
        F: Fn(Request) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = HandlerResult> + Send + 'static,
    {
        let cancel = self.cancel.child_token();

        // Build the subject prefix this handler matches:
        // _svc.<name>.m.<method>
        let mut match_prefix = Vec::with_capacity(
            SVC_PREFIX.len() + self.name.len() + METHOD_INFIX.len() + method.len(),
        );
        match_prefix.extend_from_slice(SVC_PREFIX);
        match_prefix.extend_from_slice(self.name.as_bytes());
        match_prefix.extend_from_slice(METHOD_INFIX);
        match_prefix.extend_from_slice(method);

        let boxed: BoxHandler = Arc::new(move |req| Box::pin(handler(req)));
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
        let stream_name = format!("_svc-{target}");
        let stream_name = stream_name.as_bytes();

        // Get stream info — this will fail if the stream doesn't exist
        let resp = self.client.get_stream(stream_name).await?;

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

    /// Build the service: creates stream, two consumers (queue-grouped
    /// worker for methods + per-instance private consumer for replies),
    /// starts the dispatch loop that select!s over both.
    pub async fn build(self) -> Result<Service, ClientError> {
        // Names use dashes (validate_name allows [a-zA-Z0-9_-], no dots).
        // Subjects/filters use dots (validate_subject allows dots).
        let stream_name_str = format!("_svc-{}", self.name);
        let stream_name = stream_name_str.as_bytes();

        // Process-unique instance id. Composed of the client's next
        // seq counter mixed with a compile-time random-ish constant
        // — good enough uniqueness within a single process. Used to
        // scope the reply consumer's subject filter.
        let instance_id: u64 = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(1);
            NEXT.fetch_add(1, Ordering::Relaxed)
        };
        let instance_str = instance_id.to_string();

        // Stream-scope filter (covers both methods and replies for
        // stream creation).
        let mut stream_filter = Vec::with_capacity(SVC_PREFIX.len() + self.name.len() + 2);
        stream_filter.extend_from_slice(SVC_PREFIX);
        stream_filter.extend_from_slice(self.name.as_bytes());
        stream_filter.extend_from_slice(b".>");

        // Worker filter: `_svc.<name>.m.>` — matches ONLY method calls.
        let mut worker_filter =
            Vec::with_capacity(SVC_PREFIX.len() + self.name.len() + METHOD_INFIX.len() + 1);
        worker_filter.extend_from_slice(SVC_PREFIX);
        worker_filter.extend_from_slice(self.name.as_bytes());
        worker_filter.extend_from_slice(METHOD_INFIX);
        worker_filter.push(b'>');

        // Reply filter: `_svc.<name>._r.<instance_id>.>` — matches ONLY
        // replies intended for THIS instance. Since the reply
        // consumer is not queue-grouped, no sibling instance can
        // steal these deliveries.
        let mut reply_filter = Vec::with_capacity(
            SVC_PREFIX.len() + self.name.len() + REPLY_INFIX.len() + instance_str.len() + 2,
        );
        reply_filter.extend_from_slice(SVC_PREFIX);
        reply_filter.extend_from_slice(self.name.as_bytes());
        reply_filter.extend_from_slice(REPLY_INFIX);
        reply_filter.extend_from_slice(instance_str.as_bytes());
        reply_filter.extend_from_slice(b".>");

        // Create stream (idempotent — server returns existing if already created)
        let resp = self
            .client
            .create_stream(
                stream_name,
                &stream_filter,
                0,    // max_msgs (unlimited)
                0,    // max_bytes (unlimited)
                3600, // max_age_secs (1 hour default for RPC)
                1,    // replicas
                0,    // journal_kind (tolerant)
                0,    // retention (limits)
                0,    // discard (old)
                0,    // idempotency_window_ms (disabled for RPC streams)
            )
            .await?;

        let stream_id = parse_stream_id_from_response(&resp)?;

        // Worker consumer: queue-grouped so N instances share request load.
        let worker_consumer_name = format!("_svc-{}-worker", self.name);
        let worker_resp = self
            .client
            .create_consumer(
                stream_id,
                worker_consumer_name.as_bytes(),
                worker_consumer_name.as_bytes(), // group = same name (load balanced)
                &worker_filter,
                self.max_inflight as u16,
                1,      // ack_policy: explicit
                0,      // deliver_policy: all
                0,      // deliver_mode: push
                30_000, // ack_wait_ms
                0,      // start_seq
            )
            .await?;
        let consumer_id = parse_consumer_id_from_response(&worker_resp)?;
        let mut worker_sub = self
            .client
            .subscribe(stream_id, consumer_id, &worker_filter)
            .await?;

        // Reply consumer: private per-instance, NOT queue-grouped so
        // replies are never load-balanced away. Fixes BUG-2.
        let reply_consumer_name = format!("_svc-{}-reply-{}", self.name, instance_str);
        let reply_resp = self
            .client
            .create_consumer(
                stream_id,
                reply_consumer_name.as_bytes(),
                b"", // no group — solo delivery
                &reply_filter,
                self.max_inflight as u16,
                1,      // ack_policy: explicit
                0,      // deliver_policy: all
                0,      // deliver_mode: push
                30_000, // ack_wait_ms
                0,      // start_seq
            )
            .await?;
        let reply_consumer_id = parse_consumer_id_from_response(&reply_resp)?;
        let mut reply_sub = self
            .client
            .subscribe(stream_id, reply_consumer_id, &reply_filter)
            .await?;

        let cancel = CancellationToken::new();
        let reply_mux = Arc::new(ReplyMux::new());
        let reply_mux_clone = Arc::clone(&reply_mux);
        let name_clone = self.name.clone();
        let cancel_clone = cancel.clone();
        let handlers: HandlerRegistry = Arc::new(Mutex::new(Vec::new()));
        let handlers_clone: HandlerRegistry = handlers.clone();
        let instance_str_clone = instance_str.clone();

        // Dispatch loop — select! over BOTH consumers.
        // Worker sub delivers only methods; reply sub delivers only
        // replies for THIS instance. That separation is what fixes
        // the multi-instance reply routing bug.
        tokio::spawn(async move {
            let reply_prefix = format!("_svc.{name_clone}._r.{instance_str_clone}.");
            let reply_prefix_bytes = reply_prefix.as_bytes();

            loop {
                tokio::select! {
                    biased;
                    _ = cancel_clone.cancelled() => break,
                    msg = reply_sub.recv() => {
                        let Some(msg) = msg else { break };
                        let subject = msg.subject();
                        if subject.starts_with(reply_prefix_bytes) {
                            let corr_bytes = &subject[reply_prefix_bytes.len()..];
                            if let Ok(corr_str) = std::str::from_utf8(corr_bytes) {
                                if let Ok(corr_id) = corr_str.parse::<u64>() {
                                    reply_mux_clone.complete(corr_id, msg.payload());
                                }
                            }
                        }
                        // Ack unconditionally — replies never need
                        // redelivery. A malformed subject is a
                        // logic bug, not a transient failure.
                        msg.ack();
                    }
                    msg = worker_sub.recv() => {
                        let Some(msg) = msg else { break };

                        let subject = msg.subject().to_vec();

                        // Route to registered handler by subject prefix.
                        let handler = {
                            let locked = handlers_clone.lock().unwrap();
                            locked.iter()
                                .find(|(prefix, _)| subject.starts_with(prefix))
                                .map(|(_, h)| Arc::clone(h))
                        };

                        if let Some(h) = handler {
                            let req = Request {
                                subject: subject.into_boxed_slice(),
                                payload: msg.payload(),
                                has_reply: msg.has_reply_to(),
                                seq: msg.seq,
                                consumer_id: msg.consumer_id,
                            };
                            tokio::spawn(async move {
                                match h(req).await {
                                    Ok(response) => {
                                        if msg.has_reply_to() && !response.is_empty() {
                                            let _ = msg.reply(&response);
                                        }
                                        msg.ack();
                                    }
                                    Err(_) => {
                                        msg.nack();
                                    }
                                }
                            });
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
            instance_id,
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
