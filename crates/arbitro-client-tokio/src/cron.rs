//! Cron scheduling — `client.cron("name").every("...").run(handler)`.
//!
//! A `CronBuilder` registers a cron job on the broker and binds a local
//! async callback. When the broker fires the job, the client dispatches
//! to the registered handler and sends a `CronAck` on completion.
//!
//! Multiple clients can register the same cron name — the broker picks
//! one per fire (queue semantics). On reconnect, the client
//! re-registers all active crons automatically.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use arbitro_proto::wire::cron::{
    CreateCronBody, decode_cron_fire, encode_cron_ack, encode_create_cron, encode_delete_cron,
};

use crate::error::ClientError;
use crate::state::Inner;

// ── CronContext ─────────────────────────────────────────────────────────────

/// Context passed to the cron handler callback on each fire.
#[derive(Debug, Clone)]
pub struct CronContext {
    /// Cron job name.
    pub name: String,
    /// UTC timestamp (ms since epoch) when the broker intended this fire.
    pub fire_time_ms: u64,
    /// Monotonic fire counter (1-based).
    pub fire_count: u64,
}

// ── Handler type ────────────────────────────────────────────────────────────

/// Type-erased async cron handler.
pub(crate) type CronHandler = Arc<
    dyn Fn(CronContext) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
>;

// ── CronState ───────────────────────────────────────────────────────────────

/// Shared state for all active cron registrations on this client.
pub struct CronState {
    handlers: Mutex<HashMap<Bytes, CronEntry>>,
}

impl std::fmt::Debug for CronState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CronState").finish()
    }
}

struct CronEntry {
    handler: CronHandler,
    config: CreateCronBody,
}

impl CronState {
    pub(crate) fn new() -> Self {
        Self {
            handlers: Mutex::new(HashMap::new()),
        }
    }

    /// Register a handler for a cron name.
    pub(crate) fn register(&self, name: Bytes, config: CreateCronBody, handler: CronHandler) {
        self.handlers.lock().unwrap().insert(name, CronEntry { handler, config });
    }

    /// Remove a cron handler.
    pub(crate) fn remove(&self, name: &[u8]) {
        self.handlers.lock().unwrap().remove(name);
    }

    /// Look up a handler by name (returns a clone of the Arc).
    pub(crate) fn get_handler(&self, name: &[u8]) -> Option<CronHandler> {
        self.handlers.lock().unwrap().get(name).map(|e| e.handler.clone())
    }

    /// Get all registered cron configs (for re-registration on reconnect).
    pub(crate) fn all_configs(&self) -> Vec<(Bytes, CreateCronBody)> {
        let guard = self.handlers.lock().unwrap();
        guard.iter().map(|(k, v)| (k.clone(), v.config.clone())).collect()
    }
}

// ── CronBuilder ─────────────────────────────────────────────────────────────

/// Fluent builder for cron job registration.
///
/// ```rust,no_run
/// # use arbitro_client_tokio::Client;
/// # async fn example(client: &Client) {
/// let cron = client.cron(b"billing")
///     .every(b"0 0 1 * *")
///     .tz(b"America/New_York")
///     .run(|ctx| async move {
///         println!("fire #{}", ctx.fire_count);
///     })
///     .await
///     .unwrap();
///
/// // Later:
/// cron.stop().await.unwrap();
/// # }
/// ```
#[derive(Debug)]
pub struct CronBuilder<'a> {
    client: &'a crate::client::Client,
    name: Bytes,
    every: Option<String>,
    tz: Option<String>,
    timeout_ms: u32,
    overlap: bool,
}

impl<'a> CronBuilder<'a> {
    pub(crate) fn new(client: &'a crate::client::Client, name: &[u8]) -> Self {
        Self {
            client,
            name: Bytes::copy_from_slice(name),
            every: None,
            tz: None,
            timeout_ms: 30_000, // 30s default
            overlap: false,
        }
    }

    /// Set the cron expression (required).
    pub fn every(mut self, expr: &[u8]) -> Self {
        self.every = Some(String::from_utf8_lossy(expr).to_string());
        self
    }

    /// Set the timezone (optional, defaults to UTC).
    pub fn tz(mut self, timezone: &[u8]) -> Self {
        self.tz = Some(String::from_utf8_lossy(timezone).to_string());
        self
    }

    /// Set handler timeout in milliseconds (default: 30000).
    pub fn timeout_ms(mut self, ms: u32) -> Self {
        self.timeout_ms = ms;
        self
    }

    /// Allow overlapping fires (default: false).
    pub fn overlap(mut self, allow: bool) -> Self {
        self.overlap = allow;
        self
    }

    /// Register the cron and bind the handler. Returns a handle to control
    /// the cron lifecycle.
    pub async fn run<F, Fut>(self, handler: F) -> Result<CronHandle, ClientError>
    where
        F: Fn(CronContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let every = self.every.ok_or(ClientError::Disconnected)?;

        let body = CreateCronBody {
            name: String::from_utf8_lossy(&self.name).to_string(),
            every,
            tz: self.tz,
            timeout_ms: self.timeout_ms,
            overlap: self.overlap,
        };

        // Send CreateCron to broker and await reply
        let seq = self.client.inner.seq_alloc.next();
        let frame = encode_create_cron(seq, &body);

        let rx = self.client.inner.pending.register(seq);
        crate::publish::enqueue(self.client.producer(), crate::transport::frame::WriteFrame::Mono(frame))?;
        rx.recv_async().await
            .map_err(|_| ClientError::ChannelClosed)
            .and_then(|r| r)?;

        // Register handler locally
        let handler_arc: CronHandler = Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(fut)
        });

        self.client
            .inner
            .cron_state
            .register(self.name.clone(), body, handler_arc);

        Ok(CronHandle {
            inner: Arc::clone(&self.client.inner),
            name: self.name,
        })
    }
}

// ── CronHandle ──────────────────────────────────────────────────────────────

/// Handle to a registered cron job. Use `.stop()` to unregister.
#[derive(Debug)]
pub struct CronHandle {
    inner: Arc<Inner>,
    name: Bytes,
}

impl CronHandle {
    /// Unregister this cron job from the broker and remove the local handler.
    pub async fn stop(&self) -> Result<(), ClientError> {
        let seq = self.inner.seq_alloc.next();
        let frame = encode_delete_cron(seq, &self.name);

        let rx = self.inner.pending.register(seq);
        {
            let admin = self.inner.admin_producer.lock().unwrap();
            let _ = admin.try_send(crate::transport::frame::WriteFrame::Mono(frame));
        }
        let _ = rx.recv_async().await;

        // Remove local handler
        self.inner.cron_state.remove(&self.name);
        Ok(())
    }
}

// ── Dispatch (called from reader.rs) ────────────────────────────────────────

/// Handle an incoming CronFire frame: look up handler, execute, send CronAck.
pub(crate) async fn dispatch_cron_fire(frame: Bytes, inner: &Inner) {
    use arbitro_proto::v2::header::HEADER_SIZE;

    let body = &frame[HEADER_SIZE..];
    let view = match decode_cron_fire(body) {
        Some(v) => v,
        None => return,
    };

    let name_bytes = Bytes::copy_from_slice(view.name);
    let ctx = CronContext {
        name: String::from_utf8_lossy(view.name).to_string(),
        fire_time_ms: view.fire_time_ms,
        fire_count: view.fire_count,
    };

    let handler = match inner.cron_state.get_handler(view.name) {
        Some(h) => h,
        None => {
            // No handler — send error ack
            send_cron_ack(inner, &name_bytes, false);
            return;
        }
    };

    // Execute handler
    let ok = tokio::spawn(async move { handler(ctx).await })
        .await
        .is_ok();

    send_cron_ack(inner, &name_bytes, ok);
}

fn send_cron_ack(inner: &Inner, name: &[u8], ok: bool) {
    let seq = inner.seq_alloc.next();
    let frame = encode_cron_ack(seq, name, ok);
    let wf = crate::transport::frame::WriteFrame::Mono(frame);
    if let Ok(admin) = inner.admin_producer.lock() {
        let _ = admin.try_send(wf);
    }
}

// ── Reconnect replay ───────────────────────────────────────────────────────

/// Re-register all active crons after a reconnect.
pub(crate) fn replay_crons(inner: &Inner) {
    let configs = inner.cron_state.all_configs();
    for (_name, body) in configs {
        let seq = inner.seq_alloc.next();
        let frame = encode_create_cron(seq, &body);
        let wf = crate::transport::frame::WriteFrame::Mono(frame);
        if let Ok(admin) = inner.admin_producer.lock() {
            let _ = admin.try_send(wf);
        }
    }
}
