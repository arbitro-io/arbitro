//! Workflow orchestration — `client.workflow("name").trigger("subject").step("step", handler).start()`.
//!
//! A `WorkflowBuilder` registers a workflow on the broker and binds local
//! async callbacks for each step and an optional onError handler. When
//! the broker fires a step, the client dispatches to the registered handler
//! and sends a `WorkflowResult` on completion.
//!
//! Multiple clients can register the same workflow name — the broker picks
//! one per step (queue semantics). On reconnect, the client re-registers
//! all active workflows automatically.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use arbitro_proto::wire::workflow::{
    decode_workflow_error, decode_workflow_step, encode_create_workflow, encode_delete_workflow,
    encode_workflow_result, CreateWorkflowBody, StepDef, WorkflowConfig,
};

use crate::error::ClientError;
use crate::state::Inner;

// ── WorkflowStepContext ───────────────────────────────────────────────────

/// Context passed to a workflow step handler callback.
#[derive(Debug, Clone)]
pub struct WorkflowStepContext {
    /// Workflow name.
    pub name: String,
    /// Instance ID assigned by the broker.
    pub instance_id: u32,
    /// Step index (0-based).
    pub step_index: u16,
    /// Context bytes from the previous step (or initial trigger payload).
    pub context: Vec<u8>,
}

/// Result returned by a step handler — contains the updated context.
#[derive(Debug, Clone)]
pub struct StepResult {
    /// Updated context to pass to the next step.
    pub context: Vec<u8>,
}

// ── WorkflowErrorContext ──────────────────────────────────────────────────

/// Context passed to the onError handler.
#[derive(Debug, Clone)]
pub struct WorkflowErrorContext {
    /// Workflow name.
    pub name: String,
    /// Instance ID.
    pub instance_id: u32,
    /// Error JSON from the broker.
    pub error_json: Vec<u8>,
}

// ── Handler types ─────────────────────────────────────────────────────────

/// Type-erased async workflow step handler.
pub(crate) type StepHandler = Arc<
    dyn Fn(WorkflowStepContext) -> Pin<Box<dyn Future<Output = Result<StepResult, String>> + Send>>
        + Send
        + Sync,
>;

/// Type-erased async workflow error handler.
pub(crate) type ErrorHandler =
    Arc<dyn Fn(WorkflowErrorContext) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

// ── WorkflowState ─────────────────────────────────────────────────────────

/// Shared state for all active workflow registrations on this client.
pub struct WorkflowState {
    handlers: Mutex<HashMap<Bytes, WorkflowEntry>>,
}

impl std::fmt::Debug for WorkflowState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowState").finish()
    }
}

struct WorkflowEntry {
    /// Step handlers keyed by step name.
    step_handlers: HashMap<String, StepHandler>,
    /// Optional onError handler.
    error_handler: Option<ErrorHandler>,
    /// Config for re-registration on reconnect.
    config: CreateWorkflowBody,
}

impl WorkflowState {
    pub(crate) fn new() -> Self {
        Self {
            handlers: Mutex::new(HashMap::new()),
        }
    }

    /// Register all handlers for a workflow.
    pub(crate) fn register(
        &self,
        name: Bytes,
        config: CreateWorkflowBody,
        step_handlers: HashMap<String, StepHandler>,
        error_handler: Option<ErrorHandler>,
    ) {
        self.handlers.lock().unwrap().insert(
            name,
            WorkflowEntry {
                step_handlers,
                error_handler,
                config,
            },
        );
    }

    /// Remove a workflow handler.
    pub(crate) fn remove(&self, name: &[u8]) {
        self.handlers.lock().unwrap().remove(name);
    }

    /// Look up a step handler by workflow name and step name.
    pub(crate) fn get_step_handler(
        &self,
        workflow_name: &[u8],
        step_name: &str,
    ) -> Option<StepHandler> {
        self.handlers
            .lock()
            .unwrap()
            .get(workflow_name)
            .and_then(|e| e.step_handlers.get(step_name).cloned())
    }

    /// Look up the error handler by workflow name.
    pub(crate) fn get_error_handler(&self, workflow_name: &[u8]) -> Option<ErrorHandler> {
        self.handlers
            .lock()
            .unwrap()
            .get(workflow_name)
            .and_then(|e| e.error_handler.clone())
    }

    /// Get step names by index for a given workflow.
    pub(crate) fn get_step_name(&self, workflow_name: &[u8], step_index: u16) -> Option<String> {
        self.handlers
            .lock()
            .unwrap()
            .get(workflow_name)
            .and_then(|e| {
                e.config
                    .steps
                    .get(step_index as usize)
                    .map(|s| s.name.clone())
            })
    }

    /// Get all registered workflow configs (for re-registration on reconnect).
    pub(crate) fn all_configs(&self) -> Vec<(Bytes, CreateWorkflowBody)> {
        let guard = self.handlers.lock().unwrap();
        guard
            .iter()
            .map(|(k, v)| (k.clone(), v.config.clone()))
            .collect()
    }
}

// ── WorkflowBuilder ───────────────────────────────────────────────────────

/// Fluent builder for workflow registration.
///
/// ```rust,no_run
/// # use arbitro_client_tokio::Client;
/// # async fn example(client: &Client) {
/// let wf = client.workflow(b"order-process")
///     .trigger(b"orders.created")
///     .step(b"validate", |ctx| async move {
///         Ok(arbitro_client_tokio::workflow::StepResult { context: ctx.context })
///     })
///     .step(b"process", |ctx| async move {
///         Ok(arbitro_client_tokio::workflow::StepResult { context: ctx.context })
///     })
///     .start()
///     .await
///     .unwrap();
///
/// // Later:
/// wf.stop().await.unwrap();
/// # }
/// ```
pub struct WorkflowBuilder<'a> {
    client: &'a crate::client::Client,
    name: Bytes,
    trigger_subject: Option<String>,
    steps: Vec<StepDef>,
    step_handlers: Vec<(String, StepHandler)>,
    error_handler: Option<ErrorHandler>,
    max_concurrent: u32,
    dedup_key: Option<String>,
    timeout_ms: u32,
}

impl std::fmt::Debug for WorkflowBuilder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowBuilder")
            .field("name", &self.name)
            .field("trigger_subject", &self.trigger_subject)
            .field("steps", &self.steps)
            .field("max_concurrent", &self.max_concurrent)
            .field("dedup_key", &self.dedup_key)
            .field("timeout_ms", &self.timeout_ms)
            .finish()
    }
}

impl<'a> WorkflowBuilder<'a> {
    pub(crate) fn new(client: &'a crate::client::Client, name: &[u8]) -> Self {
        Self {
            client,
            name: Bytes::copy_from_slice(name),
            trigger_subject: None,
            steps: Vec::new(),
            step_handlers: Vec::new(),
            error_handler: None,
            max_concurrent: 0,
            dedup_key: None,
            timeout_ms: 0,
        }
    }

    /// Set the trigger subject (required).
    pub fn trigger(mut self, subject: &[u8]) -> Self {
        self.trigger_subject = Some(String::from_utf8_lossy(subject).to_string());
        self
    }

    /// Add a step with a handler.
    pub fn step<F, Fut>(mut self, name: &[u8], handler: F) -> Self
    where
        F: Fn(WorkflowStepContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<StepResult, String>> + Send + 'static,
    {
        let step_name = String::from_utf8_lossy(name).to_string();
        self.steps.push(StepDef {
            name: step_name.clone(),
            timeout_ms: 30_000,
            max_retries: 0,
        });
        let handler_arc: StepHandler = Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(fut)
        });
        self.step_handlers.push((step_name, handler_arc));
        self
    }

    /// Add a step with a handler and custom timeout/retries.
    pub fn step_with_config<F, Fut>(
        mut self,
        name: &[u8],
        timeout_ms: u32,
        max_retries: u8,
        handler: F,
    ) -> Self
    where
        F: Fn(WorkflowStepContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<StepResult, String>> + Send + 'static,
    {
        let step_name = String::from_utf8_lossy(name).to_string();
        self.steps.push(StepDef {
            name: step_name.clone(),
            timeout_ms,
            max_retries,
        });
        let handler_arc: StepHandler = Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(fut)
        });
        self.step_handlers.push((step_name, handler_arc));
        self
    }

    /// Set the onError handler (optional).
    pub fn on_error<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(WorkflowErrorContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler_arc: ErrorHandler = Arc::new(move |ctx| {
            let fut = handler(ctx);
            Box::pin(fut)
        });
        self.error_handler = Some(handler_arc);
        self
    }

    /// Set max concurrent instances (default: 0 = unlimited).
    pub fn max_concurrent(mut self, n: u32) -> Self {
        self.max_concurrent = n;
        self
    }

    /// Set dedup key (JSON field name for deduplication).
    pub fn dedup_key(mut self, key: &[u8]) -> Self {
        self.dedup_key = Some(String::from_utf8_lossy(key).to_string());
        self
    }

    /// Register the workflow and bind the handlers. Returns a handle.
    pub async fn start(self) -> Result<WorkflowHandle, ClientError> {
        let trigger = self.trigger_subject.ok_or(ClientError::Disconnected)?;
        if self.steps.is_empty() {
            return Err(ClientError::Disconnected);
        }

        let body = CreateWorkflowBody {
            name: String::from_utf8_lossy(&self.name).to_string(),
            trigger,
            steps: self.steps,
            config: WorkflowConfig {
                max_concurrent: self.max_concurrent,
                dedup_key: self.dedup_key,
                timeout_ms: self.timeout_ms,
            },
        };

        // Send CreateWorkflow to broker and await reply.
        let seq = self.client.inner.seq_alloc.next();
        let frame = encode_create_workflow(seq, &body);

        let rx = self.client.inner.pending.register(seq);
        crate::publish::enqueue(
            self.client.producer(),
            crate::transport::frame::WriteFrame::Mono(frame),
        )?;
        rx.recv_async()
            .await
            .map_err(|_| ClientError::ChannelClosed)
            .and_then(|r| r)?;

        // Register handlers locally.
        let step_map: HashMap<String, StepHandler> = self.step_handlers.into_iter().collect();

        self.client.inner.workflow_state.register(
            self.name.clone(),
            body,
            step_map,
            self.error_handler,
        );

        Ok(WorkflowHandle {
            inner: Arc::clone(&self.client.inner),
            name: self.name,
        })
    }
}

// ── WorkflowHandle ────────────────────────────────────────────────────────

/// Handle to a registered workflow. Use `.stop()` to unregister.
#[derive(Debug)]
pub struct WorkflowHandle {
    inner: Arc<Inner>,
    name: Bytes,
}

impl WorkflowHandle {
    /// Unregister this workflow from the broker and remove the local handlers.
    pub async fn stop(&self) -> Result<(), ClientError> {
        let seq = self.inner.seq_alloc.next();
        let frame = encode_delete_workflow(seq, &self.name);

        let rx = self.inner.pending.register(seq);
        {
            let admin = self.inner.admin_producer.lock().unwrap();
            let _ = admin.try_send(crate::transport::frame::WriteFrame::Mono(frame));
        }
        let _ = rx.recv_async().await;

        // Remove local handlers.
        self.inner.workflow_state.remove(&self.name);
        Ok(())
    }
}

// ── Dispatch (called from reader.rs) ──────────────────────────────────────

/// Handle an incoming WorkflowStep frame: look up handler, execute, send WorkflowResult.
pub(crate) async fn dispatch_workflow_step(frame: Bytes, inner: &Inner) {
    use arbitro_proto::v2::header::HEADER_SIZE;

    let body = &frame[HEADER_SIZE..];
    let view = match decode_workflow_step(body) {
        Some(v) => v,
        None => return,
    };

    let name_bytes = Bytes::copy_from_slice(view.name);

    // Look up the step name by index.
    let step_name = match inner
        .workflow_state
        .get_step_name(view.name, view.step_index)
    {
        Some(n) => n,
        None => {
            send_workflow_result(inner, &name_bytes, view.instance_id, false, &[]);
            return;
        }
    };

    let handler = match inner.workflow_state.get_step_handler(view.name, &step_name) {
        Some(h) => h,
        None => {
            send_workflow_result(inner, &name_bytes, view.instance_id, false, &[]);
            return;
        }
    };

    let ctx = WorkflowStepContext {
        name: String::from_utf8_lossy(view.name).to_string(),
        instance_id: view.instance_id,
        step_index: view.step_index,
        context: view.context.to_vec(),
    };

    // Execute handler.
    let result = tokio::spawn(async move { handler(ctx).await }).await;

    match result {
        Ok(Ok(step_result)) => {
            send_workflow_result(
                inner,
                &name_bytes,
                view.instance_id,
                true,
                &step_result.context,
            );
        }
        _ => {
            send_workflow_result(inner, &name_bytes, view.instance_id, false, &[]);
        }
    }
}

/// Handle an incoming WorkflowError frame: execute onError handler.
pub(crate) async fn dispatch_workflow_error(frame: Bytes, inner: &Inner) {
    use arbitro_proto::v2::header::HEADER_SIZE;

    let body = &frame[HEADER_SIZE..];
    let view = match decode_workflow_error(body) {
        Some(v) => v,
        None => return,
    };

    let handler = match inner.workflow_state.get_error_handler(view.name) {
        Some(h) => h,
        None => return,
    };

    let ctx = WorkflowErrorContext {
        name: String::from_utf8_lossy(view.name).to_string(),
        instance_id: view.instance_id,
        error_json: view.error_json.to_vec(),
    };

    let _ = tokio::spawn(async move { handler(ctx).await }).await;
}

fn send_workflow_result(inner: &Inner, name: &[u8], instance_id: u32, ok: bool, context: &[u8]) {
    let seq = inner.seq_alloc.next();
    let frame = encode_workflow_result(seq, name, instance_id, ok, context);
    let wf = crate::transport::frame::WriteFrame::Mono(frame);
    if let Ok(admin) = inner.admin_producer.lock() {
        let _ = admin.try_send(wf);
    }
}

// ── Reconnect replay ─────────────────────────────────────────────────────

/// Re-register all active workflows after a reconnect.
pub(crate) fn replay_workflows(inner: &Inner) {
    let configs = inner.workflow_state.all_configs();
    for (_name, body) in configs {
        let seq = inner.seq_alloc.next();
        let frame = encode_create_workflow(seq, &body);
        let wf = crate::transport::frame::WriteFrame::Mono(frame);
        if let Ok(admin) = inner.admin_producer.lock() {
            let _ = admin.try_send(wf);
        }
    }
}
