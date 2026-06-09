//! Workflow orchestration — client-side linear pipelines over streams.
//!
//! A workflow is a chain of steps. The broker has NO workflow-specific
//! code — everything uses streams, consumer groups, publish with msg_id,
//! ack/nack.
//!
//! ```ignore
//! let wf = client.workflow(b"order-process")
//!     .trigger(b"orders.created")
//!     .trigger_stream(orders_stream_id) // auto-subscribe to trigger subject
//!     .step(b"validate", |ctx| async { Ok(StepResult { context: ctx.context }) })
//!     .step(b"charge",   |ctx| async { Ok(StepResult { context: ctx.context }) })
//!     .start().await?;
//!
//! // Trigger an instance
//! wf.trigger(&client, b"initial context").await?;
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex as TokioMutex;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::client::Client;
use crate::error::ClientError;

// ── Types ─────────────────────────────────────────────────────────────────

/// Result returned by a step handler.
#[derive(Debug, Clone)]
pub struct StepResult {
    pub context: Vec<u8>,
}

/// Context passed to a step handler.
#[derive(Debug, Clone)]
pub struct StepContext {
    pub name: Vec<u8>,
    pub instance_id: String,
    pub step_index: u16,
    pub attempt: u8,
    pub context: Vec<u8>,
}

/// Outcome returned by a step handler.
#[derive(Debug, Clone)]
pub enum StepOutcome {
    /// Proceed to the next step (or finish if this is the last step).
    Done(StepResult),
    /// Suspend execution — release the worker slot and wait for an
    /// external event or timeout.
    Suspend {
        /// Opaque state to persist while suspended. Passed back to the
        /// resume / timeout handler.
        state: Vec<u8>,
        /// Timeout in milliseconds. When elapsed, the `on_timeout`
        /// handler is invoked. 0 = no timeout.
        timeout_ms: u64,
    },
}

/// Context passed to a resume handler when a suspended step receives
/// an external event.
#[derive(Debug, Clone)]
pub struct ResumeContext {
    pub name: Vec<u8>,
    pub instance_id: String,
    pub step_index: u16,
    /// State persisted by the `run` handler when it suspended.
    pub state: Vec<u8>,
    /// Payload of the external event that triggered the resume.
    pub event: Vec<u8>,
}

/// Context passed to a timeout handler when a suspended step times out.
#[derive(Debug, Clone)]
pub struct TimeoutContext {
    pub name: Vec<u8>,
    pub instance_id: String,
    pub step_index: u16,
    /// State persisted by the `run` handler when it suspended.
    pub state: Vec<u8>,
}

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type StepHandler =
    Arc<dyn Fn(StepContext) -> BoxFut<Result<StepResult, String>> + Send + Sync + 'static>;
type SuspendRunHandler =
    Arc<dyn Fn(StepContext) -> BoxFut<Result<StepOutcome, String>> + Send + Sync + 'static>;
type ResumeHandler =
    Arc<dyn Fn(ResumeContext) -> BoxFut<Result<StepResult, String>> + Send + Sync + 'static>;
type TimeoutHandler =
    Arc<dyn Fn(TimeoutContext) -> BoxFut<Result<StepResult, String>> + Send + Sync + 'static>;

enum StepKind {
    /// Normal step — single handler.
    Normal(StepHandler),
    /// Suspend step — run/resume/timeout handlers.
    Suspend {
        run: SuspendRunHandler,
        on_resume: ResumeHandler,
        on_timeout: Option<TimeoutHandler>,
        timeout_ms: u64,
    },
}

struct StepDef {
    #[allow(dead_code)]
    name: Vec<u8>,
    kind: StepKind,
}

/// Entry in the suspended-instance registry.
struct SuspendedEntry {
    step_index: u16,
    state: Vec<u8>,
    #[allow(dead_code)] // Used in Fase 3 (Cancel)
    context: Vec<u8>,
}

/// A source pipes messages from an external stream into this workflow.
struct SourceDef {
    stream_name: Vec<u8>,
    subject: Vec<u8>,
}

// ── Task payload encoding ─────────────────────────────────────────────────
// Format: [id_len:2 LE][instance_id:id_len][step_index:2 LE][attempt:1][context...]

/// Bit flag set on `step_index` to mark compensation tasks.
const COMPENSATION_BIT: u16 = 0x8000;

/// Minimum task payload: 2 (id_len) + 0 (empty id) + 2 (step) + 1 (attempt).
const MIN_TASK_PAYLOAD: usize = 5;

fn encode_task(instance_id: &str, step_index: u16, attempt: u8, context: &[u8]) -> Vec<u8> {
    let id_bytes = instance_id.as_bytes();
    let id_len = id_bytes.len() as u16;
    let mut buf = Vec::with_capacity(2 + id_bytes.len() + 2 + 1 + context.len());
    buf.extend_from_slice(&id_len.to_le_bytes());
    buf.extend_from_slice(id_bytes);
    buf.extend_from_slice(&step_index.to_le_bytes());
    buf.push(attempt);
    buf.extend_from_slice(context);
    buf
}

fn decode_task(payload: &[u8]) -> Option<(String, u16, u8, &[u8])> {
    if payload.len() < MIN_TASK_PAYLOAD {
        return None;
    }
    let id_len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    let header = 2 + id_len + 2 + 1;
    if payload.len() < header {
        return None;
    }
    let instance_id = String::from_utf8_lossy(&payload[2..2 + id_len]).into_owned();
    let off = 2 + id_len;
    let step_index = u16::from_le_bytes([payload[off], payload[off + 1]]);
    let attempt = payload[off + 2];
    Some((instance_id, step_index, attempt, &payload[header..]))
}

// ── Instance ID generator ─────────────────────────────────────────────────

static NEXT_INSTANCE: AtomicU32 = AtomicU32::new(1);

fn next_instance_id() -> String {
    NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed).to_string()
}

// ── WorkflowBuilder ───────────────────────────────────────────────────────

/// Fluent builder for workflow registration.
#[must_use]
pub struct WorkflowBuilder {
    client: Client,
    name: Vec<u8>,
    trigger_subject: Option<Vec<u8>>,
    trigger_stream_id: Option<u32>,
    sources: Vec<SourceDef>,
    steps: Vec<StepDef>,
    compensations: Vec<Option<StepHandler>>,
    ack_wait_ms: u32,
    max_inflight: u16,
    max_context_size: usize,
    max_retries: u8,
}

impl std::fmt::Debug for WorkflowBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowBuilder")
            .field("name", &String::from_utf8_lossy(&self.name))
            .field("steps", &self.steps.len())
            .finish()
    }
}

impl WorkflowBuilder {
    pub(crate) fn new(client: Client, name: &[u8]) -> Self {
        Self {
            client,
            name: name.to_vec(),
            trigger_subject: None,
            trigger_stream_id: None,
            sources: Vec::new(),
            steps: Vec::new(),
            compensations: Vec::new(),
            ack_wait_ms: 30_000,
            max_inflight: 10,
            max_context_size: 256 * 1024,
            max_retries: 3,
        }
    }

    /// Subject pattern that triggers new workflow instances.
    pub fn trigger(mut self, subject: &[u8]) -> Self {
        self.trigger_subject = Some(subject.to_vec());
        self
    }

    /// Stream to watch for auto-trigger messages.
    /// When set together with `.trigger()`, the workflow will automatically
    /// subscribe to this stream for the trigger subject and create workflow
    /// instances when messages arrive.
    pub fn trigger_stream(mut self, stream_id: u32) -> Self {
        self.trigger_stream_id = Some(stream_id);
        self
    }

    /// Pipe messages from an external stream into this workflow.
    ///
    /// Each message matching `subject` on the given stream creates a new
    /// workflow instance. The message payload becomes the initial context.
    /// Multiple sources can be registered — each creates an independent
    /// internal consumer.  Zero impact on the publish hot-path.
    pub fn source(mut self, stream_name: &[u8], subject: &[u8]) -> Self {
        self.sources.push(SourceDef {
            stream_name: stream_name.to_vec(),
            subject: subject.to_vec(),
        });
        self
    }

    /// Append a step to the pipeline.
    pub fn step<F, Fut>(mut self, name: &[u8], handler: F) -> Self
    where
        F: Fn(StepContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<StepResult, String>> + Send + 'static,
    {
        let handler: StepHandler =
            Arc::new(move |ctx: StepContext| -> BoxFut<Result<StepResult, String>> {
                Box::pin(handler(ctx))
            });
        self.steps.push(StepDef {
            name: name.to_vec(),
            kind: StepKind::Normal(handler),
        });
        // Keep compensations vec in sync (None = no compensation for this step).
        self.compensations.push(None);
        self
    }

    /// Append a suspend step — the handler can return `StepOutcome::Suspend`
    /// to release the worker and wait for an external event or timeout.
    ///
    /// - `run`: called on first entry — returns `Done` or `Suspend`.
    /// - `on_resume`: called when an external event arrives for this instance.
    /// - `on_timeout`: called when the timeout fires (if configured and no resume arrived).
    pub fn suspend_step<FR, FRFut, FE, FEFut>(
        mut self,
        name: &[u8],
        timeout_ms: u64,
        run: FR,
        on_resume: FE,
    ) -> Self
    where
        FR: Fn(StepContext) -> FRFut + Send + Sync + 'static,
        FRFut: Future<Output = Result<StepOutcome, String>> + Send + 'static,
        FE: Fn(ResumeContext) -> FEFut + Send + Sync + 'static,
        FEFut: Future<Output = Result<StepResult, String>> + Send + 'static,
    {
        let run_h: SuspendRunHandler =
            Arc::new(move |ctx| -> BoxFut<Result<StepOutcome, String>> { Box::pin(run(ctx)) });
        let resume_h: ResumeHandler =
            Arc::new(move |ctx| -> BoxFut<Result<StepResult, String>> { Box::pin(on_resume(ctx)) });
        self.steps.push(StepDef {
            name: name.to_vec(),
            kind: StepKind::Suspend { run: run_h, on_resume: resume_h, on_timeout: None, timeout_ms },
        });
        self.compensations.push(None);
        self
    }

    /// Set the timeout handler for the most recently added suspend step.
    pub fn on_timeout<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(TimeoutContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<StepResult, String>> + Send + 'static,
    {
        let h: TimeoutHandler =
            Arc::new(move |ctx| -> BoxFut<Result<StepResult, String>> { Box::pin(handler(ctx)) });
        if let Some(step) = self.steps.last_mut() {
            if let StepKind::Suspend { on_timeout, .. } = &mut step.kind {
                *on_timeout = Some(h);
            }
        }
        self
    }

    /// Register a compensation handler for the most recently added step.
    /// When a later step fails permanently (after max_retries), compensation
    /// handlers run in reverse order for all previously-completed steps.
    pub fn compensate<F, Fut>(mut self, _step_name: &[u8], handler: F) -> Self
    where
        F: Fn(StepContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<StepResult, String>> + Send + 'static,
    {
        let h: StepHandler =
            Arc::new(move |ctx: StepContext| -> BoxFut<Result<StepResult, String>> {
                Box::pin(handler(ctx))
            });
        if let Some(last) = self.compensations.last_mut() {
            *last = Some(h);
        }
        self
    }

    /// Set ack_wait_ms for the task consumer (default: 30000).
    pub fn ack_wait_ms(mut self, ms: u32) -> Self {
        self.ack_wait_ms = ms;
        self
    }

    /// Set max_inflight for the task consumer (default: 10).
    pub fn max_inflight(mut self, n: u16) -> Self {
        self.max_inflight = n;
        self
    }

    /// Maximum allowed context size in bytes (default: 256 KB).
    /// If a step produces context larger than this, the message is nacked.
    /// Incoming context exceeding this limit is acked (discarded) with a warning.
    pub fn max_context_size(mut self, bytes: usize) -> Self {
        self.max_context_size = bytes;
        self
    }

    /// Maximum number of retry attempts per step before sending to DLQ
    /// (default: 3). When a step handler returns Err and attempt >= max_retries,
    /// the task is moved to the DLQ stream and compensation handlers run.
    pub fn max_retries(mut self, n: u8) -> Self {
        self.max_retries = n;
        self
    }

    /// Register the workflow and start processing tasks.
    pub async fn start(self) -> Result<WorkflowHandle, ClientError> {
        if self.trigger_subject.is_none() {
            return Err(ClientError::InvalidConfig("trigger subject required".into()));
        }
        if self.steps.is_empty() {
            return Err(ClientError::InvalidConfig("at least one step required".into()));
        }

        let name_str = String::from_utf8_lossy(&self.name);
        // Names use underscores (validate_name rejects dots); subjects use dots.
        let task_stream_name = format!("_wf_{name_str}_tasks");
        let task_subject = format!("_wf.{name_str}.>");
        let group_name = format!("_wf_{name_str}_workers");
        // Each worker gets a unique consumer name within the shared group.
        // This allows multiple processes to subscribe independently while
        // the consumer group handles round-robin delivery.
        let worker_uid = next_instance_id(); // unique per process
        let consumer_name = format!("_wf_{name_str}_w{worker_uid}");

        // Create internal task stream with idempotency (idempotent — ignores AlreadyExists).
        let task_stream_id = create_or_get_stream(
            &self.client,
            task_stream_name.as_bytes(),
            task_subject.as_bytes(),
            300_000, // idempotency_window_ms = 5 min
        )
        .await?;

        // Create DLQ stream (idempotent).
        let dlq_stream_name = format!("_wf_{name_str}_dlq");
        let dlq_subject = format!("_wf.{name_str}.dlq.>");
        let dlq_stream_id = create_or_get_stream(
            &self.client,
            dlq_stream_name.as_bytes(),
            dlq_subject.as_bytes(),
            0, // no idempotency needed for DLQ
        )
        .await?;

        // Each worker creates its own consumer in the shared group.
        // Consumer group round-robin ensures each task goes to one worker.
        let consumer_resp = self
            .client
            .create_consumer(
                task_stream_id,
                consumer_name.as_bytes(),
                group_name.as_bytes(), // shared group for round-robin
                task_subject.as_bytes(),
                self.max_inflight,
                1, // AckPolicy::Explicit
                0, // DeliverPolicy::All
                1, // DeliverMode::Queue
                self.ack_wait_ms,
                0, // start_seq
            )
            .await?;
        let consumer_id = u64::from_le_bytes(consumer_resp[..8].try_into().unwrap()) as u32;

        // Subscribe and spawn processing loop.
        let mut sub = self
            .client
            .subscribe(task_stream_id, consumer_id, task_subject.as_bytes())
            .await?;

        let steps = Arc::new(self.steps);
        let compensations: Arc<Vec<Option<StepHandler>>> = Arc::new(self.compensations);
        let total_steps = steps.len() as u16;
        let wf_name = Arc::new(self.name.clone());
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let client = self.client.clone();
        let max_context_size = self.max_context_size;
        let max_retries = self.max_retries;

        // Suspended-instance registry: shared between the dispatch loop
        // (writes on suspend, reads on timeout) and resume/timeout subjects.
        let suspended: Arc<TokioMutex<HashMap<String, SuspendedEntry>>> =
            Arc::new(TokioMutex::new(HashMap::new()));

        tokio::spawn({
            let wf_name = Arc::clone(&wf_name);
            // Pre-compute the UTF-8 name string once — avoids a
            // String::from_utf8_lossy allocation on every message.
            let wf_name_str: Arc<str> = String::from_utf8_lossy(&wf_name).into();
            let client = client.clone();
            let suspended = Arc::clone(&suspended);
            // Pre-compute resume/timeout/cancel subject prefixes for fast matching.
            let resume_prefix: Vec<u8> = format!("_wf.{wf_name_str}.resume.").into_bytes();
            let timeout_prefix: Vec<u8> = format!("_wf.{wf_name_str}.timeout.").into_bytes();
            let cancel_prefix: Vec<u8> = format!("_wf.{wf_name_str}.cancel.").into_bytes();
            async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel_clone.cancelled() => break,
                        msg = sub.recv() => {
                            let msg = match msg {
                                Some(m) => m,
                                None => break,
                            };

                            let payload = msg.payload();
                            let subject_bytes = msg.subject();

                            // ── Resume event: _wf.{name}.resume.{instance_id} ──
                            if subject_bytes.starts_with(&resume_prefix) {
                                let iid = String::from_utf8_lossy(
                                    &subject_bytes[resume_prefix.len()..],
                                ).into_owned();
                                let entry = suspended.lock().await.remove(&iid);
                                if let Some(entry) = entry {
                                    let sidx = entry.step_index;
                                    if let Some(step) = steps.get(sidx as usize) {
                                        if let StepKind::Suspend { on_resume, .. } = &step.kind {
                                            let rctx = ResumeContext {
                                                name: wf_name.as_ref().clone(),
                                                instance_id: iid.clone(),
                                                step_index: sidx,
                                                state: entry.state,
                                                event: payload.to_vec(),
                                            };
                                            match on_resume(rctx).await {
                                                Ok(result) => {
                                                    let next_step = sidx + 1;
                                                    if next_step < total_steps {
                                                        let mid = format!("wf:{iid}:{next_step}:0");
                                                        let subj = format!("_wf.{wf_name_str}.step.{next_step}");
                                                        let task = encode_task(&iid, next_step, 0, &result.context);
                                                        let _ = client.publish_with_id(
                                                            task_stream_id, subj.as_bytes(),
                                                            mid.as_bytes(), Bytes::from(task),
                                                        );
                                                    }
                                                    msg.ack();
                                                }
                                                Err(_) => { msg.nack(); }
                                            }
                                        } else { msg.ack(); }
                                    } else { msg.ack(); }
                                } else {
                                    // Already resumed or timed out — discard.
                                    msg.ack();
                                }
                                continue;
                            }

                            // ── Timeout event: _wf.{name}.timeout.{instance_id} ──
                            if subject_bytes.starts_with(&timeout_prefix) {
                                let iid = String::from_utf8_lossy(
                                    &subject_bytes[timeout_prefix.len()..],
                                ).into_owned();
                                let entry = suspended.lock().await.remove(&iid);
                                if let Some(entry) = entry {
                                    let sidx = entry.step_index;
                                    if let Some(step) = steps.get(sidx as usize) {
                                        if let StepKind::Suspend { on_timeout, .. } = &step.kind {
                                            if let Some(timeout_handler) = on_timeout {
                                                let tctx = TimeoutContext {
                                                    name: wf_name.as_ref().clone(),
                                                    instance_id: iid.clone(),
                                                    step_index: sidx,
                                                    state: entry.state,
                                                };
                                                match timeout_handler(tctx).await {
                                                    Ok(result) => {
                                                        let next_step = sidx + 1;
                                                        if next_step < total_steps {
                                                            let mid = format!("wf:{iid}:{next_step}:0");
                                                            let subj = format!("_wf.{wf_name_str}.step.{next_step}");
                                                            let task = encode_task(&iid, next_step, 0, &result.context);
                                                            let _ = client.publish_with_id(
                                                                task_stream_id, subj.as_bytes(),
                                                                mid.as_bytes(), Bytes::from(task),
                                                            );
                                                        }
                                                        msg.ack();
                                                    }
                                                    Err(_) => { msg.nack(); }
                                                }
                                            } else {
                                                // No timeout handler — just discard the suspended entry.
                                                msg.ack();
                                            }
                                        } else { msg.ack(); }
                                    } else { msg.ack(); }
                                } else {
                                    // Already resumed — timeout is stale.
                                    msg.ack();
                                }
                                continue;
                            }

                            // ── Cancel event: _wf.{name}.cancel.{instance_id} ──
                            if subject_bytes.starts_with(&cancel_prefix) {
                                let iid = String::from_utf8_lossy(
                                    &subject_bytes[cancel_prefix.len()..],
                                ).into_owned();
                                // Remove from suspended registry if present.
                                // If the instance is currently running (not in map),
                                // cancellation is best-effort — ack and move on.
                                let _ = suspended.lock().await.remove(&iid);
                                msg.ack();
                                continue;
                            }

                            // ── Normal task decode ──
                            let (instance_id, step_index, attempt, context) = match decode_task(&payload) {
                                Some(t) => t,
                                None => { msg.ack(); continue; }
                            };

                            // ── Context overflow guard (incoming) ──
                            if context.len() > max_context_size {
                                warn!(
                                    workflow = %wf_name_str,
                                    instance_id,
                                    step_index,
                                    context_len = context.len(),
                                    max = max_context_size,
                                    "incoming context exceeds max_context_size, discarding"
                                );
                                msg.ack();
                                continue;
                            }

                            // ── Compensation task (high bit set) ──
                            let is_compensation = step_index & COMPENSATION_BIT != 0;
                            if is_compensation {
                                let original_idx = step_index & !COMPENSATION_BIT;
                                if let Some(Some(comp_handler)) = compensations.get(original_idx as usize) {
                                    let ctx = StepContext {
                                        name: wf_name.as_ref().clone(),
                                        instance_id,
                                        step_index: original_idx,
                                        attempt,
                                        context: context.to_vec(),
                                    };
                                    // Best-effort: run compensation, ack regardless.
                                    let _ = comp_handler(ctx).await;
                                }
                                msg.ack();
                                continue;
                            }

                            // ── Step processing ──
                            if step_index as usize >= steps.len() {
                                msg.ack();
                                continue;
                            }

                            let step = &steps[step_index as usize];
                            let ctx = StepContext {
                                name: wf_name.as_ref().clone(),
                                instance_id: instance_id.clone(),
                                step_index,
                                attempt,
                                context: context.to_vec(),
                            };

                            // Unify: Normal handler returns Done, Suspend handler returns StepOutcome.
                            let outcome = match &step.kind {
                                StepKind::Normal(handler) => {
                                    handler(ctx).await.map(StepOutcome::Done)
                                }
                                StepKind::Suspend { run, .. } => run(ctx).await,
                            };

                            match outcome {
                                Ok(StepOutcome::Done(result)) => {
                                    // ── Context overflow guard (outgoing) ──
                                    if result.context.len() > max_context_size {
                                        warn!(
                                            workflow = %wf_name_str,
                                            instance_id,
                                            step_index,
                                            context_len = result.context.len(),
                                            max = max_context_size,
                                            "step produced context exceeding max_context_size, nacking"
                                        );
                                        msg.nack();
                                        continue;
                                    }

                                    let next_step = step_index + 1;
                                    if next_step < total_steps {
                                        // Advance: publish next step with idempotent msg_id.
                                        let mid = format!("wf:{instance_id}:{next_step}:0");
                                        let subj = format!(
                                            "_wf.{wf_name_str}.step.{next_step}"
                                        );
                                        let task = encode_task(
                                            &instance_id, next_step, 0, &result.context,
                                        );
                                        // Fire-and-forget publish — does NOT await broker
                                        // response. Avoids deadlock where the response
                                        // is queued behind a delivery frame in the reader.
                                        // Idempotent msg_id protects against duplicates on
                                        // retry (nack → redeliver → re-publish is deduped).
                                        match client.publish_with_id(
                                            task_stream_id,
                                            subj.as_bytes(),
                                            mid.as_bytes(),
                                            Bytes::from(task),
                                        ) {
                                            Ok(_) => { msg.ack(); }
                                            Err(_e) => {
                                                // Enqueue failed — nack for retry.
                                                msg.nack();
                                                continue;
                                            }
                                        }
                                    } else {
                                        // Last step — just ack.
                                        msg.ack();
                                    }
                                }
                                Ok(StepOutcome::Suspend { state, timeout_ms: handler_timeout }) => {
                                    // Persist in-memory and release the worker slot.
                                    suspended.lock().await.insert(instance_id.clone(), SuspendedEntry {
                                        step_index,
                                        state: state.clone(),
                                        context: context.to_vec(),
                                    });

                                    // Merge timeouts: handler can override the step default.
                                    let effective_timeout = if handler_timeout > 0 {
                                        handler_timeout
                                    } else if let StepKind::Suspend { timeout_ms, .. } = &step.kind {
                                        *timeout_ms
                                    } else {
                                        0
                                    };

                                    // Schedule timeout via tokio::sleep + fire-and-forget publish.
                                    // We avoid publish_delayed because its async response
                                    // can deadlock the dispatch loop. Since the suspended
                                    // registry is in-memory anyway, a local timer is fine.
                                    if effective_timeout > 0 {
                                        let timeout_subject = format!(
                                            "_wf.{wf_name_str}.timeout.{instance_id}"
                                        );
                                        let timeout_mid = format!(
                                            "wf:{instance_id}:timeout:{step_index}"
                                        );
                                        let cl = client.clone();
                                        let ts = task_stream_id;
                                        tokio::spawn(async move {
                                            tokio::time::sleep(
                                                std::time::Duration::from_millis(effective_timeout),
                                            ).await;
                                            let _ = cl.publish_with_id(
                                                ts,
                                                timeout_subject.as_bytes(),
                                                timeout_mid.as_bytes(),
                                                Bytes::new(),
                                            );
                                        });
                                    }

                                    msg.ack();
                                }
                                Err(err) => {
                                    // ── Max retries → DLQ + compensation ──
                                    if attempt >= max_retries {
                                        // Publish to DLQ.
                                        let dlq_subject = format!(
                                            "_wf.{wf_name_str}.dlq.{step_index}",
                                        );
                                        let mut dlq_payload = Vec::new();
                                        let id_bytes = instance_id.as_bytes();
                                        dlq_payload.extend_from_slice(&(id_bytes.len() as u16).to_le_bytes());
                                        dlq_payload.extend_from_slice(id_bytes);
                                        dlq_payload.extend_from_slice(&step_index.to_le_bytes());
                                        dlq_payload.push(attempt);
                                        // Append error length (4 LE) + error + context.
                                        let err_bytes = err.as_bytes();
                                        dlq_payload.extend_from_slice(&(err_bytes.len() as u32).to_le_bytes());
                                        dlq_payload.extend_from_slice(err_bytes);
                                        dlq_payload.extend_from_slice(context);

                                        let _ = client.publish_with_id(
                                            dlq_stream_id,
                                            dlq_subject.as_bytes(),
                                            format!("wf:{instance_id}:dlq:{step_index}").as_bytes(),
                                            Bytes::from(dlq_payload),
                                        );

                                        // Trigger compensation in reverse for completed steps.
                                        if step_index > 0 {
                                            for comp_idx in (0..step_index).rev() {
                                                let comp_step = COMPENSATION_BIT | comp_idx;
                                                let comp_subject = format!(
                                                    "_wf.{wf_name_str}.compensate.{comp_idx}",
                                                );
                                                let comp_task = encode_task(
                                                    &instance_id, comp_step, 0, context,
                                                );
                                                let comp_msg_id = format!(
                                                    "wf:{instance_id}:comp:{comp_idx}"
                                                );
                                                let _ = client.publish_with_id(
                                                    task_stream_id,
                                                    comp_subject.as_bytes(),
                                                    comp_msg_id.as_bytes(),
                                                    Bytes::from(comp_task),
                                                );
                                            }
                                        }

                                        warn!(
                                            workflow = %wf_name_str,
                                            instance_id,
                                            step_index,
                                            attempt,
                                            error = %err,
                                            "step exceeded max_retries, moved to DLQ"
                                        );
                                        msg.ack();
                                    } else {
                                        msg.nack();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        // ── Auto-trigger subscription ──
        if let (Some(trigger_subject), Some(trigger_sid)) =
            (&self.trigger_subject, self.trigger_stream_id)
        {
            let trigger_consumer_name = format!("_wf_{name_str}_trigger");
            let trigger_consumer_resp = self
                .client
                .create_consumer(
                    trigger_sid,
                    trigger_consumer_name.as_bytes(),
                    trigger_consumer_name.as_bytes(),
                    trigger_subject,
                    1,  // max_inflight
                    1,  // AckPolicy::Explicit
                    0,  // DeliverPolicy::All
                    1,  // DeliverMode::Queue
                    self.ack_wait_ms,
                    0,  // start_seq
                )
                .await?;
            let trigger_consumer_id =
                u64::from_le_bytes(trigger_consumer_resp[..8].try_into().unwrap()) as u32;

            let mut trigger_sub = self
                .client
                .subscribe(trigger_sid, trigger_consumer_id, trigger_subject)
                .await?;

            let cancel_trigger = cancel.clone();
            // Pre-compute subject prefix — avoids per-message from_utf8_lossy + format.
            let trigger_subject: Arc<str> = format!(
                "_wf.{}.step.0",
                String::from_utf8_lossy(&self.name),
            ).into();
            let trigger_client = self.client.clone();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel_trigger.cancelled() => break,
                        msg = trigger_sub.recv() => {
                            let msg = match msg {
                                Some(m) => m,
                                None => break,
                            };

                            let payload = msg.payload();
                            let instance_id = next_instance_id();
                            let msg_id = format!("wf:{instance_id}:0:0");
                            let task = encode_task(&instance_id, 0, 0, &payload);
                            let _ = trigger_client.publish_with_id(
                                task_stream_id,
                                trigger_subject.as_bytes(),
                                msg_id.as_bytes(),
                                Bytes::from(task),
                            );
                            msg.ack();
                        }
                    }
                }
            });
        }

        // ── Source subscriptions ──
        for (src_idx, src) in self.sources.iter().enumerate() {
            let src_stream_resp = self.client.get_stream(&src.stream_name).await?;
            let src_stream_id =
                u64::from_le_bytes(src_stream_resp[..8].try_into().unwrap()) as u32;

            let src_consumer_name = format!("_wf_{name_str}_src_{src_idx}");
            let src_consumer_resp = self
                .client
                .create_consumer(
                    src_stream_id,
                    src_consumer_name.as_bytes(),
                    src_consumer_name.as_bytes(),
                    &src.subject,
                    1,  // max_inflight
                    1,  // AckPolicy::Explicit
                    0,  // DeliverPolicy::All
                    1,  // DeliverMode::Queue
                    self.ack_wait_ms,
                    0,  // start_seq
                )
                .await?;
            let src_consumer_id =
                u64::from_le_bytes(src_consumer_resp[..8].try_into().unwrap()) as u32;

            let mut src_sub = self
                .client
                .subscribe(src_stream_id, src_consumer_id, &src.subject)
                .await?;

            let cancel_src = cancel.clone();
            let step0_subject: Arc<str> = format!(
                "_wf.{}.step.0",
                String::from_utf8_lossy(&self.name),
            )
            .into();
            let src_client = self.client.clone();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel_src.cancelled() => break,
                        msg = src_sub.recv() => {
                            let msg = match msg {
                                Some(m) => m,
                                None => break,
                            };

                            let payload = msg.payload();
                            let instance_id = next_instance_id();
                            let msg_id = format!("wf:{instance_id}:0:0");
                            let task = encode_task(&instance_id, 0, 0, &payload);
                            let _ = src_client.publish_with_id(
                                task_stream_id,
                                step0_subject.as_bytes(),
                                msg_id.as_bytes(),
                                Bytes::from(task),
                            );
                            msg.ack();
                        }
                    }
                }
            });
        }

        Ok(WorkflowHandle {
            name: self.name,
            task_stream_id,
            dlq_stream_id,
            cancel,
            resume_seq: AtomicU32::new(0),
        })
    }
}

// ── WorkflowHandle ────────────────────────────────────────────────────────

/// Handle returned by `WorkflowBuilder::start()`.
pub struct WorkflowHandle {
    name: Vec<u8>,
    task_stream_id: u32,
    dlq_stream_id: u32,
    cancel: CancellationToken,
    resume_seq: AtomicU32,
}

impl WorkflowHandle {
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    pub fn task_stream_id(&self) -> u32 {
        self.task_stream_id
    }

    pub fn dlq_stream_id(&self) -> u32 {
        self.dlq_stream_id
    }

    /// Trigger a new workflow instance with an explicit ID.
    ///
    /// The caller chooses the `instance_id` (e.g. a business key like
    /// `"ord_123"`). The same ID can be used by external systems to
    /// address this workflow instance.
    pub async fn trigger_with_id(
        &self,
        client: &Client,
        instance_id: &str,
        context: &[u8],
    ) -> Result<(), ClientError> {
        let msg_id = format!("wf:{instance_id}:0:0");
        let subject = format!(
            "_wf.{}.step.0",
            String::from_utf8_lossy(&self.name)
        );
        let task = encode_task(instance_id, 0, 0, context);
        client
            .publish_sync_with_id(
                self.task_stream_id,
                subject.as_bytes(),
                msg_id.as_bytes(),
                Bytes::from(task),
            )
            .await?;
        Ok(())
    }

    /// Trigger a new workflow instance with an auto-generated ID.
    ///
    /// Returns the generated instance ID so the caller can track
    /// or correlate the workflow instance.
    pub async fn trigger(
        &self,
        client: &Client,
        context: &[u8],
    ) -> Result<String, ClientError> {
        let instance_id = next_instance_id();
        self.trigger_with_id(client, &instance_id, context).await?;
        Ok(instance_id)
    }

    /// Cancel a workflow instance.
    ///
    /// If the instance is suspended, it is removed from the registry.
    /// If the instance is currently running or doesn't exist, the
    /// cancel message is a no-op (idempotent).
    pub async fn cancel(
        &self,
        client: &Client,
        instance_id: &str,
    ) -> Result<(), ClientError> {
        let subject = format!(
            "_wf.{}.cancel.{instance_id}",
            String::from_utf8_lossy(&self.name),
        );
        client
            .publish_sync_with_id(
                self.task_stream_id,
                subject.as_bytes(),
                format!("wf:{instance_id}:cancel").as_bytes(),
                Bytes::new(),
            )
            .await?;
        Ok(())
    }

    /// Resume a suspended workflow instance with an external event.
    ///
    /// Publishes a resume event to the task stream. The dispatch loop
    /// picks it up, matches it against the in-memory suspended registry,
    /// and invokes the `on_resume` handler.
    pub async fn resume(
        &self,
        client: &Client,
        instance_id: &str,
        event: &[u8],
    ) -> Result<(), ClientError> {
        let seq = self.resume_seq.fetch_add(1, Ordering::Relaxed);
        let subject = format!(
            "_wf.{}.resume.{instance_id}",
            String::from_utf8_lossy(&self.name),
        );
        client
            .publish_sync_with_id(
                self.task_stream_id,
                subject.as_bytes(),
                format!("wf:{instance_id}:resume:{seq}").as_bytes(),
                Bytes::from(event.to_vec()),
            )
            .await?;
        Ok(())
    }

    /// Stop processing tasks.
    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

impl Drop for WorkflowHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl std::fmt::Debug for WorkflowHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowHandle")
            .field("name", &String::from_utf8_lossy(&self.name))
            .finish()
    }
}

// ── Idempotent create helpers ─────────────────────────────────────────────

/// Create a stream, or get its ID if it already exists.
async fn create_or_get_stream(
    client: &Client,
    name: &[u8],
    subject: &[u8],
    idempotency_window_ms: u32,
) -> Result<u32, ClientError> {
    match client
        .create_stream(name, subject, 0, 0, 0, 1, 0, 0, 0, idempotency_window_ms)
        .await
    {
        Ok(resp) => Ok(u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32),
        Err(ClientError::Broker {
            code: arbitro_proto::error::ErrorCode::StreamAlreadyExists,
        }) => {
            // Stream exists — get its ID via get_stream.
            let resp = client.get_stream(name).await?;
            Ok(u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32)
        }
        Err(e) => Err(e),
    }
}

