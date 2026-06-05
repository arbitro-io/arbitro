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

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::Bytes;
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
    pub instance_id: u32,
    pub step_index: u16,
    pub attempt: u8,
    pub context: Vec<u8>,
}

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type StepHandler =
    Arc<dyn Fn(StepContext) -> BoxFut<Result<StepResult, String>> + Send + Sync + 'static>;

struct StepDef {
    #[allow(dead_code)]
    name: Vec<u8>,
    handler: StepHandler,
}

// ── Task payload encoding ─────────────────────────────────────────────────
// Format: [instance_id:4 LE][step_index:2 LE][attempt:1][context...]

const TASK_HEADER: usize = 4 + 2 + 1; // 7 bytes

/// Bit flag set on `step_index` to mark compensation tasks.
const COMPENSATION_BIT: u16 = 0x8000;

fn encode_task(instance_id: u32, step_index: u16, attempt: u8, context: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(TASK_HEADER + context.len());
    buf.extend_from_slice(&instance_id.to_le_bytes());
    buf.extend_from_slice(&step_index.to_le_bytes());
    buf.push(attempt);
    buf.extend_from_slice(context);
    buf
}

fn decode_task(payload: &[u8]) -> Option<(u32, u16, u8, &[u8])> {
    if payload.len() < TASK_HEADER {
        return None;
    }
    let instance_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let step_index = u16::from_le_bytes([payload[4], payload[5]]);
    let attempt = payload[6];
    Some((instance_id, step_index, attempt, &payload[TASK_HEADER..]))
}

// ── Instance ID generator ─────────────────────────────────────────────────

static NEXT_INSTANCE: AtomicU32 = AtomicU32::new(1);

fn next_instance_id() -> u32 {
    NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed)
}

// ── WorkflowBuilder ───────────────────────────────────────────────────────

/// Fluent builder for workflow registration.
#[must_use]
pub struct WorkflowBuilder {
    client: Client,
    name: Vec<u8>,
    trigger_subject: Option<Vec<u8>>,
    trigger_stream_id: Option<u32>,
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
            handler,
        });
        // Keep compensations vec in sync (None = no compensation for this step).
        self.compensations.push(None);
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

        tokio::spawn({
            let wf_name = Arc::clone(&wf_name);
            let client = client.clone();
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
                            let (instance_id, step_index, attempt, context) = match decode_task(&payload) {
                                Some(t) => t,
                                None => { msg.ack(); continue; }
                            };

                            // ── Context overflow guard (incoming) ──
                            if context.len() > max_context_size {
                                warn!(
                                    workflow = %String::from_utf8_lossy(&wf_name),
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

                            // ── Normal step processing ──
                            if step_index as usize >= steps.len() {
                                msg.ack();
                                continue;
                            }

                            let handler = &steps[step_index as usize].handler;
                            let ctx = StepContext {
                                name: wf_name.as_ref().clone(),
                                instance_id,
                                step_index,
                                attempt,
                                context: context.to_vec(),
                            };

                            match handler(ctx).await {
                                Ok(result) => {
                                    // ── Context overflow guard (outgoing) ──
                                    if result.context.len() > max_context_size {
                                        warn!(
                                            workflow = %String::from_utf8_lossy(&wf_name),
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
                                        let msg_id = format!("wf:{instance_id}:{next_step}:0");
                                        let subject = format!(
                                            "_wf.{}.step.{}",
                                            String::from_utf8_lossy(&wf_name),
                                            next_step
                                        );
                                        let task = encode_task(
                                            instance_id, next_step, 0, &result.context,
                                        );
                                        // Fire-and-forget publish — does NOT await broker
                                        // response. Avoids deadlock where the response
                                        // is queued behind a delivery frame in the reader.
                                        // Idempotent msg_id protects against duplicates on
                                        // retry (nack → redeliver → re-publish is deduped).
                                        match client.publish_with_id(
                                            task_stream_id,
                                            subject.as_bytes(),
                                            msg_id.as_bytes(),
                                            Bytes::from(task),
                                        ) {
                                            Ok(_) => {}
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
                                Err(err) => {
                                    // ── Max retries → DLQ + compensation ──
                                    if attempt >= max_retries {
                                        // Publish to DLQ.
                                        let dlq_subject = format!(
                                            "_wf.{}.dlq.{}",
                                            String::from_utf8_lossy(&wf_name),
                                            step_index,
                                        );
                                        let mut dlq_payload = Vec::new();
                                        dlq_payload.extend_from_slice(&instance_id.to_le_bytes());
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
                                                    "_wf.{}.compensate.{}",
                                                    String::from_utf8_lossy(&wf_name),
                                                    comp_idx,
                                                );
                                                let comp_task = encode_task(
                                                    instance_id, comp_step, 0, context,
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
                                            workflow = %String::from_utf8_lossy(&wf_name),
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
            let trigger_wf_name = self.name.clone();
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
                            let subject = format!(
                                "_wf.{}.step.0",
                                String::from_utf8_lossy(&trigger_wf_name),
                            );
                            let task = encode_task(instance_id, 0, 0, &payload);
                            let _ = trigger_client.publish_with_id(
                                task_stream_id,
                                subject.as_bytes(),
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

    /// Trigger a new workflow instance.
    pub async fn trigger(&self, client: &Client, context: &[u8]) -> Result<u32, ClientError> {
        let instance_id = next_instance_id();
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
        Ok(instance_id)
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

