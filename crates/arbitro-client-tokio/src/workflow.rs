//! Workflow orchestration — client-side linear pipelines over streams.
//!
//! A workflow is a chain of steps. The broker has NO workflow-specific
//! code — everything uses streams, consumer groups, publish with msg_id,
//! ack/nack.
//!
//! ```ignore
//! let wf = client.workflow(b"order-process")
//!     .trigger(b"orders.created")
//!     .step(b"validate", |ctx| async { Ok(StepResult { context: ctx.context }) })
//!     .step(b"charge",   |ctx| async { Ok(StepResult { context: ctx.context }) })
//!     .start().await?;
//!
//! // Trigger an instance
//! wf.trigger(&client, task_stream_id, b"initial context").await?;
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

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
    steps: Vec<StepDef>,
    ack_wait_ms: u32,
    max_inflight: u16,
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
            steps: Vec::new(),
            ack_wait_ms: 30_000,
            max_inflight: 10,
        }
    }

    /// Subject pattern that triggers new workflow instances.
    pub fn trigger(mut self, subject: &[u8]) -> Self {
        self.trigger_subject = Some(subject.to_vec());
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

    /// Register the workflow and start processing tasks.
    pub async fn start(self) -> Result<WorkflowHandle, ClientError> {
        if self.trigger_subject.is_none() {
            return Err(ClientError::InvalidConfig("trigger subject required".into()));
        }
        if self.steps.is_empty() {
            return Err(ClientError::InvalidConfig("at least one step required".into()));
        }

        let name_str = String::from_utf8_lossy(&self.name);
        let task_stream_name = format!("_wf.{name_str}.tasks");
        let task_subject = format!("_wf.{name_str}.>");
        let consumer_name = format!("_wf.{name_str}.workers");

        // Create internal task stream with idempotency.
        let resp = self
            .client
            .create_stream(
                task_stream_name.as_bytes(),
                task_subject.as_bytes(),
                0, 0, 0, 1, 0, 0, 0,
                300_000, // idempotency_window_ms = 5 min
            )
            .await?;
        let task_stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

        // Create consumer with ack_wait for failover.
        let consumer_resp = self
            .client
            .create_consumer(
                task_stream_id,
                consumer_name.as_bytes(),
                consumer_name.as_bytes(), // group = consumer name
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
        let total_steps = steps.len() as u16;
        let wf_name = Arc::new(self.name.clone());
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
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
                                    let _ = client.publish_sync_with_id(
                                        task_stream_id,
                                        subject.as_bytes(),
                                        msg_id.as_bytes(),
                                        Bytes::from(task),
                                    ).await;
                                }
                                msg.ack();
                            }
                            Err(_) => {
                                msg.nack();
                            }
                        }
                    }
                }
            }
        });

        Ok(WorkflowHandle {
            name: self.name,
            task_stream_id,
            cancel,
        })
    }
}

// ── WorkflowHandle ────────────────────────────────────────────────────────

/// Handle returned by `WorkflowBuilder::start()`.
pub struct WorkflowHandle {
    name: Vec<u8>,
    task_stream_id: u32,
    cancel: CancellationToken,
}

impl WorkflowHandle {
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    pub fn task_stream_id(&self) -> u32 {
        self.task_stream_id
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
