//! Workflow subsystem — in-memory multi-step orchestration registry.
//!
//! Workflows live entirely in memory. When a client calls `CreateWorkflow`,
//! the broker registers the trigger subject, steps, config, and originating
//! connection. When a message is published to a matching trigger subject,
//! the broker creates a `WorkflowInstance` and sends the first
//! `WorkflowStep` frame to a registered worker (round-robin). Each step
//! result advances the instance to the next step. Context (Vec<u8>) flows
//! between steps.
//!
//! Multiple connections can register the same workflow name — the name is
//! the dedup key. The broker picks one worker per step (queue semantics).
//! On disconnect, the connection is removed from the worker list. If the
//! list empties, the slot is removed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::transport::registry::ConnectionRegistry;
use arbitro_proto::wire::workflow::{
    WorkflowInfo, InstanceInfo, encode_workflow_step, encode_workflow_error,
};

// ── StepConfig ─────────────────────────────────────────────────────────────

/// Per-step configuration, stored in the slot.
#[derive(Debug, Clone)]
struct StepConfig {
    name: String,
    timeout_ms: u32,
    max_retries: u8,
}

// ── WorkflowSlot ───────────────────────────────────────────────────────────

/// A single workflow definition with its worker pool.
#[derive(Debug)]
struct WorkflowSlot {
    /// Subject pattern that triggers this workflow.
    trigger_subject: String,
    /// Ordered step definitions.
    steps: Vec<StepConfig>,
    /// Maximum concurrent instances (0 = unlimited).
    max_concurrent: u32,
    /// JSON key path for dedup (None = no dedup).
    dedup_key: Option<String>,
    /// Overall workflow timeout in milliseconds (0 = no timeout).
    timeout_ms: u32,
    /// Registered worker connections.
    connections: Vec<u64>,
    /// Round-robin cursor.
    cursor: usize,
}

impl WorkflowSlot {
    fn new(
        trigger_subject: String,
        steps: Vec<StepConfig>,
        max_concurrent: u32,
        dedup_key: Option<String>,
        timeout_ms: u32,
        conn_id: u64,
    ) -> Self {
        Self {
            trigger_subject,
            steps,
            max_concurrent,
            dedup_key,
            timeout_ms,
            connections: vec![conn_id],
            cursor: 0,
        }
    }

    /// Pick the next worker connection (round-robin).
    fn next_worker(&mut self) -> Option<u64> {
        if self.connections.is_empty() {
            return None;
        }
        self.cursor %= self.connections.len();
        let conn = self.connections[self.cursor];
        self.cursor = (self.cursor + 1) % self.connections.len();
        Some(conn)
    }

    /// Remove a connection from the worker list.
    fn remove_connection(&mut self, conn_id: u64) {
        self.connections.retain(|&c| c != conn_id);
        if self.cursor > 0 && self.cursor >= self.connections.len() {
            self.cursor = 0;
        }
    }
}

// ── WorkflowInstance ───────────────────────────────────────────────────────

/// Status of a workflow instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for InstanceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// A running workflow instance.
#[derive(Debug)]
struct WorkflowInstance {
    workflow_name: Bytes,
    current_step: u16,
    context: Vec<u8>,
    status: InstanceStatus,
    retries_left: u8,
    running_conn: Option<u64>,
    running_since: Option<Instant>,
    created_at: Instant,
}

// ── WorkflowRegistry ───────────────────────────────────────────────────────

/// Thread-safe registry of all active workflows and their instances.
pub struct WorkflowRegistry {
    inner: Mutex<RegistryInner>,
    next_id: AtomicU32,
}

#[derive(Debug)]
struct RegistryInner {
    /// Workflow definitions keyed by name.
    slots: HashMap<Bytes, WorkflowSlot>,
    /// Active instances keyed by instance_id.
    instances: HashMap<u32, WorkflowInstance>,
    /// Dedup map: (workflow_name, dedup_value) → instance_id.
    dedup: HashMap<(Bytes, String), u32>,
}

impl std::fmt::Debug for WorkflowRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowRegistry").finish()
    }
}

impl Default for WorkflowRegistry {
    fn default() -> Self {
        Self {
            inner: Mutex::new(RegistryInner {
                slots: HashMap::new(),
                instances: HashMap::new(),
                dedup: HashMap::new(),
            }),
            next_id: AtomicU32::new(1),
        }
    }
}

impl WorkflowRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a workflow. If the name already exists, just add the
    /// connection to the worker pool.
    pub fn create(
        &self,
        name: Bytes,
        trigger: &str,
        steps: Vec<(String, u32, u8)>, // (name, timeout_ms, max_retries)
        max_concurrent: u32,
        dedup_key: Option<String>,
        timeout_ms: u32,
        conn_id: u64,
    ) -> Result<(), String> {
        if steps.is_empty() {
            return Err("workflow must have at least one step".into());
        }

        let step_configs: Vec<StepConfig> = steps
            .into_iter()
            .map(|(name, timeout_ms, max_retries)| StepConfig {
                name,
                timeout_ms,
                max_retries,
            })
            .collect();

        let mut inner = self.inner.lock();
        if let Some(slot) = inner.slots.get_mut(&name) {
            if !slot.connections.contains(&conn_id) {
                slot.connections.push(conn_id);
                debug!(name = %String::from_utf8_lossy(&name), conn_id, "workflow worker added");
            }
        } else {
            info!(name = %String::from_utf8_lossy(&name), trigger, "workflow created");
            inner.slots.insert(
                name,
                WorkflowSlot::new(
                    trigger.to_string(),
                    step_configs,
                    max_concurrent,
                    dedup_key,
                    timeout_ms,
                    conn_id,
                ),
            );
        }
        Ok(())
    }

    /// Delete a workflow entirely.
    pub fn delete(&self, name: &[u8]) -> bool {
        let mut inner = self.inner.lock();
        let existed = inner.slots.remove(name).is_some();
        if existed {
            // Cancel all running instances for this workflow.
            let name_bytes = Bytes::copy_from_slice(name);
            let to_remove: Vec<u32> = inner
                .instances
                .iter()
                .filter(|(_, inst)| inst.workflow_name == name_bytes)
                .map(|(&id, _)| id)
                .collect();
            for id in to_remove {
                inner.instances.remove(&id);
            }
            // Clean up dedup entries for this workflow.
            inner.dedup.retain(|(wf_name, _), _| wf_name.as_ref() != name);
            info!(name = %String::from_utf8_lossy(name), "workflow deleted");
        }
        existed
    }

    /// Trigger a workflow by subject match. Returns a list of
    /// (conn_id, step_frame) for the first step to be sent.
    pub fn trigger(
        &self,
        subject: &[u8],
        initial_context: &[u8],
    ) -> Vec<(u64, Bytes)> {
        let mut inner = self.inner.lock();
        let mut results = Vec::new();

        // Find workflows whose trigger matches this subject.
        let matching: Vec<Bytes> = inner
            .slots
            .iter()
            .filter(|(_, slot)| subject_matches(&slot.trigger_subject, subject))
            .map(|(name, _)| name.clone())
            .collect();

        for name in matching {
            // Pre-extract slot info we need (immutable reads) before
            // taking a mutable borrow for next_worker().
            let (max_concurrent, dedup_key_clone, first_retries) = {
                let slot = match inner.slots.get(&name) {
                    Some(s) => s,
                    None => continue,
                };
                (
                    slot.max_concurrent,
                    slot.dedup_key.clone(),
                    slot.steps.first().map(|s| s.max_retries).unwrap_or(0),
                )
            };

            // Max concurrent check.
            if max_concurrent > 0 {
                let active = inner
                    .instances
                    .values()
                    .filter(|i| i.workflow_name == name && i.status == InstanceStatus::Running)
                    .count() as u32;
                if active >= max_concurrent {
                    debug!(
                        name = %String::from_utf8_lossy(&name),
                        "workflow trigger skipped (max_concurrent)"
                    );
                    continue;
                }
            }

            // Dedup check.
            if let Some(ref dedup_key) = dedup_key_clone {
                let dedup_val = extract_dedup_value(initial_context, dedup_key);
                if let Some(ref dedup_val) = dedup_val {
                    let key = (name.clone(), dedup_val.clone());
                    if let Some(&existing_id) = inner.dedup.get(&key) {
                        if inner
                            .instances
                            .get(&existing_id)
                            .is_some_and(|i| i.status == InstanceStatus::Running)
                        {
                            debug!(
                                name = %String::from_utf8_lossy(&name),
                                dedup_val,
                                "workflow trigger skipped (dedup)"
                            );
                            continue;
                        }
                    }
                }
            }

            // Pick a worker (needs mutable borrow on slot).
            let conn_id = match inner.slots.get_mut(&name).and_then(|s| s.next_worker()) {
                Some(c) => c,
                None => continue,
            };

            // Create instance.
            let instance_id = self.next_id.fetch_add(1, Ordering::Relaxed);

            let instance = WorkflowInstance {
                workflow_name: name.clone(),
                current_step: 0,
                context: initial_context.to_vec(),
                status: InstanceStatus::Running,
                retries_left: first_retries,
                running_conn: Some(conn_id),
                running_since: Some(Instant::now()),
                created_at: Instant::now(),
            };

            // Insert dedup entry if applicable.
            if let Some(ref dedup_key) = dedup_key_clone {
                if let Some(dedup_val) = extract_dedup_value(initial_context, dedup_key) {
                    inner.dedup.insert((name.clone(), dedup_val), instance_id);
                }
            }

            inner.instances.insert(instance_id, instance);

            let frame = encode_workflow_step(
                0,
                &name,
                instance_id,
                0,
                initial_context,
            );
            results.push((conn_id, frame));

            debug!(
                name = %String::from_utf8_lossy(&name),
                instance_id,
                conn_id,
                "workflow triggered"
            );
        }

        results
    }

    /// Advance a workflow instance after receiving a step result.
    /// Returns Some((conn_id, frame)) for the next step, or None if
    /// the workflow completed or errored.
    pub fn advance(
        &self,
        instance_id: u32,
        ok: bool,
        new_context: &[u8],
    ) -> Option<(u64, Bytes)> {
        let mut inner = self.inner.lock();

        // Extract instance state first.
        let inst = inner.instances.get(&instance_id)?;
        if inst.status != InstanceStatus::Running {
            return None;
        }
        let name = inst.workflow_name.clone();
        let current_step = inst.current_step;
        let retries_left = inst.retries_left;
        let context_clone = inst.context.clone();

        // Get step count and next retries from slot.
        let (step_count, next_step_retries) = {
            let slot = inner.slots.get(&name)?;
            let next_idx = (current_step + 1) as usize;
            let next_retries = if next_idx < slot.steps.len() {
                slot.steps[next_idx].max_retries
            } else {
                0
            };
            (slot.steps.len(), next_retries)
        };

        if !ok {
            // Step failed — check retries.
            if retries_left > 0 {
                let inst = inner.instances.get_mut(&instance_id)?;
                inst.retries_left -= 1;

                let conn_id = inner.slots.get_mut(&name).and_then(|s| s.next_worker())?;
                let inst = inner.instances.get_mut(&instance_id)?;
                inst.running_conn = Some(conn_id);
                inst.running_since = Some(Instant::now());

                let frame = encode_workflow_step(
                    0,
                    &name,
                    instance_id,
                    current_step,
                    &context_clone,
                );
                return Some((conn_id, frame));
            }

            // No retries left — send error to onError handler.
            let inst = inner.instances.get_mut(&instance_id)?;
            inst.status = InstanceStatus::Failed;
            inst.running_conn = None;
            inst.running_since = None;
            let failed_step = inst.current_step;

            let conn_id = inner.slots.get_mut(&name).and_then(|s| s.next_worker())?;
            let error_json = serde_json::to_vec(&serde_json::json!({
                "instance_id": instance_id,
                "step": failed_step,
                "message": "step failed after all retries"
            }))
            .unwrap_or_default();

            let frame = encode_workflow_error(0, &name, instance_id, &error_json);
            return Some((conn_id, frame));
        }

        // Step succeeded — advance.
        let inst = inner.instances.get_mut(&instance_id)?;
        inst.context = new_context.to_vec();
        inst.current_step += 1;

        if inst.current_step as usize >= step_count {
            // Workflow completed.
            inst.status = InstanceStatus::Completed;
            inst.running_conn = None;
            inst.running_since = None;
            debug!(
                name = %String::from_utf8_lossy(&name),
                instance_id,
                "workflow completed"
            );
            return None;
        }

        // Send next step.
        inst.retries_left = next_step_retries;
        let next_step = inst.current_step;

        let conn_id = inner.slots.get_mut(&name).and_then(|s| s.next_worker())?;
        let inst = inner.instances.get_mut(&instance_id)?;
        inst.running_conn = Some(conn_id);
        inst.running_since = Some(Instant::now());

        let frame = encode_workflow_step(
            0,
            &name,
            instance_id,
            next_step,
            new_context,
        );
        Some((conn_id, frame))
    }

    /// Cancel a running workflow instance.
    pub fn cancel(&self, instance_id: u32) -> bool {
        let mut inner = self.inner.lock();
        if let Some(inst) = inner.instances.get_mut(&instance_id) {
            if inst.status == InstanceStatus::Running {
                inst.status = InstanceStatus::Cancelled;
                inst.running_conn = None;
                inst.running_since = None;
                info!(instance_id, "workflow cancelled");
                return true;
            }
        }
        false
    }

    /// List all active workflows.
    pub fn list(&self) -> Vec<WorkflowInfo> {
        let inner = self.inner.lock();
        inner
            .slots
            .iter()
            .map(|(name, slot)| {
                let active_instances = inner
                    .instances
                    .values()
                    .filter(|i| {
                        i.workflow_name.as_ref() == name.as_ref()
                            && i.status == InstanceStatus::Running
                    })
                    .count() as u32;
                WorkflowInfo {
                    name: String::from_utf8_lossy(name).to_string(),
                    trigger: slot.trigger_subject.clone(),
                    steps: slot.steps.iter().map(|s| s.name.clone()).collect(),
                    workers: slot.connections.len() as u32,
                    active_instances,
                }
            })
            .collect()
    }

    /// List instances of a specific workflow.
    pub fn list_instances(&self, name: &[u8]) -> Vec<InstanceInfo> {
        let inner = self.inner.lock();
        let name_bytes = Bytes::copy_from_slice(name);
        inner
            .instances
            .iter()
            .filter(|(_, inst)| inst.workflow_name == name_bytes)
            .map(|(&id, inst)| InstanceInfo {
                instance_id: id,
                workflow_name: String::from_utf8_lossy(&inst.workflow_name).to_string(),
                current_step: inst.current_step,
                status: inst.status.to_string(),
            })
            .collect()
    }

    /// Tick — check for timed-out steps and overall timeouts.
    /// Returns list of (conn_id, error_frame) for timed-out instances.
    pub fn tick(&self) -> Vec<(u64, Bytes)> {
        let mut inner = self.inner.lock();
        let mut errors = Vec::new();

        // First pass: find timed-out instances (immutable borrows only).
        let timed_out: Vec<(u32, u16)> = inner
            .instances
            .iter()
            .filter(|(_, inst)| inst.status == InstanceStatus::Running)
            .filter_map(|(&id, inst)| {
                let since = inst.running_since?;
                let slot = inner.slots.get(&inst.workflow_name)?;
                let step_idx = inst.current_step as usize;
                if step_idx < slot.steps.len() {
                    let step_timeout = slot.steps[step_idx].timeout_ms;
                    if step_timeout > 0
                        && since.elapsed() > Duration::from_millis(step_timeout as u64)
                    {
                        return Some((id, inst.current_step));
                    }
                }
                if slot.timeout_ms > 0
                    && inst.created_at.elapsed()
                        > Duration::from_millis(slot.timeout_ms as u64)
                {
                    return Some((id, inst.current_step));
                }
                None
            })
            .collect();

        // Second pass: mutate timed-out instances.
        for (id, step) in timed_out {
            let name = {
                let inst = match inner.instances.get_mut(&id) {
                    Some(i) => i,
                    None => continue,
                };
                inst.status = InstanceStatus::Failed;
                inst.running_conn = None;
                inst.running_since = None;
                inst.workflow_name.clone()
            };

            if let Some(conn_id) = inner.slots.get_mut(&name).and_then(|s| s.next_worker()) {
                let error_json = serde_json::to_vec(&serde_json::json!({
                    "instance_id": id,
                    "step": step,
                    "message": "step timed out"
                }))
                .unwrap_or_default();

                let frame = encode_workflow_error(0, &name, id, &error_json);
                errors.push((conn_id, frame));
            }

            warn!(instance_id = id, "workflow step timed out");
        }

        errors
    }

    /// Remove a connection from ALL workflow slots. Called on disconnect.
    pub fn remove_connection(&self, conn_id: u64) {
        let mut inner = self.inner.lock();

        // Remove conn from all slots; remove empty slots.
        inner.slots.retain(|name, slot| {
            slot.remove_connection(conn_id);
            if slot.connections.is_empty() {
                debug!(name = %String::from_utf8_lossy(name), "workflow removed (no workers)");
                false
            } else {
                true
            }
        });

        // Cancel instances whose running connection was this one.
        for inst in inner.instances.values_mut() {
            if inst.running_conn == Some(conn_id) && inst.status == InstanceStatus::Running {
                inst.running_conn = None;
                inst.running_since = None;
                // Don't fail the instance — try to reassign on next tick
                // if the workflow slot still has workers. For now, leave
                // it in Running state with no conn.
            }
        }
    }
}

// ── workflow_engine_loop ───────────────────────────────────────────────────

/// Background task that ticks the workflow registry every 100ms checking
/// for step timeouts and sends WorkflowError frames to the chosen worker
/// connections.
pub async fn workflow_engine_loop(
    registry: std::sync::Arc<WorkflowRegistry>,
    connections: ConnectionRegistry,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown.changed() => {
                info!("workflow_engine_loop shutting down");
                return;
            }
        }

        let errors = registry.tick();
        for (conn_id, frame) in errors {
            connections.send_bytes(conn_id, frame);
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Simple subject matching: exact match or wildcard `>` at the end.
fn subject_matches(pattern: &str, subject: &[u8]) -> bool {
    let subject_str = std::str::from_utf8(subject).unwrap_or("");

    if pattern == ">" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(".>") {
        return subject_str.starts_with(prefix);
    }
    pattern == subject_str
}

/// Extract a dedup value from JSON context using a simple key path.
fn extract_dedup_value(context: &[u8], key: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(context).ok()?;
    let result = value.get(key)?;
    match result {
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}
