//! Workflow wire frames — cold path, JSON-encoded bodies.
//!
//! All workflow frames use the standard v2 Header prefix. The body after
//! the header is JSON for CreateWorkflow / ListWorkflows / ListInstances
//! replies, or a minimal fixed+variable layout for WorkflowStep /
//! WorkflowResult / CancelWorkflow / WorkflowError.

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use zerocopy::IntoBytes;

use crate::action::Action;
use crate::v2::header::{Header, HEADER_SIZE};

// ── CreateWorkflow ─────────────────────────────────────────────────────────

/// A single step definition within a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDef {
    pub name: String,
    /// Step timeout in milliseconds. 0 = no timeout.
    #[serde(default)]
    pub timeout_ms: u32,
    /// Maximum retries for this step. 0 = no retries.
    #[serde(default)]
    pub max_retries: u8,
}

/// Workflow-level configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkflowConfig {
    /// Maximum concurrent instances (0 = unlimited).
    #[serde(default)]
    pub max_concurrent: u32,
    /// JSON key path used for dedup (empty = no dedup).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedup_key: Option<String>,
    /// Overall workflow timeout in milliseconds (0 = no timeout).
    #[serde(default)]
    pub timeout_ms: u32,
}

/// JSON body for CreateWorkflow frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateWorkflowBody {
    pub name: String,
    /// Subject pattern that triggers the workflow (e.g. "orders.created").
    pub trigger: String,
    /// Ordered list of steps.
    pub steps: Vec<StepDef>,
    /// Workflow configuration.
    #[serde(default)]
    pub config: WorkflowConfig,
}

/// Encode a CreateWorkflow frame (Header + JSON body).
pub fn encode_create_workflow(seq: u64, body: &CreateWorkflowBody) -> Bytes {
    let json = serde_json::to_vec(body).expect("CreateWorkflowBody is always serializable");
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + json.len());
    let hdr = Header::new(Action::CreateWorkflow.as_u16(), json.len() as u32, seq);
    buf.put_slice(hdr.as_bytes());
    buf.put_slice(&json);
    buf.freeze()
}

/// Decode the JSON body from a CreateWorkflow frame (after header).
pub fn decode_create_workflow(body: &[u8]) -> Result<CreateWorkflowBody, serde_json::Error> {
    serde_json::from_slice(body)
}

// ── DeleteWorkflow ─────────────────────────────────────────────────────────

/// Encode a DeleteWorkflow frame. Body = workflow name bytes.
pub fn encode_delete_workflow(seq: u64, name: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + name.len());
    let hdr = Header::new(Action::DeleteWorkflow.as_u16(), name.len() as u32, seq);
    buf.put_slice(hdr.as_bytes());
    buf.put_slice(name);
    buf.freeze()
}

// ── ListWorkflows ──────────────────────────────────────────────────────────

/// Encode a ListWorkflows request frame. No body.
pub fn encode_list_workflows(seq: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_SIZE);
    let hdr = Header::new(Action::ListWorkflows.as_u16(), 0, seq);
    buf.put_slice(hdr.as_bytes());
    buf.freeze()
}

/// Single workflow entry in a ListWorkflows response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowInfo {
    pub name: String,
    pub trigger: String,
    pub steps: Vec<String>,
    pub workers: u32,
    pub active_instances: u32,
}

// ── WorkflowStep (broker → client) ────────────────────────────────────────

/// Fixed layout for WorkflowStep body:
/// ```text
/// [2 name_len][4 instance_id][2 step_index][name...][context_json...]
/// ```
pub const WORKFLOW_STEP_FIXED: usize = 2 + 4 + 2; // 8 bytes

/// Encode a WorkflowStep frame.
pub fn encode_workflow_step(
    seq: u64,
    name: &[u8],
    instance_id: u32,
    step_index: u16,
    context: &[u8],
) -> Bytes {
    let body_len = WORKFLOW_STEP_FIXED + name.len() + context.len();
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + body_len);
    let hdr = Header::new(Action::WorkflowStep.as_u16(), body_len as u32, seq);
    buf.put_slice(hdr.as_bytes());
    buf.put_u16_le(name.len() as u16);
    buf.put_u32_le(instance_id);
    buf.put_u16_le(step_index);
    buf.put_slice(name);
    buf.put_slice(context);
    buf.freeze()
}

/// Decoded WorkflowStep body.
#[derive(Debug, Clone)]
pub struct WorkflowStepView<'a> {
    pub name: &'a [u8],
    pub instance_id: u32,
    pub step_index: u16,
    pub context: &'a [u8],
}

/// Decode a WorkflowStep body (after header).
pub fn decode_workflow_step(body: &[u8]) -> Option<WorkflowStepView<'_>> {
    if body.len() < WORKFLOW_STEP_FIXED {
        return None;
    }
    let name_len = u16::from_le_bytes([body[0], body[1]]) as usize;
    let instance_id = u32::from_le_bytes(body[2..6].try_into().ok()?);
    let step_index = u16::from_le_bytes([body[6], body[7]]);
    let name = body.get(8..8 + name_len)?;
    let context = body.get(8 + name_len..)?;
    Some(WorkflowStepView { name, instance_id, step_index, context })
}

// ── WorkflowResult (client → broker) ──────────────────────────────────────

/// Fixed layout for WorkflowResult body:
/// ```text
/// [2 name_len][4 instance_id][1 status (0=ok, 1=error)][name...][context_json...]
/// ```
pub const WORKFLOW_RESULT_FIXED: usize = 2 + 4 + 1; // 7 bytes

/// Encode a WorkflowResult frame.
pub fn encode_workflow_result(
    seq: u64,
    name: &[u8],
    instance_id: u32,
    ok: bool,
    context: &[u8],
) -> Bytes {
    let body_len = WORKFLOW_RESULT_FIXED + name.len() + context.len();
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + body_len);
    let hdr = Header::new(Action::WorkflowResult.as_u16(), body_len as u32, seq);
    buf.put_slice(hdr.as_bytes());
    buf.put_u16_le(name.len() as u16);
    buf.put_u32_le(instance_id);
    buf.put_u8(if ok { 0 } else { 1 });
    buf.put_slice(name);
    buf.put_slice(context);
    buf.freeze()
}

/// Decoded WorkflowResult body.
#[derive(Debug, Clone)]
pub struct WorkflowResultView<'a> {
    pub name: &'a [u8],
    pub instance_id: u32,
    pub ok: bool,
    pub context: &'a [u8],
}

/// Decode a WorkflowResult body (after header).
pub fn decode_workflow_result(body: &[u8]) -> Option<WorkflowResultView<'_>> {
    if body.len() < WORKFLOW_RESULT_FIXED {
        return None;
    }
    let name_len = u16::from_le_bytes([body[0], body[1]]) as usize;
    let instance_id = u32::from_le_bytes(body[2..6].try_into().ok()?);
    let ok = body[6] == 0;
    let name = body.get(7..7 + name_len)?;
    let context = body.get(7 + name_len..)?;
    Some(WorkflowResultView { name, instance_id, ok, context })
}

// ── CancelWorkflow ─────────────────────────────────────────────────────────

/// Encode a CancelWorkflow frame. Body = instance_id (4 bytes).
pub fn encode_cancel_workflow(seq: u64, instance_id: u32) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + 4);
    let hdr = Header::new(Action::CancelWorkflow.as_u16(), 4, seq);
    buf.put_slice(hdr.as_bytes());
    buf.put_u32_le(instance_id);
    buf.freeze()
}

/// Decode a CancelWorkflow body (after header). Returns instance_id.
pub fn decode_cancel_workflow(body: &[u8]) -> Option<u32> {
    if body.len() < 4 {
        return None;
    }
    Some(u32::from_le_bytes(body[..4].try_into().ok()?))
}

// ── ListInstances ──────────────────────────────────────────────────────────

/// Encode a ListInstances request. Body = workflow name bytes.
pub fn encode_list_instances(seq: u64, name: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + name.len());
    let hdr = Header::new(Action::ListInstances.as_u16(), name.len() as u32, seq);
    buf.put_slice(hdr.as_bytes());
    buf.put_slice(name);
    buf.freeze()
}

/// Single instance entry in a ListInstances response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfo {
    pub instance_id: u32,
    pub workflow_name: String,
    pub current_step: u16,
    pub status: String,
}

// ── WorkflowError (broker → client) ───────────────────────────────────────

/// Fixed layout for WorkflowError body:
/// ```text
/// [2 name_len][4 instance_id][name...][error_json...]
/// ```
pub const WORKFLOW_ERROR_FIXED: usize = 2 + 4; // 6 bytes

/// Encode a WorkflowError frame.
pub fn encode_workflow_error(
    seq: u64,
    name: &[u8],
    instance_id: u32,
    error_json: &[u8],
) -> Bytes {
    let body_len = WORKFLOW_ERROR_FIXED + name.len() + error_json.len();
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + body_len);
    let hdr = Header::new(Action::WorkflowError.as_u16(), body_len as u32, seq);
    buf.put_slice(hdr.as_bytes());
    buf.put_u16_le(name.len() as u16);
    buf.put_u32_le(instance_id);
    buf.put_slice(name);
    buf.put_slice(error_json);
    buf.freeze()
}

/// Decoded WorkflowError body.
#[derive(Debug, Clone)]
pub struct WorkflowErrorView<'a> {
    pub name: &'a [u8],
    pub instance_id: u32,
    pub error_json: &'a [u8],
}

/// Decode a WorkflowError body (after header).
pub fn decode_workflow_error(body: &[u8]) -> Option<WorkflowErrorView<'_>> {
    if body.len() < WORKFLOW_ERROR_FIXED {
        return None;
    }
    let name_len = u16::from_le_bytes([body[0], body[1]]) as usize;
    let instance_id = u32::from_le_bytes(body[2..6].try_into().ok()?);
    let name = body.get(6..6 + name_len)?;
    let error_json = body.get(6 + name_len..)?;
    Some(WorkflowErrorView { name, instance_id, error_json })
}
