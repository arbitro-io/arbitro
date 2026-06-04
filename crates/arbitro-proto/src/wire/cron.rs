//! Cron wire frames — cold path, JSON-encoded bodies.
//!
//! All cron frames use the standard Envelope prefix. The body after the
//! envelope is a JSON object for CreateCron / ListCrons replies, or a
//! minimal fixed layout for CronFire / CronAck.

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use zerocopy::IntoBytes;

use crate::action::Action;
use crate::v2::header::{Header, HEADER_SIZE};

// ── CreateCron ──────────────────────────────────────────────────────────────

/// JSON body for CreateCron frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCronBody {
    pub name: String,
    pub every: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tz: Option<String>,
    /// Handler timeout in milliseconds. 0 = no timeout.
    #[serde(default)]
    pub timeout_ms: u32,
    /// Whether concurrent fires are allowed. Default: false (skip if running).
    #[serde(default)]
    pub overlap: bool,
}

/// Encode a CreateCron frame (Envelope + JSON body).
pub fn encode_create_cron(seq: u64, body: &CreateCronBody) -> Bytes {
    let json = serde_json::to_vec(body).expect("CreateCronBody is always serializable");
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + json.len());
    let env = Header::new(Action::CreateCron.as_u16(), json.len() as u32, seq);
    buf.put_slice(env.as_bytes());
    buf.put_slice(&json);
    buf.freeze()
}

/// Decode the JSON body from a CreateCron frame (after envelope has been parsed).
pub fn decode_create_cron(body: &[u8]) -> Result<CreateCronBody, serde_json::Error> {
    serde_json::from_slice(body)
}

// ── DeleteCron ──────────────────────────────────────────────────────────────

/// Encode a DeleteCron frame. Body = cron name bytes (no JSON needed).
pub fn encode_delete_cron(seq: u64, name: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + name.len());
    let env = Header::new(Action::DeleteCron.as_u16(), name.len() as u32, seq);
    buf.put_slice(env.as_bytes());
    buf.put_slice(name);
    buf.freeze()
}

// ── ListCrons ───────────────────────────────────────────────────────────────

/// Encode a ListCrons request frame. No body.
pub fn encode_list_crons(seq: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_SIZE);
    let env = Header::new(Action::ListCrons.as_u16(), 0, seq);
    buf.put_slice(env.as_bytes());
    buf.freeze()
}

/// Single cron entry in a ListCrons response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronInfo {
    pub name: String,
    pub every: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tz: Option<String>,
    pub workers: u32,
    pub paused: bool,
}

// ── CronFire (broker → client) ──────────────────────────────────────────────

/// Fixed layout for CronFire body:
/// ```text
/// [2 name_len][8 fire_time_ms][8 fire_count][name...]
/// ```
pub const CRON_FIRE_FIXED: usize = 2 + 8 + 8; // 18 bytes

/// Encode a CronFire frame.
pub fn encode_cron_fire(seq: u64, name: &[u8], fire_time_ms: u64, fire_count: u64) -> Bytes {
    let body_len = CRON_FIRE_FIXED + name.len();
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + body_len);
    let env = Header::new(Action::CronFire.as_u16(), body_len as u32, seq);
    buf.put_slice(env.as_bytes());
    buf.put_u16_le(name.len() as u16);
    buf.put_u64_le(fire_time_ms);
    buf.put_u64_le(fire_count);
    buf.put_slice(name);
    buf.freeze()
}

/// Decoded CronFire body.
#[derive(Debug, Clone)]
pub struct CronFireView<'a> {
    pub name: &'a [u8],
    pub fire_time_ms: u64,
    pub fire_count: u64,
}

/// Decode a CronFire body (after envelope).
pub fn decode_cron_fire(body: &[u8]) -> Option<CronFireView<'_>> {
    if body.len() < CRON_FIRE_FIXED {
        return None;
    }
    let name_len = u16::from_le_bytes([body[0], body[1]]) as usize;
    let fire_time_ms = u64::from_le_bytes(body[2..10].try_into().ok()?);
    let fire_count = u64::from_le_bytes(body[10..18].try_into().ok()?);
    let name = body.get(18..18 + name_len)?;
    Some(CronFireView {
        name,
        fire_time_ms,
        fire_count,
    })
}

// ── CronAck (client → broker) ───────────────────────────────────────────────

/// Fixed layout for CronAck body:
/// ```text
/// [2 name_len][1 status (0=ok, 1=error)][name...]
/// ```
pub const CRON_ACK_FIXED: usize = 2 + 1; // 3 bytes

/// Encode a CronAck frame.
pub fn encode_cron_ack(seq: u64, name: &[u8], ok: bool) -> Bytes {
    let body_len = CRON_ACK_FIXED + name.len();
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + body_len);
    let env = Header::new(Action::CronAck.as_u16(), body_len as u32, seq);
    buf.put_slice(env.as_bytes());
    buf.put_u16_le(name.len() as u16);
    buf.put_u8(if ok { 0 } else { 1 });
    buf.put_slice(name);
    buf.freeze()
}

/// Decoded CronAck body.
#[derive(Debug, Clone)]
pub struct CronAckView<'a> {
    pub name: &'a [u8],
    pub ok: bool,
}

/// Decode a CronAck body (after envelope).
pub fn decode_cron_ack(body: &[u8]) -> Option<CronAckView<'_>> {
    if body.len() < CRON_ACK_FIXED {
        return None;
    }
    let name_len = u16::from_le_bytes([body[0], body[1]]) as usize;
    let ok = body[2] == 0;
    let name = body.get(3..3 + name_len)?;
    Some(CronAckView { name, ok })
}
