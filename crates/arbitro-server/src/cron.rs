//! Cron subsystem — in-memory scheduled job registry.
//!
//! Cron jobs live entirely in memory. When a client calls `CreateCron`,
//! the broker registers the schedule and the originating connection.
//! A background task (`cron_loop`) ticks every second, fires mature
//! jobs by sending a `CronFire` frame to exactly one registered worker
//! (round-robin), and waits for a `CronAck` before allowing the next
//! fire.
//!
//! Multiple connections can register the same cron name — the name is
//! the dedup key. The broker picks one worker per fire (queue semantics).
//! On disconnect, the connection is removed from the worker list. If
//! the list empties, the slot is removed.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use croner::Cron;
use parking_lot::Mutex;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::transport::registry::ConnectionRegistry;
use arbitro_proto::wire::cron::{encode_cron_fire, CronInfo};

use chrono::Utc;

/// SEC-7: hard cap on distinct cron names in the registry, to bound
/// memory in the face of a client hammering CreateCron with unique names.
const MAX_CRON_SLOTS: usize = 10_000;
/// SEC-7: hard cap on worker connections registered against a single
/// cron slot.
const MAX_WORKERS_PER_SLOT: usize = 1_000;

/// ROB-20: default handler timeout applied when the client requests
/// `timeout_ms == 0` (no timeout). Without this, a stuck worker holds
/// `running = true` forever and starves the slot's fire cycle.
const DEFAULT_TIMEOUT_MS: u32 = 3_600_000; // 1 hour

// ── CronSlot ────────────────────────────────────────────────────────────────

/// A single cron schedule with its worker pool.
struct CronSlot {
    /// Parsed cron expression.
    cron: Cron,
    /// Original cron expression string (for ListCrons).
    every: String,
    /// Optional timezone string.
    tz: Option<String>,
    /// Handler timeout (0 = no timeout).
    timeout_ms: u32,
    /// Whether concurrent fires are allowed.
    overlap: bool,
    /// Registered worker connections.
    connections: Vec<u64>,
    /// Round-robin cursor.
    cursor: usize,
    /// Whether a fire is currently in-flight (awaiting CronAck).
    running: bool,
    /// When the running fire was sent (for timeout).
    running_since: Option<Instant>,
    /// Connection that received the current in-flight fire.
    running_conn: Option<u64>,
    /// Next scheduled fire time.
    next_fire: Option<Instant>,
    /// Monotonic fire counter.
    fire_count: u64,
    /// Whether the cron is paused.
    paused: bool,
}

impl CronSlot {
    fn new(
        cron: Cron,
        every: String,
        tz: Option<String>,
        timeout_ms: u32,
        overlap: bool,
        conn_id: u64,
    ) -> Self {
        // ROB-20: timeout_ms == 0 means "no timeout" on the wire, but
        // that lets a stuck worker wedge the slot forever. Enforce a
        // sane default instead.
        let timeout_ms = if timeout_ms == 0 {
            DEFAULT_TIMEOUT_MS
        } else {
            timeout_ms
        };
        let mut slot = Self {
            cron,
            every,
            tz,
            timeout_ms,
            overlap,
            connections: vec![conn_id],
            cursor: 0,
            running: false,
            running_since: None,
            running_conn: None,
            next_fire: None,
            fire_count: 0,
            paused: false,
        };
        slot.schedule_next();
        slot
    }

    /// Compute the next fire instant from now.
    fn schedule_next(&mut self) {
        let now_chrono = Utc::now();
        match self.cron.find_next_occurrence(&now_chrono, false) {
            Ok(next) => {
                let delta = next.signed_duration_since(now_chrono);
                let dur_ms = delta.num_milliseconds().max(100) as u64;
                self.next_fire = Some(Instant::now() + Duration::from_millis(dur_ms));
            }
            Err(_) => {
                self.next_fire = None;
            }
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
        // If the running fire was assigned to this connection, clear running.
        if self.running_conn == Some(conn_id) {
            self.running = false;
            self.running_since = None;
            self.running_conn = None;
        }
    }

    /// Check if the running fire has timed out.
    fn check_timeout(&mut self) -> bool {
        if !self.running || self.timeout_ms == 0 {
            return false;
        }
        if let Some(since) = self.running_since {
            if since.elapsed() > Duration::from_millis(self.timeout_ms as u64) {
                warn!(
                    name = %self.every,
                    conn = ?self.running_conn,
                    "cron fire timed out, clearing running state"
                );
                self.running = false;
                self.running_since = None;
                self.running_conn = None;
                return true;
            }
        }
        false
    }
}

// ── CronRegistry ────────────────────────────────────────────────────────────

/// Thread-safe registry of all active cron jobs.
pub struct CronRegistry {
    inner: Mutex<HashMap<Bytes, CronSlot>>,
}

impl Default for CronRegistry {
    fn default() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl CronRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a cron job. If the name already exists, just add the
    /// connection to the worker pool.
    pub fn create(
        &self,
        name: Bytes,
        every: &str,
        tz: Option<String>,
        timeout_ms: u32,
        overlap: bool,
        conn_id: u64,
    ) -> Result<(), String> {
        let cron = Cron::new(every)
            .with_seconds_optional()
            .parse()
            .map_err(|e| format!("invalid cron expression: {e}"))?;

        let mut map = self.inner.lock();
        if let Some(slot) = map.get_mut(&name) {
            // Name exists — add connection as another worker.
            if !slot.connections.contains(&conn_id) {
                // SEC-7: bound the worker pool per slot.
                if slot.connections.len() >= MAX_WORKERS_PER_SLOT {
                    return Err(format!(
                        "cron '{}' already has the maximum of {} workers",
                        String::from_utf8_lossy(&name),
                        MAX_WORKERS_PER_SLOT
                    ));
                }
                slot.connections.push(conn_id);
                debug!(name = %String::from_utf8_lossy(&name), conn_id, "cron worker added");
            }
        } else {
            // SEC-7: bound the total number of distinct cron slots.
            if map.len() >= MAX_CRON_SLOTS {
                return Err(format!(
                    "cron registry is at the maximum of {} slots",
                    MAX_CRON_SLOTS
                ));
            }
            info!(name = %String::from_utf8_lossy(&name), every, "cron created");
            map.insert(
                name,
                CronSlot::new(cron, every.to_string(), tz, timeout_ms, overlap, conn_id),
            );
        }
        Ok(())
    }

    /// Delete a cron job entirely (not just remove a worker).
    pub fn delete(&self, name: &[u8]) -> bool {
        let mut map = self.inner.lock();
        let existed = map.remove(name).is_some();
        if existed {
            info!(name = %String::from_utf8_lossy(name), "cron deleted");
        }
        existed
    }

    /// List all active cron jobs.
    pub fn list(&self) -> Vec<CronInfo> {
        let map = self.inner.lock();
        map.iter()
            .map(|(name, slot)| CronInfo {
                name: String::from_utf8_lossy(name).to_string(),
                every: slot.every.clone(),
                tz: slot.tz.clone(),
                workers: slot.connections.len() as u32,
                paused: slot.paused,
            })
            .collect()
    }

    /// Remove a connection from ALL cron slots. Called on disconnect.
    pub fn remove_connection(&self, conn_id: u64) {
        let mut map = self.inner.lock();
        // Remove conn from all slots; remove empty slots.
        map.retain(|name, slot| {
            slot.remove_connection(conn_id);
            if slot.connections.is_empty() {
                debug!(name = %String::from_utf8_lossy(name), "cron removed (no workers)");
                false
            } else {
                true
            }
        });
    }

    /// Acknowledge a cron fire. Called when CronAck arrives.
    pub fn ack(&self, name: &[u8], _ok: bool) {
        let mut map = self.inner.lock();
        if let Some(slot) = map.get_mut(name) {
            slot.running = false;
            slot.running_since = None;
            slot.running_conn = None;
        }
    }

    /// Tick — called every second. Returns list of (conn_id, fire_frame)
    /// for fires that should be sent.
    pub fn tick(&self) -> Vec<(u64, Bytes)> {
        let now = Instant::now();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut fires = Vec::new();
        let mut map = self.inner.lock();

        for (name, slot) in map.iter_mut() {
            // Check timeout on running fires.
            slot.check_timeout();

            // Skip paused.
            if slot.paused {
                continue;
            }

            // Check if it's time to fire.
            let should_fire = match slot.next_fire {
                Some(next) => now >= next,
                None => false,
            };

            if !should_fire {
                continue;
            }

            // Overlap guard. ROB-20: a full interval has now elapsed
            // without an ack for the in-flight fire — force-clear
            // `running` so the slot can't wedge forever regardless of
            // what the worker does with the earlier fire.
            if slot.running && !slot.overlap {
                warn!(
                    name = %String::from_utf8_lossy(name),
                    "cron fire skipped (overlap) — next interval elapsed, clearing running state"
                );
                slot.running = false;
                slot.running_since = None;
                slot.running_conn = None;
                slot.schedule_next();
                continue;
            }

            // Pick a worker.
            if let Some(conn_id) = slot.next_worker() {
                slot.fire_count += 1;
                slot.running = true;
                slot.running_since = Some(now);
                slot.running_conn = Some(conn_id);

                let frame = encode_cron_fire(
                    0, // seq — server-originated
                    name,
                    now_ms,
                    slot.fire_count,
                );
                fires.push((conn_id, frame));

                debug!(
                    name = %String::from_utf8_lossy(name),
                    conn_id,
                    fire_count = slot.fire_count,
                    "cron fired"
                );
            }

            slot.schedule_next();
        }

        fires
    }
}

// ── cron_loop ───────────────────────────────────────────────────────────────

/// Background task that ticks the cron registry every second and sends
/// CronFire frames to the chosen worker connections.
pub async fn cron_loop(
    registry: std::sync::Arc<CronRegistry>,
    connections: ConnectionRegistry,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown.changed() => {
                info!("cron_loop shutting down");
                return;
            }
        }

        let fires = registry.tick();
        for (conn_id, frame) in fires {
            connections.send_bytes(conn_id, frame);
        }
    }
}
