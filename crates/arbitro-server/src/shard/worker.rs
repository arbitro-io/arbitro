//! Shard worker — owns an `ArbitroEngine` and a single store on a
//! dedicated OS thread.
//!
//! Single store per shard (stream-agnostic). One cursor `u64` tracks
//! drain progress. No async, no locks — pure `&mut engine` on its
//! own thread.
//!
//! **Loop structure** — two-phase design that isolates drain from overhead:
//!
//! 1. **DRAIN PHASE** — tight `while gate.is_open()` inner loop.
//!    `drain_cycle` runs back-to-back with ZERO overhead between cycles.
//!    Exits only when gate locks (nothing to deliver) or cursor stalls
//!    (downstream backpressure).
//!
//! 2. **IDLE PHASE** — reached only after drain exits.
//!    Commands, accumulator flush, shutdown check, park.
//!    All management overhead lives here, never in the drain path.
//!
//! Handlers live in sibling `handlers.rs`. Drain logic in `drain.rs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arbitro_engine_v2::types::*;
use arbitro_engine_v2::{ArbitroEngine, DeltaEvents};
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::common::Gate;
use crate::shard::command::*;
use crate::shard::router::SharedStore;
use crate::transport::ConnectionRegistry;

// ── Cross-handler private types ──────────────────────────────────────────────

/// A bound consumer↔connection pair for delivery.
///
/// Created by `handle_subscribe` / `handle_bind`, iterated by
/// `drain::drain_cycle`, filtered on unsubscribe / delete.
pub(super) struct ActiveBinding {
    pub(super) binding_id: BindingId,
    pub(super) connection_id: ConnectionId,
    pub(super) consumer_id: ConsumerId,
    pub(super) stream_id: StreamId,
    /// Configured `max_inflight` cached at subscribe time. The drain hot
    /// loop passes this to `engine.consumer_has_capacity()` (~3 ns) instead
    /// of re-reading the catalog (~20 ns). Updated only on resubscribe.
    pub(super) max_inflight: u32,
    /// `AckPolicy::None` — skip inflight tracking and capacity checks in
    /// the drain hot path. Eliminates ~23 ns/msg (Vec read + comparison +
    /// inflight inc + pending push) that serve no purpose without acks.
    pub(super) fire_and_forget: bool,
    /// Cached from `engine.is_paused()` — avoids HashMap lookup per entry
    /// in the drain inner loop (~10 ns → ~1 ns). Updated by
    /// `handle_pause_consumer` / `handle_resume_consumer`.
    pub(super) paused: bool,
    /// Cached write-channel sender — cloned once from the registry at
    /// subscribe time (~26 ns). The drain uses `tx.try_send()` (~3 ns)
    /// for delivery, bypassing the registry Mutex entirely on the hot path.
    pub(super) tx: mpsc::Sender<Bytes>,
}

// ── Accumulator private types ────────────────────────────────────────────────

/// Flusher configuration — controls when accumulated entries are flushed.
pub(super) struct FlusherConfig {
    /// Flush immediately when accumulated entry count reaches this.
    pub(super) max_size: usize,
    /// Flush immediately when accumulated bytes (subject + payload) reach this.
    pub(super) max_bytes: usize,
    /// Milliseconds of silence after last entry before flushing.
    pub(super) interval_ms: u64,
}

impl Default for FlusherConfig {
    fn default() -> Self {
        Self {
            max_size: 1024,
            max_bytes: 4 * 1024 * 1024, // 4 MB
            interval_ms: 5,
        }
    }
}

/// Tracks who to reply to after an accumulated flush.
pub(super) struct AccumCaller {
    pub(super) conn_id: u64,
    pub(super) env_seq: u32,
    pub(super) entry_count: u32,
}

/// Per-stream accumulation buffer.
pub(super) struct StreamAccum {
    pub(super) store_entries: Vec<PublishEntryOwned>,
    pub(super) callers: Vec<AccumCaller>,
    pub(super) bytes: usize,
}

// ── Shard worker ─────────────────────────────────────────────────────────────

/// A shard worker that exclusively owns an `ArbitroEngine` and a single
/// stream-agnostic store.
///
/// All fields are `pub(super)` so sibling handler/drain modules can extend the
/// worker while respecting crate privacy.
pub struct ShardWorker {
    pub(super) engine: ArbitroEngine,
    /// Shared store — publish writes (from dispatch), drain reads.
    pub(super) store: SharedStore,
    /// Drain cursor — the last seq processed. Drain walks from cursor+1.
    pub(super) cursor: u64,
    /// Lowest seq that was skipped during drain (capacity/subject/paused).
    /// On ack/nack the cursor rewinds here so skipped entries get revisited.
    pub(super) rewind_cursor: Option<u64>,
    pub(super) rx: mpsc::Receiver<ShardCommand>,
    pub(super) gate: Arc<Gate>,
    pub(super) registry: ConnectionRegistry,
    /// Active bindings — iterated when gate is open to deliver.
    pub(super) bindings: Vec<ActiveBinding>,
    /// Data directory for disk-backed stores. None = memory only.
    pub(super) data_dir: Option<String>,
    /// Pre-allocated scratch buffers for the drain hot path — zero
    /// steady-state allocations after warmup.
    pub(super) drain_scratch: super::drain::DrainScratch,
    // Flusher for PublishAccumulate — batches individual publishes
    pub(super) flusher_config: FlusherConfig,
    pub(super) accum_streams: HashMap<StreamId, StreamAccum>,
    /// Deadline = last entry arrival + interval_ms. None = timer stopped.
    pub(super) accum_deadline: Option<Instant>,
    pub(super) accum_total: usize,
    pub(super) accum_bytes: usize,
    /// Shared name registry for engine-seq → client-wire id translation.
    pub(super) names: Arc<crate::common::NameRegistry>,
    /// Drain configuration — max_feed, max_age, etc.
    pub(super) drain_config: super::drain::DrainConfig,
}

impl ShardWorker {
    /// Create a new shard worker.
    pub fn new(
        engine: ArbitroEngine,
        store: SharedStore,
        rx: mpsc::Receiver<ShardCommand>,
        gate: Arc<Gate>,
        registry: ConnectionRegistry,
        data_dir: Option<String>,
        names: Arc<crate::common::NameRegistry>,
        max_feed_per_cycle: usize,
        drain_batch_size: u16,
    ) -> Self {
        Self {
            engine,
            store,
            cursor: 0,
            rewind_cursor: None,
            rx,
            gate,
            registry,
            bindings: Vec::new(),
            data_dir,
            drain_scratch: super::drain::DrainScratch::new(),
            flusher_config: FlusherConfig::default(),
            accum_streams: HashMap::new(),
            accum_deadline: None,
            accum_total: 0,
            accum_bytes: 0,
            names,
            drain_config: super::drain::DrainConfig {
                max_feed: max_feed_per_cycle,
                max_age_ms: 0, // 0 = no expiration (default)
                batch_size: drain_batch_size,
            },
        }
    }

    /// Run the shard loop — two-phase: drain then idle.
    ///
    /// **Drain phase**: tight `while gate.is_open()` loop that runs
    /// `drain_cycle` back-to-back with zero overhead between cycles.
    /// Exits only when gate locks or cursor stalls on backpressure.
    ///
    /// **Idle phase**: process commands, flush accumulator, check
    /// shutdown, park. All management overhead lives here.
    pub fn run(mut self) {
        self.gate.set_worker(std::thread::current());

        // ── Store init ────────────────────────────────────────────────────
        {
            let mut guard = self.store.lock().unwrap();
            if let Err(e) = guard.init() {
                tracing::error!(error = ?e, "store init failed");
            }
            // Recover cursor from store — drain starts after existing data.
            let info = guard.info();
            if info.last_seq > 0 {
                self.cursor = info.last_seq;
            }
        }

        'outer: loop {
            // ═══════════════════════════════════════════════════════════
            // DRAIN PHASE — tight inner loop, absolutely nothing else.
            // Runs drain_cycle back-to-back while gate is open and the
            // cursor makes progress. Zero overhead between cycles.
            // ═══════════════════════════════════════════════════════════
            while self.gate.is_open() {
                crate::lifecycle_trace!("20_gate_open_detected", 0, 0, "shard");
                // Syscall only when TTL is enabled (max_age_ms > 0).
                let now_ms = if self.drain_config.max_age_ms > 0 {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64
                } else {
                    0
                };
                let prev_cursor = self.cursor;
                let delta = {
                    let guard = self.store.lock().unwrap();
                    super::drain::drain_cycle(
                        &mut self.engine,
                        &**guard,
                        &mut self.cursor,
                        &mut self.rewind_cursor,
                        &self.bindings,
                        &self.gate,
                        &self.names,
                        &self.drain_config,
                        &mut self.drain_scratch,
                        now_ms,
                    )
                };
                self.apply_delta(delta);

                // Backpressure: cursor didn't advance → downstream full.
                // Yield 50µs so the TCP writer thread gets CPU time to
                // drain the channel. Without this, the shard thread
                // busy-loops (drain → idle → gate.is_open() → drain)
                // competing for CPU and starving the writer.
                //
                // Why 50µs: writer coalesces ~64 frames per write_vectored.
                // At ~1µs/syscall, 50µs ≈ 3200 frames drained. Enough to
                // unblock without excessive latency.
                if self.cursor == prev_cursor && self.gate.is_open() {
                    std::thread::park_timeout(
                        std::time::Duration::from_micros(50),
                    );
                    break;
                }
            }

            // ═══════════════════════════════════════════════════════════
            // IDLE PHASE — reached only when drain has no work (gate
            // locked) or stalled on backpressure. All overhead lives
            // here, never inside the drain loop.
            // ═══════════════════════════════════════════════════════════

            // Drain all pending commands (non-blocking).
            while let Ok(cmd) = self.rx.try_recv() {
                crate::lifecycle_trace!("09_worker_try_recv", 0, 0, "shard");
                match cmd {
                    ShardCommand::Shutdown => break 'outer,
                    cmd => self.dispatch_command(cmd),
                }
            }

            // Flush accumulator if thresholds met.
            if self.accum_total > 0 {
                let force = self.accum_total >= self.flusher_config.max_size
                    || self.accum_bytes >= self.flusher_config.max_bytes;
                let expired = self
                    .accum_deadline
                    .is_some_and(|d| Instant::now() >= d);
                if force || expired {
                    self.flush_accumulator();
                }
            }

            // If gate opened during command processing (e.g. subscribe
            // triggered demand_became_available) → re-enter drain now.
            if self.gate.is_open() {
                continue;
            }

            // Truly idle — park until woken by gate.release() or command.
            if self.rx.is_empty() {
                if let Some(deadline) = self.accum_deadline {
                    let remaining =
                        deadline.saturating_duration_since(Instant::now());
                    if !remaining.is_zero() {
                        std::thread::park_timeout(remaining);
                    }
                } else {
                    self.gate.acquire();
                }
            }
        }

        // ── Flush remaining accumulated entries before shutdown ───────────
        self.flush_accumulator();

        // ── Store shutdown ────────────────────────────────────────────────
        if let Err(e) = self.store.lock().unwrap().shutdown() {
            tracing::error!(error = ?e, "store shutdown failed");
        }
    }

    /// Dispatch a single command to its handler.
    ///
    /// Publish is NOT here — it goes directly to the store from the
    /// dispatch layer, bypassing the drain thread entirely.
    fn dispatch_command(&mut self, cmd: ShardCommand) {
        match cmd {
            // Hot path
            ShardCommand::PublishAccumulate(cmd) => self.handle_publish_accumulate(cmd),
            ShardCommand::Ack(cmd) => self.handle_ack(cmd),
            ShardCommand::Nack(cmd) => self.handle_nack(cmd),
            // Admin / cold path
            ShardCommand::Subscribe(cmd) => self.handle_subscribe(cmd),
            ShardCommand::Unsubscribe(cmd) => self.handle_unsubscribe(cmd),
            ShardCommand::CreateStream(cmd) => self.handle_create_stream(cmd),
            ShardCommand::DeleteStream(cmd) => self.handle_delete_stream(cmd),
            ShardCommand::CreateConsumer(cmd) => self.handle_create_consumer(cmd),
            ShardCommand::DeleteConsumer(cmd) => self.handle_delete_consumer(cmd),
            ShardCommand::OpenConnection(cmd) => self.handle_open_connection(cmd),
            ShardCommand::DrainConnection(cmd) => self.handle_drain_connection(cmd),
            ShardCommand::Bind(cmd) => self.handle_bind(cmd),
            ShardCommand::ListStreams(cmd) => self.handle_list_streams(cmd),
            ShardCommand::ListConsumers(cmd) => self.handle_list_consumers(cmd),
            ShardCommand::StoreInfo(cmd) => self.handle_store_info(cmd),
            ShardCommand::PauseConsumer(cmd) => self.handle_pause_consumer(cmd),
            ShardCommand::ResumeConsumer(cmd) => self.handle_resume_consumer(cmd),
            ShardCommand::Shutdown => {} // handled in run loop
        }
    }

    /// React to engine events. Called after any mutation that returns
    /// `DeltaEvents`.
    pub(super) fn apply_delta(&mut self, delta: DeltaEvents) {
        if !delta.demand_became_available.is_empty() {
            self.gate.release();
        }
        for binding_id in &delta.bindings_retired {
            self.bindings.retain(|b| b.binding_id != *binding_id);
        }
    }
}
