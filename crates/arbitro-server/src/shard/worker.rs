//! Shard worker — owns an `ArbitroEngine` and per-stream stores on a
//! dedicated OS thread.
//!
//! Dual-source loop: `try_recv` commands + `gate.is_open` drain delivery.
//! No async, no locks — pure `&mut engine` on its own thread.
//!
//! Handlers live in sibling `roles/*.rs` files, one per role
//! (publisher, accumulator, acker, drainer, seeder, admin). This file owns
//! the struct, its fields, the loop and the command dispatch.
//!
//! See `.agent/rules/roles.md` for the hot/cold path fences.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arbitro_engine_v2::batch::{AckEntry, NackEntry};
use arbitro_engine_v2::plugin::scheduler::ExpiredDeadline;
use arbitro_engine_v2::types::*;
use arbitro_engine_v2::ArbitroEngine;
use arbitro_store::Store;
use bytes::BytesMut;
use tokio::sync::mpsc;

use crate::common::gate::Gate;
use crate::lifecycle_trace;
use crate::shard::command::*;
use crate::transport::ConnectionRegistry;

// ── Cross-role private types ───────────────────────────────────────────────

/// A bound consumer↔connection pair for delivery.
///
/// Created by `admin::handle_subscribe` / `handle_bind`, iterated by
/// `drainer::handle_drain_deliver`, filtered on unsubscribe / delete.
pub(super) struct ActiveBinding {
    pub(super) queue_id: QueueId,
    pub(super) connection_id: ConnectionId,
    pub(super) consumer_id: ConsumerId,
    pub(super) stream_id: StreamId,
    /// Resolved at subscribe time so the drainer hot path can skip the
    /// per-claim subscription/binding edge lookups in the engine.
    pub(super) subscription_id: SubscriptionId,
    pub(super) binding_id: BindingId,
    /// Configured `max_inflight` cached at subscribe time. The drainer hot
    /// loop passes this to `engine.consumer_has_capacity()` (~3 ns) instead
    /// of re-reading the catalog (~20 ns). Updated only on resubscribe.
    pub(super) max_inflight: u32,
    /// `true` when `AckPolicy::None` — the drainer bypasses `engine.claim`
    /// (which creates PendingNodes + edges + inflight) and pops directly
    /// from `ctx.ready`. No tracking, no cleanup cost at delete time.
    pub(super) fire_and_forget: bool,
    /// Mirror of `engine.consumer_paused()` for this consumer. Maintained
    /// by `handle_pause_consumer` / `handle_resume_consumer`. The drainer
    /// uses this to skip a binding entirely without paying for any engine
    /// call when the consumer is paused.
    pub(super) paused: bool,
}

// ── Accumulator private types ─────────────────────────────────────────────

/// Flusher configuration — controls when accumulated entries are flushed.
pub(super) struct FlusherConfig {
    /// Flush immediately when accumulated entry count reaches this.
    pub(super) max_size: usize,
    /// Flush immediately when accumulated bytes (subject + payload) reach this.
    pub(super) max_bytes: usize,
    /// Milliseconds of silence after last entry before flushing.
    /// Timer resets on every new entry. Stops when no data pending.
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

// ── Shard worker ───────────────────────────────────────────────────────────

/// A shard worker that exclusively owns an `ArbitroEngine` and per-stream stores.
///
/// All fields are `pub(super)` so sibling `roles/*.rs` modules can extend the
/// worker with handler `impl` blocks while still respecting crate privacy.
pub struct ShardWorker {
    pub(super) engine: ArbitroEngine,
    pub(super) stores: HashMap<StreamId, Box<dyn Store>>,
    pub(super) rx: mpsc::Receiver<ShardCommand>,
    pub(super) gate: Gate,
    pub(super) registry: ConnectionRegistry,
    /// Active bindings — iterated when gate is open to claim + deliver.
    pub(super) bindings: Vec<ActiveBinding>,
    /// Data directory for disk-backed stores. None = memory only.
    pub(super) data_dir: Option<String>,
    /// Streams that have been seeded from store (avoid double-seeding).
    pub(super) seeded_streams: HashSet<StreamId>,
    /// Last seq published to engine per stream — drainer reads from here.
    pub(super) last_engine_seq: HashMap<StreamId, u64>,
    // Scratch buffers — allocated once, reused
    pub(super) scratch_ack: Vec<AckEntry>,
    pub(super) scratch_nack: Vec<NackEntry>,
    /// Reused per drain_deliver cycle to avoid allocating a `Vec<StreamId>`
    /// from `stores.keys()` on every wakeup.
    pub(super) scratch_stream_ids: Vec<StreamId>,
    /// Hot-path drain scratch: claimed seqs (8 B each, copied out of the
    /// engine's `ScratchReply<ClaimedEntry>` to release the engine borrow).
    pub(super) scratch_seqs: Vec<u64>,
    /// Hot-path drain scratch: assembled `RepBatch` body (envelope+fixed+entries),
    /// reused across cycles. `clear()` keeps capacity → zero alloc steady state.
    pub(super) scratch_batch_body: BytesMut,
    /// Reused per loop iteration to receive expired deadlines from
    /// `engine.tick()`. Engine doesn't currently schedule any deadlines from
    /// its runtime paths, so this is forward-looking scaffolding — the day
    /// the engine wires per-pending timeouts on claim, the worker is already
    /// draining them. Steady-state allocation: zero (clear keeps capacity).
    pub(super) scratch_expired: Vec<ExpiredDeadline>,
    /// Worker-start baseline for the engine timer wheel. The engine's
    /// scheduler initializes `current_tick = 0` and `tick(now_ms)` advances
    /// linearly from there, so passing wall-clock millis would iterate
    /// ~1.7 trillion empty slots on the first call (CPU bomb). We instead
    /// pass `start.elapsed().as_millis()`, which starts at 0 and grows
    /// monotonically — wheel advance is cheap and bounded.
    pub(super) tick_baseline: Instant,
    // Flusher for PublishAccumulate — batches individual publishes
    pub(super) flusher_config: FlusherConfig,
    pub(super) accum_streams: HashMap<StreamId, StreamAccum>,
    /// Deadline = last entry arrival + interval_ms. None = timer stopped (no data).
    pub(super) accum_deadline: Option<Instant>,
    pub(super) accum_total: usize,
    pub(super) accum_bytes: usize,
    /// Shared name registry for engine-seq → client-wire id translation when
    /// the drainer builds outbound Deliver/RepBatch envelopes. The client
    /// matches incoming frames by the wire stream id it computed locally
    /// (`fnv1a_32(name)`); the engine speaks in small sequential ids. The
    /// translation lives here on the cold-ish drain path because every other
    /// alternative (caching wire id per binding, threading it through every
    /// command) requires more invasive plumbing.
    pub(super) names: Arc<crate::common::NameRegistry>,
}

impl ShardWorker {
    /// Create a new shard worker.
    pub fn new(
        engine: ArbitroEngine,
        rx: mpsc::Receiver<ShardCommand>,
        gate: Gate,
        registry: ConnectionRegistry,
        data_dir: Option<String>,
        names: Arc<crate::common::NameRegistry>,
    ) -> Self {
        Self {
            engine,
            stores: HashMap::new(),
            rx,
            gate,
            registry,
            bindings: Vec::new(),
            data_dir,
            seeded_streams: HashSet::new(),
            last_engine_seq: HashMap::new(),
            scratch_ack: Vec::with_capacity(64),
            scratch_nack: Vec::with_capacity(64),
            scratch_stream_ids: Vec::with_capacity(16),
            scratch_seqs: Vec::with_capacity(64),
            scratch_batch_body: BytesMut::with_capacity(64 * 1024),
            scratch_expired: Vec::with_capacity(32),
            tick_baseline: Instant::now(),
            flusher_config: FlusherConfig::default(),
            accum_streams: HashMap::new(),
            accum_deadline: None,
            accum_total: 0,
            accum_bytes: 0,
            names,
        }
    }

    /// Run the shard loop. Dual-source: try_recv commands + gate drain.
    /// Parks when both are idle. Wakes via unpark (command send or gate release).
    pub fn run(mut self) {
        self.gate.set_worker(std::thread::current());

        // ── Store init ─────────────────────────────────────────────────
        for (id, store) in &mut self.stores {
            if let Err(e) = store.init() {
                tracing::error!(stream_id = id.raw(), error = ?e, "store init failed");
            }
        }

        loop {
            // 1. Drain all pending commands (non-blocking)
            let mut got_shutdown = false;
            while let Ok(cmd) = self.rx.try_recv() {
                lifecycle_trace::record("09_worker_try_recv", 0, 0, "shard");
                match cmd {
                    ShardCommand::Shutdown => { got_shutdown = true; break; }
                    cmd => self.dispatch_command(cmd),
                }
            }
            if got_shutdown { break; }

            // 1.5. Advance the engine timer wheel and drain any expired
            //      deadlines. Currently the engine never schedules from its
            //      runtime paths, so `scratch_expired` is always empty —
            //      this is wired so that when the engine eventually plumbs
            //      per-pending timeouts on claim, the worker already routes
            //      them.
            //
            //      IMPORTANT: pass elapsed-since-worker-start, NOT wall
            //      clock. The engine scheduler starts `current_tick = 0`
            //      and the tick loop iterates linearly from there to
            //      `now_ms / tick_ms` — passing wall-clock millis would
            //      spin ~1.7 trillion empty slots on the first call.
            let now_ms = self.tick_baseline.elapsed().as_millis() as u64;
            self.scratch_expired.clear();
            self.engine.tick(now_ms, &mut self.scratch_expired);
            if !self.scratch_expired.is_empty() {
                tracing::warn!(
                    expired = self.scratch_expired.len(),
                    "engine reported expired pending deadlines — server has no follow-up handler yet"
                );
                self.scratch_expired.clear();
            }

            // 2. Flush accumulator: max_size, max_bytes, or interval expired
            if self.accum_total > 0 {
                let force = self.accum_total >= self.flusher_config.max_size
                    || self.accum_bytes >= self.flusher_config.max_bytes;
                let expired = self.accum_deadline
                    .is_some_and(|d| Instant::now() >= d);
                if force || expired {
                    self.flush_accumulator();
                }
            }

            // 3. If gate is open → run delivery
            if self.gate.is_open() {
                lifecycle_trace::record("20_gate_open_detected", 0, 0, "shard");
                self.handle_drain_deliver();
            }

            // 4. Spin-wait then park if nothing to do (both mpsc empty AND gate locked)
            //    When accumulator has pending entries, park with timeout to ensure
            //    flush deadline is met. Otherwise use gate.acquire (spin 512× + park).
            if self.rx.is_empty() && !self.gate.is_open() {
                if let Some(deadline) = self.accum_deadline {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if !remaining.is_zero() {
                        std::thread::park_timeout(remaining);
                    }
                } else {
                    self.gate.acquire();
                }
            }
        }

        // ── Flush remaining accumulated entries before shutdown ────────
        self.flush_accumulator();

        // ── Store shutdown ─────────────────────────────────────────────
        for (id, store) in &mut self.stores {
            if let Err(e) = store.shutdown() {
                tracing::error!(stream_id = id.raw(), error = ?e, "store shutdown failed");
            }
        }
    }

    /// Dispatch a single command to its handler. Each handler lives in
    /// its role file under `shard/roles/`.
    fn dispatch_command(&mut self, cmd: ShardCommand) {
        match cmd {
            // Hot path
            ShardCommand::Publish(cmd) => self.handle_publish(cmd),
            ShardCommand::PublishAccumulate(cmd) => self.handle_publish_accumulate(cmd),
            ShardCommand::Ack(cmd) => self.handle_ack(cmd),
            ShardCommand::Nack(cmd) => self.handle_nack(cmd),
            ShardCommand::Claim(cmd) => self.handle_claim(cmd),
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
            // Seeder (recovery)
            ShardCommand::SeedStores(cmd) => self.handle_seed_stores(cmd),
            // System
            ShardCommand::Shutdown => {} // handled in run loop
        }
    }
}
