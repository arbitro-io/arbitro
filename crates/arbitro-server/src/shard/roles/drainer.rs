//! Drainer role — the worker that touches the journal to extract messages
//! and ships them as `RepBatch` frames to subscribed consumers.
//!
//! Per `.agent/rules/roles.md` DRAINER MUST — in this exact order:
//!   1. Guard 0 (first statement): `bindings.is_empty()` → `gate.lock` + return.
//!   2. Feed engine only for streams with ≥1 binding, up to
//!      `store.info().last_seq`. First time → seeder path. Otherwise →
//!      incremental feed from `last_engine_seq+1`. Both preserve store seqs.
//!   3. Per binding: loop `claim(64)` → `store.get(seq)` → `send_deliver_frame`.
//!   4. Close cycle: delivered something → `gate.release()`; nothing → `gate.lock()`.
//!
//! MUST NOT: reply to publishes, do engine work for streams with no bindings,
//! block network I/O, mutate `bindings`/store/catalog.
//!
//! Invariant: no speculative work. Past `bindings.is_empty()`, every stream
//! touched has ≥1 destinatario.

use arbitro_engine_v2::batch::ClaimBatch;
use arbitro_engine_v2::types::Timestamp;
use arbitro_proto::action::Action;
use arbitro_proto::wire::delivery::{DeliveryEntryHeader, RepBatchFixed};
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use tokio::sync::mpsc;

use crate::shard::worker::ShardWorker;

impl ShardWorker {
    /// Feed pending journal entries into the engine so they become claimable.
    ///
    /// Per `roles.md` DRAINER MUST #2: incremental feed uses `enqueue_ready`
    /// (preserves store seqs) — never `engine.publish` (which reassigns seqs
    /// from `ctx.next_seq` and is fragile under any desync). The seeder owns
    /// the initial bulk load for unseeded streams; this function handles the
    /// ongoing delta from `last_engine_seq+1` to `store.info().last_seq`.
    ///
    /// Skips streams with no active bindings per the "no speculative engine
    /// work" invariant.
    pub(in crate::shard) fn publish_pending_to_engine(&mut self, _now: Timestamp) {
        crate::lifecycle_trace!("p01_ppte_enter", 0, 0, "shard");
        // Reuse scratch buffer instead of allocating per call.
        self.scratch_stream_ids.clear();
        self.scratch_stream_ids.extend(self.stores.keys().copied());
        crate::lifecycle_trace!("p02_ppte_keys_collected", self.scratch_stream_ids.len() as u64, 0, "shard");

        for i in 0..self.scratch_stream_ids.len() {
            let stream_id = self.scratch_stream_ids[i];

            // No bindings on this stream → nothing will consume; skip.
            if !self.bindings.iter().any(|b| b.stream_id == stream_id) {
                continue;
            }
            crate::lifecycle_trace!("p03_binding_check_pass", stream_id.raw() as u64, 0, "shard");
            let last = self.last_engine_seq.get(&stream_id).copied().unwrap_or(0);
            let info = self.stores[&stream_id].info();
            crate::lifecycle_trace!("p04_store_info_done", info.last_seq, last, "shard");
            if info.last_seq <= last { continue; }

            let start = last + 1;
            let cap = self.max_feed_per_cycle as u64;
            let end = (start + cap).min(info.last_seq + 1);

            // Temporarily remove store to avoid borrow conflict with self.engine
            let store = self.stores.remove(&stream_id).unwrap();
            crate::lifecycle_trace!("p05_store_removed", stream_id.raw() as u64, end - start, "shard");

            let engine = &mut self.engine;
            let mut fed_last: u64 = last;
            let mut fed_entries: u64 = 0;
            let mut fed_no_match: u64 = 0;
            let mut fed_queues: u64 = 0;
            store.for_each(start, end, &mut |entry| {
                let subject_hash = arbitro_engine_v2::catalog::fnv1a_32(entry.subject);
                let pushed = engine.enqueue_ready(stream_id, entry.subject, subject_hash, entry.seq);
                fed_entries += 1;
                if pushed == 0 { fed_no_match += 1; } else { fed_queues += pushed as u64; }
                fed_last = entry.seq;
            }).ok();
            engine.flush_seed_metrics(fed_entries, fed_no_match, fed_queues);
            crate::lifecycle_trace!("p06_for_each_done", stream_id.raw() as u64, end - start, "shard");

            self.stores.insert(stream_id, store);
            self.last_engine_seq.insert(stream_id, fed_last);
            crate::lifecycle_trace!("p07_store_reinserted", stream_id.raw() as u64, 0, "shard");

            // Keep ctx.next_seq aligned so any future live publish that goes
            // through engine.publish assigns seqs after the store's tail.
            let next = fed_last + 1;
            if self.engine.ctx().next_seq < next {
                self.engine.ctx_mut().next_seq = next;
            }
        }
        crate::lifecycle_trace!("p08_ppte_exit", 0, 0, "shard");
    }

    /// Iterate all active bindings, claim from engine, read store, build one
    /// `RepBatch` frame per claim (≤ `claim_batch` entries). Loops per binding
    /// until the queue is drained or `max_inflight` is hit.
    ///
    /// Hot-path discipline (`roles.md` DRAINER + `performance.md` rule 21):
    /// * `now` is read **once** per wakeup, not per message.
    /// * Claimed entries are copied as bare seqs (8 B) into `scratch_seqs` —
    ///   no `Vec<ClaimedEntry>::to_vec()` per claim.
    /// * Subject + payload are copied straight from `Store::get` into the
    ///   reusable `scratch_batch_body` buffer; no per-entry frame allocation.
    pub(in crate::shard) fn handle_drain_deliver(&mut self) {
        crate::lifecycle_trace!("21_drainer_enter", 0, self.bindings.len() as u64, "shard");
        // Guard 0 (roles.md DRAINER MUST #1): without destinatarios the drainer
        // does zero work. No engine feed, no store read, no clock syscall.
        if self.bindings.is_empty() {
            self.gate.lock();
            return;
        }

        // Hoist the wall-clock read out of the per-message loop (rule 21).
        let now = Timestamp::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );

        // 1. Feed new journal entries into engine
        crate::lifecycle_trace!("22_feed_engine_start", 0, 0, "shard");
        self.publish_pending_to_engine(now);
        crate::lifecycle_trace!("23_feed_engine_done", 0, 0, "shard");

        let claim_batch = self.max_feed_per_cycle as u16;
        let mut any_delivered = false;
        // True if any binding hit a full claim_batch — meaning the engine
        // probably has more ready entries that we couldn't fit this cycle.
        // Used to decide whether to re-arm the gate (release) or lock it.
        // Without this, every successful drain re-fires the gate, causing
        // a full empty cycle (~5 µs of CPU) immediately after delivery.
        let mut more_pending = false;

        for i in 0..self.bindings.len() {
            let binding = &self.bindings[i];
            let queue_id = binding.queue_id;
            let connection_id = binding.connection_id;
            let consumer_id = binding.consumer_id;
            let stream_id = binding.stream_id;
            let subscription_id = binding.subscription_id;
            let binding_id = binding.binding_id;
            let max_inflight = binding.max_inflight;

            // Pre-filter (cached) — paused: skip without any engine call.
            if binding.paused {
                crate::lifecycle_trace!("24_drain_binding_paused", connection_id.0, stream_id.raw() as u64, "shard");
                continue;
            }
            let fire_and_forget = binding.fire_and_forget;
            // Tracked consumers: skip if saturated (~3 ns). Fire-and-forget
            // never tracks inflight so the check is meaningless.
            if !fire_and_forget && !self.engine.consumer_has_capacity(consumer_id, max_inflight) {
                crate::lifecycle_trace!("24_drain_binding_saturated", connection_id.0, stream_id.raw() as u64, "shard");
                continue;
            }
            crate::lifecycle_trace!("24_drain_binding_start", connection_id.0, stream_id.raw() as u64, "shard");

            // Loop: claim/pop batches until queue empty or inflight limit hit
            loop {
                self.scratch_seqs.clear();

                let max_items: u16 = if fire_and_forget {
                    // Fire-and-forget: pop directly from the ready queue.
                    // No PendingNode, no edges, no inflight tracking — the
                    // engine never learns these seqs were delivered, so
                    // consumer.delete() has zero cleanup cost.
                    crate::lifecycle_trace!("25_pop_start", connection_id.0, claim_batch as u64, "shard");
                    for _ in 0..claim_batch {
                        match self.engine.ctx_mut().ready.pop(queue_id) {
                            Some((_subject_hash, seq)) => self.scratch_seqs.push(seq),
                            None => break,
                        }
                    }
                    claim_batch
                } else {
                    // Tracked consumer: adaptive batch size capped by inflight.
                    let remaining = self
                        .engine
                        .consumer_capacity_remaining(consumer_id, max_inflight);
                    if remaining == 0 {
                        break;
                    }
                    let max = (claim_batch as u32).min(remaining) as u16;
                    crate::lifecycle_trace!("25_claim_start", connection_id.0, max as u64, "shard");
                    {
                        let claimed = self.engine.claim(
                            &ClaimBatch {
                                queue_id,
                                connection_id,
                                consumer_id,
                                max_items: max,
                                now,
                            },
                            subscription_id,
                            binding_id,
                        );
                        self.scratch_seqs.extend(claimed.entries().iter().map(|e| e.seq));
                    }
                    max
                };

                if self.scratch_seqs.is_empty() {
                    crate::lifecycle_trace!("26_claim_empty", connection_id.0, 0, "shard");
                    break;
                }
                let claimed_count = self.scratch_seqs.len();
                crate::lifecycle_trace!("26_claim_done", connection_id.0, claimed_count as u64, "shard");
                any_delivered = true;

                // 2. Build one RepBatch body in the reusable scratch buffer.
                let store = match self.stores.get(&stream_id) {
                    Some(s) => s,
                    None => break,
                };

                // 2. Build frame inline — envelope placeholder + batch header
                //    + entries, then patch envelope. Single buffer, no double copy.
                self.scratch_batch_body.clear();

                // Envelope placeholder (patched after body is complete)
                self.scratch_batch_body.extend_from_slice(&[0u8; ENVELOPE_SIZE]);

                // Batch header
                self.scratch_batch_body.extend_from_slice(
                    RepBatchFixed {
                        consumer_id: U32::new(consumer_id.0),
                        count: U16::new(claimed_count as u16),
                        _pad: U16::new(0),
                    }
                    .as_bytes(),
                );

                crate::lifecycle_trace!("27_store_get_loop_start", connection_id.0, claimed_count as u64, "shard");
                let body = &mut self.scratch_batch_body;
                // Fast path: when the engine returned a contiguous range of
                // seqs (the dominant case in DeliverPolicy::All replay and
                // any single-consumer steady-state drain), `store.for_each`
                // does one `find_lower_bound` (binary search) + a linear
                // walk over the index, instead of `claimed_count` independent
                // `seq_to_idx` lookups. Saves the per-message lookup cost on
                // the hot drain path. Slow path retained verbatim for the
                // sparse case (multi-consumer fan-out where claims interleave).
                let first = self.scratch_seqs[0];
                let last = self.scratch_seqs[claimed_count - 1];
                let contiguous = (last - first + 1) as usize == claimed_count;
                if contiguous {
                    store.for_each(first, last + 1, &mut |entry| {
                        let subj_len = entry.subject.len();
                        let data_len = subj_len + entry.payload.len();
                        let header = DeliveryEntryHeader {
                            seq: U64::new(entry.seq),
                            subj_len: U16::new(subj_len as u16),
                            data_len: U32::new(data_len as u32),
                        };
                        body.extend_from_slice(header.as_bytes());
                        body.extend_from_slice(entry.subject);
                        body.extend_from_slice(entry.payload);
                    }).ok();
                } else {
                    for &seq in &self.scratch_seqs {
                        store.get(seq, &mut |entry| {
                            let subj_len = entry.subject.len();
                            let data_len = subj_len + entry.payload.len();
                            let header = DeliveryEntryHeader {
                                seq: U64::new(seq),
                                subj_len: U16::new(subj_len as u16),
                                data_len: U32::new(data_len as u32),
                            };
                            body.extend_from_slice(header.as_bytes());
                            body.extend_from_slice(entry.subject);
                            body.extend_from_slice(entry.payload);
                        }).ok();
                    }
                }
                crate::lifecycle_trace!("28_store_get_loop_done", connection_id.0, claimed_count as u64, "shard");

                // 3. Patch envelope now that body_len is known.
                let wire_stream_id = self
                    .names
                    .stream_wire(stream_id)
                    .unwrap_or_else(|| stream_id.raw());
                let body_len = self.scratch_batch_body.len() - ENVELOPE_SIZE;
                let envelope = Envelope::new(
                    Action::RepBatch,
                    wire_stream_id,
                    body_len as u32,
                    0,
                );
                self.scratch_batch_body[..ENVELOPE_SIZE]
                    .copy_from_slice(envelope.as_bytes());
                crate::lifecycle_trace!("29_frame_built", connection_id.0, body_len as u64, "shard");
                // Fast-path send via cached tx — bypasses registry Mutex.
                // try_send: ~3 ns (vs 260 ns for mutex+clone+blocking_send).
                // On Full: stop this binding for this cycle (backpressure).
                // On Closed: connection gone, stop draining.
                let frozen = self.scratch_batch_body.split().freeze();
                crate::lifecycle_trace!("30_send_bytes_done", connection_id.0, body_len as u64, "shard");
                match self.bindings[i].tx.try_send(frozen) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Channel full — consumer is slow. Stop this
                        // binding for this cycle; re-arm gate so we
                        // retry next wakeup.
                        more_pending = true;
                        break;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        // Connection gone — stop draining this binding.
                        break;
                    }
                }

                // If claim returned fewer than max_items, queue is drained
                // (or capped by inflight). Either way, no more this cycle.
                if claimed_count < max_items as usize {
                    break;
                }
                // Hit a full batch AND we asked for the full window —
                // engine likely has more ready, re-arm the gate.
                if max_items == claim_batch {
                    more_pending = true;
                }
            }
        }

        if more_pending {
            // Engine still has work we couldn't fit this cycle — re-arm.
            self.gate.release();
            crate::lifecycle_trace!("33_drainer_exit_released", 0, 0, "shard");
        } else {
            // Either delivered everything ready, or nothing was ready.
            // Lock gate until publisher/acker/binder explicitly releases it.
            // Note: any_delivered=true with !more_pending is the steady-state
            // happy path — no need to re-fire and burn an empty cycle.
            let _ = any_delivered;
            self.gate.lock();
            crate::lifecycle_trace!("33_drainer_exit_locked", 0, 0, "shard");
        }
    }
}
