//! Shard command handlers — all commands processed on the command thread.
//!
//! The command thread **owns** the engine (`&mut self`). No Mutex.
//! After engine mutations, handlers update `SharedCounters` atomically
//! and swap `DrainSnapshot` for structural changes.

use std::time::Duration;

use arbitro_engine_v2::command::Command;
use arbitro_engine_v2::types::*;
use arbitro_proto::error::ErrorCode;
use arbitro_store::EntryRef;

use crate::common::reply_v2::{send_error_v2, send_rep_ok_v2};
use crate::shard::command::*;
use crate::shard::worker::{AccumCaller, ActiveBinding, CommandWorker, StreamAccum};

impl CommandWorker {
    // ── Hot path — accumulator ──────────────────────────────────────────

    pub(in crate::shard) fn handle_publish_accumulate(
        &mut self,
        cmd: PublishCmd,
    ) {
        if !self.engine.ctx().catalog.stream_exists(cmd.stream_id) {
            send_error_v2(
                &self.registry,
                cmd.conn_id,
                cmd.env_seq as u64,
                ErrorCode::StreamNotFound,
            );
            return;
        }

        let entry_count = cmd.entries.len() as u32;
        let entry_bytes: usize = cmd
            .entries
            .iter()
            .map(|e| e.subject.len() + e.payload.len())
            .sum();

        let accum = self
            .accum_streams
            .entry(cmd.stream_id)
            .or_insert_with(|| StreamAccum {
                store_entries: Vec::new(),
                callers: Vec::new(),
                bytes: 0,
            });

        accum.store_entries.extend(cmd.entries);
        accum.callers.push(AccumCaller {
            conn_id: cmd.conn_id,
            env_seq: cmd.env_seq,
            entry_count,
        });
        accum.bytes += entry_bytes;

        self.accum_total += entry_count as usize;
        self.accum_bytes += entry_bytes;

        let interval =
            Duration::from_millis(self.flusher_config.interval_ms);
        self.accum_deadline =
            Some(std::time::Instant::now() + interval);

        self.check_accumulator_flush();
    }

    pub(in crate::shard) fn flush_accumulator(&mut self) {
        if self.accum_total == 0 {
            return;
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let stream_ids: Vec<_> =
            self.accum_streams.keys().copied().collect();

        for stream_id in stream_ids {
            let accum =
                self.accum_streams.get_mut(&stream_id).unwrap();
            if accum.store_entries.is_empty() {
                continue;
            }

            let store_entries =
                std::mem::take(&mut accum.store_entries);
            let callers = std::mem::take(&mut accum.callers);
            accum.bytes = 0;

            let refs: Vec<EntryRef<'_>> = store_entries
                .iter()
                .map(|e| EntryRef {
                    stream_id: stream_id.raw(),
                    subject: &e.subject,
                    payload: &e.payload,
                    flags: 0,
                })
                .collect();

            match self.store.lock().unwrap().append_batch(&refs, now_ms) {
                Ok(first_seq) => {
                    let mut seq_offset = 0u64;
                    for caller in &callers {
                        send_rep_ok_v2(
                            &self.registry,
                            caller.conn_id,
                            caller.env_seq as u64,
                            first_seq + seq_offset,
                        );
                        seq_offset += caller.entry_count as u64;
                    }
                    self.gate.release();

                    // Enforce max_msgs / max_bytes capacity limits (FIFO eviction).
                    // Checked after append so callers always get a sequence number.
                    if let Some(ret) = self.stream_retention.get(&stream_id) {
                        let need_check = (ret.max_msgs > 0) || (ret.max_bytes > 0);
                        if need_check {
                            let mut store = self.store.lock().unwrap();
                            let info = store.info();
                            let excess_msgs = if ret.max_msgs > 0 {
                                info.messages.saturating_sub(ret.max_msgs)
                            } else {
                                0
                            };
                            let excess_bytes = if ret.max_bytes > 0 && info.bytes > ret.max_bytes {
                                // Estimate how many leading messages to drop to bring bytes under limit.
                                // Simple heuristic: drop proportionally using average message size.
                                let avg = if info.messages > 0 { info.bytes / info.messages } else { 1 };
                                let over = info.bytes - ret.max_bytes;
                                (over + avg - 1) / avg  // ceiling division
                            } else {
                                0
                            };
                            let excess = excess_msgs.max(excess_bytes);
                            if excess > 0 {
                                let new_first_seq = info.first_seq + excess;
                                store.truncate_front(new_first_seq);
                            }
                        }
                    }
                }
                Err(_) => {
                    for caller in &callers {
                        send_error_v2(
                            &self.registry,
                            caller.conn_id,
                            caller.env_seq as u64,
                            ErrorCode::StreamFull,
                        );
                    }
                }
            }
        }

        self.accum_deadline = None;
        self.accum_total = 0;
        self.accum_bytes = 0;
    }

    // ── Hot path — ack / nack ───────────────────────────────────────────

    pub(in crate::shard) fn handle_ack(&mut self, cmd: AckCmd) {
        crate::lifecycle_trace!(
            "a10_acker_enter",
            0,
            cmd.entries.len() as u64,
            "shard"
        );

        // Process pending drain notifications first so pending list is current.
        self.drain_notifications();

        let delta = self.engine.execute(&Command::Ack {
            consumer_id: cmd.consumer_id,
            entries: &cmd.entries,
        });

        let accepted = cmd.entries.len() as u32;
        crate::lifecycle_trace!(
            "a12_engine_ack_done",
            0,
            accepted as u64,
            "shard"
        );

        // Decrement atomic inflight counters.
        // The engine already decremented its internal counters via execute().
        // Now sync the shared atomics so drain sees freed capacity.
        if let Some(consumer) = self.engine.consumer(cmd.consumer_id) {
            let queue_id = consumer.queue_id;
            for _ in 0..accepted {
                self.counters.dec_inflight(cmd.consumer_id.0, queue_id.0);
            }
        }
        // Subject inflight decremented by apply_delta_and_sync below.

        if accepted > 0 {
            // Release gate so drain re-checks from current cursor.
            // Cursor already stopped at lowest_skipped in drain_cycle,
            // so freed capacity will be used on the next cycle.
            crate::lifecycle_trace!(
                "a12c_acker_gate_fire",
                0,
                0,
                "shard"
            );
            self.gate.release();
            crate::lifecycle_trace!(
                "a13_acker_gate_released",
                0,
                0,
                "shard"
            );
        }

        // Handle delta (demand changes, binding retirements).
        self.apply_delta_and_sync(&delta);

        let _ = cmd.reply.send(AckReply {
            accepted,
            rejected: 0,
        });
        crate::lifecycle_trace!("a14_acker_reply_sent", 0, 0, "shard");
    }

    pub(in crate::shard) fn handle_nack(&mut self, cmd: NackCmd) {
        // Process pending drain notifications first.
        self.drain_notifications();

        if cmd.delay_ms > 0 {
            // ── Delayed nack: insert into timing wheel, don't rewind yet ──
            // Engine Nack releases inflight + removes from pending NOW.
            // The wheel tick will rewind cursor later for redelivery.
            let delta = self.engine.execute(&Command::Nack {
                consumer_id: cmd.consumer_id,
                entries: &cmd.entries,
            });

            let requeued = cmd.entries.len() as u32;

            // Decrement atomic inflight counters.
            if let Some(consumer) = self.engine.consumer(cmd.consumer_id) {
                let queue_id = consumer.queue_id;
                for _ in 0..requeued {
                    self.counters.dec_inflight(cmd.consumer_id.0, queue_id.0);
                }
            }

            self.apply_delta_and_sync(&delta);

            // Insert into wheel — delay_ms converted to ticks (1 tick = 1s).
            let delay_ticks = ((cmd.delay_ms as u64 + 999) / 1000) as u32; // ceil(ms / 1000)
            self.ensure_wheel();
            let wheel = self.wheel.as_mut().unwrap();
            for entry in &cmd.entries {
                wheel.insert(
                    arbitro_common::WheelEntry {
                        seq: entry.seq,
                        consumer_id: cmd.consumer_id.0,
                        subject_hash: 0, // not needed for nack-delay rewind
                    },
                    delay_ticks,
                );
            }

            let _ = cmd.reply.send(NackReply {
                requeued,
                not_found: 0,
            });
        } else {
            // ── Immediate nack: existing behavior ─────────────────────────
            let delta = self.engine.execute(&Command::Nack {
                consumer_id: cmd.consumer_id,
                entries: &cmd.entries,
            });

            let requeued = cmd.entries.len() as u32;

            // Decrement atomic inflight counters (nack releases inflight too).
            if let Some(consumer) = self.engine.consumer(cmd.consumer_id) {
                let queue_id = consumer.queue_id;
                for _ in 0..requeued {
                    self.counters.dec_inflight(cmd.consumer_id.0, queue_id.0);
                }
            }

            self.apply_delta_and_sync(&delta);

            if requeued > 0 {
                // Rewind cursor to re-scan nacked entries.
                let min_seq = cmd.entries.iter().map(|e| e.seq).min().unwrap_or(0);
                if min_seq > 0 {
                    let cur = self.counters.cursor();
                    self.counters.set_cursor(cur.min(min_seq - 1));
                }
                self.counters.clear_rewind();
                self.gate.release();
            }

            let _ = cmd.reply.send(NackReply {
                requeued,
                not_found: 0,
            });
        }
    }

    // ── Admin — subscribe / unsubscribe ─────────────────────────────────

    pub(in crate::shard) fn handle_subscribe(
        &mut self,
        cmd: SubscribeCmd,
    ) {
        let connection_id = cmd.connection_id;
        let consumer_id = cmd.consumer_config.id;

        let stream_ok =
            self.engine.create_stream(cmd.stream_config).is_ok();
        let consumer_ok = self
            .engine
            .create_consumer(cmd.consumer_config)
            .is_ok();
        let sub_ok = self
            .engine
            .create_subscription(cmd.subscription_config)
            .is_ok();

        if stream_ok && consumer_ok && sub_ok {
            let subscription_id = SubscriptionId(consumer_id.0);
            let (result, events) =
                self.engine.subscribe(connection_id, subscription_id);

            if let Ok(binding_id) = result {
                let consumer = self.engine.consumer(consumer_id);

                let max_inflight = consumer
                    .map(|c| c.max_inflight)
                    .unwrap_or(u32::MAX);
                let stream_id = consumer
                    .map(|c| c.stream_id)
                    .unwrap_or(StreamId(0));
                let queue_id = consumer
                    .map(|c| c.queue_id)
                    .unwrap_or(QueueId(0));
                let fire_and_forget = consumer
                    .map(|c| c.ack_policy == AckPolicy::None)
                    .unwrap_or(false);
                let ack_wait_ms = consumer
                    .map(|c| c.ack_wait_ms)
                    .unwrap_or(0);

                // Skip binding if connection disappeared before subscribe
                // applied — stale demand cleaned up by mark_connection_dead.
                if let Some(write_tx) = self.registry.get_write_tx(connection_id.0) {
                    self.bindings.push(ActiveBinding {
                        binding_id,
                        connection_id,
                        consumer_id,
                        stream_id,
                        queue_id,
                        max_inflight,
                        fire_and_forget,
                        ack_wait_ms,
                        write_tx,
                    });
                }

                // Increment demand atomic (but DON'T release gate yet).
                self.counters.inc_demand(stream_id.raw());
            }

            // Apply delta (bindings_retired cleanup).
            // NOTE: gate.release() inside apply_delta_and_sync is safe because
            // we rebuild snapshot below before returning.
            let _had_demand_event = !events.demand_became_available.is_empty();
            for &bid in &events.bindings_retired {
                self.bindings.retain(|b| b.binding_id != bid);
            }

            // Rewind cursor based on deliver_policy:
            // 0 = All: rewind to 0 (replay entire store)
            // 1 = New: no rewind (only future messages)
            // 2 = ByStartSeq: rewind to start_seq - 1
            match cmd.deliver_policy {
                0 => {
                    // DeliverPolicy::All — replay from beginning.
                    self.counters.set_cursor(0);
                    self.counters.clear_rewind();
                }
                1 => {
                    // DeliverPolicy::New — cursor stays at current position.
                    // New consumer only sees messages published after subscribe.
                }
                2 => {
                    // DeliverPolicy::ByStartSeq — position the cursor at
                    // `start_seq - 1` so the next delivery is the message
                    // with sequence `start_seq`.
                    //
                    // This must work in BOTH directions:
                    //   - rewind  (current > target): replay messages we
                    //     have already delivered past
                    //   - forward (current < target): a fresh consumer
                    //     subscribing on a stream that already has a
                    //     backlog and wants to skip the first N msgs
                    //
                    // The previous implementation only handled the rewind
                    // case (`if target < current { signal_rewind }`),
                    // silently dropping the forward case — so a brand-new
                    // consumer with cursor=0 asking for start_seq=6 was
                    // served from seq=1, not seq=6.
                    let target = cmd.start_seq.saturating_sub(1);
                    self.counters.set_cursor(target);
                    self.counters.clear_rewind();
                }
                _ => {
                    // Unknown — default to All for safety.
                    self.counters.set_cursor(0);
                    self.counters.clear_rewind();
                }
            }

            // Rebuild snapshot BEFORE gate release — drain must see new binding.
            self.rebuild_and_swap_snapshot();

            // LAST: release gate — everything is ready.
            self.gate.release();
        }

        let _ =
            cmd.reply.send(stream_ok && consumer_ok && sub_ok);
    }

    pub(in crate::shard) fn handle_unsubscribe(
        &mut self,
        cmd: UnsubscribeCmd,
    ) {
        let consumer_id = ConsumerId(cmd.subscription_id.0);
        let binding_ids: Vec<_> = self
            .bindings
            .iter()
            .filter(|b| b.consumer_id == consumer_id)
            .map(|b| (b.binding_id, b.stream_id))
            .collect();

        for (bid, stream_id) in binding_ids {
            let events = self.engine.unsubscribe(bid);
            // Decrement demand.
            self.counters.dec_demand(stream_id.raw());
            self.apply_delta_and_sync(&events);
        }

        self.rebuild_and_swap_snapshot();
        let _ = cmd.reply.send(true);
    }

    // ── Admin — stream lifecycle ────────────────────────────────────────

    pub(in crate::shard) fn handle_create_stream(
        &mut self,
        cmd: CreateStreamCmd,
    ) {
        let stream_id = cmd.config.id;
        let ok = self.engine.create_stream(cmd.config).is_ok();
        if ok {
            // Persist per-stream retention config (even if all zeros).
            // Zero-valued limits are no-ops; storing them is harmless and
            // avoids a branch at set time.
            self.stream_retention.insert(stream_id, crate::shard::worker::StreamRetention {
                max_msgs:   cmd.max_msgs,
                max_bytes:  cmd.max_bytes,
                max_age_ms: cmd.max_age_ms,
            });
            self.rebuild_and_swap_snapshot();
        }
        let _ = cmd.reply.send(ok);
    }

    pub(in crate::shard) fn handle_delete_stream(
        &mut self,
        cmd: DeleteStreamCmd,
    ) {
        // Decrement demand for all bindings on this stream.
        let stream_binding_count = self
            .bindings
            .iter()
            .filter(|b| b.stream_id == cmd.stream_id)
            .count();
        for _ in 0..stream_binding_count {
            self.counters.dec_demand(cmd.stream_id.raw());
        }

        let events = self.engine.delete_stream(cmd.stream_id);
        self.apply_delta_and_sync(&events);
        self.stream_retention.remove(&cmd.stream_id);
        self.rebuild_and_swap_snapshot();
        let _ = cmd.reply.send(true);
    }

    // ── Admin — consumer lifecycle ──────────────────────────────────────

    pub(in crate::shard) fn handle_create_consumer(
        &mut self,
        cmd: CreateConsumerCmd,
    ) {
        let stream_id = cmd.config.stream_id;
        let ok = self.engine.create_consumer(cmd.config).is_ok();
        if ok {
            for (pattern, limit) in &cmd.max_subject_inflights {
                let _ = self.engine.set_max_subject_inflight(
                    stream_id, pattern, *limit,
                );
            }
        }
        let _ = cmd.reply.send(ok);
    }

    pub(in crate::shard) fn handle_delete_consumer(
        &mut self,
        cmd: DeleteConsumerCmd,
    ) {
        // Decrement demand for all bindings of this consumer.
        let consumer_bindings: Vec<_> = self
            .bindings
            .iter()
            .filter(|b| b.consumer_id == cmd.consumer_id)
            .map(|b| b.stream_id)
            .collect();
        for stream_id in consumer_bindings {
            self.counters.dec_demand(stream_id.raw());
        }

        let events = self.engine.delete_consumer(cmd.consumer_id);
        self.apply_delta_and_sync(&events);

        // Tell the drain to release this consumer's per-subject state.
        // Best-effort: if the ring is full the slot just leaks until the
        // drain next runs an Ack for this consumer (it won't — consumer
        // is gone) so we accept a one-slot leak in the truly-stuck case.
        let _ = self.drain_evt_tx.try_send(
            crate::shard::drain_events::DrainEvent::ConsumerRemoved {
                consumer_id: cmd.consumer_id,
            },
        );
        self.gate.release();

        self.rebuild_and_swap_snapshot();
        let _ = cmd.reply.send(true);
    }

    // ── Admin — connection lifecycle ────────────────────────────────────

    pub(in crate::shard) fn handle_open_connection(
        &mut self,
        cmd: OpenConnectionCmd,
    ) {
        self.engine
            .open_connection(cmd.connection_id, cmd.node_id);
        let _ = cmd.reply.send(());
    }

    pub(in crate::shard) fn handle_drain_connection(
        &mut self,
        cmd: DrainConnectionCmd,
    ) {
        // Decrement demand for all bindings of this connection.
        let conn_bindings: Vec<_> = self
            .bindings
            .iter()
            .filter(|b| b.connection_id == cmd.connection_id)
            .map(|b| b.stream_id)
            .collect();
        for stream_id in conn_bindings {
            self.counters.dec_demand(stream_id.raw());
        }

        let events =
            self.engine.mark_connection_dead(cmd.connection_id);
        self.apply_delta_and_sync(&events);
        self.rebuild_and_swap_snapshot();
        let _ = cmd.reply.send(());
    }

    // ── Admin — bind ────────────────────────────────────────────────────

    pub(in crate::shard) fn handle_bind(&mut self, cmd: BindCmd) {
        let (result, events) = self
            .engine
            .subscribe(cmd.connection_id, cmd.subscription_id);

        if let Ok(binding_id) = result {
            let consumer_id = ConsumerId(cmd.subscription_id.0);
            let consumer = self.engine.consumer(consumer_id);

            let max_inflight = consumer
                .map(|c| c.max_inflight)
                .unwrap_or(u32::MAX);
            let stream_id = consumer
                .map(|c| c.stream_id)
                .unwrap_or(StreamId(0));
            let queue_id = consumer
                .map(|c| c.queue_id)
                .unwrap_or(QueueId(0));
            let fire_and_forget = consumer
                .map(|c| c.ack_policy == AckPolicy::None)
                .unwrap_or(false);
            let ack_wait_ms = consumer
                .map(|c| c.ack_wait_ms)
                .unwrap_or(0);

            if let Some(write_tx) = self.registry.get_write_tx(cmd.connection_id.0) {
                self.bindings.push(ActiveBinding {
                    binding_id,
                    connection_id: cmd.connection_id,
                    consumer_id,
                    stream_id,
                    queue_id,
                    max_inflight,
                    fire_and_forget,
                    ack_wait_ms,
                    write_tx,
                });
            }

            // Increment demand (but don't release gate yet).
            self.counters.inc_demand(stream_id.raw());

            // Rewind cursor BEFORE snapshot.
            self.counters.set_cursor(0);
            self.counters.clear_rewind();
        }

        // Retire any bindings from delta.
        for &bid in &events.bindings_retired {
            self.bindings.retain(|b| b.binding_id != bid);
        }

        // Rebuild snapshot BEFORE gate release.
        self.rebuild_and_swap_snapshot();

        // LAST: release gate.
        self.gate.release();
        let _ = cmd.reply.send(());
    }

    // ── Query ───────────────────────────────────────────────────────────

    pub(in crate::shard) fn handle_list_streams(
        &mut self,
        cmd: ListStreamsCmd,
    ) {
        let streams = self
            .engine
            .list_streams()
            .into_iter()
            .map(|(id, name)| (id.raw(), name))
            .collect();
        let _ = cmd.reply.send(ListStreamsReply { streams });
    }

    pub(in crate::shard) fn handle_list_consumers(
        &mut self,
        cmd: ListConsumersCmd,
    ) {
        let consumers = self
            .engine
            .list_consumers()
            .into_iter()
            .map(|(cid, sid, qid, paused)| {
                (cid.0, sid.raw(), qid.0, paused)
            })
            .collect();
        let _ = cmd.reply.send(ListConsumersReply { consumers });
    }

    pub(in crate::shard) fn handle_store_info(
        &mut self,
        cmd: StoreInfoCmd,
    ) {
        let info = self.store.lock().unwrap().info();
        let _ = cmd.reply.send(StoreInfoReply {
            messages: info.messages,
            bytes: info.bytes,
        });
    }

    // ── Stream content management ───────────────────────────────────────

    pub(in crate::shard) fn handle_purge_stream(
        &mut self,
        cmd: PurgeStreamCmd,
    ) {
        let deleted = self.store.lock().unwrap().purge();
        // After a full purge the store's first_seq equals next_seq — no
        // entries exist. The drain cursor may be behind next_seq; leave it
        // where it is. New publishes will fire the gate and the drain will
        // see them at their fresh sequences.
        let _ = cmd.reply.send(deleted);
    }

    pub(in crate::shard) fn handle_drain_subject(
        &mut self,
        cmd: DrainSubjectCmd,
    ) {
        let deleted = self.store.lock().unwrap().drain(&cmd.subject);
        let _ = cmd.reply.send(deleted);
    }

    // ── Background eviction — max_age ───────────────────────────────────

    /// Evict entries older than `max_age_ms` from streams that have
    /// age-based retention configured. Called periodically from the
    /// command worker loop (cold path — every 5 seconds).
    ///
    /// Strategy: single pass over the store from first_seq forward.
    /// For each stream with max_age, track whether we've seen the first
    /// valid (non-expired) entry. The global truncation point is the
    /// minimum first-valid seq across all streams — we cannot evict
    /// past any stream's oldest valid entry. Streams without max_age
    /// implicitly constrain the truncation point to their oldest entry.
    pub(in crate::shard) fn evict_expired(&mut self) {
        // Quick check: any stream with max_age configured?
        let has_age_streams = self.stream_retention.values().any(|r| r.max_age_ms > 0);
        if !has_age_streams {
            return;
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut store = self.store.lock().unwrap();
        let info = store.info();

        if info.messages == 0 {
            return;
        }

        // Build per-stream cutoff timestamps. Indexed by stream_id.raw().
        // 0 means "no age limit" (keep everything for that stream).
        let max_stream_idx = self
            .stream_retention
            .keys()
            .map(|s| s.raw() as usize)
            .max()
            .unwrap_or(0);
        let mut cutoff_ts = vec![0u64; max_stream_idx + 1];
        for (sid, r) in &self.stream_retention {
            if r.max_age_ms > 0 {
                cutoff_ts[sid.raw() as usize] = now_ms.saturating_sub(r.max_age_ms);
            }
        }

        // Single pass: find the minimum first-valid seq across all streams.
        // For streams with max_age: first entry with timestamp >= cutoff.
        // For streams without max_age (cutoff_ts == 0): their first entry
        // is always valid and constrains truncation.
        let mut min_valid_seq: u64 = 0;
        // Track which streams we've already resolved (found first valid entry).
        let mut resolved = vec![false; max_stream_idx + 1];

        let _ = store.for_each(info.first_seq, info.last_seq + 1, &mut |entry| {
            let sid = entry.stream_id as usize;
            if sid >= resolved.len() {
                // Unknown stream — conservative: constrain to this seq.
                if min_valid_seq == 0 || entry.seq < min_valid_seq {
                    min_valid_seq = entry.seq;
                }
                return;
            }
            if resolved[sid] {
                return; // Already found first valid for this stream.
            }

            let stream_cutoff = cutoff_ts[sid];
            if stream_cutoff == 0 {
                // No age limit — this stream's first entry constrains truncation.
                resolved[sid] = true;
                if min_valid_seq == 0 || entry.seq < min_valid_seq {
                    min_valid_seq = entry.seq;
                }
            } else if entry.timestamp >= stream_cutoff {
                // First non-expired entry for this stream.
                resolved[sid] = true;
                if min_valid_seq == 0 || entry.seq < min_valid_seq {
                    min_valid_seq = entry.seq;
                }
            }
            // else: entry is expired for this stream, keep scanning.
        });

        // Truncate if we found a valid boundary past current first_seq.
        if min_valid_seq > info.first_seq {
            let deleted = store.truncate_front(min_valid_seq);
            if deleted > 0 {
                tracing::debug!(
                    deleted,
                    new_first_seq = min_valid_seq,
                    "evict_expired: truncated aged entries"
                );
            }
        }
    }

    // ── Admin — pause / resume ──────────────────────────────────────────

    pub(in crate::shard) fn handle_pause_consumer(
        &mut self,
        cmd: PauseConsumerCmd,
    ) {
        let ok = self.engine.pause_consumer(cmd.consumer_id);
        if ok {
            self.counters.set_paused(cmd.consumer_id.0, true);
        }
        let _ = cmd.reply.send(ok);
    }

    pub(in crate::shard) fn handle_resume_consumer(
        &mut self,
        cmd: ResumeConsumerCmd,
    ) {
        let ok = self.engine.resume_consumer(cmd.consumer_id);
        if ok {
            self.counters.set_paused(cmd.consumer_id.0, false);
            self.gate.release();
        }
        let _ = cmd.reply.send(ok);
    }
}
