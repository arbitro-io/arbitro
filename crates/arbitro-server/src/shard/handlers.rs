//! Shard command handlers — all cold-path management ops + hot-path
//! ack/nack/publish.
//!
//! Each handler is an `impl ShardWorker` method, called from
//! `dispatch_command` in `worker.rs`.

use std::time::Duration;

use arbitro_engine_v2::command::Command;
use arbitro_engine_v2::types::*;
use arbitro_proto::error::ErrorCode;
use arbitro_store::EntryRef;
use tokio::sync::mpsc;

use crate::common::reply::{send_error, send_rep_ok};
use crate::shard::command::*;
use crate::shard::worker::{AccumCaller, ActiveBinding, ShardWorker, StreamAccum};

impl ShardWorker {
    // NOTE: handle_publish is NOT here — publish goes directly to the
    // store from the dispatch layer. Publish and drain are independent
    // services connected only by store (data) and gate (notification).

    // ── Hot path — accumulator ──────────────────────────────────────────

    pub(in crate::shard) fn handle_publish_accumulate(
        &mut self,
        cmd: PublishCmd,
    ) {
        if !self.engine.ctx().catalog.stream_exists(cmd.stream_id) {
            send_error(
                &self.registry,
                cmd.conn_id,
                cmd.env_seq,
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

        // Reset timer on every new entry.
        let interval =
            Duration::from_millis(self.flusher_config.interval_ms);
        self.accum_deadline =
            Some(std::time::Instant::now() + interval);
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
                        send_rep_ok(
                            &self.registry,
                            caller.conn_id,
                            caller.env_seq,
                            first_seq + seq_offset,
                        );
                        seq_offset += caller.entry_count as u64;
                    }
                    self.gate.release();
                }
                Err(_) => {
                    for caller in &callers {
                        send_error(
                            &self.registry,
                            caller.conn_id,
                            caller.env_seq,
                            ErrorCode::StreamFull,
                        );
                    }
                }
            }
        }

        // Stop timer — no data pending.
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

        if accepted > 0 {
            // Rewind cursor so drain revisits entries that were skipped
            // due to capacity/subject limits that are now freed.
            if let Some(rw) = self.rewind_cursor.take() {
                if rw > 0 {
                    self.cursor = rw - 1;
                }
            }
            self.gate.release();
            crate::lifecycle_trace!(
                "a13_acker_gate_released",
                0,
                0,
                "shard"
            );
        }

        self.apply_delta(delta);

        let _ = cmd.reply.send(AckReply {
            accepted,
            rejected: 0,
        });
        crate::lifecycle_trace!("a14_acker_reply_sent", 0, 0, "shard");
    }

    pub(in crate::shard) fn handle_nack(&mut self, cmd: NackCmd) {
        let delta = self.engine.execute(&Command::Nack {
            consumer_id: cmd.consumer_id,
            entries: &cmd.entries,
        });

        let requeued = cmd.entries.len() as u32;
        self.apply_delta(delta);

        let _ = cmd.reply.send(NackReply {
            requeued,
            not_found: 0,
        });

        // Rewind cursor so drain re-scans the nacked entries.
        if requeued > 0 {
            // Rewind to the lowest nacked seq.
            let min_seq = cmd.entries.iter().map(|e| e.seq).min().unwrap_or(0);
            if min_seq > 0 {
                self.cursor = self.cursor.min(min_seq - 1);
            }
            // Also consume any pending rewind (nack trumps skip-rewind).
            self.rewind_cursor = None;
            self.gate.release();
        }
    }

    // ── Admin — subscribe / unsubscribe ─────────────────────────────────

    pub(in crate::shard) fn handle_subscribe(
        &mut self,
        cmd: SubscribeCmd,
    ) {
        let connection_id = cmd.connection_id;
        let consumer_id = cmd.consumer_config.id;
        let subscription_id = cmd.subscription_config.id;

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
            let (result, events) =
                self.engine.subscribe(connection_id, subscription_id);

            if let Ok(binding_id) = result {
                // Cache hot-path data for the drainer.
                let consumer = self.engine.consumer(consumer_id);

                let max_inflight = consumer
                    .map(|c| c.max_inflight)
                    .unwrap_or(u32::MAX);

                let stream_id = consumer
                    .map(|c| c.stream_id)
                    .unwrap_or(StreamId(0));

                let fire_and_forget = consumer
                    .map(|c| c.ack_policy == AckPolicy::None)
                    .unwrap_or(false);

                let paused = self.engine.is_paused(consumer_id);

                let tx = self
                    .registry
                    .get_sender(connection_id.0)
                    .unwrap_or_else(|| {
                        let (tx, _rx) = mpsc::channel(1);
                        tx
                    });

                self.bindings.push(ActiveBinding {
                    binding_id,
                    connection_id,
                    consumer_id,
                    stream_id,
                    max_inflight,
                    fire_and_forget,
                    paused,
                    tx,
                });
            }

            self.apply_delta(events);

            // Rewind cursor to 0 so the drain delivers historical
            // messages to the new subscriber. Without this, messages
            // published before the subscribe would never be delivered
            // because the cursor has already advanced past them.
            self.cursor = 0;
            self.rewind_cursor = None;
        }

        let _ =
            cmd.reply.send(stream_ok && consumer_ok && sub_ok);
    }

    pub(in crate::shard) fn handle_unsubscribe(
        &mut self,
        cmd: UnsubscribeCmd,
    ) {
        // Find bindings for this subscription's consumer and retire them.
        // Convention: SubscriptionId.0 == ConsumerId.0
        let consumer_id = ConsumerId(cmd.subscription_id.0);
        let binding_ids: Vec<_> = self
            .bindings
            .iter()
            .filter(|b| b.consumer_id == consumer_id)
            .map(|b| b.binding_id)
            .collect();

        for bid in binding_ids {
            let events = self.engine.unsubscribe(bid);
            self.apply_delta(events);
        }

        let _ = cmd.reply.send(true);
    }

    // ── Admin — stream lifecycle ────────────────────────────────────────

    pub(in crate::shard) fn handle_create_stream(
        &mut self,
        cmd: CreateStreamCmd,
    ) {
        let ok = self.engine.create_stream(cmd.config).is_ok();
        let _ = cmd.reply.send(ok);
    }

    pub(in crate::shard) fn handle_delete_stream(
        &mut self,
        cmd: DeleteStreamCmd,
    ) {
        let events = self.engine.delete_stream(cmd.stream_id);
        self.apply_delta(events);
        // Single store — entries for this stream remain but are skipped
        // by has_demand() returning false. No store removal needed.
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
        let events = self.engine.delete_consumer(cmd.consumer_id);
        self.apply_delta(events);
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
        let events =
            self.engine.mark_connection_dead(cmd.connection_id);
        self.apply_delta(events);
        let _ = cmd.reply.send(());
    }

    // ── Admin — bind ────────────────────────────────────────────────────

    pub(in crate::shard) fn handle_bind(&mut self, cmd: BindCmd) {
        let (result, events) = self
            .engine
            .subscribe(cmd.connection_id, cmd.subscription_id);

        if let Ok(binding_id) = result {
            // SubscriptionId.0 == ConsumerId.0 by convention.
            let consumer_id = ConsumerId(cmd.subscription_id.0);
            let consumer = self.engine.consumer(consumer_id);

            let max_inflight = consumer
                .map(|c| c.max_inflight)
                .unwrap_or(u32::MAX);
            let stream_id = consumer
                .map(|c| c.stream_id)
                .unwrap_or(StreamId(0));
            let fire_and_forget = consumer
                .map(|c| c.ack_policy == AckPolicy::None)
                .unwrap_or(false);
            let paused = self.engine.is_paused(consumer_id);

            let tx = self
                .registry
                .get_sender(cmd.connection_id.0)
                .unwrap_or_else(|| {
                    let (tx, _rx) = mpsc::channel(1);
                    tx
                });

            self.bindings.push(ActiveBinding {
                binding_id,
                connection_id: cmd.connection_id,
                consumer_id,
                stream_id,
                max_inflight,
                fire_and_forget,
                paused,
                tx,
            });

            // Rewind cursor so drain delivers pending messages to the
            // newly-bound connection.
            self.cursor = 0;
            self.rewind_cursor = None;
        }

        self.apply_delta(events);
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
        // Single store — return aggregate stats regardless of stream_id.
        let info = self.store.lock().unwrap().info();
        let _ = cmd.reply.send(StoreInfoReply {
            messages: info.messages,
            bytes: info.bytes,
        });
    }

    // ── Admin — pause / resume ──────────────────────────────────────────

    pub(in crate::shard) fn handle_pause_consumer(
        &mut self,
        cmd: PauseConsumerCmd,
    ) {
        let ok = self.engine.pause_consumer(cmd.consumer_id);
        if ok {
            // Sync cached flag so drain skips without HashMap lookup.
            for b in &mut self.bindings {
                if b.consumer_id == cmd.consumer_id {
                    b.paused = true;
                }
            }
        }
        let _ = cmd.reply.send(ok);
    }

    pub(in crate::shard) fn handle_resume_consumer(
        &mut self,
        cmd: ResumeConsumerCmd,
    ) {
        let ok = self.engine.resume_consumer(cmd.consumer_id);
        if ok {
            for b in &mut self.bindings {
                if b.consumer_id == cmd.consumer_id {
                    b.paused = false;
                }
            }
            // Wake drain — unpaused consumer may have pending work.
            self.gate.release();
        }
        let _ = cmd.reply.send(ok);
    }
}
