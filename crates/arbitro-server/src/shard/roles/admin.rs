//! Admin / control role — cold-path management ops.
//!
//! Per `.agent/rules/roles.md` ADMIN:
//!   * Always reply oneshot (drop = shard-crashed to caller).
//!   * Keep `bindings`/`stores`/`seeded_streams`/`last_engine_seq` consistent
//!     with engine catalog/graph.
//!
//! Subscribe flow: `ensure_stream` → `ensure_consumer` → `ensure_subscription`
//!   → seeder (if unseeded) → `bind` → push `ActiveBinding` → `gate.release()`.
//!
//! Delete stream: `engine.remove_stream_full` → remove `stores[id]`
//!   (purge disk if `purge_disk`) → clear `seeded_streams[id]` and
//!   `last_engine_seq[id]` → retain-filter `bindings`.
//!
//! Delete consumer / unsubscribe: engine drain → catalog remove →
//!   retain-filter `bindings`.
//!
//! MUST NOT: do hot-path work; leave half-applied state on error; rely on
//! drainer to fix inconsistencies; forget to clear `last_engine_seq` on delete.

use arbitro_engine_v2::batch::{BindBatch, BindEntry, DrainConnectionReq, OpenConnectionReq};
use arbitro_engine_v2::types::*;
use arbitro_store::{MemoryStore, Store, TolerantStore};

use crate::shard::command::*;
use crate::shard::worker::{ActiveBinding, ShardWorker};

impl ShardWorker {
    pub(in crate::shard) fn handle_subscribe(&mut self, cmd: SubscribeCmd) {
        let subscription_id = cmd.subscription_config.id;
        let connection_id = cmd.connection_id;
        let consumer_id = cmd.consumer_config.id;
        let stream_id = cmd.consumer_config.stream_id;

        let stream_ok = self.engine.ensure_stream(cmd.stream_config).is_ok();
        let consumer_ok = self.engine.ensure_consumer(cmd.consumer_config).is_ok();
        let sub_ok = self.engine.ensure_subscription(cmd.subscription_config).is_ok();

        if stream_ok && consumer_ok && sub_ok {
            // Seed engine from store if this stream has persisted messages
            // that haven't been loaded into the engine yet (recovery path).
            // seed_from_store inserts into seeded_streams; we still mark empty
            // streams as seeded so we don't re-check on every resubscribe.
            if !self.seeded_streams.contains(&stream_id) {
                let info_opt = self.stores.get(&stream_id).map(|s| s.info());
                match info_opt {
                    Some(info) if info.messages > 0 => {
                        self.seed_from_store(stream_id, &info);
                    }
                    _ => {
                        self.seeded_streams.insert(stream_id);
                    }
                }
            }

            // Resolve the consumer's real queue_id from the engine (may differ
            // from what the subscribe frame sent if the consumer was created
            // with a custom group via create_consumer).
            let queue_id = self.engine.ctx().catalog.consumer_key(consumer_id)
                .and_then(|k| self.engine.ctx().graph.get_consumer(k).map(|n| n.queue_id))
                .unwrap_or(QueueId(stream_id.raw()));

            let bind_entries = [BindEntry {
                connection_id,
                subscription_id,
            }];
            let bind_reply = self.engine.bind(&BindBatch {
                entries: &bind_entries,
                now: cmd.now,
            });

            // Resolve the engine-assigned binding_id so the drainer can pass
            // it directly to claim_with_hints (skips per-claim edge lookup).
            let binding_id = bind_reply
                .entries
                .first()
                .and_then(|key| self.engine.ctx().graph.get_binding(*key).ok())
                .map(|node| node.binding_id)
                .unwrap_or(BindingId(0));

            // Cache hot-path inputs for the drainer:
            //  - max_inflight: catalog read (~20 ns) → cached u32, drainer
            //    pre-filters with consumer_has_capacity (~3 ns).
            //  - paused: catalog + graph chain → cached bool, drainer skips
            //    paused bindings without any engine call.
            // Both are kept fresh by handle_pause_consumer/resume_consumer.
            let max_inflight = self
                .engine
                .consumer_max_inflight(consumer_id)
                .unwrap_or(u32::MAX);
            let paused = self.engine.consumer_paused(consumer_id);

            // Track active binding for delivery
            self.bindings.push(ActiveBinding {
                queue_id,
                connection_id,
                consumer_id,
                stream_id,
                subscription_id,
                binding_id,
                max_inflight,
                paused,
            });

            // Wake drain task — there may be pending messages (e.g. after recovery)
            self.gate.release();
        }

        let _ = cmd.reply.send(stream_ok && consumer_ok && sub_ok);
    }

    pub(in crate::shard) fn handle_unsubscribe(&mut self, cmd: UnsubscribeCmd) {
        let report = self.engine.drain_subscription(cmd.subscription_id, cmd.mode);
        // Remove subscription from catalog/graph/edges/match_table
        let _ = self.engine.remove_subscription(cmd.subscription_id);

        // Remove server-side bindings for this subscription's consumer
        // SubscriptionId == ConsumerId in current design
        let consumer_id = ConsumerId(cmd.subscription_id.0);
        self.bindings.retain(|b| b.consumer_id != consumer_id);

        let _ = cmd.reply.send(report);
    }

    pub(in crate::shard) fn handle_create_stream(&mut self, cmd: CreateStreamCmd) {
        let stream_id = cmd.config.id;
        let journal_kind = cmd.journal_kind;

        let ok = self.engine.ensure_stream(cmd.config).is_ok();

        // Create store if stream is new
        if ok && !self.stores.contains_key(&stream_id) {
            let mut store: Box<dyn Store> = match (journal_kind, &self.data_dir) {
                (0, _) => Box::new(MemoryStore::new()),
                (_, Some(dir)) => {
                    let path = std::path::Path::new(dir)
                        .join("streams")
                        .join(stream_id.raw().to_string());
                    Box::new(TolerantStore::new(path))
                }
                // No data_dir — fallback to memory
                (_, None) => Box::new(MemoryStore::new()),
            };
            if let Err(e) = store.init() {
                tracing::error!(stream_id = stream_id.raw(), error = ?e, "store init failed");
            }

            self.stores.insert(stream_id, store);
        }

        let _ = cmd.reply.send(ok);
    }

    pub(in crate::shard) fn handle_delete_stream(&mut self, cmd: DeleteStreamCmd) {
        // Full cascade: drain + remove all consumers, subscriptions, queues,
        // ready state, idempotency window, and finally the stream itself.
        let report = self.engine.remove_stream_full(cmd.stream_id, cmd.mode);

        // Remove store. Only purge on-disk data for live deletions.
        // During recovery replay, purge_disk=false — the store files reflect
        // the final persisted state and must not be destroyed.
        if let Some(mut store) = self.stores.remove(&cmd.stream_id) {
            if cmd.purge_disk {
                let _ = store.purge();
                if let Some(ref dir) = self.data_dir {
                    let path = std::path::Path::new(dir)
                        .join("streams")
                        .join(cmd.stream_id.raw().to_string());
                    let _ = std::fs::remove_dir_all(path);
                }
            }
        }

        // Clear seeded flag so a recreated stream can re-seed from store.
        self.seeded_streams.remove(&cmd.stream_id);
        self.last_engine_seq.remove(&cmd.stream_id);

        // Remove server-side bindings for this stream.
        self.bindings.retain(|b| b.stream_id != cmd.stream_id);

        let _ = cmd.reply.send(report);
    }

    pub(in crate::shard) fn handle_create_consumer(&mut self, cmd: CreateConsumerCmd) {
        let stream_id = cmd.config.stream_id;
        let ok = self.engine.ensure_consumer(cmd.config).is_ok();
        if ok {
            for (pattern, limit) in &cmd.max_subject_inflights {
                let _ = self.engine.set_max_subject_inflight(stream_id, pattern, *limit);
            }
        }
        let _ = cmd.reply.send(ok);
    }

    pub(in crate::shard) fn handle_delete_consumer(&mut self, cmd: DeleteConsumerCmd) {
        self.bindings.retain(|b| b.consumer_id != cmd.consumer_id);
        let report = self.engine.drain_consumer(cmd.consumer_id, cmd.mode);
        // Remove consumer from catalog/graph after draining
        let _ = self.engine.remove_consumer(cmd.consumer_id);
        let _ = cmd.reply.send(report);
    }

    pub(in crate::shard) fn handle_open_connection(&mut self, cmd: OpenConnectionCmd) {
        self.engine.open_connection(&OpenConnectionReq {
            connection_id: cmd.connection_id,
            node_id: cmd.node_id,
            now: cmd.now,
        });
        let _ = cmd.reply.send(());
    }

    pub(in crate::shard) fn handle_drain_connection(&mut self, cmd: DrainConnectionCmd) {
        // Remove server-side bindings for this connection
        self.bindings.retain(|b| b.connection_id != cmd.connection_id);

        let report = self.engine.drain_connection(&DrainConnectionReq {
            connection_id: cmd.connection_id,
            mode: cmd.mode,
            now: cmd.now,
        });
        // Remove ConnectionNode from graph + ConnectionsByNode edge. O(1).
        self.engine.remove_connection(cmd.connection_id);

        let _ = cmd.reply.send(report);
    }

    pub(in crate::shard) fn handle_bind(&mut self, cmd: BindCmd) {
        let entries = [BindEntry {
            connection_id: cmd.connection_id,
            subscription_id: cmd.subscription_id,
        }];
        self.engine.bind(&BindBatch {
            entries: &entries,
            now: cmd.now,
        });
        let _ = cmd.reply.send(());
    }

    pub(in crate::shard) fn handle_list_streams(&mut self, cmd: ListStreamsCmd) {
        let streams = self.engine.list_streams()
            .into_iter()
            .map(|(id, name)| (id.raw(), name))
            .collect();
        let _ = cmd.reply.send(ListStreamsReply { streams });
    }

    pub(in crate::shard) fn handle_list_consumers(&mut self, cmd: ListConsumersCmd) {
        let consumers = self.engine.list_consumers()
            .into_iter()
            .map(|(cid, sid, qid, paused)| (cid.0, sid.raw(), qid.0, paused))
            .collect();
        let _ = cmd.reply.send(ListConsumersReply { consumers });
    }

    pub(in crate::shard) fn handle_store_info(&mut self, cmd: StoreInfoCmd) {
        let reply = match self.stores.get(&cmd.stream_id) {
            Some(store) => {
                let info = store.info();
                StoreInfoReply { messages: info.messages, bytes: info.bytes }
            }
            None => StoreInfoReply { messages: 0, bytes: 0 },
        };
        let _ = cmd.reply.send(reply);
    }

    pub(in crate::shard) fn handle_pause_consumer(&mut self, cmd: PauseConsumerCmd) {
        let ok = self.engine.pause_consumer(cmd.consumer_id);
        if ok {
            // Keep ActiveBinding.paused mirror in sync so the drainer's
            // hot-path skip works without an engine round-trip.
            for b in self.bindings.iter_mut() {
                if b.consumer_id == cmd.consumer_id {
                    b.paused = true;
                }
            }
        }
        let _ = cmd.reply.send(ok);
    }

    pub(in crate::shard) fn handle_resume_consumer(&mut self, cmd: ResumeConsumerCmd) {
        let ok = self.engine.resume_consumer(cmd.consumer_id);
        if ok {
            for b in self.bindings.iter_mut() {
                if b.consumer_id == cmd.consumer_id {
                    b.paused = false;
                }
            }
        }
        let _ = cmd.reply.send(ok);
    }
}
