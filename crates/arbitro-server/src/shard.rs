//! Shard worker — owns an ArbitroEngine + stores on a dedicated OS thread.
//!
//! The worker runs a blocking loop: recv() command → process → send reply.
//! No async, no locks — pure &mut engine on its own thread.
//!
//! Publish path: validate stream → store.append → engine.publish →
//!   drain_fanout → RepOk + gate.release (fire & forget).
//! The shard sends RepOk directly via ConnectionRegistry — no oneshot roundtrip.
//!
//! Delivery path: drain task sends DrainDeliver → shard iterates active
//!   bindings → engine.claim per binding → store.get per entry → deliver frame.

use std::collections::{HashMap, HashSet};

use arbitro_engine_v2::batch::*;
use arbitro_engine_v2::types::*;
use arbitro_engine_v2::ArbitroEngine;
use arbitro_proto::action::Action;
use arbitro_proto::error::ErrorCode;
use arbitro_proto::wire::delivery::RepOkAction;
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_store::{EntryRef, MemoryStore, Store, TolerantStore};
use bytes::BytesMut;
use tokio::sync::mpsc;
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use crate::command::*;
use crate::gate::Gate;
use crate::transport::ConnectionRegistry;

// ── Active binding — tracked per subscribe/bind for delivery ───────────────

/// A bound consumer↔connection pair for delivery.
struct ActiveBinding {
    queue_id: QueueId,
    connection_id: ConnectionId,
    consumer_id: ConsumerId,
    stream_id: StreamId,
}

/// A shard worker that exclusively owns an `ArbitroEngine` and per-stream stores.
pub struct ShardWorker {
    engine: ArbitroEngine,
    stores: HashMap<StreamId, Box<dyn Store>>,
    rx: mpsc::Receiver<ShardCommand>,
    gate: Gate,
    registry: ConnectionRegistry,
    /// Active bindings — iterated on DrainDeliver to claim + deliver.
    bindings: Vec<ActiveBinding>,
    /// Data directory for disk-backed stores. None = memory only.
    data_dir: Option<String>,
    /// Streams that have been seeded from store (avoid double-seeding).
    seeded_streams: HashSet<StreamId>,
    /// Last seq published to engine per stream — drain_deliver reads from here.
    last_engine_seq: HashMap<StreamId, u64>,
    // Scratch buffers — allocated once, reused
    scratch_ack: Vec<AckEntry>,
    scratch_nack: Vec<NackEntry>,
}

impl ShardWorker {
    /// Create a new shard worker.
    pub fn new(
        engine: ArbitroEngine,
        rx: mpsc::Receiver<ShardCommand>,
        gate: Gate,
        registry: ConnectionRegistry,
        data_dir: Option<String>,
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
        }
    }

    /// Run the shard loop. Blocks until Shutdown or channel close.
    pub fn run(mut self) {
        // ── Store init ─────────────────────────────────────────────────
        for (id, store) in &mut self.stores {
            if let Err(e) = store.init() {
                tracing::error!(stream_id = id.raw(), error = ?e, "store init failed");
            }
        }

        while let Some(cmd) = self.rx.blocking_recv() {
            match cmd {
                ShardCommand::Publish(cmd) => self.handle_publish(cmd),
                ShardCommand::Claim(cmd) => self.handle_claim(cmd),
                ShardCommand::Ack(cmd) => self.handle_ack(cmd),
                ShardCommand::Nack(cmd) => self.handle_nack(cmd),
                ShardCommand::Subscribe(cmd) => self.handle_subscribe(cmd),
                ShardCommand::Unsubscribe(cmd) => self.handle_unsubscribe(cmd),
                ShardCommand::CreateStream(cmd) => self.handle_create_stream(cmd),
                ShardCommand::DeleteStream(cmd) => self.handle_delete_stream(cmd),
                ShardCommand::CreateConsumer(cmd) => self.handle_create_consumer(cmd),
                ShardCommand::DeleteConsumer(cmd) => self.handle_delete_consumer(cmd),
                ShardCommand::OpenConnection(cmd) => self.handle_open_connection(cmd),
                ShardCommand::DrainConnection(cmd) => self.handle_drain_connection(cmd),
                ShardCommand::Bind(cmd) => self.handle_bind(cmd),
                ShardCommand::DrainDeliver => self.handle_drain_deliver(),
                ShardCommand::ListStreams(cmd) => self.handle_list_streams(cmd),
                ShardCommand::ListConsumers(cmd) => self.handle_list_consumers(cmd),
                ShardCommand::StoreInfo(cmd) => self.handle_store_info(cmd),
                ShardCommand::PauseConsumer(cmd) => self.handle_pause_consumer(cmd),
                ShardCommand::ResumeConsumer(cmd) => self.handle_resume_consumer(cmd),
                ShardCommand::SeedStores(cmd) => self.handle_seed_stores(cmd),
                ShardCommand::Shutdown => break,
            }
        }

        // ── Store shutdown ─────────────────────────────────────────────
        for (id, store) in &mut self.stores {
            if let Err(e) = store.shutdown() {
                tracing::error!(stream_id = id.raw(), error = ?e, "store shutdown failed");
            }
        }
    }

    // ── Reply helpers (fire & forget, O(1)) ─────────────────────────────

    #[inline]
    fn send_rep_ok(&self, conn_id: u64, env_seq: u32, ref_seq: u64) {
        let envelope = Envelope {
            action: U16::new(Action::RepOk.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(0),
            msg_len: U32::new(16),
            env_seq: U32::new(env_seq),
        };
        let body = RepOkAction {
            ref_seq: U64::new(ref_seq),
            _pad: U64::new(0),
        };
        self.registry.send_parts(conn_id, &[envelope.as_bytes(), body.as_bytes()]);
    }

    #[inline]
    fn send_error(&self, conn_id: u64, env_seq: u32, code: ErrorCode) {
        use arbitro_proto::wire::delivery::RepErrorAction;
        let envelope = Envelope {
            action: U16::new(Action::RepError.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(0),
            msg_len: U32::new(16),
            env_seq: U32::new(env_seq),
        };
        let body = RepErrorAction {
            ref_seq: U64::new(env_seq as u64),
            error_code: U16::new(code.as_u16()),
            _pad: [0u8; 6],
        };
        self.registry.send_parts(conn_id, &[envelope.as_bytes(), body.as_bytes()]);
    }

    // ── Hot path handlers ───────────────────────────────────────────────

    fn handle_publish(&mut self, cmd: PublishCmd) {
        // 1. Stream exists?
        let store = match self.stores.get_mut(&cmd.stream_id) {
            Some(s) => s,
            None => {
                self.send_error(cmd.conn_id, cmd.env_seq, ErrorCode::StreamNotFound);
                return;
            }
        };

        // 2. Store — persist (source of truth)
        let store_entries: Vec<EntryRef<'_>> = cmd.entries.iter().map(|e| {
            EntryRef {
                subject: &e.subject,
                payload: &e.payload,
            }
        }).collect();

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let first_seq = match store.append_batch(&store_entries, now_ms) {
            Ok(seq) => seq,
            Err(_) => {
                self.send_error(cmd.conn_id, cmd.env_seq, ErrorCode::StreamFull);
                return;
            }
        };

        // 3. Reply + signal — engine processing happens in drain_deliver
        self.send_rep_ok(cmd.conn_id, cmd.env_seq, first_seq);
        self.gate.release();
    }

    fn handle_claim(&mut self, cmd: ClaimCmd) {
        let claimed = self.engine.claim(&ClaimBatch {
            queue_id: cmd.queue_id,
            connection_id: cmd.connection_id,
            consumer_id: cmd.consumer_id,
            max_items: cmd.max_items,
            now: cmd.now,
        });

        // Copy entries before next engine call (engine-contract rule 2)
        let entries = claimed.entries().to_vec();
        let _ = cmd.reply.send(entries);
    }

    fn handle_ack(&mut self, cmd: AckCmd) {
        self.scratch_ack.clear();
        self.scratch_ack.extend_from_slice(&cmd.entries);

        let result = self.engine.ack(&AckBatch {
            consumer_id: cmd.consumer_id,
            entries: &self.scratch_ack,
            now: cmd.now,
        });

        if result.accepted > 0 {
            self.gate.release();
        }

        let _ = cmd.reply.send(AckReply {
            accepted: result.accepted,
            rejected: result.rejected,
        });
    }

    fn handle_nack(&mut self, cmd: NackCmd) {
        self.scratch_nack.clear();
        self.scratch_nack.extend_from_slice(&cmd.entries);

        let result = self.engine.nack(&NackBatch {
            consumer_id: cmd.consumer_id,
            entries: &self.scratch_nack,
            now: cmd.now,
        });

        let requeued = result.accepted;
        let _ = cmd.reply.send(NackReply {
            requeued,
            not_found: result.rejected,
        });

        // Wake drain task to redeliver requeued messages
        if requeued > 0 {
            self.gate.release();
        }
    }

    // ── Delivery handler ───────────────────────────────────────────────

    /// Feed pending journal entries into the engine so they become claimable.
    fn publish_pending_to_engine(&mut self, now: Timestamp) {
        let stream_ids: Vec<StreamId> = self.stores.keys().copied().collect();

        for stream_id in stream_ids {
            let last = self.last_engine_seq.get(&stream_id).copied().unwrap_or(0);
            let info = self.stores[&stream_id].info();
            if info.last_seq <= last { continue; }

            let start = last + 1;
            let end = info.last_seq + 1;

            // Temporarily remove store to avoid borrow conflict with self.engine
            let store = self.stores.remove(&stream_id).unwrap();

            store.for_each(start, end, &mut |entry| {
                let publish_entry = PublishEntry {
                    subject_hash: arbitro_engine_v2::catalog::fnv1a_32(entry.subject),
                    subject: entry.subject,
                    payload: PayloadRef::Borrowed(entry.payload),
                    idempotency_key: 0,
                    credits_cost: 1,
                };
                self.engine.publish(&PublishBatch {
                    stream_id,
                    entries: &[publish_entry],
                    now,
                });
                let drain = self.engine.drain_fanout();
                drop(drain);
            }).ok();

            self.stores.insert(stream_id, store);
            self.last_engine_seq.insert(stream_id, info.last_seq);
        }
    }

    /// Iterate all active bindings, claim from engine, read store, deliver.
    /// Loops per binding until the queue is drained or max_inflight is hit.
    fn handle_drain_deliver(&mut self) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let now = Timestamp::new(now_ms);

        // 1. Feed new journal entries into engine
        self.publish_pending_to_engine(now);

        let mut any_delivered = false;

        for i in 0..self.bindings.len() {
            let binding = &self.bindings[i];
            let queue_id = binding.queue_id;
            let connection_id = binding.connection_id;
            let consumer_id = binding.consumer_id;
            let stream_id = binding.stream_id;

            let store = match self.stores.get(&stream_id) {
                Some(s) => s,
                None => continue,
            };

            // Loop: claim batches until queue empty or inflight limit hit
            loop {
                let claimed = self.engine.claim(&ClaimBatch {
                    queue_id,
                    connection_id,
                    consumer_id,
                    max_items: 64,
                    now,
                });

                let entries = claimed.entries().to_vec();
                if entries.is_empty() {
                    break;
                }

                any_delivered = true;

                for entry in &entries {
                    store.get(entry.seq, &mut |store_entry| {
                        self.send_deliver_frame(
                            connection_id.0,
                            stream_id.raw(),
                            consumer_id.0,
                            entry.seq,
                            store_entry.subject,
                            store_entry.payload,
                        );
                    }).ok();
                }

                // If claim returned fewer than max_items, queue is drained
                if entries.len() < 64 {
                    break;
                }
            }
        }

        // If we delivered anything, re-signal the gate so the drain task
        // wakes again (acks may have freed inflight slots for more delivery).
        if any_delivered {
            self.gate.release();
        }
    }

    /// Build and send a Deliver frame with full payload.
    /// Body format: [4 consumer_id][2 subj_len][subject][payload].
    fn send_deliver_frame(
        &self,
        conn_id: u64,
        stream_id: u32,
        consumer_id: u32,
        seq: u64,
        subject: &[u8],
        payload: &[u8],
    ) {
        let body_len = 4 + 2 + subject.len() + payload.len();
        let total = ENVELOPE_SIZE + body_len;

        let mut buf = BytesMut::with_capacity(total);

        let envelope = Envelope {
            action: U16::new(Action::Deliver.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(stream_id),
            msg_len: U32::new(body_len as u32),
            env_seq: U32::new(seq as u32),
        };
        buf.extend_from_slice(envelope.as_bytes());

        // Body: [4 consumer_id][2 subj_len][subject][payload]
        buf.extend_from_slice(&consumer_id.to_le_bytes());
        buf.extend_from_slice(&(subject.len() as u16).to_le_bytes());
        buf.extend_from_slice(subject);
        buf.extend_from_slice(payload);

        self.registry.send_bytes(conn_id, buf.freeze());
    }

    // ── Management handlers ─────────────────────────────────────────────

    fn handle_subscribe(&mut self, cmd: SubscribeCmd) {
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
            if !self.seeded_streams.contains(&stream_id) {
                if let Some(store) = self.stores.get(&stream_id) {
                    let info = store.info();
                    if info.messages > 0 {
                        self.seed_from_store(stream_id, &info);
                    }
                }
                self.seeded_streams.insert(stream_id);
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
            self.engine.bind(&BindBatch {
                entries: &bind_entries,
                now: cmd.now,
            });

            // Track active binding for delivery
            self.bindings.push(ActiveBinding {
                queue_id,
                connection_id,
                consumer_id,
                stream_id,
            });

            // Wake drain task — there may be pending messages (e.g. after recovery)
            self.gate.release();
        }

        let _ = cmd.reply.send(stream_ok && consumer_ok && sub_ok);
    }

    fn handle_unsubscribe(&mut self, cmd: UnsubscribeCmd) {
        let report = self.engine.drain_subscription(cmd.subscription_id, cmd.mode);
        // Remove subscription from catalog/graph/edges/match_table
        let _ = self.engine.remove_subscription(cmd.subscription_id);

        // Remove server-side bindings for this subscription's consumer
        // SubscriptionId == ConsumerId in current design
        let consumer_id = ConsumerId(cmd.subscription_id.0);
        self.bindings.retain(|b| b.consumer_id != consumer_id);

        let _ = cmd.reply.send(report);
    }

    fn handle_create_stream(&mut self, cmd: CreateStreamCmd) {
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

    fn handle_delete_stream(&mut self, cmd: DeleteStreamCmd) {
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

        // Remove server-side bindings for this stream.
        self.bindings.retain(|b| b.stream_id != cmd.stream_id);

        let _ = cmd.reply.send(report);
    }

    fn handle_create_consumer(&mut self, cmd: CreateConsumerCmd) {
        let stream_id = cmd.config.stream_id;
        let ok = self.engine.ensure_consumer(cmd.config).is_ok();
        if ok {
            for (pattern, limit) in &cmd.max_subject_inflights {
                let _ = self.engine.set_max_subject_inflight(stream_id, pattern, *limit);
            }
        }
        let _ = cmd.reply.send(ok);
    }

    fn handle_delete_consumer(&mut self, cmd: DeleteConsumerCmd) {
        self.bindings.retain(|b| b.consumer_id != cmd.consumer_id);
        let report = self.engine.drain_consumer(cmd.consumer_id, cmd.mode);
        // Remove consumer from catalog/graph after draining
        let _ = self.engine.remove_consumer(cmd.consumer_id);
        let _ = cmd.reply.send(report);
    }

    fn handle_open_connection(&mut self, cmd: OpenConnectionCmd) {
        self.engine.open_connection(&OpenConnectionReq {
            connection_id: cmd.connection_id,
            node_id: cmd.node_id,
            now: cmd.now,
        });
        let _ = cmd.reply.send(());
    }

    fn handle_drain_connection(&mut self, cmd: DrainConnectionCmd) {
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

    fn handle_bind(&mut self, cmd: BindCmd) {
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

    fn handle_list_streams(&mut self, cmd: ListStreamsCmd) {
        let streams = self.engine.list_streams()
            .into_iter()
            .map(|(id, name)| (id.raw(), name))
            .collect();
        let _ = cmd.reply.send(ListStreamsReply { streams });
    }

    fn handle_list_consumers(&mut self, cmd: ListConsumersCmd) {
        let consumers = self.engine.list_consumers()
            .into_iter()
            .map(|(cid, sid, qid, paused)| (cid.0, sid.raw(), qid.0, paused))
            .collect();
        let _ = cmd.reply.send(ListConsumersReply { consumers });
    }

    fn handle_store_info(&self, cmd: StoreInfoCmd) {
        let reply = match self.stores.get(&cmd.stream_id) {
            Some(store) => {
                let info = store.info();
                StoreInfoReply { messages: info.messages, bytes: info.bytes }
            }
            None => StoreInfoReply { messages: 0, bytes: 0 },
        };
        let _ = cmd.reply.send(reply);
    }

    fn handle_pause_consumer(&mut self, cmd: PauseConsumerCmd) {
        let ok = self.engine.pause_consumer(cmd.consumer_id);
        let _ = cmd.reply.send(ok);
    }

    fn handle_resume_consumer(&mut self, cmd: ResumeConsumerCmd) {
        let ok = self.engine.resume_consumer(cmd.consumer_id);
        let _ = cmd.reply.send(ok);
    }

    // ── Recovery ────────────────────────────────────────────────────────

    /// Seed engine from a specific stream's store. Temporarily removes the
    /// store from the map to avoid borrow conflicts with the engine.
    fn seed_from_store(&mut self, stream_id: StreamId, info: &arbitro_store::StoreInfo) -> u64 {
        let now = Timestamp::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );

        let first = info.first_seq;
        let end = info.last_seq + 1;
        let mut seeded = 0u64;

        // Temporarily take store out to avoid borrow conflict with self.engine
        let store = match self.stores.remove(&stream_id) {
            Some(s) => s,
            None => return 0,
        };

        store.for_each(first, end, &mut |entry| {
            let publish_entry = PublishEntry {
                subject_hash: arbitro_engine_v2::catalog::fnv1a_32(entry.subject),
                subject: entry.subject,
                payload: PayloadRef::Borrowed(entry.payload),
                idempotency_key: 0,
                credits_cost: 1,
            };
            let _rep = self.engine.publish(&PublishBatch {
                stream_id,
                entries: &[publish_entry],
                now,
            });
            let drain = self.engine.drain_fanout();
            drop(drain);
            seeded += 1;
        }).ok();

        // Put store back + mark engine seq as caught up
        self.stores.insert(stream_id, store);
        self.seeded_streams.insert(stream_id);
        self.last_engine_seq.insert(stream_id, info.last_seq);

        if seeded > 0 {
            tracing::info!(
                stream_id = stream_id.raw(),
                messages = seeded,
                "seeded engine from store"
            );
        }

        seeded
    }

    /// Handle SeedStores command — seed engine from all non-empty stores.
    /// Called after ALL recovery commands (streams + consumers) are replayed.
    fn handle_seed_stores(&mut self, cmd: SeedStoresCmd) {
        let mut total_seeded = 0u64;

        let stream_ids: Vec<StreamId> = self.stores.keys().copied().collect();
        for stream_id in stream_ids {
            if self.seeded_streams.contains(&stream_id) {
                continue;
            }
            let info = self.stores[&stream_id].info();
            if info.messages == 0 {
                continue;
            }
            total_seeded += self.seed_from_store(stream_id, &info);
        }

        let _ = cmd.reply.send(total_seeded);
    }
}
