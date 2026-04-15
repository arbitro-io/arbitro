//! Drain isolated bench — two scenarios that replicate production 1:1.
//!
//! Scenario 1 — PUBLISH:
//!   Mirrors `crates/arbitro-server/src/shard/roles/publisher.rs::handle_publish`:
//!   stream lookup → build EntryRef vec → now_ms → store.append_batch → ok.
//!   (no network — send_rep_ok and gate.release are excluded.)
//!
//! Scenario 2 — DRAIN:
//!   Mirrors `crates/arbitro-server/src/shard/roles/drainer.rs::handle_drain_deliver`:
//!   publish_pending_to_engine (for_each + fnv1a_32 + enqueue_ready +
//!   flush_seed_metrics) → per-binding claim loop → build inline frame
//!   (envelope placeholder + RepBatchFixed + contiguous for_each entries +
//!   envelope patch) → split().freeze() → tx.try_send to channel.
//!
//! Run: cargo bench --bench drain_isolated -p arbitro-e2e

use bytes::BytesMut;
use std::collections::HashMap;
use std::time::Instant;
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::IntoBytes;

use arbitro_engine_v2::batch::ClaimBatch;
use arbitro_engine_v2::catalog::{self, fnv1a_32};
use arbitro_engine_v2::types::*;
use arbitro_engine_v2::ArbitroEngine;
use arbitro_proto::action::Action;
use arbitro_proto::wire::delivery::{DeliveryEntryHeader, RepBatchFixed};
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_store::{EntryRef, MemoryStore, Store};

const PAYLOAD_SIZE: usize = 1024;
const PUBLISH_BATCH: usize = 256;
const CLAIM_BATCH: u16 = 256;
const MAX_FEED_PER_CYCLE: u64 = 256;

// ─── Shared helpers ─────────────────────────────────────────────────────────

fn make_engine() -> ArbitroEngine {
    let stream_id = StreamId(1);
    let queue_id = QueueId(1);
    let consumer_id = ConsumerId(1);

    let mut engine = ArbitroEngine::new();
    engine
        .ensure_stream(catalog::StreamConfig {
            id: stream_id,
            name: b"bench-stream".to_vec(),
        })
        .unwrap();
    engine
        .ensure_consumer(catalog::ConsumerConfig {
            id: consumer_id,
            queue_id,
            stream_id,
            durable: false,
            ack_policy: AckPolicy::None,
            max_inflight: u32::MAX,
        })
        .unwrap();
    engine
        .ensure_subscription(catalog::SubscriptionConfig {
            id: SubscriptionId(1),
            stream_id,
            consumer_id,
            filters: vec![],
        })
        .unwrap();
    engine
}

fn fmt_rate(msgs: usize, elapsed: std::time::Duration) -> String {
    let rate = msgs as f64 / elapsed.as_secs_f64();
    if rate >= 1_000_000_000.0 {
        format!("{:.1}G", rate / 1_000_000_000.0)
    } else if rate >= 1_000_000.0 {
        format!("{:.1}M", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}K", rate / 1_000.0)
    } else {
        format!("{:.0}", rate)
    }
}

fn fmt_ms(elapsed: std::time::Duration) -> String {
    format!("{:.2}ms", elapsed.as_secs_f64() * 1000.0)
}

// ─── Scenario 1: PUBLISH (mirrors handle_publish) ───────────────────────────
//
// For each publish call:
//   1. HashMap lookup for the stream.
//   2. Build Vec<EntryRef> from input entries (same as publisher.rs).
//   3. now_ms via SystemTime syscall.
//   4. store.append_batch.
//   5. Return Ok (no network — send_rep_ok/gate.release excluded).

struct PublishEntry {
    subject: Vec<u8>,
    payload: Vec<u8>,
}

fn scenario_publish(total_msgs: usize) {
    let stream_id = StreamId(1);

    // Build the entry pool once (not part of measurement — caller prepares cmd).
    let subject = b"bench.drain.subject".to_vec();
    let payload = vec![0x42u8; PAYLOAD_SIZE];
    let pool: Vec<PublishEntry> = (0..PUBLISH_BATCH)
        .map(|_| PublishEntry {
            subject: subject.clone(),
            payload: payload.clone(),
        })
        .collect();

    // Production state: shard's HashMap<StreamId, Box<dyn Store>>.
    let mut stores: HashMap<StreamId, Box<dyn Store>> = HashMap::new();
    stores.insert(stream_id, Box::new(MemoryStore::new()));

    let batches = total_msgs / PUBLISH_BATCH;
    let mut published = 0usize;

    let t0 = Instant::now();
    for _ in 0..batches {
        // 1. Stream exists? (mirrors `self.stores.get_mut(&cmd.stream_id)`)
        let store = match stores.get_mut(&stream_id) {
            Some(s) => s,
            None => return,
        };

        // 2. Build store_entries (mirrors `cmd.entries.iter().map(...).collect()`)
        let store_entries: Vec<EntryRef<'_>> = pool
            .iter()
            .map(|e| EntryRef {
                subject: &e.subject,
                payload: &e.payload,
            })
            .collect();

        // 3. now_ms (mirrors publisher.rs line 41-44)
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // 4. store.append_batch
        let _first_seq = store.append_batch(&store_entries, now_ms).unwrap();

        published += store_entries.len();
    }
    let elapsed = t0.elapsed();

    assert_eq!(published, batches * PUBLISH_BATCH);
    let bytes = published * (PAYLOAD_SIZE + subject.len());
    let mb = bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0;
    println!(
        "    publish                 {} ({} msg/s, {:.0} MB/s, {} batches × {})",
        fmt_ms(elapsed),
        fmt_rate(published, elapsed),
        mb,
        batches,
        PUBLISH_BATCH,
    );
}

// ─── Scenario 2: DRAIN (mirrors handle_drain_deliver) ───────────────────────
//
// Pre-populates the store with `total_msgs`, then measures the full drain:
//   1. publish_pending_to_engine (for_each + fnv1a_32 + enqueue_ready +
//      flush_seed_metrics).
//   2. Per binding: claim loop → build inline frame (envelope placeholder +
//      RepBatchFixed + contiguous for_each entries + envelope patch) →
//      split().freeze() → tx.try_send to channel.

fn scenario_drain(total_msgs: usize) {
    let stream_id = StreamId(1);
    let queue_id = QueueId(1);
    let consumer_id = ConsumerId(1);
    let subscription_id = SubscriptionId(1);
    let binding_id = BindingId(1);
    let connection_id = ConnectionId(1);

    // Pre-populate the store (not part of measurement).
    let subject = b"bench.drain.subject";
    let payload = vec![0x42u8; PAYLOAD_SIZE];
    let mut store: Box<dyn Store> = Box::new(MemoryStore::new());
    for _ in 0..total_msgs {
        store
            .append(
                EntryRef {
                    subject,
                    payload: &payload,
                },
                0,
            )
            .unwrap();
    }

    let mut engine = make_engine();
    // Channel with generous capacity so try_send never hits Full.
    let (tx, rx) = crossbeam_channel::bounded::<bytes::Bytes>(65536);

    // Consumer thread drains the channel concurrently (matches prod where
    // the writer task lives on another thread).
    let consumer_thread = std::thread::spawn(move || {
        let mut received = 0usize;
        while let Ok(frame) = rx.recv() {
            if frame.len() < ENVELOPE_SIZE + 8 {
                break;
            }
            let body = &frame[ENVELOPE_SIZE..];
            let count = u16::from_le_bytes([body[4], body[5]]) as usize;
            received += count;
        }
        received
    });

    // Scratch buffers (match ShardWorker fields).
    let mut scratch_seqs: Vec<u64> = Vec::with_capacity(CLAIM_BATCH as usize);
    let mut scratch_batch_body = BytesMut::with_capacity(
        ENVELOPE_SIZE
            + 8
            + CLAIM_BATCH as usize
                * (std::mem::size_of::<DeliveryEntryHeader>() + 19 + PAYLOAD_SIZE),
    );
    let mut last_engine_seq: u64 = 0;
    let mut drained = 0usize;

    let t0 = Instant::now();
    loop {
        // ─── publish_pending_to_engine ─────────────────────────────────────
        let info = store.info();
        let mut fed_this_cycle = false;
        if info.last_seq > last_engine_seq {
            let start = last_engine_seq + 1;
            let end = (start + MAX_FEED_PER_CYCLE).min(info.last_seq + 1);
            let mut fed_last: u64 = last_engine_seq;
            let mut fed_entries: u64 = 0;
            let mut fed_no_match: u64 = 0;
            let mut fed_queues: u64 = 0;
            let eng = &mut engine;
            store
                .for_each(start, end, &mut |entry| {
                    let subject_hash = fnv1a_32(entry.subject);
                    let pushed =
                        eng.enqueue_ready(stream_id, entry.subject, subject_hash, entry.seq);
                    fed_entries += 1;
                    if pushed == 0 {
                        fed_no_match += 1;
                    } else {
                        fed_queues += pushed as u64;
                    }
                    fed_last = entry.seq;
                })
                .ok();
            engine.flush_seed_metrics(fed_entries, fed_no_match, fed_queues);
            last_engine_seq = fed_last;
            let next = fed_last + 1;
            if engine.ctx().next_seq < next {
                engine.ctx_mut().next_seq = next;
            }
            fed_this_cycle = true;
        }

        // ─── per-binding drain (single binding here) ───────────────────────
        let mut any_delivered = false;
        let now = Timestamp::new(0);
        loop {
            scratch_seqs.clear();

            // Tracked consumer: adaptive batch size (fire_and_forget = false).
            let remaining =
                engine.consumer_capacity_remaining(consumer_id, u32::MAX);
            if remaining == 0 {
                break;
            }
            let max_items = (CLAIM_BATCH as u32).min(remaining) as u16;
            {
                let claimed = engine.claim(
                    &ClaimBatch {
                        queue_id,
                        connection_id,
                        consumer_id,
                        max_items,
                        now,
                    },
                    subscription_id,
                    binding_id,
                );
                scratch_seqs.extend(claimed.entries().iter().map(|e| e.seq));
            }
            if scratch_seqs.is_empty() {
                break;
            }
            let claimed_count = scratch_seqs.len();
            any_delivered = true;

            // ─── inline frame build (matches drainer.rs verbatim) ──────────
            scratch_batch_body.clear();
            scratch_batch_body.extend_from_slice(&[0u8; ENVELOPE_SIZE]);
            scratch_batch_body.extend_from_slice(
                RepBatchFixed {
                    consumer_id: U32::new(consumer_id.0),
                    count: U16::new(claimed_count as u16),
                    _pad: U16::new(0),
                }
                .as_bytes(),
            );

            let first = scratch_seqs[0];
            let last = scratch_seqs[claimed_count - 1];
            let contiguous = (last - first + 1) as usize == claimed_count;
            let body = &mut scratch_batch_body;
            if contiguous {
                store
                    .for_each(first, last + 1, &mut |entry| {
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
                    })
                    .ok();
            } else {
                for &seq in scratch_seqs.iter() {
                    store
                        .get(seq, &mut |entry| {
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
                        })
                        .ok();
                }
            }

            // Patch envelope in-place.
            let body_len = scratch_batch_body.len() - ENVELOPE_SIZE;
            let envelope =
                Envelope::new(Action::RepBatch, stream_id.raw(), body_len as u32, 0);
            scratch_batch_body[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());

            // split().freeze() — transfers ownership, no copy.
            let frozen = scratch_batch_body.split().freeze();
            drained += claimed_count;

            // tx.try_send — matches drainer's cached binding.tx.try_send.
            if tx.try_send(frozen).is_err() {
                break;
            }

            if claimed_count < max_items as usize {
                break;
            }
        }

        if !fed_this_cycle && !any_delivered {
            break;
        }
    }
    let elapsed = t0.elapsed();

    // Close channel and wait for consumer drain.
    drop(tx);
    let received = consumer_thread.join().unwrap();

    assert_eq!(drained, total_msgs);
    assert_eq!(received, total_msgs);
    let bytes = total_msgs * (PAYLOAD_SIZE + subject.len());
    let mb = bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0;
    println!(
        "    drain                   {} ({} msg/s, {:.0} MB/s)",
        fmt_ms(elapsed),
        fmt_rate(total_msgs, elapsed),
        mb,
    );
}

// ─── Main ───────────────────────────────────────────────────────────────────

fn main() {
    println!("Drain isolated bench — publish vs drain, exactly as production");
    println!(
        "payload={}B, publish_batch={}, claim_batch={}, max_feed_per_cycle={}",
        PAYLOAD_SIZE, PUBLISH_BATCH, CLAIM_BATCH, MAX_FEED_PER_CYCLE
    );
    println!("{}", "=".repeat(80));

    for &msgs in &[100_000, 500_000, 1_000_000, 5_000_000] {
        println!("\n--- {}k messages ---", msgs / 1000);
        println!("  Scenario 1: PUBLISH");
        scenario_publish(msgs);
        println!("  Scenario 2: DRAIN");
        scenario_drain(msgs);
    }
}
