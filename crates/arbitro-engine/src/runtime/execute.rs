//! Kernel dispatch — apply a `Command` to the engine state.
//!
//! Level 7. The single hot-path entry point that the shard drainer and
//! inbound translator call. Every mutation of engine state flows through
//! here. Returns `DeltaEvents` so the worker can react (re-arm gate,
//! clean up tx handles, etc.).
//!
//! Rule compliance:
//! - `Ordering::Relaxed` — metrics have no ordering deps.
//! - `&mut EngineContext` respects the single-writer invariant.

use std::sync::atomic::Ordering;

use crate::catalog::Pending;
use crate::command::{Command, DropReason};
use crate::context::EngineContext;
use crate::events::DeltaEvents;

/// Dispatch a single command. Returns events for the worker.
///
/// Hot path. Must be branch-predictable and alloc-free at steady state.
#[inline]
pub fn apply(ctx: &mut EngineContext, cmd: &Command<'_>) -> DeltaEvents {
    let mut events = DeltaEvents::default();
    let m = &ctx.metrics;

    match *cmd {
        Command::Delivered {
            binding_id,
            entries,
            ..
        } => {
            m.claim_entries_delivered
                .fetch_add(entries.len() as u64, Ordering::Relaxed);

            // Get binding metadata before mutating.
            let meta = ctx
                .catalog
                .binding(binding_id)
                .map(|b| (b.consumer_id.raw(), b.queue_id.raw(), b.fire_and_forget));

            if let Some((consumer_raw, queue_raw, fire_and_forget)) = meta {
                // Fire-and-forget bindings (AckPolicy::None) skip inflight
                // tracking and pending list — acks never arrive, so the
                // Vec would grow unbounded (500k × 16B = 8MB) causing
                // cache pollution and realloc spikes. retire_binding is
                // a correct no-op when pending is empty and inflight = 0.
                if !fire_and_forget {
                    if let Some(binding) = ctx.catalog.binding_mut(binding_id) {
                        for entry in entries.iter() {
                            // ROB-12: skip if this seq is already pending on
                            // this binding — avoids a duplicate entry (and a
                            // duplicate inflight increment) on redelivery.
                            if binding.pending.contains_key(&entry.seq) {
                                continue;
                            }
                            binding.pending.insert(
                                entry.seq,
                                Pending {
                                    seq: entry.seq,
                                    subject_hash: entry.subject_hash,
                                    deliveries: 1,
                                    _pad: 0,
                                },
                            );
                            ctx.inflight.inc_pending(consumer_raw, queue_raw);
                        }
                    }
                }
            }
        }

        Command::Ack {
            conn_id,
            consumer_id,
            entries,
        } => {
            m.ack_accepted
                .fetch_add(entries.len() as u64, Ordering::Relaxed);

            // F3+F4: SmallVec<[BindingId; 4]> — most consumers have 1-3 bindings,
            // avoids a heap alloc per ack batch.
            let binding_ids: smallvec::SmallVec<[crate::types::BindingId; 4]> =
                smallvec::SmallVec::from_slice(ctx.catalog.bindings_for_consumer(consumer_id));
            let mut matched = 0u64;
            for ack in entries.iter() {
                // Named entry: (connection, subscription) is a key, so the
                // binding comes back in one hash. Nothing else in this arm
                // can reach a binding the connection does not own.
                if ack.sub_id != 0 {
                    if let Some(bid) = ctx.catalog.binding_id_for_subscription(conn_id, ack.sub_id)
                    {
                        if let Some(binding) = ctx.catalog.binding_mut(bid) {
                            if binding.stream_id == ack.stream_id {
                                if let Some(pending) = binding.pending.remove(&ack.seq) {
                                    let queue_raw = binding.queue_id.raw();
                                    events.subject_hashes_acked.push((
                                        consumer_id.raw(),
                                        pending.subject_hash,
                                        pending.seq,
                                    ));
                                    ctx.inflight.dec_pending(consumer_id.raw(), queue_raw);
                                    matched += 1;
                                }
                            }
                        }
                    }
                    continue;
                }
                // Unnamed entry (`AckBatchReq`, broker-side auto-nack).
                // Invariant: each entry is matched at most once across bindings.
                for &bid in &binding_ids {
                    if let Some(binding) = ctx.catalog.binding_mut(bid) {
                        if binding.stream_id != ack.stream_id {
                            continue;
                        }
                        if let Some(pending) = binding.pending.remove(&ack.seq) {
                            let queue_raw = binding.queue_id.raw();
                            events.subject_hashes_acked.push((
                                consumer_id.raw(),
                                pending.subject_hash,
                                pending.seq,
                            ));
                            ctx.inflight.dec_pending(consumer_id.raw(), queue_raw);
                            matched += 1;
                            break;
                        }
                    }
                }
            }
            let not_found = entries.len() as u64 - matched;
            if not_found > 0 {
                m.ack_not_found.fetch_add(not_found, Ordering::Relaxed);
            }
        }

        Command::Nack {
            conn_id,
            consumer_id,
            entries,
        } => {
            m.nack_accepted
                .fetch_add(entries.len() as u64, Ordering::Relaxed);

            // Release inflight — redelivery handled by drain.
            let binding_ids: smallvec::SmallVec<[crate::types::BindingId; 4]> =
                smallvec::SmallVec::from_slice(ctx.catalog.bindings_for_consumer(consumer_id));
            let mut matched = 0u64;
            for ack in entries.iter() {
                // Named entry: (connection, subscription) is a key, so the
                // binding comes back in one hash. Nothing else in this arm
                // can reach a binding the connection does not own.
                if ack.sub_id != 0 {
                    if let Some(bid) = ctx.catalog.binding_id_for_subscription(conn_id, ack.sub_id)
                    {
                        if let Some(binding) = ctx.catalog.binding_mut(bid) {
                            if binding.stream_id == ack.stream_id {
                                if let Some(pending) = binding.pending.remove(&ack.seq) {
                                    let queue_raw = binding.queue_id.raw();
                                    events.subject_hashes_acked.push((
                                        consumer_id.raw(),
                                        pending.subject_hash,
                                        pending.seq,
                                    ));
                                    ctx.inflight.dec_pending(consumer_id.raw(), queue_raw);
                                    matched += 1;
                                }
                            }
                        }
                    }
                    continue;
                }
                // Unnamed entry (`AckBatchReq`, broker-side auto-nack).
                // Invariant: each entry is matched at most once across bindings.
                for &bid in &binding_ids {
                    if let Some(binding) = ctx.catalog.binding_mut(bid) {
                        if binding.stream_id != ack.stream_id {
                            continue;
                        }
                        if let Some(pending) = binding.pending.remove(&ack.seq) {
                            let queue_raw = binding.queue_id.raw();
                            events.subject_hashes_acked.push((
                                consumer_id.raw(),
                                pending.subject_hash,
                                pending.seq,
                            ));
                            ctx.inflight.dec_pending(consumer_id.raw(), queue_raw);
                            matched += 1;
                            break;
                        }
                    }
                }
            }
            let not_found = entries.len() as u64 - matched;
            if not_found > 0 {
                m.nack_not_found.fetch_add(not_found, Ordering::Relaxed);
            }
        }

        Command::PublishAccepted { .. } => {
            m.publish_entries_accepted.fetch_add(1, Ordering::Relaxed);
        }

        Command::Tombstone { reason, .. } => match reason {
            DropReason::Expired => {
                m.entries_expired.fetch_add(1, Ordering::Relaxed);
            }
            DropReason::Tombstoned => {
                m.entries_tombstoned.fetch_add(1, Ordering::Relaxed);
            }
            DropReason::NoSubscribers => {
                m.publish_no_match.fetch_add(1, Ordering::Relaxed);
            }
        },
    }

    events
}

/// Dispatch a slice of commands in order.
#[inline]
pub fn apply_batch(ctx: &mut EngineContext, cmds: &[Command<'_>]) -> DeltaEvents {
    let mut events = DeltaEvents::default();
    for cmd in cmds {
        events.merge(apply(ctx, cmd));
    }
    events
}
