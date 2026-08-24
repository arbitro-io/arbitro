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

            // ONE probe of `bindings`. It used to resolve the binding
            // twice — `binding()` for the metadata, then `binding_mut()`
            // to mutate — hashing the same id on every command.
            //
            // Borrowed as separate fields so the pending map and the
            // counters can be held at once; they are independent state
            // that only shares an owner.
            let EngineContext {
                catalog, inflight, ..
            } = ctx;

            if let Some(binding) = catalog.binding_mut(binding_id) {
                // Fire-and-forget bindings (AckPolicy::None) skip inflight
                // tracking and pending list — acks never arrive, so the
                // map would grow unbounded (500k × 16B = 8MB) causing
                // cache pollution and realloc spikes. retire_binding is
                // a correct no-op when pending is empty and inflight = 0.
                if !binding.fire_and_forget {
                    let consumer_raw = binding.consumer_id.raw();
                    let queue_raw = binding.queue_id.raw();
                    let mut admitted = 0u32;
                    for entry in entries.iter() {
                        // ROB-12: a seq already pending on this binding is
                        // a redelivery — it must not become a second entry
                        // or a second inflight credit.
                        //
                        // `entry()` and not `contains_key()` + `insert()`:
                        // the pair hashed the same seq twice per entry.
                        if let std::collections::hash_map::Entry::Vacant(slot) =
                            binding.pending.entry(entry.seq)
                        {
                            slot.insert(Pending {
                                seq: entry.seq,
                                subject_hash: entry.subject_hash,
                                deliveries: 1,
                                _pad: 0,
                            });
                            admitted += 1;
                        }
                    }
                    // Once for the batch, not once per entry.
                    inflight.inc_pending_by(consumer_raw, queue_raw, admitted);
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

            // Built on FIRST need, not up front. A batch whose entries all
            // name their subscription never reaches the unnamed arm, and
            // this was a hash probe plus a copy paid for nothing on every
            // such batch — which is the common shape.
            //
            // SmallVec<[BindingId; 4]>: most consumers have 1-3 bindings,
            // so even when it is needed there is no heap allocation.
            let mut binding_ids: smallvec::SmallVec<[crate::types::BindingId; 4]> =
                smallvec::SmallVec::new();
            let mut have_binding_ids = false;
            let mut matched = 0u64;
            for ack in entries.iter() {
                // (connection, subscription) is the key, so a foreign id
                // misses; consumer_id guards a frame naming its own
                // subscription while crediting someone else's consumer.
                if ack.sub_id != 0 {
                    if let Some(bid) = ctx.catalog.binding_id_for_subscription(conn_id, ack.sub_id)
                    {
                        if let Some(binding) = ctx.catalog.binding_mut(bid) {
                            if binding.stream_id == ack.stream_id
                                && binding.consumer_id == consumer_id
                            {
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
                // This arm reaches bindings through the consumer, so the
                // connection is checked here — nowhere else can.
                if !have_binding_ids {
                    binding_ids =
                        smallvec::SmallVec::from_slice(ctx.catalog.bindings_for_consumer(consumer_id));
                    have_binding_ids = true;
                }
                for &bid in &binding_ids {
                    if let Some(binding) = ctx.catalog.binding_mut(bid) {
                        if binding.stream_id != ack.stream_id
                            || binding.connection_id != conn_id
                        {
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

            // Release inflight — redelivery handled by drain. Built on
            // first need; see the Ack arm.
            let mut binding_ids: smallvec::SmallVec<[crate::types::BindingId; 4]> =
                smallvec::SmallVec::new();
            let mut have_binding_ids = false;
            let mut matched = 0u64;
            for ack in entries.iter() {
                // (connection, subscription) is the key, so a foreign id
                // misses; consumer_id guards a frame naming its own
                // subscription while crediting someone else's consumer.
                if ack.sub_id != 0 {
                    if let Some(bid) = ctx.catalog.binding_id_for_subscription(conn_id, ack.sub_id)
                    {
                        if let Some(binding) = ctx.catalog.binding_mut(bid) {
                            if binding.stream_id == ack.stream_id
                                && binding.consumer_id == consumer_id
                            {
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
                // This arm reaches bindings through the consumer, so the
                // connection is checked here — nowhere else can.
                if !have_binding_ids {
                    binding_ids =
                        smallvec::SmallVec::from_slice(ctx.catalog.bindings_for_consumer(consumer_id));
                    have_binding_ids = true;
                }
                for &bid in &binding_ids {
                    if let Some(binding) = ctx.catalog.binding_mut(bid) {
                        if binding.stream_id != ack.stream_id
                            || binding.connection_id != conn_id
                        {
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
