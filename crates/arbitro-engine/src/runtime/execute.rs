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
            ref entries,
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
                    for entry in entries.iter() {
                        ctx.inflight
                            .inc_pending(entry.subject_hash, consumer_raw, queue_raw);
                    }
                    if let Some(binding) = ctx.catalog.binding_mut(binding_id) {
                        for entry in entries.iter() {
                            binding.pending.push(Pending {
                                seq: entry.seq,
                                subject_hash: entry.subject_hash,
                                _pad: 0,
                            });
                        }
                    }
                }
            }
        }

        Command::Ack {
            consumer_id,
            ref entries,
        } => {
            m.ack_accepted
                .fetch_add(entries.len() as u64, Ordering::Relaxed);

            // Find bindings for this consumer and release matching pendings.
            let binding_ids: Vec<_> = ctx.catalog.bindings_for_consumer(consumer_id).to_vec();
            for bid in binding_ids {
                if let Some(binding) = ctx.catalog.binding_mut(bid) {
                    let queue_raw = binding.queue_id.raw();
                    for ack in entries.iter() {
                        if let Some(pos) =
                            binding.pending.iter().position(|p| p.seq == ack.seq)
                        {
                            let pending = binding.pending.swap_remove(pos);
                            events
                                .subject_hashes_acked
                                .push((consumer_id.raw(), pending.subject_hash));
                            ctx.inflight.dec_pending(
                                pending.subject_hash,
                                consumer_id.raw(),
                                queue_raw,
                            );
                        }
                    }
                }
            }
        }

        Command::Nack {
            consumer_id,
            ref entries,
        } => {
            m.nack_accepted
                .fetch_add(entries.len() as u64, Ordering::Relaxed);

            // Release inflight — redelivery handled by drain.
            let binding_ids: Vec<_> = ctx.catalog.bindings_for_consumer(consumer_id).to_vec();
            for bid in binding_ids {
                if let Some(binding) = ctx.catalog.binding_mut(bid) {
                    let queue_raw = binding.queue_id.raw();
                    for ack in entries.iter() {
                        if let Some(pos) =
                            binding.pending.iter().position(|p| p.seq == ack.seq)
                        {
                            let pending = binding.pending.swap_remove(pos);
                            events
                                .subject_hashes_acked
                                .push((consumer_id.raw(), pending.subject_hash));
                            ctx.inflight.dec_pending(
                                pending.subject_hash,
                                consumer_id.raw(),
                                queue_raw,
                            );
                        }
                    }
                }
            }
        }

        Command::PublishAccepted { .. } => {
            m.publish_entries_accepted
                .fetch_add(1, Ordering::Relaxed);
        }

        Command::Tombstone { reason, .. } => match reason {
            DropReason::Expired | DropReason::Tombstoned | DropReason::NoSubscribers => {
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
