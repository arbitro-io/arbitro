//! Kernel dispatch — apply a `Command` to the engine state.
//!
//! Level 7. The single hot-path entry point that the shard drainer and
//! inbound translator call. Every mutation of engine state on the
//! Command-based path flows through here.
//!
//! **W3.1 scope (parallel to legacy API):** this function exists so that
//! the `Command` vocabulary is live and callable, and metrics counters
//! advance on each variant. Actual state mutation (publish / ack / fanout
//! wiring) is still performed by the legacy `on_publish` / `on_ack` /
//! `drain_fanout` paths — the new drainer (Fase 2) will wire execute to
//! the existing internals. Until then, `execute` is **observational**
//! only: it touches metrics and nothing else. This keeps the server
//! compiling unchanged while the new vocabulary lands.
//!
//! Rule compliance:
//! - No allocations: every variant is a `fetch_add` + maybe a `match`.
//! - `Ordering::Relaxed` — metrics have no ordering deps.
//! - `&mut EngineContext` respects the single-writer invariant.

use std::sync::atomic::Ordering;

use crate::command::{Command, DropReason};
use crate::context::EngineContext;

/// Dispatch a single command.
///
/// Hot path. Must be branch-predictable and alloc-free.
#[inline]
pub fn apply(ctx: &mut EngineContext, cmd: &Command<'_>) {
    let m = &ctx.metrics;
    match *cmd {
        Command::Fanout {
            consumers,
            entries,
            ..
        } => {
            // Count entries × consumers as "fanout notified" for parity
            // with the legacy `publish_fanout_notified` counter. When the
            // drainer lands in Fase 2 this becomes the authoritative
            // increment site.
            let total = (consumers.len() as u64).saturating_mul(entries.len() as u64);
            m.publish_fanout_notified.fetch_add(total, Ordering::Relaxed);
            m.claim_entries_delivered
                .fetch_add(entries.len() as u64, Ordering::Relaxed);
        }

        Command::Queue { .. } => {
            m.claim_entries_delivered.fetch_add(1, Ordering::Relaxed);
            m.publish_queues_pushed.fetch_add(1, Ordering::Relaxed);
        }

        Command::Ack { entries } => {
            m.ack_accepted
                .fetch_add(entries.len() as u64, Ordering::Relaxed);
        }

        Command::Nack { entries } => {
            m.nack_accepted
                .fetch_add(entries.len() as u64, Ordering::Relaxed);
        }

        Command::RepOk { entries, .. } => {
            m.publish_entries_accepted
                .fetch_add(entries.len() as u64, Ordering::Relaxed);
        }

        Command::Tombstone { reason, .. } => match reason {
            // Expired / Tombstoned / NoSubscribers all fold into the
            // existing `publish_no_match` counter for now. A dedicated
            // drop-reason trio will land with the new metrics block in
            // W6, alongside the full drainer migration.
            DropReason::Expired
            | DropReason::Tombstoned
            | DropReason::NoSubscribers => {
                m.publish_no_match.fetch_add(1, Ordering::Relaxed);
            }
        },
    }
}

/// Dispatch a slice of commands in order.
///
/// Thin wrapper — kept as a named entry point so the drainer can call
/// `runtime::execute::apply_batch` symmetrically with `apply`.
#[inline]
pub fn apply_batch(ctx: &mut EngineContext, cmds: &[Command<'_>]) {
    for cmd in cmds {
        apply(ctx, cmd);
    }
}
