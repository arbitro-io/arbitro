//! Drain-event ring — SPSC `command → drain` channel.
//!
//! The command thread (tokio task that owns the engine) emits events that
//! mutate per-consumer drain-side state. The drain OS thread consumes
//! them at the start of each drain cycle via non-blocking `try_recv`,
//! then re-evaluates delivery capacity.
//!
//! ## Why a ring (not direct calls)
//!
//! Subject inflight is owned exclusively by the drain thread (`Vec<Option<
//! ConsumerSubjects>>` indexed by `ConsumerId`). The command thread can't
//! mutate it directly without breaking single-thread ownership and forcing
//! locks back in. The ring lets the command thread fire decrement events
//! and continue immediately — the drain applies them in order.
//!
//! ## Ordering guarantee
//!
//! `DrainEvent::Ack` is processed in the order it was sent. Since the
//! ring is SPSC, ordering is trivially preserved. The drain processes the
//! ring before each cycle, so an ack emitted at T is visible to any
//! delivery decision made at T+ε.
//!
//! ## Wake-up coupling
//!
//! After every send, the command thread calls `gate.release()` so the
//! drain wakes (if it was parked) and processes the ring even with no
//! new publishes. Multiple releases coalesce via `fetch_or` — no extra
//! cost in burst patterns.
//!
//! ## Capacity & overflow policy
//!
//! Capacity is fixed (`DRAIN_EVENT_CAP`). If the ring is full, `try_send`
//! returns `Err` and the command thread RETAINS the event for retry —
//! `pending_drain_acks` for `Ack` events, `pending_consumer_remove` (H11)
//! for `ConsumerRemoved` — both flushed at the top of every command-loop
//! iteration. Events must NEVER be dropped: since the suppression set
//! (`ConsumerSubjects::suppressed`) became authoritative for redelivery,
//! a lost `Released` leaves its seq suppressed forever (the contiguous
//! floor cannot rise past an unacked seq, and resubscribe replays from
//! above it) — permanent starvation, strictly worse than any transient
//! stall. Overflow is also NOT stall-only: `apply_delta_and_sync` pushes
//! a whole delta before `gate.release()`, so a single bulk release with
//! more than CAP pendings (bulk nack, connection death with a large
//! in-flight window) overflows deterministically while the drain is
//! parked. `silent_drops.drain_evt` stays as the overflow degradation
//! counter even though the events are retried, not lost.

use arbitro_engine_v2::types::*;
use arbitro_kit::stream::{Consumer, Producer, Ring};

/// Capacity of the SPSC ring. Power-of-two by contract.
///
/// Sized for the typical ack burst between drain cycles (drain cycles
/// run every µs-scale when busy). 2048 events ≈ 20× the largest
/// observed ack-batch in stress tests; the existing NotifyRing already
/// covers the 8192 path drain → command. Keeping this struct smaller
/// avoids piling extra stack onto `ShardRouter::spawn` (each Ring is
/// briefly stack-allocated before being moved into Arc).
pub const DRAIN_EVENT_CAP: usize = 2048;

/// SPSC ring carrying [`DrainEvent`] from command → drain.
///
/// Uses `ParkWaiter` (sync default) because both ends use only the
/// non-blocking `try_send` / `try_recv` API. The drain is woken via the
/// shared `Gate`, not via the ring's own waiter.
pub type DrainEventRing = Ring<DrainEvent, DRAIN_EVENT_CAP, arbitro_kit::ParkWaiter>;
pub type DrainEventProducer = Producer<DrainEvent, DRAIN_EVENT_CAP, arbitro_kit::ParkWaiter>;
pub type DrainEventConsumer = Consumer<DrainEvent, DRAIN_EVENT_CAP, arbitro_kit::ParkWaiter>;

/// Messages from command thread → drain thread.
///
/// Each variant maps to a single per-consumer mutation the drain must
/// apply between cycles. Keep this enum small — every byte travels the
/// ring on every event.
#[derive(Debug, Clone, Copy)]
pub enum DrainEvent {
    /// One delivered message was acked or retired. Drain decrements
    /// the per-consumer subject inflight by 1 and raises the consumer's
    /// contiguous-acked floor to `ack_floor` (monotonic max — carrying
    /// the current floor on every event makes a dropped ring event
    /// self-healing on the next ack).
    Ack {
        consumer_id: ConsumerId,
        subject_hash: u32,
        /// Contiguous-acked floor at send time (`shard/ack_floor.rs`).
        ack_floor: u64,
        /// The exact seq whose pending delivery this event refers to
        /// (0 only for `SuppressOp::None` reconciliation events).
        seq: u64,
        /// What to do with `seq` in the drain's suppression set — see
        /// [`SuppressOp`].
        op: SuppressOp,
    },
    /// Consumer was deleted / its bindings retired. Drain drops the
    /// per-consumer slot entirely so the subject map is freed.
    ConsumerRemoved { consumer_id: ConsumerId },
}

/// Effect of a released pending entry on the drain's per-consumer
/// suppression set (`ConsumerSubjects::suppressed` — delivered-or-acked
/// seqs above the contiguous floor that must not be re-sent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuppressOp {
    /// Genuine ack (engine-matched against a pending delivery): the seq
    /// is done forever — (re)assert suppression.
    Acked,
    /// Pending released WITHOUT an ack (nack, ack-timeout auto-nack,
    /// binding retirement): the seq is owed again — remove it from the
    /// suppression set so the drain may redeliver it.
    Released,
    /// Counter-only reconciliation (duplicate-delivery correction):
    /// leave the suppression set untouched.
    None,
}
