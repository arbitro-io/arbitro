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
//! Capacity is fixed (`DRAIN_EVENT_CAP`). If the ring is full, the
//! command thread loses the event (`try_send` returns `Err`). That can
//! only happen if the drain is stuck for ≥ CAP acks, which itself means
//! delivery has stalled — losing the event is no worse than the stall it
//! came from. We log it as a degradation signal and move on.

use arbitro_engine_v2::types::*;
use arbitro_kit::stream::Ring;

/// Capacity of the SPSC ring. Power-of-two by contract.
pub const DRAIN_EVENT_CAP: usize = 8192;

/// SPSC ring carrying [`DrainEvent`] from command → drain.
///
/// Uses `ParkWaiter` (sync default) because both ends use only the
/// non-blocking `try_send` / `try_recv` API. The drain is woken via the
/// shared `Gate`, not via the ring's own waiter.
pub type DrainEventRing = Ring<DrainEvent, DRAIN_EVENT_CAP, arbitro_kit::ParkWaiter>;

/// Messages from command thread → drain thread.
///
/// Each variant maps to a single per-consumer mutation the drain must
/// apply between cycles. Keep this enum small — every byte travels the
/// ring on every event.
#[derive(Debug, Clone, Copy)]
pub enum DrainEvent {
    /// One delivered message was acked or retired. Drain decrements
    /// the per-consumer subject inflight by 1.
    Ack {
        consumer_id: ConsumerId,
        subject_hash: u32,
    },
    /// Consumer was deleted / its bindings retired. Drain drops the
    /// per-consumer slot entirely so the subject map is freed.
    ConsumerRemoved { consumer_id: ConsumerId },
}
