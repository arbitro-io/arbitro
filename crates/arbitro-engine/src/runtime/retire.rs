//! `retire_binding` primitive — shared by `delete_stream`,
//! `delete_consumer`, `mark_connection_dead`.
//!
//! Level 7 — depends on context, catalog, inflight.
//!
//! Walks the binding's `Vec<Pending>`, decrements consumer/queue
//! inflight counters, emits each pending `(consumer, subject_hash)` so
//! the server can release its drain-side `ConsumerSubjects` slot,
//! removes the binding from catalog indices, emits `bindings_retired`
//! in `DeltaEvents`.

use crate::context::EngineContext;
use crate::events::DeltaEvents;
use crate::types::BindingId;

/// Retire a single binding: release inflight credits for all pending
/// entries, remove from catalog indices, emit event.
pub fn retire_binding(ctx: &mut EngineContext, binding_id: BindingId, events: &mut DeltaEvents) {
    // Catalog removes the binding from all maps/indices and returns it.
    let Some(binding) = ctx.catalog.retire_binding(binding_id, events) else {
        return;
    };

    // Release inflight for every pending entry on this binding.
    let consumer_raw = binding.consumer_id.raw();
    let queue_raw = binding.queue_id.raw();
    for pending in &binding.pending {
        events
            .subject_hashes_acked
            .push((consumer_raw, pending.subject_hash));
        ctx.inflight.dec_pending(consumer_raw, queue_raw);
    }
}

/// Retire all bindings for a stream.
pub fn retire_bindings_for_stream(
    ctx: &mut EngineContext,
    stream_id: crate::types::StreamId,
    events: &mut DeltaEvents,
) {
    let binding_ids: Vec<_> = ctx.catalog.bindings_for_stream(stream_id).to_vec();
    for bid in binding_ids {
        retire_binding(ctx, bid, events);
    }
}

/// Retire all bindings for a consumer.
pub fn retire_bindings_for_consumer(
    ctx: &mut EngineContext,
    consumer_id: crate::types::ConsumerId,
    events: &mut DeltaEvents,
) {
    let binding_ids: Vec<_> = ctx.catalog.bindings_for_consumer(consumer_id).to_vec();
    for bid in binding_ids {
        retire_binding(ctx, bid, events);
    }
}

/// Retire all bindings for a connection.
pub fn retire_bindings_for_connection(
    ctx: &mut EngineContext,
    connection_id: crate::types::ConnectionId,
    events: &mut DeltaEvents,
) {
    let binding_ids: Vec<_> = ctx.catalog.bindings_for_connection(connection_id).to_vec();
    for bid in binding_ids {
        retire_binding(ctx, bid, events);
    }
}
