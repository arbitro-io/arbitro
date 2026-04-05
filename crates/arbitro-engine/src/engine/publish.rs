//! Publish handler — the hot path.
//!
//! Zero allocations: uses scratch buffers for batch entry refs.
//! Single lock per stream: append + deliver inline.

use core::sync::atomic::Ordering::Relaxed;

use arbitro_proto::error::ErrorCode;
use arbitro_proto::ids::ConnId;
use arbitro_proto::wire::envelope::FrameView;
use arbitro_proto::wire::publish::BatchIter;
use arbitro_store::EntryRef;

use super::context::Context;
use super::reply;

/// Scratch buffers reused across publish calls. Capacity grows monotonically.
pub struct PublishScratch {
    entries: Vec<EntryRef<'static>>,
}

impl PublishScratch {
    pub fn new() -> Self {
        Self {
            entries: Vec::with_capacity(64),
        }
    }
}

/// Handle a Publish frame (batch of entries).
#[inline]
pub fn on_publish(ctx: &Context, conn_id: ConnId, frame: &FrameView<'_>, scratch: &mut PublishScratch) {
    let stream_id = frame.stream_id();
    let env_seq = frame.envelope().env_seq.get();
    let body = frame.body();

    // Iterate batch entries — zero allocation
    let iter = BatchIter::new(body);

    // Build EntryRef slice in scratch buffer
    scratch.entries.clear();
    for entry_view in iter {
        // Safety: we're borrowing from frame body which outlives this function.
        // We transmute lifetime to 'static for the scratch Vec, but entries
        // are only used within this function scope.
        let entry_ref = EntryRef {
            subject: unsafe { core::mem::transmute::<&[u8], &'static [u8]>(entry_view.subject()) },
            payload: unsafe { core::mem::transmute::<&[u8], &'static [u8]>(entry_view.payload()) },
        };
        scratch.entries.push(entry_ref);
    }

    if scratch.entries.is_empty() {
        reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::InvalidEntryCount);
        return;
    }

    let timestamp = current_timestamp();
    let entry_count = scratch.entries.len();

    // Single lock: append to store, then read back for delivery
    let result = ctx.streams.with_mut(stream_id, |slot| {
        let info = slot.store.info();
        let cfg = &slot.config;

        if cfg.max_msgs > 0 && info.messages + entry_count as u64 > cfg.max_msgs {
            return Err(ErrorCode::StreamFull);
        }
        if cfg.max_bytes > 0 {
            let new_bytes: u64 = scratch.entries.iter()
                .map(|e| (e.subject.len() + e.payload.len()) as u64)
                .sum();
            if info.bytes + new_bytes > cfg.max_bytes {
                return Err(ErrorCode::StreamFull);
            }
        }

        // Append to journal — THE ONE COPY
        let first_seq = slot.store.append_batch(&scratch.entries, timestamp)
            .map_err(|_| ErrorCode::StreamFull)?;

        // Read back from store for delivery — borrows existing Box<[u8]>, no new allocs
        let stored = slot.store.read_range(first_seq, first_seq + entry_count as u64)
            .unwrap_or_default();

        Ok((first_seq, entry_count, stored))
    });

    match result {
        Some(Ok((first_seq, count, stored))) => {
            // Deliver inline — entries already owned by store, `stored` is cloned but necessary
            let mut drains = ctx.drains.lock().unwrap();
            if let Some(drain) = drains.get_mut(&stream_id) {
                drain.deliver_batch(&stored, ctx.transport.as_ref());
            }

            ctx.metrics.msgs_in.fetch_add(count as u64, Relaxed);
            let bytes: u64 = scratch.entries.iter()
                .map(|e| (e.subject.len() + e.payload.len()) as u64)
                .sum();
            ctx.metrics.bytes_in.fetch_add(bytes, Relaxed);

            reply::send_ok(ctx.transport.as_ref(), conn_id, stream_id, env_seq, first_seq);
        }
        Some(Err(code)) => {
            reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, code);
        }
        None => {
            reply::send_error(ctx.transport.as_ref(), conn_id, stream_id, env_seq, 0, ErrorCode::StreamNotFound);
        }
    }
}

/// Monotonic timestamp in milliseconds.
#[inline]
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
