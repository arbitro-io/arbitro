//! Publish handler — the hot path.
//!
//! Publish ONLY appends to the store and signals the drain.
//! It does NOT know about consumers or delivery.
//! The drain task delivers reactively via deliver_cycle().
//!
//! ONE lock: append + signal under the same shard lock (R19).
//! RepOk sent OUTSIDE the lock — publisher unblocked immediately.

use core::sync::atomic::Ordering::Relaxed;

use arbitro_proto::error::ErrorCode;
use arbitro_proto::event::StreamEvent;
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

impl Default for PublishScratch {
    fn default() -> Self {
        Self::new()
    }
}

impl PublishScratch {
    pub fn new() -> Self {
        Self {
            entries: Vec::with_capacity(64),
        }
    }
}

/// Handle a Publish frame (batch of entries).
/// ONE lock: validate + append + signal (R19). RepOk OUTSIDE lock.
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

    // ── ONE lock: validate + append + signal (R19) ─────────────────
    let result = ctx.streams.with_mut(stream_id, |slot| {
        let info = slot.store.info();
        let cfg = &slot.config;

        use arbitro_proto::config::RetentionPolicy;

        if cfg.retention == RetentionPolicy::Limits {
            let current_info = info;
            let overflow_msgs = (current_info.messages + entry_count as u64).saturating_sub(cfg.max_msgs);
            
            // Calculate bytes added by this batch
            let added_bytes: u64 = scratch.entries.iter()
                .map(|e| (e.subject.len() + e.payload.len()) as u64)
                .sum();
            
            let overflow_bytes = if cfg.max_bytes > 0 {
                (current_info.bytes + added_bytes).saturating_sub(cfg.max_bytes)
            } else {
                0
            };

            if (cfg.max_msgs > 0 && overflow_msgs > 0) || (cfg.max_bytes > 0 && overflow_bytes > 0) {
                 // Discard old until we fit. For efficiency, we tell the store 
                 // the exact first_seq it should have now.
                 // This is much faster than one-by-one.
                 let mut target_first = current_info.first_seq;
                 
                 // If we have msgs limit, we need to jump ahead at least overflow_msgs
                 if cfg.max_msgs > 0 && overflow_msgs > 0 {
                    target_first += overflow_msgs;
                 }
                 
                 // Note: for bytes, truncate_front is less precise as it operates on segments/offsets.
                 // But once we truncate msgs, bytes will drop too.
                 slot.store.truncate_front(target_first);
            }
        } else {
            // Strict Limits Policy
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
        }

        // Append to journal — THE ONE COPY
        let first_seq = slot.store.append_batch(&scratch.entries, timestamp)
            .map_err(|_| ErrorCode::StreamFull)?;

        // Signal drain task (non-blocking, O(1)).
        // Delivery is the drain task's job, NOT publish's.
        slot.signal.release();

        // Emit broadcast event (non-blocking, O(1)).
        // Listeners (monitors, loggers, etc.) process this asynchronously.
        let _ = slot.event_tx.send(StreamEvent::MessagePublished {
            stream_id,
            first_seq,
            count: entry_count as u16,
        });

        Ok((first_seq, entry_count))
    });

    // ── RepOk OUTSIDE lock — publisher unblocked ────────────────────
    match result {
        Some(Ok((first_seq, count))) => {
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
/// Public for use by Fetch handler.
#[inline]
pub fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
