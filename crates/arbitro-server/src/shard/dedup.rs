//! Admission by message id — one rule, one place.
//!
//! The rule is a sentence: a stream with a window rejects a `msg_id` it has
//! already seen inside it. It was written out six times — three publish
//! variants at dispatch, the routed publish, the delayed publish, and the
//! router's own branch — each repeating the same borrow dance and the same
//! rollback, and each free to drift from the others.
//!
//! This is that sentence, once. What differs between callers is where the
//! `msg_id` comes from, not what happens to it.
//!
//! ## Rollback is part of admitting a batch
//!
//! A batch is all-or-nothing: the first duplicate un-records the ids the
//! same call already inserted, so a rejected batch leaves the window
//! exactly as it found it and a retry behaves like the first attempt. That
//! is not a caller's job to remember — a caller that forgot would leave the
//! window claiming ids for messages that were never stored.
//!
//! ## What it does NOT do
//!
//! It does not decide whether it may run. `Dedup` is the shard's, and the
//! shard's own code calls it. The `Option` in `open` exists only for the
//! callers that still arrive from somewhere else, and it disappears with
//! them.

use arbitro_engine_v2::types::StreamId;

use super::idempotency::{idempotency_for_stream, SharedIdempotency};

/// A stream's admission window, open for one caller.
pub(crate) struct Window {
    map: SharedIdempotency,
    stream: StreamId,
    window_ms: u32,
}

impl Window {
    /// Admit one id. `false` = already seen inside the window.
    pub(crate) fn admit(&self, msg_id: &[u8]) -> bool {
        let hash = crate::transport::dispatch_v2::idempotency_hash(msg_id);
        let tracker = idempotency_for_stream(&self.map, self.stream);
        let ok = tracker
            .borrow_mut()
            .record(self.stream, hash, msg_id, self.window_ms);
        ok
    }

    /// Admit a whole batch, or none of it.
    ///
    /// `ids` yields what each entry carries; entries without an id are
    /// skipped by the caller yielding `None` for them.
    pub(crate) fn admit_all<'a, I>(&self, ids: I) -> bool
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let tracker = idempotency_for_stream(&self.map, self.stream);
        let mut t = tracker.borrow_mut();

        // Borrowed from the caller's buffer, which outlives this call, so
        // the rollback needs no owned copies.
        let mut inserted: smallvec::SmallVec<[(u64, &'a [u8]); 16]> = smallvec::SmallVec::new();
        for msg_id in ids {
            let hash = crate::transport::dispatch_v2::idempotency_hash(msg_id);
            if !t.record(self.stream, hash, msg_id, self.window_ms) {
                for (h, id) in &inserted {
                    t.forget(self.stream, *h, id);
                }
                return false;
            }
            inserted.push((hash, msg_id));
        }
        true
    }
}

/// The shard's admission windows.
pub(crate) struct Dedup;

impl Dedup {
    /// Open `stream`'s window, or `None` when it has none — or when the
    /// caller is not on the shard that owns it, which is the case this
    /// module exists to see removed.
    pub(crate) fn open(
        map: Option<SharedIdempotency>,
        stream: StreamId,
        window_ms: u32,
    ) -> Option<Window> {
        if window_ms == 0 {
            return None;
        }
        map.map(|map| Window {
            map,
            stream,
            window_ms,
        })
    }
}
