//! Does this stream have room — one arithmetic, in one place.
//!
//! `DiscardPolicy::New` rejects a publish that would take the stream over
//! its limit, which is a sentence with three parts: is there a limit, what
//! would this publish add, and does the total fit. That sentence was
//! written three times — single publish, publish-with-reply, batch —
//! differing only in how the addition is counted, and each free to drift.
//!
//! What stays with the caller is the reply, because the three answer the
//! client differently. What moves here is the rule.
//!
//! ## Only `discard == 1` reaches this
//!
//! `DiscardPolicy::Old` evicts to make room instead of refusing, so there
//! is nothing to pre-check for it. `of` returns `None` and the caller
//! never asks — that is the fast path, and it is the common one.

use arbitro_common::name_registry::Snapshot;
use arbitro_engine_v2::types::StreamId;
use arbitro_store::StoreInfo;

/// A stream's publish-time limit. Only exists for `DiscardPolicy::New`.
#[derive(Clone, Copy)]
pub(crate) struct Quota {
    max_msgs: u64,
    max_bytes: u64,
}

impl Quota {
    /// `None` when this stream refuses nothing at publish time.
    #[inline]
    pub(crate) fn of(cat: &Snapshot<'_>, stream: StreamId) -> Option<Self> {
        let q = cat.stream_quota(stream)?;
        // 1 = New: reject the publish. 0 = Old: evict instead, nothing to
        // check here.
        if q.discard != 1 {
            return None;
        }
        Some(Self {
            max_msgs: q.max_msgs,
            max_bytes: q.max_bytes,
        })
    }

    /// Would `count` more messages and `bytes` more bytes still fit?
    ///
    /// A limit of `0` is "no limit", not "no room" — the same convention
    /// the catalog stores and the one an inverted comparison here would
    /// silently turn into a stream that refuses everything.
    #[inline]
    pub(crate) fn admits(&self, info: &StoreInfo, count: u64, bytes: u64) -> bool {
        if self.max_msgs > 0 && info.messages + count > self.max_msgs {
            return false;
        }
        if self.max_bytes > 0 && info.bytes + bytes > self.max_bytes {
            return false;
        }
        true
    }
}
