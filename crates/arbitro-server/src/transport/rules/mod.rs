//! Catalog admission rules — the single place that decides whether a
//! stream, consumer, or subscription may exist.
//!
//! One module per entity, one function per rule. Every rule is pure: it
//! takes the thing being created plus the things already claimed, and
//! returns a verdict. No locks, no I/O, no engine, no shards.
//!
//! Why here and not in the engine: the engine's catalog is sharded, so it
//! only ever sees the streams assigned to its own shard. `NameRegistry` is
//! the one structure that sees every stream and every consumer, and it
//! lives above the shard split — which is where these rules must run.

pub mod consumer_rules;
pub mod stream_rules;
pub mod subscription_rules;

/// A rejected creation. One variant per rule, so the dispatcher can map
/// each to its own wire code instead of collapsing them into one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Violation {
    /// A stream was created without a subject filter.
    StreamFilterMissing,
    /// A stream claimed `>` — every subject, leaving no slice for anyone.
    StreamFilterGlobal,
    /// Another stream already holds this exact slice.
    StreamFilterDuplicate,
    /// Another stream already holds a slice sharing subjects with this one.
    StreamFilterOverlap,
    /// A stream of that name exists holding a different slice.
    StreamFilterMismatch,
    /// The consumer's filter reaches outside its stream's slice.
    ConsumerOutsideStream,
    /// The consumer's filter nests strictly inside a sibling's.
    ConsumerNestedUnderSibling,
    /// The subscription's filter reaches outside its consumer's.
    SubscriptionOutsideConsumer,
    /// The subscription arrived without an id of its own.
    SubscriptionIdMissing,
}

impl Violation {
    /// The wire code a rejected creation travels back as.
    ///
    /// Every variant answers for itself here so no caller can flatten the
    /// set into one code. It used to: a stream rejected for claiming `>`
    /// came back as `StreamAlreadyExists`, which sent whoever hit it looking
    /// for a duplicate that did not exist.
    pub fn wire_code(self) -> arbitro_proto::error::ErrorCode {
        use arbitro_proto::error::ErrorCode as E;
        match self {
            Self::StreamFilterMissing | Self::StreamFilterGlobal => E::InvalidStreamFilter,
            Self::StreamFilterDuplicate
            | Self::StreamFilterOverlap
            | Self::StreamFilterMismatch => E::StreamFilterConflict,
            Self::ConsumerOutsideStream | Self::ConsumerNestedUnderSibling => {
                E::InvalidConsumerFilter
            }
            Self::SubscriptionOutsideConsumer | Self::SubscriptionIdMissing => {
                E::InvalidSubscriptionFilter
            }
        }
    }
}

/// `>` claims every subject, so it is the absence of a claim rather than a
/// claim to a slice. Containment and overlap rules skip it: with `>` on
/// both sides every pair collides and nothing could ever be created.
///
/// Disappears once `stream_rules::filter_is_not_global` is enforced —
/// there will be no global filters left for anything to skip.
#[inline]
pub(crate) fn is_global(filter: &[u8]) -> bool {
    filter.is_empty() || filter == b">"
}
