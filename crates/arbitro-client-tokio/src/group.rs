//! Queue-group defaulting — one rule, shared by every ergonomic
//! consumer-creating path.
//!
//! # Why this exists
//!
//! `group` and `deliver_mode` are two independent knobs, and the broker
//! treats `deliver_mode` as the sole determinant of how messages fan out.
//! An empty `group` in `DeliverMode::Queue` does **not** mean "no queue":
//! the broker allocates a real shared queue keyed `(stream_id, "")`, so
//! every no-group queue consumer on that stream silently lands in ONE
//! anonymous queue and starts stealing each other's messages.
//!
//! The fix is client-side and identical in all four clients: never put an
//! empty group on the wire. Resolve it, in order, to
//!
//! 1. the explicitly-set `group`,
//! 2. else the consumer `name`,
//! 3. else the stream name.
//!
//! This mirrors the TypeScript client (`config.group ?? config.name ??
//! streamName`) and is the reference behaviour for the whole family.
//!
//! Note the emptiness test is "is it empty", not "is it absent" — an
//! explicit `b""` must fall through to the next candidate, exactly like
//! `||` in the TS client (their `??` at `createConsumerRaw` lets an
//! explicit `''` leak onto the wire; do not port that flaw).
//!
//! # Not a server default
//!
//! `arbitro_proto::config::consumer::ConsumerConfigBuilder` contains a
//! stream-name fallback, but nothing on the wire path ever reaches it —
//! `CreateConsumer` frames are decoded straight into the dispatcher. The
//! group you send is the group you get. That is why the defaulting has to
//! happen here, in the client.
//!
//! # Where it is applied
//!
//! Every ergonomic path: [`crate::ConsumerBuilder`], `queue_subscribe`,
//! and the service / workflow builders (which pass an explicit group).
//! Deliberately NOT applied by the raw wire methods
//! `Client::create_consumer{,_with_limits}` / `upsert_consumer`: those
//! encode their arguments verbatim, which is what keeps the broker's
//! empty-group rejection reachable and testable from Rust.

use crate::error::ClientError;

/// First non-empty of `group`, `name`, `stream_name`.
///
/// Returns [`ClientError::InvalidConfig`] when all three are empty, rather
/// than putting an empty group on the wire: the broker cannot distinguish
/// "no group" from "the anonymous shared queue", so a request with nothing
/// to key the queue on is a client bug and must fail loudly and locally.
///
/// Callers that have no stream name in hand (the wire API is keyed by
/// `stream_id`) pass `b""` for `stream_name` — the consumer name then
/// carries the default, which is what every in-tree path relies on.
pub(crate) fn resolve_group<'a>(
    group: &'a [u8],
    name: &'a [u8],
    stream_name: &'a [u8],
) -> Result<&'a [u8], ClientError> {
    if !group.is_empty() {
        return Ok(group);
    }
    if !name.is_empty() {
        return Ok(name);
    }
    if !stream_name.is_empty() {
        return Ok(stream_name);
    }
    Err(ClientError::InvalidConfig(
        "consumer needs a queue group: none of group, consumer name or \
         stream name was set. An empty group is rejected by the broker — \
         it would otherwise share one anonymous queue with every other \
         group-less consumer on the stream"
            .into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_group_wins() {
        assert_eq!(
            resolve_group(b"workers", b"cname", b"sname").unwrap(),
            b"workers"
        );
    }

    #[test]
    fn empty_group_falls_back_to_consumer_name() {
        assert_eq!(resolve_group(b"", b"cname", b"sname").unwrap(), b"cname");
    }

    #[test]
    fn empty_group_and_name_fall_back_to_stream_name() {
        assert_eq!(resolve_group(b"", b"", b"sname").unwrap(), b"sname");
    }

    #[test]
    fn all_empty_is_rejected() {
        let err = resolve_group(b"", b"", b"").unwrap_err();
        match err {
            ClientError::InvalidConfig(msg) => assert!(msg.contains("queue group")),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }
}
