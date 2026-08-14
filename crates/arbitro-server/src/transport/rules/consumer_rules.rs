//! Every rule a consumer must satisfy to be created.
//!
//! A consumer reads a sub-slice of its stream. It may narrow, never widen,
//! and never step onto a sibling's ground.

use super::{is_global, Violation};
use arbitro_engine_v2::common::subject_covers;

/// A consumer declaring no filter takes its stream's slice. Returns the
/// filter the consumer should be stored with.
pub fn inherits_the_stream_filter<'a>(filter: &'a [u8], stream_filter: &'a [u8]) -> &'a [u8] {
    if filter.is_empty() {
        return stream_filter;
    }
    filter
}

/// A consumer may not reach outside the stream that holds it — it would be
/// asking for subjects the stream never receives.
pub fn stays_inside_its_stream(filter: &[u8], stream_filter: &[u8]) -> Result<(), Violation> {
    if is_global(stream_filter) || filter.is_empty() {
        return Ok(());
    }
    if !subject_covers(stream_filter, filter) {
        return Err(Violation::ConsumerOutsideStream);
    }
    Ok(())
}

/// Consumers on one stream are siblings. Equal filters are fanout — both
/// see everything, both ack independently, which is intended. A filter
/// strictly inside another's is not: the same subject would belong to two
/// readers where only one is the source of truth for its delivery.
///
/// `siblings` is every filter already held by another consumer on the same
/// stream.
pub fn not_nested_under_a_sibling(filter: &[u8], siblings: &[&[u8]]) -> Result<(), Violation> {
    if is_global(filter) {
        return Ok(());
    }
    for other in siblings {
        if is_global(other) || *other == filter {
            continue;
        }
        let nested = subject_covers(other, filter) || subject_covers(filter, other);
        if nested {
            return Err(Violation::ConsumerNestedUnderSibling);
        }
    }
    Ok(())
}

/// Every rule that applies when a consumer is created, in order.
///
/// Returns the filter to store: the consumer's own, or the stream's when it
/// declared none.
///
/// `not_nested_under_a_sibling` is written and tested but NOT called here:
/// while a stream may still hold `>`, inheritance gives every filterless
/// consumer that same `>`, which makes any consumer that does declare a
/// filter nested under it and bans all specialisation. It joins this chain
/// once `stream_rules::filter_is_not_global` is enforced.
pub fn on_create<'a>(
    filter: &'a [u8],
    stream_filter: &'a [u8],
) -> Result<&'a [u8], Violation> {
    stays_inside_its_stream(filter, stream_filter)?;
    Ok(inherits_the_stream_filter(filter, stream_filter))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_consumer_without_a_filter_inherits_the_stream() {
        assert_eq!(on_create(b"", b"orders.>"), Ok(b"orders.>".as_slice()));
    }

    #[test]
    fn a_consumer_may_narrow_within_its_stream() {
        assert_eq!(
            on_create(b"orders.premium.>", b"orders.>"),
            Ok(b"orders.premium.>".as_slice())
        );
    }

    #[test]
    fn a_consumer_may_not_reach_outside_its_stream() {
        assert_eq!(
            on_create(b"payments.>", b"orders.>"),
            Err(Violation::ConsumerOutsideStream)
        );
    }

    #[test]
    fn a_consumer_on_a_global_stream_keeps_its_own_filter() {
        assert_eq!(on_create(b"orders.>", b">"), Ok(b"orders.>".as_slice()));
    }

    #[test]
    fn siblings_may_share_one_filter() {
        assert_eq!(
            not_nested_under_a_sibling(b"orders.>", &[b"orders.>".as_slice()]),
            Ok(())
        );
    }

    #[test]
    fn a_consumer_may_not_nest_under_a_sibling() {
        assert_eq!(
            not_nested_under_a_sibling(b"orders.premium.>", &[b"orders.>".as_slice()]),
            Err(Violation::ConsumerNestedUnderSibling)
        );
    }

    #[test]
    fn disjoint_siblings_coexist() {
        assert_eq!(
            not_nested_under_a_sibling(b"orders.cheap.>", &[b"orders.premium.>".as_slice()]),
            Ok(())
        );
    }
}
