//! Every rule a subscription must satisfy to be created.
//!
//! A subscription is the narrowest thing in the chain. It sits under a
//! consumer, which sits under a stream, so confining it to its consumer
//! confines it to the stream too.

use super::{is_global, Violation};
use arbitro_engine_v2::common::subject_covers;

/// A subscription declaring no filter takes its consumer's.
pub fn inherits_the_consumer_filter(filters: &[Vec<u8>], consumer_filter: &[u8]) -> Vec<Vec<u8>> {
    if filters.is_empty() && !consumer_filter.is_empty() {
        return vec![consumer_filter.to_vec()];
    }
    filters.to_vec()
}

/// A subscription may not reach outside its consumer. Because the consumer
/// is already confined to its stream, this transitively confines the
/// subscription to the stream — checking both separately is redundant.
pub fn stays_inside_its_consumer(
    filters: &[Vec<u8>],
    consumer_filter: &[u8],
) -> Result<(), Violation> {
    if is_global(consumer_filter) {
        return Ok(());
    }
    for f in filters {
        if !subject_covers(consumer_filter, f) {
            return Err(Violation::SubscriptionOutsideConsumer);
        }
    }
    Ok(())
}

/// Every rule that applies when a subscription is created, in order.
///
/// Returns the filter list to store: its own, or the consumer's when it
/// declared none.
///
/// Currently enforced inside the engine catalog rather than here — that
/// check is local to one stream, so the shard split does not hide anything
/// from it. This module is the migration target, not yet the live path.
pub fn on_create(
    filters: &[Vec<u8>],
    consumer_filter: &[u8],
) -> Result<Vec<Vec<u8>>, Violation> {
    stays_inside_its_consumer(filters, consumer_filter)?;
    Ok(inherits_the_consumer_filter(filters, consumer_filter))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_subscription_without_a_filter_inherits_the_consumer() {
        assert_eq!(
            on_create(&[], b"orders.premium.>"),
            Ok(vec![b"orders.premium.>".to_vec()])
        );
    }

    #[test]
    fn a_subscription_may_narrow_within_its_consumer() {
        assert_eq!(
            on_create(&[b"orders.premium.eu".to_vec()], b"orders.premium.>"),
            Ok(vec![b"orders.premium.eu".to_vec()])
        );
    }

    #[test]
    fn a_subscription_may_not_reach_outside_its_consumer() {
        assert_eq!(
            on_create(&[b"orders.cheap.>".to_vec()], b"orders.premium.>"),
            Err(Violation::SubscriptionOutsideConsumer)
        );
    }

    #[test]
    fn one_bad_filter_rejects_the_whole_list() {
        assert_eq!(
            on_create(
                &[b"orders.premium.eu".to_vec(), b"payments.>".to_vec()],
                b"orders.premium.>"
            ),
            Err(Violation::SubscriptionOutsideConsumer)
        );
    }
}
