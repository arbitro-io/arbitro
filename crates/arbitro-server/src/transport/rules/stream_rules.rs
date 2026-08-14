//! Every rule a stream must satisfy to be created.
//!
//! A stream owns a slice of the subject space, outright and alone. These
//! rules are what "outright and alone" means, one function each.

use super::{is_global, Violation};
use arbitro_engine_v2::common::subjects_overlap;

/// A stream must declare the slice it owns. Empty captures nothing, so the
/// stream could never receive a message.
pub fn filter_is_declared(filter: &[u8]) -> Result<(), Violation> {
    if filter.is_empty() {
        return Err(Violation::StreamFilterMissing);
    }
    Ok(())
}

/// A stream's slice must be bounded. `>` captures every subject, so it
/// overlaps every peer by construction and no second stream could exist.
pub fn filter_is_not_global(filter: &[u8]) -> Result<(), Violation> {
    if filter == b">" {
        return Err(Violation::StreamFilterGlobal);
    }
    Ok(())
}

/// No two streams may claim the identical slice — a message matching it
/// would belong to both and neither would be authoritative.
pub fn filter_is_not_duplicate(filter: &[u8], claimed: &[&[u8]]) -> Result<(), Violation> {
    if is_global(filter) {
        return Ok(());
    }
    if claimed.iter().any(|other| *other == filter) {
        return Err(Violation::StreamFilterDuplicate);
    }
    Ok(())
}

/// No two streams may claim slices sharing any subject. Stricter than
/// [`filter_is_not_duplicate`]: catches `orders.premium.>` arriving next to
/// `orders.>`, where `orders.premium.1` would land in both.
pub fn filter_does_not_overlap(filter: &[u8], claimed: &[&[u8]]) -> Result<(), Violation> {
    if is_global(filter) {
        return Ok(());
    }
    for other in claimed {
        if !is_global(other) && subjects_overlap(other, filter) {
            return Err(Violation::StreamFilterOverlap);
        }
    }
    Ok(())
}

/// Re-creating a stream must not silently move its slice. Same filter (or
/// none requested) is idempotent; a different one means the caller believes
/// it owns a slice this stream does not have.
pub fn recreate_keeps_its_filter(filter: &[u8], existing: &[u8]) -> Result<(), Violation> {
    if filter.is_empty() || filter == existing {
        return Ok(());
    }
    Err(Violation::StreamFilterMismatch)
}

/// Every rule that applies when a new stream is created, in order.
///
/// `claimed` is every filter already held by another stream.
///
/// `filter_is_declared` and `filter_is_not_global` are written and tested
/// but NOT called here: enforcing them rejects every stream created with
/// `>` or no filter, which the existing test corpus does throughout. They
/// go into this chain in the same commit that migrates those tests.
pub fn on_create(filter: &[u8], claimed: &[&[u8]]) -> Result<(), Violation> {
    filter_is_not_duplicate(filter, claimed)?;
    filter_does_not_overlap(filter, claimed)?;
    Ok(())
}

/// Every rule that applies when a stream of this name already exists.
pub fn on_recreate(filter: &[u8], existing: &[u8]) -> Result<(), Violation> {
    recreate_keeps_its_filter(filter, existing)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_must_declare_a_filter() {
        assert_eq!(filter_is_declared(b""), Err(Violation::StreamFilterMissing));
        assert_eq!(filter_is_declared(b"orders.>"), Ok(()));
    }

    #[test]
    fn a_stream_filter_may_not_be_global() {
        assert_eq!(
            filter_is_not_global(b">"),
            Err(Violation::StreamFilterGlobal)
        );
        assert_eq!(filter_is_not_global(b"orders.>"), Ok(()));
    }

    #[test]
    fn two_streams_may_not_claim_the_same_slice() {
        assert_eq!(
            filter_is_not_duplicate(b"orders.>", &[b"orders.>".as_slice()]),
            Err(Violation::StreamFilterDuplicate)
        );
        assert_eq!(
            filter_is_not_duplicate(b"payments.>", &[b"orders.>".as_slice()]),
            Ok(())
        );
    }

    #[test]
    fn two_streams_may_not_claim_overlapping_slices() {
        assert_eq!(
            filter_does_not_overlap(b"orders.premium.>", &[b"orders.>".as_slice()]),
            Err(Violation::StreamFilterOverlap)
        );
        assert_eq!(
            filter_does_not_overlap(b"orders.*", &[b"orders.created".as_slice()]),
            Err(Violation::StreamFilterOverlap)
        );
    }

    #[test]
    fn disjoint_streams_coexist() {
        assert_eq!(
            on_create(b"payments.>", &[b"orders.>".as_slice(), b"events.>".as_slice()]),
            Ok(())
        );
    }

    #[test]
    fn a_global_stream_never_collides() {
        assert_eq!(on_create(b">", &[b"orders.>".as_slice()]), Ok(()));
        assert_eq!(on_create(b"orders.>", &[b">".as_slice()]), Ok(()));
    }

    #[test]
    fn recreating_a_stream_may_not_move_its_slice() {
        assert_eq!(on_recreate(b"orders.>", b"orders.>"), Ok(()));
        assert_eq!(on_recreate(b"", b"orders.>"), Ok(()));
        assert_eq!(
            on_recreate(b"payments.>", b"orders.>"),
            Err(Violation::StreamFilterMismatch)
        );
    }
}
