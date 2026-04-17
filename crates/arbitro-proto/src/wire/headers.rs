//! Composite Headers for the Arbitro protocol.
//!
//! These structures combine multiple fixed-size headers into a single
//! memory-contiguous block, allowing for O(1) serialization and
//! better cache locality during frame construction.

use zerocopy::byteorder::little_endian::U16;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::wire::envelope::Envelope;
use crate::wire::publish::PublishEntry;
pub use crate::wire::delivery::{RepBatchFixed, RepOkAction, RepErrorAction, DeliveryEntryHeader};

/// Header for a batch of messages (General).
/// Combines the 16B transport envelope and the 2B batch count.
#[derive(IntoBytes, Immutable, KnownLayout, Clone, Copy, FromBytes)]
#[repr(C)]
pub struct BatchHeader {
    pub env: Envelope,
    pub count: U16,
}

/// Header for a single-entry publish frame.
/// Combines Envelope, Count (1), and the first PublishEntry.
#[derive(IntoBytes, Immutable, KnownLayout, Clone, Copy, FromBytes)]
#[repr(C)]
pub struct PublishHeader {
    pub env: Envelope,
    pub count: U16,
    pub entry: PublishEntry,
}

/// Header for a delivery batch.
/// Combines Envelope and the RepBatchFixed header.
#[derive(IntoBytes, Immutable, KnownLayout, Clone, Copy, FromBytes)]
#[repr(C)]
pub struct DeliverBatchHeader {
    pub env: Envelope,
    pub batch: RepBatchFixed,
}

/// Header for a RepOk (Success) frame.
/// Combines Envelope and RepOkAction (16B + 16B = 32B).
#[derive(IntoBytes, Immutable, KnownLayout, Clone, Copy, FromBytes)]
#[repr(C)]
pub struct RepOkHeader {
    pub env: Envelope,
    pub body: RepOkAction,
}

/// Header for a RepError frame.
/// Combines Envelope and RepErrorAction (16B + 16B = 32B).
#[derive(IntoBytes, Immutable, KnownLayout, Clone, Copy, FromBytes)]
#[repr(C)]
pub struct RepErrorHeader {
    pub env: Envelope,
    pub body: RepErrorAction,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::size_of;

    #[test]
    fn validate_sizes() {
        assert_eq!(size_of::<BatchHeader>(), 16 + 2);
        assert_eq!(size_of::<PublishHeader>(), 16 + 2 + 12);
        assert_eq!(size_of::<DeliverBatchHeader>(), 16 + 4);
        assert_eq!(size_of::<RepOkHeader>(), 16 + 16); 
        assert_eq!(size_of::<RepErrorHeader>(), 16 + 16); 
    }
}
