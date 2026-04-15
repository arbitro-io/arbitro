//! Zero-copy wire cast helpers for internal transport.
//!
//! Level 1 — depends on `types`, `batch`, `fanout`, `reply` only.
//!
//! All transport types use `#[repr(C)]` + zerocopy derives.
//! These helpers cast `&[u8]` ↔ `&[T]` without any allocation or copying.
//!
//! Encode: `slice.as_bytes()` — provided by `zerocopy::IntoBytes`
//! Decode: `wire::decode_slice::<T>(bytes)` — validates alignment + length
//!
//! # Example
//! ```ignore
//! // Encode: &[FanoutEntry] → &[u8]
//! let bytes = drain.as_bytes();
//!
//! // Decode: &[u8] → &[FanoutEntry]
//! let entries = wire::decode_slice::<FanoutEntry>(bytes).unwrap();
//! ```

use zerocopy::{FromBytes, Immutable, KnownLayout};

/// Decode a byte slice into a typed slice. Zero-copy.
///
/// Returns `None` if the byte slice length is not a multiple of `size_of::<T>()`
/// or alignment requirements are not met.
#[inline]
pub fn decode_slice<T: FromBytes + Immutable + KnownLayout>(bytes: &[u8]) -> Option<&[T]> {
    <[T]>::ref_from_bytes(bytes).ok()
}

/// Decode a byte slice into a single value reference. Zero-copy.
///
/// Returns `None` if the byte slice is not exactly `size_of::<T>()` bytes
/// or alignment requirements are not met.
#[inline]
pub fn decode_ref<T: FromBytes + Immutable + KnownLayout>(bytes: &[u8]) -> Option<&T> {
    T::ref_from_bytes(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{ClaimedEntry, AckEntry};
    use crate::fanout::FanoutEntry;
    use crate::reply::RepPublish;
    use crate::types::*;
    use zerocopy::IntoBytes;

    #[test]
    fn fanout_entry_roundtrip() {
        let entries = [
            FanoutEntry::new(ConnectionId(1), 0xBEEF, 100),
            FanoutEntry::new(ConnectionId(2), 0xDEAD, 200),
        ];
        let bytes = entries.as_bytes();
        let decoded = decode_slice::<FanoutEntry>(bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].connection_id, ConnectionId(1));
        assert_eq!(decoded[0].subject_hash, 0xBEEF);
        assert_eq!(decoded[0].seq, 100);
        assert_eq!(decoded[1].connection_id, ConnectionId(2));
        assert_eq!(decoded[1].seq, 200);
    }

    #[test]
    fn claimed_entry_roundtrip() {
        let entries = [ClaimedEntry {
            seq: 42,
            pending_id: PendingId(7),
            subject_hash: 0xCAFE,
        }];
        let bytes = entries.as_bytes();
        assert_eq!(bytes.len(), 16); // 8 + 4 + 4
        let decoded = decode_slice::<ClaimedEntry>(bytes).unwrap();
        assert_eq!(decoded[0].seq, 42);
        assert_eq!(decoded[0].pending_id, PendingId(7));
        assert_eq!(decoded[0].subject_hash, 0xCAFE);
    }

    #[test]
    fn ack_entry_roundtrip() {
        let entries = [AckEntry { seq: 999 }];
        let bytes = entries.as_bytes();
        assert_eq!(bytes.len(), 8);
        let decoded = decode_slice::<AckEntry>(bytes).unwrap();
        assert_eq!(decoded[0].seq, 999);
    }

    #[test]
    fn rep_publish_roundtrip() {
        let rep = RepPublish {
            source_entries: 100,
            duplicates_skipped: 5,
            notified: 80,
            queued: 15,
        };
        let bytes = rep.as_bytes();
        assert_eq!(bytes.len(), 16);
        let decoded = decode_ref::<RepPublish>(bytes).unwrap();
        assert_eq!(decoded.source_entries, 100);
        assert_eq!(decoded.duplicates_skipped, 5);
        assert_eq!(decoded.notified, 80);
        assert_eq!(decoded.queued, 15);
    }

    #[test]
    fn bad_length_returns_none() {
        let bytes = [0u8; 7]; // not a multiple of 8 (AckEntry)
        assert!(decode_slice::<AckEntry>(&bytes).is_none());
    }

    #[test]
    fn empty_slice_roundtrip() {
        let empty: &[FanoutEntry] = &[];
        let bytes = empty.as_bytes();
        assert_eq!(bytes.len(), 0);
        let decoded = decode_slice::<FanoutEntry>(bytes).unwrap();
        assert_eq!(decoded.len(), 0);
    }
}
