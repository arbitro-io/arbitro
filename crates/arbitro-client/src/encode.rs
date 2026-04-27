//! Wire encoding helpers for outbound frames.
//!
//! Centralises the body layout of every action the client emits so the
//! `client.rs` wrappers stay one-liners and the layout lives in one place.
//!
//! ## Allocation contract
//!
//! - `encode_*` returns an exactly-sized `Vec<u8>` (one alloc, no growth).
//! - `encode_*_into(buf, ...)` appends to a caller-provided buffer so the
//!   caller can pool / reuse allocations across calls.
//! - Sizing helpers (`*_body_len`) are pure arithmetic — call them to
//!   pre-size before `encode_*_into`.

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::IntoBytes;

use arbitro_proto::wire::publish::PublishEntry;

// ─── Publish ──────────────────────────────────────────────────────────────

/// Number of bytes a publish body occupies for `entries`.
///
/// Layout: `[u32 count] + N * ([12 B header] + subject + payload)`.
#[inline]
pub(crate) fn publish_body_len(entries: &[(&[u8], &[u8])]) -> usize {
    4 + entries
        .iter()
        .map(|(s, p)| 12 + s.len() + p.len())
        .sum::<usize>()
}

/// Append a publish body to `buf`. Caller is responsible for sizing
/// (`buf.reserve(publish_body_len(entries))`) when reusing buffers.
#[inline]
pub(crate) fn encode_publish_into(buf: &mut Vec<u8>, entries: &[(&[u8], &[u8])]) {
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (subject, payload) in entries {
        let entry = PublishEntry {
            data_len: U32::new(payload.len() as u32),
            subj_len: U16::new(subject.len() as u16),
            reply_len: U16::new(0),
            flags: 0,
            _pad: [0u8; 3],
        };
        buf.extend_from_slice(entry.as_bytes());
        buf.extend_from_slice(subject);
        buf.extend_from_slice(payload);
    }
}

/// Convenience: build a fresh, exactly-sized publish body in one alloc.
#[inline]
pub(crate) fn encode_publish(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(publish_body_len(entries));
    encode_publish_into(&mut buf, entries);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_entry_round_trips_layout() {
        let entries: &[(&[u8], &[u8])] = &[(b"foo.bar", b"hello world")];
        let body = encode_publish(entries);
        assert_eq!(body.len(), publish_body_len(entries));
        assert_eq!(body.len(), 4 + 12 + 7 + 11);
        // count
        assert_eq!(&body[0..4], &1u32.to_le_bytes());
        // subj_len at offset 4 (after data_len u32)
        assert_eq!(u16::from_le_bytes([body[8], body[9]]), 7);
        // subject
        assert_eq!(&body[16..23], b"foo.bar");
        // payload
        assert_eq!(&body[23..34], b"hello world");
    }

    #[test]
    fn multi_entry_count_matches() {
        let entries: &[(&[u8], &[u8])] = &[
            (b"a", b"1"),
            (b"bb", b"22"),
            (b"ccc", b"333"),
        ];
        let body = encode_publish(entries);
        assert_eq!(body.len(), publish_body_len(entries));
        assert_eq!(&body[0..4], &3u32.to_le_bytes());
    }

    #[test]
    fn into_buf_appends_without_growing_when_presized() {
        let entries: &[(&[u8], &[u8])] = &[(b"x", b"y")];
        let need = publish_body_len(entries);
        let mut buf = Vec::with_capacity(need);
        let cap_before = buf.capacity();
        encode_publish_into(&mut buf, entries);
        assert_eq!(buf.capacity(), cap_before, "no growth on pre-sized buf");
        assert_eq!(buf.len(), need);
    }
}
