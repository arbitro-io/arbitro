//! User-facing message headers — zero-copy TLV key-value pairs.
//!
//! Wire layout (stored as the "payload" field in `EntryRef` when
//! `flags::HAS_HEADERS` is set):
//!
//! ```text
//! [payload_len : u32 LE]          ← user payload size
//! [user_payload : payload_len B]  ← untouched user data
//! [headers_len : u32 LE]          ← total headers section size
//! [count       : u16 LE]          ← number of header entries
//! [entries...  : HeaderEntry × N]
//! ```
//!
//! Each `HeaderEntry`:
//! ```text
//! [key_len : u8]
//! [val_len : u16 LE]
//! [data    : key_len + val_len B]  ← key ++ value contiguous
//! ```
//!
//! ## Zero-copy contract
//!
//! All decode functions return `&[u8]` slices into the original buffer.
//! No allocation, no parsing, no copying. Encode writes directly into
//! a caller-provided buffer via `mut_from_bytes`.
//!
//! ## `msg-id` convention
//!
//! The idempotency `msg_id` is stored as a header with key `b"msg-id"`.
//! This replaces the separate `msg_id_len` field in the PubFrame —
//! the broker extracts it from headers during dispatch and feeds it
//! to the `IdempotencyTracker`.

use zerocopy::byteorder::little_endian::{U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

// ── Well-known header keys ────────────────────────────────────────────────

/// Idempotency token. Replaces the per-frame `msg_id` field.
pub const HDR_MSG_ID: &[u8] = b"msg-id";

// ── Zerocopy DST structs ──────────────────────────────────────────────────

/// Outer wrapper: `[payload_len:4][data...]`.
/// `data` contains `[user_payload][HeadersBlock]`.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct ExtendedPayload {
    pub payload_len: U32,
    pub data: [u8],
}

impl ExtendedPayload {
    /// User payload — the first `payload_len` bytes of `data`.
    ///
    /// Returns an empty slice if `payload_len` exceeds the available
    /// data (malformed wire input).
    #[inline]
    pub fn payload(&self) -> &[u8] {
        let len = self.payload_len.get() as usize;
        if len > self.data.len() {
            return &[];
        }
        &self.data[..len]
    }

    /// The `HeadersBlock` that follows the user payload.
    #[inline]
    pub fn headers_block(&self) -> Option<&HeadersBlock> {
        let off = self.payload_len.get() as usize;
        if off > self.data.len() {
            return None;
        }
        HeadersBlock::ref_from_bytes(&self.data[off..]).ok()
    }

    /// Total wire size for a given payload + headers.
    #[inline]
    pub const fn wire_size(payload_len: usize, headers_section_len: usize) -> usize {
        4 + payload_len + headers_section_len
    }
}

/// Headers block: `[headers_len:4][count:2][entries...]`.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct HeadersBlock {
    pub headers_len: U32,
    pub count: U16,
    pub data: [u8],
}

impl HeadersBlock {
    /// Iterate over entries. Returns an iterator of `&HeaderEntry` slices.
    #[inline]
    pub fn iter(&self) -> HeaderIter<'_> {
        HeaderIter {
            data: &self.data,
            remaining: self.count.get(),
            offset: 0,
        }
    }

    /// Lookup a single key. Linear scan, zero-copy.
    #[inline]
    pub fn get(&self, needle: &[u8]) -> Option<&[u8]> {
        for entry in self.iter() {
            if entry.key() == needle {
                return Some(entry.val());
            }
        }
        None
    }

    /// Headers section size for a given set of entries.
    #[inline]
    pub fn section_size(entries: &[(&[u8], &[u8])]) -> usize {
        let entries_len: usize = entries.iter()
            .map(|(k, v)| 3 + k.len() + v.len())
            .sum();
        6 + entries_len // headers_len(4) + count(2) + entries
    }
}

/// A single header entry: `[key_len:1][val_len:2][key ++ val]`.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct HeaderEntry {
    pub key_len: u8,
    pub val_len: U16,
    pub data: [u8],
}

impl HeaderEntry {
    #[inline]
    pub fn key(&self) -> &[u8] {
        let k = self.key_len as usize;
        if k > self.data.len() {
            return &[];
        }
        &self.data[..k]
    }

    #[inline]
    pub fn val(&self) -> &[u8] {
        let k = self.key_len as usize;
        let v = self.val_len.get() as usize;
        let end = k + v;
        if end > self.data.len() {
            return &[];
        }
        &self.data[k..end]
    }

    /// Total wire size of this entry (3-byte prefix + key + val).
    #[inline]
    pub fn wire_size(&self) -> usize {
        3 + self.key_len as usize + self.val_len.get() as usize
    }
}

// ── Iterator ──────────────────────────────────────────────────────────────

/// Zero-copy iterator over header entries.
pub struct HeaderIter<'a> {
    data: &'a [u8],
    remaining: u16,
    offset: usize,
}

impl<'a> Iterator for HeaderIter<'a> {
    type Item = &'a HeaderEntry;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let entry = HeaderEntry::ref_from_bytes(&self.data[self.offset..]).ok()?;
        self.offset += entry.wire_size();
        self.remaining -= 1;
        Some(entry)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining as usize, Some(self.remaining as usize))
    }
}

// ── Encode ────────────────────────────────────────────────────────────────

/// Encode payload + headers into a pre-allocated buffer.
///
/// Buffer size must equal `ExtendedPayload::wire_size(payload.len(), headers_section)`.
/// where `headers_section = HeadersBlock::section_size(entries)`.
///
/// Returns the filled buffer as an `&ExtendedPayload` for verification.
pub fn encode_extended_payload<'a>(
    buf: &'a mut [u8],
    payload: &[u8],
    entries: &[(&[u8], &[u8])],
) -> &'a ExtendedPayload {
    let ext = ExtendedPayload::mut_from_bytes(buf).expect("buffer size mismatch");
    ext.payload_len = U32::new(payload.len() as u32);
    ext.data[..payload.len()].copy_from_slice(payload);

    let h_off = payload.len();
    let entries_len: usize = entries.iter()
        .map(|(k, v)| 3 + k.len() + v.len())
        .sum();

    let hdr = HeadersBlock::mut_from_bytes(&mut ext.data[h_off..]).expect("headers block size");
    hdr.headers_len = U32::new((2 + entries_len) as u32);
    hdr.count = U16::new(entries.len() as u16);

    let mut o = 0;
    for (k, v) in entries {
        let total = 3 + k.len() + v.len();
        let entry = HeaderEntry::mut_from_bytes(&mut hdr.data[o..o + total])
            .expect("entry size");
        entry.key_len = k.len() as u8;
        entry.val_len = U16::new(v.len() as u16);
        entry.data[..k.len()].copy_from_slice(k);
        entry.data[k.len()..k.len() + v.len()].copy_from_slice(v);
        o += total;
    }

    ExtendedPayload::ref_from_bytes(buf).expect("roundtrip")
}

/// Convenience: allocate + encode. Returns owned `Vec<u8>`.
pub fn encode_extended_payload_vec(
    payload: &[u8],
    entries: &[(&[u8], &[u8])],
) -> Vec<u8> {
    let section = HeadersBlock::section_size(entries);
    let total = ExtendedPayload::wire_size(payload.len(), section);
    let mut buf = vec![0u8; total];
    encode_extended_payload(&mut buf, payload, entries);
    buf
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_payload_and_headers() {
        let payload = b"hello world";
        let entries: &[(&[u8], &[u8])] = &[
            (b"wf-id", b"order-process"),
            (b"wf-step", &[2]),
            (b"msg-id", b"abc-123"),
        ];
        let buf = encode_extended_payload_vec(payload, entries);

        let ext = ExtendedPayload::ref_from_bytes(&buf).unwrap();
        assert_eq!(ext.payload(), b"hello world");

        let hdr = ext.headers_block().unwrap();
        assert_eq!(hdr.count.get(), 3);
        assert_eq!(hdr.get(b"wf-id").unwrap(), b"order-process");
        assert_eq!(hdr.get(b"wf-step").unwrap(), &[2]);
        assert_eq!(hdr.get(b"msg-id").unwrap(), b"abc-123");
        assert_eq!(hdr.get(b"missing"), None);
    }

    #[test]
    fn empty_headers() {
        let payload = b"data";
        let entries: &[(&[u8], &[u8])] = &[];
        let buf = encode_extended_payload_vec(payload, entries);

        let ext = ExtendedPayload::ref_from_bytes(&buf).unwrap();
        assert_eq!(ext.payload(), b"data");

        let hdr = ext.headers_block().unwrap();
        assert_eq!(hdr.count.get(), 0);
        assert_eq!(hdr.get(b"anything"), None);
    }

    #[test]
    fn iterator_visits_all_entries() {
        let entries: &[(&[u8], &[u8])] = &[
            (b"a", b"1"),
            (b"bb", b"22"),
            (b"ccc", b"333"),
        ];
        let buf = encode_extended_payload_vec(b"", entries);
        let ext = ExtendedPayload::ref_from_bytes(&buf).unwrap();
        let hdr = ext.headers_block().unwrap();

        let collected: Vec<_> = hdr.iter().map(|e| (e.key(), e.val())).collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0], (&b"a"[..], &b"1"[..]));
        assert_eq!(collected[1], (&b"bb"[..], &b"22"[..]));
        assert_eq!(collected[2], (&b"ccc"[..], &b"333"[..]));
    }

    #[test]
    fn encode_into_preallocated_buffer() {
        let payload = b"test";
        let entries: &[(&[u8], &[u8])] = &[(b"k", b"v")];
        let section = HeadersBlock::section_size(entries);
        let total = ExtendedPayload::wire_size(payload.len(), section);
        let mut buf = vec![0u8; total];

        let ext = encode_extended_payload(&mut buf, payload, entries);
        assert_eq!(ext.payload(), b"test");
        assert_eq!(ext.headers_block().unwrap().get(b"k").unwrap(), b"v");
    }

    #[test]
    fn msg_id_key_convention() {
        let entries: &[(&[u8], &[u8])] = &[
            (HDR_MSG_ID, b"wf:123:0:0"),
            (b"wf-step", &[0]),
        ];
        let buf = encode_extended_payload_vec(b"payload", entries);
        let ext = ExtendedPayload::ref_from_bytes(&buf).unwrap();
        let hdr = ext.headers_block().unwrap();
        assert_eq!(hdr.get(HDR_MSG_ID).unwrap(), b"wf:123:0:0");
    }
}
