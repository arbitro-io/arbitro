//! WAL wire format — byte-identical to the Go client's `ackstore/format.go`
//! so a log written by any arbitro client is readable by any other.
//!
//! File starts with an 8-byte magic, then length-framed records:
//!
//! ```text
//! [len u32 LE][payload len bytes][crc32c u32 LE over payload]
//! ```
//!
//! Length-framing (not fixed-size records) lets one reader loop handle every
//! op and detect torn writes: if the tail can't supply a full frame, replay
//! truncates there. crc32c (Castagnoli) guards against silent corruption; a
//! bad CRC also truncates. `payload[0]` is the op byte.

/// File magic: "ARBWAL01".
pub(crate) const FILE_MAGIC: [u8; 8] = *b"ARBWAL01";

// op codes (payload[0]) — identical to Go.
pub(crate) const OP_REGISTER: u8 = 1; // (slot_id, ts, stream, consumer)
pub(crate) const OP_RECORD: u8 = 2; // (slot_id, ts, seq)  handler ran
pub(crate) const OP_CONFIRM: u8 = 3; // (slot_id, ts, seq)  broker acked, drop
pub(crate) const OP_EXPIRE: u8 = 4; // (slot_id, ts, seq)  TTL swept, drop
pub(crate) const OP_TOMBSTONE: u8 = 5; // (slot_id, ts)       forget the slot
pub(crate) const OP_SNAPSHOT: u8 = 6; // (slot_id, ts, min, max, count, seqs[])
pub(crate) const OP_CONFIRM_UP_TO: u8 = 7; // (slot_id, ts, cursor) drop all ≤ cursor

pub(crate) const FRAME_LEN_SIZE: usize = 4;
pub(crate) const FRAME_CRC_SIZE: usize = 4;
pub(crate) const FRAME_OVERHEAD: usize = FRAME_LEN_SIZE + FRAME_CRC_SIZE;

pub(crate) const SEQ_PAYLOAD_SIZE: usize = 1 + 4 + 8 + 8; // op+slot+ts+seq
pub(crate) const TOMB_PAYLOAD_SIZE: usize = 1 + 4 + 8; // op+slot+ts
pub(crate) const REG_FIXED_PREFIX: usize = 1 + 4 + 8 + 2 + 2; // op+slot+ts+slen+clen
pub(crate) const SNAP_FIXED_PREFIX: usize = 1 + 4 + 8 + 8 + 8 + 4; // op+slot+ts+min+max+count

// ── crc32c (Castagnoli, poly 0x82F63B78 reflected) ─────────────────────────
// Matches Go's crc32.MakeTable(crc32.Castagnoli). Implemented inline to avoid
// adding a dependency and to guarantee byte-identical output across clients.

const CRC32C_POLY: u32 = 0x82F6_3B78;

struct Crc32cTable([u32; 256]);

static CRC32C: std::sync::LazyLock<Crc32cTable> = std::sync::LazyLock::new(|| {
    let mut t = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ CRC32C_POLY
            } else {
                crc >> 1
            };
            j += 1;
        }
        t[i] = crc;
        i += 1;
    }
    Crc32cTable(t)
});

pub(crate) fn crc32c(data: &[u8]) -> u32 {
    let table = &CRC32C.0;
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = (crc >> 8) ^ table[((crc ^ b as u32) & 0xFF) as usize];
    }
    crc ^ 0xFFFF_FFFF
}

// ── frame sizes ────────────────────────────────────────────────────────────

pub(crate) fn record_frame_size() -> usize {
    FRAME_OVERHEAD + SEQ_PAYLOAD_SIZE
}
pub(crate) fn tombstone_frame_size() -> usize {
    FRAME_OVERHEAD + TOMB_PAYLOAD_SIZE
}
pub(crate) fn register_frame_size(stream_len: usize, consumer_len: usize) -> usize {
    FRAME_OVERHEAD + REG_FIXED_PREFIX + stream_len + consumer_len
}
pub(crate) fn snapshot_frame_size(count: usize) -> usize {
    FRAME_OVERHEAD + SNAP_FIXED_PREFIX + count * 8
}

// ── encoders: write a full framed record into `dst`, return bytes written ───

/// Stamp the length prefix + crc suffix around a payload already written at
/// `dst[FRAME_LEN_SIZE..FRAME_LEN_SIZE+plen]`. Returns total frame size.
fn frame_finish(dst: &mut [u8], plen: usize) -> usize {
    dst[0..4].copy_from_slice(&(plen as u32).to_le_bytes());
    let crc = crc32c(&dst[FRAME_LEN_SIZE..FRAME_LEN_SIZE + plen]);
    dst[FRAME_LEN_SIZE + plen..FRAME_LEN_SIZE + plen + FRAME_CRC_SIZE]
        .copy_from_slice(&crc.to_le_bytes());
    FRAME_LEN_SIZE + plen + FRAME_CRC_SIZE
}

/// Encode a Record/Confirm/Expire/ConfirmUpTo frame (`seq` field carries the
/// cursor for ConfirmUpTo).
pub(crate) fn encode_seq_op(dst: &mut [u8], op: u8, slot_id: u32, ts_ms: u64, seq: u64) -> usize {
    let p = &mut dst[FRAME_LEN_SIZE..FRAME_LEN_SIZE + SEQ_PAYLOAD_SIZE];
    p[0] = op;
    p[1..5].copy_from_slice(&slot_id.to_le_bytes());
    p[5..13].copy_from_slice(&ts_ms.to_le_bytes());
    p[13..21].copy_from_slice(&seq.to_le_bytes());
    frame_finish(dst, SEQ_PAYLOAD_SIZE)
}

pub(crate) fn encode_tombstone(dst: &mut [u8], slot_id: u32, ts_ms: u64) -> usize {
    let p = &mut dst[FRAME_LEN_SIZE..FRAME_LEN_SIZE + TOMB_PAYLOAD_SIZE];
    p[0] = OP_TOMBSTONE;
    p[1..5].copy_from_slice(&slot_id.to_le_bytes());
    p[5..13].copy_from_slice(&ts_ms.to_le_bytes());
    frame_finish(dst, TOMB_PAYLOAD_SIZE)
}

pub(crate) fn encode_register(
    dst: &mut [u8],
    slot_id: u32,
    ts_ms: u64,
    stream: &[u8],
    consumer: &[u8],
) -> usize {
    let plen = REG_FIXED_PREFIX + stream.len() + consumer.len();
    let p = &mut dst[FRAME_LEN_SIZE..FRAME_LEN_SIZE + plen];
    p[0] = OP_REGISTER;
    p[1..5].copy_from_slice(&slot_id.to_le_bytes());
    p[5..13].copy_from_slice(&ts_ms.to_le_bytes());
    p[13..15].copy_from_slice(&(stream.len() as u16).to_le_bytes());
    p[15..17].copy_from_slice(&(consumer.len() as u16).to_le_bytes());
    p[17..17 + stream.len()].copy_from_slice(stream);
    p[17 + stream.len()..17 + stream.len() + consumer.len()].copy_from_slice(consumer);
    frame_finish(dst, plen)
}

pub(crate) fn encode_snapshot(
    dst: &mut [u8],
    slot_id: u32,
    ts_ms: u64,
    min_seq: u64,
    max_seq: u64,
    seqs: &[u64],
) -> usize {
    let plen = SNAP_FIXED_PREFIX + seqs.len() * 8;
    let p = &mut dst[FRAME_LEN_SIZE..FRAME_LEN_SIZE + plen];
    p[0] = OP_SNAPSHOT;
    p[1..5].copy_from_slice(&slot_id.to_le_bytes());
    p[5..13].copy_from_slice(&ts_ms.to_le_bytes());
    p[13..21].copy_from_slice(&min_seq.to_le_bytes());
    p[21..29].copy_from_slice(&max_seq.to_le_bytes());
    p[29..33].copy_from_slice(&(seqs.len() as u32).to_le_bytes());
    let mut off = 33;
    for &s in seqs {
        p[off..off + 8].copy_from_slice(&s.to_le_bytes());
        off += 8;
    }
    frame_finish(dst, plen)
}

// ── decoder (replay) ───────────────────────────────────────────────────────

/// Parsed view of one payload. Only the fields relevant to its op are set.
#[derive(Default)]
pub(crate) struct Decoded {
    pub op: u8,
    pub slot_id: u32,
    pub ts_ms: u64,
    pub seq: u64,        // Record/Confirm/Expire/ConfirmUpTo
    pub stream: Vec<u8>, // Register
    pub consumer: Vec<u8>,
    pub seqs: Vec<u64>, // Snapshot
}

/// Parse a CRC-validated payload. Returns `None` if structurally invalid.
pub(crate) fn decode_payload(payload: &[u8]) -> Option<Decoded> {
    if payload.is_empty() {
        return None;
    }
    let op = payload[0];
    let mut d = Decoded {
        op,
        ..Default::default()
    };
    match op {
        OP_RECORD | OP_CONFIRM | OP_EXPIRE | OP_CONFIRM_UP_TO => {
            if payload.len() != SEQ_PAYLOAD_SIZE {
                return None;
            }
            d.slot_id = u32::from_le_bytes(payload[1..5].try_into().ok()?);
            d.ts_ms = u64::from_le_bytes(payload[5..13].try_into().ok()?);
            d.seq = u64::from_le_bytes(payload[13..21].try_into().ok()?);
        }
        OP_TOMBSTONE => {
            if payload.len() != TOMB_PAYLOAD_SIZE {
                return None;
            }
            d.slot_id = u32::from_le_bytes(payload[1..5].try_into().ok()?);
            d.ts_ms = u64::from_le_bytes(payload[5..13].try_into().ok()?);
        }
        OP_REGISTER => {
            if payload.len() < REG_FIXED_PREFIX {
                return None;
            }
            d.slot_id = u32::from_le_bytes(payload[1..5].try_into().ok()?);
            d.ts_ms = u64::from_le_bytes(payload[5..13].try_into().ok()?);
            let sl = u16::from_le_bytes(payload[13..15].try_into().ok()?) as usize;
            let cl = u16::from_le_bytes(payload[15..17].try_into().ok()?) as usize;
            if 17 + sl + cl != payload.len() {
                return None;
            }
            d.stream = payload[17..17 + sl].to_vec();
            d.consumer = payload[17 + sl..17 + sl + cl].to_vec();
        }
        OP_SNAPSHOT => {
            if payload.len() < SNAP_FIXED_PREFIX {
                return None;
            }
            d.slot_id = u32::from_le_bytes(payload[1..5].try_into().ok()?);
            d.ts_ms = u64::from_le_bytes(payload[5..13].try_into().ok()?);
            let count = u32::from_le_bytes(payload[29..33].try_into().ok()?) as usize;
            if 33 + count * 8 != payload.len() {
                return None;
            }
            let mut seqs = Vec::with_capacity(count);
            let mut off = 33;
            for _ in 0..count {
                seqs.push(u64::from_le_bytes(payload[off..off + 8].try_into().ok()?));
                off += 8;
            }
            d.seqs = seqs;
        }
        _ => return None,
    }
    Some(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_matches_known_vector() {
        // crc32c of "123456789" is 0xE3069283 (standard Castagnoli check value).
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn seq_op_roundtrip() {
        let mut buf = vec![0u8; record_frame_size()];
        let n = encode_seq_op(&mut buf, OP_RECORD, 42, 1000, 777);
        assert_eq!(n, record_frame_size());
        let plen = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let payload = &buf[FRAME_LEN_SIZE..FRAME_LEN_SIZE + plen];
        let d = decode_payload(payload).unwrap();
        assert_eq!(
            (d.op, d.slot_id, d.ts_ms, d.seq),
            (OP_RECORD, 42, 1000, 777)
        );
    }

    #[test]
    fn register_roundtrip() {
        let mut buf = vec![0u8; register_frame_size(6, 6)];
        encode_register(&mut buf, 3, 500, b"orders", b"worker");
        let plen = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let d = decode_payload(&buf[FRAME_LEN_SIZE..FRAME_LEN_SIZE + plen]).unwrap();
        assert_eq!(d.op, OP_REGISTER);
        assert_eq!(d.stream, b"orders");
        assert_eq!(d.consumer, b"worker");
    }
}
