//! v2 zerocopy round-trip — the contract `arbitro` relies on for the
//! publish hot path:
//!
//!   1. Build a struct on the stack (or as a DST inside a Vec).
//!   2. Get its bytes view via `IntoBytes::as_bytes()` — no copy.
//!   3. Hand those bytes to a buffer / TCP / channel.
//!   4. On the other side, reinterpret with `FromBytes::ref_from_bytes()`
//!      — no parse, no copy, just a typed view over the same bytes.
//!   5. Calling `as_bytes()` on the reinterpreted view yields the
//!      ORIGINAL bytes back, byte-for-byte, from offset 0 to end.
//!
//! Property under test:
//!     for any struct S that derives IntoBytes + FromBytes:
//!         let bytes = S::as_bytes(&original);
//!         buf.write_all(bytes)?;
//!         let parsed = S::ref_from_bytes(&buf)?;
//!         assert_eq!(parsed.as_bytes(), bytes);   // identity
//!         assert_eq!(parsed.as_bytes(), &buf[..]);// no transformation

use std::io::Write;

use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use arbitro_proto::v2::header::{Header, HEADER_SIZE};
use arbitro_proto::v2::ingress::pub_frame::{PubBody, PubFrame, PUB_BODY_FIXED};

// ─────────────────────────────────────────────────────────────────────
// Test 1 — sized struct (stack), the user's pattern from the bench.
// ─────────────────────────────────────────────────────────────────────

#[derive(
    FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq,
)]
#[repr(C)]
struct MyNestedPod {
    a: U32,
    b: U32,
    _pad: U32,
}

#[derive(
    FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq,
)]
#[repr(C)]
struct MyPod {
    nested: MyNestedPod,
    c: U64,
}

#[test]
fn sized_struct_slice_roundtrip_via_as_bytes() {
    let originals: &[MyPod] = &[
        MyPod {
            nested: MyNestedPod {
                a: U32::new(1),
                b: U32::new(1),
                _pad: U32::new(0),
            },
            c: U64::new(1),
        },
        MyPod {
            nested: MyNestedPod {
                a: U32::new(2),
                b: U32::new(2),
                _pad: U32::new(0),
            },
            c: U64::new(2),
        },
    ];

    let bytes_view: &[u8] = originals.as_bytes();
    assert_eq!(bytes_view.len(), 2 * core::mem::size_of::<MyPod>());

    let mut buf: Vec<u8> = Vec::new();
    buf.write_all(bytes_view).unwrap();
    assert_eq!(buf.len(), bytes_view.len());

    let parsed: &[MyPod] = <[MyPod]>::ref_from_bytes(&buf[..]).expect("layout valid");
    assert_eq!(parsed, originals, "values survived round-trip");

    let reemitted: &[u8] = parsed.as_bytes();
    assert_eq!(reemitted.len(), buf.len(), "length preserved");
    assert_eq!(reemitted, &buf[..], "byte-for-byte identity");
    assert_eq!(reemitted.as_ptr(), buf.as_ptr());
}

// ─────────────────────────────────────────────────────────────────────
// Test 2 — v2 Header (16B), the universal frame prefix.
// ─────────────────────────────────────────────────────────────────────

#[test]
fn v2_header_roundtrip_via_as_bytes() {
    let original = Header::new(0x0101, 1024, 42)
        .with_flags(0b0000_0011)
        .with_entry_flags(0b0000_0010);

    let mut buf: Vec<u8> = Vec::new();
    buf.write_all(original.as_bytes()).unwrap();
    assert_eq!(buf.len(), HEADER_SIZE);

    let parsed: &Header = Header::ref_from_bytes(&buf[..]).expect("16B header");
    assert_eq!(parsed.action.get(), 0x0101);
    assert_eq!(parsed.msg_len.get(), 1024);
    assert_eq!(parsed.seq.get(), 42);
    assert_eq!(parsed.flags, 0b0000_0011);
    assert_eq!(parsed.entry_flags, 0b0000_0010);

    let reemitted = parsed.as_bytes();
    assert_eq!(reemitted, original.as_bytes());
    assert_eq!(reemitted, &buf[..]);
    assert_eq!(reemitted.as_ptr(), buf.as_ptr());
}

// ─────────────────────────────────────────────────────────────────────
// Test 3 — v2 PubFrame (DST: Header + PubBody + tail[u8]).
// ─────────────────────────────────────────────────────────────────────

#[test]
fn v2_pubframe_roundtrip_via_as_bytes() {
    let subject: &[u8] = b"orders.eu.42";
    let payload: &[u8] = &[0xAB; 256];

    let size = PubFrame::wire_size(subject.len(), 0, payload.len());
    let mut buf = vec![0u8; size];

    PubFrame::encode_into(
        &mut buf,
        /*seq*/ 777,
        /*stream_id*/ 0xCAFEBABE,
        /*flags*/ 0,
        /*entry_flags*/ 0,
        subject,
        &[],
        payload,
    );

    let wire_snapshot = buf.clone();

    let frame: &PubFrame = PubFrame::ref_from_bytes(&buf[..]).expect("layout valid");

    assert_eq!(frame.header.seq.get(), 777);
    assert_eq!(frame.body.stream_id.get(), 0xCAFEBABE);
    assert_eq!(frame.subject(), subject);
    assert_eq!(frame.payload(), payload);

    let buf_start = buf.as_ptr();
    let buf_end = unsafe { buf_start.add(buf.len()) };
    let subj_ptr = frame.subject().as_ptr();
    let pay_ptr = frame.payload().as_ptr();
    assert!(
        subj_ptr >= buf_start && subj_ptr < buf_end,
        "subject inside buf"
    );
    assert!(
        pay_ptr >= buf_start && pay_ptr < buf_end,
        "payload inside buf"
    );

    let reemitted: &[u8] = frame.as_bytes();
    assert_eq!(reemitted.len(), size, "full wire size");
    assert_eq!(
        reemitted,
        &wire_snapshot[..],
        "identity from offset 0 to end"
    );
    assert_eq!(reemitted.as_ptr(), buf_start, "view points into the buffer");
}

// ─────────────────────────────────────────────────────────────────────
// Test 4 — manual prefix construction matches encode_into output.
// ─────────────────────────────────────────────────────────────────────

#[test]
fn pubframe_decode_then_reemit_is_identity() {
    let subject: &[u8] = b"a.b.c";
    let payload: &[u8] = b"hello world";

    let header = Header::new(
        0x0101,
        (PUB_BODY_FIXED + subject.len() + payload.len()) as u32,
        1,
    );
    let body = PubBody {
        stream_id: U32::new(0),
        subject_len: U16::new(subject.len() as u16),
        msg_id_len: U16::new(0),
    };

    let mut wire: Vec<u8> = Vec::new();
    wire.extend_from_slice(header.as_bytes()); // 16B
    wire.extend_from_slice(body.as_bytes()); //  8B
    wire.extend_from_slice(subject);
    wire.extend_from_slice(payload);

    let frame: &PubFrame = PubFrame::ref_from_bytes(&wire[..]).expect("layout");

    assert_eq!(frame.header.action.get(), 0x0101);
    assert_eq!(frame.header.seq.get(), 1);
    assert_eq!(frame.subject(), subject);
    assert_eq!(frame.payload(), payload);

    let forwarded: &[u8] = frame.as_bytes();
    assert_eq!(forwarded, &wire[..], "decode → as_bytes is identity");
    assert_eq!(forwarded.as_ptr(), wire.as_ptr());
    assert_eq!(forwarded.len(), wire.len());
}

// ─────────────────────────────────────────────────────────────────────
// Test 5 — ONE struct, ONE as_bytes(), ZERO copies.
// ─────────────────────────────────────────────────────────────────────

const SUBJ_LEN: usize = 12;
const PAY_LEN: usize = 16;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy)]
#[repr(C)]
struct WholePubFrame {
    header: Header,
    body: PubBody,
    subject: [u8; SUBJ_LEN],
    payload: [u8; PAY_LEN],
}

const _: () = assert!(
    core::mem::size_of::<WholePubFrame>() == HEADER_SIZE + PUB_BODY_FIXED + SUBJ_LEN + PAY_LEN
);

#[test]
fn whole_frame_one_struct_one_as_bytes_zero_copies() {
    let frame = WholePubFrame {
        header: Header::new(0x0101, (PUB_BODY_FIXED + SUBJ_LEN + PAY_LEN) as u32, 777),
        body: PubBody {
            stream_id: U32::new(0xCAFEBABE),
            subject_len: U16::new(SUBJ_LEN as u16),
            msg_id_len: U16::new(0),
        },
        subject: *b"orders.eu.42",
        payload: *b"hello-world-1234",
    };

    let wire: &[u8] = frame.as_bytes();
    assert_eq!(
        wire.len(),
        HEADER_SIZE + PUB_BODY_FIXED + SUBJ_LEN + PAY_LEN
    );
    assert_eq!(wire.as_ptr(), &frame as *const _ as *const u8);

    let parsed: &PubFrame = PubFrame::ref_from_bytes(wire).expect("layout valid");

    assert_eq!(parsed.header.action.get(), 0x0101);
    assert_eq!(parsed.header.seq.get(), 777);
    assert_eq!(parsed.body.stream_id.get(), 0xCAFEBABE);
    assert_eq!(parsed.subject(), b"orders.eu.42");
    assert_eq!(parsed.payload(), b"hello-world-1234");

    let reemitted: &[u8] = parsed.as_bytes();
    assert_eq!(reemitted, wire);
    assert_eq!(reemitted.as_ptr(), wire.as_ptr());
    assert_eq!(reemitted.len(), wire.len());

    let frame_start = &frame as *const _ as *const u8;
    let frame_end = unsafe { frame_start.add(core::mem::size_of::<WholePubFrame>()) };
    let subj_ptr = parsed.subject().as_ptr();
    let pay_ptr = parsed.payload().as_ptr();
    assert!(subj_ptr >= frame_start && subj_ptr < frame_end);
    assert!(pay_ptr >= frame_start && pay_ptr < frame_end);
}

// ─────────────────────────────────────────────────────────────────────
// Test 6 — BATCH: N homogeneous frames as an array.
// ─────────────────────────────────────────────────────────────────────

#[test]
fn batch_homogeneous_array_one_as_bytes_zero_copies() {
    const N: usize = 4;

    let batch: [WholePubFrame; N] = [
        WholePubFrame {
            header: Header::new(0x0101, (PUB_BODY_FIXED + SUBJ_LEN + PAY_LEN) as u32, 100),
            body: PubBody {
                stream_id: U32::new(1),
                subject_len: U16::new(SUBJ_LEN as u16),
                msg_id_len: U16::new(0),
            },
            subject: *b"orders.eu.01",
            payload: *b"AAAAAAAAAAAAAAAA",
        },
        WholePubFrame {
            header: Header::new(0x0101, (PUB_BODY_FIXED + SUBJ_LEN + PAY_LEN) as u32, 101),
            body: PubBody {
                stream_id: U32::new(1),
                subject_len: U16::new(SUBJ_LEN as u16),
                msg_id_len: U16::new(0),
            },
            subject: *b"orders.eu.02",
            payload: *b"BBBBBBBBBBBBBBBB",
        },
        WholePubFrame {
            header: Header::new(0x0101, (PUB_BODY_FIXED + SUBJ_LEN + PAY_LEN) as u32, 102),
            body: PubBody {
                stream_id: U32::new(1),
                subject_len: U16::new(SUBJ_LEN as u16),
                msg_id_len: U16::new(0),
            },
            subject: *b"orders.eu.03",
            payload: *b"CCCCCCCCCCCCCCCC",
        },
        WholePubFrame {
            header: Header::new(0x0101, (PUB_BODY_FIXED + SUBJ_LEN + PAY_LEN) as u32, 103),
            body: PubBody {
                stream_id: U32::new(1),
                subject_len: U16::new(SUBJ_LEN as u16),
                msg_id_len: U16::new(0),
            },
            subject: *b"orders.eu.04",
            payload: *b"DDDDDDDDDDDDDDDD",
        },
    ];

    let wire: &[u8] = batch.as_bytes();
    assert_eq!(wire.len(), N * core::mem::size_of::<WholePubFrame>());
    assert_eq!(wire.as_ptr(), batch.as_ptr() as *const u8);

    let mut offset = 0usize;
    let mut seen_seqs = Vec::with_capacity(N);
    let mut seen_payloads: Vec<Vec<u8>> = Vec::with_capacity(N);

    while offset < wire.len() {
        let header =
            Header::ref_from_bytes(&wire[offset..offset + HEADER_SIZE]).expect("header layout");
        let frame_len = HEADER_SIZE + header.msg_len.get() as usize;

        let frame: &PubFrame =
            PubFrame::ref_from_bytes(&wire[offset..offset + frame_len]).expect("layout");

        seen_seqs.push(frame.header.seq.get());
        seen_payloads.push(frame.payload().to_vec());

        let wire_start = wire.as_ptr();
        let wire_end = unsafe { wire_start.add(wire.len()) };
        let subj_ptr = frame.subject().as_ptr();
        let pay_ptr = frame.payload().as_ptr();
        assert!(subj_ptr >= wire_start && subj_ptr < wire_end);
        assert!(pay_ptr >= wire_start && pay_ptr < wire_end);

        offset += frame_len;
    }

    assert_eq!(offset, wire.len(), "consumed every byte exactly");
    assert_eq!(seen_seqs, vec![100, 101, 102, 103]);
    assert_eq!(
        seen_payloads,
        vec![
            b"AAAAAAAAAAAAAAAA".to_vec(),
            b"BBBBBBBBBBBBBBBB".to_vec(),
            b"CCCCCCCCCCCCCCCC".to_vec(),
            b"DDDDDDDDDDDDDDDD".to_vec(),
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────
// Test 7 — BATCH heterogeneous: different subject/payload sizes.
// ─────────────────────────────────────────────────────────────────────

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy)]
#[repr(C)]
struct EntrySmall {
    header: Header,
    body: PubBody,
    subject: [u8; 5],
    payload: [u8; 4],
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy)]
#[repr(C)]
struct EntryBig {
    header: Header,
    body: PubBody,
    subject: [u8; 16],
    payload: [u8; 64],
}

#[test]
fn batch_heterogeneous_per_entry_struct_zero_copies() {
    let small = EntrySmall {
        header: Header::new(0x0101, (PUB_BODY_FIXED + 5 + 4) as u32, 1),
        body: PubBody {
            stream_id: U32::new(7),
            subject_len: U16::new(5),
            msg_id_len: U16::new(0),
        },
        subject: *b"a.b.c",
        payload: *b"PING",
    };

    let big = EntryBig {
        header: Header::new(0x0101, (PUB_BODY_FIXED + 16 + 64) as u32, 2),
        body: PubBody {
            stream_id: U32::new(7),
            subject_len: U16::new(16),
            msg_id_len: U16::new(0),
        },
        subject: *b"orders.eu.42.xx.",
        payload: [0x42; 64],
    };

    let mut wire: Vec<u8> = Vec::new();
    wire.extend_from_slice(small.as_bytes());
    wire.extend_from_slice(big.as_bytes());

    let mut offset = 0usize;
    let mut frames: Vec<(u64, Vec<u8>)> = Vec::new();

    while offset < wire.len() {
        let header = Header::ref_from_bytes(&wire[offset..offset + HEADER_SIZE]).unwrap();
        let frame_len = HEADER_SIZE + header.msg_len.get() as usize;
        let frame: &PubFrame =
            PubFrame::ref_from_bytes(&wire[offset..offset + frame_len]).expect("layout");

        frames.push((frame.header.seq.get(), frame.payload().to_vec()));
        offset += frame_len;
    }

    assert_eq!(offset, wire.len());
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].0, 1);
    assert_eq!(frames[0].1, b"PING".to_vec());
    assert_eq!(frames[1].0, 2);
    assert_eq!(frames[1].1, vec![0x42; 64]);
}
