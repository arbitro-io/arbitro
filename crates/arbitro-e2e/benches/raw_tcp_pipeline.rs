//! TEMPORARY bench — RepBatch pipeline con los codecs reales de arbitro-proto.
//!
//! Corre **dos modos** por cada batch size para separar el costo de encoding
//! del costo de red:
//!
//!   - PURE_TCP      : todos los frames pre-encodeados fuera del loop. El
//!                     sender solo hace `write_all`. Mide red + decode.
//!   - FULL_PIPELINE : el sender encodea cada frame dentro del loop. Mide
//!                     encode + red + decode (el camino real del drainer).
//!
//! Layout del frame (idéntico al que ensambla `handle_drain_deliver`):
//!   [Envelope 16 B][RepBatchFixed 8 B][N × (DeliveryEntryHeader 14 B + subj + payload)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Instant;

use arbitro_proto::action::Action;
use arbitro_proto::wire::delivery::{
    DeliveryEntryHeader, RepBatchFixed, DELIVERY_ENTRY_HEADER_SIZE, REP_BATCH_FIXED_SIZE,
};
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use zerocopy::{FromBytes, IntoBytes};
use zerocopy::byteorder::little_endian::{U16, U32, U64};

// ── Knobs ───────────────────────────────────────────────────────────────────
const TOTAL_MSGS: usize = 5_000_000;
const PAYLOAD_SIZE: usize = 64;
const SUBJECT: &[u8] = b"orders.created";

#[derive(Clone, Copy)]
enum Mode {
    PureTcp,       // frames pre-built outside loop
    FullPipeline,  // frames encoded inside sender loop
}

fn entry_size() -> usize {
    DELIVERY_ENTRY_HEADER_SIZE + SUBJECT.len() + PAYLOAD_SIZE
}

fn frame_size(batch: usize) -> usize {
    ENVELOPE_SIZE + REP_BATCH_FIXED_SIZE + batch * entry_size()
}

fn main() {
    println!();
    println!("══════════════════════════════════════════════════════════════════");
    println!("  RepBatch pipeline — arbitro-proto codecs, core 0 pinned");
    println!("  total={} msgs, payload={} B, subject={:?}",
        TOTAL_MSGS, PAYLOAD_SIZE, std::str::from_utf8(SUBJECT).unwrap());
    println!("══════════════════════════════════════════════════════════════════");

    for &batch in &[256usize, 64, 16, 1] {
        run_scenario(batch, Mode::PureTcp);
        run_scenario(batch, Mode::FullPipeline);
        println!();
    }
}

fn run_scenario(batch: usize, mode: Mode) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();

    let entry_sz = entry_size();
    let frame_sz = frame_size(batch);
    let full_frames = TOTAL_MSGS / batch;
    let tail_batch = TOTAL_MSGS % batch;

    // ── Receiver thread ─────────────────────────────────────────────────────
    let recv_handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        stream.set_nodelay(true).ok();

        let mut buf = vec![0u8; 256 * 1024];
        let mut tail: usize = 0;
        let mut decoded_msgs: usize = 0;
        let mut decoded_bytes: u64 = 0;
        let mut decoded_frames: usize = 0;

        let mut sink_seq: u64 = 0;
        let mut sink_sublen: u32 = 0;

        let start = Instant::now();

        while decoded_msgs < TOTAL_MSGS {
            let n = stream.read(&mut buf[tail..]).expect("read");
            if n == 0 { break; }
            tail += n;

            let mut cursor = 0;
            loop {
                if tail - cursor < ENVELOPE_SIZE { break; }

                let env = Envelope::ref_from_bytes(
                    &buf[cursor..cursor + ENVELOPE_SIZE],
                ).unwrap();
                let msg_len = env.msg_len.get() as usize;
                let total = ENVELOPE_SIZE + msg_len;
                if tail - cursor < total { break; }

                let _action = Action::from_u16(env.action.get()).expect("valid action");
                std::hint::black_box(_action);

                let body_start = cursor + ENVELOPE_SIZE;
                let rep = RepBatchFixed::ref_from_bytes(
                    &buf[body_start..body_start + REP_BATCH_FIXED_SIZE],
                ).unwrap();
                let count = rep.count.get() as usize;
                let mut e_off = body_start + REP_BATCH_FIXED_SIZE;
                for _ in 0..count {
                    let hdr = DeliveryEntryHeader::ref_from_bytes(
                        &buf[e_off..e_off + DELIVERY_ENTRY_HEADER_SIZE],
                    ).unwrap();
                    let subj_len = hdr.subj_len.get() as u32;
                    let data_len = hdr.data_len.get() as usize;
                    sink_seq ^= hdr.seq.get();
                    sink_sublen ^= subj_len;
                    let _first = buf[e_off + DELIVERY_ENTRY_HEADER_SIZE];
                    std::hint::black_box(_first);
                    e_off += DELIVERY_ENTRY_HEADER_SIZE + data_len;
                    decoded_msgs += 1;
                    decoded_bytes += (DELIVERY_ENTRY_HEADER_SIZE + data_len) as u64;
                }
                decoded_frames += 1;

                cursor += total;
            }

            if cursor > 0 {
                buf.copy_within(cursor..tail, 0);
                tail -= cursor;
            }
        }

        let elapsed = start.elapsed();
        std::hint::black_box((sink_seq, sink_sublen));
        (elapsed, decoded_msgs, decoded_bytes, decoded_frames)
    });

    // ── Sender thread ───────────────────────────────────────────────────────
    let sender_handle = thread::spawn(move || {
        let mut stream = TcpStream::connect(addr).expect("connect");
        stream.set_nodelay(true).ok();

        match mode {
            Mode::PureTcp => {
                // Pre-encode TODOS los frames fuera del loop.
                let mut frames: Vec<Vec<u8>> = Vec::with_capacity(full_frames + 1);
                let mut seq: u64 = 1;
                for _ in 0..full_frames {
                    frames.push(build_frame(batch, &mut seq));
                }
                if tail_batch > 0 {
                    frames.push(build_frame(tail_batch, &mut seq));
                }

                // Loop hot: solo write_all.
                for f in &frames {
                    stream.write_all(f).expect("write");
                }
            }
            Mode::FullPipeline => {
                // Encode cada frame dentro del loop (el camino real del drainer).
                let mut scratch = Vec::with_capacity(frame_sz);
                let mut seq: u64 = 1;
                for _ in 0..full_frames {
                    encode_frame_into(&mut scratch, batch, &mut seq);
                    stream.write_all(&scratch).expect("write");
                }
                if tail_batch > 0 {
                    encode_frame_into(&mut scratch, tail_batch, &mut seq);
                    stream.write_all(&scratch).expect("write tail");
                }
            }
        }

        stream.flush().ok();
    });

    sender_handle.join().expect("sender join");
    let (elapsed, decoded_msgs, decoded_bytes, decoded_frames) =
        recv_handle.join().expect("recv join");

    let elapsed_s = elapsed.as_secs_f64();
    let msg_per_s = decoded_msgs as f64 / elapsed_s;
    let gbps = (decoded_bytes as f64 / elapsed_s) / (1024.0 * 1024.0 * 1024.0);
    let ns_per_msg = elapsed.as_nanos() as f64 / decoded_msgs as f64;

    let mode_str = match mode {
        Mode::PureTcp => "PURE_TCP     ",
        Mode::FullPipeline => "FULL_PIPELINE",
    };

    println!(
        "  batch={:>4} {}│ frame={:>6}B │ {:>6} frames │ {:>7.2} M msg/s │ {:>6.2} GB/s │ {:>7.2} ns/msg │ {:?}",
        batch, mode_str, frame_sz, decoded_frames, msg_per_s / 1_000_000.0, gbps, ns_per_msg, elapsed,
    );
}

/// Build a full frame (envelope + rep_fixed + N entries) into a new Vec.
fn build_frame(batch: usize, seq: &mut u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(frame_size(batch));
    encode_frame_into(&mut v, batch, seq);
    v
}

/// Encode a full frame into `scratch` (cleared first). Advances `*seq`.
fn encode_frame_into(scratch: &mut Vec<u8>, batch: usize, seq: &mut u64) {
    scratch.clear();

    let body_len = REP_BATCH_FIXED_SIZE + batch * entry_size();

    let env = Envelope::new(
        Action::RepBatch,
        /* stream_id */ 0xDEADBEEF,
        body_len as u32,
        /* env_seq */ 0,
    );
    scratch.extend_from_slice(env.as_bytes());

    let rep = RepBatchFixed {
        consumer_id: U32::new(42),
        count: U16::new(batch as u16),
        _pad: U16::new(0),
    };
    scratch.extend_from_slice(rep.as_bytes());

    let payload: [u8; PAYLOAD_SIZE] = core::array::from_fn(|i| i as u8);
    let data_len = SUBJECT.len() + PAYLOAD_SIZE;

    for _ in 0..batch {
        let hdr = DeliveryEntryHeader {
            seq: U64::new(*seq),
            subj_len: U16::new(SUBJECT.len() as u16),
            data_len: U32::new(data_len as u32),
        };
        scratch.extend_from_slice(hdr.as_bytes());
        scratch.extend_from_slice(SUBJECT);
        scratch.extend_from_slice(&payload);
        *seq += 1;
    }
}
