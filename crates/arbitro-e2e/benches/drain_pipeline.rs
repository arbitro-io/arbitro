//! Drain pipeline — measures the full path:
//!   1. Entry<'_>   — current arbitro API (`store.for_each`)
//!   2. RawEntry    — raw bytes API (`store.for_each_raw` + inline getters)
//!   3. +TTL        — adds max_age validation per entry
//!   4. Full pipe   — for_each_raw → encode → TCP send → recv → decode

use std::hint::black_box;
use std::io::{IoSlice, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Instant;

use arbitro_store::{EntryRef, MemoryStore, Store};

// ── Config ──────────────────────────────────────────────────────────────────

const N_MESSAGES: usize = 500_000;
const SUBJECT: &[u8] = b"orders.updates.item";   // 19 bytes
const PAYLOAD_SIZE: usize = 64;
const WARMUP_RUNS: usize = 2;
const MEASURE_RUNS: usize = 5;
const STREAM_ID: u32 = 1;
const BATCH_SIZE: usize = 256;

/// TTL in milliseconds (0 = disabled).
const MAX_AGE_MS: u64 = 60_000;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn build_store() -> MemoryStore {
    let ts = now_ms();
    let mut store = MemoryStore::new();
    let payload = vec![0xAB; PAYLOAD_SIZE];
    for _ in 0..N_MESSAGES {
        let entry = EntryRef {
            stream_id: STREAM_ID,
            subject: SUBJECT,
            payload: &payload,
            flags: 0,
        };
        store.append(entry, ts).unwrap();
    }
    store
}

// ── Stage 1: Entry<'_> — arbitro's current API ──────────────────────────────

fn measure_entry(store: &MemoryStore, batch_size: u64) -> (u128, u64) {
    let mut visits: u64 = 0;
    let total = N_MESSAGES as u64;

    let start = Instant::now();
    let mut cursor: u64 = 1;
    while cursor <= total {
        let end = (cursor + batch_size).min(total + 1);
        store
            .for_each(cursor, end, &mut |entry| {
                visits += 1;
                black_box(entry.seq);
                black_box(entry.stream_id);
                black_box(entry.timestamp);
                black_box(entry.flags);
                black_box(entry.subject);
                black_box(entry.payload);
            })
            .unwrap();
        cursor = end;
    }
    (start.elapsed().as_nanos(), visits)
}

// ── Stage 1b: RawEntry — raw bytes API with inline getters ──────────────────

fn measure_raw(store: &MemoryStore, batch_size: u64) -> (u128, u64) {
    let mut visits: u64 = 0;
    let total = N_MESSAGES as u64;

    let start = Instant::now();
    let mut cursor: u64 = 1;
    while cursor <= total {
        let end = (cursor + batch_size).min(total + 1);
        store
            .for_each_raw(cursor, end, &mut |raw| {
                visits += 1;
                black_box(raw.seq);
                black_box(raw.stream_id);
                black_box(raw.timestamp);
                black_box(raw.flags);
                black_box(raw.subject());
                black_box(raw.payload());
            })
            .unwrap();
        cursor = end;
    }
    (start.elapsed().as_nanos(), visits)
}

// ── Stage 2: RawEntry + max_age TTL check ──────────────────────────────────

fn measure_raw_with_ttl(
    store: &MemoryStore,
    batch_size: u64,
    max_age_ms: u64,
    now: u64,
) -> (u128, u64, u64) {
    let mut visited: u64 = 0;
    let mut skipped: u64 = 0;
    let total = N_MESSAGES as u64;

    let start = Instant::now();
    let mut cursor: u64 = 1;
    while cursor <= total {
        let end = (cursor + batch_size).min(total + 1);
        store
            .for_each_raw(cursor, end, &mut |raw| {
                // TTL check — mirrors drain.rs predicate.
                if max_age_ms > 0
                    && raw.timestamp > 0
                    && raw.timestamp + max_age_ms <= now
                {
                    skipped += 1;
                    return;
                }
                visited += 1;
                black_box(raw.seq);
                black_box(raw.stream_id);
                black_box(raw.flags);
                black_box(raw.subject());
                black_box(raw.payload());
            })
            .unwrap();
        cursor = end;
    }
    (start.elapsed().as_nanos(), visited, skipped)
}

// ── Runners ─────────────────────────────────────────────────────────────────

fn run_entry(label: &str, store: &MemoryStore, batch_size: u64) -> u128 {
    println!("── {label} — batch_size={batch_size} ──");
    for _ in 0..WARMUP_RUNS {
        let _ = measure_entry(store, batch_size);
    }
    let mut best = u128::MAX;
    for i in 0..MEASURE_RUNS {
        let (ns, visits) = measure_entry(store, batch_size);
        if ns < best { best = ns; }
        println!(
            "  Run {} — {:>6.2} ms  |  {:>5.2} ns/msg  |  {:>7.1} M msg/s",
            i + 1,
            ns as f64 / 1e6,
            ns as f64 / visits as f64,
            (visits as f64) / (ns as f64 / 1e9) / 1e6,
        );
    }
    println!();
    best
}

fn run_raw(label: &str, store: &MemoryStore, batch_size: u64) -> u128 {
    println!("── {label} — batch_size={batch_size} ──");
    for _ in 0..WARMUP_RUNS {
        let _ = measure_raw(store, batch_size);
    }
    let mut best = u128::MAX;
    for i in 0..MEASURE_RUNS {
        let (ns, visits) = measure_raw(store, batch_size);
        if ns < best { best = ns; }
        println!(
            "  Run {} — {:>6.2} ms  |  {:>5.2} ns/msg  |  {:>7.1} M msg/s",
            i + 1,
            ns as f64 / 1e6,
            ns as f64 / visits as f64,
            (visits as f64) / (ns as f64 / 1e9) / 1e6,
        );
    }
    println!();
    best
}

// ══════════════════════════════════════════════════════════════════════════
// Stage 3 — FULL PIPELINE: for_each_raw → encode → TCP send → recv → decode
// ══════════════════════════════════════════════════════════════════════════
//
// Wire format per batch frame:
//   [4B frame_len (u32 LE)]
//   [2B entry_count (u16 LE)]
//   [2B _pad]
//   For each entry:
//     [8B seq u64 LE]
//     [4B stream_id u32 LE]
//     [8B ts u64 LE]
//     [1B flags]
//     [1B _pad]
//     [2B subj_len u16 LE]
//     [4B payload_len u32 LE]
//     [subj_len bytes subject]
//     [payload_len bytes payload]

const ENTRY_HDR_BYTES: usize = 8 + 4 + 8 + 1 + 1 + 2 + 4; // = 28
const FRAME_HDR_BYTES: usize = 4 + 2 + 2; // = 8

fn encode_entry_into(
    buf: &mut Vec<u8>,
    seq: u64,
    stream_id: u32,
    ts: u64,
    flags: u8,
    subject: &[u8],
    payload: &[u8],
) {
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&stream_id.to_le_bytes());
    buf.extend_from_slice(&ts.to_le_bytes());
    buf.push(flags);
    buf.push(0); // _pad
    buf.extend_from_slice(&(subject.len() as u16).to_le_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(subject);
    buf.extend_from_slice(payload);
}

/// Server side: iterate store with for_each_raw, encode INLINE (no scratch),
/// send via TCP. The arena slices are written directly into the frame buffer
/// inside the callback — no intermediate `.to_vec()` or owned scratch.
fn run_server(mut stream: TcpStream) -> (u128, u128, u128, u64, u64) {
    // Returns: (for_each_ns, encode_ns, write_ns, entries_sent, bytes_sent)
    let store = build_store();
    let mut frame = Vec::with_capacity(1 << 20);
    let total = N_MESSAGES as u64;

    let mut for_each_ns: u128 = 0;
    let mut encode_ns: u128 = 0;
    let mut write_ns: u128 = 0;
    let mut entries_sent: u64 = 0;
    let mut bytes_sent: u64 = 0;

    let mut cursor: u64 = 1;
    while cursor <= total {
        let end = (cursor + BATCH_SIZE as u64).min(total + 1);

        // Phase 1+2 (fused): walk store + encode each entry directly into
        // the frame buffer. No scratch, no per-msg alloc.
        let t0 = Instant::now();
        frame.clear();
        frame.extend_from_slice(&[0u8; FRAME_HDR_BYTES]);
        let mut entry_count: u16 = 0;
        store
            .for_each_raw(cursor, end, &mut |raw| {
                encode_entry_into(
                    &mut frame,
                    raw.seq,
                    raw.stream_id,
                    raw.timestamp,
                    raw.flags,
                    raw.subject(),
                    raw.payload(),
                );
                entry_count += 1;
            })
            .unwrap();
        for_each_ns += t0.elapsed().as_nanos();

        // Phase 2 (header patch only — bulk encoding fused above).
        let t0 = Instant::now();
        let frame_len = (frame.len() - 4) as u32;
        frame[0..4].copy_from_slice(&frame_len.to_le_bytes());
        frame[4..6].copy_from_slice(&entry_count.to_le_bytes());
        frame[6..8].copy_from_slice(&[0u8; 2]);
        encode_ns += t0.elapsed().as_nanos();

        // Phase 3: TCP write
        let t0 = Instant::now();
        stream.write_all(&frame).unwrap();
        write_ns += t0.elapsed().as_nanos();

        entries_sent += entry_count as u64;
        bytes_sent += frame.len() as u64;
        cursor = end;
    }

    stream.flush().unwrap();
    // Half-close write side so client sees EOF.
    let _ = stream.shutdown(std::net::Shutdown::Write);

    (for_each_ns, encode_ns, write_ns, entries_sent, bytes_sent)
}

/// Server side (writev): no per-message memcpy of payloads.
/// Builds entry headers in a contiguous scratch + captures raw pointers to
/// arena slices, then issues `write_vectored` so the kernel gathers bytes
/// directly from the store's mmap segments.
///
/// SAFETY: arena slices remain valid as long as the `MemoryStore` is not
/// mutated (no concurrent appends here — store is built once and read).
fn run_server_writev(mut stream: TcpStream) -> (u128, u128, u128, u64, u64) {
    let store = build_store();
    let total = N_MESSAGES as u64;

    // Pre-allocated scratches — reused across batches.
    let mut entry_hdrs: Vec<u8> = Vec::with_capacity(BATCH_SIZE * ENTRY_HDR_BYTES);
    let mut frame_hdr = [0u8; FRAME_HDR_BYTES];
    // Captured (ptr, len) pairs — 2 per entry (subject, payload).
    let mut arena_refs: Vec<(*const u8, usize)> = Vec::with_capacity(BATCH_SIZE * 2);

    let mut for_each_ns: u128 = 0;
    let mut encode_ns: u128 = 0;
    let mut write_ns: u128 = 0;
    let mut entries_sent: u64 = 0;
    let mut bytes_sent: u64 = 0;

    let mut cursor: u64 = 1;
    while cursor <= total {
        let end = (cursor + BATCH_SIZE as u64).min(total + 1);
        let mut count: u16 = 0;
        let mut payload_total: usize = 0;

        // Phase 1: walk store, build entry headers + capture arena pointers.
        let t0 = Instant::now();
        entry_hdrs.clear();
        arena_refs.clear();
        store
            .for_each_raw(cursor, end, &mut |raw| {
                let subj = raw.subject();
                let pay = raw.payload();

                entry_hdrs.extend_from_slice(&raw.seq.to_le_bytes());
                entry_hdrs.extend_from_slice(&raw.stream_id.to_le_bytes());
                entry_hdrs.extend_from_slice(&raw.timestamp.to_le_bytes());
                entry_hdrs.push(raw.flags);
                entry_hdrs.push(0); // _pad
                entry_hdrs.extend_from_slice(&(subj.len() as u16).to_le_bytes());
                entry_hdrs.extend_from_slice(&(pay.len() as u32).to_le_bytes());

                arena_refs.push((subj.as_ptr(), subj.len()));
                arena_refs.push((pay.as_ptr(), pay.len()));

                payload_total += subj.len() + pay.len();
                count += 1;
            })
            .unwrap();
        for_each_ns += t0.elapsed().as_nanos();

        // Phase 2: build frame header + assemble iovecs.
        let t0 = Instant::now();
        let frame_body_len = 4 /* count+pad */
            + (count as usize) * ENTRY_HDR_BYTES
            + payload_total;
        let frame_len_field = frame_body_len as u32;
        frame_hdr[0..4].copy_from_slice(&frame_len_field.to_le_bytes());
        frame_hdr[4..6].copy_from_slice(&count.to_le_bytes());
        frame_hdr[6..8].copy_from_slice(&[0u8; 2]);

        // iovecs is scoped to this batch — borrows of frame_hdr and entry_hdrs
        // end when the Vec is dropped at the end of the iteration.
        let mut iovecs: Vec<IoSlice<'_>> = Vec::with_capacity(1 + count as usize * 3);
        iovecs.push(IoSlice::new(&frame_hdr));
        for i in 0..count as usize {
            let hdr_slice =
                &entry_hdrs[i * ENTRY_HDR_BYTES..(i + 1) * ENTRY_HDR_BYTES];
            iovecs.push(IoSlice::new(hdr_slice));
            // SAFETY: arena (mmap segments in MemoryStore) is not mutated
            // while we hold these pointers; store outlives this function.
            let (sp, sl) = arena_refs[i * 2];
            let (pp, pl) = arena_refs[i * 2 + 1];
            unsafe {
                iovecs.push(IoSlice::new(std::slice::from_raw_parts(sp, sl)));
                iovecs.push(IoSlice::new(std::slice::from_raw_parts(pp, pl)));
            }
        }
        let frame_total = 4 + frame_body_len;
        encode_ns += t0.elapsed().as_nanos();

        // Phase 3: write_vectored, looping until all bytes drained.
        let t0 = Instant::now();
        let mut iov_slice: &mut [IoSlice<'_>] = iovecs.as_mut_slice();
        let mut written_total: usize = 0;
        while !iov_slice.is_empty() {
            let n = stream.write_vectored(iov_slice).unwrap();
            if n == 0 {
                break;
            }
            written_total += n;
            IoSlice::advance_slices(&mut iov_slice, n);
        }
        write_ns += t0.elapsed().as_nanos();

        debug_assert_eq!(written_total, frame_total);
        entries_sent += count as u64;
        bytes_sent += frame_total as u64;
        cursor = end;
    }

    stream.flush().unwrap();
    let _ = stream.shutdown(std::net::Shutdown::Write);

    (for_each_ns, encode_ns, write_ns, entries_sent, bytes_sent)
}

/// Client side: read from TCP, decode frames, count entries.
fn run_client(mut stream: TcpStream, expected: u64) -> (u128, u128, u64) {
    let mut read_buf = vec![0u8; 4 * 1024 * 1024];
    let mut ring: Vec<u8> = Vec::with_capacity(8 * 1024 * 1024);
    let mut recv_ns: u128 = 0;
    let mut decode_ns: u128 = 0;
    let mut decoded: u64 = 0;

    while decoded < expected {
        // Phase 4: TCP recv
        let t0 = Instant::now();
        let n = match stream.read(&mut read_buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        recv_ns += t0.elapsed().as_nanos();
        ring.extend_from_slice(&read_buf[..n]);

        // Phase 5: decode frames in ring
        let t0 = Instant::now();
        let mut cursor = 0usize;
        loop {
            if ring.len() - cursor < 4 { break; }
            let frame_len = u32::from_le_bytes(
                ring[cursor..cursor + 4].try_into().unwrap(),
            ) as usize;
            let total = 4 + frame_len;
            if ring.len() - cursor < total { break; }

            // Parse frame: [4 len][2 count][2 pad][entries...]
            let count = u16::from_le_bytes(
                ring[cursor + 4..cursor + 6].try_into().unwrap(),
            ) as u64;

            // Walk entries
            let mut e_off = cursor + FRAME_HDR_BYTES;
            for _ in 0..count {
                let seq = u64::from_le_bytes(
                    ring[e_off..e_off + 8].try_into().unwrap(),
                );
                let stream_id = u32::from_le_bytes(
                    ring[e_off + 8..e_off + 12].try_into().unwrap(),
                );
                let ts = u64::from_le_bytes(
                    ring[e_off + 12..e_off + 20].try_into().unwrap(),
                );
                let flags = ring[e_off + 20];
                let subj_len = u16::from_le_bytes(
                    ring[e_off + 22..e_off + 24].try_into().unwrap(),
                ) as usize;
                let payload_len = u32::from_le_bytes(
                    ring[e_off + 24..e_off + 28].try_into().unwrap(),
                ) as usize;
                let subject = &ring[e_off + 28..e_off + 28 + subj_len];
                let payload =
                    &ring[e_off + 28 + subj_len..e_off + 28 + subj_len + payload_len];

                black_box(seq);
                black_box(stream_id);
                black_box(ts);
                black_box(flags);
                black_box(subject);
                black_box(payload);

                e_off += ENTRY_HDR_BYTES + subj_len + payload_len;
            }

            decoded += count;
            cursor += total;
        }
        if cursor > 0 {
            ring.drain(..cursor);
        }
        decode_ns += t0.elapsed().as_nanos();
    }

    (recv_ns, decode_ns, decoded)
}

/// Full pipeline single run.
struct PipelineTimings {
    for_each_ns: u128,
    encode_ns: u128,
    write_ns: u128,
    recv_ns: u128,
    decode_ns: u128,
    total_ns: u128,
    entries: u64,
    bytes: u64,
}

fn measure_full_pipeline() -> PipelineTimings {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let t_total = Instant::now();

    let server = thread::spawn(move || {
        let (conn, _) = listener.accept().unwrap();
        conn.set_nodelay(true).unwrap();
        run_server(conn)
    });

    let expected = N_MESSAGES as u64;
    let client = thread::spawn(move || {
        let conn = TcpStream::connect(addr).unwrap();
        conn.set_nodelay(true).unwrap();
        run_client(conn, expected)
    });

    let (for_each_ns, encode_ns, write_ns, entries, bytes) = server.join().unwrap();
    let (recv_ns, decode_ns, _decoded) = client.join().unwrap();
    let total_ns = t_total.elapsed().as_nanos();

    PipelineTimings {
        for_each_ns,
        encode_ns,
        write_ns,
        recv_ns,
        decode_ns,
        total_ns,
        entries,
        bytes,
    }
}

fn measure_full_pipeline_writev() -> PipelineTimings {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let t_total = Instant::now();

    let server = thread::spawn(move || {
        let (conn, _) = listener.accept().unwrap();
        conn.set_nodelay(true).unwrap();
        run_server_writev(conn)
    });

    let expected = N_MESSAGES as u64;
    let client = thread::spawn(move || {
        let conn = TcpStream::connect(addr).unwrap();
        conn.set_nodelay(true).unwrap();
        run_client(conn, expected)
    });

    let (for_each_ns, encode_ns, write_ns, entries, bytes) = server.join().unwrap();
    let (recv_ns, decode_ns, _decoded) = client.join().unwrap();
    let total_ns = t_total.elapsed().as_nanos();

    PipelineTimings {
        for_each_ns,
        encode_ns,
        write_ns,
        recv_ns,
        decode_ns,
        total_ns,
        entries,
        bytes,
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Stage 4 — PURE TRANSFER: pre-built frames in memory
//   Only times: send → recv → decode (no store, no per-msg encode work)
// ══════════════════════════════════════════════════════════════════════════

/// Pre-build all the wire frames so the server-side timer measures
/// ONLY the cost of write_all + kernel send. No store walk, no encode.
fn prebuild_frames() -> (Vec<Vec<u8>>, u64, u64) {
    let store = build_store();
    let total = N_MESSAGES as u64;
    let mut frames: Vec<Vec<u8>> = Vec::new();
    let mut entries_total: u64 = 0;
    let mut bytes_total: u64 = 0;

    let mut cursor: u64 = 1;
    while cursor <= total {
        let end = (cursor + BATCH_SIZE as u64).min(total + 1);
        let count = (end - cursor) as usize;

        let mut frame: Vec<u8> = Vec::with_capacity(8 + count * (ENTRY_HDR_BYTES + 128));
        frame.extend_from_slice(&[0u8; FRAME_HDR_BYTES]);

        store
            .for_each_raw(cursor, end, &mut |raw| {
                encode_entry_into(
                    &mut frame,
                    raw.seq,
                    raw.stream_id,
                    raw.timestamp,
                    raw.flags,
                    raw.subject(),
                    raw.payload(),
                );
            })
            .unwrap();

        let frame_len = (frame.len() - 4) as u32;
        frame[0..4].copy_from_slice(&frame_len.to_le_bytes());
        frame[4..6].copy_from_slice(&(count as u16).to_le_bytes());
        frame[6..8].copy_from_slice(&[0u8; 2]);

        entries_total += count as u64;
        bytes_total += frame.len() as u64;
        frames.push(frame);
        cursor = end;
    }
    (frames, entries_total, bytes_total)
}

struct PureTimings {
    send_ns: u128,
    recv_ns: u128,
    decode_ns: u128,
    total_ns: u128,
    entries: u64,
    bytes: u64,
}

fn measure_pure_transfer(frames: &[Vec<u8>], entries: u64, bytes: u64) -> PureTimings {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // Send via raw pointer to avoid lifetime juggling across the spawn.
    let frames_ptr = frames.as_ptr() as usize;
    let frames_len = frames.len();

    let t_total = Instant::now();

    let server = thread::spawn(move || {
        let (mut conn, _) = listener.accept().unwrap();
        conn.set_nodelay(true).unwrap();
        // SAFETY: caller keeps `frames` alive for the duration of this fn.
        let frames: &[Vec<u8>] =
            unsafe { std::slice::from_raw_parts(frames_ptr as *const Vec<u8>, frames_len) };

        let t0 = Instant::now();
        for f in frames {
            conn.write_all(f).unwrap();
        }
        conn.flush().unwrap();
        let _ = conn.shutdown(std::net::Shutdown::Write);
        t0.elapsed().as_nanos()
    });

    let client = thread::spawn(move || {
        let conn = TcpStream::connect(addr).unwrap();
        conn.set_nodelay(true).unwrap();
        run_client(conn, entries)
    });

    let send_ns = server.join().unwrap();
    let (recv_ns, decode_ns, _decoded) = client.join().unwrap();
    let total_ns = t_total.elapsed().as_nanos();

    PureTimings {
        send_ns,
        recv_ns,
        decode_ns,
        total_ns,
        entries,
        bytes,
    }
}

fn print_pure(label: &str, t: &PureTimings) {
    let total_s = t.total_ns as f64 / 1e9;
    let mops = t.entries as f64 / total_s / 1e6;
    let gbps = (t.bytes as f64 / 1e9) / total_s;
    println!("── {label} ──");
    println!("  Total       {:>7.2} ms  |  {:>5.2} M msg/s  |  {:>4.2} GB/s",
        t.total_ns as f64 / 1e6, mops, gbps);
    let pct = |ns: u128| (ns as f64 / t.total_ns as f64) * 100.0;
    let per = |ns: u128| ns as f64 / t.entries as f64;
    println!("    send      {:>6.2} ms  ({:>5.1}%)  {:>5.2} ns/msg",
        t.send_ns as f64 / 1e6, pct(t.send_ns), per(t.send_ns));
    println!("    recv      {:>6.2} ms  ({:>5.1}%)  {:>5.2} ns/msg",
        t.recv_ns as f64 / 1e6, pct(t.recv_ns), per(t.recv_ns));
    println!("    decode    {:>6.2} ms  ({:>5.1}%)  {:>5.2} ns/msg",
        t.decode_ns as f64 / 1e6, pct(t.decode_ns), per(t.decode_ns));
    println!();
}

fn print_pipeline(label: &str, t: &PipelineTimings) {
    let total_s = t.total_ns as f64 / 1e9;
    let msg_per_s = t.entries as f64 / total_s / 1e6;
    let gb_per_s = (t.bytes as f64 / 1e9) / total_s;

    println!("── {label} ──");
    println!("  Total       {:>7.2} ms  |  {:>5.2} M msg/s  |  {:>4.2} GB/s",
        t.total_ns as f64 / 1e6, msg_per_s, gb_per_s);
    let pct = |ns: u128| (ns as f64 / t.total_ns as f64) * 100.0;
    let per = |ns: u128| (ns as f64 / t.entries as f64);
    println!("    for_each  {:>6.2} ms  ({:>5.1}%)  {:>5.2} ns/msg",
        t.for_each_ns as f64 / 1e6, pct(t.for_each_ns), per(t.for_each_ns));
    println!("    encode    {:>6.2} ms  ({:>5.1}%)  {:>5.2} ns/msg",
        t.encode_ns as f64 / 1e6, pct(t.encode_ns), per(t.encode_ns));
    println!("    write     {:>6.2} ms  ({:>5.1}%)  {:>5.2} ns/msg",
        t.write_ns as f64 / 1e6, pct(t.write_ns), per(t.write_ns));
    println!("    recv      {:>6.2} ms  ({:>5.1}%)  {:>5.2} ns/msg",
        t.recv_ns as f64 / 1e6, pct(t.recv_ns), per(t.recv_ns));
    println!("    decode    {:>6.2} ms  ({:>5.1}%)  {:>5.2} ns/msg",
        t.decode_ns as f64 / 1e6, pct(t.decode_ns), per(t.decode_ns));
    println!();
}

fn run_raw_with_ttl(
    label: &str,
    store: &MemoryStore,
    batch_size: u64,
    now: u64,
) -> u128 {
    println!("── {label} — batch_size={batch_size} ──");
    for _ in 0..WARMUP_RUNS {
        let _ = measure_raw_with_ttl(store, batch_size, MAX_AGE_MS, now);
    }
    let mut best = u128::MAX;
    for i in 0..MEASURE_RUNS {
        let (ns, visited, skipped) =
            measure_raw_with_ttl(store, batch_size, MAX_AGE_MS, now);
        if ns < best { best = ns; }
        println!(
            "  Run {} — {:>6.2} ms  |  {:>5.2} ns/msg  |  {:>7.1} M msg/s  |  visited={} skipped={}",
            i + 1,
            ns as f64 / 1e6,
            ns as f64 / (visited + skipped) as f64,
            ((visited + skipped) as f64) / (ns as f64 / 1e9) / 1e6,
            visited, skipped,
        );
    }
    println!();
    best
}

// ── Main ────────────────────────────────────────────────────────────────────

fn main() {
    println!();
    println!("Drain pipeline — Entry<'_> vs RawEntry");
    println!("=======================================");
    println!("  N messages:  {}", N_MESSAGES);
    println!("  Subject:     {:?} ({} B)",
        std::str::from_utf8(SUBJECT).unwrap(), SUBJECT.len());
    println!("  Payload:     {} B", PAYLOAD_SIZE);
    println!("  max_age_ms:  {}", MAX_AGE_MS);
    println!();

    let store = build_store();
    let info = store.info();
    println!("  Store state: {} messages, {} bytes ({:.2} MB)",
        info.messages, info.bytes, info.bytes as f64 / 1e6);
    println!();

    let current_now = now_ms();

    // ── Stage 1: Entry<'_> (arbitro actual) ──
    println!("═══ STAGE 1 — Entry<'_> (arbitro `store.for_each`) ═══");
    let e_8   = run_entry("Stage 1", &store, 8);
    let e_256 = run_entry("Stage 1", &store, 256);
    let e_all = run_entry("Stage 1", &store, N_MESSAGES as u64);

    // ── Stage 1b: RawEntry ──
    println!("═══ STAGE 1b — RawEntry (`store.for_each_raw` + getters) ═══");
    let r_8   = run_raw("Stage 1b", &store, 8);
    let r_256 = run_raw("Stage 1b", &store, 256);
    let r_all = run_raw("Stage 1b", &store, N_MESSAGES as u64);

    // ── Stage 2: RawEntry + max_age ──
    println!("═══ STAGE 2 — RawEntry + max_age TTL check ═══");
    let t_8   = run_raw_with_ttl("Stage 2", &store, 8, current_now);
    let t_256 = run_raw_with_ttl("Stage 2", &store, 256, current_now);
    let t_all = run_raw_with_ttl("Stage 2", &store, N_MESSAGES as u64, current_now);

    // ── Stage 3: FULL PIPELINE ──
    println!("═══ STAGE 3 — FULL PIPELINE (store → encode → TCP → recv → decode) ═══");
    for _ in 0..WARMUP_RUNS {
        let _ = measure_full_pipeline();
    }
    let mut best_pipeline: Option<PipelineTimings> = None;
    for i in 0..MEASURE_RUNS {
        let t = measure_full_pipeline();
        let label = format!("Run {}", i + 1);
        print_pipeline(&label, &t);
        if best_pipeline.as_ref().map(|b| t.total_ns < b.total_ns).unwrap_or(true) {
            best_pipeline = Some(t);
        }
    }
    let p = best_pipeline.unwrap();

    // ── Stage 3b: WRITEV PIPELINE ──
    println!("═══ STAGE 3b — WRITEV PIPELINE (no per-msg copy, kernel gather) ═══");
    for _ in 0..WARMUP_RUNS {
        let _ = measure_full_pipeline_writev();
    }
    let mut best_pipeline_wv: Option<PipelineTimings> = None;
    for i in 0..MEASURE_RUNS {
        let t = measure_full_pipeline_writev();
        let label = format!("Run {}", i + 1);
        print_pipeline(&label, &t);
        if best_pipeline_wv.as_ref().map(|b| t.total_ns < b.total_ns).unwrap_or(true) {
            best_pipeline_wv = Some(t);
        }
    }
    let pwv = best_pipeline_wv.unwrap();

    // ── Stage 4: PURE TRANSFER (pre-built frames, only send→recv→decode) ──
    println!("═══ STAGE 4 — PURE TRANSFER (encode→send→recv→decode, frames pre-built) ═══");
    let (frames, p_entries, p_bytes) = prebuild_frames();
    println!("  Pre-built: {} frames, {} entries, {} bytes ({:.2} MB)",
        frames.len(), p_entries, p_bytes, p_bytes as f64 / 1e6);
    println!();

    for _ in 0..WARMUP_RUNS {
        let _ = measure_pure_transfer(&frames, p_entries, p_bytes);
    }
    let mut best_pure: Option<PureTimings> = None;
    for i in 0..MEASURE_RUNS {
        let t = measure_pure_transfer(&frames, p_entries, p_bytes);
        let label = format!("Run {}", i + 1);
        print_pure(&label, &t);
        if best_pure.as_ref().map(|b| t.total_ns < b.total_ns).unwrap_or(true) {
            best_pure = Some(t);
        }
    }
    let pure = best_pure.unwrap();

    // ── Summary ──
    println!("═══ SUMMARY (best runs) ═══");
    println!();
    println!("                │   batch=8      │   batch=256    │   batch={}", N_MESSAGES);
    println!("────────────────┼────────────────┼────────────────┼────────────────");
    for (name, b8, b256, ball) in [
        ("Stage 1  Entry   ", e_8, e_256, e_all),
        ("Stage 1b RawEntry", r_8, r_256, r_all),
        ("Stage 2  +ttl    ", t_8, t_256, t_all),
    ] {
        let f = |ns: u128| -> String {
            let per = ns as f64 / N_MESSAGES as f64;
            let mops = N_MESSAGES as f64 / (ns as f64 / 1e9) / 1e6;
            format!("{:>4.2} ns | {:>5.0}M/s", per, mops)
        };
        println!("{} │ {} │ {} │ {}", name, f(b8), f(b256), f(ball));
    }
    println!();
    println!("─── Stage 3  — FULL PIPELINE   (best run) ───");
    print_pipeline("Stage 3",  &p);
    println!("─── Stage 3b — WRITEV PIPELINE (best run) ───");
    print_pipeline("Stage 3b", &pwv);
    println!("─── Stage 4  — PURE TRANSFER  (best run) ───");
    print_pure("Stage 4",  &pure);

    let speedup = p.total_ns as f64 / pwv.total_ns as f64;
    let encode_speedup = p.encode_ns as f64 / pwv.encode_ns.max(1) as f64;
    println!("─── DELTA ───");
    println!("  total   speedup:  {:.2}×  ({} → {} ms)",
        speedup,
        p.total_ns / 1_000_000,
        pwv.total_ns / 1_000_000);
    println!("  encode  speedup:  {:.2}×  ({:.2} → {:.2} ns/msg)",
        encode_speedup,
        p.encode_ns as f64 / p.entries as f64,
        pwv.encode_ns as f64 / pwv.entries as f64);
}
