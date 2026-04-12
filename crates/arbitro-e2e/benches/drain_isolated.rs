//! Incremental drain bench — measures each layer of the pipeline independently.
//!
//! Each scenario adds ONE component on top of the previous, so we can see
//! exactly where throughput degrades:
//!
//!   Layer 1: get_messages     — store.for_each only (read + validate)
//!   Layer 2: + ready queue    — enqueue_ready + pop + store read
//!   Layer 3: + frame build    — RepBatch serialization + freeze
//!   Layer 4: + channel        — crossbeam bounded send/recv
//!
//! Run: cargo bench --bench drain_isolated -p arbitro-e2e

use std::time::Instant;
use bytes::BytesMut;
use zerocopy::{FromBytes, IntoBytes};
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use arbitro_engine_v2::catalog::{self, fnv1a_32};
use arbitro_engine_v2::types::*;
use arbitro_engine_v2::ArbitroEngine;
use arbitro_proto::wire::delivery::{DeliveryEntryHeader, RepBatchFixed};
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_proto::action::Action;
use arbitro_store::{EntryRef, MemoryStore, SeedHeader, Store, TolerantStore};

const PAYLOAD_SIZE: usize = 64;
const CLAIM_BATCH: usize = 256;
const MAX_FEED_PER_CYCLE: u64 = 256;

// ─── Shared helpers ─────────────────────────────────────────────────────────

fn make_store(total_msgs: usize) -> Box<dyn Store> {
    let subject = b"bench.drain.subject";
    let payload = vec![0x42u8; PAYLOAD_SIZE];
    let mut store: Box<dyn Store> = Box::new(MemoryStore::new());
    for _ in 0..total_msgs {
        store.append(EntryRef { subject, payload: &payload }, 0).unwrap();
    }
    store
}

fn make_engine() -> ArbitroEngine {
    let stream_id = StreamId(1);
    let queue_id = QueueId(1);
    let consumer_id = ConsumerId(1);

    let mut engine = ArbitroEngine::new();
    engine.ensure_stream(catalog::StreamConfig {
        id: stream_id,
        name: b"bench-stream".to_vec(),
    }).unwrap();
    engine.ensure_consumer(catalog::ConsumerConfig {
        id: consumer_id,
        queue_id,
        stream_id,
        durable: false,
        ack_policy: AckPolicy::None,
        max_inflight: u32::MAX,
    }).unwrap();
    engine.ensure_subscription(catalog::SubscriptionConfig {
        id: SubscriptionId(1),
        stream_id,
        consumer_id,
        filters: vec![],
    }).unwrap();
    engine
}

/// Feed up to `MAX_FEED_PER_CYCLE` entries starting from `last_seq + 1`.
/// Returns the last seq fed, or `last_seq` if nothing was fed.
fn feed_engine_capped(
    store: &dyn Store,
    engine: &mut ArbitroEngine,
    last_seq: u64,
) -> u64 {
    let info = store.info();
    if info.last_seq <= last_seq { return last_seq; }
    let stream_id = StreamId(1);
    let start = last_seq + 1;
    let end = (start + MAX_FEED_PER_CYCLE).min(info.last_seq + 1);
    let mut fed_last = last_seq;
    store.for_each(start, end, &mut |entry| {
        let hash = fnv1a_32(entry.subject);
        engine.enqueue_ready(stream_id, entry.subject, hash, entry.seq);
        fed_last = entry.seq;
    }).unwrap();
    fed_last
}

/// Pop a batch of seqs from the ready queue into scratch_seqs. Returns count.
fn pop_batch(engine: &mut ArbitroEngine, scratch_seqs: &mut Vec<u64>) -> usize {
    let queue_id = QueueId(1);
    scratch_seqs.clear();
    for _ in 0..CLAIM_BATCH {
        match engine.ctx_mut().ready.pop(queue_id) {
            Some((_sh, seq)) => scratch_seqs.push(seq),
            None => break,
        }
    }
    scratch_seqs.len()
}

/// Read a contiguous batch from store into scratch_body as RepBatch wire format.
fn read_batch_into_body(
    store: &dyn Store,
    scratch_seqs: &[u64],
    consumer_id: ConsumerId,
    scratch_body: &mut BytesMut,
) {
    let count = scratch_seqs.len();
    scratch_body.clear();
    scratch_body.extend_from_slice(
        RepBatchFixed {
            consumer_id: U32::new(consumer_id.0),
            count: U16::new(count as u16),
            _pad: U16::new(0),
        }.as_bytes(),
    );

    let first = scratch_seqs[0];
    let last = scratch_seqs[count - 1];
    let body = scratch_body;
    store.for_each(first, last + 1, &mut |entry| {
        let subj_len = entry.subject.len();
        let data_len = subj_len + entry.payload.len();
        body.extend_from_slice(DeliveryEntryHeader {
            seq: U64::new(entry.seq),
            subj_len: U16::new(subj_len as u16),
            data_len: U32::new(data_len as u32),
        }.as_bytes());
        body.extend_from_slice(entry.subject);
        body.extend_from_slice(entry.payload);
    }).unwrap();
}

/// Build envelope + body into a frozen Bytes frame.
fn build_frame(stream_id: StreamId, scratch_body: &BytesMut) -> bytes::Bytes {
    let body_len = scratch_body.len();
    let envelope = Envelope::new(Action::RepBatch, stream_id.raw(), body_len as u32, 0);
    let mut frame = BytesMut::with_capacity(ENVELOPE_SIZE + body_len);
    frame.extend_from_slice(envelope.as_bytes());
    frame.extend_from_slice(scratch_body);
    frame.freeze()
}

fn fmt_rate(msgs: usize, elapsed: std::time::Duration) -> String {
    let rate = msgs as f64 / elapsed.as_secs_f64();
    if rate >= 1_000_000_000.0 {
        format!("{:.1}G", rate / 1_000_000_000.0)
    } else if rate >= 1_000_000.0 {
        format!("{:.1}M", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}K", rate / 1_000.0)
    } else {
        format!("{:.0}", rate)
    }
}

fn fmt_ms(elapsed: std::time::Duration) -> String {
    format!("{:.2}ms", elapsed.as_secs_f64() * 1000.0)
}

fn fmt_ns(elapsed: std::time::Duration) -> String {
    let ns = elapsed.as_nanos();
    if ns >= 1_000_000 {
        format!("{:.2}ms", elapsed.as_secs_f64() * 1000.0)
    } else if ns >= 1_000 {
        format!("{:.0}ns", ns)
    } else {
        format!("{}ns", ns)
    }
}

// ─── Layer 0a: store.info() ─────────────────────────────────────────────────

fn layer0_store_info(store: &dyn Store, total_msgs: usize) {
    let iterations = 1_000_000;
    let t0 = Instant::now();
    let mut last = 0u64;
    for _ in 0..iterations {
        let info = store.info();
        last = info.last_seq;
    }
    let elapsed = t0.elapsed();
    let per_call = elapsed / iterations as u32;
    assert_eq!(last, total_msgs as u64);
    println!(
        "    store.info()            {} / call ({} × 1M calls)",
        fmt_ns(per_call), fmt_ms(elapsed),
    );
}

// ─── Layer 0b: fnv1a_32 ─────────────────────────────────────────────────────

fn layer0_fnv(total_msgs: usize) {
    let subject = b"bench.drain.subject";
    let iterations = total_msgs;

    let t0 = Instant::now();
    let mut hash = 0u32;
    for _ in 0..iterations {
        hash = fnv1a_32(subject);
    }
    let elapsed = t0.elapsed();
    let per_call = elapsed / iterations as u32;
    std::hint::black_box(hash);
    println!(
        "    fnv1a_32({} bytes)      {} / call ({} × {})",
        subject.len(), fmt_ns(per_call), fmt_ms(elapsed), fmt_rate(iterations, elapsed),
    );
}

// ─── Layer 1: get_messages — store read strategies ──────────────────────────

fn layer1_get_messages(total_msgs: usize) {
    let store = make_store(total_msgs);
    let info = store.info();

    // ── A. for_each (zero-copy callback, zero-alloc) ────────────────
    {
        let mut count = 0usize;
        let t0 = Instant::now();
        store.for_each(1, info.last_seq + 1, &mut |entry| {
            std::hint::black_box(entry.subject);
            std::hint::black_box(entry.payload);
            count += 1;
        }).unwrap();
        let elapsed = t0.elapsed();
        assert_eq!(count, total_msgs);
        println!(
            "    for_each (zero-copy)    {} ({} msg/s)",
            fmt_ms(elapsed), fmt_rate(total_msgs, elapsed),
        );
    }

    // ── B. read_range (returns Vec<Entry>, allocs Vec) ──────────────
    {
        let t0 = Instant::now();
        let entries = store.read_range(1, info.last_seq + 1).unwrap();
        let read_elapsed = t0.elapsed();
        assert_eq!(entries.len(), total_msgs);

        // Iterate to simulate the work the caller would do
        let t_iter = Instant::now();
        let mut count = 0usize;
        for entry in &entries {
            std::hint::black_box(entry.subject);
            std::hint::black_box(entry.payload);
            count += 1;
        }
        let iter_elapsed = t_iter.elapsed();
        let total_elapsed = read_elapsed + iter_elapsed;
        assert_eq!(count, total_msgs);
        println!(
            "    read_range (Vec alloc)  {} ({} msg/s)  [read: {}, iter: {}]",
            fmt_ms(total_elapsed), fmt_rate(total_msgs, total_elapsed),
            fmt_ms(read_elapsed), fmt_ms(iter_elapsed),
        );
    }

    // ── C. for_each + accumulate (hash, seq) — zero-copy + batch-ready
    {
        let mut items: Vec<(u32, u64)> = Vec::with_capacity(total_msgs);
        let t0 = Instant::now();
        store.for_each(1, info.last_seq + 1, &mut |entry| {
            let hash = fnv1a_32(entry.subject);
            items.push((hash, entry.seq));
        }).unwrap();
        let elapsed = t0.elapsed();
        assert_eq!(items.len(), total_msgs);
        println!(
            "    for_each + accum hash   {} ({} msg/s)",
            fmt_ms(elapsed), fmt_rate(total_msgs, elapsed),
        );
    }

    // ── D. read_range + build batch items (subject ref + hash + seq)
    {
        let t0 = Instant::now();
        let entries = store.read_range(1, info.last_seq + 1).unwrap();
        let items: Vec<(&[u8], u32, u64)> = entries.iter()
            .map(|e| (e.subject, fnv1a_32(e.subject), e.seq))
            .collect();
        let elapsed = t0.elapsed();
        assert_eq!(items.len(), total_msgs);
        println!(
            "    read_range + batch map  {} ({} msg/s)",
            fmt_ms(elapsed), fmt_rate(total_msgs, elapsed),
        );
    }

    // ── E. Capped: for_each in 256-chunks + accum ───────────────────
    {
        let mut total_fed = 0usize;
        let mut scratch: Vec<(u32, u64)> = Vec::with_capacity(MAX_FEED_PER_CYCLE as usize);
        let t0 = Instant::now();
        let mut start = 1u64;
        let last = info.last_seq;
        while start <= last {
            let end = (start + MAX_FEED_PER_CYCLE).min(last + 1);
            scratch.clear();
            store.for_each(start, end, &mut |entry| {
                let hash = fnv1a_32(entry.subject);
                scratch.push((hash, entry.seq));
            }).unwrap();
            total_fed += scratch.len();
            start = end;
        }
        let elapsed = t0.elapsed();
        assert_eq!(total_fed, total_msgs);
        println!(
            "    for_each capped 256     {} ({} msg/s)",
            fmt_ms(elapsed), fmt_rate(total_msgs, elapsed),
        );
    }

    // ── F. Capped: read_range in 256-chunks + batch map ─────────────
    {
        let mut total_fed = 0usize;
        let t0 = Instant::now();
        let mut start = 1u64;
        let last = info.last_seq;
        while start <= last {
            let end = (start + MAX_FEED_PER_CYCLE).min(last + 1);
            let entries = store.read_range(start, end).unwrap();
            let items: Vec<(&[u8], u32, u64)> = entries.iter()
                .map(|e| (e.subject, fnv1a_32(e.subject), e.seq))
                .collect();
            total_fed += items.len();
            start = end;
        }
        let elapsed = t0.elapsed();
        assert_eq!(total_fed, total_msgs);
        println!(
            "    read_range capped 256   {} ({} msg/s)",
            fmt_ms(elapsed), fmt_rate(total_msgs, elapsed),
        );
    }

    // ── G. Zero-copy: pre-computed headers in contiguous buffer ──────
    //
    // Simulates a store that stores subject_hash at append time in a
    // fixed-size #[repr(C)] header. The drainer casts the raw byte buffer
    // directly to &[SeedHeader] — no per-entry iteration, no fnv, no decode.
    {
        // Build the contiguous buffer simulating what the store would have
        let subject_hash = fnv1a_32(b"bench.drain.subject");
        let header_size = std::mem::size_of::<SeedHeader>();
        let buf_len = header_size * total_msgs;
        let mut buf: Vec<u8> = Vec::with_capacity(buf_len);
        for i in 0..total_msgs {
            let hdr = SeedHeader {
                seq: U64::new((i + 1) as u64),
                subject_hash: U32::new(subject_hash),
            };
            buf.extend_from_slice(hdr.as_bytes());
        }
        assert_eq!(buf.len(), buf_len);

        // G1: Cast entire buffer at once — single zerocopy::Ref, no iteration
        {
            let t0 = Instant::now();
            let headers: &[SeedHeader] = <[SeedHeader]>::ref_from_bytes(&buf).unwrap();
            let cast_elapsed = t0.elapsed();

            // Iterate to build (subject_hash, seq) pairs — simulating what
            // enqueue_ready_batch needs
            let t_iter = Instant::now();
            let mut count = 0usize;
            let mut accum_seq = 0u64;
            for hdr in headers {
                accum_seq = accum_seq.wrapping_add(hdr.seq.get());
                std::hint::black_box(hdr.subject_hash.get());
                count += 1;
            }
            let iter_elapsed = t_iter.elapsed();
            std::hint::black_box(accum_seq);
            let total_elapsed = cast_elapsed + iter_elapsed;

            assert_eq!(count, total_msgs);
            println!(
                "    zerocopy cast all       {} ({} msg/s)  [cast: {}, iter: {}]",
                fmt_ms(total_elapsed), fmt_rate(total_msgs, total_elapsed),
                fmt_ns(cast_elapsed), fmt_ms(iter_elapsed),
            );
        }

        // G2: Cast in 256-chunks — simulating capped feed with zerocopy
        {
            let cap = MAX_FEED_PER_CYCLE as usize;
            let mut total_fed = 0usize;
            let t0 = Instant::now();
            let mut offset = 0usize;
            while offset < buf.len() {
                let chunk_end = (offset + cap * header_size).min(buf.len());
                let chunk = &buf[offset..chunk_end];
                let headers: &[SeedHeader] = <[SeedHeader]>::ref_from_bytes(chunk).unwrap();
                for hdr in headers {
                    std::hint::black_box(hdr.seq.get());
                    std::hint::black_box(hdr.subject_hash.get());
                }
                total_fed += headers.len();
                offset = chunk_end;
            }
            let elapsed = t0.elapsed();
            assert_eq!(total_fed, total_msgs);
            println!(
                "    zerocopy capped 256     {} ({} msg/s)",
                fmt_ms(elapsed), fmt_rate(total_msgs, elapsed),
            );
        }

        // G3: Direct pointer cast (unsafe baseline) — absolute minimum cost
        {
            let t0 = Instant::now();
            let ptr = buf.as_ptr() as *const SeedHeader;
            let headers: &[SeedHeader] = unsafe { std::slice::from_raw_parts(ptr, total_msgs) };
            let mut count = 0usize;
            let mut accum = 0u64;
            for hdr in headers {
                accum = accum.wrapping_add(hdr.seq.get());
                std::hint::black_box(hdr.subject_hash.get());
                count += 1;
            }
            let elapsed = t0.elapsed();
            std::hint::black_box(accum);
            assert_eq!(count, total_msgs);
            println!(
                "    unsafe ptr cast (base)  {} ({} msg/s)",
                fmt_ms(elapsed), fmt_rate(total_msgs, elapsed),
            );
        }
    }

    // ���─ H. store.seed_index() — zero-copy slice from store ────────
    {
        // H1: seed_index all at once (single slice, zero alloc)
        {
            let t0 = Instant::now();
            let seeds = store.seed_index(1, info.last_seq + 1);
            let cast_elapsed = t0.elapsed();
            assert_eq!(seeds.len(), total_msgs);

            let t_iter = Instant::now();
            let mut accum = 0u64;
            for s in seeds {
                accum = accum.wrapping_add(s.seq.get());
                std::hint::black_box(s.subject_hash.get());
            }
            let iter_elapsed = t_iter.elapsed();
            std::hint::black_box(accum);
            let total_elapsed = cast_elapsed + iter_elapsed;
            println!(
                "    seed_index all           {} ({} msg/s)  [slice: {}, iter: {}]",
                fmt_ms(total_elapsed), fmt_rate(total_msgs, total_elapsed),
                fmt_ns(cast_elapsed), fmt_ms(iter_elapsed),
            );
        }

        // H2: seed_index capped 256 (sub-slices, zero alloc)
        {
            let cap = MAX_FEED_PER_CYCLE;
            let mut total_fed = 0usize;
            let t0 = Instant::now();
            let mut start = 1u64;
            let last = info.last_seq;
            while start <= last {
                let end = (start + cap).min(last + 1);
                let seeds = store.seed_index(start, end);
                for s in seeds {
                    std::hint::black_box(s.seq.get());
                    std::hint::black_box(s.subject_hash.get());
                }
                total_fed += seeds.len();
                start = end;
            }
            let elapsed = t0.elapsed();
            assert_eq!(total_fed, total_msgs);
            println!(
                "    seed_index capped 256    {} ({} msg/s)",
                fmt_ms(elapsed), fmt_rate(total_msgs, elapsed),
            );
        }
    }
}

// ─── Layer 2: + ready queue (capped feed + pop + store read) ────────────────

fn layer2_ready_queue(total_msgs: usize) {
    let store = make_store(total_msgs);
    let mut engine = make_engine();
    let mut scratch_seqs: Vec<u64> = Vec::with_capacity(CLAIM_BATCH);

    let mut last_fed: u64 = 0;
    let mut drained = 0usize;
    let mut cycles = 0usize;
    let mut feed_total = std::time::Duration::ZERO;
    let mut pop_total = std::time::Duration::ZERO;

    let t0 = Instant::now();
    loop {
        // Feed up to MAX_FEED_PER_CYCLE
        let t_feed = Instant::now();
        let new_last = feed_engine_capped(store.as_ref(), &mut engine, last_fed);
        feed_total += t_feed.elapsed();
        let nothing_fed = new_last == last_fed;
        last_fed = new_last;

        // Pop + read
        let t_pop = Instant::now();
        loop {
            let count = pop_batch(&mut engine, &mut scratch_seqs);
            if count == 0 { break; }
            let first = scratch_seqs[0];
            let last = scratch_seqs[count - 1];
            store.for_each(first, last + 1, &mut |entry| {
                assert!(!entry.subject.is_empty());
                assert_eq!(entry.payload.len(), PAYLOAD_SIZE);
            }).unwrap();
            drained += count;
        }
        pop_total += t_pop.elapsed();

        cycles += 1;
        if nothing_fed { break; }
    }
    let total_elapsed = t0.elapsed();

    assert_eq!(drained, total_msgs);
    println!(
        "    feed (capped {})      {} ({} msg/s)",
        MAX_FEED_PER_CYCLE, fmt_ms(feed_total), fmt_rate(total_msgs, feed_total),
    );
    println!(
        "    pop + store.for_each    {} ({} msg/s)",
        fmt_ms(pop_total), fmt_rate(total_msgs, pop_total),
    );
    println!(
        "    total ({} cycles)    {} ({} msg/s)",
        cycles, fmt_ms(total_elapsed), fmt_rate(total_msgs, total_elapsed),
    );
}

// ─── Layer 3: + frame build (RepBatch + envelope + freeze) ──────────────────

fn layer3_frame_build(total_msgs: usize) {
    let store = make_store(total_msgs);
    let mut engine = make_engine();
    let stream_id = StreamId(1);
    let consumer_id = ConsumerId(1);

    let mut scratch_seqs: Vec<u64> = Vec::with_capacity(CLAIM_BATCH);
    let mut scratch_body = BytesMut::with_capacity(
        8 + CLAIM_BATCH * (std::mem::size_of::<DeliveryEntryHeader>() + 19 + PAYLOAD_SIZE),
    );
    let mut last_fed: u64 = 0;
    let mut drained = 0usize;
    let mut total_frames = 0usize;
    let mut total_bytes = 0usize;

    let t0 = Instant::now();
    loop {
        let new_last = feed_engine_capped(store.as_ref(), &mut engine, last_fed);
        let nothing_fed = new_last == last_fed;
        last_fed = new_last;

        loop {
            let count = pop_batch(&mut engine, &mut scratch_seqs);
            if count == 0 { break; }

            read_batch_into_body(store.as_ref(), &scratch_seqs, consumer_id, &mut scratch_body);
            let frame = build_frame(stream_id, &scratch_body);

            total_bytes += frame.len();
            total_frames += 1;
            drained += count;
        }

        if nothing_fed { break; }
    }
    let elapsed = t0.elapsed();

    assert_eq!(drained, total_msgs);
    let mb = total_bytes as f64 / elapsed.as_secs_f64() / 1_048_576.0;
    println!(
        "    feed+pop+frame+freeze   {} ({} msg/s, {:.0} MB/s, {} frames)",
        fmt_ms(elapsed), fmt_rate(total_msgs, elapsed), mb, total_frames,
    );
}

// ─── Layer 4: + channel (crossbeam bounded send/recv) ───────────────────────

fn layer4_channel(total_msgs: usize) {
    let store = make_store(total_msgs);
    let mut engine = make_engine();
    let stream_id = StreamId(1);
    let consumer_id = ConsumerId(1);

    let (tx, rx) = crossbeam_channel::bounded::<bytes::Bytes>(65536);

    let consumer = std::thread::spawn(move || {
        let mut total_received = 0usize;
        while let Ok(frame) = rx.recv() {
            if frame.len() < ENVELOPE_SIZE + 8 { break; }
            let body = &frame[ENVELOPE_SIZE..];
            let count = u16::from_le_bytes([body[4], body[5]]) as usize;
            total_received += count;
        }
        total_received
    });

    let mut scratch_seqs: Vec<u64> = Vec::with_capacity(CLAIM_BATCH);
    let mut scratch_body = BytesMut::with_capacity(
        8 + CLAIM_BATCH * (std::mem::size_of::<DeliveryEntryHeader>() + 19 + PAYLOAD_SIZE),
    );
    let mut last_fed: u64 = 0;
    let mut drained = 0usize;

    let t0 = Instant::now();
    loop {
        let new_last = feed_engine_capped(store.as_ref(), &mut engine, last_fed);
        let nothing_fed = new_last == last_fed;
        last_fed = new_last;

        loop {
            let count = pop_batch(&mut engine, &mut scratch_seqs);
            if count == 0 { break; }

            read_batch_into_body(store.as_ref(), &scratch_seqs, consumer_id, &mut scratch_body);
            let frame = build_frame(stream_id, &scratch_body);
            tx.send(frame).unwrap();
            drained += count;
        }

        if nothing_fed { break; }
    }
    drop(tx);
    let received = consumer.join().unwrap();
    let elapsed = t0.elapsed();

    assert_eq!(drained, total_msgs);
    assert_eq!(received, total_msgs);
    println!(
        "    + crossbeam send/recv   {} ({} msg/s)",
        fmt_ms(elapsed), fmt_rate(total_msgs, elapsed),
    );
}

// ─── Layer 1T: TolerantStore comparison ─────────────────────────────────────

fn layer1_tolerant(total_msgs: usize) {
    let tmp = std::env::temp_dir().join("arbitro_bench_tolerant");
    let _ = std::fs::remove_dir_all(&tmp);
    let _ = std::fs::create_dir_all(&tmp);

    let mut store: Box<dyn Store> = Box::new(TolerantStore::new(tmp.clone()));
    store.init().unwrap();

    let subject = b"bench.drain.subject";
    let payload = vec![0x42u8; PAYLOAD_SIZE];
    for _ in 0..total_msgs {
        store.append(EntryRef { subject, payload: &payload }, 0).unwrap();
    }
    let info = store.info();
    assert_eq!(info.messages, total_msgs as u64, "tolerant: message count mismatch");
    assert_eq!(info.first_seq, 1, "tolerant: first_seq should be 1");
    assert_eq!(info.last_seq, total_msgs as u64, "tolerant: last_seq mismatch");

    // Verify seed_index matches for_each data
    {
        let seeds = store.seed_index(1, info.last_seq + 1);
        assert_eq!(seeds.len(), total_msgs, "tolerant: seed_index length mismatch");
        let expected_hash = fnv1a_32(b"bench.drain.subject");
        for (i, s) in seeds.iter().enumerate() {
            assert_eq!(s.seq.get(), (i + 1) as u64, "tolerant: seq mismatch at {}", i);
            assert_eq!(s.subject_hash.get(), expected_hash, "tolerant: hash mismatch at {}", i);
        }
    }

    // A. for_each (validates data integrity)
    {
        let mut count = 0usize;
        let t0 = Instant::now();
        store.for_each(1, info.last_seq + 1, &mut |entry| {
            assert_eq!(entry.subject, b"bench.drain.subject", "tolerant: subject corrupted at seq {}", entry.seq);
            assert_eq!(entry.payload.len(), PAYLOAD_SIZE, "tolerant: payload len wrong at seq {}", entry.seq);
            count += 1;
        }).unwrap();
        let elapsed = t0.elapsed();
        assert_eq!(count, total_msgs);
        println!(
            "    for_each                {} ({} msg/s)",
            fmt_ms(elapsed), fmt_rate(total_msgs, elapsed),
        );
    }

    // B. seed_index all
    {
        let t0 = Instant::now();
        let seeds = store.seed_index(1, info.last_seq + 1);
        let slice_elapsed = t0.elapsed();
        assert_eq!(seeds.len(), total_msgs);

        let t_iter = Instant::now();
        let mut accum = 0u64;
        for s in seeds {
            accum = accum.wrapping_add(s.seq.get());
            std::hint::black_box(s.subject_hash.get());
        }
        let iter_elapsed = t_iter.elapsed();
        std::hint::black_box(accum);
        let total_elapsed = slice_elapsed + iter_elapsed;
        println!(
            "    seed_index all           {} ({} msg/s)  [slice: {}, iter: {}]",
            fmt_ms(total_elapsed), fmt_rate(total_msgs, total_elapsed),
            fmt_ns(slice_elapsed), fmt_ms(iter_elapsed),
        );
    }

    // C. seed_index capped 256
    {
        let cap = MAX_FEED_PER_CYCLE;
        let mut total_fed = 0usize;
        let t0 = Instant::now();
        let mut start = 1u64;
        let last = info.last_seq;
        while start <= last {
            let end = (start + cap).min(last + 1);
            let seeds = store.seed_index(start, end);
            for s in seeds {
                std::hint::black_box(s.seq.get());
                std::hint::black_box(s.subject_hash.get());
            }
            total_fed += seeds.len();
            start = end;
        }
        let elapsed = t0.elapsed();
        assert_eq!(total_fed, total_msgs);
        println!(
            "    seed_index capped 256    {} ({} msg/s)",
            fmt_ms(elapsed), fmt_rate(total_msgs, elapsed),
        );
    }

    drop(store);
    let _ = std::fs::remove_dir_all(&tmp);
}

// ─── Main ───────────────────────────────────────────────────────────────────

fn main() {
    println!("Incremental drain bench — one layer at a time");
    println!("payload={}B, batch={}, max_feed_per_cycle={}", PAYLOAD_SIZE, CLAIM_BATCH, MAX_FEED_PER_CYCLE);
    println!("{}", "=".repeat(80));

    for &msgs in &[10_000, 100_000, 500_000, 1_000_000, 5_000_000] {
        println!("\n--- {}k messages ---", msgs / 1000);

        let store = make_store(msgs);

        println!("  Layer 0: primitives");
        layer0_store_info(store.as_ref(), msgs);
        layer0_fnv(msgs);

        println!("  Layer 1: get_messages");
        layer1_get_messages(msgs);

        println!("  Layer 1T: TolerantStore");
        layer1_tolerant(msgs);

        println!("  Layer 2: + ready queue");
        layer2_ready_queue(msgs);

        println!("  Layer 3: + frame build");
        layer3_frame_build(msgs);

        println!("  Layer 4: + channel");
        layer4_channel(msgs);
    }
}
