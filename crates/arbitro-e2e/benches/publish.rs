//! Benchmark: raw publish + receive throughput across journal kinds.
//!
//! Two pipelines measured end-to-end per journal kind:
//!
//! **publish** — producer side:
//!   client.encode → TCP send → server recv → decode → store.append → PubAck
//!
//! **receive** — consumer side (replay from a prefilled store):
//!   drain(for_each) → group batch → TCP send → client recv → decode
//!
//! Runs six configurations total:
//!   - memory   / single  (publish)
//!   - memory   / batch   (publish)
//!   - memory   / receive (drain after publish)
//!   - tolerant / single  (publish)
//!   - tolerant / batch   (publish)
//!   - tolerant / receive (drain after publish)
//!
//! Safety:
//!   - Tolerant data directory is created under a unique /tmp path.
//!   - At teardown we verify the directory contains non-empty files
//!     (proof that the tolerant journal actually wrote to disk).
//!   - Only if the verification passes do we remove the directory.
//!     If it's empty something is wrong — we leave it for inspection
//!     and exit with a non-zero status.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use arbitro_client::Client;
use arbitro_proto::action::Action;
use arbitro_proto::config::{JournalKind, StreamConfig};
use arbitro_proto::wire::delivery::{
    DeliveryEntryHeader, RepBatchFixed, DELIVERY_ENTRY_HEADER_SIZE, REP_BATCH_FIXED_SIZE,
};
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_server::{ArbitroServer, Config};
use arbitro_store::{EntryRef, MemoryStore, Store, TolerantStore};
use bytes::{Bytes, BytesMut};
use std::io::IoSlice;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Total messages per run. Kept well below the 1000-msg smoke-test cap in
/// `.agent/rules/testing.md` unless the caller overrides via env var.
const DEFAULT_TOTAL_MSGS: u64 = 1000;
/// Batch size for the batch variant.
const DEFAULT_BATCH_SIZE: usize = 100;

fn env_u64(var: &str, fallback: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(fallback)
}

fn env_usize(var: &str, fallback: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(fallback)
}

fn portpicker() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Create a server with the given optional data directory. The server is
/// spawned on its own task and given 100 ms to bind.
async fn spawn_server(data_dir: Option<&Path>, shard_count: usize) -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let mut config = Config::default()
        .listen_addr(addr.clone())
        .max_connections(16)
        .shard_count(shard_count)
        .write_buffer_cap(1024 * 1024);
    if let Some(dir) = data_dir {
        config = config.data_dir(dir.to_string_lossy().into_owned());
    }
    let server = ArbitroServer::new(config);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    addr
}

/// Unique per-run temp dir for the tolerant journal under /tmp.
fn make_tolerant_dir(tag: &str) -> PathBuf {
    let mut base = std::env::temp_dir();
    let unique = format!(
        "arbitro-bench-publish-{}-{}",
        tag,
        std::process::id()
    );
    base.push(unique);
    std::fs::create_dir_all(&base).expect("create tolerant dir");
    base
}

/// Walk `dir` and return (file_count, total_bytes).
fn dir_stats(dir: &Path) -> (usize, u64) {
    let mut files = 0usize;
    let mut bytes = 0u64;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let (f, b) = dir_stats(&path);
                files += f;
                bytes += b;
            } else if let Ok(meta) = entry.metadata() {
                files += 1;
                bytes += meta.len();
            }
        }
    }
    (files, bytes)
}

/// Print a tree listing with file sizes — debug helper.
fn print_dir_tree(dir: &Path, indent: usize) {
    if let Ok(rd) = std::fs::read_dir(dir) {
        let mut entries: Vec<_> = rd.flatten().collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            let name = entry.file_name();
            let prefix = " ".repeat(indent * 2);
            if path.is_dir() {
                println!("{prefix}{}/", name.to_string_lossy());
                print_dir_tree(&path, indent + 1);
            } else if let Ok(meta) = entry.metadata() {
                let size = meta.len();
                let size_str = if size >= 1024 * 1024 {
                    format!("{:.1} MB", size as f64 / (1024.0 * 1024.0))
                } else if size >= 1024 {
                    format!("{:.1} KB", size as f64 / 1024.0)
                } else {
                    format!("{size} B")
                };
                println!("{prefix}{} [{size_str}]", name.to_string_lossy());
            }
        }
    }
}

/// Verify the tolerant journal wrote non-empty data to `dir`, then remove
/// the dir. Returns `Ok(())` on success; `Err(..)` if the dir is empty or
/// the removal failed.
fn verify_and_cleanup_tolerant(dir: &Path) -> Result<(usize, u64), String> {
    let (files, bytes) = dir_stats(dir);
    if files == 0 || bytes == 0 {
        return Err(format!(
            "tolerant journal at {dir:?} is empty (files={files}, bytes={bytes}) — \
             left for inspection"
        ));
    }
    std::fs::remove_dir_all(dir)
        .map_err(|e| format!("remove_dir_all({dir:?}) failed: {e}"))?;
    Ok((files, bytes))
}

struct RunResult {
    label: String,
    mode: &'static str,
    total_msgs: u64,
    elapsed: Duration,
}

impl RunResult {
    fn throughput(&self) -> f64 {
        self.total_msgs as f64 / self.elapsed.as_secs_f64()
    }
}

/// Publish `total_msgs` one by one. Each publish is a fresh await → one
/// request/response per message.
async fn run_publish_single(
    addr: &str,
    stream_name: &[u8],
    total_msgs: u64,
    payload: &[u8],
) -> Duration {
    let client = Client::connect(addr).await.unwrap();
    let subject: &[u8] = b"bench.publish.single";
    let start = Instant::now();
    for _ in 0..total_msgs {
        client.publish(stream_name, subject, payload).await.unwrap();
    }
    start.elapsed()
}

/// Publish `total_msgs` in chunks of `batch_size`. Each call is a single
/// request carrying `batch_size` entries.
async fn run_publish_batch(
    addr: &str,
    stream_name: &[u8],
    total_msgs: u64,
    batch_size: usize,
    payload: &[u8],
) -> Duration {
    let client = Client::connect(addr).await.unwrap();
    let subject: &[u8] = b"bench.publish.batch";
    let batches = total_msgs / batch_size as u64;
    let start = Instant::now();
    for _ in 0..batches {
        let entries: Vec<(&[u8], &[u8])> =
            (0..batch_size).map(|_| (subject, payload)).collect();
        client.publish_batch(stream_name, &entries).await.unwrap();
    }
    start.elapsed()
}

/// Result of the drain-pipeline simulation.
struct PipelineStats {
    elapsed: Duration,
    decoded: u64,
    frames_sent: usize,
}

/// Per-stage timings for the drain pipeline, isolated.
struct StageStats {
    fetch: Duration,
    encode: Duration, // differential: fetch_encode - fetch
    send: Duration,
    recv: Duration,
    decode: Duration,
    // Zero-copy (scatter-gather) variant for encode+send.
    encode_zc: Duration,
    send_zc: Duration,
    recv_zc: Duration,
    decode_zc: Duration,
    // In-place zerocopy-view variant (fixed-size PackedEntry).
    encode_view: Duration,
    frames_count: usize,
    decoded_count: u64,
    recv_bytes: u64,
}

/// Per-entry owned scatter-gather slices. Header is copied into a small
/// stack-sized array (permitted copy #3: "header on stack → write buffer").
/// Subject/payload are `Bytes` clones — Arc bump, no copy.
struct EntryIov {
    header: [u8; DELIVERY_ENTRY_HEADER_SIZE],
    subject: Bytes,
    payload: Bytes,
}

/// Build a fresh store of the requested kind, pre-filled with `total_msgs`
/// entries (each with the same subject + payload).
fn build_prefilled_store(
    journal_kind: JournalKind,
    data_dir: Option<&Path>,
    total_msgs: u64,
    subject: &[u8],
    payload: &[u8],
) -> Box<dyn Store> {
    // Pre-alloc the memory arena to the exact needed size (same footprint
    // concept as tolerant's 64 MB mmap) so the arena doesn't realloc during
    // pre-fill. This matches tolerant's "one large up-front allocation".
    let data_cap = (total_msgs as usize) * (subject.len() + payload.len());
    let index_cap = total_msgs as usize;
    let mut store: Box<dyn Store> = match journal_kind {
        JournalKind::Memory => Box::new(MemoryStore::with_capacity(data_cap, index_cap)),
        JournalKind::Tolerant => {
            let dir = data_dir.expect("tolerant requires data_dir");
            let mut s = TolerantStore::new(dir.to_path_buf());
            s.init().expect("tolerant init");
            Box::new(s)
        }
        _ => panic!("unsupported journal kind"),
    };

    // Pre-fill in chunks to exercise `append_batch`.
    const APPEND_CHUNK: u64 = 100;
    let entry = EntryRef {
        stream_id: 1,
        subject,
        payload,
        flags: 0,
    };
    let full_batches = total_msgs / APPEND_CHUNK;
    let remainder = (total_msgs % APPEND_CHUNK) as usize;
    let chunk: Vec<EntryRef<'_>> = (0..APPEND_CHUNK).map(|_| entry).collect();
    for _ in 0..full_batches {
        store.append_batch(&chunk, 0).expect("append_batch");
    }
    if remainder > 0 {
        let tail: Vec<EntryRef<'_>> = (0..remainder).map(|_| entry).collect();
        store.append_batch(&tail, 0).expect("append_batch tail");
    }
    store
}

/// Simulate the drain pipeline:
///   store.for_each (batches of `max_feed`) → encode RepBatch frame →
///   mpsc channel → TCP writer → TCP reader → decode → count
///
/// Returns elapsed time measured from the first `for_each` start to the
/// moment the decoder has counted `total_msgs` entries.
async fn run_drain_pipeline(
    journal_kind: JournalKind,
    data_dir: Option<&Path>,
    total_msgs: u64,
    max_feed: usize,
    payload: &[u8],
) -> PipelineStats {
    let subject: &[u8] = b"bench.pipeline";
    let store = build_prefilled_store(journal_kind, data_dir, total_msgs, subject, payload);
    let last_seq = store.info().last_seq;

    // ── TCP loopback pair ────────────────────────────────────────────────
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client_fut = TcpStream::connect(addr);
    let server_fut = async { listener.accept().await.unwrap().0 };
    let (client_res, server_stream) = tokio::join!(client_fut, server_fut);
    let mut client_stream = client_res.unwrap();
    let mut server_stream = server_stream;

    // ── mpsc channel: drain task → tcp writer task ───────────────────────
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(8);

    // ── Reader task (consumer side): read frames, decode, count ──────────
    let decoded = Arc::new(AtomicU64::new(0));
    let rc = decoded.clone();
    let reader_handle = tokio::spawn(async move {
        let mut env_buf = [0u8; ENVELOPE_SIZE];
        loop {
            if client_stream.read_exact(&mut env_buf).await.is_err() {
                break;
            }
            let envelope = Envelope::ref_from_bytes(&env_buf).unwrap();
            let body_len = envelope.msg_len.get() as usize;
            let mut body = vec![0u8; body_len];
            if client_stream.read_exact(&mut body).await.is_err() {
                break;
            }
            // Parse RepBatchFixed (count at the front of body).
            let fixed = RepBatchFixed::ref_from_bytes(&body[..REP_BATCH_FIXED_SIZE]).unwrap();
            let count = fixed.count.get() as usize;

            // Iterate entries.
            let mut off = REP_BATCH_FIXED_SIZE;
            for _ in 0..count {
                let hdr =
                    DeliveryEntryHeader::ref_from_bytes(&body[off..off + DELIVERY_ENTRY_HEADER_SIZE])
                        .unwrap();
                let data_len = hdr.data_len.get() as usize;
                off += DELIVERY_ENTRY_HEADER_SIZE + data_len;
                rc.fetch_add(1, Relaxed);
            }
        }
    });

    // ── TCP writer task (server side): channel → TCP ─────────────────────
    let writer_handle = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if server_stream.write_all(&frame).await.is_err() {
                break;
            }
        }
        let _ = server_stream.shutdown().await;
    });

    // ── Drain task (inline on this task — measures end-to-end) ───────────
    let start = Instant::now();
    let mut cursor = 0u64;
    let mut frames_sent = 0usize;

    while cursor < last_seq {
        let from = cursor + 1;
        let to = (from + max_feed as u64).min(last_seq + 1);

        let mut body = BytesMut::with_capacity(64 * 1024);
        body.extend_from_slice(&[0u8; ENVELOPE_SIZE]);
        body.extend_from_slice(
            RepBatchFixed {
                count: U16::new(0),
                _pad: U16::new(0),
            }
            .as_bytes(),
        );
        let mut count: u16 = 0;

        store
            .for_each(from, to, &mut |entry| {
                let subj_len = entry.subject.len();
                let data_len = subj_len + entry.payload.len();
                body.extend_from_slice(
                    DeliveryEntryHeader {
                        consumer_id: U32::new(1),
                        seq: U64::new(entry.seq),
                        subj_len: U16::new(subj_len as u16),
                        data_len: U32::new(data_len as u32),
                        subject_hash: U32::new(0),
                    }
                    .as_bytes(),
                );
                body.extend_from_slice(entry.subject);
                body.extend_from_slice(entry.payload);
                count += 1;
            })
            .unwrap();

        if count == 0 {
            break;
        }

        // Patch RepBatchFixed count (right after envelope placeholder).
        let count_offset = ENVELOPE_SIZE;
        body[count_offset..count_offset + 2].copy_from_slice(&count.to_le_bytes());

        // Patch envelope.
        let body_len = body.len() - ENVELOPE_SIZE;
        let envelope = Envelope::new(Action::RepBatch, 1, body_len as u32, 0);
        body[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());

        let frame = body.freeze();
        tx.send(frame).await.expect("channel send");
        frames_sent += 1;
        cursor = to - 1;
    }
    drop(tx); // signal writer task to finish

    // Wait for decoder to observe all entries.
    loop {
        if decoded.load(Relaxed) >= total_msgs {
            break;
        }
        if start.elapsed() > Duration::from_secs(10) {
            panic!(
                "pipeline timeout: decoded {} / {total_msgs}",
                decoded.load(Relaxed)
            );
        }
        tokio::task::yield_now().await;
    }
    let elapsed = start.elapsed();
    let final_count = decoded.load(Relaxed);

    // Cleanup tasks.
    writer_handle.abort();
    reader_handle.abort();

    PipelineStats {
        elapsed,
        decoded: final_count,
        frames_sent,
    }
}

// ── Packed entry for fixed-size encoding ────────────────────────────────────
//
// subject_len = 14 ("bench.stages"/"bench.pipeline"), payload_len = 64.
// Using a fixed-size zerocopy struct lets us obtain a `&mut [PackedEntry]`
// view over the destination buffer and write fields in place — no intermediate
// Vec, no final `extend_from_slice` copy. Only the variable data (subject,
// payload) is memcpy'd once, directly to its final location in the wire
// buffer. Header fields are written directly via the zerocopy view (no copy).

const PACKED_SUBJ_LEN: usize = 14;
const PACKED_PAYLOAD_LEN: usize = 64;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct PackedEntry {
    header: DeliveryEntryHeader,
    subject: [u8; PACKED_SUBJ_LEN],
    payload: [u8; PACKED_PAYLOAD_LEN],
}
const PACKED_ENTRY_SIZE: usize = core::mem::size_of::<PackedEntry>();
const _: () = assert!(
    PACKED_ENTRY_SIZE == DELIVERY_ENTRY_HEADER_SIZE + PACKED_SUBJ_LEN + PACKED_PAYLOAD_LEN
);

/// In-place zerocopy encode: allocate the final buffer once, obtain mutable
/// views of `&mut Envelope`, `&mut RepBatchFixed`, and `&mut [PackedEntry]`
/// via zerocopy, then write fields directly. The only memcpys are the
/// variable-length `subject` and `payload` going to their final wire slot
/// (2 memcpys per entry instead of 3, and header fields are stores, not
/// memcpys). This is what `zerocopy` is actually for: treat the destination
/// buffer AS the struct layout, no intermediate materialization.
///
/// Called directly from within `store.for_each` callback so the borrowed
/// `entry.subject` / `entry.payload` never escape — they're copied to
/// their final wire slot in the same lifetime scope.
fn build_frame_zerocopy_view_from_store(
    store: &dyn Store,
    from: u64,
    to: u64,
) -> (Bytes, usize) {
    let capacity_hint = (to - from) as usize;
    let body_size = REP_BATCH_FIXED_SIZE + capacity_hint * PACKED_ENTRY_SIZE;
    let total = ENVELOPE_SIZE + body_size;
    let mut buf = vec![0u8; total];

    let mut off = ENVELOPE_SIZE + REP_BATCH_FIXED_SIZE;
    let mut count: u16 = 0;
    store
        .for_each(from, to, &mut |entry| {
            // In-place mutable view of the next PackedEntry slot.
            let slot_bytes = &mut buf[off..off + PACKED_ENTRY_SIZE];
            let slot = PackedEntry::mut_from_bytes(slot_bytes).unwrap();
            // Direct field stores — no memcpy for the header.
            slot.header.consumer_id = U32::new(1);
            slot.header.seq = U64::new(entry.seq);
            slot.header.subj_len = U16::new(PACKED_SUBJ_LEN as u16);
            slot.header.data_len =
                U32::new((PACKED_SUBJ_LEN + PACKED_PAYLOAD_LEN) as u32);
            slot.header.subject_hash = U32::new(0);
            // Only two memcpys: subject and payload, directly to their
            // final wire location (no stack/intermediate buffer).
            slot.subject.copy_from_slice(entry.subject);
            slot.payload.copy_from_slice(entry.payload);
            off += PACKED_ENTRY_SIZE;
            count += 1;
        })
        .unwrap();

    // Patch envelope + RepBatchFixed with real count/body_len.
    let real_body_size = REP_BATCH_FIXED_SIZE + (count as usize) * PACKED_ENTRY_SIZE;
    let real_total = ENVELOPE_SIZE + real_body_size;
    buf.truncate(real_total);

    let (env_bytes, rest) = buf.split_at_mut(ENVELOPE_SIZE);
    let (fixed_bytes, _) = rest.split_at_mut(REP_BATCH_FIXED_SIZE);
    let env = Envelope::mut_from_bytes(env_bytes).unwrap();
    env.action = U16::new(Action::RepBatch.as_u16());
    env.flags = 0;
    env._rsv = 0;
    env.stream_id = U32::new(1);
    env.msg_len = U32::new(real_body_size as u32);
    env.env_seq = U32::new(0);
    let fixed = RepBatchFixed::mut_from_bytes(fixed_bytes).unwrap();
    fixed.count = U16::new(count);
    fixed._pad = U16::new(0);

    (Bytes::from(buf), count as usize)
}

/// Snapshot the store as owned `Bytes` per entry (Arc-backed).
/// This lives OUTSIDE any measured stage — it simulates a Store that would
/// expose `Bytes` natively. In the current Store trait, the arena is only
/// borrowed as `&[u8]` so this one-time copy is the cost of the adapter.
/// Refactoring Store to expose `Bytes` is tracked as a separate task.
fn snapshot_store_as_bytes(store: &dyn Store) -> Vec<(u64, Bytes, Bytes)> {
    let info = store.info();
    let mut out = Vec::with_capacity(info.messages as usize);
    store
        .for_each(info.first_seq, info.last_seq + 1, &mut |entry| {
            out.push((
                entry.seq,
                Bytes::copy_from_slice(entry.subject),
                Bytes::copy_from_slice(entry.payload),
            ));
        })
        .unwrap();
    out
}

/// Build a scatter-gather frame for one batch of up to `max_feed` entries.
/// Returns `(prefix, iovs)` where:
///   - `prefix` is the envelope + RepBatchFixed as one owned Vec
///     (copy #3, ENVELOPE_SIZE + REP_BATCH_FIXED_SIZE = 20 B total).
///   - `iovs` contains per-entry header (copy #3, 22 B) + Bytes slices
///     (Arc bump, zero copy) for subject and payload.
fn build_scatter_frame(
    snap: &[(u64, Bytes, Bytes)],
    start: usize,
    end: usize,
) -> (Vec<u8>, Vec<EntryIov>) {
    let count = end - start;
    let mut prefix = vec![0u8; ENVELOPE_SIZE + REP_BATCH_FIXED_SIZE];
    prefix[ENVELOPE_SIZE..].copy_from_slice(
        RepBatchFixed {
            count: U16::new(count as u16),
            _pad: U16::new(0),
        }
        .as_bytes(),
    );

    let mut iovs: Vec<EntryIov> = Vec::with_capacity(count);
    let mut total_body = REP_BATCH_FIXED_SIZE;
    for (seq, subj, pld) in &snap[start..end] {
        let subj_len = subj.len();
        let data_len = subj_len + pld.len();
        let hdr = DeliveryEntryHeader {
            consumer_id: U32::new(1),
            seq: U64::new(*seq),
            subj_len: U16::new(subj_len as u16),
            data_len: U32::new(data_len as u32),
            subject_hash: U32::new(0),
        };
        let mut header = [0u8; DELIVERY_ENTRY_HEADER_SIZE];
        header.copy_from_slice(hdr.as_bytes());
        iovs.push(EntryIov {
            header,
            subject: subj.clone(), // Arc bump
            payload: pld.clone(),  // Arc bump
        });
        total_body += DELIVERY_ENTRY_HEADER_SIZE + data_len;
    }

    let envelope = Envelope::new(Action::RepBatch, 1, total_body as u32, 0);
    prefix[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());

    (prefix, iovs)
}

/// Send a scatter-gather frame using `write_vectored`. Loops until all
/// bytes are written (handles partial writes).
async fn send_scatter_frame(
    stream: &mut TcpStream,
    prefix: &[u8],
    iovs: &[EntryIov],
) -> std::io::Result<()> {
    // Build the full IoSlice list once.
    // Layout: [prefix], then per entry: [header][subject][payload].
    let mut slices: Vec<IoSlice> = Vec::with_capacity(1 + iovs.len() * 3);
    slices.push(IoSlice::new(prefix));
    for iov in iovs {
        slices.push(IoSlice::new(&iov.header));
        slices.push(IoSlice::new(&iov.subject));
        slices.push(IoSlice::new(&iov.payload));
    }

    // Write loop — `write_vectored` may write partial bytes. Advance on each call.
    let total: usize = slices.iter().map(|s| s.len()).sum();
    let mut written = 0usize;
    let mut idx = 0usize;
    let mut offset_in_slice = 0usize;

    while written < total {
        // Build a temporary slice view starting at `idx` with `offset_in_slice`
        // applied to the first entry. For simplicity, write_vectored a contiguous
        // tail of `slices`, then advance.
        //
        // Optimisation potential: use `std::io::IoSlice::advance_slices` (nightly).
        // Here we use the simple approach: build an adjusted vec on partial writes.
        let n = if offset_in_slice == 0 {
            stream.write_vectored(&slices[idx..]).await?
        } else {
            // First slice partial — slice it down.
            let first = &slices[idx];
            let first_remaining = IoSlice::new(&first.as_ref()[offset_in_slice..]);
            let mut tmp: Vec<IoSlice> = Vec::with_capacity(slices.len() - idx);
            tmp.push(first_remaining);
            for s in &slices[idx + 1..] {
                tmp.push(IoSlice::new(s.as_ref()));
            }
            stream.write_vectored(&tmp).await?
        };
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write_vectored returned 0",
            ));
        }
        written += n;

        // Advance (idx, offset_in_slice) by `n`.
        let mut remaining = n;
        while remaining > 0 && idx < slices.len() {
            let available = slices[idx].len() - offset_in_slice;
            if remaining < available {
                offset_in_slice += remaining;
                remaining = 0;
            } else {
                remaining -= available;
                idx += 1;
                offset_in_slice = 0;
            }
        }
    }
    Ok(())
}

/// Build one RepBatch frame from `from..to` of `store`. Mirrors drain's encode.
fn build_frame(store: &dyn Store, from: u64, to: u64) -> (Bytes, usize) {
    let mut body = BytesMut::with_capacity(64 * 1024);
    body.extend_from_slice(&[0u8; ENVELOPE_SIZE]);
    body.extend_from_slice(
        RepBatchFixed {
            count: U16::new(0),
            _pad: U16::new(0),
        }
        .as_bytes(),
    );
    let mut count: u16 = 0;
    store
        .for_each(from, to, &mut |entry| {
            let subj_len = entry.subject.len();
            let data_len = subj_len + entry.payload.len();
            body.extend_from_slice(
                DeliveryEntryHeader {
                    consumer_id: U32::new(1),
                    seq: U64::new(entry.seq),
                    subj_len: U16::new(subj_len as u16),
                    data_len: U32::new(data_len as u32),
                    subject_hash: U32::new(0),
                }
                .as_bytes(),
            );
            body.extend_from_slice(entry.subject);
            body.extend_from_slice(entry.payload);
            count += 1;
        })
        .unwrap();
    if count == 0 {
        return (Bytes::new(), 0);
    }
    // Patch headers.
    let count_offset = ENVELOPE_SIZE;
    body[count_offset..count_offset + 2].copy_from_slice(&count.to_le_bytes());
    let body_len = body.len() - ENVELOPE_SIZE;
    let envelope = Envelope::new(Action::RepBatch, 1, body_len as u32, 0);
    body[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());
    (body.freeze(), count as usize)
}

/// Measure each pipeline stage in isolation:
///   fetch → encode → send → recv → decode
/// All stages act on the same `total_msgs` entries; frames built in the
/// encode stage are reused for send/recv/decode so no redundant work is
/// timed twice. Differential isolation is used for encode:
///   encode_pure = (fetch + encode) − fetch
async fn run_stage_measurements(
    journal_kind: JournalKind,
    data_dir: Option<&Path>,
    total_msgs: u64,
    max_feed: usize,
    payload: &[u8],
) -> StageStats {
    let subject: &[u8] = b"bench.pipeline"; // 14 bytes — matches PACKED_SUBJ_LEN
    let store = build_prefilled_store(journal_kind, data_dir, total_msgs, subject, payload);
    let last_seq = store.info().last_seq;

    // ── Stage 1: fetch — for_each with noop callback ────────────────────
    let t0 = Instant::now();
    let mut fetched = 0u64;
    store
        .for_each(1, last_seq + 1, &mut |entry| {
            // Touch the fields so the compiler can't elide the walk.
            std::hint::black_box(entry.seq);
            std::hint::black_box(entry.subject.as_ptr());
            std::hint::black_box(entry.payload.as_ptr());
            fetched += 1;
        })
        .unwrap();
    let fetch_elapsed = t0.elapsed();
    assert_eq!(fetched, total_msgs, "fetch count mismatch");

    // ── Stage 2: fetch+encode — build frames in BytesMut ────────────────
    let t1 = Instant::now();
    let mut frames: Vec<Bytes> = Vec::new();
    let mut cursor = 0u64;
    while cursor < last_seq {
        let from = cursor + 1;
        let to = (from + max_feed as u64).min(last_seq + 1);
        let (frame, count) = build_frame(&*store, from, to);
        if count == 0 {
            break;
        }
        frames.push(frame);
        cursor = to - 1;
    }
    let fetch_encode_elapsed = t1.elapsed();
    let encode_pure = fetch_encode_elapsed.saturating_sub(fetch_elapsed);

    // Total bytes that will travel the wire.
    let total_bytes: u64 = frames.iter().map(|f| f.len() as u64).sum();

    // ── TCP loopback for stages 3 & 4 ────────────────────────────────────
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client_fut = TcpStream::connect(addr);
    let server_fut = async { listener.accept().await.unwrap().0 };
    let (client_res, server_stream) = tokio::join!(client_fut, server_fut);
    let mut client_stream = client_res.unwrap();
    let mut server_stream = server_stream;

    // Spawn the receiver FIRST so the send side can stream through the
    // kernel buffer without stalling on a full SO_RCVBUF.
    let recv_bytes_target = total_bytes;
    let recv_handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        let mut received = 0u64;
        // Pre-alloc — we know the total size ahead of time.
        let mut raw = Vec::<u8>::with_capacity(recv_bytes_target as usize);
        let t_recv_start = Instant::now();
        while received < recv_bytes_target {
            let n = client_stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
            received += n as u64;
        }
        let recv_elapsed = t_recv_start.elapsed();
        (recv_elapsed, raw, received)
    });

    // ── Stage 3: send — write_all of all pre-built frames ───────────────
    let t_send_start = Instant::now();
    for frame in &frames {
        server_stream.write_all(frame).await.unwrap();
    }
    server_stream.flush().await.unwrap();
    let send_elapsed = t_send_start.elapsed();
    let _ = server_stream.shutdown().await;

    // Wait for the receiver.
    let (recv_elapsed, raw_bytes, recv_bytes) = recv_handle.await.unwrap();

    // ── Stage 5: decode — zerocopy parse of the raw bytes ───────────────
    let decoded = decode_raw(&raw_bytes);
    let t_decode_start = Instant::now();
    let _decoded2 = decode_raw(&raw_bytes); // re-time it for reliable µs
    let decode_elapsed = t_decode_start.elapsed();

    // ── Zero-copy variant (scatter-gather encode + write_vectored) ─────
    // Snapshot the store arena into owned Bytes (outside the timer) so the
    // encode stage does NOT copy subject/payload — only the per-entry
    // header is materialized on stack (copy #3, permitted).
    let snapshot = snapshot_store_as_bytes(&*store);

    // Encode-zc: build IoV frames for each batch, no subject/payload copy.
    let t_encode_zc = Instant::now();
    let mut zc_frames: Vec<(Vec<u8>, Vec<EntryIov>)> = Vec::new();
    let mut cursor_zc = 0usize;
    while cursor_zc < snapshot.len() {
        let end = (cursor_zc + max_feed).min(snapshot.len());
        let frame = build_scatter_frame(&snapshot, cursor_zc, end);
        zc_frames.push(frame);
        cursor_zc = end;
    }
    let encode_zc_elapsed = t_encode_zc.elapsed();

    // Fresh TCP pair for the zero-copy round.
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();
    let client_fut2 = TcpStream::connect(addr2);
    let server_fut2 = async { listener2.accept().await.unwrap().0 };
    let (client_res2, server_stream2) = tokio::join!(client_fut2, server_fut2);
    let mut client_stream2 = client_res2.unwrap();
    let mut server_stream2 = server_stream2;

    let recv_bytes_target2 = recv_bytes; // same total bytes expected
    let recv_handle2 = tokio::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        let mut received = 0u64;
        let mut raw = Vec::<u8>::with_capacity(recv_bytes_target2 as usize);
        let t_start = Instant::now();
        while received < recv_bytes_target2 {
            let n = client_stream2.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
            received += n as u64;
        }
        (t_start.elapsed(), raw, received)
    });

    // Send-zc: write_vectored per frame.
    let t_send_zc = Instant::now();
    for (prefix, iovs) in &zc_frames {
        send_scatter_frame(&mut server_stream2, prefix, iovs)
            .await
            .unwrap();
    }
    server_stream2.flush().await.unwrap();
    let send_zc_elapsed = t_send_zc.elapsed();
    let _ = server_stream2.shutdown().await;

    let (recv_zc_elapsed, raw_bytes_zc, _) = recv_handle2.await.unwrap();

    // Decode-zc: same parse logic (unchanged — verifies equivalence).
    let t_decode_zc = Instant::now();
    let decoded_zc = decode_raw(&raw_bytes_zc);
    let decode_zc_elapsed = t_decode_zc.elapsed();

    // ── In-place zerocopy-view encode (PackedEntry, fixed sizes) ────────
    // Walk the store and encode each batch directly into a pre-sized buffer
    // using a mutable `&mut PackedEntry` view. Header fields are written as
    // stores (no memcpy); only subject & payload are memcpy'd, directly to
    // their final wire slot (no intermediate stack/heap buffer).
    let t_encode_view = Instant::now();
    let mut view_frames: Vec<Bytes> = Vec::new();
    let mut cursor_v = 0u64;
    while cursor_v < last_seq {
        let from = cursor_v + 1;
        let to = (from + max_feed as u64).min(last_seq + 1);
        let (frame, count) = build_frame_zerocopy_view_from_store(&*store, from, to);
        if count == 0 {
            break;
        }
        view_frames.push(frame);
        cursor_v = to - 1;
    }
    let encode_view_elapsed = t_encode_view.elapsed();

    // Verify correctness: decode the resulting frames and ensure count matches.
    let mut view_raw: Vec<u8> = Vec::with_capacity(recv_bytes as usize);
    for f in &view_frames {
        view_raw.extend_from_slice(f);
    }
    let decoded_view = decode_raw(&view_raw);
    assert_eq!(
        decoded_view, total_msgs,
        "encode-view decode mismatch: got {decoded_view}, expected {total_msgs}"
    );

    assert_eq!(decoded, total_msgs, "decode count mismatch (copy path)");
    assert_eq!(
        decoded_zc, total_msgs,
        "decode count mismatch (scatter-gather path)"
    );

    StageStats {
        fetch: fetch_elapsed,
        encode: encode_pure,
        send: send_elapsed,
        recv: recv_elapsed,
        decode: decode_elapsed,
        encode_zc: encode_zc_elapsed,
        send_zc: send_zc_elapsed,
        recv_zc: recv_zc_elapsed,
        decode_zc: decode_zc_elapsed,
        encode_view: encode_view_elapsed,
        frames_count: frames.len(),
        decoded_count: decoded,
        recv_bytes,
    }
}

/// Parse a concatenation of RepBatch frames, return the total entry count.
fn decode_raw(raw: &[u8]) -> u64 {
    let mut decoded = 0u64;
    let mut off = 0usize;
    while off < raw.len() {
        let envelope = Envelope::ref_from_bytes(&raw[off..off + ENVELOPE_SIZE]).unwrap();
        let body_len = envelope.msg_len.get() as usize;
        off += ENVELOPE_SIZE;
        let body = &raw[off..off + body_len];
        let fixed = RepBatchFixed::ref_from_bytes(&body[..REP_BATCH_FIXED_SIZE]).unwrap();
        let count = fixed.count.get() as usize;
        let mut inner = REP_BATCH_FIXED_SIZE;
        for _ in 0..count {
            let hdr = DeliveryEntryHeader::ref_from_bytes(
                &body[inner..inner + DELIVERY_ENTRY_HEADER_SIZE],
            )
            .unwrap();
            let data_len = hdr.data_len.get() as usize;
            inner += DELIVERY_ENTRY_HEADER_SIZE + data_len;
            decoded += 1;
        }
        off += body_len;
    }
    decoded
}

/// Run all variants for one journal kind.
/// - `publish/single`  — end-to-end via ArbitroServer, one msg per await.
/// - `publish/batch`   — end-to-end via ArbitroServer, `batch_size` per call.
/// - `pipeline`        — simulated drain pipeline (no server), frame by frame.
async fn run_for_kind(
    journal_kind: JournalKind,
    label: &str,
    server_data_dir: Option<&Path>,
    pipeline_data_dir: Option<&Path>,
    stages_data_dir: Option<&Path>,
    shard_count: usize,
    total_msgs: u64,
    batch_size: usize,
    max_feed: usize,
    payload: &[u8],
) -> (Vec<RunResult>, PipelineStats, StageStats) {
    // ── publish (end-to-end via server) ─────────────────────────────────
    let addr = spawn_server(server_data_dir, shard_count).await;
    let setup = Client::connect(&addr).await.unwrap();

    let stream_pub: &[u8] = b"publish_bench";
    setup
        .create_stream(
            &StreamConfig::new(stream_pub, b">")
                .journal_kind(journal_kind)
                .build(),
        )
        .await
        .unwrap();

    let single_elapsed = run_publish_single(&addr, stream_pub, total_msgs, payload).await;
    let batch_elapsed =
        run_publish_batch(&addr, stream_pub, total_msgs, batch_size, payload).await;

    // ── stage microbenchmarks (no server) ───────────────────────────────
    let stages =
        run_stage_measurements(journal_kind, stages_data_dir, total_msgs, max_feed, payload)
            .await;

    // ── pipeline simulation (no server) ─────────────────────────────────
    let pipeline_stats =
        run_drain_pipeline(journal_kind, pipeline_data_dir, total_msgs, max_feed, payload)
            .await;

    let results = vec![
        RunResult {
            label: label.into(),
            mode: "publish/single",
            total_msgs,
            elapsed: single_elapsed,
        },
        RunResult {
            label: label.into(),
            mode: "publish/batch",
            total_msgs,
            elapsed: batch_elapsed,
        },
        RunResult {
            label: label.into(),
            mode: "pipeline/fetch",
            total_msgs,
            elapsed: stages.fetch,
        },
        RunResult {
            label: label.into(),
            mode: "pipeline/encode",
            total_msgs,
            elapsed: stages.encode,
        },
        RunResult {
            label: label.into(),
            mode: "pipeline/send",
            total_msgs,
            elapsed: stages.send,
        },
        RunResult {
            label: label.into(),
            mode: "pipeline/recv",
            total_msgs,
            elapsed: stages.recv,
        },
        RunResult {
            label: label.into(),
            mode: "pipeline/decode",
            total_msgs,
            elapsed: stages.decode,
        },
        RunResult {
            label: label.into(),
            mode: "pipeline/encode-zc",
            total_msgs,
            elapsed: stages.encode_zc,
        },
        RunResult {
            label: label.into(),
            mode: "pipeline/send-zc",
            total_msgs,
            elapsed: stages.send_zc,
        },
        RunResult {
            label: label.into(),
            mode: "pipeline/recv-zc",
            total_msgs,
            elapsed: stages.recv_zc,
        },
        RunResult {
            label: label.into(),
            mode: "pipeline/decode-zc",
            total_msgs,
            elapsed: stages.decode_zc,
        },
        RunResult {
            label: label.into(),
            mode: "pipeline/encode-view",
            total_msgs,
            elapsed: stages.encode_view,
        },
        RunResult {
            label: label.into(),
            mode: "pipeline/total-e2e",
            total_msgs,
            elapsed: pipeline_stats.elapsed,
        },
    ];
    (results, pipeline_stats, stages)
}

#[tokio::main]
async fn main() {
    let total_msgs = env_u64("BENCH_PUBLISH_MSGS", DEFAULT_TOTAL_MSGS);
    let batch_size = env_usize("BENCH_PUBLISH_BATCH", DEFAULT_BATCH_SIZE);
    let shard_count = env_usize("BENCH_SHARD_COUNT", 1);
    let max_feed = env_usize("BENCH_MAX_FEED", 256);
    let payload = vec![0u8; 64];

    println!(
        "Config: total_msgs={total_msgs}, batch_size={batch_size}, max_feed={max_feed}, shards={shard_count}, payload=64B"
    );

    // ── Memory journal ─────────────────────────────────────────────────
    println!("\n▸ Running: journal=Memory");
    let (mem_results, mem_pipeline, mem_stages) = run_for_kind(
        JournalKind::Memory,
        "memory",
        None,
        None,
        None,
        shard_count,
        total_msgs,
        batch_size,
        max_feed,
        &payload,
    )
    .await;

    // ── Tolerant journal ───────────────────────────────────────────────
    println!("\n▸ Running: journal=Tolerant");
    let tol_server_dir = make_tolerant_dir("server");
    let tol_pipeline_dir = make_tolerant_dir("pipeline");
    let tol_stages_dir = make_tolerant_dir("stages");
    let (tol_results, tol_pipeline, tol_stages) = run_for_kind(
        JournalKind::Tolerant,
        "tolerant",
        Some(&tol_server_dir),
        Some(&tol_pipeline_dir),
        Some(&tol_stages_dir),
        shard_count,
        total_msgs,
        batch_size,
        max_feed,
        &payload,
    )
    .await;

    // Show tolerant dir contents before cleanup.
    println!("\n▸ Tolerant server dir ({}):", tol_server_dir.display());
    print_dir_tree(&tol_server_dir, 1);
    println!("\n▸ Tolerant pipeline dir ({}):", tol_pipeline_dir.display());
    print_dir_tree(&tol_pipeline_dir, 1);
    println!("\n▸ Tolerant stages dir ({}):", tol_stages_dir.display());
    print_dir_tree(&tol_stages_dir, 1);

    let server_cleanup = verify_and_cleanup_tolerant(&tol_server_dir);
    let pipeline_cleanup = verify_and_cleanup_tolerant(&tol_pipeline_dir);
    let stages_cleanup = verify_and_cleanup_tolerant(&tol_stages_dir);

    // ── Unified report — one row per stage, per journal ─────────────────
    println!("\n+-----------+---------------------+-------------+--------------------+");
    println!("| Journal   | Stage               | Elapsed     | Throughput         |");
    println!("+-----------+---------------------+-------------+--------------------+");
    print_results_block(&mem_results);
    println!("|           |                     |             |                    |");
    print_results_block(&tol_results);
    println!("+-----------+---------------------+-------------+--------------------+");

    // Validations.
    println!();
    report_pipeline("memory", &mem_pipeline, total_msgs, max_feed);
    report_pipeline("tolerant", &tol_pipeline, total_msgs, max_feed);
    report_stages("memory", &mem_stages, total_msgs, max_feed);
    report_stages("tolerant", &tol_stages, total_msgs, max_feed);

    let mut cleanup_ok = true;
    for (tag, res) in [
        ("server", server_cleanup),
        ("pipeline", pipeline_cleanup),
        ("stages", stages_cleanup),
    ] {
        match res {
            Ok((files, bytes)) => println!(
                "✓ tolerant[{tag}] verified: {files} file(s), {bytes} bytes → cleaned up"
            ),
            Err(e) => {
                eprintln!("✗ tolerant[{tag}] cleanup FAILED: {e}");
                cleanup_ok = false;
            }
        }
    }

    let pipeline_ok =
        mem_pipeline.decoded == total_msgs && tol_pipeline.decoded == total_msgs;
    let stages_ok =
        mem_stages.decoded_count == total_msgs && tol_stages.decoded_count == total_msgs;
    if !cleanup_ok || !pipeline_ok || !stages_ok {
        std::process::exit(2);
    }
}

fn print_results_block(results: &[RunResult]) {
    for r in results {
        println!(
            "| {:<9} | {:<19} | {:>10.2?} | {:>12.0} msg/s |",
            r.label,
            r.mode,
            r.elapsed,
            r.throughput()
        );
    }
}

fn report_pipeline(label: &str, stats: &PipelineStats, expected: u64, max_feed: usize) {
    let expected_frames = (expected as usize + max_feed - 1) / max_feed;
    let ok = stats.decoded == expected && stats.frames_sent == expected_frames;
    let status = if ok { "✓" } else { "✗" };
    println!(
        "{status} pipeline[{label}]: frames_sent={} (expected {}), decoded={}/{}",
        stats.frames_sent, expected_frames, stats.decoded, expected
    );
}

fn report_stages(label: &str, stats: &StageStats, expected: u64, max_feed: usize) {
    let expected_frames = (expected as usize + max_feed - 1) / max_feed;
    let ok = stats.decoded_count == expected
        && stats.frames_count == expected_frames
        && stats.recv_bytes > 0;
    let status = if ok { "✓" } else { "✗" };
    println!(
        "{status} stages[{label}]: frames={} (expected {}), decoded={}/{}, recv_bytes={}",
        stats.frames_count, expected_frames, stats.decoded_count, expected, stats.recv_bytes
    );
}
