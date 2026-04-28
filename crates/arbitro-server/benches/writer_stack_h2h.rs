//! `writer_stack_h2h` — end-to-end stack comparison through real TCP.
//!
//! Compares the two viable client-writer architectures with each
//! primitive used in its NATIVE mode (no async/sync mixing):
//!
//!   A. **pure async**: tokio runtime (workers W), `tokio::sync::mpsc`,
//!      tokio writer task, native `Sender::send().await` waker park.
//!   B. **pure sync**:  N std::threads as producers, `kit::Mpsc` with
//!      `producer.send()` parking on `std::thread::park`, 1 std::thread
//!      drain. No tokio on the producer/writer side at all.
//!
//! Both patterns push N × FRAMES_PER_PROD frames through TCP and the
//! receiver counts them. Reader runs on a small std::thread-hosted tokio
//! runtime so the listener-side conditions are identical across patterns.
//!
//! Reports min / p50 / p99 / max so noise is visible. This is the bench
//! to read when deciding "should the client migrate to a sync stack?".
//! Companion bench `channel_native_h2h` isolates the channel cost (no TCP).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Builder;
use tokio::sync::mpsc;

use arbitro_kit::route::Mpsc;

const N_PRODUCERS: usize = 16;              // ≤ physical cores to avoid over-subscription noise
const FRAMES_PER_PROD: usize = 1000;        // bench_safety ≤ 1000
const PAYLOAD_SIZE: usize = 256;
const HDR_SIZE: usize = 16;
const FRAME_TOTAL: usize = HDR_SIZE + PAYLOAD_SIZE;
const TOKIO_CHAN_CAP: usize = 8192;
const KIT_RING_CAP: usize = 256;
const RUNS: usize = 20;                     // p50/p99 stable across scheduler noise
const WARMUP: usize = 2;
const TOKIO_WORKER_SWEEP: &[usize] = &[1, 4, 8, 16];

#[inline]
fn build_frame(sub_id: u32, seq: u32) -> Bytes {
    let mut buf = vec![0u8; FRAME_TOTAL];
    buf[0..4].copy_from_slice(&sub_id.to_le_bytes());
    buf[4..8].copy_from_slice(&seq.to_le_bytes());
    buf[8..12].copy_from_slice(&(PAYLOAD_SIZE as u32).to_le_bytes());
    let mut chk: u32 = 0;
    for i in 0..PAYLOAD_SIZE {
        let b = ((sub_id.wrapping_mul(31)
            .wrapping_add(seq.wrapping_mul(17))
            .wrapping_add(i as u32)) & 0xff) as u8;
        buf[HDR_SIZE + i] = b;
        chk = chk.wrapping_add(b as u32);
    }
    buf[12..16].copy_from_slice(&chk.to_le_bytes());
    Bytes::from(buf)
}

async fn run_reader(mut sock: TcpStream, expected: usize) -> (usize, usize) {
    let mut backlog: Vec<u8> = Vec::with_capacity(1 << 20);
    let mut buf = vec![0u8; 1 << 16];
    let mut received = 0usize;
    let mut errors = 0usize;
    while received < expected {
        let n = match sock.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        backlog.extend_from_slice(&buf[..n]);
        while backlog.len() >= HDR_SIZE {
            let len = u32::from_le_bytes(backlog[8..12].try_into().unwrap()) as usize;
            if len != PAYLOAD_SIZE {
                errors += 1; received += 1;
                backlog.drain(..HDR_SIZE);
                continue;
            }
            if backlog.len() < HDR_SIZE + len { break; }
            let chk_recv = u32::from_le_bytes(backlog[12..16].try_into().unwrap());
            let mut chk: u32 = 0;
            for &b in &backlog[HDR_SIZE..HDR_SIZE + len] { chk = chk.wrapping_add(b as u32); }
            if chk != chk_recv { errors += 1; }
            received += 1;
            backlog.drain(..HDR_SIZE + len);
        }
    }
    (received, errors)
}

/// Spawn a tokio runtime on a std::thread that just hosts the listener +
/// reader so both patterns face identical receiver-side conditions.
fn spawn_reader_thread(total: usize) -> (std::sync::mpsc::Receiver<(usize, usize)>, std::net::SocketAddr) {
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("reader".into())
        .spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                addr_tx.send(listener.local_addr().unwrap()).unwrap();
                let (sock, _) = listener.accept().await.unwrap();
                sock.set_nodelay(true).ok();
                let res = run_reader(sock, total).await;
                result_tx.send(res).unwrap();
            });
        })
        .unwrap();
    (result_rx, addr_rx.recv().unwrap())
}

/// Pattern A: pure async stack — tokio runtime hosts producers + drain,
/// using tokio::sync::mpsc + tokio TcpStream. `send().await` parks via
/// native waker.
fn run_pure_async(workers: usize) -> (u128, usize, usize) {
    let total = N_PRODUCERS * FRAMES_PER_PROD;
    let (result_rx, addr) = spawn_reader_thread(total);

    let rt = Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap();

    let elapsed = rt.block_on(async move {
        let stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).ok();
        let (tx, mut rx) = mpsc::channel::<Bytes>(TOKIO_CHAN_CAP);

        let writer_h = tokio::spawn(async move {
            let mut w = stream;
            let mut batch: Vec<Bytes> = Vec::with_capacity(1024);
            loop {
                match rx.recv().await {
                    Some(b) => batch.push(b),
                    None => break,
                }
                while let Ok(b) = rx.try_recv() {
                    batch.push(b);
                    if batch.len() >= 1024 { break; }
                }
                let mut slices: Vec<std::io::IoSlice> =
                    batch.iter().map(|b| std::io::IoSlice::new(b)).collect();
                let mut s = slices.as_mut_slice();
                while !s.is_empty() {
                    match w.write_vectored(s).await {
                        Ok(0) => return,
                        Ok(k) => std::io::IoSlice::advance_slices(&mut s, k),
                        Err(_) => return,
                    }
                }
                batch.clear();
            }
            let _ = w.flush().await;
        });

        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(N_PRODUCERS);
        for sub_id in 0..N_PRODUCERS as u32 {
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                for seq in 0..FRAMES_PER_PROD as u32 {
                    let frame = build_frame(sub_id, seq);
                    let _ = tx.send(frame).await;     // native waker park if full
                }
            }));
        }
        for h in handles { let _ = h.await; }
        drop(tx);
        let _ = writer_h.await;
        t0.elapsed().as_nanos()
    });

    let (received, errors) = result_rx.recv().unwrap();
    (elapsed, received, errors)
}

/// Pattern B: pure sync stack — 50 std::threads as producers (`send()`
/// blocks via std::thread::park when ring full), kit::Mpsc, 1 std::thread
/// drain with blocking std TcpStream + write_vectored. No tokio anywhere
/// on the producer/writer side.
fn run_pure_sync() -> (u128, usize, usize) {
    let total = N_PRODUCERS * FRAMES_PER_PROD;
    let (result_rx, addr) = spawn_reader_thread(total);

    let std_stream = std::net::TcpStream::connect(addr).unwrap();
    std_stream.set_nodelay(true).ok();
    std_stream.set_nonblocking(false).ok();

    let (producers, consumer, shutdown) =
        Mpsc::<Bytes, KIT_RING_CAP>::new(N_PRODUCERS);
    let written = Arc::new(AtomicU64::new(0));
    let written_c = written.clone();

    let writer_h = std::thread::Builder::new()
        .name("kit-writer".into())
        .spawn(move || {
            use std::io::{IoSlice, Write};
            consumer.bind();
            let mut w = std_stream;
            let mut batch: Vec<Bytes> = Vec::with_capacity(1024);
            while (written_c.load(Ordering::Relaxed) as usize) < total {
                match consumer.recv_batch(|b: Bytes| { batch.push(b); }) {
                    Ok(_) => {}
                    Err(_) => break,
                }
                if batch.is_empty() { continue; }
                let mut slices: Vec<IoSlice> =
                    batch.iter().map(|b| IoSlice::new(b)).collect();
                let mut s = slices.as_mut_slice();
                while !s.is_empty() {
                    match w.write_vectored(s) {
                        Ok(0) => return,
                        Ok(k) => IoSlice::advance_slices(&mut s, k),
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(_) => return,
                    }
                }
                written_c.fetch_add(batch.len() as u64, Ordering::Relaxed);
                batch.clear();
            }
            let _ = w.flush();
        })
        .unwrap();

    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(N_PRODUCERS);
    for (sub_id, producer) in producers.into_iter().enumerate() {
        handles.push(std::thread::Builder::new()
            .name(format!("prod-{sub_id}"))
            .spawn(move || {
                producer.bind();
                let sub_id = sub_id as u32;
                for seq in 0..FRAMES_PER_PROD as u32 {
                    let frame = build_frame(sub_id, seq);
                    producer.send(frame);     // native park on backpressure
                }
            })
            .unwrap());
    }
    for h in handles { let _ = h.join(); }
    let _ = writer_h.join();
    let elapsed = t0.elapsed().as_nanos();
    shutdown.signal();

    let (received, errors) = result_rx.recv().unwrap();
    (elapsed, received, errors)
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() { return 0; }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn run_pattern<F: Fn() -> (u128, usize, usize)>(name: &str, runs_fn: F)
    -> Vec<(u128, usize, usize)>
{
    let total = N_PRODUCERS * FRAMES_PER_PROD;
    for _ in 0..WARMUP { let _ = runs_fn(); }
    let mut runs: Vec<(u128, usize, usize)> = (0..RUNS).map(|_| runs_fn()).collect();
    runs.sort_by_key(|r| r.0);

    // Sanity check: every run delivered all messages.
    for (i, &(_, r, e)) in runs.iter().enumerate() {
        if r != total || e != 0 {
            eprintln!("  {} run {}: FAIL rcv={}/{} err={}", name, i + 1, r, total, e);
        }
    }

    let times: Vec<u128> = runs.iter().map(|r| r.0).collect();
    let min = times[0];
    let p50 = percentile(&times, 0.50);
    let p99 = percentile(&times, 0.99);
    let max = *times.last().unwrap();

    let bps = |ns: u128| (total as f64 * FRAME_TOTAL as f64) / (ns as f64 / 1e9) / (1024.0 * 1024.0);
    let mps = |ns: u128| total as f64 / (ns as f64 / 1e9);

    println!("  {:<22}  min={:>6.2}ms  p50={:>6.2}ms  p99={:>6.2}ms  max={:>6.2}ms",
             name,
             min as f64 / 1e6, p50 as f64 / 1e6, p99 as f64 / 1e6, max as f64 / 1e6);
    println!("  {:<22}  msg/s p50={:>10.0}  MB/s p50={:>8.1}  (msg/s min={:>10.0})",
             "", mps(p50), bps(p50), mps(min));

    runs
}

fn main() {
    let total = N_PRODUCERS * FRAMES_PER_PROD;
    println!("=== writer: pure async vs pure sync ===");
    println!("N_PRODUCERS={}  FRAMES_PER_PROD={}  FRAME={} B  RUNS={}",
             N_PRODUCERS, FRAMES_PER_PROD, FRAME_TOTAL, RUNS);
    println!("Total bytes/run = {} B = {:.1} MB",
             total * FRAME_TOTAL,
             (total * FRAME_TOTAL) as f64 / (1024.0 * 1024.0));
    println!("Reporting min / p50 / p99 / max so noise is visible.");
    println!();

    // Pattern B (pure sync) — runtime-agnostic, no worker concept.
    println!("════ B. pure sync stack ({} std::threads, kit::Mpsc, std TCP) ════", N_PRODUCERS);
    let sync_runs = run_pattern("sync(std)", run_pure_sync);
    println!();

    // Pattern A (pure async) — sweep tokio worker_threads.
    let mut async_p50_by_workers: Vec<(usize, u128)> = Vec::new();
    for &workers in TOKIO_WORKER_SWEEP {
        println!("════ A. pure async stack (tokio workers={}, tokio::mpsc) ════", workers);
        let async_runs = run_pattern(&format!("async(w={})", workers),
                                      || run_pure_async(workers));
        let mut times: Vec<u128> = async_runs.iter().map(|r| r.0).collect();
        times.sort();
        async_p50_by_workers.push((workers, percentile(&times, 0.50)));
        println!();
    }

    // Final summary at p50.
    let mut sync_times: Vec<u128> = sync_runs.iter().map(|r| r.0).collect();
    sync_times.sort();
    let sync_p50 = percentile(&sync_times, 0.50);
    let bps = |ns: u128| (total as f64 * FRAME_TOTAL as f64) / (ns as f64 / 1e9) / (1024.0 * 1024.0);
    let mps = |ns: u128| total as f64 / (ns as f64 / 1e9);

    println!("════ FINAL SUMMARY (p50) ════");
    println!("  sync(std)       : msg/s={:>10.0}  MB/s={:>8.1}",
             mps(sync_p50), bps(sync_p50));
    for (workers, p50) in &async_p50_by_workers {
        let ratio = sync_p50 as f64 / *p50 as f64;
        let label = if ratio > 1.0 { "async wins" }
                    else if ratio < 1.0 { "sync wins" }
                    else { "tie" };
        println!("  async(w={:<2})       : msg/s={:>10.0}  MB/s={:>8.1}  (sync/async={:.2}× — {})",
                 workers, mps(*p50), bps(*p50), ratio, label);
    }
}
