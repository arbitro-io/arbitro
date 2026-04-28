//! drain_slow_consumer — measures whether one slow TCP consumer blocks
//! the drain thread from delivering to fast consumers.
//!
//! ## Scenario
//!
//! Mirrors the real arbitro drain: 1 shard thread (sync) produces frames
//! and dispatches each one to its target connection's TCP write half.
//! Some connections are FAST (drain immediately), one is SLOW (artificial
//! delay per recv). With a small TCP send buffer, the server's write to
//! the slow conn eventually blocks on `writable().await`.
//!
//! ## Variants
//!
//! V1 DIRECT — `write_all_blocking` per frame (current arbitro path).
//!   Shard thread writes synchronously. If the slow conn blocks, the
//!   shard cannot deliver to other conns either — they all wait.
//!
//! V2 KIT — per-conn `kit::Stream<Bytes>` + tokio writer task per conn.
//!   Shard pushes to its conn's kit::Stream (cheap, ~3 ns) and continues.
//!   The writer task does the TCP write asynchronously. Only the slow
//!   conn's writer is blocked; fast conns' writers proceed independently.
//!
//! ## Measurement
//!
//! - `drain_wall_ns`: time for the shard to finish dispatching all
//!   frames (i.e., the loop that simulates real drain finishing).
//! - `fast_done_ns`: time for the FAST consumers to receive all their
//!   frames. This is the metric that matters for fairness — if the
//!   slow conn blocks the shard, fast consumers wait too.
//! - `slow_done_ns`: when the slow conn finally receives its last frame.

#![allow(unused)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

use arbitro_kit::stream::Stream;

fn env_usize(var: &str, default: usize) -> usize {
    std::env::var(var).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn env_u64(var: &str, default: u64) -> u64 {
    std::env::var(var).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[derive(Clone, Copy)]
struct Cfg {
    n_conns: usize,
    n_slow: usize,
    frames_per_conn: usize,
    frame_size: usize,
    /// Slow consumer artificial sleep between recvs (microseconds).
    slow_delay_us: u64,
    /// Server-side socket SO_SNDBUF to force backpressure earlier.
    sndbuf_bytes: usize,
}

#[derive(Clone)]
struct Result {
    name: &'static str,
    drain_wall_ns: u128,
    fast_done_ns: u128,
    slow_done_ns: u128,
}

fn print_header() {
    println!("{:<48} {:>12} {:>12} {:>12}",
             "variant", "drain (ms)", "fast (ms)", "slow (ms)");
    println!("{}", "─".repeat(90));
}
fn print_result(r: &Result) {
    println!("{:<48} {:>12.2} {:>12.2} {:>12.2}",
             r.name,
             r.drain_wall_ns as f64 / 1e6,
             r.fast_done_ns as f64 / 1e6,
             r.slow_done_ns as f64 / 1e6);
}

// ── Consumers (TCP clients) ─────────────────────────────────────────────

/// Spawn `n_conns` consumer tokio tasks. The first `n_slow` are slow
/// (sleep `slow_delay_us` between socket reads). All record per-conn
/// "last byte received" timestamps. Returns the addr to bind to.
async fn run_consumers(
    addr: SocketAddr,
    cfg: Cfg,
    arrived: Arc<Vec<AtomicU64>>,        // count of bytes received
    last_ts: Arc<Vec<AtomicU64>>,        // ns since epoch when last frame arrived
) -> Vec<tokio::task::JoinHandle<()>> {
    (0..cfg.n_conns)
        .map(|cid| {
            let arrived = arrived.clone();
            let last_ts = last_ts.clone();
            tokio::spawn(async move {
                let mut sock = TcpStream::connect(addr).await.unwrap();
                sock.set_nodelay(true).ok();
                // Don't shrink recv buf — we WANT slow conn to backpressure
                // via its read pace, not its buffer size.
                let mut buf = vec![0u8; 16 * 1024];
                let target = (cfg.frames_per_conn * cfg.frame_size) as u64;
                let is_slow = cid < cfg.n_slow;
                loop {
                    if is_slow {
                        tokio::time::sleep(Duration::from_micros(cfg.slow_delay_us)).await;
                    }
                    match sock.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            let prev = arrived[cid].fetch_add(n as u64, Ordering::Relaxed);
                            if prev + n as u64 >= target {
                                last_ts[cid].store(now_ns(), Ordering::Release);
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            })
        })
        .collect()
}

#[inline]
fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// ── Server-side TCP write helpers ───────────────────────────────────────

fn set_sndbuf(stream: &TcpStream, bytes: usize) {
    use std::os::fd::AsRawFd;
    let fd = stream.as_raw_fd();
    let optval = bytes as libc::c_int;
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

// ── V1 — direct write_all_blocking from shard thread ─────────────────────

fn v1_direct(rt: &Runtime, cfg: Cfg) -> Result {
    let arrived = Arc::new((0..cfg.n_conns).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
    let last_ts = Arc::new((0..cfg.n_conns).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
    let arrived_c = arrived.clone();
    let last_ts_c = last_ts.clone();

    rt.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let n_conns = cfg.n_conns;

        // Server-side: accept N socks; collect write halves.
        let acc_handle = tokio::spawn(async move {
            let mut socks = Vec::with_capacity(n_conns);
            for _ in 0..n_conns {
                let (s, _) = listener.accept().await.unwrap();
                s.set_nodelay(true).ok();
                set_sndbuf(&s, cfg.sndbuf_bytes);
                socks.push(s);
            }
            socks
        });

        // Spawn consumers.
        let consumers = run_consumers(addr, cfg, arrived_c.clone(), last_ts_c.clone()).await;

        let socks = acc_handle.await.unwrap();
        // Split: server-side write halves.
        let halves: Vec<Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>> =
            socks.into_iter().map(|s| {
                let (_rh, wh) = s.into_split();
                Arc::new(tokio::sync::Mutex::new(wh))
            }).collect();

        // Drain (sync thread) — direct write_all_blocking equivalent.
        let halves_arc = Arc::new(halves);
        let halves_for_drain = halves_arc.clone();
        let handle = tokio::runtime::Handle::current();
        let drain_start = Arc::new(AtomicU64::new(0));
        let drain_done = Arc::new(AtomicU64::new(0));
        let ds = drain_start.clone();
        let dd = drain_done.clone();
        let drain_thread = thread::spawn(move || {
            ds.store(now_ns(), Ordering::Release);
            let frame: Bytes = Bytes::from(vec![0xABu8; cfg.frame_size]);
            // Round-robin: deliver one frame to each conn, then loop.
            for f in 0..cfg.frames_per_conn {
                for cid in 0..cfg.n_conns {
                    let wh = halves_for_drain[cid].clone();
                    let frame = frame.clone();
                    handle.block_on(async move {
                        let mut g = wh.lock().await;
                        let _ = g.write_all(&frame).await;
                    });
                }
            }
            dd.store(now_ns(), Ordering::Release);
        });

        drain_thread.join().unwrap();
        let drain_wall_ns = (drain_done.load(Ordering::Acquire)
            - drain_start.load(Ordering::Acquire)) as u128;

        // Wait for consumers.
        for h in consumers { let _ = h.await; }

        // Compute fast_done: max ts over fast conns. slow_done: max over slow.
        let drain_start_v = drain_start.load(Ordering::Acquire);
        let mut fast_max = 0u64;
        let mut slow_max = 0u64;
        for cid in 0..cfg.n_conns {
            let ts = last_ts[cid].load(Ordering::Acquire);
            if cid < cfg.n_slow { slow_max = slow_max.max(ts); }
            else                { fast_max = fast_max.max(ts); }
        }
        Result {
            name: "V1 DIRECT  shard→write_all_blocking",
            drain_wall_ns,
            fast_done_ns: fast_max.saturating_sub(drain_start_v) as u128,
            slow_done_ns: slow_max.saturating_sub(drain_start_v) as u128,
        }
    })
}

// ── V2 — per-conn kit::Stream<Bytes> + writer task ──────────────────────

fn v2_kit(rt: &Runtime, cfg: Cfg) -> Result {
    let arrived = Arc::new((0..cfg.n_conns).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
    let last_ts = Arc::new((0..cfg.n_conns).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
    let arrived_c = arrived.clone();
    let last_ts_c = last_ts.clone();

    rt.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let n_conns = cfg.n_conns;

        let acc_handle = tokio::spawn(async move {
            let mut socks = Vec::with_capacity(n_conns);
            for _ in 0..n_conns {
                let (s, _) = listener.accept().await.unwrap();
                s.set_nodelay(true).ok();
                set_sndbuf(&s, cfg.sndbuf_bytes);
                socks.push(s);
            }
            socks
        });

        let consumers = run_consumers(addr, cfg, arrived_c.clone(), last_ts_c.clone()).await;
        let socks = acc_handle.await.unwrap();

        // Per-conn kit::Stream<Bytes> + tokio writer task.
        let mut kit_streams: Vec<Arc<Stream<Bytes>>> = Vec::with_capacity(n_conns);
        let mut writer_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(n_conns);
        for s in socks {
            let (_rh, mut wh) = s.into_split();
            let kit_stream: Arc<Stream<Bytes>> = Arc::new(Stream::new());
            kit_streams.push(kit_stream.clone());
            writer_tasks.push(tokio::spawn(async move {
                kit_stream.set_consumer(thread::current());
                // We need a sync recv loop, but we're in a tokio task.
                // Use a small chunk per iter via try_recv + yield to keep
                // task cooperative. Each iter writes one frame.
                let total = cfg.frames_per_conn;
                let mut got = 0;
                while got < total {
                    if let Some(frame) = kit_stream.try_recv() {
                        if wh.write_all(&frame).await.is_err() { break; }
                        got += 1;
                    } else {
                        tokio::task::yield_now().await;
                    }
                }
                let _ = wh.shutdown().await;
            }));
        }

        let kit_streams_arc = Arc::new(kit_streams);
        let kit_for_drain = kit_streams_arc.clone();
        let drain_start = Arc::new(AtomicU64::new(0));
        let drain_done = Arc::new(AtomicU64::new(0));
        let ds = drain_start.clone();
        let dd = drain_done.clone();
        let drain_thread = thread::spawn(move || {
            ds.store(now_ns(), Ordering::Release);
            let frame: Bytes = Bytes::from(vec![0xABu8; cfg.frame_size]);
            for _ in 0..cfg.frames_per_conn {
                for cid in 0..cfg.n_conns {
                    kit_for_drain[cid].send(frame.clone());
                }
            }
            dd.store(now_ns(), Ordering::Release);
        });
        drain_thread.join().unwrap();
        let drain_wall_ns = (drain_done.load(Ordering::Acquire)
            - drain_start.load(Ordering::Acquire)) as u128;

        // Wait for consumers + writer tasks.
        for h in consumers { let _ = h.await; }
        for h in writer_tasks { let _ = h.await; }

        let drain_start_v = drain_start.load(Ordering::Acquire);
        let mut fast_max = 0u64;
        let mut slow_max = 0u64;
        for cid in 0..cfg.n_conns {
            let ts = last_ts[cid].load(Ordering::Acquire);
            if cid < cfg.n_slow { slow_max = slow_max.max(ts); }
            else                { fast_max = fast_max.max(ts); }
        }
        Result {
            name: "V2 KIT     shard→kit::Stream→writer task",
            drain_wall_ns,
            fast_done_ns: fast_max.saturating_sub(drain_start_v) as u128,
            slow_done_ns: slow_max.saturating_sub(drain_start_v) as u128,
        }
    })
}

fn main() {
    let cfg = Cfg {
        n_conns:         env_usize("BENCH_DRAIN_CONNS", 4),
        n_slow:          env_usize("BENCH_DRAIN_SLOW",  1),
        frames_per_conn: env_usize("BENCH_DRAIN_FRAMES", 200),
        frame_size:      env_usize("BENCH_DRAIN_FSIZE", 1024),
        slow_delay_us:   env_u64("BENCH_DRAIN_DELAY_US", 500),
        sndbuf_bytes:    env_usize("BENCH_DRAIN_SNDBUF", 16 * 1024),
    };

    println!("=== drain_slow_consumer ===");
    println!("conns={}  slow={}  frames/conn={}  frame_size={}B  slow_delay={}µs  sndbuf={}B",
             cfg.n_conns, cfg.n_slow, cfg.frames_per_conn,
             cfg.frame_size, cfg.slow_delay_us, cfg.sndbuf_bytes);
    println!();
    println!("Goal: when one consumer is SLOW, does the shard thread block?");
    println!("- drain (ms): how long the shard takes to finish dispatching");
    println!("- fast (ms):  when fast consumers received all their frames");
    println!("- slow (ms):  when slow consumer received all its frames");
    println!();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(env_usize("BENCH_DRAIN_WORKERS", 4))
        .enable_all()
        .build()
        .unwrap();

    print_header();
    let r1 = v1_direct(&rt, cfg);
    print_result(&r1);
    let r2 = v2_kit(&rt, cfg);
    print_result(&r2);

    println!();
    let drain_speedup = r1.drain_wall_ns as f64 / r2.drain_wall_ns.max(1) as f64;
    let fast_speedup  = r1.fast_done_ns  as f64 / r2.fast_done_ns.max(1)  as f64;
    println!("kit drain vs direct: {:.1}× speedup on drain wall time", drain_speedup);
    println!("kit fast vs direct:  {:.1}× speedup on fast-conn delivery", fast_speedup);
    println!("Done.");
}
