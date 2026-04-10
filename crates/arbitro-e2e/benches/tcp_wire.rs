//! Raw TCP benchmark: Batch Ack (Simulated) vs Flusher Service (Pipelined).
//!
//! This benchmark compares two high-performance batching strategies:
//!   1. BatchAck (Simulated): Client sends 512 msgs, server sends 1 response.
//!   2. Flusher Service (Pipelined): Real Arbitro Flusher service on a dedicated thread.

extern crate libc;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use arbitro_proto::config::StreamConfig;
use arbitro_server::{ArbitroServer, Config as ServerConfig};
use arbitro_server::flusher::Flusher;
use tokio::runtime::Runtime;

// ── Settings ────────────────────────────────────────────────────

const MSGS_PER_CONN: u32 = 10_000;
const CONCURRENCY: &[usize] = &[1, 2, 4, 8, 16, 32];
const BATCH_ACK_SIZE: u32 = 512;

// ── Wire constants ──────────────────────────────────────────────

const ENVELOPE_SIZE: usize = 16;
const ACTION_CONNECT: u16 = 0x0603;
const ACTION_CONNECTED: u16 = 0x0604;
const ACTION_REPOK: u16 = 0x0203;
const ACTION_PUBLISH: u16 = 0x0101;
const ACTION_PUBLISH_ACCUMULATE: u16 = 0x0102;

const REPOK_FRAME: usize = ENVELOPE_SIZE + 16;
const BATCH_ACK_BODY: usize = 8;
const BATCH_ACK_FRAME: usize = ENVELOPE_SIZE + BATCH_ACK_BODY;

// ── Helpers ─────────────────────────────────────────────────────

#[cfg(unix)]
pub fn pin_to_core(core_id: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core_id, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}
#[cfg(not(unix))]
pub fn pin_to_core(_: usize) {}

fn make_envelope(action: u16, stream_id: u32, msg_len: u32, env_seq: u32) -> [u8; ENVELOPE_SIZE] {
    let mut buf = [0u8; ENVELOPE_SIZE];
    buf[0..2].copy_from_slice(&action.to_le_bytes());
    buf[4..8].copy_from_slice(&stream_id.to_le_bytes());
    buf[8..12].copy_from_slice(&msg_len.to_le_bytes());
    buf[12..16].copy_from_slice(&env_seq.to_le_bytes());
    buf
}

fn tcp_read_exact(stream: &mut TcpStream, buf: &mut [u8]) {
    stream.read_exact(buf).expect("tcp read failed");
}

fn fnv1a_32(data: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in data { h ^= b as u32; h = h.wrapping_mul(0x0100_0193); }
    h
}

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Frame de publish para el servidor real (Publish o PublishAccumulate).
fn build_publish_real(action: u16, stream_id: u32, seq: u32) -> Vec<u8> {
    let subject = b"bench.subject";
    let payload = [0u8; 64];
    let body_len = 2 + 12 + subject.len() + payload.len();
    let env = make_envelope(action, stream_id, body_len as u32, seq);
    let mut frame = Vec::with_capacity(ENVELOPE_SIZE + body_len);
    frame.extend_from_slice(&env);
    frame.extend_from_slice(&1u16.to_le_bytes());
    let mut entry_hdr = [0u8; 12];
    entry_hdr[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    entry_hdr[4..6].copy_from_slice(&(subject.len() as u16).to_le_bytes());
    frame.extend_from_slice(&entry_hdr);
    frame.extend_from_slice(subject);
    frame.extend_from_slice(&payload);
    frame
}

fn cpu_time_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts); }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn rss_kb() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = s.split_whitespace().nth(1).and_then(|v| v.parse().ok()).unwrap_or(0);
    pages * 4
}

fn build_publish_accumulate(seq: u32) -> Vec<u8> {
    let subject = b"bench.msg";
    let payload = [0u8; 64];
    let body_len = 2 + 12 + subject.len() + payload.len();
    let env = make_envelope(ACTION_PUBLISH_ACCUMULATE, 0, body_len as u32, seq);
    let mut frame = Vec::with_capacity(ENVELOPE_SIZE + body_len);
    frame.extend_from_slice(&env);
    frame.extend_from_slice(&1u16.to_le_bytes()); // entry count
    let mut entry = [0u8; 12];
    entry[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    entry[4..6].copy_from_slice(&(subject.len() as u16).to_le_bytes());
    frame.extend_from_slice(&entry);
    frame.extend_from_slice(subject);
    frame.extend_from_slice(&payload);
    frame
}

fn start_flusher_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for s in listener.incoming() {
            if let Ok(mut stream) = s {
                std::thread::spawn(move || {
                    stream.set_nodelay(true).unwrap();
                    let mut hdr = [0u8; ENVELOPE_SIZE];
                    if stream.read_exact(&mut hdr).is_err() { return; }
                    let _ = stream.write_all(&make_envelope(ACTION_CONNECTED, 0, 16, 0));
                    let _ = stream.write_all(&[0u8; 16]);
                    
                    let mut write_stream = stream.try_clone().unwrap();
                    let flusher = Flusher::new()
                        .on_flush(move |seqs| {
                            let mut buf = vec![0u8; seqs.len() * REPOK_FRAME];
                            for (i, &seq) in seqs.iter().enumerate() {
                                let env = make_envelope(ACTION_REPOK, 0, 16, seq);
                                buf[i*REPOK_FRAME..i*REPOK_FRAME+ENVELOPE_SIZE].copy_from_slice(&env);
                            }
                            let _ = write_stream.write_all(&buf);
                        })
                        .spawn();

                    loop {
                        let mut env_buf = [0u8; ENVELOPE_SIZE];
                        if stream.read_exact(&mut env_buf).is_err() { break; }
                        let msg_len = u32::from_le_bytes([env_buf[8], env_buf[9], env_buf[10], env_buf[11]]) as usize;
                        let env_seq = u32::from_le_bytes([env_buf[12], env_buf[13], env_buf[14], env_buf[15]]);
                        if msg_len > 0 {
                            let mut skip = vec![0u8; msg_len];
                            if stream.read_exact(&mut skip).is_err() { break; }
                        }
                        flusher.push(env_seq, msg_len);
                    }
                });
            }
        }
    });
    addr
}

// ── BatchAck Server ─────────────────────────────────────────────

fn spawn_batch_server(stop: Arc<AtomicBool>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        listener.set_nonblocking(true).unwrap();
        while !stop.load(Relaxed) {
            if let Ok((mut stream, _)) = listener.accept() {
                std::thread::spawn(move || {
                    stream.set_nodelay(true).unwrap();
                    let mut hdr = [0u8; ENVELOPE_SIZE];
                    if stream.read_exact(&mut hdr).is_ok() {
                        let _ = stream.write_all(&make_envelope(ACTION_CONNECTED, 0, 16, 0));
                        let _ = stream.write_all(&[0u8; 16]);
                        let mut count = 0;
                        loop {
                            let mut env = [0u8; ENVELOPE_SIZE];
                            if stream.read_exact(&mut env).is_err() { break; }
                            count += 1;
                            if count >= BATCH_ACK_SIZE {
                                let last_seq = u32::from_le_bytes([env[12], env[13], env[14], env[15]]);
                                let mut ack = [0u8; BATCH_ACK_FRAME];
                                ack[..ENVELOPE_SIZE].copy_from_slice(&make_envelope(ACTION_REPOK, 0, 8, last_seq));
                                if stream.write_all(&ack).is_err() { break; }
                                count = 0;
                            }
                        }
                    }
                });
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    });
    addr
}

// ── Main Bench Logic ────────────────────────────────────────────

struct ThreadResult { elapsed: Duration, msgs: u64 }

/// Todos los mensajes se lanzan como tokio::spawn independientes.
/// Cada task envía su publish y awaits su RepOk sin bloquear a los demás.
/// Un reader task demultiplexa los RepOks por env_seq via oneshot channels.
/// El Flusher recibe todos los msgs casi simultáneamente → acumula → flush en batch.
fn run_bench_tokio(addr: &str) -> ThreadResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::oneshot;

    tokio::runtime::Runtime::new().unwrap().block_on(async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (mut reader, mut writer) = stream.into_split();

        // Handshake
        writer.write_all(&make_envelope(ACTION_CONNECT, 0, 16, 0)).await.unwrap();
        writer.write_all(&[0u8; 16]).await.unwrap();
        let mut hdr = [0u8; ENVELOPE_SIZE + 16];
        reader.read_exact(&mut hdr).await.unwrap();

        // Writer task: cada task envía su frame por channel, sin contención
        let (write_tx, mut write_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(MSGS_PER_CONN as usize);
        tokio::spawn(async move {
            while let Some(data) = write_rx.recv().await {
                writer.write_all(&data).await.unwrap();
            }
        });

        let pending: Arc<tokio::sync::Mutex<HashMap<u32, oneshot::Sender<()>>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        // Reader task: lee RepOks y despacha al task correcto via oneshot
        let pending_r = pending.clone();
        tokio::spawn(async move {
            loop {
                let mut frame = [0u8; REPOK_FRAME];
                if reader.read_exact(&mut frame).await.is_err() { break; }
                let seq = u32::from_le_bytes([frame[12], frame[13], frame[14], frame[15]]);
                if let Some(tx) = pending_r.lock().await.remove(&seq) { let _ = tx.send(()); }
            }
        });

        let start = Instant::now();
        let seq_counter = Arc::new(AtomicU32::new(1));
        let mut join_set = tokio::task::JoinSet::new();

        for _ in 0..MSGS_PER_CONN {
            let write_tx = write_tx.clone();
            let pending = pending.clone();
            let seq = seq_counter.fetch_add(1, Relaxed);

            join_set.spawn(async move {
                let (tx, rx) = oneshot::channel();
                pending.lock().await.insert(seq, tx);
                write_tx.send(build_publish_accumulate(seq)).await.unwrap();
                let _ = rx.await;
            });
        }

        while join_set.join_next().await.is_some() {}

        ThreadResult { elapsed: start.elapsed(), msgs: MSGS_PER_CONN as u64 }
    })
}

/// Conecta al servidor real, lanza MSGS_PER_CONN tasks concurrentes.
/// Cada task envía un frame (Publish o PublishAccumulate) y awaita su RepOk.
fn run_bench_real_tokio(rt: &Runtime, addr: &str, action: u16, stream_id: u32) -> ThreadResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::oneshot;

    rt.block_on(async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (mut reader, mut writer) = stream.into_split();

        // Handshake
        writer.write_all(&make_envelope(ACTION_CONNECT, 0, 16, 0)).await.unwrap();
        writer.write_all(&[0u8; 16]).await.unwrap();
        let mut hdr = [0u8; ENVELOPE_SIZE + 16];
        reader.read_exact(&mut hdr).await.unwrap();

        // Writer task: recibe frames por channel, escribe sin contención
        let (write_tx, mut write_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(MSGS_PER_CONN as usize);
        tokio::spawn(async move {
            while let Some(data) = write_rx.recv().await {
                writer.write_all(&data).await.unwrap();
            }
        });

        // Pending map compartido entre tasks y reader
        let pending: Arc<tokio::sync::Mutex<HashMap<u32, oneshot::Sender<()>>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        // Reader task: lee RepOks y despacha por env_seq
        let pending_r = pending.clone();
        tokio::spawn(async move {
            loop {
                let mut frame = [0u8; REPOK_FRAME];
                if reader.read_exact(&mut frame).await.is_err() { break; }
                let seq = u32::from_le_bytes([frame[12], frame[13], frame[14], frame[15]]);
                if let Some(tx) = pending_r.lock().await.remove(&seq) { let _ = tx.send(()); }
            }
        });

        let start = Instant::now();
        let seq_counter = Arc::new(AtomicU32::new(1));
        let mut join_set = tokio::task::JoinSet::new();

        for _ in 0..MSGS_PER_CONN {
            let write_tx = write_tx.clone();
            let pending = pending.clone();
            let seq = seq_counter.fetch_add(1, Relaxed);
            join_set.spawn(async move {
                let (tx, rx) = oneshot::channel();
                pending.lock().await.insert(seq, tx);
                write_tx.send(build_publish_real(action, stream_id, seq)).await.unwrap();
                let _ = rx.await;
            });
        }

        while join_set.join_next().await.is_some() {}
        ThreadResult { elapsed: start.elapsed(), msgs: MSGS_PER_CONN as u64 }
    })
}

fn main() {
    // ── Real arbitro server ─────────────────────────────────────────
    let rt = Runtime::new().unwrap();
    let real_port = pick_port();
    let real_addr = format!("127.0.0.1:{real_port}");
    {
        let listen = real_addr.clone();
        rt.spawn(async move {
            let cfg = ServerConfig::default().listen_addr(listen).max_connections(200);
            let _ = ArbitroServer::new(cfg).run().await;
        });
    }
    std::thread::sleep(Duration::from_millis(150));

    // Crear stream "bench" con filtro ">"
    let stream_id = fnv1a_32(b"bench");
    rt.block_on(async {
        let client = arbitro_client::Client::connect_with_timeout(
            &real_addr, Duration::from_secs(5),
        ).await.unwrap();
        client.create_stream(&StreamConfig::new(b"bench", b">").build()).await.unwrap();
    });

    let batch_stop = Arc::new(AtomicBool::new(false));

    // 1. BatchAck Simulated
    let batch_addr = spawn_batch_server(batch_stop.clone());
    std::thread::sleep(Duration::from_millis(100));
    println!("\n[ BatchAck (Simulated) — 1 reply per 512 msgs ]");
    print_header();
    for &n in CONCURRENCY {
        let rss_before = rss_kb();
        let cpu_before = cpu_time_ns();
        let results = run_bench(n, &batch_addr, true);
        let cpu_after = cpu_time_ns();
        let rss_after = rss_kb();
        print_row(&format!("{n} conn"), &results, cpu_before, cpu_after, rss_before, rss_after);
    }

    // 2. Flusher Fire & Forget
    let flusher_addr = start_flusher_server();
    std::thread::sleep(Duration::from_millis(100));
    println!("\n[ Flusher — Fire & Forget (send all → read all RepOks) ]");
    print_header();
    for &n in CONCURRENCY {
        let rss_before = rss_kb();
        let cpu_before = cpu_time_ns();
        let results = run_bench(n, &flusher_addr, false);
        let cpu_after = cpu_time_ns();
        let rss_after = rss_kb();
        print_row(&format!("{n} conn"), &results, cpu_before, cpu_after, rss_before, rss_after);
    }

    // 3. Flusher tokio::spawn — cada msg awaita su RepOk sin bloquear a los demás
    let flusher_addr2 = start_flusher_server();
    std::thread::sleep(Duration::from_millis(100));
    println!("\n[ Flusher — tokio::spawn por msg (todos en vuelo simultáneamente) ]");
    print_header();
    {
        let rss_before = rss_kb();
        let cpu_before = cpu_time_ns();
        let result = run_bench_tokio(&flusher_addr2);
        let cpu_after = cpu_time_ns();
        let rss_after = rss_kb();
        print_row(&format!("{MSGS_PER_CONN} msgs"), &[result], cpu_before, cpu_after, rss_before, rss_after);
    }

    // 4. Publish single — real server, concurrent tokio::spawn
    println!("\n[ Publish — real server, concurrent tokio::spawn ({MSGS_PER_CONN} msgs) ]");
    print_header();
    {
        let rss_before = rss_kb();
        let cpu_before = cpu_time_ns();
        let result = run_bench_real_tokio(&rt, &real_addr, ACTION_PUBLISH, stream_id);
        let cpu_after = cpu_time_ns();
        let rss_after = rss_kb();
        print_row(&format!("{MSGS_PER_CONN} msgs"), &[result], cpu_before, cpu_after, rss_before, rss_after);
    }

    // 5. PublishAccumulate — real server, concurrent tokio::spawn
    println!("\n[ PublishAccumulate — real server, concurrent tokio::spawn ({MSGS_PER_CONN} msgs) ]");
    print_header();
    {
        let rss_before = rss_kb();
        let cpu_before = cpu_time_ns();
        let result = run_bench_real_tokio(&rt, &real_addr, ACTION_PUBLISH_ACCUMULATE, stream_id);
        let cpu_after = cpu_time_ns();
        let rss_after = rss_kb();
        print_row(&format!("{MSGS_PER_CONN} msgs"), &[result], cpu_before, cpu_after, rss_before, rss_after);
    }

    batch_stop.store(true, Relaxed);
}

fn run_bench(n: usize, addr: &str, is_batch: bool) -> Vec<ThreadResult> {
    let barrier = Arc::new(Barrier::new(n + 1));
    let mut handles = Vec::new();
    for _ in 0..n {
        let addr = addr.to_string();
        let bar = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let mut tcp = TcpStream::connect(addr).unwrap();
            tcp.set_nodelay(true).unwrap();
            let _ = tcp.write_all(&make_envelope(ACTION_CONNECT, 0, 16, 0));
            let _ = tcp.write_all(&[0u8; 16]);
            let mut hdr = [0u8; ENVELOPE_SIZE];
            tcp_read_exact(&mut tcp, &mut hdr);
            tcp_read_exact(&mut tcp, &mut [0u8; 16]);
            
            bar.wait();
            let start = Instant::now();
            let mut seq = 1;
            if is_batch {
                let batches = MSGS_PER_CONN / BATCH_ACK_SIZE;
                for _ in 0..batches {
                    for _ in 0..BATCH_ACK_SIZE {
                        let env = make_envelope(ACTION_PUBLISH, 0, 0, seq);
                        tcp.write_all(&env).expect("write failed");
                        seq += 1;
                    }
                    let mut ack = [0u8; BATCH_ACK_FRAME];
                    tcp_read_exact(&mut tcp, &mut ack);
                }
            } else {
                for _ in 0..MSGS_PER_CONN {
                    let frame = build_publish_accumulate(seq);
                    tcp.write_all(&frame).expect("write failed");
                    seq += 1;
                }
                let mut received = 0;
                let mut buf = [0u8; REPOK_FRAME * 512];
                while received < MSGS_PER_CONN {
                    if let Ok(n_bytes) = tcp.read(&mut buf) {
                        if n_bytes == 0 { break; }
                        received += (n_bytes / REPOK_FRAME) as u32;
                    } else { break; }
                }
            }
            ThreadResult { elapsed: start.elapsed(), msgs: MSGS_PER_CONN as u64 }
        }));
    }
    barrier.wait();
    handles.into_iter().map(|h| h.join().unwrap()).collect()
}

fn print_header() {
    println!("  {:12} | {:>15} | {:>10} | {:>7} | {:>9} | {:>9}",
        "Config", "Throughput", "Avg Lat", "CPU%", "RSS(KB)", "ΔRSS(KB)");
    println!("  {}", "-".repeat(75));
}

fn print_row(label: &str, results: &[ThreadResult], cpu_before: u64, cpu_after: u64, rss_before: u64, rss_after: u64) {
    let wall = results.iter().map(|r| r.elapsed).max().unwrap();
    let total_msgs: u64 = results.iter().map(|r| r.msgs).sum();
    let throughput = total_msgs as f64 / wall.as_secs_f64();
    let avg = wall / (total_msgs as u32 / results.len() as u32);
    let cpu_pct = (cpu_after.saturating_sub(cpu_before) as f64 / wall.as_nanos() as f64) * 100.0;
    let rss_delta = rss_after as i64 - rss_before as i64;
    println!("  {:12} | {:>10.0} msg/s | {:>8.2?} | {:>5.1}% | {:>9} | {:>+9}",
        label, throughput, avg, cpu_pct, rss_after, rss_delta);
}
