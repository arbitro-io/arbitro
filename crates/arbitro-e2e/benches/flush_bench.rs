//! Isolated TCP-based benchmark for the Flusher Service vs Individual ACK.

extern crate libc;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

// ── Wire Constants ──────────────────────────────────────────────

const ENVELOPE_SIZE: usize = 16;
const ACTION_PUBLISH: u16 = 0x0101;
const ACTION_REPOK: u16 = 0x0203;
const REPOK_FRAME: usize = ENVELOPE_SIZE + 16;

// ── Helpers ─────────────────────────────────────────────────────

#[cfg(unix)]
fn cpu_time_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts);
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

#[cfg(not(unix))]
fn cpu_time_ns() -> u64 { 0 }

#[cfg(unix)]
fn rss_kb() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = s.split_whitespace().nth(1).and_then(|v| v.parse().ok()).unwrap_or(0);
    pages * 4
}

#[cfg(not(unix))]
fn rss_kb() -> u64 { 0 }

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
pub fn pin_to_core(_core_id: usize) {}

fn make_envelope(action: u16, stream_id: u32, msg_len: u32, env_seq: u32) -> [u8; ENVELOPE_SIZE] {
    let mut buf = [0u8; ENVELOPE_SIZE];
    buf[0..2].copy_from_slice(&action.to_le_bytes());
    buf[4..8].copy_from_slice(&stream_id.to_le_bytes());
    buf[8..12].copy_from_slice(&msg_len.to_le_bytes());
    buf[12..16].copy_from_slice(&env_seq.to_le_bytes());
    buf
}

// ── Flusher Service Builder ─────────────────────────────────────────

pub struct Flusher {
    tx: std::sync::mpsc::SyncSender<FlusherEntry>,
}

#[derive(Clone, Copy)]
pub struct FlusherEntry {
    pub env_seq: u32,
    pub bytes: usize,
}

pub struct FlusherBuilder<F> {
    interval_ms: u64,
    max_bytes: usize,
    max_size: usize,
    on_flush: Option<F>,
}

impl FlusherBuilder<()> {
    pub fn new() -> Self {
        Self {
            interval_ms: 5,
            max_bytes: 1024 * 1024,
            max_size: 512,
            on_flush: None,
        }
    }
}

impl<F> FlusherBuilder<F> {
    pub fn interval(mut self, ms: u64) -> Self { self.interval_ms = ms; self }
    pub fn max_bytes(mut self, bytes: usize) -> Self { self.max_bytes = bytes; self }
    pub fn max_size(mut self, size: usize) -> Self { self.max_size = size; self }
    pub fn on_flush<F2>(self, f: F2) -> FlusherBuilder<F2>
    where F2: FnMut(&[u32]) + Send + 'static,
    {
        FlusherBuilder {
            interval_ms: self.interval_ms,
            max_bytes: self.max_bytes,
            max_size: self.max_size,
            on_flush: Some(f),
        }
    }
}

impl<F> FlusherBuilder<F>
where F: FnMut(&[u32]) + Send + 'static,
{
    pub fn spawn(self) -> Flusher {
        let (tx, rx) = std::sync::mpsc::sync_channel::<FlusherEntry>(100_000);
        let max_size = self.max_size;
        let max_bytes = self.max_bytes;
        let interval_ms = self.interval_ms;
        let mut on_flush = self.on_flush.unwrap();

        std::thread::spawn(move || {
            pin_to_core(0);
            let mut pending_seqs = Vec::with_capacity(max_size);
            let mut total_bytes = 0;
            let timeout = Duration::from_millis(interval_ms);

            loop {
                let msg = if pending_seqs.is_empty() {
                    rx.recv().ok()
                } else {
                    match rx.recv_timeout(timeout) {
                        Ok(msg) => Some(msg),
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            if !pending_seqs.is_empty() {
                                on_flush(&pending_seqs);
                                pending_seqs.clear();
                                total_bytes = 0;
                            }
                            continue;
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => None,
                    }
                };

                match msg {
                    Some(entry) => {
                        pending_seqs.push(entry.env_seq);
                        total_bytes += entry.bytes;
                        if pending_seqs.len() >= max_size || total_bytes >= max_bytes {
                            on_flush(&pending_seqs);
                            pending_seqs.clear();
                            total_bytes = 0;
                        }
                    }
                    None => {
                        if !pending_seqs.is_empty() { on_flush(&pending_seqs); }
                        break;
                    }
                }
            }
        });
        Flusher { tx }
    }
}

impl Flusher {
    pub fn new() -> FlusherBuilder<()> { FlusherBuilder::new() }
    #[inline(always)]
    pub fn push(&self, env_seq: u32, msg_bytes: usize) {
        let _ = self.tx.send(FlusherEntry { env_seq, bytes: msg_bytes });
    }
}

// ── Server Handlers ──────────────────────────────────────────────────

fn handle_individual_conn(mut stream: TcpStream) {
    stream.set_nodelay(true).unwrap();
    let mut hdr = [0u8; ENVELOPE_SIZE];
    let repok_body = [0u8; 16];
    
    loop {
        if stream.read_exact(&mut hdr).is_err() { break; }
        let msg_len = u32::from_le_bytes([hdr[8], hdr[9], hdr[10], hdr[11]]) as usize;
        let env_seq = u32::from_le_bytes([hdr[12], hdr[13], hdr[14], hdr[15]]);
        
        // Skip body
        if msg_len > 0 {
            let mut body = vec![0u8; msg_len];
            let _ = stream.read_exact(&mut body);
        }

        // Write individual REPOK
        let mut resp = [0u8; REPOK_FRAME];
        resp[..ENVELOPE_SIZE].copy_from_slice(&make_envelope(ACTION_REPOK, 0, 16, env_seq));
        resp[ENVELOPE_SIZE..].copy_from_slice(&repok_body);
        if stream.write_all(&resp).is_err() { break; }
    }
}

fn handle_batch_conn(stream: TcpStream) {
    stream.set_nodelay(true).unwrap();
    let mut read_stream = stream.try_clone().unwrap();
    let mut write_stream = stream;

    let flusher = Flusher::new()
        .interval(2)
        .max_size(512)
        .on_flush(move |seqs| {
            if seqs.is_empty() { return; }
            let mut buffer = vec![0u8; seqs.len() * REPOK_FRAME];
            for (i, &seq) in seqs.iter().enumerate() {
                let offset = i * REPOK_FRAME;
                buffer[offset..offset+ENVELOPE_SIZE].copy_from_slice(&make_envelope(ACTION_REPOK, 0, 16, seq));
            }
            let _ = write_stream.write_all(&buffer);
        })
        .spawn();

    let mut hdr = [0u8; ENVELOPE_SIZE];
    loop {
        if read_stream.read_exact(&mut hdr).is_err() { break; }
        let msg_len = u32::from_le_bytes([hdr[8], hdr[9], hdr[10], hdr[11]]) as usize;
        let env_seq = u32::from_le_bytes([hdr[12], hdr[13], hdr[14], hdr[15]]);
        
        if msg_len > 0 {
            let mut body = vec![0u8; msg_len];
            let _ = read_stream.read_exact(&mut body);
        }
        flusher.push(env_seq, msg_len);
    }
}

fn start_server(mode: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mode = mode.to_string();

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            if mode == "individual" {
                std::thread::spawn(|| handle_individual_conn(stream));
            } else {
                std::thread::spawn(|| handle_batch_conn(stream));
            }
        }
    });
    addr
}

// ── Benchmark ─────────────────────────────────────────────────────────

fn run_bench(name: &str, mode: &str, iterations: u32) {
    println!("Scenario: {}", name);
    let addr = start_server(mode);
    
    // Connect
    let mut tcp = TcpStream::connect(&addr).expect("connect");
    tcp.set_nodelay(true).unwrap();

    let rss_before = rss_kb();
    let cpu_before = cpu_time_ns();
    let start = Instant::now();

    let (tx, rx) = std::sync::mpsc::channel();
    let mut read_tcp = tcp.try_clone().unwrap();

    // Spawn reader thread
    std::thread::spawn(move || {
        let mut count = 0;
        let mut buf = [0u8; REPOK_FRAME * 1024]; // Read buffer
        while count < iterations {
            match read_tcp.read(&mut buf) {
                Ok(n) if n > 0 => {
                    // Decouple frames: Each frame is REPOK_FRAME bytes
                    let frames = n / REPOK_FRAME;
                    count += frames as u32;
                }
                _ => break,
            }
        }
        tx.send(()).unwrap();
    });

    // Sender loop
    let pub_frame = {
        let mut f = Vec::with_capacity(ENVELOPE_SIZE + 64);
        f.extend_from_slice(&make_envelope(ACTION_PUBLISH, 0, 64, 0));
        f.extend_from_slice(&[0u8; 64]);
        f
    };

    for i in 0..iterations {
        // Patch sequence in metadata (bytes 12..16)
        let mut frame = pub_frame.clone();
        frame[12..16].copy_from_slice(&i.to_le_bytes());
        tcp.write_all(&frame).unwrap();
    }

    // Wait for reader
    rx.recv().unwrap();

    let elapsed = start.elapsed();
    let cpu_after = cpu_time_ns();
    let rss_after = rss_kb();

    let cpu_ns = cpu_after.saturating_sub(cpu_before);
    let cpu_pct = if elapsed.as_nanos() > 0 { (cpu_ns as f64 / elapsed.as_nanos() as f64) * 100.0 } else { 0.0 };
    let rss_delta = rss_after as i64 - rss_before as i64;
    let throughput = (iterations as f64) / elapsed.as_secs_f64();

    println!("Processed {} frames in {:?}", iterations, elapsed);
    println!("Throughput: {:>10.0} msgs/sec", throughput);
    println!("CPU Load:   {:>10.1} %", cpu_pct);
    println!("RSS (Mem):  {:>10} KB (Δ {:+})", rss_after, rss_delta);
    println!("{:-<60}", "");
    
    // Give some time for threads to cleanup
    std::thread::sleep(Duration::from_millis(100));
}

fn main() {
    pin_to_core(0);
    println!("Flusher Service Benchmark - TCP WIRE VERSION");
    println!("{:=<60}", "");

    // 1. Individual Ack (Ping/Pong overhead over TCP)
    run_bench("1-to-1 Individual Ack (Real TCP)", "individual", 200_000);

    // 2. Batch Ack (Optimized Arbitro Flow over TCP)
    run_bench("Batch Ack Aggregated (Real TCP)", "batch", 1_000_000);

    println!("{:=<60}", "");
}
