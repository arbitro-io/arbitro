//! Consumer cursor perf bench.
//!
//! Measures:
//!   A. In-memory get   — NameRegistry::consumer_cursor() HashMap lookup under Mutex.
//!   B. In-memory set   — NameRegistry::set_consumer_cursor() HashMap insert under Mutex.
//!   C. Cold recovery   — scan a persisted command_log file to find latest cursor per id.
//!
//! Scenario C simulates what happens on server startup: read the full command_log,
//! extract the last CMD_CURSOR_UPDATE for each consumer_id, populate the in-memory map.

use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::time::Instant;

const N_CONSUMERS: usize = 10_000;
const N_ROUNDS: usize = 100_000;

fn header(title: &str) {
    println!("\n── {} ──", title);
    println!(
        "{:<44} {:>14} {:>14} {:>14}",
        "operation", "mean_ns/op", "total_ms", "ops/sec"
    );
    println!("{}", "─".repeat(90));
}

fn row(name: &str, elapsed_ns: u64, ops: usize) {
    let mean = elapsed_ns as f64 / ops as f64;
    let total_ms = elapsed_ns as f64 / 1e6;
    let ops_sec = (ops as f64) / (elapsed_ns as f64 / 1e9);
    println!(
        "{:<44} {:>14.2} {:>14.2} {:>14.0}",
        name, mean, total_ms, ops_sec
    );
}

// ── A + B: in-memory HashMap under Mutex (mirrors NameRegistry::consumer_cursors) ──
fn bench_inmem() {
    use std::sync::Mutex;

    header("A + B. in-memory HashMap<u32,u64> under Mutex");

    let map: Mutex<HashMap<u32, u64, foldhash::fast::FixedState>> = Mutex::new(
        HashMap::with_capacity_and_hasher(N_CONSUMERS, foldhash::fast::FixedState::default()),
    );

    // Preload
    {
        let mut g = map.lock().unwrap();
        for i in 0..N_CONSUMERS {
            g.insert(i as u32, i as u64 * 100);
        }
    }

    // A. get
    {
        let t0 = Instant::now();
        let mut sum = 0u64;
        for i in 0..N_ROUNDS {
            let k = (i % N_CONSUMERS) as u32;
            let v = map.lock().unwrap().get(&k).copied().unwrap_or(0);
            sum = sum.wrapping_add(v);
        }
        let ns = t0.elapsed().as_nanos() as u64;
        std::hint::black_box(sum);
        row("consumer_cursor(id) get", ns, N_ROUNDS);
    }

    // B. set
    {
        let t0 = Instant::now();
        for i in 0..N_ROUNDS {
            let k = (i % N_CONSUMERS) as u32;
            let v = i as u64;
            map.lock().unwrap().insert(k, v);
        }
        let ns = t0.elapsed().as_nanos() as u64;
        row("set_consumer_cursor(id, seq)", ns, N_ROUNDS);
    }
}

// ── C. Cold recovery — scan a persisted command_log ──
//
// Real command_log entry for CMD_CURSOR_UPDATE:
//   header (~24 B) + body (4 B consumer_id LE + 8 B last_acked_seq LE) = ~36 B total
// We use a simplified 16-byte record: [4 kind][4 consumer_id][8 last_acked_seq]
// which is even more optimistic than the real format (so this bench is a
// lower bound on real recovery cost, not upper bound).
fn bench_recovery(n_entries: usize) {
    header(&format!("C. cold recovery scan — {} CMD_CURSOR_UPDATE entries", n_entries));

    let path = format!("/tmp/arbitro/consumer_cursor_bench_{}.log", n_entries);
    let _ = std::fs::remove_file(&path);

    // Write entries: simulate write-ahead log of cursor updates.
    // Each consumer gets ~n_entries/N_CONSUMERS updates (last write wins).
    {
        let mut f = std::fs::File::create(&path).expect("create log");
        let mut buf = [0u8; 16];
        buf[0..4].copy_from_slice(&0x0301_u32.to_le_bytes()); // kind = CMD_CURSOR_UPDATE
        for i in 0..n_entries {
            let cid = (i % N_CONSUMERS) as u32;
            let seq = i as u64;
            buf[4..8].copy_from_slice(&cid.to_le_bytes());
            buf[8..16].copy_from_slice(&seq.to_le_bytes());
            f.write_all(&buf).expect("write");
        }
        f.sync_all().expect("fsync"); // force to disk
    }

    let file_size = std::fs::metadata(&path).unwrap().len();
    println!("  log file size: {} bytes ({:.1} MB)", file_size, file_size as f64 / 1e6);

    // Scan: read all entries, keep last for each consumer_id
    {
        let mut cursors: HashMap<u32, u64, foldhash::fast::FixedState> =
            HashMap::with_capacity_and_hasher(N_CONSUMERS, foldhash::fast::FixedState::default());
        let t0 = Instant::now();
        let f = std::fs::File::open(&path).expect("open");
        let mut r = BufReader::with_capacity(64 * 1024, f);
        let mut buf = [0u8; 16];
        let mut n = 0usize;
        while r.read_exact(&mut buf).is_ok() {
            let cid = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
            let seq = u64::from_le_bytes([
                buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
            ]);
            cursors.insert(cid, seq);
            n += 1;
        }
        let ns = t0.elapsed().as_nanos() as u64;
        println!(
            "  scanned {} entries in {:.2} ms → {} unique cursors → {:.0} entries/sec",
            n,
            ns as f64 / 1e6,
            cursors.len(),
            n as f64 / (ns as f64 / 1e9)
        );

        // After scan, per-id get is trivial (in-memory HashMap).
        // Report per-id get from the reconstructed map for reference.
        let t0 = Instant::now();
        let mut sum = 0u64;
        for i in 0..N_ROUNDS {
            let k = (i % N_CONSUMERS) as u32;
            sum = sum.wrapping_add(*cursors.get(&k).unwrap_or(&0));
        }
        let ns = t0.elapsed().as_nanos() as u64;
        std::hint::black_box(sum);
        row("post-recovery get_by_id (in-memory)", ns, N_ROUNDS);
    }

    let _ = std::fs::remove_file(&path);
}

fn main() {
    println!(
        "consumer_cursor bench — N_CONSUMERS={} N_ROUNDS={}",
        N_CONSUMERS, N_ROUNDS
    );

    bench_inmem();
    bench_recovery(100_000);
    bench_recovery(1_000_000);
    bench_recovery(10_000_000);

    println!("\ndone.");
}
