//! Cross-client WAL compatibility harness.
//!
//! The ackstore's whole reason for being a hand-rolled WAL instead of SQLite
//! is that all clients share ONE on-disk format. This example makes that
//! claim executable: it writes a known log with the Rust implementation, or
//! replays a log written by another client and prints the recovered live set,
//! so the two can be diffed.
//!
//! ```text
//! cargo run --example ackstore_xcompat -- write <dir>
//! cargo run --example ackstore_xcompat -- read  <dir>
//! ```
//!
//! The `write` fixture is fixed (no clock/randomness in the seq set) so the
//! sibling clients can assert against it verbatim:
//!
//!   orders/worker  -> live { 3, 5, 7, 9, 10..=12 }   (1,2,4 confirmed-up-to, 6 confirmed)
//!   payments/w2    -> live { 42 }
//!   empty/slot     -> registered, no live seqs
//!
//! `read` prints one `stream|consumer|seq,seq,...` line per slot, sorted, so a
//! byte-for-byte diff against the other client's output is the assertion.

use arbitro_client_tokio::ackstore::{store::Store, wal::Wal, WalConfig};

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_default();
    let dir = args.next().expect("usage: ackstore_xcompat <write|read> <dir>");

    match mode.as_str() {
        "write" => write_fixture(&dir),
        "read" => read_fixture(&dir),
        other => panic!("unknown mode {other:?}, expected write|read"),
    }
}

fn open(dir: &str) -> std::sync::Arc<Wal> {
    let mut cfg = WalConfig::new(dir);
    cfg.fsync = true;
    Wal::open(cfg).expect("open wal")
}

fn write_fixture(dir: &str) {
    let _ = std::fs::remove_dir_all(dir);
    let wal = open(dir);

    let orders = wal.slot("orders", "worker").expect("slot");
    for seq in 1..=12u64 {
        orders.check_record(seq).expect("record");
    }
    // Out-of-order removals exercise both removal ops and the min/max gate.
    orders.confirm_up_to(2).expect("confirm_up_to");
    orders.confirm(4).expect("confirm");
    orders.confirm(6).expect("confirm");
    orders.confirm(8).expect("confirm");

    let payments = wal.slot("payments", "w2").expect("slot");
    payments.check_record(42).expect("record");

    // Registered but empty: its Register must survive so the id can never be
    // reused for a different (stream, consumer).
    wal.slot("empty", "slot").expect("slot");

    wal.sync().expect("sync");
    wal.close().expect("close");
    println!("wrote fixture to {dir}");
}

fn read_fixture(dir: &str) {
    let wal = open(dir);
    let mut lines: Vec<String> = Vec::new();
    for info in wal.list_slots() {
        // `SlotInfo` carries bounds, not the set — probe the whole range via
        // the public `seen()` so this reads exactly like a real consumer.
        let slot = wal.slot(&info.stream, &info.consumer).expect("slot");
        let mut seqs = Vec::new();
        if info.live > 0 {
            for seq in info.min_seq..=info.max_seq {
                if slot.seen(seq) {
                    seqs.push(seq.to_string());
                }
            }
        }
        lines.push(format!("{}|{}|{}", info.stream, info.consumer, seqs.join(",")));
    }
    lines.sort();
    for l in lines {
        println!("{l}");
    }
    wal.close().expect("close");
}
