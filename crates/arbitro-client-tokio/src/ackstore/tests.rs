//! Real tests for the ackstore WAL: crash recovery, TTL, corruption, snapshot,
//! ConfirmUpTo, compaction, concurrency — mirroring the Go suite.
#![allow(dead_code)] // test clock helper exposes set/add for symmetry

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::memory::MemoryStore;
use super::store::Store;
use super::wal::{Wal, WalConfig};

/// Injectable clock for deterministic TTL tests.
struct FakeClock(Arc<AtomicI64>);
impl FakeClock {
    fn new(start: i64) -> Self {
        FakeClock(Arc::new(AtomicI64::new(start)))
    }
    fn clock(&self) -> super::wal::Clock {
        let c = self.0.clone();
        Arc::new(move || c.load(Ordering::Relaxed))
    }
    fn set(&self, ms: i64) {
        self.0.store(ms, Ordering::Relaxed);
    }
    fn add(&self, ms: i64) {
        self.0.fetch_add(ms, Ordering::Relaxed);
    }
}

fn tmp_dir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    // Unique-ish without external deps: pid + a monotonic counter.
    use std::sync::atomic::AtomicU64;
    static CTR: AtomicU64 = AtomicU64::new(0);
    let n = CTR.fetch_add(1, Ordering::Relaxed);
    p.push(format!("ackstore-test-{}-{}", std::process::id(), n));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn open(dir: &std::path::Path, fsync: bool) -> Arc<Wal> {
    let mut cfg = WalConfig::new(dir);
    cfg.fsync = fsync;
    Wal::open(cfg).unwrap()
}

// --- basic correctness ---

#[test]
fn basic_dedup() {
    let dir = tmp_dir();
    let w = open(&dir, false);
    let s = w.slot("orders", "worker").unwrap();
    assert!(s.check_record(1).unwrap(), "first is new");
    assert!(!s.check_record(1).unwrap(), "redelivery is dup");
    assert!(s.check_record(2).unwrap(), "2 is new");
    w.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn fresh_gate() {
    let dir = tmp_dir();
    let w = open(&dir, false);
    let s = w.slot("s", "c").unwrap();
    assert!(s.fresh(100), "fresh on empty");
    s.check_record(50).unwrap();
    assert!(s.fresh(51), "above max is fresh");
    assert!(!s.fresh(50), "in-bounds needs check");
    w.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn consumer_recreate_same_name() {
    let dir = tmp_dir();
    let w = open(&dir, false);
    let s1 = w.slot("jobs", "worker").unwrap();
    assert!(s1.check_record(100).unwrap());
    w.sync().unwrap();
    // Recreated under SAME name → same slot → job recognized as done.
    let s2 = w.slot("jobs", "worker").unwrap();
    assert!(!s2.check_record(100).unwrap(), "must dedup after recreate");
    w.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn consumer_recreate_different_name() {
    let dir = tmp_dir();
    let w = open(&dir, false);
    w.slot("jobs", "worker").unwrap().check_record(100).unwrap();
    let s2 = w.slot("jobs", "worker-v2").unwrap();
    assert!(s2.check_record(100).unwrap(), "different name = fresh");
    w.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn same_seq_different_streams() {
    let dir = tmp_dir();
    let w = open(&dir, false);
    w.slot("orders", "w").unwrap().check_record(42).unwrap();
    let pay = w.slot("payments", "w").unwrap();
    assert!(pay.check_record(42).unwrap(), "seq is stream-scoped");
    w.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

// --- crash recovery ---

#[test]
fn crash_recovery() {
    let dir = tmp_dir();
    {
        let w = open(&dir, true);
        let s = w.slot("orders", "worker").unwrap();
        for seq in 1..=100 {
            s.check_record(seq).unwrap();
        }
        w.sync().unwrap();
        // Drop without close = crash (but synced, so durable).
    }
    let w2 = open(&dir, true);
    let s = w2.slot("orders", "worker").unwrap();
    for seq in 1..=100 {
        assert!(!s.check_record(seq).unwrap(), "seq {seq} should survive");
    }
    assert!(s.check_record(101).unwrap(), "101 is new");
    w2.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn confirm_survives_restart() {
    let dir = tmp_dir();
    {
        let w = open(&dir, true);
        let s = w.slot("s", "c").unwrap();
        s.check_record(1).unwrap();
        s.check_record(2).unwrap();
        s.check_record(3).unwrap();
        s.confirm(2).unwrap();
        w.sync().unwrap();
        w.close().unwrap();
    }
    let w2 = open(&dir, false);
    let s = w2.slot("s", "c").unwrap();
    assert!(!s.check_record(1).unwrap(), "1 tracked");
    assert!(!s.check_record(3).unwrap(), "3 tracked");
    assert!(s.check_record(2).unwrap(), "2 confirmed → fresh");
    w2.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

// --- TTL ---

#[test]
fn ttl_expiry() {
    let dir = tmp_dir();
    let clk = FakeClock::new(1_000_000);
    let mut cfg = WalConfig::new(&dir);
    cfg.ttl = Some(Duration::from_millis(1000));
    cfg.sweep_interval = Duration::from_millis(60);
    cfg.now = clk.clock();
    let w = Wal::open(cfg).unwrap();
    let s = w.slot("s", "c").unwrap();
    s.check_record(1).unwrap();
    clk.add(2000);
    // Wait for the sweeper.
    let mut ok = false;
    for _ in 0..40 {
        if w.metrics().expired >= 1 {
            ok = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(ok, "expected expiry");
    assert!(s.check_record(1).unwrap(), "fresh after expiry");
    w.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn ttl_not_resurrected_on_restart() {
    let dir = tmp_dir();
    let clk1 = FakeClock::new(1_000_000);
    {
        let mut cfg = WalConfig::new(&dir);
        cfg.fsync = true;
        cfg.now = clk1.clock();
        let w = Wal::open(cfg).unwrap();
        w.slot("s", "c").unwrap().check_record(1).unwrap();
        w.sync().unwrap();
        w.close().unwrap();
    }
    // Reopen far in the future with a TTL → old record must not resurrect.
    let clk2 = FakeClock::new(1_000_000 + 10_000_000);
    let mut cfg = WalConfig::new(&dir);
    cfg.ttl = Some(Duration::from_millis(1000));
    cfg.now = clk2.clock();
    let w2 = Wal::open(cfg).unwrap();
    let s = w2.slot("s", "c").unwrap();
    assert!(s.check_record(1).unwrap(), "expired-on-restart is fresh");
    w2.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

// --- corruption ---

#[test]
fn torn_tail_truncated() {
    let dir = tmp_dir();
    {
        let w = open(&dir, true);
        let s = w.slot("s", "c").unwrap();
        s.check_record(1).unwrap();
        s.check_record(2).unwrap();
        w.sync().unwrap();
        w.close().unwrap();
    }
    // Append garbage.
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(dir.join("ackstore.log"))
        .unwrap();
    f.write_all(&[0xFF, 0xFF, 0x00]).unwrap();
    drop(f);

    let w2 = open(&dir, false);
    let s = w2.slot("s", "c").unwrap();
    assert!(!s.check_record(1).unwrap(), "1 recovered");
    assert!(!s.check_record(2).unwrap(), "2 recovered");
    assert!(s.check_record(3).unwrap(), "write after recovery works");
    w2.sync().unwrap();
    w2.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn corrupt_crc_truncated() {
    let dir = tmp_dir();
    {
        let w = open(&dir, true);
        let s = w.slot("s", "c").unwrap();
        s.check_record(1).unwrap();
        s.check_record(2).unwrap();
        s.check_record(3).unwrap();
        w.sync().unwrap();
        w.close().unwrap();
    }
    let path = dir.join("ackstore.log");
    let mut data = std::fs::read(&path).unwrap();
    let n = data.len();
    data[n - 6] ^= 0xFF; // corrupt a record near the end
    std::fs::write(&path, &data).unwrap();

    let w2 = open(&dir, false);
    let s = w2.slot("s", "c").unwrap();
    assert!(!s.check_record(1).unwrap(), "pre-corruption 1 survives");
    w2.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

// --- snapshot ---

#[test]
fn snapshot_restore() {
    let dir = tmp_dir();
    {
        let w = open(&dir, true);
        let s = w.slot("s", "c").unwrap();
        for seq in 1..=50 {
            s.check_record(seq).unwrap();
        }
        w.snapshot().unwrap();
        for seq in 51..=60 {
            s.check_record(seq).unwrap();
        }
        s.confirm(55).unwrap();
        w.sync().unwrap();
        w.close().unwrap();
    }
    let w2 = open(&dir, false);
    let s = w2.slot("s", "c").unwrap();
    for seq in 1..=60 {
        let is_new = s.check_record(seq).unwrap();
        if seq == 55 {
            assert!(is_new, "55 confirmed → fresh");
        } else {
            assert!(!is_new, "seq {seq} survives snapshot");
        }
    }
    w2.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

// --- ConfirmUpTo ---

#[test]
fn confirm_up_to() {
    let dir = tmp_dir();
    let w = open(&dir, false);
    let s = w.slot("s", "c").unwrap();
    for seq in 1..=100 {
        s.check_record(seq).unwrap();
    }
    let removed = s.confirm_up_to(50).unwrap();
    assert_eq!(removed, 50);
    for seq in 1..=50 {
        assert!(!s.seen(seq), "seq {seq} dropped");
    }
    for seq in 51..=100 {
        assert!(s.seen(seq), "seq {seq} kept");
    }
    assert_eq!(s.info().live, 50);
    w.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn confirm_up_to_survives_restart() {
    let dir = tmp_dir();
    {
        let w = open(&dir, true);
        let s = w.slot("s", "c").unwrap();
        for seq in 1..=100 {
            s.check_record(seq).unwrap();
        }
        s.confirm_up_to(60).unwrap();
        w.sync().unwrap();
        w.close().unwrap();
    }
    let w2 = open(&dir, false);
    let s = w2.slot("s", "c").unwrap();
    for seq in 1..=60 {
        assert!(s.check_record(seq).unwrap(), "seq {seq} fresh after confirm");
    }
    w2.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

// --- compaction ---

#[test]
fn compact_shrinks_file() {
    let dir = tmp_dir();
    let w = open(&dir, true);
    let s = w.slot("s", "c").unwrap();
    for seq in 1..=10_000 {
        s.check_record(seq).unwrap();
    }
    s.confirm_up_to(9990).unwrap();
    w.sync().unwrap();
    let before = w.metrics().file_size;
    w.compact().unwrap();
    let after = w.metrics().file_size;
    assert!(after < before, "compaction shrinks: {before} → {after}");
    for seq in 9991..=10_000 {
        assert!(s.seen(seq), "seq {seq} survives compaction");
    }
    assert!(!s.seen(5000), "confirmed 5000 not seen");
    assert!(s.check_record(20000).unwrap(), "write after compaction");
    w.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn compact_survives_restart() {
    let dir = tmp_dir();
    {
        let w = open(&dir, true);
        let s = w.slot("orders", "worker").unwrap();
        for seq in 1..=500 {
            s.check_record(seq).unwrap();
        }
        s.confirm_up_to(490).unwrap();
        w.sync().unwrap();
        w.compact().unwrap();
        w.close().unwrap();
    }
    let w2 = open(&dir, false);
    let s = w2.slot("orders", "worker").unwrap();
    for seq in 491..=500 {
        assert!(!s.check_record(seq).unwrap(), "seq {seq} survives compact+restart");
    }
    assert!(s.check_record(100).unwrap(), "confirmed 100 fresh");
    w2.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn auto_compaction() {
    let dir = tmp_dir();
    let mut cfg = WalConfig::new(&dir);
    cfg.compact_at_bytes = 8 * 1024;
    let w = Wal::open(cfg).unwrap();
    let s = w.slot("s", "c").unwrap();
    for round in 0..20u64 {
        let base = round * 1000;
        for i in 0..1000 {
            s.check_record(base + i).unwrap();
        }
        s.confirm_up_to(base + 999).unwrap();
        w.sync().unwrap();
    }
    let size = w.metrics().file_size;
    assert!(size <= 64 * 1024, "auto-compaction bounds file: {size}");
    w.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

/// Regression (audit finding 2): async handlers ack out of order, so seq 101
/// can be recorded before 100. A `min_seq = fifo.first()` (insertion order)
/// would set min=101 and make `seen(100)` short-circuit to false via the
/// bounds gate (100 < 101) without ever consulting the exact set — a silent
/// dedup miss. `min_seq` must track the true minimum.
#[test]
fn out_of_order_record_still_seen() {
    let dir = tmp_dir();
    let w = open(&dir, false);
    let s = w.slot("s", "c").unwrap();
    s.check_record(101).unwrap();
    s.check_record(100).unwrap();
    s.check_record(102).unwrap();
    assert!(s.seen(100), "100 recorded out of order must still be seen");
    assert!(s.seen(101), "101 seen");
    assert!(s.seen(102), "102 seen");
    assert!(!s.seen(99), "99 never recorded");
    w.close().unwrap();

    // Same invariant for the memory backend.
    let m = MemoryStore::new(0);
    let ms = m.slot("s", "c").unwrap();
    ms.check_record(101).unwrap();
    ms.check_record(100).unwrap();
    assert!(ms.seen(100), "memory: 100 out of order still seen");
    assert!(!ms.seen(99), "memory: 99 never recorded");

    std::fs::remove_dir_all(&dir).ok();
}

/// Regression (audit finding 1): a slot whose live set has been fully confirmed
/// (empty) must keep its Register through compaction. Otherwise a record
/// written to that still-live slot AFTER compaction has no Register earlier in
/// the file, so replay can't map its slot_id → names and silently drops it (and
/// could reuse the id for a different consumer).
#[test]
fn compact_empty_slot_then_record_survives_restart() {
    let dir = tmp_dir();
    {
        let w = open(&dir, true);
        let s = w.slot("jobs", "worker").unwrap();
        for seq in 1..=100 {
            s.check_record(seq).unwrap();
        }
        s.confirm_up_to(100).unwrap(); // live set now empty
        w.sync().unwrap();
        w.compact().unwrap(); // must NOT drop the (empty) slot's Register
        assert!(
            s.check_record(200).unwrap(),
            "record after compaction on the now-empty slot"
        );
        w.sync().unwrap();
        w.close().unwrap();
    }
    // Reopen: the post-compaction record must be recoverable — its Register
    // survived compaction so replay maps slot_id → (jobs, worker).
    let w2 = open(&dir, false);
    let s = w2.slot("jobs", "worker").unwrap();
    assert!(
        !s.check_record(200).unwrap(),
        "post-compaction record must survive restart (dedup holds)"
    );
    assert!(s.check_record(50).unwrap(), "confirmed 50 is fresh again");
    w2.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

// --- concurrency ---

#[test]
fn concurrent_stress_with_restart() {
    let dir = tmp_dir();
    {
        let w = open(&dir, false);
        let s = w.slot("s", "c").unwrap();
        let mut handles = Vec::new();
        for g in 0..8u64 {
            let s = s.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..125u64 {
                    s.check_record(g * 125 + i).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        w.sync().unwrap();
        w.close().unwrap();
    }
    let w2 = open(&dir, false);
    let s = w2.slot("s", "c").unwrap();
    let mut missing = 0;
    for seq in 0..1000 {
        if s.check_record(seq).unwrap() {
            missing += 1;
        }
    }
    assert_eq!(missing, 0, "no records lost across concurrent-write + restart");
    w2.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

// --- backend conformance ---

#[test]
fn memory_backend_dedup() {
    let m = MemoryStore::new(0);
    let s = m.slot("s", "c").unwrap();
    assert!(s.check_record(1).unwrap());
    assert!(!s.check_record(1).unwrap());
    assert_eq!(s.confirm_up_to(1).unwrap(), 1);
    assert!(s.check_record(1).unwrap(), "fresh after confirm_up_to");
}
