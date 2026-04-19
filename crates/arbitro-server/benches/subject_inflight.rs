//! Subject inflight — measure before proposing any change.
//!
//! Contenders:
//! 1. BucketArray      — `Box<[AtomicU32]>` with hash % N (actual)
//! 2. HashMapAHash     — `HashMap<u32, u32, ahash>` (single-thread baseline,
//!                        NOT usable concurrently — measures HashMap overhead
//!                        without lock noise)
//! 3. RwLockHashMap    — `RwLock<HashMap<u32, AtomicU32, ahash>>`
//! 4. Papaya           — `papaya::HashMap<u32, AtomicU32>`
//!
//! Workloads:
//! A. Read-heavy hot (drain check on N hot subjects)
//! B. Read-heavy cold (drain check on 10k distinct subjects)
//! C. Inc-only (delivery hot path, single writer)
//! D. Concurrent realistic:
//!    - Reader thread: 99% has_room + 1% inc (drain)
//!    - Writer thread: dec trickle (command, ACKs)
//!    Measures reader throughput under realistic contention.

#![allow(unused)]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

// ── 1. BucketArray (current) ───────────────────────────────────────────────

const SLOTS: usize = 16384;

struct BucketArray {
    buckets: Box<[AtomicU32]>,
}

impl BucketArray {
    fn new() -> Self {
        let mut v = Vec::with_capacity(SLOTS);
        for _ in 0..SLOTS {
            v.push(AtomicU32::new(0));
        }
        Self { buckets: v.into_boxed_slice() }
    }
    #[inline(always)]
    fn slot(hash: u32) -> usize { hash as usize % SLOTS }
    #[inline(always)]
    fn has_room(&self, hash: u32, max: u32) -> bool {
        self.buckets[Self::slot(hash)].load(Ordering::Relaxed) < max
    }
    #[inline(always)]
    fn inc(&self, hash: u32) {
        self.buckets[Self::slot(hash)].fetch_add(1, Ordering::Relaxed);
    }
    #[inline(always)]
    fn dec(&self, hash: u32) {
        self.buckets[Self::slot(hash)].fetch_sub(1, Ordering::Relaxed);
    }
}

// ── 2. HashMap + ahash (single-thread baseline) ────────────────────────────
// NOT concurrent. Shows the HashMap overhead without lock noise.

struct HashMapAHash {
    map: std::cell::UnsafeCell<HashMap<u32, u32, ahash::RandomState>>,
}
unsafe impl Sync for HashMapAHash {} // for bench single-thread use only

impl HashMapAHash {
    fn new() -> Self {
        Self {
            map: std::cell::UnsafeCell::new(
                HashMap::with_hasher(ahash::RandomState::new()),
            ),
        }
    }
    #[inline]
    fn has_room(&self, hash: u32, max: u32) -> bool {
        let m = unsafe { &*self.map.get() };
        match m.get(&hash) {
            Some(c) => *c < max,
            None => true,
        }
    }
    #[inline]
    fn inc(&self, hash: u32) {
        let m = unsafe { &mut *self.map.get() };
        *m.entry(hash).or_insert(0) += 1;
    }
    #[inline]
    fn dec(&self, hash: u32) {
        let m = unsafe { &mut *self.map.get() };
        if let Some(c) = m.get_mut(&hash) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                m.remove(&hash);
            }
        }
    }
}

// ── 3. RwLock<HashMap + ahash> ─────────────────────────────────────────────

struct RwLockHashMap {
    map: RwLock<HashMap<u32, AtomicU32, ahash::RandomState>>,
}

impl RwLockHashMap {
    fn new() -> Self {
        Self { map: RwLock::new(HashMap::with_hasher(ahash::RandomState::new())) }
    }
    #[inline]
    fn has_room(&self, hash: u32, max: u32) -> bool {
        let g = self.map.read().unwrap();
        match g.get(&hash) {
            Some(c) => c.load(Ordering::Relaxed) < max,
            None => true,
        }
    }
    #[inline]
    fn inc(&self, hash: u32) {
        {
            let g = self.map.read().unwrap();
            if let Some(c) = g.get(&hash) {
                c.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        let mut g = self.map.write().unwrap();
        g.entry(hash).or_insert_with(|| AtomicU32::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    fn dec(&self, hash: u32) {
        let prev;
        {
            let g = self.map.read().unwrap();
            match g.get(&hash) {
                Some(c) => prev = c.fetch_sub(1, Ordering::Relaxed),
                None => return,
            }
        }
        if prev == 1 {
            let mut g = self.map.write().unwrap();
            if let Some(c) = g.get(&hash) {
                if c.load(Ordering::Relaxed) == 0 {
                    g.remove(&hash);
                }
            }
        }
    }
}

// ── 4. papaya ──────────────────────────────────────────────────────────────

struct Papaya {
    map: papaya::HashMap<u32, AtomicU32, ahash::RandomState>,
}

impl Papaya {
    fn new() -> Self {
        Self {
            map: papaya::HashMap::builder()
                .hasher(ahash::RandomState::new())
                .build(),
        }
    }
    #[inline]
    fn has_room(&self, hash: u32, max: u32) -> bool {
        let g = self.map.pin();
        match g.get(&hash) {
            Some(c) => c.load(Ordering::Relaxed) < max,
            None => true,
        }
    }
    #[inline]
    fn inc(&self, hash: u32) {
        let g = self.map.pin();
        match g.get(&hash) {
            Some(c) => { c.fetch_add(1, Ordering::Relaxed); }
            None => { let _ = g.get_or_insert_with(hash, || AtomicU32::new(1)); }
        }
    }
    #[inline]
    fn dec(&self, hash: u32) {
        let g = self.map.pin();
        if let Some(c) = g.get(&hash) {
            let prev: u32 = c.fetch_sub(1, Ordering::Relaxed);
            if prev == 1 {
                g.remove(&hash);
            }
        }
    }
}

// ── Workload helpers ───────────────────────────────────────────────────────

fn gen_hashes(count: usize, distinct: bool) -> Vec<u32> {
    if distinct {
        (0..count).map(|i| (i as u32).wrapping_mul(2654435761)).collect()
    } else {
        let pool: Vec<u32> = (0..32)
            .map(|i| (i as u32).wrapping_mul(2654435761))
            .collect();
        (0..count).map(|i| pool[i % pool.len()]).collect()
    }
}

fn ns_per(elapsed: Duration, iters: u64) -> f64 {
    elapsed.as_nanos() as f64 / iters as f64
}

// ── Workload A: Read-heavy hot (drain check) ───────────────────────────────

fn bench_read<F: Fn(u32, u32) -> bool>(label: &str, hashes: &[u32], iters: u64, f: F) {
    for &h in hashes.iter().take(256) {
        black_box(f(h, 100));
    }
    let start = Instant::now();
    for i in 0..iters {
        let h = hashes[(i as usize) % hashes.len()];
        black_box(f(h, 100));
    }
    let el = start.elapsed();
    println!(
        "  read  {label:18} | {:>6.2} ns/op | {:>12.0} ops/s",
        ns_per(el, iters),
        iters as f64 / el.as_secs_f64()
    );
}

// ── Workload C: Inc-only (delivery) ────────────────────────────────────────

fn bench_inc<F: Fn(u32)>(label: &str, hashes: &[u32], iters: u64, f: F) {
    for &h in hashes.iter().take(256) {
        f(h);
    }
    let start = Instant::now();
    for i in 0..iters {
        let h = hashes[(i as usize) % hashes.len()];
        f(h);
    }
    let el = start.elapsed();
    println!(
        "  inc   {label:18} | {:>6.2} ns/op | {:>12.0} ops/s",
        ns_per(el, iters),
        iters as f64 / el.as_secs_f64()
    );
}

// ── Workload D: Concurrent realistic ───────────────────────────────────────
//
// Reader thread: 99% has_room + 1% inc (simulates drain delivery pattern)
// Writer thread: dec at controlled rate (simulates ACK arrival)
// Reports: reader throughput.

struct ConcurrentStats {
    reader_reads: AtomicU64,
    reader_incs: AtomicU64,
    writer_decs: AtomicU64,
}

fn bench_concurrent<S, R, W, Dec>(
    label: &str,
    duration: Duration,
    state: Arc<S>,
    read: R,
    write: W,
    dec: Dec,
    hashes: Vec<u32>,
) where
    S: Send + Sync + 'static,
    R: Fn(&S, u32, u32) -> bool + Send + Sync + 'static,
    W: Fn(&S, u32) + Send + Sync + 'static,
    Dec: Fn(&S, u32) + Send + Sync + 'static,
{
    let stats = Arc::new(ConcurrentStats {
        reader_reads: AtomicU64::new(0),
        reader_incs: AtomicU64::new(0),
        writer_decs: AtomicU64::new(0),
    });
    let stop = Arc::new(AtomicBool::new(false));

    let reader_h = {
        let state = state.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let hashes = hashes.clone();
        std::thread::spawn(move || {
            let mut i: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let h = hashes[(i as usize) % hashes.len()];
                // 99 reads per 1 inc
                for _ in 0..99 {
                    black_box(read(&state, h, 100));
                }
                write(&state, h);
                stats.reader_reads.fetch_add(99, Ordering::Relaxed);
                stats.reader_incs.fetch_add(1, Ordering::Relaxed);
                i = i.wrapping_add(1);
            }
        })
    };

    let writer_h = {
        let state = state.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let hashes = hashes.clone();
        std::thread::spawn(move || {
            // Writer runs slower — simulates ACK trickle.
            let mut i: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let h = hashes[(i as usize) % hashes.len()];
                dec(&state, h);
                stats.writer_decs.fetch_add(1, Ordering::Relaxed);
                // Throttle: ACKs come at ~1/10th the rate of deliveries
                std::thread::sleep(Duration::from_micros(10));
                i = i.wrapping_add(1);
            }
        })
    };

    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);
    reader_h.join().unwrap();
    writer_h.join().unwrap();

    let reads = stats.reader_reads.load(Ordering::Relaxed);
    let incs = stats.reader_incs.load(Ordering::Relaxed);
    let decs = stats.writer_decs.load(Ordering::Relaxed);
    let total_reads_per_s = reads as f64 / duration.as_secs_f64();
    let ns_per_read = duration.as_nanos() as f64 / reads as f64;

    println!(
        "  2thd  {label:18} | {:>6.2} ns/read | {:>12.0} reads/s | incs={} decs={}",
        ns_per_read, total_reads_per_s, incs, decs
    );
}

fn main() {
    println!("\nSubject inflight — baseline + concurrent strategies");
    println!("====================================================\n");

    let iters: u64 = 5_000_000;

    // ── Workload A: read-heavy hot ─────────────────────────────────
    println!("── A. Read-heavy, 32 hot subjects (drain `has_room`) ──\n");
    let hashes = gen_hashes(1024, false);

    let bucket = BucketArray::new();
    let hm_ahash = HashMapAHash::new();
    let rwlock_hm = RwLockHashMap::new();
    let papaya = Papaya::new();
    // Seed all with 1 inflight each
    for &h in &hashes[..32] {
        bucket.inc(h);
        hm_ahash.inc(h);
        rwlock_hm.inc(h);
        papaya.inc(h);
    }

    bench_read("BucketArray",    &hashes, iters, |h, m| bucket.has_room(h, m));
    bench_read("HashMap+ahash",  &hashes, iters, |h, m| hm_ahash.has_room(h, m));
    bench_read("RwLock<HashMap>",&hashes, iters, |h, m| rwlock_hm.has_room(h, m));
    bench_read("Papaya",         &hashes, iters, |h, m| papaya.has_room(h, m));

    // ── Workload B: read-heavy cold (10k distinct) ─────────────────
    println!("\n── B. Read-heavy, 10k distinct subjects (cardinality) ──\n");
    let hashes = gen_hashes(10_000, true);

    let bucket = BucketArray::new();
    let hm_ahash = HashMapAHash::new();
    let rwlock_hm = RwLockHashMap::new();
    let papaya = Papaya::new();
    for &h in &hashes {
        bucket.inc(h);
        hm_ahash.inc(h);
        rwlock_hm.inc(h);
        papaya.inc(h);
    }

    bench_read("BucketArray",    &hashes, iters, |h, m| bucket.has_room(h, m));
    bench_read("HashMap+ahash",  &hashes, iters, |h, m| hm_ahash.has_room(h, m));
    bench_read("RwLock<HashMap>",&hashes, iters, |h, m| rwlock_hm.has_room(h, m));
    bench_read("Papaya",         &hashes, iters, |h, m| papaya.has_room(h, m));

    // ── Workload C: inc-only (delivery) ────────────────────────────
    println!("\n── C. Inc-only, 32 hot subjects (delivery hot path) ──\n");
    let hashes = gen_hashes(1024, false);

    let bucket = BucketArray::new();
    let hm_ahash = HashMapAHash::new();
    let rwlock_hm = RwLockHashMap::new();
    let papaya = Papaya::new();

    bench_inc("BucketArray",    &hashes, iters, |h| bucket.inc(h));
    bench_inc("HashMap+ahash",  &hashes, iters, |h| hm_ahash.inc(h));
    bench_inc("RwLock<HashMap>",&hashes, iters, |h| rwlock_hm.inc(h));
    bench_inc("Papaya",         &hashes, iters, |h| papaya.inc(h));

    // ── Workload D: concurrent realistic ───────────────────────────
    println!("\n── D. Concurrent: drain (99% read + 1% inc) + command (dec trickle) ──\n");
    let dur = Duration::from_secs(2);
    let hashes = gen_hashes(256, false);

    let bucket = Arc::new(BucketArray::new());
    for &h in &hashes[..32] { bucket.inc(h); }
    bench_concurrent(
        "BucketArray", dur, bucket,
        |s, h, m| s.has_room(h, m),
        |s, h| s.inc(h),
        |s, h| s.dec(h),
        hashes.clone(),
    );

    let rwlock_hm = Arc::new(RwLockHashMap::new());
    for &h in &hashes[..32] { rwlock_hm.inc(h); }
    bench_concurrent(
        "RwLock<HashMap>", dur, rwlock_hm,
        |s, h, m| s.has_room(h, m),
        |s, h| s.inc(h),
        |s, h| s.dec(h),
        hashes.clone(),
    );

    let papaya = Arc::new(Papaya::new());
    for &h in &hashes[..32] { papaya.inc(h); }
    bench_concurrent(
        "Papaya", dur, papaya,
        |s, h, m| s.has_room(h, m),
        |s, h| s.inc(h),
        |s, h| s.dec(h),
        hashes.clone(),
    );

    // ── Workload E: write-heavy churn (NEW keys + remove-at-zero bursts) ─
    //
    // Reader: continuous has_room on a stable pool of 256 hot subjects.
    // Writer: infinite churn — insert fresh key (new hash every iter), then
    //         immediately dec it to 0 → forces remove.
    //         Exercises BOTH write-lock paths: `entry().or_insert()` AND
    //         `map.remove()` in RwLock<HashMap>.
    //
    // This is the realistic production pattern: new subjects constantly
    // appearing (different subject strings on every request) + ACKs
    // decrementing them to 0.
    println!("\n── E. Write-churn: insert new key + remove-at-zero (stresses WRITE lock) ──\n");
    let dur = Duration::from_secs(2);
    let hot_hashes = gen_hashes(256, false);

    let rwlock_hm = Arc::new(RwLockHashMap::new());
    for &h in &hot_hashes[..32] { rwlock_hm.inc(h); }
    bench_churn(
        "RwLock<HashMap>", dur, rwlock_hm,
        |s, h, m| s.has_room(h, m),
        |s, h| s.inc(h),
        |s, h| s.dec(h),
        hot_hashes.clone(),
    );

    let papaya = Arc::new(Papaya::new());
    for &h in &hot_hashes[..32] { papaya.inc(h); }
    bench_churn(
        "Papaya", dur, papaya,
        |s, h, m| s.has_room(h, m),
        |s, h| s.inc(h),
        |s, h| s.dec(h),
        hot_hashes.clone(),
    );

    let bucket = Arc::new(BucketArray::new());
    for &h in &hot_hashes[..32] { bucket.inc(h); }
    bench_churn(
        "BucketArray", dur, bucket,
        |s, h, m| s.has_room(h, m),
        |s, h| s.inc(h),
        |s, h| s.dec(h),
        hot_hashes.clone(),
    );

    println!();
}

// ── Workload E helper: reader vs full-throttle write churn ────────────────
//
// Reader thread: reads `has_room` on `hot_hashes` in a rotating pattern.
// Writer thread: infinite loop of (inc(seed), dec(seed)) with seed starting
//                at a value FAR from the reader's pool to guarantee each
//                inc creates a NEW key (exercising write lock on insert)
//                and each dec reaches 0 (exercising write lock on remove).
//
// Reports reader throughput under real write-lock contention.
fn bench_churn<S, R, Inc, Dec>(
    label: &str,
    duration: Duration,
    state: Arc<S>,
    read: R,
    inc: Inc,
    dec: Dec,
    hot_hashes: Vec<u32>,
) where
    S: Send + Sync + 'static,
    R: Fn(&S, u32, u32) -> bool + Send + Sync + 'static,
    Inc: Fn(&S, u32) + Send + Sync + 'static,
    Dec: Fn(&S, u32) + Send + Sync + 'static,
{
    let stats = Arc::new(ConcurrentStats {
        reader_reads: AtomicU64::new(0),
        reader_incs: AtomicU64::new(0),
        writer_decs: AtomicU64::new(0),
    });
    let stop = Arc::new(AtomicBool::new(false));

    // Reader: drain scenario — pure has_room calls on hot pool.
    let reader_h = {
        let state = state.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        let hashes = hot_hashes.clone();
        std::thread::spawn(move || {
            let mut i: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let h = hashes[(i as usize) % hashes.len()];
                black_box(read(&state, h, 100));
                stats.reader_reads.fetch_add(1, Ordering::Relaxed);
                i = i.wrapping_add(1);
            }
        })
    };

    // Writer: churn cycle — fresh key every iter.
    // inc(new_key) → forces write-lock insert path
    // dec(new_key) → count hits 0 → forces write-lock remove path
    let writer_h = {
        let state = state.clone();
        let stats = stats.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            // Start far from reader's pool — never collides.
            let mut seed: u32 = 0xDEAD_0000;
            while !stop.load(Ordering::Relaxed) {
                inc(&state, seed);
                stats.reader_incs.fetch_add(1, Ordering::Relaxed);
                dec(&state, seed);
                stats.writer_decs.fetch_add(1, Ordering::Relaxed);
                seed = seed.wrapping_add(1);
            }
        })
    };

    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);
    reader_h.join().unwrap();
    writer_h.join().unwrap();

    let reads = stats.reader_reads.load(Ordering::Relaxed);
    let incs = stats.reader_incs.load(Ordering::Relaxed);
    let decs = stats.writer_decs.load(Ordering::Relaxed);
    let total_reads_per_s = reads as f64 / duration.as_secs_f64();
    let ns_per_read = duration.as_nanos() as f64 / reads as f64;
    let writer_churn_per_s = incs as f64 / duration.as_secs_f64();

    println!(
        "  churn {label:18} | {:>6.2} ns/read | {:>12.0} reads/s | writer_churn={:>10.0} cycles/s",
        ns_per_read, total_reads_per_s, writer_churn_per_s
    );
}
