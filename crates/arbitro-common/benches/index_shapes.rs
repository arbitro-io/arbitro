//! What an index costs to fill, read and empty.
//!
//! The catalog keeps `HashMap<K, Vec<BindingId>>` membership lists. The
//! question is whether a `HashSet` should replace the `Vec`: it removes in
//! O(1) with no bookkeeping, while the `Vec` needs either a scan or a
//! stored position.
//!
//! Insert, iterate and remove are measured separately because they do not
//! happen at the same rate. A binding is inserted once, removed once, and
//! its stream's list is walked by the drain EVERY cycle -- so a shape that
//! wins on removal and loses on iteration is losing.
//!
//! Sizes are steady-state so growth is not being measured: each iteration
//! fills a container that already owns its capacity.

use std::collections::HashSet;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

type FS = foldhash::fast::FixedState;

/// Per-element insert into a container that already has capacity.
fn insert(c: &mut Criterion) {
    let mut g = c.benchmark_group("index/insert");
    for &n in &[8usize, 200, 1000] {
        g.throughput(Throughput::Elements(n as u64));

        g.bench_function(BenchmarkId::new("vec_push", n), |b| {
            let mut v: Vec<u32> = Vec::with_capacity(n);
            b.iter(|| {
                v.clear();
                for i in 0..n as u32 {
                    v.push(black_box(i));
                }
                black_box(v.len())
            })
        });

        g.bench_function(BenchmarkId::new("hashset_insert", n), |b| {
            let mut s: HashSet<u32, FS> = HashSet::with_capacity_and_hasher(n, FS::default());
            b.iter(|| {
                s.clear();
                for i in 0..n as u32 {
                    s.insert(black_box(i));
                }
                black_box(s.len())
            })
        });
    }
    g.finish();
}

/// Walking the whole list -- what the drain does every cycle.
fn iterate(c: &mut Criterion) {
    let mut g = c.benchmark_group("index/iterate");
    for &n in &[8usize, 200, 1000, 30_000] {
        g.throughput(Throughput::Elements(n as u64));
        let v: Vec<u32> = (0..n as u32).collect();
        let s: HashSet<u32, FS> = (0..n as u32).collect();

        g.bench_function(BenchmarkId::new("vec", n), |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for x in v.iter() {
                    acc += *x as u64;
                }
                black_box(acc)
            })
        });
        g.bench_function(BenchmarkId::new("hashset", n), |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for x in s.iter() {
                    acc += *x as u64;
                }
                black_box(acc)
            })
        });

        // Same set, built at the exact size instead of grown by doubling.
        // `collect` overshoots -- 1000 elements land in ~1792 slots -- and
        // iteration visits the empty ones too. This isolates how much of
        // the gap is that overshoot rather than the shape itself.
        let tight: HashSet<u32, FS> = {
            let mut t = HashSet::with_capacity_and_hasher(n, FS::default());
            t.extend(0..n as u32);
            t
        };
        g.bench_function(BenchmarkId::new("hashset_tight", n), |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for x in tight.iter() {
                    acc += *x as u64;
                }
                black_box(acc)
            })
        });
    }
    g.finish();
}

/// Emptying the whole index one element at a time -- the teardown shape.
///
/// `vec_scan` is what the code does today. `vec_known_pos` is the same Vec
/// with the position already in hand, which is what storing it on the
/// binding would buy. `hashset_remove` needs no position at all.
fn remove(c: &mut Criterion) {
    let mut g = c.benchmark_group("index/remove_all");
    for &n in &[8usize, 200, 1000, 30_000] {
        g.throughput(Throughput::Elements(n as u64));
        let base: Vec<u32> = (0..n as u32).collect();

        // The scan is O(n^2) to empty; at 30k that is 450M compares per
        // iteration, which would take minutes and tell us nothing we do
        // not already know from the 1000 case.
        if n <= 1000 {
        g.bench_function(BenchmarkId::new("vec_scan", n), |b| {
            b.iter_batched(
                || base.clone(),
                |mut v| {
                    for id in 0..n as u32 {
                        if let Some(pos) = v.iter().position(|x| *x == id) {
                            v.swap_remove(pos);
                        }
                    }
                    black_box(v.len())
                },
                criterion::BatchSize::SmallInput,
            )
        });
        }

        // Removing from the tail backwards: the position is known and no
        // element ever moves, which is the best case a stored position can
        // reach. The realistic case pays one fixup per out-of-order remove.
        g.bench_function(BenchmarkId::new("vec_known_pos", n), |b| {
            b.iter_batched(
                || base.clone(),
                |mut v| {
                    for _ in 0..n {
                        let pos = v.len() - 1;
                        v.swap_remove(pos);
                    }
                    black_box(v.len())
                },
                criterion::BatchSize::SmallInput,
            )
        });

        g.bench_function(BenchmarkId::new("hashset_remove", n), |b| {
            b.iter_batched(
                || -> HashSet<u32, FS> { (0..n as u32).collect() },
                |mut s| {
                    for id in 0..n as u32 {
                        s.remove(&id);
                    }
                    black_box(s.len())
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    g.finish();
}

/// Filling an index from EMPTY, allocation and growth included.
///
/// The other groups reuse a container that already owns its capacity, so
/// they measure the shape and not the allocator. This one measures what a
/// stream's list actually costs over its life: `Vec` grows by doubling,
/// copying everything each time, and `HashSet` also rehashes.
///
/// `with_capacity` is the floor -- what it would cost if the final size
/// were known up front.
fn grow(c: &mut Criterion) {
    let mut g = c.benchmark_group("index/grow_from_empty");
    for &n in &[8usize, 200, 1000, 30_000] {
        g.throughput(Throughput::Elements(n as u64));

        g.bench_function(BenchmarkId::new("vec_push", n), |b| {
            b.iter(|| {
                let mut v: Vec<u32> = Vec::new();
                for i in 0..n as u32 {
                    v.push(black_box(i));
                }
                black_box(v.len())
            })
        });

        g.bench_function(BenchmarkId::new("vec_with_capacity", n), |b| {
            b.iter(|| {
                let mut v: Vec<u32> = Vec::with_capacity(n);
                for i in 0..n as u32 {
                    v.push(black_box(i));
                }
                black_box(v.len())
            })
        });

        g.bench_function(BenchmarkId::new("hashset_insert", n), |b| {
            b.iter(|| {
                let mut s: HashSet<u32, FS> = HashSet::default();
                for i in 0..n as u32 {
                    s.insert(black_box(i));
                }
                black_box(s.len())
            })
        });
    }
    g.finish();
}

/// ONE removal: does this value exist, and take it out.
///
/// This is what an index does when a single binding retires -- not the
/// bulk empty measured above. The element removed sits in the MIDDLE, so
/// a scan pays its average rather than its best or worst case.
///
/// `hashset_remove` checks and removes in one probe and needs no position.
/// `vec_scan` is today's code. `vec_known_pos` is what storing the
/// position buys, including the fixup the moved element needs.
fn remove_one(c: &mut Criterion) {
    let mut g = c.benchmark_group("index/remove_one");
    for &n in &[8usize, 200, 1000, 30_000] {
        let target = (n / 2) as u32;
        let base: Vec<u32> = (0..n as u32).collect();

        g.bench_function(BenchmarkId::new("vec_scan", n), |b| {
            b.iter_batched_ref(
                || base.clone(),
                |v| {
                    if let Some(pos) = v.iter().position(|x| *x == black_box(target)) {
                        black_box(v.swap_remove(pos));
                    }
                },
                criterion::BatchSize::SmallInput,
            )
        });

        g.bench_function(BenchmarkId::new("vec_known_pos", n), |b| {
            b.iter_batched_ref(
                || base.clone(),
                |v| {
                    let pos = black_box(target as usize);
                    black_box(v.swap_remove(pos));
                    // The element that moved into the hole now has a stale
                    // stored position; in the catalog this is one lookup to
                    // correct it. Counted here as the read it would be.
                    if pos < v.len() {
                        black_box(v[pos]);
                    }
                },
                criterion::BatchSize::SmallInput,
            )
        });

        g.bench_function(BenchmarkId::new("hashset_remove", n), |b| {
            b.iter_batched_ref(
                || -> HashSet<u32, FS> { (0..n as u32).collect() },
                |s| black_box(s.remove(&black_box(target))),
                criterion::BatchSize::SmallInput,
            )
        });
    }
    g.finish();
}

/// A bitmap over the same ids: the value IS the bit position.
///
/// Membership, insert and remove are one bit operation and do not care how
/// big the set is. Iteration is the opposite: it scans WORDS of the whole
/// id range, so it is fastest when the ids are packed and worst when a
/// handful of ids sit in a wide range -- which is this catalog's shape,
/// because binding ids are never reused.
///
/// `dense` = ids 0..n. `sparse` = n ids scattered over a 30k range, the
/// realistic case after churn.
fn bitmap(c: &mut Criterion) {
    let mut g = c.benchmark_group("index/bitmap");

    for &n in &[8usize, 1000] {
        // Dense: n ids in n slots.
        let words = n.div_ceil(64).max(1);
        let mut dense = vec![0u64; words];
        for i in 0..n {
            dense[i / 64] |= 1 << (i % 64);
        }
        // Sparse: the same n ids spread over 30k, as churn would leave them.
        let range = 30_000usize;
        let mut sparse = vec![0u64; range.div_ceil(64)];
        let step = range / n.max(1);
        for k in 0..n {
            let id = k * step;
            sparse[id / 64] |= 1 << (id % 64);
        }
        let v: Vec<u32> = (0..n as u32).collect();

        g.throughput(Throughput::Elements(n as u64));
        g.bench_function(BenchmarkId::new("iterate_dense", n), |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for (wi, w) in dense.iter().enumerate() {
                    let mut bits = *w;
                    while bits != 0 {
                        acc += (wi * 64) as u64 + bits.trailing_zeros() as u64;
                        bits &= bits - 1;
                    }
                }
                black_box(acc)
            })
        });
        g.bench_function(BenchmarkId::new("iterate_sparse_over_30k", n), |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for (wi, w) in sparse.iter().enumerate() {
                    let mut bits = *w;
                    while bits != 0 {
                        acc += (wi * 64) as u64 + bits.trailing_zeros() as u64;
                        bits &= bits - 1;
                    }
                }
                black_box(acc)
            })
        });
        g.bench_function(BenchmarkId::new("iterate_vec", n), |b| {
            b.iter(|| {
                let mut acc = 0u64;
                for x in v.iter() {
                    acc += *x as u64;
                }
                black_box(acc)
            })
        });

        g.throughput(Throughput::Elements(1));
        let probe = (n / 2) as u32;
        g.bench_function(BenchmarkId::new("contains_bit", n), |b| {
            b.iter(|| {
                let i = black_box(probe) as usize;
                black_box(dense[i / 64] & (1 << (i % 64)) != 0)
            })
        });
        g.bench_function(BenchmarkId::new("remove_bit", n), |b| {
            let mut m = dense.clone();
            b.iter(|| {
                let i = black_box(probe) as usize;
                m[i / 64] &= !(1u64 << (i % 64));
                black_box(m[i / 64])
            })
        });
    }
    g.finish();
}

criterion_group!(benches, insert, iterate, remove, grow, remove_one, bitmap);
criterion_main!(benches);
