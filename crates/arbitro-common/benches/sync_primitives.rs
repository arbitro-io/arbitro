//! What a lock costs when nobody is fighting for it.
//!
//! The question this answers is narrow on purpose: **one thread, zero
//! contention, take the lock and put it back**. That is the case the
//! broker actually runs in once state has a single owner — the lock is
//! never contended, it is just still there, and the argument for removing
//! it has to be made against its uncontended cost, not its contended one.
//!
//! Contended numbers are a different measurement and are NOT here. A
//! contended `Mutex` costs a futex syscall and a context switch, which is
//! three orders of magnitude worse and would drown the thing being asked
//! about.
//!
//! Every row does the same work at the bottom — read a `u64`, add one,
//! write it back — so the difference between rows is the primitive and
//! nothing else. `plain` is the floor: the same work with no wrapper.
//!
//! Run pinned to one core. Anything else measures the scheduler.

use std::cell::{Cell, RefCell};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use criterion::{criterion_group, criterion_main, Criterion};

fn primitives(c: &mut Criterion) {
    let mut g = c.benchmark_group("uncontended");

    // The floor. Everything below is this plus a wrapper.
    let mut plain: u64 = 0;
    g.bench_function("plain/&mut", |b| {
        b.iter(|| {
            plain += 1;
            black_box(plain)
        })
    });

    let cell = Cell::new(0u64);
    g.bench_function("cell/get_set", |b| {
        b.iter(|| {
            cell.set(cell.get() + 1);
            black_box(cell.get())
        })
    });

    let refcell = RefCell::new(0u64);
    g.bench_function("refcell/borrow_mut", |b| {
        b.iter(|| {
            let mut v = refcell.borrow_mut();
            *v += 1;
            black_box(*v)
        })
    });

    let atomic = AtomicU64::new(0);
    g.bench_function("atomic/load_relaxed", |b| {
        b.iter(|| black_box(atomic.load(Ordering::Relaxed)))
    });
    g.bench_function("atomic/fetch_add_relaxed", |b| {
        b.iter(|| black_box(atomic.fetch_add(1, Ordering::Relaxed)))
    });

    let std_mutex = Mutex::new(0u64);
    g.bench_function("std_mutex/lock", |b| {
        b.iter(|| {
            let mut v = std_mutex.lock().unwrap();
            *v += 1;
            black_box(*v)
        })
    });

    let std_rw = RwLock::new(0u64);
    g.bench_function("std_rwlock/read", |b| {
        b.iter(|| {
            let v = std_rw.read().unwrap();
            black_box(*v)
        })
    });
    g.bench_function("std_rwlock/write", |b| {
        b.iter(|| {
            let mut v = std_rw.write().unwrap();
            *v += 1;
            black_box(*v)
        })
    });

    let pl_mutex = parking_lot::Mutex::new(0u64);
    g.bench_function("parking_lot_mutex/lock", |b| {
        b.iter(|| {
            let mut v = pl_mutex.lock();
            *v += 1;
            black_box(*v)
        })
    });

    let pl_rw = parking_lot::RwLock::new(0u64);
    g.bench_function("parking_lot_rwlock/read", |b| {
        b.iter(|| {
            let v = pl_rw.read();
            black_box(*v)
        })
    });
    g.bench_function("parking_lot_rwlock/write", |b| {
        b.iter(|| {
            let mut v = pl_rw.write();
            *v += 1;
            black_box(*v)
        })
    });

    // What the name registry's hot reads actually pay: acquiring the
    // guard, not indexing through it.
    let arc_swap = arc_swap::ArcSwap::from_pointee(0u64);
    g.bench_function("arc_swap/load", |b| {
        b.iter(|| {
            let g = arc_swap.load();
            black_box(**g)
        })
    });
    g.bench_function("arc_swap/load_full", |b| {
        b.iter(|| black_box(*arc_swap.load_full()))
    });

    g.finish();
}

criterion_group!(benches, primitives);
criterion_main!(benches);
