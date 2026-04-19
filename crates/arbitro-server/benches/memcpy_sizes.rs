//! memcpy cost across sizes — how does copy time scale with buffer size?
//!
//! Measures three primitives at a ladder of sizes from 64 B to 1 MB:
//!
//!   - `copy_from_slice`   — idiomatic Rust, compiles to memcpy
//!   - `ptr::copy_nonoverlapping` — raw, same lowering
//!   - `BytesMut::extend_from_slice` — the one the drain uses
//!
//! Also models the TCP-batching decision:
//!
//!   N × small_buf  vs  1 × big_buf
//!
//! to show the amortisation effect of fewer-larger copies (same total bytes).
//!
//! Run:
//!   wsl bash -lc "cd /mnt/.../arbitro && \
//!     cargo bench --bench memcpy_sizes -p arbitro-server --no-run"
//!   wsl bash -lc "cp .../target/release/deps/memcpy_sizes-* /tmp/arbitro-bench/ \
//!     && cd /tmp/arbitro-bench && ./memcpy_sizes-* --bench"

#![allow(unused)]

use std::hint::black_box;
use std::time::Instant;

use bytes::BytesMut;

const SIZES: &[usize] = &[
    64,          //   64 B
    256,         //  256 B
    1024,        //   1 KB
    4096,        //   4 KB
    16_384,      //  16 KB
    65_536,      //  64 KB
    200_000,     // ~200 KB
    1_048_576,   //   1 MB
];

/// How many memcpys per size to measure.
fn iters_for_size(bytes: usize) -> usize {
    // Keep total work per size roughly constant (~500 MB moved) so timing is stable.
    (500 * 1024 * 1024 / bytes).max(1000)
}

fn fmt_size(b: usize) -> String {
    if b < 1024 {
        format!("{b} B")
    } else if b < 1024 * 1024 {
        format!("{:.1} KB", b as f64 / 1024.0)
    } else {
        format!("{:.1} MB", b as f64 / (1024.0 * 1024.0))
    }
}

fn bench_copy_from_slice(size: usize, iters: usize) -> f64 {
    let src = vec![0xA5u8; size];
    let mut dst = vec![0u8; size];

    // warmup
    for _ in 0..iters.min(1000) {
        dst.copy_from_slice(&src);
        black_box(&dst);
    }

    let start = Instant::now();
    for _ in 0..iters {
        dst.copy_from_slice(&src);
        black_box(&dst);
    }
    let elapsed = start.elapsed();
    elapsed.as_nanos() as f64 / iters as f64
}

fn bench_ptr_copy(size: usize, iters: usize) -> f64 {
    let src = vec![0xA5u8; size];
    let mut dst = vec![0u8; size];

    for _ in 0..iters.min(1000) {
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), size);
        }
        black_box(&dst);
    }

    let start = Instant::now();
    for _ in 0..iters {
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), size);
        }
        black_box(&dst);
    }
    let elapsed = start.elapsed();
    elapsed.as_nanos() as f64 / iters as f64
}

fn bench_bytesmut_extend(size: usize, iters: usize) -> f64 {
    let src = vec![0xA5u8; size];
    // Use a reusable BytesMut that we clear each iter to avoid constantly
    // allocating. This mirrors how the drain reuses its body buffer.
    let mut dst = BytesMut::with_capacity(size);

    for _ in 0..iters.min(1000) {
        dst.clear();
        dst.extend_from_slice(&src);
        black_box(&dst);
    }

    let start = Instant::now();
    for _ in 0..iters {
        dst.clear();
        dst.extend_from_slice(&src);
        black_box(&dst);
    }
    let elapsed = start.elapsed();
    elapsed.as_nanos() as f64 / iters as f64
}

fn main() {
    println!();
    println!("========================================================");
    println!("            memcpy cost vs buffer size");
    println!("========================================================");
    println!();
    println!(
        "  {:>10} | {:>12} | {:>12} | {:>14} | {:>10}",
        "size", "copy_from_slice", "ptr::copy_noo", "BytesMut.extend", "ns/byte"
    );
    println!("{}", "-".repeat(72));

    let mut results: Vec<(usize, f64)> = Vec::new();
    for &size in SIZES {
        let iters = iters_for_size(size);
        let t_cfs = bench_copy_from_slice(size, iters);
        let t_ptr = bench_ptr_copy(size, iters);
        let t_bm = bench_bytesmut_extend(size, iters);
        let ns_per_byte = t_cfs / size as f64;

        println!(
            "  {:>10} | {:>9.1} ns | {:>9.1} ns | {:>11.1} ns | {:>6.3} ns",
            fmt_size(size),
            t_cfs,
            t_ptr,
            t_bm,
            ns_per_byte,
        );
        results.push((size, t_cfs));
    }

    // ── Amortisation: N small copies vs 1 big copy (same total bytes) ───
    println!();
    println!("--------------------------------------------------------");
    println!("  Amortisation: N x small  vs  1 x big (same total bytes)");
    println!("--------------------------------------------------------");
    println!();
    println!(
        "  {:>18} | {:>11} | {:>11} | {:>8}",
        "scenario (same total)", "N x small", "1 x big", "speedup"
    );
    println!("{}", "-".repeat(60));

    let scenarios = [
        (64usize * 1024, 16usize, 4096usize),     //   1 MB total : 16 x 64KB  vs 4KB x 1 ? no. Let me fix.
    ];
    // Better: express "total bytes" explicitly.
    let totals = [
        ("200 KB", 200_000usize),
        ("1 MB  ", 1_048_576usize),
        ("4 MB  ", 4 * 1_048_576usize),
    ];
    let chunk_sizes = [4096usize, 16_384usize, 65_536usize];

    for &(label, total) in &totals {
        for &chunk in &chunk_sizes {
            if chunk >= total {
                continue;
            }
            let n_small = total / chunk;
            let iters = (1_000_000_000 / total).max(100);

            // N × small
            let src = vec![0xA5u8; chunk];
            let mut dst = BytesMut::with_capacity(total);
            for _ in 0..10 {
                dst.clear();
                for _ in 0..n_small {
                    dst.extend_from_slice(&src);
                }
                black_box(&dst);
            }
            let start = Instant::now();
            for _ in 0..iters {
                dst.clear();
                for _ in 0..n_small {
                    dst.extend_from_slice(&src);
                }
                black_box(&dst);
            }
            let t_small = start.elapsed().as_nanos() as f64 / iters as f64;

            // 1 × big
            let src_big = vec![0xA5u8; total];
            let mut dst_big = BytesMut::with_capacity(total);
            for _ in 0..10 {
                dst_big.clear();
                dst_big.extend_from_slice(&src_big);
                black_box(&dst_big);
            }
            let start = Instant::now();
            for _ in 0..iters {
                dst_big.clear();
                dst_big.extend_from_slice(&src_big);
                black_box(&dst_big);
            }
            let t_big = start.elapsed().as_nanos() as f64 / iters as f64;

            let speedup = t_small / t_big;
            println!(
                "  {label} {n_small:>4}x{chunksize:>5}B | {t_small:>8.2?}us | {t_big:>8.2?}us | {speedup:>5.2}x",
                chunksize = chunk,
                t_small = t_small / 1000.0,
                t_big = t_big / 1000.0,
            );
        }
    }

    println!();
}
