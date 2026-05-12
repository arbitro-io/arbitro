---
description: Real-world optimization best practices for high-performance systems
arguments: []
---

# PERFORMANCE OPTIMIZATION WORKFLOW

This workflow applies modern systems engineering practices to optimize the hot paths of the broker.

## 1. Zero-Copy & Allocation Audit
- [ ] **Hot Path Allocations**: Run with a heap profiler (e.g., `dhat`). Zero `malloc`/`free` calls should occur during steady-state Publish/Claim/Ack.
- [ ] **Redundant Copies**: Audit `ShardCommand` and `transport.rs`. Identify any `Vec::to_vec()` or `String::from()` on data that could travel as `&[u8]` or `Bytes`.
- [ ] **SmallVec/ArrayVec**: Replace `Vec<T>` with stack-allocated containers for small, bounded collections (e.g., `<8` entries).

## 2. Cache & Memory Layout
- [ ] **Cache Line Alignment**: Verify that critical hot structs (ring buffer entries, counters) are `#[repr(C)]` and padded to 64 bytes to prevent false sharing.
- [ ] **Data Locality**: Audit lookup tables. Ensure dense IDs use direct indexing (`Vec<T>`) to maximize prefetcher efficiency.
- [ ] **Pointer Chasing**: Identify and eliminate unnecessary indirection (e.g., `Box<T>` or `Arc<T>` on hot path internals).

## 3. Syscall & I/O Efficiency
- [ ] **Vectored I/O**: Ensure `drain.rs` uses `write_vectored` (gathering multiple frame headers and bodies) to minimize syscall frequency.
- [ ] **Coalescing Logic**: Audit the `Flusher` and `Accumulator`. Verify that they successfully batch small writes into large, sequential disk append operations.
- [ ] **Timestamp Throttling**: Use a single `Timestamp` per batch. Never call `Instant::now()` or clock syscalls inside per-message loops.

## 4. Lock & Thread Contention
- [ ] **Lock-Free Primitives**: Replace `Mutex` with `Atomics` or `RwLock` where appropriate. Use `ArcSwap` for topology snapshots that are read frequently but updated rarely.
- [ ] **Sharding Granularity**: Verify that sharding (e.g., `64-way` StreamMap) is sufficient to prevent contention during high parallel load.
- [ ] **Thread Pinning**: In ultra-low latency scenarios, consider pinning shard threads to specific CPU cores to avoid context switch overhead and cache migration.

## 5. Benchmarking & Profiling
- [ ] **WSL-Native Benches**: Always run benchmarks in native WSL (`/tmp/arbitro`) to avoid the 9P bridge overhead.
- [ ] **Criterion Analysis**: Use `Criterion.rs` for micro-benchmarks. Look for regressions in `ns/iter` after every structural change.
- [ ] **Flamegraphs**: Generate flamegraphs to identify unexpected "purple" (kernel) or "green" (non-application) time in the hot path.
