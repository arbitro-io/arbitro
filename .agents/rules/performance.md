---
description: Hardware sympathy and zero-copy rules — MANDATORY
---

# HARDWARE SYMPATHY & ZERO-COPY

## Zero-Copy
1. **No copies on hot path**: One copy max (into journal). Use `zerocopy::ref_from_bytes`.
2. **Lazy views**: Never parse fields you don't read.
3. **No `String`/`Vec`**: Use `&[u8]` or `Box<[u8]>`.
4. **No `copy_from_slice`**: Build headers on stack; use `send_parts`.
5. **Reuse buffers**: Pre-allocate scratch buffers; `.clear()` per batch.

## Cache Sympathy
6. **64B Alignment**: Hot structs must be cache-line aligned.
7. **No false sharing**: Pad mutable fields.
8. **Sequential access**: Arrays/Vecs beat pointer chasing.
9. **Small hot data**: Envelope=16B, PublishEntry=12B.

## Allocation Discipline
10. **Zero malloc on hot path**: Pre-allocate everything.
11. **`Box<[u8]>`**: 16B vs 24B for `Vec`.

## ID Storage — Dense vs Sparse (INVIOLABLE)
**Container determined by key shape.**

### Dense IDs (Monotonic 0..N) → `Vec<T>` / `Box<[T]>`
Lookup: **O(1) (~1.4 ns)** direct indexing.
- Applies to: `ConnectionId`, `ConsumerId`, `QueueId`, `StreamId`, `SubscriptionId`.
- **Enforcement**: `iter().find()` or `binary_search()` on dense keys is a violation.

### Sparse IDs (Hashes/User Bytes) → `HashMap` with `ahash`
Lookup: **O(1) (~3 ns)**.
- Applies to: `subject_hash`, content hashes.
- **Rule**: Standard `HashMap` (SipHash) is too slow; always use `ahash`.

### Decision Table
| ID Origin | Shape | Container | Cost |
|---|---|---|---|
| Registry (Bounded) | Dense 0..N | `Vec` direct index | ~1.4 ns |
| Registry (Monotonic) | Dense-unbounded | `HashMap + ahash` | ~3 ns |
| Content Hash | Sparse u32/u64 | `HashMap + ahash` | ~3 ns |
| Composite / Any | Any | `HashMap + ahash` | ~3.5 ns |
| Small Cache (≤8) | Any | `ArrayVec` linear scan | ~1.5 ns |

## Syscall Minimization
13. **Batch I/O**: `write_vectored` for multiple frames.
14. **No `Instant::now()`**: Pass timestamps from caller.
15. **No logging**: Info/Tracing only on cold paths.

## Branch Prediction
16. **Batch-as-standard**: Single message = batch(1).
17. **Predictable dispatch**: Order match arms by frequency (Publish/Deliver/Ack first).

## Lock Discipline
19. **Single lock per stream**: Append under shard lock; release fast.
20. **No contention**: Sharded `StreamMap` (64-way).
21. **Batch delivery**: Collect entries; send one frame via `write_vectored`.

## Engine Integration
22. **Engine types as bytes**: Use `as_bytes()` pointer cast for `FanoutEntry`, `AckEntry`, etc.
23. **No mirror types**: Engine types ARE the wire types.
24. **`send_parts`**: Build envelope on stack; zero-alloc wire frames.
