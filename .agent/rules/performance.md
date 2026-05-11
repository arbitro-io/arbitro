---
description: Strict hardware sympathy and zero-copy rules for all arbitro crates
---

# HARDWARE SYMPATHY & ZERO-COPY — MANDATORY

Every design decision must respect the hardware. No exceptions.

## Zero-Copy

1. **Never copy data on the hot path** — use `zerocopy::ref_from_bytes` to overlay structs on raw `&[u8]`. One copy max: into the journal.
2. **Lazy views over eager extraction** — structs hold `&[u8]`, getters decode on access. Never parse fields you don't read.
3. **No `String`, no `Vec<u8>` on hot path** — use `&[u8]` slices, `Box<[u8]>` for owned data. `String::from_utf8` only on management/cold path.
4. **No `Bytes::copy_from_slice`** — build headers on the stack, send via `send_parts`. Zero heap allocation for replies.
5. **No `Vec::new()` per batch** — use pre-allocated scratch buffers, `.clear()` per batch. Capacity grows monotonically, never shrinks.

## Cache Sympathy

6. **Cache-line alignment (64B)** — hot structs (`CreditSlot`, ring entries) must be 64 bytes. Use `#[repr(C)]` + `const _: () = assert!(size_of::<T>() == 64)`.
7. **No false sharing** — independent mutable fields on separate cache lines. Pad with `_pad` bytes if needed.
8. **Sequential access patterns** — prefer arrays/Vecs over linked structures. Linear scans beat pointer chasing.
9. **Keep hot data small** — fewer cache misses. Envelope = 16B, PublishEntry = 12B, headers = 32B.

## Allocation Discipline

10. **Zero allocations on publish/deliver** — the hot path must not call `malloc`. Pre-allocate everything.
11. **`Box<[u8]>` over `Vec<u8>`** for owned byte data — 16 bytes vs 24 bytes, no unused capacity.
12. **Reuse buffers** — `Flusher`, `PublishScratch`, frame builders all `.clear()` and reuse.

## ID Storage — Dense vs Sparse (INVIOLABLE)

**The shape of the key determines the container. Never use HashMap for dense keys; never use a bucket array for sparse keys.**

### Dense IDs → bucket array (Vec<T> / Box<[T]>)

Any ID assigned monotonically (0, 1, 2, 3, …) by a registry is DENSE. Use a direct-indexed array:

```rust
// ✅ ConnectionId is assigned sequentially by ConnectionRegistry
// Lookup: O(1) worst-case, ~1 ns (one cache-line load, zero hashing)
writers_by_conn: Vec<Option<WriterHandle>>,   // indexed by conn.0
writers_by_conn[conn.0 as usize]              // direct load

// ❌ WRONG — wasteful hashing for a dense key
writers_by_conn: HashMap<ConnectionId, WriterHandle>
```

Applies to: `ConnectionId`, `ConsumerId`, `QueueId`, `StreamId`, `SubscriptionId`, `BindingId` — all assigned sequentially by `NameRegistry` / catalog.

**Hot-path cost** (measured): Vec indexed ~1 ns vs HashMap ~10-15 ns (hash + probe + entry API). **10× faster per lookup**, and with branch-predictable layout.

### Sparse IDs → HashMap with ahash

Any key that is a hash, user-supplied bytes, or otherwise spread across the full `u32`/`u64` range is SPARSE. A Vec would need GiB. Use `HashMap` with **ahash** (not SipHash):

```rust
// ✅ subject_hash is fnv1a_32 of arbitrary bytes → sparse across u32
use ahash::RandomState;
subject_inflight: HashMap<u32, u32, RandomState>

// ❌ WRONG — std HashMap uses SipHash (~3× slower)
subject_inflight: HashMap<u32, u32>  // std default

// ❌ WRONG — bucket array with modulo produces collisions
// (two distinct subjects can hash to the same slot → over-count)
subject_inflight: Box<[AtomicU32; 16384]>
```

Applies to: `subject_hash`, arbitrary `u32`/`u64` content hashes, user-provided keys.

### Decision table

Three distinct shapes, three distinct containers. `binary_search` is **not** a general-purpose answer — it loses to HashMap+ahash at every realistic size (see `lookup_strategies` bench).

| ID origin | Shape | Container | Lookup cost |
|---|---|---|---|
| Registry, bounded (≤ 10k active, recycled on free) | Dense-bounded 0..N | `Vec<T>` / `Box<[T]>` direct index | **~1.4 ns** |
| Registry, monotonic (never recycled) | Dense-unbounded | `HashMap<K, V, ahash::RandomState>` | ~2.6-3.5 ns |
| Content hash / user bytes | Sparse u32/u64 | `HashMap<K, V, ahash::RandomState>` | ~2.6-3.5 ns |
| Composite key `(u32, u64)` or similar | Any | `HashMap<K, V, ahash::RandomState>` | ~3.4 ns |
| Single cache (≤ 8 entries) | Any | `SmallVec` / `ArrayVec` linear scan | ~1-2 ns |

### Never use `binary_search` for dense lookups

Measured `lookup_strategies` bench (10M ops, 5% miss rate):

| N | HashMap+ahash | Box direct | binary_search |
|---|--------------:|-----------:|--------------:|
| 10 | 2.6 ns | 1.4 ns | 3.4 ns |
| 1,000 | 2.6 ns | 1.4 ns | 9.0 ns |
| 10,000 | 2.8 ns | 1.4 ns | 15.5 ns |

For composite keys `(u32, u64)`, the gap widens dramatically:

| N | HashMap+ahash | binary_search |
|---|--------------:|--------------:|
| 10 | 3.5 ns | 14.0 ns |
| 10,000 | 3.8 ns | **51.2 ns** |

Binary search grows `O(log N)` with bad cache behavior; HashMap is `O(1)` constant with a single hash + probe. Use HashMap+ahash whenever direct-indexing is not feasible.

### Enforcement

Anywhere the code does `.iter().find(|x| x.id == target)` on a dense-keyed slice, it **violates** this rule. Similarly, `.binary_search_by(...)` over a sorted dense-keyed `Vec` is a **violation** — it loses to HashMap+ahash at every size.

Acceptable lookups:
- **Dense-bounded** (e.g. `ConsumerId` ≤ 10k): `Vec<T>` / `Box<[T]>` direct index.
- **Dense-unbounded / sparse / composite**: `HashMap<K, V, ahash::RandomState>`.
- **≤ 8 entries in a cache line**: linear scan via `SmallVec` outside inner loops.

Linear scans and binary searches over larger collections are banned on any hot path.

## Syscall Minimization

13. **Batch I/O** — `write_vectored` for multiple frames in one syscall. Never one `write_all` per message.
14. **No `Instant::now()` on hot path** — only when `ack_wait` is configured. Timestamps passed from caller.
15. **No tracing/logging on hot path** — atomic counters only. `tracing::info!` only on cold/management path.

## Branch Prediction

16. **Batch-as-standard** — single message = batch(count=1). One code path, no branching on single vs batch.
17. **`#[inline]` on hot getters** — zerocopy view accessors, sequence math, entry matching.
18. **Predictable dispatch** — action match arms ordered by frequency. Hot actions (Publish, Deliver, Ack) first.

## Lock Discipline

19. **Single lock per stream** — one `Mutex` per drain. Append under shard lock, release fast, signal drain.
20. **No lock contention across streams** — sharded `StreamMap` (64-way). Streams never share locks.
21. **Batch delivery** — drain collects entries per consumer, sends one frame per batch via `write_vectored`. Never one send per message.

## Engine Integration (arbitro-engine v2)

22. **Engine types as bytes** — `FanoutEntry`, `ClaimedEntry`, `AckEntry`, `RepPublish` have `IntoBytes+FromBytes+#[repr(C)]`. Use `as_bytes()` pointer cast, not field-by-field copy.
23. **No owned mirror types** — never define `FanoutEntryOwned` or `ClaimedEntryOwned`. Engine types ARE the wire types. The only owned types are in `command.rs` for channel crossing (`Vec<FanoutEntry>`, not `Vec<FanoutEntryOwned>`).
24. **send_parts for wire replies** — build envelope header on the stack, send body as `as_bytes()` slice reference. Zero heap allocation for building wire frames.
25. **Scratch buffers in shard** — pre-allocated `Vec<AckEntry>`, `Vec<NackEntry>`. `.clear()` per batch, capacity grows monotonically, never shrinks. No allocation on steady-state hot path.
26. **Ack reply is zero-alloc** — `AckReply { accepted: u32, rejected: u32 }` is 8 bytes, inline in oneshot. No Vec, no Box, no Bytes.
27. **Publish fanout is one-alloc** — `Vec<FanoutEntry>` from `drain.entries().to_vec()`. Single allocation, amortized over batch. Acceptable because publish is not the tightest hot path (ack is).
