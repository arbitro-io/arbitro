---
description: Hardware sympathy and performance rules — cache, allocation, syscall, branch discipline. INVIOLABLE.
---

# HARDWARE SYMPATHY & PERFORMANCE — MANDATORY

Every design decision must respect the hardware. No exceptions.

---

## Zero-Copy

1. **Never copy data on the hot path** — complete copy budget: exactly 3 copies allowed (see code-zero-copy.md).
2. **Wire codec is zerocopy** — all transport types use `zerocopy` crate: `IntoBytes`, `FromBytes`, `TryFromBytes`. Encode = `as_bytes()` (~400ps pointer cast). Decode = `wire::decode_slice()` (~700ps alignment check). No serialization.
3. **No `String`, no `Vec<u8>` on hot path** — use `&[u8]` slices, `Bytes` for owned. `from_utf8` only on management path.
4. **Hot path replies are `&ScratchReply<T>`** — pre-allocated in EngineContext, recycled with `reset()`. Zero allocation per reply. `as_bytes()` for wire encoding.
5. **No `Vec::new()` per batch** — pre-allocated scratch buffers, `.clear()` per batch. Capacity grows monotonically, never shrinks.

## Cache Sympathy

6. **Cache-line alignment (64B)** — hot structs must be 64 bytes or power-of-2. Use `#[repr(C)]` + `const _: () = assert!(size_of::<T>() == 64)`.
7. **No false sharing** — independent mutable fields on separate cache lines. Pad with `_pad` bytes.
8. **Sequential access patterns** — prefer arrays/Vecs (Slabs) over linked structures. Linear scans beat pointer chasing.
9. **Keep hot data small** — PendingNode ≤ 128B, CreditEntry = 8B, EdgeEntry = 8B. Fewer cache misses.

## Allocation Discipline

10. **Zero allocations on publish/ack/deliver** — the hot path must not call `malloc`. Pre-allocate everything.
11. **Slab over HashMap for hot-path lookups** — array index O(1) worst-case vs hash O(1) amortized.
12. **Reuse buffers** — PublishScratch, reply scratch, all `.clear()` and reuse.

## ID Storage — Dense vs Sparse (INVIOLABLE)

**The shape of the key determines the container. Never use HashMap for dense keys; never use a bucket array for sparse keys.**

### Dense IDs (0, 1, 2, …) → bucket array

Any ID assigned monotonically by a registry is DENSE — use direct-indexed `Vec<T>` / `Box<[T]>`:

```rust
// ✅ ConsumerId dense → array index ~1 ns
consumer: Vec<u32>;
consumer[consumer_id.0 as usize] += 1;

// ❌ HashMap for dense key is ~10-15 ns (hash + probe + entry API)
consumer: HashMap<ConsumerId, u32>
```

Dense IDs in this crate: `ConsumerId`, `QueueId`, `StreamId`, `BindingId`, `SubscriptionId`, `ConnectionId`.

### Sparse IDs (content hashes) → HashMap + ahash

Any key spread across u32/u64 (hash of arbitrary bytes) is SPARSE. Vec would need GiB:

```rust
// ✅ subject_hash spans full u32 range
subject: HashMap<u32, u32, ahash::RandomState>

// ❌ std HashMap default uses SipHash (~3× slower than ahash)
subject: HashMap<u32, u32>

// ❌ bucket array with modulo loses accuracy (collisions → over-count)
subject: Box<[AtomicU32; 16384]>
```

Sparse keys in this crate: `subject_hash`, `tx_hash` (idempotency).

### Linear scan exception

Linear `.iter().find()` is acceptable **only** when:
- The collection is bounded to ≤ 8 elements (cache-line fits), AND
- The scan happens outside the innermost loop (once per drain cycle, not per entry).

Anywhere else, `.iter().find(|x| x.id == target)` on dense-keyed data is a **rule violation** and must be replaced with direct indexing or an ahash side-index.

## Syscall Minimization

13. **Batch I/O** — `write_vectored` for multiple frames. Never one `write_all` per message.
14. **No `Instant::now()` on hot path** — timestamps passed from caller.
15. **No tracing/logging on hot path** — atomic counters only. `tracing::info!` only on management path.

## Branch Prediction

16. **Batch-as-standard** — single = batch(count=1). One code path, no branching.
17. **`#[inline]` on hot getters** — slab accessors, edge lookups, match table, inflight counters.
18. **Predictable dispatch** — action match arms ordered by frequency. Hot actions (Publish, Ack, Claim) first.

## Lock Discipline

19. **Single-threaded engine core** — no locks inside EngineContext. `&mut self` is the synchronization.
20. **No lock contention across streams** — if sharding is needed at boundaries, 64-way minimum.
21. **Batch delivery** — drain collects entries per consumer, sends one frame per batch.

---

## PERFORMANCE TARGETS

| Operation | Target | Measured | Notes |
|---|---|---|---|
| Publish per entry | **≤ 300 ns** | ~150-250 ns | dedup + match + store + enqueue |
| Ack per entry | **≤ 120 ns** | ~100-120 ns | slab + 3 counters + credits + 7 edges |
| Claim per entry | **≤ 200 ns** | ~150-200 ns | pop ready + build Pending + send |
| Wire encode (any size) | **< 1 ns** | ~400 ps | `as_bytes()` pointer cast |
| Wire decode (any size) | **< 1 ns** | ~700 ps | `wire::decode_slice()` alignment check |
| Wire roundtrip | **< 1 ns** | ~750 ps | encode + decode combined |
| Edge lookup | **≤ 5 ns** | — | TypeId hash + get |
| Slab get/remove | **≤ 5 ns** | — | Array index + generation check |
| Match table | **≤ 20 ns** | — | Hash & mask + iterate consumers |
| Idempotency | **≤ 15 ns** | — | Hash + probe |
| Inflight inc/dec | **≤ 5 ns** | — | Array index + counter op |
| release_pending total | **≤ 120 ns** | ~100 ns | All 7 steps combined |
| Drain connection O(k) | **k × 120 ns** | — | k = this connection's pending count |

---

## O(1) GUARANTEES

| Operation | Complexity | Data structure |
|---|---|---|
| Get entity by ID | O(1) worst-case | TypedSlab (array index) |
| Get plugin by type | O(1) | TypeId hash |
| Get edge index by type | O(1) | TypeId hash |
| Subject → consumers | O(1) | Precomputed match_table (hash & mask) |
| Inflight check/update | O(1) | Counter array index |
| Credit check/release | O(1) | Counter array index |
| Edge insert/remove | O(1) amortized | HashMap per edge type |
| Ready queue push/pop | O(1) | VecDeque |
| Idempotency check | O(1) | Hash + linear probe (bounded) |
| Timer cancel | O(1) | Timer wheel slot mark |

---

## ANTI-PATTERNS THAT KILL PERFORMANCE

1. ❌ `HashMap<ConsumerId, Consumer>` for hot lookups → ✅ `TypedSlab<ConsumerNode>` (array index)
2. ❌ Filter evaluation at deliver time → ✅ Precomputed match table at subscription time
3. ❌ Walking ownership graph during ack → ✅ Inline parent IDs in PendingNode
4. ❌ O(S×C) disconnect cleanup → ✅ `edges.take::<PendingByConnection>()` O(k)
5. ❌ `Vec::with_capacity()` per batch → ✅ Pre-allocated scratch, `.clear()` per batch
6. ❌ Global scan for subject inflight → ✅ Counter array indexed by subject_hash
7. ❌ Lock per slab operation → ✅ Single-threaded engine, `&mut self`
8. ❌ `Bytes::copy_from_slice` in fan-out → ✅ `Bytes::clone()` (3ns Arc bump)
9. ❌ Re-parsing header per accessor → ✅ Parse once in FrameView constructor
10. ❌ `format!()` for error messages on hot path → ✅ Error enum with pre-formatted variants
