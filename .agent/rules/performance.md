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
