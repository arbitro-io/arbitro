---
description: Banned patterns — what is forbidden on the hot path, in library code, and architecturally. INVIOLABLE.
---

# ANTI-PATTERNS — WHAT IS FORBIDDEN

---

## HOT PATH BANS

| Banned | Reason | Required alternative |
|---|---|---|
| `String::from(...)`, `to_string()` | heap alloc | `&str`, `&[u8]`, `Arc<str>` |
| `Vec::new()`, `Vec::with_capacity()` inside a loop | alloc per iteration | pre-allocate before loop, `clear()` to reuse |
| `Box::new(...)` on hot path | heap alloc | pre-allocate at init time |
| `Arc::new(...)` on hot path | heap alloc | pre-allocate at startup |
| `from_utf8(subject)` | O(N) scan | keep as `&[u8]`, validate only at ingress |
| `format!(...)` | alloc | never on hot path |
| `Box<dyn Trait>` for dispatch | vtable per call | enum or const generics |
| `Bytes::copy_from_slice` after ingress | heap alloc | `Bytes::clone()` (Arc bump) or `.slice()` |
| `Vec::collect()` in matching | 2 alloc per match | iterator-based token walking |
| Manual byte-by-byte header reads | error-prone, slower | `zerocopy::ref_from_prefix` |
| Re-overlaying `ref_from_prefix` per accessor | redundant work | overlay ONCE, store reference |
| `HashMap` on inner deliver loop | hash per lookup | slab index (array offset) |
| `Ordering::SeqCst` | full MFENCE (~20 ns) | justify or downgrade to AcqRel/Relaxed |
| `tracing`/`log` macros on hot path | format string alloc | `AtomicU64::fetch_add` + metrics thread |
| `Instant::now()` on hot path | syscall | timestamps passed from caller |
| Parent pointer chasing during release | extra lookups | all IDs inline in PendingNode |
| Runtime subject filter evaluation | O(N) per publish | precomputed match table |
| `fsync` per message | kills throughput | group-commit or async flush |
| Global O(N) scan after indexed lookup | defeats purpose of index | index the candidate set |

---

## LIBRARY CODE BANS

| Banned | Reason | Required alternative |
|---|---|---|
| `unwrap()` in library paths | panics in production | `?`, `match`, `if let` |
| `expect("...")` in library paths | same | propagate with `?` |
| `Box<dyn Error>` return type | allocates on error | domain error enum (`EngineError`) |
| `transmute` | undefined behavior | `zerocopy` traits (`FromBytes`, `ref_from_prefix`) |
| Raw pointer casts (`as *const T`) | unsafe, UB risk | `zerocopy` traits |
| `std::sync::mpsc` | not needed | engine is single-threaded `&mut self` |
| `tokio` / `async-std` / `smol` | engine is sync | engine has no async, no I/O, no threads |
| `Arc` / `Mutex` / `RwLock` | engine is `&mut self` | borrow checker is the synchronization |
| `serde` / `serde_json` | too slow | wire codec is `zerocopy` — pointer cast |
| Circular crate dependencies | architecture violation | fix the layer design |

---

## ARCHITECTURAL BANS

| Banned | Reason |
|---|---|
| Engine depending on Transport impl | engine is a pure library — no I/O, no transport |
| `async fn` inside engine | engine is synchronous `&mut self` — no async |
| Any I/O (TCP, files, clock) inside engine | engine is pure computation, caller owns I/O |
| Store returning owned data from `get` | O(1) callback pattern — zero-copy borrow |
| Multiple validations of same frame | validate once at decode, trust downstream |
| Hot path calling management path | management may allocate; hot path must not |
| Wire types using `serde` instead of `zerocopy` | wire codec is pointer-cast, not serialization |
| `graph/` importing from `edge/` | graph is source of truth, edges are indexes |
| `edge/` importing from `plugin/` | edges are generic, plugins are specific |
| `plugin/` importing from `runtime/` | plugins provide mechanisms, runtime uses them |
| `runtime/` importing from `lib.rs` | root is facade only |
| Any module importing from a higher level | strict DAG — only import from below |

---

## DEPENDENCY BANS (engine crate)

| Banned crate | Reason | Alternative |
|---|---|---|
| `tokio` | engine is pure sync library — no async, no I/O | caller owns async runtime |
| `async-std`, `smol` | same — no async runtime inside engine | caller decides |
| `serde`, `serde_json` | too slow, allocates | `zerocopy` — pointer cast encode/decode |
| `parking_lot` | engine is `&mut self` — no locks needed | borrow checker |
| `crossbeam` | no cross-thread communication inside engine | single-threaded |
| `log` / `tracing` on hot path | format alloc | scratch buffers, atomic counters |
| `reqwest` | not applicable | — |

### Allowed crates

| Crate | Purpose |
|---|---|
| `ahash` | Fast hash maps (edge indexes, match table, idempotency) |
| `bytes` | Zero-copy byte buffers for PayloadRef |
| `zerocopy` | Wire codec: `IntoBytes`, `FromBytes`, `TryFromBytes` |
| `criterion` | Benchmarks (dev-dependency only) |

---

## CODE STYLE BANS

| Banned | Reason |
|---|---|
| Comments that restate the code | noise — only WHY comments are valid |
| `pub` on internal types without doc | either document or `pub(crate)` |
| Functions > 60 lines | extract into named functions |
| Files > 400 lines | split into submodules |
| 4+ levels of module nesting | wrong decomposition — flatten |
| `mod.rs` containing logic | `mod.rs` is for re-exports only |
| `bool` parameters for behavior flags | use an enum |
| Unnamed `0x01`, `0x0101` literals | use named constants |

---

## BENCHMARK LOCATION RULES

| Location | Allowed content |
|---|---|
| `benches/` | Criterion benchmarks only |
| `examples/` | Runnable usage examples only |
| `tests/` | Integration tests (public API only) |
| `src/` | `#[cfg(test)]` unit tests inline at module bottom |

**Never:**
- Put benchmarks at the crate root or in `src/`
- Mix benchmark logic with production logic
- Import internal types in E2E benches — use only the public API
