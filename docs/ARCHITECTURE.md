# Arbitro — Architecture & Technical Context

> This document is the source of truth for a new session picking up arbitro.
> It is intentionally long but organized by section. **If you are new here,
> read the Table of Contents first and jump to the section you need.**

## Table of Contents

1. [Where we are today (completed)](#1-where-we-are-today-completed)
2. [Where we are going (open work)](#2-where-we-are-going-open-work)
3. [Crate / module layout](#3-crate--module-layout)
4. [Shard actor model — the core decision](#4-shard-actor-model)
5. [Sharding strategy — what is and isn't sharded](#5-sharding-strategy)
6. [Shared-memory data structures — BucketArray vs HashMap decision matrix](#6-shared-memory-data-structures)
7. [The star feature: `MaxSubjectInflight`](#7-the-star-feature-maxsubjectinflight)
8. [Lock-free primitives in the hot path](#8-lock-free-primitives)
9. [Wire protocol](#9-wire-protocol)
10. [Storage backends](#10-storage-backends)
11. [Testing rules](#11-testing-rules)
12. [Benchmark rules (WSL discipline)](#12-benchmark-rules)
13. [Agent rules (code-zero-copy, hot-cold-path, anti-patterns)](#13-agent-rules)
14. [How to evolve the system — concrete next steps](#14-how-to-evolve)
15. [Glossary](#15-glossary)

---

## 1. Where we are today (completed)

Arbitro is a working broker that passes **~180 tests with `-race`** and sustains:

| Metric | Today |
|--------|-------|
| Publish batch throughput | 14.2M msg/s (25 k msgs, 64 B, WSL) |
| Replay drain (single stream) | 4.3M msg/s @ 500 k msgs |
| Replay drain (single stream) | 3.9M msg/s @ 1 M msgs |
| MaxSubjectInflight enforcement | 100 % correct under load |
| Durability | 0 % loss on SIGKILL with TolerantStore |

**Features already shipped**:

- Stream create / delete / list
- Consumer create / delete / list / pause / resume
- Publish (single + batch, sync + fire-and-forget)
- Subscribe (pull + callback), unsubscribe
- Fanout + queue delivery modes with correct group dedup
- Ack / Nack (single + batch), AckSync for durability checkpoints
- `MaxInflight` per consumer
- `MaxSubjectInflight` per pattern with wildcard resolution, min-wins when multiple patterns match
- Persistence: memory store + tolerant disk store (0xAF magic byte, crash-safe)
- Metadata command log for admin replay on restart
- Shard-parallel architecture: drain thread + command thread per shard, zero mutex between them
- Per-entry `consumer_id` in wire DeliveryBatch (enables broadcast collapse — one frame delivers to many consumers on the same TCP connection)
- Lifecycle trace (feature-gated, zero-cost when off)
- Connection registry with dead-connection cleanup
- Ack timeout via per-shard timing wheel (auto-nack after `ack_wait_ms`)
- Nack with delay (`msg.nack_delay(ms)` — delayed redelivery via same timing wheel)

**Where code lives** (crate-level):

```
crates/arbitro-proto       — wire protocol (zerocopy), opcodes, validators
crates/arbitro-engine      — oracle: catalog, matcher, inflight, events, runtime
crates/arbitro-common      — Gate, NameRegistry, IdPool (shared primitives)
crates/arbitro-store       — Store trait + Memory + Tolerant backends
crates/arbitro-server      — shard, transport, persistence, main binary
crates/arbitro-client      — client SDK (Client, Consumer, Subscription, Message)
crates/arbitro-e2e         — integration tests + standalone bench binaries
```

---

## 2. Where we are going (open work)

Priority-ordered backlog.

### ~~P0 — Subject scavenging~~ (completed)

`SharedCounters::subject` is now `papaya::HashMap<(u32, u32), AtomicU32, foldhash::fast::FixedState>`
keyed by `(consumer_id, subject_hash)`. Entries are left at zero on ack (ABA-safe)
rather than removed; memory is bounded to the working set of distinct
`(consumer, subject)` pairs seen. See §6 and §8 for the runtime shape, and the
shard-side call sites in `drain.rs` (`inc_subject`, `subject_has_room`) and
`worker.rs` (`dec_subject`).

### P1 — Multi-language clients

Wire protocol is stable. Provide:
- TypeScript client (`arbitro-ts`) — Node + browser via WebSocket bridge
- Go client — see the scrapped `arbitro-go-server` for the wire encoding prototype
- Both need: publish, subscribe, ack batch, reconnect, consumer config including `MaxSubjectInflight`

### P2 — Clustering (Raft)

- Replicate stream + consumer metadata across N nodes via the existing `arbitro-raft` crate scaffolding
- Stream ownership stays single-writer (one primary per stream) to preserve ordering
- Store replication via log shipping (the command log journal is already sequentialized)
- **Do not** shard consumers across nodes in v1 — the consumer's pending list is per-binding; a single node owning the consumer keeps semantics simple

### P3 — Prometheus + adaptive flow control

- Emit `/metrics` endpoint with subject-pressure histograms, per-consumer throughput, queue fairness
- Adaptive flow: detect starving subjects and temporarily raise their share of the global `max_inflight`

### P4 — Cross-shard subject aggregation

Today each shard has its own `max_subject_inflight` counter. If a single subject spans multiple streams (and hence multiple shards), global limit enforcement requires a shared counter.

Options:
- Broadcast subject-hash deltas to all shards (O(N shards) per ack)
- Centralize in one "gatekeeper" shard (adds RTT for cross-shard subjects)
- Hash-affinity: ensure any given `(pattern, subject_hash)` always lands on the same shard via an ownership table

Not a blocker for MVP because most real deployments have streams that are naturally shard-local.

### P5 — Stream export / migration / filtered backup

**Goal.** Before deleting a stream, operators should be able to (a) take a backup of its messages to disk, (b) filter that backup by subject pattern, and (c) optionally republish the kept messages into another stream. Same primitives also serve scheduled backups of a live stream.

**Feasibility.** High — everything lives on the admin / command path and reuses existing pieces:

- `Store::for_each(start, end, cb)` already walks a shard's linear log with zero-copy `Entry<'_>` borrows. Admin-path cost; drain is untouched.
- The engine's subject matcher (`MatchTable` / trie) can be invoked offline against each entry's `subject_hash + subject` to apply a pattern filter without duplicating logic.
- Republish into the target stream goes through the existing publish handler, so destination sequence numbers, storage backend, and durability semantics stay consistent.

**Performance invariant.** This feature must add **zero** cost to the hot path:

- No new fields on `Entry`, `PublishEntry`, `DeliveryEntryHeader`, or any `SharedCounters` slot.
- No new branch in `drain_cycle` or `process_drain_entry`. The drain never sees export state.
- Export runs on the command thread (or a dedicated admin task) reading the store behind the same lock the drain uses; admin ops are naturally batched and rare.
- If streaming export to disk, use `for_each` into a pre-allocated `BytesMut` and a sink writer — no per-entry heap allocation beyond the sink's own buffering.

**Shape sketch** (not a commitment, just the direction):

- New admin ops in `arbitro-server::shard::handlers`: `ExportStream { stream, filter: Option<pattern>, sink }`, `ImportStream { stream, source }`, `MigrateStream { from, to, filter }` built as `Export → delete? → Import`.
- Wire-level: new opcodes under the `0x04xx` stream family (mirrors `PurgeStream` / `DrainSubject`).
- Quiescence: `MigrateStream` pauses publishes to `from` at the shard boundary (existing pause mechanism extended from consumer to stream), drains, exports, imports, then deletes. No hot-path code touched — the pause flag is checked inside the already-cold publish handler, not inside the drain.
- Backup format: framed records reusing `PublishEntry` layout + a small header (magic, version, stream name, schema hash). Identical to `TolerantStore` segment records on purpose — a backup file *is* a replay-able log.

**Caveats / when to skip.**

- Cross-shard migration (source and target live on different shards) is a P4-adjacent problem: the import side must publish through the target stream's shard, which is already how all publishes are routed. No new cross-shard coordination is needed *as long as* the operator accepts that destination sequence numbers are freshly assigned (no attempt to preserve source seqs).
- If in the future any of this starts requiring hot-path branches or new shared-state fields, stop and reconsider — the design premise here is admin-path-only. The feature is explicitly optional: if it can't be added without touching §8 shared primitives, it should not be added.

---

## 3. Crate / module layout

### `arbitro-proto` (wire protocol)

```
src/
├── action.rs         # opcodes (0x01xx–0x07xx), is_hot()
├── envelope.rs       # 16 B frame envelope
├── wire/
│   ├── publish.rs    # PublishEntry (12 B) + BatchIter
│   ├── delivery.rs   # RepBatchFixed (4 B), DeliveryEntryHeader (22 B)
│   │                 #   including per-entry consumer_id for broadcast collapse
│   ├── subscribe.rs  # SubscribeFixed (20 B)
│   ├── manager.rs    # CreateConsumerFixed (28 B) + SubjectLimitIter trailer
│   ├── stream.rs, system.rs, headers.rs
├── config/
│   ├── stream.rs     # StreamConfig + wire_hash_32 (foldhash)
│   ├── consumer.rs   # ConsumerConfig builder, MaxSubjectInflight
├── validate.rs       # subject / pattern validation (rejects malformed)
```

All wire structs are `#[repr(C)]` + `FromBytes + IntoBytes`. Size assertions at compile time. Parsing is a pointer cast (~400 ps).

### `arbitro-engine` (oracle)

```
src/
├── types.rs          # StreamId, ConsumerId, QueueId, BindingId, ConnectionId
├── events.rs         # DeltaEvents (#[must_use])
├── command.rs        # Command enum (Delivered, Ack, Nack, …)
├── catalog/
│   ├── mod.rs        # streams, consumers, bindings + 3 secondary indices
│   ├── match_table.rs # subject_hash → Vec<MatchEntry>, limit trie, limit values
├── inflight/mod.rs   # InFlightCounters: dense Vec + sparse HashMap, subject tracking gate
├── runtime/
│   ├── execute.rs    # apply(Command) → DeltaEvents
│   ├── retire.rs     # retire_binding (dec credits, remove from indices)
├── lib.rs            # public ArbitroEngine facade, &mut self, no locks
```

The engine is **strictly single-threaded** (`&mut self`). It owns mutation; the shard owns concurrency.

### `arbitro-common`

```
src/
├── gate.rs           # Cache-line aligned wake/lock doorbell (spin 512 → park)
├── name_registry.rs  # wire-id ↔ sequential-id translation, queue key mapping
├── id_pool.rs        # monotonic allocator
├── wheel.rs          # Hashed timing wheel (ack-timeout + nack-delay)
```

### `arbitro-store`

```
src/
├── store.rs          # Store trait: append, for_each, mark_tombstone, purge, info
├── memory.rs         # in-memory arena
├── tolerant.rs       # mmap-backed segments with 0xAF magic byte + CRC
```

`Entry<'a>` carries its own `stream_id` — the store is stream-agnostic.

### `arbitro-server`

```
src/
├── shard/
│   ├── worker.rs       # DrainWorker + CommandWorker
│   ├── shared.rs       # SharedCounters (atomics), SnapshotSwap, DrainSnapshot,
│   │                   # DrainNotification channel
│   ├── drain.rs        # drain_cycle + DrainScratch
│   ├── handlers.rs     # publish, subscribe, ack, …
│   ├── accumulator.rs  # per-connection wire-frame buckets
│   ├── router.rs       # stream → shard mapping (hash % N)
├── transport/
│   ├── mod.rs, dispatch.rs, registry.rs, connection.rs
├── persistence/
│   ├── command_log.rs  # WAL for metadata (Create/Delete Stream/Consumer)
│   ├── recovery.rs     # replay at startup
├── lifecycle_trace.rs  # opt-in profiling (feature = "lifecycle_trace")
```

### `arbitro-client`

```
src/
├── client.rs           # public Client API
├── inner.rs            # connection state, pending map, subscription routing
├── consumer.rs, subscription.rs, message.rs
├── conn.rs             # reconnect loop
```

---

## 4. Shard actor model

**This is the central design decision of the whole broker.** A single shard is the unit of concurrency.

### Per-shard threads

```
┌──────────── Shard N ────────────┐
│                                 │
│  ┌───────────────┐              │
│  │ Drain thread  │ ← pure loop: │
│  │ (OS thread)   │   gate → scan│
│  │               │   store →    │
│  │ reads only    │   dispatch   │
│  └───────────────┘              │
│                                 │
│   shared state (lock-free):     │
│     SharedCounters atomics      │
│     SnapshotSwap<DrainSnapshot> │
│     mpsc<DrainNotification>     │
│                                 │
│  ┌────────────────┐             │
│  │ Command thread │ ← owns:     │
│  │ (tokio task)   │   ArbitroEngine│
│  │                │   Store     │
│  │ &mut engine    │   NameReg   │
│  └────────────────┘             │
│                                 │
└─────────────────────────────────┘
```

Key invariants:

- **Drain never touches the engine.** Reads atomics + snapshot only.
- **Command never blocks on drain.** Sends mpsc notifications; drain processes them inside its own next cycle.
- **Engine is `&mut self`**, so the compiler guarantees single-writer safety. No `Arc<Mutex<...>>` anywhere in the engine.
- **No spinning.** The Gate uses 512 iterations of `hint::spin_loop()` then falls back to `thread::park()` until command calls `gate.release()`. Idle CPU = 0 %.

### Gate protocol

```rust
// crates/arbitro-common/src/gate.rs
pub struct Gate {
    locked: AtomicBool,
    parked: AtomicBool,
    worker: UnsafeCell<Option<Thread>>,
}
```

- `acquire()` — drain calls. Fast path ~80 ns, park path 0 % CPU.
- `release()` — command calls after publish. Unparks drain.
- `lock()` — drain calls when no more work.

### Communication graph

```
 client TCP → transport read loop → shard.submit(cmd) → cmdCh
                                                        ↓
                                     command thread handles cmd
                                                        ↓
                                     store.append → engine.execute → counters.inc_demand
                                                        ↓
                                     gate.release()  (wake drain)
                                     snapshot.swap() if structural change
                                                        ↓
                                     reply via ReplyTo channel
                                                        ↓
                              transport write loop → client TCP


drain thread (parallel):
  gate.acquire() → store.for_each → match → counters.has_room / has_capacity
                                                        ↓
                                   writer.try_send(frame)  (directly to TCP)
                                                        ↓
                                   counters.inc_inflight / inc_subject
                                   notify_tx.try_send(Delivered{binding_id, entries})
                                                        ↓
                              command thread handles notification → engine.record_delivery
```

---

## 5. Sharding strategy

### Shard count

`ShardCount = config.shard_count` (default 1; scale up for multi-tenant clusters).

### Shard picker

```rust
// arbitro-server/src/shard/router.rs
fn pick(stream_name: &[u8]) -> Shard {
    shards[wire_hash_32(stream_name) % shards.len()]
}
```

Hash-based. A stream always lands on the same shard. Consumers, bindings, and the store for a stream all live on that shard.

### What IS sharded (per-shard owned)

| State | Owner | Rationale |
|-------|-------|-----------|
| `ArbitroEngine` | Command thread | Single-writer semantics, zero locks |
| `Store` (one per shard) | Drain reads, Command writes (mutex) | Per-stream log isolation |
| `SharedCounters` (consumer inflight, demand, paused) | Drain reads, Command writes | Atomics per shard for cache locality |
| `DrainSnapshot` (bindings + match tables) | Command builds, Drain reads via `SnapshotSwap` | Rebuilt only on structural change |
| `ActiveBinding` list | Command owns, snapshot passes to drain | Per-binding tx handle cached |
| `MatchTable` (subject → consumers, limit trie) | Per-stream inside engine | Per-stream data is naturally shard-local |
| Cursor (drain position) | Drain writes, both read | Per-shard AtomicU64 |
| `DrainNotification` mpsc channel | Drain sender, Command receiver | Per-shard |

### What is NOT sharded (global or cross-shard)

| State | Where it lives | Rationale |
|-------|----------------|-----------|
| `ConnectionRegistry` (conn_id → writer) | `arbitro-server::transport::registry` | A client has ONE TCP connection that may interact with multiple shards; the registry is connection-scoped |
| `NameRegistry` (wire-hash ↔ sequential-id) | `arbitro-common::name_registry` behind one Mutex | Stream names are global. Cold path (only admin ops). Mutex is cheap at the rates this sees |
| `IdPool` | `arbitro-common::id_pool` | Monotonic allocator; cold path |
| TCP listener + accept loop | `arbitro-server::transport::listener` | Global input; dispatches to shards |
| Per-connection read/write goroutines | `arbitro-server::transport::connection` | Each TCP connection has its own pair; cross-shard routing happens inside the read loop |
| Persistence command log | `arbitro-server::persistence::command_log` | Global serial log of admin ops (CreateStream, etc.) for replay |

### Cross-shard invariants

- A message is never routed to another shard after ingest. The shard that owns the stream owns all its bindings and their delivery.
- Admin replies (CreateStream, CreateConsumer) reach the command thread on the correct shard and return directly; no cross-shard coordination.
- Consumer `pause` / `resume` is shard-local (consumers live on one shard).
- `MaxSubjectInflight` enforcement is **per shard**. For a subject pattern that matches across multiple streams that live on different shards, each shard enforces the limit independently (so the effective global limit is `limit × N_shards`). See roadmap P4.

---

## 6. Shared-memory data structures

This is the most important technical discussion in the project and the part most likely to be wrong if rewritten carelessly.

### The rule: shape determines container

| Key shape | Example | Container | Access cost |
|-----------|---------|-----------|-------------|
| Dense, monotonic `u32` | `ConsumerId`, `QueueId`, `StreamId` | `Box<[AtomicU32]>` | ~1 ns (load + index) |
| Dense, bounded `u32` | `binding_idx` within snapshot | `Vec<T>` / `Box<[T]>` | ~1 ns |
| Sparse 32-bit hash | `subject_hash` | `HashMap<u32, _, foldhash>` or `papaya::HashMap` | 3–30 ns |
| Sparse 64-bit compound | `(consumer_id, subject_hash)` | `papaya::HashMap<u64, _>` | ~30 ns (lock-free) |

### Why BucketArray wins for dense IDs

```rust
// fast: 1 load, no hash
let inflight = counters.consumer[consumer_id.0 as usize].load(Relaxed);

// slow: hash + bucket walk + key compare
let inflight = counters.consumer_hm.get(&consumer_id).copied().unwrap_or(0);
```

Measured difference: **~2 ns** (BucketArray) vs **~10-15 ns** (HashMap with foldhash). On a hot delivery loop that hits this 10M+ times per second, the difference is whole cores.

Additional wins:
- **Cache-line alignment** — consecutive ConsumerIds land in the same cache line, so iterating or checking neighbors is free.
- **Zero lock contention** — `AtomicU32::load(Relaxed)` has no fence on x86 (compiles to MOV).
- **Trivial memory footprint** — 4 bytes × 4096 slots = 16 KB, fits in L2.

### Why HashMap for sparse hashes

A `subject_hash: u32` has 4 billion possible values. A dense array would need 16 GB. Only a working set of hundreds-to-thousands of subjects is active at a time.

Two HashMap flavors in use:

**Single-threaded** — `HashMap<u32, u32, foldhash::fast::FixedState>` inside the engine (`inflight/mod.rs`). Owned by command thread, no locks.

**Lock-free** — `papaya::HashMap<(u32, u32), AtomicU32, foldhash::fast::FixedState>` for shared subject counters in `SharedCounters::subject`. Key is `(consumer_id, subject_hash)` for per-consumer isolation. Entries are left at zero on decrement (ABA-safe — removing a zeroed entry races with a concurrent increment); working-set memory stays bounded by distinct pairs observed. Benchmarks in `crates/arbitro-server/benches/subject_inflight.rs` show ~34 M reads/s vs RwLock's ~7 M under write churn.

### Why a fixed bucket array was rejected

An earlier design used `Box<[AtomicU32]>` of fixed size with `hash % N` slotting. It was rejected because two subjects sharing a slot starve each other: a saturated slot blocks every subject that hashes to it. The papaya map above has no collision domain — each `(consumer, subject)` pair owns its own counter.

### Decision matrix summary

```
┌────────────────────────┬──────────────────────┬───────────────────────┐
│ Data                   │ Primary container    │ Rationale             │
├────────────────────────┼──────────────────────┼───────────────────────┤
│ consumer inflight      │ Box<[AtomicU32]>     │ dense IDs             │
│ queue inflight         │ Box<[AtomicU32]>     │ dense IDs             │
│ demand (per stream)    │ Box<[AtomicU32]>     │ dense IDs             │
│ paused flag            │ Box<[AtomicBool]>    │ dense IDs             │
│ cursor                 │ AtomicU64            │ single scalar         │
│ subject inflight (map) │ HashMap<u32,u32>     │ sparse hash (engine)  │
│ subject inflight (atm) │ papaya::HashMap      │ sparse hash (shared)  │
│                        │ <(u32,u32),AtomicU32>│ lock-free, per-cons.  │
│ streams                │ HashMap<StreamId,_>  │ sparse, admin path    │
│ consumers              │ HashMap<ConsumerId,_>│ sparse, admin path    │
│ bindings               │ HashMap<BindingId,_> │ sparse, admin path    │
│ by_stream index        │ HashMap<StreamId,    │ adjacency — sparse    │
│                        │   Vec<BindingId>>    │                       │
│ match table exact      │ HashMap<u32,         │ subject_hash is sparse│
│                        │   Vec<MatchEntry>>   │                       │
│ match table pattern    │ SubjectTrie (arena)  │ pattern traversal     │
│ pending acks per bind  │ Vec<Pending> inline  │ small N, linear scan  │
│ name registry          │ HashMap<wire,seq>    │ sparse wire-hash      │
└────────────────────────┴──────────────────────┴───────────────────────┘
```

### Scratch buffers — always pre-allocated, always reused

Every hot-path allocation in the drain uses `DrainScratch` (`crates/arbitro-server/src/shard/drain.rs`):

```rust
pub struct DrainScratch {
    pub body: BytesMut,                  // frame buffer
    pub matches: Vec<MatchEntry>,        // resolved recipients
    pub served_queues: Vec<QueueId>,     // dedup set
    pub dead_connections: Vec<Connection>,
    pub pending: PendingBatch,
    pub resolve_cache: HashMap<(u32, u32), Vec<MatchEntry>>,
    pub subject_limit_cache: HashMap<(u32, u32), Option<u32>>,
}
```

Cleared via `.clear()` each cycle, never re-allocated. Zero heap pressure at steady state.

---

## 7. The star feature: `MaxSubjectInflight`

### Configuration

```rust
ConsumerConfig::new(b"worker", b"orders")
    .ack_policy(AckPolicy::Explicit)
    .max_inflight(10_000)
    .max_subject_inflight(b"orders.premium.>", 30)
    .max_subject_inflight(b"orders.basic.>", 10)
    .max_subject_inflight(b"orders.freemium.>", 1)
```

### Wire encoding

`CreateConsumer` body carries a trailer with the limits:

```
[fixed CreateConsumerFixed 28B]
[name][group][subject]
[4: limits_count]
[for each: 4 limit] [2 pattern_len] [pattern]
```

### Server-side storage

On `create_consumer`:

1. Engine validates that `AckPolicy::Explicit` is set (limits require ack mode). Rejects otherwise.
2. Engine enables subject tracking: `InFlightCounters::enable_subject_tracking()` flips a sticky flag that enables the `HashMap<u32, u32>` write path.
3. For each limit:
   - If the pattern is literal → `MatchTable::max_subject_inflights.insert(hash, limit)`.
   - If it contains `*` or `>` → `MatchTable::limit_patterns.push((pattern, limit))` and rebuild `limit_trie`.

Code: `crates/arbitro-engine/src/catalog/match_table.rs`.

### Hot-path enforcement (drain)

```rust
// crates/arbitro-server/src/shard/drain.rs::process_drain_entry
if match_table.has_subject_limits() {
    let limit = resolve_cache.entry(cache_key).or_insert_with(|| {
        match_table.resolve_subject_limit_readonly(subject_hash, entry.subject)
    });
    if let Some(max) = *limit {
        if !counters.subject_has_room(subject_hash, max) {
            more_pending = true;
            track_skipped(lowest_skipped, entry.seq);
            return;
        }
    }
}
```

**Key properties**:

1. `has_subject_limits()` is a fast gate — skip everything if the stream has no limits.
2. `resolve_subject_limit_readonly()` walks the limit trie and returns the minimum limit matching `subject` (min-wins semantics for overlapping patterns).
3. The per-cycle `subject_limit_cache` avoids re-walking the trie for the same hash within a cycle.
4. `subject_has_room(consumer, hash, max)` is an atomic load against the papaya map keyed by `(consumer_id, subject_hash)`.
5. If no room, the cursor does **not** advance beyond `lowest_skipped` — on the next cycle, the skipped entries get retried.

### Release on ack / retire

```rust
// engine::runtime::execute.rs — Command::Ack
for ack in entries {
    let pending = binding.pending.swap_remove(pos);
    events.subject_hashes_acked.push(pending.subject_hash);
    ctx.inflight.dec_pending(pending.subject_hash, consumer_raw, queue_raw);
}
```

The worker receives `DeltaEvents` and calls `counters.dec_subject(consumer_id, hash)` for each. The engine-side HashMap entry is removed at zero (bounded working-set memory). On the drain-side papaya map, the counter is decremented to zero but the entry is intentionally **not** removed — papaya's remove is ABA-unsafe (dec 1→0, concurrent inc back to 1, remove deletes the live entry). Leaving zeroed entries is safe (`has_room` returns true) and keeps memory bounded to the distinct `(consumer, subject)` pairs ever observed.

### Correctness proof sketch

- **Per-consumer isolation**: the key is `(consumer_id, subject_hash)`. Two consumers with the same subject have independent counters. Proven by integration test `max_subject_inflight_per_consumer_isolation`.
- **Minimum wins**: `resolve_subject_limit_readonly` iterates trie matches and returns the min. Proven by `max_subject_inflight_multiple_patterns_min_wins`.
- **Ack unblocks**: after `ack`, the cursor rewinds to `lowest_skipped - 1` on next drain cycle, and the freed slot lets the next message through. Proven by `max_subject_inflight_multiple_patterns` (4th premium arrives after first is acked).

---

## 8. Lock-free primitives

### Gate (`arbitro-common::gate`)

A cache-line aligned doorbell that wakes the drain when work arrives, and parks when idle.

```rust
#[repr(align(64))]
pub struct Gate {
    locked: AtomicBool,
    parked: AtomicBool,
    worker: UnsafeCell<Option<Thread>>,
}
```

- `acquire()` — drain calls. Spin 512 × with `std::hint::spin_loop()`, then `thread::park()`. Fast path ≤ 80 ns.
- `release()` — command calls. Stores `false` into `locked`, unparks worker if `parked == true`.
- `lock()` — drain calls when cursor has caught up.

### SnapshotSwap (`arbitro-server::shard::shared`)

Atomic pointer swap for structural state that drain reads:

```rust
pub struct SnapshotSwap<T> {
    inner: ArcSwap<T>,
}
```

Uses `arc-swap` crate. Drain calls `load()` → clone the Arc (3 ns bump); command calls `store()` → swap pointer (O(1)).

Rebuilt only on:
- Subscribe / unsubscribe
- Delete stream / consumer
- Mark connection dead (which retires bindings)

### SharedCounters (`arbitro-server::shard::shared`)

```rust
pub struct SharedCounters {
    consumer: Box<[AtomicU32]>,
    queue: Box<[AtomicU32]>,
    demand: Box<[AtomicU32]>,
    paused: Box<[AtomicBool]>,
    subject: papaya::HashMap<(u32, u32), AtomicU32, foldhash::fast::FixedState>,
    cursor: AtomicU64,
    rewind: AtomicU64,
    total_demand: AtomicU32,
}
```

All writes use `Relaxed` ordering. Ordering isn't needed because causality is enforced at the protocol level:
- Drain never reads a counter before the command has incremented it (drain starts only on wake → command released gate → gate.release has release semantics on the park wait).
- Command never reads counters that drain writes while drain might be mid-write (snapshot rebuild is exclusive).

### DrainNotification mpsc channel

```rust
pub enum DrainNotification {
    Delivered { binding_id: BindingId, entries: Vec<DeliveredEntry>, ... },
    ConnectionDead(ConnectionId),
}
```

- Drain sends via `try_send` (non-blocking; if saturated, backpressure triggers skip).
- Command receives via `recv` in its select loop.

This is the only mutex-free cross-thread channel.

### Timing Wheel (`arbitro-common::wheel`)

A per-shard hashed timing wheel that handles both **ack-timeout** (auto-nack
on expiry) and **nack-with-delay** (deferred redelivery). O(1) insert,
O(expired) per tick. One wheel per shard, owned by the CommandWorker.

```rust
pub struct WheelEntry {
    pub seq: u64,           // message sequence
    pub consumer_id: u32,   // which consumer
    pub subject_hash: u32,  // 0 = nack-delay, != 0 = ack-timeout
}
// 16 bytes, Copy
```

**Configuration**: 120 buckets, 1-second resolution per tick (max delay = 120s).
The wheel is created lazily (`Option<TimingWheel>`) — zero overhead when no
consumer has `ack_wait_ms > 0` and no `nack_delay` is used.

**Ack-timeout flow**:
1. Drain delivers message → command thread receives `Delivered` notification.
2. If consumer has `ack_wait_ms > 0`, insert entry into wheel with `delay_ticks = ack_wait_ms / 1000`.
3. On tick: if entry still in `binding.pending` → auto-nack + dec inflight + rewind cursor.
4. If already acked (not in pending) → skip (lazy cancel, zero cost).

**Nack-with-delay flow**:
1. Client calls `msg.nack_delay(5000)`.
2. Server receives `NackCmd { delay_ms: 5000 }`.
3. Engine nacks immediately (releases inflight), entry inserted into wheel with `subject_hash = 0`.
4. On tick: `subject_hash == 0` → just rewind cursor (no pending check needed). Drain re-delivers.

**Lazy cancel** — the ack path never touches the wheel. When a message is acked
normally, the wheel entry becomes stale. On the next tick the `still_pending`
check returns false and the entry is silently discarded.

### Lifecycle

1. Shard starts → spawn drain thread + command tokio task.
2. Publish arrives → command thread appends to store, releases gate.
3. Drain wakes → scans store → dispatches frames directly to connection writers → atomically bumps `consumer_inflight` / `subject_inflight` / cursor.
4. Drain pushes `Delivered` notification → command thread processes → updates engine's `Binding::pending` for future ack/retire. If `ack_wait_ms > 0`, inserts into timing wheel.
5. Ack arrives → command thread mutates engine → emits `DeltaEvents::subject_hashes_acked` → worker decrements atomic counters and rewinds cursor if needed.
6. Wheel tick (every 1s) → advances wheel → processes expired entries (auto-nack or delayed rewind).

---

## 9. Wire protocol

### Envelope (16 B, `#[repr(C)]`)

```
[2 action] [1 flags] [1 rsv] [4 stream_id] [4 msg_len] [4 env_seq]
```

### Action codes

```
0x01xx  Publish, PublishAccumulate
0x02xx  Deliver, Ack, Nack, RepOk, RepError, RepBatch, BatchAck, FanoutBatch,
        AckSync, BatchAckSync
0x03xx  Subscribe, Unsubscribe
0x04xx  CreateStream, DeleteStream, ListStreams, PurgeStream, DrainSubject
0x05xx  CreateConsumer, DeleteConsumer, ListConsumers, Pause, Resume
0x06xx  Ping, Pong, Connect, Connected, Disconnect
0x07xx  Stats, StatsReply
```

### Frame layouts that matter

**DeliveryBatch** — the hottest frame:

```
[Envelope 16B]
[RepBatchFixed 4B: count + pad]       # consumer_id REMOVED to enable broadcast collapse
[for each entry:]
    [DeliveryEntryHeader 22B: consumer_id u32, seq u64, subj_len u16,
                              data_len u32, subject_hash u32]
    [subject bytes]
    [payload bytes]
```

Broadcast collapse: when N consumers on one connection match the same publish, we emit one frame with N entries, each carrying its own `consumer_id`. The client dispatcher routes each entry to the right subscription.

**CreateConsumer** — carries MaxSubjectInflight trailer:

```
[CreateConsumerFixed 28B]
[name][group][subject]
[4: limits_count]
[for each: 4 limit] [2 pattern_len] [pattern]
```

**BatchAck** — `subject_hash` is echoed so the broker can release credits in O(1) arithmetic without a store lookup.

### `wire_hash_32` — the canonical hash (foldhash)

```rust
pub fn wire_hash_32(data: &[u8]) -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = foldhash::fast::FixedState::default().build_hasher();
    h.write(data);
    h.finish() as u32
}
```

Used for: stream wire id, queue key, subject hash, name lookup.

**Why foldhash** — deterministic across processes (required: the wire protocol encodes `wire_hash_32(stream_name)` and every node must agree), SIMD-friendly (~1 ns for typical subjects, 5–20× faster than the previous FNV-1a sequential byte loop), and has a formal spec (unlike rustc-internal FxHash). The same hasher is reused for every `HashMap<_, _, foldhash::fast::FixedState>` in the codebase — single hash function across wire + in-memory indices. HashDoS is a non-issue: Arbitro does not accept untrusted subject names from anonymous clients.

### Zero-copy decoding

All wire views (`PublishView`, `RepBatchView`, `BatchAckView`, etc.) are thin wrappers around `&[u8]` with lazy accessors. `ref_from_bytes::<T>(buf)` is a pointer cast that costs ~400 ps. No parsing, no allocation.

---

## 10. Storage backends

### `MemoryStore`

- `Vec<StoredEntry>` behind a `Mutex` (drain takes `&*store_guard` for iteration).
- `append(entry_ref, timestamp)` pushes.
- `for_each(start, end, callback)` walks the slice, handing each callback a `&Entry<'_>` that borrows subject / payload directly from the store arena. Zero copy.
- `mark_tombstone(seq)` sets the flag bit on the entry.
- `info()` returns `StoreInfo { messages, bytes, first_seq, last_seq }`.

Threading: the drain holds `mu.lock()` for the duration of `for_each`. This is fine because the drain thread is the only reader that needs high throughput, and the command thread's appends are batched + short.

### `TolerantStore` (disk)

- mmap-backed segments with sequential layout.
- Each record starts with `0xAF` magic byte + 4-byte length + CRC32 + payload.
- On startup, replay scans all segments; on corruption, truncates to the last known-good boundary (so a SIGKILL mid-write doesn't poison the journal).
- Segment rotation at 256 MB.
- `for_each` is identical interface to memory: the mmap pointer gives borrowed subject/payload.

### Store invariant

The store is **agnostic to stream**: each `Entry` carries its own `stream_id`. One store per shard, not per stream — this is the single biggest architectural decision on the storage side. It enables the drain to walk a single linear log instead of managing N logs per stream.

---

## 11. Testing rules

Read `crates/arbitro-engine/.agent/rules/testing.md` before any `cargo test` or `cargo bench`. Highlights:

- **Race detector required** — every PR runs `cargo test --workspace` with no fails.
- **E2E tests live in `crates/arbitro-e2e/tests/*`** — start a real `ArbitroServer` on a random port, connect a real `Client`, assert full lifecycle. Never parse wire frames manually in tests — use the client.
- **Integration tests must call `t.Cleanup`** (Go-style in Rust: `tokio::test` with explicit `shutdown` at end of test). Persisting state across tests leaks into the next one.
- **Helpers in `tests/helpers.rs` or `helpers_test.go`**: `start_server`, `connect`, `collect_until_idle`. Reuse.
- **Table-driven tests** for subject matching and pattern validation — 50+ cases in `crates/arbitro-engine/src/common/subject.rs`.

Full test inventory:

```
crates/arbitro-e2e/tests/
├── integration.rs       # publish/subscribe/ack lifecycle — 15+ cases
├── invariants.rs        # property-based correctness — 20+ cases
│                        # including max_subject_inflight_multiple_patterns
├── persistence.rs       # restart cycles, crash recovery — 13 cases
├── lifecycle_flow.rs    # feature-gated trace dump
```

---

## 12. Benchmark rules

Read `crates/arbitro-engine/.agent/rules/testing.md` §benchmarks. Critical:

1. **WSL only** for benchmarks. Windows is ~10× slower on TCP loopback due to the scheduler and WinSock overhead. Don't publish Windows numbers.
2. **Compile from `/mnt/...`** but **run from `/tmp/...`** — the 9P filesystem bridge drops `mmap` throughput by orders of magnitude.
3. **Always wrap in `timeout 120`** — a broken drain can hang indefinitely.
4. **Tee to `/tmp/bench.log`** so you can re-check numbers without re-running.

Canonical bench invocation:

```bash
wsl bash -lc "cd /mnt/d/.../arbitro && cargo bench --bench throughput --no-run 2>&1"
wsl bash -lc "mkdir -p /tmp/arbitro-bench && \
              cp -a target/release/deps/throughput-* /tmp/arbitro-bench/"
wsl bash -lc "cd /tmp/arbitro-bench && \
              BENCH_MODE=replay BENCH_REPLAY_MSGS=500000 BENCH_CONCURRENCY=1 \
              timeout 120 ./throughput-* --bench 2>&1 | tee /tmp/bench.log"
```

Key benches:

- `crates/arbitro-e2e/benches/throughput.rs` — publish + replay scaling
- `crates/arbitro-e2e/benches/fanout.rs` — multi-subscriber distribution
- `crates/arbitro-e2e/benches/limits.rs` — MaxSubjectInflight isolation under load
- `crates/arbitro-e2e/benches/chaos.rs` — sustained load + SIGKILL recovery
- `crates/arbitro-server/benches/subject_inflight.rs` — counter-strategy comparison (RwLock vs papaya vs DashMap vs bucket)

---

## 13. Agent rules

Files under `crates/arbitro-engine/.agent/rules/`. They are **inviolable** for new code:

| File | What to remember |
|------|------------------|
| `code-zero-copy.md` | No `copy_from_slice` after ingest. `Bytes::clone()` is 3 ns. Scratch buffers reused. |
| `code-hot-cold-path.md` | Hot path = 0 heap allocs, 0 HashMap lookups on inner loops, 0 lock acquisitions, 0 virtual dispatch. |
| `code-anti-patterns.md` | Banned on hot path: `format!`, `String::from`, `Box<dyn Trait>`, `Ordering::SeqCst` without justification, `Instant::now()`, tracing macros. |
| `code-concurrency.md` | Engine is `&mut self`, no locks. Metric counters = `AtomicU64::fetch_add(Relaxed)`. |
| `arch-modules.md` | DAG imports — Level N imports only from Levels < N. Never sideways, never up. |
| `arch-structs.md` | Newtypes over primitives (`StreamId(u32)`). Cache-line alignment for hot structs. Explicit `_pad` fields; `const _: () = assert!(size_of::<T>() == N)`. |
| `performance.md` | Shape determines container (Vec for dense, HashMap for sparse). Batch I/O. No syscalls on hot path. |
| `testing.md` | `-race` required. WSL benches. Timeout every bench. |

---

## 14. How to evolve

### Adding a new feature: the default process

1. **Add wire format** in `arbitro-proto` with `#[repr(C)]` + size assertion.
2. **Add engine state** in `arbitro-engine`. Mutation = `&mut self`, emits `DeltaEvents`.
3. **Add shard handler** in `arbitro-server::shard::handlers` — command thread path only. Call `s.gate.release()` or `s.rebuild_snapshot()` if drain-visible state changed.
4. **Add integration test** in `arbitro-e2e::tests::integration` or `invariants`.
5. **Add bench** if the feature is on the hot path. Run per `testing.md` rules.
6. **Update this document** in §5 (sharding) and §6 (data structures) if a new shared data structure lands.

### `SharedCounters::subject` papaya migration (completed)

Landed. The map now lives at [`shard/shared.rs`](../crates/arbitro-server/src/shard/shared.rs) as
`papaya::HashMap<(u32, u32), AtomicU32, foldhash::fast::FixedState>`. Call sites:
- drain increment: `shard/drain.rs::process_drain_entry` → `counters.inc_subject(consumer_id, subject_hash)`
- drain gate: `shard/drain.rs` → `counters.subject_has_room(consumer_id, subject_hash, max)`
- command decrement on ack: `shard/worker.rs::handle_delta_and_sync` → `counters.dec_subject(cid, sh)`

Zero entries are intentionally retained (ABA-safe). Working-set memory is bounded
to the distinct `(consumer, subject)` pairs observed over the process's lifetime.

### Migrating single → multi-shard replication (P2 groundwork)

- Keep the shard actor model.
- Add a per-stream "replica set" field tracked in metadata.
- Command log (`arbitro-server::persistence::command_log`) already sequentializes admin ops — wrap it in a Raft-replicated log.
- Store-level replication is harder; start with metadata only (CreateStream / CreateConsumer replicate; data stays shard-local). This gives HA for control plane first, data plane later.

---

## 15. Glossary

- **Shard** — a unit of concurrency owning one engine + one store + two threads (drain + command).
- **Stream** — a named logical channel. A stream lives on exactly one shard.
- **Consumer** — a named subscriber to a stream, with ack policy, filters, and optional `MaxSubjectInflight` limits.
- **Binding** — the runtime pairing of a consumer + a connection + a subscription (filter). Holds pending acks.
- **Queue** — a group identifier. Consumers that share a `(stream, group)` receive round-robin (one consumer per message); different groups receive fan-out (each gets every message).
- **Subject** — a dot-delimited name attached to each published message.
- **Drain cycle** — one pass of the drain thread from `cursor + 1` forward up to `cursor + max_feed`. Delivers what it can, skips what's blocked, advances cursor only past fully-processed entries.
- **Broadcast collapse** — emitting a single DeliverBatch frame on one TCP connection that contains entries for multiple consumers.
- **Magic byte `0xAF`** — validates the start of a persisted record in `TolerantStore`. Corruption after the last valid byte is truncated on startup.
- **Snapshot swap** — atomic replacement of the `DrainSnapshot` pointer when structural state changes. Drain readers see either the old or the new state, never torn.
- **`#[must_use] DeltaEvents`** — the compiler forces callers to inspect engine output (wake drain? retire bindings? release subjects?).
- **Timing Wheel** — hashed timing wheel (120 buckets, 1s/tick). Handles ack-timeout (auto-nack) and nack-delay (deferred redelivery). One per shard, lazy-initialized.
- **Lazy cancel** — ack path never touches the wheel. On tick, if the entry is no longer pending, it's silently discarded. Zero overhead for the happy path.

---

*Last updated for the architecture as of this commit. If any invariant in this document disagrees with the code, the code is wrong — or this document needs an update. Open a PR either way.*
