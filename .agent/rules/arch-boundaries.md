---
description: Crate boundaries, deduplication rules, and module responsibilities — INVIOLABLE
---

# CRATE BOUNDARIES

The server is the **async coordination layer** between transport and the sync engine. It owns tokio, channels, and threads. The engine owns nothing async.

## ENGINE CONTRACT — BOUNDARY

| Crate | Owns | Never Owns |
|---|---|---|
| `arbitro-engine` (external) | Engine logic, graph, edges, plugins, types, subject matching, inflight, credit | tokio, async, threads, channels, I/O, Arc, Mutex |
| `arbitro-server` | Shard routing, channels, thread lifecycle, async API, transport, frame dispatch | Engine logic, graph mutations, edge indexes, plugins |
| `arbitro-store` | Journal persistence (append, read, purge) | Routing, delivery, matching |
| `arbitro-proto` | Wire protocol, action codes, configs, error codes | Engine logic, transport logic |
| `arbitro-common` | Gates (async signal), Flusher (I/O coalescing), patterns_overlap | Engine logic, inflight tracking, credit |
| `arbitro-proto` (metadata) | Zerocopy metadata commands (`MetadataCommandView`, `MetadataApplier` trait) | Engine logic, transport, file I/O |

**The server calls `ArbitroEngine` methods. It never reaches into `EngineContext` internals.**

### Correct
```rust
let reply = self.engine.publish(&batch);
let claimed = self.engine.claim(&batch);
```

### Banned
```rust
ctx.graph.insert_pending(node);
ctx.edges.get_mut::<PendingByConsumer>();
ctx.inflight.inc(scope, key);
ctx.catalog.ensure_stream(...);
```

## DEDUPLICATION — USE ENGINE, NOT COMMON

The engine already implements these. Using common's version creates duplicate code paths.

| Feature | USE (source of truth) | DO NOT USE |
|---|---|---|
| `subject_matches()` | `arbitro_engine::common::subject` | `arbitro_common::subject` |
| `SubjectTrie` | `arbitro_engine::common::trie` | `arbitro_common::subject_trie` |
| `fnv1a_32()` | `arbitro_engine::catalog::fnv1a_32` | any other hash |
| `AckPolicy` | `arbitro_engine::types::AckPolicy` | `arbitro_proto::config::AckPolicy` |
| `DeliverMode` | `arbitro_engine::types::DeliverMode` | `arbitro_proto::config::DeliverMode` |
| Inflight tracking | engine internal (via `claim`/`ack` API) | `arbitro_common::CreditMap` |
| Credit tracking | engine internal (via plugin) | `arbitro_common::CreditMap` |

## USE COMMON FOR (engine doesn't have)

| Feature | Crate | Reason |
|---|---|---|
| `Gate` / `ReactiveGate` | `arbitro-common` | Async signaling — engine is sync |
| `Flusher` / `FlushConfig` | `arbitro-common` | I/O write coalescing — server concern |
| `patterns_overlap()` | `arbitro-common` | Engine doesn't implement this |

## USE PROTO FOR (wire format source of truth)

| Feature | Crate |
|---|---|
| `Envelope` / `FrameView` | `arbitro_proto::wire` |
| `Action` enum (all codes) | `arbitro_proto::action` |
| All wire body types | `arbitro_proto::wire::*` |
| `ErrorCode` | `arbitro_proto::error` |
| `StreamConfig` (wire-level) | `arbitro_proto::config` |
| `ConsumerConfig` (wire-level) | `arbitro_proto::config` |

## CONFIG TYPE MAPPING (proto → engine)

Server parses wire `StreamConfig` (from proto) → maps to engine's `StreamConfig` for `ensure_stream()`.
Server parses wire `ConsumerConfig` (from proto) → maps to engine's `ConsumerConfig` for `ensure_consumer()`.

Never import both simultaneously. Convert at the boundary:

```rust
// In wire_parse.rs or transport.rs
fn map_stream_config(wire: &proto::config::StreamConfig) -> engine::catalog::StreamConfig {
    engine::catalog::StreamConfig {
        id: StreamId(wire.stream_id),
        name: wire.name.to_vec(),
    }
}
```

## MODULE RESPONSIBILITIES

| Module | Responsibility | Depends On |
|---|---|---|
| `command.rs` | ShardCommand enum + reply types | `arbitro-engine::types`, `arbitro-engine::batch`, `arbitro-engine::fanout` |
| `shard.rs` | ShardWorker: blocking recv, calls engine, owns Store | `command`, `arbitro-engine`, `arbitro-store` |
| `handle.rs` | ShardHandle: async API with oneshot replies | `command`, `tokio::sync` |
| `router.rs` | Server: spawn shards, route by stream_id | `handle`, `shard`, `config` |
| `transport.rs` | TCP accept, read/write loops, ConnectionRegistry | `arbitro-proto`, `router`, `command` |
| `wire_parse.rs` | Frame body parsing (all zerocopy) | `arbitro-proto::wire` |
| `drain_task.rs` | Reactive delivery loop | `handle`, `arbitro-common::Gate` |
| `command_log.rs` | File-based command log (length-prefix framing, zerocopy) | `arbitro-proto::metadata` |
| `recovery.rs` | MetadataApplier impl + startup replay | `command_log`, `handle` |
| `config.rs` | ServerConfig | standalone |

### Dependency direction (strict)
```
handle.rs ──→ command.rs ──→ arbitro-engine::types
router.rs ──→ handle.rs, shard.rs, config.rs
shard.rs  ──→ command.rs, arbitro-engine (public API only), arbitro-store
transport.rs ──→ arbitro-proto, router.rs, command.rs
wire_parse.rs ──→ arbitro-proto::wire (only)
drain_task.rs ──→ handle.rs, arbitro-common::Gate
command_log.rs ──→ arbitro-proto::metadata
recovery.rs ──→ command_log.rs, handle.rs
```

No circular dependencies. No module imports from a sibling that imports it back.

## ADDING NEW OPERATIONS

1. Add command variant to `ShardCommand` in `command.rs`
2. Add owned request/reply structs in `command.rs`
3. Add `handle_*` method in `shard.rs` (calls engine, sends reply)
4. Add match arm in `ShardWorker::run()`
5. Add async method in `handle.rs` (builds command, sends to channel, awaits reply)
6. Add wire parsing in `wire_parse.rs`
7. Add dispatch arm in `transport.rs`
8. **Never add engine logic in the server** — if it needs new behavior, add it to `arbitro-engine` first
