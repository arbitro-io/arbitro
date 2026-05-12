---
description: Crate boundaries and module responsibilities — INVIOLABLE
---

# CRATE BOUNDARIES

The server is the **async coordination layer** between transport and the sync engine. It owns tokio, channels, and threads. The engine owns nothing async.

## ENGINE CONTRACT — BOUNDARY

| Crate | Owns | Never Owns |
|---|---|---|
| `arbitro-engine` | Engine logic, graph, edges, plugins, types, subject matching | tokio, async, threads, channels, I/O |
| `arbitro-server` | Shard routing, channels, thread lifecycle, frame dispatch | Engine logic, graph mutations, edges |
| `arbitro-store` | Journal persistence (append, read, purge) | Routing, delivery, matching |
| `arbitro-proto` | Wire protocol, action codes, zerocopy metadata | Engine logic, transport logic |
| `arbitro-common`| Gates (async signal), Flusher (I/O coalescing) | Engine logic, inflight tracking |

**Server calls `ArbitroEngine` methods. It never reaches into `EngineContext` internals.**

## DEDUPLICATION — USE ENGINE, NOT COMMON

| Feature | USE (source of truth) | DO NOT USE |
|---|---|---|
| `subject_matches()` | `arbitro_engine::common::subject` | `arbitro_common::subject` |
| `SubjectTrie` | `arbitro_engine::common::trie` | `arbitro_common::subject_trie` |
| `fnv1a_32()` | `arbitro_engine::catalog::fnv1a_32` | any other hash |
| Config Enums | `arbitro_engine::types` | `arbitro_proto::config` |

## CONFIG TYPE MAPPING (proto → engine)

Convert at the boundary; never import both simultaneously.
```rust
fn map_stream_config(wire: &proto::config::StreamConfig) -> engine::catalog::StreamConfig {
    engine::catalog::StreamConfig { id: StreamId(wire.stream_id), name: wire.name.to_vec() }
}
```

## MODULE RESPONSIBILITIES

| Module | Responsibility | Depends On |
|---|---|---|
| `command.rs` | ShardCommand enum + reply types | `arbitro-engine` |
| `shard.rs` | ShardWorker: recv, calls engine, owns Store | `command`, `arbitro-engine`, `arbitro-store` |
| `handle.rs` | ShardHandle: async API with oneshot replies | `command`, `tokio::sync` |
| `router.rs` | Server: spawn shards, route by stream_id | `handle`, `shard` |
| `transport.rs`| TCP accept, read/write loops, Registry | `arbitro-proto`, `router`, `command` |
| `wire_parse.rs`| Frame body parsing (zerocopy) | `arbitro-proto::wire` |
| `recovery.rs` | MetadataApplier impl + startup replay | `command_log`, `handle` |

### Dependency direction (strict)
`handle -> command -> engine` | `router -> handle, shard, config` | `transport -> proto, router, command`. No circular dependencies.

## ADDING NEW OPERATIONS
1. Add variant to `ShardCommand` and owned structs in `command.rs`.
2. Add `handle_*` in `shard.rs` and match arm in `ShardWorker::run()`.
3. Add async method in `handle.rs`, wire parsing in `wire_parse.rs`, and dispatch in `transport.rs`.
4. **Never add engine logic in the server** — add it to `arbitro-engine` first.
