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

## MODULE RESPONSIBILITIES (current layout — M28 refresh)

The server is split into `shard/`, `transport/`, `common/`, and
`persistence/` modules. The historical single `shard.rs` / `transport.rs`
files no longer exist.

| Module | Responsibility | Depends On |
|---|---|---|
| `shard/command.rs` | `ShardCommand` enum + reply types | `arbitro-engine` |
| `shard/worker.rs` | `DrainWorker` (OS thread) + `CommandWorker` (tokio task); owns engine | `arbitro-engine`, `arbitro-store` |
| `shard/drain.rs` | `drain_read` / `drain_deliver` for the DrainWorker | `worker`, `arbitro-store` |
| `shard/handlers.rs` | Per-command handler impls on `CommandWorker` | `worker`, `arbitro-engine` |
| `shard/idempotency.rs` | Per-shard dedup tracker | `arbitro-engine` |
| `shard/consumer_subjects.rs` | Drain-owned per-(consumer,subject) inflight | — |
| `shard/drain_events.rs` | SPSC ring (command → drain) | — |
| `shard/handle.rs` | `ShardHandle`: async API w/ oneshot replies | `command`, `tokio::sync` |
| `shard/router.rs` | `ShardRouter`: spawn shards, route by stream_id | `handle`, `worker` |
| `shard/shared.rs` | `SharedCounters` + `DrainSnapshot` + ring types | — |
| `transport/dispatch_v2.rs` | v2 frame dispatch (HOT publishes + COLD mgmt) | `arbitro-proto`, `router` |
| `transport/registry.rs` | `ConnectionRegistry` + per-conn writer tasks | `tokio::sync`, `common::session` |
| `transport/tls.rs` | Optional TLS acceptor | `tokio-rustls` (feature-gated) |
| `common/silent_drops.rs` | H10 silent-drop counters (Arc<atomics>) | — |
| `common/reply_v2.rs` | RepOk / RepError builders | `transport::registry` |
| `common/session.rs` | `Session`, `ConnIdGen`, write-buffer cap | `tokio::sync` |
| `persistence/command_log.rs` | Optional metadata journal | `arbitro-store` |
| `persistence/recovery.rs` | `ReplayApplier` for startup replay | `command_log`, `router` |
| `server.rs` | Accept loop, keepalive, metrics_loop, shutdown | every module above |

### Dependency direction (strict)
`handle -> command -> engine` | `router -> handle, worker, drain, shared` | `transport -> proto, router, common`. No circular dependencies.

## ADDING NEW OPERATIONS
1. Add variant to `ShardCommand` and owned structs in `command.rs`.
2. Add `handle_*` in `shard.rs` and match arm in `ShardWorker::run()`.
3. Add async method in `handle.rs`, wire parsing in `wire_parse.rs`, and dispatch in `transport.rs`.
4. **Never add engine logic in the server** — add it to `arbitro-engine` first.
