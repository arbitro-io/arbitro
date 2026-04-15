---
description: Module dependency DAG — strict level-based imports, single responsibility, no circular deps. INVIOLABLE.
---

# MODULE ARCHITECTURE — STRICT DEPENDENCY DAG

---

## DEPENDENCY LEVELS

```
Level 0 (leaf):  types, error, metrics              → no internal deps
Level 1:         batch, reply, wire                 → types, error ONLY
Level 2:         graph/slab, graph/node             → types, error ONLY
Level 3:         graph/mod, edge/*, inflight, ready, idempotency → Level 0-2
Level 4:         plugin/*                           → Level 0-3
Level 5:         catalog/*                          → Level 0-4
Level 6:         context                            → Level 0-5 (accessor, no logic)
Level 7:         runtime/*, admin/*                 → Level 0-6
Level 8:         lib.rs                             → everything (root facade)
```

**HARD RULE: A module may ONLY import from modules at a LOWER level. Never sideways, never up.**

---

## MODULE RESPONSIBILITY MATRIX

| Module | Owns | MUST NOT |
|---|---|---|
| `types.rs` | All ID newtypes, Timestamp, PayloadRef, CreditScope | Import any other module |
| `error.rs` | EngineError, ErrorCode | Import except `types` |
| `metrics.rs` | `EngineMetrics` (repr(C, align(64)) atomic counters), `MetricsSnapshot` — the ONLY sanctioned hot-path observability | Import any other module; take locks; hold `&mut self` across `fetch_add` |
| `wire.rs` | `decode_slice`, `decode_ref` — zerocopy wire helpers | Import except `types` |
| `graph/slab.rs` | TypedSlab<T> generational arena | Know about edges, plugins, delivery |
| `graph/node.rs` | Entity structs: ConnectionNode, ConsumerNode, PendingNode | Know about edges, plugins, runtime |
| `graph/mod.rs` | GraphStore facade over typed slabs | Know about edge semantics, delivery |
| `edge/mod.rs` | EdgeRegistry, EdgeIndex trait, notify_removed | Know about specific edge implementations |
| `edge/builtin.rs` | Concrete edge indexes (PendingBy*, BindingsBy*) | Modify inflight, touch plugins, perform delivery |
| `edge/plugin.rs` | PluginEdge trait for custom edges | Know about specific plugins |
| `plugin/mod.rs` | PluginRegistry, Plugin trait, TypeId access | Know about specific plugin implementations |
| `plugin/scheduler.rs` | Timer wheel for deadlines | Know about ack semantics or delivery |
| `plugin/credit.rs` | Multi-scope credit counter arrays | Know about delivery or graph structure |
| `plugin/event_bus.rs` | Event dispatch | Know about event handling logic |
| `plugin/config.rs` | Config command log persistence | Know about runtime operations |
| `catalog/match_table.rs` | Precomputed subject→consumer hash table | Perform delivery (only builds the map) |
| `catalog/mod.rs` | CatalogApi: ensure_stream/consumer/subscription | Know about runtime operations |
| `ready/ring.rs` | ReadySubjectRing round-robin | Know about consumers, transport |
| `inflight/mod.rs` | 3 counter arrays (subject, consumer, queue) | Know about delivery, edges, plugins |
| `idempotency/mod.rs` | Time-bucketed exact hash set | Know about any module except `types` |
| `runtime/publish.rs` | Parse, dedup, match, store, enqueue | NOT deliver, NOT modify consumer state |
| `runtime/ack.rs` | Lookup pending, execute release protocol | NOT deliver, NOT modify match tables |
| `runtime/claim.rs` | Pop ready, build Pending, dec credits, send | NOT store, NOT modify match tables |
| `runtime/seed.rs` | Replay a store-backed entry into every matching ready queue (dedup via `ctx.seed_scratch`, no cap) | Assign seqs (comes from store), touch idempotency |
| `runtime/drain.rs` | Mass removal (disconnect, delete consumer) | NOT store, NOT match subjects |
| `runtime/bind.rs` | Manage subscription↔connection edges | NOT deliver, NOT modify store |
| `context.rs` | EngineContext struct (wiring) | NOT contain logic, only provide access |
| `lib.rs` | ArbitroEngine builder, public facade | NOT contain processing logic |

---

## ENFORCEMENT

1. `mod.rs` files are for re-exports ONLY — no logic
2. Files must not exceed 400 lines — split into submodules
3. Functions must not exceed 60 lines — extract helpers
4. No `pub` without doc comment — either document or `pub(crate)`
5. CI denies forbidden `use` paths (grep for violations)

---

## SINGLE RESPONSIBILITY EXAMPLES

```rust
// ✅ publish.rs ONLY does: parse → dedup → match → store → enqueue
// It does NOT deliver, does NOT modify consumer state

// ✅ ack.rs ONLY does: slab lookup → release_pending
// It does NOT deliver, does NOT rebuild match tables

// ✅ claim.rs ONLY does: pop ready → build Pending → send
// It does NOT modify store, does NOT evaluate filters

// ❌ A file that publishes AND delivers — two responsibilities
// ❌ A file that acks AND rebuilds match tables — two responsibilities
```
