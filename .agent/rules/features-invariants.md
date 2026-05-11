---
description: Per-feature invariants, identity rules, and validation gaps — INVIOLABLE
---

# FEATURES & INVARIANTS

Each feature of the broker has a set of implicit invariants. When one is broken, the feature silently degrades (fanout becomes queue, queue collapses to one consumer, `max_subject_inflight` leaks across tenants, acks decrement the wrong counter). This file enumerates each feature, its invariants, and the validation gaps that let users violate them.

---

## Identity model (read first)

The entire broker keys behavior off three IDs. Understand how they are allocated before reading anything else.

| ID | How it is derived | Allocated at | Key paths |
|---|---|---|---|
| `StreamId` | `fnv1a_32(name)` on the client → registered as a small sequential int on the server via `NameRegistry.get_or_create_stream(wire_id)` | First `CreateStream` for that name | `crates/arbitro-common/src/name_registry.rs:145` |
| `ConsumerId` | **By name only**. `NameRegistry.get_or_create_consumer(name)` returns a sequential int. Stream is NOT part of the key. | First `CreateConsumer` for that name | `crates/arbitro-common/src/name_registry.rs:195` |
| `QueueId` | Content-addressed by `(stream_id, group_bytes)`. Same stream + same group → same queue → round-robin sharing. | First time the tuple is seen | `crates/arbitro-common/src/name_registry.rs:231` |

### CRITICAL INVARIANT — consumer name uniqueness

**`ConsumerId` is keyed by NAME ONLY, across every stream in the process.** Two `create_consumer` calls with the same name — even against different streams — collapse to the same `ConsumerId`. This is NOT documented in the public API and is a primary source of silent misbehavior:

- Per-consumer state (`max_subject_inflight` counter, `paused`, `pending`) is keyed by `ConsumerId`. Two logical consumers sharing a `ConsumerId` share that state.
- The limits bench failure (2026-04) was exactly this: three clients all used `isolation_tester` → one `ConsumerId` → one subject counter → throttling collapsed.

Workarounds until a validation pass lands:
- Always suffix consumer names with a client/tenant discriminator (`isolation_tester_c{i}`).
- Prefer the new `Client::get_or_create_consumer` helper — it makes the idempotency explicit and forces the caller to think about whether a shared name is intended.

---

## Feature matrix

Each row: what the feature does, the invariants that must hold for it to work, the error raised when violated (if any), and the validation gap.

### 1. Fanout delivery (`DeliverMode::Fanout`)

- **Does:** every matching binding receives a copy of every matching message.
- **Invariants:**
  1. Each logical consumer has a **unique name** (otherwise their bindings collapse onto one `ConsumerId` and become queue-like).
  2. `group` is either empty or distinct per consumer (shared group → shared `QueueId` → round-robin).
- **Errors raised:** none at validation time. A collision produces silent queue-like behavior.
- **Gap:** no warning when two consumers with the same group are configured as `Fanout`.

### 2. Queue delivery (`DeliverMode::Queue`)

- **Does:** round-robin one message across all bindings sharing the same `QueueId`.
- **Invariants:**
  1. All queue-group members must share the **same `group` bytes** and the **same `stream_id`** (otherwise they resolve to different `QueueId`s and do not share load).
  2. Members may have **different consumer names** — each needs its own `ConsumerId` to track inflight independently. Sharing a name is a bug (see §Identity).
  3. Rotation is `entry.seq % n` across the queue group (`crates/arbitro-server/src/shard/drain.rs`). This only gives fairness when `n` is stable across the drain cycle.
- **Errors raised:** none.
- **Gap:** no validation that queue members agree on `group`; a typo silently isolates a member.

### 3. `max_subject_inflight`

- **Does:** caps outstanding deliveries per `(consumer_id, subject_hash)`. When the counter reaches the limit, the drain skips the entry, rewinds the cursor, and retries after acks decrement the counter.
- **Invariants:**
  1. **Consumer MUST be unique per tenant** (see §Identity). Shared name → shared counter → one tenant throttles everyone.
  2. **`AckPolicy` MUST be `Explicit`.** Fire-and-forget consumers never decrement the counter (no ack arrives) — the limit would trigger once then wedge.
  3. The limit is set at `CreateConsumer` time. Re-creating a consumer with a different limit returns `ConsumerAlreadyExists` — the old limit persists.
- **Errors raised:** `ConsumerAlreadyExists` when re-creating with different config (see §Validation gaps).
- **Gap:** no runtime check that `AckPolicy::None` consumers have an empty `max_subject_inflights` list; mis-configured clients silently wedge.
- **Code:** `crates/arbitro-server/src/shard/shared.rs` (counter), `crates/arbitro-server/src/shard/drain.rs` (check + rewind), `crates/arbitro-engine/src/runtime/execute.rs` (ack decrement via `DeltaEvents.subject_hashes_acked`).

### 4. `max_inflight` (per-consumer)

- **Does:** caps total outstanding pendings per `ConsumerId` across all subjects/bindings.
- **Invariants:**
  1. Same as `max_subject_inflight`: unique consumer name, `AckPolicy::Explicit`.
  2. `u32::MAX` means "unbounded" — the drain skips the capacity check for fire-and-forget bindings (`Binding.fire_and_forget` flag).
- **Errors raised:** none.
- **Gap:** no check on `max_inflight == 0` (would prevent any delivery).

### 5. Pause / Resume

- **Does:** `pause_consumer(id)` sets `Binding.paused = true` on every binding for that consumer; drain skips entries but does NOT rewind the cursor — pending will redeliver only after `resume_consumer`.
- **Invariants:**
  1. Paused state is replicated into `ActiveBinding.paused` at subscribe time and updated at pause/resume (cached to avoid HashMap lookup on the drain inner loop).
  2. A paused consumer still accumulates pendings on its bindings from deliveries that were already in-flight when pause was called.
- **Errors raised:** none. Pausing a non-existent consumer is silently ignored.
- **Gap:** no `ConsumerNotFound` on `pause_consumer` / `resume_consumer`.

### 6. Ack / Nack

- **Does:** `Ack` removes a pending from the binding, decrements the per-consumer subject counter, and emits `subject_hashes_acked` in `DeltaEvents`; `Nack` is the same decrement but flagged for redelivery by drain.
- **Invariants:**
  1. Wire format embeds `subject_hash` in both `DeliveryEntryHeader` and `AckEntry`. The client MUST echo the hash it received. Missing / wrong hash → counter never decrements → subject wedges at the inflight limit.
  2. Ack is O(1) arithmetic — the engine never re-reads the store.
- **Errors raised:** silently dropped if `seq` is unknown. Duplicate acks are no-ops.
- **Gap:** no diagnostic counter for "ack with unknown seq" — masks client bugs.

### 7. Stream TTL (`max_age_secs`)

- **Does:** drain discards entries older than `now - max_age_ms` and emits `Command::Tombstone { reason: Expired }`.
- **Invariants:**
  1. TTL is evaluated at delivery time, not at publish. A backlog exceeding TTL is silently discarded.
  2. `max_age_secs = 0` disables the check.
- **Errors raised:** none (expected behavior, not an error).
- **Gap:** no metric for expired-on-delivery count exposed to the client.

### 8. AckPolicy::None (fire-and-forget)

- **Does:** skips `pending.push` and `inflight.inc_pending` on delivery (Fix 1 from `concurrent-coalescing-marble.md`). Saves ~8MB heap at 500k msgs and ~20 ns per message.
- **Invariants:**
  1. `max_subject_inflight` and `max_inflight` are effectively disabled — acks never arrive to decrement.
  2. `retire_binding` is still correct: iterating an empty `pending` and `inflight=0` is a no-op.
- **Errors raised:** none.
- **Gap:** configuring both `AckPolicy::None` and any `max_subject_inflight` limit is silently accepted but the limit never fires; should be rejected at `CreateConsumer`.

### 9. Stream and Consumer deletion

- **Does:** `delete_stream` / `delete_consumer` retire all bindings first (releasing inflight counters), then remove the entity from the catalog.
- **Invariants:**
  1. `retire_binding` MUST iterate `binding.pending` and decrement every counter it touched. Skipping this leaks credits.
  2. Deleting a stream does NOT cascade to its consumers (consumers are name-keyed, not stream-keyed) — the client must delete them explicitly.
- **Errors raised:** `StreamNotFound` / `ConsumerNotFound` if unknown.
- **Gap:** orphaned consumers persist in `NameRegistry.consumers_by_name` after their sole stream is deleted; the name stays reserved.

---

## Validation gaps — summary

Listed in severity order. Each is a silent failure that can damage production workloads:

| # | Gap | Location | Symptom |
|---|---|---|---|
| 1 | Duplicate consumer name across tenants silently collapses onto one `ConsumerId` | `crates/arbitro-server/src/transport/dispatch.rs:563` (the `_created` bool is ignored) | `max_subject_inflight` and `max_inflight` shared across tenants; pause affects everyone |
| 2 | `create_consumer` with `AckPolicy::None` + `max_subject_inflights.non_empty()` is accepted | `crates/arbitro-server/src/transport/dispatch.rs:584` | Subject wedges at the first delivery |
| 3 | Re-creating a consumer with a different config returns `ConsumerAlreadyExists` but the old config stays — callers get an error with no way to reconcile | `crates/arbitro-server/src/shard/handlers.rs` (cold path) | Silent stale config |
| 4 | `pause_consumer` / `resume_consumer` silently accept unknown `ConsumerId` | engine `pause_consumer` returns `bool`, server ignores it | Pause seems to work, never does |
| 5 | Queue members with typo in `group` silently isolate — no warning | n/a | Load imbalance that looks like slow consumer |
| 6 | Ack with unknown `seq` silently dropped, no counter | `crates/arbitro-engine/src/runtime/execute.rs` | Masks client bugs |
| 7 | Fanout consumers sharing a `group` silently become round-robin | n/a | Messages appear to "vanish" on alternate consumers |
| 8 | `max_inflight = 0` accepted | n/a | No deliveries, no diagnostic |

---

## Client API — idempotent helpers

The raw `Client::create_stream` / `Client::create_consumer` calls surface `ErrorCode::StreamAlreadyExists` / `ErrorCode::ConsumerAlreadyExists` as `ClientError::Broker(...)`. In practice callers almost always want "create if missing, succeed if present".

The following helpers are provided to avoid hand-rolling this at every call site (and to centralize the decision of what counts as idempotent):

- `Client::get_or_create_stream(&StreamConfig) -> Result<(), ClientError>`
- `Client::get_or_create_consumer(&ConsumerConfig) -> Result<Consumer, ClientError>`

Both swallow `StreamAlreadyExists` / `ConsumerAlreadyExists` and return a handle to the pre-existing resource. Any other error propagates unchanged.

**When to NOT use the helper:** in bootstrap flows where you want a hard failure on config drift (e.g. CI that recreates a fresh broker per run). Use the raw `create_*` then.

---

## Testing invariants

When adding a new feature, add an e2e test that violates each invariant and asserts the expected behavior (error or documented silent degradation). A feature with zero violation tests is a feature with zero documented invariants.

Relevant benches that exercise the multi-tenant invariants:
- `crates/arbitro-e2e/benches/limits.rs` — Stage 3 `multi_client_isolated_latency` (one stream + one consumer per client, unique names)
- `crates/arbitro-e2e/benches/chaos.rs` — `BENCH_CHAOS_CONSUMERS` (N parallel consumers with unique names + groups)
- `crates/arbitro-e2e/benches/ack.rs` — Stage 3 multi-client ack throughput
