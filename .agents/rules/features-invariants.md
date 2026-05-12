---
description: Per-feature invariants and identity rules — INVIOLABLE
---

# FEATURES & INVARIANTS

## Identity model
| ID | Derived from | Allocated at |
|---|---|---|
| `StreamId` | `fnv1a_32(name)` | First `CreateStream` |
| `ConsumerId` | **NAME ONLY** (Global) | First `CreateConsumer` |
| `QueueId` | `(stream_id, group)` | First time tuple is seen |

### CRITICAL: Consumer Name Uniqueness
`ConsumerId` is keyed by **NAME ONLY** across all streams. Shared names share state (`max_subject_inflight`, `paused`). 
- **Workaround**: Suffix consumer names with tenant discriminators (`isolation_tester_c1`).

## Feature Matrix
1. **Fanout (`DeliverMode::Fanout`)**: Every matching binding receives a copy.
   - *Invariant*: Unique consumer names and distinct groups (shared groups cause silent queueing).
2. **Queue (`DeliverMode::Queue`)**: Round-robin across bindings with the same `QueueId`.
   - *Invariant*: All members must share the same `group` and `stream_id`.
3. **`max_subject_inflight`**: Caps outstanding deliveries per `(consumer, subject)`.
   - *Invariant*: `AckPolicy::Explicit` required. `AckPolicy::None` will wedge immediately.
   - *Gap*: Limit is set at creation; re-creation with different limit fails (`ConsumerAlreadyExists`).
4. **`max_inflight`**: Total outstanding pendings per `ConsumerId`.
   - *Invariant*: `AckPolicy::Explicit` required. `u32::MAX` means unbounded.
5. **Pause / Resume**: Sets `paused = true` on bindings.
   - *Invariant*: Cached on `ActiveBinding`. Paused consumers still accumulate pendings already in-flight.
6. **Ack / Nack**: `Ack` removes pending; `Nack` flags for redelivery.
   - *Invariant*: Client MUST echo `subject_hash`. Wrong hash = wedge.
7. **Stream TTL (`max_age_secs`)**: Discards entries older than `now - TTL` at delivery.
   - *Invariant*: Evaluated at delivery, not publish. `0` disables check.
8. **AckPolicy::None (Fire-and-forget)**: Saves ~8MB heap and ~20 ns/msg.
   - *Invariant*: Limits are effectively disabled (no acks to decrement).
9. **Deletion**: `delete_stream` / `delete_consumer`.
   - *Invariant*: `retire_binding` MUST decrement inflight counters to avoid credit leaks. Stream deletion does NOT cascade to consumers.

## Validation Gaps (Severity Order)
- **Shared Consumer Names**: Tenant isolation leak (`dispatch.rs`).
- **AckPolicy::None + Limits**: Accepted at creation but wedges immediately.
- **Stale Config**: Re-creation error leaves old config active.
- **Unknown Ack Seq**: Silently dropped (masks client bugs).
- **Fanout with Groups**: Silently becomes Queue.

## Client API — Idempotent Helpers
- `get_or_create_stream` / `get_or_create_consumer`: Swallow `AlreadyExists` errors.
- Use raw `create_*` for bootstrap flows requiring hard failure on config drift.
