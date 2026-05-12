---
description: Shard worker role boundaries — INVIOLABLE
---

# ROLES

Shards parallelize CPU work between network I/O and storage:
`NET IN → [I/O pool] → Shard.INGRESS → Disk Writer → DISK → Disk Reader → Shard.EGRESS → NET OUT`

- **Hot path**: ns/µs budget.
- **Cold path**: per-session/config.

## Primitive Ownership
| Primitive | Write | Read |
|---|---|---|
| `Store` | Ingress.Publisher/Accumulator | Egress.Drainer, Seeder |
| `Engine` | Ingress.Acker/Admin | Egress.Drainer |
| `Gate` | Ingress roles (open) | Worker Loop (wait), Egress.Drainer (close) |
| `Bindings` | Ingress.Admin | Egress.Drainer |

**Fences**:
- Ingress never delivers; Egress never replies.
- Disk I/O is asynchronous (MPSC to Disk Writer).

## Worker Loop
1. **INGRESS**: Drain channel -> Publisher/Accumulator/Acker/Admin.
2. Flush accumulator (deadline/threshold).
3. **EGRESS**: If `gate.is_open()` -> Drainer.
4. **PARK**: If channel empty && gate closed.

---

# INGRESS (Command-driven)

## PUBLISHER [hot]
- persist -> ack. `store.append_batch` -> Reply -> `gate.release()`.
- **Invariant**: Zero subscriber cost = append + reply + release.

## ACCUMULATOR [hot]
- Buffer small publishes -> batch store append.
- **Invariant**: Atomically succeed or fail for all buffered callers.

## ACKER [hot]
- `engine.ack` / `engine.nack`.
- **Rule**: `gate.release()` ONLY if work was accepted/requeued.

## PULL_HANDLER [hot]
- On-demand fetch (`fetcher::fetch`).
- **Invariant**: Read-only on topology. No drain-style fan-out.

## ADMIN / CONTROL [cold]
- `create/delete`, `subscribe/unsubscribe`, `pause/resume`.
- **Flow**: Structure change -> Update Engine -> Publish Topology Snapshot.
- **Invariant**: State must be self-consistent before returning.

## SEEDER [cold]
- Recovery or first Push sub on non-empty stream.
- `enqueue_ready` store entries into engine.
- **Rule**: Never call `engine.publish` (preserves store seqs).

---

# EGRESS (Data-driven)

## DRAINER [hot]
- `gate.is_open()` -> Deliver frames.
1. **Guard**: Exit and `gate.lock()` if `bindings.is_empty()`.
2. **Feed**: Incremental engine feed from `last_engine_seq + 1`.
3. **Loop**: `claim(64)` -> `store.get()` -> `send_deliver_frame`.
4. **Close**: Delivered something -> `release()`; Nothing -> `lock()`.

---

# SHARED PRIMITIVES

## FETCHER
- Pure read function: `fetch(store, cursor, filter, limits)`.
- Advances cursor regardless of matches. No mutation.

---

# CROSS-ROLE RULES
1. **Single Writer**: Gate close = Egress.Drainer only. Admin = Bindings/Topology only.
2. **Doorbell Protocol**: Ingress rings the Gate; only Egress decides to stop listening.
3. **No Speculation**: Don't iterate "in case" a consumer appears; use the Seeder.
4. **Isolation**: Latency of one role must not impact others (e.g., Admin ⊥ Publisher).
5. **Push/Pull Segregation**: `SubscriptionKind` determines the slot. No runtime `if` in hot loops.
6. **Async Disk**: Shard never calls `fsync` inline; enqueues to Disk Writer.
