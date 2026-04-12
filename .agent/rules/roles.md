---
description: Shard worker role boundaries — hot vs cold path, ownership fences — INVIOLABLE
---

# ROLES

Shard worker is single-threaded, multiplexes disjoint roles. Crossing a boundary leaks work or couples latency. Before adding/moving a handler, identify the owning role. If none fits, define a new one here.

- **Hot path** — per-message, ns/µs budget.
- **Cold path** — per-session/config/lifetime, µs/ms budget.

## Primitive ownership

| Primitive | Read | Write |
|---|---|---|
| `Store` (per stream) | drainer, seeder | publisher, accumulator.flush |
| `ArbitroEngine` | drainer | drainer, seeder, acker, admin |
| `Gate` | worker loop | publisher/accumulator/acker/admin = open; **drainer only** = close |
| `bindings` | drainer | admin only |
| `seeded_streams`, `last_engine_seq` | drainer | seeder, admin |
| `accum_streams` | accumulator | accumulator |

**Non-negotiable fences:** publisher never touches engine. Drainer never replies to publish. Admin never delivers. Seeder runs at most once per stream.

## Worker loop

```
loop {
    1. drain rx → {publisher | accumulator.enqueue | acker | admin}    // mixed hot+cold
    2. flush_accumulator if deadline/threshold                          // hot
    3. if gate.is_open() → drainer                                      // hot
    4. rx empty && gate closed → park
}
```

Gate = doorbell. Opened by any role that creates deliverable conditions. Closed only by drainer.

---

# HOT PATH

## PUBLISHER

- **At:** `shard.rs :: handle_publish`
- **Trigger:** `Publish` cmd
- **Job:** accept → persist → ack

**MUST:**
1. `store.append_batch`
2. Reply `RepOk(first_seq)` or `RepError`
3. `gate.release()` after ack

**MUST NOT:** touch engine, read store after write, build Deliver frames, know about consumers, couple latency to drainer.

**Invariant:** zero subscribers cost = `store.append + reply + gate.release`. Anything more is a bug.

## ACCUMULATOR

- **At:** `handle_publish_accumulate` (enqueue) + `flush_accumulator` (loop step 2)
- **Trigger:** `PublishAccumulate` cmd, or deadline/size/byte threshold
- **Job:** buffer small publishes → one batched store append

**MUST:** buffer with caller metadata (conn_id, env_seq, entry_count); reset deadline on enqueue; on flush run the publisher path atomically (`append_batch` → reply all callers with computed seqs → one `gate.release()`); on failure reply `StreamFull` to all buffered callers.

**MUST NOT:** ack before store append; hold callers past deadline; touch engine; use when caller needs per-msg sync ack (use `publish_sync`).

**Invariant:** all buffered callers succeed atomically or all fail. No partial state.

## ACKER

- **At:** `handle_ack`, `handle_nack`
- **Trigger:** `Ack`/`Nack` cmds
- **Job:** feedback from consumers into engine state

**MUST:**
1. Copy entries into `scratch_ack` / `scratch_nack` (engine-contract borrow rules)
2. Call `engine.ack` / `engine.nack`
3. Reply with counts
4. `gate.release()` **iff** `accepted > 0` (ack) or `requeued > 0` (nack)

**MUST NOT:** deliver messages; touch store; mutate bindings/last_engine_seq; release gate unconditionally.

**Invariant:** engine state reflects request exactly. Gate released iff drainer would find new work.

## DRAINER

- **At:** `handle_drain_deliver` (+ `publish_pending_to_engine`, `send_deliver_frame`)
- **Trigger:** worker loop step 3, only if `gate.is_open()`
- **Job:** store data + waiting consumers → Deliver frames

**MUST — in this exact order:**

1. **Guard 0 (first statement):**
   ```rust
   if self.bindings.is_empty() { self.gate.lock(); return; }
   ```
2. **Feed engine** only for streams with ≥1 binding, up to `store.info().last_seq`. First time → seeder path (`enqueue_ready`). Otherwise → incremental feed from `last_engine_seq+1`. Both preserve store seqs.
3. **Per binding:** loop `claim(64)` → `store.get(seq)` → `send_deliver_frame`, until claim returns < max_items or inflight full. Copy `ClaimedEntry` before next engine call.
4. **Close cycle:** delivered something → `gate.release()`; nothing → `gate.lock()`.

**MUST NOT:** reply to publishes; do engine work for streams with no bindings (per-stream check inside feed, not just Guard 0); block network I/O; mutate `bindings`/store/catalog.

**Invariant:** no speculative work. Past `bindings.is_empty()`, every stream touched has ≥1 destinatario.

---

# COLD PATH

## ADMIN / CONTROL

- **At:** `handle_{create,delete}_stream`, `handle_{create,delete}_consumer`, `handle_{subscribe,unsubscribe}`, `handle_{open,drain}_connection`, `handle_bind`, `handle_{pause,resume}_consumer`, `handle_list_*`, `handle_store_info`
- **Job:** management ops, keep shard state consistent

**MUST:** always reply oneshot (drop = shard-crashed to caller); keep `bindings`/`stores`/`seeded_streams`/`last_engine_seq` consistent with engine catalog/graph.

**Subscribe flow:** `ensure_stream` → `ensure_consumer` → `ensure_subscription` → seeder (if unseeded) → `bind` → push `ActiveBinding` → `gate.release()`.

**Delete stream:** `engine.remove_stream_full` → remove `stores[id]` (purge disk if `purge_disk`) → clear `seeded_streams[id]` and `last_engine_seq[id]` → retain-filter `bindings`.

**Delete consumer / unsubscribe:** engine drain → catalog remove → retain-filter `bindings`.

**MUST NOT:** do hot-path work; leave half-applied state on error; rely on drainer to fix inconsistencies; forget to clear `last_engine_seq` on delete (recreated stream would never feed).

**Invariant:** after ack, shard state is self-consistent; drainer runs with no special cases.

## SEEDER

- **At:** `seed_from_store` (called from subscribe path + `handle_seed_stores` during recovery)
- **Trigger:** first subscribe on non-empty stream, or recovery
- **Job:** bulk-load store → engine `ctx.ready` preserving store seqs

**MUST:**
1. Temp-remove `stores[id]` to avoid borrow conflict
2. `store.for_each(first..=last)` → `engine.enqueue_ready(stream, subject, subject_hash, seq)` per entry
3. Reinsert store
4. `seeded_streams.insert(id)`
5. `last_engine_seq[id] = info.last_seq`
6. `ctx.next_seq = max(ctx.next_seq, last_seq + 1)`

**MUST NOT:** call `engine.publish` (would reassign seqs + double-count `next_seq`); send Deliver frames; run concurrently with `publish_pending_to_engine` on same stream; run twice on same stream (check `seeded_streams` first).

**Invariant after return:** `seeded_streams.contains(id)` ∧ `last_engine_seq[id] == info.last_seq` ∧ `ctx.next_seq > info.last_seq` ∧ every store entry reachable from `ctx.ready` under its store seq.

---

# CROSS-ROLE RULES

1. **Single writer per primitive.** See ownership table. Gate close = drainer only. Bindings mutate = admin only. Store append = publisher/accumulator only. Publisher never writes engine.
2. **Gate is a doorbell, not a queue.** Ring when you create deliverable conditions. Only drainer decides to stop listening.
3. **No speculative engine work.** Iterating "in case a consumer shows up" = wrong role. That's the seeder, triggered by subscribe.
4. **Latency isolation.** Publisher ⊥ drainer load. Acker ⊥ publish rate. Admin ⊥ hot-path load (beyond queue slot). Drainer ⊥ publish rate (beyond gate wakeup).
5. **Hot cannot call cold.** Only allowed hot→cold handoff is `gate.release()` (signal, not call).
6. **Cold may touch hot-path infrastructure** (seeder writes `ctx.ready`, admin writes `bindings`) but must leave hot-path invariants intact on return.
7. **When in doubt, this file wins over `shard.rs`.** If a handler violates this file, the handler is wrong.
