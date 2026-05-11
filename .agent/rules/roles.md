---
description: Shard worker role boundaries — ingress/egress flows, hot vs cold path, ownership fences — INVIOLABLE
---

# ROLES

Each shard multiplexes two orthogonal flows:

- **Ingress** — command-driven. Client request → validate → act → reply.
- **Egress** — data-driven. New data in store → drain → fan-out to push subscribers.

Physical bottlenecks frame the architecture. There is one network entry and one storage exit; shards parallelize CPU work between them:

```
  NET IN ──▶ [I/O pool] ──▶ N shards.INGRESS ──▶ 1 disk writer ──▶ DISK
                                                                    │
  NET OUT ◀── [I/O pool] ◀── N shards.EGRESS ◀── 1 disk reader ◀────┘
```

Within a shard, ingress and egress share the topology snapshot (egress reads, ingress writes) but never cross primitive ownership.

- **Hot path** — per-message, ns/µs budget.
- **Cold path** — per-session/config/lifetime, µs/ms budget.

## Primitive ownership

| Primitive | Read | Write |
|---|---|---|
| `Store` (per stream) | egress.drainer, ingress.seeder, fetcher | ingress.publisher, ingress.accumulator.flush |
| `ArbitroEngine` | egress.drainer | egress.drainer, ingress.{seeder, acker, admin} |
| `Gate` | worker loop | ingress.{publisher, accumulator, acker, admin} = open; **egress.drainer only** = close |
| `bindings` | egress.drainer, fetcher | ingress.admin only |
| `topology snapshot` (`ArcSwap`) | egress.drainer, fetcher | ingress.admin only |
| `seeded_streams`, `last_engine_seq` | egress.drainer | ingress.{seeder, admin} |
| `accum_streams` | ingress.accumulator | ingress.accumulator |
| `inflight` tables | egress.drainer, ingress.acker | egress.drainer, ingress.acker |

**Non-negotiable fences:**
- Ingress never delivers frames. Egress never replies to commands.
- Publisher never touches engine. Drainer never replies to publishes.
- Admin never delivers. Seeder runs at most once per stream.
- Disk I/O is never synchronous on the shard thread: ingress appends via MPSC to a dedicated disk writer; egress reads via the store's cached backend.

## Worker loop

```
loop {
    // INGRESS — command-driven
    1. drain rx → {publisher | accumulator.enqueue | acker | admin | pull_handler}
    2. flush_accumulator if deadline/threshold

    // EGRESS — data-driven
    3. if gate.is_open() → drainer

    4. rx empty && gate closed → park
}
```

Gate = doorbell. Opened by any ingress role that creates deliverable conditions. Closed only by egress.drainer.

---

# INGRESS

Command-driven. Every action starts from a client frame. Validates, mutates, replies. Never delivers, never reads for delivery.

## PUBLISHER [hot]

- **At:** `shard.rs :: handle_publish`
- **Trigger:** `Publish` cmd
- **Job:** accept → persist → ack

**MUST:**
1. `store.append_batch` (enqueued to disk writer via MPSC; shard never blocks on fsync)
2. Reply `RepOk(first_seq)` or `RepError`
3. `gate.release()` after ack

**MUST NOT:** touch engine, read store after write, build Deliver frames, know about consumers, couple latency to drainer, synchronously fsync.

**Invariant:** zero subscribers cost = `store.append + reply + gate.release`. Anything more is a bug.

## ACCUMULATOR [hot]

- **At:** `handle_publish_accumulate` (enqueue) + `flush_accumulator` (loop step 2)
- **Trigger:** `PublishAccumulate` cmd, or deadline/size/byte threshold
- **Job:** buffer small publishes → one batched store append

**MUST:** buffer with caller metadata (conn_id, env_seq, entry_count); reset deadline on enqueue; on flush run the publisher path atomically (`append_batch` → reply all callers with computed seqs → one `gate.release()`); on failure reply `StreamFull` to all buffered callers.

**MUST NOT:** ack before store append; hold callers past deadline; touch engine; use when caller needs per-msg sync ack (use `publish_sync`).

**Invariant:** all buffered callers succeed atomically or all fail. No partial state.

## ACKER [hot]

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

## PULL_HANDLER [hot]

- **At:** `handle_pull`
- **Trigger:** `Pull` cmd
- **Job:** on-demand fetch for a pull subscription → reply to requester

**MUST:**
1. Validate target sub is `SubscriptionKind::Pull`; reject if Push
2. Call `fetcher::fetch(store, cursor, sub.filter, limits)` (shared primitive — see below)
3. Reply with `(entries, new_cursor)` or empty batch
4. If `wait_ms > 0` and batch empty, register on stream Gate for long-poll; wake on new data or timeout

**MUST NOT:** touch `push_subs`; build drain-style fan-out; mutate engine state; couple to drainer loop; deliver to anyone other than the requesting client.

**Invariant:** PULL is strictly read-only on topology. Cursor advances in the reply; server-side cursor only moves on explicit ack.

## ADMIN / CONTROL [cold]

- **At:** `handle_{create,delete}_stream`, `handle_{create,delete}_consumer`, `handle_{subscribe,unsubscribe}`, `handle_{open,drain}_connection`, `handle_bind`, `handle_{pause,resume}_consumer`, `handle_list_*`, `handle_store_info`
- **Job:** management ops, keep shard state consistent

**MUST:** always reply oneshot (drop = shard-crashed to caller); keep `bindings`/`stores`/`seeded_streams`/`last_engine_seq` consistent with engine catalog/graph; publish a new topology snapshot on any structural change.

**Subscribe flow:** `ensure_stream` → `ensure_consumer` → `ensure_subscription` (respects `SubscriptionKind` — Push → `push_subs` slot, Pull → `pull_subs` slot) → seeder (if unseeded and at least one Push sub exists) → `bind` → push `ActiveBinding` → `gate.release()` only for Push subs (Pull never triggers drain).

**Delete stream:** `engine.remove_stream_full` → remove `stores[id]` (purge disk if `purge_disk`) → clear `seeded_streams[id]` and `last_engine_seq[id]` → retain-filter `bindings`.

**Delete consumer / unsubscribe:** engine drain → catalog remove → retain-filter `bindings` (correct slot by kind).

**MUST NOT:** do hot-path work; leave half-applied state on error; rely on drainer to fix inconsistencies; forget to clear `last_engine_seq` on delete (recreated stream would never feed); omit topology snapshot publish after structural change.

**Invariant:** after ack, shard state is self-consistent; drainer runs with no special cases.

## SEEDER [cold]

- **At:** `seed_from_store` (called from subscribe path + `handle_seed_stores` during recovery)
- **Trigger:** first Push subscribe on non-empty stream, or recovery
- **Job:** bulk-load store → engine `ctx.ready` preserving store seqs

**MUST:**
1. Temp-remove `stores[id]` to avoid borrow conflict
2. `store.for_each(first..=last)` → `engine.enqueue_ready(stream, subject, subject_hash, seq)` per entry
3. Reinsert store
4. `seeded_streams.insert(id)`
5. `last_engine_seq[id] = info.last_seq`
6. `ctx.next_seq = max(ctx.next_seq, last_seq + 1)`

**MUST NOT:** call `engine.publish` (would reassign seqs + double-count `next_seq`); send Deliver frames; run concurrently with `publish_pending_to_engine` on same stream; run twice on same stream (check `seeded_streams` first); run for streams with zero Push subs (Pull-only streams never feed the engine).

**Invariant after return:** `seeded_streams.contains(id)` ∧ `last_engine_seq[id] == info.last_seq` ∧ `ctx.next_seq > info.last_seq` ∧ every store entry reachable from `ctx.ready` under its store seq.

---

# EGRESS

Data-driven. Starts from Gate wakeup, ends at dispatched frames. Never parses commands, never replies to clients.

## DRAINER [hot]

- **At:** `handle_drain_deliver` (+ `publish_pending_to_engine`, `send_deliver_frame`)
- **Trigger:** worker loop step 3, only if `gate.is_open()`
- **Job:** store data + `push_subs` → Deliver frames

**MUST — in this exact order:**

1. **Guard 0 (first statement):**
   ```rust
   if self.bindings.is_empty() { self.gate.lock(); return; }
   ```
2. **Feed engine** only for streams with ≥1 Push binding, up to `store.info().last_seq`. First time → seeder path (`enqueue_ready`). Otherwise → incremental feed from `last_engine_seq+1`. Both preserve store seqs.
3. **Per Push binding:** loop `claim(64)` → `store.get(seq)` → `send_deliver_frame`, until claim returns < max_items or inflight full. Copy `ClaimedEntry` before next engine call.
4. **Close cycle:** delivered something → `gate.release()`; nothing → `gate.lock()`.

**MUST NOT:** reply to publishes; iterate `pull_subs` (they are on-demand and structurally invisible here); do engine work for streams with no Push bindings (per-stream check inside feed, not just Guard 0); block network I/O; mutate `bindings`/store/catalog.

**Invariant:** no speculative work. Past `bindings.is_empty()`, every stream touched has ≥1 Push destinatario. Pull subscriptions are strictly invisible to the drainer.

---

# SHARED PRIMITIVES

## FETCHER

Pure read function. No thread, no actor, no state. Called by `egress.drainer` and `ingress.pull_handler`.

```rust
fn fetch(
    store:     &Store,
    cursor:    Seq,
    filter:    &SubjectFilter,
    max_msgs:  u32,
    max_bytes: u64,
) -> (Vec<Entry>, Seq /* new_cursor */)
```

**MUST:** advance cursor regardless of match (skip non-matching entries); stop at `max_msgs`, `max_bytes`, or end-of-store; apply the same `match_table` logic as drain's per-entry filter check.

**MUST NOT:** mutate store, engine, topology, or any counter; hold any lock beyond the read; assume caller's cursor model.

**Callers:**
- `egress.drainer` — stream-wide cursor (`last_engine_seq[stream]`), iterates across all `push_subs` of the stream
- `ingress.pull_handler` — per-request cursor from `Pull` cmd, single `pull_sub`

---

# CROSS-ROLE RULES

1. **Single writer per primitive.** See ownership table. Gate close = egress.drainer only. Bindings mutate = ingress.admin only. Store append = ingress.{publisher, accumulator} only (via disk writer, not inline fsync). Topology snapshot publish = ingress.admin only.
2. **Gate is a doorbell, not a queue.** Any ingress role that creates deliverable conditions rings it. Only egress.drainer decides to stop listening.
3. **No speculative engine work.** Iterating "in case a consumer shows up" = wrong role. That's the seeder, triggered by subscribe.
4. **Latency isolation.** `ingress.publisher ⊥ egress.drainer` load. `ingress.acker ⊥` publish rate. `ingress.admin ⊥` hot-path load (beyond queue slot). `egress.drainer ⊥` publish rate (beyond gate wakeup). `ingress.pull_handler ⊥ egress.drainer` (they share fetcher but operate on disjoint sub slots).
5. **Hot cannot call cold.** Only allowed hot→cold handoff is `gate.release()` (signal, not call).
6. **Cold may touch hot-path infrastructure** (seeder writes `ctx.ready`, admin writes `bindings` and topology snapshot) but must leave hot-path invariants intact on return.
7. **Push and Pull are structurally segregated.** `SubscriptionKind` determines which slot (`push_subs` / `pull_subs`) the sub lives in. Drainer sees only `push_subs`; pull_handler sees only `pull_subs`. No runtime `if kind == Push` in the hot loop.
8. **Disk I/O never blocks the shard.** Publisher and accumulator enqueue to a dedicated disk writer via MPSC; the shard never calls `fsync` inline. Drainer and fetcher read through the store's cached handle; blocking disk reads must be wrapped by the store backend, not the shard.
9. **When in doubt, this file wins over `shard.rs`.** If a handler violates this file, the handler is wrong.
