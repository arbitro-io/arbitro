---
description: HISTORICAL — superseded by the consumer-owned-counters refactor (2026-05). Reference values for old engine introspection methods that have since been deleted. Do not act on this file's advice; consult engine-contract.md and ARCHITECTURE.md instead.
---

# ENGINE UPGRADE — 2026-04 (SUPERSEDED)

> **This document is historical.** Many of the engine introspection
> methods listed below (`subject_has_room`, `subject_inflight`,
> `queue_inflight`, `consumer_capacity_remaining`, `is_paused`,
> `subject_tracking_enabled`, `metrics`, `ctx_mut`, `execute_batch`,
> `match_table`, `total_ack_pending`) were deleted in the
> `refactor/consumer-owned-counters` PR after a usage audit found zero
> external callers. The current engine API is documented in
> `engine-contract.md`. The "use SharedCounters from the drain"
> direction this doc points at remains correct in spirit; the specific
> method names do not. Subject inflight is no longer in
> `SharedCounters` — it is drain-owned in
> `arbitro-server::shard::consumer_subjects::ConsumerSubjects`. See
> `ARCHITECTURE.md` §7 for the current shape.

This is a **delta**, not a replacement. `engine-contract.md` is still the
contract. Everything below is what the server must add or fix to consume the
engine optimally after the 2026-04 changes:

- `854ab03` — claim hot loop consolidated to a single shared slab borrow
- `dbe768a` — inflight introspection + metrics re-exported at crate root

The engine is now sustaining **89-99 ns / publish-claim-ack** in benches. The
server is the only piece left that can blow that budget. This document lists,
in priority order, the things the server must change to stop being the
bottleneck.

---

## 1. CLAIM SIGNATURE — BREAKING

The old `engine.claim(&batch)` is gone. New signature requires the caller to
pass already-resolved IDs:

```rust
pub fn claim(
    &mut self,
    batch: &ClaimBatch,
    subscription_id: SubscriptionId,
    binding_id: BindingId,
) -> &ScratchReply<ClaimedEntry>;
```

**Why:** the engine used to do a `resolve_subscription` + `resolve_binding`
per claim batch. Both are HashMap lookups (`~25 ns` each). The drainer
already has these IDs cached on `ActiveBinding` since subscribe time.
Forcing the caller to pass them removes ~50 ns per batch.

### Fix in server

`crates/arbitro-server/src/shard/worker.rs:34-43` already stores both fields
on `ActiveBinding` (`subscription_id`, `binding_id`). Just pass them through:

```rust
// shard/roles/drainer.rs — wherever the claim happens
let reply = self.engine.claim(
    &batch,
    binding.subscription_id,   // already cached
    binding.binding_id,        // already cached
);
```

### Stale-hint detection

In debug builds the engine `debug_assert_eq!`s the hints against a fresh
resolve. **The server must invalidate / recompute the `ActiveBinding`
entry on:**

- `handle_unsubscribe` — drop the binding from `ShardWorker.bindings`
- `handle_unbind` (if exposed)
- `handle_delete_consumer` — drop all bindings for that consumer
- `handle_drain_connection` — drop all bindings for that connection

If the server forgets, `cargo test` (debug) will panic with
`stale subscription hint` / `stale binding hint`. In release the wrong claim
batch silently targets the wrong subscription. **Treat this as a correctness
invariant, not an optimization.**

---

## 2. INFLIGHT INTROSPECTION — USE INSTEAD OF REWALKING

The drainer used to ask the engine "is this consumer at capacity?" by
issuing a claim and reading the `RepClaim` status. That wastes a full claim
cycle on a guaranteed empty result.

The engine now exposes 9 introspection methods at the crate root. The hot
ones are `~2-10 ns` and safe to call inside the drain loop:

```rust
// Tier 1 — hot path safe (2-10 ns each)
engine.consumer_inflight(consumer_id) -> u32
engine.queue_inflight(queue_id) -> u32
engine.subject_inflight(subject_hash) -> u32
engine.consumer_has_capacity(consumer_id, max_inflight) -> bool
engine.consumer_capacity_remaining(consumer_id, max_inflight) -> u32
engine.subject_has_capacity(subject_hash, max_inflight) -> bool

// Tier 2 — cold path, cache the result on ActiveBinding
engine.consumer_max_inflight(consumer_id) -> Option<u32>
engine.consumer_paused(consumer_id) -> bool
engine.subject_max_inflight(stream_id, subject_hash) -> Option<u32>
engine.subject_tracking_enabled() -> bool
```

### Required fix in `ActiveBinding`

Cache `max_inflight` at subscribe time. The drainer must **never** call the
Tier 2 methods inside the loop. Add to
`crates/arbitro-server/src/shard/worker.rs:34`:

```rust
pub(super) struct ActiveBinding {
    // ... existing ...
    pub(super) subscription_id: SubscriptionId,
    pub(super) binding_id: BindingId,

    // NEW — cached at handle_subscribe / handle_bind time
    pub(super) max_inflight: u32,
}
```

Then the drainer pre-filters before building a claim batch:

```rust
for binding in &self.bindings {
    if !self.engine.consumer_has_capacity(binding.consumer_id, binding.max_inflight) {
        continue; // skip without touching the engine claim path
    }
    // … build batch …
}
```

This skips the entire claim hot path for full consumers. Saves ~150 ns per
saturated consumer per cycle.

---

## 3. METRICS — DRAIN FROM A SECOND THREAD

The engine exposes `EngineMetrics` (cache-aligned, `Send + Sync`, all
counters Relaxed). The server should:

1. Snapshot once at startup: `let metrics = engine.metrics();` — keeps a
   `&'static`-like reference for the lifetime of the shard thread.
2. Spawn **one** metrics aggregator task (not per shard) that snapshots
   every shard's `metrics().snapshot()` on a 1-second tick and emits to
   Prometheus / log / wherever.
3. **Never** read counters inside the shard loop. They're for external
   consumption only.

```rust
// In ShardWorker construction
let metrics_handle: &EngineMetrics = engine.metrics();
// Hand metrics_handle to the aggregator via a Vec<&'static EngineMetrics>
// (transmuted lifetime is sound — engine lives as long as the shard thread)
```

Counters available: see `arbitro_engine_v2::EngineMetrics` (22 fields,
publish/claim/ack/nack/seed/drain).

---

## 4. STOP CREATING WRAPPER TYPES — USE ENGINE TYPES DIRECTLY

`engine-contract.md` §"ENGINE TYPES TRAVEL AS BYTES" is the rule:

> **Never define owned mirror types.** Use engine types directly. The only
> owned types are in `command.rs` for crossing the channel boundary
> (Vec of engine types, not custom structs).

The server is currently violating this. Inventory of wrappers to remove:

| File | Wrapper | Replace with |
|---|---|---|
| `shard/command.rs:76` | `PublishEntryOwned` | `Vec<u8>` for subject + `Bytes` for payload, or `engine::PublishEntryOwned` if it exists; else build the engine batch directly inside the transport task and send the **batch** through the channel |
| `shard/worker.rs:69` | `AccumCaller` | Channel a `(ConnectionId, u32 env_seq, u32 entry_count)` triple via `arrayvec::ArrayVec` instead of a struct |
| `shard/worker.rs:76` | `StreamAccum` | Hold `Vec<engine::PublishEntry<'static>>` directly using `Bytes` slices; the accumulator is the only place that needs owned storage, and even there `Bytes::clone()` is refcount only |
| Anywhere `*Cmd` exists with fields that mirror an engine batch | the `*Cmd` itself | Send the engine batch type through the channel directly. The channel is `mpsc::Sender<ShardCommand>` — `ShardCommand` can wrap engine types as long as they own their backing storage (which `Bytes` does for free). |

### The principle

Engine types like `AckEntry` (8B), `ClaimedEntry` (16B), `FanoutEntry` (24B),
`PublishEntry`, `NackEntry` are `#[repr(C)] + IntoBytes + FromBytes`. They
travel as bytes already. **The server should never hand-roll a parallel
struct hierarchy.** Every wrapper adds:

- An allocation per request (for the wrapper Vec)
- A copy per entry (wrapper → engine type at dispatch time)
- A type-conversion function nobody enjoys maintaining

The **only** legitimate owned type at the channel boundary is one that
holds `Bytes` (subject, payload) + a `SmallVec` of engine entries that
borrow into the same `Bytes`. Anything else is pre-engine-2026 cruft.

### Concrete refactor target

`shard/command.rs` should shrink from ~25 owned `*Cmd` structs to roughly:

```rust
pub enum ShardCommand {
    // Hot path — use engine types directly
    Publish(engine::PublishBatchOwned),     // engine ships this; Bytes-backed
    Ack(engine::AckBatchOwned),
    Nack(engine::NackBatchOwned),
    Claim {
        batch: engine::ClaimBatch,           // already plain Copy
        subscription_id: SubscriptionId,
        binding_id: BindingId,
        reply: oneshot::Sender<ClaimReply>,
    },

    // Cold path — config crosses as engine config types
    EnsureStream(engine::StreamConfig, oneshot::Sender<...>),
    EnsureConsumer(engine::ConsumerConfig, oneshot::Sender<...>),
    // ...

    Shutdown,
}
```

If the engine doesn't yet ship `PublishBatchOwned` / `AckBatchOwned`, that's
the **one** new type pair to introduce, and it should live in the engine
crate (`arbitro-engine/src/batch/owned.rs`) so it's reusable and tested
under engine bench.

---

## 5. ACCUMULATOR — REUSE THE ENGINE'S PUBLISH BATCH BUFFER

`shard/worker.rs:76` (`StreamAccum`) keeps a `Vec<PublishEntryOwned>` and
re-builds an engine `PublishBatch` at flush time. That rebuild is pure
overhead.

Alternative: keep `Vec<engine::PublishEntry<'static>>` directly, where the
`&[u8]` slices are backed by a per-stream `BytesMut` that grows monotonically
and is cleared on flush. The flush call is then:

```rust
let result = self.engine.publish(&PublishBatch {
    stream_id,
    entries: &accum.entries,
    timestamp: now,
});
```

Zero copy from accumulator → engine. The accumulator is the hottest
non-claim path in the server; this is worth doing.

---

## 6. WHAT NOT TO TOUCH

These are already correct — leave them alone:

- `ShardRouter::shard_for(stream_id)` — sharding by `stream_id.raw() %
  count` matches the engine's expectation that each `StreamId` lives in
  exactly one engine instance. Do **not** introduce cross-shard routing.
- `Gate` primitive (spin 512× + park) — already optimal for the
  command/drain dual-source loop.
- The role split (publisher/accumulator/acker/drainer/seeder/admin) — the
  engine has no opinion on internal organization, this is purely a server
  concern and the current split is fine.
- Per-shard `tokio::sync::mpsc` for command ingress — the engine doesn't
  care about the channel implementation, only that the call sites are sync
  and on a single thread per shard.

---

## 7. VERIFICATION CHECKLIST

After applying the above:

- [ ] `cargo build --release` clean, no warnings
- [ ] `cargo test --release` passes (engine debug-asserts only fire in
      debug builds, but release tests still verify correctness paths)
- [ ] `cargo test` (debug) passes — this is where stale-hint asserts fire;
      a green debug run is the only way to know `ActiveBinding`
      invalidation is correct
- [ ] `rg 'PublishEntryOwned\|AccumCaller\|StreamAccum' crates/arbitro-server/src/`
      — every match should be either deleted or replaced with a Bytes-backed
      engine type
- [ ] `rg 'engine\.claim\(&[a-z_]*\)' crates/arbitro-server/src/` — should
      be **zero** matches; every `claim` call must pass three arguments
- [ ] Throughput bench from `arbitro-engine` still reports ≤ 100 ns/msg on
      the same hardware — the server changes must not regress engine numbers
- [ ] An end-to-end TCP→TCP roundtrip on a single stream matches within
      1.5× of the in-process engine bench. If it's worse, the server is
      adding overhead beyond the channel + parse/encode budget.

---

## 8. THE LONG-TERM DIRECTION

Once 1-5 are done, the server is a thin shell:

```
TCP frame  →  parse  →  ShardCommand (engine types)  →  channel  →
  shard thread  →  engine.{publish,claim,ack}  →  drain_events  →
    channel  →  encode  →  TCP frame
```

No wrapper types, no resolve-on-every-claim, no per-loop catalog lookups,
no metrics inside the hot path. The only owned allocations per request
are the `Bytes` from the parser and the channel slot. Everything else
borrows or is `Copy`.

**That is the shape that lets the engine's 90-100 ns/msg show up at the
TCP boundary.** Anything else is the server bleeding nanoseconds the
engine already saved.
