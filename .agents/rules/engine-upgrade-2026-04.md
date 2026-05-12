---
description: HISTORICAL — Superseded by the consumer-owned-counters refactor (2026-05). Consult engine-contract.md for the current API.
---

# ENGINE UPGRADE — 2026-04 (SUPERSEDED)

> **HISTORICAL**: This document records the 2026-04 claim signature change. The introspection methods mentioned here have since been deleted or moved. **Use engine-contract.md as the primary reference.**

## 1. CLAIM SIGNATURE — INVIOLABLE
New signature requires resolved IDs to avoid expensive HashMap lookups in the hot path.

```rust
pub fn claim(&mut self, batch: &ClaimBatch, sub_id: SubscriptionId, bind_id: BindingId) -> &ScratchReply<ClaimedEntry>;
```

**Fix in server**: `ActiveBinding` must cache `subscription_id` and `binding_id`. Pass them directly from the cached entry in the drainer.

**Stale-hint detection**: Server **MUST** invalidate / drop `ActiveBinding` on:
- `unsubscribe`, `unbind`, `delete_consumer`, or `drain_connection`.
Failure to invalidate will cause `debug_assert` panics in debug builds and silent corruption in release.

## 2. INFLIGHT INTROSPECTION
Use the following root-exported methods for pre-filtering in the drain loop (~2-10 ns):
- `engine.consumer_has_capacity(id, max)`
- `engine.subject_has_capacity(hash, max)`

**Rule**: Cache `max_inflight` on `ActiveBinding` at subscribe/bind time. Never call configuration lookups inside the hot loop.

## 3. METRICS
- Snapshot once at startup: `let metrics = engine.metrics();`.
- Use a single aggregator task for the entire process.
- **Never** read counters inside the shard loop.

## 4. TYPE WRAPPERS — BANNED
**Rule**: Never define owned mirror types. Use engine types directly. 
- Replace `PublishEntryOwned` with `Vec<u8>` for subject + `Bytes` for payload.
- Channel engine batch types through `ShardCommand` directly.
- Aim for zero-copy from accumulator → engine.
