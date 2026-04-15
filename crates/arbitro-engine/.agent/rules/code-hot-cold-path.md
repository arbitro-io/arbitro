---
description: Hot path vs management path — definitions, budgets, and what is allowed on each. INVIOLABLE.
---

# HOT PATH vs MANAGEMENT PATH

---

## DEFINITIONS

**Hot path** — any code executed per-message during steady-state operation:
- `publish` (receive frame → dedup → match → store → enqueue ready)
- `claim` / `deliver` (pop ready → build Pending → dec credits → send)
- `ack` / `nack` (release_pending → dec inflight → release credits → remove edges)
- Subject matching (precomputed match table lookup)
- Edge index lookups (get, remove)
- Slab operations (get, remove)

**Management path** — any code executed per-session or per-configuration change:
- `connect` / `disconnect`
- `ensure_stream` / `ensure_consumer` / `ensure_subscription`
- Plugin init / recover / shutdown
- Match table rebuild (at subscription time)
- Admin operations (pause, limits, drain node)

---

## HOT PATH BUDGET

| Resource | Budget per entry | Violation action |
|---|---|---|
| Heap allocations | **0** | Pre-allocate, use scratch, use `Bytes::clone()` |
| Lock acquisitions | **0** | Single-threaded engine core, no locks on processing |
| Virtual dispatch | **0** on inner loop | Use enum or monomorphize |
| `from_utf8` scans | **0** | Keep subject as `&[u8]` |
| Blocking syscalls | **0** | Non-blocking transport only |
| Header heap alloc | **0** | Stack headers, never `Bytes::copy_from_slice` |
| HashMap lookups | **0** on inner loop | Slab index (array offset), TypeId hash for edge/plugin |
| Pointer chasing | **0** | Inline IDs in entities, no parent traversal |

---

## PERFORMANCE TARGETS (measured baselines — never regress)

| Operation | Target | Measured | Method |
|---|---|---|---|
| Publish per entry | **≤ 300 ns** | ~150-250 ns | dedup + match + store + enqueue |
| Ack per entry | **≤ 120 ns** | ~100-120 ns | slab remove + 3 counters + credits + 7 edges |
| Claim per entry | **≤ 200 ns** | ~150-200 ns | pop ready + build Pending + send |
| Wire encode (any size) | **< 1 ns** | ~400 ps | `as_bytes()` pointer cast |
| Wire decode (any size) | **< 1 ns** | ~700 ps | `wire::decode_slice()` alignment check |
| Slab get/remove | **≤ 5 ns** | — | Array index + generation check |
| Edge get | **≤ 5 ns** | — | TypeId hash + HashMap get |
| Edge remove | **≤ 5 ns** | — | TypeId hash + HashMap remove |
| Match table lookup | **≤ 20 ns** | — | Hash & mask + iterate 1-3 consumers |
| Idempotency check | **≤ 15 ns** | — | Hash + linear probe (max 16) |
| Plugin access | **≤ 5 ns** | — | TypeId hash |
| Inflight dec/inc | **≤ 5 ns** | — | Counter index + inc/dec |
| release_pending | **≤ 120 ns** | ~100 ns | All 7 steps combined |

---

## RULES

### 1. Hot path functions MUST NEVER call management path functions

If a function is unsure which path it is on — it is hot path. Apply hot path rules.

```rust
// ❌ on_publish calling ensure_stream
fn on_publish_batch(&mut self, ctx: &mut EngineContext, batch: &PublishBatch) {
    self.ensure_stream(batch.stream); // management path! might allocate
}

// ✅ Streams exist before publish — catalog manages lifecycle separately
fn on_publish_batch(&mut self, ctx: &mut EngineContext, batch: &PublishBatch) {
    let match_table = ctx.catalog.match_table(batch.stream); // O(1) lookup
}
```

### 2. Ergonomic types are allowed on management path only

```rust
// ✅ Management path — String, HashMap, allocation OK
fn ensure_consumer(&mut self, config: ConsumerConfig) -> Result<(), EngineError> {
    let name = config.name.to_string(); // alloc OK here
}

// ❌ These types on hot path
fn on_publish(&mut self, subject: String, data: Vec<u8>) // forbidden
```

### 3. Engine core is single-threaded — no locks needed

The `EngineContext` is owned by a single thread. All runtime operations (publish, ack, claim, drain) execute on this thread. No `Mutex`, no `RwLock`, no atomics for internal state. Only atomic counters for cross-thread metrics.

```rust
// ✅ Direct mutable access — no lock, no contention
fn on_ack(&mut self, ctx: &mut EngineContext, pending_id: PendingId) {
    let pending = ctx.graph.remove::<PendingNode>(pending_id); // direct &mut
    ctx.inflight.dec(Subject, pending.subject_hash);            // direct &mut
}

// ❌ Locking inside the engine core
fn on_ack(&mut self, ctx: &mut EngineContext, pending_id: PendingId) {
    let guard = ctx.graph.lock().unwrap(); // banned — single-threaded, no lock needed
}
```

### 4. Batch-as-standard — single = batch(count=1)

One code path for all operations. No branching on single vs batch.

```rust
// ✅ Always batch — single message is batch of 1
pub fn on_ack_batch(&mut self, ctx: &mut EngineContext, batch: &AckBatch) {
    for entry in batch.entries {
        self.release_pending(ctx, entry.pending);
    }
    ctx.signal_drain_once(); // signal ONCE for entire batch
}

// ❌ Separate single and batch paths
pub fn on_ack(&mut self, ctx: &mut EngineContext, pending_id: PendingId) { ... }
pub fn on_batch_ack(&mut self, ctx: &mut EngineContext, batch: &[PendingId]) { ... }
```

### 5. Precomputed over runtime evaluation

Subject→consumer resolution is computed at subscription time, not at publish/deliver time.

```rust
// ✅ Precomputed at ensure_subscription — O(1) at publish time
let consumers = ctx.match_table.lookup(subject_hash); // array index + mask

// ❌ Evaluating filters at publish time — O(N) per subject × consumers
for consumer in &all_consumers {
    if subject_matches(&consumer.filter, subject) { ... } // O(N) scan
}
```

---

## ENGINE PURITY CONTRACT

ArbitroDB is a **pure synchronous library** — `&mut self` engine. It is NOT
a server. The engine:

- **Never blocks** — pure computation, ≤300ns per entry
- **Never allocates** on hot path — scratch buffers pre-allocated
- **Never does I/O** — no sockets, no files, no clocks
- **Never spawns threads** — single `&mut self`
- **Never uses async** — synchronous return by reference
- **Never owns the event loop** — caller decides when to call

```rust
// The protocol layer (NOT the engine) drives everything
fn process_frame(engine: &mut ArbitroEngine, frame: &[u8]) {
    match frame[0] {
        OP_PUBLISH => {
            let reply = engine.publish(&batch);      // sync, ~200ns
            send_reply(reply.as_bytes());            // ~400ps encode
        }
        OP_ACK => {
            let result = engine.ack(&ack_batch);
            send_reply(result.as_bytes());
        }
    }
}
```
