---
description: Contract between arbitro-server and arbitro-engine — what to call, when, and how — INVIOLABLE
---

# ENGINE CONTRACT

The server is a **caller** of `ArbitroEngine`. It must respect the engine's API: batch-first, sync, `&mut self`, zero-copy within shard.

## ENGINE API — ALLOWED CALLS

### Hot path (per-message, called in shard loop)

| Method | Input | Output | Notes |
|---|---|---|---|
| `engine.publish(&PublishBatch)` | borrowed entries | `RepPublish` (16B Copy) | Always drain_fanout after |
| `engine.drain_fanout()` | — | `FanoutDrain<'_>` | RAII guard, drop after reading |
| `engine.claim(&ClaimBatch)` | batch params | `&ScratchReply<ClaimedEntry>` | Borrow released on next call |
| `engine.ack(&AckBatch)` | borrowed entries | `&ScratchReply<AckResult>` | Borrow released on next call |
| `engine.nack(&NackBatch)` | borrowed entries | `&ScratchReply<NackResult>` | Borrow released on next call |
| `engine.bind(&BindBatch)` | borrowed entries | `RepOk<SlabKey>` | Management frequency |

### Management path (per-session/config)

| Method | Input | Output | Notes |
|---|---|---|---|
| `engine.ensure_stream(StreamConfig)` | owned config | `Result<SlabKey>` | Idempotent |
| `engine.ensure_consumer(ConsumerConfig)` | owned config | `Result<SlabKey>` | Idempotent |
| `engine.ensure_subscription(SubscriptionConfig)` | owned config | `Result<SlabKey>` | Idempotent |
| `engine.open_connection(&OpenConnectionReq)` | borrowed req | `SlabKey` | Once per connection |
| `engine.drain_connection(&DrainConnectionReq)` | borrowed req | `DrainReport` | On disconnect |
| `engine.drain_subscription(id, mode)` | — | `DrainReport` | On unsubscribe |
| `engine.drain_consumer(id, mode)` | — | `DrainReport` | Admin |
| `engine.drain_queue(id, mode)` | — | `DrainReport` | Admin |
| `engine.drain_node(id, mode, now)` | — | `DrainReport` | Admin |
| `engine.pause_consumer(id)` | — | `bool` | Admin |
| `engine.resume_consumer(id)` | — | `bool` | Admin |
| `engine.set_subject_limit(stream, pattern, limit)` | — | `Result<()>` | Admin |
| `engine.tick(now_ms, &mut expired)` | — | fills vec | Scheduler tick |
| `engine.drain_events()` | — | `Vec<EngineEvent>` | Event bus drain |

## CALLING RULES — INVIOLABLE

### 1. Always drain fanout after publish
```rust
// Correct
let result = engine.publish(&batch);
let fanout = engine.drain_fanout();
// read fanout entries, copy what you need
drop(fanout);

// WRONG — fanout from batch1 is lost
engine.publish(&batch1);
engine.publish(&batch2);
```

### 2. Copy ScratchReply results before next engine call
```rust
// Correct — copy out before next call
let claimed = engine.claim(&batch);
let entries: Vec<_> = claimed.entries().to_vec();
// now safe to call engine again

// WRONG — dangling reference
let claimed = engine.claim(&batch1);
let acked = engine.ack(&batch2); // claimed is now invalid
```

### 3. Catalog setup before hot path
```rust
// Correct order
engine.ensure_stream(config)?;
engine.ensure_consumer(config)?;
engine.ensure_subscription(config)?;
engine.open_connection(&req);
engine.bind(&batch);
// NOW ready for publish/claim/ack
```

### 4. Never call engine from async context
```rust
// Correct — shard thread
fn handle_publish(&mut self, cmd: PublishCmd) {
    self.engine.publish(&batch); // sync, on shard thread
}

// WRONG — async task
tokio::spawn(async move {
    engine.publish(&batch); // engine is not Send
});
```

### 5. Timestamp discipline
The server provides timestamps. The engine never reads the clock.
```rust
let now = Timestamp::new(timestamp_nanos());
// pass `now` to every batch
```

## COMMAND LIFECYCLE

```
Transport task                     Channel                    Shard thread
─────────────                     ───────                    ────────────
1. Parse wire frame (proto)  ──→
2. Build owned command       ──→
3. Create oneshot reply      ──→
4. send(cmd).await           ──→   [queued]            ──→   5. blocking_recv()
                                                            6. Convert owned → borrowed
                                                            7. Call engine method
                                                            8. Copy result to reply
                                                            9. reply_tx.send(result)
10. rx.await ← ────────────────────────────────────────────
11. Build wire response frame
12. Send to connection
```

**Key invariant**: steps 6-9 happen on the shard thread with `&mut engine`. No other thread touches the engine during this time.

## ENGINE TYPES TRAVEL AS BYTES

Engine types with `IntoBytes + FromBytes + #[repr(C)]` can be cast to/from `&[u8]`:

| Type | Size | Hot Path Direction |
|---|---|---|
| `FanoutEntry` | 24B | engine → `as_bytes()` → reply |
| `ClaimedEntry` | 16B | engine → `as_bytes()` → reply |
| `AckEntry` | 8B | wire → `ref_from_bytes` → engine |
| `RepPublish` | 16B | engine → inline Copy |
| `AckResult` | 1B | engine → inline |
| `NackResult` | 1B | engine → inline |

**Never define owned mirror types.** Use engine types directly. The only owned types are in `command.rs` for crossing the channel boundary (Vec of engine types, not custom structs).

## ERROR HANDLING

### Shard errors
- `SendError::ShardDown`: shard thread exited, channel closed.
- Propagate to transport as connection error.
- **Never restart a shard silently.** Log and let operator decide.

### Engine errors
- `EngineResult::Err(EngineError)`: catalog/validation error.
- Not a shard failure — shard is healthy, request was invalid.
- Map to wire `ErrorCode` and send `RepError` frame.

### Reply channel errors
- `oneshot::Receiver` `RecvError` = sender dropped without sending.
- Means shard crashed/panicked. Treat as `ShardDown`.
