---
description: Contract between arbitro-server and arbitro-engine — INVIOLABLE
---

# ENGINE CONTRACT

The server is a **caller** of `ArbitroEngine`: batch-first, sync, `&mut self`, zero-copy within shard.

## ENGINE API — ALLOWED CALLS

| Method | Output | Notes |
|---|---|---|
| `publish(&PublishBatch)` | `RepPublish` | Always `drain_fanout` after |
| `drain_fanout()` | `FanoutDrain` | RAII guard, drop after reading |
| `claim(&ClaimBatch, sub, bind)` | `&ScratchReply` | Borrow released on next call |
| `ack(&AckBatch)` | `&ScratchReply` | Borrow released on next call |
| `nack(&NackBatch)` | `&ScratchReply` | Borrow released on next call |
| `ensure_{stream, consumer}` | `Result<SlabKey>` | Idempotent |
| `open/drain_connection` | `SlabKey`/`Report` | Management |
| `tick(now, &mut expired)` | — | Scheduler tick |

## CALLING RULES — INVIOLABLE

1. **Always drain fanout after publish**: `engine.publish(&batch); engine.drain_fanout();`. Fanout from batch1 is lost if batch2 is called before drain.
2. **Copy ScratchReply results**: Copy results (e.g. `.to_vec()`) before the next engine call to avoid dangling references.
3. **Catalog setup first**: Ensure entities are created and connection bound before hot-path calls.
4. **No async calls**: Engine is not `Send`. Call only from the shard thread.
5. **Timestamp discipline**: Server provides timestamps. Engine never reads the clock.

## COMMAND LIFECYCLE
1. Parse wire frame -> Build owned command -> Create oneshot.
2. `send(cmd).await` -> Shard `blocking_recv()`.
3. Convert owned → borrowed -> Call engine method -> Copy result to reply -> `tx.send()`.
4. Transport `rx.await` -> Build response frame -> Send.

## ENGINE TYPES TRAVEL AS BYTES
Engine types (`#[repr(C)] + IntoBytes + FromBytes`) cast to/from `&[u8]`.

| Type | Size | Direction |
|---|---|---|
| `FanoutEntry` | 24B | engine → reply |
| `ClaimedEntry` | 16B | engine → reply |
| `AckEntry` | 8B | wire → engine |
| `RepPublish` | 16B | engine → inline Copy |

**Rule**: Never define owned mirror types. Use engine types directly. Owned types in `command.rs` only hold `Vec` or `Bytes` for channel crossing.

## ERROR HANDLING
- **Shard errors**: `SendError` / `RecvError` = shard crashed. Log and let operator decide. Never restart silently.
- **Engine errors**: Validation/catalog errors are not shard failures. Map to `ErrorCode` and send `RepError`.
