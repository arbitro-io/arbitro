---
description: Concurrency model, channel patterns, shutdown protocol, and thread safety — INVIOLABLE
---

# CONCURRENCY RULES

The server (`arbitro-server`) is the **only place** where async, threads, and channels exist.

## SHARD OWNERSHIP MODEL — INVIOLABLE

Each `ArbitroEngine` is **exclusively owned** by one `ShardWorker` thread. No sharing, no locks.

```
Thread "shard-0" → owns Engine 0 + Store 0 → &mut self calls only
Thread "shard-1" → owns Engine 1 + Store 1 → &mut self calls only
Thread "shard-N" → owns Engine N + Store N → &mut self calls only
```

### Rules

1. **One engine per thread.** Never accessed from two threads.
2. **No Arc\<Engine\>.** If you need Arc, the architecture is broken.
3. **No Mutex\<Engine\>.** If you need Mutex, the architecture is broken.
4. **No RwLock\<Engine\>.** If you need RwLock, the architecture is broken.
5. **No engine references escape the shard thread.** All results copied into reply types.

## CHANNEL PATTERNS

### Shard channel: `mpsc::channel<ShardCommand>`
- **Sender** (async side): held by `ShardHandle`, cloneable, shared across tokio tasks
- **Receiver** (sync side): held by `ShardWorker`, `try_recv()` in dual-source loop + `thread::park()`
- **Wake**: `ShardHandle.send()` calls `shard_thread.unpark()` after `tx.send()`
- **Capacity**: configurable (default 4096). Backpressure if full — `.await` blocks sender
- **Direction**: always transport → shard. Never shard → shard.

### Reply channel: `oneshot::channel`
- Created per-command by the caller
- `Sender` travels with the command into the shard thread
- `Receiver` is `.await`ed by the caller (tokio task)
- **Never store a oneshot::Sender.** Consumed within the handler.

### Correct
```rust
let (tx, rx) = oneshot::channel();
shard.send(ShardCommand::Ack(AckCmd { reply: tx, .. })).await?;
let result = rx.await?;
```

### Banned
```rust
let shared = Arc::new(Mutex::new(HashMap::new())); // NO shared state
let engine_ref: &Engine = ...;
tokio::spawn(async move { engine_ref.publish(...) }); // NO engine in async
```

## SHARD ROUTING

Deterministic: `stream_id.raw() % shard_count`.

### Rules

1. **All operations for a stream → same shard.** Publish, claim, ack for stream X all route to shard `X % N`.
2. **Connection operations may span shards.** `open_connection` and `drain_connection` sent to active shards only.
3. **Router never buffers commands.** Forwards immediately.
4. **Router never inspects payloads.** Only reads `stream_id` for routing.

## SHUTDOWN PROTOCOL

### Sequence
1. `Server::shutdown()` sends `ShardCommand::Shutdown` to each shard
2. Each `ShardWorker` breaks out of its loop
3. Channel drops, senders get `SendError`
4. Shard threads exit naturally

### Rules

1. **Never `thread.join()` from async context.** Use `spawn_blocking` if needed.
2. **Never force-kill shard threads.** Let them drain naturally.
3. **Always send Shutdown before dropping Server.** Prevents unprocessed commands.
4. **Shutdown is idempotent.** Second Shutdown is harmless.

## BACKPRESSURE

When shard channel is full (4096 pending), sender `.await` blocks. This is **intentional**.

### Rules

1. **Never `try_send` to silently drop commands.** Use `.await` send.
2. **Never increase capacity to "fix" slowness.** Fix the bottleneck shard.
3. **Monitor channel depth.** Near-capacity = shard overloaded.

## THREAD NAMING — MANDATORY

All shard threads are named: `shard-0`, `shard-1`, etc.

```rust
std::thread::Builder::new()
    .name(format!("shard-{shard_id}"))
    .spawn(move || worker.run())
```

Never use unnamed threads (`std::thread::spawn`).

## DRAIN DELIVERY — ON SHARD THREAD VIA GATE

Delivery runs **directly on the shard thread** — no async drain task, no middleman.

The shard thread serves **two wakeup sources** in a unified loop:
1. **mpsc commands** — publish, ack, nack, subscribe, etc. (`try_recv`)
2. **Gate signal** — "drain work available, deliver now" (`gate.is_open()`)

The shard parks when **both** are idle. Either source wakes it:
- `ShardHandle.send()` sends command + calls `shard_thread.unpark()`
- `gate.release()` sets locked=false + calls `shard_thread.unpark()`

```
Shard loop:  try_recv commands → gate.is_open? → handle_drain_deliver → park
Gate opens:  publish (new messages), ack (freed inflight), nack (requeued), subscribe/bind
Gate closes: handle_drain_deliver found nothing to deliver
```

### Gate (`crate::gate::Gate`)

```rust
#[repr(align(64))]
pub struct Gate {
    locked: AtomicBool,     // true = no work pending
    parked: AtomicBool,     // true = shard thread is parked
    worker: UnsafeCell<Option<std::thread::Thread>>,
}
```

- Lives inside `ShardWorker` — one per shard thread. **Not Clone, not Arc.**
- External callers (ShardHandle) wake the shard via `shard_thread.unpark()`, not gate.
- Gate is internal to the shard thread's own delivery scheduling.

### Rules

1. **No async drain task.** Delivery runs on the shard thread via Gate.
2. **`try_recv` is not spinning.** Shard parks when both mpsc and gate are idle.
3. **Spurious unpark is safe.** Extra loop iteration (~5ns), no harm.
4. **Gate.release() from shard handlers only.** Called after publish/ack/nack/subscribe/bind.
