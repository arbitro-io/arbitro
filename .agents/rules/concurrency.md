---
description: Concurrency model, channel patterns, and thread safety — INVIOLABLE
---

# CONCURRENCY RULES

The server (`arbitro-server`) is the **only place** where async, threads, and channels exist.

## SHARD OWNERSHIP MODEL — INVIOLABLE

Each `ArbitroEngine` is **exclusively owned** by one `ShardWorker` thread. No sharing, no locks.

1. **One engine per thread.** No `Arc<Engine>`, `Mutex<Engine>`, or `RwLock<Engine>`.
2. **No engine references escape the shard thread.** All results copied into reply types.

## CHANNEL PATTERNS

### Shard channel: `mpsc::channel<ShardCommand>`
- held by `ShardHandle`, cloneable. The **`CommandWorker`** (one tokio
  task per shard, lives in `shard/worker.rs`) drains it via
  `tokio::select!`. The **`DrainWorker`** (one OS thread per shard,
  same file) is a separate primitive and never touches the command
  channel — it sleeps on the Gate.
- **Wake**: `ShardHandle.send().await` is sufficient; we no longer use
  `try_recv() + thread::park()` on the command path.
- **Direction**: always transport → shard. Never shard → shard.

### Reply channel: `oneshot::channel`
- `Sender` travels with the command; `Receiver` is `.await`ed by the caller.
- **Never store a oneshot::Sender.** Consumed within the handler.

## SHARD ROUTING
Deterministic: `stream_id.raw() % shard_count`.
1. **All operations for a stream → same shard.** (Publish, claim, ack).
2. **Router never buffers or inspects payloads.** forwards immediately.

## SHUTDOWN PROTOCOL
1. `Server::shutdown()` sends `ShardCommand::Shutdown` to each shard.
2. Shard threads exit naturally. Never `thread.join()` from async; never force-kill.
3. Shutdown is idempotent.

## BACKPRESSURE
When shard channel is full (4096), sender `.await` blocks. **Intentional**.
1. Never `try_send` to drop commands. Never increase capacity to "fix" slowness.

## THREAD NAMING — MANDATORY
All shard threads must be named: `shard-0`, `shard-1`, etc. Never use unnamed threads.

## DRAIN DELIVERY — ON DRAIN THREAD VIA GATE
Delivery runs on a **dedicated drain OS thread** (`drain-N`).

The drain thread blocks on `gate.acquire()` (0% CPU).
```
Drain loop: gate.acquire() → while gate.is_open() { drain_cycle() } → park
Gate opens: publish, ack, nack, subscribe/bind
Gate closes: drain_cycle found nothing to deliver
```

### Gate (`arbitro_common::Gate`)
Wraps `SignalSet` with a single signal.
- `release()`: atomic `fetch_or` + `thread::unpark`.
- `acquire()`: fast-paths on bitmap, falls to `thread::park` (0% CPU).
- `lock()`: atomic `fetch_and` clears the bit.
- **Rule**: `set_worker(thread)` must be called before any `release()`.
- **Rule**: 0% CPU idle (blocks via `thread::park`), coalescing is safe (multiple releases merge).
