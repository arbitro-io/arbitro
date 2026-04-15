---
description: Concurrency rules — pure synchronous library, single-threaded &mut self engine, no async, no tokio, no I/O
---

# CONCURRENCY RULES

ArbitroDB is a **pure synchronous library**. It is NOT a server. It owns no
threads, no async runtime, no I/O, no channels. The engine is `&mut self` —
the Rust borrow checker IS the synchronization. The caller (protocol layer)
decides the thread model, async runtime, and transport.

---

## 1. ENGINE IS `&mut self` — NO LOCKS, NO ATOMICS

```rust
// ✅ EngineContext is &mut — single owner, no locks needed
let reply = engine.publish(&batch);      // sync call, ~200ns
let claimed = engine.claim(&claim);      // sync call, ~200ns
let result = engine.ack(&ack_batch);     // sync call, ~100ns

// ❌ Locking inside engine — architecture violation
fn publish(&mut self, ...) {
    self.graph.lock().unwrap().insert(entry);  // BANNED
}
```

**Rule:** If you need a Mutex inside the engine, you have broken the architecture.
`&mut self` guarantees exclusive access at compile time. No runtime cost.

---

## 2. ENGINE OWNS NO ASYNC, NO THREADS, NO I/O

The engine MUST NOT:
- Import `tokio`, `async-std`, `smol`, or any async runtime
- Use `async fn`, `.await`, `Future`, `Pin`, `Poll`
- Spawn threads or tasks
- Open sockets, files, or any I/O handle
- Call `Instant::now()` — timestamps are passed in by the caller
- Use channels (`mpsc`, `crossbeam`, `flume`)
- Use `Arc`, `Mutex`, `RwLock`, `Condvar` internally

```rust
// ✅ Engine is a pure function: input → output
pub fn publish(&mut self, batch: &PublishBatch) -> RepPublish { ... }
pub fn claim(&mut self, batch: &ClaimBatch) -> &ScratchReply<ClaimedEntry> { ... }
pub fn ack(&mut self, batch: &AckBatch) -> &ScratchReply<AckResult> { ... }

// ❌ BANNED inside engine
async fn publish(&mut self, ...) { ... }           // no async
tokio::spawn(async move { ... });                   // no spawning
std::thread::spawn(|| { ... });                     // no threads
TcpStream::connect("...");                          // no I/O
std::time::Instant::now();                          // no clock
```

---

## 3. CALLER OWNS CONCURRENCY

The protocol layer (NOT part of this library) decides:
- Which async runtime (tokio, io_uring, mio, or none)
- Which transport (TCP, Unix socket, shared memory)
- Which thread model (single, thread-per-core, work-stealing)
- When to poll for events and when to call the engine
- How to frame/parse wire protocol
- When to call `drain_fanout()` and how to deliver notifications

```rust
// Protocol layer drives everything — engine is just a library call
fn process_frame(engine: &mut ArbitroEngine, frame: &[u8]) {
    match frame[0] {
        OP_PUBLISH => {
            let batch = parse_publish(frame);
            let reply = engine.publish(&batch);      // sync, ~200ns
            send_reply(reply.as_bytes());            // ~400ps encode
        }
        OP_ACK => {
            let batch = parse_ack(frame);
            let result = engine.ack(&batch);
            send_reply(result.as_bytes());
        }
    }
}
```

---

## 4. NO SPIN LOOPS

Never use `try_recv` spin loops. This rule applies to the protocol layer
(not the engine, which has no I/O). Use OS-level blocking (epoll, kqueue,
io_uring) or `Notify`-based wakeup.

```rust
// ❌ Spin loop — burns CPU
loop {
    match rx.try_recv() {
        Ok(msg) => handle(msg),
        Err(_) => std::hint::spin_loop(),  // BANNED
    }
}
```

---

## 5. FALSE SHARING PREVENTION

If any struct is shared across threads at the protocol-layer boundary,
it must be `#[repr(C, align(64))]`:

```rust
// ✅ Separate cache lines for independent atomic counters
#[repr(C, align(64))]
pub struct MetricCounter {
    pub count: AtomicU64,
    _pad: [u8; 56],
}
```

Note: Inside the engine core, there is no multi-threaded access.
This rule applies only to types exposed at the boundary.

---

## 6. ATOMIC ORDERING — IF EVER NEEDED AT BOUNDARY

If atomics appear at the protocol-layer boundary (metrics counters),
use the weakest correct ordering. Every `Ordering` that is not `Relaxed`
requires a one-line justification inline.

```rust
// Relaxed: counter only, no ordering dependency
metrics.published.fetch_add(1, Ordering::Relaxed);
```

**`SeqCst` is banned** unless proven necessary — full MFENCE on x86, ~20ns.

---

## 7. TYPES ARE `Send + Sync` WHERE NEEDED

Types that the protocol layer might share across threads must be
`Send + Sync`. Enforce at compile time:

```rust
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PendingNode>();
};
```

- No `Rc<T>` or `RefCell<T>` in types that cross boundaries
- No raw pointers without `// SAFETY:` comment

---

## BANNED CRATES (inside engine)

| Crate | Why banned |
|---|---|
| `tokio` | Engine has no async, no I/O, no threads |
| `async-std`, `smol` | Same — no async runtime |
| `parking_lot` | Engine is single-threaded, no locks needed |
| `crossbeam` | No cross-thread communication inside engine |
| `flume`, `kanal` | No channels inside engine |
| `serde` / `serde_json` | Wire codec is zerocopy, not serialization |
| `tracing` / `log` | Hot path uses scratch buffers, not format strings |
